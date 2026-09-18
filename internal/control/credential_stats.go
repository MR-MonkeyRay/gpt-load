package control

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"gorm.io/gorm"

	"gpt-load/internal/channel"
	"gpt-load/internal/execution"
	"gpt-load/internal/outboundproxy"
	app_errors "gpt-load/internal/platform/errors"
	"gpt-load/internal/storage/models"
	subscriptionruntime "gpt-load/internal/subscription/runtime"
)

// Manual stats refresh contract: one fixed probe request, repeated until the
// upstream returns a complete turn state.
const (
	statsRefreshInput       = "ping"
	statsRefreshMaxAttempts = 5
	statsRefreshTimeout     = 60 * time.Second
)

// CredentialStatsRefreshResponse is the captured turn state of one credential.
type CredentialStatsRefreshResponse struct {
	TurnState     string `json:"turn_state"`
	Attempts      int    `json:"attempts"`
	RefreshedAtMS int64  `json:"refreshed_at_ms"`
}

type statsRefreshFlight struct {
	done   chan struct{}
	result CredentialStatsRefreshResponse
	err    error
}

// RefreshCredentialStats probes one credential for the codex turn state and
// persists the value as a fixed per-credential request header. Concurrent
// refreshes of the same credential join the in-flight probe.
func (s *Service) RefreshCredentialStats(
	ctx context.Context,
	groupID uint,
	credentialID uint,
) (CredentialStatsRefreshResponse, error) {
	if groupID == 0 || credentialID == 0 {
		return CredentialStatsRefreshResponse{}, app_errors.ErrValidation
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.statsMu.Lock()
	if existing := s.statsFlights[credentialID]; existing != nil {
		s.statsMu.Unlock()
		select {
		case <-ctx.Done():
			return CredentialStatsRefreshResponse{}, ctx.Err()
		case <-existing.done:
			return existing.result, existing.err
		}
	}
	if s.statsFlights == nil {
		s.statsFlights = make(map[uint]*statsRefreshFlight)
	}
	flight := &statsRefreshFlight{done: make(chan struct{})}
	s.statsFlights[credentialID] = flight
	s.statsMu.Unlock()
	defer func() {
		s.statsMu.Lock()
		if s.statsFlights[credentialID] == flight {
			delete(s.statsFlights, credentialID)
		}
		close(flight.done)
		s.statsMu.Unlock()
	}()
	flight.result, flight.err = s.refreshCredentialStatsOnce(ctx, groupID, credentialID)
	return flight.result, flight.err
}

func (s *Service) refreshCredentialStatsOnce(
	ctx context.Context,
	groupID uint,
	credentialID uint,
) (CredentialStatsRefreshResponse, error) {
	group, credential, err := s.loadStatsRefreshTarget(ctx, groupID, credentialID)
	if err != nil {
		return CredentialStatsRefreshResponse{}, err
	}
	channelID := channel.ID(group.ChannelID)
	// The whole refresh, including any credential token refresh, uses the stats
	// proxy policy.
	network, err := s.statsNetworkContext(ctx, s.db)
	if err != nil {
		return CredentialStatsRefreshResponse{}, err
	}
	ctx = subscriptionruntime.WithNetworkContext(ctx, network)
	transport, err := outboundproxy.ResolveTransport(network.Proxy)
	if err != nil {
		return CredentialStatsRefreshResponse{}, app_errors.ErrInternalServer
	}
	preparedCredential, err := s.prepareStoredSubscriptionCredential(ctx, group, credential)
	if err != nil {
		return CredentialStatsRefreshResponse{}, err
	}
	target, err := s.resolveSubscriptionTarget(channelID, group.Params)
	if err != nil {
		return CredentialStatsRefreshResponse{}, app_errors.ErrInternalServer
	}
	if s.probeSubscriptionTurnState == nil {
		return CredentialStatsRefreshResponse{}, app_errors.ErrValidation
	}
	request := subscriptionruntime.StatsProbeRequest{
		Model:                execution.CodexTurnStateModel,
		Input:                statsRefreshInput,
		ProxyURL:             statsProbeProxyURL(transport),
		ProxyFromEnvironment: transport.FromEnvironment,
	}
	var lastErr error
	attempts := 0
	for attempts < statsRefreshMaxAttempts {
		if err := ctx.Err(); err != nil {
			return CredentialStatsRefreshResponse{}, err
		}
		attempts++
		attemptContext, cancel := context.WithTimeout(ctx, statsRefreshTimeout)
		probed, probeErr := s.probeSubscriptionTurnState(attemptContext, channelID, preparedCredential, target, request)
		cancel()
		if probeErr != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return CredentialStatsRefreshResponse{}, ctxErr
			}
			lastErr = probeErr
			continue
		}
		lastErr = nil
		if len(probed.TurnState) == execution.CodexTurnStateLength {
			refreshedAtMS := s.now().UTC().UnixMilli()
			if err := s.persistCredentialTurnState(ctx, groupID, credentialID, probed.TurnState); err != nil {
				return CredentialStatsRefreshResponse{}, err
			}
			return CredentialStatsRefreshResponse{
				TurnState:     probed.TurnState,
				Attempts:      attempts,
				RefreshedAtMS: refreshedAtMS,
			}, nil
		}
		lastErr = fmt.Errorf("captured turn state length %d does not match %d", len(probed.TurnState), execution.CodexTurnStateLength)
	}
	if lastErr == nil {
		lastErr = errors.New("turn state probe produced no result")
	}
	return CredentialStatsRefreshResponse{}, statsRefreshError(lastErr)
}

// loadStatsRefreshTarget loads the subscription group and credential that own
// one manual stats refresh. The channel must expose the stats probe capability.
func (s *Service) loadStatsRefreshTarget(
	ctx context.Context,
	groupID uint,
	credentialID uint,
) (models.Group, models.Credential, error) {
	var group models.Group
	var credential models.Credential
	err := s.withReadSnapshot(ctx, func(tx *gorm.DB) error {
		if err := tx.Take(&group, groupID).Error; err != nil {
			return err
		}
		if normalizeGroupConnectionType(group.ConnectionType) != models.ConnectionTypeSubscription {
			return app_errors.ErrValidation
		}
		if _, supported := s.subscriptions.StatsProbe(channel.ID(group.ChannelID)); !supported {
			return app_errors.ErrValidation
		}
		return tx.Where("id = ? AND group_id = ?", credentialID, groupID).Take(&credential).Error
	})
	if err != nil {
		if errors.Is(err, gorm.ErrRecordNotFound) {
			return group, credential, app_errors.ErrResourceNotFound
		}
		var apiErr *app_errors.APIError
		if errors.As(err, &apiErr) {
			return group, credential, err
		}
		return group, credential, app_errors.ParseDBError(err)
	}
	return group, credential, nil
}

// persistCredentialTurnState durably stores the captured value and publishes it
// to the runtime credential registry. Turn state is mutable runtime state, so
// publication never invalidates in-flight requests.
func (s *Service) persistCredentialTurnState(
	ctx context.Context,
	groupID uint,
	credentialID uint,
	turnState string,
) error {
	return s.writeCredentialConfig(ctx, groupID, credentialID, func(tx *gorm.DB) error {
		result := tx.Model(&models.Credential{}).
			Where("id = ? AND group_id = ?", credentialID, groupID).
			Update("turn_state", turnState)
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return app_errors.ErrResourceNotFound
		}
		return nil
	}, func() error {
		if !s.registry.SetCredentialTurnState(credentialID, turnState) {
			return fmt.Errorf("publish credential turn state: credential %d is unavailable", credentialID)
		}
		return nil
	})
}

// statsProbeProxyURL maps the resolved stats proxy onto the provider's
// transport sentinels: "direct" for a direct connection, empty for the process
// environment, and the explicit endpoint otherwise.
func statsProbeProxyURL(transport outboundproxy.ProxyTransport) string {
	switch {
	case transport.Direct:
		return "direct"
	case transport.FromEnvironment:
		return ""
	default:
		return transport.URL
	}
}

// statsRefreshError classifies one failed stats refresh without leaking the
// upstream response body.
func statsRefreshError(err error) error {
	var upstream *subscriptionruntime.UpstreamHTTPError
	if errors.As(err, &upstream) && upstream != nil {
		switch upstream.StatusCode {
		case http.StatusUnauthorized, http.StatusForbidden:
			return app_errors.ErrCredentialReauthorizationRequired
		default:
			return app_errors.ErrBadGateway
		}
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return app_errors.ErrBadGateway
}
