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

// Manual state refresh contract: one fixed probe request, repeated until the
// upstream returns a complete turn state.
const (
	stateRefreshInput       = "ping"
	stateRefreshMaxAttempts = 5
	stateRefreshTimeout     = 60 * time.Second
	// stateRefreshLogLimit 是详情一次回看的刷新记录条数。
	stateRefreshLogLimit = 20
)

// state refresh failure classifications recorded in the durable refresh log.
const (
	stateRefreshFailureUnauthorized   = "unauthorized"
	stateRefreshFailureUpstream       = "upstream_error"
	stateRefreshFailureLengthMismatch = "length_mismatch"
	stateRefreshFailureTimeout        = "timeout"
	stateRefreshFailureCanceled       = "canceled"
	stateRefreshFailureInternal       = "internal"
)

// CredentialStateRefreshResponse is the captured turn state of one credential.
type CredentialStateRefreshResponse struct {
	TurnState     string `json:"turn_state"`
	Attempts      int    `json:"attempts"`
	RefreshedAtMS int64  `json:"refreshed_at_ms"`
}

// CredentialStateRefreshLogResponse is one durable refresh record. A successful
// record carries the captured state and the probe request that produced it; a
// failed record carries the failure classification and the observed length.
type CredentialStateRefreshLogResponse struct {
	ID          uint   `json:"id"`
	Status      string `json:"status"`
	ErrorCode   string `json:"error_code,omitempty"`
	TurnState   string `json:"turn_state"`
	StateLength int    `json:"state_length"`
	Attempts    int    `json:"attempts"`
	HTTPStatus  *int   `json:"http_status,omitempty"`
	Model       string `json:"model"`
	Input       string `json:"input"`
	ProxyURL    string `json:"proxy_url"`
	BaseURL     string `json:"base_url"`
	DurationMS  int64  `json:"duration_ms"`
	CreatedAtMS int64  `json:"created_at_ms"`
}

// CredentialStateResponse is the stored turn state together with the recent
// refresh records of one credential.
type CredentialStateResponse struct {
	TurnState       string `json:"turn_state"`
	TurnStateLength int    `json:"turn_state_length"`
	// RequiredLength 是保留状态所需的完整长度，供界面说明保留规则。
	RequiredLength int                                 `json:"required_length"`
	RefreshedAtMS  *int64                              `json:"refreshed_at_ms"`
	Logs           []CredentialStateRefreshLogResponse `json:"logs"`
}

type stateRefreshFlight struct {
	done   chan struct{}
	result CredentialStateRefreshResponse
	err    error
}

// stateRefreshOutcome is the recorded shape of one refresh probe sequence.
type stateRefreshOutcome struct {
	Model       string
	Input       string
	ProxyURL    string
	BaseURL     string
	StateLength int
	HTTPStatus  *int
}

// stateRefreshLengthError reports a probe result that is not a complete turn
// state. Only complete states are retained.
type stateRefreshLengthError struct{ observed int }

func (err stateRefreshLengthError) Error() string {
	return fmt.Sprintf("captured turn state length %d does not match %d", err.observed, execution.CodexTurnStateLength)
}

// GetCredentialState reads the stored turn state, its record time, and the
// recent refresh records of one subscription credential.
func (s *Service) GetCredentialState(
	ctx context.Context,
	groupID uint,
	credentialID uint,
) (CredentialStateResponse, error) {
	if groupID == 0 || credentialID == 0 {
		return CredentialStateResponse{}, app_errors.ErrValidation
	}
	response := CredentialStateResponse{
		RequiredLength: execution.CodexTurnStateLength,
		Logs:           []CredentialStateRefreshLogResponse{},
	}
	err := s.withReadSnapshot(ctx, func(tx *gorm.DB) error {
		if _, err := s.loadStateRefreshGroup(tx, groupID); err != nil {
			return err
		}
		var credential models.Credential
		if err := tx.Where("id = ? AND group_id = ?", credentialID, groupID).Take(&credential).Error; err != nil {
			return err
		}
		response.TurnState = credential.TurnState
		response.TurnStateLength = len(credential.TurnState)
		if credential.TurnState != "" && credential.TurnStateRefreshedAtMS > 0 {
			refreshedAtMS := credential.TurnStateRefreshedAtMS
			response.RefreshedAtMS = &refreshedAtMS
		}
		var logs []models.CredentialStateRefreshLog
		if err := tx.Where("group_id = ? AND credential_id = ?", groupID, credentialID).
			Order("created_at_ms DESC").Order("id DESC").Limit(stateRefreshLogLimit).Find(&logs).Error; err != nil {
			return err
		}
		for _, row := range logs {
			response.Logs = append(response.Logs, credentialStateRefreshLogResponse(row))
		}
		return nil
	})
	if err != nil {
		return CredentialStateResponse{}, mapStateRefreshReadError(err)
	}
	return response, nil
}

// RefreshCredentialState probes one credential for the codex turn state and
// persists the value as a fixed per-credential request header. Concurrent
// refreshes of the same credential join the in-flight probe.
func (s *Service) RefreshCredentialState(
	ctx context.Context,
	groupID uint,
	credentialID uint,
) (CredentialStateRefreshResponse, error) {
	if groupID == 0 || credentialID == 0 {
		return CredentialStateRefreshResponse{}, app_errors.ErrValidation
	}
	if ctx == nil {
		ctx = context.Background()
	}
	s.stateMu.Lock()
	if existing := s.stateFlights[credentialID]; existing != nil {
		s.stateMu.Unlock()
		select {
		case <-ctx.Done():
			return CredentialStateRefreshResponse{}, ctx.Err()
		case <-existing.done:
			return existing.result, existing.err
		}
	}
	if s.stateFlights == nil {
		s.stateFlights = make(map[uint]*stateRefreshFlight)
	}
	flight := &stateRefreshFlight{done: make(chan struct{})}
	s.stateFlights[credentialID] = flight
	s.stateMu.Unlock()
	defer func() {
		s.stateMu.Lock()
		if s.stateFlights[credentialID] == flight {
			delete(s.stateFlights, credentialID)
		}
		close(flight.done)
		s.stateMu.Unlock()
	}()
	flight.result, flight.err = s.refreshCredentialStateOnce(ctx, groupID, credentialID)
	return flight.result, flight.err
}

func (s *Service) refreshCredentialStateOnce(
	ctx context.Context,
	groupID uint,
	credentialID uint,
) (CredentialStateRefreshResponse, error) {
	group, credential, err := s.loadStateRefreshTarget(ctx, groupID, credentialID)
	if err != nil {
		return CredentialStateRefreshResponse{}, err
	}
	channelID := channel.ID(group.ChannelID)
	// The whole refresh, including any credential token refresh, uses the state
	// proxy policy.
	network, err := s.stateNetworkContext(ctx, s.db)
	if err != nil {
		return CredentialStateRefreshResponse{}, err
	}
	ctx = subscriptionruntime.WithNetworkContext(ctx, network)
	transport, err := outboundproxy.ResolveTransport(network.Proxy)
	if err != nil {
		return CredentialStateRefreshResponse{}, app_errors.ErrInternalServer
	}
	preparedCredential, err := s.prepareStoredSubscriptionCredential(ctx, group, credential)
	if err != nil {
		return CredentialStateRefreshResponse{}, err
	}
	target, err := s.resolveSubscriptionTarget(channelID, group.Params)
	if err != nil {
		return CredentialStateRefreshResponse{}, app_errors.ErrInternalServer
	}
	if s.probeSubscriptionTurnState == nil {
		return CredentialStateRefreshResponse{}, app_errors.ErrValidation
	}
	request := subscriptionruntime.StateProbeRequest{
		Model:                execution.CodexTurnStateModel,
		Input:                stateRefreshInput,
		ProxyURL:             stateProbeProxyURL(transport),
		ProxyFromEnvironment: transport.FromEnvironment,
	}
	outcome := stateRefreshOutcome{
		Model:    request.Model,
		Input:    request.Input,
		ProxyURL: request.ProxyURL,
	}
	if baseURL, baseURLErr := target.BaseURL(); baseURLErr == nil {
		outcome.BaseURL = baseURL
	}
	startedAt := s.now()
	var lastErr error
	attempts := 0
	for attempts < stateRefreshMaxAttempts {
		if err := ctx.Err(); err != nil {
			return CredentialStateRefreshResponse{}, err
		}
		attempts++
		attemptContext, cancel := context.WithTimeout(ctx, stateRefreshTimeout)
		probed, probeErr := s.probeSubscriptionTurnState(attemptContext, channelID, preparedCredential, target, request)
		cancel()
		if probeErr != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return CredentialStateRefreshResponse{}, ctxErr
			}
			lastErr = probeErr
			outcome.StateLength = 0
			outcome.HTTPStatus = upstreamStatusCode(probeErr)
			continue
		}
		lastErr = nil
		outcome.StateLength = len(probed.TurnState)
		outcome.HTTPStatus = nil
		if len(probed.TurnState) == execution.CodexTurnStateLength {
			refreshedAtMS := s.now().UTC().UnixMilli()
			record := models.CredentialStateRefreshLog{
				GroupID:      groupID,
				CredentialID: credentialID,
				Status:       models.CredentialStateRefreshSucceeded,
				TurnState:    probed.TurnState,
				StateLength:  len(probed.TurnState),
				Attempts:     attempts,
				Model:        outcome.Model,
				Input:        outcome.Input,
				ProxyURL:     outcome.ProxyURL,
				BaseURL:      outcome.BaseURL,
				DurationMS:   stateRefreshDurationMS(s.now(), startedAt),
				CreatedAtMS:  refreshedAtMS,
			}
			if err := s.persistCredentialTurnState(ctx, groupID, credentialID, probed.TurnState, refreshedAtMS, record); err != nil {
				return CredentialStateRefreshResponse{}, err
			}
			return CredentialStateRefreshResponse{
				TurnState:     probed.TurnState,
				Attempts:      attempts,
				RefreshedAtMS: refreshedAtMS,
			}, nil
		}
		lastErr = stateRefreshLengthError{observed: len(probed.TurnState)}
	}
	if lastErr == nil {
		lastErr = errors.New("turn state probe produced no result")
	}
	classified := stateRefreshError(lastErr)
	if err := s.recordStateRefreshFailure(ctx, groupID, credentialID, outcome, attempts, lastErr, startedAt); err != nil {
		return CredentialStateRefreshResponse{}, fmt.Errorf("%w: state refresh log: %v", classified, err)
	}
	return CredentialStateRefreshResponse{}, classified
}

// loadStateRefreshTarget loads the subscription group and credential that own
// one manual state refresh. The channel must expose the state probe capability.
func (s *Service) loadStateRefreshTarget(
	ctx context.Context,
	groupID uint,
	credentialID uint,
) (models.Group, models.Credential, error) {
	var group models.Group
	var credential models.Credential
	err := s.withReadSnapshot(ctx, func(tx *gorm.DB) error {
		loaded, err := s.loadStateRefreshGroup(tx, groupID)
		group = loaded
		if err != nil {
			return err
		}
		return tx.Where("id = ? AND group_id = ?", credentialID, groupID).Take(&credential).Error
	})
	if err != nil {
		return group, credential, mapStateRefreshReadError(err)
	}
	return group, credential, nil
}

// loadStateRefreshGroup loads the group that owns a manual state refresh and
// verifies its channel exposes the state probe capability.
func (s *Service) loadStateRefreshGroup(tx *gorm.DB, groupID uint) (models.Group, error) {
	var group models.Group
	if err := tx.Take(&group, groupID).Error; err != nil {
		return group, err
	}
	if normalizeGroupConnectionType(group.ConnectionType) != models.ConnectionTypeSubscription {
		return group, app_errors.ErrValidation
	}
	if _, supported := s.subscriptions.StateProbe(channel.ID(group.ChannelID)); !supported {
		return group, app_errors.ErrValidation
	}
	return group, nil
}

// mapStateRefreshReadError maps a failed state read onto the public error
// contract.
func mapStateRefreshReadError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return app_errors.ErrResourceNotFound
	}
	var apiErr *app_errors.APIError
	if errors.As(err, &apiErr) {
		return err
	}
	return app_errors.ParseDBError(err)
}

// persistCredentialTurnState durably stores the captured value and its record
// time, and appends the refresh record in the same transaction so the stored
// state, its record time, and its log entry always agree. Turn state is mutable
// runtime state, so publication never invalidates in-flight requests.
func (s *Service) persistCredentialTurnState(
	ctx context.Context,
	groupID uint,
	credentialID uint,
	turnState string,
	refreshedAtMS int64,
	record models.CredentialStateRefreshLog,
) error {
	return s.writeCredentialConfig(ctx, groupID, credentialID, func(tx *gorm.DB) error {
		result := tx.Model(&models.Credential{}).
			Where("id = ? AND group_id = ?", credentialID, groupID).
			Updates(map[string]any{
				"turn_state":                 turnState,
				"turn_state_refreshed_at_ms": refreshedAtMS,
			})
		if result.Error != nil {
			return result.Error
		}
		if result.RowsAffected == 0 {
			return app_errors.ErrResourceNotFound
		}
		return tx.Create(&record).Error
	}, func() error {
		if !s.registry.SetCredentialTurnState(credentialID, turnState) {
			return fmt.Errorf("publish credential turn state: credential %d is unavailable", credentialID)
		}
		return nil
	})
}

// recordStateRefreshFailure appends the durable record of one failed refresh.
// The classified refresh error stays the primary result; a failed log write
// only adds context to it.
func (s *Service) recordStateRefreshFailure(
	ctx context.Context,
	groupID uint,
	credentialID uint,
	outcome stateRefreshOutcome,
	attempts int,
	cause error,
	startedAt time.Time,
) error {
	row := models.CredentialStateRefreshLog{
		GroupID:      groupID,
		CredentialID: credentialID,
		Status:       models.CredentialStateRefreshFailed,
		ErrorCode:    stateRefreshFailureCode(cause),
		StateLength:  outcome.StateLength,
		Attempts:     attempts,
		HTTPStatus:   outcome.HTTPStatus,
		Model:        outcome.Model,
		Input:        outcome.Input,
		ProxyURL:     outcome.ProxyURL,
		BaseURL:      outcome.BaseURL,
		DurationMS:   stateRefreshDurationMS(s.now(), startedAt),
		CreatedAtMS:  s.now().UTC().UnixMilli(),
	}
	if err := s.db.WithContext(ctx).Create(&row).Error; err != nil {
		return app_errors.ParseDBError(err)
	}
	return nil
}

func credentialStateRefreshLogResponse(row models.CredentialStateRefreshLog) CredentialStateRefreshLogResponse {
	return CredentialStateRefreshLogResponse{
		ID:          row.ID,
		Status:      string(row.Status),
		ErrorCode:   row.ErrorCode,
		TurnState:   row.TurnState,
		StateLength: row.StateLength,
		Attempts:    row.Attempts,
		HTTPStatus:  row.HTTPStatus,
		Model:       row.Model,
		Input:       row.Input,
		ProxyURL:    row.ProxyURL,
		BaseURL:     row.BaseURL,
		DurationMS:  row.DurationMS,
		CreatedAtMS: row.CreatedAtMS,
	}
}

// stateRefreshFailureCode classifies one probe failure for the durable log.
func stateRefreshFailureCode(err error) string {
	var lengthErr stateRefreshLengthError
	if errors.As(err, &lengthErr) {
		return stateRefreshFailureLengthMismatch
	}
	if status := upstreamStatusCode(err); status != nil {
		if *status == http.StatusUnauthorized || *status == http.StatusForbidden {
			return stateRefreshFailureUnauthorized
		}
		return stateRefreshFailureUpstream
	}
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return stateRefreshFailureTimeout
	case errors.Is(err, context.Canceled):
		return stateRefreshFailureCanceled
	default:
		return stateRefreshFailureInternal
	}
}

// upstreamStatusCode extracts the provider-neutral upstream status of one probe
// failure.
func upstreamStatusCode(err error) *int {
	var upstream *subscriptionruntime.UpstreamHTTPError
	if !errors.As(err, &upstream) || upstream == nil {
		return nil
	}
	status := upstream.StatusCode
	return &status
}

// stateRefreshDurationMS 把一次刷新的耗时收敛到非负毫秒。
func stateRefreshDurationMS(now time.Time, startedAt time.Time) int64 {
	duration := now.Sub(startedAt).Milliseconds()
	if duration < 0 {
		return 0
	}
	return duration
}

// stateProbeProxyURL maps the resolved state proxy onto the provider's
// transport sentinels: "direct" for a direct connection, empty for the process
// environment, and the explicit endpoint otherwise.
func stateProbeProxyURL(transport outboundproxy.ProxyTransport) string {
	switch {
	case transport.Direct:
		return "direct"
	case transport.FromEnvironment:
		return ""
	default:
		return transport.URL
	}
}

// stateRefreshError classifies one failed state refresh without leaking the
// upstream response body.
func stateRefreshError(err error) error {
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
