package control

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"

	"gpt-load/internal/channel"
	"gpt-load/internal/execution"
	"gpt-load/internal/outboundproxy"
	app_errors "gpt-load/internal/platform/errors"
	"gpt-load/internal/platform/utils"
	"gpt-load/internal/storage/models"
	subscriptionruntime "gpt-load/internal/subscription/runtime"
)

// Manual state refresh contract: one fixed probe request repeated until the
// upstream returns a complete turn state. The run is a background task because
// a capture may take arbitrarily long; the operator stops it explicitly.
const (
	stateRefreshInput = "ping"
	// defaultStateRefreshInterval 是两次探测之间的间隔，避免持续失败时冲击上游。
	defaultStateRefreshInterval = time.Second
	// stateRefreshTimeout 是单次探测的上限；探测拿到响应头即断开，不会等待流结束。
	stateRefreshTimeout = 60 * time.Second
	// stateRefreshStopTimeout 是停止刷新时等待在途探测收敛的上限。
	stateRefreshStopTimeout = 5 * time.Second
	// stateRefreshLogLimit 是详情一次回看的刷新记录条数。
	stateRefreshLogLimit = 50
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

// CredentialStateRefreshLogResponse is one durable refresh record: one probe
// request and its result. A successful record carries the captured state and
// the probe request that produced it; a failed record carries the failure
// classification and the observed length.
type CredentialStateRefreshLogResponse struct {
	ID          uint   `json:"id"`
	Status      string `json:"status"`
	ErrorCode   string `json:"error_code,omitempty"`
	TurnState   string `json:"turn_state"`
	StateLength int    `json:"state_length"`
	// Attempts 是这次探测在其刷新运行中的序号，从 1 开始。
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
	RequiredLength int    `json:"required_length"`
	RefreshedAtMS  *int64 `json:"refreshed_at_ms"`
	// ExpiresAtMS 是保留状态自身携带的有效期；为空表示该值无法解析出有效期。
	ExpiresAtMS *int64 `json:"expires_at_ms"`
	// Running 表示该凭据当前是否有后台刷新在运行。
	Running bool                                `json:"running"`
	Logs    []CredentialStateRefreshLogResponse `json:"logs"`
}

// stateRefreshRun is one in-flight background refresh of a single credential.
type stateRefreshRun struct {
	cancel context.CancelFunc
	done   chan struct{}
}

// stateRefreshOutcome is the recorded shape of one refresh probe.
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

// GetCredentialState reads the stored turn state, its record time, its replay
// expiry, whether a refresh is running, and the recent refresh records of one
// subscription credential.
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
	// 先取运行态：运行结束会先写完记录再摘除运行标记，因此 running=false 之后的
	// 读取一定能看到最后一次探测的记录。
	response.Running = s.stateRefreshRunning(credentialID)
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
		if expiresAtMS := execution.TurnStateExpiryMS(credential.TurnState); expiresAtMS > 0 {
			response.ExpiresAtMS = &expiresAtMS
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

// StartCredentialStateRefresh starts the background refresh run of one
// credential. The run repeats the probe until it captures a complete turn
// state, cannot prepare another attempt, or is stopped. Starting a credential
// that is already refreshing is idempotent.
func (s *Service) StartCredentialStateRefresh(
	ctx context.Context,
	groupID uint,
	credentialID uint,
) (CredentialStateResponse, error) {
	if groupID == 0 || credentialID == 0 {
		return CredentialStateResponse{}, app_errors.ErrValidation
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// 先校验目标：不可刷新的凭据必须同步报错，而不是在后台静默失败。
	if _, _, err := s.loadStateRefreshTarget(ctx, groupID, credentialID); err != nil {
		return CredentialStateResponse{}, err
	}
	s.stateMu.Lock()
	if _, running := s.stateRuns[credentialID]; !running {
		if s.stateRuns == nil {
			s.stateRuns = make(map[uint]*stateRefreshRun)
		}
		// 刷新必须活过发起它的请求，但保留请求上下文中的取值。
		runContext, cancel := context.WithCancel(context.WithoutCancel(ctx))
		run := &stateRefreshRun{cancel: cancel, done: make(chan struct{})}
		s.stateRuns[credentialID] = run
		go s.runCredentialStateRefresh(runContext, groupID, credentialID, run)
	}
	s.stateMu.Unlock()
	return s.GetCredentialState(ctx, groupID, credentialID)
}

// StopCredentialStateRefresh cancels the in-flight refresh run of one
// credential and waits for its probe to converge, so the returned snapshot
// already carries the closing record. Stopping an idle credential is a no-op.
func (s *Service) StopCredentialStateRefresh(
	ctx context.Context,
	groupID uint,
	credentialID uint,
) (CredentialStateResponse, error) {
	if groupID == 0 || credentialID == 0 {
		return CredentialStateResponse{}, app_errors.ErrValidation
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if _, _, err := s.loadStateRefreshTarget(ctx, groupID, credentialID); err != nil {
		return CredentialStateResponse{}, err
	}
	s.stateMu.Lock()
	run := s.stateRuns[credentialID]
	s.stateMu.Unlock()
	if run != nil {
		run.cancel()
		s.waitStateRefresh(ctx, run.done)
	}
	return s.GetCredentialState(ctx, groupID, credentialID)
}

// stopStateRefreshes cancels every in-flight refresh run and waits for them to
// converge, so the control runtime drains before storage closes.
func (s *Service) stopStateRefreshes() {
	s.stateMu.Lock()
	runs := make([]*stateRefreshRun, 0, len(s.stateRuns))
	for _, run := range s.stateRuns {
		runs = append(runs, run)
	}
	s.stateMu.Unlock()
	for _, run := range runs {
		run.cancel()
	}
	deadline := time.NewTimer(stateRefreshStopTimeout)
	defer deadline.Stop()
	for _, run := range runs {
		select {
		case <-run.done:
		case <-deadline.C:
			return
		}
	}
}

// waitStateRefresh waits for one run to finish, bounded by the request context
// and the stop timeout.
func (s *Service) waitStateRefresh(ctx context.Context, done <-chan struct{}) {
	timer := time.NewTimer(stateRefreshStopTimeout)
	defer timer.Stop()
	select {
	case <-done:
	case <-ctx.Done():
	case <-timer.C:
	}
}

func (s *Service) stateRefreshRunning(credentialID uint) bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	_, running := s.stateRuns[credentialID]
	return running
}

// runCredentialStateRefresh repeats one probe attempt until the run captures a
// complete turn state, cannot prepare another attempt, or is stopped.
func (s *Service) runCredentialStateRefresh(
	ctx context.Context,
	groupID uint,
	credentialID uint,
	run *stateRefreshRun,
) {
	defer func() {
		s.stateMu.Lock()
		if s.stateRuns[credentialID] == run {
			delete(s.stateRuns, credentialID)
		}
		close(run.done)
		s.stateMu.Unlock()
	}()
	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			return
		}
		if stop := s.probeCredentialTurnState(ctx, groupID, credentialID, attempt); stop {
			return
		}
		timer := time.NewTimer(s.stateRefreshDelay())
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// stateRefreshDelay is the pause between two probe attempts.
func (s *Service) stateRefreshDelay() time.Duration {
	if s.stateRefreshInterval <= 0 {
		return defaultStateRefreshInterval
	}
	return s.stateRefreshInterval
}

// stateRefreshProbe is everything one probe attempt needs after preparation.
type stateRefreshProbe struct {
	outcome    stateRefreshOutcome
	channelID  channel.ID
	credential subscriptionruntime.Credential
	target     subscriptionruntime.Target
	request    subscriptionruntime.StateProbeRequest
}

// probeCredentialTurnState runs and records one probe attempt. It reports
// whether the run must stop: after a capture, after a failure that makes the
// target unrefreshable, after a failed record write, or on cancellation.
func (s *Service) probeCredentialTurnState(
	ctx context.Context,
	groupID uint,
	credentialID uint,
	attempt int,
) bool {
	startedAt := s.now()
	probe, err := s.prepareStateRefreshProbe(ctx, groupID, credentialID)
	if err != nil {
		// 停止发生在准备阶段：这次探测没有发出请求，不留档。
		if ctx.Err() != nil {
			return true
		}
		// 目标无法刷新时重试没有意义，记录后结束运行。
		s.recordAttemptFailure(ctx, groupID, credentialID, fixedStateRefreshOutcome(), attempt, err, startedAt)
		return true
	}
	attemptContext, cancel := context.WithTimeout(ctx, stateRefreshTimeout)
	probed, probeErr := s.probeSubscriptionTurnState(attemptContext, probe.channelID, probe.credential, probe.target, probe.request)
	cancel()
	if probeErr != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			// 运行被停止：这次探测的结果就是取消，仍然留档。
			probeErr = ctxErr
		} else {
			probe.outcome.HTTPStatus = upstreamStatusCode(probeErr)
		}
		stop := s.recordAttemptFailure(ctx, groupID, credentialID, probe.outcome, attempt, probeErr, startedAt)
		// 凭据被上游拒绝时重试不会成功，其余上游错误继续探测。
		return stop || stateRefreshFatalProbeError(probeErr)
	}
	probe.outcome.StateLength = len(probed.TurnState)
	if len(probed.TurnState) != execution.CodexTurnStateLength {
		return s.recordAttemptFailure(ctx, groupID, credentialID, probe.outcome, attempt,
			stateRefreshLengthError{observed: len(probed.TurnState)}, startedAt)
	}
	refreshedAtMS := s.now().UTC().UnixMilli()
	record := models.CredentialStateRefreshLog{
		GroupID:      groupID,
		CredentialID: credentialID,
		Status:       models.CredentialStateRefreshSucceeded,
		TurnState:    probed.TurnState,
		StateLength:  len(probed.TurnState),
		Attempts:     attempt,
		Model:        probe.outcome.Model,
		Input:        probe.outcome.Input,
		ProxyURL:     probe.outcome.ProxyURL,
		BaseURL:      probe.outcome.BaseURL,
		DurationMS:   stateRefreshDurationMS(s.now(), startedAt),
		CreatedAtMS:  refreshedAtMS,
	}
	if err := s.persistCredentialTurnState(ctx, groupID, credentialID, probed.TurnState, refreshedAtMS, record); err != nil {
		s.logStateRefreshFailure(groupID, credentialID, err)
	}
	return true
}

// fixedStateRefreshOutcome is the identity of the fixed probe request, known
// before any preparation step runs.
func fixedStateRefreshOutcome() stateRefreshOutcome {
	return stateRefreshOutcome{Model: execution.CodexTurnStateModel, Input: stateRefreshInput}
}

// prepareStateRefreshProbe resolves everything one probe attempt needs: the
// target, the state proxy policy, the prepared credential, and the fixed probe
// request. The whole probe, including any credential token refresh, uses the
// state proxy policy; the captured value is replayed on the direct data path.
func (s *Service) prepareStateRefreshProbe(
	ctx context.Context,
	groupID uint,
	credentialID uint,
) (stateRefreshProbe, error) {
	probe := stateRefreshProbe{outcome: fixedStateRefreshOutcome()}
	group, credential, err := s.loadStateRefreshTarget(ctx, groupID, credentialID)
	if err != nil {
		return probe, err
	}
	probe.channelID = channel.ID(group.ChannelID)
	network, err := s.stateNetworkContext(ctx, s.db)
	if err != nil {
		return probe, err
	}
	ctx = subscriptionruntime.WithNetworkContext(ctx, network)
	transport, err := outboundproxy.ResolveTransport(network.Proxy)
	if err != nil {
		return probe, app_errors.ErrInternalServer
	}
	preparedCredential, err := s.prepareStoredSubscriptionCredential(ctx, group, credential)
	if err != nil {
		return probe, err
	}
	target, err := s.resolveSubscriptionTarget(probe.channelID, group.Params)
	if err != nil {
		return probe, app_errors.ErrInternalServer
	}
	if s.probeSubscriptionTurnState == nil {
		return probe, app_errors.ErrValidation
	}
	probe.credential = preparedCredential
	probe.target = target
	probe.request = subscriptionruntime.StateProbeRequest{
		Model:                probe.outcome.Model,
		Input:                probe.outcome.Input,
		ProxyURL:             stateProbeProxyURL(transport),
		ProxyFromEnvironment: transport.FromEnvironment,
	}
	probe.outcome.ProxyURL = probe.request.ProxyURL
	if baseURL, baseURLErr := target.BaseURL(); baseURLErr == nil {
		probe.outcome.BaseURL = baseURL
	}
	return probe, nil
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

// recordAttemptFailure appends the durable record of one failed probe attempt.
// It reports whether the run must stop, which happens when the record cannot be
// written: storage trouble would otherwise hide every later result.
func (s *Service) recordAttemptFailure(
	ctx context.Context,
	groupID uint,
	credentialID uint,
	outcome stateRefreshOutcome,
	attempt int,
	cause error,
	startedAt time.Time,
) bool {
	if err := s.recordStateRefreshFailure(ctx, groupID, credentialID, outcome, attempt, cause, startedAt); err != nil {
		s.logStateRefreshFailure(groupID, credentialID, err)
		return true
	}
	return false
}

// recordStateRefreshFailure appends the durable record of one failed probe
// attempt. The record survives cancellation of the run that produced it.
func (s *Service) recordStateRefreshFailure(
	ctx context.Context,
	groupID uint,
	credentialID uint,
	outcome stateRefreshOutcome,
	attempt int,
	cause error,
	startedAt time.Time,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	row := models.CredentialStateRefreshLog{
		GroupID:      groupID,
		CredentialID: credentialID,
		Status:       models.CredentialStateRefreshFailed,
		ErrorCode:    stateRefreshFailureCode(cause),
		StateLength:  outcome.StateLength,
		Attempts:     attempt,
		HTTPStatus:   outcome.HTTPStatus,
		Model:        outcome.Model,
		Input:        outcome.Input,
		ProxyURL:     outcome.ProxyURL,
		BaseURL:      outcome.BaseURL,
		DurationMS:   stateRefreshDurationMS(s.now(), startedAt),
		CreatedAtMS:  s.now().UTC().UnixMilli(),
	}
	if err := s.db.WithContext(context.WithoutCancel(ctx)).Create(&row).Error; err != nil {
		return app_errors.ParseDBError(err)
	}
	return nil
}

// logStateRefreshFailure reports a background refresh failure that no request
// can return to its caller.
func (s *Service) logStateRefreshFailure(groupID uint, credentialID uint, err error) {
	utils.LogPlaneBestEffort(
		logrus.StandardLogger(),
		logrus.WarnLevel,
		utils.LogPlaneControl,
		logrus.Fields{"group_id": groupID, "credential_id": credentialID},
		fmt.Sprintf("State refresh run stopped: %v", err),
	)
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

// stateRefreshFatalProbeError reports a probe failure that retrying cannot fix:
// the upstream rejected the credential itself.
func stateRefreshFatalProbeError(err error) bool {
	status := upstreamStatusCode(err)
	return status != nil && (*status == http.StatusUnauthorized || *status == http.StatusForbidden)
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
	// 准备阶段的失败沿用公共错误分类，界面才能区分“需要重新授权”和“上游异常”。
	var apiErr *app_errors.APIError
	if errors.As(err, &apiErr) && apiErr != nil {
		switch apiErr.Code {
		case app_errors.ErrCredentialReauthorizationRequired.Code, app_errors.ErrCredentialAuthOutcomeUnknown.Code:
			return stateRefreshFailureUnauthorized
		case app_errors.ErrCredentialRefreshTemporarilyUnavailable.Code, app_errors.ErrBadGateway.Code:
			return stateRefreshFailureUpstream
		}
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

// stateRefreshDurationMS clamps one probe duration to a non-negative number of
// milliseconds.
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
