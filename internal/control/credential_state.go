package control

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"net/http"
	"strings"
	"time"

	"github.com/sirupsen/logrus"
	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"gpt-load/internal/channel"
	"gpt-load/internal/execution"
	"gpt-load/internal/outboundproxy"
	app_errors "gpt-load/internal/platform/errors"
	"gpt-load/internal/platform/utils"
	"gpt-load/internal/state"
	"gpt-load/internal/storage/models"
	subscriptionruntime "gpt-load/internal/subscription/runtime"
)

// Manual state refresh contract: one fixed probe request repeated until the
// upstream returns a complete turn state for the selected model. The run is a
// background task because a capture may take arbitrarily long; the operator
// stops it explicitly. A credential may refresh several models at once, so runs
// and captures are always scoped to one model.
const (
	stateRefreshInput = "ping"
	// stateRefreshMinInterval / stateRefreshMaxInterval 是两次探测之间的随机间隔，
	// 既避免持续失败时冲击上游，也避免固定节奏被上游识别。
	stateRefreshMinInterval = time.Second
	stateRefreshMaxInterval = 10 * time.Second
	// stateRefreshRateLimitDelay 是上游返回 429 后的固定休息时间。
	stateRefreshRateLimitDelay = 5 * time.Second
	// stateRefreshTimeout 是单次探测的上限；探测拿到响应头即断开，不会等待流结束。
	stateRefreshTimeout = 60 * time.Second
	// stateRefreshStopTimeout 是停止刷新时等待在途探测收敛的上限。
	stateRefreshStopTimeout = 5 * time.Second
	// stateRefreshLogLimit 是详情一次回看的刷新记录条数。
	stateRefreshLogLimit = 10
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

// CredentialStateRefreshRequest selects the model one refresh run captures the
// turn state for.
type CredentialStateRefreshRequest struct {
	Model string `json:"model"`
}

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

// CredentialStateModelResponse is the turn state retained for one model of one
// credential. A model that is refreshing without a capture yet is reported with
// an empty state so the界面 can show the run in progress.
type CredentialStateModelResponse struct {
	Model       string `json:"model"`
	TurnState   string `json:"turn_state"`
	StateLength int    `json:"state_length"`
	// RefreshedAtMS 是这次捕获的写入时刻；没有捕获时为空。
	RefreshedAtMS *int64 `json:"refreshed_at_ms"`
	// ExpiresAtMS 是这次捕获的有效期：记录时间 + 1 小时；没有记录时间时为空。
	ExpiresAtMS *int64 `json:"expires_at_ms"`
	// Running 表示该模型当前是否有后台刷新在运行。
	Running bool `json:"running"`
}

// CredentialStateResponse is the stored turn state of every refreshed model of
// one credential, together with the recent refresh records of the selected
// model.
type CredentialStateResponse struct {
	// RequiredLength 是保留状态所需的完整长度，供界面说明保留规则。
	RequiredLength int `json:"required_length"`
	// AvailableModels 是该分组配置的模型，也就是可以刷新 State 的模型。
	AvailableModels []string `json:"available_models"`
	// Model 是本次回看的模型；请求未指定时由服务端选出默认模型。
	Model string `json:"model"`
	// Running 表示 Model 当前是否有后台刷新在运行。
	Running bool                                `json:"running"`
	States  []CredentialStateModelResponse      `json:"states"`
	Logs    []CredentialStateRefreshLogResponse `json:"logs"`
}

// stateRefreshKey identifies one background refresh run: turn state is bound to
// a credential and a model, so each pair refreshes on its own.
type stateRefreshKey struct {
	credentialID uint
	model        string
}

// stateRefreshRun is one in-flight background refresh of a single credential
// and model.
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

// GetCredentialState reads the turn state retained for every refreshed model,
// each capture time and replay expiry, whether a refresh is running, and the
// recent refresh records of one subscription credential and model.
func (s *Service) GetCredentialState(
	ctx context.Context,
	groupID uint,
	credentialID uint,
	model string,
) (CredentialStateResponse, error) {
	if groupID == 0 || credentialID == 0 {
		return CredentialStateResponse{}, app_errors.ErrValidation
	}
	// 先取运行态：运行结束会先写完记录再摘除运行标记，因此 running=false 之后的
	// 读取一定能看到最后一次探测的记录。
	runningModels := s.stateRefreshRunningModels(credentialID)
	response := CredentialStateResponse{
		RequiredLength:  execution.CodexTurnStateLength,
		AvailableModels: []string{},
		States:          []CredentialStateModelResponse{},
		Logs:            []CredentialStateRefreshLogResponse{},
	}
	err := s.withReadSnapshot(ctx, func(tx *gorm.DB) error {
		group, err := s.loadStateRefreshGroup(tx, groupID)
		if err != nil {
			return err
		}
		var credential models.Credential
		if err := tx.Where("id = ? AND group_id = ?", credentialID, groupID).Take(&credential).Error; err != nil {
			return err
		}
		availableModels, err := groupStateRefreshModels(group)
		if err != nil {
			return err
		}
		response.AvailableModels = availableModels
		var captures []models.CredentialTurnState
		if err := tx.Where("credential_id = ?", credentialID).Order("model ASC").Find(&captures).Error; err != nil {
			return err
		}
		response.Model = resolveStateRefreshModel(model, availableModels, captures)
		for _, row := range captures {
			response.States = append(response.States, credentialStateModelResponse(row))
		}
		var logs []models.CredentialStateRefreshLog
		if err := tx.Where("group_id = ? AND credential_id = ? AND model = ?", groupID, credentialID, response.Model).
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
	response.Running = runningModels[response.Model]
	covered := false
	for index := range response.States {
		response.States[index].Running = runningModels[response.States[index].Model]
		if response.States[index].Model == response.Model {
			covered = true
		}
	}
	if response.Running && !covered {
		response.States = append(response.States, CredentialStateModelResponse{
			Model: response.Model, Running: true,
		})
	}
	return response, nil
}

// StartCredentialStateRefresh starts the background refresh run of one
// credential and model. The run repeats the probe until it captures a complete
// turn state for that model, cannot prepare another attempt, or is stopped.
// Starting a model that is already refreshing is idempotent.
func (s *Service) StartCredentialStateRefresh(
	ctx context.Context,
	groupID uint,
	credentialID uint,
	model string,
) (CredentialStateResponse, error) {
	if groupID == 0 || credentialID == 0 {
		return CredentialStateResponse{}, app_errors.ErrValidation
	}
	model = strings.TrimSpace(model)
	if model == "" {
		return CredentialStateResponse{}, app_errors.ErrValidation
	}
	if ctx == nil {
		ctx = context.Background()
	}
	// 先校验目标：不可刷新的凭据和分组没有配置的模型必须同步报错，而不是在后台
	// 静默失败。
	if _, _, err := s.loadStateRefreshTarget(ctx, groupID, credentialID, model); err != nil {
		return CredentialStateResponse{}, err
	}
	key := stateRefreshKey{credentialID: credentialID, model: model}
	s.stateMu.Lock()
	if _, running := s.stateRuns[key]; !running {
		if s.stateRuns == nil {
			s.stateRuns = make(map[stateRefreshKey]*stateRefreshRun)
		}
		// 刷新必须活过发起它的请求，但保留请求上下文中的取值。
		runContext, cancel := context.WithCancel(context.WithoutCancel(ctx))
		run := &stateRefreshRun{cancel: cancel, done: make(chan struct{})}
		s.stateRuns[key] = run
		go s.runCredentialStateRefresh(runContext, groupID, credentialID, model, run)
	}
	s.stateMu.Unlock()
	return s.GetCredentialState(ctx, groupID, credentialID, model)
}

// StopCredentialStateRefresh cancels the in-flight refresh runs of one
// credential and waits for their probes to converge, so the returned snapshot
// already carries the closing records. An empty model stops every run of the
// credential; stopping an idle credential is a no-op.
func (s *Service) StopCredentialStateRefresh(
	ctx context.Context,
	groupID uint,
	credentialID uint,
	model string,
) (CredentialStateResponse, error) {
	if groupID == 0 || credentialID == 0 {
		return CredentialStateResponse{}, app_errors.ErrValidation
	}
	model = strings.TrimSpace(model)
	if ctx == nil {
		ctx = context.Background()
	}
	// 停止只校验分组与凭据：模型可以来自历史捕获，不再要求分组当前仍提供它。
	if _, _, err := s.loadStateRefreshTarget(ctx, groupID, credentialID, ""); err != nil {
		return CredentialStateResponse{}, err
	}
	for _, run := range s.cancelStateRefreshRuns(credentialID, model) {
		s.waitStateRefresh(ctx, run.done)
	}
	return s.GetCredentialState(ctx, groupID, credentialID, model)
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

// cancelStateRefreshRuns cancels the matching runs and returns them for the
// caller to wait on. An empty model matches every run of the credential.
func (s *Service) cancelStateRefreshRuns(credentialID uint, model string) []*stateRefreshRun {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	runs := make([]*stateRefreshRun, 0, len(s.stateRuns))
	for key, run := range s.stateRuns {
		if key.credentialID != credentialID || model != "" && key.model != model {
			continue
		}
		runs = append(runs, run)
	}
	for _, run := range runs {
		run.cancel()
	}
	return runs
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

// stateRefreshRunningModels reports the models of one credential that currently
// have a refresh run.
func (s *Service) stateRefreshRunningModels(credentialID uint) map[string]bool {
	s.stateMu.Lock()
	defer s.stateMu.Unlock()
	running := make(map[string]bool, len(s.stateRuns))
	for key := range s.stateRuns {
		if key.credentialID == credentialID {
			running[key.model] = true
		}
	}
	return running
}

// runCredentialStateRefresh repeats one probe attempt until the run captures a
// complete turn state, cannot prepare another attempt, or is stopped.
func (s *Service) runCredentialStateRefresh(
	ctx context.Context,
	groupID uint,
	credentialID uint,
	model string,
	run *stateRefreshRun,
) {
	key := stateRefreshKey{credentialID: credentialID, model: model}
	defer func() {
		s.stateMu.Lock()
		if s.stateRuns[key] == run {
			delete(s.stateRuns, key)
		}
		close(run.done)
		s.stateMu.Unlock()
	}()
	// 上一次运行留下的失败记录不属于这一次运行，先清掉再开始探测。
	if err := s.pruneFailedStateRefreshLogs(ctx, groupID, credentialID, model); err != nil {
		s.logStateRefreshFailure(groupID, credentialID, err)
	}
	for attempt := 1; ; attempt++ {
		if ctx.Err() != nil {
			return
		}
		stop, rateLimited := s.probeCredentialTurnState(ctx, groupID, credentialID, model, attempt)
		if stop {
			return
		}
		timer := time.NewTimer(s.stateRefreshDelay(rateLimited))
		select {
		case <-ctx.Done():
			timer.Stop()
			return
		case <-timer.C:
		}
	}
}

// stateRefreshDelay is the pause between two probe attempts: a fixed rest after
// an upstream rate limit, otherwise a random interval inside the configured
// range so a long refresh never hammers the upstream on a fixed rhythm.
func (s *Service) stateRefreshDelay(rateLimited bool) time.Duration {
	if rateLimited {
		return s.stateRefreshRateLimitDelay
	}
	minimum := s.stateRefreshMinInterval
	if minimum <= 0 {
		minimum = stateRefreshMinInterval
	}
	maximum := s.stateRefreshMaxInterval
	if maximum < minimum {
		maximum = minimum
	}
	if maximum == minimum {
		return minimum
	}
	return minimum + time.Duration(rand.Int64N(int64(maximum-minimum)+1))
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
// whether the run must stop (after a capture, after a failure that makes the
// target unrefreshable, after a failed record write, or on cancellation) and
// whether the next attempt must rest because the upstream rate limited the
// probe.
func (s *Service) probeCredentialTurnState(
	ctx context.Context,
	groupID uint,
	credentialID uint,
	model string,
	attempt int,
) (bool, bool) {
	startedAt := s.now()
	probe, err := s.prepareStateRefreshProbe(ctx, groupID, credentialID, model)
	if err != nil {
		// 停止发生在准备阶段：这次探测没有发出请求，不留档。
		if ctx.Err() != nil {
			return true, false
		}
		// 目标无法刷新时重试没有意义，记录后结束运行。
		s.recordAttemptFailure(ctx, groupID, credentialID, fixedStateRefreshOutcome(model), attempt, err, startedAt)
		return true, false
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
		return stop || stateRefreshFatalProbeError(probeErr), stateRefreshRateLimited(probeErr)
	}
	probe.outcome.StateLength = len(probed.TurnState)
	if len(probed.TurnState) != execution.CodexTurnStateLength {
		return s.recordAttemptFailure(ctx, groupID, credentialID, probe.outcome, attempt,
			stateRefreshLengthError{observed: len(probed.TurnState)}, startedAt), false
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
	if err := s.persistCredentialTurnState(ctx, groupID, credentialID, model, probed.TurnState, refreshedAtMS, record); err != nil {
		s.logStateRefreshFailure(groupID, credentialID, err)
	}
	return true, false
}

// fixedStateRefreshOutcome is the identity of the fixed probe request of one
// model, known before any preparation step runs.
func fixedStateRefreshOutcome(model string) stateRefreshOutcome {
	return stateRefreshOutcome{Model: model, Input: stateRefreshInput}
}

// prepareStateRefreshProbe resolves everything one probe attempt needs: the
// target, the state proxy policy, the prepared credential, and the probe
// request of the selected model. The whole probe, including any credential token
// refresh, uses the state proxy policy; the captured value is replayed on the
// direct data path.
func (s *Service) prepareStateRefreshProbe(
	ctx context.Context,
	groupID uint,
	credentialID uint,
	model string,
) (stateRefreshProbe, error) {
	probe := stateRefreshProbe{outcome: fixedStateRefreshOutcome(model)}
	group, credential, err := s.loadStateRefreshTarget(ctx, groupID, credentialID, model)
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
		Model:                model,
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
// one manual state refresh. The channel must expose the state probe capability
// and, when a model is selected, the group must serve that model.
func (s *Service) loadStateRefreshTarget(
	ctx context.Context,
	groupID uint,
	credentialID uint,
	model string,
) (models.Group, models.Credential, error) {
	var group models.Group
	var credential models.Credential
	err := s.withReadSnapshot(ctx, func(tx *gorm.DB) error {
		loaded, err := s.loadStateRefreshGroup(tx, groupID)
		group = loaded
		if err != nil {
			return err
		}
		if model != "" {
			availableModels, err := groupStateRefreshModels(group)
			if err != nil {
				return err
			}
			if !containsStateRefreshModel(availableModels, model) {
				return app_errors.ErrValidation
			}
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

// persistCredentialTurnState durably stores the captured value of one model and
// its record time, retires the failures of the run that produced it, and
// appends the refresh record in the same transaction so the stored state, its
// record time, and its log entry always agree. Turn state is mutable runtime
// state, so publication never invalidates in-flight requests.
func (s *Service) persistCredentialTurnState(
	ctx context.Context,
	groupID uint,
	credentialID uint,
	model string,
	turnState string,
	refreshedAtMS int64,
	record models.CredentialStateRefreshLog,
) error {
	return s.writeCredentialConfig(ctx, groupID, credentialID, func(tx *gorm.DB) error {
		// 捕获成功后这次运行的失败记录不再有意义，与成功记录同事务清理。
		if err := pruneFailedStateRefreshLogs(tx, groupID, credentialID, model); err != nil {
			return err
		}
		row := models.CredentialTurnState{
			CredentialID:  credentialID,
			Model:         model,
			TurnState:     turnState,
			RefreshedAtMS: refreshedAtMS,
		}
		if err := tx.Clauses(clause.OnConflict{
			Columns: []clause.Column{{Name: "credential_id"}, {Name: "model"}},
			DoUpdates: clause.Assignments(map[string]any{
				"turn_state":      turnState,
				"refreshed_at_ms": refreshedAtMS,
			}),
		}).Create(&row).Error; err != nil {
			return err
		}
		return tx.Create(&record).Error
	}, func() error {
		if !s.registry.SetCredentialTurnState(credentialID, model, turnState, refreshedAtMS) {
			return fmt.Errorf("publish credential turn state: credential %d is unavailable", credentialID)
		}
		return nil
	})
}

// pruneFailedStateRefreshLogs drops the failed refresh records of one credential
// and model. Failed records describe the run that produced them, so they are
// retired as soon as that run captures a state or a new run starts.
func (s *Service) pruneFailedStateRefreshLogs(
	ctx context.Context,
	groupID uint,
	credentialID uint,
	model string,
) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return pruneFailedStateRefreshLogs(s.db.WithContext(context.WithoutCancel(ctx)), groupID, credentialID, model)
}

func pruneFailedStateRefreshLogs(tx *gorm.DB, groupID uint, credentialID uint, model string) error {
	return tx.Where(
		"group_id = ? AND credential_id = ? AND model = ? AND status = ?",
		groupID, credentialID, model, models.CredentialStateRefreshFailed,
	).Delete(&models.CredentialStateRefreshLog{}).Error
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

func credentialStateModelResponse(row models.CredentialTurnState) CredentialStateModelResponse {
	response := CredentialStateModelResponse{
		Model:       row.Model,
		TurnState:   row.TurnState,
		StateLength: len(row.TurnState),
	}
	if row.TurnState != "" && row.RefreshedAtMS > 0 {
		refreshedAtMS := row.RefreshedAtMS
		response.RefreshedAtMS = &refreshedAtMS
	}
	if expiresAtMS := state.NewTurnState(row.TurnState, row.RefreshedAtMS).ExpiresAtMS; expiresAtMS > 0 {
		response.ExpiresAtMS = &expiresAtMS
	}
	return response
}

// groupStateRefreshModels reads the models a group serves. Only a configured
// model can be probed: the captured state is replayed for exactly that model.
func groupStateRefreshModels(group models.Group) ([]string, error) {
	groupModels := make([]GroupModel, 0)
	if err := decodeGroupDiscoveryJSON(group.Models, &groupModels); err != nil {
		return nil, fmt.Errorf("decode group %d models: %w", group.ID, err)
	}
	models := make([]string, 0, len(groupModels))
	for _, model := range groupModels {
		if id := strings.TrimSpace(model.ID); id != "" {
			models = append(models, id)
		}
	}
	return models, nil
}

// resolveStateRefreshModel picks the model one state read refers to. Without an
// explicit selection the default probe model wins, then the first configured
// model, then the model of the first retained capture.
func resolveStateRefreshModel(
	requested string,
	availableModels []string,
	captures []models.CredentialTurnState,
) string {
	if requested != "" {
		return requested
	}
	if containsStateRefreshModel(availableModels, execution.CodexTurnStateModel) {
		return execution.CodexTurnStateModel
	}
	if len(availableModels) != 0 {
		return availableModels[0]
	}
	for _, row := range captures {
		if row.Model != "" {
			return row.Model
		}
	}
	return ""
}

func containsStateRefreshModel(models []string, model string) bool {
	for _, candidate := range models {
		if candidate == model {
			return true
		}
	}
	return false
}

// stateRefreshFatalProbeError reports a probe failure that retrying cannot fix:
// the upstream rejected the credential itself.
func stateRefreshFatalProbeError(err error) bool {
	status := upstreamStatusCode(err)
	return status != nil && (*status == http.StatusUnauthorized || *status == http.StatusForbidden)
}

// stateRefreshRateLimited reports an upstream rate limit, which must be answered
// with a fixed rest instead of the regular probe interval.
func stateRefreshRateLimited(err error) bool {
	status := upstreamStatusCode(err)
	return status != nil && *status == http.StatusTooManyRequests
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
