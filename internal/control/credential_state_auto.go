package control

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"

	"gpt-load/internal/execution"
	app_errors "gpt-load/internal/platform/errors"
	"gpt-load/internal/state"
	"gpt-load/internal/storage/models"
)

// Automatic state refresh contract: one operator opt-in per credential. While it
// is on, the control plane keeps the turn state of the models the credential
// actually serves valid on its own. The data plane reports every (credential,
// model) it dispatches; a background sweep starts the same probe run a manual
// refresh starts as soon as a used model has no complete state or its state is
// about to expire, and stops refreshing a model that was not requested during
// its state's validity window until it is requested again. Only the models a
// credential serves are refreshed, never every model its group configures.
const (
	// stateAutoRefreshHorizon 是自动刷新的提前量：剩余有效期只剩这么多时再次探测，
	// 让 State 在用户使用期间始终可用。
	stateAutoRefreshHorizon = 10 * time.Minute
	// stateAutoRefreshSweepInterval 是自动刷新巡检的周期。
	stateAutoRefreshSweepInterval = 30 * time.Second
	// stateAutoRefreshRetryDelay 是一次没有拿到完整 State 的运行结束后，同一
	// (凭据, 模型) 再次探测前的冷却：上游拒绝或存储失败时避免巡检立刻重开运行。
	stateAutoRefreshRetryDelay = 5 * time.Minute
	// stateAutoRefreshUseFlushDelay 是数据面观测到的用点落库的最小间隔：巡检据此在
	// 重启后仍能判断某个模型是否还在被使用。
	stateAutoRefreshUseFlushDelay = time.Minute
)

// CredentialStateAutoRefreshRequest selects whether one credential keeps the
// turn state of the models it serves valid on its own.
type CredentialStateAutoRefreshRequest struct {
	Enabled bool `json:"enabled"`
}

// stateAutoModel is the data-plane view of one (credential, model) pair: the
// instant of its last observed use, and whether that use already reached the
// durable used set.
type stateAutoModel struct {
	usedAt time.Time
	// recorded 表示这次用点已经落到用过的模型表上；写失败会清掉它，让下一次请求
	// 重写。
	recorded bool
	// idlePaused 表示最近一次巡检判定这个模型在当前 State 的有效期内没有请求，
	// 自动刷新已停；下一次请求据此立刻唤醒巡检，不必等下一个周期。
	idlePaused bool
}

// stateAutoRefresh is the control-plane bookkeeping of the auto refresh opt-in:
// the credentials that opted in, the last use of each of their models, and when
// a watched model may be probed again after a run ended without a valid state.
type stateAutoRefresh struct {
	mu          sync.Mutex
	credentials map[uint]struct{}
	models      map[stateRefreshKey]stateAutoModel
	retryAt     map[stateRefreshKey]time.Time
	// wake 让「刚被使用」和「刚被开启」立刻触发一次巡检，而不必等下一个周期。
	wake   chan struct{}
	writes sync.WaitGroup
}

// ObserveTurnStateUse records that one credential served one upstream model on a
// real attempt. It runs on the data path: it only reads the opt-in set and hands
// the durable write to a background task. Only opted-in credentials are tracked,
// so the used set stays exactly the models automatic refresh must keep valid.
func (s *Service) ObserveTurnStateUse(credentialID uint, model string) {
	if s == nil || credentialID == 0 {
		return
	}
	model = strings.TrimSpace(model)
	if model == "" || !s.autoRefreshEnabled(credentialID) {
		return
	}
	key := stateRefreshKey{credentialID: credentialID, model: model}
	usedAt := s.now()
	s.autoRefresh.mu.Lock()
	if s.autoRefresh.models == nil {
		s.autoRefresh.models = make(map[stateRefreshKey]stateAutoModel)
	}
	entry := s.autoRefresh.models[key]
	entry.usedAt = usedAt
	resume := entry.idlePaused
	entry.idlePaused = false
	persist := !entry.recorded
	entry.recorded = true
	s.autoRefresh.models[key] = entry
	s.autoRefresh.mu.Unlock()
	if persist {
		// 第一次用到这个模型：落库，让重启后的巡检也能看到这个用点。
		s.autoRefresh.writes.Add(1)
		go func() {
			defer s.autoRefresh.writes.Done()
			s.persistUsedModel(key, usedAt)
		}()
	}
	if persist || resume {
		// 新模型或刚被判定空闲的模型：立刻巡检一次，让自动刷新尽快开始。
		s.wakeStateAutoRefresh()
	}
}

// autoRefreshEnabled reports whether one credential opted into automatic state
// refresh.
func (s *Service) autoRefreshEnabled(credentialID uint) bool {
	if s == nil || credentialID == 0 {
		return false
	}
	s.autoRefresh.mu.Lock()
	defer s.autoRefresh.mu.Unlock()
	_, enabled := s.autoRefresh.credentials[credentialID]
	return enabled
}

// setAutoRefreshCredential adds or removes one credential from the opt-in set.
func (s *Service) setAutoRefreshCredential(credentialID uint, enabled bool) {
	s.autoRefresh.mu.Lock()
	defer s.autoRefresh.mu.Unlock()
	if s.autoRefresh.credentials == nil {
		s.autoRefresh.credentials = make(map[uint]struct{})
	}
	if enabled {
		s.autoRefresh.credentials[credentialID] = struct{}{}
		return
	}
	delete(s.autoRefresh.credentials, credentialID)
	// 关掉自动刷新后，这个凭据的用点不再有意义；用过的模型本身留在库里，重新开启
	// 后按新的请求重新纳入。
	for key := range s.autoRefresh.models {
		if key.credentialID == credentialID {
			delete(s.autoRefresh.models, key)
			delete(s.autoRefresh.retryAt, key)
		}
	}
}

// replaceAutoRefreshCredentials makes the opt-in set exactly the credentials
// that are currently enabled, so a change made outside this process is honored
// at the next sweep.
func (s *Service) replaceAutoRefreshCredentials(credentialIDs []uint) {
	enabled := make(map[uint]struct{}, len(credentialIDs))
	for _, credentialID := range credentialIDs {
		enabled[credentialID] = struct{}{}
	}
	s.autoRefresh.mu.Lock()
	defer s.autoRefresh.mu.Unlock()
	s.autoRefresh.credentials = enabled
	for key := range s.autoRefresh.models {
		if _, ok := enabled[key.credentialID]; !ok {
			delete(s.autoRefresh.models, key)
			delete(s.autoRefresh.retryAt, key)
		}
	}
}

// autoRefreshUse reports the last use the data plane observed for one
// (credential, model) pair.
func (s *Service) autoRefreshUse(key stateRefreshKey) (time.Time, bool) {
	s.autoRefresh.mu.Lock()
	defer s.autoRefresh.mu.Unlock()
	entry, ok := s.autoRefresh.models[key]
	return entry.usedAt, ok
}

// forgetAutoRefreshModel drops the in-memory view of one pair. A later use
// records it again.
func (s *Service) forgetAutoRefreshModel(key stateRefreshKey) {
	s.autoRefresh.mu.Lock()
	defer s.autoRefresh.mu.Unlock()
	delete(s.autoRefresh.models, key)
	delete(s.autoRefresh.retryAt, key)
}

// releaseAutoRefreshModel lets a later use retry a durable write that failed.
func (s *Service) releaseAutoRefreshModel(key stateRefreshKey) {
	s.autoRefresh.mu.Lock()
	defer s.autoRefresh.mu.Unlock()
	entry, ok := s.autoRefresh.models[key]
	if !ok {
		return
	}
	entry.recorded = false
	s.autoRefresh.models[key] = entry
}

// autoRefreshRetryAllowed reports whether the cooldown after the previous run of
// one pair has elapsed.
func (s *Service) autoRefreshRetryAllowed(key stateRefreshKey, now time.Time) bool {
	s.autoRefresh.mu.Lock()
	defer s.autoRefresh.mu.Unlock()
	retryAt, ok := s.autoRefresh.retryAt[key]
	return !ok || !now.Before(retryAt)
}

// holdAutoRefreshRetry pushes the next automatic start of one pair out by the
// retry delay. A run that captures a valid state makes this irrelevant: the
// state is then valid for an hour, so the sweep skips it anyway.
func (s *Service) holdAutoRefreshRetry(key stateRefreshKey, now time.Time) {
	s.autoRefresh.mu.Lock()
	defer s.autoRefresh.mu.Unlock()
	if s.autoRefresh.retryAt == nil {
		s.autoRefresh.retryAt = make(map[stateRefreshKey]time.Time)
	}
	s.autoRefresh.retryAt[key] = now.Add(stateAutoRefreshRetryDelay)
}

// wakeStateAutoRefresh asks the sweep to run now instead of at the next tick.
func (s *Service) wakeStateAutoRefresh() {
	if s == nil || s.autoRefresh.wake == nil {
		return
	}
	select {
	case s.autoRefresh.wake <- struct{}{}:
	default:
	}
}

// SetCredentialStateAutoRefresh turns the automatic turn state refresh of one
// credential on or off. Enabling only records the choice: the background sweep
// keeps the state of the models the credential actually serves valid from the
// next request on. Disabling stops the runs automatic refresh started, so an
// operator-started run is never canceled by this switch.
func (s *Service) SetCredentialStateAutoRefresh(
	ctx context.Context,
	groupID uint,
	credentialID uint,
	enabled bool,
) (CredentialStateResponse, error) {
	if groupID == 0 || credentialID == 0 {
		return CredentialStateResponse{}, app_errors.ErrValidation
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if _, _, err := s.loadStateRefreshTarget(ctx, groupID, credentialID, ""); err != nil {
		return CredentialStateResponse{}, err
	}
	if err := s.writeCredentialConfig(ctx, groupID, credentialID, func(tx *gorm.DB) error {
		return tx.Model(&models.Credential{}).Where("id = ? AND group_id = ?", credentialID, groupID).
			Update("state_auto_refresh", enabled).Error
	}, nil); err != nil {
		return CredentialStateResponse{}, err
	}
	if enabled {
		s.setAutoRefreshCredential(credentialID, true)
		s.wakeStateAutoRefresh()
	} else {
		s.setAutoRefreshCredential(credentialID, false)
		for _, run := range s.cancelAutoRefreshRuns(credentialID, "") {
			s.waitStateRefresh(ctx, run.done)
		}
	}
	return s.GetCredentialState(ctx, groupID, credentialID, "")
}

// loadAutoRefreshCredentials fills the opt-in set from persistence so the data
// path recognizes the enabled credentials before the first sweep.
func (s *Service) loadAutoRefreshCredentials(ctx context.Context) error {
	credentialIDs, err := s.queryAutoRefreshCredentials(ctx)
	if err != nil {
		return err
	}
	s.replaceAutoRefreshCredentials(credentialIDs)
	return nil
}

// runStateAutoRefresh keeps the turn state of the models the opted-in
// credentials serve valid. It runs for the whole control runtime: the sweep is
// the only writer of automatic refresh runs, and every run it starts converges
// before storage closes.
func (s *Service) runStateAutoRefresh(ctx context.Context) {
	if s == nil {
		return
	}
	interval := s.stateAutoSweepInterval
	if interval <= 0 {
		interval = stateAutoRefreshSweepInterval
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		s.sweepStateAutoRefresh(ctx)
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		case <-s.autoRefresh.wake:
		}
	}
}

// sweepStateAutoRefresh refreshes every used model of the opted-in credentials
// that needs a new state right now.
func (s *Service) sweepStateAutoRefresh(ctx context.Context) {
	if s == nil || ctx.Err() != nil {
		return
	}
	credentialIDs, err := s.queryAutoRefreshCredentials(ctx)
	if err != nil {
		s.logStateAutoRefreshFailure(0, 0, err)
		return
	}
	s.replaceAutoRefreshCredentials(credentialIDs)
	if len(credentialIDs) == 0 {
		return
	}
	used, err := s.queryUsedModels(ctx, credentialIDs)
	if err != nil {
		s.logStateAutoRefreshFailure(0, 0, err)
		return
	}
	now := s.now()
	for _, row := range used {
		if ctx.Err() != nil {
			return
		}
		s.sweepUsedModel(ctx, row, now)
	}
}

// sweepUsedModel decides what one used model needs: a new probe run, nothing, or
// the end of its automatic refresh because it stopped being used.
func (s *Service) sweepUsedModel(ctx context.Context, row models.CredentialUsedModel, now time.Time) {
	key := stateRefreshKey{credentialID: row.CredentialID, model: row.Model}
	ref, ok := s.registry.CredentialRef(row.CredentialID)
	if !ok {
		s.forgetAutoRefreshModel(key)
		return
	}
	lastUsedMS := row.UsedAtMS
	if observed, ok := s.autoRefreshUse(key); ok {
		if usedAtMS := observed.UTC().UnixMilli(); usedAtMS > lastUsedMS {
			lastUsedMS = usedAtMS
			// 数据面的用点还没落库：巡检顺手写下去，重启后仍能判断这个模型是否还在
			// 被使用。
			if usedAtMS-row.UsedAtMS >= stateAutoRefreshUseFlushDelay.Milliseconds() {
				if err := s.updateUsedModel(ctx, key, usedAtMS); err != nil {
					s.logStateAutoRefreshFailure(ref.GroupID, key.credentialID, err)
				}
			}
		}
	}
	capture, captured := ref.TurnStates[row.Model]
	// 判据是「当前 State 的有效期窗口内有没有请求」：窗口是 [捕获时刻, 捕获时刻+1h]，
	// 捕获时刻由有效期反推。窗口还没结束时看窗口起点；窗口已经结束时，只有窗口之后的
	// 请求才算「现在还在用」，否则这个模型在窗口内没被请求、之后也没有新请求，就该停。
	idleThresholdMS := now.Add(-execution.CodexTurnStateTTL).UnixMilli()
	if captured && capture.ExpiresAtMS > 0 {
		idleThresholdMS = capture.ExpiresAtMS - execution.CodexTurnStateTTL.Milliseconds()
		if now.After(time.UnixMilli(capture.ExpiresAtMS)) {
			idleThresholdMS = capture.ExpiresAtMS
		}
	}
	if lastUsedMS < idleThresholdMS {
		// 这个 State 的有效期内没有任何请求：停止自动刷新；用点与记录都留着，下一次
		// 请求会把用点刷新，巡检随即接手。这样「停止」与「新请求」不必争用同一条
		// 记录，重启后也能按持久化的用点继续判断。
		s.pauseAutoRefreshModel(ctx, key)
		return
	}
	if s.stateRefreshRunning(key) || !stateAutoRefreshDue(capture, captured, now) {
		return
	}
	if !s.autoRefreshRetryAllowed(key, now) {
		return
	}
	s.startAutoRefreshRun(ctx, key, ref.GroupID)
}

// stateAutoRefreshDue reports whether one capture is missing, unusable, or about
// to expire. Only a complete state counts as usable: any other length is a
// different token shape and is replaced instead of replayed.
func stateAutoRefreshDue(capture state.TurnState, captured bool, now time.Time) bool {
	if !captured {
		return true
	}
	if _, complete := execution.CompleteTurnState(capture.Value); !complete {
		return true
	}
	return capture.ExpiresAtMS <= now.Add(stateAutoRefreshHorizon).UnixMilli()
}

// startAutoRefreshRun validates one used model and starts the probe run that
// keeps its state valid. A model its group no longer serves is not probed: the
// used record stays so a request never re-writes it on every attempt, and the
// retry cooldown re-checks the group configuration a few minutes later.
func (s *Service) startAutoRefreshRun(ctx context.Context, key stateRefreshKey, groupID uint) {
	if _, _, err := s.loadStateRefreshTarget(ctx, groupID, key.credentialID, key.model); err != nil {
		if !errors.Is(err, app_errors.ErrValidation) && !errors.Is(err, app_errors.ErrResourceNotFound) {
			s.logStateAutoRefreshFailure(groupID, key.credentialID, err)
		}
		s.holdAutoRefreshRetry(key, s.now())
		return
	}
	s.holdAutoRefreshRetry(key, s.now())
	s.startStateRefreshRun(ctx, groupID, key.credentialID, key.model, true)
}

// pauseAutoRefreshModel stops the runs automatic state refresh started for one
// used model. The model stays in the used set with its last known use: the next
// request refreshes that use, and the following sweep resumes refreshing, so the
// pause never races a request the way deleting the record would.
func (s *Service) pauseAutoRefreshModel(ctx context.Context, key stateRefreshKey) {
	s.markAutoRefreshIdle(key)
	for _, run := range s.cancelAutoRefreshRuns(key.credentialID, key.model) {
		s.waitStateRefresh(ctx, run.done)
	}
}

// markAutoRefreshIdle remembers that one pair was judged idle, so the next
// request wakes the sweep immediately instead of waiting for the next tick.
func (s *Service) markAutoRefreshIdle(key stateRefreshKey) {
	s.autoRefresh.mu.Lock()
	defer s.autoRefresh.mu.Unlock()
	entry, ok := s.autoRefresh.models[key]
	if !ok {
		return
	}
	entry.idlePaused = true
	s.autoRefresh.models[key] = entry
}

// queryAutoRefreshCredentials reads the active credentials that opted into
// automatic state refresh.
func (s *Service) queryAutoRefreshCredentials(ctx context.Context) ([]uint, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var credentialIDs []uint
	err := s.db.WithContext(ctx).Model(&models.Credential{}).
		Where("state_auto_refresh = ? AND status = ?", true, models.CredentialStatusActive).
		Order("id ASC").Pluck("id", &credentialIDs).Error
	if err != nil {
		return nil, app_errors.ParseDBError(err)
	}
	return credentialIDs, nil
}

// queryUsedModels reads the models the given credentials have served.
func (s *Service) queryUsedModels(ctx context.Context, credentialIDs []uint) ([]models.CredentialUsedModel, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	var rows []models.CredentialUsedModel
	err := s.db.WithContext(ctx).Where("credential_id IN ?", credentialIDs).
		Order("credential_id ASC").Order("model ASC").Find(&rows).Error
	if err != nil {
		return nil, app_errors.ParseDBError(err)
	}
	return rows, nil
}

// persistUsedModel records one first use of a (credential, model) pair. A failed
// write releases the in-memory claim so a later request records it again.
func (s *Service) persistUsedModel(key stateRefreshKey, usedAt time.Time) {
	ctx, cancel := context.WithTimeout(context.Background(), stateRefreshTimeout)
	defer cancel()
	row := models.CredentialUsedModel{
		CredentialID: key.credentialID,
		Model:        key.model,
		UsedAtMS:     usedAt.UTC().UnixMilli(),
	}
	err := s.db.WithContext(ctx).Clauses(clause.OnConflict{
		Columns:   []clause.Column{{Name: "credential_id"}, {Name: "model"}},
		DoUpdates: clause.Assignments(map[string]any{"used_at_ms": row.UsedAtMS}),
	}).Create(&row).Error
	if err != nil {
		s.releaseAutoRefreshModel(key)
		s.logStateAutoRefreshFailure(0, key.credentialID, app_errors.ParseDBError(err))
	}
}

// updateUsedModel advances the durable last use of one pair. The write is
// conditional so a flush never moves the stored instant backwards while a newer
// request is being recorded at the same time.
func (s *Service) updateUsedModel(ctx context.Context, key stateRefreshKey, usedAtMS int64) error {
	if ctx == nil {
		ctx = context.Background()
	}
	err := s.db.WithContext(ctx).Model(&models.CredentialUsedModel{}).
		Where("credential_id = ? AND model = ? AND used_at_ms < ?", key.credentialID, key.model, usedAtMS).
		Update("used_at_ms", usedAtMS).Error
	if err != nil {
		return app_errors.ParseDBError(err)
	}
	return nil
}
