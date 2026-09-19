package control

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

	"gpt-load/internal/channel"
	"gpt-load/internal/execution"
	"gpt-load/internal/platform/config"
	"gpt-load/internal/storage/models"
	subscriptionruntime "gpt-load/internal/subscription/runtime"
	"gpt-load/internal/testutil/turnstatetest"
)

// autoRefreshProbe 让 fixture 的探测返回固定值，并记录调用次数。运行在后台
// goroutine 里，所以调用次数与「已进入探测」都用并发安全的方式暴露。
type autoRefreshProbe struct {
	calls   atomic.Int64
	value   string
	err     error
	block   bool
	entered chan struct{}
	once    sync.Once
}

func (probe *autoRefreshProbe) count() int { return int(probe.calls.Load()) }

// installAutoRefreshProbe 把探测换成确定性的假探测：返回给定值（或一直阻塞到运行被
// 停止），并统计调用次数。
func installAutoRefreshProbe(t *testing.T, fixture serviceFixture, probe *autoRefreshProbe) {
	t.Helper()
	fixture.service.probeSubscriptionTurnState = func(
		ctx context.Context,
		_ channel.ID,
		_ subscriptionruntime.Credential,
		_ subscriptionruntime.Target,
		_ subscriptionruntime.StateProbeRequest,
	) (subscriptionruntime.StateProbeResult, error) {
		probe.calls.Add(1)
		if probe.entered != nil {
			probe.once.Do(func() { close(probe.entered) })
		}
		if probe.block {
			<-ctx.Done()
			return subscriptionruntime.StateProbeResult{}, ctx.Err()
		}
		if probe.err != nil {
			return subscriptionruntime.StateProbeResult{}, probe.err
		}
		return subscriptionruntime.StateProbeResult{TurnState: probe.value}, nil
	}
}

// waitProbeEntered 等待后台运行真正进入探测。
func waitProbeEntered(t *testing.T, probe *autoRefreshProbe) {
	t.Helper()
	select {
	case <-probe.entered:
	case <-time.After(10 * time.Second):
		t.Fatal("probe was not entered")
	}
}

// waitAutoRefreshUseRecorded 等待数据面把一次用点落库。
func waitAutoRefreshUseRecorded(t *testing.T, fixture serviceFixture, credentialID uint, model string) models.CredentialUsedModel {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var row models.CredentialUsedModel
		err := fixture.db.Where("credential_id = ? AND model = ?", credentialID, model).Take(&row).Error
		if err == nil {
			return row
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("used model %q was not recorded", model)
	return models.CredentialUsedModel{}
}

// usedModelRow 读取一条用点记录，不存在时返回 nil。
func usedModelRow(t *testing.T, fixture serviceFixture, credentialID uint, model string) *models.CredentialUsedModel {
	t.Helper()
	var row models.CredentialUsedModel
	err := fixture.db.Where("credential_id = ? AND model = ?", credentialID, model).Take(&row).Error
	if err != nil {
		if strings.Contains(err.Error(), "record not found") {
			return nil
		}
		t.Fatal(err)
	}
	return &row
}

// TestSetCredentialStateAutoRefreshRecordsChoice 保证开关落在凭据上、随快照返回，并
// 立刻改变数据面与巡检共用的内存开关；关闭时删掉用过的模型的内存视图。
func TestSetCredentialStateAutoRefreshRecordsChoice(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	state, err := fixture.service.GetCredentialState(t.Context(), groupID, credentialID, "")
	if err != nil {
		t.Fatalf("GetCredentialState() error = %v", err)
	}
	if state.AutoRefresh {
		t.Fatalf("credential opted in by default: %#v", state)
	}

	enabled, err := fixture.service.SetCredentialStateAutoRefresh(t.Context(), groupID, credentialID, true)
	if err != nil {
		t.Fatalf("SetCredentialStateAutoRefresh(true) error = %v", err)
	}
	if !enabled.AutoRefresh || !fixture.service.autoRefreshEnabled(credentialID) {
		t.Fatalf("enabled snapshot = %#v", enabled)
	}
	fixture.service.ObserveTurnStateUse(credentialID, stateRefreshTestModel)
	waitAutoRefreshUseRecorded(t, fixture, credentialID, stateRefreshTestModel)

	disabled, err := fixture.service.SetCredentialStateAutoRefresh(t.Context(), groupID, credentialID, false)
	if err != nil {
		t.Fatalf("SetCredentialStateAutoRefresh(false) error = %v", err)
	}
	if disabled.AutoRefresh || fixture.service.autoRefreshEnabled(credentialID) {
		t.Fatalf("disabled snapshot = %#v", disabled)
	}
	if _, observed := fixture.service.autoRefreshUse(stateRefreshKey{
		credentialID: credentialID, model: stateRefreshTestModel,
	}); observed {
		t.Fatal("disable kept the in-memory used model")
	}
	// 用过的模型留在库里：重新开启后按新的请求继续维护。
	if usedModelRow(t, fixture, credentialID, stateRefreshTestModel) == nil {
		t.Fatal("disable dropped the durable used model")
	}

	var stored models.Credential
	if err := fixture.db.Take(&stored, credentialID).Error; err != nil {
		t.Fatal(err)
	}
	if stored.StateAutoRefresh {
		t.Fatal("disable did not persist")
	}
	if _, err := fixture.service.SetCredentialStateAutoRefresh(t.Context(), groupID, 0, true); err == nil {
		t.Fatal("missing credential accepted")
	}
}

// TestAutoRefreshStartsRunForUsedModelWithoutCompleteState 保证巡检对「用过但没有完整
// State」的模型自行开跑，捕获到 292 后停止并落库，用点也随数据面记录落库。
func TestAutoRefreshStartsRunForUsedModelWithoutCompleteState(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	now := time.UnixMilli(1_800_000_000_000)
	fixture.service.now = func() time.Time { return now }
	complete := turnstatetest.Value(0)
	probe := &autoRefreshProbe{value: complete}
	installAutoRefreshProbe(t, fixture, probe)
	if _, err := fixture.service.SetCredentialStateAutoRefresh(t.Context(), groupID, credentialID, true); err != nil {
		t.Fatalf("enable auto refresh: %v", err)
	}

	fixture.service.ObserveTurnStateUse(credentialID, stateRefreshTestModel)
	used := waitAutoRefreshUseRecorded(t, fixture, credentialID, stateRefreshTestModel)
	if used.UsedAtMS != now.UnixMilli() {
		t.Fatalf("used at = %d, want %d", used.UsedAtMS, now.UnixMilli())
	}
	if probe.count() != 0 {
		t.Fatalf("probe ran before the sweep: %d", probe.count())
	}

	fixture.service.sweepStateAutoRefresh(t.Context())
	waitStateRefreshIdle(t, fixture.service, credentialID, stateRefreshTestModel)

	capture := turnStateCapture(t, fixture, credentialID, stateRefreshTestModel)
	if capture == nil || capture.TurnState != complete || capture.RefreshedAtMS != now.UnixMilli() {
		t.Fatalf("captured turn state = %#v", capture)
	}
	if probe.count() == 0 {
		t.Fatal("sweep did not probe the used model")
	}
	// 用过的模型只维护它自己：分组配置的另一个模型没有请求，巡检不碰。
	if probe.count() != 1 {
		t.Fatalf("probe calls = %d, want 1", probe.count())
	}
}

// TestAutoRefreshKeepsRunningWhileTheModelIsUsed 保证 State 只剩不到提前量时自动再次
// 探测，而剩余充足的 State 不重跑；没有任何请求的凭据不参与。
func TestAutoRefreshKeepsRunningWhileTheModelIsUsed(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	now := time.UnixMilli(1_800_000_000_000)
	fixture.service.now = func() time.Time { return now }
	probe := &autoRefreshProbe{value: turnstatetest.Value(1)}
	installAutoRefreshProbe(t, fixture, probe)
	if _, err := fixture.service.SetCredentialStateAutoRefresh(t.Context(), groupID, credentialID, true); err != nil {
		t.Fatalf("enable auto refresh: %v", err)
	}

	// 剩余充足的 State：用点落在有效期窗口内，但不该重跑。
	if !fixture.registry.SetCredentialTurnState(credentialID, stateRefreshTestModel, turnstatetest.Value(2), now.UnixMilli()) {
		t.Fatal("publish fresh capture")
	}
	fixture.service.ObserveTurnStateUse(credentialID, stateRefreshTestModel)
	waitAutoRefreshUseRecorded(t, fixture, credentialID, stateRefreshTestModel)
	fixture.service.sweepStateAutoRefresh(t.Context())
	if probe.count() != 0 || fixture.service.stateRefreshRunning(stateRefreshKey{
		credentialID: credentialID, model: stateRefreshTestModel,
	}) {
		t.Fatalf("fresh state was refreshed: calls=%d", probe.count())
	}

	// 剩余不足提前量：同一个用点触发重跑。有效期窗口从捕获时刻起算，所以这里把捕获
	// 放在足够久之前，让剩余有效期落到提前量以内。
	refreshedAtMS := now.Add(-execution.CodexTurnStateTTL + stateAutoRefreshHorizon - time.Minute).UnixMilli()
	if !fixture.registry.SetCredentialTurnState(credentialID, stateRefreshTestModel, turnstatetest.Value(3), refreshedAtMS) {
		t.Fatal("publish expiring capture")
	}
	fixture.service.sweepStateAutoRefresh(t.Context())
	waitStateRefreshIdle(t, fixture.service, credentialID, stateRefreshTestModel)
	capture := turnStateCapture(t, fixture, credentialID, stateRefreshTestModel)
	if probe.count() == 0 || capture == nil || capture.TurnState != turnstatetest.Value(1) ||
		capture.RefreshedAtMS != now.UnixMilli() {
		t.Fatalf("expiring state was not refreshed: calls=%d capture=%#v", probe.count(), capture)
	}

	// 没有开启自动刷新的凭据：数据面不上报，巡检也不维护。
	otherGroup := models.Group{
		Name: "auto-refresh-off", ChannelID: string(channel.Codex),
		ConnectionType: models.ConnectionTypeSubscription,
		Params:         models.JSON(`{}`), Models: models.JSON(fmt.Sprintf(`[{"id":%q}]`, stateRefreshTestModel)), Enabled: true,
	}
	if err := fixture.db.Create(&otherGroup).Error; err != nil {
		t.Fatal(err)
	}
	other := models.Credential{
		GroupID: otherGroup.ID, Data: "{}", Fingerprint: "fp", IdentityFingerprint: "identity",
	}
	if err := fixture.db.Create(&other).Error; err != nil {
		t.Fatal(err)
	}
	fixture.service.ObserveTurnStateUse(other.ID, stateRefreshTestModel)
	if row := usedModelRow(t, fixture, other.ID, stateRefreshTestModel); row != nil {
		t.Fatalf("credential without auto refresh was tracked: %#v", row)
	}
}

// TestAutoRefreshStopsWatchingAModelIdleForItsWholeStateWindow 保证 State 的有效期内
// 没有请求的模型停止自动刷新（运行被取消、用点保留），下一次请求再把它纳入维护。
func TestAutoRefreshStopsWatchingAModelIdleForItsWholeStateWindow(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	now := time.UnixMilli(1_800_000_000_000)
	fixture.service.now = func() time.Time { return now }
	probe := &autoRefreshProbe{block: true, entered: make(chan struct{})}
	installAutoRefreshProbe(t, fixture, probe)
	if _, err := fixture.service.SetCredentialStateAutoRefresh(t.Context(), groupID, credentialID, true); err != nil {
		t.Fatalf("enable auto refresh: %v", err)
	}
	// 一个即将过期的 State：请求落在它的有效期窗口内，巡检应当接着探测。
	refreshedAtMS := now.Add(-execution.CodexTurnStateTTL + stateAutoRefreshHorizon - time.Minute).UnixMilli()
	if !fixture.registry.SetCredentialTurnState(credentialID, stateRefreshTestModel, turnstatetest.Value(4), refreshedAtMS) {
		t.Fatal("publish expiring capture")
	}
	key := stateRefreshKey{credentialID: credentialID, model: stateRefreshTestModel}
	fixture.service.ObserveTurnStateUse(credentialID, stateRefreshTestModel)
	waitAutoRefreshUseRecorded(t, fixture, credentialID, stateRefreshTestModel)
	fixture.service.sweepStateAutoRefresh(t.Context())
	if !fixture.service.stateRefreshRunning(key) {
		t.Fatal("sweep did not start the used model")
	}
	waitProbeEntered(t, probe)

	// 这个 State 的有效期内没有任何请求：运行被取消，用点与记录保留，只记下它已空闲。
	if err := fixture.db.Model(&models.CredentialUsedModel{}).
		Where("credential_id = ? AND model = ?", credentialID, stateRefreshTestModel).
		Update("used_at_ms", refreshedAtMS-time.Minute.Milliseconds()).Error; err != nil {
		t.Fatal(err)
	}
	fixture.service.forgetAutoRefreshModel(key)

	fixture.service.sweepStateAutoRefresh(t.Context())
	waitStateRefreshIdle(t, fixture.service, credentialID, stateRefreshTestModel)
	if probe.count() == 0 {
		t.Fatal("the idle model was never probed")
	}
	if used := usedModelRow(t, fixture, credentialID, stateRefreshTestModel); used == nil {
		t.Fatal("the paused model lost its used record")
	}

	// 下一次请求把它重新纳入维护：用点被刷新到窗口之后，巡检重新开跑。
	fixture.service.ObserveTurnStateUse(credentialID, stateRefreshTestModel)
	fixture.service.sweepStateAutoRefresh(t.Context())
	if !fixture.service.stateRefreshRunning(key) {
		t.Fatal("a new request did not resume automatic refresh")
	}
	stopped, err := fixture.service.StopCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshTestModel)
	if err != nil {
		t.Fatalf("StopCredentialStateRefresh() error = %v", err)
	}
	if stopped.Running {
		t.Fatalf("run did not stop: %#v", stopped)
	}
}

// TestAutoRefreshLeavesOperatorRunsAlone 保证巡检只停止自己开跑的运行：人工发起的
// 运行在模型空闲时继续，只有停止按钮或关闭开关才会结束它。
func TestAutoRefreshLeavesOperatorRunsAlone(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	now := time.UnixMilli(1_800_000_000_000)
	fixture.service.now = func() time.Time { return now }
	probe := &autoRefreshProbe{block: true, entered: make(chan struct{})}
	installAutoRefreshProbe(t, fixture, probe)
	if _, err := fixture.service.SetCredentialStateAutoRefresh(t.Context(), groupID, credentialID, true); err != nil {
		t.Fatalf("enable auto refresh: %v", err)
	}
	if _, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshTestModel); err != nil {
		t.Fatalf("StartCredentialStateRefresh() error = %v", err)
	}
	key := stateRefreshKey{credentialID: credentialID, model: stateRefreshTestModel}
	if err := fixture.db.Create(&models.CredentialUsedModel{
		CredentialID: credentialID, Model: stateRefreshTestModel,
		UsedAtMS: now.Add(-2 * time.Hour).UnixMilli(),
	}).Error; err != nil {
		t.Fatal(err)
	}

	fixture.service.sweepStateAutoRefresh(t.Context())
	if !fixture.service.stateRefreshRunning(key) {
		t.Fatal("sweep canceled an operator-started run")
	}

	// 关闭开关只停止自动刷新开跑的运行，人工运行仍然由停止按钮结束。
	if _, err := fixture.service.SetCredentialStateAutoRefresh(t.Context(), groupID, credentialID, false); err != nil {
		t.Fatalf("disable auto refresh: %v", err)
	}
	if !fixture.service.stateRefreshRunning(key) {
		t.Fatal("disable canceled an operator-started run")
	}
	if _, err := fixture.service.StopCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshTestModel); err != nil {
		t.Fatalf("StopCredentialStateRefresh() error = %v", err)
	}
	waitStateRefreshIdle(t, fixture.service, credentialID, stateRefreshTestModel)
}

// TestAutoRefreshAdoptsOperatorStart 保证操作者对正在自动刷新的模型点「刷新 State」
// 之后，这条运行归操作者所有：关闭开关不会取消它。
func TestAutoRefreshAdoptsOperatorStart(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	now := time.UnixMilli(1_800_000_000_000)
	fixture.service.now = func() time.Time { return now }
	probe := &autoRefreshProbe{block: true, entered: make(chan struct{})}
	installAutoRefreshProbe(t, fixture, probe)
	if _, err := fixture.service.SetCredentialStateAutoRefresh(t.Context(), groupID, credentialID, true); err != nil {
		t.Fatalf("enable auto refresh: %v", err)
	}
	fixture.service.ObserveTurnStateUse(credentialID, stateRefreshTestModel)
	waitAutoRefreshUseRecorded(t, fixture, credentialID, stateRefreshTestModel)
	fixture.service.sweepStateAutoRefresh(t.Context())
	key := stateRefreshKey{credentialID: credentialID, model: stateRefreshTestModel}
	if !fixture.service.stateRefreshRunning(key) {
		t.Fatal("sweep did not start the used model")
	}
	waitProbeEntered(t, probe)

	if _, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshTestModel); err != nil {
		t.Fatalf("StartCredentialStateRefresh() error = %v", err)
	}
	if _, err := fixture.service.SetCredentialStateAutoRefresh(t.Context(), groupID, credentialID, false); err != nil {
		t.Fatalf("disable auto refresh: %v", err)
	}
	if !fixture.service.stateRefreshRunning(key) {
		t.Fatal("disable canceled a run the operator adopted")
	}
	if _, err := fixture.service.StopCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshTestModel); err != nil {
		t.Fatalf("StopCredentialStateRefresh() error = %v", err)
	}
	waitStateRefreshIdle(t, fixture.service, credentialID, stateRefreshTestModel)
}

// TestAutoRefreshDisableStopsRunsItStarted 保证关闭开关会结束自动刷新开跑的运行。
func TestAutoRefreshDisableStopsRunsItStarted(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	now := time.UnixMilli(1_800_000_000_000)
	fixture.service.now = func() time.Time { return now }
	probe := &autoRefreshProbe{block: true, entered: make(chan struct{})}
	installAutoRefreshProbe(t, fixture, probe)
	if _, err := fixture.service.SetCredentialStateAutoRefresh(t.Context(), groupID, credentialID, true); err != nil {
		t.Fatalf("enable auto refresh: %v", err)
	}
	fixture.service.ObserveTurnStateUse(credentialID, stateRefreshTestModel)
	waitAutoRefreshUseRecorded(t, fixture, credentialID, stateRefreshTestModel)
	fixture.service.sweepStateAutoRefresh(t.Context())
	if !fixture.service.stateRefreshRunning(stateRefreshKey{
		credentialID: credentialID, model: stateRefreshTestModel,
	}) {
		t.Fatal("sweep did not start the used model")
	}
	waitProbeEntered(t, probe)

	disabled, err := fixture.service.SetCredentialStateAutoRefresh(t.Context(), groupID, credentialID, false)
	if err != nil {
		t.Fatalf("disable auto refresh: %v", err)
	}
	if disabled.Running || len(disabled.RunningModels) != 0 {
		t.Fatalf("disable left a run: %#v", disabled)
	}
	waitStateRefreshIdle(t, fixture.service, credentialID, stateRefreshTestModel)
}

// TestAutoRefreshRetriesAfterCooldownWhenTheUpstreamCannotProduceAState 保证一次没有拿到
// 完整 State 的运行不会让巡检立刻重开：同一 (凭据, 模型) 在冷却期内被跳过，冷却过后
// 再试；上游拒绝（401）时同样只是记录，不会退化成每次巡检都探测。
func TestAutoRefreshRetriesAfterCooldownWhenTheUpstreamCannotProduceAState(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	now := time.UnixMilli(1_800_000_000_000)
	fixture.service.now = func() time.Time { return now }
	probe := &autoRefreshProbe{err: &subscriptionruntime.UpstreamHTTPError{StatusCode: http.StatusUnauthorized}}
	installAutoRefreshProbe(t, fixture, probe)
	if _, err := fixture.service.SetCredentialStateAutoRefresh(t.Context(), groupID, credentialID, true); err != nil {
		t.Fatalf("enable auto refresh: %v", err)
	}
	fixture.service.ObserveTurnStateUse(credentialID, stateRefreshTestModel)
	waitAutoRefreshUseRecorded(t, fixture, credentialID, stateRefreshTestModel)

	fixture.service.sweepStateAutoRefresh(t.Context())
	waitStateRefreshIdle(t, fixture.service, credentialID, stateRefreshTestModel)
	first := probe.count()
	if first == 0 {
		t.Fatal("sweep did not probe the used model")
	}
	// 冷却期内：同一用点不再重开运行。
	fixture.service.sweepStateAutoRefresh(t.Context())
	fixture.service.sweepStateAutoRefresh(t.Context())
	if probe.count() != first {
		t.Fatalf("probe calls = %d, want %d during the cooldown", probe.count(), first)
	}
	// 冷却过后：仍然没有完整 State，于是再试一次。
	now = now.Add(stateAutoRefreshRetryDelay + time.Second)
	fixture.service.sweepStateAutoRefresh(t.Context())
	waitStateRefreshIdle(t, fixture.service, credentialID, stateRefreshTestModel)
	if probe.count() <= first {
		t.Fatalf("probe calls = %d, want more than %d after the cooldown", probe.count(), first)
	}
}

// TestAutoRefreshBacksOffWhenTheGroupNoLongerServesTheModel 保证分组不再提供的模型不会
// 被探测，用点也不会被删（否则每次请求都会重写一次），只是按冷却期退避。
func TestAutoRefreshBacksOffWhenTheGroupNoLongerServesTheModel(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	now := time.UnixMilli(1_800_000_000_000)
	fixture.service.now = func() time.Time { return now }
	probe := &autoRefreshProbe{value: turnstatetest.Value(7)}
	installAutoRefreshProbe(t, fixture, probe)
	if _, err := fixture.service.SetCredentialStateAutoRefresh(t.Context(), groupID, credentialID, true); err != nil {
		t.Fatalf("enable auto refresh: %v", err)
	}
	// 一个曾经用过、但分组配置里已经移除的模型。
	const retired = "gpt-5-retired"
	if err := fixture.db.Create(&models.CredentialUsedModel{
		CredentialID: credentialID, Model: retired, UsedAtMS: now.UnixMilli(),
	}).Error; err != nil {
		t.Fatal(err)
	}
	fixture.service.ObserveTurnStateUse(credentialID, retired)
	fixture.service.sweepStateAutoRefresh(t.Context())
	if probe.count() != 0 {
		t.Fatalf("probe calls = %d, want 0 for a model the group does not serve", probe.count())
	}
	if used := usedModelRow(t, fixture, credentialID, retired); used == nil {
		t.Fatal("the backed-off model lost its used record")
	}
}

// TestAutoRefreshStopsWatchingAnExpiredWindowWithoutANewRequest 保证窗口结束后没有新请求
// 的模型停刷：即使旧请求落在已过期的窗口内，也不该继续为一个没人用的模型探测。
func TestAutoRefreshStopsWatchingAnExpiredWindowWithoutANewRequest(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	now := time.UnixMilli(1_800_000_000_000)
	fixture.service.now = func() time.Time { return now }
	probe := &autoRefreshProbe{block: true, entered: make(chan struct{})}
	installAutoRefreshProbe(t, fixture, probe)
	if _, err := fixture.service.SetCredentialStateAutoRefresh(t.Context(), groupID, credentialID, true); err != nil {
		t.Fatalf("enable auto refresh: %v", err)
	}
	// 已经过期的窗口：捕获在 70 分钟前，窗口是 [捕获, 捕获+1h]。
	refreshedAtMS := now.Add(-execution.CodexTurnStateTTL - 10*time.Minute).UnixMilli()
	if !fixture.registry.SetCredentialTurnState(credentialID, stateRefreshTestModel, turnstatetest.Value(8), refreshedAtMS) {
		t.Fatal("publish expired capture")
	}
	key := stateRefreshKey{credentialID: credentialID, model: stateRefreshTestModel}
	// 用点落在窗口内（捕获后 30 分钟），但窗口结束后没有任何请求。
	if err := fixture.db.Create(&models.CredentialUsedModel{
		CredentialID: credentialID, Model: stateRefreshTestModel,
		UsedAtMS: refreshedAtMS + 30*time.Minute.Milliseconds(),
	}).Error; err != nil {
		t.Fatal(err)
	}
	fixture.service.sweepStateAutoRefresh(t.Context())
	if probe.count() != 0 || fixture.service.stateRefreshRunning(key) {
		t.Fatalf("expired idle window was refreshed: calls=%d", probe.count())
	}
	// 窗口结束之后的请求重新纳入维护。
	fixture.service.ObserveTurnStateUse(credentialID, stateRefreshTestModel)
	fixture.service.sweepStateAutoRefresh(t.Context())
	if !fixture.service.stateRefreshRunning(key) {
		t.Fatal("a request after the window did not resume automatic refresh")
	}
	if _, err := fixture.service.StopCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshTestModel); err != nil {
		t.Fatalf("StopCredentialStateRefresh() error = %v", err)
	}
}

// TestAutoRefreshFlushesObservedUse 保证数据面的用点最终落库：巡检只在内存里的用点比
// 库里新了足够久时写一次，重启后仍能判断这个模型是否还在被使用。
func TestAutoRefreshFlushesObservedUse(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	now := time.UnixMilli(1_800_000_000_000)
	fixture.service.now = func() time.Time { return now }
	probe := &autoRefreshProbe{block: true, entered: make(chan struct{})}
	installAutoRefreshProbe(t, fixture, probe)
	if _, err := fixture.service.SetCredentialStateAutoRefresh(t.Context(), groupID, credentialID, true); err != nil {
		t.Fatalf("enable auto refresh: %v", err)
	}
	fixture.service.ObserveTurnStateUse(credentialID, stateRefreshTestModel)
	waitAutoRefreshUseRecorded(t, fixture, credentialID, stateRefreshTestModel)
	// 一个剩余充足的 State：巡检只负责把用点写下去，不重跑。
	if !fixture.registry.SetCredentialTurnState(credentialID, stateRefreshTestModel, turnstatetest.Value(6), now.UnixMilli()) {
		t.Fatal("publish fresh capture")
	}

	// 刚过一分钟的用点：巡检顺手把它写下去。
	now = now.Add(stateAutoRefreshUseFlushDelay + time.Second)
	fixture.service.ObserveTurnStateUse(credentialID, stateRefreshTestModel)
	fixture.service.sweepStateAutoRefresh(t.Context())
	used := usedModelRow(t, fixture, credentialID, stateRefreshTestModel)
	if used == nil || used.UsedAtMS != now.UnixMilli() {
		t.Fatalf("used at = %#v, want %d", used, now.UnixMilli())
	}
}

// TestRunStateAutoRefreshSweepsUntilContextCancelled 保证巡检循环在运行期自行发现新
// 用点（不必等外部触发），并在上下文取消后退出。
func TestRunStateAutoRefreshSweepsUntilContextCancelled(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	probe := &autoRefreshProbe{value: turnstatetest.Value(5)}
	installAutoRefreshProbe(t, fixture, probe)
	fixture.service.stateAutoSweepInterval = time.Millisecond
	if _, err := fixture.service.SetCredentialStateAutoRefresh(t.Context(), groupID, credentialID, true); err != nil {
		t.Fatalf("enable auto refresh: %v", err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		fixture.service.runStateAutoRefresh(ctx)
	}()

	fixture.service.ObserveTurnStateUse(credentialID, stateRefreshTestModel)
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if capture := turnStateCapture(t, fixture, credentialID, stateRefreshTestModel); capture != nil &&
			capture.TurnState == turnstatetest.Value(5) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	if capture := turnStateCapture(t, fixture, credentialID, stateRefreshTestModel); capture == nil ||
		capture.TurnState != turnstatetest.Value(5) {
		t.Fatalf("sweep loop did not refresh the used model: %#v", capture)
	}
	cancel()
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("sweep loop did not stop")
	}
}

// TestStateAutoRefreshRouteTogglesTheCredential 保证开关接口把选择写进凭据并把快照
// 返回给前端，且缺参、非订阅凭据被拒绝。
func TestStateAutoRefreshRouteTogglesTheCredential(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	initControlI18n(t)
	engine := gin.New()
	NewServer(&config.Config{AuthKey: "test-auth-key"}, fixture.service).RegisterRoutes(engine)
	authorize := func(request *http.Request) *http.Request {
		request.Header.Set("Authorization", "Bearer test-auth-key")
		request.Header.Set("Content-Type", "application/json")
		return request
	}
	path := fmt.Sprintf("/api/groups/%d/credentials/%d/state-refresh/auto", groupID, credentialID)

	enabled := httptest.NewRecorder()
	engine.ServeHTTP(enabled, authorize(httptest.NewRequest(http.MethodPut, path, strings.NewReader(`{"enabled":true}`))))
	if enabled.Code != http.StatusOK {
		t.Fatalf("enable = %d %s", enabled.Code, enabled.Body)
	}
	var enableEnvelope struct {
		Data CredentialStateResponse `json:"data"`
	}
	if err := json.Unmarshal(enabled.Body.Bytes(), &enableEnvelope); err != nil {
		t.Fatal(err)
	}
	if !enableEnvelope.Data.AutoRefresh ||
		enableEnvelope.Data.RequiredLength != execution.CodexTurnStateLength {
		t.Fatalf("enable payload = %#v", enableEnvelope.Data)
	}

	disabled := httptest.NewRecorder()
	engine.ServeHTTP(disabled, authorize(httptest.NewRequest(http.MethodPut, path, strings.NewReader(`{"enabled":false}`))))
	if disabled.Code != http.StatusOK {
		t.Fatalf("disable = %d %s", disabled.Code, disabled.Body)
	}
	var disableEnvelope struct {
		Data CredentialStateResponse `json:"data"`
	}
	if err := json.Unmarshal(disabled.Body.Bytes(), &disableEnvelope); err != nil {
		t.Fatal(err)
	}
	if disableEnvelope.Data.AutoRefresh {
		t.Fatalf("disable payload = %#v", disableEnvelope.Data)
	}

	malformed := httptest.NewRecorder()
	engine.ServeHTTP(malformed, authorize(httptest.NewRequest(http.MethodPut, path, strings.NewReader(`{"enabled":"yes"}`))))
	if malformed.Code != http.StatusBadRequest {
		t.Fatalf("malformed body = %d %s", malformed.Code, malformed.Body)
	}

	group := models.Group{
		Name: "api-key-auto", ChannelID: string(channel.OpenAI),
		ConnectionType: models.ConnectionTypeAPIKey, Params: models.JSON(`{}`),
		Models: models.JSON(`[]`), Enabled: true,
	}
	if err := fixture.db.Create(&group).Error; err != nil {
		t.Fatal(err)
	}
	other := models.Credential{GroupID: group.ID, Data: "{}", Fingerprint: "fp", IdentityFingerprint: "identity"}
	if err := fixture.db.Create(&other).Error; err != nil {
		t.Fatal(err)
	}
	denied := httptest.NewRecorder()
	engine.ServeHTTP(denied, authorize(httptest.NewRequest(http.MethodPut, fmt.Sprintf(
		"/api/groups/%d/credentials/%d/state-refresh/auto", group.ID, other.ID,
	), strings.NewReader(`{"enabled":true}`))))
	if denied.Code != http.StatusBadRequest {
		t.Fatalf("api-key credential = %d %s", denied.Code, denied.Body)
	}
}

// TestEnsureInitialStateLoadsAutoRefreshCredentials 保证进程启动后第一个请求就知道
// 哪些凭据开启了自动刷新，不必等第一次巡检。
func TestEnsureInitialStateLoadsAutoRefreshCredentials(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	if err := fixture.db.Model(&models.Credential{}).Where("id = ?", credentialID).
		Update("state_auto_refresh", true).Error; err != nil {
		t.Fatal(err)
	}
	fixture.service.setAutoRefreshCredential(credentialID, false)
	if err := fixture.service.EnsureInitialState(t.Context()); err != nil {
		t.Fatalf("EnsureInitialState() error = %v", err)
	}
	if !fixture.service.autoRefreshEnabled(credentialID) {
		t.Fatal("startup did not load the opted-in credential")
	}
	if _, err := fixture.service.GetCredentialState(t.Context(), groupID, credentialID, ""); err != nil {
		t.Fatalf("GetCredentialState() error = %v", err)
	}
}
