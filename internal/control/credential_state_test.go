package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"
	"gorm.io/gorm"

	"gpt-load/internal/channel"
	"gpt-load/internal/channel/modules"
	"gpt-load/internal/execution"
	"gpt-load/internal/outboundproxy"
	"gpt-load/internal/platform/config"
	app_errors "gpt-load/internal/platform/errors"
	"gpt-load/internal/storage/models"
	subscriptionruntime "gpt-load/internal/subscription/runtime"
	"gpt-load/internal/testutil/turnstatetest"
)

// 分组配置的两个可刷新模型：默认探测模型与另一个模型。
const (
	stateRefreshTestModel  = execution.CodexTurnStateModel
	stateRefreshOtherModel = "gpt-5.2"
)

// newStateRefreshFixture returns a subscription fixture serving two models whose
// refresh runs probe as fast as the test can observe them.
func newStateRefreshFixture(t *testing.T) (serviceFixture, uint, uint) {
	t.Helper()
	fixture, groupID, credentialID := newSubscriptionCredentialFixture(t)
	fixture.service.stateRefreshMinInterval = time.Millisecond
	fixture.service.stateRefreshMaxInterval = time.Millisecond
	groupModels := fmt.Sprintf(`[{"id":%q},{"id":%q}]`, stateRefreshOtherModel, stateRefreshTestModel)
	if err := fixture.db.Model(&models.Group{}).Where("id = ?", groupID).
		Update("models", models.JSON(groupModels)).Error; err != nil {
		t.Fatal(err)
	}
	return fixture, groupID, credentialID
}

func waitStateRefreshIdle(t *testing.T, service *Service, credentialID uint, model string) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if !service.stateRefreshRunningModels(credentialID)[model] {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("state refresh run did not stop")
}

func stateRefreshRecords(
	t *testing.T,
	fixture serviceFixture,
	credentialID uint,
	model string,
) []models.CredentialStateRefreshLog {
	t.Helper()
	var rows []models.CredentialStateRefreshLog
	if err := fixture.db.Where("credential_id = ? AND model = ?", credentialID, model).
		Order("id ASC").Find(&rows).Error; err != nil {
		t.Fatal(err)
	}
	return rows
}

func waitStateRefreshRecords(
	t *testing.T,
	fixture serviceFixture,
	credentialID uint,
	model string,
	count int,
) []models.CredentialStateRefreshLog {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if rows := stateRefreshRecords(t, fixture, credentialID, model); len(rows) >= count {
			return rows
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("state refresh recorded fewer than %d attempts", count)
	return nil
}

// waitStateRefreshPruned waits until every refresh record of one model is newer
// than the given record id, which is how a run retiring its predecessor's
// failures shows up.
func waitStateRefreshPruned(t *testing.T, fixture serviceFixture, credentialID uint, model string, afterID uint) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		rows := stateRefreshRecords(t, fixture, credentialID, model)
		stale := false
		for _, row := range rows {
			if row.ID <= afterID {
				stale = true
				break
			}
		}
		if !stale && len(rows) != 0 {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("failed refresh records before %d were not retired", afterID)
}

func turnStateCapture(t *testing.T, fixture serviceFixture, credentialID uint, model string) *models.CredentialTurnState {
	t.Helper()
	var row models.CredentialTurnState
	err := fixture.db.Where("credential_id = ? AND model = ?", credentialID, model).Take(&row).Error
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return nil
	}
	if err != nil {
		t.Fatal(err)
	}
	return &row
}

func modelState(t *testing.T, state CredentialStateResponse, model string) CredentialStateModelResponse {
	t.Helper()
	for _, entry := range state.States {
		if entry.Model == model {
			return entry
		}
	}
	t.Fatalf("model %q is missing from %#v", model, state.States)
	return CredentialStateModelResponse{}
}

func TestStartCredentialStateRefreshCapturesCompleteTurnState(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	now := time.UnixMilli(1_800_000_000_000)
	fixture.service.now = func() time.Time { return now }
	complete := turnstatetest.Value(0)
	calls := 0
	var probed subscriptionruntime.StateProbeRequest
	fixture.service.probeSubscriptionTurnState = func(
		_ context.Context,
		channelID channel.ID,
		_ subscriptionruntime.Credential,
		_ subscriptionruntime.Target,
		request subscriptionruntime.StateProbeRequest,
	) (subscriptionruntime.StateProbeResult, error) {
		if channelID != channel.Codex {
			t.Fatalf("probe channel = %q", channelID)
		}
		calls++
		probed = request
		if calls < 3 {
			return subscriptionruntime.StateProbeResult{TurnState: "incomplete"}, nil
		}
		return subscriptionruntime.StateProbeResult{TurnState: complete}, nil
	}

	started, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshTestModel)
	if err != nil {
		t.Fatalf("StartCredentialStateRefresh() error = %v", err)
	}
	if !started.Running || started.Model != stateRefreshTestModel ||
		len(started.AvailableModels) != 2 || started.AvailableModels[0] != stateRefreshOtherModel {
		t.Fatalf("started refresh = %#v", started)
	}
	if probe, ok := fixture.service.subscriptions.StateProbe(channel.Codex); !ok || probe.ID() != modules.CodexStateProbe {
		t.Fatalf("codex state probe = %#v, found = %t", probe, ok)
	}
	if probe, ok := fixture.service.subscriptions.StateProbe(channel.OpenAI); ok || probe != nil {
		t.Fatalf("api-key channel exposed a state probe: %#v", probe)
	}
	waitStateRefreshIdle(t, fixture.service, credentialID, stateRefreshTestModel)

	// 运行期间逐次留档，捕获成功后只保留这次运行的成功记录。
	records := stateRefreshRecords(t, fixture, credentialID, stateRefreshTestModel)
	if len(records) != 1 {
		t.Fatalf("refresh records = %#v", records)
	}
	record := records[0]
	if record.Status != models.CredentialStateRefreshSucceeded || record.ErrorCode != "" ||
		record.Attempts != 3 || record.StateLength != execution.CodexTurnStateLength ||
		record.TurnState != complete || record.GroupID != groupID ||
		record.Model != stateRefreshTestModel || record.Input != stateRefreshInput ||
		record.ProxyURL != "direct" || record.HTTPStatus != nil || record.CreatedAtMS != now.UnixMilli() {
		t.Fatalf("successful refresh record = %#v", record)
	}
	if probed.Model != stateRefreshTestModel || probed.Input != "ping" {
		t.Fatalf("probe request = %#v", probed)
	}
	// No state override and no global proxy is configured, so the probe runs direct.
	if probed.ProxyURL != "direct" || probed.ProxyFromEnvironment {
		t.Fatalf("probe proxy = %#v", probed)
	}

	capture := turnStateCapture(t, fixture, credentialID, stateRefreshTestModel)
	if capture == nil || capture.TurnState != complete || capture.RefreshedAtMS != now.UnixMilli() {
		t.Fatalf("persisted turn state = %#v", capture)
	}
	ref, ok := fixture.registry.CredentialRef(credentialID)
	if !ok || ref.TurnStateFor(stateRefreshTestModel, now) != complete {
		t.Fatalf("published reference = %#v, found = %t", ref, ok)
	}
	// 捕获只属于它自己的模型，不会泄漏给别的模型。
	if ref.TurnStateFor(stateRefreshOtherModel, now) != "" {
		t.Fatalf("capture leaked to another model: %#v", ref)
	}

	state, err := fixture.service.GetCredentialState(t.Context(), groupID, credentialID, "")
	if err != nil {
		t.Fatalf("GetCredentialState() error = %v", err)
	}
	if state.Model != stateRefreshTestModel || state.Running ||
		state.RequiredLength != execution.CodexTurnStateLength || len(state.Logs) != 1 {
		t.Fatalf("state payload = %#v", state)
	}
	entry := modelState(t, state, stateRefreshTestModel)
	if entry.TurnState != complete || entry.StateLength != execution.CodexTurnStateLength ||
		entry.ExpiresAtMS == nil || *entry.ExpiresAtMS != now.Add(time.Hour).UnixMilli() ||
		entry.RefreshedAtMS == nil || *entry.RefreshedAtMS != now.UnixMilli() || entry.Running {
		t.Fatalf("retained state entry = %#v", entry)
	}
}

func TestCredentialStateRefreshRecordsEveryProbeAttemptUntilStopped(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	// 上游在未就绪时返回非完整长度（生产观测到 312），运行必须继续探测并逐次留档。
	observed := strings.Repeat("s", 312)
	fixture.service.probeSubscriptionTurnState = func(
		context.Context,
		channel.ID,
		subscriptionruntime.Credential,
		subscriptionruntime.Target,
		subscriptionruntime.StateProbeRequest,
	) (subscriptionruntime.StateProbeResult, error) {
		return subscriptionruntime.StateProbeResult{TurnState: observed}, nil
	}
	if _, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshTestModel); err != nil {
		t.Fatalf("StartCredentialStateRefresh() error = %v", err)
	}
	records := waitStateRefreshRecords(t, fixture, credentialID, stateRefreshTestModel, 3)
	if _, err := fixture.service.StopCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshTestModel); err != nil {
		t.Fatalf("StopCredentialStateRefresh() error = %v", err)
	}
	waitStateRefreshIdle(t, fixture.service, credentialID, stateRefreshTestModel)

	records = stateRefreshRecords(t, fixture, credentialID, stateRefreshTestModel)
	if len(records) < 3 {
		t.Fatalf("refresh records = %#v", records)
	}
	for index, record := range records {
		if record.Status != models.CredentialStateRefreshFailed || record.ErrorCode != "length_mismatch" ||
			record.StateLength != len(observed) || record.Attempts != index+1 {
			t.Fatalf("refresh record %d = %#v", index, record)
		}
	}
	if turnStateCapture(t, fixture, credentialID, stateRefreshTestModel) != nil {
		t.Fatal("incomplete turn state was persisted")
	}

	// 失败记录只属于产生它的那次运行：新运行开始时会先退休上一轮的失败记录。
	previous := records[len(records)-1]
	if _, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshTestModel); err != nil {
		t.Fatalf("StartCredentialStateRefresh() error = %v", err)
	}
	waitStateRefreshPruned(t, fixture, credentialID, stateRefreshTestModel, previous.ID)
	if _, err := fixture.service.StopCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshTestModel); err != nil {
		t.Fatalf("StopCredentialStateRefresh() error = %v", err)
	}
	waitStateRefreshIdle(t, fixture.service, credentialID, stateRefreshTestModel)
	for _, record := range stateRefreshRecords(t, fixture, credentialID, stateRefreshTestModel) {
		if record.ID <= previous.ID {
			t.Fatalf("previous run kept a failed record: %#v", record)
		}
	}
}

func TestStopCredentialStateRefreshCancelsInFlightProbe(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	entered := make(chan struct{})
	probeContexts := make(chan context.Context, 1)
	fixture.service.probeSubscriptionTurnState = func(
		ctx context.Context,
		_ channel.ID,
		_ subscriptionruntime.Credential,
		_ subscriptionruntime.Target,
		_ subscriptionruntime.StateProbeRequest,
	) (subscriptionruntime.StateProbeResult, error) {
		probeContexts <- ctx
		close(entered)
		<-ctx.Done()
		return subscriptionruntime.StateProbeResult{}, ctx.Err()
	}
	started, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshTestModel)
	if err != nil {
		t.Fatalf("StartCredentialStateRefresh() error = %v", err)
	}
	if !started.Running {
		t.Fatalf("started refresh = %#v", started)
	}
	<-entered
	// 运行中的刷新再次点击开始是幂等的：不新建运行，也不重复探测。
	again, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshTestModel)
	if err != nil {
		t.Fatalf("StartCredentialStateRefresh() error = %v", err)
	}
	if !again.Running {
		t.Fatalf("idempotent start = %#v", again)
	}
	// 停止一个模型不影响同一凭据的其它模型。
	if _, err := fixture.service.StopCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshOtherModel); err != nil {
		t.Fatalf("StopCredentialStateRefresh(other model) error = %v", err)
	}
	stillRunning, err := fixture.service.GetCredentialState(t.Context(), groupID, credentialID, stateRefreshTestModel)
	if err != nil {
		t.Fatalf("GetCredentialState() error = %v", err)
	}
	if !stillRunning.Running {
		t.Fatalf("stopping another model stopped the run: %#v", stillRunning)
	}

	stopped, err := fixture.service.StopCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshTestModel)
	if err != nil {
		t.Fatalf("StopCredentialStateRefresh() error = %v", err)
	}
	if stopped.Running {
		t.Fatalf("stopped refresh = %#v", stopped)
	}
	if len(stopped.Logs) != 1 || stopped.Logs[0].Status != string(models.CredentialStateRefreshFailed) ||
		stopped.Logs[0].ErrorCode != "canceled" || stopped.Logs[0].Attempts != 1 {
		t.Fatalf("canceled refresh records = %#v", stopped.Logs)
	}
	probeContext := <-probeContexts
	if !errors.Is(probeContext.Err(), context.Canceled) {
		t.Fatalf("probe context error = %v", probeContext.Err())
	}
	// 空闲凭据再次停止是空操作。
	if idle, err := fixture.service.StopCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshTestModel); err != nil || idle.Running {
		t.Fatalf("idle stop = %#v / %v", idle, err)
	}
}

func TestCredentialStateRefreshKeepsExpiredCaptureOutOfReplay(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	recordedAt := time.Now().Add(-2 * time.Hour).Truncate(time.Millisecond)
	fixture.service.now = func() time.Time { return recordedAt }
	expired := turnstatetest.Value(0)
	fixture.service.probeSubscriptionTurnState = func(
		context.Context,
		channel.ID,
		subscriptionruntime.Credential,
		subscriptionruntime.Target,
		subscriptionruntime.StateProbeRequest,
	) (subscriptionruntime.StateProbeResult, error) {
		return subscriptionruntime.StateProbeResult{TurnState: expired}, nil
	}
	if _, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshTestModel); err != nil {
		t.Fatalf("StartCredentialStateRefresh() error = %v", err)
	}
	waitStateRefreshIdle(t, fixture.service, credentialID, stateRefreshTestModel)

	capture := turnStateCapture(t, fixture, credentialID, stateRefreshTestModel)
	if capture == nil || capture.TurnState != expired || capture.RefreshedAtMS != recordedAt.UnixMilli() {
		t.Fatalf("persisted turn state = %#v", capture)
	}
	// 记录时间 + 1 小时已经过去：捕获仍然留档展示，但绝不注入到上游请求。
	ref, ok := fixture.registry.CredentialRef(credentialID)
	if !ok || ref.TurnStateFor(stateRefreshTestModel, time.Now()) != "" {
		t.Fatalf("expired reference = %#v, found = %t", ref, ok)
	}
	if ref.TurnStateFor(stateRefreshTestModel, recordedAt.Add(59*time.Minute)) != expired {
		t.Fatalf("capture expired before its validity ended: %#v", ref)
	}
	state, err := fixture.service.GetCredentialState(t.Context(), groupID, credentialID, stateRefreshTestModel)
	if err != nil {
		t.Fatalf("GetCredentialState() error = %v", err)
	}
	entry := modelState(t, state, stateRefreshTestModel)
	if entry.TurnState != expired || entry.ExpiresAtMS == nil ||
		*entry.ExpiresAtMS != recordedAt.Add(time.Hour).UnixMilli() {
		t.Fatalf("expired state payload = %#v", state)
	}
}

func TestCredentialStateRefreshStopsOnUnrefreshableTarget(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	var calls atomic.Int64
	var status atomic.Int64
	status.Store(http.StatusUnauthorized)
	fixture.service.probeSubscriptionTurnState = func(
		context.Context,
		channel.ID,
		subscriptionruntime.Credential,
		subscriptionruntime.Target,
		subscriptionruntime.StateProbeRequest,
	) (subscriptionruntime.StateProbeResult, error) {
		calls.Add(1)
		return subscriptionruntime.StateProbeResult{}, &subscriptionruntime.UpstreamHTTPError{StatusCode: int(status.Load())}
	}
	// 无法刷新（非订阅组）的凭据同步报错，不启动运行。
	apiKeyGroup := models.Group{
		Name: "api-key-state", ChannelID: string(channel.OpenAI),
		ConnectionType: models.ConnectionTypeAPIKey, Params: models.JSON(`{}`),
		Models: models.JSON(`[]`), Enabled: true,
	}
	if err := fixture.db.Create(&apiKeyGroup).Error; err != nil {
		t.Fatal(err)
	}
	other := models.Credential{GroupID: apiKeyGroup.ID, Data: "{}", Fingerprint: "fp", IdentityFingerprint: "identity"}
	if err := fixture.db.Create(&other).Error; err != nil {
		t.Fatal(err)
	}
	if _, err := fixture.service.StartCredentialStateRefresh(t.Context(), apiKeyGroup.ID, other.ID, stateRefreshTestModel); !errors.Is(err, app_errors.ErrValidation) {
		t.Fatalf("api-key start error = %v", err)
	}
	if _, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID+1000, stateRefreshTestModel); !errors.Is(err, app_errors.ErrResourceNotFound) {
		t.Fatalf("missing credential start error = %v", err)
	}
	// 未选择模型、或分组未配置该模型时同步拒绝，而不是后台静默失败。
	for _, model := range []string{"", "  ", "gpt-unknown"} {
		if _, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID, model); !errors.Is(err, app_errors.ErrValidation) {
			t.Fatalf("model %q start error = %v", model, err)
		}
	}

	// 上游拒绝授权：重试不会成功，运行在留档后自行结束。
	if _, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshTestModel); err != nil {
		t.Fatalf("StartCredentialStateRefresh() error = %v", err)
	}
	waitStateRefreshIdle(t, fixture.service, credentialID, stateRefreshTestModel)
	records := waitStateRefreshRecords(t, fixture, credentialID, stateRefreshTestModel, 1)
	if len(records) != 1 || records[0].Status != models.CredentialStateRefreshFailed ||
		records[0].ErrorCode != "unauthorized" || records[0].HTTPStatus == nil ||
		*records[0].HTTPStatus != http.StatusUnauthorized || records[0].Attempts != 1 {
		t.Fatalf("unauthorized refresh records = %#v", records)
	}

	// 其余上游错误继续探测，直到人工停止。
	status.Store(http.StatusServiceUnavailable)
	if _, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshTestModel); err != nil {
		t.Fatalf("StartCredentialStateRefresh() error = %v", err)
	}
	records = waitStateRefreshRecords(t, fixture, credentialID, stateRefreshTestModel, 3)
	if _, err := fixture.service.StopCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshTestModel); err != nil {
		t.Fatalf("StopCredentialStateRefresh() error = %v", err)
	}
	for _, record := range records {
		if record.Status != models.CredentialStateRefreshFailed || record.ErrorCode != "upstream_error" ||
			record.HTTPStatus == nil || *record.HTTPStatus != http.StatusServiceUnavailable ||
			record.TurnState != "" || record.StateLength != 0 {
			t.Fatalf("upstream failure record = %#v", record)
		}
	}
	if calls.Load() < 3 {
		t.Fatalf("probe calls = %d", calls.Load())
	}
}

func TestCredentialStateRefreshUsesStateProxyPrecedence(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	now := time.UnixMilli(1_800_000_000_000)
	fixture.service.now = func() time.Time { return now }
	complete := turnstatetest.Value(0)
	probed := make(chan subscriptionruntime.StateProbeRequest, 4)
	fixture.service.probeSubscriptionTurnState = func(
		_ context.Context,
		_ channel.ID,
		_ subscriptionruntime.Credential,
		_ subscriptionruntime.Target,
		request subscriptionruntime.StateProbeRequest,
	) (subscriptionruntime.StateProbeResult, error) {
		probed <- request
		return subscriptionruntime.StateProbeResult{TurnState: complete}, nil
	}
	update := func(key, value string) {
		t.Helper()
		if _, err := fixture.service.UpdateSettings(t.Context(), SettingsUpdateRequest{
			Settings: map[string]json.RawMessage{key: json.RawMessage(value)},
		}); err != nil {
			t.Fatalf("UpdateSettings(%s) error = %v", key, err)
		}
	}
	nextProbe := func() subscriptionruntime.StateProbeRequest {
		t.Helper()
		if _, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshTestModel); err != nil {
			t.Fatalf("StartCredentialStateRefresh() error = %v", err)
		}
		request := <-probed
		waitStateRefreshIdle(t, fixture.service, credentialID, stateRefreshTestModel)
		return request
	}

	update(outboundproxy.SystemSettingKey, `{"mode":"custom","url":"http://global.example.com:8080"}`)
	if got := nextProbe(); got.ProxyURL != "http://global.example.com:8080" || got.ProxyFromEnvironment {
		t.Fatalf("global proxy probe = %#v", got)
	}

	update(outboundproxy.StateSystemSettingKey, `{"mode":"custom","url":"http://state.example.com:8080"}`)
	if got := nextProbe(); got.ProxyURL != "http://state.example.com:8080" {
		t.Fatalf("state override probe = %#v", got)
	}

	update(outboundproxy.StateSystemSettingKey, "null")
	if got := nextProbe(); got.ProxyURL != "http://global.example.com:8080" {
		t.Fatalf("state reset probe = %#v", got)
	}
}

// 刷新间隔是 1~10 秒之间的随机值，上游返回 429 时固定休息 5 秒。
func TestStateRefreshDelayPolicy(t *testing.T) {
	t.Parallel()

	fixture, _, _ := newStateRefreshFixture(t)
	service := fixture.service
	service.stateRefreshMinInterval = stateRefreshMinInterval
	service.stateRefreshMaxInterval = stateRefreshMaxInterval
	service.stateRefreshRateLimitDelay = stateRefreshRateLimitDelay
	if got := service.stateRefreshDelay(true); got != 5*time.Second {
		t.Fatalf("rate limited delay = %s", got)
	}
	seen := make(map[time.Duration]bool)
	for range 64 {
		delay := service.stateRefreshDelay(false)
		if delay < time.Second || delay > 10*time.Second {
			t.Fatalf("probe delay = %s", delay)
		}
		seen[delay] = true
	}
	if len(seen) < 2 {
		t.Fatalf("probe delay is not randomized: %#v", seen)
	}
}

// 详情一次只回看最新的十条记录，且记录是凭据级的：两个模型的记录共用这一个上限。
func TestCredentialStateLogLimitKeepsNewestRecords(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	base := int64(1_800_000_000_000)
	modelsOf := [2]string{stateRefreshTestModel, stateRefreshOtherModel}
	for index := range 12 {
		row := models.CredentialStateRefreshLog{
			GroupID: groupID, CredentialID: credentialID,
			Status: models.CredentialStateRefreshSucceeded, StateLength: execution.CodexTurnStateLength,
			Attempts: 1, Model: modelsOf[index%2], Input: stateRefreshInput,
			CreatedAtMS: base + int64(index),
		}
		if err := fixture.db.Create(&row).Error; err != nil {
			t.Fatal(err)
		}
	}
	state, err := fixture.service.GetCredentialState(t.Context(), groupID, credentialID, stateRefreshTestModel)
	if err != nil {
		t.Fatalf("GetCredentialState() error = %v", err)
	}
	if len(state.Logs) != stateRefreshLogLimit {
		t.Fatalf("refresh records = %d", len(state.Logs))
	}
	if state.Logs[0].CreatedAtMS != base+11 || state.Logs[len(state.Logs)-1].CreatedAtMS != base+2 {
		t.Fatalf("refresh records = %#v", state.Logs)
	}
	// 两个模型的记录都进同一份列表：最新十条里两种模型都在。
	recorded := make(map[string]bool, 2)
	for _, record := range state.Logs {
		recorded[record.Model] = true
	}
	if !recorded[stateRefreshTestModel] || !recorded[stateRefreshOtherModel] {
		t.Fatalf("refresh records = %#v", state.Logs)
	}
}

// 同一凭据的多个模型各自刷新、各自留档，互不覆盖。
func TestCredentialStateRefreshIsolatesModels(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	now := time.UnixMilli(1_800_000_000_000)
	fixture.service.now = func() time.Time { return now }
	captures := map[string]string{
		stateRefreshTestModel:  turnstatetest.Value(0),
		stateRefreshOtherModel: turnstatetest.Value(1),
	}
	entered := make(chan string, 2)
	release := make(chan struct{})
	fixture.service.probeSubscriptionTurnState = func(
		_ context.Context,
		_ channel.ID,
		_ subscriptionruntime.Credential,
		_ subscriptionruntime.Target,
		request subscriptionruntime.StateProbeRequest,
	) (subscriptionruntime.StateProbeResult, error) {
		entered <- request.Model
		<-release
		return subscriptionruntime.StateProbeResult{TurnState: captures[request.Model]}, nil
	}

	if _, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshTestModel); err != nil {
		t.Fatalf("StartCredentialStateRefresh() error = %v", err)
	}
	if _, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshOtherModel); err != nil {
		t.Fatalf("StartCredentialStateRefresh() error = %v", err)
	}
	// 两个模型同时刷新：各自报告运行态，凭据级快照列出全部在跑刷新的模型，
	// 刷新记录不再按模型过滤，界面靠记录自带的模型名区分。
	first, err := fixture.service.GetCredentialState(t.Context(), groupID, credentialID, stateRefreshTestModel)
	if err != nil {
		t.Fatalf("GetCredentialState() error = %v", err)
	}
	second, err := fixture.service.GetCredentialState(t.Context(), groupID, credentialID, stateRefreshOtherModel)
	if err != nil {
		t.Fatalf("GetCredentialState() error = %v", err)
	}
	if !first.Running || !second.Running || first.Model != stateRefreshTestModel || second.Model != stateRefreshOtherModel {
		t.Fatalf("running snapshots = %#v / %#v", first, second)
	}
	wantRunning := []string{stateRefreshOtherModel, stateRefreshTestModel}
	if !slices.Equal(first.RunningModels, wantRunning) || !slices.Equal(second.RunningModels, wantRunning) {
		t.Fatalf("running models = %#v / %#v, want %#v", first.RunningModels, second.RunningModels, wantRunning)
	}
	close(release)
	waitStateRefreshIdle(t, fixture.service, credentialID, stateRefreshTestModel)
	waitStateRefreshIdle(t, fixture.service, credentialID, stateRefreshOtherModel)

	for _, model := range []string{stateRefreshTestModel, stateRefreshOtherModel} {
		records := stateRefreshRecords(t, fixture, credentialID, model)
		if len(records) != 1 || records[0].Status != models.CredentialStateRefreshSucceeded ||
			records[0].TurnState != captures[model] || records[0].Model != model {
			t.Fatalf("refresh records of %s = %#v", model, records)
		}
		capture := turnStateCapture(t, fixture, credentialID, model)
		if capture == nil || capture.TurnState != captures[model] {
			t.Fatalf("persisted turn state of %s = %#v", model, capture)
		}
	}

	ref, ok := fixture.registry.CredentialRef(credentialID)
	if !ok {
		t.Fatalf("credential %d is not published", credentialID)
	}
	for model, value := range captures {
		if got := ref.TurnStateFor(model, now); got != value {
			t.Fatalf("published %s state = %q, want %q", model, got, value)
		}
	}
	state, err := fixture.service.GetCredentialState(t.Context(), groupID, credentialID, stateRefreshOtherModel)
	if err != nil {
		t.Fatalf("GetCredentialState() error = %v", err)
	}
	if len(state.States) != 2 || len(state.RunningModels) != 0 {
		t.Fatalf("state payload = %#v", state)
	}
	// 记录是凭据级的：两个模型各自的一次捕获都在同一份列表里。
	recorded := make(map[string]bool, len(state.Logs))
	for _, record := range state.Logs {
		recorded[record.Model] = true
	}
	if len(state.Logs) != 2 || !recorded[stateRefreshTestModel] || !recorded[stateRefreshOtherModel] {
		t.Fatalf("refresh records = %#v", state.Logs)
	}
	for model, value := range captures {
		if entry := modelState(t, state, model); entry.TurnState != value || entry.StateLength != execution.CodexTurnStateLength {
			t.Fatalf("retained %s state = %#v", model, entry)
		}
	}
}

func TestCredentialStateRefreshRoutesStartStopAndRejectAPIKeyGroups(t *testing.T) {
	t.Parallel()

	initControlI18n(t)
	fixture, groupID, credentialID := newStateRefreshFixture(t)
	entered := make(chan struct{})
	fixture.service.probeSubscriptionTurnState = func(
		ctx context.Context,
		_ channel.ID,
		_ subscriptionruntime.Credential,
		_ subscriptionruntime.Target,
		_ subscriptionruntime.StateProbeRequest,
	) (subscriptionruntime.StateProbeResult, error) {
		select {
		case <-entered:
		default:
			close(entered)
		}
		<-ctx.Done()
		return subscriptionruntime.StateProbeResult{}, ctx.Err()
	}
	engine := gin.New()
	NewServer(&config.Config{AuthKey: "test-auth-key"}, fixture.service).RegisterRoutes(engine)
	authorize := func(request *http.Request) *http.Request {
		request.Header.Set("Authorization", "Bearer test-auth-key")
		request.Header.Set("Content-Type", "application/json")
		return request
	}
	path := fmt.Sprintf("/api/groups/%d/credentials/%d/state-refresh", groupID, credentialID)

	started := httptest.NewRecorder()
	engine.ServeHTTP(started, authorize(httptest.NewRequest(http.MethodPost, path,
		strings.NewReader(fmt.Sprintf(`{"model":%q}`, stateRefreshTestModel)))))
	if started.Code != http.StatusOK {
		t.Fatalf("state refresh start = %d %s", started.Code, started.Body)
	}
	var startEnvelope struct {
		Data CredentialStateResponse `json:"data"`
	}
	if err := json.Unmarshal(started.Body.Bytes(), &startEnvelope); err != nil {
		t.Fatal(err)
	}
	if !startEnvelope.Data.Running || startEnvelope.Data.Model != stateRefreshTestModel {
		t.Fatalf("state refresh start payload = %#v", startEnvelope.Data)
	}
	<-entered

	// 未选择模型或选择分组未配置的模型时同步拒绝。
	for _, body := range []string{`{}`, `{"model":"gpt-unknown"}`} {
		denied := httptest.NewRecorder()
		engine.ServeHTTP(denied, authorize(httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))))
		if denied.Code != http.StatusBadRequest {
			t.Fatalf("state refresh start %s = %d %s", body, denied.Code, denied.Body)
		}
	}

	stopped := httptest.NewRecorder()
	engine.ServeHTTP(stopped, authorize(httptest.NewRequest(http.MethodDelete, path+"?model="+stateRefreshTestModel, nil)))
	if stopped.Code != http.StatusOK {
		t.Fatalf("state refresh stop = %d %s", stopped.Code, stopped.Body)
	}
	var stopEnvelope struct {
		Data CredentialStateResponse `json:"data"`
	}
	if err := json.Unmarshal(stopped.Body.Bytes(), &stopEnvelope); err != nil {
		t.Fatal(err)
	}
	if stopEnvelope.Data.Running || len(stopEnvelope.Data.Logs) != 1 ||
		stopEnvelope.Data.Logs[0].ErrorCode != "canceled" {
		t.Fatalf("state refresh stop payload = %#v", stopEnvelope.Data)
	}

	read := httptest.NewRecorder()
	engine.ServeHTTP(read, authorize(httptest.NewRequest(http.MethodGet, path, nil)))
	if read.Code != http.StatusOK {
		t.Fatalf("state read = %d %s", read.Code, read.Body)
	}
	var readEnvelope struct {
		Data CredentialStateResponse `json:"data"`
	}
	if err := json.Unmarshal(read.Body.Bytes(), &readEnvelope); err != nil {
		t.Fatal(err)
	}
	if readEnvelope.Data.Running || len(readEnvelope.Data.Logs) != 1 ||
		readEnvelope.Data.Model != stateRefreshTestModel ||
		readEnvelope.Data.RequiredLength != execution.CodexTurnStateLength {
		t.Fatalf("state read payload = %#v", readEnvelope.Data)
	}

	group := models.Group{
		Name: "api-key-state", ChannelID: string(channel.OpenAI),
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
	deniedPath := fmt.Sprintf("/api/groups/%d/credentials/%d/state-refresh", group.ID, other.ID)
	for _, method := range []string{http.MethodPost, http.MethodDelete, http.MethodGet} {
		denied := httptest.NewRecorder()
		engine.ServeHTTP(denied, authorize(httptest.NewRequest(method, deniedPath, nil)))
		if denied.Code != http.StatusBadRequest {
			t.Fatalf("api-key %s = %d %s", method, denied.Code, denied.Body)
		}
	}
}

// A credential that needs reauthorization cannot be probed: the run records why
// and stops instead of hammering the upstream.
func TestCredentialStateRefreshRecordsReauthorizationRequired(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	probes := 0
	fixture.service.probeSubscriptionTurnState = func(
		context.Context,
		channel.ID,
		subscriptionruntime.Credential,
		subscriptionruntime.Target,
		subscriptionruntime.StateProbeRequest,
	) (subscriptionruntime.StateProbeResult, error) {
		probes++
		return subscriptionruntime.StateProbeResult{}, nil
	}
	if err := fixture.db.Model(&models.Credential{}).
		Where("id = ?", credentialID).
		Update("auth_state", models.CredentialAuthStateReauthorizationRequired).Error; err != nil {
		t.Fatal(err)
	}

	if _, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID, stateRefreshTestModel); err != nil {
		t.Fatalf("StartCredentialStateRefresh() error = %v", err)
	}
	waitStateRefreshIdle(t, fixture.service, credentialID, stateRefreshTestModel)
	records := waitStateRefreshRecords(t, fixture, credentialID, stateRefreshTestModel, 1)
	if len(records) != 1 || records[0].Status != models.CredentialStateRefreshFailed ||
		records[0].ErrorCode != "unauthorized" || records[0].Attempts != 1 ||
		records[0].Model != stateRefreshTestModel || records[0].Input != stateRefreshInput {
		t.Fatalf("reauthorization refresh records = %#v", records)
	}
	if probes != 0 {
		t.Fatalf("probe calls = %d, want 0", probes)
	}
}
