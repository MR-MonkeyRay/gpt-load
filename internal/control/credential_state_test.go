package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gin-gonic/gin"

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

// newStateRefreshFixture returns a subscription fixture whose refresh runs
// probe as fast as the test can observe them.
func newStateRefreshFixture(t *testing.T) (serviceFixture, uint, uint) {
	t.Helper()
	fixture, groupID, credentialID := newSubscriptionCredentialFixture(t)
	fixture.service.stateRefreshInterval = time.Millisecond
	return fixture, groupID, credentialID
}

func waitStateRefreshIdle(t *testing.T, service *Service, credentialID uint) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		if !service.stateRefreshRunning(credentialID) {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("state refresh run did not stop")
}

func waitStateRefreshRecords(t *testing.T, fixture serviceFixture, credentialID uint, count int) []models.CredentialStateRefreshLog {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		var rows []models.CredentialStateRefreshLog
		if err := fixture.db.Where("credential_id = ?", credentialID).Order("id ASC").Find(&rows).Error; err != nil {
			t.Fatal(err)
		}
		if len(rows) >= count {
			return rows
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("state refresh recorded fewer than %d attempts", count)
	return nil
}

func TestStartCredentialStateRefreshCapturesCompleteTurnState(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	now := time.UnixMilli(1_800_000_000_000)
	fixture.service.now = func() time.Time { return now }
	complete := turnstatetest.Token(now.Add(time.Hour))
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

	started, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID)
	if err != nil {
		t.Fatalf("StartCredentialStateRefresh() error = %v", err)
	}
	if !started.Running {
		t.Fatalf("started refresh = %#v", started)
	}
	if probe, ok := fixture.service.subscriptions.StateProbe(channel.Codex); !ok || probe.ID() != modules.CodexStateProbe {
		t.Fatalf("codex state probe = %#v, found = %t", probe, ok)
	}
	if probe, ok := fixture.service.subscriptions.StateProbe(channel.OpenAI); ok || probe != nil {
		t.Fatalf("api-key channel exposed a state probe: %#v", probe)
	}
	waitStateRefreshIdle(t, fixture.service, credentialID)

	records := waitStateRefreshRecords(t, fixture, credentialID, 3)
	if len(records) != 3 {
		t.Fatalf("refresh records = %#v", records)
	}
	for index, record := range records {
		if record.Attempts != index+1 || record.GroupID != groupID || record.Model != execution.CodexTurnStateModel ||
			record.Input != stateRefreshInput || record.ProxyURL != "direct" || record.CreatedAtMS != now.UnixMilli() {
			t.Fatalf("refresh record %d = %#v", index, record)
		}
	}
	if records[0].Status != models.CredentialStateRefreshFailed || records[0].ErrorCode != "length_mismatch" ||
		records[0].StateLength != len("incomplete") || records[0].TurnState != "" {
		t.Fatalf("first refresh record = %#v", records[0])
	}
	if records[2].Status != models.CredentialStateRefreshSucceeded || records[2].ErrorCode != "" ||
		records[2].StateLength != execution.CodexTurnStateLength || records[2].TurnState != complete {
		t.Fatalf("successful refresh record = %#v", records[2])
	}
	if probed.Model != execution.CodexTurnStateModel || probed.Input != "ping" {
		t.Fatalf("probe request = %#v", probed)
	}
	// No state override and no global proxy is configured, so the probe runs direct.
	if probed.ProxyURL != "direct" || probed.ProxyFromEnvironment {
		t.Fatalf("probe proxy = %#v", probed)
	}

	var row models.Credential
	if err := fixture.db.Take(&row, credentialID).Error; err != nil {
		t.Fatal(err)
	}
	if row.TurnState != complete || row.TurnStateRefreshedAtMS != now.UnixMilli() {
		t.Fatalf("persisted turn state = %#v", row)
	}
	ref, ok := fixture.registry.CredentialRef(credentialID)
	if !ok || ref.TurnState != complete {
		t.Fatalf("published reference = %#v, found = %t", ref, ok)
	}

	state, err := fixture.service.GetCredentialState(t.Context(), groupID, credentialID)
	if err != nil {
		t.Fatalf("GetCredentialState() error = %v", err)
	}
	if state.Running || state.TurnState != complete || state.RequiredLength != execution.CodexTurnStateLength ||
		state.ExpiresAtMS == nil || *state.ExpiresAtMS != now.Add(time.Hour).UnixMilli() ||
		state.RefreshedAtMS == nil || *state.RefreshedAtMS != now.UnixMilli() || len(state.Logs) != 3 {
		t.Fatalf("state payload = %#v", state)
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
	if _, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID); err != nil {
		t.Fatalf("StartCredentialStateRefresh() error = %v", err)
	}
	records := waitStateRefreshRecords(t, fixture, credentialID, 3)
	if _, err := fixture.service.StopCredentialStateRefresh(t.Context(), groupID, credentialID); err != nil {
		t.Fatalf("StopCredentialStateRefresh() error = %v", err)
	}
	waitStateRefreshIdle(t, fixture.service, credentialID)

	records = waitStateRefreshRecords(t, fixture, credentialID, len(records))
	for index, record := range records {
		if record.Status != models.CredentialStateRefreshFailed || record.ErrorCode != "length_mismatch" ||
			record.StateLength != len(observed) || record.Attempts != index+1 {
			t.Fatalf("refresh record %d = %#v", index, record)
		}
	}
	var row models.Credential
	if err := fixture.db.Take(&row, credentialID).Error; err != nil {
		t.Fatal(err)
	}
	if row.TurnState != "" {
		t.Fatalf("incomplete turn state was persisted: %q", row.TurnState)
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
	started, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID)
	if err != nil {
		t.Fatalf("StartCredentialStateRefresh() error = %v", err)
	}
	if !started.Running {
		t.Fatalf("started refresh = %#v", started)
	}
	<-entered
	// 运行中的刷新再次点击开始是幂等的：不新建运行，也不重复探测。
	again, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID)
	if err != nil {
		t.Fatalf("StartCredentialStateRefresh() error = %v", err)
	}
	if !again.Running {
		t.Fatalf("idempotent start = %#v", again)
	}

	stopped, err := fixture.service.StopCredentialStateRefresh(t.Context(), groupID, credentialID)
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
	if idle, err := fixture.service.StopCredentialStateRefresh(t.Context(), groupID, credentialID); err != nil || idle.Running {
		t.Fatalf("idle stop = %#v / %v", idle, err)
	}
}

func TestCredentialStateRefreshSkipsExpiredCapture(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newStateRefreshFixture(t)
	now := time.UnixMilli(1_800_000_000_000)
	fixture.service.now = func() time.Time { return now }
	// 运行期时钟固定在记录时间上，因此有效期必须相对真实时间构造，才能表达“已过期”。
	expiredAt := time.Now().Add(-time.Minute).Truncate(time.Second)
	expired := turnstatetest.Token(expiredAt)
	fixture.service.probeSubscriptionTurnState = func(
		context.Context,
		channel.ID,
		subscriptionruntime.Credential,
		subscriptionruntime.Target,
		subscriptionruntime.StateProbeRequest,
	) (subscriptionruntime.StateProbeResult, error) {
		return subscriptionruntime.StateProbeResult{TurnState: expired}, nil
	}
	if _, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID); err != nil {
		t.Fatalf("StartCredentialStateRefresh() error = %v", err)
	}
	waitStateRefreshIdle(t, fixture.service, credentialID)

	var row models.Credential
	if err := fixture.db.Take(&row, credentialID).Error; err != nil {
		t.Fatal(err)
	}
	if row.TurnState != expired {
		t.Fatalf("persisted turn state = %q", row.TurnState)
	}
	// 过期值仍然留档展示，但绝不注入到上游请求。
	ref, ok := fixture.registry.CredentialRef(credentialID)
	if !ok || ref.TurnState != "" {
		t.Fatalf("expired reference = %#v, found = %t", ref, ok)
	}
	state, err := fixture.service.GetCredentialState(t.Context(), groupID, credentialID)
	if err != nil {
		t.Fatalf("GetCredentialState() error = %v", err)
	}
	if state.ExpiresAtMS == nil || *state.ExpiresAtMS != expiredAt.UnixMilli() ||
		state.TurnState != expired {
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
	if _, err := fixture.service.StartCredentialStateRefresh(t.Context(), apiKeyGroup.ID, other.ID); !errors.Is(err, app_errors.ErrValidation) {
		t.Fatalf("api-key start error = %v", err)
	}
	if _, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID+1000); !errors.Is(err, app_errors.ErrResourceNotFound) {
		t.Fatalf("missing credential start error = %v", err)
	}

	// 上游拒绝授权：重试不会成功，运行在留档后自行结束。
	if _, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID); err != nil {
		t.Fatalf("StartCredentialStateRefresh() error = %v", err)
	}
	waitStateRefreshIdle(t, fixture.service, credentialID)
	records := waitStateRefreshRecords(t, fixture, credentialID, 1)
	if len(records) != 1 || records[0].Status != models.CredentialStateRefreshFailed ||
		records[0].ErrorCode != "unauthorized" || records[0].HTTPStatus == nil ||
		*records[0].HTTPStatus != http.StatusUnauthorized || records[0].Attempts != 1 {
		t.Fatalf("unauthorized refresh records = %#v", records)
	}

	// 其余上游错误继续探测，直到人工停止。
	status.Store(http.StatusServiceUnavailable)
	if _, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID); err != nil {
		t.Fatalf("StartCredentialStateRefresh() error = %v", err)
	}
	records = waitStateRefreshRecords(t, fixture, credentialID, 3)
	if _, err := fixture.service.StopCredentialStateRefresh(t.Context(), groupID, credentialID); err != nil {
		t.Fatalf("StopCredentialStateRefresh() error = %v", err)
	}
	for _, record := range records[1:] {
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
	complete := turnstatetest.Token(now.Add(time.Hour))
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
		if _, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID); err != nil {
			t.Fatalf("StartCredentialStateRefresh() error = %v", err)
		}
		request := <-probed
		waitStateRefreshIdle(t, fixture.service, credentialID)
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
	engine.ServeHTTP(started, authorize(httptest.NewRequest(http.MethodPost, path, strings.NewReader(`{}`))))
	if started.Code != http.StatusOK {
		t.Fatalf("state refresh start = %d %s", started.Code, started.Body)
	}
	var startEnvelope struct {
		Data CredentialStateResponse `json:"data"`
	}
	if err := json.Unmarshal(started.Body.Bytes(), &startEnvelope); err != nil {
		t.Fatal(err)
	}
	if !startEnvelope.Data.Running {
		t.Fatalf("state refresh start payload = %#v", startEnvelope.Data)
	}
	<-entered

	stopped := httptest.NewRecorder()
	engine.ServeHTTP(stopped, authorize(httptest.NewRequest(http.MethodDelete, path, nil)))
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

	if _, err := fixture.service.StartCredentialStateRefresh(t.Context(), groupID, credentialID); err != nil {
		t.Fatalf("StartCredentialStateRefresh() error = %v", err)
	}
	waitStateRefreshIdle(t, fixture.service, credentialID)
	records := waitStateRefreshRecords(t, fixture, credentialID, 1)
	if len(records) != 1 || records[0].Status != models.CredentialStateRefreshFailed ||
		records[0].ErrorCode != "unauthorized" || records[0].Attempts != 1 ||
		records[0].Model != execution.CodexTurnStateModel || records[0].Input != stateRefreshInput {
		t.Fatalf("reauthorization refresh records = %#v", records)
	}
	if probes != 0 {
		t.Fatalf("probe calls = %d, want 0", probes)
	}
}
