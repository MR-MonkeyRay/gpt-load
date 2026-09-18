package control

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
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
)

func TestRefreshCredentialStateRetriesUntilCompleteTurnState(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newSubscriptionCredentialFixture(t)
	now := time.UnixMilli(1_800_000_000_000)
	fixture.service.now = func() time.Time { return now }
	complete := strings.Repeat("s", execution.CodexTurnStateLength)
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

	response, err := fixture.service.RefreshCredentialState(t.Context(), groupID, credentialID)
	if err != nil {
		t.Fatalf("RefreshCredentialState() error = %v", err)
	}
	if probe, ok := fixture.service.subscriptions.StateProbe(channel.Codex); !ok || probe.ID() != modules.CodexStateProbe {
		t.Fatalf("codex state probe = %#v, found = %t", probe, ok)
	}
	if probe, ok := fixture.service.subscriptions.StateProbe(channel.OpenAI); ok || probe != nil {
		t.Fatalf("api-key channel exposed a state probe: %#v", probe)
	}
	if response.TurnState != complete || response.Attempts != 3 || response.RefreshedAtMS != now.UnixMilli() {
		t.Fatalf("response = %#v", response)
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
	if row.TurnState != complete {
		t.Fatalf("persisted turn state = %q", row.TurnState)
	}
	ref, ok := fixture.registry.CredentialRef(credentialID)
	if !ok || ref.TurnState != complete {
		t.Fatalf("published reference = %#v, found = %t", ref, ok)
	}
}

func TestRefreshCredentialStateStopsAfterBoundedAttempts(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newSubscriptionCredentialFixture(t)
	calls := 0
	fixture.service.probeSubscriptionTurnState = func(
		context.Context,
		channel.ID,
		subscriptionruntime.Credential,
		subscriptionruntime.Target,
		subscriptionruntime.StateProbeRequest,
	) (subscriptionruntime.StateProbeResult, error) {
		calls++
		return subscriptionruntime.StateProbeResult{TurnState: "incomplete"}, nil
	}
	if _, err := fixture.service.RefreshCredentialState(t.Context(), groupID, credentialID); err == nil {
		t.Fatal("RefreshCredentialState() accepted an incomplete turn state")
	}
	if calls != stateRefreshMaxAttempts {
		t.Fatalf("probe attempts = %d, want %d", calls, stateRefreshMaxAttempts)
	}
	var row models.Credential
	if err := fixture.db.Take(&row, credentialID).Error; err != nil {
		t.Fatal(err)
	}
	if row.TurnState != "" {
		t.Fatalf("turn state was persisted after a failed refresh: %q", row.TurnState)
	}

	fixture.service.probeSubscriptionTurnState = func(
		context.Context,
		channel.ID,
		subscriptionruntime.Credential,
		subscriptionruntime.Target,
		subscriptionruntime.StateProbeRequest,
	) (subscriptionruntime.StateProbeResult, error) {
		return subscriptionruntime.StateProbeResult{}, &subscriptionruntime.UpstreamHTTPError{StatusCode: 401}
	}
	_, err := fixture.service.RefreshCredentialState(t.Context(), groupID, credentialID)
	if !errors.Is(err, app_errors.ErrCredentialReauthorizationRequired) {
		t.Fatalf("unauthorized probe error = %v", err)
	}
}

func TestCredentialStateRefreshRoutePublishesTurnStateAndRejectsAPIKeyGroups(t *testing.T) {
	t.Parallel()

	initControlI18n(t)
	fixture, groupID, credentialID := newSubscriptionCredentialFixture(t)
	complete := strings.Repeat("s", execution.CodexTurnStateLength)
	fixture.service.probeSubscriptionTurnState = func(
		context.Context,
		channel.ID,
		subscriptionruntime.Credential,
		subscriptionruntime.Target,
		subscriptionruntime.StateProbeRequest,
	) (subscriptionruntime.StateProbeResult, error) {
		return subscriptionruntime.StateProbeResult{TurnState: complete}, nil
	}
	engine := gin.New()
	NewServer(&config.Config{AuthKey: "test-auth-key"}, fixture.service).RegisterRoutes(engine)

	request := httptest.NewRequest(http.MethodPost,
		fmt.Sprintf("/api/groups/%d/credentials/%d/state-refresh", groupID, credentialID),
		strings.NewReader(`{}`))
	request.Header.Set("Authorization", "Bearer test-auth-key")
	request.Header.Set("Content-Type", "application/json")
	response := httptest.NewRecorder()
	engine.ServeHTTP(response, request)
	if response.Code != http.StatusOK {
		t.Fatalf("state refresh = %d %s", response.Code, response.Body)
	}
	var envelope struct {
		Data CredentialStateRefreshResponse `json:"data"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &envelope); err != nil {
		t.Fatal(err)
	}
	if envelope.Data.TurnState != complete || envelope.Data.Attempts != 1 {
		t.Fatalf("state refresh payload = %#v", envelope.Data)
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
	denied := httptest.NewRequest(http.MethodPost,
		fmt.Sprintf("/api/groups/%d/credentials/%d/state-refresh", group.ID, other.ID),
		strings.NewReader(`{}`))
	denied.Header.Set("Authorization", "Bearer test-auth-key")
	denied.Header.Set("Content-Type", "application/json")
	deniedResponse := httptest.NewRecorder()
	engine.ServeHTTP(deniedResponse, denied)
	if deniedResponse.Code != http.StatusBadRequest {
		t.Fatalf("api-key state refresh = %d %s", deniedResponse.Code, deniedResponse.Body)
	}
}

func TestRefreshCredentialStateUsesStateProxyPrecedence(t *testing.T) {
	t.Parallel()

	fixture, groupID, credentialID := newSubscriptionCredentialFixture(t)
	complete := strings.Repeat("s", execution.CodexTurnStateLength)
	probed := make(chan subscriptionruntime.StateProbeRequest, 1)
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
		if _, err := fixture.service.RefreshCredentialState(t.Context(), groupID, credentialID); err != nil {
			t.Fatalf("RefreshCredentialState() error = %v", err)
		}
		return <-probed
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
