package gateway

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"gpt-load/internal/dialect"
	"gpt-load/internal/execution"
	"gpt-load/internal/protocol"
	"gpt-load/internal/state"
	"gpt-load/internal/testutil/turnstatetest"
)

// A captured turn state reaches the upstream request only for the model it was
// resolved for: the handler picks the value by model, the execution layer only
// injects what it was given.
func TestTurnStateInjectionFollowsTheResolvedModel(t *testing.T) {
	base := func() ForwardInput {
		return ForwardInput{
			Dialect:         dialect.NewOpenAIResponses(),
			Group:           state.GroupView{},
			Request:         &dialect.ParsedRequest{Method: "POST", Path: "/v1/responses", Header: map[string][]string{}, Body: []byte(`{"model":"gpt-6-astra","input":"ping"}`)},
			ExternalModel:   "gpt-6-astra",
			ClientProtocol:  protocol.OpenAIResponses,
			Operation:       execution.OperationResponsesCreate,
			ChannelID:       "codex",
			RequestID:       "req-1",
			AttemptID:       "req-1:1",
			AttemptSequence: 1,
			RouteMode:       execution.RouteNative,
			Credential:      execution.NewCredentialSnapshot(7, 1, 1, []byte(`{"access_token":"token"}`)),
		}
	}
	// 解析模型时上游模型优先，缺省时回退到外部模型。
	for _, test := range []struct {
		upstream string
		external string
		want     string
	}{
		{upstream: "gpt-5-codex", external: "gpt-6-astra", want: "gpt-5-codex"},
		{upstream: "", external: "gpt-6-astra", want: "gpt-6-astra"},
		{upstream: "", external: "", want: ""},
	} {
		if got := TurnStateModel(test.upstream, test.external); got != test.want {
			t.Fatalf("TurnStateModel(%q, %q) = %q, want %q", test.upstream, test.external, got, test.want)
		}
	}

	input := base()
	input.UpstreamModelID = "gpt-6-astra"
	input.CredentialTurnState = "state-value"
	spec, err := newExecutionAttemptSpec(input)
	if err != nil {
		t.Fatalf("spec error = %v", err)
	}
	if got := spec.Header.Get(execution.CodexTurnStateHeader); got != "state-value" {
		t.Fatalf("injected header = %q, want state-value", got)
	}

	// 没有解析出捕获值的请求绝不注入。
	empty := base()
	empty.UpstreamModelID = "gpt-6-astra"
	spec, err = newExecutionAttemptSpec(empty)
	if err != nil {
		t.Fatalf("spec error = %v", err)
	}
	if got := spec.Header.Get(execution.CodexTurnStateHeader); got != "" {
		t.Fatalf("empty state injected: %q", got)
	}

	if strings.TrimSpace(execution.CodexTurnStateModel) != "gpt-6-astra" || execution.CodexTurnStateLength != 292 {
		t.Fatalf("constants drifted: %q/%d", execution.CodexTurnStateModel, execution.CodexTurnStateLength)
	}
}

// A captured turn state reaches the upstream request and the request log only
// for the model it was captured for, and only while it is still valid.
func TestHandlerRecordsInjectedTurnStateOnlyForItsModel(t *testing.T) {
	captured := turnstatetest.Value(0)
	for _, test := range []struct {
		name  string
		model string
		// refreshedAtMS 是发布捕获时的记录时间：有效期由它 + 1 小时决定。
		refreshedAtMS int64
		// wantInput 是进入执行层的状态：过期值在注册表里就不会发布。
		wantInput string
		// wantValue 是请求日志记录的状态：只有目标模型会真正注入。
		wantValue string
	}{
		{
			name: "captured model", model: execution.CodexTurnStateModel,
			refreshedAtMS: time.Now().UnixMilli(), wantInput: captured, wantValue: captured,
		},
		{
			name: "other model", model: "gpt-5",
			refreshedAtMS: time.Now().UnixMilli(), wantInput: "", wantValue: "",
		},
		{
			name: "expired state", model: execution.CodexTurnStateModel,
			refreshedAtMS: time.Now().Add(-2 * time.Hour).UnixMilli(), wantInput: "", wantValue: "",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			forwarder := &scriptedForwarder{results: []UpstreamResult{{
				StatusCode: http.StatusOK, Header: make(http.Header),
				Body: []byte(`{"id":"chatcmpl-1"}`), RequestWritten: true,
			}}}
			sink := &recordingRequestLogSink{}
			engine, _, _, registry := newRequestLogHandlerTestRuntimeWithModel(
				t, forwarder, &recordingAccessKeyRPMLimiter{}, sink, test.model, "sk-first",
			)
			if !registry.SetCredentialTurnState(1, execution.CodexTurnStateModel, captured, test.refreshedAtMS) {
				t.Fatal("SetCredentialTurnState() did not publish the value")
			}
			request := httptest.NewRequest(
				http.MethodPost,
				"/v1/chat/completions",
				strings.NewReader(fmt.Sprintf(`{"model":%q}`, test.model)),
			)
			request.Header.Set("Authorization", "Bearer gl-client")
			response := httptest.NewRecorder()
			engine.ServeHTTP(response, request)

			if response.Code != http.StatusOK || len(forwarder.inputs) != 1 {
				t.Fatalf("response/inputs = %d/%#v", response.Code, forwarder.inputs)
			}
			if got := forwarder.inputs[0].CredentialTurnState; got != test.wantInput {
				t.Fatalf("forward credential turn state = %q, want %q", got, test.wantInput)
			}
			events := sink.snapshot()
			if len(events) != 1 || events[0].TurnState != test.wantValue {
				t.Fatalf("recorded turn state = %#v", events)
			}
		})
	}
}
