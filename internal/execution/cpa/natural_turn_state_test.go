package cpa

import (
	"bytes"
	"context"
	"io"
	"net/http"
	"strings"
	"sync"
	"testing"
	"time"

	"gpt-load/internal/execution"
	"gpt-load/internal/subscription/providers/codex"
)

type recordingTurnStateObserver struct {
	mu           sync.Mutex
	observations []execution.TurnStateObservation
	uses         []turnStateUse
}

type turnStateUse struct {
	credentialID uint
	model        string
}

func (observer *recordingTurnStateObserver) ObserveTurnState(observation execution.TurnStateObservation) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	observer.observations = append(observer.observations, observation)
}

func (observer *recordingTurnStateObserver) ObserveTurnStateUse(credentialID uint, model string) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	observer.uses = append(observer.uses, turnStateUse{credentialID: credentialID, model: model})
}

func (observer *recordingTurnStateObserver) recordedUses() []turnStateUse {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return append([]turnStateUse(nil), observer.uses...)
}

func (observer *recordingTurnStateObserver) recorded() []execution.TurnStateObservation {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return append([]execution.TurnStateObservation(nil), observer.observations...)
}

func completeTurnState() string {
	return strings.Repeat("s", execution.CodexTurnStateLength)
}

// rotatedTurnState 是上游签发的另一种令牌形状（生产实测 312 字节）：长度不是本
// 产品约定的值，任何载体上的这种值都不是捕获。
func rotatedTurnState() string {
	return strings.Repeat("s", 312)
}

// An attempt that carried no state reports the complete state its upstream
// returned, so the control plane can retain it for the same credential and
// model. A replayed state, a header of any other length, and a header-less
// attempt never report anything.
func TestAdapterReportsNaturalTurnStateOnlyForAttemptsWithoutAReplayedState(t *testing.T) {
	for _, test := range []struct {
		name      string
		turnState string
		replayed  bool
		want      string
	}{
		{name: "complete state", turnState: completeTurnState(), want: completeTurnState()},
		{name: "upstream rotated length", turnState: rotatedTurnState()},
		{name: "incomplete state", turnState: "short"},
		{name: "missing state"},
		{name: "replayed state", turnState: completeTurnState(), replayed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			adapter, _, _, keyService, row := newAdapterFixture(t, credentialJSON("access", "refresh", time.Now().Add(time.Hour)))
			observer := &recordingTurnStateObserver{}
			adapter.SetTurnStateObserver(observer)
			headers := make(http.Header)
			if test.turnState != "" {
				headers.Set(execution.CodexTurnStateHeader, test.turnState)
			}
			setCodexExecutor(t, adapter, &fakeExecutor{result: codex.ExecuteResponse{
				Payload: []byte(`{"id":"resp_1","model":"gpt-5","output":[]}`),
				Headers: headers,
			}})
			spec := validSpec(t, row, keyService)
			spec.TurnStateReplayed = test.replayed

			adapter.Execute(t.Context(), spec)

			observations := observer.recorded()
			if test.want == "" {
				if len(observations) != 0 {
					t.Fatalf("observations = %#v, want none", observations)
				}
				return
			}
			if len(observations) != 1 {
				t.Fatalf("observations = %#v", observations)
			}
			observation := observations[0]
			if observation.CredentialID != spec.Credential.ID ||
				observation.IdentityGeneration != spec.Credential.IdentityGeneration ||
				observation.Model != spec.UpstreamModel || observation.TurnState != test.want ||
				observation.ObservedAtMS <= 0 {
				t.Fatalf("observation = %#v", observation)
			}
		})
	}
}

// A streaming attempt reports the state of its response headers before the
// stream is consumed, and a stream without the header reports nothing.
func TestAdapterReportsNaturalTurnStateFromStreamHeaders(t *testing.T) {
	adapter, _, _, keyService, row := newAdapterFixture(t, credentialJSON("access", "refresh", time.Now().Add(time.Hour)))
	observer := &recordingTurnStateObserver{}
	adapter.SetTurnStateObserver(observer)
	chunks := make(chan codex.ExecuteStreamChunk, 1)
	chunks <- codex.ExecuteStreamChunk{Payload: []byte(`data: {"type":"response.completed","response":{"id":"resp_1","model":"gpt-5"}}`)}
	close(chunks)
	setCodexExecutor(t, adapter, &fakeExecutor{stream: &codex.ExecuteStreamResponse{
		Headers: http.Header{execution.CodexTurnStateHeader: {completeTurnState()}},
		Chunks:  chunks,
	}})

	adapter.ExecuteStream(t.Context(), validSpec(t, row, keyService), func(execution.StreamEvent) error { return nil })

	observations := observer.recorded()
	if len(observations) != 1 || observations[0].TurnState != completeTurnState() {
		t.Fatalf("observations = %#v", observations)
	}
}

// The state is read from the real upstream response the Codex executor returns,
// so the same header a manual refresh probes for is what a live attempt captures.
func TestAdapterCapturesTurnStateFromRealUpstreamResponse(t *testing.T) {
	adapter, _, _, keyService, row := newAdapterFixture(t, credentialJSON("access", "refresh", time.Now().Add(time.Hour)))
	observer := &recordingTurnStateObserver{}
	adapter.SetTurnStateObserver(observer)
	complete := completeTurnState()
	var upstreamStates []string
	transport := roundTripperFunc(func(req *http.Request) (*http.Response, error) {
		body, err := io.ReadAll(req.Body)
		if err != nil {
			return nil, err
		}
		upstreamStates = append(upstreamStates, req.Header.Get(execution.CodexTurnStateHeader))
		header := http.Header{"Content-Type": {"text/event-stream"}}
		header.Set(execution.CodexTurnStateHeader, complete)
		payload := `data: {"type":"response.completed","response":{"id":"resp_live","object":"response","status":"completed","model":"gpt-5","output":[],"usage":{"input_tokens":4,"output_tokens":1,"total_tokens":5}}}` + "\n\n"
		if !bytes.Contains(body, []byte(`"stream":true`)) {
			header.Set("Content-Type", "application/json")
			payload = `{"id":"resp_live","object":"response","status":"completed","model":"gpt-5","output":[]}`
		}
		return &http.Response{StatusCode: 200, Header: header, Body: io.NopCloser(strings.NewReader(payload)), Request: req}, nil
	})
	ctx := context.WithValue(t.Context(), "cliproxy.roundtripper", http.RoundTripper(transport))

	if evidence := adapter.ExecuteStream(ctx, validSpec(t, row, keyService), func(execution.StreamEvent) error { return nil }).Error; evidence != nil {
		t.Fatalf("stream execution failed: %+v", evidence)
	}
	if evidence := adapter.Execute(ctx, validSpec(t, row, keyService)).Error; evidence != nil {
		t.Fatalf("unary execution failed: %+v", evidence)
	}

	observations := observer.recorded()
	if len(observations) != 2 {
		t.Fatalf("observations = %#v", observations)
	}
	for _, observation := range observations {
		if observation.TurnState != complete || observation.Model != "gpt-5" || observation.CredentialID != row.ID {
			t.Fatalf("observation = %#v", observation)
		}
	}
	// 这两次尝试都没有回放 state，上游也没有收到 state 请求头。
	if len(upstreamStates) != 2 || upstreamStates[0] != "" || upstreamStates[1] != "" {
		t.Fatalf("upstream request states = %#v", upstreamStates)
	}
}

type turnStateEventSession struct {
	execution.WebsocketSession
	event  []byte
	result execution.WebsocketResult
}

func (session *turnStateEventSession) ExecuteTurn(ctx context.Context, _ []byte, emit func(context.Context, []byte) error) execution.WebsocketResult {
	if err := emit(ctx, session.event); err != nil {
		return execution.WebsocketResult{Error: &execution.ErrorEvidence{Code: "consumer_failed"}}
	}
	return session.result
}

// WS 上游把 state 放在元数据事件里，观察包装必须在转发事件的同时采下它，
// 且已回放 state 的尝试不得再次上报；长度不符的元数据值同样不上报。
func TestObservedWebsocketSessionReportsTurnStateFromMetadataEvents(t *testing.T) {
	state := completeTurnState()
	event := []byte(`{"type":"codex.response.metadata","headers":{"x-codex-turn-state":"` + state + `"}}`)
	for _, test := range []struct {
		name     string
		event    []byte
		replayed bool
		want     bool
	}{
		{name: "natural capture", event: event, want: true},
		{name: "rotated length", event: []byte(`{"type":"codex.response.metadata","headers":{"x-codex-turn-state":"` + rotatedTurnState() + `"}}`)},
		{name: "replayed state", event: event, replayed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			adapter, _, _, keyService, row := newAdapterFixture(t, credentialJSON("access", "refresh", time.Now().Add(time.Hour)))
			observer := &recordingTurnStateObserver{}
			adapter.SetTurnStateObserver(observer)
			spec := validSpec(t, row, keyService)
			spec.TurnStateReplayed = test.replayed
			session := &observedWebsocketSession{
				WebsocketSession: &turnStateEventSession{event: test.event},
				adapter:          adapter, turnStateSpec: spec, baseURL: "https://upstream.example", proxyURL: "direct",
			}
			forwarded := 0
			session.ExecuteTurn(t.Context(), nil, func(_ context.Context, payload []byte) error {
				forwarded++
				if !bytes.Equal(payload, test.event) {
					t.Fatalf("forwarded event = %s", payload)
				}
				return nil
			})

			if forwarded != 1 {
				t.Fatalf("forwarded events = %d", forwarded)
			}
			observations := observer.recorded()
			if !test.want {
				if len(observations) != 0 {
					t.Fatalf("observations = %#v, want none", observations)
				}
				return
			}
			if len(observations) != 1 {
				t.Fatalf("observations = %#v", observations)
			}
			observation := observations[0]
			if observation.TurnState != state || observation.Model != spec.UpstreamModel ||
				observation.CredentialID != spec.Credential.ID ||
				observation.IdentityGeneration != spec.Credential.IdentityGeneration ||
				observation.BaseURL != "https://upstream.example" || observation.ProxyURL != "direct" {
				t.Fatalf("observation = %#v", observation)
			}
		})
	}
}

// 自动 State 刷新只维护真实请求过的模型，所以每一次进入派发的尝试都要上报
// (凭据, 上游模型)：上游返回长度不符的令牌（生产实测 312 字节）时同样上报，
// 否则那个模型永远不会被自动刷新；只走本地计数的尝试没有派发，不上报。
func TestAdapterReportsUsedModelForEveryDispatchedAttempt(t *testing.T) {
	adapter, _, _, keyService, row := newAdapterFixture(t, credentialJSON("access", "refresh", time.Now().Add(time.Hour)))
	observer := &recordingTurnStateObserver{}
	adapter.SetTurnStateObserver(observer)
	setCodexExecutor(t, adapter, &fakeExecutor{result: codex.ExecuteResponse{
		Payload: []byte(`{"id":"resp_1","model":"gpt-5","output":[]}`),
		Headers: http.Header{execution.CodexTurnStateHeader: {rotatedTurnState()}},
	}})
	spec := validSpec(t, row, keyService)

	result := adapter.Execute(t.Context(), spec)
	if result.Error != nil {
		t.Fatalf("execute = %+v", result)
	}
	uses := observer.recordedUses()
	if len(uses) != 1 || uses[0].credentialID != spec.Credential.ID || uses[0].model != spec.UpstreamModel {
		t.Fatalf("uses = %#v", uses)
	}
	if observations := observer.recorded(); len(observations) != 0 {
		t.Fatalf("rotated state was captured: %#v", observations)
	}
}

// 上游模型才是 State 的归属：客户端模型不同、上游模型相同的请求上报同一个模型。
func TestAdapterReportsUsedModelByUpstreamModel(t *testing.T) {
	adapter, _, _, keyService, row := newAdapterFixture(t, credentialJSON("access", "refresh", time.Now().Add(time.Hour)))
	observer := &recordingTurnStateObserver{}
	adapter.SetTurnStateObserver(observer)
	setCodexExecutor(t, adapter, &fakeExecutor{result: codex.ExecuteResponse{
		Payload: []byte(`{"id":"resp_1","model":"gpt-5","output":[]}`),
	}})
	spec := validSpec(t, row, keyService)
	spec.ClientModel = "client-alias"
	spec.UpstreamModel = "gpt-5-codex"

	if result := adapter.Execute(t.Context(), spec); result.Error != nil {
		t.Fatalf("execute = %+v", result)
	}
	uses := observer.recordedUses()
	if len(uses) != 1 || uses[0].model != "gpt-5-codex" {
		t.Fatalf("uses = %#v", uses)
	}
}

// 本地计数的尝试不经过上游凭据，因此不算「用过」这个模型。
func TestAdapterDoesNotReportUsedModelForLocalTokenCount(t *testing.T) {
	adapter, _, _, keyService, row := newAdapterFixture(t, credentialJSON("access", "refresh", time.Now().Add(time.Hour)))
	observer := &recordingTurnStateObserver{}
	adapter.SetTurnStateObserver(observer)
	preparer := &fakeCredentialPreparer{delegate: adapter.credentials}
	adapter.credentials = preparer
	setCodexExecutor(t, adapter, &fakeExecutor{countResult: codex.ExecuteResponse{
		Payload: []byte(`{"object":"response.input_tokens","input_tokens":7}`),
	}})
	spec := validSpec(t, row, keyService)
	spec.Operation = execution.OperationResponsesInputTokens
	spec.Body = []byte(`{"model":"gpt-5","input":"hello"}`)

	result := adapter.Execute(t.Context(), spec)
	if result.Error != nil || result.DispatchState != execution.DispatchLocal || preparer.calls != 0 {
		t.Fatalf("result=%+v prepareCalls=%d", result, preparer.calls)
	}
	if uses := observer.recordedUses(); len(uses) != 0 {
		t.Fatalf("uses = %#v, want none", uses)
	}
}

// 打开 WS 会话也是一次真实派发：没有响应头可看也要上报，自动刷新才能在 WS 使用
// 期间维持 State。
func TestAdapterReportsUsedModelWhenOpeningWebsocket(t *testing.T) {
	adapter, _, _, keyService, row := newAdapterFixture(t, credentialJSON("access", "refresh", time.Now().Add(time.Hour)))
	observer := &recordingTurnStateObserver{}
	adapter.SetTurnStateObserver(observer)
	spec := validSpec(t, row, keyService)

	session, result := adapter.OpenWebsocket(t.Context(), spec)
	if result.Error != nil || session == nil {
		t.Fatalf("open=%+v", result)
	}
	defer session.Close()
	uses := observer.recordedUses()
	if len(uses) != 1 || uses[0].credentialID != spec.Credential.ID || uses[0].model != spec.UpstreamModel {
		t.Fatalf("uses = %#v", uses)
	}
}

// 首次使用 WS 连接：握手响应头先到，可能带着另一种长度的令牌；真正的值在随后的
// 元数据事件里。长度不符的握手值必须被丢弃，否则它会占用捕获名额，首次 WS 使用
// 就永远拿不到 state。
func TestObservedWebsocketSessionIgnoresRotatedHandshakeStateBeforeMetadataEvent(t *testing.T) {
	adapter, _, _, keyService, row := newAdapterFixture(t, credentialJSON("access", "refresh", time.Now().Add(time.Hour)))
	observer := &recordingTurnStateObserver{}
	adapter.SetTurnStateObserver(observer)
	state := completeTurnState()
	session := &observedWebsocketSession{
		WebsocketSession: &turnStateEventSession{
			event: []byte(`{"type":"codex.response.metadata","headers":{"x-codex-turn-state":"` + state + `"}}`),
		},
		adapter: adapter, turnStateSpec: validSpec(t, row, keyService),
		baseURL: "https://upstream.example", proxyURL: "direct",
	}
	session.observeHeaders(http.Header{execution.CodexTurnStateHeader: {rotatedTurnState()}}, time.Now())
	session.ExecuteTurn(t.Context(), nil, func(context.Context, []byte) error { return nil })

	observations := observer.recorded()
	if len(observations) != 1 || observations[0].TurnState != state {
		t.Fatalf("observations = %#v", observations)
	}
}

// An adapter without an observer keeps no state and never fails the attempt.
func TestAdapterToleratesMissingTurnStateObserver(t *testing.T) {
	adapter, _, _, keyService, row := newAdapterFixture(t, credentialJSON("access", "refresh", time.Now().Add(time.Hour)))
	setCodexExecutor(t, adapter, &fakeExecutor{result: codex.ExecuteResponse{
		Payload: []byte(`{"id":"resp_1","model":"gpt-5","output":[]}`),
		Headers: http.Header{execution.CodexTurnStateHeader: {completeTurnState()}},
	}})

	if result := adapter.Execute(t.Context(), validSpec(t, row, keyService)); result.Error != nil {
		t.Fatalf("unobserved attempt failed: %#v", result.Error)
	}
}
