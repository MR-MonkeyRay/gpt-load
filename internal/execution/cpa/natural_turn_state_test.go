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
}

func (observer *recordingTurnStateObserver) ObserveTurnState(observation execution.TurnStateObservation) {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	observer.observations = append(observer.observations, observation)
}

func (observer *recordingTurnStateObserver) recorded() []execution.TurnStateObservation {
	observer.mu.Lock()
	defer observer.mu.Unlock()
	return append([]execution.TurnStateObservation(nil), observer.observations...)
}

func completeTurnState() string {
	return strings.Repeat("s", execution.CodexTurnStateLength)
}

// rotatedTurnState 是上游调整令牌长度后真实出现的形状（生产已观察到 312）。
func rotatedTurnState() string {
	return strings.Repeat("s", 312)
}

// An attempt that carried no state reports the complete state its upstream
// returned, so the control plane can retain it for the same credential and
// model. A replayed state, an incomplete header, and a header-less attempt never
// report anything.
func TestAdapterReportsNaturalTurnStateOnlyForAttemptsWithoutAReplayedState(t *testing.T) {
	for _, test := range []struct {
		name      string
		turnState string
		replayed  bool
		want      string
	}{
		{name: "complete state", turnState: completeTurnState(), want: completeTurnState()},
		{name: "upstream rotated length", turnState: rotatedTurnState(), want: rotatedTurnState()},
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
// 且已回放 state 的尝试不得再次上报。
func TestObservedWebsocketSessionReportsTurnStateFromMetadataEvents(t *testing.T) {
	state := completeTurnState()
	event := []byte(`{"type":"codex.response.metadata","headers":{"x-codex-turn-state":"` + state + `"}}`)
	for _, test := range []struct {
		name     string
		replayed bool
		want     bool
	}{
		{name: "natural capture", want: true},
		{name: "replayed state", replayed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			adapter, _, _, keyService, row := newAdapterFixture(t, credentialJSON("access", "refresh", time.Now().Add(time.Hour)))
			observer := &recordingTurnStateObserver{}
			adapter.SetTurnStateObserver(observer)
			spec := validSpec(t, row, keyService)
			spec.TurnStateReplayed = test.replayed
			session := &observedWebsocketSession{
				WebsocketSession: &turnStateEventSession{event: event},
				adapter:          adapter, turnStateSpec: spec, baseURL: "https://upstream.example", proxyURL: "direct",
			}
			forwarded := 0
			session.ExecuteTurn(t.Context(), nil, func(_ context.Context, payload []byte) error {
				forwarded++
				if !bytes.Equal(payload, event) {
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
