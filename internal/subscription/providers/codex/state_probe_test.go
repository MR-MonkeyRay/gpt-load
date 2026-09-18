package codex

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"reflect"
	"testing"
)

type stateProbeExecutor struct {
	request  ExecuteRequest
	streamed context.Context
	response *ExecuteStreamResponse
	err      error
}

func (e *stateProbeExecutor) Execute(context.Context, string, Credential, ExecuteRequest) (ExecuteResponse, error) {
	return ExecuteResponse{}, errors.New("unexpected Execute call")
}

func (e *stateProbeExecutor) CountTokens(context.Context, string, Credential, ExecuteRequest) (ExecuteResponse, error) {
	return ExecuteResponse{}, errors.New("unexpected CountTokens call")
}

func (e *stateProbeExecutor) ExecuteStream(
	ctx context.Context,
	_ string,
	_ Credential,
	request ExecuteRequest,
) (*ExecuteStreamResponse, error) {
	e.streamed = ctx
	e.request = request
	return e.response, e.err
}

func TestProbeTurnStateSendsFixedRequestAndDisconnectsAfterHeaders(t *testing.T) {
	t.Parallel()

	executor := &stateProbeExecutor{response: &ExecuteStreamResponse{
		Headers: http.Header{"X-Codex-Turn-State": {"  turn-state-value  "}},
	}}
	value, err := ProbeTurnState(t.Context(), executor, Credential{}, "https://upstream.example", StateProbeRequest{
		Model: "gpt-6-astra", Input: "ping", ProxyURL: "direct",
	})
	if err != nil {
		t.Fatalf("ProbeTurnState() error = %v", err)
	}
	if value != "turn-state-value" {
		t.Fatalf("turn state = %q", value)
	}
	if executor.request.Format != "openai-response" || executor.request.RequestPath != "/v1/responses" ||
		executor.request.BaseURL != "https://upstream.example" || executor.request.ProxyURL != "direct" ||
		executor.request.ProxyFromEnvironment {
		t.Fatalf("probe request = %#v", executor.request)
	}
	var payload struct {
		Model  string `json:"model"`
		Input  string `json:"input"`
		Stream bool   `json:"stream"`
	}
	if err := json.Unmarshal(executor.request.Payload, &payload); err != nil {
		t.Fatal(err)
	}
	if payload.Model != "gpt-6-astra" || payload.Input != "ping" || !payload.Stream {
		t.Fatalf("probe payload = %s", executor.request.Payload)
	}
	if !reflect.DeepEqual(executor.streamed.Err(), context.Canceled) {
		t.Fatalf("probe stream context error = %v, want cancellation", executor.streamed.Err())
	}
}

func TestProbeTurnStateReportsMissingHeaderAndUpstreamFailure(t *testing.T) {
	t.Parallel()

	value, err := ProbeTurnState(t.Context(), &stateProbeExecutor{
		response: &ExecuteStreamResponse{Headers: http.Header{}},
	}, Credential{}, "https://upstream.example", StateProbeRequest{Model: "gpt-6-astra", Input: "ping"})
	if err != nil || value != "" {
		t.Fatalf("missing header = %q / %v", value, err)
	}

	_, err = ProbeTurnState(t.Context(), &stateProbeExecutor{
		err: &UpstreamHTTPError{Operation: "responses", StatusCode: http.StatusUnauthorized},
	}, Credential{}, "https://upstream.example", StateProbeRequest{Model: "gpt-6-astra", Input: "ping"})
	var upstream *UpstreamHTTPError
	if !errors.As(err, &upstream) || upstream.StatusCode != http.StatusUnauthorized {
		t.Fatalf("probe error = %#v / %v", upstream, err)
	}

	if _, err := ProbeTurnState(t.Context(), nil, Credential{}, "https://upstream.example", StateProbeRequest{}); err == nil {
		t.Fatal("ProbeTurnState() accepted a nil executor")
	}
}
