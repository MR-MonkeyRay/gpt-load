package execution

import (
	"strings"
	"time"
)

// Codex turn state is a request-side header captured per upstream model: the
// value is captured by a manual state refresh and replayed for that model only.
const (
	CodexTurnStateHeader = "X-Codex-Turn-State"
	// CodexTurnStateModel is the model a state refresh selects by default; any
	// model the group serves can be refreshed.
	CodexTurnStateModel  = "gpt-6-astra"
	CodexTurnStateLength = 292
	// CodexTurnStateTTL is how long a captured turn state stays replayable after
	// it was recorded. The captured value carries no readable expiry of its own,
	// so the record time is the only authority on validity.
	CodexTurnStateTTL = time.Hour
)

// CompleteTurnState normalizes one observed turn state and reports whether it is
// complete. An upstream may return a partial or empty header; only the exact
// capture length is a usable turn state, and anything else yields no value.
func CompleteTurnState(value string) (string, bool) {
	trimmed := strings.TrimSpace(value)
	if len(trimmed) != CodexTurnStateLength {
		return "", false
	}
	return trimmed, true
}

// TurnStateModel resolves the model a turn state is scoped to. States are never
// shared between models, so the upstream model wins and the client model is the
// fallback.
func TurnStateModel(upstreamModel, clientModel string) string {
	if upstreamModel != "" {
		return upstreamModel
	}
	return clientModel
}

// TurnStateObservation is one complete turn state an upstream returned on its
// own for a real data-plane attempt, reported so the control plane can retain it
// and replay it for the same credential and model.
type TurnStateObservation struct {
	CredentialID uint
	// IdentityGeneration binds the capture to the credential identity that
	// produced it, so a re-authenticated credential never inherits it.
	IdentityGeneration uint64
	// Model is the resolved upstream model the state belongs to.
	Model     string
	TurnState string
	// ProxyURL and BaseURL describe the attempt that produced the state; they
	// are recorded for diagnosis and are never used to replay it.
	ProxyURL     string
	BaseURL      string
	ObservedAtMS int64
}

// TurnStateObserver receives complete turn states observed on real responses.
// Implementations must return immediately and never block the data plane.
type TurnStateObserver interface {
	ObserveTurnState(observation TurnStateObservation)
}
