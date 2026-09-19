package state

import (
	"strings"
	"time"

	"gpt-load/internal/execution"
)

// TurnState is one captured codex turn state together with the expiry embedded
// in its value. A capture is bound to a credential and an upstream model.
type TurnState struct {
	Value string
	// ExpiresAtMS is derived from the captured value itself and never persisted
	// separately; 0 means the capture carries no readable expiry.
	ExpiresAtMS int64
}

// TurnStates maps an upstream model to the turn state captured for it. A set is
// replaced wholesale, never mutated in place, so readers may share one map
// without locking.
type TurnStates map[string]TurnState

// NewTurnStates builds the capture set of one credential from persisted values.
// Captures without a model or a value are dropped; a capture whose expiry cannot
// be read is kept so the operator can still see it, but it is never replayed.
func NewTurnStates(captures map[string]string) TurnStates {
	if len(captures) == 0 {
		return nil
	}
	states := make(TurnStates, len(captures))
	for model, value := range captures {
		model = strings.TrimSpace(model)
		value = strings.TrimSpace(value)
		if model == "" || value == "" {
			continue
		}
		states[model] = TurnState{Value: value, ExpiresAtMS: execution.TurnStateExpiryMS(value)}
	}
	if len(states) == 0 {
		return nil
	}
	return states
}

// WithCapture returns a copy of the capture set with one model's value replaced.
func (states TurnStates) WithCapture(model, value string, expiresAtMS int64) TurnStates {
	updated := make(TurnStates, len(states)+1)
	for existing, capture := range states {
		updated[existing] = capture
	}
	updated[model] = TurnState{Value: value, ExpiresAtMS: expiresAtMS}
	return updated
}

// Valid returns the turn state captured for one upstream model while its
// embedded expiry is still in the future. A capture is replayed only for the
// model it was captured for.
func (states TurnStates) Valid(model string, now time.Time) string {
	capture, ok := states[model]
	if !ok || capture.Value == "" || capture.ExpiresAtMS <= now.UnixMilli() {
		return ""
	}
	return capture.Value
}
