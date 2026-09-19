package state

import (
	"strings"
	"time"

	"gpt-load/internal/execution"
)

// TurnState is one captured codex turn state together with its replay expiry.
// The value itself carries no readable expiry, so validity is always the record
// time plus the fixed turn state TTL.
type TurnState struct {
	Value string
	// ExpiresAtMS is the record time plus the turn state TTL; 0 means the
	// capture has no record time, which makes it unusable for replay because its
	// validity cannot be proven.
	ExpiresAtMS int64
}

// NewTurnState builds one capture from its persisted value and record time.
func NewTurnState(value string, refreshedAtMS int64) TurnState {
	return TurnState{Value: value, ExpiresAtMS: turnStateExpiryMS(refreshedAtMS)}
}

// turnStateExpiryMS derives the replay expiry of a capture recorded at
// refreshedAtMS.
func turnStateExpiryMS(refreshedAtMS int64) int64 {
	if refreshedAtMS <= 0 {
		return 0
	}
	return refreshedAtMS + execution.CodexTurnStateTTL.Milliseconds()
}

// TurnStates maps an upstream model to the turn state captured for it. A set is
// replaced wholesale, never mutated in place, so readers may share one map
// without locking.
type TurnStates map[string]TurnState

// NewTurnStates builds the capture set of one credential from persisted
// captures. Captures without a model or a value are dropped; a capture without a
// record time is kept so the operator can still see it, but it is never
// replayed.
func NewTurnStates(captures map[string]TurnState) TurnStates {
	if len(captures) == 0 {
		return nil
	}
	states := make(TurnStates, len(captures))
	for model, capture := range captures {
		model = strings.TrimSpace(model)
		capture.Value = strings.TrimSpace(capture.Value)
		if model == "" || capture.Value == "" {
			continue
		}
		states[model] = capture
	}
	if len(states) == 0 {
		return nil
	}
	return states
}

// WithCapture returns a copy of the capture set with one model's capture
// replaced by a value recorded at refreshedAtMS.
func (states TurnStates) WithCapture(model, value string, refreshedAtMS int64) TurnStates {
	updated := make(TurnStates, len(states)+1)
	for existing, capture := range states {
		updated[existing] = capture
	}
	updated[model] = NewTurnState(value, refreshedAtMS)
	return updated
}

// Valid returns the turn state captured for one upstream model while its replay
// expiry is still in the future. A capture is replayed only for the model it was
// captured for.
func (states TurnStates) Valid(model string, now time.Time) string {
	capture, ok := states[model]
	if !ok || capture.Value == "" || capture.ExpiresAtMS <= now.UnixMilli() {
		return ""
	}
	return capture.Value
}
