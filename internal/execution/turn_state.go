package execution

import "time"

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
