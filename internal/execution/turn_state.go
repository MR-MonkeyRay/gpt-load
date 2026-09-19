package execution

import (
	"encoding/base64"
	"encoding/binary"
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
)

// maxTurnStateExpirySeconds bounds a decoded expiry to a plausible range so a
// malformed capture can never turn into a nonsense deadline. 4_102_444_800 is
// 2100-01-01T00:00:00Z.
const maxTurnStateExpirySeconds = 4_102_444_800

// TurnStateExpiryMS decodes the expiry embedded in one captured turn state and
// returns it as epoch milliseconds. The value is a Fernet-style token: a version
// byte followed by a big-endian uint64 seconds timestamp. It returns 0 when the
// value carries no readable expiry, which makes the value unusable for replay
// because its validity cannot be proven.
func TurnStateExpiryMS(value string) int64 {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return 0
	}
	raw, err := decodeTurnStateToken(trimmed)
	if err != nil || len(raw) < 9 {
		return 0
	}
	seconds := binary.BigEndian.Uint64(raw[1:9])
	if seconds == 0 || seconds > maxTurnStateExpirySeconds {
		return 0
	}
	return int64(seconds) * int64(time.Second/time.Millisecond)
}

// decodeTurnStateToken accepts the urlsafe base64 spelling of the upstream
// capture, padded or unpadded.
func decodeTurnStateToken(value string) ([]byte, error) {
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err == nil {
		return raw, nil
	}
	return base64.URLEncoding.DecodeString(value)
}
