// Package turnstatetest builds captured codex turn states for tests. The value
// mirrors the upstream capture: a Fernet-style token of exactly the accepted
// length whose embedded expiry is the requested instant.
package turnstatetest

import (
	"encoding/base64"
	"encoding/binary"
	"time"

	"gpt-load/internal/execution"
)

// Token returns a complete turn state of exactly the accepted length whose
// embedded expiry is expiresAt.
func Token(expiresAt time.Time) string {
	raw := make([]byte, execution.CodexTurnStateLength/4*3)
	raw[0] = 0x80
	binary.BigEndian.PutUint64(raw[1:9], uint64(expiresAt.Unix()))
	for index := 9; index < len(raw); index++ {
		raw[index] = byte(index)
	}
	value := base64.RawURLEncoding.EncodeToString(raw)
	if len(value) != execution.CodexTurnStateLength {
		panic("turnstatetest.Token produced an unexpected length")
	}
	return value
}
