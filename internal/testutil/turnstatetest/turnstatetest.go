// Package turnstatetest builds captured codex turn states for tests. The value
// mirrors the upstream capture: a token of exactly the accepted length whose
// embedded timestamp is the token's own issue time, not its expiry. Validity is
// never carried by the value, so tests decide it through the record time they
// publish with; the seed only makes two captures distinguishable.
package turnstatetest

import (
	"encoding/base64"
	"encoding/binary"
	"time"

	"gpt-load/internal/execution"
)

// issuedAt 是测试令牌内嵌的签发时刻：固定在很久以前，任何从值里推断有效期的实现
// 都会立刻判定它过期。
var issuedAt = time.Date(2020, time.January, 1, 0, 0, 0, 0, time.UTC)

// Value returns a complete turn state of exactly the accepted length. Different
// seeds produce different values.
func Value(seed byte) string {
	raw := make([]byte, execution.CodexTurnStateLength/4*3)
	raw[0] = 0x80
	binary.BigEndian.PutUint64(raw[1:9], uint64(issuedAt.Unix()))
	raw[9] = seed
	for index := 10; index < len(raw); index++ {
		raw[index] = byte(index)
	}
	value := base64.RawURLEncoding.EncodeToString(raw)
	if len(value) != execution.CodexTurnStateLength {
		panic("turnstatetest.Value produced an unexpected length")
	}
	return value
}
