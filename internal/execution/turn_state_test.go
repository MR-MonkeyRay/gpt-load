package execution

import (
	"encoding/base64"
	"strings"
	"testing"
)

// encodedTurnState 生成一个指定长度的 base64url 令牌，形状与上游签发的 Fernet
// 令牌一致。
func encodedTurnState(length int) string {
	raw := make([]byte, length/4*3)
	raw[0] = 0x80
	for index := range raw {
		raw[index] = byte(index)
	}
	return base64.RawURLEncoding.EncodeToString(raw)
}

// Only an exactly complete state is usable: surrounding whitespace is trimmed
// and everything else is rejected.
func TestCompleteTurnStateAcceptsOnlyExactLength(t *testing.T) {
	complete := strings.Repeat("s", CodexTurnStateLength)
	for _, test := range []struct {
		name  string
		value string
		want  string
		ok    bool
	}{
		{name: "complete", value: complete, want: complete, ok: true},
		{name: "padded", value: "  " + complete + "\n", want: complete, ok: true},
		{name: "short", value: complete[:CodexTurnStateLength-1]},
		{name: "long", value: complete + "s"},
		// 上游线上签发的令牌比约定长度长（实测 312）：不是本产品要的值。
		{name: "upstream rotated length", value: encodedTurnState(312)},
		{name: "empty", value: "   "},
	} {
		t.Run(test.name, func(t *testing.T) {
			value, ok := CompleteTurnState(test.value)
			if ok != test.ok || value != test.want {
				t.Fatalf("CompleteTurnState(%q) = %q, %t", test.value, value, ok)
			}
		})
	}
}

// A turn state is scoped to one model: the upstream model wins, and the client
// model is only the fallback.
func TestTurnStateModelPrefersUpstreamModel(t *testing.T) {
	if got := TurnStateModel("gpt-5-codex", "gpt-6-astra"); got != "gpt-5-codex" {
		t.Fatalf("TurnStateModel() = %q", got)
	}
	if got := TurnStateModel("", "gpt-6-astra"); got != "gpt-6-astra" {
		t.Fatalf("TurnStateModel() = %q", got)
	}
}
