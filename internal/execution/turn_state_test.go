package execution

import (
	"encoding/base64"
	"strings"
	"testing"
)

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

// 自然采集按令牌本身判定完整性：上游把令牌从 292 加长到 312 后真实值仍必须可用，
// 而空值、非 base64 内容、占位级短值和异常长内容必须被拒绝。缩短但仍落在可用
// 区间内的令牌无法与真实令牌区分，按设计接受。
func TestUsableTurnStateAcceptsUpstreamLengthChanges(t *testing.T) {
	canonical := encodedTurnState(CodexTurnStateLength)
	rotated := encodedTurnState(312)
	for _, test := range []struct {
		name  string
		value string
		want  string
		ok    bool
	}{
		{name: "canonical length", value: canonical, want: canonical, ok: true},
		{name: "upstream rotated length", value: rotated, want: rotated, ok: true},
		{name: "padded", value: " " + rotated + "\n", want: rotated, ok: true},
		{name: "below floor", value: encodedTurnState(60)},
		{name: "above ceiling", value: encodedTurnState(maxUsableTurnStateLength + 4)},
		{name: "not base64url", value: strings.Repeat("!", 292)},
		{name: "too few decoded bytes", value: encodedTurnState(40)},
		{name: "empty", value: "   "},
	} {
		t.Run(test.name, func(t *testing.T) {
			value, ok := UsableTurnState(test.value)
			if ok != test.ok || value != test.want {
				t.Fatalf("UsableTurnState(len=%d) = %t, want %t", len(test.value), ok, test.ok)
			}
		})
	}
}
