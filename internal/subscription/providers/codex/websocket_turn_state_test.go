package codex

import (
	"strings"
	"testing"
)

// WS 上游用元数据事件投递 turn state，握手响应头不保证携带；事件拼写、Header
// 大小写和值类型都由上游决定，任何一处不识别都会让 WS 永远采不到 state。
func TestWebsocketTurnStateReadsMetadataFrameHeaders(t *testing.T) {
	state := strings.Repeat("S", 292)
	for _, test := range []struct {
		name    string
		payload string
		want    string
	}{
		{
			name:    "live capture kind",
			payload: `{"type":"codex.response.metadata","headers":{"x-codex-turn-state":"` + state + `","x-models-etag":"W/\"e2e\""}}`,
			want:    state,
		},
		{
			name:    "client accepted alias",
			payload: `{"type":"response.metadata","headers":{"x-codex-turn-state":"` + state + `"}}`,
			want:    state,
		},
		{
			name:    "canonical header case",
			payload: `{"type":"response.metadata","headers":{"X-Codex-Turn-State":"` + state + `"}}`,
			want:    state,
		},
		{
			name:    "unrelated header",
			payload: `{"type":"codex.response.metadata","headers":{"x-models-etag":"W/\"e2e\""}}`,
			want:    "",
		},
		{
			name:    "unrelated kind",
			payload: `{"type":"codex.rate_limits","headers":{"x-codex-turn-state":"` + state + `"}}`,
			want:    "",
		},
		{
			name:    "non string value",
			payload: `{"type":"codex.response.metadata","headers":{"x-codex-turn-state":292}}`,
			want:    "",
		},
		{
			name:    "headers not an object",
			payload: `{"type":"codex.response.metadata","metadata":{"conversation_id":"conv_1"}}`,
			want:    "",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := WebsocketTurnState([]byte(test.payload)); got != test.want {
				t.Fatalf("WebsocketTurnState = %q, want %q", got, test.want)
			}
		})
	}
}
