package codex

import (
	"strings"

	"github.com/buger/jsonparser"

	"gpt-load/internal/execution"
)

// codexWebsocketMetadataKinds 是上游用事件投递响应级元数据的两种拼写：
// 线上抓包为 codex.response.metadata，OpenAI 客户端同时接受 response.metadata。
var codexWebsocketMetadataKinds = map[string]struct{}{
	"codex.response.metadata": {},
	"response.metadata":       {},
}

// WebsocketTurnState 读取 Codex WS 元数据事件 headers 里的 turn state。
// HTTP/SSE 由响应头投递 state，WS 上游改由元数据事件投递，握手响应头不保证携带。
// 事件本身不改动，返回值经 CompleteTurnState 判定长度后才可用。
func WebsocketTurnState(payload []byte) string {
	kind, err := jsonparser.GetString(payload, "type")
	if err != nil {
		return ""
	}
	if _, ok := codexWebsocketMetadataKinds[kind]; !ok {
		return ""
	}
	state := ""
	// 帧内 Header 名按线上拼写为小写，这里不区分大小写以免漏采。
	_ = jsonparser.ObjectEach(payload, func(key, value []byte, dataType jsonparser.ValueType, _ int) error {
		if dataType != jsonparser.String || !strings.EqualFold(string(key), execution.CodexTurnStateHeader) {
			return nil
		}
		decoded, err := jsonparser.ParseString(value)
		if err != nil {
			return nil
		}
		state = decoded
		return nil
	}, "headers")
	return state
}
