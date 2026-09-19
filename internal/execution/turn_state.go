package execution

import (
	"encoding/base64"
	"strings"
	"time"
)

// Codex turn state is a request-side header captured per upstream model: the
// value is captured by a manual state refresh and replayed for that model only.
const (
	CodexTurnStateHeader = "X-Codex-Turn-State"
	// CodexTurnStateModel is the model a state refresh selects by default; any
	// model the group serves can be refreshed.
	CodexTurnStateModel = "gpt-6-astra"
	// CodexTurnStateLength is the length the manual refresh waits for: the probe
	// keeps issuing requests until the upstream returns exactly this length.
	// Natural capture does not use it; see UsableTurnState.
	CodexTurnStateLength = 292
	// CodexTurnStateTTL is how long a captured turn state stays replayable after
	// it was recorded. The captured value carries no readable expiry of its own,
	// so the record time is the only authority on validity.
	CodexTurnStateTTL = time.Hour
)

// CompleteTurnState normalizes one observed turn state and reports whether it is
// complete. It is the manual refresh contract: a refresh keeps probing until the
// upstream returns a header of exactly this length.
func CompleteTurnState(value string) (string, bool) {
	trimmed := strings.TrimSpace(value)
	if len(trimmed) != CodexTurnStateLength {
		return "", false
	}
	return trimmed, true
}

// turn state 是上游自签发的 Fernet 令牌：编码后可解码、且长度远大于任何
// 截断或占位值。自然采集按这两个事实判定，而不是按某一个具体长度，否则
// 上游调整令牌长度后每一个真实值都会被判为不完整而丢弃。
const (
	// minUsableTurnStateLength 排除被截断的令牌和错误内容。
	minUsableTurnStateLength = 64
	// maxUsableTurnStateLength 排除把响应正文等非令牌内容当作 state。
	maxUsableTurnStateLength = 4096
	// minUsableTurnStateBytes 是解码后的最少字节数。
	minUsableTurnStateBytes = 32
)

// UsableTurnState normalizes one turn state an upstream returned on its own and
// reports whether it is a usable token. Natural capture uses this instead of the
// manual refresh's fixed length so a real capture survives an upstream token
// format change, while truncated or non-token values are still dropped.
func UsableTurnState(value string) (string, bool) {
	trimmed := strings.TrimSpace(value)
	if len(trimmed) < minUsableTurnStateLength || len(trimmed) > maxUsableTurnStateLength {
		return "", false
	}
	// 上游同时使用带填充与不带填充的两种 base64url 编码。
	decoded, err := base64.RawURLEncoding.DecodeString(strings.TrimRight(trimmed, "="))
	if err != nil || len(decoded) < minUsableTurnStateBytes {
		return "", false
	}
	return trimmed, true
}

// TurnStateModel resolves the model a turn state is scoped to. States are never
// shared between models, so the upstream model wins and the client model is the
// fallback.
func TurnStateModel(upstreamModel, clientModel string) string {
	if upstreamModel != "" {
		return upstreamModel
	}
	return clientModel
}

// TurnStateObservation is one complete turn state an upstream returned on its
// own for a real data-plane attempt, reported so the control plane can retain it
// and replay it for the same credential and model.
type TurnStateObservation struct {
	CredentialID uint
	// IdentityGeneration binds the capture to the credential identity that
	// produced it, so a re-authenticated credential never inherits it.
	IdentityGeneration uint64
	// Model is the resolved upstream model the state belongs to.
	Model     string
	TurnState string
	// ProxyURL and BaseURL describe the attempt that produced the state; they
	// are recorded for diagnosis and are never used to replay it.
	ProxyURL     string
	BaseURL      string
	ObservedAtMS int64
}

// TurnStateObserver receives complete turn states observed on real responses.
// Implementations must return immediately and never block the data plane.
type TurnStateObserver interface {
	ObserveTurnState(observation TurnStateObservation)
}
