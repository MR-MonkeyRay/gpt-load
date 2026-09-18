package execution

import (
	"encoding/base64"
	"encoding/binary"
	"strings"
	"testing"
	"time"
)

func TestTurnStateExpiryMSDecodesEmbeddedExpiry(t *testing.T) {
	t.Parallel()

	expiresAt := time.Date(2026, time.September, 18, 12, 0, 0, 0, time.UTC)
	raw := make([]byte, 9)
	raw[0] = 0x80
	binary.BigEndian.PutUint64(raw[1:9], uint64(expiresAt.Unix()))
	if got := TurnStateExpiryMS(base64.RawURLEncoding.EncodeToString(raw)); got != expiresAt.UnixMilli() {
		t.Fatalf("unpadded expiry = %d, want %d", got, expiresAt.UnixMilli())
	}
	if got := TurnStateExpiryMS(base64.URLEncoding.EncodeToString(raw)); got != expiresAt.UnixMilli() {
		t.Fatalf("padded expiry = %d, want %d", got, expiresAt.UnixMilli())
	}
	if got := TurnStateExpiryMS("  " + base64.RawURLEncoding.EncodeToString(raw) + "  "); got != expiresAt.UnixMilli() {
		t.Fatalf("padded whitespace expiry = %d, want %d", got, expiresAt.UnixMilli())
	}
}

func TestTurnStateExpiryMSRejectsUnreadableCaptures(t *testing.T) {
	t.Parallel()

	zeroed := make([]byte, 9)
	zeroed[0] = 0x80
	outOfRange := make([]byte, 9)
	outOfRange[0] = 0x80
	binary.BigEndian.PutUint64(outOfRange[1:9], maxTurnStateExpirySeconds+1)
	for _, test := range []struct {
		name  string
		value string
	}{
		{name: "empty", value: ""},
		{name: "not base64", value: "!!!!"},
		{name: "too short", value: base64.RawURLEncoding.EncodeToString([]byte{0x80, 0x01})},
		{name: "zero expiry", value: base64.RawURLEncoding.EncodeToString(zeroed)},
		{name: "implausible expiry", value: base64.RawURLEncoding.EncodeToString(outOfRange)},
		{name: "non token capture", value: strings.Repeat("s", CodexTurnStateLength)},
	} {
		t.Run(test.name, func(t *testing.T) {
			if got := TurnStateExpiryMS(test.value); got != 0 {
				t.Fatalf("TurnStateExpiryMS(%q) = %d, want 0", test.value, got)
			}
		})
	}
}
