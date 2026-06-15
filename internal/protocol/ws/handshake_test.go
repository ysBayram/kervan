package ws

import (
	"testing"

	"github.com/gobwas/ws"
)

func TestAcceptKey(t *testing.T) {
	key := "dGhlIHNhbXBsZSBub25jZQ=="
	expected := "s3pPLMBiTxaQ9kYGzzhZRbK+xOo="
	got := ws.AcceptKey(key)
	if got != expected {
		t.Errorf("AcceptKey(%q) = %q, want %q", key, got, expected)
	}
}

func TestGenerateClientID(t *testing.T) {
	id1 := GenerateClientID()
	id2 := GenerateClientID()
	if id1 == id2 {
		t.Error("expected unique client IDs")
	}
	if len(id1) < 10 {
		t.Errorf("clientID too short: %q", id1)
	}
}
