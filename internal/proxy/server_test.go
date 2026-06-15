package proxy

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ysBayram/kervan/internal/config"
)

func TestNewServer(t *testing.T) {
	cfg := &config.Config{
		NodeID: "test-node",
		Session: config.SessionConfig{
			ShardCount:     256,
			ReadBufferSize: 4096,
		},
		Listeners: []config.ListenerConfig{
			{Bind: ":0", Protocol: "tcp", MaxSessions: 100},
		},
		MetricsAddr: ":0",
	}
	srv, err := NewServer(cfg)
	if err != nil {
		t.Fatalf("NewServer failed: %v", err)
	}
	if srv == nil {
		t.Fatal("server is nil")
	}
	if srv.sessions == nil {
		t.Error("sessions manager is nil")
	}
	if srv.poller == nil {
		t.Error("poller is nil")
	}
}

func TestGenerateClientID(t *testing.T) {
	id := generateClientID(42, "test-node")
	expected := "42@test-node"
	if id != expected {
		t.Errorf("generateClientID = %q, want %q", id, expected)
	}
}
