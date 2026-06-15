package config

import (
	"os"
	"testing"
	"time"
)

func TestLoadConfig(t *testing.T) {
	data := `
node_id: "test-node"
listeners:
  - bind: ":8080"
    protocol: "websocket"
    max_sessions: 10000
upstream:
  addr: "localhost:9000"
session:
  shard_count: 256
  idle_timeout: 15m
  read_buffer_size: 4096
metrics_addr: ":9090"
`
	f, err := os.CreateTemp("", "kervan-config-*.yaml")
	if err != nil {
		t.Fatal(err)
	}
	defer os.Remove(f.Name())
	if _, err := f.Write([]byte(data)); err != nil {
		t.Fatal(err)
	}
	f.Close()

	cfg, err := Load(f.Name())
	if err != nil {
		t.Fatalf("Load failed: %v", err)
	}
	if cfg.NodeID != "test-node" {
		t.Errorf("NodeID = %q, want %q", cfg.NodeID, "test-node")
	}
	if len(cfg.Listeners) != 1 {
		t.Fatalf("len(listeners) = %d, want 1", len(cfg.Listeners))
	}
	if cfg.Listeners[0].Bind != ":8080" {
		t.Errorf("Bind = %q, want %q", cfg.Listeners[0].Bind, ":8080")
	}
	if cfg.Listeners[0].MaxSessions != 10000 {
		t.Errorf("MaxSessions = %d, want 10000", cfg.Listeners[0].MaxSessions)
	}
	if cfg.Session.ShardCount != 256 {
		t.Errorf("ShardCount = %d, want 256", cfg.Session.ShardCount)
	}
	if cfg.Session.IdleTimeout != 15*time.Minute {
		t.Errorf("IdleTimeout = %v, want 15m", cfg.Session.IdleTimeout)
	}
	if cfg.Upstream.Addr != "localhost:9000" {
		t.Errorf("Upstream.Addr = %q, want %q", cfg.Upstream.Addr, "localhost:9000")
	}
}

func TestLoadConfigMissingFile(t *testing.T) {
	if _, err := Load("/nonexistent/path.yaml"); err == nil {
		t.Error("expected error for missing file")
	}
}
