package node

import (
	"os"
	"testing"
)

func TestLoadIdentityFromNodeID(t *testing.T) {
	id := LoadIdentity("test-node")
	if id.NodeID != "test-node" {
		t.Errorf("NodeID = %q, want %q", id.NodeID, "test-node")
	}
	if id.StartTime == 0 {
		t.Error("StartTime should not be zero")
	}
}

func TestLoadIdentityFromEnv(t *testing.T) {
	os.Setenv("POD_NAME", "kervan-pod-0")
	defer os.Unsetenv("POD_NAME")

	id := LoadIdentity("")
	if id.NodeID != "kervan-pod-0" {
		t.Errorf("NodeID = %q, want %q", id.NodeID, "kervan-pod-0")
	}
}

func TestLoadIdentityFallback(t *testing.T) {
	os.Unsetenv("POD_NAME")
	id := LoadIdentity("")
	if id.NodeID == "" {
		t.Error("NodeID should fallback to hostname")
	}
}
