package target

import (
	"context"
	"testing"
	"time"

	"github.com/ysBayram/kervan/internal/coordination"
	"github.com/ysBayram/kervan/internal/health"
	"github.com/ysBayram/kervan/internal/session"
)

func TestRegistryAddGetTarget(t *testing.T) {
	r := NewRegistry(RegistryConfig{}, struct {
		Router   *Router
		Sessions *session.SessionManager
		Store    coordination.CoordinationStore
		Prober   health.Prober
		NodeID   string
	}{
		Router: NewRouter(),
		Store:  &coordination.NoopStore{},
		NodeID: "test",
	})

	tgt := &Target{ID: "test-1", Addr: "localhost:9000"}
	r.AddTarget(tgt)

	got, ok := r.GetTarget("test-1")
	if !ok {
		t.Fatal("target not found")
	}
	if got.Addr != "localhost:9000" {
		t.Errorf("Addr = %q, want %q", got.Addr, "localhost:9000")
	}
}

func TestRegistryRemoveTarget(t *testing.T) {
	r := NewRegistry(RegistryConfig{}, struct {
		Router   *Router
		Sessions *session.SessionManager
		Store    coordination.CoordinationStore
		Prober   health.Prober
		NodeID   string
	}{
		Router: NewRouter(),
		Store:  &coordination.NoopStore{},
		NodeID: "test",
	})

	r.AddTarget(&Target{ID: "test-1"})
	r.RemoveTarget("test-1")
	if _, ok := r.GetTarget("test-1"); ok {
		t.Error("target still exists after remove")
	}
}
