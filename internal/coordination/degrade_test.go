package coordination

import (
	"context"
	"testing"
	"time"
)

func TestDegradeStateTransition(t *testing.T) {
	dc := NewDegradeController(&NoopStore{})
	if dc.IsDegraded() {
		t.Error("should not be degraded initially")
	}
	dc.EnterDegraded()
	if !dc.IsDegraded() {
		t.Error("should be degraded after EnterDegraded")
	}
	dc.ExitDegraded()
	if dc.IsDegraded() {
		t.Error("should not be degraded after ExitDegraded")
	}
}

func TestDegradeAutoTransition(t *testing.T) {
	dc := NewDegradeController(&NoopStore{})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go dc.Run(ctx)
	time.Sleep(50 * time.Millisecond)
	cancel()
}
