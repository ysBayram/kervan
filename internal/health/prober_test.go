package health

import (
	"context"
	"net"
	"testing"
	"time"
)

func TestPassiveErrorRate(t *testing.T) {
	p := NewPassiveObserver(time.Minute)
	targetID := "test-target"

	// 5 successes, 5 failures
	for i := 0; i < 5; i++ {
		p.RecordWrite(targetID, nil)
	}
	for i := 0; i < 5; i++ {
		p.RecordWrite(targetID, net.ErrClosed)
	}

	rate := p.RecordWrite(targetID, net.ErrClosed)
	if rate < 0.5 || rate > 0.6 {
		t.Errorf("error rate = %f, want ~0.55", rate)
	}
}

func TestPassiveNoErrors(t *testing.T) {
	p := NewPassiveObserver(time.Minute)
	for i := 0; i < 10; i++ {
		p.RecordWrite("test", nil)
	}
	rate := p.RecordWrite("test", nil)
	if rate != 0 {
		t.Errorf("expected 0 error rate, got %f", rate)
	}
}
