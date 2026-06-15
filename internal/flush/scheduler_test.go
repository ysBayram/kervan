package flush

import (
	"testing"

	"github.com/ysBayram/kervan/internal/buffer"
	"github.com/ysBayram/kervan/internal/session"
	"github.com/ysBayram/kervan/pkg/buffer"
)

type mockWriter struct {
	writeFn func(sess *session.ClientSession, payload []byte) (int, error)
}

func (m *mockWriter) WriteFrame(sess *session.ClientSession, payload []byte) (int, error) {
	return m.writeFn(sess, payload)
}

func TestDrainEmpty(t *testing.T) {
	sess := &session.ClientSession{
		RingBuffer: buffer.NewRingBuffer(buffer.BufferConfig{Capacity: 10, MaxBytes: 1 << 20}, buffer.NewPool()),
	}
	s := NewScheduler(&mockWriter{writeFn: func(_ *session.ClientSession, p []byte) (int, error) { return len(p), nil }})
	if err := s.Drain(sess, buffer.NewPool()); err != nil {
		t.Errorf("expected nil, got %v", err)
	}
}

func TestDrainFull(t *testing.T) {
	pool := buffer.NewPool()
	rb := buffer.NewRingBuffer(buffer.BufferConfig{Capacity: 10, MaxBytes: 1 << 20}, pool)
	payload := []byte("hello")
	for i := 0; i < 5; i++ {
		if err := rb.Enqueue(payload); err != nil {
			t.Fatal(err)
		}
	}
	sess := &session.ClientSession{RingBuffer: rb}

	var written int
	s := NewScheduler(&mockWriter{writeFn: func(_ *session.ClientSession, p []byte) (int, error) {
		written++
		return len(p), nil
	}})
	if err := s.Drain(sess, pool); err != nil {
		t.Errorf("Drain: %v", err)
	}
	if written != 5 {
		t.Errorf("written = %d, want 5", written)
	}
	if rb.Len() != 0 {
		t.Errorf("buffer should be empty, Len = %d", rb.Len())
	}
}
