package buffer

import (
	"testing"
	"time"

	"github.com/ysBayram/kervan/pkg/buffer"
)

func newTestRing(capacity uint32, policy BackpressurePolicy) *RingBuffer {
	return NewRingBuffer(BufferConfig{
		Capacity:           capacity,
		MaxBytes:           1 << 30,
		BackpressurePolicy: policy,
		BlockTimeout:       time.Second,
	}, buffer.NewPool())
}

func TestRingFIFOOrder(t *testing.T) {
	rb := newTestRing(1024, DropOldest)
	n := 1000
	for i := 0; i < n; i++ {
		payload := []byte{byte(i)}
		if err := rb.Enqueue(payload); err != nil {
			t.Fatalf("Enqueue %d: %v", i, err)
		}
	}
	for i := 0; i < n; i++ {
		entry, ok := rb.Dequeue()
		if !ok {
			t.Fatalf("Dequeue %d: not ok", i)
		}
		if len(entry.Payload()) != 1 || entry.Payload()[0] != byte(i) {
			t.Errorf("Dequeue %d: got %v, want %d", i, entry.Payload(), byte(i))
		}
		rb.AdvanceHead()
	}
}

func TestRingDropOldest(t *testing.T) {
	rb := newTestRing(3, DropOldest)
	rb.Enqueue([]byte("a"))
	rb.Enqueue([]byte("b"))
	rb.Enqueue([]byte("c"))
	rb.Enqueue([]byte("d"))
	if rb.Len() != 3 {
		t.Errorf("Len = %d, want 3", rb.Len())
	}
	entry, ok := rb.Dequeue()
	if !ok {
		t.Fatal("Dequeue not ok")
	}
	if string(entry.Payload()) != "b" {
		t.Errorf("head = %q, want %q", string(entry.Payload()), "b")
	}
}

func TestRingReject(t *testing.T) {
	rb := newTestRing(2, Reject)
	rb.Enqueue([]byte("a"))
	rb.Enqueue([]byte("b"))
	err := rb.Enqueue([]byte("c"))
	if err != ErrBufferFull {
		t.Errorf("expected ErrBufferFull, got %v", err)
	}
	if rb.Len() != 2 {
		t.Errorf("Len = %d, want 2", rb.Len())
	}
}

func TestRingBlock(t *testing.T) {
	rb := newTestRing(1, Block)
	rb.Enqueue([]byte("a"))
	err := rb.Enqueue([]byte("b"))
	if err != ErrWouldBlock {
		t.Errorf("expected ErrWouldBlock, got %v", err)
	}
	entry, ok := rb.Dequeue()
	if !ok {
		t.Fatal("Dequeue not ok")
	}
	if string(entry.Payload()) != "a" {
		t.Errorf("head = %q, want %q", string(entry.Payload()), "a")
	}
	rb.AdvanceHead()
}

func TestRingMaxBytes(t *testing.T) {
	rb := NewRingBuffer(BufferConfig{
		Capacity:           100,
		MaxBytes:           10,
		BackpressurePolicy: Reject,
	}, buffer.NewPool())
	rb.Enqueue([]byte("1234567890"))
	err := rb.Enqueue([]byte("x"))
	if err != ErrBufferFull {
		t.Errorf("expected ErrBufferFull, got %v", err)
	}
}

func TestInlineVsPooled(t *testing.T) {
	rb := newTestRing(10, DropOldest)
	small := make([]byte, 255)
	if err := rb.Enqueue(small); err != nil {
		t.Fatal(err)
	}
	e1, _ := rb.Dequeue()
	if e1.extBuf != nil {
		t.Error("expected inline for 255-byte payload")
	}
	rb.AdvanceHead()

	large := make([]byte, 257)
	if err := rb.Enqueue(large); err != nil {
		t.Fatal(err)
	}
	e2, _ := rb.Dequeue()
	if e2.extBuf == nil {
		t.Error("expected pooled for 257-byte payload")
	}
	rb.AdvanceHead()
}

func TestSeqNumMonotonic(t *testing.T) {
	rb := newTestRing(10, DropOldest)
	var prev uint64
	for i := 0; i < 10; i++ {
		rb.Enqueue([]byte{byte(i)})
		entry, _ := rb.Dequeue()
		if entry.seqNum <= prev {
			t.Errorf("seqNum not monotonic: %d <= %d", entry.seqNum, prev)
		}
		prev = entry.seqNum
		rb.AdvanceHead()
	}
}

func TestAdvanceHeadReleasesPool(t *testing.T) {
	pool := buffer.NewPool()
	rb := NewRingBuffer(BufferConfig{Capacity: 3, MaxBytes: 1 << 20, BackpressurePolicy: DropOldest}, pool)
	payload := make([]byte, 512)
	rb.Enqueue(payload)
	entry, _ := rb.Dequeue()
	if entry.extBuf == nil {
		t.Fatal("expected pooled entry")
	}
	rb.AdvanceHead()
	buf := pool.Acquire(512)
	if len(buf) != 512 {
		t.Errorf("pool hit: len = %d", len(buf))
	}
}
