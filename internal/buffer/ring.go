package buffer

import (
	"sync/atomic"
	"time"

	"github.com/ysBayram/kervan/pkg/buffer"
)

type BufferConfig struct {
	Capacity           uint32
	MaxBytes           uint64
	InlineThreshold    int
	MaxFrameSize       int
	BackpressurePolicy BackpressurePolicy
	BlockTimeout       time.Duration
}

type BufferMetrics struct {
	Enqueued atomic.Uint64
	Dropped  atomic.Uint64
	Rejected atomic.Uint64
}

type RingBuffer struct {
	capacity  uint32
	maxBytes  uint64
	head      atomic.Uint32
	tail      atomic.Uint32
	count     atomic.Uint32
	byteCount atomic.Uint64
	entries   []RingEntry
	pool      *buffer.Pool
	policy    BackpressurePolicy
	seq       atomic.Uint64
	metrics   *BufferMetrics
	blockCh   chan struct{}
}

func NewRingBuffer(cfg BufferConfig, pool *buffer.Pool) *RingBuffer {
	return &RingBuffer{
		capacity: cfg.Capacity,
		maxBytes: cfg.MaxBytes,
		entries:  make([]RingEntry, cfg.Capacity),
		pool:     pool,
		policy:   cfg.BackpressurePolicy,
		metrics:  &BufferMetrics{},
		blockCh:  make(chan struct{}, 1),
	}
}

func (rb *RingBuffer) Enqueue(payload []byte) error {
	if len(payload) > buffer.MaxFrameSize {
		rb.metrics.Rejected.Add(1)
		return ErrFrameTooLarge
	}
	if rb.IsFull() {
		switch rb.policy {
		case DropOldest:
			rb.evictHead()
		case Reject:
			rb.metrics.Rejected.Add(1)
			return ErrBufferFull
		case Block:
			return ErrWouldBlock
		}
	}
	if rb.byteCount.Load()+uint64(len(payload)) > rb.maxBytes {
		switch rb.policy {
		case DropOldest:
			rb.evictHead()
		case Reject:
			rb.metrics.Rejected.Add(1)
			return ErrBufferFull
		case Block:
			return ErrWouldBlock
		}
	}

	tail := rb.tail.Load()
	entry := &rb.entries[tail%rb.capacity]
	entry.seqNum = rb.seq.Add(1)
	entry.enqueued = time.Now().UnixNano()
	entry.length = uint32(len(payload))

	if len(payload) <= InlineThreshold {
		copy(entry.inline[:], payload)
	} else {
		entry.extBuf = rb.pool.Acquire(len(payload))
		copy(entry.extBuf, payload)
	}

	rb.tail.Store(tail + 1)
	rb.count.Add(1)
	rb.byteCount.Add(uint64(len(payload)))
	rb.metrics.Enqueued.Add(1)
	return nil
}

func (rb *RingBuffer) Dequeue() (*RingEntry, bool) {
	if rb.count.Load() == 0 {
		return nil, false
	}
	head := rb.head.Load()
	entry := &rb.entries[head%rb.capacity]
	return entry, true
}

func (rb *RingBuffer) Peek() (*RingEntry, bool) {
	return rb.Dequeue()
}

func (rb *RingBuffer) AdvanceHead() {
	head := rb.head.Load()
	entry := &rb.entries[head%rb.capacity]
	rb.byteCount.Add(-uint64(entry.length))
	entry.Reset(rb.pool)
	rb.head.Store(head + 1)
	if rb.count.Load() > 0 {
		rb.count.Add(^uint32(0))
	}
}

func (rb *RingBuffer) Len() uint32 {
	return rb.count.Load()
}

func (rb *RingBuffer) ByteLen() uint64 {
	return rb.byteCount.Load()
}

func (rb *RingBuffer) IsFull() bool {
	return rb.count.Load() >= rb.capacity
}

func (rb *RingBuffer) evictHead() {
	head := rb.head.Load()
	entry := &rb.entries[head%rb.capacity]
	rb.byteCount.Add(-uint64(entry.length))
	entry.Reset(rb.pool)
	rb.head.Store(head + 1)
	if rb.count.Load() > 0 {
		rb.count.Add(^uint32(0))
	}
	rb.metrics.Dropped.Add(1)
}
