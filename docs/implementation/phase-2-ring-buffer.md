# Phase 2: In-Memory Ring Buffering & Backpressure Management

**Phase ID:** KERVAN-IMPL-002  
**Status:** Planned  
**Depends On:** Phase 1 (NetPoller, SessionManager, relay seam)  
**Blocks:** Phase 3 (FlushScheduler, at-least-once drain)  
**TDD Reference:** §4.2, §5.1, §7.1, §7.4–§7.5  
**ADR Reference:** ADR-003 (sync.Pool, inline threshold, zero-allocation hot path)

---

## 1. Objective & Scope

**Objective:** Implement the per-session bounded FIFO ring buffer—the caravanserai—with power-of-two `sync.Pool` byte reuse, inline small-frame storage, configurable backpressure policies, and a FlushScheduler that delivers at-least-once semantics to the upstream write path.

### Included

- `RingBuffer` slab-backed bounded queue with atomic head/tail/count
- `RingEntry` with inline `[256]byte` storage and pooled overflow buffers
- Size-class `sync.Pool` allocator (`pkg/buffer/pool.go`)
- Backpressure policies: `DropOldest`, `Reject`, `Block`
- `FlushScheduler` consumer loop integrated with NetPoller write readiness
- Monotonic `seqNum` per entry for at-least-once dedup hints
- Relay path refactor: `Active` → direct write; non-Active → enqueue
- Prometheus metrics: `kervan_buffer_depth`, `kervan_buffer_dropped_total`, `kervan_flush_total`, `kervan_pool_hit_ratio`
- Static analysis lint rule for `make([]byte` in hot packages

### Excluded

- TargetRegistry-driven Freeze/Thaw transitions (Phase 3 injects state changes)
- Automatic failover on upstream death (Phase 3; Phase 2 provides manual `Freeze()` test hook)
- Coordination store persistence (Phase 4)
- Durable WAL / mmap spill (TDD §12 future work)
- Backend→client direction buffering

---

## 2. Target Directory & File Structure

```
kervan/
├── internal/
│   ├── buffer/
│   │   ├── ring.go                  # RingBuffer, Enqueue, Dequeue, Peek
│   │   ├── entry.go                 # RingEntry struct, inline payload
│   │   ├── backpressure.go          # Policy enum + enforcement
│   │   ├── ring_test.go
│   │   └── backpressure_test.go
│   ├── flush/
│   │   ├── scheduler.go             # FlushScheduler: drain head→tail
│   │   ├── ack.go                   # Write-completion / ACK tracking
│   │   └── scheduler_test.go
│   ├── session/
│   │   └── session.go               # ADD: RingBuffer *, seqNum counter
│   └── proxy/
│       └── relay.go                 # MODIFY: forwardClientPayload dispatch
├── pkg/
│   └── buffer/
│       ├── pool.go                  # Size-class sync.Pool, acquire/release
│       ├── pool_test.go
│       └── sizes.go                 # InlineThreshold, MaxFrameSize, sizeClass()
└── configs/
    └── kervan.example.yaml          # ADD: buffer.* section
```

### Configuration Additions

```yaml
buffer:
  max_entries: 1024              # ring capacity (entries)
  max_bytes: 2097152             # 2 MB total queued bytes per session
  inline_threshold: 256          # bytes; store in RingEntry inline array
  max_frame_size: 65536          # 64 KB; reject frames exceeding this
  backpressure_policy: drop_oldest  # drop_oldest | reject | block
  block_timeout: 5s              # Block policy escalation to Reject
```

---

## 3. Step-by-Step Coding Roadmap

### Step 2.1 — Size-Class Buffer Pool

**`pkg/buffer/sizes.go`:**

```go
const (
    InlineThreshold = 256
    MaxFrameSize    = 65536
    NumSizeClasses  = 9 // 256, 512, 1K, 2K, 4K, 8K, 16K, 32K, 64K
)

func SizeClass(n int) int {
    switch {
    case n <= 256:  return 0
    case n <= 512:  return 1
    case n <= 1024: return 2
    case n <= 2048: return 3
    case n <= 4096: return 4
    case n <= 8192: return 5
    case n <= 16384: return 6
    case n <= 32768: return 7
    default:        return 8 // 65536
    }
}

func ClassCapacity(class int) int {
    return 256 << class
}
```

**`pkg/buffer/pool.go`:**

```go
type Pool struct {
    pools     [NumSizeClasses]sync.Pool
    hits      atomic.Uint64
    misses    atomic.Uint64
}

func NewPool() *Pool

func (p *Pool) Acquire(size int) []byte {
    class := SizeClass(size)
    buf := p.pools[class].Get()
    if buf == nil {
        p.misses.Add(1)
        return make([]byte, ClassCapacity(class))[:size]
    }
    p.hits.Add(1)
    return buf.([]byte)[:size]
}

func (p *Pool) Release(buf []byte) {
    class := SizeClass(cap(buf))
    p.pools[class].Put(buf[:cap(buf)])
}

func (p *Pool) HitRatio() float64 {
    h, m := p.hits.Load(), p.misses.Load()
    if h+m == 0 { return 1.0 }
    return float64(h) / float64(h+m)
}
```

**Invariant (document in code):**

```
acquire() → copy payload → enqueue → dequeue → flush write → ACK → release()
```

---

### Step 2.2 — RingEntry & Inline Storage

**`internal/buffer/entry.go`:**

```go
type RingEntry struct {
    inline   [InlineThreshold]byte
    extBuf   []byte    // nil if inline; non-nil if pooled
    length   uint32
    seqNum   uint64
    enqueued int64     // unix nano
}

func (e *RingEntry) Payload() []byte {
    if e.extBuf != nil {
        return e.extBuf[:e.length]
    }
    return e.inline[:e.length]
}

func (e *RingEntry) Reset(pool *buffer.Pool) {
    if e.extBuf != nil {
        pool.Release(e.extBuf)
        e.extBuf = nil
    }
    e.length = 0
}
```

---

### Step 2.3 — RingBuffer Core

**`internal/buffer/ring.go`:**

```go
type RingBuffer struct {
    capacity  uint32
    maxBytes  uint64
    head      atomic.Uint32
    tail      atomic.Uint32
    count     atomic.Uint32
    byteCount atomic.Uint64
    entries   []RingEntry          // pre-allocated slab, len = capacity
    pool      *buffer.Pool
    policy    BackpressurePolicy
    seq       atomic.Uint64        // monotonic sequence generator
    metrics   *BufferMetrics
    blockCh   chan struct{}        // Block policy: poller parks read
}

type BufferMetrics struct {
    Enqueued  atomic.Uint64
    Dropped   atomic.Uint64
    Rejected  atomic.Uint64
}

func NewRingBuffer(cfg BufferConfig, pool *buffer.Pool) *RingBuffer

func (rb *RingBuffer) Enqueue(payload []byte) (err error)
func (rb *RingBuffer) Dequeue() (entry *RingEntry, ok bool)
func (rb *RingBuffer) Peek() (*RingEntry, bool)
func (rb *RingBuffer) Len() uint32
func (rb *RingBuffer) ByteLen() uint64
func (rb *RingBuffer) IsFull() bool

// Consumer advances head after successful ACK
func (rb *RingBuffer) AdvanceHead(pool *buffer.Pool) 
```

**Enqueue algorithm (single-producer — NetPoller worker):**

```go
func (rb *RingBuffer) Enqueue(payload []byte) error {
    if len(payload) > MaxFrameSize {
        return ErrFrameTooLarge
    }
    if rb.IsFull() {
        switch rb.policy {
        case DropOldest:
            rb.evictHead() // release pooled buf, increment dropped metric
        case Reject:
            rb.metrics.Rejected.Add(1)
            return ErrBufferFull
        case Block:
            // Return ErrWouldBlock; caller (relay) disables PollRead until space
            return ErrWouldBlock
        }
    }
    if rb.byteCount.Load()+uint64(len(payload)) > rb.maxBytes {
        // same policy dispatch on byte limit
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
```

**DropOldest eviction:**

```go
func (rb *RingBuffer) evictHead() {
    head := rb.head.Load()
    entry := &rb.entries[head%rb.capacity]
    rb.byteCount.Add(-uint64(entry.length))
    entry.Reset(rb.pool)
    rb.head.Store(head + 1)
    rb.count.Add(^uint32(0)) // decrement
    rb.metrics.Dropped.Add(1)
}
```

---

### Step 2.4 — Backpressure Policy Integration with NetPoller

**`internal/buffer/backpressure.go`:**

```go
type BackpressurePolicy int

const (
    DropOldest BackpressurePolicy = iota
    Reject
    Block
)

func ParsePolicy(s string) (BackpressurePolicy, error)
```

**`internal/proxy/relay.go` modification:**

```go
func (s *Server) forwardClientPayload(sess *session.ClientSession, payload []byte) error {
    state := sess.GetState()
    switch state {
    case session.StateActive:
        return s.writeUpstream(sess, payload)
    case session.StateFrozen, session.StateDraining:
        err := sess.RingBuffer.Enqueue(payload)
        if errors.Is(err, buffer.ErrWouldBlock) {
            s.poller.Mod(sess.ClientFD, 0) // disable PollRead
            sess.SetReadParked(true)
        }
        if errors.Is(err, buffer.ErrBufferFull) && sess.Protocol == protocol.WebSocket {
            return ws.SendClose(sess.ClientFD, ws.StatusTryAgainLater)
        }
        return err
    default:
        return session.ErrSessionClosed
    }
}
```

**Unpark read when space available:**

```go
func (s *Server) onBufferSpaceAvailable(sess *session.ClientSession) {
    if sess.ReadParked() {
        s.poller.Mod(sess.ClientFD, netpoll.PollRead)
        sess.SetReadParked(false)
    }
}
```

---

### Step 2.5 — FlushScheduler

**`internal/flush/scheduler.go`:**

```go
type Scheduler struct {
    server *proxy.Server  // access poller, upstream write
}

func (fs *Scheduler) Drain(sess *session.ClientSession) error {
    for {
        entry, ok := sess.RingBuffer.Peek()
        if !ok { return nil }

        n, err := fs.writeUpstreamFrame(sess, entry.Payload())
        if err != nil {
            return err // head NOT advanced; at-least-once preserved
        }
        if n != len(entry.Payload()) {
            return ErrShortWrite
        }

        fs.onAck(sess, entry)
    }
}

func (fs *Scheduler) onAck(sess *session.ClientSession, entry *buffer.RingEntry) {
    sess.RingBuffer.AdvanceHead(sess.RingBuffer.pool)
    fs.metrics.FlushTotal.Add(1)
    fs.recordFlushLatency(entry.Enqueued)
    fs.server.onBufferSpaceAvailable(sess)
}
```

**Write completion detection (Phase 2 — edge-triggered write):**

```go
func (s *Server) handleUpstreamWrite(ev netpoll.Event) error {
    sess := s.fdReg.Lookup(int(ev.FD))
    return s.flushScheduler.Drain(sess)
}
```

Register upstream fd with `PollWrite` when buffer non-empty and upstream writable.

---

### Step 2.6 — Session Integration

**`internal/session/session.go` additions:**

```go
type ClientSession struct {
    // ... Phase 1 fields
    RingBuffer  *buffer.RingBuffer
    readParked  atomic.Bool
}

func (s *ClientSession) AttachBuffer(rb *buffer.RingBuffer)
```

Buffer allocated at session creation (cold path):

```go
func (s *Server) createSession(clientFD int, proto Protocol) *ClientSession {
    sess := &ClientSession{ /* ... */ }
    sess.AttachBuffer(buffer.NewRingBuffer(s.cfg.Buffer, s.bytePool))
    return sess
}
```

---

### Step 2.7 — Test Hook for Manual Freeze

Phase 3 owns automatic Freeze. Phase 2 provides a test/admin hook:

```go
// internal/session/session.go
func (s *ClientSession) Freeze() bool {
    return s.CASState(StateActive, StateFrozen)
}

func (s *ClientSession) Thaw() bool {
    return s.CASState(StateFrozen, StateDraining)
}
```

Used by integration tests to validate enqueue during simulated upstream outage.

---

### Step 2.8 — Metrics

```go
// internal/metrics/buffer.go
kervan_buffer_depth          // Histogram, observe on Enqueue
kervan_buffer_dropped_total  // CounterVec{policy}
kervan_flush_total           // Counter
kervan_flush_latency_seconds // Histogram
kervan_pool_hit_ratio        // GaugeFunc from Pool.HitRatio()
```

---

## 4. Memory & Performance Bounds

| Constraint | Target | Notes |
|------------|--------|-------|
| `Enqueue` (inline payload, no eviction) | **0 allocs/op** | Payload ≤ 256 B copied into pre-allocated entry |
| `Enqueue` (pooled payload) | **0 allocs/op** steady-state | Pool hit; miss only on cold start |
| `Dequeue` / `AdvanceHead` | **0 allocs/op** | In-place head advance |
| `DropOldest` eviction | **0 allocs/op** | Release to pool, no new allocation |
| Ring buffer slab | Allocated **once** per session | `make([]RingEntry, capacity)` at create |
| Per-session memory | `capacity × (InlineThreshold + entryMeta) + pooled bytes` | Default ≤ 2 MB via `max_bytes` |
| Pool retained memory | Not GC-reclaimed | Document RSS vs heap; set `GOMEMLIMIT` |
| seqNum | `atomic.Uint64.Add(1)` | No lock |

### GC Guidance

```go
// Forbidden in internal/buffer/, internal/flush/, internal/proxy/relay.go hot paths:
make([]byte, n)   // use pool.Acquire
append([]byte...)  // on pooled slices
fmt.Sprintf        // in Enqueue/Drain
```

Add `//go:generate` linter or `scripts/check-no-make-bytes.sh` in CI.

### Internal Fragmentation

513-byte payload → 1024-byte size class (50% waste). Document in operator guide; tunable via `inline_threshold` increase if workload is predominantly sub-512 B.

---

## 5. Testing, Verification & Benchmarking Criteria

### 5.1 Unit Tests

| Test | Scenario |
|------|----------|
| `TestRingFIFOOrder` | Enqueue 1000 entries, dequeue preserves order |
| `TestRingDropOldest` | Fill buffer, enqueue 1 more, verify head evicted, tail added |
| `TestRingReject` | Full buffer → `ErrBufferFull`, count unchanged |
| `TestRingBlock` | Full buffer → `ErrWouldBlock`; after dequeue, enqueue succeeds |
| `TestRingMaxBytes` | Enqueue until byte limit; policy fires at byte not entry boundary |
| `TestInlineVsPooled` | 255 B inline (extBuf nil); 257 B pooled (extBuf non-nil) |
| `TestSeqNumMonotonic` | seqNum strictly increasing across enqueues |
| `TestAdvanceHeadReleasesPool` | Pooled entry released; inline entry zeroed |
| `TestPoolHitRatio` | 10k acquire/release cycles → hit ratio > 0.99 after warmup |

### 5.2 Integration Tests

```go
func TestFreezeEnqueueDrain(t *testing.T) {
    // Active session, upstream echo
    // sess.Freeze()
    // Client sends 100 messages while frozen
    // sess.Thaw(); trigger FlushScheduler.Drain
    // Upstream receives 100 messages in FIFO order with matching seqNum
}

func TestAtLeastOnceOnPartialFlush(t *testing.T) {
    // Enqueue 50 messages, Freeze
    // Thaw, drain 25, kill upstream fd mid-drain
    // Reconnect upstream, drain again
    // Upstream receives messages 1-25 once, 26-50 at least once
    // Messages 1-25 may appear twice (at-least-once); verify seqNum ordering
}

func TestClientDisconnectMidBuffer(t *testing.T) {
    // Freeze, enqueue 500 messages, client RST
    // Assert: all pooled buffers released, no leak (runtime.ReadMemStats)
}

func TestBackpressureRejectClosesWS(t *testing.T) {
    // Reject policy, fill buffer, next send → WS close 1013
}

func TestBlockPolicyUnpark(t *testing.T) {
    // Block policy, fill buffer, client read parked
    // Drain 1 entry, assert PollRead re-armed
}
```

### 5.3 Benchmark Tests

```go
func BenchmarkRingEnqueueInline(b *testing.B) {
    rb := newTestRing(1024, DropOldest)
    payload := make([]byte, 128) // outside loop
    b.ReportAllocs()
    for i := 0; i < b.N; i++ {
        rb.Enqueue(payload)
        if rb.IsFull() { rb.AdvanceHead(pool) } // steady-state
    }
    // GATE: 0 allocs/op
}

func BenchmarkRingEnqueuePooled(b *testing.B) {
    payload := make([]byte, 4096)
    // GATE: 0 allocs/op after pool warmup (first iteration may miss)
}

func BenchmarkFlushDrain(b *testing.B) {
    // Pre-filled buffer, mock upstream pipe
    // GATE: 0 allocs/op per drained entry
}
```

**Warmup requirement:** Run `b.ResetTimer()` after 1000 pool-warming iterations; gate applies to post-warmup `allocs/op`.

---

## 6. Definition of Done

- [ ] **`RingBuffer`** implements FIFO enqueue/dequeue with atomic head/tail/count
- [ ] **Inline threshold** (256 B) and **size-class pool** (256 B–64 KB) operational
- [ ] All three **backpressure policies** implemented and config-selectable
- [ ] **`FlushScheduler`** drains buffer in FIFO order; head not advanced until write succeeds
- [ ] **At-least-once** verified: mid-drain upstream kill retains un-ACKed head entry
- [ ] **`BenchmarkRingEnqueueInline`**: 0 allocs/op
- [ ] **`BenchmarkRingEnqueuePooled`**: 0 allocs/op (post-warmup)
- [ ] **`BenchmarkFlushDrain`**: 0 allocs/op
- [ ] **Integration**: `TestFreezeEnqueueDrain`, `TestAtLeastOnceOnPartialFlush`, `TestClientDisconnectMidBuffer` pass
- [ ] **Metrics** exported: buffer depth, dropped, flush total, flush latency, pool hit ratio
- [ ] **Lint/CI**: hot-path packages fail build if `make([]byte` detected (excluding tests)
- [ ] **`forwardClientPayload`** dispatches on session state (Active vs Frozen/Draining)
- [ ] **Manual Freeze/Thaw** hooks available for Phase 3 integration
- [ ] **Memory leak test**: 1k connect/freeze/enqueue/disconnect cycles; `runtime.MemStats.HeapInuse` stable ±10%

---

## Appendix — Phase 2 → Phase 3 Handoff

Phase 3 replaces manual `Freeze()`/`Thaw()` with TargetRegistry-driven transitions. Phase 2 must expose:

```go
// internal/buffer/ring.go
func (rb *RingBuffer) Len() uint32       // TargetRegistry observes queue depth
func (rb *RingBuffer) ByteLen() uint64

// internal/flush/scheduler.go
func (fs *Scheduler) Drain(sess *session.ClientSession) error  // called by Phase 3 Thaw
```

---

*Previous: [Phase 1 — Core Engine](./phase-1-core-engine.md) · Next: [Phase 3 — Failover Routing](./phase-3-failover-routing.md)*
