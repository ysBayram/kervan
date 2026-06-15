# Phase 1: High-Performance Connection Handling & Sharded Session Management

**Phase ID:** KERVAN-IMPL-001  
**Status:** Planned  
**Depends On:** None  
**Blocks:** Phase 2, Phase 3, Phase 4  
**TDD Reference:** §4.1, §5.2, §5.3, §7.1–§7.3, §8.1–§8.2  
**ADR Reference:** ADR-001, ADR-003 (goroutine budget, NetPoller)

---

## 1. Objective & Scope

**Objective:** Deliver the Kervan core I/O engine—a fixed-goroutine NetPoller with edge-triggered epoll/kqueue and a 256-shard SessionManager—that accepts, tracks, and bidirectionally proxies TCP and WebSocket client connections without goroutine-per-connection overhead.

### Included

- Go module bootstrap (`go.mod`, `cmd/kervan/main.go`)
- Cross-platform NetPoller abstraction (Linux epoll primary; kqueue for macOS/BSD)
- TCP listener with non-blocking accept loop integrated into NetPoller
- WebSocket upgrade handshake (RFC 6455) via `github.com/gobwas/ws` (zero-copy framing library; **not** `gorilla/websocket`)
- Sharded `SessionManager` with FNV-1a hash routing
- `ClientSession` struct with atomic state field (lifecycle stub; transitions fully implemented in Phase 3)
- Passthrough proxy mode: client ↔ single static upstream (no buffering, no failover—proves I/O path)
- Basic configuration loading (YAML via `gopkg.in/yaml.v3`)
- Structured logging (`log/slog`) and skeleton Prometheus metrics registry

### Excluded

- Ring buffer enqueue/dequeue (Phase 2)
- Backpressure policies (Phase 2)
- TargetRegistry, health probes, Freeze/Thaw (Phase 3)
- External coordination store / etcd (Phase 4)
- TLS termination (deferred per TDD §2.2)
- Authentication / authorization
- Durable persistence

---

## 2. Target Directory & File Structure

```
kervan/
├── cmd/
│   └── kervan/
│       └── main.go                    # Entrypoint: parse flags, load config, start Server
├── internal/
│   ├── config/
│   │   ├── config.go                  # ServerConfig, ListenerConfig structs
│   │   └── config_test.go
│   ├── netpoll/
│   │   ├── poller.go                  # NetPoller interface + shared types
│   │   ├── epoll_linux.go             //go:build linux
│   │   ├── kqueue_bsd.go              //go:build darwin || freebsd || openbsd
│   │   ├── poll_fallback.go           //go:build !linux && !darwin && !freebsd && !openbsd
│   │   ├── fd.go                      # FD wrapper, setNonblock, dup
│   │   └── poller_test.go
│   ├── session/
│   │   ├── session.go                 # ClientSession struct + state constants
│   │   ├── manager.go                 # SessionManager + sharded map
│   │   ├── hash.go                    # FNV-1a shard index
│   │   ├── manager_test.go
│   │   └── session_test.go
│   ├── protocol/
│   │   ├── tcp/
│   │   │   └── stream.go              # Raw byte-stream read/write handlers
│   │   └── ws/
│   │       ├── handshake.go           # HTTP upgrade → WebSocket
│   │       ├── frame.go               # gobwas/ws read/write integration
│   │       └── handshake_test.go
│   ├── proxy/
│   │   ├── server.go                  # Top-level Server: listener + poller + session mgr
│   │   ├── accept.go                  # Accept loop, session creation
│   │   ├── relay.go                   # Client↔upstream passthrough (Phase 1 only)
│   │   └── server_test.go
│   └── metrics/
│       ├── registry.go                # Prometheus registry wrapper
│       └── sessions.go                # kervan_sessions_active gauge
├── pkg/
│   └── api/
│       └── types.go                   # Exported stable types (SessionState, errors)
├── configs/
│   └── kervan.example.yaml
├── go.mod
├── go.sum
└── Makefile                           # build, test, bench, lint targets
```

### Key External Dependencies (Phase 1)

| Package | Purpose | Rationale |
|---------|---------|-----------|
| `github.com/gobwas/ws` | WebSocket framing | Zero-allocation WS read/write; no `net/http` per-frame overhead |
| `golang.org/x/sys/unix` | epoll/kqueue syscalls | Direct OS event notification |
| `github.com/prometheus/client_golang` | Metrics | Standard observability |
| `gopkg.in/yaml.v3` | Config | Human-readable operator config |

**Explicitly rejected for Phase 1:** `gnet` (opinionated event loop conflicts with custom SessionManager integration), `gorilla/websocket` (allocates per frame), standard `net/http` server (goroutine-per-request on upgrade path).

---

## 3. Step-by-Step Coding Roadmap

### Step 1.1 — Module Bootstrap

```bash
go mod init github.com/ysBayram/kervan
```

**`cmd/kervan/main.go`:**

```go
func main() {
    cfg, err := config.Load(os.Args[1]) // default: configs/kervan.yaml
    if err != nil { log.Fatal(err) }
    srv := proxy.NewServer(cfg)
    if err := srv.Run(context.Background()); err != nil { log.Fatal(err) }
}
```

**`internal/config/config.go`:**

```go
type Config struct {
    NodeID       string           `yaml:"node_id"`
    Listeners    []ListenerConfig `yaml:"listeners"`
    Upstream     UpstreamConfig   `yaml:"upstream"`      // static single target (Phase 1)
    Session      SessionConfig    `yaml:"session"`
    MetricsAddr  string           `yaml:"metrics_addr"`  // ":9090"
}

type ListenerConfig struct {
    Bind         string `yaml:"bind"`          // ":8080"
    Protocol     string `yaml:"protocol"`      // "tcp" | "websocket"
    MaxSessions  int    `yaml:"max_sessions"`  // 10000
}

type SessionConfig struct {
    ShardCount       uint32        `yaml:"shard_count"`        // 256, power of 2
    IdleTimeout      time.Duration `yaml:"idle_timeout"`       // 15m
    ReadBufferSize   int           `yaml:"read_buffer_size"`   // 4096
}
```

---

### Step 1.2 — NetPoller Core

**`internal/netpoll/poller.go`:**

```go
type PollOp uint8

const (
    PollRead PollOp = 1 << iota
    PollWrite
    PollReadWrite = PollRead | PollWrite
)

type Handler func(ev Event) error

type Event struct {
    FD      int32
    Op      PollOp
    Flags   uint32   // EPOLLERR | EPOLLHUP, etc.
    UserData uintptr // *session.ClientSession
}

type Poller interface {
    Add(fd int, op PollOp, userData uintptr) error
    Mod(fd int, op PollOp) error
    Del(fd int) error
    Wait(batch []Event) (n int, err error)
    Close() error
}

type Config struct {
    BatchSize int // default 512
}

func New(cfg Config) (Poller, error)
```

**`internal/netpoll/epoll_linux.go`:**

```go
type epollPoller struct {
    epfd   int
    events []unix.EpollEvent // pre-allocated, len = BatchSize
}

func (p *epollPoller) Add(fd int, op PollOp, userData uintptr) error {
    var ev unix.EpollEvent
    ev.Events = unix.EPOLLET | opToEpoll(op)
    ev.Fd = int32(fd)
    ev.Pad = int32(userData) // store session ptr via unsafe or fd→session side map
    return unix.EpollCtl(p.epfd, unix.EPOLL_CTL_ADD, fd, &ev)
}
```

**Design note:** Store `*ClientSession` in a parallel `[]atomic.Pointer[ClientSession]` indexed by fd slot, OR use `unix.EpollEvent.Pad` with `uintptr(unsafe.Pointer(session))`. Prefer **fd-indexed side table** to avoid unsafe pointer in epoll event (GC visibility):

```go
type fdRegistry struct {
    slots []atomic.Pointer[session.ClientSession] // sized to RLIMIT_NOFILE
}

func (r *fdRegistry) Register(fd int, s *session.ClientSession)
func (r *fdRegistry) Lookup(fd int) *session.ClientSession
```

---

### Step 1.3 — Session Types & Atomic State

**`internal/session/session.go`:**

```go
type State uint32

const (
    StateCreated State = iota
    StateActive
    StateFrozen      // used Phase 3+
    StateDraining    // used Phase 3+
    StateClosed
)

type ClientSession struct {
    ClientID     string
    ClientFD     int
    UpstreamFD   int           // Phase 1: direct upstream fd
    State        atomic.Uint32
    Protocol     Protocol      // TCP | WebSocket
    CreatedAt    int64         // unix nano
    LastActivity atomic.Int64  // unix nano, updated on each read/write
    // RingBuffer attached Phase 2
    // TargetID attached Phase 3
    _            [64]byte      // cache line padding
}

func (s *ClientSession) CASState(from, to State) bool {
    return s.State.CompareAndSwap(uint32(from), uint32(to))
}

func (s *ClientSession) GetState() State {
    return State(s.State.Load())
}

type Protocol uint8

const (
    ProtocolTCP Protocol = iota
    ProtocolWebSocket
)
```

---

### Step 1.4 — Sharded SessionManager

**`internal/session/hash.go`:**

```go
const DefaultShardCount = 256

func ShardIndex(clientID string, mask uint32) uint32 {
    h := fnv.New32a()
    h.Write([]byte(clientID)) // cold path only (session create); NOT hot path
    return h.Sum32() & mask
}
```

**Hot path optimization:** At session creation, precompute `shardIdx uint8` on `ClientSession` to avoid re-hashing on every lookup:

```go
type ClientSession struct {
    // ...
    shardIdx uint8
}
```

**`internal/session/manager.go`:**

```go
type shard struct {
    mu       sync.RWMutex
    sessions map[string]*ClientSession
}

type SessionManager struct {
    shards    []*shard
    shardMask uint32
    config    SessionConfig
    active    atomic.Int64  // gauge backing
}

func NewManager(cfg SessionConfig) *SessionManager

func (sm *SessionManager) shardFor(clientID string) *shard
func (sm *SessionManager) shardByIndex(idx uint8) *shard

// Hot path — called from NetPoller handler
func (sm *SessionManager) GetByFD(reg *fdRegistry, fd int) (*ClientSession, bool)

func (sm *SessionManager) Get(clientID string) (*ClientSession, bool)
func (sm *SessionManager) Insert(sess *ClientSession) error  // rejects duplicate ClientID
func (sm *SessionManager) Remove(clientID string) (*ClientSession, bool)
func (sm *SessionManager) ActiveCount() int64
func (sm *SessionManager) RangeShard(idx uint8, fn func(*ClientSession) bool)
```

---

### Step 1.5 — TCP Listener & Accept Integration

**`internal/proxy/accept.go`:**

```go
func (s *Server) acceptLoop(ctx context.Context, ln net.Listener, proto protocol.Protocol) error {
    for {
        conn, err := ln.Accept()
        if err != nil { /* handle */ }
        rawConn := conn.(syscall.Conn)
        raw, _ := rawConn.SyscallConn()
        var fd int
        raw.Control(func(fdPtr uintptr) { fd = int(fdPtr) })
        setNonblock(fd)

        sess := s.createSession(fd, proto)
        s.poller.Add(fd, netpoll.PollRead, fdSlot(sess))
        s.sessions.Insert(sess)
    }
}
```

**ClientID assignment (Phase 1):**

```go
func generateClientID(fd int) string {
    // Phase 1: fd-based ephemeral ID
    // Phase 4: extract from WS subprotocol header or query param
    return strconv.Itoa(fd) + "@" + nodeID
}
```

---

### Step 1.6 — WebSocket Handshake

**`internal/protocol/ws/handshake.go`:**

```go
import "github.com/gobwas/ws"

func Upgrade(clientFD int, readBuf []byte) (clientID string, err error) {
    // Read HTTP upgrade request from clientFD (blocking once, cold path)
    // ws.Upgrade() or manual parse via gobwas/http
    // Return ClientID from Sec-WebSocket-Key hash or X-Client-ID header
}

func ReadFrame(r io.Reader, buf []byte) (opcode ws.OpCode, payload []byte, err error)
func WriteFrame(w io.Writer, opcode ws.OpCode, payload []byte) error
```

Use a **pre-allocated read buffer per poller worker** (not per session):

```go
type workerLocal struct {
    readBuf [MaxFrameSize]byte // 64KB max WS frame
}
```

---

### Step 1.7 — Relay Passthrough (Phase 1 Proxy)

**`internal/proxy/relay.go`:**

```go
func (s *Server) handleClientRead(ev netpoll.Event) error {
    sess := s.fdReg.Lookup(int(ev.FD))
    if sess == nil { return errUnknownFD }

    n, err := unix.Read(sess.ClientFD, s.workerBuf[:])
    if err == unix.EAGAIN { return s.poller.Mod(sess.ClientFD, netpoll.PollRead) }
    if n == 0 { return s.closeSession(sess) }

    sess.LastActivity.Store(time.Now().UnixNano())

    // Phase 1: direct write to upstream (no buffer)
    _, werr := unix.Write(sess.UpstreamFD, s.workerBuf[:n])
    if werr != nil { return s.handleUpstreamError(sess, werr) }

    return s.poller.Mod(sess.ClientFD, netpoll.PollRead)
}

func (s *Server) handleUpstreamRead(ev netpoll.Event) error {
    // Mirror: upstream → client
}
```

**Upstream connection (Phase 1 static):**

```go
func (s *Server) dialUpstream(cfg UpstreamConfig) (int, error) {
    // net.Dial → extract fd → setNonblock → poller.Add
}
```

---

### Step 1.8 — Poller Worker Pool

**`internal/proxy/server.go`:**

```go
type Server struct {
    cfg       *config.Config
    poller    netpoll.Poller
    sessions  *session.SessionManager
    fdReg     *fdRegistry
    workerBuf []byte               // per-worker in worker struct, not here
    metrics   *metrics.Registry
}

func (s *Server) Run(ctx context.Context) error {
    // Start GOMAXPROCS poller workers
    for i := 0; i < runtime.GOMAXPROCS(0); i++ {
        go s.pollerWorker(ctx, i)
    }
    // Start accept loops, metrics HTTP
    <-ctx.Done()
    return s.Shutdown()
}

func (s *Server) pollerWorker(ctx context.Context, id int) {
    local := workerLocal{ readBuf: make([]byte, s.cfg.Session.ReadBufferSize) }
    events := make([]netpoll.Event, 512) // allocated once per worker
    for {
        n, err := s.poller.Wait(events)
        for i := 0; i < n; i++ {
            s.dispatch(events[i], &local)
        }
    }
}
```

**Goroutine budget (Phase 1):** `GOMAXPROCS` poller workers + 1 accept per listener + 1 metrics = **≤ GOMAXPROCS + listeners + 2**.

---

### Step 1.9 — Metrics Skeleton

**`internal/metrics/sessions.go`:**

```go
var (
    SessionsActive = promauto.NewGauge(prometheus.GaugeOpts{
        Name: "kervan_sessions_active",
        Help: "Current active client sessions",
    })
)
```

Update `SessionsActive` on Insert/Remove only (cold path).

---

## 4. Memory & Performance Bounds

| Constraint | Target | Enforcement |
|------------|--------|-------------|
| Goroutines (10k connections) | ≤ `GOMAXPROCS + listeners + 2` | Code review; `TestGoroutineBudget` |
| Hot-path heap allocs (relay loop) | **0 allocs/op** | `BenchmarkRelay` with `-benchmem` |
| Session struct size | ≤ 256 bytes (excl. buffers) | `unsafe.Sizeof` static test |
| Shard lock hold time (read path) | ≤ 100 ns (map lookup only) | `BenchmarkSessionManagerGet` |
| epoll Wait batch | 512 events per syscall | Pre-allocated slice per worker |
| FD→session lookup | O(1), no hash | Array-indexed `fdRegistry` |
| Read buffer | 1 buffer per poller worker, reused | No per-session readBuf |

### sync.Pool (Phase 1 — minimal)

Phase 1 defers payload pooling to Phase 2. Allowed pools in Phase 1:

```go
var eventPool = sync.Pool{New: func() any { return new(netpoll.Event) }} // optional, if events heap-allocated
```

**Prohibited in Phase 1 hot path:** `make([]byte, n)` inside `handleClientRead` / `handleUpstreamRead`. Use worker-local fixed buffer and slice `buf[:n]`.

### File Descriptor Limits

```go
// main.go init
func raiseFDLimit() {
    var lim syscall.Rlimit
    syscall.Getrlimit(syscall.RLIMIT_NOFILE, &lim)
    lim.Cur = lim.Max
    syscall.Setrlimit(syscall.RLIMIT_NOFILE, &lim)
}
```

Pre-size `fdRegistry.slots` to `lim.Cur`.

---

## 5. Testing, Verification & Benchmarking Criteria

### 5.1 Unit Tests

| Package | Test | Edge Cases |
|---------|------|------------|
| `session/hash` | `TestShardIndexDistribution` | Uniform distribution across 256 shards (chi-squared) |
| `session/manager` | `TestInsertDuplicateRejected` | Same ClientID twice → error |
| `session/manager` | `TestConcurrentGetInsert` | 100 goroutines × 1000 ops, `-race` clean |
| `session/session` | `TestCASState` | Invalid transitions fail; valid succeed |
| `netpoll` | `TestEpollAddModDel` | Add fd, write readiness, delete, no leak |
| `protocol/ws` | `TestHandshakeValid` | RFC 6455 compliant upgrade |
| `protocol/ws` | `TestHandshakeMissingKey` | Reject malformed request |

### 5.2 Integration Tests

**`internal/proxy/server_integration_test.go`** (build tag `integration`):

```go
func TestTCPPassthrough(t *testing.T) {
    // Start mock upstream TCP echo server
    // Start Kervan proxy
    // Client sends 1MB payload
    // Assert echo received, session count = 1, then 0 after close
}

func TestWebSocketPassthrough(t *testing.T) {
    // Client WS connect → send text frames → upstream receives → response returned
}

func TestClientDisconnectMidStream(t *testing.T) {
    // Client sends 50% of 1MB then RST
    // Assert: upstream fd closed, session removed from manager, no goroutine leak
}

func TestUpstreamDisconnectClientStaysOpen(t *testing.T) {
    // Kill upstream mid-stream
    // Phase 1 behavior: client connection remains open (error logged, upstream fd removed)
    // NOTE: buffering not yet available; document as known limitation until Phase 2
}

func TestTenThousandConcurrentConnections(t *testing.T) {
    if testing.Short() { t.Skip() }
    // 10k idle TCP connections; assert ActiveCount == 10000, goroutine count stable
}
```

### 5.3 Benchmark Tests

**`internal/proxy/relay_bench_test.go`:**

```go
func BenchmarkRelayClientToUpstream(b *testing.B) {
    // Pre-wired session, non-blocking pipe fds
    b.ReportAllocs()
    b.SetBytes(4096)
    for i := 0; i < b.N; i++ {
        srv.handleClientRead(ev)
    }
    // GATE: allocs/op MUST == 0
}

func BenchmarkSessionManagerGet(b *testing.B) {
    // Pre-populated 10k sessions
    // GATE: allocs/op == 0
}
```

**CI gate:**

```makefile
bench-gate:
	go test -bench=BenchmarkRelay -benchmem -count=5 ./internal/proxy/ | tee bench.txt
	go test -bench=BenchmarkSessionManagerGet -benchmem -count=5 ./internal/session/
	# Script fails if any allocs/op > 0 on hot-path benchmarks
```

### 5.4 Manual Verification Checklist

- [ ] `curl --include --no-buffer -H "Connection: Upgrade" ...` WebSocket upgrade succeeds
- [ ] `ss -tnp | grep kervan` shows 10k ESTAB with stable process thread count
- [ ] `go tool pprof -alloc_space` under load shows no allocs in `handleClientRead`

---

## 6. Definition of Done

All items must pass before merging Phase 1 milestone and opening Phase 2 work:

- [ ] **Module compiles** on `linux/amd64` and `darwin/arm64`
- [ ] **TCP and WebSocket** listeners accept connections and passthrough to static upstream
- [ ] **SessionManager** uses 256-shard map; `Get`/`Insert`/`Remove` covered by unit tests
- [ ] **NetPoller** uses epoll (Linux) / kqueue (macOS) edge-triggered mode
- [ ] **Zero goroutine-per-connection**: 10k idle connections with goroutine count ≤ `GOMAXPROCS + 4`
- [ ] **`BenchmarkRelayClientToUpstream`**: `0 allocs/op` at 4096-byte reads
- [ ] **`BenchmarkSessionManagerGet`**: `0 allocs/op`
- [ ] **Race detector clean**: `go test -race ./...` passes
- [ ] **Integration**: `TestTCPPassthrough`, `TestWebSocketPassthrough`, `TestClientDisconnectMidStream` pass
- [ ] **Metrics**: `kervan_sessions_active` reflects live session count
- [ ] **Config**: `configs/kervan.example.yaml` documents all Phase 1 settings
- [ ] **Documentation**: README quickstart with build/run instructions
- [ ] **Code review sign-off** on fdRegistry design and shard pre-index on ClientSession

---

## Appendix — Phase 1 → Phase 2 Handoff

Phase 2 will attach `*buffer.RingBuffer` to `ClientSession` and replace direct `unix.Write` in `relay.go` with a dispatch function that checks session state. Phase 1 must expose this seam:

```go
// internal/proxy/relay.go — seam for Phase 2
func (s *Server) forwardClientPayload(sess *session.ClientSession, payload []byte) error {
    // Phase 1: direct upstream write
    // Phase 2: if sess.State == Active → write; else → ringBuffer.Enqueue()
    return s.writeUpstream(sess, payload)
}
```

---

*Next: [Phase 2 — In-Memory Ring Buffering](./phase-2-ring-buffer.md)*
