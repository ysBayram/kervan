# Phase 3: Dynamic Upstream Target Routing & Failover Lifecycles

**Phase ID:** KERVAN-IMPL-003  
**Status:** Planned  
**Depends On:** Phase 1 (NetPoller, SessionManager), Phase 2 (RingBuffer, FlushScheduler)  
**Blocks:** Phase 4 (coordination store publishes target health)  
**TDD Reference:** §4.3, §6, §6.4, §8.3  
**ADR Reference:** ADR-001 (Freeze/Thaw FSM), ADR-002 (local target cache; store integration deferred to Phase 4)

---

## 1. Objective & Scope

**Objective:** Implement the TargetRegistry with backend discovery, active/passive health probing, consistent-hash routing, and the full Freeze/Thaw failover state machine—ensuring client connections survive upstream pod death while buffered payloads drain in FIFO at-least-once order to replacement targets.

### Included

- `TargetRegistry` with in-memory target cache and health state machine
- Backend discovery adapters: static file, Kubernetes Endpoints (in-cluster)
- Active health probes: TCP connect, HTTP GET `/health`, WebSocket ping
- Passive health: upstream write error rate threshold (fast-path Freeze)
- Consistent-hash routing: `(route, ClientID) → TargetID`
- Automatic **Freeze** on target unhealthy: CAS `Active → Frozen`, deregister upstream fd
- Automatic **Thaw** on replacement target healthy: dial upstream, CAS `Frozen → Draining`, invoke FlushScheduler
- Mid-drain failure handling: CAS `Draining → Frozen`
- Session enumeration by TargetID for bulk Freeze
- Idle session eviction and `MaxFreezeDuration` forced disconnect
- Prometheus: `kervan_target_health`, `kervan_sessions_frozen`, `kervan_failover_total`
- Chaos-test harness for rolling-update simulation

### Excluded

- External store read/write for target health (Phase 4; Phase 3 uses local cache only with `Publish()` stub interface)
- Session lease acquisition (Phase 4)
- Cross-node target health sharing (Phase 4)
- TLS to upstream
- Multi-route weighted load balancing (v0.2)

---

## 2. Target Directory & File Structure

```
kervan/
├── internal/
│   ├── target/
│   │   ├── registry.go             # TargetRegistry core
│   │   ├── target.go               # Target, TargetID, TargetState structs
│   │   ├── router.go               # Consistent hash ring
│   │   ├── freeze.go               # Freeze/Thaw orchestration
│   │   ├── registry_test.go
│   │   └── router_test.go
│   ├── discovery/
│   │   ├── discovery.go              # Discovery interface
│   │   ├── static.go                 # File-based target list
│   │   └── kubernetes.go             //go:build k8s — Endpoints watch
│   ├── health/
│   │   ├── prober.go                 # HealthProber interface
│   │   ├── tcp.go                    # TCP connect probe
│   │   ├── http.go                   # HTTP /health probe
│   │   ├── ws.go                     # WebSocket ping probe
│   │   ├── passive.go                # Write error rate observer
│   │   └── prober_test.go
│   ├── coordination/
│   │   └── store.go                  # CoordinationStore interface (stub/mock)
│   ├── session/
│   │   └── session.go                # ADD: TargetID, route name
│   └── proxy/
│       ├── server.go                 # Wire TargetRegistry
│       ├── upstream.go               # Dynamic upstream dial + fd lifecycle
│       └── relay.go                  # Passive error → registry.NotifyWriteError
├── configs/
│   └── kervan.example.yaml           # ADD: targets.*, health.*, failover.*
```

---

## 3. Step-by-Step Coding Roadmap

### Step 3.1 — Target Model & State Machine

**`internal/target/target.go`:**

```go
type TargetID string

type TargetState uint32

const (
    TargetUnknown TargetState = iota
    TargetHealthy
    TargetUnhealthy
)

type Target struct {
    ID       TargetID
    Addr     string            // "10.0.1.5:8080"
    Route    string            // logical route name
    Weight   int32
    State    atomic.Uint32     // TargetState
    Failures atomic.Uint32     // consecutive probe failures
    Successes atomic.Uint32    // consecutive probe successes
    LastProbe atomic.Int64     // unix nano
}

func (t *Target) CASState(from, to TargetState) bool
func (t *Target) GetState() TargetState
```

---

### Step 3.2 — CoordinationStore Interface (Stub)

**`internal/coordination/store.go`:**

```go
type TargetEvent struct {
    Target Target
    Type   EventType // Added, Removed, HealthChanged
}

type Lease interface {
    Refresh(ctx context.Context) error
    Release() error
    Key() string
}

type CoordinationStore interface {
    RegisterTarget(ctx context.Context, target Target) error
    WatchTargets(ctx context.Context) (<-chan TargetEvent, error)
    AcquireSessionLease(ctx context.Context, clientID string, ttl time.Duration) (Lease, error)
    PublishTargetHealth(ctx context.Context, id TargetID, state TargetState) error
    WatchTargetHealth(ctx context.Context) (<-chan TargetEvent, error)
}

// Phase 3: in-memory noop implementation
type NoopStore struct{}
```

Phase 4 replaces `NoopStore` with etcd/Redis adapter. Phase 3 calls `PublishTargetHealth` but does not depend on watch feedback.

---

### Step 3.3 — TargetRegistry

**`internal/target/registry.go`:**

```go
type RegistryConfig struct {
    ProbeInterval      time.Duration // 2s
    UnhealthyThreshold uint32        // 3
    HealthyThreshold   uint32        // 2
    MaxFreezeDuration  time.Duration // 5m
    DrainTimeout       time.Duration // 30s
    ErrorRateWindow    time.Duration // 5s
    ErrorRateThreshold float64       // 0.5 (50% writes fail)
}

type Registry struct {
    cfg       RegistryConfig
    targets   sync.Map              // TargetID → *Target
    router    *Router
    sessions  *session.SessionManager
    store     coordination.CoordinationStore
    prober    health.Prober
    freeze    *FreezeController
    metrics   *TargetMetrics
    nodeID    string
}

func NewRegistry(cfg RegistryConfig, deps RegistryDeps) *Registry

func (r *Registry) Run(ctx context.Context) error  // probe loop + discovery watch
func (r *Registry) AddTarget(t *Target)
func (r *Registry) RemoveTarget(id TargetID)
func (r *Registry) GetTarget(id TargetID) (*Target, bool)
func (r *Registry) SelectTarget(route, clientID string) (*Target, error)
func (r *Registry) NotifyWriteError(targetID TargetID, err error)
func (r *Registry) OnTargetHealthy(t *Target)
func (r *Registry) OnTargetUnhealthy(t *Target)
```

**Probe loop (1 goroutine + bounded worker pool for probes):**

```go
func (r *Registry) probeLoop(ctx context.Context) {
    ticker := time.NewTicker(r.cfg.ProbeInterval)
    for {
        select {
        case <-ctx.Done(): return
        case <-ticker.C:
            r.targets.Range(func(key, val any) bool {
                t := val.(*Target)
                r.probePool.Submit(func() { r.probeTarget(t) })
                return true
            })
        }
    }
}
```

**Probe worker pool:** `github.com/panjf2000/ants/v2` or fixed channel buffer of size `GOMAXPROCS`—NOT unbounded goroutines per target.

---

### Step 3.4 — Consistent Hash Router

**`internal/target/router.go`:**

```go
type Router struct {
    mu    sync.RWMutex
    ring  *consistent.Consistent  // github.com/buraksezer/consistent
    route map[string][]TargetID   // route → target IDs
}

func NewRouter() *Router

func (rt *Router) UpdateRoute(route string, targets []TargetID)
func (rt *Router) Select(route, clientID string) (TargetID, error) {
    rt.mu.RLock()
    defer rt.mu.RUnlock()
    member := rt.ring.LocateKey([]byte(route + ":" + clientID))
    return TargetID(member.String()), nil
}
```

**Alternative (zero-dep):** Implement minimal consistent hash with xxhash and sorted virtual node ring in `router.go` to avoid external dependency. Document choice in PR.

**Health-aware selection:** Skip `TargetUnhealthy` members; walk ring to next healthy node (bounded attempts = ring size).

```go
func (rt *Router) SelectHealthy(route, clientID string, isHealthy func(TargetID) bool) (TargetID, error)
```

---

### Step 3.5 — Discovery Adapters

**`internal/discovery/discovery.go`:**

```go
type Discovery interface {
    Watch(ctx context.Context) (<-chan DiscoveryEvent, error)
}

type DiscoveryEvent struct {
    Targets []Target
    Route   string
}
```

**`internal/discovery/static.go`:**

```go
type StaticDiscovery struct {
    Path string // YAML file
}
// Poll file every 30s or inotify; emit DiscoveryEvent on change
```

**`internal/discovery/kubernetes.go`:**

```go
// Watch core/v1 Endpoints for configured Service namespace/name
// Map EndpointSubset → Target{ID: podIP:port, Addr: ...}
```

---

### Step 3.6 — Health Probers

**`internal/health/prober.go`:**

```go
type ProbeType int

const (
    ProbeTCP ProbeType = iota
    ProbeHTTP
    ProbeWebSocket
)

type ProbeConfig struct {
    Type     ProbeType
    HTTPPath string // "/health"
    Timeout  time.Duration // 1s
}

type Prober interface {
    Probe(ctx context.Context, addr string) error
}
```

**`internal/health/passive.go`:**

```go
type PassiveObserver struct {
    windows sync.Map // TargetID → *errorWindow
}

type errorWindow struct {
    total   atomic.Uint64
    errors  atomic.Uint64
    resetAt atomic.Int64
}

func (p *PassiveObserver) RecordWrite(targetID TargetID, err error) float64
// Returns current error rate; Registry triggers fast-path Freeze if > threshold
```

**Fast-path Freeze trigger:**

```go
func (r *Registry) NotifyWriteError(targetID TargetID, err error) {
    rate := r.passive.RecordWrite(targetID, err)
    if rate >= r.cfg.ErrorRateThreshold {
        if t, ok := r.GetTarget(targetID); ok {
            r.triggerFreeze(t, reasonPassive)
        }
    }
}
```

---

### Step 3.7 — FreezeController

**`internal/target/freeze.go`:**

```go
type FreezeController struct {
    registry *Registry
    server   *proxy.Server
    flush    *flush.Scheduler
}

type FreezeReason int

const (
    reasonProbe FreezeReason = iota
    reasonPassive
    reasonAdmin
)

func (fc *FreezeController) FreezeTarget(targetID TargetID, reason FreezeReason) error {
    t, ok := fc.registry.GetTarget(targetID)
    if !ok { return ErrTargetNotFound }

    t.CASState(TargetHealthy, TargetUnhealthy)
    fc.registry.store.PublishTargetHealth(context.Background(), targetID, TargetUnhealthy)

    // Enumerate sessions bound to this target
    fc.registry.sessions.RangeAll(func(sess *session.ClientSession) bool {
        if sess.TargetID != targetID { return true }
        fc.freezeSession(sess)
        return true
    })
    return nil
}

func (fc *FreezeController) freezeSession(sess *session.ClientSession) {
    if !sess.CASState(session.StateActive, session.StateFrozen) {
        return // already frozen or draining
    }
    fc.server.DeregisterUpstream(sess)  // close upstream fd, remove from poller
    sess.UpstreamFD = -1
    fc.metrics.SessionsFrozen.Add(1)
    fc.metrics.FailoverTotal.WithLabelValues("freeze").Inc()

    // Start MaxFreezeDuration timer
    fc.scheduleFreezeTimeout(sess)
}
```

---

### Step 3.8 — ThawController

```go
func (fc *FreezeController) ThawTarget(oldTargetID TargetID, newTarget *Target) error {
    fc.registry.sessions.RangeAll(func(sess *session.ClientSession) bool {
        if sess.TargetID != oldTargetID { return true }
        if !sess.CASState(session.StateFrozen, session.StateDraining) {
            return true
        }
        fc.thawSession(sess, newTarget)
        return true
    })
    return nil
}

func (fc *FreezeController) thawSession(sess *session.ClientSession, target *Target) {
    fd, err := fc.server.DialUpstream(target.Addr)
    if err != nil {
        sess.CASState(session.StateDraining, session.StateFrozen)
        return
    }
    sess.UpstreamFD = fd
    sess.TargetID = target.ID
    fc.server.RegisterUpstream(sess, fd)

    // Drain with timeout
    ctx, cancel := context.WithTimeout(context.Background(), fc.registry.cfg.DrainTimeout)
    defer cancel()

    if err := fc.flush.Drain(sess); err != nil {
        sess.CASState(session.StateDraining, session.StateFrozen)
        fc.server.DeregisterUpstream(sess)
        return
    }

    if sess.RingBuffer.Len() == 0 {
        sess.CASState(session.StateDraining, session.StateActive)
        fc.metrics.SessionsFrozen.Add(-1)
        fc.metrics.FailoverTotal.WithLabelValues("thaw").Inc()
    }
}
```

**Mid-drain failure:** Any `Drain` error or upstream write error during `StateDraining` → CAS back to `Frozen`, preserve buffer head.

---

### Step 3.9 — Upstream FD Lifecycle

**`internal/proxy/upstream.go`:**

```go
func (s *Server) DialUpstream(addr string) (fd int, err error) {
    // net.DialTimeout → SyscallConn → extract fd → setNonblock
}

func (s *Server) RegisterUpstream(sess *session.ClientSession, fd int) {
    s.fdReg.Register(fd, sess) // or separate upstream registry
    s.poller.Add(fd, netpoll.PollRead|netpoll.PollWrite, fdSlot(sess))
}

func (s *Server) DeregisterUpstream(sess *session.ClientSession) {
    if sess.UpstreamFD >= 0 {
        s.poller.Del(sess.UpstreamFD)
        unix.Close(sess.UpstreamFD)
    }
}
```

---

### Step 3.10 — Session Target Binding

At session creation:

```go
target, err := registry.SelectTarget(route, clientID)
sess.TargetID = target.ID
fd, _ := server.DialUpstream(target.Addr)
sess.UpstreamFD = fd
```

On target health transition (Registry callback):

```go
func (r *Registry) OnTargetUnhealthy(t *Target) {
    if t.Failures.Load() >= r.cfg.UnhealthyThreshold {
        r.freeze.FreezeTarget(t.ID, reasonProbe)
    }
}

func (r *Registry) OnTargetHealthy(t *Target) {
    if t.Successes.Load() >= r.cfg.HealthyThreshold {
        // Find sessions frozen on this route; thaw to t (may differ from original pod)
        r.freeze.ThawRoute(t.Route, t)
    }
}
```

---

### Step 3.11 — SessionManager Range Extension

**`internal/session/manager.go` addition:**

```go
func (sm *SessionManager) RangeAll(fn func(*ClientSession) bool) {
    for _, sh := range sm.shards {
        sh.mu.RLock()
        for _, sess := range sh.sessions {
            if !fn(sess) { sh.mu.RUnlock(); return }
        }
        sh.mu.RUnlock()
    }
}

func (sm *SessionManager) CountByTarget(id target.TargetID) int
func (sm *SessionManager) CountByState(state State) int64
```

`RangeAll` is cold path (failover); RLock per shard acceptable.

---

### Step 3.12 — MaxFreezeDuration & Idle Eviction

```go
func (fc *FreezeController) scheduleFreezeTimeout(sess *session.ClientSession) {
    time.AfterFunc(fc.registry.cfg.MaxFreezeDuration, func() {
        if sess.GetState() == session.StateFrozen {
            fc.server.CloseSession(sess, closeReasonFreezeTimeout)
        }
    })
}
```

Idle eviction (Phase 1 `LastActivity`):

```go
func (r *Registry) evictionLoop(ctx context.Context) {
    // Every 60s, RangeAll sessions where now - LastActivity > IdleTimeout
}
```

---

## 4. Memory & Performance Bounds

| Constraint | Target | Notes |
|------------|--------|-------|
| Probe loop goroutines | 1 + `GOMAXPROCS` probe workers | No goroutine per target |
| `FreezeTarget` session scan | O(sessions) cold path | Acceptable on failure; not hot path |
| `SelectHealthy` | O(log n) ring walk | n = targets per route, typically < 100 |
| Passive error window | Fixed-size counter per target | No append/growth |
| Target cache | `sync.Map` | Read-mostly after discovery |
| Hot relay path | Unchanged from Phase 2 | Freeze/Thaw off hot path |
| Failover detection latency | ≤ 6s default (3 failures × 2s) | Passive path ≤ ErrorRateWindow |

### Failover Path Allocation Budget

Freeze/Thaw are cold paths. Allocation allowed but bounded:

- `RangeAll` iteration: 0 allocs if using indexed shard scan (no slice copy)
- `DialUpstream`: allocates (cold path, acceptable)
- Probe HTTP client: reuse `http.Client` with shared transport

**Prohibited:** Spawning unbounded goroutine per session on Freeze. Use `RangeAll` synchronous freeze in probe callback; batch in chunks of 256 if session count > 10k to avoid long pause (yield with `runtime.Gosched()` every 256 sessions).

---

## 5. Testing, Verification & Benchmarking Criteria

### 5.1 Unit Tests

| Test | Scenario |
|------|----------|
| `TestTargetStateMachine` | UNKNOWN→HEALTHY→UNHEALTHY→HEALTHY with thresholds |
| `TestRouterConsistentHash` | Same ClientID always maps to same target (stable ring) |
| `TestRouterSkipsUnhealthy` | Unhealthy target skipped; next healthy selected |
| `TestPassiveErrorRate` | 5/10 write errors → rate 0.5 → triggers Freeze |
| `TestFreezeCAS` | Only Active sessions frozen; Draining unaffected |
| `TestDrainTimeout` | Drain exceeds 30s → session returns to Frozen |
| `TestMaxFreezeDuration` | Session closed after 5m frozen |

### 5.2 Integration Tests (Chaos)

```go
func TestRollingUpdate(t *testing.T) {
    // 3 mock upstream pods (A, B, C), discovery emits A,B,C
    // 100 client sessions connected via Kervan
    // Kill pod A (SIGKILL on mock server)
    // Assert: all 100 client connections still open
    // Assert: sessions frozen, buffer accumulating client sends
    // Start pod D as replacement; discovery updates
    // Assert: thaw completes, buffered messages delivered FIFO
    // Assert: kervan_failover_total{freeze}=1, {thaw}=1
}

func TestMidDrainUpstreamKill(t *testing.T) {
    // 500 buffered messages, thaw starts
    // Kill new upstream after 250 flushed
    // Assert: session returns Frozen, head at message 251
    // Restart upstream, re-thaw
    // Assert: messages 251-500 delivered; 1-250 not duplicated at head

func TestPassiveFastPathFreeze(t *testing.T) {
    // Upstream accepts TCP but RST on every write
    // Assert: Freeze before probe interval × threshold

func TestOCPPCallOrdering(t *testing.T) {
    // WebSocket JSON CALL messages [1..20] during freeze
    // After thaw, upstream receives in order 1..20
}

func TestConcurrentFreezeThaw(t *testing.T) {
    // 10k sessions, freeze all, thaw all, -race clean
}
```

### 5.3 Benchmark Tests

```go
func BenchmarkRouterSelect(b *testing.B) {
    // 100 targets, random ClientIDs
    // GATE: 0 allocs/op
}

func BenchmarkPassiveRecordWrite(b *testing.B) {
    // GATE: 0 allocs/op
}
```

Failover scan (`RangeAll` + Freeze) is not benchmark-gated; measure separately:

```go
func BenchmarkFreezeTenKSessions(b *testing.B) {
    // Target: < 500ms to freeze 10k sessions (informative, not CI gate)
}
```

---

## 6. Definition of Done

- [ ] **TargetRegistry** manages target lifecycle with probe loop and discovery watch
- [ ] **Consistent-hash routing** selects healthy targets per `(route, ClientID)`
- [ ] **Active probes** (TCP, HTTP, WS) and **passive write-error** fast path operational
- [ ] **Freeze** transitions `Active → Frozen`, deregisters upstream fd, client connection stays open
- [ ] **Thaw** transitions `Frozen → Draining → Active`, drains ring buffer FIFO via FlushScheduler
- [ ] **Mid-drain failure** returns to `Frozen` with head entry preserved (at-least-once)
- [ ] **`MaxFreezeDuration`** and **idle eviction** enforced
- [ ] **Integration**: `TestRollingUpdate`, `TestMidDrainUpstreamKill`, `TestOCPPCallOrdering` pass
- [ ] **`BenchmarkRouterSelect`**: 0 allocs/op
- [ ] **Metrics**: `kervan_target_health`, `kervan_sessions_frozen`, `kervan_failover_total`
- [ ] **CoordinationStore** interface implemented with `NoopStore`; `PublishTargetHealth` called on state change
- [ ] **Kubernetes discovery** behind build tag `k8s`; static discovery works without K8s
- [ ] **Documentation**: failover timing parameters documented in example config
- [ ] **Race detector clean** on chaos integration tests

---

## Appendix — Phase 3 → Phase 4 Handoff

Phase 4 replaces `NoopStore` with etcd/Redis and subscribes to cross-node target health watches. Phase 3 must ensure:

```go
// All health transitions call:
store.PublishTargetHealth(ctx, targetID, state)

// Registry exposes:
func (r *Registry) ApplyRemoteHealth(id TargetID, state TargetState)
// Phase 4: called from store watch callback
```

Session leases attach at session creation in Phase 4 without changing Freeze/Thaw logic.

---

*Previous: [Phase 2 — Ring Buffer](./phase-2-ring-buffer.md) · Next: [Phase 4 — Distributed Cluster](./phase-4-distributed-cluster.md)*
