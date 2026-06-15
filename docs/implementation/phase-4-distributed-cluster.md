# Phase 4: Distributed State Store & Horizontal Cluster Topology

**Phase ID:** KERVAN-IMPL-004  
**Status:** Planned  
**Depends On:** Phase 1 (SessionManager, node identity), Phase 3 (TargetRegistry, CoordinationStore interface)  
**Blocks:** Production multi-node deployment  
**TDD Reference:** §3.1, §4.3.4, §9.3, §6.3  
**ADR Reference:** ADR-002 (Shared-Nothing + external store, session leases, routing epoch)

---

## 1. Objective & Scope

**Objective:** Integrate an external distributed coordination store (etcd primary; Redis Cluster secondary adapter), implement session lease management and routing epoch propagation, and deliver production-ready horizontal cluster topology with graceful degradation, split-brain mitigation, and Kubernetes deployment manifests.

### Included

- `etcd` coordination adapter via `go.etcd.io/etcd/client/v3`
- `redis` coordination adapter via `github.com/redis/go-redis/v9` (secondary, feature-parity subset)
- Session lease acquire/refresh/release on session lifecycle
- Split-brain rejection when duplicate ClientID lease held by another node
- Target health publish/subscribe across nodes
- Routing epoch increment on topology change; stale binding invalidation
- Node identity (`node_id`) registration and heartbeat
- Graceful degradation mode when store unreachable
- Helm chart / K8s manifests: Deployment, Service, ConfigMap, PodDisruptionBudget
- Load balancer affinity documentation (L4 sticky sessions)
- End-to-end multi-node integration test (3 Kervan nodes + etcd)
- Operational runbook sections: store outage, node drain, split-brain recovery

### Excluded

- Cross-node session migration / connection handoff (TDD §12 future work)
- Durable buffer replication across nodes
- Embedded Raft consensus
- Multi-region active-active
- Service mesh control plane integration (Istio/Linkerd adapters)

---

## 2. Target Directory & File Structure

```
kervan/
├── internal/
│   ├── coordination/
│   │   ├── store.go                  # CoordinationStore interface (from Phase 3)
│   │   ├── keys.go                   # Key space constants
│   │   ├── lease.go                  # Session lease manager
│   │   ├── epoch.go                  # Routing epoch watcher
│   │   ├── degrade.go                # Graceful degradation controller
│   │   └── mock/store_mock.go        # Test mock
│   ├── coordination/etcd/
│   │   ├── store.go                  # etcd implementation
│   │   ├── watch.go
│   │   └── store_test.go             //go:build integration
│   ├── coordination/redis/
│   │   ├── store.go                  # Redis Cluster implementation
│   │   └── store_test.go             //go:build integration
│   ├── node/
│   │   ├── identity.go               # NodeID, registration, heartbeat
│   │   └── identity_test.go
│   ├── session/
│   │   └── manager.go                # MODIFY: lease attach on Insert
│   ├── target/
│   │   └── registry.go               # MODIFY: watch remote health, epoch invalidation
│   └── proxy/
│       └── server.go                 # Wire coordination store, degradation mode
├── deploy/
│   ├── kubernetes/
│   │   ├── namespace.yaml
│   │   ├── configmap.yaml
│   │   ├── deployment.yaml
│   │   ├── service.yaml
│   │   ├── pdb.yaml
│   │   └── servicemonitor.yaml       # Prometheus Operator optional
│   └── helm/
│       └── kervan/
│           ├── Chart.yaml
│           ├── values.yaml
│           └── templates/
│               ├── deployment.yaml
│               ├── service.yaml
│               └── configmap.yaml
├── configs/
│   └── kervan.example.yaml           # ADD: coordination.* section
└── docs/
    └── operations/
        ├── cluster-topology.md
        ├── runbook-store-outage.md
        └── runbook-node-drain.md
```

### Key Space (Authoritative)

**`internal/coordination/keys.go`:**

```go
const (
    KeyPrefix           = "kervan/"
    KeyTarget           = KeyPrefix + "targets/"       // + {targetID}
    KeyLease            = KeyPrefix + "leases/"        // + {clientID}
    KeyRoutingEpoch     = KeyPrefix + "routing/epoch"
    KeyNode             = KeyPrefix + "nodes/"         // + {nodeID}
)

// KeyTarget value (JSON):
type TargetRecord struct {
    ID        string `json:"id"`
    Addr      string `json:"addr"`
    Route     string `json:"route"`
    State     uint32 `json:"state"`
    NodeID    string `json:"node_id"`    // publishing node (informational)
    UpdatedAt int64  `json:"updated_at"`
}

// KeyLease value (JSON):
type LeaseRecord struct {
    NodeID    string `json:"node_id"`
    Epoch     uint64 `json:"epoch"`
    CreatedAt int64  `json:"created_at"`
}

// KeyRoutingEpoch value: uint64 big-endian string
```

---

## 3. Step-by-Step Coding Roadmap

### Step 4.1 — Node Identity

**`internal/node/identity.go`:**

```go
type Identity struct {
    NodeID    string
    StartTime int64
    Addr      string // pod IP or configured advertise addr
}

func LoadIdentity(cfg config.Config) (*Identity, error) {
    // node_id from: cfg.NodeID || POD_NAME || hostname
}

func (id *Identity) Register(ctx context.Context, store coordination.CoordinationStore) error
func (id *Identity) HeartbeatLoop(ctx context.Context, store coordination.CoordinationStore, interval time.Duration)
```

**Heartbeat key:** `kervan/nodes/{nodeID}` with TTL 30s, refreshed every 10s. Value: JSON `{addr, sessions_active, started_at}`.

---

### Step 4.2 — etcd Store Adapter

**`internal/coordination/etcd/store.go`:**

```go
type EtcdStore struct {
    client *clientv3.Client
    lease  clientv3.Lease
    id     *node.Identity
}

func NewEtcdStore(endpoints []string, tls config.TLSConfig) (*EtcdStore, error)

func (s *EtcdStore) RegisterTarget(ctx context.Context, t target.Target) error {
    key := keys.KeyTarget + string(t.ID)
    val, _ := json.Marshal(targetRecordFrom(t, s.id.NodeID))
    _, err := s.client.Put(ctx, key, string(val), clientv3.WithLease(s.targetLeaseID))
    return err
}

func (s *EtcdStore) PublishTargetHealth(ctx context.Context, id target.TargetID, state target.TargetState) error

func (s *EtcdStore) WatchTargetHealth(ctx context.Context) (<-chan coordination.TargetEvent, error) {
    ch := make(chan coordination.TargetEvent, 256)
    go func() {
        rch := s.client.Watch(ctx, keys.KeyTarget, clientv3.WithPrefix())
        for wresp := range rch {
            for _, ev := range wresp.Events {
                ch <- parseTargetEvent(ev)
            }
        }
    }()
    return ch, nil
}
```

Use a **single etcd lease** per Kervan node for all target keys; refresh via keepalive goroutine.

---

### Step 4.3 — Session Lease Manager

**`internal/coordination/lease.go`:**

```go
type LeaseManager struct {
    store  coordination.CoordinationStore
    nodeID string
    cfg    LeaseConfig
}

type LeaseConfig struct {
    TTL          time.Duration // IdleSessionTimeout + 30s grace
    RefreshEvery time.Duration // TTL / 3
}

type SessionLease struct {
    clientID  string
    store     coordination.CoordinationStore
    leaseID   clientv3.LeaseID // etcd-specific; abstract in interface
    cancel    context.CancelFunc
}

func (lm *LeaseManager) Acquire(ctx context.Context, clientID string) (*SessionLease, error)
func (lm *LeaseManager) Refresh(ctx context.Context, sl *SessionLease) error
func (lm *LeaseManager) Release(sl *SessionLease) error
```

**Acquire algorithm (etcd):**

```go
func (lm *LeaseManager) Acquire(ctx context.Context, clientID string) (*SessionLease, error) {
    key := keys.KeyLease + clientID

    // Transaction: create if not exists
    txn := lm.client.Txn(ctx)
    txn.If(clientv3.Compare(clientv3.CreateRevision(key), "=", 0)).
        Then(clientv3.OpPut(key, leaseRecordJSON(lm.nodeID), clientv3.WithLease(leaseID))).
        Else(clientv3.OpGet(key))

    resp, err := txn.Commit()
    if err != nil { return nil, err }

    if !resp.Succeeded {
        // Key exists — check owner
        existing := parseLeaseRecord(resp.Responses[0].GetResponseRange().Kvs[0])
        if existing.NodeID != lm.nodeID {
            return nil, coordination.ErrLeaseHeldByOther
        }
        // Same node reconnect — reclaim (idempotent)
    }

    go lm.refreshLoop(sl)
    return sl, nil
}
```

**Redis alternative:** `SET kervan/leases/{clientID} {json} NX EX {ttl}`; refresh via `EXPIRE`; compare nodeID on conflict.

---

### Step 4.4 — Routing Epoch

**`internal/coordination/epoch.go`:**

```go
type EpochWatcher struct {
    store     coordination.CoordinationStore
    current   atomic.Uint64
    onChange  func(newEpoch uint64)
}

func (ew *EpochWatcher) Run(ctx context.Context) error {
    // Initial read of kervan/routing/epoch
    // Watch for changes
}

func (ew *EpochWatcher) Increment(ctx context.Context) (uint64, error) {
    // CAS increment on topology change (discovery adds/removes target)
}
```

**TargetRegistry integration:**

```go
func (r *Registry) onEpochChange(newEpoch uint64) {
    r.router.InvalidateAll()
    r.sessions.RangeAll(func(sess *session.ClientSession) bool {
        // Re-resolve target binding if session Active and not Frozen
        if sess.GetState() == session.StateActive {
            t, _ := r.SelectTarget(sess.Route, sess.ClientID)
            if t.ID != sess.TargetID {
                r.rebindSession(sess, t) // cold path: redial upstream
            }
        }
        return true
    })
}
```

**Epoch increment triggers:**

- Discovery emits target Added or Removed
- Manual admin API call (`POST /admin/bump-epoch`)

---

### Step 4.5 — Remote Health Watch Integration

**`internal/target/registry.go` modification:**

```go
func (r *Registry) watchRemoteHealth(ctx context.Context) {
    ch, err := r.store.WatchTargetHealth(ctx)
    for ev := range ch {
        switch ev.Type {
        case coordination.EventHealthChanged:
            if ev.Target.State == target.TargetUnhealthy {
                // Only freeze if we have sessions on this target AND we didn't originate the event
                r.ApplyRemoteHealth(ev.Target.ID, ev.Target.State)
            }
        }
    }
}

func (r *Registry) ApplyRemoteHealth(id target.TargetID, state target.TargetState) {
    local, ok := r.GetTarget(id)
    if !ok { return }
    local.CASState(local.GetState(), state)
    if state == target.TargetUnhealthy {
        r.freeze.FreezeTarget(id, reasonRemote)
    }
}
```

**Originator suppression:** Publish includes `node_id`; ignore watch events where `node_id == self` (already handled locally).

---

### Step 4.6 — Graceful Degradation

**`internal/coordination/degrade.go`:**

```go
type DegradeController struct {
    store       coordination.CoordinationStore
    registry    *target.Registry
    degraded    atomic.Bool
    lastKnown   sync.Map // TargetID → TargetRecord snapshot
}

func (dc *DegradeController) Run(ctx context.Context) {
    // Ping store every 5s
    // On 3 consecutive failures → enter degraded mode
    // On recovery → exit degraded, resync from store
}

func (dc *DegradeController) EnterDegraded() {
    dc.degraded.Store(true)
    dc.registry.SetProbeInterval(probeInterval / 2) // elevate local probing
    // Stop publishing (avoid stale writes); continue local passive health
}

func (dc *DegradeController) ExitDegraded(ctx context.Context) {
    dc.degraded.Store(false)
    dc.registry.SetProbeInterval(defaultProbeInterval)
    dc.resyncTargets(ctx)
}
```

**Behavior during degradation (per ADR-002):**

- Existing sessions and in-memory buffers: **unaffected**
- New session leases: **best-effort** (allow if store down + configurable `allow_leaseless_sessions: false` for strict mode)
- Target discovery: fall back to last-known snapshot + local probes
- Alert: `kervan_store_degraded = 1`

---

### Step 4.7 — Session Manager Lease Integration

**`internal/session/manager.go` modification:**

```go
func (sm *SessionManager) Insert(sess *ClientSession, lease *coordination.SessionLease) error {
    if sm.leaseMgr != nil {
        sl, err := sm.leaseMgr.Acquire(context.Background(), sess.ClientID)
        if errors.Is(err, coordination.ErrLeaseHeldByOther) {
            return ErrSplitBrain
        }
        if err != nil && !sm.cfg.AllowLeaselessOnDegrade {
            return err
        }
        sess.Lease = sl
    }
    // existing shard insert
}

func (sm *SessionManager) Remove(clientID string) {
    sess, ok := sm.remove(clientID)
    if ok && sess.Lease != nil {
        sm.leaseMgr.Release(sess.Lease)
    }
}
```

**Accept path rejects duplicate ClientID:**

```go
func (s *Server) acceptSession(...) error {
    if err := s.sessions.Insert(sess, nil); errors.Is(err, session.ErrSplitBrain) {
        conn.Close()
        metrics.SplitBrainRejected.Inc()
        return err
    }
}
```

---

### Step 4.8 — Redis Adapter (Secondary)

**`internal/coordination/redis/store.go`:**

```go
type RedisStore struct {
    client *redis.ClusterClient
    id     *node.Identity
}

// Feature parity subset:
// - PublishTargetHealth: SET with TTL + PUBLISH kervan:health channel
// - WatchTargetHealth: SUBSCRIBE kervan:health
// - AcquireSessionLease: SET NX EX
// - Routing epoch: INCR kervan/routing/epoch
```

Document latency characteristics: Redis pub/sub ~1ms vs etcd watch ~10–100ms. Recommend Redis for health fanout-heavy deployments; etcd for strong lease semantics.

---

### Step 4.9 — Configuration

```yaml
coordination:
  backend: etcd                    # etcd | redis
  etcd:
    endpoints:
      - "etcd-0.etcd:2379"
      - "etcd-1.etcd:2379"
      - "etcd-2.etcd:2379"
    dial_timeout: 5s
    tls:
      enabled: false
  redis:
    addrs:
      - "redis-cluster:6379"
    password: ""
  lease:
    ttl: 930s                      # idle_timeout + 30s
    refresh_every: 300s
  degrade:
    probe_interval: 5s
    failure_threshold: 3
  allow_leaseless_on_degrade: false

node_id: "${POD_NAME}"             # injected by downward API
```

---

### Step 4.10 — Kubernetes Deployment

**`deploy/kubernetes/deployment.yaml` highlights:**

```yaml
spec:
  replicas: 3
  template:
    spec:
      containers:
        - name: kervan
          resources:
            limits:
              memory: "24Gi"       # 10k sessions × ~2MB
          env:
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: GOMEMLIMIT
              value: "20GiB"
          securityContext:
            capabilities:
              add: ["NET_BIND_SERVICE"]
          # hostNetwork: true      # optional, for epoll at scale
```

**Service:** `sessionAffinity: ClientIP` or external LB with consistent hash on `X-Client-ID` (document both patterns in `cluster-topology.md`).

**PodDisruptionBudget:** `minAvailable: 2` for 3-replica deployment.

---

### Step 4.11 — Multi-Node Integration Test

**`internal/coordination/etcd/store_integration_test.go`:**

```go
//go:build integration

func TestThreeNodeClusterFailover(t *testing.T) {
    // Start etcd testcontainer
    // Start 3 Kervan Server instances (different node_id, different ports)
    // Client A → node 1, Client B → node 2
    // Kill backend target
    // Assert: both clients stay connected on their respective nodes
    // Assert: etcd shows target UNHEALTHY
    // Assert: node 3 watch receives health event (even without sessions on node 3)
}

func TestSplitBrainRejection(t *testing.T) {
    // Node 1 holds lease for ClientID "device-001"
    // Node 2 receives connection claiming "device-001"
    // Assert: node 2 rejects, node 1 session unaffected
}

func TestStoreDegradation(t *testing.T) {
    // Pause etcd container
    // Assert: kervan_store_degraded=1, existing sessions active
    // Assert: elevated probe rate
    // Unpause etcd, assert resync
}

func TestRoutingEpochBump(t *testing.T) {
    // Add target via discovery, increment epoch
    // Assert: active sessions rebind to new ring
}
```

---

## 4. Memory & Performance Bounds

| Constraint | Target | Notes |
|------------|--------|-------|
| Store operations on hot path | **Zero** | Leases refreshed on cold timer, not per message |
| Lease refresh goroutine | 1 per active session | Amortized: batch refresh if etcd supports multiple leases (prefer gRPC keepalive stream) |
| Watch callback | No heap alloc in parse | Pre-allocated JSON decode buffer or manual parse |
| Target health watch | Coalesce events within 50ms window | Avoid freeze storm on flapping target |
| etcd client connections | 1 gRPC conn per node | Shared across watch/put/txn |
| Degraded mode probe interval | 2× frequency | Temporary CPU increase acceptable |

### Lease Refresh Optimization

Batch session lease refresh to avoid N goroutines for N sessions:

```go
func (lm *LeaseManager) BatchRefresh(ctx context.Context, leases []*SessionLease) error {
    // etcd: single Lease.KeepAliveOnce with multiple lease IDs if supported
    // OR: refresh every 300s in groups of 100 via worker pool
}
```

**Target:** ≤ 1 goroutine for lease keepalive regardless of session count (use etcd lease keepalive stream per lease OR consolidate to fewer leases with key prefixes—evaluate in implementation; document chosen approach).

### Network RTT Budget (Cold Path)

| Operation | Frequency | RTT Impact |
|-----------|-----------|------------|
| Lease acquire | Once per session | ~1–5ms |
| Lease refresh | Every 300s | Amortized negligible |
| Target health publish | On state change | ~1–5ms |
| Epoch watch | Rare | N/A |

---

## 5. Testing, Verification & Benchmarking Criteria

### 5.1 Unit Tests

| Test | Scenario |
|------|----------|
| `TestKeyFormatting` | Key paths match spec |
| `TestLeaseRecordJSON` | Round-trip serialize/deserialize |
| `TestEpochIncrement` | Monotonic increase |
| `TestDegradeStateTransition` | 3 failures → degraded; recovery → normal |
| `TestOriginatorSuppression` | Ignore self-published health events |
| `TestRedisLeaseNX` | Second acquirer gets ErrLeaseHeldByOther |

### 5.2 Integration Tests

| Test | Scenario |
|------|----------|
| `TestThreeNodeClusterFailover` | Full multi-node failover |
| `TestSplitBrainRejection` | Duplicate ClientID rejected |
| `TestStoreDegradation` | etcd pause/resume |
| `TestRoutingEpochBump` | Rebind on topology change |
| `TestNodeHeartbeat` | Node key TTL refreshed; stale node evicted from registry |
| `TestCrossNodeHealthFanout` | Node A publishes unhealthy; Node B local cache updated |

### 5.3 Benchmark Tests

```go
func BenchmarkParseTargetEvent(b *testing.B) {
    // GATE: 0 allocs/op
}

func BenchmarkLeaseRecordSerialize(b *testing.B) {
    // GATE: 0 allocs/op with sync.Pool for JSON buffer
}
```

Store I/O benchmarks are informative only (network-bound):

```go
func BenchmarkEtcdPublishTargetHealth(b *testing.B) {
    // Informative: p99 < 10ms on LAN
}
```

### 5.4 Load Test (Manual / CI Nightly)

```bash
# 3 Kervan nodes, 10k sessions total (~3333 each)
# etcd 3-node cluster
# Kill 1 backend pod every 60s for 10 minutes
# Assert:
#   - client disconnect rate == 0
#   - split_brain_rejected_total == 0
#   - store_degraded == 0
#   - p99 flush latency < 500ms post-thaw
```

---

## 6. Definition of Done

- [ ] **etcd adapter** implements full `CoordinationStore` interface with integration tests
- [ ] **Redis adapter** implements lease + health publish/subscribe subset
- [ ] **Session leases** acquired on Insert, refreshed periodically, released on Remove
- [ ] **Split-brain rejection** when duplicate ClientID lease held by remote node
- [ ] **Target health** published on local state change; watched and applied on peer nodes
- [ ] **Routing epoch** increments on topology change; active sessions rebind
- [ ] **Graceful degradation** when store unreachable; existing sessions continue
- [ ] **Multi-node integration tests** pass with testcontainers (etcd + 3 Kervan instances)
- [ ] **K8s manifests** deploy 3-replica cluster with ConfigMap, Service, PDB
- [ ] **Helm chart** parameterizes replicas, resources, coordination backend
- [ ] **Operations docs**: cluster topology, store outage runbook, node drain runbook
- [ ] **Metrics**: `kervan_store_degraded`, `kervan_split_brain_rejected_total`, `kervan_routing_epoch`
- [ ] **Example config** documents all coordination settings
- [ ] **Load test report** (nightly or manual) attached to release artifact
- [ ] **Security review**: etcd/Redis TLS configuration documented; least-privilege key prefixes

---

## Appendix A — Cluster Topology Reference

```
                    ┌─────────────────┐
                    │  L4 Load Balancer│
                    │ (ClientIP sticky)│
                    └────────┬─────────┘
           ┌─────────────────┼─────────────────┐
           ▼                 ▼                 ▼
    ┌─────────────┐   ┌─────────────┐   ┌─────────────┐
    │ Kervan Pod 0│   │ Kervan Pod 1│   │ Kervan Pod 2│
    │ node_id: k0 │   │ node_id: k1 │   │ node_id: k2 │
    │ sessions:   │   │ sessions:   │   │ sessions:   │
    │  local only │   │  local only │   │  local only │
    └──────┬──────┘   └──────┬──────┘   └──────┬──────┘
           │                 │                 │
           └─────────────────┼─────────────────┘
                             ▼
                    ┌─────────────────┐
                    │  etcd Cluster   │
                    │ (metadata only) │
                    │ leases, health, │
                    │ routing epoch   │
                    └─────────────────┘
                             │
                             ▼
                    ┌─────────────────┐
                    │ Backend Pods    │
                    │ (ephemeral)     │
                    └─────────────────┘
```

**Shared-nothing invariant:** Payload bytes never traverse etcd. Only metadata crosses the coordination plane.

---

## Appendix B — Node Drain Procedure (Summary)

Document fully in `docs/operations/runbook-node-drain.md`:

1. Cordon: set node weight to 0 in discovery (stop new sessions via LB).
2. Wait for `kervan_sessions_active` → 0 on draining node (or timeout).
3. Release all session leases (clean etcd delete).
4. SIGTERM pod; verify lease keys removed.
5. Uncordon replacement node.

---

## Appendix C — v0.1 Release Criteria (All Phases)

| Criterion | Phase |
|-----------|-------|
| 10k concurrent connections, 0 allocs/op hot path | 1, 2 |
| Client survives backend rolling update | 3 |
| 3-node cluster with etcd, split-brain safe | 4 |
| p99 proxy overhead ≤ 500 µs steady-state | 1 |
| p99 GC pause ≤ 1 ms under load | 2 |
| Failover detection ≤ 6s | 3 |
| Store degradation non-fatal to existing sessions | 4 |

---

*Previous: [Phase 3 — Failover Routing](./phase-3-failover-routing.md) · [Implementation Plan Index](./README.md)*
