# Kervan — Architectural Decision Records

**Version:** 0.1.0-draft  
**Status:** Source of Truth (Bootstrap)  
**Last Updated:** 2026-06-11  
**Authors:** Kervan Core Team

---

## Dedication — The Caravanserai Philosophy

Across the ancient Silk Road, trade did not flow in a single uninterrupted stream. Merchants traveled through deserts, mountain passes, and contested territories where storms, bandits, and collapsed bridges could sever a route at any moment. Yet the cargo—spices, silk, coin—could not be abandoned. **Caravanserais** (*kervansaray* in Turkish) were fortified waystations built at regular intervals along these routes: walled enclosures where a caravan could halt, shelter its goods, rest its animals, and wait until the path ahead was safe again.

**Kervan** draws its name from the caravan (*kervan*) and the sanctuary (*serai*) that protected it. In this system, each client payload is cargo of intrinsic value—an OCPP command, a financial tick, a chat message, an IoT telemetry frame. The upstream backend is the distant market; the network between them is unpredictable terrain. When a backend pod crashes, a rolling update severs the route, or latency spikes into degradation, a conventional reverse proxy drops the client connection and the cargo is lost.

Kervan refuses that trade-off. It is the caravanserai at the edge: a stateful, connection-resilient Layer 7 proxy that holds cargo in a bounded, zero-copy ring buffer until the route to the destination is restored, then flushes it in strict FIFO order with at-least-once delivery. The front-end connection—the bond between client and proxy—is sacred and must survive every storm the backend endures.

---

## Table of Contents

- [ADR Index](#adr-index)
- [ADR-001: Custom Go Proxy vs. Out-of-the-Box Proxies](#adr-001-custom-go-proxy-vs-out-of-the-box-proxies)
- [ADR-002: State and Consensus Management](#adr-002-state-and-consensus-management)
- [ADR-003: Memory Allocations and GC Pressure](#adr-003-memory-allocations-and-gc-pressure)
- [ADR Template (Future Records)](#adr-template-future-records)

---

## ADR Index

| ID | Title | Status | Date |
|----|-------|--------|------|
| [ADR-001](#adr-001-custom-go-proxy-vs-out-of-the-box-proxies) | Custom Go Proxy vs. Out-of-the-Box Proxies | Accepted | 2026-06-11 |
| [ADR-002](#adr-002-state-and-consensus-management) | State and Consensus Management | Accepted | 2026-06-11 |
| [ADR-003](#adr-003-memory-allocations-and-gc-pressure) | Memory Allocations and GC Pressure | Accepted | 2026-06-11 |

---

## ADR-001: Custom Go Proxy vs. Out-of-the-Box Proxies

| Metadata | Value |
|----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-11 |
| **Deciders** | Kervan Core Team |
| **Related** | TDD §3, §4.2, §6 |

### Context

Kervan's primary requirement is **connection immutability with inbound payload buffering**: when an upstream backend target becomes unavailable, the proxy must continue accepting client data, retain it in a bounded FIFO queue, and replay it once a healthy target is available—all without dropping the client-facing WebSocket or TCP session.

We evaluated mature, production-grade reverse proxies:

| Proxy | Strengths | Limitation for Kervan |
|-------|-----------|----------------------|
| **HAProxy** | Extreme L4/L7 performance, mature health checks, sticky sessions | Connection replay requires external stick-table + Lua; no native per-connection inbound message queue across upstream failure; WebSocket buffering is pass-through only |
| **Envoy** | Rich L7 routing, outlier detection, extensibility via WASM/Lua | Upstream disconnect triggers downstream reset unless complex fault-injection and retry policies are applied; retries are request-scoped (HTTP), not byte-stream/WebSocket frame accumulation across dead upstream sockets |
| **Nginx** | Ubiquitous, `proxy_read_timeout` tuning, upstream keepalive | On upstream failure, client connection is typically closed or returns 502; no built-in per-client ring buffer with FIFO drain to a *new* upstream socket on the same client session |

The fundamental gap across all three is **architectural**, not configurational:

1. **Connection model**: Standard reverse proxies model a **pair of coupled streams** (client ↔ proxy ↔ upstream). When the upstream leg breaks, the proxy's default obligation is to signal failure to the client and tear down the session. Keeping the client leg alive while the upstream leg is detached and later reattached to a different target is not a first-class lifecycle.

2. **Buffering semantics**: HAProxy and Nginx buffer *responses* for slow clients (reverse direction). Envoy's buffer filter operates on HTTP bodies with size limits tied to request lifecycle. None provide a **bounded, per-client, inbound FIFO queue** that accumulates during upstream outage and drains with at-least-once semantics to a replacement upstream on the *same* client socket.

3. **WebSocket / long-lived TCP**: These protocols assume hours-long sessions. Proxy retry mechanisms (HTTP 503 retry, Envoy route retry) operate on discrete requests, not interleaved WebSocket frames or undelimited TCP byte streams where message boundaries span multiple read calls.

4. **Stateful control**: Freeze/Thaw—atomically halting upstream writes, switching session state, and resuming drain—is application-level state machine logic that cannot be expressed declaratively in proxy config languages without brittle scripting.

Building on existing proxies would require embedding custom C/Lua/WASM modules that reimplement 80% of Kervan's core anyway, while fighting the proxy's default teardown semantics.

### Decision

**Build Kervan as a purpose-built, standalone Go binary** implementing a custom Layer 7 proxy with native:

- Per-session ring buffers with configurable backpressure
- Freeze/Thaw failover state machine
- NetPoller-driven I/O without goroutine-per-connection
- First-class WebSocket and raw TCP support

Go is selected for:

- Native concurrency primitives (`sync.Pool`, sharded maps, atomics)
- Excellent cross-compilation for edge gateways (ARM IoT, x86 cloud)
- Mature ecosystem for epoll/kqueue bindings (`golang.org/x/sys/unix`)
- Single static binary deployment aligned with Kubernetes sidecar/DaemonSet patterns

We will **not** fork or embed HAProxy/Envoy/Nginx as the data plane.

### Consequences

#### Positive

- Full control over session lifecycle (Active → Frozen → Draining → Active) without fighting upstream proxy defaults.
- Unified codebase for buffering, health, routing, and I/O optimization.
- Direct implementation of zero-allocation hot paths tuned for 10k+ sessions.
- Simpler operational model: one binary, one config schema, one metrics surface.

#### Negative

- We assume responsibility for protocol correctness (WebSocket framing, TCP edge cases) that mature proxies have hardened over decades.
- No free integration with existing service mesh control planes (Istio, Linkerd) beyond standard K8s Service discovery; mesh integration is a future adapter concern.
- Smaller community than Envoy; operators cannot leverage existing HAProxy/Envoy expertise for Kervan-specific buffering semantics.
- Must implement our own TLS, rate limiting, and WAF if needed (initially deferred to ingress).

#### Mitigations

- Extensive integration tests against OCPP 1.6J, RFC 6455 compliance suites, and chaos tests (upstream kill during active buffer drain).
- Prometheus metrics and OpenTelemetry hooks from day one for operability parity.
- Document clear deployment boundaries: Kervan behind ingress TLS termination, not replacing it.

---

## ADR-002: State and Consensus Management

| Metadata | Value |
|----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-11 |
| **Deciders** | Kervan Core Team |
| **Related** | TDD §4.1, §4.3, §6.3 |

### Context

Kervan nodes are horizontally scaled behind an L4 load balancer with connection affinity (ClientID / source IP sticky). Each node holds **hot session state**: open file descriptors, ring buffer contents, and flush cursors. We must decide:

1. How nodes coordinate without becoming a distributed bottleneck.
2. How to mitigate **split-brain** (two nodes believing they own the same ClientID).
3. Whether to embed consensus (Raft) or delegate to an external distributed store.

#### Option A: Shared-Nothing with External Coordination Store

- Session payload data remains **local to the node** that holds the client connection.
- An external store (etcd or Redis Cluster) holds **metadata only**: target health, session leases, routing epoch.
- Split-brain prevention via **distributed leases**: a node must hold a valid lease on `ClientID` to accept traffic for that session (used during reconnect/handoff scenarios).

#### Option B: Embedded Raft (e.g., HashiCorp raft, etcd raft library)

- Each Kervan node participates in a Raft cluster; session metadata (and optionally buffer spill) is replicated via log entries.
- Strong consistency for all coordination state without external dependency.

#### Option C: Fully Shared-Nothing, No Coordination Store

- Pure sticky LB; no cross-node awareness.
- Split-brain impossible during normal operation but also no failover if the owning node dies.

### Decision

Adopt **Option A: Shared-Nothing local session state with an External Distributed Store (etcd preferred; Redis Cluster acceptable)** for coordination metadata only.

**Payload bytes never leave the owning node** during normal operation. The coordination store provides:

| Key Space | Value | TTL |
|-----------|-------|-----|
| `kervan/targets/{id}` | Health, last probe, weight | 30s (refreshed) |
| `kervan/leases/{clientID}` | `{nodeID, epoch}` | Session lifetime + grace |
| `kervan/routing/epoch` | Global routing generation | Persistent |

**Split-brain mitigation**:

1. On session creation, node acquires lease `kervan/leases/{clientID}` with TTL = `IdleSessionTimeout + grace`.
2. Lease is refreshed on activity; released on clean shutdown.
3. If a second node receives traffic for a leased ClientID (LB misconfiguration), it rejects or redirects based on lease holder (configurable).
4. Routing epoch increments on topology change; nodes watch epoch and invalidate stale target bindings.

**Why not embedded Raft (Option B)**:

| Factor | Embedded Raft | External Store |
|--------|---------------|----------------|
| Operational complexity | Kervan operators must run and tune a Raft cluster *inside* the proxy fleet | Reuse existing etcd/Redis ops teams and tooling |
| Scaling independence | Raft cluster size caps throughput of *all* metadata writes; large Kervan fleets share one consensus group | Coordination store scales independently (etcd 3-node, Redis Cluster) |
| Blast radius | Bug in Kervan Raft integration corrupts consensus state | Store failure degrades discovery/leases; existing connections continue (shared-nothing payload path) |
| Memory / CPU | Each Kervan node runs Raft goroutines + log cache | Lean Kervan process; network RTT to store on cold paths only |
| Maturity | Raft library integration, snapshotting, membership changes are non-trivial | Battle-tested lease primitives (`etcd/concurrency`, Redis `SET NX EX`) |

Embedded Raft is justified when the system *must* replicate payload data or perform strongly-consistent leader election on every message. Kervan's latency budget and shared-nothing payload locality make that unnecessary for v0.1.

**Why not Option C alone**:

- No target health sharing across nodes (each probes independently—acceptable but wasteful).
- No foundation for future session handoff or cross-node drain.
- Insufficient for multi-region routing epoch coordination.

### Consequences

#### Positive

- Horizontal scaling of Kervan nodes is limited only by LB capacity and per-node session memory—not by consensus throughput.
- Payload path is entirely local: no network hop to store on every enqueue (microsecond budget preserved).
- Operators can use managed etcd (cloud provider) or existing Redis Cluster investments.
- Split-brain is addressable via lease semantics without custom consensus implementation.

#### Negative

- **External dependency**: etcd/Redis outage affects new session routing and target discovery; running sessions continue but failovers may be delayed.
- **Eventual consistency window**: Target health propagation has watch latency (~10–100ms etcd, ~1ms Redis pub/sub).
- **Not strongly consistent for payload**: Cross-node session migration (future) requires explicit handoff protocol, not automatic Raft replication.

#### Mitigations

- Local target health cache with passive fast-path (write error detection) reduces dependence on store watch latency during acute failures.
- Graceful degradation mode: if store unreachable, operate on last-known target set with elevated probe frequency; alert operators.
- Document RTO/RPO: node failure loses in-memory buffer (accepted for v0.1); durable WAL is future ADR scope.

---

## ADR-003: Memory Allocations and GC Pressure

| Metadata | Value |
|----------|-------|
| **Status** | Accepted |
| **Date** | 2026-06-11 |
| **Deciders** | Kervan Core Team |
| **Related** | TDD §4.2, §5, §7 |

### Context

At 10,000 concurrent sessions with sustained message rates (e.g., 100 msg/s per session for tickers = 1M msg/s aggregate), heap allocation per message would generate gigabytes of garbage per second. Go's concurrent GC would consume significant CPU and introduce pause latency spikes incompatible with financial and charging-station SLAs.

We evaluated three buffer lifecycle strategies:

| Strategy | Description |
|----------|-------------|
| **A. Allocate per message** | `make([]byte, n)` on each read; simplest, highest GC pressure |
| **B. sync.Pool size classes** | Power-of-two pools; acquire on read, release after flush ACK |
| **C. mmap ring buffer (single slab per session)** | One mmap'd region per session; bump-pointer within slab |

Additional consideration: **goroutine-per-connection** (common Go anti-pattern) allocates ~2–8 KB stack per goroutine plus scheduler overhead, scaling poorly beyond a few thousand connections.

### Decision

Adopt a **hybrid of B and C** with strict hot-path allocation rules:

1. **Ring buffer entries**: Pre-allocated slab of `RingEntry` structs at session creation (zero alloc per enqueue/dequeue metadata).

2. **Payload bytes**:
   - **Inline storage** for payloads ≤ 256 bytes (covers majority of IoT heartbeats, ticker deltas).
   - **sync.Pool size classes** (256 B → 16 KB) for larger frames; power-of-two rounding.
   - Pool `New` functions allocate once; steady-state hits pooled objects exclusively.

3. **No goroutine-per-connection**: Fixed NetPoller worker pool (`GOMAXPROCS`) handles all fd readiness via epoll/kqueue edge-triggered batching.

4. **Explicit buffer lifetime contract**:
   ```
   acquire() → enqueue → flush write → backend ACK → release()
   ```
   Buffers are **never** retained beyond flush ACK; no cross-session sharing.

5. **GC tuning**: Document recommended `GOMEMLIMIT` (e.g., 80% of container memory limit) and optional `GOGC=off` with ballast for production deployments exceeding 5k sessions.

**Rejected: pure mmap per session (Option C alone)** for all payload data because:

- Variable frame sizes create fragmentation within mmap slabs.
- Go slice semantics and GC scan of pointer-free mmap regions add complexity (`unsafe` + `syscall.Mmap` lifecycle management).
- Size-class pools provide 95%+ of the benefit with idiomatic Go.

**Rejected: allocate per message (Option A)** due to measured GC overhead: at 1M msg/s × 1 KB average, allocation rate exceeds 1 GB/s, driving GC CPU above 30% in prototype benchmarks.

### Consequences

#### Positive

- Steady-state hot path (healthy backend, inline-eligible frames) achieves **zero heap allocations per message**.
- GC pause p99 target ≤ 1 ms becomes achievable at 10k sessions with tuned pools and `GOMEMLIMIT`.
- `kervan_pool_hit_ratio` metric enables data-driven pool size tuning.
- Inline threshold eliminates pool contention for small messages (majority case in IoT/OCPP).

#### Negative

- **Pool poisoning risk**: If a buffer is returned to the pool while still referenced (use-after-free), data corruption occurs. Strict ownership transfer via lint rules and race detector CI are mandatory.
- **Memory overhead**: Pools retain memory under load drops (GC does not reclaim pooled objects). RSS may exceed active working set.
- **Size class internal fragmentation**: 513-byte payload uses 1 KB class (50% waste for that frame).
- **Complexity**: Developers must follow acquire/release discipline; violations are subtle bugs.

#### Mitigations

- Static analysis rule: flag `make([]byte` in `internal/proxy/` and `internal/buffer/` packages (exceptions require ADR amendment).
- Pool hit ratio alert if `< 0.90` over 5-minute window.
- Integration tests with `-race` and load tests with `GODEBUG=gctrace=1` in CI performance gate.
- Document size class tuning guide per tenant workload profile.

### Implementation Notes

```go
// Size class selection — branchless-friendly
func sizeClass(n int) int {
    if n <= 256  { return 0 }
    if n <= 512  { return 1 }
    if n <= 1024 { return 2 }
    // ...
    return maxClass
}

// Hot path invariant (documented, enforced in review)
// INVARIANT: buf is released exactly once, after flush ACK, by the consumer.
func (s *FlushScheduler) onAck(entry *RingEntry) {
    if entry.length > inlineThreshold {
        releaseBuffer(entry.buf())
    }
    s.advanceHead()
}
```

---

## ADR Template (Future Records)

Use this template for subsequent architectural decisions.

```markdown
## ADR-NNN: Title

| Metadata | Value |
|----------|-------|
| **Status** | Proposed / Accepted / Deprecated / Superseded by ADR-XXX |
| **Date** | YYYY-MM-DD |
| **Deciders** | |
| **Related** | |

### Context

[Describe the forces at play: technical, political, organizational. What is the issue that motivates this decision?]

### Decision

[Describe the decision and rationale. Use active voice: "We will ..."]

### Consequences

#### Positive
- 

#### Negative
- 

#### Mitigations
- 
```

---

## Document History

| Version | Date | Change |
|---------|------|--------|
| 0.1.0-draft | 2026-06-11 | Initial bootstrap ADRs 001–003 |

---

*These records constitute the authoritative architectural decision log for Kervan. New decisions must be appended with monotonically increasing ADR numbers. Superseded ADRs retain their history with updated status and cross-reference.*
