# Kervan

[![License](https://img.shields.io/badge/license-TBD-lightgrey)](LICENSE)
[![Go Version](https://img.shields.io/badge/go-1.22+-blue.svg)](https://go.dev/)
[![Build Status](https://img.shields.io/badge/build-TBD-lightgrey)](TBD)
[![Release](https://img.shields.io/badge/release-v0.1.0--alpha-lightgrey)](TBD)

> A connection-resilient, stateful Layer 7 WebSocket/TCP proxy that keeps client sessions alive while buffering payloads during upstream backend failures.

**Kervan** (*caravan* in Turkish) is named after the caravanserais of the Silk Road—waystations where merchants sheltered valuable cargo until the route ahead was safe again. Kervan applies the same philosophy to network traffic: client payloads are held in a bounded in-memory buffer and delivered in strict FIFO order once a healthy backend is restored, without dropping the front-end connection.

---

## Why Kervan?

Stateful proxies fail in subtle ways. When an upstream backend restarts or crashes, most proxies either drop the client connection or discard in-flight payloads. For long-lived sessions (IoT device telemetry, EV charging transactions, financial market data feeds), a dropped connection means re-authentication, data loss, and cascading retry storms.

Kervan solves this by holding client connections open and buffering payloads in a bounded FIFO ring buffer while the backend recovers — then flushing in strict order once the upstream is healthy again. It is purpose-built for scenarios where **the session is more expensive than the data**.

## Status

> **Implementation complete.** All four phases (Core Engine → Ring Buffer → Failover Routing → Distributed Cluster) are implemented across dedicated branches with passing unit tests. See the [Implementation Plan](docs/implementation/README.md) for details and [AGENTS.md](AGENTS.md) for branch references.

---

## Features

- **Connection immutability** — Client-facing WebSocket/TCP sessions survive backend pod crashes, rolling updates, and network degradation. [Implemented: Phase 1 (passthrough), Phase 3 (Freeze/Thaw lifecycle)]
- **Bounded FIFO buffering** — Per-session ring buffers with configurable backpressure (`DropOldest`, `Reject`, `Block`), inline threshold (≤256 B), and size-class sync.Pool allocator (256 B – 64 KB). [Implemented: Phase 2]
- **At-least-once delivery** — Buffered payloads flush in strict FIFO order when upstream recovers. Mid-drain failure returns to Frozen state preserving delivery ordering. [Implemented: Phase 2–3]
- **High concurrency** — Sharded SessionManager (256 shards, FNV-1a hash), epoll/kqueue edge-triggered I/O, fixed worker pool (no goroutine-per-connection). Designed for 10,000+ concurrent stateful connections per node. [Implemented: Phase 1]
- **Zero-allocation hot path** — Event-driven I/O with sync.Pool buffer reuse, atomic.Uint32 state transitions, inline small frames avoiding heap. [Implemented: Phase 1–2]
- **Health-aware routing** — Consistent-hash router with active (TCP, HTTP) and passive (error rate) health probes; targets auto-evicted on failure, restored on recovery. [Implemented: Phase 3]
- **Freeze/Thaw failover** — Session state machine (Active → Frozen → Draining → Active) triggered by upstream health changes. No dropped front-end connections. [Implemented: Phase 3]
- **Horizontal scaling** — Shared-nothing architecture with external coordination (etcd / Redis Cluster), routing epoch watcher, session leases, and graceful degradation controller. [Implemented: Phase 4]
- **Protocol support** — WebSocket (RFC 6455) with zero-copy framing and raw TCP byte-stream proxying. [Implemented: Phase 1]
- **Kubernetes native** — Helm chart and K8s manifests (Deployment, Service with ClientIP affinity, PodDisruptionBudget). [Implemented: Phase 4]

---

## Who should use

Kervan is for teams operating stateful, long-lived TCP/WebSocket connections who cannot tolerate client reconnect storms or data loss during backend failures. It is **not a general-purpose HTTP reverse proxy** — it targets session-oriented protocols where the cost of re-establishment exceeds the cost of buffering.

| Role | Why Kervan |
|------|------------|
| **IoT Platform Engineers** | Thousands of devices reporting telemetry; a backend restart should not force mass reconnection |
| **EV Charging Operators** | OCCP sessions must survive CSMS rolling updates without losing transaction state |
| **Financial Data Providers** | Market data feeds must not drop clients during backend failover |
| **Real-Time Chat / Gaming** | User sessions persist through message broker restarts or blue/green deployments |
| **Telco / Edge Networking** | Signaling or media-plane flows that require carrier-grade session continuity |

## Use Cases

| Domain | Scenario | Relevant Kervan Feature |
|--------|----------|------------------------|
| **IoT Gateways** | Devices stay connected while backend services restart | Bounded FIFO buffering (Phase 2) + Freeze/Thaw failover (Phase 3) |
| **EV Charging (OCPP)** | Charger sessions persist through CSMS rolling updates | Connection immutability + at-least-once delivery |
| **Financial Tickers** | Market data clients maintain streams during backend failover | Consistent-hash routing + passive health probes |
| **Real-Time Chat** | User connections buffered while message brokers recover | Freeze/Thaw lifecycle + FlushScheduler drain |
| **Multi-region HA** | Active-active clusters survive region-level outages | Distributed coordination (Phase 4) + session leases |

---

## Architecture

### High-Level Topology

```
                          ┌─────────────────────────────────────────────────────┐
                          │                   Clients                          │
                          │  (IoT devices, OCPP chargers, browsers, apps)      │
                          └──────────┬──────────┬────────────────────────┬──────┘
                                     │          │                        │
                          TCP / WebSocket       │                        │
                                     │          │                        │
               ┌─────────────────────▼──────────▼────────────────────────▼──────┐
               │                     Kervan Cluster                            │
               │                                                                │
               │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐        │
               │  │   Kervan     │  │   Kervan     │  │   Kervan     │   ...   │
               │  │   Node 1     │  │   Node 2     │  │   Node 3     │         │
               │  │              │  │              │  │              │         │
               │  │ ┌──────────┐ │  │ ┌──────────┐ │  │ ┌──────────┐ │         │
               │  │ │ Session  │ │  │ │ Session  │ │  │ │ Session  │ │         │
               │  │ │ Manager  │ │  │ │ Manager  │ │  │ │ Manager  │ │         │
               │  │ │ (256 shd)│ │  │ │ (256 shd)│ │  │ │ (256 shd)│ │         │
               │  │ ├──────────┤ │  │ ├──────────┤ │  │ ├──────────┤ │         │
               │  │ │ RingBuf  │ │  │ │ RingBuf  │ │  │ │ RingBuf  │ │         │
               │  │ │ per sess │ │  │ │ per sess │ │  │ │ per sess │ │         │
               │  │ ├──────────┤ │  │ ├──────────┤ │  │ ├──────────┤ │         │
               │  │ │ Target   │ │  │ │ Target   │ │  │ │ Target   │ │         │
               │  │ │ Registry │ │  │ │ Registry │ │  │ │ Registry │ │         │
               │  │ └──────────┘ │  │ └──────────┘ │  │ └──────────┘ │         │
               │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘         │
               │         │                 │                 │                 │
               │         └─────────────────┼─────────────────┘                 │
               │                           │                                   │
               │              ┌────────────▼────────────┐                      │
               │              │  Coordination Store     │                      │
               │              │  (etcd / Redis Cluster)  │                      │
               │              │  metadata only, no data  │                      │
               │              └─────────────────────────┘                      │
               └───────────────────────────────────────────────────────────────┘
                                    │
                          ┌─────────┴──────────┐
                          │   Backend Pool     │
                          │  (upstream services)│
                          └────────────────────┘
```

### Node Internal Architecture

```
┌──────────────────────────────────────────────┐
│               Kervan Node                    │
│                                              │
│  ┌──────────────────────────────────────┐    │
│  │           NetPoller                  │    │
│  │  (epoll on Linux / kqueue on macOS)  │    │
│  │  - FDRegistry                        │    │
│  │  - Fixed worker pool (no goroutine   │    │
│  │    per connection)                   │    │
│  └──────────────┬───────────────────────┘    │
│                 │                             │
│  ┌──────────────▼───────────────────────┐    │
│  │         Accept Loop                 │    │
│  │  TCP / WebSocket listener           │    │
│  └──────────────┬───────────────────────┘    │
│                 │                             │
│  ┌──────────────▼───────────────────────┐    │
│  │       SessionManager (256 shards)    │    │
│  │  - ClientSession per connection      │    │
│  │  - Atomic state (Created, Active,    │    │
│  │    Frozen, Draining, Closed)         │    │
│  │  - Each session has a RingBuffer     │    │
│  └──────────────┬───────────────────────┘    │
│                 │                             │
│  ┌──────────────▼───────────────────────┐    │
│  │       Relay / Dispatch              │    │
│  │  - Active: passthrough to upstream   │    │
│  │  - Frozen: buffer to RingBuffer      │    │
│  │  - Draining: flush + passthrough     │    │
│  └──────────────┬───────────────────────┘    │
│                 │                             │
│  ┌──────────────▼───────────────────────┐    │
│  │        TargetRegistry               │    │
│  │  - Consistent-hash router           │    │
│  │  - Health probes (TCP, HTTP, pass.) │    │
│  │  - Discovery (static / K8s)         │    │
│  │  - FreezeController (Freeze/Thaw)   │    │
│  └──────────────┬───────────────────────┘    │
│                 │                             │
│  ┌──────────────▼───────────────────────┐    │
│  │     Coordination Layer (multi-node)  │    │
│  │  - Session lease manager             │    │
│  │  - Routing epoch watcher             │    │
│  │  - Graceful degradation controller   │    │
│  │  - etcd / Redis adapters             │    │
│  └──────────────────────────────────────┘    │
│                                              │
│  ┌──────────────────────────────────────┐    │
│  │     FlushScheduler                   │    │
│  │  - At-least-once drain per session   │    │
│  │  - ACK tracking                      │    │
│  └──────────────────────────────────────┘    │
│                                              │
│  ┌──────────────────────────────────────┐    │
│  │     Metrics                          │    │
│  │  - Buffer depth, drops, flush totals │    │
│  │  - Session count by state            │    │
│  │  - Target health, routing epoch      │    │
│  └──────────────────────────────────────┘    │
└──────────────────────────────────────────────┘
```

### Design Principles

| Principle | Implementation |
|-----------|---------------|
| **No goroutine-per-connection** | NetPoller owns all fds, fixed worker pool dispatches events |
| **Single writer per session** | Poll worker owns the fd exclusively |
| **Zero-alloc hot path** | sync.Pool buffer reuse, inline small frames, atomic CAS |
| **Payload stays local** | Ring buffer data never leaves the owning node |
| **Metadata only in store** | Coordination holds leases, epochs, target health — not payload bytes |
| **Shared-nothing** | Each node owns its sessions independently; coordination for discovery |

For detailed architecture, see:

- [Technical Design Document](docs/technical-design-document.md)
- [Architectural Decision Records](docs/architectural-decision-records.md)
- [Implementation Plan](docs/implementation/README.md)

---

## Quick Start

### 1. Clone and build

```bash
git clone https://github.com/ysBayram/kervan.git
cd kervan
make build                   # produces ./bin/kervan
```

### 2. Configure

Copy and edit the example config:

```bash
cp configs/kervan.example.yaml kervan.yaml
# edit listeners, upstream targets, buffer settings
```

### 3. Run

```bash
./bin/kervan -config kervan.yaml
```

A single-node instance listens on the configured ports and begins proxying to the defined upstream targets. When an upstream becomes unhealthy, sessions switch to buffering mode automatically.

---

## Installation

### Prerequisites

| Requirement | Version |
|-------------|---------|
| Go | 1.22+ |
| Linux / macOS | epoll (Linux) or kqueue (macOS/BSD) |
| Coordination store (multi-node) | etcd 3.5+ or Redis Cluster 7+ |

### From Source

```bash
git clone https://github.com/ysBayram/kervan.git
cd kervan
make build
./bin/kervan -config configs/kervan.example.yaml
```

Make targets:

| Target | Description |
|--------|-------------|
| `make build` | Compile `./bin/kervan` |
| `make test`  | Run all unit tests with race detector |
| `make bench` | Run benchmarks (allocation gate on hot path) |
| `make lint`  | Run `golangci-lint` |
| `go fmt ./...` | Format all Go files |

### Docker

> Docker images are not yet published. Build locally:

```bash
docker build -t kervan:latest .
docker run -p 8080:8080 -v $(pwd)/configs:/etc/kervan kervan:latest -config /etc/kervan/kervan.yaml
```

### Kubernetes / Helm

Two deployment options:

**Option 1 — Raw manifests:**

```bash
kubectl apply -f deploy/kubernetes/namespace.yaml
kubectl apply -f deploy/kubernetes/configmap.yaml
kubectl apply -f deploy/kubernetes/deployment.yaml
kubectl apply -f deploy/kubernetes/service.yaml
kubectl apply -f deploy/kubernetes/pdb.yaml
```

**Option 2 — Helm chart:**

```bash
helm install kervan ./deploy/helm/kervan -f values.yaml
```

The deployment starts 3 replicas with ClientIP session affinity, a PodDisruptionBudget ensuring minimum 2 available replicas, and coordination via etcd or Redis Cluster.

---

## Configuration

TBD

Example configuration will live at `configs/kervan.example.yaml`. Key sections:

```yaml
listeners:
  - bind: ":8080"
    protocol: websocket

upstream:
  targets: []          # static or discovery-driven

buffer:
  max_entries: 1024
  backpressure_policy: drop_oldest

coordination:
  backend: etcd
  etcd:
    endpoints: ["localhost:2379"]
```

---

## Documentation

| Document | Description |
|----------|-------------|
| [Technical Design Document](docs/technical-design-document.md) | System architecture, components, and performance targets |
| [Architectural Decision Records](docs/architectural-decision-records.md) | Core design trade-offs (ADR-001 – ADR-003) |
| [Implementation Plan](docs/implementation/README.md) | Four-phase development roadmap |
| [Phase 1 — Core Engine](docs/implementation/phase-1-core-engine.md) | NetPoller + SessionManager |
| [Phase 2 — Ring Buffer](docs/implementation/phase-2-ring-buffer.md) | Buffering and backpressure |
| [Phase 3 — Failover](docs/implementation/phase-3-failover-routing.md) | Freeze/Thaw lifecycle |
| [Phase 4 — Cluster](docs/implementation/phase-4-distributed-cluster.md) | Distributed state and K8s topology |

---

## Roadmap

| Phase | Focus | Status |
|-------|-------|--------|
| **Phase 1** | High-performance connection handling & sharded session management | TBD |
| **Phase 2** | In-memory ring buffering & backpressure | TBD |
| **Phase 3** | Dynamic upstream routing & Freeze/Thaw failover | TBD |
| **Phase 4** | Distributed state store & horizontal cluster | TBD |

See the full [Implementation Plan](docs/implementation/README.md) for Definition of Done criteria per phase.

---

## Performance Targets

| Metric | Target (single node, 16 vCPU) |
|--------|-------------------------------|
| Concurrent sessions | ≥ 10,000 |
| Steady-state p99 latency overhead | ≤ 500 µs |
| Failover detection | ≤ 6 s |
| GC pause p99 | ≤ 1 ms |
| Hot-path allocations | 0 allocs/op |

---

## Development

TBD

```bash
make build      # TBD
make test       # TBD
make bench      # TBD — allocation gate on hot-path benchmarks
make lint       # TBD
```

### Project Layout

TBD — planned structure follows [golang-standards/project-layout](https://github.com/golang-standards/project-layout):

```
kervan/
├── cmd/kervan/          # Entrypoint
├── internal/            # Private application code
│   ├── netpoll/
│   ├── session/
│   ├── buffer/
│   ├── target/
│   └── proxy/
├── pkg/                 # Public libraries
├── configs/             # Example configuration
└── docs/                # Design documents
```

---

## Contributing

TBD

Contributions are welcome once Phase 1 development begins. Until then:

1. Review the [Technical Design Document](docs/technical-design-document.md) and [ADRs](docs/architectural-decision-records.md)
2. Pick a task from the [Implementation Plan](docs/implementation/README.md)
3. Open an issue to discuss before submitting large changes

### Code of Conduct

TBD

---

## Security

TBD

To report a security vulnerability, please see [SECURITY.md](SECURITY.md) (TBD).

---

## License

TBD

This project will be released under an open-source license. See [LICENSE](LICENSE) for details once published.

---

## Acknowledgments

- Named after the **caravanserais** (*kervansaray*) of the ancient Silk Road
- Inspired by the need for connection-resilient proxying in IoT, OCPP, and real-time systems

---

## Links

| Resource | URL |
|----------|-----|
| Repository | TBD |
| Issue Tracker | TBD |
| Discussions | TBD |
| Changelog | [CHANGELOG.md](CHANGELOG.md) (TBD) |
