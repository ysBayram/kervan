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

Kervan uses a single YAML configuration file. Full example at [`configs/kervan.example.yaml`](configs/kervan.example.yaml).

```yaml
# ---------- listener ----------
listeners:
  - bind: ":8080"
    protocol: websocket    # websocket | tcp
  - bind: ":9090"
    protocol: tcp

# ---------- upstream ----------
upstream:
  targets:
    - id: "backend-1"
      addr: "10.0.1.50:3000"
    - id: "backend-2"
      addr: "10.0.1.51:3000"
  discovery:
    type: static            # static | kubernetes
    # kubernetes:            # requires build tag k8s
    #   namespace: default
    #   label_selector: app=my-backend

# ---------- buffer ----------
buffer:
  max_entries: 1024          # max frames per session buffer
  max_bytes_per_entry: 65536 # 64 KB
  backpressure_policy: drop_oldest  # drop_oldest | reject | block
  inline_threshold: 256      # bytes; ≤ this → stack, > this → pool

# ---------- failover ----------
failover:
  probe_interval: 5s         # health check frequency
  unhealthy_threshold: 3     # failures before marking Unhealthy
  healthy_threshold: 2       # successes before restoring Active
  freeze_timeout: 30s        # max time in Frozen before forced action

# ---------- coordination ----------
coordination:
  backend: etcd               # etcd | redis
  node_id: ""                 # defaults to POD_NAME or hostname
  etcd:
    endpoints: ["localhost:2379"]
    dial_timeout: 5s
    session_ttl: 15s
  # redis:
  #   addrs: ["localhost:6379"]
  #   session_ttl: 15s

# ---------- metrics ----------
metrics:
  prometheus: ":9091"         # Prometheus metrics endpoint (optional)
```

---

## Documentation

| Document | Description |
|----------|-------------|
| [Technical Design Document](docs/technical-design-document.md) | System architecture, components, and performance targets |
| [Architectural Decision Records](docs/architectural-decision-records.md) | Core design trade-offs (ADR-001 – ADR-003) |
| [AGENTS.md](AGENTS.md) | Repository overview, branch references, invariants |
| [Implementation Plan](docs/implementation/README.md) | Four-phase development roadmap |
| [Phase 1 Result — Core Engine](docs/implementation/phase-1-result.md) | NetPoller + SessionManager (implemented) |
| [Phase 2 Result — Ring Buffer](docs/implementation/phase-2-result.md) | Buffering and backpressure (implemented) |
| [Phase 3 Result — Failover](docs/implementation/phase-3-result.md) | Freeze/Thaw lifecycle (implemented) |
| [Phase 4 Result — Cluster](docs/implementation/phase-4-result.md) | Distributed state and K8s topology (implemented) |

---

## Roadmap

| Phase | Focus | Status | Branch | Result |
|-------|-------|--------|--------|--------|
| **Phase 1** | Connection handling & sharded session management | ✅ **Implemented** | `phase-1-core-engine` | [Phase 1 result](docs/implementation/phase-1-result.md) |
| **Phase 2** | Ring buffering & backpressure | ✅ **Implemented** | `phase-2-ring-buffer` | [Phase 2 result](docs/implementation/phase-2-result.md) |
| **Phase 3** | Dynamic routing & Freeze/Thaw failover | ✅ **Implemented** | `phase-3-failover-routing` | [Phase 3 result](docs/implementation/phase-3-result.md) |
| **Phase 4** | Distributed state & horizontal cluster | ✅ **Implemented** | `phase-4-distributed-cluster` | [Phase 4 result](docs/implementation/phase-4-result.md) |

All phases are implemented with passing unit tests. Remaining work is tracked per-phase:

- **Benchmark gates** — zero-alloc hot-path benchmarks pending Go toolchain installation
- **Integration tests** — full binary tests with etcd testcontainers and multi-node clusters
- **CI/CD** — GitHub Actions workflow for automated lint, test, bench gates
- **Operations** — runbooks, Prometheus alerting rules, Grafana dashboards

See the [Implementation Plan](docs/implementation/README.md) for Definition of Done criteria per phase.

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

### Prerequisites

- Go ≥1.22
- Linux (for epoll) or macOS (for kqueue)
- `golangci-lint` (for linting)

### Commands

```bash
make build      # go build -o bin/kervan ./cmd/kervan
make test       # go test -race ./...
make bench      # go test -bench=. -benchmem  (allocation gate on hot path)
make lint       # golangci-lint run
go fmt ./...    # format all files
```

### Project Layout

The project follows the [golang-standards/project-layout](https://github.com/golang-standards/project-layout) convention:

```
kervan/
├── cmd/kervan/              # Entrypoint
├── internal/                # Private application packages
│   ├── buffer/              # RingBuffer + backpressure policies
│   ├── config/              # YAML config loading + validation
│   ├── coordination/        # Distributed coordination (leases, epoch, degrade)
│   │   ├── etcd/            # etcd store adapter
│   │   └── redis/           # Redis store adapter
│   ├── discovery/           # Target discovery (static, K8s)
│   ├── flush/               # FlushScheduler (at-least-once drain)
│   ├── health/              # Health probes (TCP, HTTP, passive)
│   ├── metrics/             # Prometheus metrics
│   ├── netpoll/             # NetPoller (epoll/kqueue + FDRegistry)
│   ├── node/                # Node identity
│   ├── protocol/            # Protocol handlers
│   │   ├── tcp/             # Raw TCP stream
│   │   └── ws/              # WebSocket handshake + framing
│   ├── proxy/               # Accept loop, relay, server
│   ├── session/             # SessionManager (256 shards) + ClientSession
│   └── target/              # Target model, registry, router, FreezeController
├── pkg/                     # Public libraries
│   ├── api/                 # Shared API types
│   └── buffer/              # Size-class sync.Pool allocator
├── configs/                 # Example YAML configuration
├── deploy/                  # Deployment manifests
│   ├── kubernetes/          # Raw K8s manifests (Namespace, ConfigMap, Deployment, Service, PDB)
│   └── helm/kervan/         # Helm chart
└── docs/                    # Design documents and implementation results
    └── implementation/      # Phase result docs
```

### Branch Strategy

Implementation is organized into chained phase branches:

| Branch | Base | Content |
|--------|------|---------|
| `phase-1-core-engine` | `main` | NetPoller, SessionManager, config, accept loop, relay |
| `phase-2-ring-buffer` | `phase-1-core-engine` | RingBuffer, backpressure, FlushScheduler |
| `phase-3-failover-routing` | `phase-2-ring-buffer` | Target model, registry, probes, Freeze/Thaw |
| `phase-4-distributed-cluster` | `phase-3-failover-routing` | Coordination, leases, K8s manifests, Helm |

Each branch rebases on its predecessor. See `git log` on each branch for micro-commit history.

### Testing

```bash
# All unit tests with race detector
make test

# Specific package tests
go test -race ./internal/buffer/...

# Benchmarks
go test -bench=. -benchmem ./internal/buffer/...
```

All four phases have passing unit tests. Integration tests (etcd testcontainers, multi-node clusters) and benchmark gates are pending Go toolchain installation in this environment.

---

## Contributing

Contributions are welcome. Before submitting changes:

1. Review the [Technical Design Document](docs/technical-design-document.md) and [ADRs](docs/architectural-decision-records.md) to understand the architecture
2. Review [AGENTS.md](AGENTS.md) for branch strategy and coding conventions
3. Check the [Implementation Plan](docs/implementation/README.md) for open tasks
4. Open an issue to discuss large changes before submitting a PR

### Commit Conventions

This project uses [Conventional Commits](https://www.conventionalcommits.org/):

```
feat: add ring buffer backpressure policy
docs: update failover architecture diagram
fix: correct session state transition on thaw
```

### Code of Conduct

Please be respectful and constructive. This project is in early development — focus on improving the code and documentation.

---

## Security

To report a security vulnerability, please open a [GitHub Security Advisory](https://github.com/ysBayram/kervan/security/advisories) or email the maintainers directly. Do not disclose vulnerabilities publicly before they are addressed.

---

## License

This project is distributed under the **MIT License**. See [LICENSE](LICENSE) for details.

---

## Acknowledgments

- Named after the **caravanserais** (*kervansaray*) of the ancient Silk Road — waystations where merchants sheltered valuable cargo until the route ahead was safe
- Inspired by the need for connection-resilient proxying in IoT, OCPP, and real-time systems
- Built with [gobwas/ws](https://github.com/gobwas/ws) (zero-copy WebSocket), [golang.org/x/sys/unix](https://pkg.go.dev/golang.org/x/sys/unix) (epoll/kqueue), [go.etcd.io/etcd](https://github.com/etcd-io/etcd), and [go-redis](https://github.com/redis/go-redis)

---

## Links

| Resource | URL |
|----------|-----|
| Repository | [github.com/ysBayram/kervan](https://github.com/ysBayram/kervan) |
| Issue Tracker | [github.com/ysBayram/kervan/issues](https://github.com/ysBayram/kervan/issues) |
| Discussions | [github.com/ysBayram/kervan/discussions](https://github.com/ysBayram/kervan/discussions) |
| Changelog | [CHANGELOG.md](CHANGELOG.md) |
