# Kervan

[![License](https://img.shields.io/badge/license-TBD-lightgrey)](LICENSE)
[![Go Version](https://img.shields.io/badge/go-1.22+-blue.svg)](https://go.dev/)
[![Build Status](https://img.shields.io/badge/build-TBD-lightgrey)](TBD)
[![Release](https://img.shields.io/badge/release-v0.1.0--alpha-lightgrey)](TBD)

> A connection-resilient, stateful Layer 7 WebSocket/TCP proxy that keeps client sessions alive while buffering payloads during upstream backend failures.

**Kervan** (*caravan* in Turkish) is named after the caravanserais of the Silk Road—waystations where merchants sheltered valuable cargo until the route ahead was safe again. Kervan applies the same philosophy to network traffic: client payloads are held in a bounded in-memory buffer and delivered in strict FIFO order once a healthy backend is restored, without dropping the front-end connection.

---

## Status

> **Early development.** The project is in the design and planning phase. Application code has not been implemented yet. See [Implementation Plan](docs/implementation/README.md) for the phased roadmap.

---

## Features

- **Connection immutability** — Client-facing WebSocket/TCP sessions survive backend pod crashes, rolling updates, and network degradation
- **Bounded FIFO buffering** — Per-session ring buffers with configurable backpressure (`DropOldest`, `Reject`, `Block`)
- **At-least-once delivery** — Buffered payloads flush in order when upstream recovers
- **High concurrency** — Designed for 10,000+ concurrent stateful connections per node
- **Zero-allocation hot path** — Event-driven I/O with `sync.Pool` buffer reuse and no goroutine-per-connection
- **Horizontal scaling** — Shared-nothing architecture with external coordination (etcd / Redis Cluster)
- **Protocol support** — WebSocket (RFC 6455) and raw TCP byte-stream proxying

---

## Use Cases

| Domain | Scenario |
|--------|----------|
| **IoT Gateways** | Devices stay connected while backend services restart |
| **EV Charging (OCPP)** | Charger sessions persist through CSMS rolling updates |
| **Financial Tickers** | Market data clients maintain streams during backend failover |
| **Real-Time Chat** | User connections buffered while message brokers recover |

---

## Architecture

```
Clients ──▶ Kervan Node ──▶ Backend Pool
              │
              ├── NetPoller (epoll/kqueue)
              ├── SessionManager (sharded map)
              ├── Ring Buffer (per session)
              └── TargetRegistry (health + routing)
              │
              └── Coordination Store (etcd / Redis) — metadata only
```

For detailed architecture, see:

- [Technical Design Document](docs/technical-design-document.md)
- [Architectural Decision Records](docs/architectural-decision-records.md)
- [Implementation Plan](docs/implementation/README.md)

---

## Quick Start

TBD

```bash
# Installation and first run — coming in Phase 1
go install github.com/ysBayram/kervan/cmd/kervan@latest
kervan -config configs/kervan.yaml
```

---

## Installation

TBD

### Prerequisites

| Requirement | Version |
|-------------|---------|
| Go | 1.22+ |
| Linux / macOS | epoll (Linux) or kqueue (macOS/BSD) |
| Coordination store (multi-node) | etcd 3.5+ or Redis Cluster 7+ |

### From Source

TBD

```bash
git clone https://github.com/ysBayram/kervan.git
cd kervan
make build
./bin/kervan -config configs/kervan.example.yaml
```

### Docker

TBD

```bash
docker pull ghcr.io/ysBayram/kervan:latest   # TBD
docker run -p 8080:8080 -v $(pwd)/configs:/etc/kervan ghcr.io/ysBayram/kervan:latest
```

### Kubernetes / Helm

TBD — see [Phase 4 deployment manifests](docs/implementation/phase-4-distributed-cluster.md).

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
