# AGENTS.md — Kervan

## Status

This repo starts as **design / planning phase** on `main`. Implementation lives on phase branches (`phase-1-core-engine` → `phase-4-distributed-cluster`). On `main` there are no `*.go` files yet — the authoritative design source remains `/docs/technical-design-document.md`. Do not write code on `main`.

## Authoritative docs

| File | What it covers |
|------|----------------|
| `docs/technical-design-document.md` | System spec: 4 pillars, data structures, failover lifecycle, perf targets |
| `docs/architectural-decision-records.md` | ADR-001 (custom Go proxy), ADR-002 (shared-nothing + coordination store), ADR-003 (zero-alloc hot path) |
| `docs/implementation/` | Phased roadmap: Phase 1 (Core) → 2 (Ring Buffer) → 3 (Failover) → 4 (Cluster) |

Cross-reference all docs before writing code — design decisions are scattered across them.

## Implementation branches

Each phase has a dedicated branch with micro commits and an implementation result:

| Branch | Base | Files | Results |
|--------|------|-------|---------|
| `phase-1-core-engine` | `main` | 27+ | `docs/implementation/phase-1-result.md` |
| `phase-2-ring-buffer` | `phase-1-core-engine` | 40+ | `docs/implementation/phase-2-result.md` |
| `phase-3-failover-routing` | `phase-2-ring-buffer` | 60+ | `docs/implementation/phase-3-result.md` |
| `phase-4-distributed-cluster` | `phase-3-failover-routing` | 70+ | `docs/implementation/phase-4-result.md` |

Branches are chained: each rebases on the prior phase. Use `git log` on a branch to see its micro-commit history before working.

## Planned stack

- **Language:** Go ≥1.22, module path `github.com/ysBayram/kervan`
- **WebSocket:** `github.com/gobwas/ws` (zero-copy), **not** gorilla/websocket
- **Config:** `gopkg.in/yaml.v3`
- **Platform I/O:** `golang.org/x/sys/unix` (epoll/kqueue)
- **Coordination:** `go.etcd.io/etcd/client/v3` or `github.com/redis/go-redis/v9` (Phase 4)
- **Linter:** `golangci-lint`
- **Layout:** `cmd/kervan/main.go` entrypoint, `internal/` private packages, `pkg/` public libs

## Hard architectural invariants (documented, not negotiable)

- **No goroutine-per-connection.** Use epoll/kqueue + fixed worker pool.
- **Single writer per session** — NetPoller worker owns the fd.
- **Hot path must benchmark to 0 allocs/op.** Verify with `go test -bench=. -benchmem`.
- **Session state transitions:** `atomic.Uint32` CAS only — never a mutex on hot path.
- **Payload bytes never leave the owning node.** Coordination store holds metadata only.
- **Performance targets:** 10k+ concurrent sessions, p99 latency overhead ≤500µs, failover detection ≤6s, GC pause p99 ≤1ms.

## Planned commands (will exist after Phase 1)

```bash
make build          # go build -o bin/kervan ./cmd/kervan
make test           # go test -race ./...
make bench          # go test -bench=. -benchmem  (allocation gates on CI)
make lint           # golangci-lint run
go fmt ./...
```

Use conventional commits per CONTRIBUTING.md. Branch from `main`.
