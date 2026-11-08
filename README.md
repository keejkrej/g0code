# g0code

A Go port of [T3 Code](https://github.com/pingdotgg/t3code), targeting [MyGo](https://mygo.egoist.dev/docs) for the desktop shell.

## Source pin

The orchestration port is based on **t3code `v0.1.7`**, commit:

```text
322f6544dfad0a3c8a0c8cbecf2c7f7874b14268
```

The pinned source is upstream `https://github.com/pingdotgg/t3code`, not the moving local `main` branch. Important references at that revision:

- `docs/orchestration-v2/`
- `packages/contracts/src/orchestrationV2.ts`
- `packages/provider-core/src/server/IdAllocator.ts`
- `packages/provider-core/src/server/ProviderAdapter.ts`
- `apps/server/src/orchestration-v2/Orchestrator.ts`
- `apps/server/src/orchestration-v2/ProjectionStore.ts`
- `apps/server/src/orchestration-v2/ProviderEventIngestor.ts`

## Orchestration package

`orchestration` is a UI-independent domain module. It does not import MyGo or a provider SDK. A later MyGo-bound application service can expose commands, snapshots, and event streams while provider and SQLite adapters satisfy the package seams.

Implemented in this first vertical slice:

- Typed app-owned IDs and t3code-compatible deterministic ID derivation/escaping.
- Threads, runs, attempts, execution nodes, subagents, provider sessions/threads/turns, messages, turn items, runtime requests, plans, checkpoints, transfers, and handoffs.
- Atomic event batches, monotonically increasing cursors, command receipts, duplicate replay, rejected receipts, and command-ID conflict checks.
- Deterministic projection folding and full replay from stored events.
- Thread creation/archive/delete, message dispatch, queued-run cancellation/release, and queue promotion.
- Provider normalization/ingestion with raw-event deduplication and optional active-attempt guards.
- Root-only run completion: child/subagent terminals update their own graph nodes without settling the parent run.
- Interrupt requests as post-commit effects; acknowledgement does not mark a run terminal.
- Live-only runtime-request responses as post-commit effects.
- Lazy thread forks with native-fork selection when capabilities/refs allow it, otherwise an auditable portable handoff.
- Provider switching with durable full/delta context handoff artifacts.
- A concurrency-safe in-memory `Store` adapter for development and replay tests.
- Read-only local CLI adapters for Codex, Claude Code, Pi, Agy (ACP), and Fx (ACP), with fixture-backed parity and opt-in live integration tests. See `orchestration/cli/README.md`.

### Public seams

- `Orchestrator.Dispatch`: accepts a command and returns committed events, a cursor, and new provider effects.
- `Orchestrator.Ingest`: normalizes provider lifecycle/entity events into the same durable log.
- `Orchestrator.Snapshot` / `List` / `Events`: snapshot-plus-cursor reads and event replay.
- `Store`: atomic persistence seam. Implementations must preserve `Commit` semantics.
- `ProviderResolver`: maps provider instances to drivers and capabilities.
- `Apply` / `Replay`: deterministic projection verification.

Provider effects are returned **only for a newly committed command**, not for receipt replays. Effect IDs are deterministic for executor deduplication. The in-memory adapter does not persist effects; before using this in production, add the durable effect outbox described by t3code's v2 docs or an equivalent committed handoff to the effect worker.

### Not ported yet

This is not yet the whole t3code server. The following remain separate adapters or later domain slices:

- Rich provider tool calls, approvals, subagent execution, active steering, and long-lived native provider sessions.
- Additional provider adapters beyond the initial Codex/Claude/Pi/Agy/Fx slice.
- SQLite repositories/migrations, websocket transport, and snapshot pagination.
- Filesystem checkpoint capture/restore and rollback execution.
- Delegated-task delivery, remote links, PR watchers, title generation, usage-limit recovery, and background-work reactors.
- The MyGo application, bound services, generated TypeScript client, and frontend.

## MCP server

A standalone stdio MCP server is available at `cmd/g0code-mcp`. It exposes provider capabilities, thread creation/read/list/send/wait/interrupt, delegated tasks, and pending-request responses through the live orchestrator.

```sh
go build -o /tmp/g0code-mcp ./cmd/g0code-mcp
/tmp/g0code-mcp -workspace /path/to/project -provider codex
```

See `orchestration/mcp/README.md` for tools and protocol verification. The Tailscale-first remote-device design is recorded in `docs/connect-architecture.md`.

## Development

MyGo requires Go 1.27 or later; this module declares Go 1.27. The orchestration package itself currently uses only the standard library.

```sh
go test ./...
go test -race ./...
go vet ./...
```
