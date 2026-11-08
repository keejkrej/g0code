# Local Mac MVP readiness

## Verdict

**Ready for a supervised local trial on this Mac.** The native MyGo app can create threads, run coding agents, delegate child work, expose tool activity and approvals, publish child results, and stop active work. SQLite preserves events, receipts, and provider effects.

This is an early CLI-first/desktop MVP, not a complete t3code port or an unattended-production system. The initial providers are Codex, Claude Code, Pi, Agy, and Fx. Codex has the strongest live coding coverage. Provider-specific features and failure modes beyond the tested paths should still be treated as experimental.

## Working locally

| Capability | Status | Evidence/limits |
| --- | --- | --- |
| MyGo desktop app | Ready for local trial | Thread sidebar, timeline, composer, provider/model/runtime controls, delegation, stop, and parent/child approvals |
| Provider/model selection | Working native catalogs | Concrete default model, reasoning/effort, service tier, thinking, and fast mode where supported |
| Approval-required, auto-accept-edits, auto, full-access modes. | Explicit runtime ceilings | `full-access` remains dangerous by design |
| Standalone MCP server | Ready for local use | `cmd/g0code-mcp`; stdio MCP initialize/list/call verified |
| Codex native integration | Working coding path | Persistent app-server, thread resume, native tool events/approvals/interrupt; live edit and delegated edit tests |
| Pi native integration | Working coding path | Persistent RPC, native tools, blocking permission extension, abort, settled usage |
| Agy/Fx native integration | Working coding path | Managed ACP, tool lifecycle, permission options, resume/load, session cancel |
| Claude native integration | Protocol ready, account limited | Streaming query tools/control requests verified; local account currently returns durable `usage_limit` failure |
| Durable state | Ready for current model | SQLite events, receipts, and multi-thread commits; close/reopen tests |
| Pending provider effects | Ready for current model | Leased at-least-once outbox, heartbeat renewal, stale-worker protection |
| Delegation | Working local path | Active-parent child creation, atomic lineage, result transfer, status/cancel tools, live child edit |
| Tool calls and approvals | Working tested paths | Command/file/dynamic tool nodes and items, native accept/decline responses, denied-write regression |
| Active steering/restart | Not implemented | Queueing works; explicit steer/restart remains a later slice |
| Checkpoints/rollback | Not implemented | Graph records exist; filesystem capture/restore does not |
| Complete provider parity | Not claimed | Rich provider-specific plans, subagents, compaction, background work, and attachments need more fixtures |
| Remote devices | Design only | Tailscale-first architecture documented; no daemon/control plane implementation |

## Verified acceptance workflow

The following real-provider paths have run on this Mac:

1. **Native tool turn:** Codex created and read `result.txt` in a temporary workspace, producing `G0CODE_TOOLS_OK`.
2. **Desktop turn:** MyGo's New thread/Send actions drove Codex to create and verify `G0CODE_DESKTOP_OK`.
3. **Delegated desktop turn:** an active parent delegated a child task; the child created and verified `child-result.txt` as `G0CODE_CHILD_OK`; the parent published its result and was interrupted through native Codex control.
4. **Approval round trips:** a native Codex command was accepted through a runtime request, and an Fx write was declined and projected as failed without changing the original tool semantics.
5. **Durability:** provider turns and parent/child delegation survived close/reopen; pending effects were claimed after restart and long-running claims renewed without duplicate execution.

The app therefore crosses the original blocker: orchestrated agents can inspect, edit, and test a workspace under runtime policy rather than only returning text.

## Run the desktop MVP

```sh
go tool mygo dev
```

Or build a packaged app:

```sh
go tool mygo build
```

The default state is `~/.g0code/orchestration.sqlite`. New threads default to `auto-accept-edits`: workspace edits are allowed, while commands use the provider's supervised approval path. Use `approval-required` for narrower supervision. `full-access` is explicit and should be reserved for trusted workspaces.

## Run the MCP interface

```sh
go build -o /tmp/g0code-mcp ./cmd/g0code-mcp
/tmp/g0code-mcp -workspace /path/to/project -provider codex
```

## Next hardening work

- Active steering and interrupt/restart attempts.
- More live Pi/Agy/Fx/Claude coding tasks and failure/retry scenarios.
- Rich plans, attachments, background work, native subagents, and compaction parity.
- Filesystem checkpoints and rollback.
- Automatic authenticated MCP injection into provider sessions for fully autonomous host-to-host delegation.
- Production GUI packaging/signing and the optional Tailscale device daemon/control plane.
