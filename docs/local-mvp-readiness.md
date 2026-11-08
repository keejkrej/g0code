# Local Mac MVP readiness

## Verdict

**Ready for local protocol experiments and text-only agent delegation. Not ready to be a daily coding-agent orchestrator.**

The current MCP server can run locally on this Mac, create durable threads, dispatch native provider turns, delegate child work, wait/read results, and recover pending effects. No GUI, Vercel deployment, Tailscale device, or cloud account is needed for that local workflow.

The hard blocker for a useful coding MVP is provider tool execution: the current adapters deliberately run agents with tools disabled or restricted to read-only text turns. A child can explain an answer, but cannot reliably inspect, edit, and test a repository through the orchestrated path.

## What works locally now

| Capability | Status | Evidence/limits |
| --- | --- | --- |
| Standalone MCP server | Ready for local use | `cmd/g0code-mcp`; stdio MCP initialize/list/call verified |
| Codex native integration | Text-only ready | Persistent app-server, thread resume, native interrupt; live two-turn test |
| Pi native integration | Text-only ready | Persistent RPC, prompt ACKs, abort, settled usage; live two-turn test |
| Agy/Fx native integration | Text-only ready | Managed ACP, resume/load, config options, session cancel; live two-turn tests |
| Claude native integration | Protocol ready, account limited | Streaming query path verified; local account currently returns durable `usage_limit` failure |
| Durable state | Ready for current model | SQLite events, receipts, and multi-thread commits; close/reopen tests |
| Pending provider effects | Ready for current model | Leased at-least-once outbox with stale-worker protection |
| Delegation | Text-only ready | Active-parent child creation, atomic lineage, result transfer, status/cancel tools |
| Thread debugging | Basic ready | Durable reads, recent runs, timeline items, pending-request tools, structured failures |
| Tool calls and approvals | **Not ready** | Rich provider frames are not normalized/executed; test adapters disable tools |
| Coding changes/tests by children | **Not ready** | No end-to-end repository mutation workflow |
| Active steering/restart | Not ready | Explicit steer/restart paths remain unimplemented |
| Checkpoints/rollback | Not ready | Graph records exist; filesystem capture/restore does not |
| MyGo GUI | Not implemented | No windows, bound application service, or frontend yet |
| Remote devices | Design only | Tailscale-first architecture documented; no daemon/control plane implementation |

## What “MVP ready” should mean

A local MVP is ready when this workflow succeeds repeatedly without fixture-only substitutions:

1. Start `g0code-mcp` once against a selected repository and persistent SQLite state.
2. Connect a real host agent (initially Codex or Pi) to that MCP server.
3. Ask the host to delegate an implementation task to another installed provider.
4. The child reads relevant files, makes a scoped edit, and runs a focused test inside the selected workspace.
5. Tool calls and required approvals appear as durable orchestration items and are handled through the normal runtime-request path.
6. The child returns a stable result; the host can read it and continue its own turn.
7. Stop/cancel works through the provider's native control operation.
8. Restarting the MCP server preserves history and safely recovers pending work without duplicating completed turns.

Current status: steps 1–3 have the infrastructure, but step 4 is blocked by the text-only provider tool policy. The remaining steps need an end-to-end host-agent test with real tool execution.

## Recommended next slice

Build **safe workspace tool execution + rich provider normalization**, then run the acceptance workflow on this Mac:

- Normalize command/file/tool lifecycle into execution nodes, turn items, and runtime requests.
- Enable provider-native read tools first, then workspace-scoped edits and commands under t3code-compatible runtime policies.
- Keep `approval-required` as the default for commands and writes during MVP testing.
- Persist approval decisions and provider callback outcomes.
- Add fixture parity for `tool_call_read_only`, `tool_call_denied_write`, `turn_interrupt_mid_tool`, and `subagent_v2` before enabling the workflow by default.
- Add one real host-to-child MCP integration test where a child edits a temporary repository and its test passes.

A MyGo GUI is not required for this CLI-first MVP. It should follow once the local orchestration workflow is reliable.
