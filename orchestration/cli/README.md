# Local CLI adapters

This package connects local coding-agent CLIs to `orchestration.Orchestrator` using post-commit effects and normalized provider events.

```text
Dispatch
  -> committed run graph + provider.turn.start effect
  -> cli.Executor.Execute
  -> native CLI frames
  -> Orchestrator.Ingest (guarded by run + active attempt)
  -> terminal root event
  -> completed/failed/interrupted run projection
```

The adapters support native coding tools and supervised runtime approvals in addition to text turns. Codex uses a persistent `codex app-server` JSON-RPC session, Claude Code uses the streaming Agent SDK query protocol (`stream-json` input and output), Pi uses a persistent `pi --mode rpc` session with a blocking permission extension, and Agy/Fx use managed ACP sessions over stdio. Agy and Fx share the ACP transport and normalizer.

## Fixture-backed parity

`testdata/t3code-v0.1.7/` contains scenario inputs, original assertion modules, and native recordings copied from t3code commit `322f6544dfad0a3c8a0c8cbecf2c7f7874b14268`.

`ReplayAdapter` and `SequenceAdapter` replace only process transport. Tests drive the same adapter frame handlers, executor, orchestrator, store, and projections as live turns. Native fake peers additionally consume the fixtures' `expect_outbound` operations, failing if Codex does not use app-server, Pi does not use RPC, Claude does not use streaming query input, or ACP diverges from the recorded request sequence.

| Pinned scenario | Providers | Ported oracle behavior |
| --- | --- | --- |
| `simple` | Codex, Claude, Pi, Fx, generic ACP | One completed run; root/assistant nodes; user/assistant messages and items; settled assistant text; semantic graph integrity; contiguous event sequences |
| `simple` Pi details | Pi | Exact recorded reasoning content, settled token usage/context window, and native user-entry turn correlation |
| `queued_turn` | Codex, Claude, Fx, generic ACP | Two completed runs; queued intake; queued-to-running-to-completed transitions; preserved `queued_turn` intent; conversation role order |
| `turn_interrupt` | Codex, Claude, Fx, generic ACP | Interrupted run/attempt/root; idle provider thread; linked request/result turn items |
| `multi_turn` + `provider_thread_resume` inputs | Live providers below | Two runs on one provider thread using the native resume ref |

These are semantic parity tests, not a claim of byte-for-byte equality for every internal row or coverage of every t3code fixture. Tool calls, approvals, subagents, checkpoints, steering, and provider-specific rich items still need their own fixture slices.

Run replay tests with the ordinary suite:

```sh
go test ./...
go test -race ./...
```

## Live verification

Live tests are doubly opt-in (build tag and environment), use temporary workspaces, disable tools, deny ACP permission callbacks, and never use bypass-permission flags:

```sh
G0CODE_LIVE=1 go test -tags=live ./orchestration/cli \
  -run '^TestLiveCLIOrchestration$' -count=1 -v
```

Verified with the installed versions:

| Provider | Version | Result |
| --- | --- | --- |
| Codex | `codex-cli 0.167.0` | Initial and resumed turns completed |
| Claude Code | `2.1.319` | Account weekly limit produced a durable `failed` run with `usage_limit` metadata, as expected |
| Pi | `1.5.0` | Initial and resumed turns completed |
| Agy via ACP | `1.5.3` | Initial and resumed turns completed |
| Fx via ACP | `0.0.16` | Initial and resumed turns completed |

A successful live case checks exact assistant text, native resume refs, two distinct messages per turn, graph integrity, and reuse of one provider thread. Claude's live usage-limit case is terminal and does not attempt a second charged request.

## Current adapter limits

- Persistent native sessions with native resume refs and provider-specific control operations.
- Text, reasoning, command, file-change, and dynamic-tool normalization; richer provider-specific items remain follow-up work.
- Interrupt uses Codex `turn/interrupt`, Claude SDK control requests, Pi `abort`, and ACP `session/cancel`.
- Provider model value `default` uses the CLI's configured model. Other values are passed through to the CLI.
- Effects use the orchestration durable outbox with at-least-once execution and lease recovery.
