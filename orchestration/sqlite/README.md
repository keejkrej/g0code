# SQLite durability

`sqlite.Store` implements the orchestration `Store` and `OutboxStore` seams using `database/sql` and pinned `modernc.org/sqlite` (pure Go, no cgo).

## Durable tables

- `orchestration_events`: normalized event JSON, global monotonic sequence, app thread, command, event type, and raw-event correlation.
- `orchestration_receipts`: accepted/rejected command receipts and result cursors.
- `orchestration_effects`: provider effects, status, attempts, worker lease, errors, and timestamps.

`Commit` uses `BEGIN IMMEDIATE` so expected thread cursors, event batches, receipts, and effect enqueueing commit atomically. Multi-thread delegated-task batches use the same transaction. Projection validation runs before insert, and snapshots rebuild through the shared `orchestration.Replay` implementation.

## Effect recovery

Effects use at-least-once execution semantics:

```text
pending
  -> claimed running (worker ID + lease + attempts + 1)
  -> completed or failed

running + expired lease
  -> claimed by another worker
```

Only the current leaseholder can complete an effect. A failed effect remains `failed` with its error; automatic retry policy is a later slice. Provider adapters must use deterministic effect IDs and native lifecycle correlation to make replay safe.

`EffectWorker.RunOnce` claims and executes a batch; `Run` polls for new work. The MCP binary runs this worker instead of directly executing effects.

## Tests

The suite covers:

- Snapshot and event replay after close/reopen.
- Receipt replay without duplicate effects.
- Effect JSON type restoration.
- Claim, completion, and failure persistence.
- Expired-lease takeover and stale-worker rejection.
- Provider-turn completion through the worker after restart.
- Atomic parent/child delegation recovery.
