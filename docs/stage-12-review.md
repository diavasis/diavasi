# Stage 12 review: observability and operational hardening

**Status:** complete. Stop for review before Stage 13.

## Delivered

- Owned Prometheus registry and `GET /metrics` (`text/plain; version=0.0.4`, bearer auth)
- Counters for fetch, deliver, ack, replay, bytes, restarts, disconnects, and adapter errors
- Scrape-time gauges for buffer, in-flight, consumer count, checkpoint lag, and `diavasi_groups_running`
- `GET /ready` (store read, no auth) beside existing `GET /health`
- `GET /v1/groups/{id}/diagnostics` and `diavasi group diagnostics`
- Process-local `last_stop_reason` and `recovered` on the supervisor
- `DIAVASI_LOG_FORMAT=text|json`, with `group_id` and `consumer_id` on fetch, ack, restart, and disconnect logs
- Debug spans `group_fetch` and `group_ack`, off at the default `info` filter
- [ADR 0013](adr/0013-observability.md), [observability notes](observability.md), [tutorial](tutorials/stage-12-observability.md)

## Guarantees verified

- `/health` and `/ready` answer without a token. `/metrics` is Prometheus text and reports `diavasi_groups_running`.
- After a pause, diagnostics reports `last_stop_reason=paused`, `recovered=false`, and the ack counter. The running gauge is 0.
- Aborting a group task respawns it, increments `diavasi_group_restarts_total`, records `task aborted`, and leaves the committed cursor at the start.
- A source that returns an error increments `diavasi_adapter_errors_total`, records that error text, then the supervisor respawns the group. The committed cursor does not move.
- A checkpoint write that fails before the transaction commits returns `SimulatedCrash(BeforeTxnCommit)`. Reopening the store shows the previous cursor.
- A consumer that holds a batch still fills the buffer only up to its cap. Checkpoint lag is buffer records plus in-flight records, and the gauges report that lag.

Disk-full is the same in-process store write failure. There is no overnight soak in CI.

## Intentionally not in Stage 12

- An OpenTelemetry exporter
- A durable stop reason (it is process-local; the checkpoint stays in the store)
- Reconciliation (Stage 13)

## Quality gate

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all --all-features
cargo deny check
```

`cargo deny check` reports advisories, bans, licenses, and sources ok. Adapter tests skipped where the matching URL was unset.

## Stop

Stage 12 is done. Do not start Stage 13 without an explicit instruction.
