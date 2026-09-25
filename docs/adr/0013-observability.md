# ADR 0013: Observability stays in-process

## Status

Accepted for Stage 12 onward.

## Context

`GET /metrics` was a stub. Operators could see lifecycle and the durable cursor, and they could not see fetch position, in-flight work, replay, or why a group task had exited. The architecture list names Prometheus counters and gauges at server, adapter, group, and consumer scope, plus structured logs and optional tracing.

## Decision

- One `prometheus` registry is owned by the process. It is not the crate's process-global default, so tests do not share series.
- `GET /metrics` requires the same bearer token as `/v1` and returns Prometheus text (`text/plain; version=0.0.4`).
- Counters increment on the event. Gauges for buffer, in-flight, consumer count, and checkpoint lag are replaced on each scrape from live group handles. A stopped group drops those series.
- `diavasi_group_checkpoint_lag` is the count of records in the buffer plus records assigned and not yet acked. Cursor tuples are not subtracted. That count is the same shape for synthetic ids, Postgres keys, Redis ids, and Scylla keys.
- `GET /health` stays liveness and does not touch the store. `GET /ready` reads the store and returns 503 when that read fails. Neither route requires a token.
- `GET /v1/groups/{id}/diagnostics` is the operator checklist. `last_stop_reason` and `recovered` live in the supervisor. They are cleared when the process exits. The durable lifecycle and checkpoint are unchanged and are not given a new store field.
- `diavasi serve` logs text by default. `DIAVASI_LOG_FORMAT=json` selects JSON lines. `RUST_LOG` defaults to `info`. Fetch and ack spans are `tracing` debug spans. This stage does not add an OpenTelemetry exporter. Raising the filter is how those spans appear.

## Consequences

A scrape after a group pauses no longer reports that group's buffer. Counters from the process lifetime remain. A restart of the process resets counters and forgets the stop reason. The store still has the last committed cursor.

Debug spans stay off at the default filter, so the hot path does not export traces. A later exporter can subscribe to the same spans.
