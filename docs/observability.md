# Observability

Stage 12 makes a running server explainable. The control plane already had liveness, status, and the durable checkpoint. This page is the metrics, readiness, and diagnostics surface. Decision record: [ADR 0013](adr/0013-observability.md).

## Health and readiness

| Route | Auth | Meaning |
| --- | --- | --- |
| `GET /health` | none | The process is up. Body `ok`. |
| `GET /ready` | none | The store can be read. Body `ok`, or `not ready` with HTTP 503. |
| `GET /metrics` | bearer | Prometheus text, `text/plain; version=0.0.4`. |
| `GET /v1/groups/{id}/diagnostics` | bearer | One group's position, lag, counters, and last stop. |

`/health` does not read the store. `/ready` does. A process can be alive and not ready.

## Metrics

Names use a `diavasi_` prefix. Counters move when the event happens. Gauges are filled when `/metrics` is scraped, from groups whose runtime is up. A paused group disappears from the gauge series. Its counters stay until the process exits.

Labels `group_id` and `adapter` are on the record counters. `adapter` is `synthetic` when the group has no connection, otherwise the connection kind (`postgres`, `mongodb`, `redis`, `scylla`). `consumer_id` is only on disconnects.

| Metric | Kind | Labels |
| --- | --- | --- |
| `diavasi_groups_running` | gauge | none |
| `diavasi_group_records_fetched_total` | counter | `group_id`, `adapter` |
| `diavasi_group_records_delivered_total` | counter | `group_id`, `adapter` |
| `diavasi_group_records_acked_total` | counter | `group_id`, `adapter` |
| `diavasi_group_records_replayed_total` | counter | `group_id`, `adapter` |
| `diavasi_group_bytes_total` | counter | `group_id`, `adapter` |
| `diavasi_group_buffer_records` | gauge | `group_id` |
| `diavasi_group_buffer_bytes` | gauge | `group_id` |
| `diavasi_group_inflight_records` | gauge | `group_id` |
| `diavasi_group_checkpoint_lag` | gauge | `group_id` |
| `diavasi_consumer_count` | gauge | `group_id` |
| `diavasi_group_fetch_latency_seconds` | histogram | `group_id`, `adapter` |
| `diavasi_group_ack_latency_seconds` | histogram | `group_id`, `adapter` |
| `diavasi_checkpoint_write_latency_seconds` | histogram | `group_id` |
| `diavasi_group_restarts_total` | counter | `group_id` |
| `diavasi_consumer_disconnects_total` | counter | `group_id`, `consumer_id` |
| `diavasi_adapter_errors_total` | counter | `group_id`, `adapter` |

`diavasi_group_checkpoint_lag` is records sitting in the buffer plus records assigned to a consumer and not yet acked. It is not a distance between cursor tuples. A slow consumer grows this number until the buffer cap stops further fetches. The buffer gauge stays at or under `max_buffer_records`. In-flight records sit outside that cap, bounded by the batches already assigned.

`diavasi_group_restarts_total` counts supervisor respawns after an unexpected task exit (source error, abort, panic). A pause does not increment it.

## Diagnostics

```bash
diavasi group diagnostics demo
diavasi --output json group diagnostics demo
```

The JSON object has:

- `running` and `lifecycle`
- `committed_cursor` (live engine while running, otherwise the store)
- `fetched_cursor` (live read position; null when the runtime is down, and null at the start of a stream)
- `buffer_records`, `buffer_bytes`, `inflight_records`, `consumers`
- `records_fetched`, `records_delivered`, `records_acked`, `records_replayed`, `bytes`
- `checkpoint_lag`, `restarts`, `consumer_disconnects`, `adapter_errors`
- `last_stop_reason` and `recovered`

`last_stop_reason` is `paused` after a clean pause, the source error text after a failed read, `task aborted` after an abort, or `task panicked` after a panic. `recovered` is true after the supervisor respawns that group in this process. Both are forgotten when the process exits. The checkpoint in the store is the durable position.

An explicit `group start` or `group resume` clears `recovered` for that group. The previous reason remains until the next exit.

## Dashboard

`diavasi tui` is a live view of the control plane. It polls `/health`, `/ready`, `/v1/status`, `/v1/groups`, and the diagnostics of the highlighted group. It uses the same `--url` and `--token` as the other client commands (`DIAVASI_URL`, `DIAVASI_API_TOKEN`).

```bash
export DIAVASI_URL=http://127.0.0.1:7700
export DIAVASI_API_TOKEN=dev-token
diavasi tui
```

![Diavasi TUI with groups demo and trades. trades is selected and its diagnostics show a full buffer, lag 1024, and last stop paused.](screenshots/diavasi-Screenshot-TUI.png)

The screen has four panes.

**server.** The control-plane URL, `health` and `ready` (`ok` or `down`), the server version, and how many groups are running.

**groups.** One row per group: id, whether the runtime is up, and the lifecycle (`Running`, `Stopped`, and the other states). The highlighted row is the group whose diagnostics are on the right. In the screenshot, `demo` and `trades` are both running, and `trades` is selected.

**diagnostics.** The same fields as `diavasi group diagnostics`:

- `running`, `lifecycle`, and `recovered`
- `committed` and `fetched` cursors. `-` means there is no position yet. A JSON array such as `[{"U64":1024}]` is the logical cursor.
- `buffer` records and bytes, `inflight`, and `lag` (buffer records plus in-flight records)
- `fetched/delivered/acked/replayed`, payload `bytes`, `restarts`, `disconnects`, and `adapter errors`
- `consumers`, or `-` when nobody is joined
- `last stop`, or `-` when this process has not stopped the group

In the screenshot, `trades` has fetched 1024 records into a full buffer, nothing is in flight, so lag is 1024, and the last stop in this process was `paused`. Nothing has been acked, so the committed cursor is still `-`.

**keys.** Shown on the bottom bar:

| Key | Action |
| --- | --- |
| `j` / `k` | Move the selection down or up. Arrow keys do the same. |
| `s` | Start the selected group. |
| `p` | Pause it (the runtime stops, the definition stays). |
| `d` | Drain it. |
| `r` | Refresh now. |
| `q` | Quit. Esc and Ctrl-C also quit. |

The view refreshes about once a second. A failed request is printed in red in the diagnostics pane. The last good group list stays on screen.

## Logs

`diavasi serve` writes text logs. `RUST_LOG` selects the filter and defaults to `info`.

```bash
DIAVASI_LOG_FORMAT=json diavasi serve --bind 127.0.0.1:7700 --data-bind 127.0.0.1:7710 \
  --store /tmp/diavasi/meta.redb --token "$DIAVASI_API_TOKEN"
```

`DIAVASI_LOG_FORMAT=text` is the default. Any other value is rejected with a line on stderr and the text formatter is used.

Fetch failures, ack failures, restarts, and consumer disconnects include `group_id`. Disconnects and data-plane ack failures also include `consumer_id`.

Fetch and ack use debug spans named `group_fetch` and `group_ack`. The default `info` filter does not enable them. `RUST_LOG=debug` prints them. There is no trace exporter in this stage.
