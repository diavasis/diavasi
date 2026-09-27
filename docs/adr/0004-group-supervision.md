# ADR 0004: Per-group Tokio supervision

## Status

Accepted in v0.3.0.

## Context

Diavasi must run many consumer groups in one process. A failure in one group must not take down unrelated groups. Stage 1/2 already concentrate buffer, inflight, ACK, and checkpoint logic in `GroupEngine` / `DurableGroup`.

## Decision

- One **GroupRuntime** task tree per group.
- A **single owner task** mutates `DurableGroup` via a bounded command mailbox. No `Arc<Mutex<GroupEngine>>` shared across consumers.
- Fetch and timeout tickers are child tasks that only wake the owner (`Fetch` / `Tick` commands).
- An adapter source lives in its own fetch task. The owner sends it one read request at a time and receives the rows as a `Fetched` command, so acks, assigns, and pause never wait behind a database query. After a read that returns nothing, the next read waits twice as long as the last, from `fetch_interval` (5 ms) up to `idle_fetch_max` (1 s). A read that returns rows resets the wait.
- `GroupSupervisor` tracks running groups, supports graceful `stop_group` and hard `abort_group`, and on unexpected exit reopens from `StateStore` and respawns (`Failed` recovery path via existing `DurableGroup::open` / `recover_from`).
- A failed group restarts after 250 ms. Each further consecutive failure doubles the delay, up to 30 s. A group that ran for 60 s before failing starts over at 250 ms. A restart that cannot open its source is scheduled again the same way; the group is never dropped from supervision.
- A source error is `Transient` (connection, timeout) or `Contract` (the data broke the declared contract: wrong type, undecodable record, record not after the cursor, entries removed before delivery). A contract failure is not restarted, because the same data would fail again. The group stays stopped with the error as its stop reason until an operator starts it.
- Starting a group has three steps: reserve it under the supervisor lock, open its store record and source without the lock, and spawn it under the lock. A slow database therefore does not block other control-plane requests or data-plane joins.
- Checkpointing remains inside Stage 2 `DurableGroup::ack`. Stage 3 does not add a second durability path.

## Consequences

- In-process consumers use `GroupHandle` (join / leave / assign / ack) until Stage 5 exposes a network data plane.
- Panic or abort of one group is recoverable; other groups keep running.
- Channel capacity and engine buffer caps provide backpressure.
- Control plane (Stage 4) drives the supervisor over HTTP; Stage 3 acceptance did not require it.
