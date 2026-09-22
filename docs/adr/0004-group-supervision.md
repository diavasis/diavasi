# ADR 0004: Per-group Tokio supervision

## Status

Accepted for Stage 3 onward.

## Context

Diavasi must run many consumer groups in one process. A failure in one group must not take down unrelated groups. Stage 1/2 already concentrate buffer, inflight, ACK, and checkpoint logic in `GroupEngine` / `DurableGroup`.

## Decision

- One **GroupRuntime** task tree per group.
- A **single owner task** mutates `DurableGroup` via a bounded command mailbox. No `Arc<Mutex<GroupEngine>>` shared across consumers.
- Fetch and timeout loops are child tasks that only wake the owner (`Fetch` / `Tick` commands).
- `GroupSupervisor` tracks running groups, supports graceful `stop_group` and hard `abort_group`, and on unexpected exit reopens from `StateStore` and respawns (`Failed` recovery path via existing `DurableGroup::open` / `recover_from`).
- Checkpointing remains inside Stage 2 `DurableGroup::ack`. Stage 3 does not add a second durability path.

## Consequences

- In-process consumers use `GroupHandle` (join / leave / assign / ack) until Stage 5 exposes a network data plane.
- Panic or abort of one group is recoverable; other groups keep running.
- Channel capacity and engine buffer caps provide backpressure.
- Control plane (Stage 4) drives the supervisor over HTTP; Stage 3 acceptance did not require it.
