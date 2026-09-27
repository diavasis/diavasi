# ADR 0002: Batch ACK and contiguous commit

## Status

Accepted for Stage 1 onward.

## Decision

- Delivery acknowledgements are **batch ACK** only in Stage 1.
- The durable/safe progress cursor (`committed`) advances only across a **contiguous** prefix of the group's traversal order.
- Out-of-order batch completion is allowed; gaps hold the committed cursor.
- In-flight assignments are **not** part of the durable snapshot. Restart resumes from `committed` and may **replay** uncertain work (at-least-once).
- Fetched progress may lead committed progress because of buffering.
- By default an ack is answered after the checkpoint it produced is written. Acks that wait in the group's mailbox together share one write. With `--checkpoint-interval-ms`, acks are answered at once and the checkpoint is written at most once per interval and on pause, drain, and shutdown; a crash then replays up to one interval of acked records. Either way the committed cursor in the store never runs ahead of the acks.

## Consequences

- Applications must tolerate duplicate delivery around crashes, timeouts, and reconnects.
- Exactly-once effects require application-level idempotency (not provided by Diavasi).
- Stage 2 persists `committed` (and group definition), not payloads or in-flight sets.
- Individual and cumulative ACK modes remain possible later without changing the contiguous-commit invariant.
