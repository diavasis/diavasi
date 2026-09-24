# Resource model

What one Diavasi process spends on a consumer group. Figures are the configured caps plus the tasks the runtime actually starts. There is no process-wide memory accountant: resident set size is not the bound.

## Idle group

A group that is running and has no consumers, or whose buffer is empty:

- One owner task. It is the only task that mutates that group's engine.
- One fetch ticker (default 5ms) and one timeout ticker (default 25ms).
- One PostgreSQL connection when the group is bound to `postgres`, including the task that drives that connection. Consumer count does not add connections.
- An empty buffer. `max_buffer_records` and `max_buffer_bytes` are reserved as caps, not allocated up front.
- A redb group record. Checkpoint bytes are written when an ack advances the committed cursor, not on a timer.

A group with no `connection_id`, or a connection whose kind is not `postgres`, has the same tasks and no database connection. Its source is the in-process synthetic generator.

## Active group

While consumers are attached and rows are flowing, add:

- Buffered records, at most `max_buffer_records` and `max_buffer_bytes`. Fetch stops when either cap is hit.
- In-flight batches. Each data-plane stream has its own `max_in_flight` (protocol default 1, cap 1024). Each batch holds at most `batch_max_records`. A record in flight is a copy of the buffered record, so a full buffer plus in-flight batches is the working set for that group.
- One protobuf copy of each record as it is written to a stream.
- One redb write transaction per ack that advances the committed cursor. A batch that does not extend the contiguous prefix does not write a new checkpoint.

A batch that times out, or an assign whose caller is gone, returns to the buffer when the cap has room. If the buffer was already refilled, the batch stays in flight and a later tick tries again. It is not dropped, and the buffer stays within its cap.

## What is bounded

| Resource | Bound |
| --- | --- |
| Buffered records | `max_buffer_records` |
| Buffered bytes | `max_buffer_bytes` |
| Records in one batch | `batch_max_records` |
| Unacked batches on one stream | `max_in_flight` |
| Postgres connections for one running group | 1 |
| MongoDB clients for one running group | 1 |

## What is unbounded on purpose

- Consumers per group. Each consumer is another gRPC stream and another in-flight window. A cap here would reject a join that the crash-recovery path needs to replay.
- Groups per process. Each running group adds one owner, two tickers, and one source client when the group is bound to Postgres or MongoDB. The operator's bound is how many groups they start.
- Supervisor restarts after a fetch error. The group task sleeps 200ms, exits, and is opened again from the committed cursor. A restart cap would stop recovery.

## What the Stage 7 measurement showed

On a release build against Compose Postgres (`docs/bench/stage-07.md`):

- A 200-record batch keeps the pipeline near 1.6e4 records/s for 64-byte payloads. A 20-record batch drops that to about 1.8e3 records/s, because each ack that extends the cursor is its own redb transaction.
- Payload width from 64 to 1024 bytes changes records/s only slightly and scales bytes/s with the payload.
- Four consumers on one group, and two groups, do not add Postgres connections. They add streams and, for two groups, a second owner and a second connection.
- An assign that lost its caller used to leave the batch in flight until `batch_timeout`. The data plane now keeps that assign running across heartbeats, and the owner returns the batch to the buffer if the reply is gone. Throughput is no longer the batch timeout.
