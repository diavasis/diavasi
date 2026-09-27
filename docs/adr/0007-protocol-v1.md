# ADR 0007: Protocol v1

## Status

Accepted in v0.5.0.

## Context

External consumers need a small, versioned session on top of `GroupHandle`. The Stage 0 bench protobuf is not that protocol and stays unchanged.

## Decision

Package `diavasi.data.v1`. Each stream is one consumer. The first client frame is `Hello`, then `JoinGroup`. Anything else is `Error` and the stream closes.

| Opcode | Direction | Behavior |
| --- | --- | --- |
| `Hello` / `HelloAck` | client / server | Negotiate version 1. Unknown versions close. |
| `JoinGroup` / `Joined` | client / server | `GroupHandle::join` on a running group. A consumer id already held by another open stream is taken over: that stream receives error `2` and closes, and its unacked batches return to the buffer. |
| `RecordBatch` | server | At most `batch_max_records` records, and at most 4 MiB less 64 KiB of payload so the batch fits a gRPC client's default 4 MiB message limit. A single larger record is sent alone. |
| `Ack` | client | `GroupHandle::ack`. An ack for a batch this stream was never sent, or acked twice, is an error. An ack for a batch that timed out and was delivered again is accepted and has no effect; the server counts it in `diavasi_group_stale_acks_total`. |
| `Nack` | reserved | Rejected. The field stays in the oneof. |
| `Heartbeat` | both | Missed client traffic beyond the timeout leaves the consumer. |
| `FlowControl` | client | Caps unacked batches on that stream. Default is 1. |
| `Error` | server | Stable numeric code plus a message. |
| `Leave` | client | `leave_consumer`, which requeues in-flight batches. |

Auth is gRPC metadata `authorization: Bearer <token>` before any join. A dropped stream also leaves and requeues.

Error codes: `1` bad version, `2` bad state, `3` unknown ack, `4` duplicate ack, `5` group not running, `6` unsupported, `7` internal, `8` heartbeat timeout.

A failed join returns `5` when the group is not running, `2` for an empty or invalid group or consumer id, and `7` for anything else.

## Consequences

- Clients do not compute checkpoints. They ack batch ids.
- At-least-once replay after disconnect is the existing engine behavior.
- SDKs in Stage 11 speak this protocol. The Python and Elixir compatibility clients, and the later SDKs, each live in their own repository. The index is [clients/README.md](../../clients/README.md).
