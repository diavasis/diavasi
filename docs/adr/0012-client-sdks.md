# ADR 0012: Client SDKs speak protocol v1 and do not checkpoint

## Status

Accepted for Stage 11 onward.

## Context

The server owns delivery. `diavasi.data.v1` is one bidi RPC, `DataPlane.Consume`. [ADR 0007](0007-protocol-v1.md) defines the frames: `Hello`, `JoinGroup`, `RecordBatch`, `Ack`, `FlowControl`, `Heartbeat`, `Leave`. `Nack` is reserved. A dropped stream is how unacked batches return. The durable cursor lives in the server store. A client that stored a cursor would be a second checkpoint.

Stage 5 shipped two compatibility loops, in Python and Elixir. Stage 11 turns those into libraries and adds the same session in Rust, Go, JavaScript, Java, C#, and C.

## Decision

Every SDK is a thin client of protocol v1:

- TLS to the data-plane address, a CA file, a bearer token, `group_id`, `consumer_id`, and `max_in_flight` (default 1).
- Send `Hello` version 1, then `JoinGroup` after `HelloAck`.
- Yield each `RecordBatch`. The caller acks by `batch_id`. Clients do not dedupe on `record_id`, because that field is 0 for Redis and for compound or token-scan Scylla keys.
- Send `FlowControl` once after `Joined`. Answer a server `Heartbeat` with a `Heartbeat`. Send `Leave` on a clean stop.
- Reconnect opens a new stream with the same consumer id. The server replays unacked batches. The client does not resume from a key.
- Surface protocol `Error` codes 1 through 8. An unknown version, a missing or wrong token, and a group that is not running fail the call.

The native client is C, using the gRPC C stack against the same proto. The Rust client is `clients/rust` (`diavasi-client`). It is not a dependency of the server crate. No SDK is published from this stage.

## Consequences

Application code acks batches and can reconnect. It does not compute ordering tuples or call the store. A future language is another tree under `clients/` with the same session, a README, an example, and a test that skips until `DIAVASI_DATA_ADDR`, `DIAVASI_CA`, and `DIAVASI_API_TOKEN` are set.
