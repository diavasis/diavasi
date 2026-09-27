# Operations

How a running server behaves, what its limits are, and what to do when a group stops.

## Lifecycle

`GET /v1/groups/{id}` reports `lifecycle` and `running`. They agree:

| `lifecycle` | `running` | Meaning |
| --- | --- | --- |
| `Stopped` | false | Created, paused, or drained. `group start` runs it. |
| `Running` | true | Reading the source, delivering, accepting acks and consumers. |
| `Draining` | true | Delivering what it already read. Reads nothing new and refuses new consumers. Becomes `Stopped` when every record is acked. |
| `Recovering` | false | Failed and waiting for its next restart. |
| `Failed` | false | Stopped by bad data. Not restarted; see below. |

The running group writes its lifecycle to the store. When the server starts, groups recorded as `Running` or `Draining` start again.

## Why a group stopped

`GET /v1/groups/{id}/diagnostics` reports `last_stop_reason` for the current process.

| Reason | What happened | What to do |
| --- | --- | --- |
| `paused` | `group pause`. | `group start` when wanted. |
| `drained` | A drain finished. | Nothing, or `group start`. |
| `shutdown` | The server stopped cleanly. | The group starts with the server. |
| `task aborted`, `task panicked` | The group task died. | The supervisor restarts it. A panic is a bug; report it with the logs. |
| A connection or timeout message | The source could not be reached (transient). | The supervisor restarts it with growing delays; fix the database or network. |
| A data message, such as a wrong type, an undecodable value, a record not after the cursor, or `trimmed past the committed cursor` | The data broke the source contract. The lifecycle is `Failed`. | Fix the data or the `source_spec`, then `group start`. Restarting reads the same data and fails again. |

## Restarts

A group that fails with a transient error restarts from its committed cursor after 250 ms, then 500 ms, doubling to 30 s while it keeps failing. A group that ran for 60 s before failing starts over at 250 ms. There is no restart limit. `diavasi_group_restarts_total` counts successful restarts and `diavasi_group_recovery_failures_total` counts restarts that could not open the source. `group pause` cancels a pending restart.

## Shutdown and start

SIGINT or SIGTERM stops the HTTP and gRPC servers, and each running group saves its progress without changing its lifecycle. Consumers see their stream end; unacked batches are delivered again after the restart. At start, a group whose source cannot be opened yet goes onto the restart schedule instead of failing the server.

## Delivery and checkpoints

Delivery is at-least-once. A record can be delivered again after a crash, a pause, a batch timeout, or a consumer that drops its stream. Consumers must tolerate repeats.

By default an ack is answered after the committed cursor it produced is written. Acks that wait together share one write. `--checkpoint-interval-ms <n>` answers acks at once and writes at most every `n` ms; a crash then replays up to `n` ms of acked records in exchange for fewer writes.

## Limits

| Limit | Value |
| --- | --- |
| Id length and characters | 1 to 128, `A-Z a-z 0-9 . _ - :` |
| Request body | 88 KiB |
| Connection secret | 1 byte to 16 KiB |
| `config_json` | 64 KiB |
| `ordering_contract` | 1024 bytes |
| `batch_timeout_ms` | at least 100 |
| Batch payload | 4 MiB less 64 KiB, or one larger record alone |
| `max_in_flight` per stream | 1 to 1024, default 1 |
| Heartbeat | server sends every 5 s; closes a stream silent for 30 s |
| Idle wait for records | a consumer's request waits up to 1 s |
| Idle source reads | back off from 5 ms to 1 s while the source has nothing new |
| Command answer | 5 s, then `503` |

Consumers per group and groups per process are not capped.

## Security

- One bearer token protects `/v1`, `/metrics`, and the data plane. Anyone with it can create connections, read diagnostics, and consume every group.
- The control plane is plain HTTP. Bind it to localhost, or put a TLS proxy in front, when it is reachable from other hosts.
- The data plane is TLS. Without `--tls-cert` and `--tls-key`, the server generates a local CA (`dataplane-ca.crt`), a certificate for `localhost` and `127.0.0.1`, and a key readable only by its owner, next to the store. Clients trust `dataplane-ca.crt`. Remote clients connect through a name in the certificate, or use a certificate of your own.
- Connection secrets are sealed with ChaCha20-Poly1305 under the store key. `config_json`, group definitions, and cursors are not encrypted.
- The store key is 32 bytes as 64 hex characters (`DIAVASI_STORE_KEY` or `--store-key`). The server refuses to start without it when the store holds a connection, and refuses a key that cannot open the stored secrets. There is no key rotation: to change the key, delete and recreate the connections.

## Backup and upgrade

The state is the store file (`--store`), the data-plane TLS files next to it, and the store key. Stop the server (SIGTERM) before copying the store file; redb allows one writer and the copy of a running store may be torn. Keep the key with the backup and apart from it: without the key the connection secrets cannot be opened.

The store records a schema version. A server refuses a store with a version it does not support, newer or older; this release reads version 1.
