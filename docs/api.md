# HTTP API

The control plane listens on `--bind` (default `127.0.0.1:7700`), over HTTPS when the server has `--http-tls-cert` and `--http-tls-key`. Bodies are JSON. Every route except `/health` and `/ready` needs `Authorization: Bearer <token>`; a missing or wrong token is `401` with the body `unauthorized`. A request body over 88 KiB is `413`.

The examples use:

```bash
export DIAVASI_URL=http://127.0.0.1:7700
export AUTH="Authorization: Bearer $DIAVASI_API_TOKEN"
```

## Errors

A failed request returns a status and `{"error": "<message>"}`.

| Status | Meaning |
| --- | --- |
| 400 | Invalid input: a malformed id, a zero cap, a `batch_timeout_ms` under 100, a source spec the adapter rejects. |
| 404 | The group or connection does not exist. |
| 409 | The request conflicts with state: the id exists, the group is running (delete) or not running (drain), a second drain, a connection still in use, a temporary store key, a backup path that exists. |
| 503 | The group did not accept the command within 5 seconds. Retry. |
| 500 | Anything else. |

Ids of groups, connections, and consumers are 1 to 128 characters from `A-Z a-z 0-9 . _ - :`.

## Health and metrics

| Route | Auth | Returns |
| --- | --- | --- |
| `GET /health` | none | `200 ok` while the process runs. |
| `GET /ready` | none | `200 ok` when the store can be read, otherwise `503 not ready`. |
| `GET /metrics` | bearer | Prometheus text. See [observability.md](observability.md). |
| `GET /v1/status` | bearer | Version, schema version, running groups, bind address. |

```bash
curl -s -H "$AUTH" $DIAVASI_URL/v1/status
```

```json
{"version": "0.12.0", "schema_version": 1, "running_groups": ["orders"], "bind": "127.0.0.1:7700"}
```

## Connections

A connection holds how to reach a database. The secret is sealed with the store key and never returned.

| Route | Returns |
| --- | --- |
| `POST /v1/connections` | The connection. `409` when the id exists or the server runs with a temporary store key. |
| `GET /v1/connections` | Every connection, sorted by id. |
| `GET /v1/connections/{id}` | One connection. |
| `DELETE /v1/connections/{id}` | `200` with an empty body. `409` while a group uses it. |

```bash
curl -s -H "$AUTH" -H 'content-type: application/json' $DIAVASI_URL/v1/connections -d '{
  "id": "pg-main", "kind": "postgres",
  "config_json": {"host": "db.internal", "port": 5432, "dbname": "app", "user": "diavasi", "sslmode": "require"},
  "secret": "s3cret"}'
```

```json
{"id": "pg-main", "kind": "postgres",
 "config_json": {"host": "db.internal", "port": 5432, "dbname": "app", "user": "diavasi", "sslmode": "require"},
 "secret_sealed": true}
```

`kind` is `postgres`, `mongodb`, `redis`, or `scylla`. The `config_json` keys for each are in [docs/adapters](adapters/); an unknown key fails the first group that uses the connection, and the error names the key. `secret` is 1 byte to 16 KiB; `config_json` is at most 64 KiB.

## Groups

| Route | Returns |
| --- | --- |
| `POST /v1/groups` | The group, stopped. For an adapter group the source is checked first. |
| `GET /v1/groups` | Every group, sorted by id. |
| `GET /v1/groups/{id}` | One group. |
| `DELETE /v1/groups/{id}` | `200` with an empty body. `409` while running. Removes the checkpoint and the group's metrics. |
| `POST /v1/groups/{id}/start` | The group, running. Starting a running group is not an error. |
| `POST /v1/groups/{id}/resume` | Same as start. |
| `POST /v1/groups/{id}/pause` | The group, stopped. Progress is saved; unacked batches are delivered again on the next start. |
| `POST /v1/groups/{id}/drain` | The group, draining. It delivers what it has read, reads nothing new, refuses new consumers, and stops when every record is acked. `409` when not running or already draining. |
| `GET /v1/groups/{id}/consumers` | Joined consumers. Empty when not running. |
| `GET /v1/groups/{id}/checkpoint` | The stored and live committed cursors. |
| `GET /v1/groups/{id}/diagnostics` | Position, lag, counters, and the last stop. See [observability.md](observability.md). |

Create:

```bash
curl -s -H "$AUTH" -H 'content-type: application/json' $DIAVASI_URL/v1/groups -d '{
  "group_id": "orders",
  "max_buffer_records": 4096, "max_buffer_bytes": 8388608,
  "batch_max_records": 200, "batch_timeout_ms": 30000,
  "ordering_contract": "postgres-keyset",
  "connection_id": "pg-main",
  "source_spec": {"table": "app.orders", "order_by": [{"column": "id", "type": "int8"}], "payload": ["id", "total"]}}'
```

| Field | Rule |
| --- | --- |
| `group_id` | An id as above. |
| `total_records`, `payload_size` | Synthetic groups only (no `connection_id`): records `1..=total_records` of `payload_size` bytes. Optional, default `0`. A non-zero value on an adapter group is `400`. |
| `max_buffer_records`, `max_buffer_bytes` | At least 1. Read-ahead stops at either cap. |
| `batch_max_records` | 1 to `max_buffer_records`. A batch also stops before 4 MiB less 64 KiB of payload. |
| `batch_timeout_ms` | At least 100. A batch not acked in time is delivered again. |
| `ordering_contract` | Optional label, at most 1024 bytes. Stored and shown, not interpreted. Defaults to `synthetic-u64`, or to the connection's `kind`. |
| `connection_id`, `source_spec` | Both or neither. The spec keys depend on the adapter; an unknown key or a wrong type is `400`, with the key named. |

A group as returned:

```json
{"group_id": "orders", "total_records": 0, "payload_size": 0,
 "max_buffer_records": 4096, "max_buffer_bytes": 8388608, "batch_max_records": 200,
 "batch_timeout_ms": 30000, "ordering_contract": "postgres-keyset",
 "connection_id": "pg-main", "lifecycle": "Running", "next_batch_id": 1532, "running": true}
```

`lifecycle` agrees with `running`; see [operations.md](operations.md#lifecycle).

Checkpoint:

```json
{"group_id": "orders", "durable_cursor": [{"I64": 9001}], "live_cursor": [{"I64": 9001}]}
```

A cursor is `null` at the start of the stream, otherwise an array of tagged values in order-by order: `I64` for integers and timestamps (microseconds for Postgres, milliseconds for MongoDB dates), `U64` for synthetic ids and Redis stream id parts, `Bytes` for text and binary keys. `live_cursor` is `null` when the group is not running and can lead `durable_cursor` by one checkpoint interval.

## Store backup

| Route | Returns |
| --- | --- |
| `POST /v1/store/backup` | Copies the store to a new file on the server host and returns what it copied. |

```bash
curl -s -H "$AUTH" -H 'content-type: application/json' $DIAVASI_URL/v1/store/backup \
  -d '{"path": "/var/backups/diavasi/meta-2026-09-27.redb"}'
```

```json
{"path": "/var/backups/diavasi/meta-2026-09-27.redb", "connections": 2, "groups": 5}
```

`path` must be absolute (`400`) and must not exist (`409`); the parent directory must exist. The copy is one consistent read of connections, groups, and checkpoints, taken while groups keep running. Secrets stay sealed with the store key, so the backup needs that key to be used.

## Data plane

Consumers do not use HTTP. They open `DataPlane.Consume` on `--data-bind` with TLS and the same bearer token. The session is in [ADR 0007](adr/0007-protocol-v1.md) and the SDKs in [clients/README.md](../clients/README.md).
