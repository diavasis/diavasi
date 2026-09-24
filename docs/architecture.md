# Diavasi architecture (implemented behavior only where marked)

## Purpose

Diavasi adds durable consumer-group semantics to ordinary existing database data. It does not own ingestion. Producers keep writing their databases as they do today.

Central abstraction:

```text
connection + query + ordering contract + consumer group
    -> durable, resumable, parallel-consumable stream
```

## Design principles (summary)

1. Silent loss is unacceptable. Downtime is acceptable.
2. Single server instance first. No clustering until demonstrated demand.
3. One isolated logical process per consumer group.
4. One source traversal per active consumer group.
5. At-least-once by default. On ambiguity, replay rather than skip.
6. Persist logical progress, not live database cursors.
7. The user owns the source mutation contract.
8. Bound everything.
9. Backpressure is fundamental.
10. Adapters translate database mechanics; core owns consumer-group semantics.
11. Separate control and data planes.
12. Benchmark before choosing the data-plane transport.

## Process model

One Diavasi server hosts many independent consumer-group runtimes under a supervisor. A group failure must not take down unrelated groups.

## Status

| Area | Status |
| --- | --- |
| Data-plane transport | Stage 5 frozen: gRPC over HTTP/2 with TLS ([ADR 0006](adr/0006-data-plane-transport.md)) |
| Core domain | Stage 1 complete (in-memory `GroupEngine`; see docs/tutorials/stage-01-core-domain.md) |
| Durable metadata store | Stage 2 complete (`StateStore` + redb; see docs/tutorials/stage-02-durable-store.md) |
| Group runtime / supervision | Stage 3 complete (`GroupSupervisor`; see docs/tutorials/stage-03-group-runtime.md) |
| Control plane / CLI | Stage 4 complete (`diavasi::control` + `diavasi` CLI; see docs/tutorials/stage-04-control-plane.md) |
| Database adapters | Stage 6: PostgreSQL keyset reader ([ADR 0008](adr/0008-postgres-query-contract.md)). Stage 8: MongoDB find keyset ([ADR 0009](adr/0009-mongodb-query-contract.md)). Stage 9: Redis Streams ([ADR 0010](adr/0010-redis-query-contract.md)). ScyllaDB is still a placeholder |
| Performance / resource model | Stage 7: end-to-end Postgres benchmark and idle vs active group cost ([docs/bench/stage-07.md](bench/stage-07.md), [docs/resource-model.md](resource-model.md)) |

## Control plane (Stage 4)

Operators talk to a versioned HTTP API (`/v1`) served by axum inside the Diavasi process. The `diavasi` CLI is an HTTP client (plus `diavasi serve` which hosts the control plane). Auth is a shared bearer token; connection secrets are sealed at rest and never returned after create. Lifecycle verbs map onto `GroupSupervisor` (`pause` = graceful stop). See [ADR 0005](adr/0005-control-plane.md).

## Data plane (Stage 5)

Consumers connect with TLS gRPC (`diavasi.data.v1`) to a running group. The session maps onto `GroupHandle`: join, bounded assign, ack, and leave. A dropped stream requeues unacked batches. Auth is the same bearer token as the control plane. See [ADR 0006](adr/0006-data-plane-transport.md) and [ADR 0007](adr/0007-protocol-v1.md).

## PostgreSQL adapter (Stage 6)

A group bound to a `postgres` connection reads one declared table in a total order and feeds rows into the existing buffer, ack, and checkpoint path. Resume is the committed logical cursor, not a server-side cursor. See [ADR 0008](adr/0008-postgres-query-contract.md) and [docs/adapters/postgresql.md](adapters/postgresql.md).

## MongoDB adapter (Stage 8)

A group bound to a `mongodb` connection reads one collection with `find` in a declared order, including a descending key, and feeds documents into that same path. Resume is the committed logical cursor, not a Mongo cursor. See [ADR 0009](adr/0009-mongodb-query-contract.md) and [docs/adapters/mongodb.md](adapters/mongodb.md).

## Redis adapter (Stage 9)

A group bound to a `redis` connection reads one stream with `XGROUP SETID` and `XREADGROUP`, then feeds entries into that same path. Resume is the committed stream id, not the Redis pending list. `diavasi serve` routes `postgres`, `mongodb`, and `redis`. See [ADR 0010](adr/0010-redis-query-contract.md) and [docs/adapters/redis.md](adapters/redis.md).

## Resource model (Stage 7)

An idle running group is one owner task, a fetch ticker, a timeout ticker, and, for Postgres, one connection, with an empty buffer. An active group adds the configured buffer and the in-flight batches on each stream. Consumers per group, groups per process, and fetch-error restarts are unbounded on purpose. See [docs/resource-model.md](resource-model.md).

## Non-goals (initial releases)

Distributed clustering, consensus, Kafka compatibility, exactly-once marketing, CDC, source ingestion, stream processing framework, large web UI, probabilistic reconciliation as correctness.
