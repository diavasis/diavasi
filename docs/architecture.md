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
| Data-plane transport | Stage 0 complete; provisional recommendation gRPC+TLS (not frozen) |
| Core domain | Stage 1 complete (in-memory `GroupEngine`; see docs/tutorials/stage-01-core-domain.md) |
| Durable metadata store | Not implemented |
| Control plane / CLI | CLI placeholder only |
| Database adapters | Placeholders only |

## Non-goals (initial releases)

Distributed clustering, consensus, Kafka compatibility, exactly-once marketing, CDC, source ingestion, stream processing framework, large web UI, probabilistic reconciliation as correctness.
