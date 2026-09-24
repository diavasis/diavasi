# Stage 6 review: PostgreSQL adapter

**Status:** complete. Stop for review before Stage 7.

## Delivered

- `connection_id` and `source_spec` persisted on `GroupRecord` (schema version stays 1)
- `RecordSource`, `GroupEngine::ingest`, and supervisor open/recovery without a crate cycle
- Keyset reader in `diavasi-adapter-postgres`, installed by `diavasi serve`
- CLI `group create --source-json`
- Contract tests against `DATABASE_URL`, skipped when it is unset
- CI Postgres 16 service
- [ADR 0008](adr/0008-postgres-query-contract.md), [adapter notes](adapters/postgresql.md), [tutorial](tutorials/stage-06-postgres.md)

## Guarantees verified

- An `int8` primary key, a `timestamptz` plus id key, and a text key are delivered in order.
- An empty table assigns nothing.
- An insert ahead of the cursor appears. An insert behind it does not.
- A payload update does not redeliver a passed row. A delete is omitted on resume from the committed cursor.
- An ordering-column update that moves a key behind the cursor is a miss, and the test asserts that miss.
- A dropped backend is retried or the group task restarts from the committed cursor.
- Restarting Diavasi from the store resumes at the committed cursor.
- Two consumers, a crash, and resume do not omit rows still at or ahead of the committed cursor.
- The data plane consumes Postgres rows through the existing session.
- A table of tens of thousands of rows is read in CI. A millions-row test is ignored and is not the Stage 7 bench.

## Intentionally not in Stage 6

- MongoDB, Redis, ScyllaDB
- Stage 7 end-to-end benchmarks and resource model
- Docker Compose demo
- CDC and logical replication
- A server-side cursor as the resume mechanism

## Quality gate

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all --all-features
cargo deny check
```

The `integration` CI job sets `DATABASE_URL` so the Postgres tests run. A local `cargo test` without that variable still passes; those tests return immediately.

## Stop

Stage 6 is done. Do not start Stage 7 without an explicit instruction.
