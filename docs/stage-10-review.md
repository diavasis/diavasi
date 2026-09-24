# Stage 10 review: ScyllaDB adapter

**Status:** complete. Stop for review before Stage 11.

## Delivered

- `ScyllaFactory` partition keyset and token-scan reader in `diavasi-adapter-scylla`
- CLI router entry for `scylla`, and `diavasi test scylla`
- Contract tests against `SCYLLA_URL`, skipped when it is unset and failed when it is set but unreachable
- CI `scylladb/scylla:2026.1` service on the integration job only, with `--smp 1 --memory 1G --overprovisioned 1`
- [ADR 0011](adr/0011-scylla-query-contract.md), [adapter notes](adapters/scylla.md), [tutorial](tutorials/stage-10-scylla.md)
- The crate is in the tag-triggered crates.io workflow, after Redis and before the CLI. This stage does not publish

## Guarantees verified

- One partition is delivered in clustering order. An empty partition assigns nothing.
- Resume after a checkpoint delivers the following rows. An insert ahead appears. An insert behind the committed clustering cursor does not. A delete of an unread row is omitted.
- A missing table fails at create. A spec with neither `partition` nor `scan` fails at create. A partition object that does not name every partition key fails at create. `double` is rejected as a key column.
- A descending clustering column is delivered high to low, and the stored tuple still increases.
- A compound clustering key uses each column's schema direction.
- A token scan returns rows in token order across several partitions and resumes after the first row.
- Two consumers share one traversal.
- The data plane consumes the partition through the existing session. A single positive bigint clustering column is the wire `record_id`.
- A poisoned session is replaced, statements are prepared again, and the following page is read.
- CI and the local gate read a partition of 20,000 rows. A million-row test is ignored.

## Intentionally not in Stage 10

- CDC, materialized views as their own source, secondary indexes, and parallel token-range splits
- The S3 adapter
- Thin SDKs (Stage 11)
- A real `cargo publish`

## Quality gate

```bash
./scripts/check.sh
```

The `integration` CI job sets `DATABASE_URL`, `MONGODB_URL`, `REDIS_URL`, and `SCYLLA_URL`. The `check`, `coverage`, and `deny` jobs do not start database containers. A local `cargo test` without `SCYLLA_URL` still passes; those tests return immediately. `./scripts/check.sh` defaults `SCYLLA_URL` to `127.0.0.1:9042`.
