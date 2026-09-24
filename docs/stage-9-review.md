# Stage 9 review: Redis Streams adapter

**Status:** complete. Stop for review before Stage 10.

## Delivered

- `RedisFactory` stream reader in `diavasi-adapter-redis`
- CLI router entry for `redis`
- Contract tests against `REDIS_URL`, skipped when it is unset
- CI `redis:7-alpine` service on the integration job only
- [ADR 0010](adr/0010-redis-query-contract.md), [adapter notes](adapters/redis.md), [tutorial](tutorials/stage-09-redis.md)
- The crate is in the tag-triggered crates.io workflow. This stage does not publish

## Guarantees verified

- Stream ids `9-1` and `10-0` are delivered in numeric order.
- An empty stream assigns nothing.
- `XGROUP SETID` rewinds a read that was not checkpointed, then the following page is read.
- An `XADD` ahead of the committed cursor appears. `XDEL` of an unread entry is omitted on resume.
- A missing key and a non-stream key fail at create.
- A field inclusion list drops the other fields.
- A poisoned connection is replaced and the following page is read.
- Two consumers share one traversal.
- The data plane consumes stream entries through the existing session.

## Intentionally not in Stage 9

- ScyllaDB and the S3 adapter
- Hashes, lists, sets, and sorted sets
- Blocking reads, `XAUTOCLAIM`, and the Redis pending list as the checkpoint
- A real `cargo publish`

## Quality gate

```bash
./scripts/check.sh
```

The `integration` CI job sets `DATABASE_URL`, `MONGODB_URL`, and `REDIS_URL`. The other jobs leave those variables unset. A local `cargo test` without them still passes; those tests return immediately.

## Stop

Stage 9 is done. Do not start Stage 10 without an explicit instruction.

## crates.io

`.github/workflows/release.yml` publishes `diavasi-adapter-redis` after MongoDB and before the CLI, on a tag `vX.Y.Z` that matches `workspace.package.version`. Before the first tag, that crate needs a trusted publisher for this repository, workflow `release.yml`, and environment `release`. Scylla stays `publish = false`.
