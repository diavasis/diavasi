# Stage 8 review: MongoDB adapter

**Status:** complete. Stop for review before Stage 9.

## Delivered

- `MongoFactory` find keyset in `diavasi-adapter-mongodb`
- `SourceFactory::kind` and a CLI router for `postgres` and `mongodb`
- Create and start reject an unknown connection kind instead of treating it as synthetic
- Contract tests against `MONGODB_URL`, skipped when it is unset
- CI `mongo:7` service
- [ADR 0009](adr/0009-mongodb-query-contract.md), [adapter notes](adapters/mongodb.md), [tutorial](tutorials/stage-08-mongodb.md)
- Tag-triggered crates.io workflow. This stage does not publish

## Guarantees verified

- Default `_id` order, an `int64` key, a string key, and a compound ascending/descending key are delivered in traversal order.
- `date`, `bool`, and `binData` keys round-trip.
- An empty collection assigns nothing.
- An insert ahead of the cursor appears. An insert behind it does not.
- A field update does not redeliver a passed document. A delete is omitted on resume from the committed cursor.
- A sort-field update that moves a key behind the cursor is a miss, and the test asserts that miss.
- A poisoned client is replaced and the following page is read.
- Restarting Diavasi from the store resumes at the committed cursor.
- Two consumers, a crash, and resume do not omit documents still at or ahead of the committed cursor.
- The data plane consumes MongoDB documents through the existing session.
- A collection of tens of thousands of documents is read in CI. A millions-document test is ignored.

## Intentionally not in Stage 8

- Redis, ScyllaDB, and the S3 adapter
- Aggregation pipelines, change streams, and tailable cursors
- A server-side Mongo cursor as the resume mechanism
- A real `cargo publish`

## Quality gate

```bash
./scripts/check.sh
```

CI sets `DATABASE_URL` and `MONGODB_URL` so both adapter suites run. A local `cargo test` without those variables still passes; those tests return immediately.

## Stop

Stage 8 is done. Do not start Stage 9 without an explicit instruction.

## crates.io

`.github/workflows/release.yml` publishes on a tag `vX.Y.Z` that matches `workspace.package.version`. Authentication is crates.io trusted publishing (`id-token: write`, environment `release`). Before the first tag, each of `diavasi`, `diavasi-adapter-postgres`, `diavasi-adapter-mongodb`, and `diavasi-cli` needs a trusted publisher for this repository, workflow `release.yml`, and environment `release`. Redis and Scylla stay `publish = false`.
