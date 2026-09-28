# Diavasi

[![ci](https://github.com/diavasis/diavasi/actions/workflows/ci.yml/badge.svg)](https://github.com/diavasis/diavasi/actions/workflows/ci.yml)
[![crates.io](https://img.shields.io/crates/v/diavasi.svg)](https://crates.io/crates/diavasi)
[![docs.rs](https://docs.rs/diavasi-core/badge.svg)](https://docs.rs/diavasi-core)
[![rust](https://img.shields.io/badge/rust-1.85%2B-000000?logo=rust)](https://www.rust-lang.org)
[![license](https://img.shields.io/github/license/diavasis/diavasi)](https://github.com/diavasis/diavasi/blob/main/LICENSE)

Durable consumer groups for existing databases.

## Introduction

### What

Diavasi adds a durable, resumable, parallel consumer group on top of data you already store. Producers keep writing Postgres, MongoDB, Redis, or Scylla. Diavasi reads that data in the order you declare, hands batches to your workers, and remembers a logical cursor in its own store. Delivery is at-least-once: when a batch is in doubt, Diavasi replays it.

```text
connection + query + ordering contract + consumer group
  -> durable, resumable, parallel-consumable stream
```

The control plane is HTTP. The `diavasi` executable hosts it (`diavasi serve`) and talks to it (`diavasi group`, `diavasi status`, `diavasi tui`). The data plane is a TLS gRPC stream, `DataPlane.Consume`. The server owns the cursor. A client acks by `batch_id` and stores nothing.

### Why

A consumer group usually means copying the data into a log. Diavasi leaves the rows, documents, stream entries, and partitions where they are, and adds the group on the side: shared progress, parallel consumers, and a resume point that survives a process restart.

### When, and when not

Use Diavasi to share one ordered read of an existing table, collection, stream, or partition across workers, and to resume that read after a crash.

Leave these jobs to other systems. They are the non-goals in [docs/architecture.md](docs/architecture.md):

- Ingestion, CDC, and replacing the database.
- Exactly-once delivery.
- Clustering and consensus.
- A stream processor or a Kafka-compatible API.

## Develop with Diavasi

### Get started

Docker runs the server. Python is the first consumer. The server creates a synthetic group named `demo` (eight records, no database) and the example prints them.

```bash
docker compose -f clients/docker-compose.yml --profile python up --abort-on-container-exit
```

The Python container prints `record_ids 1 2 3 4 5 6 7 8` and exits. Compose then stops the server. The first build compiles the server and takes a few minutes.

The same command works with `--profile elixir`, `rust`, `go`, `js`, `java`, `csharp`, or `c`. The walkthrough for a server you start yourself is [docs/tutorials/stage-11-sdks.md](docs/tutorials/stage-11-sdks.md).

### Installation

Build the executable from this repo. The toolchain is the stable channel in `rust-toolchain.toml`. `diavasi serve` and `diavasi test` run the server. `diavasi group`, `diavasi status`, and `diavasi tui` talk HTTP to a server that is already running.

```bash
cargo build -p diavasi
export PATH="$PWD/target/debug:$PATH"
```

The Compose file in Get started is the container path. Database images, for when a group reads a real source, are in [docker-compose.yml](docker-compose.yml):


| Service  | Image                    | Host port |
| -------- | ------------------------ | --------- |
| Postgres | `postgres:16-alpine`     | 5433      |
| MongoDB  | `mongo:7`                | 27017     |
| Redis    | `redis:7-alpine`         | 6379      |
| ScyllaDB | `scylladb/scylla:2026.1` | 9042      |


Postgres is on 5433 so it does not collide with a local server on 5432. Start them with `docker compose up -d`.

### Configuration

`diavasi serve` takes the bind addresses, the store path, and the API token.

```bash
mkdir -p /tmp/diavasi
export DIAVASI_API_TOKEN=dev-token
export DIAVASI_STORE_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef

diavasi serve \
  --bind 127.0.0.1:7700 \
  --data-bind 127.0.0.1:7710 \
  --store /tmp/diavasi/meta.redb \
  --token "$DIAVASI_API_TOKEN" \
  --store-key "$DIAVASI_STORE_KEY"
```


| Flag          | Environment         | Role                                                                      |
| ------------- | ------------------- | ------------------------------------------------------------------------- |
| `--bind`      |                     | Control plane. Default `127.0.0.1:7700`.                                  |
| `--data-bind` |                     | Data plane, TLS gRPC. Default `127.0.0.1:7710`.                           |
| `--store`     |                     | redb file. The data-plane CA is written next to it as `dataplane-ca.crt`. |
| `--token`     | `DIAVASI_API_TOKEN` | Bearer token for `/v1` and for `DataPlane.Consume`.                       |
| `--store-key` | `DIAVASI_STORE_KEY` | 32-byte hex key for secrets in the store. Required once the store holds a connection. |
| `--tls-cert`, `--tls-key` | | Data-plane certificate and key. Both or neither. When omitted, a local CA and certificate are generated next to the store. |
| `--tls-san` | | Extra DNS name or IP for the generated data-plane certificate, besides `localhost` and `127.0.0.1`. Repeatable. |
| `--http-tls-cert`, `--http-tls-key` | | Control-plane certificate and key. Both or neither. When set, the control plane serves HTTPS. |
| `--checkpoint-interval-ms` | `DIAVASI_CHECKPOINT_INTERVAL_MS` | 0 writes each ack's checkpoint before answering it. A larger value writes at most once per interval; a crash can replay up to one interval of acked records. |


`GET /health` and `GET /ready` need no token. Every `/v1` route and `GET /metrics` require `Authorization: Bearer <token>`. Every route, body, and status code is in [docs/api.md](docs/api.md). Limits, security, restarts, and what to do when a group stops are in [docs/operations.md](docs/operations.md).

### Running

A group with no connection reads the synthetic source.

```bash
export DIAVASI_URL=http://127.0.0.1:7700
export DIAVASI_API_TOKEN=dev-token
export PATH="$PWD/target/debug:$PATH"

diavasi group create --group-id demo --total-records 100
diavasi group start demo
diavasi status
diavasi checkpoint show demo
diavasi group drain demo
diavasi group pause demo
diavasi group delete demo
```

`drain` delivers what the group has already read, reads nothing new, and stops the group when those records are acked. `pause` stops it at once; unacked batches are delivered again on the next start. SIGINT or SIGTERM stops the server cleanly: each group saves its progress, and groups that were running start again with the server.

`--output json` prints machine-readable responses.

`diavasi test` creates a table, collection, or stream named `diavasi_test_<pid>`, inserts rows, starts a temporary server, consumes them, and drops that object when it finishes (`--keep` leaves it). Compose does not preload data, so `-n` and `-b` choose the size.

```bash
diavasi test postgres -n 10000 -b 1024
diavasi test mongo -n 10000 -b 1024
diavasi test redis -n 10000 -b 1024
diavasi test scylla -n 10000 -b 1024
```

Postgres uses `DATABASE_URL` or `postgres://diavasi:diavasi@127.0.0.1:5433/diavasi`. MongoDB uses `MONGODB_URL`, Redis uses `REDIS_URL`, and ScyllaDB uses `SCYLLA_URL` or `127.0.0.1:9042`. The first Scylla start can take a minute. Adapter contracts: [Postgres](docs/adapters/postgresql.md), [MongoDB](docs/adapters/mongodb.md), [Redis](docs/adapters/redis.md), [ScyllaDB](docs/adapters/scylla.md).

### Monitoring

`GET /health` reports that the process is up. `GET /ready` reports that the store can be read. `GET /metrics` is Prometheus text. `diavasi status` lists running groups. `diavasi group diagnostics <id>` reports the committed cursor, the fetched cursor, buffer and in-flight records, ack and replay counters, and why the group last stopped.

`diavasi tui` is that same view, live. It needs `DIAVASI_URL` and `DIAVASI_API_TOKEN`.

```bash
diavasi tui
```

![Diavasi TUI with groups demo and trades. trades is selected and its diagnostics show a full buffer, lag 1024, and last stop paused.](docs/screenshots/diavasi-Screenshot-TUI.png)

`j` and `k` move between groups. `s` starts the selected group, `p` pauses it, `d` drains it, `r` refreshes, and `q` quits. Pane by pane: [docs/observability.md](docs/observability.md).

### Coding consumers

A consumer opens `DataPlane.Consume`, sends `Hello` version 1, joins a group, and yields batches. The caller acks by `batch_id`. The client stores no cursor and does not dedupe on `record_id`, because that field is 0 for Redis and for some Scylla keys. Dropping the stream is how unacked batches return. Reconnect with the same consumer id and the server replays them. If the old stream is still open, the new one takes it over: the old stream receives error 2 and closes.

- [Python](https://github.com/diavasis/diavasi-python) 0.1.0, `pip install diavasi-data==0.1.0`
- [Elixir](https://github.com/diavasis/diavasi-elixir) 0.1.0, Hex `{:diavasi, "~> 0.1.0"}`
- [Rust](https://github.com/diavasis/diavasi-client) 0.1.0, crates.io `diavasi-client = "0.1.0"`
- [Go](https://github.com/diavasis/diavasi-go) 0.1.0, `go get github.com/diavasis/diavasi-go@v0.1.0`
- [JavaScript](https://github.com/diavasis/diavasi-js) 0.1.0, `npm install @diavasi/data@0.1.0`
- [Java](https://github.com/diavasis/diavasi-java) 0.1.0, Maven `dev.diavasi:diavasi-data:0.1.0`
- [C#](https://github.com/diavasis/diavasi-dotnet) 0.1.0, NuGet `Diavasi.Data`
- [C](https://github.com/diavasis/diavasi-c) 0.1.0, git tag `v0.1.0`
- [Zig](https://github.com/diavasis/diavasi-zig) 0.1.0, which calls the C library

### FAQ

**Who stores the cursor?** The server, in the redb store. Clients ack batches. They do not checkpoint.

**What happens to a batch that was not acked?** The server keeps it. The next stream with the same consumer id receives it again.

**Why can `record_id` be 0?** Redis stream ids and some Scylla keys do not fit in one integer. Identity is the server's cursor, not `record_id`.

**What does a bad token return?** gRPC status `UNAUTHENTICATED` and the message `unauthorized`.

**What does a group that is not running return?** Protocol error 5. Codes 1 through 8 are in [ADR 0007](docs/adr/0007-protocol-v1.md). The client session is [ADR 0012](docs/adr/0012-client-sdks.md).

### Use cases and examples

- Synthetic group, no database: the Get started command, and [docs/tutorials/stage-11-sdks.md](docs/tutorials/stage-11-sdks.md).
- Postgres keyset: [docs/tutorials/stage-06-postgres.md](docs/tutorials/stage-06-postgres.md).
- MongoDB find: [docs/tutorials/stage-08-mongodb.md](docs/tutorials/stage-08-mongodb.md).
- Redis Streams: [docs/tutorials/stage-09-redis.md](docs/tutorials/stage-09-redis.md).
- ScyllaDB partition or token scan: [docs/tutorials/stage-10-scylla.md](docs/tutorials/stage-10-scylla.md).

### Client tools

Language, install, and the Compose profile for each SDK are in [clients/README.md](clients/README.md). That page also covers the JupyterLab image and the Elixir Livebook.

## Road map


| Release/Stage | Focus                                                                                                           | Status  |
| ------------- | --------------------------------------------------------------------------------------------------------------- | ------- |
| v0.0.0        | Workspace/CI + data-plane transport bake-off                                                                    | Done    |
| v0.1.0        | In-memory consumer-group domain                                                                                 | Done    |
| v0.2.0        | Durable metadata store (redb)                                                                                   | Done    |
| v0.3.0        | Supervised per-group Tokio runtime                                                                              | Done    |
| v0.4.0        | HTTP control plane + CLI                                                                                        | Done    |
| v0.5.0        | Protocol v1 data plane (TLS gRPC, auth, backpressure)                                                           | Done    |
| v0.5.0/Demo   | Livebook: server, synthetic group, Python and Elixir clients ([notebook](https://github.com/diavasis/diavasi-elixir/blob/main/notebooks/demo.livemd)) | Done    |
| v0.6.0        | PostgreSQL adapter. After it lands, Docker Compose replaces the synthetic source                                | Done    |
| v0.7.0        | End-to-end Postgres benchmarks / resource model                                                                 | Done    |
| v0.8.0        | MongoDB adapter. Object `_id` or a declared sort; resume is a `find` keyset                                     | Done    |
| v0.9.0        | Redis adapter. Stream id order; resume is `XGROUP SETID` plus `XREADGROUP`                                      | Done    |
| v0.10.0       | ScyllaDB adapter. One partition in clustering order, or an explicit token scan. Resume is the logical key       | Done    |
| v0.11.0       | Thin SDKs (Elixir, Rust, Python, Go, JavaScript, Java, C#, C)                                                   | Done    |
| v0.12.0       | Metrics, soak, operator diagnostics, and a ratatui client of the HTTP API (`diavasi tui`)                       | Done    |
| v0.13.0       | S3 adapter. Object key is the order; resume is `ListObjects` `StartAfter`                                       | Planned |
| v0.14.0       | Reconciliation research (ADR only)                                                                              | Planned |
| v0.15.0       | Tauri 2 app on the same HTTP API                                                                                | Planned |


Stage tutorials and reviews live under [docs/](docs/).

## Developing Diavasi

### Building

```bash
docker compose up -d
./scripts/check.sh
```

The script runs `cargo fmt`, `cargo clippy`, `cargo test`, `cargo deny`, and `cargo llvm-cov`. It keeps `DATABASE_URL`, `MONGODB_URL`, `REDIS_URL`, and `SCYLLA_URL` when those are already set, and otherwise uses the Compose URLs above so the adapter tests run. Line coverage must stay at or above 85%. The transport harness and the end-to-end bench are excluded from that number.

`clients/scripts/compat.sh` starts a synthetic group, clones the client repositories at `v0.1.0`, and runs each SDK. CI runs that suite in the `sdks` job. `check`, `coverage`, and `deny` do not install those toolchains. Push the client repositories before that job can succeed.

The C client image builds `diavasi_consume`, joins the synthetic group `demo`, and exits. Compose then stops the server. The first build compiles the server.

```bash
docker compose -f clients/docker-compose.yml --profile c up --abort-on-container-exit
```

### Architecture

[docs/architecture.md](docs/architecture.md) is the map of the running system: one process, one supervised runtime per group, a logical cursor in redb, and adapters that only implement `RecordSource`.


| Crate                      | Role                                                             |
| -------------------------- | ---------------------------------------------------------------- |
| `diavasi-core`             | Library (core, store, runtime, control plane, data plane)        |
| `diavasi`                  | Executable: `serve`, `test`, and the admin commands, including `tui` |
| `diavasi-adapter-postgres` | PostgreSQL keyset source                                         |
| `diavasi-adapter-mongodb`  | MongoDB find keyset source                                       |
| `diavasi-adapter-redis`    | Redis Streams source                                             |
| `diavasi-adapter-scylla`   | ScyllaDB partition and token-scan source                         |


## License

Apache-2.0