# Diavasi

Durable consumer groups for existing databases.

## Introduction

Diavasi adds durable, resumable, parallel consumer-group semantics on top of data you already store. Producers keep writing their databases as they do today. Diavasi does not own ingestion or replace your database.

The central idea:

```text
connection + query + ordering contract + consumer group
  -> durable, resumable, parallel-consumable stream
```

Progress is a logical checkpoint, not a live database cursor. Delivery is at-least-once: on ambiguity, Diavasi replays rather than skips. Control plane (HTTP + CLI) and data plane stay separate.

Early development: Stages 0–8 are in place (transport bake-off, core domain, durable store, group supervision, control plane, TLS gRPC data plane, PostgreSQL keyset adapter, end-to-end benchmark and resource model, MongoDB find adapter). Redis and ScyllaDB adapters are still ahead.

## Roadmap

| Release/Stage | Focus | Status |
| --- | --- | --- |
| v0.0.0 | Workspace/CI + data-plane transport bake-off | Done |
| v0.1.0 | In-memory consumer-group domain | Done |
| v0.2.0 | Durable metadata store (redb) | Done |
| v0.3.0 | Supervised per-group Tokio runtime | Done |
| v0.4.0 | HTTP control plane + CLI | Done |
| v0.5.0 | Protocol v1 data plane (TLS gRPC, auth, backpressure) | Done |
| v0.5.0/Demo | Livebook: server, synthetic group, Python and Elixir clients ([notebook](clients/elixir/notebooks/demo.livemd)) | Next |
| v0.6.0 | PostgreSQL adapter. After it lands, Docker Compose replaces the synthetic source | Done |
| v0.7.0  | End-to-end Postgres benchmarks / resource model | Done |
| v0.8.0 | MongoDB adapter. Object `_id` or a declared sort; resume is a `find` keyset | Done |
| v0.9.0–v0.10.0 | Redis and ScyllaDB adapters | Planned |
| v0.11.0  | Thin SDKs (Elixir, Rust, Python, Go) | Planned |
| v0.12.0  | Metrics, soak, operator diagnostics. First a ratatui client of the HTTP API, then a Tauri 2 app on the same API | Planned |
| v0.13.0  | S3 adapter. Object key is the order; resume is `ListObjects` `StartAfter` | Planned |
| v0.14.0  | Reconciliation research (ADR only) | Planned |

Stage tutorials and reviews live under [`docs/`](docs/). Architecture: [`docs/architecture.md`](docs/architecture.md).

## Quick Start and Guide

Requires a recent Rust stable toolchain (see `rust-toolchain.toml`).

### Build

```bash
cargo build -p diavasi-cli
export PATH="$PWD/target/debug:$PATH"
```

### Run a local control plane

```bash
mkdir -p /tmp/diavasi
export DIAVASI_API_TOKEN=dev-token
export DIAVASI_STORE_KEY=0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef

diavasi serve \
  --bind 127.0.0.1:7700 \
  --store /tmp/diavasi/meta.redb \
  --token "$DIAVASI_API_TOKEN" \
  --store-key "$DIAVASI_STORE_KEY"
```

### Drive a synthetic group lifecycle

In another shell:

```bash
export DIAVASI_URL=http://127.0.0.1:7700
export DIAVASI_API_TOKEN=dev-token
export PATH="$PWD/target/debug:$PATH"

diavasi connection add \
  --id demo-pg \
  --kind synthetic \
  --config-json '{"host":"localhost"}' \
  --secret 'never-echoed-again'

diavasi group create --group-id demo --total-records 100
diavasi group start demo
diavasi status
diavasi checkpoint show demo
diavasi group drain demo
diavasi group pause demo
diavasi group delete demo
diavasi connection delete demo-pg
```

The group has no connection, so it reads the synthetic source. The connection commands above only exercise the control plane.

`GET /health` needs no token. All `/v1` routes require `Authorization: Bearer <token>` (or `--token` / `DIAVASI_API_TOKEN` on the CLI).

Use `--output json` for machine-readable responses.

### Learn more

- Stage 4 walkthrough: [docs/tutorials/stage-04-control-plane.md](docs/tutorials/stage-04-control-plane.md)
- Control-plane ADR: [docs/adr/0005-control-plane.md](docs/adr/0005-control-plane.md)
- Transport bake-off: [docs/transport-benchmark.md](docs/transport-benchmark.md)
- Postgres end-to-end benchmark: [docs/bench/stage-07.md](docs/bench/stage-07.md)

## Development

Start the database backends:

```bash
docker compose up -d
```

Postgres is published on host port 5433 so it does not collide with a local server on 5432. The adapter tests use `postgres://diavasi:diavasi@127.0.0.1:5433/diavasi`. MongoDB listens on `27017`, Redis on `6379`, and ScyllaDB on `9042`. Images are the smallest official variants: `postgres:16-alpine`, `redis:7-alpine`, `mongo:7` (no Alpine build), and `scylladb/scylla:2026.1`.

```bash
./scripts/check.sh
```

The script runs `cargo fmt`, `cargo clippy`, `cargo test`, `cargo deny`, and `cargo llvm-cov`. It keeps `DATABASE_URL` and `MONGODB_URL` when those are already set, and otherwise uses the Compose URLs above so the PostgreSQL and MongoDB adapter tests run. Without a reachable database those tests skip and the adapter is reported as uncovered. Line coverage must stay at or above 85%. The transport harness and the end-to-end bench are excluded from that number.

## Workspace

| Crate | Role |
| --- | --- |
| `diavasi` | Server library (core, store, runtime, control plane) |
| `diavasi-cli` | Admin CLI (`diavasi`) |
| `diavasi-adapter-postgres` | PostgreSQL keyset source |
| `diavasi-adapter-mongodb` | MongoDB find keyset source |
| `diavasi-adapter-redis` | Redis source (planned) |
| `diavasi-adapter-scylla` | ScyllaDB source (planned) |

## License

Apache-2.0
