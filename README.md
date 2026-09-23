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

Early development: Stages 0–5 are in place (transport bake-off, core domain, durable store, group supervision, control plane, TLS gRPC data plane). Real database adapters are still ahead.

## Roadmap

| Stage | Focus | Status |
| --- | --- | --- |
| 0 | Workspace/CI + data-plane transport bake-off | Done |
| 1 | In-memory consumer-group domain | Done |
| 2 | Durable metadata store (redb) | Done |
| 3 | Supervised per-group Tokio runtime | Done |
| 4 | HTTP control plane + CLI | Done |
| 5 | Protocol v1 data plane (TLS gRPC, auth, backpressure) | Done |
| Demo | Livebook: server, synthetic group, Python and Elixir clients ([notebook](clients/elixir/notebooks/demo.livemd)) | Next |
| 6 | PostgreSQL adapter. After it lands, Docker Compose replaces the synthetic source | Planned |
| 7 | End-to-end Postgres benchmarks / resource model | Planned |
| 8–10 | MongoDB, Redis, ScyllaDB adapters | Planned |
| 11 | Thin SDKs (Elixir, Rust, Python, Go) | Planned |
| 12 | Metrics, soak, operator diagnostics. First a ratatui client of the HTTP API, then a Tauri 2 app on the same API | Planned |
| 13 | Reconciliation research (ADR only) | Planned |

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
  --kind postgres \
  --config-json '{"host":"localhost"}' \
  --secret 'never-echoed-again'

diavasi group create --group-id demo --total-records 100 --connection-id demo-pg
diavasi group start demo
diavasi status
diavasi checkpoint show demo
diavasi group drain demo
diavasi group pause demo
diavasi group delete demo
diavasi connection delete demo-pg
```

`GET /health` needs no token. All `/v1` routes require `Authorization: Bearer <token>` (or `--token` / `DIAVASI_API_TOKEN` on the CLI).

Use `--output json` for machine-readable responses.

### Learn more

- Stage 4 walkthrough: [docs/tutorials/stage-04-control-plane.md](docs/tutorials/stage-04-control-plane.md)
- Control-plane ADR: [docs/adr/0005-control-plane.md](docs/adr/0005-control-plane.md)
- Transport bake-off: [docs/transport-benchmark.md](docs/transport-benchmark.md)

## Development

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all --all-features
cargo deny check
```

## Workspace

| Crate | Role |
| --- | --- |
| `diavasi` | Server library (core, store, runtime, control plane) |
| `diavasi-cli` | Admin CLI (`diavasi`) |
| `diavasi-adapter-postgres` | PostgreSQL source (planned) |
| `diavasi-adapter-mongodb` | MongoDB source (planned) |
| `diavasi-adapter-redis` | Redis source (planned) |
| `diavasi-adapter-scylla` | ScyllaDB source (planned) |

## License

Apache-2.0
