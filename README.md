# Diavasi

Durable consumer groups for existing databases. Turn queries into resumable, parallel streams without changing your producers.

## Status

Early development. Transport selection for the data plane is undecided until Stage 0 benchmarks are reviewed.

## Workspace

| Crate | Role |
| --- | --- |
| `diavasi` | Server library and binaries |
| `diavasi-cli` | Admin CLI |
| `diavasi-adapter-postgres` | PostgreSQL source (planned) |
| `diavasi-adapter-mongodb` | MongoDB source (planned) |
| `diavasi-adapter-redis` | Redis source (planned) |
| `diavasi-adapter-scylla` | ScyllaDB source (planned) |

## Development

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all
```

See [docs/architecture.md](docs/architecture.md) and [docs/transport-benchmark.md](docs/transport-benchmark.md).

## License

Apache-2.0
