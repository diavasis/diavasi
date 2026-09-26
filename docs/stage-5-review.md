# Stage 5 review: production data plane

**Status:** complete. Stop for review before Stage 6.

## Delivered

- Frozen transport: gRPC over HTTP/2 with TLS ([ADR 0006](adr/0006-data-plane-transport.md))
- Protocol v1 `diavasi.data.v1` ([ADR 0007](adr/0007-protocol-v1.md))
- `diavasi::dataplane` session mapped onto `GroupHandle` (auth, join, flow control, ack, heartbeat, leave/requeue)
- `diavasi serve` starts the data plane next to the control plane
- Compatibility clients: Rust, Python, and Elixir. Those trees later moved to [diavasi-client](https://github.com/diavasis/diavasi-client), [diavasi-python](https://github.com/diavasis/diavasi-python), and [diavasi-elixir](https://github.com/diavasis/diavasi-elixir). The other languages are listed in [clients/README.md](../clients/README.md).
- Tutorial: [stage-05-data-plane.md](tutorials/stage-05-data-plane.md)

## Guarantees verified

- Missing bearer token is rejected before join.
- A Rust client consumes a synthetic group over TLS and acks every record.
- Flow control holds a slow client to one in-flight batch and the buffer stays within its cap.
- An abrupt disconnect requeues unacked records; a second consumer finishes the group with no omissions.
- Python and Elixir clients consume the same synthetic group against `diavasi serve`.

## Intentionally not in Stage 5

- PostgreSQL or other real adapters (Stage 6)
- Polished multi-language SDKs (Stage 11)
- Prometheus metrics (Stage 12)
- RBAC

## Quality gate

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all --all-features
cargo deny check
```

## Stop

Stage 5 is done. Do not start Stage 6 without an explicit instruction.
