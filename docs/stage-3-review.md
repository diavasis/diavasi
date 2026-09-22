# Stage 3 review: consumer-group runtime and supervision

**Status:** complete — stop for review before Stage 4.

## Delivered

- Branch `v0.3.0/Consumer-group_runtime-supervision`
- `diavasi::runtime`:
  - `GroupSupervisor` (start / stop / abort / supervise_once)
  - `GroupRuntime` single-owner mailbox + fetch/timeout children
  - `GroupHandle` in-process client
- Isolation, recover, soak, backpressure, and cap tests
- [ADR 0004](adr/0004-group-supervision.md)
- Tutorial: [stage-03-group-runtime.md](tutorials/stage-03-group-runtime.md)

## Guarantees verified

- Multiple synthetic groups drain concurrently.
- Aborting one group leaves others healthy.
- Unexpected exit + `supervise_once` respawns from durable committed cursor.
- Repeated kill/recover soak completes the source without Diavasi-caused omissions.
- Buffer record/byte caps hold under the runtime (including stalled fetch).

## Intentionally not in Stage 3

- HTTP control plane / CLI (Stage 4)
- Production data plane (Stage 5)
- Real database adapters

## Quality gate

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all --all-features
cargo deny check
```

## Stop

Stage 3 is done. Do not start Stage 4 without an explicit instruction.
