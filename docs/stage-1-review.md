# Stage 1 review

## What changed

- New `diavasi::core` domain: ids, ordering/cursors, lifecycle, bounded buffer, synthetic source, contiguous ACK tracker, in-flight tracker, consumer registry, `GroupEngine`
- Scenario tests and `proptest` properties for cursor/gap/backpressure/restart invariants
- ADR 0002 (batch ACK + contiguous commit)
- Tutorial: [docs/tutorials/stage-01-core-domain.md](tutorials/stage-01-core-domain.md)

## Tests added

- Unit: ordering, lifecycle, buffer, source, ack gap behavior
- Scenario: one consumer, multi-consumer, out-of-order ACK, leave, timeout, duplicate ACK, restart snapshot, backpressure
- Properties: monotonic committed cursor, fair drain without loss, crash mid-flight replay

## Commands run

```text
cargo test -p diavasi --lib core:: --all-features
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all --all-features
cargo deny check
```

## Remaining risks

- Contiguous tracker assumes delivered keys are noted in traversal order via assign; correct for Stage 1 synthetic source
- `Duration` in `GroupConfig` serde relies on serde's Duration support for snapshots
- Module name `core` shadows the usual path mnemonic; use `diavasi::core` / `crate::core` explicitly

## Deviations

- None material vs Stage 1 plan

## Acceptance

- [x] Architecture invariants testable without a database are executable and passing
- [x] ADR 0002 written
- [x] Stage tutorial written and linked
- [x] Quality gate (fmt, clippy, test, deny)

**Stop.** Await explicit instruction before Stage 2.
