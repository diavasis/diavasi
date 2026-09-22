# Stage 2 review: durable state and crash recovery

**Status:** complete — stop for review before Stage 3.

## Delivered

- Branch `v0.2.0/Durable_state-crash_recovery` rebased onto Stage 1 (`diavasi::core` present).
- `diavasi::store`:
  - `StateStore` trait
  - `RedbStore` (schema v1: meta, connections, groups, checkpoints)
  - ChaCha20-Poly1305 secret sealing (`DIAVASI_STORE_KEY`)
  - `DurableGroup` + `CrashPoint` injection
- Recovery tests (CRUD, before/after commit crashes, all crash points, restart soak)
- [ADR 0003](adr/0003-redb-metadata-store.md)
- Tutorial: [stage-02-durable-store.md](tutorials/stage-02-durable-store.md)

## Backend choice

**redb** chosen over sled, native_db, and SQLite for Stage 2. Rationale in ADR 0003. `StateStore` keeps a future SQLite backend possible without changing `DurableGroup`.

## Guarantees verified

- Durable cursor advances only with contiguous commit (Stage 1 rule preserved).
- Crash before redb `commit()` → reopen at previous cursor; replay.
- Crash after `commit()` → reopen at new cursor.
- Soak with injected aborts drains fully; every synthetic key assigned ≥ once (at-least-once accounting).
- Connection secret plaintext not present as UTF-8 in the DB file.

## Intentionally not in Stage 2

- Tokio supervision / group runtime (Stage 3)
- HTTP control plane / CLI lifecycle (Stage 4)
- Production data plane (Stage 5)
- Real adapters
- Second store backend

## Quality gate

Run from repo root:

```bash
cargo fmt --check
cargo clippy --all-targets --all-features -- -D warnings
cargo test --all --all-features
cargo deny check
```

## Stop

Stage 2 is done. Do not start Stage 3 without an explicit instruction.
