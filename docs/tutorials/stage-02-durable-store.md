# Tutorial: Stage 2 durable store

This tutorial teaches Stage 2 Diavasi durability: what is persisted, why the fetched/committed boundary matters under crashes, and how recovery tests prove no silent loss.

Related code: `crates/diavasi-core/src/store/`. Decision record: [ADR 0003](../adr/0003-redb-metadata-store.md). Stage 1 domain: [stage-01 tutorial](stage-01-core-domain.md).

## 1. What Stage 2 is

Stage 2 adds a **durable metadata store** and a thin wrapper around `GroupEngine`:

- `StateStore`: the persistence trait for connections, groups, and checkpoints.
- `RedbStore`: the redb implementation (pure Rust, ACID transactions).
- `DurableGroup`: an engine with a store. It writes when the committed cursor advances.
- Crash-injection hooks for recovery tests

Still absent: Tokio group runtime (Stage 3), HTTP/CLI lifecycle (Stage 4), production data plane (Stage 5), real DB adapters.

**Guarantee:** after process death, reopen resumes at or before the last successful durable commit and may replay uncertain work. Diavasi must not omit records that still belong in the traversable result set.

## 2. Why this shape

### Fetched vs committed (durability boundary)

```text
committed          fetched
    |                 |
    v                 v
[ACK][ACK][IN-FLIGHT][BUFFER]
         ^
         |
    not durable
```

Only **committed** is written to redb. Buffer, in-flight batches, and fetched cursor live in memory. After a crash they are gone; the source is re-read from the durable cursor (at-least-once).

### Why redb

Metadata is key/value shaped. redb gives ACID transactions and crash-safe defaults without a C toolchain. The `StateStore` trait keeps SQLite replaceable later.

### Why seal secrets now

Connection records are stored early so Stage 4+ control plane can manage them. Secrets are sealed with ChaCha20-Poly1305 under `DIAVASI_STORE_KEY` so a raw DB file copy does not expose plaintext passwords.

## 3. Core concepts

### StateStore

```text
put/get/list/delete connection
put/get/list/delete group
load_checkpoint / commit_checkpoint
commit_progress(group, cursor)   # atomic group + checkpoint
```

`commit_progress` is one redb write transaction. Durability is observed only after `commit()` returns `Ok`.

### DurableGroup

```text
create / open
start, join_consumer, poll_fetch, assign_batch
ack(batch_id) -> maybe commit_progress
snapshot_to_store()  # controlled shutdown
```

`ack` calls `GroupEngine::ack`. If `committed_cursor` advanced, it runs crash hooks and then `commit_progress`.

### Crash points

```text
AfterDeliver
AfterAckApplied
BeforeCheckpointCompute
BeforeTxnBegin
BeforeTxnCommit    # after staging writes, before redb commit
AfterTxnCommit
AfterAckResponse
```

Tests abort at a point, drop the process-local state, reopen the same DB file, and drain to completion.

## 4. How to use (tests as the Stage 2 “how”)

Create and advance:

```rust
let store = Arc::new(RedbStore::create(path)?);
let mut g = DurableGroup::create(store.clone(), config, "synthetic-u64")?;
g.start()?;
g.join_consumer(consumer)?;
g.poll_fetch()?;
let batch = g.assign_batch(&consumer)?;
g.ack(batch.id)?;  // persists if committed advanced
```

Recover after “crash”:

```rust
let store = Arc::new(RedbStore::open(path)?);
let mut g = DurableGroup::open(store, &group_id)?;
// committed_cursor == last durable checkpoint
g.join_consumer(consumer)?;
// continue poll / assign / ack; expect possible replays
```

Seal a connection secret:

```rust
let key = StoreKey::from_env_or_generate()?;
let sealed = seal_secret(&key, password_bytes)?;
// store ConnectionRecord { sealed_secret: sealed, ... }
// later adapters: open_secret(&key, &record.sealed_secret)?
```

## 5. How to run recovery tests

From the repo root:

```bash
cargo test -p diavasi-core --lib store::
```

Important cases:

| Test | What it proves |
| --- | --- |
| `ack_advances_durable_cursor` | Reopen sees the new cursor |
| `crash_before_txn_commit_*` | Reopen at previous cursor; work replayed |
| `crash_after_txn_commit_*` | Reopen at new cursor |
| `each_crash_point_allows_safe_resume` | Every hook point still drains fully |
| `restart_soak_no_omissions` | Repeated mid-drain crashes; every key delivered ≥ once |
| `connection_crud_secrets_sealed` | Ciphertext on disk; plaintext password not in DB bytes |

## 6. Misconceptions

- **“Fetched is safe.”** No. Only committed is durable.
- **“Crash after ACK always means the consumer’s work is checkpointed.”** Only if `commit_progress` returned `Ok`. In-memory ACK without a durable commit is replayed.
- **“redb means exactly-once.”** No. Diavasi is at-least-once; apps need idempotency for exactly-once effects.
- **“We persist in-flight to avoid duplicates.”** Deliberately not. Replaying uncertain work is preferred over risking silent loss.
- **“List connections returns passwords.”** No. Secrets stay sealed; `open_secret` is explicit and for adapters later.

## 7. What Stage 2 does not do

- Supervise groups under Tokio
- Expose HTTP/CLI for connections and groups
- Speak the production gRPC data plane
- Talk to Postgres/Mongo/Redis/Scylla

Those arrive in later stages on top of this store.
