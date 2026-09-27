# Tutorial: Stage 3 group runtime

This tutorial teaches Stage 3 Diavasi supervision: why each consumer group gets its own Tokio task tree, how the single-owner mailbox works, and how recovery from the durable store is proven.

Related code: `crates/diavasi/src/runtime/`. Decision record: [ADR 0004](../adr/0004-group-supervision.md). Prior stages: [Stage 1](stage-01-core-domain.md), [Stage 2](stage-02-durable-store.md).

## 1. What Stage 3 is

Stage 3 wraps Stage 2 `DurableGroup` in a supervised async runtime:

- `GroupSupervisor`: starts and stops groups, and restarts a group that fails.
- `GroupRuntime`: one owner task per group, with a fetch task and two tickers.
- `GroupHandle`: the in-process client (join, assign, ack).

Still absent: HTTP/CLI (Stage 4), production gRPC data plane (Stage 5), real DB adapters.

**Guarantee:** aborting or panicking one group does not stop others; the killed group can be respawned from the last durable committed cursor and finish without Diavasi-caused omissions.

## 2. Why this shape

### One owner, not a shared mutex

Stage 1 already proved correctness with a synchronous engine. Sharing that engine under `Mutex` across assign/ack/fetch tasks would reintroduce races and lock contention. Instead, all mutations go through one mailbox:

```text
clients --RuntimeCommand--> owner task --> DurableGroup --> StateStore
                                ^
                     Fetch/Tick children
```

### Isolation

Each group is its own task tree. `abort_group` cancels only that tree. Unrelated groups keep consuming.

### Recover from store, not from RAM

In-flight assignments are not durable (Stage 2). After abort, `supervise_once` opens `DurableGroup` from redb and resumes at the committed cursor (at-least-once replay).

## 3. Core concepts

### GroupSupervisor

```text
start_group(id)   // DurableGroup::open + spawn runtime
stop_group(id)    // graceful snapshot + exit, no respawn
abort_group(id)   // hard kill (tests / fault injection)
supervise_once()  // join finished tasks; respawn unexpected exits
```

### GroupHandle

```text
join / leave / assign / ack / snapshot_cursor / buffer_stats / stop
```

Commands are sent on a bounded `mpsc` channel; replies use `oneshot`.

### Fetch and tick children

- Fetch loop periodically asks the owner to `poll_fetch`
- Tick loop asks the owner to `tick` (batch timeouts)
- If the command channel is closed, children exit

## 4. How to use (tests as the Stage 3 “how”)

Persist a synthetic group, then supervise it:

```rust
let store = Arc::new(RedbStore::create(path)?);
let _ = DurableGroup::create(store.clone(), config, "synthetic-u64")?;
let mut sup = GroupSupervisor::new(store);
let handle = sup.start_group(&group_id).await?;
handle.join(consumer).await?;
let batch = handle.assign(&consumer).await?;
handle.ack(batch.id).await?;
```

Recover after a kill:

```rust
sup.abort_group(&group_id)?;
tokio::time::sleep(Duration::from_millis(20)).await;
let recovered = sup.supervise_once().await?;
let handle = sup.get_handle(&group_id).unwrap();
// cursor == last durable checkpoint; continue assign/ack
```

## 5. How to run Stage 3 tests

```bash
cargo test -p diavasi --lib runtime::
```

| Test | What it proves |
| --- | --- |
| `many_groups_isolated` | Concurrent drains across groups |
| `kill_one_others_healthy` | Abort A; B and C still finish |
| `killed_group_recovers` | Respawn resumes at durable cursor |
| `repeated_kill_recover_soak` | Multiple kills; full drain; no omissions |
| `backpressure_under_runtime` | Buffer fills to cap; fetch stalls |
| `caps_hold_while_consuming` | Buffer caps hold while a consumer drains the source |
| `stop_group_no_respawn` | Clean stop does not respawn |

## 6. Misconceptions

- **“Supervision replaces durability.”** No. Checkpoints still happen only in `DurableGroup::ack`.
- **“Abort loses committed work.”** Committed cursor in redb survives; unacked work may replay.
- **“Consumers talk to the engine directly.”** Stage 3 consumers use `GroupHandle` only.
- **“This is the production data plane.”** Network protocol arrives in Stage 5; Stage 3 is in-process.

## 7. What Stage 3 does not do

- HTTP control plane / CLI lifecycle commands
- TLS gRPC delivery to external consumers
- Real Postgres/Mongo/Redis/Scylla adapters
