# Tutorial: Stage 1 core domain

This tutorial teaches the Stage 1 Diavasi consumer-group domain: what it is, why it is shaped this way, and how the in-memory implementation works. It assumes Rust fluency, not prior Diavasi knowledge.

Related code lives under `crates/diavasi/src/core/`. Related decision record: [ADR 0002](../adr/0002-ack-checkpoint-model.md).

## 1. What Stage 1 is

Stage 1 implements a **database-neutral, in-memory consumer group**:

- One logical group owns one ordered traversal of a synthetic source.
- Multiple consumers share that traversal (they do not each scan the source).
- Progress is tracked with a **logical ordering cursor**, not a live database cursor object.
- Delivery is **at-least-once**: uncertain work is replayed, never silently skipped.

Deliberately absent in Stage 1:

- Networking and the production data plane
- SQLite or any durable store (Stage 2)
- Tokio supervision / real group runtime (Stage 3)
- HTTP control plane and CLI lifecycle (Stage 4)
- Real database adapters (Stage 6+)

**Guarantee (Stage 1, in-memory):** within `GroupEngine`, the committed cursor never advances over an unacknowledged gap, and restart from a snapshot resumes at or before that committed cursor.

**Limitation:** without Stage 2 durability, a process kill loses all state unless the test harness keeps a snapshot.

## 2. Why these abstractions

### Fetched vs committed

The reader may run ahead of acknowledgements because of buffering:

```text
committed
   |
   v
[ACK][ACK][ACK][IN-FLIGHT][BUFFERED][BUFFERED]
                                  ^
                                  |
                               fetched
```

Only **committed** is safe after a crash. Fetched and in-flight work past committed may be replayed.

### One traversal per group

If each consumer scanned the source independently, you would get duplicate full scans and no shared checkpoint. Diavasi makes the **group** the unit that reads the source once and fans out batches.

### At-least-once, never silent loss

When Diavasi is unsure whether a batch was processed (timeout, consumer death, crash before durable commit), it **replays**. Duplicates are acceptable; dropping a record that still belongs in the traversable result set is not.

### Why a total ordering tuple

A single timestamp is unsafe if ties exist. Stage 1 uses `OrderingValue`: a non-empty tuple of atoms (`U64`, `I64`, `Bytes`) with lexicographic order. The synthetic source uses a single `U64` key `1..=N`.

### Why batch ACK first

Batch ACK matches high-throughput delivery and keeps the protocol small. The commit rule does not depend on ACK granularity: committed advances only across contiguous completed records in traversal order.

## 3. Core concepts

### Record and batch

- `Record`: `ordering` + `payload`
- `Batch`: `BatchId` + `ConsumerId` + records assigned together
- ACK names a `BatchId`

### Lifecycle

```text
Stopped -> Starting -> Running
Running -> Draining -> Stopped
Running -> Failed -> Recovering -> Running
```

Illegal transitions return `CoreError::InvalidTransition`. Dispatch (`poll_fetch`, `assign_batch`, `ack`) is allowed in `Running` and `Draining`.

### Work states

```text
available (buffer / requeue front)
   -> assigned / in-flight
   -> acked (feeds contiguous commit)
   -> committed (safe cursor)
```

A record is assigned to at most one live consumer. Leave or timeout moves in-flight records back to the front of the buffer for redelivery.

### Bounded buffer and backpressure

`BoundedBuffer` enforces hard `max_records` and `max_bytes`. `poll_fetch` stops when the next record would not fit. A slow consumer therefore stalls the source reader without unbounded memory growth.

### Contiguous commit

`ContiguousCommitTracker`:

1. When records are assigned, their orderings are noted in delivery order (`open_order`).
2. When a batch is ACKed, those orderings are marked complete (may be out of order).
3. `committed` advances while the front of `open_order` is complete; otherwise it waits on the gap.

## 4. How the code is shaped

| Module | Role |
| --- | --- |
| `ids` | `GroupId`, `ConsumerId`, `BatchId` |
| `ordering` | `OrderingAtom`, `OrderingValue`, `LogicalCursor` |
| `record` | `Record`, `Batch` |
| `lifecycle` | `GroupLifecycle` transitions |
| `buffer` | `BoundedBuffer` |
| `source` | `SyntheticSource` |
| `ack` | `ContiguousCommitTracker` |
| `inflight` | `Assignment`, `InFlightTracker` |
| `consumers` | `ConsumerRegistry` |
| `group` | `GroupEngine`, `GroupConfig`, `GroupSnapshot` |

`GroupEngine` API (synchronous, no async):

```text
start / drain / stop / fail
recover_from(snapshot)
join_consumer / leave_consumer
poll_fetch() -> records added to buffer
assign_batch(consumer) -> Batch
ack(batch_id)
tick(now)  // timeout requeue
snapshot()
```

## 5. Worked examples

### One consumer drain

```rust
use diavasi::core::{ConsumerId, GroupConfig, GroupEngine, GroupId};
use std::time::Duration;

let mut engine = GroupEngine::new(GroupConfig {
    group_id: GroupId::new("demo").unwrap(),
    total_records: 10,
    payload_size: 8,
    max_buffer_records: 8,
    max_buffer_bytes: 4096,
    batch_max_records: 3,
    batch_timeout: Duration::from_secs(30),
}).unwrap();
engine.start().unwrap();
let c = ConsumerId::new("c1").unwrap();
engine.join_consumer(c.clone()).unwrap();

loop {
    let _ = engine.poll_fetch().unwrap();
    match engine.assign_batch(&c) {
        Ok(batch) => engine.ack(batch.id).unwrap(),
        Err(diavasi::core::CoreError::NoWork) => break,
        Err(e) => panic!("{e}"),
    }
}
```

After a full drain, `committed_cursor` is `Some(OrderingValue::single_u64(10))`.

### Out-of-order ACK

Assign batches B1, B2, B3. ACK B2 and B3 first: committed stays at start. ACK B1: committed jumps to the end of the contiguous completed prefix (through B3's records).

### Consumer leave

Assign a batch to C1, then `leave_consumer(C1)`. The batch returns to the buffer front. C2 can `assign_batch` and receive the same orderings again (at-least-once).

### Restart from snapshot

1. ACK some batches so `committed` advances.
2. Assign another batch but do not ACK.
3. `snapshot()` then `GroupEngine::recover_from(snapshot)`.
4. In-flight state is gone. Source resumes after `committed`. Previously unacked records are fetched again and can be assigned.

## 6. Invariants and tests

| Invariant | How it is proven |
| --- | --- |
| Committed never moves backward | scenario + `proptest` random action sequences |
| Committed never skips a gap | `out_of_order_ack_gap`, property tests |
| Restart does not resume past committed | `restart_from_snapshot_replays_uncommitted`, crash property |
| Uncommitted work is redelivered | leave, timeout, crash tests |
| Buffer caps hold | `buffer_backpressure`, property asserts |
| No double-assign to two live consumers | multi-consumer scenarios; one `InFlightTracker` owner |

Run:

```bash
cargo test -p diavasi --lib core:: --all-features
```

A bug would be: advancing `committed` while an earlier ordering remains unacked; losing a source key across `recover_from`; or growing the buffer past configured caps.

## 7. Practicalities and misconceptions

**Misconception:** the durable cursor is a PostgreSQL cursor.  
**Reality:** it is a logical ordering tuple. Database cursors are runtime optimizations only (later stages).

**Misconception:** fetched progress is safe after crash.  
**Reality:** only committed is safe; fetched-ahead records may replay.

**Misconception:** ACK order must match assign order.  
**Reality:** ACK may be out of order; commit waits for gaps.

**Misconception:** Stage 1 already survives `kill -9`.  
**Reality:** only if the test keeps a `GroupSnapshot`. Durability is Stage 2.

## 8. What Stage 2 adds

Stage 2 persists group definition and `committed_cursor` through a narrow `StateStore` (SQLite). In-flight work still will not be durable; recovery continues to replay from committed. The `GroupEngine` snapshot shape is the seam that Stage 2 hardens.


