use std::time::Duration;

use tokio::sync::oneshot;

use crate::core::{Batch, BatchId, ConsumerId, GroupLifecycle, LogicalCursor, Record, SourceError};

use super::error::RuntimeResult;

/// Commands sent to the single-owner group runtime task.
pub enum RuntimeCommand {
    /// Add a consumer to the group.
    Join {
        /// The consumer to add.
        consumer: ConsumerId,
        /// Receives the result.
        reply: oneshot::Sender<RuntimeResult<()>>,
    },
    /// Remove a consumer and return its batches in flight to the buffer.
    Leave {
        /// The consumer to remove.
        consumer: ConsumerId,
        /// Receives the result.
        reply: oneshot::Sender<RuntimeResult<()>>,
    },
    /// Assign a batch now, or answer `NoWork`.
    Assign {
        /// The consumer that receives the batch.
        consumer: ConsumerId,
        /// Receives the batch or `NoWork`.
        reply: oneshot::Sender<RuntimeResult<Batch>>,
    },
    /// Assign a batch, waiting up to `wait` for records to arrive before
    /// answering `NoWork`.
    AssignWait {
        /// The consumer that receives the batch.
        consumer: ConsumerId,
        /// Longest time to wait for records.
        wait: Duration,
        /// Receives the batch, or `NoWork` after `wait`.
        reply: oneshot::Sender<RuntimeResult<Batch>>,
    },
    /// Complete a batch.
    Ack {
        /// The batch to complete.
        batch_id: BatchId,
        /// Receives the result after the checkpoint is written (see `checkpoint_interval`).
        reply: oneshot::Sender<RuntimeResult<()>>,
    },
    /// Read the committed cursor.
    SnapshotCursor {
        /// Receives the cursor.
        reply: oneshot::Sender<RuntimeResult<LogicalCursor>>,
    },
    /// Read buffer and in-flight counts.
    BufferStats {
        /// Receives the counts.
        reply: oneshot::Sender<RuntimeResult<BufferStats>>,
    },
    /// Read the lifecycle.
    Lifecycle {
        /// Receives the lifecycle.
        reply: oneshot::Sender<RuntimeResult<GroupLifecycle>>,
    },
    /// List the joined consumers.
    ListConsumers {
        /// Receives the sorted consumer ids.
        reply: oneshot::Sender<RuntimeResult<Vec<ConsumerId>>>,
    },
    /// Read positions, counts, and consumers in one call.
    LiveSnapshot {
        /// Receives the snapshot.
        reply: oneshot::Sender<RuntimeResult<LiveSnapshot>>,
    },
    /// Start a drain. See [`GroupEngine::drain`](crate::core::GroupEngine::drain).
    Drain {
        /// Receives the result.
        reply: oneshot::Sender<RuntimeResult<()>>,
    },
    /// Wake the owner to consider a fetch. Adapter reads run in a separate
    /// fetch task and come back as [`RuntimeCommand::Fetched`].
    Fetch,
    /// The fetch task finished a read that started at `cursor`.
    Fetched {
        /// The fetched position when the read started.
        cursor: LogicalCursor,
        /// The records, or why the read failed.
        result: Result<Vec<Record>, SourceError>,
        /// How long the read took.
        latency: Duration,
    },
    /// Wake the owner to requeue timed-out in-flight batches.
    Tick,
    /// Operator pause: record `Stopped`, snapshot, and exit.
    Stop {
        /// Receives the result of the final snapshot.
        reply: oneshot::Sender<RuntimeResult<()>>,
    },
    /// Process shutdown: snapshot without changing the lifecycle, and exit.
    Shutdown {
        /// Receives the result of the final snapshot.
        reply: oneshot::Sender<RuntimeResult<()>>,
    },
}

/// A running group's positions, counts, and consumers at one moment.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveSnapshot {
    /// The lifecycle.
    pub lifecycle: GroupLifecycle,
    /// Last position with every earlier record acked, in memory.
    pub committed: LogicalCursor,
    /// Last position read from the source.
    pub fetched: LogicalCursor,
    /// Records in the buffer.
    pub buffer_records: usize,
    /// Payload bytes in the buffer.
    pub buffer_bytes: usize,
    /// Records assigned and not yet acked.
    pub inflight_records: usize,
    /// Joined consumers, sorted.
    pub consumers: Vec<ConsumerId>,
}

/// Buffer and in-flight counts with the buffer caps.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferStats {
    /// Records in the buffer.
    pub buffer_len: usize,
    /// Payload bytes in the buffer.
    pub buffer_bytes: usize,
    /// Batches assigned and not yet acked.
    pub inflight_len: usize,
    /// The buffer record cap.
    pub max_buffer_records: usize,
    /// The buffer byte cap.
    pub max_buffer_bytes: usize,
}
