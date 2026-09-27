use std::time::Duration;

use tokio::sync::oneshot;

use crate::core::{Batch, BatchId, ConsumerId, GroupLifecycle, LogicalCursor, Record, SourceError};

use super::error::RuntimeResult;

/// Commands sent to the single-owner group runtime task.
pub enum RuntimeCommand {
    Join {
        consumer: ConsumerId,
        reply: oneshot::Sender<RuntimeResult<()>>,
    },
    Leave {
        consumer: ConsumerId,
        reply: oneshot::Sender<RuntimeResult<()>>,
    },
    /// Assign a batch now, or answer `NoWork`.
    Assign {
        consumer: ConsumerId,
        reply: oneshot::Sender<RuntimeResult<Batch>>,
    },
    /// Assign a batch, waiting up to `wait` for records to arrive before
    /// answering `NoWork`.
    AssignWait {
        consumer: ConsumerId,
        wait: Duration,
        reply: oneshot::Sender<RuntimeResult<Batch>>,
    },
    Ack {
        batch_id: BatchId,
        reply: oneshot::Sender<RuntimeResult<()>>,
    },
    SnapshotCursor {
        reply: oneshot::Sender<RuntimeResult<LogicalCursor>>,
    },
    BufferStats {
        reply: oneshot::Sender<RuntimeResult<BufferStats>>,
    },
    Lifecycle {
        reply: oneshot::Sender<RuntimeResult<GroupLifecycle>>,
    },
    ListConsumers {
        reply: oneshot::Sender<RuntimeResult<Vec<ConsumerId>>>,
    },
    LiveSnapshot {
        reply: oneshot::Sender<RuntimeResult<LiveSnapshot>>,
    },
    Drain {
        reply: oneshot::Sender<RuntimeResult<()>>,
    },
    /// Wake the owner to consider a fetch. Adapter reads run in a separate
    /// fetch task and come back as [`RuntimeCommand::Fetched`].
    Fetch,
    /// The fetch task finished a read that started at `cursor`.
    Fetched {
        cursor: LogicalCursor,
        result: Result<Vec<Record>, SourceError>,
        latency: Duration,
    },
    /// Wake the owner to requeue timed-out in-flight batches.
    Tick,
    /// Operator pause: record `Stopped`, snapshot, and exit.
    Stop {
        reply: oneshot::Sender<RuntimeResult<()>>,
    },
    /// Process shutdown: snapshot without changing the lifecycle, and exit.
    Shutdown {
        reply: oneshot::Sender<RuntimeResult<()>>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LiveSnapshot {
    pub lifecycle: GroupLifecycle,
    pub committed: LogicalCursor,
    pub fetched: LogicalCursor,
    pub buffer_records: usize,
    pub buffer_bytes: usize,
    pub inflight_records: usize,
    pub consumers: Vec<ConsumerId>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferStats {
    pub buffer_len: usize,
    pub buffer_bytes: usize,
    pub inflight_len: usize,
    pub max_buffer_records: usize,
    pub max_buffer_bytes: usize,
}
