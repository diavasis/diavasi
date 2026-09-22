use tokio::sync::oneshot;

use crate::core::{Batch, BatchId, ConsumerId, GroupLifecycle, LogicalCursor};

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
    Assign {
        consumer: ConsumerId,
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
    Drain {
        reply: oneshot::Sender<RuntimeResult<()>>,
    },
    /// Wake the owner to pull from the synthetic source into the buffer.
    Fetch,
    /// Wake the owner to requeue timed-out in-flight batches.
    Tick,
    /// Graceful stop: snapshot then exit the owner loop.
    Stop {
        reply: oneshot::Sender<RuntimeResult<()>>,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferStats {
    pub buffer_len: usize,
    pub buffer_bytes: usize,
    pub inflight_len: usize,
    pub max_buffer_records: usize,
    pub max_buffer_bytes: usize,
}
