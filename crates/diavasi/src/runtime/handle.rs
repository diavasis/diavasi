use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use crate::core::{Batch, BatchId, ConsumerId, GroupId, GroupLifecycle, LogicalCursor};

use super::command::{BufferStats, LiveSnapshot, RuntimeCommand};
use super::error::{RuntimeError, RuntimeResult};

const DEFAULT_SEND_TIMEOUT: Duration = Duration::from_secs(5);

/// Cloneable in-process client for one running group.
#[derive(Clone)]
pub struct GroupHandle {
    pub group_id: GroupId,
    tx: mpsc::Sender<RuntimeCommand>,
}

impl GroupHandle {
    pub(crate) fn new(group_id: GroupId, tx: mpsc::Sender<RuntimeCommand>) -> Self {
        Self { group_id, tx }
    }

    async fn call<T>(
        &self,
        build: impl FnOnce(oneshot::Sender<RuntimeResult<T>>) -> RuntimeCommand,
    ) -> RuntimeResult<T> {
        let (reply_tx, reply_rx) = oneshot::channel();
        self.tx
            .send_timeout(build(reply_tx), DEFAULT_SEND_TIMEOUT)
            .await
            .map_err(|e| match e {
                mpsc::error::SendTimeoutError::Timeout(_) => RuntimeError::ChannelFull,
                mpsc::error::SendTimeoutError::Closed(_) => RuntimeError::ChannelClosed,
            })?;
        reply_rx.await.map_err(|_| RuntimeError::ChannelClosed)?
    }

    pub async fn join(&self, consumer: ConsumerId) -> RuntimeResult<()> {
        self.call(|reply| RuntimeCommand::Join { consumer, reply })
            .await
    }

    pub async fn leave(&self, consumer: &ConsumerId) -> RuntimeResult<()> {
        let consumer = consumer.clone();
        self.call(|reply| RuntimeCommand::Leave { consumer, reply })
            .await
    }

    pub async fn assign(&self, consumer: &ConsumerId) -> RuntimeResult<Batch> {
        let consumer = consumer.clone();
        self.call(|reply| RuntimeCommand::Assign { consumer, reply })
            .await
    }

    /// Assign a batch, waiting up to `wait` for records. Answers `NoWork`
    /// only after the wait, so an idle consumer does not poll.
    pub async fn assign_wait(&self, consumer: &ConsumerId, wait: Duration) -> RuntimeResult<Batch> {
        let consumer = consumer.clone();
        self.call(|reply| RuntimeCommand::AssignWait {
            consumer,
            wait,
            reply,
        })
        .await
    }

    pub async fn ack(&self, batch_id: BatchId) -> RuntimeResult<()> {
        self.call(|reply| RuntimeCommand::Ack { batch_id, reply })
            .await
    }

    pub async fn snapshot_cursor(&self) -> RuntimeResult<LogicalCursor> {
        self.call(|reply| RuntimeCommand::SnapshotCursor { reply })
            .await
    }

    pub async fn buffer_stats(&self) -> RuntimeResult<BufferStats> {
        self.call(|reply| RuntimeCommand::BufferStats { reply })
            .await
    }

    pub async fn lifecycle(&self) -> RuntimeResult<GroupLifecycle> {
        self.call(|reply| RuntimeCommand::Lifecycle { reply }).await
    }

    pub async fn list_consumers(&self) -> RuntimeResult<Vec<ConsumerId>> {
        self.call(|reply| RuntimeCommand::ListConsumers { reply })
            .await
    }

    pub async fn live_snapshot(&self) -> RuntimeResult<LiveSnapshot> {
        self.call(|reply| RuntimeCommand::LiveSnapshot { reply })
            .await
    }

    pub async fn drain(&self) -> RuntimeResult<()> {
        self.call(|reply| RuntimeCommand::Drain { reply }).await
    }

    /// Pause: the group records `Stopped` and its task exits.
    pub async fn stop(&self) -> RuntimeResult<()> {
        self.call(|reply| RuntimeCommand::Stop { reply }).await
    }

    /// Process shutdown: the group saves its progress, keeps its lifecycle,
    /// and its task exits.
    pub async fn shutdown(&self) -> RuntimeResult<()> {
        self.call(|reply| RuntimeCommand::Shutdown { reply }).await
    }
}
