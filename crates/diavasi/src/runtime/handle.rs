use std::time::Duration;

use tokio::sync::{mpsc, oneshot};

use crate::core::{Batch, BatchId, ConsumerId, GroupId, LogicalCursor};

use super::command::{BufferStats, RuntimeCommand};
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

    pub async fn stop(&self) -> RuntimeResult<()> {
        self.call(|reply| RuntimeCommand::Stop { reply }).await
    }
}
