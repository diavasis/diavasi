use thiserror::Error;

use crate::core::{CoreError, SourceError};
use crate::store::StoreError;

/// Result of a runtime operation.
pub type RuntimeResult<T> = Result<T, RuntimeError>;

/// Why a group operation or task failed.
#[derive(Debug, Error)]
pub enum RuntimeError {
    /// The store failed.
    #[error(transparent)]
    Store(StoreError),

    /// The group engine refused the operation.
    #[error(transparent)]
    Core(#[from] CoreError),

    /// The group has no running task.
    #[error("group not running: {0}")]
    GroupNotRunning(String),

    /// The group already runs or is starting.
    #[error("group already running: {0}")]
    GroupAlreadyRunning(String),

    /// The ack was applied but its checkpoint write failed; the records may be delivered again after a restart.
    #[error("checkpoint write failed: {0}")]
    CheckpointFailed(String),

    /// The group task has exited.
    #[error("runtime command channel closed")]
    ChannelClosed,

    /// The group task did not accept the command within 5 seconds.
    #[error("runtime command channel full")]
    ChannelFull,

    /// The group task panicked or was aborted.
    #[error("runtime task panicked or was aborted")]
    TaskFailed,

    /// Reading the source failed. See [`SourceError`] for what happens next.
    #[error("{0}")]
    Source(SourceError),
}

impl From<StoreError> for RuntimeError {
    fn from(err: StoreError) -> Self {
        match err {
            StoreError::Core(core) => Self::Core(core),
            other => Self::Store(other),
        }
    }
}
