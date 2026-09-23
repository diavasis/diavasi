use thiserror::Error;

use crate::core::CoreError;
use crate::store::StoreError;

pub type RuntimeResult<T> = Result<T, RuntimeError>;

#[derive(Debug, Error)]
pub enum RuntimeError {
    #[error(transparent)]
    Store(StoreError),

    #[error(transparent)]
    Core(#[from] CoreError),

    #[error("group not running: {0}")]
    GroupNotRunning(String),

    #[error("group already running: {0}")]
    GroupAlreadyRunning(String),

    #[error("runtime command channel closed")]
    ChannelClosed,

    #[error("runtime command channel full")]
    ChannelFull,

    #[error("runtime task panicked or was aborted")]
    TaskFailed,

    #[error("source: {0}")]
    Source(String),

    #[error("shutdown")]
    Shutdown,
}

impl From<StoreError> for RuntimeError {
    fn from(err: StoreError) -> Self {
        match err {
            StoreError::Core(core) => Self::Core(core),
            other => Self::Store(other),
        }
    }
}
