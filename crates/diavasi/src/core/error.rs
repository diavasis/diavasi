use thiserror::Error;

pub type CoreResult<T> = Result<T, CoreError>;

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CoreError {
    #[error("invalid argument: {0}")]
    InvalidArgument(&'static str),
    #[error("invalid lifecycle transition from {from:?} to {to:?}")]
    InvalidTransition {
        from: crate::core::lifecycle::GroupLifecycle,
        to: crate::core::lifecycle::GroupLifecycle,
    },
    #[error("group is not running (state={0:?})")]
    NotRunning(crate::core::lifecycle::GroupLifecycle),
    #[error("consumer not joined: {0}")]
    UnknownConsumer(String),
    #[error("consumer already joined: {0}")]
    DuplicateConsumer(String),
    #[error("buffer at capacity")]
    BufferFull,
    #[error("no work available to assign")]
    NoWork,
    #[error("batch not in flight: {0}")]
    UnknownBatch(u64),
}
