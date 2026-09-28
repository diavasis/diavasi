use thiserror::Error;

/// Result of a [`GroupEngine`](super::GroupEngine) operation.
pub type CoreResult<T> = Result<T, CoreError>;

/// Why the group engine refused an operation.
///
/// ```
/// use diavasi::core::{ConsumerId, CoreError};
/// let err = ConsumerId::new("").unwrap_err();
/// assert!(matches!(err, CoreError::InvalidArgument(_)));
/// ```
#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum CoreError {
    /// An argument was out of range, such as an empty id or a zero cap.
    #[error("invalid argument: {0}")]
    InvalidArgument(String),
    /// The lifecycle cannot move from `from` to `to`, for example a drain of a stopped group.
    #[error("invalid lifecycle transition from {from:?} to {to:?}")]
    InvalidTransition {
        /// The state the group was in.
        from: crate::core::lifecycle::GroupLifecycle,
        /// The state that was requested.
        to: crate::core::lifecycle::GroupLifecycle,
    },
    /// The group is not in a state that accepts this operation; the value is its state.
    #[error("group is not running (state={0:?})")]
    NotRunning(crate::core::lifecycle::GroupLifecycle),
    /// The consumer id has not joined the group.
    #[error("consumer not joined: {0}")]
    UnknownConsumer(String),
    /// The consumer id has already joined the group.
    #[error("consumer already joined: {0}")]
    DuplicateConsumer(String),
    /// The read-ahead buffer has no room for the record.
    #[error("buffer at capacity")]
    BufferFull,
    /// Nothing is buffered to assign. Try again after the next fetch.
    #[error("no work available to assign")]
    NoWork,
    /// No batch with this id is in flight.
    #[error("batch not in flight: {0}")]
    UnknownBatch(u64),
}
