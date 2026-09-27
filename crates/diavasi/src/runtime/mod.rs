//! Per-group Tokio runtime and supervisor (Stage 3).
//!
//! One owner task per group mutates [`DurableGroup`](crate::store::DurableGroup)
//! via a bounded command mailbox. No shared `Mutex` around the engine.

mod command;
mod error;
mod group_runtime;
mod handle;
mod source_factory;
mod supervisor;

#[cfg(test)]
mod tests;

pub use command::{BufferStats, LiveSnapshot, RuntimeCommand};
pub use error::{RuntimeError, RuntimeResult};
pub use group_runtime::{
    DEFAULT_FETCH_INTERVAL, DEFAULT_IDLE_FETCH_MAX, GroupExit, GroupRuntimeConfig,
    spawn_group_runtime,
};
pub use handle::GroupHandle;
pub use source_factory::{SourceFactory, SourceOpen, check_keys};
pub use supervisor::{
    FailedStart, GroupOutcome, GroupSupervisor, OpenedGroup, RETRY_FIRST, RETRY_MAX,
    RETRY_RESET_AFTER, StartPlan,
};
