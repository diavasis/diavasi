//! One task per running group, and the supervisor that starts, stops, and
//! restarts them.
//!
//! Each group has an owner task that is the only code touching its
//! [`DurableGroup`](crate::store::DurableGroup). Other code sends it
//! [`RuntimeCommand`]s through a [`GroupHandle`]. An adapter source runs in
//! a separate fetch task, so the owner never waits on the network.
//! [`GroupSupervisor`] opens groups from the store, restarts failed ones
//! with backoff, and pauses or shuts them down. Adapters plug in through
//! [`SourceFactory`].

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
