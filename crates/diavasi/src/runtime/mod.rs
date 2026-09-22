//! Per-group Tokio runtime and supervisor (Stage 3).
//!
//! One owner task per group mutates [`DurableGroup`](crate::store::DurableGroup)
//! via a bounded command mailbox. No shared `Mutex` around the engine.

mod command;
mod error;
mod group_runtime;
mod handle;
mod supervisor;

#[cfg(test)]
mod tests;

pub use command::{BufferStats, RuntimeCommand};
pub use error::{RuntimeError, RuntimeResult};
pub use group_runtime::{GroupRuntimeConfig, spawn_group_runtime};
pub use handle::GroupHandle;
pub use supervisor::GroupSupervisor;
