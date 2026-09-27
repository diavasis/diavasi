//! Durable metadata store (Stage 2).
//!
//! Persists connection definitions, group definitions, and committed cursors.
//! Does not persist payloads, buffers, or in-flight assignments.

mod crash;
mod crypto;
mod durable;
mod error;
mod redb;
mod types;

pub mod trait_;

pub use crash::{CrashAction, CrashHook, CrashPoint, no_crash};
pub use crypto::{MASTER_KEY_ENV, StoreKey, open_secret, seal_secret};
pub use durable::DurableGroup;
pub use error::{StoreError, StoreResult};
pub use redb::RedbStore;
pub use trait_::StateStore;
pub use types::{ConnectionRecord, GroupRecord, SealedSecret};

#[cfg(test)]
mod tests;
