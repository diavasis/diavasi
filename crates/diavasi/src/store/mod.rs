//! Durable metadata: connections, groups, and committed cursors.
//!
//! [`StateStore`] is the persistence interface and [`RedbStore`] its redb
//! implementation. [`DurableGroup`] wraps a [`GroupEngine`](crate::core::GroupEngine)
//! and writes its committed cursor. Connection secrets are sealed with a
//! [`StoreKey`] ([`seal_secret`], [`open_secret`]). Payloads, buffers, and
//! in-flight batches are not stored; a restart reads them again from the
//! committed cursor. [`CrashHook`] lets tests stop the ack path at each
//! [`CrashPoint`].

mod crash;
mod crypto;
mod durable;
mod error;
mod redb;
mod types;

/// The [`StateStore`] trait.
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
