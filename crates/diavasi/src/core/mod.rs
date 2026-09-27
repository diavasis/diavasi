//! Database-neutral consumer-group domain (Stage 1).
//!
//! In-memory only: no networking, no SQLite, no Tokio supervision.

mod ack;
mod buffer;
mod consumers;
pub mod encoding;
mod error;
mod group;
mod ids;
mod inflight;
mod lifecycle;
mod ordering;
mod record;
mod source;

#[cfg(test)]
mod proptests;

pub use ack::ContiguousCommitTracker;
pub use buffer::BoundedBuffer;
pub use consumers::ConsumerRegistry;
pub use error::{CoreError, CoreResult};
pub use group::{AckOutcome, GroupConfig, GroupEngine, GroupSnapshot, MAX_BATCH_BYTES};
pub use ids::{BatchId, ConsumerId, GroupId};
pub use inflight::{Assignment, InFlightTracker};
pub use lifecycle::GroupLifecycle;
pub use ordering::{LogicalCursor, OrderingAtom, OrderingValue};
pub use record::{Batch, Record};
pub use source::{RecordSource, SourceError, SyntheticSource};
