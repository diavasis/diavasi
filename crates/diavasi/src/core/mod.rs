//! The consumer-group engine, without I/O.
//!
//! [`GroupEngine`] holds one group's state: a bounded read-ahead buffer
//! ([`BoundedBuffer`]), the joined consumers ([`ConsumerRegistry`]), the
//! batches assigned and not yet acked ([`InFlightTracker`]), and the committed
//! cursor ([`ContiguousCommitTracker`]). Records carry an [`OrderingValue`];
//! the committed cursor only moves forward, and only across a contiguous run
//! of acked records, so a restart from it never skips a record.
//!
//! Sources implement [`RecordSource`]. [`SyntheticSource`] is the built-in
//! source for tests and demos. Nothing here touches the network, the store,
//! or a runtime: [`crate::store`] adds durability and [`crate::runtime`] adds
//! the task that drives an engine.

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
