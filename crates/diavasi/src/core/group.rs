use std::time::{Duration, Instant};

use serde::{Deserialize, Serialize};

use super::ack::ContiguousCommitTracker;
use super::buffer::BoundedBuffer;
use super::consumers::ConsumerRegistry;
use super::error::{CoreError, CoreResult};
use super::ids::{BatchId, ConsumerId, GroupId};
use super::inflight::{Assignment, InFlightTracker};
use super::lifecycle::GroupLifecycle;
use super::ordering::{LogicalCursor, is_after};
use super::record::{Batch, Record};
use super::source::SyntheticSource;

/// Largest payload total in one batch: the 4 MiB default decode limit of gRPC
/// clients, less room for framing. A batch holds at least one record even
/// when that record alone is larger.
pub const MAX_BATCH_BYTES: usize = 4 * 1024 * 1024 - 64 * 1024;

/// What an ack did.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AckOutcome {
    /// The batch was in flight; its records count as complete.
    Applied,
    /// The batch was not in flight: it was already acked, or it timed out
    /// and was requeued, so its records will be delivered again.
    Stale,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GroupConfig {
    pub group_id: GroupId,
    pub total_records: u64,
    pub payload_size: usize,
    pub max_buffer_records: usize,
    pub max_buffer_bytes: usize,
    pub batch_max_records: usize,
    pub batch_timeout: Duration,
}

/// Durable-facing snapshot for Stage 1 restart (in-memory stand-in for Stage 2).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GroupSnapshot {
    pub config: GroupConfig,
    pub lifecycle: GroupLifecycle,
    pub committed_cursor: LogicalCursor,
    pub next_batch_id: u64,
}

/// Synchronous in-memory consumer-group engine.
#[derive(Debug)]
pub struct GroupEngine {
    config: GroupConfig,
    lifecycle: GroupLifecycle,
    source: SyntheticSource,
    buffer: BoundedBuffer,
    consumers: ConsumerRegistry,
    inflight: InFlightTracker,
    commit: ContiguousCommitTracker,
    fetched_cursor: LogicalCursor,
    next_batch_id: u64,
}

impl GroupEngine {
    pub fn new(config: GroupConfig) -> CoreResult<Self> {
        if config.batch_max_records == 0 {
            return Err(CoreError::InvalidArgument(
                "batch_max_records must be non-zero",
            ));
        }
        let buffer = BoundedBuffer::new(config.max_buffer_records, config.max_buffer_bytes)?;
        let source = SyntheticSource::new(config.total_records, config.payload_size);
        Ok(Self {
            config,
            lifecycle: GroupLifecycle::Stopped,
            source,
            buffer,
            consumers: ConsumerRegistry::new(),
            inflight: InFlightTracker::new(),
            commit: ContiguousCommitTracker::new(),
            fetched_cursor: None,
            next_batch_id: 1,
        })
    }

    pub fn lifecycle(&self) -> GroupLifecycle {
        self.lifecycle
    }

    pub fn committed_cursor(&self) -> &LogicalCursor {
        self.commit.committed()
    }

    pub fn fetched_cursor(&self) -> &LogicalCursor {
        &self.fetched_cursor
    }

    pub fn buffer_len(&self) -> usize {
        self.buffer.len()
    }

    pub fn buffer_bytes(&self) -> usize {
        self.buffer.bytes()
    }

    pub fn buffer_free_records(&self) -> usize {
        self.buffer.remaining_record_slots()
    }

    pub fn inflight_len(&self) -> usize {
        self.inflight.len()
    }

    pub fn inflight_records(&self) -> usize {
        self.inflight.record_count()
    }

    pub fn config(&self) -> &GroupConfig {
        &self.config
    }

    pub fn start(&mut self) -> CoreResult<()> {
        self.lifecycle = self.lifecycle.transition_to(GroupLifecycle::Starting)?;
        self.lifecycle = self.lifecycle.transition_to(GroupLifecycle::Running)?;
        Ok(())
    }

    /// Stop reading the source and refuse new consumers. Records already
    /// fetched are still delivered and acked. See [`Self::is_drained`].
    pub fn drain(&mut self) -> CoreResult<()> {
        self.lifecycle = self.lifecycle.transition_to(GroupLifecycle::Draining)?;
        Ok(())
    }

    /// True when a draining group has delivered and received acks for
    /// everything it fetched.
    pub fn is_drained(&self) -> bool {
        self.lifecycle == GroupLifecycle::Draining
            && self.buffer.is_empty()
            && self.inflight.is_empty()
    }

    /// Operator pause: a running or draining group becomes `Stopped`.
    pub fn pause(&mut self) -> CoreResult<()> {
        self.lifecycle = self.lifecycle.transition_to(GroupLifecycle::Stopped)?;
        Ok(())
    }

    pub fn list_consumers(&self) -> Vec<ConsumerId> {
        self.consumers.ids()
    }

    pub fn stop(&mut self) -> CoreResult<()> {
        match self.lifecycle {
            GroupLifecycle::Draining => {
                self.lifecycle = self.lifecycle.transition_to(GroupLifecycle::Stopped)?;
            }
            GroupLifecycle::Stopped => {}
            other => {
                return Err(CoreError::InvalidTransition {
                    from: other,
                    to: GroupLifecycle::Stopped,
                });
            }
        }
        Ok(())
    }

    pub fn fail(&mut self) -> CoreResult<()> {
        self.lifecycle = self.lifecycle.transition_to(GroupLifecycle::Failed)?;
        Ok(())
    }

    pub fn snapshot(&self) -> GroupSnapshot {
        GroupSnapshot {
            config: self.config.clone(),
            lifecycle: self.lifecycle,
            committed_cursor: self.commit.committed().clone(),
            next_batch_id: self.next_batch_id,
        }
    }

    /// Reconstruct from durable snapshot. In-flight work is not restored; source
    /// resumes after committed cursor (at-least-once replay of uncertain work).
    pub fn recover_from(snapshot: GroupSnapshot) -> CoreResult<Self> {
        let mut engine = Self::new(snapshot.config)?;
        engine
            .commit
            .reset_from_committed(snapshot.committed_cursor.clone());
        engine.fetched_cursor = snapshot.committed_cursor;
        engine.next_batch_id = snapshot.next_batch_id.max(1);
        engine.lifecycle = GroupLifecycle::Failed;
        engine.lifecycle = engine.lifecycle.transition_to(GroupLifecycle::Recovering)?;
        engine.lifecycle = engine.lifecycle.transition_to(GroupLifecycle::Running)?;
        Ok(engine)
    }

    /// Join a consumer. A draining group accepts no new consumers.
    pub fn join_consumer(&mut self, id: ConsumerId) -> CoreResult<()> {
        self.ensure_dispatch()?;
        if self.lifecycle == GroupLifecycle::Draining {
            return Err(CoreError::NotRunning(self.lifecycle));
        }
        self.consumers.join(id)
    }

    pub fn leave_consumer(&mut self, id: &ConsumerId) -> CoreResult<()> {
        self.ensure_dispatch()?;
        self.consumers.leave(id)?;
        let returned = self.inflight.take_for_consumer(id);
        self.requeue_assignments(returned);
        Ok(())
    }

    /// Pull from the synthetic source into the buffer while capacity remains.
    /// Returns number of records fetched. A draining group fetches nothing.
    pub fn poll_fetch(&mut self) -> CoreResult<usize> {
        self.ensure_dispatch()?;
        if self.lifecycle == GroupLifecycle::Draining {
            return Ok(0);
        }
        let slots = self.buffer.remaining_record_slots();
        if slots == 0 {
            return Ok(0);
        }
        let page = self.source.fetch_after(&self.fetched_cursor, slots);
        let mut fetched = 0usize;
        for record in page {
            if !self.buffer.can_accept(&record) {
                break;
            }
            self.fetched_cursor = Some(record.ordering.clone());
            self.buffer.push_back(record)?;
            fetched += 1;
        }
        Ok(fetched)
    }

    /// Push records already read from an external source. Stops when the buffer is full.
    /// The fetched cursor advances only for records that were accepted. A
    /// draining group accepts nothing.
    pub fn ingest(&mut self, records: Vec<Record>) -> CoreResult<usize> {
        self.ensure_dispatch()?;
        if self.lifecycle == GroupLifecycle::Draining {
            return Ok(0);
        }
        let mut fetched = 0usize;
        for record in records {
            if !is_after(&self.fetched_cursor, &record.ordering) {
                return Err(CoreError::InvalidArgument(
                    "source returned a record that is not after the fetched cursor",
                ));
            }
            if !self.buffer.can_accept(&record) {
                break;
            }
            self.fetched_cursor = Some(record.ordering.clone());
            self.buffer.push_back(record)?;
            fetched += 1;
        }
        Ok(fetched)
    }

    pub fn assign_batch(&mut self, consumer_id: &ConsumerId) -> CoreResult<Batch> {
        self.ensure_dispatch()?;
        if !self.consumers.contains(consumer_id) {
            return Err(CoreError::UnknownConsumer(consumer_id.to_string()));
        }
        let mut records: Vec<Record> = Vec::new();
        let mut bytes = 0usize;
        while records.len() < self.config.batch_max_records {
            let Some(next) = self.buffer.front() else {
                break;
            };
            let size = next.byte_len();
            if !records.is_empty() && bytes.saturating_add(size) > MAX_BATCH_BYTES {
                break;
            }
            bytes = bytes.saturating_add(size);
            records.push(self.buffer.pop_front().expect("front exists"));
        }
        if records.is_empty() {
            return Err(CoreError::NoWork);
        }
        let batch_id = BatchId::from_u64(self.next_batch_id);
        self.next_batch_id = self.next_batch_id.saturating_add(1);
        self.commit
            .note_delivered(records.iter().map(|r| r.ordering.clone()));
        let assignment = Assignment {
            batch_id,
            consumer_id: consumer_id.clone(),
            records: records.clone(),
            assigned_at: Instant::now(),
        };
        self.inflight.insert(assignment)?;
        Ok(Batch {
            id: batch_id,
            consumer_id: consumer_id.clone(),
            records,
        })
    }

    /// Return a batch to the buffer when the assignee will never ack it.
    pub fn requeue_batch(&mut self, batch_id: BatchId) {
        if let Some(assignment) = self.inflight.take(batch_id) {
            self.requeue_assignments(vec![assignment]);
        }
    }

    /// Complete a batch. A batch that is not in flight is a no-op and
    /// returns [`AckOutcome::Stale`].
    pub fn ack(&mut self, batch_id: BatchId) -> CoreResult<AckOutcome> {
        self.ensure_dispatch()?;
        let Some(assignment) = self.inflight.take(batch_id) else {
            return Ok(AckOutcome::Stale);
        };
        self.commit
            .complete(assignment.records.into_iter().map(|r| r.ordering));
        Ok(AckOutcome::Applied)
    }

    /// Return batches older than `batch_timeout` to the buffer. Returns how
    /// many batches were requeued; a batch that does not fit stays in flight.
    pub fn tick(&mut self, now: Instant) -> CoreResult<usize> {
        self.ensure_dispatch()?;
        let timed_out = self.inflight.take_timed_out(now, self.config.batch_timeout);
        Ok(self.requeue_assignments(timed_out))
    }

    /// Returns how many assignments went back to the buffer.
    fn requeue_assignments(&mut self, assignments: Vec<Assignment>) -> usize {
        let mut requeued = 0;
        // Preserve original order by pushing each assignment's records front-first.
        // A batch that does not fit stays in flight and is retried on a later tick.
        for assignment in assignments.into_iter().rev() {
            let extra_bytes: usize = assignment.records.iter().map(|r| r.byte_len()).sum();
            let fits = self.buffer.len() + assignment.records.len()
                <= self.config.max_buffer_records
                && self.buffer.bytes().saturating_add(extra_bytes) <= self.config.max_buffer_bytes;
            if !fits {
                let _ = self.inflight.insert(assignment);
                continue;
            }
            let mut records = assignment.records;
            while let Some(record) = records.pop() {
                self.buffer
                    .push_front(record)
                    .expect("batch fits in the buffer");
            }
            requeued += 1;
        }
        requeued
    }

    fn ensure_dispatch(&self) -> CoreResult<()> {
        if self.lifecycle.allows_dispatch() {
            Ok(())
        } else {
            Err(CoreError::NotRunning(self.lifecycle))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ordering::OrderingValue;
    use std::time::Duration;

    fn cfg(total: u64, buf_records: usize, batch: usize) -> GroupConfig {
        GroupConfig {
            group_id: GroupId::new("g1").unwrap(),
            total_records: total,
            payload_size: 8,
            max_buffer_records: buf_records,
            max_buffer_bytes: 1024 * 1024,
            batch_max_records: batch,
            batch_timeout: Duration::from_secs(60),
        }
    }

    fn consume_to_end(engine: &mut GroupEngine, consumer: &ConsumerId) {
        loop {
            let _ = engine.poll_fetch().unwrap();
            match engine.assign_batch(consumer) {
                Ok(batch) => {
                    engine.ack(batch.id).unwrap();
                }
                Err(CoreError::NoWork) => {
                    if engine.buffer_len() == 0
                        && engine.inflight_len() == 0
                        && engine
                            .source
                            .fetch_after(engine.fetched_cursor(), 1)
                            .is_empty()
                    {
                        break;
                    }
                    if engine.inflight_len() > 0 {
                        // Every batch is acked when assigned, so nothing is
                        // left in flight to wait for.
                        break;
                    }
                    let _ = engine.poll_fetch().unwrap();
                    if engine.buffer_len() == 0 {
                        break;
                    }
                }
                Err(e) => panic!("{e}"),
            }
        }
    }

    #[test]
    fn one_consumer_drains_all() {
        let mut engine = GroupEngine::new(cfg(20, 10, 4)).unwrap();
        engine.start().unwrap();
        let c = ConsumerId::new("c1").unwrap();
        engine.join_consumer(c.clone()).unwrap();
        consume_to_end(&mut engine, &c);
        assert_eq!(
            engine.committed_cursor(),
            &Some(OrderingValue::single_u64(20))
        );
    }

    #[test]
    fn multi_consumer_partition() {
        let mut engine = GroupEngine::new(cfg(30, 10, 3)).unwrap();
        engine.start().unwrap();
        let c1 = ConsumerId::new("c1").unwrap();
        let c2 = ConsumerId::new("c2").unwrap();
        engine.join_consumer(c1.clone()).unwrap();
        engine.join_consumer(c2.clone()).unwrap();
        let mut pending = Vec::new();
        loop {
            let _ = engine.poll_fetch().unwrap();
            let a = engine.assign_batch(&c1);
            let b = engine.assign_batch(&c2);
            match (a, b) {
                (Ok(ba), Ok(bb)) => {
                    pending.push(ba.id);
                    pending.push(bb.id);
                }
                (Ok(ba), Err(CoreError::NoWork)) => pending.push(ba.id),
                (Err(CoreError::NoWork), Ok(bb)) => pending.push(bb.id),
                (Err(CoreError::NoWork), Err(CoreError::NoWork)) => {
                    if pending.is_empty()
                        && engine.buffer_len() == 0
                        && engine
                            .source
                            .fetch_after(engine.fetched_cursor(), 1)
                            .is_empty()
                    {
                        break;
                    }
                }
                (Err(e), _) | (_, Err(e)) => panic!("{e}"),
            }
            // Ack oldest first sometimes, newest first other times.
            if pending.len() >= 2 {
                let id = pending.pop().unwrap();
                engine.ack(id).unwrap();
            } else if let Some(id) = pending.first().copied() {
                engine.ack(id).unwrap();
                pending.remove(0);
            }
        }
        while let Some(id) = pending.pop() {
            engine.ack(id).unwrap();
        }
        assert_eq!(
            engine.committed_cursor(),
            &Some(OrderingValue::single_u64(30))
        );
    }

    #[test]
    fn out_of_order_ack_gap() {
        let mut engine = GroupEngine::new(cfg(6, 10, 2)).unwrap();
        engine.start().unwrap();
        let c = ConsumerId::new("c1").unwrap();
        engine.join_consumer(c.clone()).unwrap();
        assert_eq!(engine.poll_fetch().unwrap(), 6);
        let b1 = engine.assign_batch(&c).unwrap();
        let b2 = engine.assign_batch(&c).unwrap();
        let b3 = engine.assign_batch(&c).unwrap();
        engine.ack(b2.id).unwrap();
        engine.ack(b3.id).unwrap();
        assert_eq!(engine.committed_cursor(), &None);
        engine.ack(b1.id).unwrap();
        assert_eq!(
            engine.committed_cursor(),
            &Some(OrderingValue::single_u64(6))
        );
    }

    #[test]
    fn consumer_leave_requeues() {
        let mut engine = GroupEngine::new(cfg(4, 10, 2)).unwrap();
        engine.start().unwrap();
        let c1 = ConsumerId::new("c1").unwrap();
        let c2 = ConsumerId::new("c2").unwrap();
        engine.join_consumer(c1.clone()).unwrap();
        engine.join_consumer(c2.clone()).unwrap();
        let _ = engine.poll_fetch().unwrap();
        let b = engine.assign_batch(&c1).unwrap();
        engine.leave_consumer(&c1).unwrap();
        assert!(engine.inflight.get(b.id).is_none());
        let b2 = engine.assign_batch(&c2).unwrap();
        assert_eq!(b2.records[0].ordering, b.records[0].ordering);
        engine.ack(b2.id).unwrap();
        consume_to_end(&mut engine, &c2);
        assert_eq!(
            engine.committed_cursor(),
            &Some(OrderingValue::single_u64(4))
        );
    }

    #[test]
    fn timeout_requeues() {
        let mut config = cfg(4, 10, 2);
        config.batch_timeout = Duration::from_millis(1);
        let mut engine = GroupEngine::new(config).unwrap();
        engine.start().unwrap();
        let c = ConsumerId::new("c1").unwrap();
        engine.join_consumer(c.clone()).unwrap();
        let _ = engine.poll_fetch().unwrap();
        let b = engine.assign_batch(&c).unwrap();
        let n = engine
            .tick(Instant::now() + Duration::from_secs(1))
            .unwrap();
        assert_eq!(n, 1);
        assert!(engine.inflight.get(b.id).is_none());
        let b2 = engine.assign_batch(&c).unwrap();
        assert_eq!(b2.records[0].ordering, b.records[0].ordering);
    }

    #[test]
    fn timeout_keeps_the_batch_inflight_when_the_buffer_is_full() {
        let mut config = cfg(6, 4, 2);
        config.batch_timeout = Duration::from_millis(1);
        let mut engine = GroupEngine::new(config).unwrap();
        engine.start().unwrap();
        let c = ConsumerId::new("c1").unwrap();
        engine.join_consumer(c.clone()).unwrap();
        let _ = engine.poll_fetch().unwrap();
        let b = engine.assign_batch(&c).unwrap();
        let _ = engine.poll_fetch().unwrap();
        assert_eq!(engine.buffer_len(), 4);
        engine
            .tick(Instant::now() + Duration::from_secs(1))
            .unwrap();
        assert_eq!(engine.buffer_len(), 4);
        assert!(engine.inflight.get(b.id).is_some());
        engine.ack(b.id).unwrap();
        assert!(engine.inflight.get(b.id).is_none());
    }

    /// B18: `tick` reports batches returned to the buffer, not batches that
    /// timed out and stayed in flight because the buffer was full.
    #[test]
    fn regress_b18_tick_counts_only_requeued_batches() {
        let mut config = cfg(6, 4, 2);
        config.batch_timeout = Duration::from_millis(1);
        let mut engine = GroupEngine::new(config).unwrap();
        engine.start().unwrap();
        let c = ConsumerId::new("c1").unwrap();
        engine.join_consumer(c.clone()).unwrap();
        engine.poll_fetch().unwrap();
        let b = engine.assign_batch(&c).unwrap();
        engine.poll_fetch().unwrap();
        assert_eq!(engine.buffer_len(), 4);
        let later = Instant::now() + Duration::from_secs(1);
        assert_eq!(engine.tick(later).unwrap(), 0, "nothing fit in the buffer");
        assert!(engine.inflight.get(b.id).is_some());
    }

    /// B19: an ack for a batch that timed out and was redelivered under a new
    /// id is stale. It does not complete the records.
    #[test]
    fn regress_b19_ack_after_timeout_is_stale() {
        let mut config = cfg(4, 10, 2);
        config.batch_timeout = Duration::from_millis(1);
        let mut engine = GroupEngine::new(config).unwrap();
        engine.start().unwrap();
        let c = ConsumerId::new("c1").unwrap();
        engine.join_consumer(c.clone()).unwrap();
        engine.poll_fetch().unwrap();
        let first = engine.assign_batch(&c).unwrap();
        let later = Instant::now() + Duration::from_secs(1);
        assert_eq!(engine.tick(later).unwrap(), 1);
        let again = engine.assign_batch(&c).unwrap();
        assert_eq!(engine.ack(first.id).unwrap(), AckOutcome::Stale);
        assert_eq!(engine.committed_cursor(), &None);
        assert_eq!(engine.ack(again.id).unwrap(), AckOutcome::Applied);
        assert_eq!(
            engine.committed_cursor(),
            &Some(OrderingValue::single_u64(2))
        );
    }

    /// G3: a batch stops before it would exceed `MAX_BATCH_BYTES`, but always
    /// carries at least one record.
    #[test]
    fn regress_g03_batches_stay_under_the_byte_cap() {
        let mut config = cfg(0, 16, 16);
        config.max_buffer_bytes = 64 * 1024 * 1024;
        let mut engine = GroupEngine::new(config).unwrap();
        engine.start().unwrap();
        let c = ConsumerId::new("c1").unwrap();
        engine.join_consumer(c.clone()).unwrap();
        let mib = 1024 * 1024;
        let records = (1..=6)
            .map(|id| Record {
                ordering: OrderingValue::single_u64(id),
                payload: bytes::Bytes::from(vec![0u8; if id == 5 { 5 * mib } else { mib }]),
            })
            .collect();
        assert_eq!(engine.ingest(records).unwrap(), 6);
        let sizes = |batch: &Batch| batch.records.iter().map(Record::byte_len).sum::<usize>();
        let first = engine.assign_batch(&c).unwrap();
        assert_eq!(first.records.len(), 3);
        assert!(sizes(&first) <= MAX_BATCH_BYTES);
        let second = engine.assign_batch(&c).unwrap();
        assert_eq!(
            second.records.len(),
            1,
            "record 4 alone; record 5 would exceed"
        );
        let oversized = engine.assign_batch(&c).unwrap();
        assert_eq!(
            oversized.records.len(),
            1,
            "a record over the cap still goes out"
        );
        assert!(sizes(&oversized) > MAX_BATCH_BYTES);
    }

    /// B2: a draining group stops reading the source and refuses new consumers,
    /// but still hands out and accepts acks for what it already holds.
    #[test]
    fn regress_b02_draining_stops_fetch_and_rejects_joins() {
        let mut engine = GroupEngine::new(cfg(20, 4, 2)).unwrap();
        engine.start().unwrap();
        let c = ConsumerId::new("c1").unwrap();
        engine.join_consumer(c.clone()).unwrap();
        engine.poll_fetch().unwrap();
        let first = engine.assign_batch(&c).unwrap();
        engine.drain().unwrap();
        let fetched = engine.fetched_cursor().clone();

        let _ = engine.poll_fetch();
        assert_eq!(
            engine.fetched_cursor(),
            &fetched,
            "poll_fetch while draining"
        );
        let _ = engine.ingest(vec![Record {
            ordering: OrderingValue::single_u64(99),
            payload: bytes::Bytes::from_static(b"x"),
        }]);
        assert_eq!(engine.fetched_cursor(), &fetched, "ingest while draining");
        assert!(
            engine
                .join_consumer(ConsumerId::new("c2").unwrap())
                .is_err(),
            "join while draining"
        );

        engine.ack(first.id).unwrap();
        let second = engine.assign_batch(&c).unwrap();
        engine.ack(second.id).unwrap();
        assert_eq!(engine.committed_cursor(), &fetched);
    }

    #[test]
    fn requeue_batch_returns_an_unacked_assignment() {
        let mut engine = GroupEngine::new(cfg(4, 10, 2)).unwrap();
        engine.start().unwrap();
        let c = ConsumerId::new("c1").unwrap();
        engine.join_consumer(c.clone()).unwrap();
        let _ = engine.poll_fetch().unwrap();
        let batch = engine.assign_batch(&c).unwrap();
        assert_eq!(engine.inflight_len(), 1);
        engine.requeue_batch(batch.id);
        assert_eq!(engine.inflight_len(), 0);
        let again = engine.assign_batch(&c).unwrap();
        assert_eq!(again.records[0].ordering, batch.records[0].ordering);
    }

    #[test]
    fn duplicate_ack_noop() {
        let mut engine = GroupEngine::new(cfg(2, 10, 2)).unwrap();
        engine.start().unwrap();
        let c = ConsumerId::new("c1").unwrap();
        engine.join_consumer(c.clone()).unwrap();
        let _ = engine.poll_fetch().unwrap();
        let b = engine.assign_batch(&c).unwrap();
        engine.ack(b.id).unwrap();
        let committed = engine.committed_cursor().clone();
        engine.ack(b.id).unwrap();
        engine.ack(BatchId::from_u64(999)).unwrap();
        assert_eq!(engine.committed_cursor(), &committed);
    }

    #[test]
    fn restart_from_snapshot_replays_uncommitted() {
        let mut engine = GroupEngine::new(cfg(10, 10, 2)).unwrap();
        engine.start().unwrap();
        let c = ConsumerId::new("c1").unwrap();
        engine.join_consumer(c.clone()).unwrap();
        let _ = engine.poll_fetch().unwrap();
        let b1 = engine.assign_batch(&c).unwrap();
        engine.ack(b1.id).unwrap();
        let _b2 = engine.assign_batch(&c).unwrap();
        // Crash before ack of b2.
        let snap = engine.snapshot();
        assert_eq!(snap.committed_cursor, Some(OrderingValue::single_u64(2)));
        let mut engine = GroupEngine::recover_from(snap).unwrap();
        engine.join_consumer(c.clone()).unwrap();
        consume_to_end(&mut engine, &c);
        assert_eq!(
            engine.committed_cursor(),
            &Some(OrderingValue::single_u64(10))
        );
    }

    #[test]
    fn buffer_backpressure() {
        let mut engine = GroupEngine::new(cfg(100, 4, 2)).unwrap();
        engine.start().unwrap();
        assert_eq!(engine.poll_fetch().unwrap(), 4);
        assert_eq!(engine.buffer_len(), 4);
        let n2 = engine.poll_fetch().unwrap();
        assert_eq!(n2, 0);
        let c = ConsumerId::new("c1").unwrap();
        engine.join_consumer(c.clone()).unwrap();
        let b = engine.assign_batch(&c).unwrap();
        engine.ack(b.id).unwrap();
        let n3 = engine.poll_fetch().unwrap();
        assert!(n3 > 0);
    }
}
