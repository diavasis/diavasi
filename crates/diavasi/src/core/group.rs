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

    pub fn config(&self) -> &GroupConfig {
        &self.config
    }

    pub fn start(&mut self) -> CoreResult<()> {
        self.lifecycle = self.lifecycle.transition_to(GroupLifecycle::Starting)?;
        self.lifecycle = self.lifecycle.transition_to(GroupLifecycle::Running)?;
        Ok(())
    }

    pub fn drain(&mut self) -> CoreResult<()> {
        self.lifecycle = self.lifecycle.transition_to(GroupLifecycle::Draining)?;
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

    pub fn join_consumer(&mut self, id: ConsumerId) -> CoreResult<()> {
        self.ensure_dispatch()?;
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
    /// Returns number of records fetched.
    pub fn poll_fetch(&mut self) -> CoreResult<usize> {
        self.ensure_dispatch()?;
        let mut fetched = 0usize;
        while self.buffer.remaining_record_slots() > 0 {
            let Some(record) = self
                .source
                .fetch_after(&self.fetched_cursor, 1)
                .into_iter()
                .next()
            else {
                break;
            };
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
    /// The fetched cursor advances only for records that were accepted.
    pub fn ingest(&mut self, records: Vec<Record>) -> CoreResult<usize> {
        self.ensure_dispatch()?;
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
        let mut records = Vec::new();
        while records.len() < self.config.batch_max_records {
            match self.buffer.pop_front() {
                Some(r) => records.push(r),
                None => break,
            }
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

    pub fn ack(&mut self, batch_id: BatchId) -> CoreResult<()> {
        self.ensure_dispatch()?;
        let Some(assignment) = self.inflight.take(batch_id) else {
            // Duplicate / stale ACK: no-op.
            return Ok(());
        };
        self.commit
            .complete(assignment.records.into_iter().map(|r| r.ordering));
        Ok(())
    }

    pub fn tick(&mut self, now: Instant) -> CoreResult<usize> {
        self.ensure_dispatch()?;
        let timed_out = self.inflight.take_timed_out(now, self.config.batch_timeout);
        let n = timed_out.len();
        self.requeue_assignments(timed_out);
        Ok(n)
    }

    fn requeue_assignments(&mut self, assignments: Vec<Assignment>) {
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
        }
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

    fn drain_one(engine: &mut GroupEngine, consumer: &ConsumerId) {
        loop {
            let _ = engine.poll_fetch().unwrap();
            match engine.assign_batch(consumer) {
                Ok(batch) => engine.ack(batch.id).unwrap(),
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
                        // waiting on acks we should have done
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
        drain_one(&mut engine, &c);
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
        drain_one(&mut engine, &c2);
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
        std::thread::sleep(Duration::from_millis(5));
        let n = engine.tick(Instant::now()).unwrap();
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
        std::thread::sleep(Duration::from_millis(5));
        assert_eq!(engine.tick(Instant::now()).unwrap(), 1);
        assert_eq!(engine.buffer_len(), 4);
        assert!(engine.inflight.get(b.id).is_some());
        engine.ack(b.id).unwrap();
        assert!(engine.inflight.get(b.id).is_none());
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
        drain_one(&mut engine, &c);
        assert_eq!(
            engine.committed_cursor(),
            &Some(OrderingValue::single_u64(10))
        );
    }

    #[test]
    fn buffer_backpressure() {
        let mut engine = GroupEngine::new(cfg(100, 4, 2)).unwrap();
        engine.start().unwrap();
        let n = engine.poll_fetch().unwrap();
        assert!(n <= 4);
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
