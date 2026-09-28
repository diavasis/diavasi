use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::error::{CoreError, CoreResult};
use super::ids::{BatchId, ConsumerId};
use super::record::Record;

/// A batch handed to a consumer and not yet acked.
#[derive(Debug, Clone)]
pub struct Assignment {
    /// The batch id the consumer acks.
    pub batch_id: BatchId,
    /// The consumer holding the batch.
    pub consumer_id: ConsumerId,
    /// The records, kept so they can be redelivered.
    pub records: Vec<Record>,
    /// When the batch was assigned. `batch_timeout` counts from here.
    pub assigned_at: Instant,
}

/// Batches assigned to consumers and not yet acked.
#[derive(Debug, Default)]
pub struct InFlightTracker {
    by_batch: HashMap<BatchId, Assignment>,
    /// Records across all assignments, kept current on every change.
    records: usize,
}

impl InFlightTracker {
    /// An empty tracker.
    pub fn new() -> Self {
        Self::default()
    }

    /// Batches in flight.
    pub fn len(&self) -> usize {
        self.by_batch.len()
    }

    /// Records across all batches in flight.
    pub fn record_count(&self) -> usize {
        self.records
    }

    /// True when nothing is in flight.
    pub fn is_empty(&self) -> bool {
        self.by_batch.is_empty()
    }

    /// Track a new assignment. Fails when its batch id is already in flight.
    pub fn insert(&mut self, assignment: Assignment) -> CoreResult<()> {
        if self.by_batch.contains_key(&assignment.batch_id) {
            return Err(CoreError::InvalidArgument(format!(
                "batch {} is already in flight",
                assignment.batch_id
            )));
        }
        self.records += assignment.records.len();
        self.by_batch.insert(assignment.batch_id, assignment);
        Ok(())
    }

    fn remove(&mut self, batch_id: &BatchId) -> Option<Assignment> {
        let assignment = self.by_batch.remove(batch_id)?;
        self.records -= assignment.records.len();
        Some(assignment)
    }

    /// The assignment for `batch_id`.
    pub fn get(&self, batch_id: BatchId) -> Option<&Assignment> {
        self.by_batch.get(&batch_id)
    }

    /// Remove and return the assignment for `batch_id`.
    pub fn take(&mut self, batch_id: BatchId) -> Option<Assignment> {
        self.remove(&batch_id)
    }

    /// Remove and return every assignment held by `consumer_id`.
    pub fn take_for_consumer(&mut self, consumer_id: &ConsumerId) -> Vec<Assignment> {
        let ids: Vec<BatchId> = self
            .by_batch
            .iter()
            .filter(|(_, a)| &a.consumer_id == consumer_id)
            .map(|(id, _)| *id)
            .collect();
        ids.into_iter().filter_map(|id| self.remove(&id)).collect()
    }

    /// Remove and return assignments at least `timeout` old at `now`.
    pub fn take_timed_out(&mut self, now: Instant, timeout: Duration) -> Vec<Assignment> {
        let ids: Vec<BatchId> = self
            .by_batch
            .iter()
            .filter(|(_, a)| now.duration_since(a.assigned_at) >= timeout)
            .map(|(id, _)| *id)
            .collect();
        ids.into_iter().filter_map(|id| self.remove(&id)).collect()
    }

    /// Forget every assignment.
    pub fn clear(&mut self) {
        self.by_batch.clear();
        self.records = 0;
    }
}
