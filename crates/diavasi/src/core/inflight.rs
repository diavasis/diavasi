use std::collections::HashMap;
use std::time::{Duration, Instant};

use super::error::{CoreError, CoreResult};
use super::ids::{BatchId, ConsumerId};
use super::record::Record;

#[derive(Debug, Clone)]
pub struct Assignment {
    pub batch_id: BatchId,
    pub consumer_id: ConsumerId,
    pub records: Vec<Record>,
    pub assigned_at: Instant,
}

#[derive(Debug, Default)]
pub struct InFlightTracker {
    by_batch: HashMap<BatchId, Assignment>,
}

impl InFlightTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn len(&self) -> usize {
        self.by_batch.len()
    }

    pub fn is_empty(&self) -> bool {
        self.by_batch.is_empty()
    }

    pub fn insert(&mut self, assignment: Assignment) -> CoreResult<()> {
        if self.by_batch.contains_key(&assignment.batch_id) {
            return Err(CoreError::InvalidArgument("duplicate batch id"));
        }
        self.by_batch.insert(assignment.batch_id, assignment);
        Ok(())
    }

    pub fn get(&self, batch_id: BatchId) -> Option<&Assignment> {
        self.by_batch.get(&batch_id)
    }

    pub fn take(&mut self, batch_id: BatchId) -> Option<Assignment> {
        self.by_batch.remove(&batch_id)
    }

    pub fn take_for_consumer(&mut self, consumer_id: &ConsumerId) -> Vec<Assignment> {
        let ids: Vec<BatchId> = self
            .by_batch
            .iter()
            .filter(|(_, a)| &a.consumer_id == consumer_id)
            .map(|(id, _)| *id)
            .collect();
        ids.into_iter()
            .filter_map(|id| self.by_batch.remove(&id))
            .collect()
    }

    pub fn take_timed_out(&mut self, now: Instant, timeout: Duration) -> Vec<Assignment> {
        let ids: Vec<BatchId> = self
            .by_batch
            .iter()
            .filter(|(_, a)| now.duration_since(a.assigned_at) >= timeout)
            .map(|(id, _)| *id)
            .collect();
        ids.into_iter()
            .filter_map(|id| self.by_batch.remove(&id))
            .collect()
    }

    pub fn clear(&mut self) {
        self.by_batch.clear();
    }
}
