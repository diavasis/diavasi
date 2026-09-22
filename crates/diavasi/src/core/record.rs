use bytes::Bytes;

use super::ids::{BatchId, ConsumerId};
use super::ordering::OrderingValue;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub ordering: OrderingValue,
    pub payload: Bytes,
}

impl Record {
    pub fn byte_len(&self) -> usize {
        self.payload.len()
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch {
    pub id: BatchId,
    pub consumer_id: ConsumerId,
    pub records: Vec<Record>,
}

impl Batch {
    pub fn orderings(&self) -> impl Iterator<Item = &OrderingValue> {
        self.records.iter().map(|r| &r.ordering)
    }
}
