use bytes::Bytes;

use super::ordering::{LogicalCursor, OrderingValue, is_after};
use super::record::Record;

/// Deterministic in-memory ordered source for Stage 1.
#[derive(Debug, Clone)]
pub struct SyntheticSource {
    total_records: u64,
    payload_size: usize,
}

impl SyntheticSource {
    pub fn new(total_records: u64, payload_size: usize) -> Self {
        Self {
            total_records,
            payload_size,
        }
    }

    pub fn total_records(&self) -> u64 {
        self.total_records
    }

    pub fn payload_size(&self) -> usize {
        self.payload_size
    }

    pub fn record_at(&self, id: u64) -> Option<Record> {
        if id == 0 || id > self.total_records {
            return None;
        }
        Some(Record {
            ordering: OrderingValue::single_u64(id),
            payload: Bytes::from(vec![0xAB; self.payload_size]),
        })
    }

    /// Fetch up to `limit` records strictly after `cursor`.
    pub fn fetch_after(&self, cursor: &LogicalCursor, limit: usize) -> Vec<Record> {
        if limit == 0 {
            return Vec::new();
        }
        let start = match cursor {
            None => 1u64,
            Some(v) => match v.atoms() {
                [super::ordering::OrderingAtom::U64(x)] => x.saturating_add(1),
                _ => 1,
            },
        };
        let mut out = Vec::new();
        let mut id = start;
        while out.len() < limit && id <= self.total_records {
            if let Some(r) = self.record_at(id) {
                debug_assert!(is_after(cursor, &r.ordering));
                out.push(r);
            }
            id += 1;
        }
        out
    }

    pub fn resume(&self, cursor: &LogicalCursor) -> Vec<Record> {
        self.fetch_after(cursor, usize::MAX)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fetch_and_resume() {
        let src = SyntheticSource::new(5, 4);
        let batch = src.fetch_after(&None, 3);
        assert_eq!(batch.len(), 3);
        assert_eq!(batch[0].ordering, OrderingValue::single_u64(1));
        let after = Some(batch[2].ordering.clone());
        let rest = src.fetch_after(&after, 10);
        assert_eq!(rest.len(), 2);
        assert_eq!(rest[0].ordering, OrderingValue::single_u64(4));
    }
}
