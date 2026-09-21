use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use crate::protocol::{Record, RecordBatch};

/// Synthetic producer that can outpace any candidate transport.
#[derive(Debug)]
pub struct SyntheticProducer {
    next_record_id: AtomicU64,
    next_batch_id: AtomicU64,
    payload: Vec<u8>,
    batch_size: u32,
    total_records: u64,
}

impl SyntheticProducer {
    pub fn new(record_bytes: usize, batch_size: u32, total_records: u64) -> Self {
        Self {
            next_record_id: AtomicU64::new(1),
            next_batch_id: AtomicU64::new(1),
            payload: vec![0xAB; record_bytes],
            batch_size,
            total_records,
        }
    }

    pub fn remaining(&self) -> u64 {
        let next = self.next_record_id.load(Ordering::Relaxed);
        self.total_records.saturating_sub(next.saturating_sub(1))
    }

    pub fn done(&self) -> bool {
        self.remaining() == 0
    }

    pub fn next_batch(&self) -> Option<RecordBatch> {
        let start = self.next_record_id.load(Ordering::Relaxed);
        if start > self.total_records {
            return None;
        }
        let end = (start + u64::from(self.batch_size) - 1).min(self.total_records);
        let count = end - start + 1;
        if count == 0 {
            return None;
        }
        if self
            .next_record_id
            .compare_exchange(start, end + 1, Ordering::SeqCst, Ordering::Relaxed)
            .is_err()
        {
            return self.next_batch();
        }
        let batch_id = self.next_batch_id.fetch_add(1, Ordering::Relaxed);
        let records = (start..=end)
            .map(|record_id| Record {
                record_id,
                payload: self.payload.clone(),
            })
            .collect();
        let sent_at_unix_ns = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_nanos() as i64)
            .unwrap_or(0);
        Some(RecordBatch {
            batch_id,
            records,
            sent_at_unix_ns,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn produces_exact_total() {
        let p = SyntheticProducer::new(8, 10, 25);
        let mut n = 0u64;
        while let Some(b) = p.next_batch() {
            n += b.records.len() as u64;
        }
        assert_eq!(n, 25);
        assert!(p.done());
    }
}
