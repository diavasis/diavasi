use super::error::{CoreError, CoreResult};
use super::record::Record;

/// Hard-capped read-ahead buffer (record count and bytes).
#[derive(Debug, Default)]
pub struct BoundedBuffer {
    records: std::collections::VecDeque<Record>,
    bytes: usize,
    max_records: usize,
    max_bytes: usize,
}

impl BoundedBuffer {
    pub fn new(max_records: usize, max_bytes: usize) -> CoreResult<Self> {
        if max_records == 0 || max_bytes == 0 {
            return Err(CoreError::InvalidArgument("buffer caps must be non-zero"));
        }
        Ok(Self {
            records: std::collections::VecDeque::new(),
            bytes: 0,
            max_records,
            max_bytes,
        })
    }

    pub fn len(&self) -> usize {
        self.records.len()
    }

    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    pub fn bytes(&self) -> usize {
        self.bytes
    }

    pub fn max_records(&self) -> usize {
        self.max_records
    }

    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    pub fn remaining_record_slots(&self) -> usize {
        self.max_records.saturating_sub(self.records.len())
    }

    pub fn remaining_bytes(&self) -> usize {
        self.max_bytes.saturating_sub(self.bytes)
    }

    pub fn can_accept(&self, record: &Record) -> bool {
        self.records.len() < self.max_records
            && self.bytes.saturating_add(record.byte_len()) <= self.max_bytes
    }

    pub fn push_back(&mut self, record: Record) -> CoreResult<()> {
        if !self.can_accept(&record) {
            return Err(CoreError::BufferFull);
        }
        self.bytes += record.byte_len();
        self.records.push_back(record);
        Ok(())
    }

    pub fn push_front(&mut self, record: Record) -> CoreResult<()> {
        if !self.can_accept(&record) {
            return Err(CoreError::BufferFull);
        }
        self.bytes += record.byte_len();
        self.records.push_front(record);
        Ok(())
    }

    pub fn front(&self) -> Option<&Record> {
        self.records.front()
    }

    pub fn pop_front(&mut self) -> Option<Record> {
        let record = self.records.pop_front()?;
        self.bytes = self.bytes.saturating_sub(record.byte_len());
        Some(record)
    }

    pub fn clear(&mut self) {
        self.records.clear();
        self.bytes = 0;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::ordering::OrderingValue;
    use bytes::Bytes;

    fn rec(id: u64, n: usize) -> Record {
        Record {
            ordering: OrderingValue::single_u64(id),
            payload: Bytes::from(vec![0; n]),
        }
    }

    #[test]
    fn respects_caps() {
        let mut buf = BoundedBuffer::new(2, 100).unwrap();
        buf.push_back(rec(1, 40)).unwrap();
        buf.push_back(rec(2, 40)).unwrap();
        assert!(buf.push_back(rec(3, 40)).is_err());
        assert_eq!(buf.len(), 2);
    }
}
