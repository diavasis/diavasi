use super::error::{CoreError, CoreResult};
use super::record::Record;

/// Records fetched and not yet assigned, capped by count and by payload bytes.
///
/// ```
/// use bytes::Bytes;
/// use diavasi::core::{BoundedBuffer, OrderingValue, Record};
/// let mut buffer = BoundedBuffer::new(2, 1024)?;
/// let record = |id| Record { ordering: OrderingValue::single_u64(id), payload: Bytes::from_static(b"x") };
/// buffer.push_back(record(1))?;
/// buffer.push_back(record(2))?;
/// assert!(buffer.push_back(record(3)).is_err(), "record cap reached");
/// assert_eq!(buffer.pop_front().unwrap().ordering, OrderingValue::single_u64(1));
/// # Ok::<(), diavasi::core::CoreError>(())
/// ```
#[derive(Debug, Default)]
pub struct BoundedBuffer {
    records: std::collections::VecDeque<Record>,
    bytes: usize,
    max_records: usize,
    max_bytes: usize,
}

impl BoundedBuffer {
    /// An empty buffer. Both caps must be at least 1.
    pub fn new(max_records: usize, max_bytes: usize) -> CoreResult<Self> {
        if max_records == 0 || max_bytes == 0 {
            return Err(CoreError::InvalidArgument(format!(
                "buffer caps must be non-zero: max_records={max_records}, max_bytes={max_bytes}"
            )));
        }
        Ok(Self {
            records: std::collections::VecDeque::new(),
            bytes: 0,
            max_records,
            max_bytes,
        })
    }

    /// Records held.
    pub fn len(&self) -> usize {
        self.records.len()
    }

    /// True when no record is held.
    pub fn is_empty(&self) -> bool {
        self.records.is_empty()
    }

    /// Payload bytes held.
    pub fn bytes(&self) -> usize {
        self.bytes
    }

    /// The record cap.
    pub fn max_records(&self) -> usize {
        self.max_records
    }

    /// The byte cap.
    pub fn max_bytes(&self) -> usize {
        self.max_bytes
    }

    /// Records that still fit under the record cap.
    pub fn remaining_record_slots(&self) -> usize {
        self.max_records.saturating_sub(self.records.len())
    }

    /// Payload bytes that still fit under the byte cap.
    pub fn remaining_bytes(&self) -> usize {
        self.max_bytes.saturating_sub(self.bytes)
    }

    /// True when `record` fits under both caps.
    pub fn can_accept(&self, record: &Record) -> bool {
        self.records.len() < self.max_records
            && self.bytes.saturating_add(record.byte_len()) <= self.max_bytes
    }

    /// Append a record. [`CoreError::BufferFull`] when it does not fit.
    pub fn push_back(&mut self, record: Record) -> CoreResult<()> {
        if !self.can_accept(&record) {
            return Err(CoreError::BufferFull);
        }
        self.bytes += record.byte_len();
        self.records.push_back(record);
        Ok(())
    }

    /// Put a record back at the front, for redelivery. [`CoreError::BufferFull`] when it does not fit.
    pub fn push_front(&mut self, record: Record) -> CoreResult<()> {
        if !self.can_accept(&record) {
            return Err(CoreError::BufferFull);
        }
        self.bytes += record.byte_len();
        self.records.push_front(record);
        Ok(())
    }

    /// The next record to assign.
    pub fn front(&self) -> Option<&Record> {
        self.records.front()
    }

    /// Remove and return the next record to assign.
    pub fn pop_front(&mut self) -> Option<Record> {
        let record = self.records.pop_front()?;
        self.bytes = self.bytes.saturating_sub(record.byte_len());
        Some(record)
    }

    /// Drop every record.
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
