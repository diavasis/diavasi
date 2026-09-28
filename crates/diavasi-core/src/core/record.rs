use bytes::Bytes;

use super::ids::{BatchId, ConsumerId};
use super::ordering::OrderingValue;

/// One record read from a source: its position in the stream and its bytes.
///
/// ```
/// use bytes::Bytes;
/// use diavasi::core::{OrderingValue, Record};
/// let record = Record {
///     ordering: OrderingValue::single_u64(7),
///     payload: Bytes::from_static(br#"{"id":7}"#),
/// };
/// assert_eq!(record.byte_len(), 8);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    /// Position in the group's total order. Strictly increasing along the stream.
    pub ordering: OrderingValue,
    /// The record as the consumer receives it, usually a JSON object.
    pub payload: Bytes,
}

impl Record {
    /// Payload length in bytes. Buffer and batch caps count this.
    pub fn byte_len(&self) -> usize {
        self.payload.len()
    }
}

/// Records handed to one consumer. The consumer acks the whole batch by `id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Batch {
    /// Batch id, unique within the group and increasing.
    pub id: BatchId,
    /// The consumer the batch was assigned to.
    pub consumer_id: ConsumerId,
    /// Records in stream order.
    pub records: Vec<Record>,
}

impl Batch {
    /// The ordering of each record, in order.
    pub fn orderings(&self) -> impl Iterator<Item = &OrderingValue> {
        self.records.iter().map(|r| &r.ordering)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn record_byte_len() {
        let r = Record {
            ordering: OrderingValue::single_u64(1),
            payload: Bytes::from_static(b"abcd"),
        };
        assert_eq!(r.byte_len(), 4);
        let empty = Record {
            ordering: OrderingValue::single_u64(2),
            payload: Bytes::new(),
        };
        assert_eq!(empty.byte_len(), 0);
    }

    #[test]
    fn batch_orderings_iterates_in_record_order() {
        let batch = Batch {
            id: BatchId::from_u64(7),
            consumer_id: ConsumerId::new("c1").unwrap(),
            records: vec![
                Record {
                    ordering: OrderingValue::single_u64(1),
                    payload: Bytes::from_static(b"a"),
                },
                Record {
                    ordering: OrderingValue::single_u64(2),
                    payload: Bytes::from_static(b"b"),
                },
            ],
        };
        let keys: Vec<_> = batch.orderings().cloned().collect();
        assert_eq!(
            keys,
            vec![OrderingValue::single_u64(1), OrderingValue::single_u64(2)]
        );
        assert_eq!(batch.id.as_u64(), 7);
        assert_eq!(batch.consumer_id.as_str(), "c1");
    }
}
