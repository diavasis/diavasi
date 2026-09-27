use bytes::Bytes;
use futures::future::BoxFuture;

use super::ordering::{LogicalCursor, OrderingValue, is_after};
use super::record::Record;

/// Deterministic in-memory source: records `1..=total_records`, ordered by
/// id, each with the same `payload_size`-byte payload.
#[derive(Debug, Clone)]
pub struct SyntheticSource {
    total_records: u64,
    payload_size: usize,
    /// One payload shared by every record.
    payload: Bytes,
}

impl SyntheticSource {
    /// Records `1..=total_records`, each with a `payload_size`-byte payload.
    ///
    /// ```
    /// use diavasi::core::{OrderingValue, SyntheticSource};
    /// let source = SyntheticSource::new(5, 4);
    /// let first = source.fetch_after(&None, 2);
    /// assert_eq!(first.len(), 2);
    /// let rest = source.fetch_after(&Some(OrderingValue::single_u64(2)), 10);
    /// assert_eq!(rest.len(), 3);
    /// ```
    pub fn new(total_records: u64, payload_size: usize) -> Self {
        Self {
            total_records,
            payload_size,
            payload: Bytes::from(vec![0xAB; payload_size]),
        }
    }

    /// Number of records the source holds.
    pub fn total_records(&self) -> u64 {
        self.total_records
    }

    /// Payload length of every record, in bytes.
    pub fn payload_size(&self) -> usize {
        self.payload_size
    }

    /// Record `id`, or `None` outside `1..=total_records`.
    pub fn record_at(&self, id: u64) -> Option<Record> {
        if id == 0 || id > self.total_records {
            return None;
        }
        Some(Record {
            ordering: OrderingValue::single_u64(id),
            payload: self.payload.clone(),
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

    /// Every record after `cursor`.
    pub fn resume(&self, cursor: &LogicalCursor) -> Vec<Record> {
        self.fetch_after(cursor, usize::MAX)
    }
}

/// Failure while reading an external source. The kind decides what the
/// supervisor does next.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SourceError {
    /// The source could not be reached or did not answer: a dropped
    /// connection, a timeout, a server that is down. The group restarts with
    /// growing delays until the source answers.
    #[error("{0}")]
    Transient(String),
    /// The data broke the declared contract: a value of the wrong type, a
    /// record that cannot be decoded, a record that is not after the cursor,
    /// entries removed before delivery. Reading again returns the same data,
    /// so the group stops until an operator fixes the source or the spec.
    #[error("{0}")]
    Contract(String),
}

impl SourceError {
    /// The error text without its kind.
    pub fn message(&self) -> &str {
        match self {
            Self::Transient(message) | Self::Contract(message) => message,
        }
    }

    /// True for [`SourceError::Transient`].
    pub fn is_transient(&self) -> bool {
        matches!(self, Self::Transient(_))
    }
}

/// An ordered source a group reads. Adapters implement it; the runtime calls
/// it from one task at a time.
///
/// `fetch_after` returns up to `limit` records strictly after `cursor`, in
/// increasing [`OrderingValue`] order, and an empty vector when it has
/// nothing new. `cursor` is `None` at the start of the stream. Return
/// [`SourceError::Transient`] when the source could not be reached, and
/// [`SourceError::Contract`] when the data is wrong: the first is retried
/// with backoff, the second stops the group.
///
/// A source over rows already in memory, sorted by id:
///
/// ```
/// use bytes::Bytes;
/// use diavasi::core::{LogicalCursor, OrderingValue, Record, RecordSource, SourceError};
/// use futures::future::BoxFuture;
///
/// struct VecSource {
///     rows: Vec<(u64, Bytes)>,
/// }
///
/// impl RecordSource for VecSource {
///     fn fetch_after<'a>(
///         &'a mut self,
///         cursor: &'a LogicalCursor,
///         limit: usize,
///     ) -> BoxFuture<'a, Result<Vec<Record>, SourceError>> {
///         Box::pin(async move {
///             let records = self
///                 .rows
///                 .iter()
///                 .map(|(id, payload)| Record {
///                     ordering: OrderingValue::single_u64(*id),
///                     payload: payload.clone(),
///                 })
///                 .filter(|record| cursor.as_ref().is_none_or(|c| &record.ordering > c))
///                 .take(limit)
///                 .collect();
///             Ok(records)
///         })
///     }
/// }
///
/// # futures::executor::block_on(async {
/// let mut source = VecSource {
///     rows: (1..=3).map(|id| (id, Bytes::from(format!(r#"{{"id":{id}}}"#)))).collect(),
/// };
/// let page = source.fetch_after(&Some(OrderingValue::single_u64(1)), 10).await.unwrap();
/// assert_eq!(page.len(), 2);
/// # });
/// ```
pub trait RecordSource: Send {
    /// Up to `limit` records strictly after `cursor`, in order.
    fn fetch_after<'a>(
        &'a mut self,
        cursor: &'a LogicalCursor,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Record>, SourceError>>;
}

impl RecordSource for SyntheticSource {
    fn fetch_after<'a>(
        &'a mut self,
        cursor: &'a LogicalCursor,
        limit: usize,
    ) -> BoxFuture<'a, Result<Vec<Record>, SourceError>> {
        let records = SyntheticSource::fetch_after(self, cursor, limit);
        Box::pin(async move { Ok(records) })
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
