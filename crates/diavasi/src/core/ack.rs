use std::collections::{HashSet, VecDeque};

use super::ordering::{LogicalCursor, OrderingValue};

/// Tracks completed orderings and advances committed only contiguously.
///
/// Delivered keys are enqueued in traversal order. Completions may arrive
/// out of order; `committed` advances only while the front key is complete.
/// A completion counts only for a key that is open, that is, delivered and
/// not yet committed.
///
/// ```
/// use diavasi::core::{ContiguousCommitTracker, OrderingValue};
/// let k = OrderingValue::single_u64;
/// let mut tracker = ContiguousCommitTracker::new();
/// tracker.note_delivered([k(1), k(2), k(3)]);
/// tracker.complete([k(2), k(3)]);
/// assert_eq!(tracker.committed(), &None, "1 is still open");
/// tracker.complete([k(1)]);
/// assert_eq!(tracker.committed(), &Some(k(3)));
/// ```
#[derive(Debug, Default)]
pub struct ContiguousCommitTracker {
    committed: LogicalCursor,
    /// Keys delivered (assigned at least once) but not yet under committed,
    /// in delivery order.
    open_order: VecDeque<OrderingValue>,
    /// The keys of `open_order`, for constant-time membership checks.
    open: HashSet<OrderingValue>,
    /// Open keys that have been acked.
    completed: HashSet<OrderingValue>,
}

impl ContiguousCommitTracker {
    /// A tracker with no committed position.
    pub fn new() -> Self {
        Self::default()
    }

    /// The last position with every earlier delivered record acked.
    pub fn committed(&self) -> &LogicalCursor {
        &self.committed
    }

    /// Delivered positions after the committed cursor.
    pub fn open_len(&self) -> usize {
        self.open_order.len()
    }

    /// Record that these keys have been delivered in order. A key that is
    /// already open or at or before the committed cursor is ignored, so a
    /// redelivery does not enqueue it twice.
    pub fn note_delivered<I>(&mut self, keys: I)
    where
        I: IntoIterator<Item = OrderingValue>,
    {
        for key in keys {
            if self.committed.as_ref().is_some_and(|c| &key <= c) {
                continue;
            }
            if self.open.insert(key.clone()) {
                self.open_order.push_back(key);
            }
        }
    }

    /// Mark keys complete (ack) and advance committed across the contiguous
    /// completed prefix. Keys that are not open are ignored.
    pub fn complete<I>(&mut self, keys: I)
    where
        I: IntoIterator<Item = OrderingValue>,
    {
        for key in keys {
            if self.open.contains(&key) {
                self.completed.insert(key);
            }
        }
        self.advance();
    }

    fn advance(&mut self) {
        while let Some(front) = self.open_order.front() {
            if !self.completed.remove(front) {
                break;
            }
            let key = self.open_order.pop_front().expect("front");
            self.open.remove(&key);
            self.committed = Some(key);
        }
    }

    /// Start over from a stored committed cursor, forgetting open positions.
    pub fn reset_from_committed(&mut self, committed: LogicalCursor) {
        self.committed = committed;
        self.open_order.clear();
        self.open.clear();
        self.completed.clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(n: u64) -> OrderingValue {
        OrderingValue::single_u64(n)
    }

    #[test]
    fn gap_blocks_commit() {
        let mut t = ContiguousCommitTracker::new();
        t.note_delivered([k(1), k(2), k(3)]);
        t.complete([k(2), k(3)]);
        assert_eq!(t.committed(), &None);
        t.complete([k(1)]);
        assert_eq!(t.committed(), &Some(k(3)));
    }

    #[test]
    fn in_order_advances() {
        let mut t = ContiguousCommitTracker::new();
        t.note_delivered([k(1), k(2)]);
        t.complete([k(1)]);
        assert_eq!(t.committed(), &Some(k(1)));
        t.complete([k(2)]);
        assert_eq!(t.committed(), &Some(k(2)));
    }

    /// B17: completing a key before it is delivered must not count as an ack
    /// of a later delivery of that key.
    #[test]
    fn regress_b17_completion_before_delivery_does_not_commit() {
        let mut t = ContiguousCommitTracker::new();
        t.complete([k(1)]);
        t.note_delivered([k(1), k(2)]);
        t.complete([k(2)]);
        assert_eq!(t.committed(), &None, "key 1 was never acked after delivery");
    }

    /// P5: noting delivery is linear in the number of keys, not quadratic.
    #[test]
    fn regress_p05_note_delivered_is_linear() {
        let mut t = ContiguousCommitTracker::new();
        let started = std::time::Instant::now();
        t.note_delivered((1..=20_000).map(k));
        let elapsed = started.elapsed();
        assert_eq!(t.open_len(), 20_000);
        assert!(
            elapsed < std::time::Duration::from_secs(1),
            "20k keys took {elapsed:?}"
        );
    }
}
