use std::collections::{BTreeSet, VecDeque};

use super::ordering::{LogicalCursor, OrderingValue};

/// Tracks completed orderings and advances committed only contiguously.
///
/// Delivered keys are enqueued in traversal order. Completions may arrive
/// out of order; `committed` advances only while the front key is complete.
#[derive(Debug, Default)]
pub struct ContiguousCommitTracker {
    committed: LogicalCursor,
    /// Keys delivered (assigned at least once) but not yet under committed.
    open_order: VecDeque<OrderingValue>,
    completed: BTreeSet<OrderingValue>,
}

impl ContiguousCommitTracker {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn committed(&self) -> &LogicalCursor {
        &self.committed
    }

    pub fn open_len(&self) -> usize {
        self.open_order.len()
    }

    /// Record that these keys have been delivered in order (append-only).
    pub fn note_delivered<I>(&mut self, keys: I)
    where
        I: IntoIterator<Item = OrderingValue>,
    {
        for key in keys {
            if self.open_order.back() == Some(&key) {
                continue;
            }
            if self.committed.as_ref().is_some_and(|c| &key <= c) {
                continue;
            }
            if self.open_order.iter().any(|k| k == &key) {
                continue;
            }
            self.open_order.push_back(key);
        }
    }

    /// Mark keys complete (ACK). Advances committed across contiguous prefix.
    pub fn complete<I>(&mut self, keys: I)
    where
        I: IntoIterator<Item = OrderingValue>,
    {
        for key in keys {
            self.completed.insert(key);
        }
        self.advance();
    }

    fn advance(&mut self) {
        while let Some(front) = self.open_order.front() {
            if self.completed.contains(front) {
                let key = self.open_order.pop_front().expect("front");
                self.completed.remove(&key);
                self.committed = Some(key);
            } else {
                break;
            }
        }
    }

    pub fn reset_from_committed(&mut self, committed: LogicalCursor) {
        self.committed = committed;
        self.open_order.clear();
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
}
