use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

use super::error::{CoreError, CoreResult};

/// One component of a total ordering tuple.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderingAtom {
    I64(i64),
    U64(u64),
    Bytes(Vec<u8>),
}

impl PartialOrd for OrderingAtom {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for OrderingAtom {
    fn cmp(&self, other: &Self) -> Ordering {
        use OrderingAtom::*;
        match (self, other) {
            (I64(a), I64(b)) => a.cmp(b),
            (U64(a), U64(b)) => a.cmp(b),
            (Bytes(a), Bytes(b)) => a.cmp(b),
            (I64(_), _) => Ordering::Less,
            (_, I64(_)) => Ordering::Greater,
            (U64(_), Bytes(_)) => Ordering::Less,
            (Bytes(_), U64(_)) => Ordering::Greater,
        }
    }
}

/// Total ordering tuple. Empty tuples are rejected.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OrderingValue(Vec<OrderingAtom>);

impl OrderingValue {
    pub fn new(atoms: Vec<OrderingAtom>) -> CoreResult<Self> {
        if atoms.is_empty() {
            return Err(CoreError::InvalidArgument(
                "ordering tuple must be non-empty",
            ));
        }
        Ok(Self(atoms))
    }

    pub fn single_u64(v: u64) -> Self {
        Self(vec![OrderingAtom::U64(v)])
    }

    pub fn atoms(&self) -> &[OrderingAtom] {
        &self.0
    }

    /// Successor for Stage 1 synthetic single-u64 keys.
    pub fn succ_u64(&self) -> Option<Self> {
        match self.0.as_slice() {
            [OrderingAtom::U64(v)] => Some(Self::single_u64(v.checked_add(1)?)),
            _ => None,
        }
    }
}

/// Last durably safe position. `None` means start of stream.
pub type LogicalCursor = Option<OrderingValue>;

/// True when `key` is strictly after `cursor` in traversal order.
pub fn is_after(cursor: &LogicalCursor, key: &OrderingValue) -> bool {
    match cursor {
        None => true,
        Some(c) => key > c,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn lexicographic_order() {
        let a = OrderingValue::single_u64(1);
        let b = OrderingValue::single_u64(2);
        assert!(a < b);
        assert!(is_after(&None, &a));
        assert!(!is_after(&Some(b.clone()), &a));
        assert!(is_after(&Some(a), &b));
    }
}
