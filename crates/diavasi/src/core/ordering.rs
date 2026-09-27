use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

use super::error::{CoreError, CoreResult};

/// One component of a total ordering tuple.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum OrderingAtom {
    /// A signed integer, timestamp, or complemented descending value.
    I64(i64),
    /// An unsigned integer, such as a synthetic id or a Redis stream id part.
    U64(u64),
    /// Bytes compared lexicographically: text in byte order, a binary key, or an encoded descending value.
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

/// A record's position in the group's total order: a non-empty tuple of
/// [`OrderingAtom`]s, compared left to right.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
pub struct OrderingValue(Vec<OrderingAtom>);

impl OrderingValue {
    /// A tuple of atoms. Fails when `atoms` is empty.
    ///
    /// Atoms of different kinds compare `I64 < U64 < Bytes`. A cursor
    /// serializes as a JSON array of tagged atoms, for example
    /// `[{"I64": 1700000000123456}, {"I64": 42}]`.
    ///
    /// ```
    /// use diavasi::core::{OrderingAtom, OrderingValue};
    /// let a = OrderingValue::new(vec![OrderingAtom::I64(1), OrderingAtom::Bytes(b"b".to_vec())])?;
    /// let b = OrderingValue::new(vec![OrderingAtom::I64(2)])?;
    /// assert!(a < b);
    /// assert_eq!(serde_json::to_string(&b).unwrap(), r#"[{"I64":2}]"#);
    /// # Ok::<(), diavasi::core::CoreError>(())
    /// ```
    pub fn new(atoms: Vec<OrderingAtom>) -> CoreResult<Self> {
        if atoms.is_empty() {
            return Err(CoreError::InvalidArgument(
                "ordering tuple must be non-empty".into(),
            ));
        }
        Ok(Self(atoms))
    }

    /// A one-atom `U64` tuple, as synthetic records use.
    pub fn single_u64(v: u64) -> Self {
        Self(vec![OrderingAtom::U64(v)])
    }

    /// The atoms, left to right.
    pub fn atoms(&self) -> &[OrderingAtom] {
        &self.0
    }

    /// For a one-atom `U64` tuple, the next value. `None` for other shapes or at `u64::MAX`.
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

    #[test]
    fn atom_same_variant_ordering() {
        assert!(OrderingAtom::I64(-1) < OrderingAtom::I64(0));
        assert!(OrderingAtom::U64(1) < OrderingAtom::U64(2));
        assert!(OrderingAtom::Bytes(b"a".to_vec()) < OrderingAtom::Bytes(b"b".to_vec()));
        assert_eq!(
            OrderingAtom::U64(5).partial_cmp(&OrderingAtom::U64(5)),
            Some(Ordering::Equal)
        );
    }

    #[test]
    fn atom_cross_variant_rank() {
        // I64 < U64 < Bytes
        assert!(OrderingAtom::I64(i64::MAX) < OrderingAtom::U64(0));
        assert!(OrderingAtom::U64(u64::MAX) < OrderingAtom::Bytes(vec![]));
        assert!(OrderingAtom::Bytes(vec![0]) > OrderingAtom::I64(0));
        assert!(OrderingAtom::Bytes(vec![0]) > OrderingAtom::U64(0));
    }

    #[test]
    fn ordering_value_new_rejects_empty() {
        let err = OrderingValue::new(vec![]).unwrap_err();
        assert!(matches!(err, CoreError::InvalidArgument(_)));
    }

    #[test]
    fn ordering_value_new_and_atoms() {
        let v = OrderingValue::new(vec![
            OrderingAtom::I64(1),
            OrderingAtom::U64(2),
            OrderingAtom::Bytes(b"x".to_vec()),
        ])
        .unwrap();
        assert_eq!(v.atoms().len(), 3);
        assert!(v < OrderingValue::new(vec![OrderingAtom::I64(2)]).unwrap());
    }

    #[test]
    fn succ_u64_happy_and_edge() {
        assert_eq!(
            OrderingValue::single_u64(41).succ_u64().unwrap(),
            OrderingValue::single_u64(42)
        );
        assert!(OrderingValue::single_u64(u64::MAX).succ_u64().is_none());
        assert!(
            OrderingValue::new(vec![OrderingAtom::I64(1)])
                .unwrap()
                .succ_u64()
                .is_none()
        );
        assert!(
            OrderingValue::new(vec![OrderingAtom::U64(1), OrderingAtom::U64(2)])
                .unwrap()
                .succ_u64()
                .is_none()
        );
    }

    #[test]
    fn is_after_equal_is_false() {
        let k = OrderingValue::single_u64(3);
        assert!(!is_after(&Some(k.clone()), &k));
    }
}
