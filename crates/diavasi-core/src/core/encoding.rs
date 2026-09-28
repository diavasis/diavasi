//! Order-preserving encodings for descending sort keys.
//!
//! A logical cursor increases along the stream. For a field sorted in
//! descending order, adapters store an encoding of the value that increases as
//! the value falls, and decode it to build the next query.

/// A signed value that sorts in the stream's direction: the value itself, or
/// its bitwise complement when `descending`.
///
/// ```
/// use diavasi::core::encoding::order_i64;
/// assert!(order_i64(5, true) < order_i64(3, true));
/// assert_eq!(order_i64(order_i64(-7, true), true), -7);
/// ```
pub fn order_i64(value: i64, descending: bool) -> i64 {
    if descending { !value } else { value }
}

/// Bytes that sort in the stream's direction. Ascending keeps the bytes.
/// Descending stores the bitwise complement of a memcomparable encoding (8-byte
/// groups, each followed by its used length), which also reverses the order of
/// a value and its own prefix.
///
/// ```
/// use diavasi::core::encoding::{order_bytes, unorder_bytes};
/// let (ab, abc) = (order_bytes(b"ab", true), order_bytes(b"abc", true));
/// assert!(abc < ab, "descending: the longer value sorts first");
/// assert_eq!(unorder_bytes(&ab, true).unwrap(), b"ab");
/// ```
pub fn order_bytes(bytes: &[u8], descending: bool) -> Vec<u8> {
    if descending {
        encode_memcomparable(bytes)
            .into_iter()
            .map(|b| !b)
            .collect()
    } else {
        bytes.to_vec()
    }
}

/// The original bytes of an [`order_bytes`] value.
pub fn unorder_bytes(bytes: &[u8], descending: bool) -> Result<Vec<u8>, String> {
    if descending {
        let flipped: Vec<u8> = bytes.iter().map(|b| !b).collect();
        decode_memcomparable(&flipped)
    } else {
        Ok(bytes.to_vec())
    }
}

fn encode_memcomparable(src: &[u8]) -> Vec<u8> {
    let mut buf = Vec::with_capacity((src.len() / 8 + 1) * 9);
    let mut chunks = src.chunks(8);
    loop {
        let chunk = chunks.next().unwrap_or(&[]);
        let mut group = [0u8; 9];
        group[..chunk.len()].copy_from_slice(chunk);
        group[8] = chunk.len() as u8;
        buf.extend_from_slice(&group);
        if chunk.len() < 8 {
            return buf;
        }
    }
}

fn decode_memcomparable(src: &[u8]) -> Result<Vec<u8>, String> {
    if src.is_empty() || src.len() % 9 != 0 {
        return Err("bad ordered bytes".into());
    }
    let mut out = Vec::new();
    for group in src.chunks_exact(9) {
        let n = group[8] as usize;
        if n > 8 || group[n..8].iter().any(|byte| *byte != 0) {
            return Err("bad ordered bytes".into());
        }
        out.extend_from_slice(&group[..n]);
        if n < 8 {
            return Ok(out);
        }
    }
    Err("ordered bytes ended on a full group".into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    proptest! {
        #[test]
        fn descending_bytes_reverse_order_and_round_trip(
            a in proptest::collection::vec(any::<u8>(), 0..40),
            b in proptest::collection::vec(any::<u8>(), 0..40),
        ) {
            let (ea, eb) = (order_bytes(&a, true), order_bytes(&b, true));
            prop_assert_eq!(a.cmp(&b).reverse(), ea.cmp(&eb));
            prop_assert_eq!(unorder_bytes(&ea, true).unwrap(), a);
        }

        #[test]
        fn descending_i64_reverses_order(a in any::<i64>(), b in any::<i64>()) {
            prop_assert_eq!(a.cmp(&b).reverse(), order_i64(a, true).cmp(&order_i64(b, true)));
        }
    }

    #[test]
    fn ascending_is_identity_and_bad_input_fails() {
        assert_eq!(order_bytes(b"xyz", false), b"xyz");
        assert_eq!(order_i64(-3, false), -3);
        assert!(unorder_bytes(&[1, 2, 3], true).is_err());
    }
}
