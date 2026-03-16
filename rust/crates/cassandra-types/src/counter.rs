// Licensed under Apache License, Version 2.0.

//! Counter mutation and value semantics.
//!
//! Counters in Cassandra are distributed CRDT-like values that only support
//! increment/decrement operations.  On the wire, a counter value is an i64
//! in big-endian format — identical to `Bigint`.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.context.CounterContext`
//! - `org.apache.cassandra.serializers.CounterSerializer`

use byteorder::{BigEndian, ByteOrder};

/// A counter mutation (delta to apply).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CounterMutation {
    pub delta: i64,
}

/// A resolved counter value.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CounterValue {
    pub value: i64,
}

impl CounterValue {
    /// Apply a delta to this counter, returning the new value.
    pub fn apply_delta(&self, mutation: &CounterMutation) -> CounterValue {
        CounterValue {
            value: self.value.wrapping_add(mutation.delta),
        }
    }

    /// Serialize to wire format (big-endian i64, identical to Bigint).
    pub fn to_bytes(&self) -> [u8; 8] {
        let mut b = [0u8; 8];
        BigEndian::write_i64(&mut b, self.value);
        b
    }

    /// Deserialize from wire format.
    pub fn from_bytes(data: &[u8; 8]) -> Self {
        CounterValue {
            value: BigEndian::read_i64(data),
        }
    }
}

impl CounterMutation {
    /// Serialize the delta to wire format (big-endian i64).
    pub fn to_bytes(&self) -> [u8; 8] {
        let mut b = [0u8; 8];
        BigEndian::write_i64(&mut b, self.delta);
        b
    }
}

/// Returns true if all columns in the column family are counter types.
/// Counter column families must not mix counter and non-counter columns.
pub fn validate_counter_only(is_counter_column: &[bool]) -> bool {
    if is_counter_column.is_empty() {
        return true;
    }
    let first = is_counter_column[0];
    is_counter_column.iter().all(|&c| c == first)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn apply_delta_positive() {
        let cv = CounterValue { value: 10 };
        let m = CounterMutation { delta: 5 };
        assert_eq!(cv.apply_delta(&m).value, 15);
    }

    #[test]
    fn apply_delta_negative() {
        let cv = CounterValue { value: 10 };
        let m = CounterMutation { delta: -3 };
        assert_eq!(cv.apply_delta(&m).value, 7);
    }

    #[test]
    fn wire_format_identity_with_bigint() {
        // Counter wire format is identical to Bigint (big-endian i64)
        let cv = CounterValue { value: 42 };
        let bytes = cv.to_bytes();
        assert_eq!(bytes, 42i64.to_be_bytes());
    }

    #[test]
    fn roundtrip() {
        for v in [i64::MIN, -1, 0, 1, i64::MAX] {
            let cv = CounterValue { value: v };
            let bytes = cv.to_bytes();
            let back = CounterValue::from_bytes(&bytes);
            assert_eq!(cv, back);
        }
    }

    #[test]
    fn wrapping_overflow() {
        let cv = CounterValue { value: i64::MAX };
        let m = CounterMutation { delta: 1 };
        assert_eq!(cv.apply_delta(&m).value, i64::MIN);
    }

    #[test]
    fn validate_counter_only_homogeneous() {
        assert!(validate_counter_only(&[true, true, true]));
        assert!(validate_counter_only(&[false, false]));
        assert!(validate_counter_only(&[]));
    }

    #[test]
    fn validate_counter_only_mixed() {
        assert!(!validate_counter_only(&[true, false, true]));
    }
}
