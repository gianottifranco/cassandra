// Licensed under Apache License, Version 2.0.

//! Type-aware byte comparison matching Java's `AbstractType.compare`.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.marshal.AbstractType.compare(ByteBuffer, ByteBuffer)`

use crate::native::CqlType;
use crate::vint::decode_vint;
use byteorder::{BigEndian, ByteOrder};
use std::cmp::Ordering;

/// Compare two serialized CQL values of the given type.
///
/// Returns the same ordering as Java's `AbstractType.compare()`.
pub fn compare_bytes(cql_type: &CqlType, left: &[u8], right: &[u8]) -> Ordering {
    match cql_type {
        // Signed integer types: compare as big-endian signed
        CqlType::Int => cmp_signed_fixed::<4>(left, right),
        CqlType::Bigint | CqlType::Timestamp | CqlType::Time | CqlType::Counter => {
            cmp_signed_fixed::<8>(left, right)
        }
        CqlType::Smallint => cmp_signed_fixed::<2>(left, right),
        CqlType::Tinyint => (left[0] as i8).cmp(&(right[0] as i8)),

        // Unsigned byte comparison (ASCII, UTF8, blob)
        CqlType::Ascii | CqlType::Varchar | CqlType::Blob => left.cmp(right),

        // Boolean: false < true
        CqlType::Boolean => left[0].cmp(&right[0]),

        // Float: IEEE 754 comparison with special NaN handling
        CqlType::Float => cmp_float(left, right),
        CqlType::Double => cmp_double(left, right),

        // UUID: compare as two unsigned longs with version-aware ordering
        CqlType::Uuid => cmp_uuid(left, right),
        CqlType::Timeuuid => cmp_timeuuid(left, right),

        // Date: unsigned 32-bit
        CqlType::Date => {
            let a = BigEndian::read_u32(left);
            let b = BigEndian::read_u32(right);
            a.cmp(&b)
        }

        // Inet: lexicographic
        CqlType::Inet => left.cmp(right),

        // Varint: variable-length signed integer comparison
        CqlType::Varint => cmp_varint(left, right),

        // Reversed: flip the comparison
        CqlType::Reversed(inner) => compare_bytes(inner, left, right).reverse(),

        // Decimal: compare as (scale, unscaled_varint).
        // Java oracle: DecimalType.compare
        CqlType::Decimal => cmp_decimal(left, right),

        // Duration: compare months, then days, then nanoseconds.
        // Java oracle: DurationType.compare
        CqlType::Duration => cmp_duration(left, right),

        // List/Set: element-wise comparison using inner type.
        // Java oracle: ListType.compare / SetType.compare
        CqlType::List(inner, _) | CqlType::Set(inner, _) => {
            cmp_collection_seq(inner, left, right)
        }

        // Map: compare key-value pairs in order.
        // Java oracle: MapType.compare
        CqlType::Map(key_type, value_type, _) => cmp_map(key_type, value_type, left, right),

        // Tuple: positional comparison by field type.
        // Java oracle: TupleType.compare
        CqlType::Tuple(types) => cmp_tuple(types, left, right),

        // UDT: same wire format as Tuple.
        // Java oracle: UserType.compare
        CqlType::Udt { field_types, .. } => cmp_tuple(field_types, left, right),

        // Default: unsigned byte-by-byte (safe fallback)
        _ => left.cmp(right),
    }
}

fn cmp_signed_fixed<const N: usize>(left: &[u8], right: &[u8]) -> Ordering {
    // XOR the sign bit to convert signed to unsigned comparison
    let mut a = [0u8; 8];
    let mut b = [0u8; 8];
    a[8 - N..].copy_from_slice(&left[..N]);
    b[8 - N..].copy_from_slice(&right[..N]);
    a[8 - N] ^= 0x80;
    b[8 - N] ^= 0x80;
    a.cmp(&b)
}

fn cmp_float(left: &[u8], right: &[u8]) -> Ordering {
    let a = BigEndian::read_f32(left);
    let b = BigEndian::read_f32(right);
    // Match Java Float.compareTo: NaN is considered equal to NaN and greater than all other values
    a.total_cmp(&b)
}

fn cmp_double(left: &[u8], right: &[u8]) -> Ordering {
    let a = BigEndian::read_f64(left);
    let b = BigEndian::read_f64(right);
    a.total_cmp(&b)
}

/// UUID comparison matching Java's `UUIDType.compare`.
/// Compares version first, then for same version, compares as unsigned.
fn cmp_uuid(left: &[u8], right: &[u8]) -> Ordering {
    let v_a = (left[6] >> 4) & 0x0F;
    let v_b = (right[6] >> 4) & 0x0F;
    if v_a != v_b {
        return v_a.cmp(&v_b);
    }
    // Same version: unsigned comparison of all 16 bytes
    left.cmp(right)
}

/// TimeUUID comparison matching Java's `TimeUUIDType.compare`.
/// Compares by embedded timestamp first, then by remaining bytes.
fn cmp_timeuuid(left: &[u8], right: &[u8]) -> Ordering {
    // Extract 60-bit timestamp from UUID v1 layout
    let ts_a = timeuuid_timestamp(left);
    let ts_b = timeuuid_timestamp(right);
    ts_a.cmp(&ts_b).then_with(|| {
        // Tiebreak: compare clock_seq and node (bytes 8-15)
        left[8..].cmp(&right[8..])
    })
}

fn timeuuid_timestamp(uuid: &[u8]) -> u64 {
    let time_low = BigEndian::read_u32(&uuid[0..4]) as u64;
    let time_mid = BigEndian::read_u16(&uuid[4..6]) as u64;
    let time_hi = (BigEndian::read_u16(&uuid[6..8]) & 0x0FFF) as u64;
    (time_hi << 48) | (time_mid << 32) | time_low
}

/// Decimal comparison: scale first (i32), then unscaled varint.
fn cmp_decimal(left: &[u8], right: &[u8]) -> Ordering {
    if left.len() < 4 || right.len() < 4 {
        return left.len().cmp(&right.len());
    }
    let scale_l = BigEndian::read_i32(&left[0..4]);
    let scale_r = BigEndian::read_i32(&right[0..4]);
    scale_l
        .cmp(&scale_r)
        .then_with(|| cmp_varint(&left[4..], &right[4..]))
}

/// Duration comparison: months, then days, then nanoseconds.
///
/// Falls back to raw byte comparison if vint decoding fails, rather than
/// silently masking corrupt data with default values.
fn cmp_duration(left: &[u8], right: &[u8]) -> Ordering {
    let Ok((ml, n1l)) = decode_vint(left) else {
        return left.cmp(right);
    };
    let Ok((dl, n2l)) = decode_vint(&left[n1l..]) else {
        return left.cmp(right);
    };
    let Ok((nl, _)) = decode_vint(&left[n1l + n2l..]) else {
        return left.cmp(right);
    };
    let Ok((mr, n1r)) = decode_vint(right) else {
        return left.cmp(right);
    };
    let Ok((dr, n2r)) = decode_vint(&right[n1r..]) else {
        return left.cmp(right);
    };
    let Ok((nr, _)) = decode_vint(&right[n1r + n2r..]) else {
        return left.cmp(right);
    };
    ml.cmp(&mr).then(dl.cmp(&dr)).then(nl.cmp(&nr))
}

/// Read a length-prefixed element; returns (slice, bytes_consumed).
fn read_elem(data: &[u8], pos: usize) -> (&[u8], usize) {
    if pos + 4 > data.len() {
        return (&data[0..0], 0);
    }
    let len = BigEndian::read_i32(&data[pos..]);
    if len < 0 {
        return (&data[0..0], 4);
    }
    let len = len as usize;
    let end = (pos + 4 + len).min(data.len());
    (&data[pos + 4..end], 4 + len)
}

/// Read an optional length-prefixed element (null = -1).
fn read_optional_elem(data: &[u8], pos: usize) -> (Option<&[u8]>, usize) {
    if pos + 4 > data.len() {
        return (None, 0);
    }
    let len = BigEndian::read_i32(&data[pos..]);
    if len < 0 {
        (None, 4)
    } else {
        let len = len as usize;
        let end = (pos + 4 + len).min(data.len());
        (Some(&data[pos + 4..end]), 4 + len)
    }
}

/// Element-wise comparison for List and Set.
fn cmp_collection_seq(inner: &CqlType, left: &[u8], right: &[u8]) -> Ordering {
    if left.len() < 4 || right.len() < 4 {
        return left.len().cmp(&right.len());
    }
    let cnt_l = BigEndian::read_i32(&left[0..4]) as usize;
    let cnt_r = BigEndian::read_i32(&right[0..4]) as usize;
    let mut pos_l = 4usize;
    let mut pos_r = 4usize;
    let min_count = cnt_l.min(cnt_r);
    for _ in 0..min_count {
        let (el_l, n_l) = read_elem(left, pos_l);
        let (el_r, n_r) = read_elem(right, pos_r);
        pos_l += n_l;
        pos_r += n_r;
        let cmp = compare_bytes(inner, el_l, el_r);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    cnt_l.cmp(&cnt_r)
}

/// Key-then-value comparison for Map.
fn cmp_map(key_type: &CqlType, value_type: &CqlType, left: &[u8], right: &[u8]) -> Ordering {
    if left.len() < 4 || right.len() < 4 {
        return left.len().cmp(&right.len());
    }
    let cnt_l = BigEndian::read_i32(&left[0..4]) as usize;
    let cnt_r = BigEndian::read_i32(&right[0..4]) as usize;
    let mut pos_l = 4usize;
    let mut pos_r = 4usize;
    let min_count = cnt_l.min(cnt_r);
    for _ in 0..min_count {
        let (kl, nkl) = read_elem(left, pos_l);
        let (kr, nkr) = read_elem(right, pos_r);
        pos_l += nkl;
        pos_r += nkr;
        let cmp = compare_bytes(key_type, kl, kr);
        if cmp != Ordering::Equal {
            return cmp;
        }
        let (vl, nvl) = read_elem(left, pos_l);
        let (vr, nvr) = read_elem(right, pos_r);
        pos_l += nvl;
        pos_r += nvr;
        let cmp = compare_bytes(value_type, vl, vr);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    cnt_l.cmp(&cnt_r)
}

/// Positional comparison for Tuple and UDT.
fn cmp_tuple(types: &[CqlType], left: &[u8], right: &[u8]) -> Ordering {
    let mut pos_l = 0usize;
    let mut pos_r = 0usize;
    for ty in types {
        let (el_l, n_l) = read_optional_elem(left, pos_l);
        let (el_r, n_r) = read_optional_elem(right, pos_r);
        pos_l += n_l;
        pos_r += n_r;
        match (el_l, el_r) {
            (None, None) => continue,
            (None, Some(_)) => return Ordering::Less,
            (Some(_), None) => return Ordering::Greater,
            (Some(l), Some(r)) => {
                let cmp = compare_bytes(ty, l, r);
                if cmp != Ordering::Equal {
                    return cmp;
                }
            }
        }
    }
    Ordering::Equal
}

/// Variable-length signed integer comparison.
fn cmp_varint(left: &[u8], right: &[u8]) -> Ordering {
    if left.is_empty() && right.is_empty() {
        return Ordering::Equal;
    }
    if left.is_empty() {
        return Ordering::Less;
    }
    if right.is_empty() {
        return Ordering::Greater;
    }

    let sign_a = (left[0] & 0x80) != 0; // negative
    let sign_b = (right[0] & 0x80) != 0;

    match (sign_a, sign_b) {
        (true, false) => Ordering::Less,
        (false, true) => Ordering::Greater,
        _ => {
            // Same sign, compare by length then content
            let len_ord = left.len().cmp(&right.len());
            if sign_a {
                // Negative: shorter is greater, then reverse byte comparison
                len_ord.reverse().then_with(|| left.cmp(right))
            } else {
                // Positive: longer is greater, then byte comparison
                len_ord.then_with(|| left.cmp(right))
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn int_comparison() {
        let a = 42i32.to_be_bytes();
        let b = 100i32.to_be_bytes();
        let neg = (-1i32).to_be_bytes();
        assert_eq!(compare_bytes(&CqlType::Int, &a, &b), Ordering::Less);
        assert_eq!(compare_bytes(&CqlType::Int, &neg, &a), Ordering::Less);
    }

    #[test]
    fn bigint_comparison() {
        let a = 0i64.to_be_bytes();
        let neg = (-1i64).to_be_bytes();
        assert_eq!(compare_bytes(&CqlType::Bigint, &neg, &a), Ordering::Less);
    }

    #[test]
    fn text_comparison() {
        assert_eq!(
            compare_bytes(&CqlType::Varchar, b"abc", b"abd"),
            Ordering::Less
        );
        assert_eq!(
            compare_bytes(&CqlType::Varchar, b"abc", b"abc"),
            Ordering::Equal
        );
    }

    #[test]
    fn float_nan_handling() {
        let nan = f32::NAN.to_be_bytes();
        let one = 1.0f32.to_be_bytes();
        // NaN should be greater than all values (matching Java)
        assert_eq!(
            compare_bytes(&CqlType::Float, &nan, &one),
            Ordering::Greater
        );
    }

    #[test]
    fn reversed_flips_order() {
        let rev = CqlType::Reversed(Box::new(CqlType::Int));
        let a = 42i32.to_be_bytes();
        let b = 100i32.to_be_bytes();
        assert_eq!(compare_bytes(&rev, &a, &b), Ordering::Greater);
    }

    #[test]
    fn duration_corrupt_falls_back_to_byte_comparison() {
        // Corrupt data that can't be vint-decoded should fall back to byte comparison
        // rather than silently returning a wrong ordering
        let a = [0xFF, 0xFF, 0xFF]; // truncated vint
        let b = [0xFF, 0xFF, 0xFE];
        assert_eq!(compare_bytes(&CqlType::Duration, &a, &b), Ordering::Greater);
    }

    #[test]
    fn tinyint_signed() {
        let a = [0xFEu8]; // -2 as i8
        let b = [0x01u8]; // 1 as i8
        assert_eq!(compare_bytes(&CqlType::Tinyint, &a, &b), Ordering::Less);
    }
}
