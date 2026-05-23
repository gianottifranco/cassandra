// Licensed under Apache License, Version 2.0.

//! Order-preserving byte encoding for BTI/SAI index support.
//!
//! The `ByteComparable` trait produces byte sequences whose unsigned
//! lexicographic order matches the semantic ordering of the original type.
//! This is required for building B-tree and trie indexes (BTI) and
//! Storage-Attached Indexes (SAI) that rely on byte-ordered keys.
//!
//! ## Encoding rules
//!
//! | Type           | Encoding                                          |
//! |----------------|---------------------------------------------------|
//! | Signed ints    | XOR sign bit (flip 0x80 on MSB)                   |
//! | Floats         | IEEE 754 order-preserving transform                |
//! | Strings/blobs  | Escape 0x00 bytes, terminate with 0x00 0x00        |
//! | UUIDs          | Version-aware: version byte, then raw bytes        |
//! | Varints        | Length-prefixed with sign-aware encoding            |
//!
//! ## Java Oracle
//! - `org.apache.cassandra.utils.bytecomparable.ByteComparable`
//! - `org.apache.cassandra.utils.bytecomparable.ByteSource`

use crate::native::CqlType;
use byteorder::{BigEndian, ByteOrder};

/// Separator markers for multi-component keys.
pub const SEPARATOR: u8 = 0x40;
pub const TERMINATOR: u8 = 0x38;

/// End-of-stream marker returned by [`ByteSource::next`].
pub const BYTE_SOURCE_END: i32 = -1;

/// Cursor over a byte-comparable sequence.
///
/// Cassandra's Java `ByteSource` exposes bytes one-at-a-time so trie code can
/// stream comparisons without allocating whole keys. This Rust equivalent owns
/// the encoded bytes but preserves the cursor-style API used by trie and range
/// bound code.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ByteSource {
    bytes: Vec<u8>,
    offset: usize,
}

impl ByteSource {
    /// Create a source from already encoded bytes.
    pub fn new(bytes: Vec<u8>) -> Self {
        Self { bytes, offset: 0 }
    }

    /// Create a source by encoding a serialized CQL value.
    pub fn from_value(cql_type: &CqlType, data: &[u8]) -> Self {
        Self::new(encode_byte_comparable(cql_type, data))
    }

    /// Return the next byte, or [`BYTE_SOURCE_END`] once exhausted.
    pub fn next_byte(&mut self) -> i32 {
        if self.offset >= self.bytes.len() {
            return BYTE_SOURCE_END;
        }
        let byte = self.bytes[self.offset];
        self.offset += 1;
        i32::from(byte)
    }

    /// Peek at the next byte without advancing.
    pub fn peek(&self) -> i32 {
        self.bytes
            .get(self.offset)
            .map(|byte| i32::from(*byte))
            .unwrap_or(BYTE_SOURCE_END)
    }

    /// Reset this source to the first byte.
    pub fn reset(&mut self) {
        self.offset = 0;
    }

    /// Return true when all bytes have been consumed.
    pub fn is_exhausted(&self) -> bool {
        self.offset >= self.bytes.len()
    }

    /// Remaining encoded bytes.
    pub fn remaining(&self) -> &[u8] {
        &self.bytes[self.offset..]
    }

    /// Full encoded byte sequence.
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Consume the source and return the encoded bytes.
    pub fn into_bytes(self) -> Vec<u8> {
        self.bytes
    }
}

/// Encode a serialized CQL value into an order-preserving byte sequence.
pub fn encode_byte_comparable(cql_type: &CqlType, data: &[u8]) -> Vec<u8> {
    match cql_type {
        CqlType::Tinyint => encode_signed_int(data),
        CqlType::Smallint => encode_signed_int(data),
        CqlType::Int | CqlType::Date => encode_signed_int(data),
        CqlType::Bigint | CqlType::Timestamp | CqlType::Time | CqlType::Counter => {
            encode_signed_int(data)
        }
        CqlType::Float => encode_float32(data),
        CqlType::Double => encode_float64(data),
        CqlType::Ascii | CqlType::Varchar | CqlType::Blob => encode_byte_string(data),
        CqlType::Uuid | CqlType::Timeuuid => encode_uuid(data),
        CqlType::Boolean => data.to_vec(),
        CqlType::Varint => encode_varint(data),
        CqlType::Reversed(inner) => {
            let encoded = encode_byte_comparable(inner, data);
            // Invert all bytes to reverse the ordering
            encoded.into_iter().map(|b| !b).collect()
        }
        CqlType::Vector(inner, dimensions) => encode_vector(inner, *dimensions, data),
        // Default: use raw bytes (already byte-ordered for unsigned types)
        _ => data.to_vec(),
    }
}

/// Concatenate already encoded components using Cassandra-style separators.
pub fn encode_components(components: &[Vec<u8>]) -> Vec<u8> {
    let mut result = Vec::new();
    for (idx, component) in components.iter().enumerate() {
        if idx > 0 {
            result.push(SEPARATOR);
        }
        result.extend_from_slice(component);
    }
    result.push(TERMINATOR);
    result
}

/// Encode serialized CQL components using their corresponding types.
pub fn encode_typed_components(components: &[(&CqlType, &[u8])]) -> Vec<u8> {
    let encoded = components
        .iter()
        .map(|(ty, data)| encode_byte_comparable(ty, data))
        .collect::<Vec<_>>();
    encode_components(&encoded)
}

/// Return a short separator `s` such that `previous < s <= next` when one can
/// be represented by a strict prefix/range increment.
///
/// This is useful for trie summaries and index block separators: it keeps the
/// separator as short as possible while preserving unsigned lexicographic order.
pub fn separator_between(previous: &[u8], next: &[u8]) -> Vec<u8> {
    if previous >= next {
        return next.to_vec();
    }

    let common = previous
        .iter()
        .zip(next.iter())
        .take_while(|(a, b)| a == b)
        .count();

    if common == previous.len() {
        return previous.to_vec();
    }

    let prev_byte = previous[common];
    let next_byte = next[common];
    if prev_byte < 0xff && prev_byte + 1 < next_byte {
        let mut separator = previous[..common].to_vec();
        separator.push(prev_byte + 1);
        separator
    } else {
        let mut separator = next[..=common].to_vec();
        if separator.as_slice() <= previous {
            separator = next.to_vec();
        }
        separator
    }
}

/// Return the shortest byte string that sorts strictly after `key`, if one
/// exists by incrementing a trailing byte and truncating the suffix.
pub fn successor(key: &[u8]) -> Option<Vec<u8>> {
    let idx = key.iter().rposition(|byte| *byte != 0xff)?;
    let mut result = key[..=idx].to_vec();
    result[idx] += 1;
    Some(result)
}

/// XOR the sign bit to make signed integers sort correctly as unsigned bytes.
fn encode_signed_int(data: &[u8]) -> Vec<u8> {
    let mut result = data.to_vec();
    if !result.is_empty() {
        result[0] ^= 0x80;
    }
    result
}

/// IEEE 754 order-preserving float encoding.
///
/// For positive floats: flip the sign bit.
/// For negative floats: flip all bits.
/// This ensures correct unsigned byte ordering.
fn encode_float32(data: &[u8]) -> Vec<u8> {
    if data.len() < 4 {
        return data.to_vec();
    }
    let bits = BigEndian::read_u32(data);
    let encoded = if bits & 0x80000000 != 0 {
        !bits // negative: flip all bits
    } else {
        bits ^ 0x80000000 // positive: flip sign bit
    };
    encoded.to_be_bytes().to_vec()
}

fn encode_float64(data: &[u8]) -> Vec<u8> {
    if data.len() < 8 {
        return data.to_vec();
    }
    let bits = BigEndian::read_u64(data);
    let encoded = if bits & 0x8000000000000000 != 0 {
        !bits
    } else {
        bits ^ 0x8000000000000000
    };
    encoded.to_be_bytes().to_vec()
}

/// Escape-encoded byte string for order-preserving comparison.
///
/// Replaces 0x00 bytes with 0x00 0xFF, then terminates with 0x00 0x00.
fn encode_byte_string(data: &[u8]) -> Vec<u8> {
    let mut result = Vec::with_capacity(data.len() + 2);
    for &b in data {
        result.push(b);
        if b == 0x00 {
            result.push(0xFF); // escape 0x00 → 0x00 0xFF
        }
    }
    result.push(0x00);
    result.push(0x00); // terminator
    result
}

/// UUID encoding: version byte first for version-aware ordering, then raw bytes.
fn encode_uuid(data: &[u8]) -> Vec<u8> {
    if data.len() != 16 {
        return data.to_vec();
    }
    let version = (data[6] >> 4) & 0x0F;
    let mut result = Vec::with_capacity(17);
    result.push(version);
    result.extend_from_slice(data);
    result
}

/// Variable-length integer: encode with sign-aware length prefix.
///
/// The encoding ensures that:
/// - Negative varints sort before positive ones
/// - Within the same sign, longer values sort higher for positive, lower for negative
fn encode_varint(data: &[u8]) -> Vec<u8> {
    if data.is_empty() {
        return vec![0x80]; // zero
    }
    let negative = data[0] & 0x80 != 0;
    let len = data.len() as u8;
    let mut result = Vec::with_capacity(data.len() + 2);

    if negative {
        // Negative: length encodes inversely (shorter magnitude = closer to zero = larger)
        result.push(0x80u8.wrapping_sub(len));
    } else {
        // Positive: longer = larger
        result.push(0x80u8.wrapping_add(len));
    }
    result.extend_from_slice(data);
    result
}

fn encode_vector(inner: &CqlType, dimensions: u32, data: &[u8]) -> Vec<u8> {
    let Some(element_size) = inner.fixed_size() else {
        return data.to_vec();
    };
    let expected_len = element_size.saturating_mul(dimensions as usize);
    if data.len() != expected_len {
        return data.to_vec();
    }
    let mut result = Vec::with_capacity(data.len());
    for idx in 0..dimensions as usize {
        let start = idx * element_size;
        let end = start + element_size;
        result.extend_from_slice(&encode_byte_comparable(inner, &data[start..end]));
    }
    result
}

/// Decode a byte-comparable encoded signed integer back to the original bytes.
pub fn decode_signed_int(data: &[u8]) -> Vec<u8> {
    let mut result = data.to_vec();
    if !result.is_empty() {
        result[0] ^= 0x80;
    }
    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn signed_int_ordering() {
        // -1 (0xFF..FF) should sort before 0 (0x00..00) should sort before 1 (0x00..01)
        let neg1 = encode_byte_comparable(&CqlType::Int, &(-1i32).to_be_bytes());
        let zero = encode_byte_comparable(&CqlType::Int, &0i32.to_be_bytes());
        let pos1 = encode_byte_comparable(&CqlType::Int, &1i32.to_be_bytes());
        assert!(neg1 < zero, "neg1 < zero");
        assert!(zero < pos1, "zero < pos1");
    }

    #[test]
    fn signed_int_roundtrip() {
        for v in [i32::MIN, -1, 0, 1, i32::MAX] {
            let encoded = encode_byte_comparable(&CqlType::Int, &v.to_be_bytes());
            let decoded = decode_signed_int(&encoded);
            let back = i32::from_be_bytes([decoded[0], decoded[1], decoded[2], decoded[3]]);
            assert_eq!(back, v);
        }
    }

    #[test]
    fn float_ordering() {
        let neg = encode_byte_comparable(&CqlType::Float, &(-1.0f32).to_be_bytes());
        let zero = encode_byte_comparable(&CqlType::Float, &0.0f32.to_be_bytes());
        let pos = encode_byte_comparable(&CqlType::Float, &1.0f32.to_be_bytes());
        assert!(neg < zero);
        assert!(zero < pos);
    }

    #[test]
    fn double_ordering() {
        let neg = encode_byte_comparable(&CqlType::Double, &(-1.0f64).to_be_bytes());
        let zero = encode_byte_comparable(&CqlType::Double, &0.0f64.to_be_bytes());
        let pos = encode_byte_comparable(&CqlType::Double, &1.0f64.to_be_bytes());
        assert!(neg < zero);
        assert!(zero < pos);
    }

    #[test]
    fn string_ordering_with_null_bytes() {
        let a = encode_byte_comparable(&CqlType::Varchar, &[0x00, 0x01]);
        let b = encode_byte_comparable(&CqlType::Varchar, &[0x00, 0x02]);
        let c = encode_byte_comparable(&CqlType::Varchar, &[0x01]);
        assert!(a < b);
        assert!(a < c);
    }

    #[test]
    fn string_prefix_ordering() {
        // "a" should sort before "ab"
        let a = encode_byte_comparable(&CqlType::Varchar, b"a");
        let ab = encode_byte_comparable(&CqlType::Varchar, b"ab");
        assert!(a < ab);
    }

    #[test]
    fn reversed_flips_order() {
        let rev = CqlType::Reversed(Box::new(CqlType::Int));
        let a = encode_byte_comparable(&rev, &1i32.to_be_bytes());
        let b = encode_byte_comparable(&rev, &2i32.to_be_bytes());
        assert!(a > b, "reversed should flip ordering");
    }

    #[test]
    fn varint_ordering() {
        // -1 < 0 < 1 < 128
        let neg1 = encode_byte_comparable(&CqlType::Varint, &[0xFF]); // -1
        let zero = encode_byte_comparable(&CqlType::Varint, &[0x00]); // 0
        let pos1 = encode_byte_comparable(&CqlType::Varint, &[0x01]); // 1
        let pos128 = encode_byte_comparable(&CqlType::Varint, &[0x00, 0x80]); // 128
        assert!(neg1 < zero);
        assert!(zero < pos1);
        assert!(pos1 < pos128);
    }

    #[test]
    fn uuid_encoding_includes_version() {
        let mut uuid = [0u8; 16];
        uuid[6] = 0x40; // version 4
        let encoded = encode_byte_comparable(&CqlType::Uuid, &uuid);
        assert_eq!(encoded.len(), 17);
        assert_eq!(encoded[0], 4); // version byte
    }

    #[test]
    fn bigint_ordering() {
        let neg = encode_byte_comparable(&CqlType::Bigint, &(-100i64).to_be_bytes());
        let zero = encode_byte_comparable(&CqlType::Bigint, &0i64.to_be_bytes());
        let pos = encode_byte_comparable(&CqlType::Bigint, &100i64.to_be_bytes());
        assert!(neg < zero);
        assert!(zero < pos);
    }

    #[test]
    fn vector_orders_by_encoded_elements() {
        let ty = CqlType::Vector(Box::new(CqlType::Float), 2);
        let neg = [-1.0_f32, 2.0]
            .into_iter()
            .flat_map(f32::to_be_bytes)
            .collect::<Vec<_>>();
        let zero = [0.0_f32, 2.0]
            .into_iter()
            .flat_map(f32::to_be_bytes)
            .collect::<Vec<_>>();
        let later_second = [0.0_f32, 3.0]
            .into_iter()
            .flat_map(f32::to_be_bytes)
            .collect::<Vec<_>>();

        assert!(encode_byte_comparable(&ty, &neg) < encode_byte_comparable(&ty, &zero));
        assert!(encode_byte_comparable(&ty, &zero) < encode_byte_comparable(&ty, &later_second));
    }

    #[test]
    fn byte_source_streams_encoded_bytes() {
        let encoded = encode_byte_comparable(&CqlType::Varchar, b"a");
        let mut source = ByteSource::from_value(&CqlType::Varchar, b"a");

        assert_eq!(source.peek(), i32::from(encoded[0]));
        assert_eq!(source.next_byte(), i32::from(encoded[0]));
        assert_eq!(source.remaining(), &encoded[1..]);
        while source.next_byte() != BYTE_SOURCE_END {}
        assert!(source.is_exhausted());
        assert_eq!(source.next_byte(), BYTE_SOURCE_END);

        source.reset();
        assert_eq!(source.as_bytes(), encoded.as_slice());
    }

    #[test]
    fn typed_components_use_separators_and_terminator() {
        let key = encode_typed_components(&[
            (&CqlType::Int, &1i32.to_be_bytes()),
            (&CqlType::Varchar, b"abc"),
        ]);

        assert!(key.contains(&SEPARATOR));
        assert_eq!(key.last().copied(), Some(TERMINATOR));

        let first = encode_typed_components(&[
            (&CqlType::Int, &1i32.to_be_bytes()),
            (&CqlType::Varchar, b"abc"),
        ]);
        let second = encode_typed_components(&[
            (&CqlType::Int, &2i32.to_be_bytes()),
            (&CqlType::Varchar, b"abc"),
        ]);
        assert!(first < second);
    }

    #[test]
    fn separator_between_shortens_index_bounds() {
        let previous = b"abc123";
        let next = b"abd999";
        let separator = separator_between(previous, next);

        assert!(separator.as_slice() > previous.as_slice());
        assert!(separator.as_slice() <= next.as_slice());
        assert_eq!(separator, b"abd");
    }

    #[test]
    fn separator_between_handles_adjacent_bytes() {
        let previous = b"abc";
        let next = b"abd";
        let separator = separator_between(previous, next);

        assert!(separator.as_slice() > previous.as_slice());
        assert!(separator.as_slice() <= next.as_slice());
        assert_eq!(separator, next);
    }

    #[test]
    fn successor_increments_last_non_ff_byte() {
        assert_eq!(successor(&[0x01, 0x02, 0xff]), Some(vec![0x01, 0x03]));
        assert_eq!(successor(&[0xff, 0xff]), None);
    }
}
