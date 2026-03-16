// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

//! Variable-length integer (VInt) encoding matching Java's `VIntCoding`.
//!
//! Signed integers are zigzag-encoded before varint compression.
//! The wire format encodes the byte count in the leading bits of the first byte:
//!
//! | Byte count | First byte prefix | Value bits |
//! |------------|-------------------|------------|
//! | 1          | 0xxxxxxx          | 7          |
//! | 2          | 10xxxxxx          | 14         |
//! | 3          | 110xxxxx          | 21         |
//! | 4          | 1110xxxx          | 28         |
//! | 5          | 11110xxx          | 35         |
//! | 6          | 111110xx          | 42         |
//! | 7          | 1111110x          | 49         |
//! | 8          | 11111110          | 56         |
//! | 9          | 11111111          | 64         |
//!
//! ## Java Oracle
//! - `org.apache.cassandra.utils.vint.VIntCoding`

use std::fmt;

/// Error type for VInt decoding failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum VintError {
    /// Input buffer is too short to decode a complete VInt.
    UnexpectedEof,
}

impl fmt::Display for VintError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "unexpected end of vint data")
    }
}

impl std::error::Error for VintError {}

/// Encode a signed `i64` as Cassandra VInt bytes (zigzag + varint).
///
/// # Examples
/// ```
/// # use cassandra_types::vint::encode_vint;
/// assert_eq!(encode_vint(0), vec![0x00]);
/// assert_eq!(encode_vint(-1), vec![0x01]);
/// assert_eq!(encode_vint(1), vec![0x02]);
/// ```
pub fn encode_vint(v: i64) -> Vec<u8> {
    encode_uvint(zigzag_encode(v))
}

/// Decode a Cassandra VInt from bytes.
///
/// Returns `(value, bytes_consumed)`.
///
/// # Errors
/// Returns [`VintError::UnexpectedEof`] if the buffer is too short.
pub fn decode_vint(data: &[u8]) -> Result<(i64, usize), VintError> {
    let (u, len) = decode_uvint(data)?;
    Ok((zigzag_decode(u), len))
}

// ── Zigzag helpers ──────────────────────────────────────────────────────────

/// Zigzag-encode a signed `i64` to `u64`.
///
/// Formula: `(v << 1) ^ (v >> 63)` — maps small negatives to small positives.
fn zigzag_encode(v: i64) -> u64 {
    (v.wrapping_shl(1) ^ v.wrapping_shr(63)) as u64
}

/// Zigzag-decode a `u64` back to `i64`.
fn zigzag_decode(v: u64) -> i64 {
    ((v >> 1) as i64) ^ -((v & 1) as i64)
}

// ── Unsigned VInt (wire layer) ──────────────────────────────────────────────

fn compute_uvint_size(v: u64) -> usize {
    if v < (1u64 << 7) {
        1
    } else if v < (1u64 << 14) {
        2
    } else if v < (1u64 << 21) {
        3
    } else if v < (1u64 << 28) {
        4
    } else if v < (1u64 << 35) {
        5
    } else if v < (1u64 << 42) {
        6
    } else if v < (1u64 << 49) {
        7
    } else if v < (1u64 << 56) {
        8
    } else {
        9
    }
}

fn encode_uvint(v: u64) -> Vec<u8> {
    let size = compute_uvint_size(v);
    let mut buf = vec![0u8; size];

    if size == 1 {
        buf[0] = v as u8;
        return buf;
    }

    if size == 9 {
        buf[0] = 0xFF;
        for i in 0..8 {
            buf[1 + i] = (v >> (56 - 8 * i)) as u8;
        }
        return buf;
    }

    // For size 2..=8:
    //   extra = size - 1 extra bytes after the first
    //   First byte: `extra` leading 1-bits, one stop 0-bit, then (8-size) value bits
    //   Remaining bytes: big-endian value bytes
    let extra = size - 1;

    // Write the extra bytes in big-endian order (last first)
    for i in 0..extra {
        buf[size - 1 - i] = (v >> (8 * i)) as u8;
    }

    // First byte: marker (extra leading 1s, implicit stop 0) | high value bits
    let marker: u8 = 0xFFu8.wrapping_shl((8 - extra) as u32);
    let high_bits = (v >> (8 * extra)) as u8;
    buf[0] = marker | high_bits;

    buf
}

fn decode_uvint(data: &[u8]) -> Result<(u64, usize), VintError> {
    if data.is_empty() {
        return Err(VintError::UnexpectedEof);
    }

    let first = data[0];
    let extra = first.leading_ones() as usize; // number of leading 1-bits
    let size = extra + 1; // total byte count

    if extra == 8 {
        // First byte is 0xFF: 9-byte encoding, next 8 bytes are the full value
        if data.len() < 9 {
            return Err(VintError::UnexpectedEof);
        }
        let mut v = 0u64;
        for i in 0..8 {
            v = (v << 8) | data[1 + i] as u64;
        }
        return Ok((v, 9));
    }

    if data.len() < size {
        return Err(VintError::UnexpectedEof);
    }

    // Mask to extract value bits from first byte: low (8-size) bits
    let mask: u8 = (1u8.wrapping_shl((8 - size) as u32)).wrapping_sub(1);
    let mut v = (first & mask) as u64;

    for i in 1..size {
        v = (v << 8) | data[i] as u64;
    }

    Ok((v, size))
}

// ─── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(v: i64) {
        let bytes = encode_vint(v);
        let (decoded, len) = decode_vint(&bytes).expect("decode failed");
        assert_eq!(decoded, v, "roundtrip failed for {}", v);
        assert_eq!(len, bytes.len(), "consumed bytes mismatch for {}", v);
    }

    #[test]
    fn rt_zero() {
        roundtrip(0);
        assert_eq!(encode_vint(0), vec![0x00]);
    }

    #[test]
    fn rt_minus_one() {
        roundtrip(-1);
        // zigzag(-1) = 1, size=1
        assert_eq!(encode_vint(-1), vec![0x01]);
    }

    #[test]
    fn rt_one() {
        roundtrip(1);
        assert_eq!(encode_vint(1), vec![0x02]);
    }

    #[test]
    fn rt_boundaries() {
        // 1-byte boundary: values with zigzag <= 127
        roundtrip(63);
        roundtrip(-64);
        // 2-byte boundary
        roundtrip(64);
        roundtrip(-65);
        // Large values
        roundtrip(i32::MAX as i64);
        roundtrip(i32::MIN as i64);
        roundtrip(i64::MAX);
        roundtrip(i64::MIN);
    }

    #[test]
    fn rt_all_byte_widths() {
        // Force each byte width by using known zigzag values
        let cases: &[(i64, usize)] = &[
            (0, 1),          // zigzag=0, 1 byte
            (63, 1),         // zigzag=126, 1 byte
            (64, 2),         // zigzag=128, 2 bytes
            (8191, 2),       // zigzag=16382 < 2^14, 2 bytes
            (8192, 3),       // zigzag=16384 >= 2^14, 3 bytes
            (i64::MIN, 9),   // zigzag=u64::MAX, 9 bytes
            (i64::MAX, 9),   // zigzag=u64::MAX-1, 9 bytes
        ];
        for &(v, expected_len) in cases {
            let bytes = encode_vint(v);
            assert_eq!(bytes.len(), expected_len, "length mismatch for {}", v);
            let (decoded, _) = decode_vint(&bytes).unwrap();
            assert_eq!(decoded, v, "value mismatch for {}", v);
        }
    }

    #[test]
    fn decode_partial_returns_eof() {
        let full = encode_vint(i64::MAX);
        assert_eq!(full.len(), 9);
        assert_eq!(
            decode_vint(&full[..4]),
            Err(VintError::UnexpectedEof)
        );
    }

    #[test]
    fn decode_empty_returns_eof() {
        assert_eq!(decode_vint(&[]), Err(VintError::UnexpectedEof));
    }

    #[test]
    fn golden_zero() {
        // Java: VIntCoding.writeVInt(0) = [0x00]
        assert_eq!(encode_vint(0), vec![0x00]);
    }

    #[test]
    fn golden_minus_one() {
        // Java: VIntCoding.writeVInt(-1) = [0x01]  (zigzag=1)
        assert_eq!(encode_vint(-1), vec![0x01]);
    }

    #[test]
    fn zigzag_properties() {
        // zigzag maps small negatives to small positives
        assert!(zigzag_encode(-1) < zigzag_encode(i64::MIN));
        assert!(zigzag_encode(0) < zigzag_encode(1));
        // round trip
        for v in [0i64, 1, -1, 127, -128, i32::MAX as i64, i32::MIN as i64] {
            assert_eq!(zigzag_decode(zigzag_encode(v)), v);
        }
    }
}
