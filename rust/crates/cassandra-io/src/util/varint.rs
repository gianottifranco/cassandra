// Licensed under Apache License, Version 2.0.

//! Variable-length integer encoding compatible with Java's VIntCoding.
//!
//! Unsigned VInt uses a prefix encoding where the number of leading 1-bits in
//! the first byte indicates how many extra bytes follow. Values 0-127 fit in a
//! single byte. Signed VInt uses zig-zag encoding on top of the unsigned form.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.utils.vint.VIntCoding`

use std::io::{self, Read, Write};

// ---------------------------------------------------------------------------
// Sizing
// ---------------------------------------------------------------------------

/// Returns the number of bytes needed to encode `value` as an unsigned VInt.
pub fn unsigned_vint_size(value: u64) -> usize {
    // Number of meaningful bits, then map to byte count.
    let magnitude = 64 - value.leading_zeros() as usize; // 0 for value==0
    match magnitude {
        0..=7 => 1,
        8..=14 => 2,
        15..=21 => 3,
        22..=28 => 4,
        29..=35 => 5,
        36..=42 => 6,
        43..=49 => 7,
        50..=56 => 8,
        _ => 9,
    }
}

/// Number of extra bytes that follow the first byte, derived from the leading
/// one-bits in `first_byte`.
#[inline]
fn extra_bytes_for_first(first_byte: u8) -> usize {
    (!first_byte).leading_zeros() as usize
}

// ---------------------------------------------------------------------------
// Zig-zag helpers
// ---------------------------------------------------------------------------

#[inline]
fn zigzag_encode(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

#[inline]
fn zigzag_decode(v: u64) -> i64 {
    ((v >> 1) as i64) ^ (-((v & 1) as i64))
}

// ---------------------------------------------------------------------------
// Buffer-based encode / decode
// ---------------------------------------------------------------------------

/// Encodes `value` as an unsigned VInt into `buf`. Returns the number of bytes
/// written. The caller must ensure `buf` is large enough (at most 9 bytes).
pub fn encode_unsigned_vint(value: u64, buf: &mut [u8]) -> usize {
    let size = unsigned_vint_size(value);
    if size == 1 {
        buf[0] = value as u8;
        return 1;
    }

    // 9-byte special case: first byte is all 1-bits (0xFF), remaining 8 bytes
    // hold the full u64 big-endian.
    if size == 9 {
        buf[0] = 0xFF;
        let be = value.to_be_bytes();
        buf[1..9].copy_from_slice(&be);
        return 9;
    }

    let extra = size - 1;
    // First byte: `extra` leading 1-bits, then a 0 separator, then data bits.
    let data_bits_in_first = 8 - extra - 1;
    let total_extra_bits = extra * 8;

    // Prefix mask: e.g. extra=1 -> 0x80, extra=2 -> 0xC0 ...
    let prefix: u8 = !((1u16 << (8 - extra)) as u8 - 1);
    let first_data = ((value >> total_extra_bits) as u8) & ((1u8 << data_bits_in_first) - 1);
    buf[0] = prefix | first_data;

    // Remaining bytes, big-endian.
    for (i, byte) in buf.iter_mut().enumerate().take(size).skip(1) {
        let shift = (size - 1 - i) * 8;
        *byte = (value >> shift) as u8;
    }
    size
}

/// Decodes an unsigned VInt from `buf`. Returns `(value, bytes_consumed)`.
pub fn decode_unsigned_vint(buf: &[u8]) -> (u64, usize) {
    let first = buf[0];
    let extra = extra_bytes_for_first(first);

    if extra == 0 {
        return (first as u64, 1);
    }

    // 9-byte special case: first byte is 0xFF, next 8 bytes are u64 BE.
    if extra >= 8 {
        let mut be = [0u8; 8];
        be.copy_from_slice(&buf[1..9]);
        return (u64::from_be_bytes(be), 9);
    }

    let size = extra + 1;
    // Mask off the prefix bits in the first byte.
    let data_bits_in_first = 8 - extra - 1;
    let mask = (1u8 << data_bits_in_first) - 1;
    let mut value = (first & mask) as u64;

    for byte in buf.iter().take(size).skip(1) {
        value = (value << 8) | (*byte as u64);
    }
    (value, size)
}

/// Encodes a signed VInt (zig-zag + unsigned). Returns bytes written.
pub fn encode_vint(value: i64, buf: &mut [u8]) -> usize {
    encode_unsigned_vint(zigzag_encode(value), buf)
}

/// Decodes a signed VInt. Returns `(value, bytes_consumed)`.
pub fn decode_vint(buf: &[u8]) -> (i64, usize) {
    let (raw, n) = decode_unsigned_vint(buf);
    (zigzag_decode(raw), n)
}

// ---------------------------------------------------------------------------
// Stream-based read / write
// ---------------------------------------------------------------------------

/// Reads an unsigned VInt from a `Read` stream.
pub fn read_unsigned_vint(reader: &mut (impl Read + ?Sized)) -> io::Result<u64> {
    let mut first = [0u8; 1];
    reader.read_exact(&mut first)?;
    let extra = extra_bytes_for_first(first[0]);

    if extra == 0 {
        return Ok(first[0] as u64);
    }

    // 9-byte special case: first byte 0xFF, next 8 bytes are u64 BE.
    if extra >= 8 {
        let mut be = [0u8; 8];
        reader.read_exact(&mut be)?;
        return Ok(u64::from_be_bytes(be));
    }

    let data_bits_in_first = 8 - extra - 1;
    let mask = (1u8 << data_bits_in_first) - 1;
    let mut value = (first[0] & mask) as u64;

    let mut rest = [0u8; 8];
    reader.read_exact(&mut rest[..extra])?;
    for byte in rest.iter().take(extra) {
        value = (value << 8) | (*byte as u64);
    }
    Ok(value)
}

/// Writes an unsigned VInt to a `Write` stream. Returns bytes written.
pub fn write_unsigned_vint(writer: &mut (impl Write + ?Sized), value: u64) -> io::Result<usize> {
    let mut buf = [0u8; 9];
    let n = encode_unsigned_vint(value, &mut buf);
    writer.write_all(&buf[..n])?;
    Ok(n)
}

/// Reads a signed VInt (zig-zag encoded) from a `Read` stream.
pub fn read_vint(reader: &mut (impl Read + ?Sized)) -> io::Result<i64> {
    let raw = read_unsigned_vint(reader)?;
    Ok(zigzag_decode(raw))
}

/// Writes a signed VInt (zig-zag encoded) to a `Write` stream. Returns bytes
/// written.
pub fn write_vint(writer: &mut (impl Write + ?Sized), value: i64) -> io::Result<usize> {
    write_unsigned_vint(writer, zigzag_encode(value))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_zigzag_roundtrip() {
        for &v in &[0i64, 1, -1, 42, -42, i64::MAX, i64::MIN] {
            assert_eq!(zigzag_decode(zigzag_encode(v)), v);
        }
    }

    #[test]
    fn test_unsigned_vint_size() {
        assert_eq!(unsigned_vint_size(0), 1);
        assert_eq!(unsigned_vint_size(127), 1);
        assert_eq!(unsigned_vint_size(128), 2);
        assert_eq!(unsigned_vint_size(16383), 2);
        assert_eq!(unsigned_vint_size(16384), 3);
        assert_eq!(unsigned_vint_size(u64::MAX), 9);
    }

    #[test]
    fn test_encode_decode_unsigned_single_byte() {
        for v in 0..=127u64 {
            let mut buf = [0u8; 9];
            let n = encode_unsigned_vint(v, &mut buf);
            assert_eq!(n, 1);
            let (decoded, consumed) = decode_unsigned_vint(&buf);
            assert_eq!(consumed, 1);
            assert_eq!(decoded, v);
        }
    }

    #[test]
    fn test_encode_decode_unsigned_multi_byte() {
        let cases: &[u64] = &[128, 255, 256, 16383, 16384, 1 << 20, 1 << 48, u64::MAX];
        for &v in cases {
            let mut buf = [0u8; 9];
            let n = encode_unsigned_vint(v, &mut buf);
            assert!(n > 1);
            let (decoded, consumed) = decode_unsigned_vint(&buf);
            assert_eq!(consumed, n, "value={v}");
            assert_eq!(decoded, v, "value={v}");
        }
    }

    #[test]
    fn test_signed_vint_roundtrip() {
        let cases: &[i64] = &[0, 1, -1, 127, -128, 1000, -1000, i64::MAX, i64::MIN];
        for &v in cases {
            let mut buf = [0u8; 9];
            let n = encode_vint(v, &mut buf);
            let (decoded, consumed) = decode_vint(&buf);
            assert_eq!(consumed, n, "value={v}");
            assert_eq!(decoded, v, "value={v}");
        }
    }

    #[test]
    fn test_stream_unsigned_vint_roundtrip() {
        let cases: &[u64] = &[0, 1, 127, 128, 16383, 16384, u64::MAX];
        for &v in cases {
            let mut data = Vec::new();
            let written = write_unsigned_vint(&mut data, v).unwrap();
            assert_eq!(written, unsigned_vint_size(v));
            let mut cursor = Cursor::new(&data);
            let decoded = read_unsigned_vint(&mut cursor).unwrap();
            assert_eq!(decoded, v, "value={v}");
        }
    }

    #[test]
    fn test_stream_signed_vint_roundtrip() {
        let cases: &[i64] = &[0, 1, -1, 127, -128, i64::MAX, i64::MIN];
        for &v in cases {
            let mut data = Vec::new();
            write_vint(&mut data, v).unwrap();
            let mut cursor = Cursor::new(&data);
            let decoded = read_vint(&mut cursor).unwrap();
            assert_eq!(decoded, v);
        }
    }

    /// Known test vectors compatible with Java VIntCoding.
    #[test]
    fn test_java_compatible_vectors() {
        // 0 -> single byte 0x00
        let mut buf = [0u8; 9];
        let n = encode_unsigned_vint(0, &mut buf);
        assert_eq!(&buf[..n], &[0x00]);

        // 127 -> single byte 0x7F
        let n = encode_unsigned_vint(127, &mut buf);
        assert_eq!(&buf[..n], &[0x7F]);

        // 128 -> two bytes: 0x80 prefix + 0x80
        let n = encode_unsigned_vint(128, &mut buf);
        assert_eq!(n, 2);
        assert_eq!(buf[0] & 0x80, 0x80); // leading 1-bit
    }

    #[test]
    fn test_multiple_values_in_stream() {
        let values: &[u64] = &[0, 42, 128, 65535, u64::MAX];
        let mut data = Vec::new();
        for &v in values {
            write_unsigned_vint(&mut data, v).unwrap();
        }
        let mut cursor = Cursor::new(&data);
        for &expected in values {
            let decoded = read_unsigned_vint(&mut cursor).unwrap();
            assert_eq!(decoded, expected);
        }
    }
}
