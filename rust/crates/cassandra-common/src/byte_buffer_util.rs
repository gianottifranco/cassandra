// Licensed under Apache License, Version 2.0.

//! Byte buffer helpers inspired by Cassandra's `ByteBufferUtil`.
//!
//! The Rust codebase mostly uses slices, `Vec<u8>`, and `bytes::Bytes`
//! instead of Java `ByteBuffer`, but the same utility surface is useful for
//! unsigned byte ordering, hex conversion, UTF-8 conversion, and length-prefixed
//! serialization.

use std::cmp::Ordering;
use std::io::{self, Read, Write};
use std::str::Utf8Error;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use bytes::Bytes;
use thiserror::Error;

/// Errors returned by byte-buffer utility conversions.
#[derive(Debug, Error)]
pub enum ByteBufferError {
    /// Hex strings must contain two characters per byte.
    #[error("hex input has odd length")]
    OddHexLength,
    /// A non-hex character was found at the given byte index.
    #[error("invalid hex character at byte index {index}: {byte:#x}")]
    InvalidHexCharacter { index: usize, byte: u8 },
    /// UTF-8 decoding failed.
    #[error("invalid UTF-8: {0}")]
    InvalidUtf8(#[from] Utf8Error),
}

/// Clone bytes into an immutable buffer.
pub fn bytes(data: impl AsRef<[u8]>) -> Bytes {
    Bytes::copy_from_slice(data.as_ref())
}

/// Clone a byte slice into an owned vector.
pub fn clone_bytes(data: &[u8]) -> Vec<u8> {
    data.to_vec()
}

/// Decode UTF-8 bytes into a string slice.
pub fn string(data: &[u8]) -> Result<&str, ByteBufferError> {
    Ok(std::str::from_utf8(data)?)
}

/// Compare byte slices using Java's unsigned byte ordering.
pub fn compare_unsigned(left: &[u8], right: &[u8]) -> Ordering {
    left.cmp(right)
}

/// Return true when `data` starts with `prefix`.
pub fn starts_with(data: &[u8], prefix: &[u8]) -> bool {
    data.starts_with(prefix)
}

/// Return true when `data` ends with `suffix`.
pub fn ends_with(data: &[u8], suffix: &[u8]) -> bool {
    data.ends_with(suffix)
}

/// Find the last occurrence of `needle` in `data`.
pub fn last_index_of(data: &[u8], needle: u8) -> Option<usize> {
    data.iter().rposition(|byte| *byte == needle)
}

/// Convert bytes to lowercase hexadecimal.
pub fn to_hex(data: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(data.len() * 2);
    for byte in data {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }
    out
}

/// Decode a hexadecimal string.
pub fn from_hex(input: &str) -> Result<Vec<u8>, ByteBufferError> {
    let bytes = input.as_bytes();
    if bytes.len() % 2 != 0 {
        return Err(ByteBufferError::OddHexLength);
    }

    let mut out = Vec::with_capacity(bytes.len() / 2);
    for (pair_index, pair) in bytes.chunks_exact(2).enumerate() {
        let high = hex_value(pair[0]).ok_or(ByteBufferError::InvalidHexCharacter {
            index: pair_index * 2,
            byte: pair[0],
        })?;
        let low = hex_value(pair[1]).ok_or(ByteBufferError::InvalidHexCharacter {
            index: pair_index * 2 + 1,
            byte: pair[1],
        })?;
        out.push((high << 4) | low);
    }
    Ok(out)
}

/// Write bytes with a signed 32-bit big-endian length prefix.
pub fn write_bytes_with_i32_length<W: Write>(writer: &mut W, data: &[u8]) -> io::Result<()> {
    let len = i32::try_from(data.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "byte buffer too large"))?;
    writer.write_i32::<BigEndian>(len)?;
    writer.write_all(data)
}

/// Read bytes with a signed 32-bit big-endian length prefix.
pub fn read_bytes_with_i32_length<R: Read>(reader: &mut R) -> io::Result<Vec<u8>> {
    let len = reader.read_i32::<BigEndian>()?;
    if len < 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "negative byte buffer length",
        ));
    }
    read_exact_len(reader, len as usize)
}

/// Write optional bytes with Cassandra's protocol-style i32 length prefix.
///
/// `None` is encoded as `-1`.
pub fn write_bytes_opt_with_i32_length<W: Write>(
    writer: &mut W,
    data: Option<&[u8]>,
) -> io::Result<()> {
    match data {
        Some(bytes) => write_bytes_with_i32_length(writer, bytes),
        None => writer.write_i32::<BigEndian>(-1),
    }
}

/// Read optional bytes with Cassandra's protocol-style i32 length prefix.
///
/// `-1` is decoded as `None`; other negative lengths are rejected.
pub fn read_bytes_opt_with_i32_length<R: Read>(reader: &mut R) -> io::Result<Option<Vec<u8>>> {
    let len = reader.read_i32::<BigEndian>()?;
    if len == -1 {
        return Ok(None);
    }
    if len < -1 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "invalid negative byte buffer length",
        ));
    }
    read_exact_len(reader, len as usize).map(Some)
}

/// Write bytes with an unsigned 16-bit big-endian length prefix.
pub fn write_bytes_with_u16_length<W: Write>(writer: &mut W, data: &[u8]) -> io::Result<()> {
    let len = u16::try_from(data.len())
        .map_err(|_| io::Error::new(io::ErrorKind::InvalidInput, "byte buffer too large"))?;
    writer.write_u16::<BigEndian>(len)?;
    writer.write_all(data)
}

/// Read bytes with an unsigned 16-bit big-endian length prefix.
pub fn read_bytes_with_u16_length<R: Read>(reader: &mut R) -> io::Result<Vec<u8>> {
    let len = reader.read_u16::<BigEndian>()? as usize;
    read_exact_len(reader, len)
}

fn read_exact_len<R: Read>(reader: &mut R, len: usize) -> io::Result<Vec<u8>> {
    let mut out = vec![0u8; len];
    reader.read_exact(&mut out)?;
    Ok(out)
}

fn hex_value(byte: u8) -> Option<u8> {
    match byte {
        b'0'..=b'9' => Some(byte - b'0'),
        b'a'..=b'f' => Some(byte - b'a' + 10),
        b'A'..=b'F' => Some(byte - b'A' + 10),
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compare_unsigned_matches_java_byte_ordering() {
        assert_eq!(compare_unsigned(&[0x7f], &[0x80]), Ordering::Less);
        assert_eq!(compare_unsigned(&[0xff], &[0x00]), Ordering::Greater);
        assert_eq!(compare_unsigned(&[1, 2], &[1, 2, 0]), Ordering::Less);
    }

    #[test]
    fn hex_roundtrip_accepts_upper_and_lowercase() {
        let bytes = vec![0x00, 0x0f, 0x10, 0xab, 0xff];
        assert_eq!(to_hex(&bytes), "000f10abff");
        assert_eq!(from_hex("000F10ABff").unwrap(), bytes);
    }

    #[test]
    fn rejects_invalid_hex() {
        assert!(matches!(
            from_hex("abc"),
            Err(ByteBufferError::OddHexLength)
        ));
        assert!(matches!(
            from_hex("zz"),
            Err(ByteBufferError::InvalidHexCharacter { index: 0, .. })
        ));
    }

    #[test]
    fn utf8_string_conversion() {
        assert_eq!(string(b"cassandra").unwrap(), "cassandra");
        assert!(matches!(
            string(&[0xff]),
            Err(ByteBufferError::InvalidUtf8(_))
        ));
    }

    #[test]
    fn i32_length_prefixed_bytes_roundtrip() {
        let mut buf = Vec::new();
        write_bytes_with_i32_length(&mut buf, b"abc").unwrap();
        let mut input = buf.as_slice();
        assert_eq!(read_bytes_with_i32_length(&mut input).unwrap(), b"abc");
        assert!(input.is_empty());
    }

    #[test]
    fn optional_i32_length_prefixed_bytes_roundtrip() {
        let mut buf = Vec::new();
        write_bytes_opt_with_i32_length(&mut buf, None).unwrap();
        write_bytes_opt_with_i32_length(&mut buf, Some(b"value")).unwrap();

        let mut input = buf.as_slice();
        assert_eq!(read_bytes_opt_with_i32_length(&mut input).unwrap(), None);
        assert_eq!(
            read_bytes_opt_with_i32_length(&mut input).unwrap(),
            Some(b"value".to_vec())
        );
    }

    #[test]
    fn u16_length_prefixed_bytes_roundtrip() {
        let mut buf = Vec::new();
        write_bytes_with_u16_length(&mut buf, b"short").unwrap();
        let mut input = buf.as_slice();
        assert_eq!(read_bytes_with_u16_length(&mut input).unwrap(), b"short");
    }

    #[test]
    fn slice_helpers_match_bytebufferutil_use_cases() {
        let data = b"abcabc";
        assert!(starts_with(data, b"ab"));
        assert!(ends_with(data, b"bc"));
        assert_eq!(last_index_of(data, b'a'), Some(3));
        assert_eq!(clone_bytes(data), data);
        assert_eq!(bytes(data), Bytes::copy_from_slice(data));
    }
}
