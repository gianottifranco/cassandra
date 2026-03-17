// Licensed under Apache License, Version 2.0.

//! Read-side data primitives matching Java's `DataInput` / `DataInputPlus`.
//!
//! Provides big-endian primitive reads, VInt decoding, and length-prefixed
//! UTF-8 string reads. A blanket implementation is provided for every type
//! that implements [`std::io::Read`].
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.util.DataInputPlus`

use std::io::{self, Read};

use super::varint;

/// Extended data-input trait for Cassandra on-disk / on-wire formats.
pub trait DataInputPlus: Read {
    /// Reads a single byte (unsigned).
    fn read_byte(&mut self) -> io::Result<u8> {
        let mut buf = [0u8; 1];
        self.read_exact(&mut buf)?;
        Ok(buf[0])
    }

    /// Reads a big-endian signed 16-bit integer.
    fn read_short(&mut self) -> io::Result<i16> {
        let mut buf = [0u8; 2];
        self.read_exact(&mut buf)?;
        Ok(i16::from_be_bytes(buf))
    }

    /// Reads a big-endian signed 32-bit integer.
    fn read_int(&mut self) -> io::Result<i32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        Ok(i32::from_be_bytes(buf))
    }

    /// Reads a big-endian signed 64-bit integer.
    fn read_long(&mut self) -> io::Result<i64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(i64::from_be_bytes(buf))
    }

    /// Reads a signed VInt (zig-zag decoded).
    fn read_vint(&mut self) -> io::Result<i64> {
        varint::read_vint(self)
    }

    /// Reads an unsigned VInt.
    fn read_unsigned_vint(&mut self) -> io::Result<u64> {
        varint::read_unsigned_vint(self)
    }

    /// Reads a big-endian IEEE 754 single-precision float.
    fn read_float(&mut self) -> io::Result<f32> {
        let mut buf = [0u8; 4];
        self.read_exact(&mut buf)?;
        Ok(f32::from_be_bytes(buf))
    }

    /// Reads a big-endian IEEE 754 double-precision float.
    fn read_double(&mut self) -> io::Result<f64> {
        let mut buf = [0u8; 8];
        self.read_exact(&mut buf)?;
        Ok(f64::from_be_bytes(buf))
    }

    /// Reads a length-prefixed UTF-8 string. The length prefix is a big-endian
    /// unsigned 16-bit integer indicating the byte length of the string.
    fn read_utf(&mut self) -> io::Result<String> {
        let mut len_buf = [0u8; 2];
        self.read_exact(&mut len_buf)?;
        let len = u16::from_be_bytes(len_buf) as usize;
        let mut str_buf = vec![0u8; len];
        self.read_exact(&mut str_buf)?;
        String::from_utf8(str_buf).map_err(|e| {
            io::Error::new(io::ErrorKind::InvalidData, e)
        })
    }

    /// Reads exactly enough bytes to fill `buf`, or returns an error.
    fn read_fully(&mut self, buf: &mut [u8]) -> io::Result<()> {
        self.read_exact(buf)
    }
}

/// Blanket implementation: every `Read` is automatically a `DataInputPlus`.
impl<R: Read> DataInputPlus for R {}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_read_byte() {
        let mut cur = Cursor::new(vec![0xAB]);
        assert_eq!(cur.read_byte().unwrap(), 0xAB);
    }

    #[test]
    fn test_read_short() {
        let mut cur = Cursor::new(0x1234i16.to_be_bytes().to_vec());
        assert_eq!(cur.read_short().unwrap(), 0x1234);
    }

    #[test]
    fn test_read_int() {
        let mut cur = Cursor::new(42i32.to_be_bytes().to_vec());
        assert_eq!(cur.read_int().unwrap(), 42);
    }

    #[test]
    fn test_read_long() {
        let mut cur = Cursor::new(i64::MIN.to_be_bytes().to_vec());
        assert_eq!(cur.read_long().unwrap(), i64::MIN);
    }

    #[test]
    fn test_read_float() {
        let mut cur = Cursor::new(std::f32::consts::PI.to_be_bytes().to_vec());
        let val = cur.read_float().unwrap();
        assert!((val - std::f32::consts::PI).abs() < f32::EPSILON);
    }

    #[test]
    fn test_read_double() {
        let mut cur = Cursor::new(std::f64::consts::E.to_be_bytes().to_vec());
        let val = cur.read_double().unwrap();
        assert!((val - std::f64::consts::E).abs() < f64::EPSILON);
    }

    #[test]
    fn test_read_utf_empty() {
        let mut data = Vec::new();
        data.extend_from_slice(&0u16.to_be_bytes());
        let mut cur = Cursor::new(data);
        assert_eq!(cur.read_utf().unwrap(), "");
    }

    #[test]
    fn test_read_utf_ascii() {
        let s = "hello";
        let mut data = Vec::new();
        data.extend_from_slice(&(s.len() as u16).to_be_bytes());
        data.extend_from_slice(s.as_bytes());
        let mut cur = Cursor::new(data);
        assert_eq!(cur.read_utf().unwrap(), "hello");
    }

    #[test]
    fn test_read_utf_multibyte() {
        let s = "\u{00e9}\u{1f600}"; // e-acute + emoji
        let bytes = s.as_bytes();
        let mut data = Vec::new();
        data.extend_from_slice(&(bytes.len() as u16).to_be_bytes());
        data.extend_from_slice(bytes);
        let mut cur = Cursor::new(data);
        assert_eq!(cur.read_utf().unwrap(), s);
    }

    #[test]
    fn test_read_fully() {
        let mut cur = Cursor::new(vec![1, 2, 3, 4]);
        let mut buf = [0u8; 4];
        cur.read_fully(&mut buf).unwrap();
        assert_eq!(buf, [1, 2, 3, 4]);
    }

    #[test]
    fn test_read_vint_signed() {
        let mut data = Vec::new();
        varint::write_vint(&mut data, -42).unwrap();
        let mut cur = Cursor::new(data);
        assert_eq!(cur.read_vint().unwrap(), -42);
    }

    #[test]
    fn test_read_unsigned_vint() {
        let mut data = Vec::new();
        varint::write_unsigned_vint(&mut data, 300).unwrap();
        let mut cur = Cursor::new(data);
        assert_eq!(cur.read_unsigned_vint().unwrap(), 300);
    }

    #[test]
    fn test_roundtrip_all_types() {
        // Write a sequence of typed values, then read them back.
        let mut data = Vec::new();
        data.push(0xFFu8);
        data.extend_from_slice(&1234i16.to_be_bytes());
        data.extend_from_slice(&(-99i32).to_be_bytes());
        data.extend_from_slice(&i64::MAX.to_be_bytes());
        data.extend_from_slice(&1.5f32.to_be_bytes());
        data.extend_from_slice(&2.5f64.to_be_bytes());
        let s = "test";
        data.extend_from_slice(&(s.len() as u16).to_be_bytes());
        data.extend_from_slice(s.as_bytes());

        let mut cur = Cursor::new(data);
        assert_eq!(cur.read_byte().unwrap(), 0xFF);
        assert_eq!(cur.read_short().unwrap(), 1234);
        assert_eq!(cur.read_int().unwrap(), -99);
        assert_eq!(cur.read_long().unwrap(), i64::MAX);
        assert!((cur.read_float().unwrap() - 1.5).abs() < f32::EPSILON);
        assert!((cur.read_double().unwrap() - 2.5).abs() < f64::EPSILON);
        assert_eq!(cur.read_utf().unwrap(), "test");
    }
}
