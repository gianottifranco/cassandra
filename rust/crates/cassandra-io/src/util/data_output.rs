// Licensed under Apache License, Version 2.0.

//! Write-side data primitives matching Java's `DataOutput` / `DataOutputPlus`.
//!
//! Provides big-endian primitive writes, VInt encoding, and length-prefixed
//! UTF-8 string writes. A blanket implementation is provided for every type
//! that implements [`std::io::Write`].
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.util.DataOutputPlus`

use std::io::{self, Write};

use super::varint;

/// Extended data-output trait for Cassandra on-disk / on-wire formats.
pub trait DataOutputPlus: Write {
    /// Writes a single byte.
    fn write_byte(&mut self, v: u8) -> io::Result<()> {
        self.write_all(&[v])
    }

    /// Writes a big-endian signed 16-bit integer.
    fn write_short(&mut self, v: i16) -> io::Result<()> {
        self.write_all(&v.to_be_bytes())
    }

    /// Writes a big-endian signed 32-bit integer.
    fn write_int(&mut self, v: i32) -> io::Result<()> {
        self.write_all(&v.to_be_bytes())
    }

    /// Writes a big-endian signed 64-bit integer.
    fn write_long(&mut self, v: i64) -> io::Result<()> {
        self.write_all(&v.to_be_bytes())
    }

    /// Writes a signed VInt (zig-zag encoded).
    fn write_vint(&mut self, v: i64) -> io::Result<usize> {
        varint::write_vint(self, v)
    }

    /// Writes an unsigned VInt.
    fn write_unsigned_vint(&mut self, v: u64) -> io::Result<usize> {
        varint::write_unsigned_vint(self, v)
    }

    /// Writes a big-endian IEEE 754 single-precision float.
    fn write_float(&mut self, v: f32) -> io::Result<()> {
        self.write_all(&v.to_be_bytes())
    }

    /// Writes a big-endian IEEE 754 double-precision float.
    fn write_double(&mut self, v: f64) -> io::Result<()> {
        self.write_all(&v.to_be_bytes())
    }

    /// Writes a length-prefixed UTF-8 string. The length prefix is a
    /// big-endian unsigned 16-bit integer.
    ///
    /// # Errors
    ///
    /// Returns an error if the string byte length exceeds `u16::MAX`.
    fn write_utf(&mut self, s: &str) -> io::Result<()> {
        let bytes = s.as_bytes();
        if bytes.len() > u16::MAX as usize {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "string too long for u16 length prefix",
            ));
        }
        self.write_all(&(bytes.len() as u16).to_be_bytes())?;
        self.write_all(bytes)
    }
}

/// Blanket implementation: every `Write` is automatically a `DataOutputPlus`.
impl<W: Write> DataOutputPlus for W {}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::util::data_input::DataInputPlus;
    use std::io::Cursor;

    #[test]
    fn test_write_byte() {
        let mut buf = Vec::new();
        buf.write_byte(0xAB).unwrap();
        assert_eq!(buf, vec![0xAB]);
    }

    #[test]
    fn test_write_short() {
        let mut buf = Vec::new();
        buf.write_short(0x1234).unwrap();
        assert_eq!(buf, 0x1234i16.to_be_bytes().to_vec());
    }

    #[test]
    fn test_write_int() {
        let mut buf = Vec::new();
        buf.write_int(-99).unwrap();
        assert_eq!(buf, (-99i32).to_be_bytes().to_vec());
    }

    #[test]
    fn test_write_long() {
        let mut buf = Vec::new();
        buf.write_long(i64::MAX).unwrap();
        assert_eq!(buf, i64::MAX.to_be_bytes().to_vec());
    }

    #[test]
    fn test_write_float() {
        let mut buf = Vec::new();
        buf.write_float(1.5).unwrap();
        let mut cur = Cursor::new(buf);
        assert!((cur.read_float().unwrap() - 1.5).abs() < f32::EPSILON);
    }

    #[test]
    fn test_write_double() {
        let mut buf = Vec::new();
        buf.write_double(2.5).unwrap();
        let mut cur = Cursor::new(buf);
        assert!((cur.read_double().unwrap() - 2.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_write_utf_empty() {
        let mut buf = Vec::new();
        buf.write_utf("").unwrap();
        let mut cur = Cursor::new(buf);
        assert_eq!(cur.read_utf().unwrap(), "");
    }

    #[test]
    fn test_write_utf_ascii() {
        let mut buf = Vec::new();
        buf.write_utf("hello").unwrap();
        let mut cur = Cursor::new(buf);
        assert_eq!(cur.read_utf().unwrap(), "hello");
    }

    #[test]
    fn test_write_utf_multibyte() {
        let s = "\u{00e9}\u{1f600}";
        let mut buf = Vec::new();
        buf.write_utf(s).unwrap();
        let mut cur = Cursor::new(buf);
        assert_eq!(cur.read_utf().unwrap(), s);
    }

    #[test]
    fn test_write_vint_signed() {
        let mut buf = Vec::new();
        buf.write_vint(-42).unwrap();
        let mut cur = Cursor::new(buf);
        assert_eq!(cur.read_vint().unwrap(), -42);
    }

    #[test]
    fn test_write_unsigned_vint() {
        let mut buf = Vec::new();
        buf.write_unsigned_vint(300).unwrap();
        let mut cur = Cursor::new(buf);
        assert_eq!(cur.read_unsigned_vint().unwrap(), 300);
    }

    #[test]
    fn test_roundtrip_all_types() {
        let mut data = Vec::new();
        data.write_byte(0xFF).unwrap();
        data.write_short(1234).unwrap();
        data.write_int(-99).unwrap();
        data.write_long(i64::MAX).unwrap();
        data.write_float(1.5).unwrap();
        data.write_double(2.5).unwrap();
        data.write_utf("test").unwrap();
        data.write_vint(-1000).unwrap();
        data.write_unsigned_vint(9999).unwrap();

        let mut cur = Cursor::new(data);
        assert_eq!(cur.read_byte().unwrap(), 0xFF);
        assert_eq!(cur.read_short().unwrap(), 1234);
        assert_eq!(cur.read_int().unwrap(), -99);
        assert_eq!(cur.read_long().unwrap(), i64::MAX);
        assert!((cur.read_float().unwrap() - 1.5).abs() < f32::EPSILON);
        assert!((cur.read_double().unwrap() - 2.5).abs() < f64::EPSILON);
        assert_eq!(cur.read_utf().unwrap(), "test");
        assert_eq!(cur.read_vint().unwrap(), -1000);
        assert_eq!(cur.read_unsigned_vint().unwrap(), 9999);
    }
}
