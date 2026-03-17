// Licensed under Apache License, Version 2.0.

//! Encode and decode integers in the minimal number of bytes based on their
//! magnitude. Used by Cassandra serialization to save space when the value
//! range is known to be small.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.utils.ByteBufferUtil`

use std::io::{self, Read, Write};

/// Returns the minimal byte size needed to represent `value`: 1, 2, 3, 4, 6,
/// or 8.
pub fn sized_int_size(value: i64) -> usize {
    // Use the absolute magnitude (handle i64::MIN carefully).
    let mag = if value == i64::MIN {
        return 8;
    } else {
        value.unsigned_abs()
    };

    if mag <= 0x7F {
        1
    } else if mag <= 0x7FFF {
        2
    } else if mag <= 0x7F_FFFF {
        3
    } else if mag <= 0x7FFF_FFFF {
        4
    } else if mag <= 0x7FFF_FFFF_FFFF {
        6
    } else {
        8
    }
}

/// Writes `value` as a big-endian integer using exactly `size` bytes.
///
/// Valid sizes are 1, 2, 3, 4, 6, and 8. The value is sign-extended / trimmed
/// to `size` bytes.
pub fn write_sized_int(writer: &mut impl Write, value: i64, size: usize) -> io::Result<()> {
    let be = value.to_be_bytes(); // 8 bytes, big-endian
    let start = 8 - size;
    writer.write_all(&be[start..])
}

/// Reads a big-endian signed integer of `size` bytes and sign-extends it to
/// `i64`.
///
/// Valid sizes are 1, 2, 3, 4, 6, and 8.
pub fn read_sized_int(reader: &mut impl Read, size: usize) -> io::Result<i64> {
    let mut raw = [0u8; 8];
    reader.read_exact(&mut raw[8 - size..])?;

    // Sign-extend: if the high bit of the read portion is set, fill the
    // leading bytes with 0xFF.
    if raw[8 - size] & 0x80 != 0 {
        for byte in raw.iter_mut().take(8 - size) {
            *byte = 0xFF;
        }
    }

    Ok(i64::from_be_bytes(raw))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn test_sized_int_size_boundaries() {
        // 1-byte range: -128..=127
        assert_eq!(sized_int_size(0), 1);
        assert_eq!(sized_int_size(127), 1);
        assert_eq!(sized_int_size(-128), 2); // -128 mag > 0x7F
        assert_eq!(sized_int_size(-1), 1);

        // 2-byte range
        assert_eq!(sized_int_size(128), 2);
        assert_eq!(sized_int_size(0x7FFF), 2);
        assert_eq!(sized_int_size(-0x7FFF), 2);

        // 3-byte range
        assert_eq!(sized_int_size(0x8000), 3);
        assert_eq!(sized_int_size(0x7F_FFFF), 3);

        // 4-byte range
        assert_eq!(sized_int_size(0x80_0000), 4);
        assert_eq!(sized_int_size(0x7FFF_FFFF), 4);

        // 6-byte range
        assert_eq!(sized_int_size(0x1_0000_0000i64), 6);
        assert_eq!(sized_int_size(0x7FFF_FFFF_FFFFi64), 6);

        // 8-byte range
        assert_eq!(sized_int_size(0x8000_0000_0000i64), 8);
        assert_eq!(sized_int_size(i64::MAX), 8);
        assert_eq!(sized_int_size(i64::MIN), 8);
    }

    #[test]
    fn test_roundtrip_all_sizes() {
        let cases: &[(i64, usize)] = &[
            (0, 1),
            (42, 1),
            (-1, 1),
            (127, 1),
            (128, 2),
            (0x7FFF, 2),
            (-0x7FFF, 2),
            (0x8000, 3),
            (0x7F_FFFF, 3),
            (0x80_0000, 4),
            (0x7FFF_FFFF, 4),
            (0x1_0000_0000i64, 6),
            (i64::MAX, 8),
            (i64::MIN, 8),
        ];

        for &(value, expected_size) in cases {
            let size = sized_int_size(value);
            assert_eq!(size, expected_size, "size mismatch for value={value}");

            let mut data = Vec::new();
            write_sized_int(&mut data, value, size).unwrap();
            assert_eq!(data.len(), size, "written bytes mismatch for value={value}");

            let mut cur = Cursor::new(data);
            let decoded = read_sized_int(&mut cur, size).unwrap();
            assert_eq!(decoded, value, "roundtrip failed for value={value}");
        }
    }

    #[test]
    fn test_write_read_negative() {
        let value = -42i64;
        let size = sized_int_size(value);
        let mut data = Vec::new();
        write_sized_int(&mut data, value, size).unwrap();
        let mut cur = Cursor::new(data);
        assert_eq!(read_sized_int(&mut cur, size).unwrap(), value);
    }

    #[test]
    fn test_explicit_size_larger_than_needed() {
        // Writing a small value in more bytes than needed should still
        // round-trip correctly.
        let value = 42i64;
        for &size in &[2, 3, 4, 6, 8] {
            let mut data = Vec::new();
            write_sized_int(&mut data, value, size).unwrap();
            let mut cur = Cursor::new(data);
            assert_eq!(
                read_sized_int(&mut cur, size).unwrap(),
                value,
                "size={size}"
            );
        }
    }

    #[test]
    fn test_negative_in_larger_size() {
        let value = -1i64;
        for &size in &[1, 2, 3, 4, 6, 8] {
            let mut data = Vec::new();
            write_sized_int(&mut data, value, size).unwrap();
            let mut cur = Cursor::new(data);
            assert_eq!(
                read_sized_int(&mut cur, size).unwrap(),
                value,
                "size={size}"
            );
        }
    }
}
