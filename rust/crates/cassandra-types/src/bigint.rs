// Licensed under Apache License, Version 2.0.

//! Arbitrary-precision integer and decimal wire-format helpers.
//!
//! Cassandra's `IntegerType` (varint) and `DecimalType` use a big-endian
//! two's-complement representation on the wire.  This module provides
//! pure-Rust conversion between string representations and those byte arrays
//! without pulling in a heavy bignum crate.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.serializers.IntegerSerializer`
//! - `org.apache.cassandra.serializers.DecimalSerializer`

use crate::marshal::{MarshalError, MarshalResult};

/// Parse a decimal string (e.g. `"-123"`) into a minimal big-endian
/// two's-complement byte array suitable for `CqlValue::Varint`.
///
/// The result is the shortest byte sequence that preserves the sign bit.
pub fn string_to_varint(s: &str) -> MarshalResult<Vec<u8>> {
    let s = s.trim();
    if s.is_empty() {
        return Err(MarshalError::InvalidData {
            type_name: "varint".into(),
            reason: "empty string".into(),
        });
    }

    let (negative, digits) = if let Some(rest) = s.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = s.strip_prefix('+') {
        (false, rest)
    } else {
        (false, s)
    };

    if digits.is_empty() || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return Err(MarshalError::InvalidData {
            type_name: "varint".into(),
            reason: format!("cannot parse '{}' as integer", s),
        });
    }

    // Convert decimal digits → big-endian unsigned magnitude bytes via
    // schoolbook multiply-and-add.
    let mut mag = vec![0u8]; // big-endian magnitude
    for &d in digits.as_bytes() {
        let digit = d - b'0';
        // mag = mag * 10 + digit
        let mut carry: u16 = digit as u16;
        for byte in mag.iter_mut().rev() {
            let prod = (*byte as u16) * 10 + carry;
            *byte = prod as u8;
            carry = prod >> 8;
        }
        while carry > 0 {
            mag.insert(0, carry as u8);
            carry >>= 8;
        }
    }

    if !negative {
        // Positive: strip leading 0x00 bytes but keep one if the high bit is set
        while mag.len() > 1 && mag[0] == 0 {
            mag.remove(0);
        }
        // If high bit set, prepend a 0x00 sign byte
        if mag[0] & 0x80 != 0 {
            mag.insert(0, 0x00);
        }
        return Ok(mag);
    }

    // Negative: compute two's complement of magnitude.
    // If magnitude is all zeros → result is [0].
    if mag.iter().all(|&b| b == 0) {
        return Ok(vec![0]);
    }

    // Ensure enough room for the sign bit
    if mag[0] & 0x80 != 0 {
        mag.insert(0, 0x00);
    }

    // Bitwise NOT
    for byte in mag.iter_mut() {
        *byte = !*byte;
    }
    // Add 1
    let mut carry = true;
    for byte in mag.iter_mut().rev() {
        if carry {
            let (val, overflow) = byte.overflowing_add(1);
            *byte = val;
            carry = overflow;
        }
    }

    // Strip redundant leading 0xFF bytes (keep if next byte's high bit is 0)
    while mag.len() > 1 && mag[0] == 0xFF && (mag[1] & 0x80) != 0 {
        mag.remove(0);
    }

    Ok(mag)
}

/// Convert a varint (big-endian two's-complement) byte array to its decimal
/// string representation.
pub fn varint_to_string(data: &[u8]) -> String {
    if data.is_empty() {
        return "0".to_string();
    }

    let negative = data[0] & 0x80 != 0;
    let mut mag = data.to_vec();

    if negative {
        // Two's complement → magnitude: NOT then add 1
        for byte in mag.iter_mut() {
            *byte = !*byte;
        }
        let mut carry = true;
        for byte in mag.iter_mut().rev() {
            if carry {
                let (val, overflow) = byte.overflowing_add(1);
                *byte = val;
                carry = overflow;
            }
        }
    }

    // Convert big-endian bytes to decimal string via repeated divmod by 10.
    let mut decimal_digits = Vec::new();
    loop {
        let mut remainder: u16 = 0;
        let mut all_zero = true;
        for byte in mag.iter_mut() {
            let dividend = (remainder << 8) | (*byte as u16);
            *byte = (dividend / 10) as u8;
            remainder = dividend % 10;
            if *byte != 0 {
                all_zero = false;
            }
        }
        decimal_digits.push(b'0' + remainder as u8);
        if all_zero {
            break;
        }
    }

    decimal_digits.reverse();
    let s = String::from_utf8(decimal_digits).unwrap();
    if negative { format!("-{}", s) } else { s }
}

/// Parse a decimal string like `"123.456"` or `"1.23E+4"` into Cassandra
/// Decimal wire format: `[i32 scale][varint unscaled]`.
///
/// The scale is the number of digits to the right of the decimal point.
pub fn string_to_decimal(s: &str) -> MarshalResult<Vec<u8>> {
    let s = s.trim();

    // Split into coefficient and optional exponent
    let upper = s.to_uppercase();
    let (coeff, exp) = match upper.split_once('E') {
        Some((c, e)) => {
            let exp: i32 = e.parse().map_err(|_| MarshalError::InvalidData {
                type_name: "decimal".into(),
                reason: format!("invalid exponent in '{}'", s),
            })?;
            (c, exp)
        }
        None => (upper.as_str(), 0i32),
    };

    // Parse sign
    let (negative, coeff) = if let Some(rest) = coeff.strip_prefix('-') {
        (true, rest)
    } else if let Some(rest) = coeff.strip_prefix('+') {
        (false, rest)
    } else {
        (false, coeff)
    };

    // Split on decimal point
    let (int_part, frac_part) = match coeff.split_once('.') {
        Some((i, f)) => (i, f),
        None => (coeff, ""),
    };

    // Scale = number of fractional digits minus exponent
    let scale = frac_part.len() as i32 - exp;

    // Unscaled integer = int_part + frac_part (as a single integer string)
    let combined = format!(
        "{}{}{}",
        if negative { "-" } else { "" },
        int_part,
        frac_part
    );
    let unscaled = string_to_varint(&combined)?;

    let mut result = Vec::with_capacity(4 + unscaled.len());
    result.extend_from_slice(&scale.to_be_bytes());
    result.extend_from_slice(&unscaled);
    Ok(result)
}

/// Convert Cassandra Decimal wire format `[i32 scale][varint unscaled]` to
/// a decimal string.
pub fn decimal_to_string(data: &[u8]) -> Result<String, MarshalError> {
    if data.len() < 4 {
        return Err(MarshalError::InvalidData {
            type_name: "decimal".into(),
            reason: "need at least 4 bytes for scale".into(),
        });
    }
    let scale = i32::from_be_bytes([data[0], data[1], data[2], data[3]]);
    let unscaled_str = varint_to_string(&data[4..]);

    if scale <= 0 {
        // No decimal point needed; multiply by 10^(-scale)
        let zeros = (-scale) as usize;
        return Ok(format!("{}{}", unscaled_str, "0".repeat(zeros)));
    }

    let scale = scale as usize;
    let (negative, digits) = if let Some(rest) = unscaled_str.strip_prefix('-') {
        (true, rest.to_string())
    } else {
        (false, unscaled_str)
    };

    let sign = if negative { "-" } else { "" };

    if digits.len() <= scale {
        // Need leading zeros after decimal point
        let leading = scale - digits.len();
        Ok(format!("{}0.{}{}", sign, "0".repeat(leading), digits))
    } else {
        let split = digits.len() - scale;
        Ok(format!("{}{}.{}", sign, &digits[..split], &digits[split..]))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn varint_zero() {
        assert_eq!(string_to_varint("0").unwrap(), vec![0x00]);
    }

    #[test]
    fn varint_positive_small() {
        // 1 → 0x01
        assert_eq!(string_to_varint("1").unwrap(), vec![0x01]);
        // 127 → 0x7F
        assert_eq!(string_to_varint("127").unwrap(), vec![0x7F]);
        // 128 → 0x0080 (need sign byte)
        assert_eq!(string_to_varint("128").unwrap(), vec![0x00, 0x80]);
    }

    #[test]
    fn varint_negative() {
        // -1 → 0xFF
        assert_eq!(string_to_varint("-1").unwrap(), vec![0xFF]);
        // -128 → 0x80
        assert_eq!(string_to_varint("-128").unwrap(), vec![0x80]);
        // -129 → 0xFF7F
        assert_eq!(string_to_varint("-129").unwrap(), vec![0xFF, 0x7F]);
    }

    #[test]
    fn varint_large() {
        // 256 → 0x0100
        assert_eq!(string_to_varint("256").unwrap(), vec![0x01, 0x00]);
        // 65535 → 0x00FFFF
        assert_eq!(string_to_varint("65535").unwrap(), vec![0x00, 0xFF, 0xFF]);
    }

    #[test]
    fn varint_roundtrip() {
        for v in [-1000000, -1, 0, 1, 127, 128, 255, 256, 1000000] {
            let bytes = string_to_varint(&v.to_string()).unwrap();
            let s = varint_to_string(&bytes);
            assert_eq!(s, v.to_string(), "roundtrip failed for {}", v);
        }
    }

    #[test]
    fn varint_invalid() {
        assert!(string_to_varint("").is_err());
        assert!(string_to_varint("abc").is_err());
        assert!(string_to_varint("12.3").is_err());
    }

    #[test]
    fn decimal_simple() {
        // "123.45" → scale=2, unscaled=12345
        let bytes = string_to_decimal("123.45").unwrap();
        let scale = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        assert_eq!(scale, 2);
        let unscaled = varint_to_string(&bytes[4..]);
        assert_eq!(unscaled, "12345");
    }

    #[test]
    fn decimal_negative() {
        let bytes = string_to_decimal("-0.001").unwrap();
        let scale = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        assert_eq!(scale, 3);
        let unscaled = varint_to_string(&bytes[4..]);
        assert_eq!(unscaled, "-1");
    }

    #[test]
    fn decimal_roundtrip() {
        for s in &["0", "1", "-1", "123.456", "-0.001", "100"] {
            let bytes = string_to_decimal(s).unwrap();
            let back = decimal_to_string(&bytes).unwrap();
            // Parse both to verify numeric equivalence
            let orig: f64 = s.parse().unwrap();
            let round: f64 = back.parse().unwrap();
            assert!(
                (orig - round).abs() < 1e-10,
                "roundtrip for '{}': got '{}'",
                s,
                back
            );
        }
    }

    #[test]
    fn decimal_with_exponent() {
        // "1.5E+2" = 150, scale = -1 (1 frac digit - 2 exp)
        let bytes = string_to_decimal("1.5E+2").unwrap();
        let scale = i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        assert_eq!(scale, -1); // 1 fractional digit - exponent 2
    }
}
