// Licensed under Apache License, Version 2.0.

//! BigDecimal-style helpers for CQL decimal arithmetic.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.marshal.DecimalType`

use cassandra_types::bigint;
use num_bigint::BigInt;
use num_traits::{Signed, ToPrimitive, Zero};

const MIN_SCALE: i32 = 32;
const MIN_SIGNIFICANT_DIGITS: i32 = MIN_SCALE;
const MAX_SCALE: i32 = 1000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DecimalValue {
    unscaled: BigInt,
    scale: i32,
}

impl DecimalValue {
    pub fn from_i64(value: i64) -> Self {
        Self {
            unscaled: BigInt::from(value),
            scale: 0,
        }
    }

    pub fn from_bigint(value: BigInt) -> Self {
        Self {
            unscaled: value,
            scale: 0,
        }
    }

    pub fn from_f64(value: f64) -> Result<Self, String> {
        if value.is_nan() {
            return Err("A NaN cannot be converted into a decimal".to_string());
        }
        if value.is_infinite() {
            return Err("An infinite number cannot be converted into a decimal".to_string());
        }
        Self::from_string(&value.to_string())
    }

    pub fn from_string(value: &str) -> Result<Self, String> {
        let bytes = bigint::string_to_decimal(value).map_err(|err| err.to_string())?;
        Self::from_bytes(&bytes)
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Self, String> {
        if bytes.len() < 4 {
            return Err(format!(
                "invalid decimal bytes: expected at least 4, got {}",
                bytes.len()
            ));
        }
        Ok(Self {
            scale: i32::from_be_bytes(bytes[0..4].try_into().expect("slice has 4 bytes")),
            unscaled: BigInt::from_signed_bytes_be(&bytes[4..]),
        })
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = self.scale.to_be_bytes().to_vec();
        bytes.extend_from_slice(&self.unscaled.to_signed_bytes_be());
        bytes
    }

    pub fn add(&self, other: &Self) -> Self {
        let scale = self.scale.max(other.scale);
        Self {
            unscaled: self.unscaled_at_scale(scale) + other.unscaled_at_scale(scale),
            scale,
        }
    }

    pub fn subtract(&self, other: &Self) -> Self {
        let scale = self.scale.max(other.scale);
        Self {
            unscaled: self.unscaled_at_scale(scale) - other.unscaled_at_scale(scale),
            scale,
        }
    }

    pub fn multiply(&self, other: &Self) -> Self {
        Self {
            unscaled: &self.unscaled * &other.unscaled,
            scale: self.scale.saturating_add(other.scale),
        }
    }

    pub fn divide(&self, other: &Self) -> Result<Self, String> {
        if other.unscaled.is_zero() {
            return Err("division by zero".to_string());
        }

        let quotient_first_digit_pos =
            (self.precision() - self.scale) - (other.precision() - other.scale);
        let scale = (MIN_SIGNIFICANT_DIGITS - quotient_first_digit_pos)
            .max(self.scale)
            .max(other.scale)
            .max(MIN_SCALE)
            .min(MAX_SCALE);

        Ok(Self {
            unscaled: self.divide_unscaled(other, scale),
            scale,
        }
        .strip_trailing_zeros())
    }

    pub fn divide_i64_half_even_same_scale(&self, divisor: i64) -> Result<Self, String> {
        if divisor == 0 {
            return Err("division by zero".to_string());
        }
        Ok(Self {
            unscaled: div_round_half_even(self.unscaled.clone(), BigInt::from(divisor)),
            scale: self.scale,
        })
    }

    pub fn remainder(&self, other: &Self) -> Result<Self, String> {
        if other.unscaled.is_zero() {
            return Err("modulo by zero".to_string());
        }
        let scale = self.scale.max(other.scale);
        let left = self.unscaled_at_scale(scale);
        let right = other.unscaled_at_scale(scale);
        let quotient = &left / &right;
        Ok(Self {
            unscaled: left - quotient * right,
            scale,
        })
    }

    pub fn negate(&self) -> Self {
        Self {
            unscaled: -&self.unscaled,
            scale: self.scale,
        }
    }

    pub fn abs(&self) -> Self {
        Self {
            unscaled: self.unscaled.abs(),
            scale: self.scale,
        }
    }

    pub fn round_half_up_zero_scale(&self) -> Self {
        if self.scale == 0 {
            return self.clone();
        }
        if self.scale < 0 {
            return Self {
                unscaled: &self.unscaled * pow10_u64((-self.scale) as u64),
                scale: 0,
            };
        }

        Self {
            unscaled: div_round_half_up(self.unscaled.clone(), pow10_i32(self.scale)),
            scale: 0,
        }
    }

    pub fn exp(&self) -> Result<Self, String> {
        let value = self.to_f64()?;
        decimal_from_f64_math_result(value.exp(), "decimal exp result")
    }

    pub fn log(&self) -> Result<Self, String> {
        if self.unscaled.is_zero() || self.unscaled.is_negative() {
            return Err("Natural log of number zero or less".to_string());
        }
        let value = self.to_f64()?;
        decimal_from_f64_math_result(value.ln(), "decimal log result")
    }

    pub fn log10(&self) -> Result<Self, String> {
        if self.unscaled.is_zero() || self.unscaled.is_negative() {
            return Err("Log10 of number zero or less".to_string());
        }
        let value = self.to_f64()?;
        decimal_from_f64_math_result(value.log10(), "decimal log10 result")
    }

    pub fn strip_trailing_zeros(mut self) -> Self {
        if self.unscaled.is_zero() {
            self.scale = 0;
            return self;
        }
        let ten = BigInt::from(10);
        while (&self.unscaled % &ten).is_zero() {
            self.unscaled /= &ten;
            self.scale = self.scale.saturating_sub(1);
        }
        self
    }

    fn unscaled_at_scale(&self, target_scale: i32) -> BigInt {
        debug_assert!(target_scale >= self.scale);
        &self.unscaled * pow10_i32(target_scale - self.scale)
    }

    fn divide_unscaled(&self, other: &Self, target_scale: i32) -> BigInt {
        let exponent = other.scale as i64 + target_scale as i64 - self.scale as i64;
        let mut numerator = self.unscaled.clone();
        let mut denominator = other.unscaled.clone();
        if exponent >= 0 {
            numerator *= pow10_u64(exponent as u64);
        } else {
            denominator *= pow10_u64((-exponent) as u64);
        }
        div_round_half_up(numerator, denominator)
    }

    fn precision(&self) -> i32 {
        let digits = self.unscaled.abs().to_str_radix(10);
        let digits = digits.trim_start_matches('0');
        if digits.is_empty() {
            1
        } else {
            digits.len() as i32
        }
    }

    fn to_f64(&self) -> Result<f64, String> {
        let value = self
            .unscaled
            .to_f64()
            .ok_or_else(|| "decimal value is outside supported math range".to_string())?;
        Ok(value / 10f64.powi(self.scale))
    }
}

fn decimal_from_f64_math_result(value: f64, label: &str) -> Result<DecimalValue, String> {
    if !value.is_finite() {
        return Err(format!("{label} is not finite"));
    }
    DecimalValue::from_string(&value.to_string())
}

fn div_round_half_up(numerator: BigInt, denominator: BigInt) -> BigInt {
    let quotient = &numerator / &denominator;
    let remainder = &numerator % &denominator;
    if remainder.is_zero() {
        return quotient;
    }

    let twice_remainder = remainder.abs() * 2;
    if twice_remainder >= denominator.abs() {
        let same_sign = numerator.sign() == denominator.sign();
        quotient
            + if same_sign {
                BigInt::from(1)
            } else {
                BigInt::from(-1)
            }
    } else {
        quotient
    }
}

fn div_round_half_even(numerator: BigInt, denominator: BigInt) -> BigInt {
    let quotient = &numerator / &denominator;
    let remainder = &numerator % &denominator;
    if remainder.is_zero() {
        return quotient;
    }

    let twice_remainder = remainder.abs() * 2;
    let denominator_abs = denominator.abs();
    let should_increment = if twice_remainder > denominator_abs {
        true
    } else if twice_remainder == denominator_abs {
        !(&quotient % BigInt::from(2)).is_zero()
    } else {
        false
    };

    if should_increment {
        let same_sign = numerator.sign() == denominator.sign();
        quotient
            + if same_sign {
                BigInt::from(1)
            } else {
                BigInt::from(-1)
            }
    } else {
        quotient
    }
}

fn pow10_i32(exp: i32) -> BigInt {
    pow10_u64(exp as u64)
}

fn pow10_u64(exp: u64) -> BigInt {
    BigInt::from(10).pow(exp.try_into().unwrap_or(u32::MAX))
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_types::bigint::decimal_to_string;

    #[test]
    fn decimal_add_subtract_and_multiply_preserve_scale() {
        let left = DecimalValue::from_string("10.50").unwrap();
        let right = DecimalValue::from_string("2.25").unwrap();

        assert_eq!(
            decimal_to_string(&left.add(&right).to_bytes()).unwrap(),
            "12.75"
        );
        assert_eq!(
            decimal_to_string(&left.subtract(&right).to_bytes()).unwrap(),
            "8.25"
        );
        assert_eq!(
            decimal_to_string(&left.multiply(&right).to_bytes()).unwrap(),
            "23.6250"
        );
    }

    #[test]
    fn decimal_divide_uses_java_scale_prediction_and_half_up_rounding() {
        let left = DecimalValue::from_string("1").unwrap();
        let right = DecimalValue::from_string("8").unwrap();

        assert_eq!(
            decimal_to_string(&left.divide(&right).unwrap().to_bytes()).unwrap(),
            "0.125"
        );
    }

    #[test]
    fn decimal_remainder_matches_integral_quotient_remainder() {
        let left = DecimalValue::from_string("5.5").unwrap();
        let right = DecimalValue::from_string("2").unwrap();

        assert_eq!(
            decimal_to_string(&left.remainder(&right).unwrap().to_bytes()).unwrap(),
            "1.5"
        );
    }

    #[test]
    fn decimal_half_even_division_keeps_input_scale() {
        let one = DecimalValue::from_string("1.0").unwrap();
        let half = one.divide_i64_half_even_same_scale(2).unwrap();
        assert_eq!(decimal_to_string(&half.to_bytes()).unwrap(), "0.5");

        let one_integer = DecimalValue::from_string("1").unwrap();
        let rounded = one_integer.divide_i64_half_even_same_scale(2).unwrap();
        assert_eq!(decimal_to_string(&rounded.to_bytes()).unwrap(), "0");
    }

    #[test]
    fn decimal_abs_preserves_scale() {
        let value = DecimalValue::from_string("-10.50").unwrap();
        assert_eq!(decimal_to_string(&value.abs().to_bytes()).unwrap(), "10.50");
    }

    #[test]
    fn decimal_round_half_up_zero_scale_matches_big_decimal() {
        for (input, expected) in [
            ("2.4", "2"),
            ("2.5", "3"),
            ("-2.4", "-2"),
            ("-2.5", "-3"),
            ("123", "123"),
        ] {
            let value = DecimalValue::from_string(input).unwrap();
            assert_eq!(
                decimal_to_string(&value.round_half_up_zero_scale().to_bytes()).unwrap(),
                expected
            );
        }
    }

    #[test]
    fn decimal_exp_log_log10_cover_exact_identity_values() {
        let zero = DecimalValue::from_string("0").unwrap();
        assert_eq!(
            decimal_to_string(&zero.exp().unwrap().to_bytes()).unwrap(),
            "1"
        );

        let one = DecimalValue::from_string("1").unwrap();
        assert_eq!(
            decimal_to_string(&one.log().unwrap().to_bytes()).unwrap(),
            "0"
        );

        let thousand = DecimalValue::from_string("1000").unwrap();
        assert_eq!(
            decimal_to_string(&thousand.log10().unwrap().to_bytes()).unwrap(),
            "3"
        );
    }

    #[test]
    fn decimal_logs_reject_zero_or_negative_like_java() {
        for input in ["0", "-1"] {
            let value = DecimalValue::from_string(input).unwrap();
            assert_eq!(
                value.log().unwrap_err(),
                "Natural log of number zero or less"
            );
            assert_eq!(value.log10().unwrap_err(), "Log10 of number zero or less");
        }
    }
}
