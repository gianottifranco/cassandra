// Licensed under Apache License, Version 2.0.

//! Math, length, and JSON functions.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.functions.MathFcts`
//! - `org.apache.cassandra.cql3.functions.ToJsonFct`
//! - `org.apache.cassandra.cql3.functions.FromJsonFct`

use super::registry::{CqlFunction, FunctionRegistry};
use cassandra_types::CqlType;
use std::sync::Arc;

/// Register all math/length/JSON functions.
pub fn register_all(registry: &FunctionRegistry) {
    // Math functions for each numeric type
    registry.register(Arc::new(AbsInt));
    registry.register(Arc::new(AbsBigint));
    registry.register(Arc::new(AbsFloat));
    registry.register(Arc::new(AbsDouble));

    // Ceil/Floor/Round for float/double
    registry.register(Arc::new(CeilFloat));
    registry.register(Arc::new(CeilDouble));
    registry.register(Arc::new(FloorFloat));
    registry.register(Arc::new(FloorDouble));
    registry.register(Arc::new(RoundFloat));
    registry.register(Arc::new(RoundDouble));

    // Length
    registry.register(Arc::new(LengthText));
    registry.register(Arc::new(LengthBlob));

    // JSON
    registry.register(Arc::new(ToJsonFunction));
    registry.register(Arc::new(FromJsonFunction));
}

// ── Math: abs ─────────────────────────────────────────────────────────

macro_rules! define_abs {
    ($name:ident, $cql_type:expr, $rust_type:ty) => {
        struct $name;

        impl CqlFunction for $name {
            fn name(&self) -> &str {
                "abs"
            }
            fn arg_types(&self) -> Vec<CqlType> {
                vec![$cql_type]
            }
            fn return_type(&self) -> CqlType {
                $cql_type
            }
            fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
                match args.first().and_then(|a| *a) {
                    Some(bytes) => {
                        let arr: [u8; std::mem::size_of::<$rust_type>()] =
                            bytes.try_into().map_err(|_| "invalid bytes")?;
                        let v = <$rust_type>::from_be_bytes(arr);
                        let result = v.abs();
                        Ok(Some(result.to_be_bytes().to_vec()))
                    }
                    None => Ok(None),
                }
            }
        }
    };
}

define_abs!(AbsInt, CqlType::Int, i32);
define_abs!(AbsBigint, CqlType::Bigint, i64);
define_abs!(AbsFloat, CqlType::Float, f32);
define_abs!(AbsDouble, CqlType::Double, f64);

// ── Math: ceil, floor, round ──────────────────────────────────────────

macro_rules! define_float_math {
    ($name:ident, $cql_type:expr, $rust_type:ty, $op:ident) => {
        struct $name;

        impl CqlFunction for $name {
            fn name(&self) -> &str {
                stringify!($op)
            }
            fn arg_types(&self) -> Vec<CqlType> {
                vec![$cql_type]
            }
            fn return_type(&self) -> CqlType {
                $cql_type
            }
            fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
                match args.first().and_then(|a| *a) {
                    Some(bytes) => {
                        let arr: [u8; std::mem::size_of::<$rust_type>()] =
                            bytes.try_into().map_err(|_| "invalid bytes")?;
                        let v = <$rust_type>::from_be_bytes(arr);
                        let result = v.$op();
                        Ok(Some(result.to_be_bytes().to_vec()))
                    }
                    None => Ok(None),
                }
            }
        }
    };
}

define_float_math!(CeilFloat, CqlType::Float, f32, ceil);
define_float_math!(CeilDouble, CqlType::Double, f64, ceil);
define_float_math!(FloorFloat, CqlType::Float, f32, floor);
define_float_math!(FloorDouble, CqlType::Double, f64, floor);
define_float_math!(RoundFloat, CqlType::Float, f32, round);
define_float_math!(RoundDouble, CqlType::Double, f64, round);

// ── Length ─────────────────────────────────────────────────────────────

struct LengthText;

impl CqlFunction for LengthText {
    fn name(&self) -> &str {
        "length"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Varchar]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Int
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        match args.first().and_then(|a| *a) {
            Some(bytes) => {
                let s = std::str::from_utf8(bytes).map_err(|_| "invalid utf8")?;
                Ok(Some((s.len() as i32).to_be_bytes().to_vec()))
            }
            None => Ok(None),
        }
    }
}

struct LengthBlob;

impl CqlFunction for LengthBlob {
    fn name(&self) -> &str {
        "length"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Blob]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Int
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        match args.first().and_then(|a| *a) {
            Some(bytes) => Ok(Some((bytes.len() as i32).to_be_bytes().to_vec())),
            None => Ok(None),
        }
    }
}

// ── toJson ────────────────────────────────────────────────────────────

struct ToJsonFunction;

impl CqlFunction for ToJsonFunction {
    fn name(&self) -> &str {
        "tojson"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![] // accepts any type
    }
    fn return_type(&self) -> CqlType {
        CqlType::Varchar
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        // Basic toJson: converts value bytes to JSON string representation.
        // Full implementation would need the source CqlType at runtime.
        // For now, returns the bytes as a JSON string or "null".
        match args.first().and_then(|a| *a) {
            Some(bytes) => {
                // Try to interpret as UTF-8 text first
                if let Ok(s) = std::str::from_utf8(bytes) {
                    let json = format!("\"{}\"", s.replace('\\', "\\\\").replace('"', "\\\""));
                    Ok(Some(json.into_bytes()))
                } else {
                    // Hex-encode binary data
                    let hex = hex::encode(bytes);
                    let json = format!("\"0x{}\"", hex);
                    Ok(Some(json.into_bytes()))
                }
            }
            None => Ok(Some(b"null".to_vec())),
        }
    }
}

// ── fromJson ──────────────────────────────────────────────────────────

struct FromJsonFunction;

impl CqlFunction for FromJsonFunction {
    fn name(&self) -> &str {
        "fromjson"
    }
    fn arg_types(&self) -> Vec<CqlType> {
        vec![CqlType::Varchar]
    }
    fn return_type(&self) -> CqlType {
        CqlType::Blob // actual type determined by context
    }
    fn execute(&self, args: &[Option<&[u8]>]) -> Result<Option<Vec<u8>>, String> {
        // Basic fromJson: parses JSON text to a value.
        // Full implementation would need target CqlType at runtime.
        match args.first().and_then(|a| *a) {
            Some(bytes) => {
                let s = std::str::from_utf8(bytes).map_err(|_| "invalid utf8")?;
                let trimmed = s.trim();
                if trimmed == "null" {
                    Ok(None)
                } else if trimmed.starts_with('"') && trimmed.ends_with('"') {
                    // String value: strip quotes
                    let inner = &trimmed[1..trimmed.len() - 1];
                    Ok(Some(inner.as_bytes().to_vec()))
                } else if let Ok(v) = trimmed.parse::<i64>() {
                    Ok(Some(v.to_be_bytes().to_vec()))
                } else if let Ok(v) = trimmed.parse::<f64>() {
                    Ok(Some(v.to_be_bytes().to_vec()))
                } else if trimmed == "true" {
                    Ok(Some(vec![1]))
                } else if trimmed == "false" {
                    Ok(Some(vec![0]))
                } else {
                    // Return as-is (e.g. for arrays/objects)
                    Ok(Some(bytes.to_vec()))
                }
            }
            None => Ok(None),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn abs_int() {
        let f = AbsInt;
        let bytes = (-42i32).to_be_bytes();
        let result = f.execute(&[Some(&bytes)]).unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), 42);
    }

    #[test]
    fn abs_double() {
        let f = AbsDouble;
        let bytes = (-3.14f64).to_be_bytes();
        let result = f.execute(&[Some(&bytes)]).unwrap().unwrap();
        let v = f64::from_be_bytes(result.try_into().unwrap());
        assert!((v - 3.14).abs() < 1e-10);
    }

    #[test]
    fn ceil_float() {
        let f = CeilFloat;
        let bytes = 2.3f32.to_be_bytes();
        let result = f.execute(&[Some(&bytes)]).unwrap().unwrap();
        let v = f32::from_be_bytes(result.try_into().unwrap());
        assert_eq!(v, 3.0);
    }

    #[test]
    fn floor_double() {
        let f = FloorDouble;
        let bytes = 2.7f64.to_be_bytes();
        let result = f.execute(&[Some(&bytes)]).unwrap().unwrap();
        let v = f64::from_be_bytes(result.try_into().unwrap());
        assert_eq!(v, 2.0);
    }

    #[test]
    fn round_float() {
        let f = RoundFloat;
        let bytes = 2.5f32.to_be_bytes();
        let result = f.execute(&[Some(&bytes)]).unwrap().unwrap();
        let v = f32::from_be_bytes(result.try_into().unwrap());
        assert_eq!(v, 3.0);
    }

    #[test]
    fn length_text() {
        let f = LengthText;
        let bytes = b"hello";
        let result = f.execute(&[Some(bytes.as_slice())]).unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), 5);
    }

    #[test]
    fn length_blob() {
        let f = LengthBlob;
        let bytes = vec![1, 2, 3, 4];
        let result = f.execute(&[Some(&bytes)]).unwrap().unwrap();
        assert_eq!(i32::from_be_bytes(result.try_into().unwrap()), 4);
    }

    #[test]
    fn to_json_text() {
        let f = ToJsonFunction;
        let bytes = b"hello";
        let result = f.execute(&[Some(bytes.as_slice())]).unwrap().unwrap();
        assert_eq!(String::from_utf8(result).unwrap(), "\"hello\"");
    }

    #[test]
    fn to_json_null() {
        let f = ToJsonFunction;
        let result = f.execute(&[None]).unwrap().unwrap();
        assert_eq!(String::from_utf8(result).unwrap(), "null");
    }

    #[test]
    fn from_json_integer() {
        let f = FromJsonFunction;
        let bytes = b"42";
        let result = f
            .execute(&[Some(bytes.as_slice())])
            .unwrap()
            .unwrap();
        let v = i64::from_be_bytes(result.try_into().unwrap());
        assert_eq!(v, 42);
    }

    #[test]
    fn from_json_null() {
        let f = FromJsonFunction;
        let bytes = b"null";
        let result = f.execute(&[Some(bytes.as_slice())]).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn from_json_string() {
        let f = FromJsonFunction;
        let bytes = b"\"hello\"";
        let result = f
            .execute(&[Some(bytes.as_slice())])
            .unwrap()
            .unwrap();
        assert_eq!(String::from_utf8(result).unwrap(), "hello");
    }
}
