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

//! Free-function facade unifying codec, comparator, and validation.
//!
//! This module mirrors the `AbstractType` interface from Java but implements
//! it as free functions dispatching via `match` on [`CqlType`] rather than
//! through trait objects or virtual methods.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.marshal.AbstractType`

use std::cmp::Ordering;
use std::net::IpAddr;

use crate::bigint;
use crate::codec::{CodecError, CqlValue};
use crate::comparator::compare_bytes;
use crate::marshal::{MarshalError, MarshalResult};
use crate::native::CqlType;

// ── Core dispatch functions ──────────────────────────────────────────────────

/// Compare two serialized CQL values of the given type.
///
/// Equivalent to `AbstractType.compare(ByteBuffer, ByteBuffer)`.
pub fn compare(cql_type: &CqlType, left: &[u8], right: &[u8]) -> Ordering {
    compare_bytes(cql_type, left, right)
}

/// Validate that `data` is a well-formed encoding for `cql_type`.
///
/// Equivalent to `AbstractType.validate(ByteBuffer)`.
///
/// # Errors
/// Returns [`MarshalError`] if the bytes cannot be decoded as `cql_type`.
pub fn validate(cql_type: &CqlType, data: &[u8]) -> MarshalResult<()> {
    CqlValue::deserialize_value(cql_type, data)
        .map(|_| ())
        .map_err(codec_to_marshal(cql_type))
}

/// Serialize a [`CqlValue`] to bytes (no length prefix).
///
/// Equivalent to `AbstractType.decompose(T)` → raw bytes.
pub fn serialize(_cql_type: &CqlType, value: &CqlValue) -> Vec<u8> {
    value.serialize_value()
}

/// Deserialize bytes into a [`CqlValue`] of the given type.
///
/// Equivalent to `AbstractType.compose(ByteBuffer)`.
///
/// # Errors
/// Returns [`MarshalError`] if the bytes are malformed.
pub fn deserialize(cql_type: &CqlType, data: &[u8]) -> MarshalResult<CqlValue> {
    CqlValue::deserialize_value(cql_type, data).map_err(codec_to_marshal(cql_type))
}

/// Parse a CQL literal string into bytes for the given type.
///
/// Handles common CQL literal forms:
/// - Integers for numeric types
/// - Quoted strings for text types
/// - `true`/`false` for boolean
/// - Hex blobs `0x...`
///
/// ## Java Oracle
/// `AbstractType.fromString(String)` / `CQL3Type.fromCQLLiteral(String)`
///
/// # Errors
/// Returns [`MarshalError`] if the string cannot be parsed as the given type.
pub fn from_cql_string(cql_type: &CqlType, s: &str) -> MarshalResult<Vec<u8>> {
    use byteorder::{BigEndian, ByteOrder};

    let s = s.trim();
    match cql_type {
        CqlType::Boolean => match s.to_lowercase().as_str() {
            "true" | "1" => Ok(vec![1]),
            "false" | "0" => Ok(vec![0]),
            _ => Err(MarshalError::InvalidData {
                type_name: "boolean".into(),
                reason: format!("cannot parse '{}'", s),
            }),
        },
        CqlType::Tinyint => parse_int(s, "tinyint").and_then(|n| {
            if n >= i8::MIN as i64 && n <= i8::MAX as i64 {
                Ok(vec![n as i8 as u8])
            } else {
                Err(MarshalError::Overflow)
            }
        }),
        CqlType::Smallint => parse_int(s, "smallint").and_then(|n| {
            if n >= i16::MIN as i64 && n <= i16::MAX as i64 {
                let mut b = vec![0u8; 2];
                BigEndian::write_i16(&mut b, n as i16);
                Ok(b)
            } else {
                Err(MarshalError::Overflow)
            }
        }),
        CqlType::Int => parse_int(s, "int").and_then(|n| {
            if n >= i32::MIN as i64 && n <= i32::MAX as i64 {
                let mut b = vec![0u8; 4];
                BigEndian::write_i32(&mut b, n as i32);
                Ok(b)
            } else {
                Err(MarshalError::Overflow)
            }
        }),
        CqlType::Bigint | CqlType::Counter | CqlType::Timestamp | CqlType::Time => {
            parse_int(s, &cql_type.cql_name()).map(|n| {
                let mut b = vec![0u8; 8];
                BigEndian::write_i64(&mut b, n);
                b
            })
        }
        CqlType::Float => s
            .parse::<f32>()
            .map(|v| {
                let mut b = vec![0u8; 4];
                BigEndian::write_f32(&mut b, v);
                b
            })
            .map_err(|_| MarshalError::InvalidData {
                type_name: "float".into(),
                reason: format!("cannot parse '{}'", s),
            }),
        CqlType::Double => s
            .parse::<f64>()
            .map(|v| {
                let mut b = vec![0u8; 8];
                BigEndian::write_f64(&mut b, v);
                b
            })
            .map_err(|_| MarshalError::InvalidData {
                type_name: "double".into(),
                reason: format!("cannot parse '{}'", s),
            }),
        CqlType::Ascii => {
            if s.is_ascii() {
                Ok(s.as_bytes().to_vec())
            } else {
                Err(MarshalError::InvalidData {
                    type_name: "ascii".into(),
                    reason: "non-ASCII characters".into(),
                })
            }
        }
        CqlType::Varchar => Ok(s.as_bytes().to_vec()),
        CqlType::Blob => parse_hex_blob(s),
        CqlType::Varint => bigint::string_to_varint(s),
        CqlType::Decimal => bigint::string_to_decimal(s),
        CqlType::Inet => {
            let addr: IpAddr = s.parse().map_err(|_| MarshalError::InvalidData {
                type_name: "inet".into(),
                reason: format!("cannot parse '{}' as IP address", s),
            })?;
            match addr {
                IpAddr::V4(v4) => Ok(v4.octets().to_vec()),
                IpAddr::V6(v6) => Ok(v6.octets().to_vec()),
            }
        }
        CqlType::Uuid | CqlType::Timeuuid => {
            let hex: String = s.chars().filter(|c| *c != '-').collect();
            if hex.len() != 32 {
                return Err(MarshalError::InvalidData {
                    type_name: cql_type.cql_name(),
                    reason: format!("invalid UUID '{}'", s),
                });
            }
            (0..hex.len())
                .step_by(2)
                .map(|i| {
                    hex.get(i..i + 2)
                        .and_then(|h| u8::from_str_radix(h, 16).ok())
                        .ok_or(MarshalError::InvalidData {
                            type_name: cql_type.cql_name(),
                            reason: format!("invalid hex in UUID at position {}", i),
                        })
                })
                .collect()
        }
        CqlType::Date => {
            let n: u32 = s.parse().map_err(|_| MarshalError::InvalidData {
                type_name: "date".into(),
                reason: format!("cannot parse '{}' as date", s),
            })?;
            Ok(n.to_be_bytes().to_vec())
        }
        CqlType::Vector(inner, dimensions) => {
            if **inner != CqlType::Float {
                return Err(MarshalError::UnsupportedType(cql_type.cql_name()));
            }
            let components = parse_vector_literal(s, *dimensions)?;
            let vector = crate::vector::VectorValue::new(components);
            vector.validate().map_err(|err| MarshalError::InvalidData {
                type_name: cql_type.cql_name(),
                reason: err.to_string(),
            })?;
            Ok(vector.serialize())
        }
        _ => Err(MarshalError::UnsupportedType(cql_type.cql_name())),
    }
}

/// Return the CQL3 type name string.
///
/// Equivalent to `AbstractType.asCQL3Type().toString()`.
pub fn cql_type_name(cql_type: &CqlType) -> String {
    cql_type.cql_name()
}

// ── Private helpers ──────────────────────────────────────────────────────────

fn codec_to_marshal(cql_type: &CqlType) -> impl Fn(CodecError) -> MarshalError + '_ {
    move |e| match e {
        CodecError::InvalidUtf8 => MarshalError::Utf8Error,
        CodecError::InvalidLength { expected, got } => MarshalError::InvalidSize {
            expected,
            actual: got,
        },
        CodecError::TooShort { minimum, got } => MarshalError::InvalidSize {
            expected: minimum,
            actual: got,
        },
        CodecError::UnsupportedType(name) => MarshalError::UnsupportedType(name),
        CodecError::InvalidVector(reason) => MarshalError::InvalidData {
            type_name: "vector".to_string(),
            reason,
        },
        CodecError::TrailingBytes { consumed, total } => MarshalError::InvalidData {
            type_name: cql_type.cql_name(),
            reason: format!("trailing bytes after {} of {} bytes", consumed, total),
        },
        CodecError::OutOfRange { type_name, value } => MarshalError::InvalidData {
            type_name: type_name.to_string(),
            reason: format!("value {} out of range", value),
        },
    }
}

fn parse_int(s: &str, type_name: &str) -> MarshalResult<i64> {
    s.parse::<i64>().map_err(|_| MarshalError::InvalidData {
        type_name: type_name.to_string(),
        reason: format!("cannot parse '{}' as integer", s),
    })
}

fn parse_hex_blob(s: &str) -> MarshalResult<Vec<u8>> {
    let s = s
        .strip_prefix("0x")
        .or_else(|| s.strip_prefix("0X"))
        .unwrap_or(s);
    (0..s.len())
        .step_by(2)
        .map(|i| {
            s.get(i..i + 2)
                .and_then(|h| u8::from_str_radix(h, 16).ok())
                .ok_or(MarshalError::InvalidData {
                    type_name: "blob".into(),
                    reason: format!("invalid hex at position {}", i),
                })
        })
        .collect()
}

fn parse_vector_literal(s: &str, dimensions: u32) -> MarshalResult<Vec<f32>> {
    let inner = s
        .strip_prefix('[')
        .and_then(|rest| rest.strip_suffix(']'))
        .ok_or_else(|| MarshalError::InvalidData {
            type_name: "vector".into(),
            reason: format!("vector literal must be enclosed in brackets: '{}'", s),
        })?
        .trim();

    let components = if inner.is_empty() {
        Vec::new()
    } else {
        inner
            .split(',')
            .map(|part| {
                let part = part.trim();
                part.parse::<f32>().map_err(|_| MarshalError::InvalidData {
                    type_name: "vector".into(),
                    reason: format!("cannot parse vector component '{}'", part),
                })
            })
            .collect::<MarshalResult<Vec<_>>>()?
    };

    if components.len() != dimensions as usize {
        return Err(MarshalError::InvalidData {
            type_name: "vector".into(),
            reason: format!(
                "dimension mismatch: expected {}, got {}",
                dimensions,
                components.len()
            ),
        });
    }

    Ok(components)
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compare_ints() {
        let a = serialize(&CqlType::Int, &CqlValue::Int(1));
        let b = serialize(&CqlType::Int, &CqlValue::Int(2));
        assert_eq!(compare(&CqlType::Int, &a, &b), Ordering::Less);
    }

    #[test]
    fn validate_int_ok() {
        let bytes = CqlValue::Int(42).serialize_value();
        assert!(validate(&CqlType::Int, &bytes).is_ok());
    }

    #[test]
    fn validate_int_err() {
        assert!(validate(&CqlType::Int, &[0u8; 3]).is_err());
    }

    #[test]
    fn from_cql_string_int() {
        let bytes = from_cql_string(&CqlType::Int, "42").unwrap();
        let val = deserialize(&CqlType::Int, &bytes).unwrap();
        assert_eq!(val, CqlValue::Int(42));
    }

    #[test]
    fn from_cql_string_bool() {
        let t = from_cql_string(&CqlType::Boolean, "true").unwrap();
        assert_eq!(t, vec![1]);
        let f = from_cql_string(&CqlType::Boolean, "false").unwrap();
        assert_eq!(f, vec![0]);
    }

    #[test]
    fn from_cql_string_blob() {
        let b = from_cql_string(&CqlType::Blob, "0xDEAD").unwrap();
        assert_eq!(b, vec![0xDE, 0xAD]);
    }

    #[test]
    fn cql_type_name_int() {
        assert_eq!(cql_type_name(&CqlType::Int), "int");
    }

    #[test]
    fn serialize_deserialize_roundtrip() {
        let val = CqlValue::Bigint(i64::MAX);
        let bytes = serialize(&CqlType::Bigint, &val);
        let out = deserialize(&CqlType::Bigint, &bytes).unwrap();
        assert_eq!(out, val);
    }

    #[test]
    fn from_cql_string_vector_float() {
        let ty = CqlType::Vector(Box::new(CqlType::Float), 3);
        let bytes = from_cql_string(&ty, "[1.0, -2.5, 3.25]").unwrap();
        let out = deserialize(&ty, &bytes).unwrap();
        assert_eq!(
            out,
            CqlValue::Vector(crate::vector::VectorValue::new(vec![1.0, -2.5, 3.25]))
        );
    }

    #[test]
    fn from_cql_string_vector_rejects_dimension_mismatch() {
        let ty = CqlType::Vector(Box::new(CqlType::Float), 3);
        assert!(matches!(
            from_cql_string(&ty, "[1.0, 2.0]"),
            Err(MarshalError::InvalidData { .. })
        ));
    }

    #[test]
    fn from_cql_string_vector_rejects_non_finite_component() {
        let ty = CqlType::Vector(Box::new(CqlType::Float), 2);
        assert!(matches!(
            from_cql_string(&ty, "[1.0, NaN]"),
            Err(MarshalError::InvalidData { .. })
        ));
    }
}
