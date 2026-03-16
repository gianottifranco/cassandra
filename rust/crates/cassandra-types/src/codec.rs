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

//! Binary codec for CQL values (native protocol internal format).
//!
//! ## Java Oracle
//! - `org.apache.cassandra.serializers.*`

use crate::native::CqlType;
use byteorder::{BigEndian, ByteOrder};
use serde::{Deserialize, Serialize};
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

/// A materialized CQL value.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CqlValue {
    Null,
    Ascii(String),
    Bigint(i64),
    Blob(Vec<u8>),
    Boolean(bool),
    Counter(i64),
    Decimal {
        unscaled: Vec<u8>,
        scale: i32,
    },
    Double(f64),
    Float(f32),
    Int(i32),
    Timestamp(i64),
    Uuid([u8; 16]),
    Varchar(String),
    Varint(Vec<u8>),
    Timeuuid([u8; 16]),
    Inet(IpAddr),
    Date(u32),
    Time(i64),
    Smallint(i16),
    Tinyint(i8),
    Duration {
        months: i32,
        days: i32,
        nanoseconds: i64,
    },
    Empty,
    List(Vec<CqlValue>),
    Set(Vec<CqlValue>),
    Map(Vec<(CqlValue, CqlValue)>),
    Tuple(Vec<Option<CqlValue>>),
    Udt(Vec<(String, Option<CqlValue>)>),
}

impl CqlValue {
    /// Serialize this value to bytes (no length prefix).
    pub fn serialize_value(&self) -> Vec<u8> {
        match self {
            CqlValue::Null | CqlValue::Empty => vec![],
            CqlValue::Ascii(s) | CqlValue::Varchar(s) => s.as_bytes().to_vec(),
            CqlValue::Bigint(v)
            | CqlValue::Counter(v)
            | CqlValue::Timestamp(v)
            | CqlValue::Time(v) => {
                let mut b = vec![0u8; 8];
                BigEndian::write_i64(&mut b, *v);
                b
            }
            CqlValue::Blob(b) | CqlValue::Varint(b) => b.clone(),
            CqlValue::Boolean(b) => vec![if *b { 1 } else { 0 }],
            CqlValue::Decimal { unscaled, scale } => {
                let mut b = vec![0u8; 4];
                BigEndian::write_i32(&mut b, *scale);
                b.extend_from_slice(unscaled);
                b
            }
            CqlValue::Double(v) => {
                let mut b = vec![0u8; 8];
                BigEndian::write_f64(&mut b, *v);
                b
            }
            CqlValue::Float(v) => {
                let mut b = vec![0u8; 4];
                BigEndian::write_f32(&mut b, *v);
                b
            }
            CqlValue::Int(v) => {
                let mut b = vec![0u8; 4];
                BigEndian::write_i32(&mut b, *v);
                b
            }
            CqlValue::Uuid(bytes) | CqlValue::Timeuuid(bytes) => bytes.to_vec(),
            CqlValue::Inet(addr) => match addr {
                IpAddr::V4(v4) => v4.octets().to_vec(),
                IpAddr::V6(v6) => v6.octets().to_vec(),
            },
            CqlValue::Date(v) => {
                let mut b = vec![0u8; 4];
                BigEndian::write_u32(&mut b, *v);
                b
            }
            CqlValue::Smallint(v) => {
                let mut b = vec![0u8; 2];
                BigEndian::write_i16(&mut b, *v);
                b
            }
            CqlValue::Tinyint(v) => vec![*v as u8],
            CqlValue::Duration {
                months,
                days,
                nanoseconds,
            } => {
                let mut b = Vec::with_capacity(16);
                let mut t4 = [0u8; 4];
                BigEndian::write_i32(&mut t4, *months);
                b.extend_from_slice(&t4);
                BigEndian::write_i32(&mut t4, *days);
                b.extend_from_slice(&t4);
                let mut t8 = [0u8; 8];
                BigEndian::write_i64(&mut t8, *nanoseconds);
                b.extend_from_slice(&t8);
                b
            }
            CqlValue::List(items) | CqlValue::Set(items) => {
                let mut b = Vec::new();
                let mut lb = [0u8; 4];
                BigEndian::write_i32(&mut lb, items.len() as i32);
                b.extend_from_slice(&lb);
                for item in items {
                    write_lp(&mut b, &item.serialize_value());
                }
                b
            }
            CqlValue::Map(entries) => {
                let mut b = Vec::new();
                let mut lb = [0u8; 4];
                BigEndian::write_i32(&mut lb, entries.len() as i32);
                b.extend_from_slice(&lb);
                for (k, v) in entries {
                    write_lp(&mut b, &k.serialize_value());
                    write_lp(&mut b, &v.serialize_value());
                }
                b
            }
            CqlValue::Tuple(fields) => {
                let mut b = Vec::new();
                for f in fields {
                    write_opt(&mut b, f);
                }
                b
            }
            CqlValue::Udt(fields) => {
                let mut b = Vec::new();
                for (_, v) in fields {
                    write_opt(&mut b, v);
                }
                b
            }
        }
    }

    /// Deserialize a native scalar from bytes (no length prefix).
    pub fn deserialize_value(cql_type: &CqlType, data: &[u8]) -> Result<Self, CodecError> {
        match cql_type {
            CqlType::Ascii => Ok(CqlValue::Ascii(to_str(data)?.to_string())),
            CqlType::Varchar => Ok(CqlValue::Varchar(to_str(data)?.to_string())),
            CqlType::Bigint => {
                ck(data, 8)?;
                Ok(CqlValue::Bigint(BigEndian::read_i64(data)))
            }
            CqlType::Counter => {
                ck(data, 8)?;
                Ok(CqlValue::Counter(BigEndian::read_i64(data)))
            }
            CqlType::Timestamp => {
                ck(data, 8)?;
                Ok(CqlValue::Timestamp(BigEndian::read_i64(data)))
            }
            CqlType::Time => {
                ck(data, 8)?;
                Ok(CqlValue::Time(BigEndian::read_i64(data)))
            }
            CqlType::Blob => Ok(CqlValue::Blob(data.to_vec())),
            CqlType::Boolean => {
                ck(data, 1)?;
                Ok(CqlValue::Boolean(data[0] != 0))
            }
            CqlType::Double => {
                ck(data, 8)?;
                Ok(CqlValue::Double(BigEndian::read_f64(data)))
            }
            CqlType::Float => {
                ck(data, 4)?;
                Ok(CqlValue::Float(BigEndian::read_f32(data)))
            }
            CqlType::Int => {
                ck(data, 4)?;
                Ok(CqlValue::Int(BigEndian::read_i32(data)))
            }
            CqlType::Uuid => {
                ck(data, 16)?;
                let mut b = [0u8; 16];
                b.copy_from_slice(data);
                Ok(CqlValue::Uuid(b))
            }
            CqlType::Timeuuid => {
                ck(data, 16)?;
                let mut b = [0u8; 16];
                b.copy_from_slice(data);
                Ok(CqlValue::Timeuuid(b))
            }
            CqlType::Inet => match data.len() {
                4 => Ok(CqlValue::Inet(IpAddr::V4(Ipv4Addr::new(
                    data[0], data[1], data[2], data[3],
                )))),
                16 => {
                    let mut b = [0u8; 16];
                    b.copy_from_slice(data);
                    Ok(CqlValue::Inet(IpAddr::V6(Ipv6Addr::from(b))))
                }
                _ => Err(CodecError::InvalidLength {
                    expected: 4,
                    got: data.len(),
                }),
            },
            CqlType::Date => {
                ck(data, 4)?;
                Ok(CqlValue::Date(BigEndian::read_u32(data)))
            }
            CqlType::Smallint => {
                ck(data, 2)?;
                Ok(CqlValue::Smallint(BigEndian::read_i16(data)))
            }
            CqlType::Tinyint => {
                ck(data, 1)?;
                Ok(CqlValue::Tinyint(data[0] as i8))
            }
            CqlType::Varint => Ok(CqlValue::Varint(data.to_vec())),
            CqlType::Decimal => {
                if data.len() < 4 {
                    return Err(CodecError::TooShort {
                        minimum: 4,
                        got: data.len(),
                    });
                }
                Ok(CqlValue::Decimal {
                    scale: BigEndian::read_i32(data),
                    unscaled: data[4..].to_vec(),
                })
            }
            CqlType::Empty => Ok(CqlValue::Empty),
            CqlType::Reversed(inner) => Self::deserialize_value(inner, data),
            _ => Err(CodecError::UnsupportedType(cql_type.cql_name())),
        }
    }
}

fn ck(data: &[u8], expected: usize) -> Result<(), CodecError> {
    if data.len() != expected {
        Err(CodecError::InvalidLength {
            expected,
            got: data.len(),
        })
    } else {
        Ok(())
    }
}
fn to_str(data: &[u8]) -> Result<&str, CodecError> {
    std::str::from_utf8(data).map_err(|_| CodecError::InvalidUtf8)
}
fn write_lp(buf: &mut Vec<u8>, data: &[u8]) {
    let mut lb = [0u8; 4];
    BigEndian::write_i32(&mut lb, data.len() as i32);
    buf.extend_from_slice(&lb);
    buf.extend_from_slice(data);
}
fn write_opt(buf: &mut Vec<u8>, val: &Option<CqlValue>) {
    match val {
        Some(v) => write_lp(buf, &v.serialize_value()),
        None => {
            let mut n = [0u8; 4];
            BigEndian::write_i32(&mut n, -1);
            buf.extend_from_slice(&n);
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CodecError {
    InvalidLength { expected: usize, got: usize },
    TooShort { minimum: usize, got: usize },
    InvalidUtf8,
    UnsupportedType(String),
}
impl fmt::Display for CodecError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidLength { expected, got } => {
                write!(f, "expected {} bytes, got {}", expected, got)
            }
            Self::TooShort { minimum, got } => write!(f, "need >= {} bytes, got {}", minimum, got),
            Self::InvalidUtf8 => write!(f, "invalid UTF-8"),
            Self::UnsupportedType(n) => write!(f, "unsupported type: {}", n),
        }
    }
}
impl std::error::Error for CodecError {}

#[cfg(test)]
mod tests {
    use super::*;
    fn rt(t: &CqlType, v: CqlValue) {
        let bytes = v.serialize_value();
        let d = CqlValue::deserialize_value(t, &bytes).unwrap();
        match (&v, &d) {
            (CqlValue::Double(_), CqlValue::Double(_))
            | (CqlValue::Float(_), CqlValue::Float(_)) => {
                assert_eq!(v.serialize_value(), d.serialize_value())
            }
            _ => assert_eq!(v, d),
        }
    }
    #[test]
    fn rt_int() {
        rt(&CqlType::Int, CqlValue::Int(42));
    }
    #[test]
    fn rt_bigint() {
        rt(&CqlType::Bigint, CqlValue::Bigint(i64::MAX));
    }
    #[test]
    fn rt_bool() {
        rt(&CqlType::Boolean, CqlValue::Boolean(true));
    }
    #[test]
    fn rt_double() {
        rt(&CqlType::Double, CqlValue::Double(3.14));
    }
    #[test]
    fn rt_float() {
        rt(&CqlType::Float, CqlValue::Float(2.71));
    }
    #[test]
    fn rt_varchar() {
        rt(&CqlType::Varchar, CqlValue::Varchar("hello".into()));
    }
    #[test]
    fn rt_blob() {
        rt(&CqlType::Blob, CqlValue::Blob(vec![0xDE, 0xAD]));
    }
    #[test]
    fn rt_uuid() {
        rt(&CqlType::Uuid, CqlValue::Uuid([1; 16]));
    }
    #[test]
    fn rt_timestamp() {
        rt(&CqlType::Timestamp, CqlValue::Timestamp(1_710_000_000_000));
    }
    #[test]
    fn rt_inet_v4() {
        rt(
            &CqlType::Inet,
            CqlValue::Inet(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        );
    }
    #[test]
    fn rt_smallint() {
        rt(&CqlType::Smallint, CqlValue::Smallint(-32768));
    }
    #[test]
    fn rt_tinyint() {
        rt(&CqlType::Tinyint, CqlValue::Tinyint(-128));
    }
    #[test]
    fn golden_int_42() {
        assert_eq!(
            CqlValue::Int(42).serialize_value(),
            vec![0x00, 0x00, 0x00, 0x2A]
        );
    }
    #[test]
    fn wrong_len() {
        assert!(CqlValue::deserialize_value(&CqlType::Int, &[0u8; 3]).is_err());
    }
}
