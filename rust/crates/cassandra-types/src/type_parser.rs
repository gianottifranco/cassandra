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

//! Parser for Java-style CQL marshal type strings.
//!
//! Handles the `org.apache.cassandra.db.marshal.*` class name format used
//! internally in Cassandra's schema tables and system_schema.
//!
//! ## Supported formats
//!
//! - Simple: `UTF8Type`, `org.apache.cassandra.db.marshal.Int32Type`
//! - Parameterized: `ListType(Int32Type)`, `MapType(UTF8Type,Int32Type)`
//! - Frozen: `FrozenType(ListType(Int32Type))`
//! - Reversed: `ReversedType(Int32Type)`
//! - Composite: `CompositeType(UTF8Type,Int32Type)`
//! - Tuple: `TupleType(Int32Type,UTF8Type)`
//! - UDT: `UserType(keyspace,name,field1hex,type1,...)`
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.marshal.TypeParser`

use crate::native::CqlType;
use std::fmt;

/// Error type for type string parsing failures.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    /// An unknown type name was encountered.
    UnknownType(String),
    /// The input string is malformed.
    Malformed(String),
    /// Unexpected end of input.
    UnexpectedEof,
}

impl fmt::Display for ParseError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownType(name) => write!(f, "unknown type: '{}'", name),
            Self::Malformed(msg) => write!(f, "malformed type string: {}", msg),
            Self::UnexpectedEof => write!(f, "unexpected end of type string"),
        }
    }
}

impl std::error::Error for ParseError {}

/// Parse a Java-style marshal type string into a [`CqlType`].
///
/// Accepts both short names (`UTF8Type`) and fully-qualified names
/// (`org.apache.cassandra.db.marshal.UTF8Type`).
///
/// # Errors
/// Returns [`ParseError`] if the string is malformed or contains an unknown type name.
pub fn parse_type(s: &str) -> Result<CqlType, ParseError> {
    let s = s.trim();
    if s.is_empty() {
        return Err(ParseError::UnexpectedEof);
    }
    let mut parser = Parser::new(s);
    let ty = parser.parse_type()?;
    Ok(ty)
}

// ── Internal parser ──────────────────────────────────────────────────────────

struct Parser<'a> {
    input: &'a str,
    pos: usize,
}

impl<'a> Parser<'a> {
    fn new(input: &'a str) -> Self {
        Self { input, pos: 0 }
    }

    fn remaining(&self) -> &str {
        &self.input[self.pos..]
    }

    fn peek(&self) -> Option<char> {
        self.remaining().chars().next()
    }

    fn advance_by(&mut self, n: usize) {
        self.pos += n;
    }

    fn consume_char(&mut self, c: char) -> Result<(), ParseError> {
        if self.remaining().starts_with(c) {
            self.advance_by(c.len_utf8());
            Ok(())
        } else {
            Err(ParseError::Malformed(format!(
                "expected '{}' at position {} in '{}'",
                c, self.pos, self.input
            )))
        }
    }

    /// Read a type name token: alphanumeric, dots, underscores, hyphens.
    fn read_type_name(&mut self) -> Result<&'a str, ParseError> {
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c.is_alphanumeric() || c == '.' || c == '_' || c == '-' {
                self.advance_by(c.len_utf8());
            } else {
                break;
            }
        }
        if self.pos == start {
            return Err(ParseError::UnexpectedEof);
        }
        Ok(&self.input[start..self.pos])
    }

    /// Parse a comma-separated list of types inside `(...)`.
    fn parse_type_params(&mut self) -> Result<Vec<CqlType>, ParseError> {
        self.consume_char('(')?;
        let mut types = Vec::new();
        loop {
            // skip whitespace
            while self.peek() == Some(' ') {
                self.advance_by(1);
            }
            if self.peek() == Some(')') {
                break;
            }
            types.push(self.parse_type()?);
            while self.peek() == Some(' ') {
                self.advance_by(1);
            }
            match self.peek() {
                Some(',') => {
                    self.advance_by(1);
                }
                Some(')') => break,
                Some(c) => {
                    return Err(ParseError::Malformed(format!(
                        "expected ',' or ')' but got '{}' at pos {}",
                        c, self.pos
                    )));
                }
                None => return Err(ParseError::UnexpectedEof),
            }
        }
        self.consume_char(')')?;
        Ok(types)
    }

    /// Parse a single type (possibly parameterized).
    fn parse_type(&mut self) -> Result<CqlType, ParseError> {
        let name = self.read_type_name()?;
        // Strip package prefix
        let short = strip_package(name);

        // Check if parameterized
        if self.peek() == Some('(') {
            match short {
                "FrozenType" => {
                    let mut params = self.parse_type_params()?;
                    if params.len() != 1 {
                        return Err(ParseError::Malformed(
                            "FrozenType requires exactly 1 parameter".into(),
                        ));
                    }
                    let inner = params.remove(0);
                    return Ok(freeze(inner));
                }
                "ReversedType" => {
                    let mut params = self.parse_type_params()?;
                    if params.len() != 1 {
                        return Err(ParseError::Malformed(
                            "ReversedType requires exactly 1 parameter".into(),
                        ));
                    }
                    return Ok(CqlType::Reversed(Box::new(params.remove(0))));
                }
                "ListType" => {
                    let mut params = self.parse_type_params()?;
                    if params.len() != 1 {
                        return Err(ParseError::Malformed(
                            "ListType requires exactly 1 parameter".into(),
                        ));
                    }
                    return Ok(CqlType::List(Box::new(params.remove(0)), false));
                }
                "SetType" => {
                    let mut params = self.parse_type_params()?;
                    if params.len() != 1 {
                        return Err(ParseError::Malformed(
                            "SetType requires exactly 1 parameter".into(),
                        ));
                    }
                    return Ok(CqlType::Set(Box::new(params.remove(0)), false));
                }
                "MapType" => {
                    let mut params = self.parse_type_params()?;
                    if params.len() != 2 {
                        return Err(ParseError::Malformed(
                            "MapType requires exactly 2 parameters".into(),
                        ));
                    }
                    let value = params.remove(1);
                    let key = params.remove(0);
                    return Ok(CqlType::Map(Box::new(key), Box::new(value), false));
                }
                "TupleType" => {
                    let params = self.parse_type_params()?;
                    return Ok(CqlType::Tuple(params));
                }
                "CompositeType" => {
                    // Composite is treated as a tuple for CQL purposes
                    let params = self.parse_type_params()?;
                    return Ok(CqlType::Tuple(params));
                }
                "UserType" => {
                    return self.parse_user_type();
                }
                _ => {
                    // Unknown parameterized type — consume params and fall through
                    let _ = self.parse_type_params()?;
                    return Err(ParseError::UnknownType(short.to_string()));
                }
            }
        }

        // Simple (non-parameterized) type
        map_simple_name(short)
    }

    /// Parse UserType(ks,name,field1hex,type1,...)
    fn parse_user_type(&mut self) -> Result<CqlType, ParseError> {
        self.consume_char('(')?;

        let keyspace = self.read_raw_token()?;
        self.consume_char(',')?;
        let name_hex = self.read_raw_token()?;
        let name = hex_decode_str(name_hex).unwrap_or_else(|_| name_hex.to_string());

        let mut field_names = Vec::new();
        let mut field_types = Vec::new();

        while self.peek() == Some(',') {
            self.advance_by(1); // consume ','
            // field name is hex-encoded
            let fname_hex = self.read_raw_token()?;
            let fname = hex_decode_str(fname_hex).unwrap_or_else(|_| fname_hex.to_string());
            self.consume_char(':')?;
            let ftype = self.parse_type()?;
            field_names.push(fname);
            field_types.push(ftype);
        }

        self.consume_char(')')?;

        Ok(CqlType::Udt {
            keyspace: keyspace.to_string(),
            name,
            field_names,
            field_types,
            is_multi_cell: true,
        })
    }

    /// Read a token that may contain non-type characters (used for UDT params).
    fn read_raw_token(&mut self) -> Result<&'a str, ParseError> {
        let start = self.pos;
        while let Some(c) = self.peek() {
            if c == ',' || c == ')' || c == ':' {
                break;
            }
            self.advance_by(c.len_utf8());
        }
        if self.pos == start {
            return Err(ParseError::UnexpectedEof);
        }
        Ok(&self.input[start..self.pos])
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────────

/// Strip the Java package prefix from a class name.
fn strip_package(name: &str) -> &str {
    if let Some(pos) = name.rfind('.') {
        &name[pos + 1..]
    } else {
        name
    }
}

/// Map a short Java class name to a [`CqlType`].
fn map_simple_name(name: &str) -> Result<CqlType, ParseError> {
    match name {
        "AsciiType" => Ok(CqlType::Ascii),
        "LongType" => Ok(CqlType::Bigint),
        "BytesType" => Ok(CqlType::Blob),
        "BooleanType" => Ok(CqlType::Boolean),
        "CounterColumnType" => Ok(CqlType::Counter),
        "DecimalType" => Ok(CqlType::Decimal),
        "DoubleType" => Ok(CqlType::Double),
        "FloatType" => Ok(CqlType::Float),
        "Int32Type" => Ok(CqlType::Int),
        "TimestampType" => Ok(CqlType::Timestamp),
        "UUIDType" => Ok(CqlType::Uuid),
        "UTF8Type" => Ok(CqlType::Varchar),
        "IntegerType" => Ok(CqlType::Varint),
        "TimeUUIDType" => Ok(CqlType::Timeuuid),
        "InetAddressType" => Ok(CqlType::Inet),
        "SimpleDateType" => Ok(CqlType::Date),
        "TimeType" => Ok(CqlType::Time),
        "ShortType" => Ok(CqlType::Smallint),
        "ByteType" => Ok(CqlType::Tinyint),
        "DurationType" => Ok(CqlType::Duration),
        "EmptyType" => Ok(CqlType::Empty),
        // Aliases
        "DateType" => Ok(CqlType::Timestamp), // legacy DateType maps to timestamp
        other => Err(ParseError::UnknownType(other.to_string())),
    }
}

/// Freeze the inner type of a collection.
fn freeze(ty: CqlType) -> CqlType {
    match ty {
        CqlType::List(inner, _) => CqlType::List(inner, true),
        CqlType::Set(inner, _) => CqlType::Set(inner, true),
        CqlType::Map(k, v, _) => CqlType::Map(k, v, true),
        CqlType::Udt {
            keyspace,
            name,
            field_names,
            field_types,
            ..
        } => CqlType::Udt {
            keyspace,
            name,
            field_names,
            field_types,
            is_multi_cell: false,
        },
        other => other, // scalars/tuples are already frozen
    }
}

/// Decode a hex-encoded string (used for UDT field names in marshal strings).
fn hex_decode_str(hex: &str) -> Result<String, ()> {
    let bytes: Option<Vec<u8>> = (0..hex.len())
        .step_by(2)
        .map(|i| {
            hex.get(i..i + 2)
                .and_then(|s| u8::from_str_radix(s, 16).ok())
        })
        .collect();
    bytes
        .and_then(|b| String::from_utf8(b).ok())
        .ok_or(())
}

// ─── Tests ───────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn simple_types() {
        assert_eq!(parse_type("UTF8Type").unwrap(), CqlType::Varchar);
        assert_eq!(parse_type("Int32Type").unwrap(), CqlType::Int);
        assert_eq!(parse_type("LongType").unwrap(), CqlType::Bigint);
        assert_eq!(parse_type("BooleanType").unwrap(), CqlType::Boolean);
        assert_eq!(parse_type("DurationType").unwrap(), CqlType::Duration);
        assert_eq!(parse_type("EmptyType").unwrap(), CqlType::Empty);
    }

    #[test]
    fn fully_qualified() {
        assert_eq!(
            parse_type("org.apache.cassandra.db.marshal.UTF8Type").unwrap(),
            CqlType::Varchar
        );
        assert_eq!(
            parse_type("org.apache.cassandra.db.marshal.Int32Type").unwrap(),
            CqlType::Int
        );
    }

    #[test]
    fn list_type() {
        let ty = parse_type("ListType(Int32Type)").unwrap();
        assert_eq!(ty, CqlType::List(Box::new(CqlType::Int), false));
    }

    #[test]
    fn frozen_list() {
        let ty = parse_type("FrozenType(ListType(Int32Type))").unwrap();
        assert_eq!(ty, CqlType::List(Box::new(CqlType::Int), true));
    }

    #[test]
    fn map_type() {
        let ty = parse_type("MapType(UTF8Type,Int32Type)").unwrap();
        assert_eq!(
            ty,
            CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int), false)
        );
    }

    #[test]
    fn set_type() {
        let ty = parse_type("SetType(UTF8Type)").unwrap();
        assert_eq!(ty, CqlType::Set(Box::new(CqlType::Varchar), false));
    }

    #[test]
    fn tuple_type() {
        let ty = parse_type("TupleType(Int32Type,UTF8Type)").unwrap();
        assert_eq!(ty, CqlType::Tuple(vec![CqlType::Int, CqlType::Varchar]));
    }

    #[test]
    fn reversed_type() {
        let ty = parse_type("ReversedType(LongType)").unwrap();
        assert_eq!(ty, CqlType::Reversed(Box::new(CqlType::Bigint)));
    }

    #[test]
    fn nested_collection() {
        // FrozenType(MapType(UTF8Type,FrozenType(ListType(Int32Type))))
        let ty = parse_type(
            "FrozenType(MapType(UTF8Type,FrozenType(ListType(Int32Type))))",
        )
        .unwrap();
        assert!(matches!(ty, CqlType::Map(_, _, true)));
    }

    #[test]
    fn unknown_type_error() {
        assert!(matches!(
            parse_type("UnknownCoolType"),
            Err(ParseError::UnknownType(_))
        ));
    }

    #[test]
    fn empty_string_error() {
        assert!(matches!(parse_type(""), Err(ParseError::UnexpectedEof)));
    }
}
