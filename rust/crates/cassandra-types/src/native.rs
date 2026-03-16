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

//! CQL native type enumeration.
//!
//! Maps 1:1 to the `AbstractType` subclass hierarchy from
//! `org.apache.cassandra.db.marshal.*`.
//!
//! Unlike Java's class-based polymorphism, we use a flat enum for exhaustive
//! matching and zero-cost dispatch.

use std::fmt;

use serde::{Deserialize, Serialize};

/// A CQL native or complex type.
///
/// This enum covers all types supported by CQL3 including native scalars,
/// collections, tuples, and user-defined types (by reference).
///
/// ## Java Oracle
///
/// Each variant corresponds to a concrete subclass of
/// `org.apache.cassandra.db.marshal.AbstractType`:
///
/// | CQL type    | Java class          | Protocol ID |
/// |-------------|---------------------|-------------|
/// | ascii       | AsciiType           | 0x0001      |
/// | bigint      | LongType            | 0x0002      |
/// | blob        | BytesType           | 0x0003      |
/// | boolean     | BooleanType         | 0x0004      |
/// | counter     | CounterColumnType   | 0x0005      |
/// | decimal     | DecimalType         | 0x0006      |
/// | double      | DoubleType          | 0x0007      |
/// | float       | FloatType           | 0x0008      |
/// | int         | Int32Type           | 0x0009      |
/// | timestamp   | TimestampType       | 0x000B      |
/// | uuid        | UUIDType            | 0x000C      |
/// | varchar     | UTF8Type            | 0x000D      |
/// | varint      | IntegerType         | 0x000E      |
/// | timeuuid    | TimeUUIDType        | 0x000F      |
/// | inet        | InetAddressType     | 0x0010      |
/// | date        | SimpleDateType      | 0x0011      |
/// | time        | TimeType            | 0x0012      |
/// | smallint    | ShortType           | 0x0013      |
/// | tinyint     | ByteType            | 0x0014      |
/// | duration    | DurationType        | 0x0015      |
/// | list        | ListType            | 0x0020      |
/// | map         | MapType             | 0x0021      |
/// | set         | SetType             | 0x0022      |
/// | udt         | UserType            | 0x0030      |
/// | tuple       | TupleType           | 0x0031      |
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum CqlType {
    // ── Native scalar types ──
    /// ASCII string (US-ASCII only).
    Ascii,
    /// 64-bit signed integer.
    Bigint,
    /// Arbitrary bytes (blob).
    Blob,
    /// Boolean (true/false).
    Boolean,
    /// Distributed counter.
    Counter,
    /// Arbitrary-precision decimal.
    Decimal,
    /// 64-bit IEEE 754 floating point.
    Double,
    /// 32-bit IEEE 754 floating point.
    Float,
    /// 32-bit signed integer.
    Int,
    /// Millisecond-precision timestamp.
    Timestamp,
    /// RFC 4122 UUID.
    Uuid,
    /// UTF-8 string (alias: text).
    Varchar,
    /// Arbitrary-precision integer.
    Varint,
    /// Version 1 time-based UUID.
    Timeuuid,
    /// IPv4 or IPv6 address.
    Inet,
    /// Date without time (days since epoch).
    Date,
    /// Time of day (nanoseconds since midnight).
    Time,
    /// 16-bit signed integer.
    Smallint,
    /// 8-bit signed integer.
    Tinyint,
    /// Duration type (months, days, nanoseconds).
    Duration,
    /// Empty type (internal, used for thrift compat).
    Empty,

    // ── Complex types (references inner types) ──
    /// Ordered list: `list<T>`.
    List(Box<CqlType>, bool),
    /// Unique set: `set<T>`.
    Set(Box<CqlType>, bool),
    /// Key-value map: `map<K, V>`.
    Map(Box<CqlType>, Box<CqlType>, bool),
    /// Fixed-length tuple: `tuple<T1, T2, ...>`.
    Tuple(Vec<CqlType>),
    /// User-defined type reference (keyspace, name, field_names, field_types).
    Udt {
        keyspace: String,
        name: String,
        field_names: Vec<String>,
        field_types: Vec<CqlType>,
        is_multi_cell: bool,
    },

    /// Reversed type wrapper (for DESC clustering order).
    Reversed(Box<CqlType>),

    /// Fixed-dimension vector type: `vector<T, n>`.
    ///
    /// Added in Cassandra 5.0 for vector similarity search.
    /// The inner type is typically Float, and the u32 is the dimension count.
    Vector(Box<CqlType>, u32),
}

impl CqlType {
    /// The CQL native protocol option ID for this type.
    ///
    /// Returns `None` for complex types that need sub-type encoding.
    pub fn protocol_id(&self) -> Option<u16> {
        match self {
            CqlType::Ascii => Some(0x0001),
            CqlType::Bigint => Some(0x0002),
            CqlType::Blob => Some(0x0003),
            CqlType::Boolean => Some(0x0004),
            CqlType::Counter => Some(0x0005),
            CqlType::Decimal => Some(0x0006),
            CqlType::Double => Some(0x0007),
            CqlType::Float => Some(0x0008),
            CqlType::Int => Some(0x0009),
            CqlType::Timestamp => Some(0x000B),
            CqlType::Uuid => Some(0x000C),
            CqlType::Varchar => Some(0x000D),
            CqlType::Varint => Some(0x000E),
            CqlType::Timeuuid => Some(0x000F),
            CqlType::Inet => Some(0x0010),
            CqlType::Date => Some(0x0011),
            CqlType::Time => Some(0x0012),
            CqlType::Smallint => Some(0x0013),
            CqlType::Tinyint => Some(0x0014),
            CqlType::Duration => Some(0x0015),
            CqlType::List(_, _) => Some(0x0020),
            CqlType::Map(_, _, _) => Some(0x0021),
            CqlType::Set(_, _) => Some(0x0022),
            CqlType::Udt { .. } => Some(0x0030),
            CqlType::Tuple(_) => Some(0x0031),
            CqlType::Vector(_, _) => Some(0x0032),
            CqlType::Empty | CqlType::Reversed(_) => None,
        }
    }

    /// CQL3 string representation of this type.
    pub fn cql_name(&self) -> String {
        match self {
            CqlType::Ascii => "ascii".to_string(),
            CqlType::Bigint => "bigint".to_string(),
            CqlType::Blob => "blob".to_string(),
            CqlType::Boolean => "boolean".to_string(),
            CqlType::Counter => "counter".to_string(),
            CqlType::Decimal => "decimal".to_string(),
            CqlType::Double => "double".to_string(),
            CqlType::Float => "float".to_string(),
            CqlType::Int => "int".to_string(),
            CqlType::Timestamp => "timestamp".to_string(),
            CqlType::Uuid => "uuid".to_string(),
            CqlType::Varchar => "text".to_string(),
            CqlType::Varint => "varint".to_string(),
            CqlType::Timeuuid => "timeuuid".to_string(),
            CqlType::Inet => "inet".to_string(),
            CqlType::Date => "date".to_string(),
            CqlType::Time => "time".to_string(),
            CqlType::Smallint => "smallint".to_string(),
            CqlType::Tinyint => "tinyint".to_string(),
            CqlType::Duration => "duration".to_string(),
            CqlType::Empty => "empty".to_string(),
            CqlType::List(inner, frozen) => {
                if *frozen {
                    format!("frozen<list<{}>>", inner.cql_name())
                } else {
                    format!("list<{}>", inner.cql_name())
                }
            }
            CqlType::Set(inner, frozen) => {
                if *frozen {
                    format!("frozen<set<{}>>", inner.cql_name())
                } else {
                    format!("set<{}>", inner.cql_name())
                }
            }
            CqlType::Map(key, value, frozen) => {
                if *frozen {
                    format!("frozen<map<{}, {}>>", key.cql_name(), value.cql_name())
                } else {
                    format!("map<{}, {}>", key.cql_name(), value.cql_name())
                }
            }
            CqlType::Tuple(types) => {
                let inner: Vec<_> = types.iter().map(|t| t.cql_name()).collect();
                format!("tuple<{}>", inner.join(", "))
            }
            CqlType::Udt { keyspace, name, .. } => {
                format!("{}.{}", keyspace, name)
            }
            CqlType::Reversed(inner) => inner.cql_name(),
            CqlType::Vector(inner, dims) => {
                format!("vector<{}, {}>", inner.cql_name(), dims)
            }
        }
    }

    /// Returns `true` if this is a collection type (list, set, map).
    pub fn is_collection(&self) -> bool {
        matches!(
            self,
            CqlType::List(..) | CqlType::Set(..) | CqlType::Map(..)
        )
    }

    /// Returns `true` for multi-cell (non-frozen) complex types.
    pub fn is_multi_cell(&self) -> bool {
        match self {
            CqlType::List(_, frozen) | CqlType::Set(_, frozen) | CqlType::Map(_, _, frozen) => {
                !frozen
            }
            CqlType::Udt { is_multi_cell, .. } => *is_multi_cell,
            _ => false,
        }
    }

    /// Returns `true` if this is a counter type.
    pub fn is_counter(&self) -> bool {
        matches!(self, CqlType::Counter)
    }

    /// Returns `true` if this type uses the reversed comparator.
    pub fn is_reversed(&self) -> bool {
        matches!(self, CqlType::Reversed(_))
    }

    /// Unwrap the inner type if this is a `Reversed` wrapper.
    pub fn unwrap_reversed(&self) -> &CqlType {
        match self {
            CqlType::Reversed(inner) => inner,
            other => other,
        }
    }

    /// Returns the fixed serialization size for native types, or `None` for variable-length.
    pub fn fixed_size(&self) -> Option<usize> {
        match self {
            CqlType::Tinyint => Some(1),
            CqlType::Smallint => Some(2),
            CqlType::Int | CqlType::Float | CqlType::Date => Some(4),
            CqlType::Bigint
            | CqlType::Double
            | CqlType::Timestamp
            | CqlType::Counter
            | CqlType::Time => Some(8),
            CqlType::Uuid | CqlType::Timeuuid => Some(16),
            CqlType::Boolean => Some(1),
            CqlType::Empty => Some(0),
            CqlType::Vector(inner, dims) => inner.fixed_size().map(|s| s * (*dims as usize)),
            _ => None,
        }
    }

    /// Parse a CQL type name into a `CqlType`.
    ///
    /// Handles simple type names only in this phase. Complex types
    /// (collections, tuples, UDTs) require a full parser.
    pub fn from_cql_name(name: &str) -> Option<CqlType> {
        match name.to_lowercase().as_str() {
            "ascii" => Some(CqlType::Ascii),
            "bigint" => Some(CqlType::Bigint),
            "blob" => Some(CqlType::Blob),
            "boolean" => Some(CqlType::Boolean),
            "counter" => Some(CqlType::Counter),
            "decimal" => Some(CqlType::Decimal),
            "double" => Some(CqlType::Double),
            "float" => Some(CqlType::Float),
            "int" => Some(CqlType::Int),
            "timestamp" => Some(CqlType::Timestamp),
            "uuid" => Some(CqlType::Uuid),
            "text" | "varchar" => Some(CqlType::Varchar),
            "varint" => Some(CqlType::Varint),
            "timeuuid" => Some(CqlType::Timeuuid),
            "inet" => Some(CqlType::Inet),
            "date" => Some(CqlType::Date),
            "time" => Some(CqlType::Time),
            "smallint" => Some(CqlType::Smallint),
            "tinyint" => Some(CqlType::Tinyint),
            "duration" => Some(CqlType::Duration),
            "empty" => Some(CqlType::Empty),
            _ => None,
        }
    }
}

/// Parse a CQL-syntax type string like `frozen<list<int>>`, `map<text, int>`,
/// or `tuple<int, text>`.
///
/// This handles the CQL3 form as opposed to the Java marshal class name form
/// handled by [`crate::type_parser::parse_type`].
pub fn parse_cql_type(s: &str) -> Option<CqlType> {
    let s = s.trim();
    if s.is_empty() {
        return None;
    }

    // Check for parameterized types
    if let Some(inner) = s.strip_prefix("frozen<").and_then(|r| r.strip_suffix('>')) {
        let inner_type = parse_cql_type(inner)?;
        return Some(freeze_cql(inner_type));
    }

    if let Some(inner) = s.strip_prefix("list<").and_then(|r| r.strip_suffix('>')) {
        let elem = parse_cql_type(inner)?;
        return Some(CqlType::List(Box::new(elem), false));
    }

    if let Some(inner) = s.strip_prefix("set<").and_then(|r| r.strip_suffix('>')) {
        let elem = parse_cql_type(inner)?;
        return Some(CqlType::Set(Box::new(elem), false));
    }

    if let Some(inner) = s.strip_prefix("map<").and_then(|r| r.strip_suffix('>')) {
        // Split on the top-level comma (not inside nested angle brackets)
        let split = split_top_level_comma(inner)?;
        let key = parse_cql_type(split.0.trim())?;
        let value = parse_cql_type(split.1.trim())?;
        return Some(CqlType::Map(Box::new(key), Box::new(value), false));
    }

    if let Some(inner) = s.strip_prefix("tuple<").and_then(|r| r.strip_suffix('>')) {
        let parts = split_all_top_level_commas(inner);
        let types: Option<Vec<CqlType>> = parts.iter().map(|p| parse_cql_type(p.trim())).collect();
        return Some(CqlType::Tuple(types?));
    }

    // Simple scalar type
    CqlType::from_cql_name(s)
}

/// Split a string on the first top-level comma (respecting `<>` nesting).
fn split_top_level_comma(s: &str) -> Option<(&str, &str)> {
    let mut depth = 0;
    for (i, c) in s.char_indices() {
        match c {
            '<' => depth += 1,
            '>' => depth -= 1,
            ',' if depth == 0 => return Some((&s[..i], &s[i + 1..])),
            _ => {}
        }
    }
    None
}

/// Split a string on all top-level commas.
fn split_all_top_level_commas(s: &str) -> Vec<&str> {
    let mut parts = Vec::new();
    let mut depth = 0;
    let mut start = 0;
    for (i, c) in s.char_indices() {
        match c {
            '<' => depth += 1,
            '>' => depth -= 1,
            ',' if depth == 0 => {
                parts.push(&s[start..i]);
                start = i + 1;
            }
            _ => {}
        }
    }
    parts.push(&s[start..]);
    parts
}

fn freeze_cql(ty: CqlType) -> CqlType {
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
        other => other,
    }
}

impl fmt::Display for CqlType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.cql_name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn protocol_ids() {
        assert_eq!(CqlType::Ascii.protocol_id(), Some(0x0001));
        assert_eq!(CqlType::Int.protocol_id(), Some(0x0009));
        assert_eq!(CqlType::Varchar.protocol_id(), Some(0x000D));
        assert_eq!(CqlType::Timestamp.protocol_id(), Some(0x000B));
    }

    #[test]
    fn cql_names() {
        assert_eq!(CqlType::Int.cql_name(), "int");
        assert_eq!(CqlType::Varchar.cql_name(), "text");
        assert_eq!(CqlType::Timeuuid.cql_name(), "timeuuid");
    }

    #[test]
    fn collection_cql_names() {
        let list = CqlType::List(Box::new(CqlType::Int), false);
        assert_eq!(list.cql_name(), "list<int>");

        let frozen_map = CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int), true);
        assert_eq!(frozen_map.cql_name(), "frozen<map<text, int>>");
    }

    #[test]
    fn from_cql_name_round_trip() {
        let names = &[
            "ascii",
            "bigint",
            "blob",
            "boolean",
            "counter",
            "decimal",
            "double",
            "float",
            "int",
            "timestamp",
            "uuid",
            "text",
            "varint",
            "timeuuid",
            "inet",
            "date",
            "time",
            "smallint",
            "tinyint",
            "duration",
        ];
        for name in names {
            let ty = CqlType::from_cql_name(name).unwrap_or_else(|| panic!("missing: {}", name));
            let cql = ty.cql_name();
            // Re-parse should yield the same type
            let ty2 = CqlType::from_cql_name(&cql).unwrap();
            assert_eq!(ty, ty2, "round-trip failed for {}", name);
        }
    }

    #[test]
    fn varchar_text_alias() {
        let t1 = CqlType::from_cql_name("text").unwrap();
        let t2 = CqlType::from_cql_name("varchar").unwrap();
        assert_eq!(t1, t2);
    }

    #[test]
    fn is_collection() {
        assert!(CqlType::List(Box::new(CqlType::Int), false).is_collection());
        assert!(CqlType::Set(Box::new(CqlType::Int), false).is_collection());
        assert!(
            CqlType::Map(Box::new(CqlType::Int), Box::new(CqlType::Varchar), false).is_collection()
        );
        assert!(!CqlType::Int.is_collection());
    }

    #[test]
    fn is_multi_cell() {
        assert!(CqlType::List(Box::new(CqlType::Int), false).is_multi_cell());
        assert!(!CqlType::List(Box::new(CqlType::Int), true).is_multi_cell());
    }

    #[test]
    fn fixed_size() {
        assert_eq!(CqlType::Int.fixed_size(), Some(4));
        assert_eq!(CqlType::Bigint.fixed_size(), Some(8));
        assert_eq!(CqlType::Uuid.fixed_size(), Some(16));
        assert_eq!(CqlType::Varchar.fixed_size(), None);
        assert_eq!(CqlType::Blob.fixed_size(), None);
    }

    #[test]
    fn parse_cql_type_simple() {
        assert_eq!(parse_cql_type("int"), Some(CqlType::Int));
        assert_eq!(parse_cql_type("text"), Some(CqlType::Varchar));
        assert_eq!(parse_cql_type("  bigint  "), Some(CqlType::Bigint));
    }

    #[test]
    fn parse_cql_type_list() {
        assert_eq!(
            parse_cql_type("list<int>"),
            Some(CqlType::List(Box::new(CqlType::Int), false))
        );
    }

    #[test]
    fn parse_cql_type_frozen_list() {
        assert_eq!(
            parse_cql_type("frozen<list<int>>"),
            Some(CqlType::List(Box::new(CqlType::Int), true))
        );
    }

    #[test]
    fn parse_cql_type_map() {
        assert_eq!(
            parse_cql_type("map<text, int>"),
            Some(CqlType::Map(
                Box::new(CqlType::Varchar),
                Box::new(CqlType::Int),
                false
            ))
        );
    }

    #[test]
    fn parse_cql_type_frozen_map_nested() {
        assert_eq!(
            parse_cql_type("frozen<map<text, list<int>>>"),
            Some(CqlType::Map(
                Box::new(CqlType::Varchar),
                Box::new(CqlType::List(Box::new(CqlType::Int), false)),
                true
            ))
        );
    }

    #[test]
    fn parse_cql_type_tuple() {
        assert_eq!(
            parse_cql_type("tuple<int, text>"),
            Some(CqlType::Tuple(vec![CqlType::Int, CqlType::Varchar]))
        );
    }

    #[test]
    fn parse_cql_type_unknown() {
        assert_eq!(parse_cql_type("unknown"), None);
        assert_eq!(parse_cql_type(""), None);
    }

    #[test]
    fn reversed_unwrap() {
        let reversed = CqlType::Reversed(Box::new(CqlType::Bigint));
        assert!(reversed.is_reversed());
        assert_eq!(*reversed.unwrap_reversed(), CqlType::Bigint);
        assert!(!CqlType::Bigint.is_reversed());
        assert_eq!(*CqlType::Bigint.unwrap_reversed(), CqlType::Bigint);
    }
}
