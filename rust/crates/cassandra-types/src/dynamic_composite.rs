// Licensed under Apache License, Version 2.0.

//! Legacy `DynamicCompositeType` wire format.
//!
//! Dynamic composites extend `CompositeType` by prefixing each component with
//! either a comparator alias (`0x8000 | alias`) or a UTF-8 marshal type name.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.marshal.DynamicCompositeType`

use crate::native::CqlType;
use crate::type_parser::{ParseError, parse_type};
use byteorder::{BigEndian, ByteOrder};
use std::collections::BTreeMap;

/// A single dynamic composite component.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DynamicComponent {
    pub comparator: CqlType,
    pub value: Vec<u8>,
    pub eoc: u8,
}

/// Errors returned when decoding a dynamic composite value.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DynamicParseError {
    TruncatedComparatorHeader,
    TruncatedComparatorName,
    TruncatedValueLength,
    TruncatedValue,
    UnknownAlias(u8),
    InvalidComparatorName,
    InvalidComparatorType(ParseError),
    LengthOverflow,
}

/// Serialize components using Java's `DynamicCompositeType` layout.
pub fn serialize_dynamic_composite(
    components: &[DynamicComponent],
    aliases: &BTreeMap<u8, CqlType>,
) -> Result<Vec<u8>, DynamicParseError> {
    let mut out = Vec::new();
    for (idx, component) in components.iter().enumerate() {
        if let Some(alias) = aliases
            .iter()
            .find_map(|(alias, ty)| (ty == &component.comparator).then_some(*alias))
        {
            let mut header = [0u8; 2];
            BigEndian::write_u16(&mut header, 0x8000 | alias as u16);
            out.extend_from_slice(&header);
        } else {
            let name = java_marshal_name(&component.comparator);
            let name_bytes = name.as_bytes();
            if name_bytes.len() > 0x7FFF {
                return Err(DynamicParseError::LengthOverflow);
            }
            let mut len = [0u8; 2];
            BigEndian::write_u16(&mut len, name_bytes.len() as u16);
            out.extend_from_slice(&len);
            out.extend_from_slice(name_bytes);
        }

        if component.value.len() > 0x7FFF {
            return Err(DynamicParseError::LengthOverflow);
        }
        let mut value_len = [0u8; 2];
        BigEndian::write_u16(&mut value_len, component.value.len() as u16);
        out.extend_from_slice(&value_len);
        out.extend_from_slice(&component.value);
        out.push(if idx + 1 == components.len() {
            component.eoc
        } else {
            0
        });
    }
    Ok(out)
}

/// Deserialize Java `DynamicCompositeType` bytes into components.
pub fn deserialize_dynamic_composite(
    data: &[u8],
    aliases: &BTreeMap<u8, CqlType>,
) -> Result<Vec<DynamicComponent>, DynamicParseError> {
    let mut pos = 0usize;
    let mut components = Vec::new();
    while pos < data.len() {
        if pos + 2 > data.len() {
            return Err(DynamicParseError::TruncatedComparatorHeader);
        }
        let header = BigEndian::read_u16(&data[pos..pos + 2]);
        pos += 2;

        let comparator = if (header & 0x8000) != 0 {
            let alias = (header & 0x00FF) as u8;
            aliases
                .get(&alias)
                .cloned()
                .ok_or(DynamicParseError::UnknownAlias(alias))?
        } else {
            let name_len = header as usize;
            if pos + name_len > data.len() {
                return Err(DynamicParseError::TruncatedComparatorName);
            }
            let name = std::str::from_utf8(&data[pos..pos + name_len])
                .map_err(|_| DynamicParseError::InvalidComparatorName)?;
            pos += name_len;
            parse_type(name).map_err(DynamicParseError::InvalidComparatorType)?
        };

        if pos + 2 > data.len() {
            return Err(DynamicParseError::TruncatedValueLength);
        }
        let value_len = BigEndian::read_u16(&data[pos..pos + 2]) as usize;
        pos += 2;
        if pos + value_len + 1 > data.len() {
            return Err(DynamicParseError::TruncatedValue);
        }
        let value = data[pos..pos + value_len].to_vec();
        pos += value_len;
        let eoc = data[pos];
        pos += 1;
        components.push(DynamicComponent {
            comparator,
            value,
            eoc,
        });
    }
    Ok(components)
}

pub(crate) fn java_marshal_name(cql_type: &CqlType) -> String {
    match cql_type {
        CqlType::Ascii => "AsciiType".to_string(),
        CqlType::Bigint => "LongType".to_string(),
        CqlType::Blob => "BytesType".to_string(),
        CqlType::Boolean => "BooleanType".to_string(),
        CqlType::Counter => "CounterColumnType".to_string(),
        CqlType::Decimal => "DecimalType".to_string(),
        CqlType::Double => "DoubleType".to_string(),
        CqlType::Float => "FloatType".to_string(),
        CqlType::Int => "Int32Type".to_string(),
        CqlType::Timestamp => "TimestampType".to_string(),
        CqlType::Uuid => "UUIDType".to_string(),
        CqlType::Varchar => "UTF8Type".to_string(),
        CqlType::Varint => "IntegerType".to_string(),
        CqlType::Timeuuid => "TimeUUIDType".to_string(),
        CqlType::Inet => "InetAddressType".to_string(),
        CqlType::Date => "SimpleDateType".to_string(),
        CqlType::Time => "TimeType".to_string(),
        CqlType::Smallint => "ShortType".to_string(),
        CqlType::Tinyint => "ByteType".to_string(),
        CqlType::Duration => "DurationType".to_string(),
        CqlType::Empty => "EmptyType".to_string(),
        CqlType::List(inner, _) => format!("ListType({})", java_marshal_name(inner)),
        CqlType::Set(inner, _) => format!("SetType({})", java_marshal_name(inner)),
        CqlType::Map(key, value, _) => {
            format!(
                "MapType({},{})",
                java_marshal_name(key),
                java_marshal_name(value)
            )
        }
        CqlType::Tuple(types) => render_parameterized("TupleType", types),
        CqlType::Composite(types) => render_parameterized("CompositeType", types),
        CqlType::DynamicComposite(aliases) => {
            let params = aliases
                .iter()
                .map(|(alias, ty)| format!("{}=>{}", *alias as char, java_marshal_name(ty)))
                .collect::<Vec<_>>()
                .join(",");
            format!("DynamicCompositeType({})", params)
        }
        CqlType::Udt {
            keyspace,
            name,
            field_names,
            field_types,
            ..
        } => {
            let mut params = vec![keyspace.clone(), hex_encode(name.as_bytes())];
            for (field_name, field_type) in field_names.iter().zip(field_types.iter()) {
                params.push(format!(
                    "{}:{}",
                    hex_encode(field_name.as_bytes()),
                    java_marshal_name(field_type)
                ));
            }
            format!("UserType({})", params.join(","))
        }
        CqlType::Reversed(inner) => format!("ReversedType({})", java_marshal_name(inner)),
        CqlType::Vector(inner, dimensions) => {
            format!("VectorType({},{})", java_marshal_name(inner), dimensions)
        }
    }
}

fn render_parameterized(name: &str, types: &[CqlType]) -> String {
    let inner = types
        .iter()
        .map(java_marshal_name)
        .collect::<Vec<_>>()
        .join(",");
    format!("{}({})", name, inner)
}

fn hex_encode(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut out = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0F) as usize] as char);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::composite::EOC_END;

    #[test]
    fn aliased_and_named_components_round_trip() {
        let aliases = BTreeMap::from([(b'b', CqlType::Blob), (b't', CqlType::Timeuuid)]);
        let components = vec![
            DynamicComponent {
                comparator: CqlType::Blob,
                value: b"test1".to_vec(),
                eoc: 0,
            },
            DynamicComponent {
                comparator: CqlType::Varint,
                value: vec![42],
                eoc: EOC_END,
            },
        ];

        let bytes = serialize_dynamic_composite(&components, &aliases).unwrap();
        assert_eq!(&bytes[0..2], &[0x80, b'b']);
        assert_eq!(&bytes[10..23], b"\0\x0bIntegerType");

        let decoded = deserialize_dynamic_composite(&bytes, &aliases).unwrap();
        assert_eq!(decoded, components);
    }

    #[test]
    fn rejects_unknown_alias() {
        let bytes = [0x80, b'x', 0x00, 0x00, 0x00];
        assert_eq!(
            deserialize_dynamic_composite(&bytes, &BTreeMap::new()),
            Err(DynamicParseError::UnknownAlias(b'x'))
        );
    }
}
