// Licensed under Apache License, Version 2.0.

//! Type-aware term-to-bytes binding.
//!
//! Converts CQL AST terms to their wire-format bytes using the target column's
//! CqlType to choose the correct encoding width and format.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cql3.Term.prepare`
//! - `org.apache.cassandra.cql3.Term.Terminal.get`
//! - `org.apache.cassandra.cql3.Constants`
//! - `org.apache.cassandra.cql3.Lists`, `Maps`, `Sets`

use byteorder::{BigEndian, ByteOrder};
use cassandra_cql::ast::{Literal, Term};
use cassandra_types::CqlType;

/// Convert a [`Term`] to bytes using the target `CqlType` to select encoding.
///
/// Returns `None` for bind markers (resolved at execute time) and unsupported terms.
///
/// Equivalent to `Term.Terminal.get(ProtocolVersion)` dispatched by type.
pub fn typed_term_to_bytes(term: &Term, target: &CqlType) -> Option<Vec<u8>> {
    match term {
        Term::Literal(lit) => typed_literal_to_bytes(lit, target),
        Term::BindMarker(_) => None,
        Term::FunctionCall(_, _) => None,
        Term::TypeHint(type_hint, inner) => type_hint
            .resolve()
            .and_then(|hinted_type| typed_term_to_bytes(inner, &hinted_type))
            .or_else(|| typed_term_to_bytes(inner, target)),
        Term::CollectionElement { .. } => None,
        Term::CollectionLiteral(elems) => collection_literal_to_bytes(elems, target),
        Term::MapLiteral(pairs) => {
            map_literal_to_bytes(pairs, target).or_else(|| udt_map_literal_to_bytes(pairs, target))
        }
        Term::TupleLiteral(elems) => tuple_literal_to_bytes(elems, target)
            .or_else(|| udt_tuple_literal_to_bytes(elems, target)),
    }
}

fn typed_literal_to_bytes(lit: &Literal, target: &CqlType) -> Option<Vec<u8>> {
    match (lit, target) {
        // Null is always null
        (Literal::Null, _) => None,

        // Boolean
        (Literal::Boolean(b), CqlType::Boolean) => Some(vec![if *b { 1 } else { 0 }]),

        // Integer literals — choose width based on target type
        (Literal::Integer(n), CqlType::Tinyint) => Some(vec![*n as i8 as u8]),
        (Literal::Integer(n), CqlType::Smallint) => {
            let mut b = vec![0u8; 2];
            BigEndian::write_i16(&mut b, *n as i16);
            Some(b)
        }
        (Literal::Integer(n), CqlType::Int) => {
            let mut b = vec![0u8; 4];
            BigEndian::write_i32(&mut b, *n as i32);
            Some(b)
        }
        (
            Literal::Integer(n),
            CqlType::Bigint | CqlType::Counter | CqlType::Timestamp | CqlType::Time,
        ) => {
            let mut b = vec![0u8; 8];
            BigEndian::write_i64(&mut b, *n);
            Some(b)
        }
        (Literal::Integer(n), CqlType::Varint) => {
            // Encode as big-endian two's complement (minimal bytes)
            Some(encode_varint(*n))
        }
        (Literal::Integer(n), CqlType::Float) => {
            let mut b = vec![0u8; 4];
            BigEndian::write_f32(&mut b, *n as f32);
            Some(b)
        }
        (Literal::Integer(n), CqlType::Double) => {
            let mut b = vec![0u8; 8];
            BigEndian::write_f64(&mut b, *n as f64);
            Some(b)
        }
        (Literal::Integer(n), CqlType::Date) => {
            // CQL date literal as days offset from epoch
            let mut b = vec![0u8; 4];
            BigEndian::write_u32(&mut b, *n as u32);
            Some(b)
        }
        // Default integer encoding (fallback for unrecognized target)
        (Literal::Integer(n), _) => {
            let mut b = vec![0u8; 8];
            BigEndian::write_i64(&mut b, *n);
            Some(b)
        }

        // Float literals
        (Literal::Float(f), CqlType::Float) => {
            let mut b = vec![0u8; 4];
            BigEndian::write_f32(&mut b, *f as f32);
            Some(b)
        }
        (Literal::Float(f), CqlType::Double | CqlType::Decimal) => {
            let mut b = vec![0u8; 8];
            BigEndian::write_f64(&mut b, *f);
            Some(b)
        }
        (Literal::Float(f), _) => {
            let mut b = vec![0u8; 8];
            BigEndian::write_f64(&mut b, *f);
            Some(b)
        }

        // String literals — validate encoding based on target
        (Literal::String(s), CqlType::Ascii) => {
            if s.is_ascii() {
                Some(s.as_bytes().to_vec())
            } else {
                None // invalid: non-ASCII in ASCII column
            }
        }
        (Literal::String(s), CqlType::Varchar | CqlType::Blob) => Some(s.as_bytes().to_vec()),
        (Literal::String(s), _) => Some(s.as_bytes().to_vec()),

        // UUID
        (Literal::Uuid(s), CqlType::Uuid | CqlType::Timeuuid) => parse_uuid_bytes(s),
        (Literal::Uuid(s), _) => parse_uuid_bytes(s),

        // Blob
        (Literal::Blob(bytes), _) => Some(bytes.clone()),

        // Boolean coerced to non-boolean target (treat as 1-byte integer)
        (Literal::Boolean(b), _) => Some(vec![if *b { 1 } else { 0 }]),
    }
}

fn collection_literal_to_bytes(elems: &[Term], target: &CqlType) -> Option<Vec<u8>> {
    match target {
        CqlType::List(inner, _) | CqlType::Set(inner, _) => {
            let mut buf = vec![0u8; 4];
            BigEndian::write_i32(&mut buf, elems.len() as i32);
            for elem in elems {
                let bytes = typed_term_to_bytes(elem, inner)?;
                let mut lb = [0u8; 4];
                BigEndian::write_i32(&mut lb, bytes.len() as i32);
                buf.extend_from_slice(&lb);
                buf.extend_from_slice(&bytes);
            }
            Some(buf)
        }
        CqlType::Vector(inner, dimensions) => {
            if elems.len() != *dimensions as usize {
                return None;
            }
            let Some(element_size) = inner.fixed_size() else {
                return None;
            };
            let mut buf = Vec::with_capacity(element_size * elems.len());
            for elem in elems {
                let bytes = typed_term_to_bytes(elem, inner)?;
                if bytes.len() != element_size {
                    return None;
                }
                buf.extend_from_slice(&bytes);
            }
            Some(buf)
        }
        _ => None,
    }
}

fn map_literal_to_bytes(pairs: &[(Term, Term)], target: &CqlType) -> Option<Vec<u8>> {
    if let CqlType::Map(key_type, value_type, _) = target {
        let mut buf = vec![0u8; 4];
        BigEndian::write_i32(&mut buf, pairs.len() as i32);
        for (k, v) in pairs {
            let kb = typed_term_to_bytes(k, key_type)?;
            let vb = typed_term_to_bytes(v, value_type)?;
            let mut lb = [0u8; 4];
            BigEndian::write_i32(&mut lb, kb.len() as i32);
            buf.extend_from_slice(&lb);
            buf.extend_from_slice(&kb);
            BigEndian::write_i32(&mut lb, vb.len() as i32);
            buf.extend_from_slice(&lb);
            buf.extend_from_slice(&vb);
        }
        Some(buf)
    } else {
        None
    }
}

fn tuple_literal_to_bytes(elems: &[Term], target: &CqlType) -> Option<Vec<u8>> {
    if let CqlType::Tuple(field_types) = target {
        if elems.len() != field_types.len() {
            return None;
        }
        let mut buf = Vec::new();
        for (elem, ftype) in elems.iter().zip(field_types.iter()) {
            match typed_term_to_bytes(elem, ftype) {
                Some(bytes) => {
                    let mut lb = [0u8; 4];
                    BigEndian::write_i32(&mut lb, bytes.len() as i32);
                    buf.extend_from_slice(&lb);
                    buf.extend_from_slice(&bytes);
                }
                None => {
                    // null field: write -1
                    let mut lb = [0u8; 4];
                    BigEndian::write_i32(&mut lb, -1i32);
                    buf.extend_from_slice(&lb);
                }
            }
        }
        Some(buf)
    } else {
        None
    }
}

fn udt_map_literal_to_bytes(pairs: &[(Term, Term)], target: &CqlType) -> Option<Vec<u8>> {
    let CqlType::Udt {
        field_names,
        field_types,
        ..
    } = target
    else {
        return None;
    };

    let mut buf = Vec::new();
    for (field_name, field_type) in field_names.iter().zip(field_types.iter()) {
        let value = pairs.iter().find_map(|(key, value)| {
            literal_field_name(key)
                .filter(|name| name == field_name)
                .map(|_| value)
        });
        write_udt_field(&mut buf, value, field_type);
    }
    Some(buf)
}

fn udt_tuple_literal_to_bytes(elems: &[Term], target: &CqlType) -> Option<Vec<u8>> {
    let CqlType::Udt { field_types, .. } = target else {
        return None;
    };

    let mut buf = Vec::new();
    for (idx, field_type) in field_types.iter().enumerate() {
        write_udt_field(&mut buf, elems.get(idx), field_type);
    }
    Some(buf)
}

fn literal_field_name(term: &Term) -> Option<&str> {
    match term {
        Term::Literal(Literal::String(name)) => Some(name.as_str()),
        _ => None,
    }
}

fn write_udt_field(buf: &mut Vec<u8>, term: Option<&Term>, field_type: &CqlType) {
    match term.and_then(|term| typed_term_to_bytes(term, field_type)) {
        Some(bytes) => {
            let mut lb = [0u8; 4];
            BigEndian::write_i32(&mut lb, bytes.len() as i32);
            buf.extend_from_slice(&lb);
            buf.extend_from_slice(&bytes);
        }
        None => {
            let mut lb = [0u8; 4];
            BigEndian::write_i32(&mut lb, -1i32);
            buf.extend_from_slice(&lb);
        }
    }
}

/// Encode a signed i64 as a minimal big-endian two's complement byte array.
fn encode_varint(n: i64) -> Vec<u8> {
    if n == 0 {
        return vec![0];
    }
    let be = n.to_be_bytes();
    // Find the first byte that is not a sign extension
    let sign = if n < 0 { 0xFF } else { 0x00 };
    let sign_ext = if n < 0 { 0x80 } else { 0x00 };
    let mut start = 0usize;
    while start < 7 && be[start] == sign && (be[start + 1] & 0x80) == sign_ext {
        start += 1;
    }
    be[start..].to_vec()
}

fn parse_uuid_bytes(s: &str) -> Option<Vec<u8>> {
    let hex: String = s.chars().filter(|c| *c != '-').collect();
    if hex.len() != 32 {
        return None;
    }
    (0..hex.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&hex[i..i + 2], 16).ok())
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn integer_to_tinyint() {
        let bytes = typed_term_to_bytes(&Term::Literal(Literal::Integer(42)), &CqlType::Tinyint);
        assert_eq!(bytes, Some(vec![42u8]));
    }

    #[test]
    fn integer_to_smallint() {
        let bytes = typed_term_to_bytes(&Term::Literal(Literal::Integer(1000)), &CqlType::Smallint);
        assert_eq!(bytes, Some(vec![0x03, 0xE8]));
    }

    #[test]
    fn integer_to_int() {
        let bytes = typed_term_to_bytes(&Term::Literal(Literal::Integer(42)), &CqlType::Int);
        assert_eq!(bytes, Some(vec![0x00, 0x00, 0x00, 0x2A]));
    }

    #[test]
    fn integer_to_bigint() {
        let bytes = typed_term_to_bytes(&Term::Literal(Literal::Integer(100)), &CqlType::Bigint);
        assert_eq!(bytes.unwrap().len(), 8);
    }

    #[test]
    fn string_ascii_valid() {
        let bytes = typed_term_to_bytes(
            &Term::Literal(Literal::String("hello".into())),
            &CqlType::Ascii,
        );
        assert_eq!(bytes, Some(b"hello".to_vec()));
    }

    #[test]
    fn string_non_ascii_rejected() {
        let bytes = typed_term_to_bytes(
            &Term::Literal(Literal::String("héllo".into())),
            &CqlType::Ascii,
        );
        assert_eq!(bytes, None);
    }

    #[test]
    fn type_hint_controls_literal_encoding() {
        let term = Term::TypeHint(
            cassandra_cql::ast::CqlTypeName::Simple("bigint".to_string()),
            Box::new(Term::Literal(Literal::Integer(7))),
        );

        assert_eq!(
            typed_term_to_bytes(&term, &CqlType::Int),
            Some(7i64.to_be_bytes().to_vec())
        );
    }

    #[test]
    fn null_returns_none() {
        let bytes = typed_term_to_bytes(&Term::Literal(Literal::Null), &CqlType::Int);
        assert_eq!(bytes, None);
    }

    #[test]
    fn bind_marker_returns_none() {
        use cassandra_cql::ast::BindMarker;
        let bytes = typed_term_to_bytes(&Term::BindMarker(BindMarker::Anonymous), &CqlType::Int);
        assert_eq!(bytes, None);
    }

    #[test]
    fn collection_literal_rejects_unresolved_nested_terms() {
        use cassandra_cql::ast::BindMarker;

        let target = CqlType::List(Box::new(CqlType::Int), false);
        let term = Term::CollectionLiteral(vec![
            Term::Literal(Literal::Integer(1)),
            Term::BindMarker(BindMarker::Anonymous),
        ]);

        assert_eq!(typed_term_to_bytes(&term, &target), None);
    }

    #[test]
    fn map_literal_rejects_unresolved_nested_terms() {
        use cassandra_cql::ast::BindMarker;

        let target = CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int), false);
        let term = Term::MapLiteral(vec![(
            Term::Literal(Literal::String("a".to_string())),
            Term::BindMarker(BindMarker::Anonymous),
        )]);

        assert_eq!(typed_term_to_bytes(&term, &target), None);
    }

    #[test]
    fn tuple_literal_rejects_arity_mismatch() {
        let target = CqlType::Tuple(vec![CqlType::Int, CqlType::Int]);
        let too_short = Term::TupleLiteral(vec![Term::Literal(Literal::Integer(10))]);
        let too_long = Term::TupleLiteral(vec![
            Term::Literal(Literal::Integer(10)),
            Term::Literal(Literal::Integer(20)),
            Term::Literal(Literal::Integer(30)),
        ]);

        assert_eq!(typed_term_to_bytes(&too_short, &target), None);
        assert_eq!(typed_term_to_bytes(&too_long, &target), None);
    }

    #[test]
    fn udt_map_literal_serializes_in_field_order() {
        let target = CqlType::Udt {
            keyspace: "ks".to_string(),
            name: "address".to_string(),
            field_names: vec!["street".to_string(), "zip".to_string(), "city".to_string()],
            field_types: vec![CqlType::Varchar, CqlType::Int, CqlType::Varchar],
            is_multi_cell: false,
        };
        let term = Term::MapLiteral(vec![
            (
                Term::Literal(Literal::String("zip".to_string())),
                Term::Literal(Literal::Integer(90210)),
            ),
            (
                Term::Literal(Literal::String("street".to_string())),
                Term::Literal(Literal::String("Main".to_string())),
            ),
        ]);

        let bytes = typed_term_to_bytes(&term, &target).unwrap();
        assert_eq!(BigEndian::read_i32(&bytes[0..4]), 4);
        assert_eq!(&bytes[4..8], b"Main");
        assert_eq!(BigEndian::read_i32(&bytes[8..12]), 4);
        assert_eq!(BigEndian::read_i32(&bytes[12..16]), 90210);
        assert_eq!(BigEndian::read_i32(&bytes[16..20]), -1);
    }

    #[test]
    fn udt_tuple_literal_serializes_by_position() {
        let target = CqlType::Udt {
            keyspace: "ks".to_string(),
            name: "point".to_string(),
            field_names: vec!["x".to_string(), "y".to_string()],
            field_types: vec![CqlType::Int, CqlType::Int],
            is_multi_cell: false,
        };
        let term = Term::TupleLiteral(vec![
            Term::Literal(Literal::Integer(10)),
            Term::Literal(Literal::Integer(20)),
        ]);

        let bytes = typed_term_to_bytes(&term, &target).unwrap();
        assert_eq!(BigEndian::read_i32(&bytes[0..4]), 4);
        assert_eq!(BigEndian::read_i32(&bytes[4..8]), 10);
        assert_eq!(BigEndian::read_i32(&bytes[8..12]), 4);
        assert_eq!(BigEndian::read_i32(&bytes[12..16]), 20);
    }

    #[test]
    fn vector_literal_serializes_as_fixed_width_elements() {
        let target = CqlType::Vector(Box::new(CqlType::Float), 3);
        let term = Term::CollectionLiteral(vec![
            Term::Literal(Literal::Float(1.0)),
            Term::Literal(Literal::Integer(-2)),
            Term::Literal(Literal::Float(3.25)),
        ]);

        let bytes = typed_term_to_bytes(&term, &target).unwrap();
        let expected = [1.0_f32, -2.0, 3.25]
            .into_iter()
            .flat_map(f32::to_be_bytes)
            .collect::<Vec<_>>();
        assert_eq!(bytes, expected);
    }

    #[test]
    fn vector_literal_rejects_wrong_dimension() {
        let target = CqlType::Vector(Box::new(CqlType::Float), 3);
        let term = Term::CollectionLiteral(vec![
            Term::Literal(Literal::Float(1.0)),
            Term::Literal(Literal::Float(2.0)),
        ]);

        assert_eq!(typed_term_to_bytes(&term, &target), None);
    }
}
