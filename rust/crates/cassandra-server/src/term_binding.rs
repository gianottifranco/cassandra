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
        Term::TypeHint(_, inner) => typed_term_to_bytes(inner, target),
        Term::CollectionLiteral(elems) => collection_literal_to_bytes(elems, target),
        Term::MapLiteral(pairs) => map_literal_to_bytes(pairs, target),
        Term::TupleLiteral(elems) => tuple_literal_to_bytes(elems, target),
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
                let bytes = typed_term_to_bytes(elem, inner).unwrap_or_default();
                let mut lb = [0u8; 4];
                BigEndian::write_i32(&mut lb, bytes.len() as i32);
                buf.extend_from_slice(&lb);
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
            let kb = typed_term_to_bytes(k, key_type).unwrap_or_default();
            let vb = typed_term_to_bytes(v, value_type).unwrap_or_default();
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
}
