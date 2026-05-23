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

//! Type system differential tests: round-trip, ordering, and golden-byte tests.
//!
//! Each test verifies that our Rust implementation produces byte-for-byte output
//! matching the Java oracle (`org.apache.cassandra.serializers.*`,
//! `org.apache.cassandra.db.marshal.*`).

#![allow(dead_code, unused_imports)]

use cassandra_types::{
    CqlType, CqlValue,
    comparator::compare_bytes,
    type_compat::{AssignmentResult, is_compatible_with, test_assignment},
    type_parser::parse_type,
    vint::{decode_vint, encode_vint},
};
use std::cmp::Ordering;
use std::net::{IpAddr, Ipv4Addr};

// ── Helper ───────────────────────────────────────────────────────────────────

fn roundtrip(ty: &CqlType, val: CqlValue) {
    let bytes = val.serialize_value();
    let decoded = CqlValue::deserialize_value(ty, &bytes)
        .unwrap_or_else(|e| panic!("deserialize failed for {:?}: {}", ty, e));
    // Compare via re-serialization (avoids float NaN equality issues)
    assert_eq!(
        val.serialize_value(),
        decoded.serialize_value(),
        "roundtrip mismatch for {:?}",
        ty
    );
}

// ── Round-trip tests for every CqlType variant ───────────────────────────────

#[test]
fn rt_ascii() {
    roundtrip(&CqlType::Ascii, CqlValue::Ascii("hello".into()));
}

#[test]
fn rt_bigint() {
    roundtrip(&CqlType::Bigint, CqlValue::Bigint(i64::MAX));
    roundtrip(&CqlType::Bigint, CqlValue::Bigint(i64::MIN));
    roundtrip(&CqlType::Bigint, CqlValue::Bigint(0));
}

#[test]
fn rt_blob() {
    roundtrip(&CqlType::Blob, CqlValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]));
    roundtrip(&CqlType::Blob, CqlValue::Blob(vec![]));
}

#[test]
fn rt_boolean() {
    roundtrip(&CqlType::Boolean, CqlValue::Boolean(true));
    roundtrip(&CqlType::Boolean, CqlValue::Boolean(false));
}

#[test]
fn rt_double() {
    roundtrip(&CqlType::Double, CqlValue::Double(std::f64::consts::PI));
    roundtrip(&CqlType::Double, CqlValue::Double(f64::INFINITY));
    roundtrip(&CqlType::Double, CqlValue::Double(0.0));
}

#[test]
fn rt_float() {
    roundtrip(&CqlType::Float, CqlValue::Float(std::f32::consts::E));
    roundtrip(&CqlType::Float, CqlValue::Float(0.0));
}

#[test]
fn rt_int() {
    roundtrip(&CqlType::Int, CqlValue::Int(42));
    roundtrip(&CqlType::Int, CqlValue::Int(-1));
    roundtrip(&CqlType::Int, CqlValue::Int(i32::MIN));
}

#[test]
fn rt_smallint() {
    roundtrip(&CqlType::Smallint, CqlValue::Smallint(-32768));
    roundtrip(&CqlType::Smallint, CqlValue::Smallint(32767));
}

#[test]
fn rt_tinyint() {
    roundtrip(&CqlType::Tinyint, CqlValue::Tinyint(-128));
    roundtrip(&CqlType::Tinyint, CqlValue::Tinyint(127));
}

#[test]
fn rt_varchar() {
    roundtrip(&CqlType::Varchar, CqlValue::Varchar("hello world".into()));
    roundtrip(&CqlType::Varchar, CqlValue::Varchar("".into()));
    roundtrip(&CqlType::Varchar, CqlValue::Varchar("日本語".into()));
}

#[test]
fn rt_uuid() {
    roundtrip(&CqlType::Uuid, CqlValue::Uuid([0xAB; 16]));
}

#[test]
fn rt_timeuuid() {
    roundtrip(&CqlType::Timeuuid, CqlValue::Timeuuid([0x01; 16]));
}

#[test]
fn rt_timestamp() {
    roundtrip(&CqlType::Timestamp, CqlValue::Timestamp(1_710_000_000_000));
}

#[test]
fn rt_date() {
    roundtrip(&CqlType::Date, CqlValue::Date(19000));
}

#[test]
fn rt_time() {
    roundtrip(&CqlType::Time, CqlValue::Time(86_400_000_000_000));
}

#[test]
fn rt_inet_v4() {
    roundtrip(
        &CqlType::Inet,
        CqlValue::Inet(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))),
    );
}

#[test]
fn rt_varint() {
    roundtrip(&CqlType::Varint, CqlValue::Varint(vec![0x01, 0x00]));
    roundtrip(&CqlType::Varint, CqlValue::Varint(vec![0x00]));
}

#[test]
fn rt_decimal() {
    roundtrip(
        &CqlType::Decimal,
        CqlValue::Decimal {
            scale: 2,
            unscaled: vec![0x01, 0x86, 0xA0], // 100000
        },
    );
}

#[test]
fn rt_duration() {
    roundtrip(
        &CqlType::Duration,
        CqlValue::Duration {
            months: 12,
            days: 30,
            nanoseconds: 86_400_000_000_000,
        },
    );
    roundtrip(
        &CqlType::Duration,
        CqlValue::Duration {
            months: 0,
            days: 0,
            nanoseconds: 0,
        },
    );
    roundtrip(
        &CqlType::Duration,
        CqlValue::Duration {
            months: -1,
            days: -1,
            nanoseconds: -1,
        },
    );
}

#[test]
fn rt_list() {
    let ty = CqlType::List(Box::new(CqlType::Int), false);
    roundtrip(
        &ty,
        CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2), CqlValue::Int(3)]),
    );
    roundtrip(&ty, CqlValue::List(vec![]));
}

#[test]
fn rt_set() {
    let ty = CqlType::Set(Box::new(CqlType::Varchar), false);
    roundtrip(
        &ty,
        CqlValue::Set(vec![
            CqlValue::Varchar("a".into()),
            CqlValue::Varchar("b".into()),
        ]),
    );
}

#[test]
fn rt_map() {
    let ty = CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int), false);
    roundtrip(
        &ty,
        CqlValue::Map(vec![(CqlValue::Varchar("key".into()), CqlValue::Int(42))]),
    );
}

#[test]
fn rt_tuple() {
    let ty = CqlType::Tuple(vec![CqlType::Int, CqlType::Varchar]);
    roundtrip(
        &ty,
        CqlValue::Tuple(vec![
            Some(CqlValue::Int(1)),
            Some(CqlValue::Varchar("x".into())),
        ]),
    );
    // With null field
    roundtrip(&ty, CqlValue::Tuple(vec![Some(CqlValue::Int(1)), None]));
}

#[test]
fn rt_empty() {
    roundtrip(&CqlType::Empty, CqlValue::Empty);
}

// ── Ordering tests ───────────────────────────────────────────────────────────

#[test]
fn ordering_int() {
    let a = CqlValue::Int(-1).serialize_value();
    let b = CqlValue::Int(0).serialize_value();
    let c = CqlValue::Int(1).serialize_value();
    assert_eq!(compare_bytes(&CqlType::Int, &a, &b), Ordering::Less);
    assert_eq!(compare_bytes(&CqlType::Int, &b, &c), Ordering::Less);
    assert_eq!(compare_bytes(&CqlType::Int, &c, &a), Ordering::Greater);
    assert_eq!(compare_bytes(&CqlType::Int, &a, &a), Ordering::Equal);
}

#[test]
fn ordering_bigint_neg() {
    let neg = CqlValue::Bigint(-1).serialize_value();
    let zero = CqlValue::Bigint(0).serialize_value();
    assert_eq!(compare_bytes(&CqlType::Bigint, &neg, &zero), Ordering::Less);
}

#[test]
fn ordering_text_lexicographic() {
    let a = b"apple".to_vec();
    let b = b"banana".to_vec();
    assert_eq!(compare_bytes(&CqlType::Varchar, &a, &b), Ordering::Less);
    assert_eq!(compare_bytes(&CqlType::Varchar, &b, &a), Ordering::Greater);
}

#[test]
fn ordering_reversed() {
    let rev = CqlType::Reversed(Box::new(CqlType::Int));
    let small = CqlValue::Int(1).serialize_value();
    let large = CqlValue::Int(100).serialize_value();
    // Reversed: larger value comes first
    assert_eq!(compare_bytes(&rev, &large, &small), Ordering::Less);
}

#[test]
fn ordering_list() {
    let ty = CqlType::List(Box::new(CqlType::Int), false);
    let a = CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2)]).serialize_value();
    let b = CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(3)]).serialize_value();
    assert_eq!(compare_bytes(&ty, &a, &b), Ordering::Less);
}

#[test]
fn ordering_tuple() {
    let ty = CqlType::Tuple(vec![CqlType::Int, CqlType::Varchar]);
    let a = CqlValue::Tuple(vec![
        Some(CqlValue::Int(1)),
        Some(CqlValue::Varchar("a".into())),
    ])
    .serialize_value();
    let b = CqlValue::Tuple(vec![
        Some(CqlValue::Int(1)),
        Some(CqlValue::Varchar("b".into())),
    ])
    .serialize_value();
    assert_eq!(compare_bytes(&ty, &a, &b), Ordering::Less);
}

// ── Golden byte tests (matching Java oracle output) ──────────────────────────

#[test]
fn golden_int_42() {
    // Java: Int32Type.instance.decompose(42) = 0x0000002A
    let bytes = CqlValue::Int(42).serialize_value();
    assert_eq!(bytes, vec![0x00, 0x00, 0x00, 0x2A]);
}

#[test]
fn golden_bigint_max() {
    // Java: LongType.instance.decompose(Long.MAX_VALUE)
    let bytes = CqlValue::Bigint(i64::MAX).serialize_value();
    assert_eq!(bytes, vec![0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF]);
}

#[test]
fn golden_boolean_true() {
    assert_eq!(CqlValue::Boolean(true).serialize_value(), vec![0x01]);
}

#[test]
fn golden_boolean_false() {
    assert_eq!(CqlValue::Boolean(false).serialize_value(), vec![0x00]);
}

#[test]
fn golden_tinyint_neg() {
    // Java: ByteType.instance.decompose(-1) = 0xFF
    assert_eq!(CqlValue::Tinyint(-1).serialize_value(), vec![0xFF]);
}

#[test]
fn golden_duration_zero() {
    // Duration(0, 0, 0) → vint(0), vint(0), vint(0) = [0x00, 0x00, 0x00]
    let bytes = CqlValue::Duration {
        months: 0,
        days: 0,
        nanoseconds: 0,
    }
    .serialize_value();
    assert_eq!(bytes, vec![0x00, 0x00, 0x00]);
}

#[test]
fn golden_duration_one_day() {
    // Duration(0, 1, 0): months=0→[0x00], days=1→zigzag(1)=2→[0x02], nanos=0→[0x00]
    let bytes = CqlValue::Duration {
        months: 0,
        days: 1,
        nanoseconds: 0,
    }
    .serialize_value();
    assert_eq!(bytes, vec![0x00, 0x02, 0x00]);
}

#[test]
fn golden_list_empty() {
    // Java: ListType<Int32Type>.instance.serialize([]) = 0x00000000
    let bytes = CqlValue::List(vec![]).serialize_value();
    assert_eq!(bytes, vec![0x00, 0x00, 0x00, 0x00]);
}

#[test]
fn golden_list_one_int() {
    // Java: ListType<Int32Type>.instance.serialize([42])
    // = 0x00000001 (count) + 0x00000004 (len) + 0x0000002A (value)
    let bytes = CqlValue::List(vec![CqlValue::Int(42)]).serialize_value();
    assert_eq!(
        bytes,
        vec![
            0x00, 0x00, 0x00, 0x01, // count = 1
            0x00, 0x00, 0x00, 0x04, // element length = 4
            0x00, 0x00, 0x00, 0x2A, // value = 42
        ]
    );
}

// ── VInt edge case tests ──────────────────────────────────────────────────────

#[test]
fn vint_zero() {
    assert_eq!(encode_vint(0), vec![0x00]);
    let (v, _) = decode_vint(&[0x00]).unwrap();
    assert_eq!(v, 0);
}

#[test]
fn vint_neg_one() {
    assert_eq!(encode_vint(-1), vec![0x01]);
    let (v, _) = decode_vint(&[0x01]).unwrap();
    assert_eq!(v, -1);
}

#[test]
fn vint_i64_min_max() {
    for &val in &[i64::MIN, i64::MAX, i64::MIN + 1, i64::MAX - 1] {
        let encoded = encode_vint(val);
        let (decoded, consumed) = decode_vint(&encoded).unwrap();
        assert_eq!(decoded, val, "vint roundtrip failed for {}", val);
        assert_eq!(consumed, encoded.len());
    }
}

// ── Type compatibility matrix ─────────────────────────────────────────────────

#[test]
fn compat_matrix_numeric() {
    // Integer widening must hold
    assert!(is_compatible_with(&CqlType::Tinyint, &CqlType::Smallint));
    assert!(is_compatible_with(&CqlType::Tinyint, &CqlType::Int));
    assert!(is_compatible_with(&CqlType::Tinyint, &CqlType::Bigint));
    assert!(is_compatible_with(&CqlType::Smallint, &CqlType::Int));
    assert!(is_compatible_with(&CqlType::Int, &CqlType::Bigint));
    assert!(is_compatible_with(&CqlType::Float, &CqlType::Double));

    // Narrowing must NOT hold
    assert!(!is_compatible_with(&CqlType::Bigint, &CqlType::Int));
    assert!(!is_compatible_with(&CqlType::Double, &CqlType::Float));
    assert!(!is_compatible_with(&CqlType::Int, &CqlType::Tinyint));
}

#[test]
fn compat_matrix_text() {
    assert!(is_compatible_with(&CqlType::Ascii, &CqlType::Varchar));
    assert!(!is_compatible_with(&CqlType::Varchar, &CqlType::Ascii));
    assert!(is_compatible_with(&CqlType::Varchar, &CqlType::Varchar));
}

#[test]
fn compat_matrix_collections() {
    let list_int = CqlType::List(Box::new(CqlType::Int), false);
    let list_bigint = CqlType::List(Box::new(CqlType::Bigint), false);
    let list_int_frozen = CqlType::List(Box::new(CqlType::Int), true);

    // Covariant element type
    assert!(is_compatible_with(&list_int, &list_bigint));
    assert!(!is_compatible_with(&list_bigint, &list_int));
    // Frozen/non-frozen mismatch
    assert!(!is_compatible_with(&list_int, &list_int_frozen));
}

#[test]
fn assignment_exact_and_compatible() {
    assert_eq!(
        test_assignment(&CqlType::Int, &CqlType::Int),
        AssignmentResult::Exact
    );
    assert_eq!(
        test_assignment(&CqlType::Bigint, &CqlType::Int),
        AssignmentResult::Compatible
    );
    assert_eq!(
        test_assignment(&CqlType::Boolean, &CqlType::Int),
        AssignmentResult::NotAssignable
    );
}

// ── TypeParser tests ─────────────────────────────────────────────────────────

#[test]
fn parser_scalar_types() {
    assert_eq!(parse_type("UTF8Type").unwrap(), CqlType::Varchar);
    assert_eq!(parse_type("Int32Type").unwrap(), CqlType::Int);
    assert_eq!(parse_type("LongType").unwrap(), CqlType::Bigint);
    assert_eq!(parse_type("DurationType").unwrap(), CqlType::Duration);
}

#[test]
fn parser_fully_qualified() {
    assert_eq!(
        parse_type("org.apache.cassandra.db.marshal.UTF8Type").unwrap(),
        CqlType::Varchar
    );
}

#[test]
fn parser_list_type() {
    let ty = parse_type("ListType(Int32Type)").unwrap();
    assert_eq!(ty, CqlType::List(Box::new(CqlType::Int), false));
}

#[test]
fn parser_frozen_map() {
    let ty = parse_type("FrozenType(MapType(UTF8Type,Int32Type))").unwrap();
    assert_eq!(
        ty,
        CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int), true)
    );
}

#[test]
fn parser_reversed() {
    let ty = parse_type("ReversedType(LongType)").unwrap();
    assert_eq!(ty, CqlType::Reversed(Box::new(CqlType::Bigint)));
}

// ── Collection serialization conformance ─────────────────────────────────────

#[test]
fn collection_serialization_map() {
    let ty = CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Int), false);
    let val = CqlValue::Map(vec![(CqlValue::Varchar("hello".into()), CqlValue::Int(42))]);
    let bytes = val.serialize_value();
    let decoded = CqlValue::deserialize_value(&ty, &bytes).unwrap();
    assert_eq!(
        decoded,
        CqlValue::Map(vec![(CqlValue::Varchar("hello".into()), CqlValue::Int(42)),])
    );
}

#[test]
fn collection_serialization_nested() {
    // list<list<int>> round-trip (outer list of inner lists)
    let inner_ty = CqlType::List(Box::new(CqlType::Int), true);
    let outer_ty = CqlType::List(Box::new(inner_ty), false);
    let inner = CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2)]);
    let outer = CqlValue::List(vec![inner]);
    let bytes = outer.serialize_value();
    let decoded = CqlValue::deserialize_value(&outer_ty, &bytes).unwrap();
    assert_eq!(decoded, outer);
}
