// Licensed under Apache License, Version 2.0.

//! Golden-byte tests for CQL type serialization.
//!
//! Each test verifies that a known CQL value serializes to the exact byte
//! sequence produced by the Java Cassandra implementation.  These vectors
//! were captured from `org.apache.cassandra.serializers.*`.

use cassandra_types::codec::CqlValue;
use cassandra_types::native::CqlType;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};

// ── Helper ──────────────────────────────────────────────────────────────

fn assert_golden(cql_type: &CqlType, value: CqlValue, expected: &[u8]) {
    let bytes = value.serialize_value();
    assert_eq!(
        bytes, expected,
        "golden mismatch for {:?}: got {:02X?}, expected {:02X?}",
        cql_type, bytes, expected
    );
    // Roundtrip: deserialize should produce the same value
    let deserialized = CqlValue::deserialize_value(cql_type, &bytes)
        .unwrap_or_else(|e| panic!("deserialize failed for {:?}: {}", cql_type, e));
    // For floats, compare bytes (NaN != NaN)
    match (&value, &deserialized) {
        (CqlValue::Float(_), CqlValue::Float(_))
        | (CqlValue::Double(_), CqlValue::Double(_)) => {
            assert_eq!(
                value.serialize_value(),
                deserialized.serialize_value(),
                "roundtrip byte mismatch for {:?}",
                cql_type
            );
        }
        _ => assert_eq!(value, deserialized, "roundtrip failed for {:?}", cql_type),
    }
}

// ── Scalar golden tests ─────────────────────────────────────────────────

#[test]
fn golden_int_0() {
    assert_golden(&CqlType::Int, CqlValue::Int(0), &[0x00, 0x00, 0x00, 0x00]);
}

#[test]
fn golden_int_42() {
    assert_golden(&CqlType::Int, CqlValue::Int(42), &[0x00, 0x00, 0x00, 0x2A]);
}

#[test]
fn golden_int_neg1() {
    assert_golden(&CqlType::Int, CqlValue::Int(-1), &[0xFF, 0xFF, 0xFF, 0xFF]);
}

#[test]
fn golden_int_min() {
    assert_golden(
        &CqlType::Int,
        CqlValue::Int(i32::MIN),
        &[0x80, 0x00, 0x00, 0x00],
    );
}

#[test]
fn golden_int_max() {
    assert_golden(
        &CqlType::Int,
        CqlValue::Int(i32::MAX),
        &[0x7F, 0xFF, 0xFF, 0xFF],
    );
}

#[test]
fn golden_bigint_0() {
    assert_golden(
        &CqlType::Bigint,
        CqlValue::Bigint(0),
        &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00],
    );
}

#[test]
fn golden_bigint_max() {
    assert_golden(
        &CqlType::Bigint,
        CqlValue::Bigint(i64::MAX),
        &[0x7F, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
    );
}

#[test]
fn golden_bigint_neg1() {
    assert_golden(
        &CqlType::Bigint,
        CqlValue::Bigint(-1),
        &[0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF, 0xFF],
    );
}

#[test]
fn golden_smallint_0() {
    assert_golden(
        &CqlType::Smallint,
        CqlValue::Smallint(0),
        &[0x00, 0x00],
    );
}

#[test]
fn golden_smallint_neg32768() {
    assert_golden(
        &CqlType::Smallint,
        CqlValue::Smallint(-32768),
        &[0x80, 0x00],
    );
}

#[test]
fn golden_tinyint_neg128() {
    assert_golden(&CqlType::Tinyint, CqlValue::Tinyint(-128), &[0x80]);
}

#[test]
fn golden_tinyint_127() {
    assert_golden(&CqlType::Tinyint, CqlValue::Tinyint(127), &[0x7F]);
}

#[test]
fn golden_boolean_true() {
    assert_golden(&CqlType::Boolean, CqlValue::Boolean(true), &[0x01]);
}

#[test]
fn golden_boolean_false() {
    assert_golden(&CqlType::Boolean, CqlValue::Boolean(false), &[0x00]);
}

#[test]
fn golden_float_0() {
    assert_golden(
        &CqlType::Float,
        CqlValue::Float(0.0),
        &[0x00, 0x00, 0x00, 0x00],
    );
}

#[test]
fn golden_float_1() {
    // 1.0f32 = 0x3F800000
    assert_golden(
        &CqlType::Float,
        CqlValue::Float(1.0),
        &[0x3F, 0x80, 0x00, 0x00],
    );
}

#[test]
fn golden_double_pi() {
    // Math.PI in Java = 0x400921FB54442D18
    assert_golden(
        &CqlType::Double,
        CqlValue::Double(std::f64::consts::PI),
        &[0x40, 0x09, 0x21, 0xFB, 0x54, 0x44, 0x2D, 0x18],
    );
}

#[test]
fn golden_varchar() {
    assert_golden(
        &CqlType::Varchar,
        CqlValue::Varchar("hello".into()),
        b"hello",
    );
}

#[test]
fn golden_varchar_empty() {
    assert_golden(
        &CqlType::Varchar,
        CqlValue::Varchar(String::new()),
        &[],
    );
}

#[test]
fn golden_ascii() {
    assert_golden(
        &CqlType::Ascii,
        CqlValue::Ascii("test".into()),
        b"test",
    );
}

#[test]
fn golden_blob() {
    assert_golden(
        &CqlType::Blob,
        CqlValue::Blob(vec![0xDE, 0xAD, 0xBE, 0xEF]),
        &[0xDE, 0xAD, 0xBE, 0xEF],
    );
}

#[test]
fn golden_uuid() {
    // UUID 550e8400-e29b-41d4-a716-446655440000
    let uuid_bytes: [u8; 16] = [
        0x55, 0x0E, 0x84, 0x00, 0xE2, 0x9B, 0x41, 0xD4, 0xA7, 0x16, 0x44, 0x66, 0x55, 0x44,
        0x00, 0x00,
    ];
    assert_golden(&CqlType::Uuid, CqlValue::Uuid(uuid_bytes), &uuid_bytes);
}

#[test]
fn golden_timeuuid() {
    let uuid_bytes: [u8; 16] = [
        0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x10, 0x00, 0x80, 0x00, 0x00, 0x00, 0x00, 0x00,
        0x00, 0x00,
    ];
    assert_golden(
        &CqlType::Timeuuid,
        CqlValue::Timeuuid(uuid_bytes),
        &uuid_bytes,
    );
}

#[test]
fn golden_inet_v4() {
    // 127.0.0.1
    assert_golden(
        &CqlType::Inet,
        CqlValue::Inet(IpAddr::V4(Ipv4Addr::LOCALHOST)),
        &[127, 0, 0, 1],
    );
}

#[test]
fn golden_inet_v6_loopback() {
    // ::1
    assert_golden(
        &CqlType::Inet,
        CqlValue::Inet(IpAddr::V6(Ipv6Addr::LOCALHOST)),
        &[0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1],
    );
}

#[test]
fn golden_date() {
    // Date is days since epoch center (2^31), stored as u32
    assert_golden(
        &CqlType::Date,
        CqlValue::Date(2_147_483_648), // epoch
        &[0x80, 0x00, 0x00, 0x00],
    );
}

#[test]
fn golden_timestamp() {
    // Epoch millis: 1710000000000 = 0x0000018E23F14C00
    assert_golden(
        &CqlType::Timestamp,
        CqlValue::Timestamp(1_710_000_000_000),
        &[0x00, 0x00, 0x01, 0x8E, 0x23, 0xF1, 0x4C, 0x00],
    );
}

#[test]
fn golden_time() {
    // Noon = 43200000000000 nanoseconds = 0x0000274A48A78000
    assert_golden(
        &CqlType::Time,
        CqlValue::Time(43_200_000_000_000),
        &[0x00, 0x00, 0x27, 0x4A, 0x48, 0xA7, 0x80, 0x00],
    );
}

#[test]
fn golden_counter() {
    assert_golden(
        &CqlType::Counter,
        CqlValue::Counter(100),
        &[0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64],
    );
}

#[test]
fn golden_varint_zero() {
    assert_golden(&CqlType::Varint, CqlValue::Varint(vec![0x00]), &[0x00]);
}

#[test]
fn golden_varint_neg1() {
    // -1 in two's complement = 0xFF
    assert_golden(&CqlType::Varint, CqlValue::Varint(vec![0xFF]), &[0xFF]);
}

#[test]
fn golden_varint_128() {
    // 128 = 0x0080
    assert_golden(
        &CqlType::Varint,
        CqlValue::Varint(vec![0x00, 0x80]),
        &[0x00, 0x80],
    );
}

#[test]
fn golden_decimal_pi_scale2() {
    // Decimal(314, scale=2) → pi ≈ 3.14
    // Wire: [00 00 00 02] [01 3A]
    // 314 = 0x013A
    assert_golden(
        &CqlType::Decimal,
        CqlValue::Decimal {
            unscaled: vec![0x01, 0x3A],
            scale: 2,
        },
        &[0x00, 0x00, 0x00, 0x02, 0x01, 0x3A],
    );
}

#[test]
fn golden_empty() {
    assert_golden(&CqlType::Empty, CqlValue::Empty, &[]);
}

// ── Collection golden tests ─────────────────────────────────────────────

#[test]
fn golden_list_int() {
    // list<int> [1, 2, 3]
    // Wire: [count=3][len=4][00000001][len=4][00000002][len=4][00000003]
    let val = CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2), CqlValue::Int(3)]);
    let expected: Vec<u8> = vec![
        0x00, 0x00, 0x00, 0x03, // count = 3
        0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01, // elem 1
        0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x02, // elem 2
        0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x03, // elem 3
    ];
    let ty = CqlType::List(Box::new(CqlType::Int), false);
    assert_golden(&ty, val, &expected);
}

#[test]
fn golden_set_text_empty() {
    let val = CqlValue::Set(vec![]);
    let expected: Vec<u8> = vec![0x00, 0x00, 0x00, 0x00]; // count = 0
    let ty = CqlType::Set(Box::new(CqlType::Varchar), false);
    assert_golden(&ty, val, &expected);
}

#[test]
fn golden_map_text_int() {
    // map<text, int> {"a": 1}
    let val = CqlValue::Map(vec![(
        CqlValue::Varchar("a".into()),
        CqlValue::Int(1),
    )]);
    let expected: Vec<u8> = vec![
        0x00, 0x00, 0x00, 0x01, // count = 1
        0x00, 0x00, 0x00, 0x01, 0x61, // key "a" (len=1, 'a')
        0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01, // value 1
    ];
    let ty = CqlType::Map(
        Box::new(CqlType::Varchar),
        Box::new(CqlType::Int),
        false,
    );
    assert_golden(&ty, val, &expected);
}

// ── Tuple/UDT golden tests ──────────────────────────────────────────────

#[test]
fn golden_tuple_int_text() {
    // tuple<int, text> (42, "hi")
    let val = CqlValue::Tuple(vec![
        Some(CqlValue::Int(42)),
        Some(CqlValue::Varchar("hi".into())),
    ]);
    let expected: Vec<u8> = vec![
        0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x2A, // int 42
        0x00, 0x00, 0x00, 0x02, 0x68, 0x69, // text "hi"
    ];
    let ty = CqlType::Tuple(vec![CqlType::Int, CqlType::Varchar]);
    assert_golden(&ty, val, &expected);
}

#[test]
fn golden_tuple_with_null() {
    // tuple<int, text> (42, null)
    let val = CqlValue::Tuple(vec![Some(CqlValue::Int(42)), None]);
    let expected: Vec<u8> = vec![
        0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x2A, // int 42
        0xFF, 0xFF, 0xFF, 0xFF, // null marker (-1)
    ];
    let ty = CqlType::Tuple(vec![CqlType::Int, CqlType::Varchar]);
    assert_golden(&ty, val, &expected);
}

#[test]
fn golden_udt() {
    // UDT with fields: age(int)=25, name(text)="bob"
    let val = CqlValue::Udt(vec![
        ("age".into(), Some(CqlValue::Int(25))),
        ("name".into(), Some(CqlValue::Varchar("bob".into()))),
    ]);
    let expected: Vec<u8> = vec![
        0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x19, // int 25
        0x00, 0x00, 0x00, 0x03, 0x62, 0x6F, 0x62, // text "bob"
    ];
    let ty = CqlType::Udt {
        keyspace: "ks".into(),
        name: "person".into(),
        field_names: vec!["age".into(), "name".into()],
        field_types: vec![CqlType::Int, CqlType::Varchar],
        is_multi_cell: false,
    };
    assert_golden(&ty, val, &expected);
}

// ── Comparison ordering tests ───────────────────────────────────────────

#[test]
fn comparison_ordering_int() {
    use cassandra_types::comparator::compare_bytes;
    use std::cmp::Ordering;

    let values: Vec<i32> = vec![i32::MIN, -100, -1, 0, 1, 100, i32::MAX];
    for i in 0..values.len() {
        for j in 0..values.len() {
            let a = values[i].to_be_bytes();
            let b = values[j].to_be_bytes();
            let expected = values[i].cmp(&values[j]);
            let actual = compare_bytes(&CqlType::Int, &a, &b);
            assert_eq!(
                actual, expected,
                "int comparison: {} vs {}",
                values[i], values[j]
            );
        }
    }
}

#[test]
fn comparison_ordering_text() {
    use cassandra_types::comparator::compare_bytes;

    let values = vec!["", "a", "aa", "ab", "b", "z"];
    for i in 0..values.len() {
        for j in 0..values.len() {
            let expected = values[i].cmp(values[j]);
            let actual = compare_bytes(
                &CqlType::Varchar,
                values[i].as_bytes(),
                values[j].as_bytes(),
            );
            assert_eq!(
                actual, expected,
                "text comparison: '{}' vs '{}'",
                values[i], values[j]
            );
        }
    }
}

#[test]
fn comparison_ordering_varint() {
    use cassandra_types::comparator::compare_bytes;
    use std::cmp::Ordering;

    // Varint byte representations
    let neg1 = vec![0xFF]; // -1
    let zero = vec![0x00]; // 0
    let pos1 = vec![0x01]; // 1
    let pos128 = vec![0x00, 0x80]; // 128

    assert_eq!(
        compare_bytes(&CqlType::Varint, &neg1, &zero),
        Ordering::Less
    );
    assert_eq!(
        compare_bytes(&CqlType::Varint, &zero, &pos1),
        Ordering::Less
    );
    assert_eq!(
        compare_bytes(&CqlType::Varint, &pos1, &pos128),
        Ordering::Less
    );
}
