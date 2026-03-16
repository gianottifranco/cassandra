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

//! Fuzz and property-based testing strategies.
//!
//! Provides proptest strategies for generating random CQL protocol frames,
//! CQL values, and error codes. These are used for property-based testing
//! of protocol codecs and type serialization roundtrips.

use crate::comparators::protocol::FrameHeader;

use proptest::prelude::*;

/// Encode a frame header to bytes.
pub fn encode_header(header: &FrameHeader) -> Vec<u8> {
    let mut buf = Vec::with_capacity(9);
    buf.push(header.version);
    buf.push(header.flags);
    buf.extend_from_slice(&header.stream_id.to_be_bytes());
    buf.push(header.opcode);
    buf.extend_from_slice(&header.body_length.to_be_bytes());
    buf
}

/// Build an error frame body from an error code and message.
pub fn build_error_body(error_code: i32, message: &str) -> Vec<u8> {
    let msg_bytes = message.as_bytes();
    let mut body = Vec::with_capacity(4 + 2 + msg_bytes.len());
    body.extend_from_slice(&error_code.to_be_bytes());
    body.extend_from_slice(&(msg_bytes.len() as u16).to_be_bytes());
    body.extend_from_slice(msg_bytes);
    body
}

/// Serialize a CQL [int] value (4 bytes, big-endian).
pub fn serialize_cql_int(value: i32) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

/// Serialize a CQL [bigint] value (8 bytes, big-endian).
pub fn serialize_cql_bigint(value: i64) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

/// Serialize a CQL [smallint] value (2 bytes, big-endian).
pub fn serialize_cql_smallint(value: i16) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

/// Serialize a CQL [tinyint] value (1 byte).
pub fn serialize_cql_tinyint(value: i8) -> Vec<u8> {
    vec![value as u8]
}

/// Serialize a CQL [boolean] value (1 byte).
pub fn serialize_cql_boolean(value: bool) -> Vec<u8> {
    vec![if value { 0x01 } else { 0x00 }]
}

/// Serialize a CQL [float] value (4 bytes, IEEE 754 big-endian).
pub fn serialize_cql_float(value: f32) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

/// Serialize a CQL [double] value (8 bytes, IEEE 754 big-endian).
pub fn serialize_cql_double(value: f64) -> Vec<u8> {
    value.to_be_bytes().to_vec()
}

/// Serialize a CQL [text/varchar] value (UTF-8 bytes, no length prefix in value).
pub fn serialize_cql_text(value: &str) -> Vec<u8> {
    value.as_bytes().to_vec()
}

/// Deserialize a CQL [int] value from 4 big-endian bytes.
pub fn deserialize_cql_int(data: &[u8]) -> Option<i32> {
    if data.len() != 4 {
        return None;
    }
    Some(i32::from_be_bytes([data[0], data[1], data[2], data[3]]))
}

/// Deserialize a CQL [bigint] value from 8 big-endian bytes.
pub fn deserialize_cql_bigint(data: &[u8]) -> Option<i64> {
    if data.len() != 8 {
        return None;
    }
    Some(i64::from_be_bytes(data.try_into().ok()?))
}

/// Deserialize a CQL [smallint] value from 2 big-endian bytes.
pub fn deserialize_cql_smallint(data: &[u8]) -> Option<i16> {
    if data.len() != 2 {
        return None;
    }
    Some(i16::from_be_bytes([data[0], data[1]]))
}

/// Deserialize a CQL [tinyint] value from 1 byte.
pub fn deserialize_cql_tinyint(data: &[u8]) -> Option<i8> {
    if data.len() != 1 {
        return None;
    }
    Some(data[0] as i8)
}

/// Deserialize a CQL [boolean] value from 1 byte.
pub fn deserialize_cql_boolean(data: &[u8]) -> Option<bool> {
    if data.len() != 1 {
        return None;
    }
    Some(data[0] != 0)
}

/// Deserialize a CQL [float] value from 4 big-endian bytes.
pub fn deserialize_cql_float(data: &[u8]) -> Option<f32> {
    if data.len() != 4 {
        return None;
    }
    Some(f32::from_be_bytes([data[0], data[1], data[2], data[3]]))
}

/// Deserialize a CQL [double] value from 8 big-endian bytes.
pub fn deserialize_cql_double(data: &[u8]) -> Option<f64> {
    if data.len() != 8 {
        return None;
    }
    Some(f64::from_be_bytes(data.try_into().ok()?))
}

// ── Proptest strategies ────────────────────────────────────────────────

/// CQL typed value for property-based testing.
#[derive(Debug, Clone, PartialEq)]
pub enum CqlTestValue {
    Int(i32),
    Bigint(i64),
    Smallint(i16),
    Tinyint(i8),
    Boolean(bool),
    Float(f32),
    Double(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl CqlTestValue {
    /// Serialize to CQL wire format bytes.
    pub fn serialize(&self) -> Vec<u8> {
        match self {
            CqlTestValue::Int(v) => serialize_cql_int(*v),
            CqlTestValue::Bigint(v) => serialize_cql_bigint(*v),
            CqlTestValue::Smallint(v) => serialize_cql_smallint(*v),
            CqlTestValue::Tinyint(v) => serialize_cql_tinyint(*v),
            CqlTestValue::Boolean(v) => serialize_cql_boolean(*v),
            CqlTestValue::Float(v) => serialize_cql_float(*v),
            CqlTestValue::Double(v) => serialize_cql_double(*v),
            CqlTestValue::Text(v) => serialize_cql_text(v),
            CqlTestValue::Blob(v) => v.clone(),
        }
    }
}

// ── Proptest strategies (behind #[cfg(test)]) ──────────────────────────

pub fn arb_frame_header() -> impl Strategy<Value = FrameHeader> {
    (
        prop_oneof![Just(0x04u8), Just(0x84u8), Just(0x05u8), Just(0x85u8)], // version
        0u8..=0x0F,                                                          // flags (4 bits used)
        any::<i16>(),                                                        // stream_id
        0u8..=0x10,                                                          // opcode (valid range)
        0u32..=1_000_000,                                                    // body_length
    )
        .prop_map(
            |(version, flags, stream_id, opcode, body_length)| FrameHeader {
                version,
                flags,
                stream_id,
                opcode,
                body_length,
            },
        )
}

pub fn arb_error_code() -> impl Strategy<Value = i32> {
    prop_oneof![
        // Known Cassandra error codes
        Just(0x0000i32), // Server error
        Just(0x000A),    // Protocol error
        Just(0x0100),    // Bad credentials
        Just(0x1000),    // Unavailable
        Just(0x1001),    // Overloaded
        Just(0x1002),    // Is bootstrapping
        Just(0x1003),    // Truncation error
        Just(0x1100),    // Write timeout
        Just(0x1200),    // Read timeout
        Just(0x1300),    // Read failure
        Just(0x1400),    // Function failure
        Just(0x1500),    // Write failure
        Just(0x2000),    // Syntax error
        Just(0x2100),    // Unauthorized
        Just(0x2200),    // Invalid
        Just(0x2300),    // Config error
        Just(0x2400),    // Already exists
        Just(0x2500),    // Unprepared
        // Out-of-range codes for robustness testing
        any::<i32>(),
    ]
}

pub fn arb_cql_value() -> impl Strategy<Value = CqlTestValue> {
    prop_oneof![
        any::<i32>().prop_map(CqlTestValue::Int),
        any::<i64>().prop_map(CqlTestValue::Bigint),
        any::<i16>().prop_map(CqlTestValue::Smallint),
        any::<i8>().prop_map(CqlTestValue::Tinyint),
        any::<bool>().prop_map(CqlTestValue::Boolean),
        // Use finite floats to avoid NaN comparison issues
        (-1e10f32..1e10f32).prop_map(CqlTestValue::Float),
        (-1e20f64..1e20f64).prop_map(CqlTestValue::Double),
        "[a-zA-Z0-9 ]{0,256}".prop_map(CqlTestValue::Text),
        proptest::collection::vec(any::<u8>(), 0..512).prop_map(CqlTestValue::Blob),
    ]
}

/// Strategy for generating a random mutation row for the storage engine.
pub fn arb_mutation() -> impl Strategy<Value = (String, String, Vec<u8>, Vec<u8>, i64)> {
    (
        "[a-z]{1,8}",                                   // keyspace
        "[a-z]{1,8}",                                   // table
        proptest::collection::vec(any::<u8>(), 1..64),  // partition key
        proptest::collection::vec(any::<u8>(), 1..256), // value
        0i64..1_000_000_000,                            // timestamp
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::comparators::protocol;

    #[test]
    fn header_roundtrip() {
        let header = FrameHeader {
            version: 0x84,
            flags: 0x00,
            stream_id: 42,
            opcode: 0x08,
            body_length: 256,
        };
        let encoded = encode_header(&header);
        let decoded = protocol::parse_header(&encoded).unwrap();
        assert_eq!(header, decoded);
    }

    #[test]
    fn error_body_roundtrip() {
        let code = crate::comparators::error::error_codes::SYNTAX_ERROR;
        let message = "no viable alternative at input 'SELCT'";
        let body = build_error_body(code, message);
        let (parsed_code, parsed_msg) = crate::comparators::error::parse_error_body(&body).unwrap();
        assert_eq!(parsed_code, code);
        assert_eq!(parsed_msg, message);
    }

    #[test]
    fn cql_int_serialization() {
        assert_eq!(serialize_cql_int(42), vec![0x00, 0x00, 0x00, 0x2a]);
        assert_eq!(serialize_cql_int(-1), vec![0xff, 0xff, 0xff, 0xff]);
        assert_eq!(serialize_cql_int(0), vec![0x00, 0x00, 0x00, 0x00]);
    }

    #[test]
    fn cql_bigint_serialization() {
        assert_eq!(
            serialize_cql_bigint(i64::MAX),
            vec![0x7f, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff]
        );
    }

    #[test]
    fn cql_boolean_serialization() {
        assert_eq!(serialize_cql_boolean(true), vec![0x01]);
        assert_eq!(serialize_cql_boolean(false), vec![0x00]);
    }

    #[test]
    fn cql_text_serialization() {
        assert_eq!(serialize_cql_text("hello"), b"hello".to_vec());
        assert_eq!(serialize_cql_text(""), Vec::<u8>::new());
    }

    #[test]
    fn cql_float_serialization() {
        let bytes = serialize_cql_float(2.71f32);
        // IEEE 754 big-endian for 2.71
        assert_eq!(bytes.len(), 4);
        let reconstructed = f32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
        assert!((reconstructed - 2.71f32).abs() < f32::EPSILON);
    }

    #[test]
    fn golden_type_serialization_matches() {
        // Cross-check our serialization functions against the golden reference
        let reference: serde_json::Value =
            crate::golden::load_json("types/serialization_reference.json")
                .expect("Failed to load type reference");
        let types = reference.get("types").unwrap();

        // int: 42 -> 0000002a
        let int_ref = types.get("int").unwrap();
        let expected_hex = int_ref.get("serialized_hex").unwrap().as_str().unwrap();
        assert_eq!(hex::encode(serialize_cql_int(42)), expected_hex);

        // boolean true -> 01
        let bool_ref = types.get("boolean_true").unwrap();
        let expected_hex = bool_ref.get("serialized_hex").unwrap().as_str().unwrap();
        assert_eq!(hex::encode(serialize_cql_boolean(true)), expected_hex);

        // boolean false -> 00
        let bool_ref = types.get("boolean_false").unwrap();
        let expected_hex = bool_ref.get("serialized_hex").unwrap().as_str().unwrap();
        assert_eq!(hex::encode(serialize_cql_boolean(false)), expected_hex);

        // tinyint 127 -> 7f
        let tiny_ref = types.get("tinyint").unwrap();
        let expected_hex = tiny_ref.get("serialized_hex").unwrap().as_str().unwrap();
        assert_eq!(hex::encode(serialize_cql_tinyint(127)), expected_hex);

        // smallint 32000 -> 7d00
        let small_ref = types.get("smallint").unwrap();
        let expected_hex = small_ref.get("serialized_hex").unwrap().as_str().unwrap();
        assert_eq!(hex::encode(serialize_cql_smallint(32000)), expected_hex);

        // bigint max -> 7fffffffffffffff
        let big_ref = types.get("bigint").unwrap();
        let expected_hex = big_ref.get("serialized_hex").unwrap().as_str().unwrap();
        assert_eq!(
            hex::encode(serialize_cql_bigint(9223372036854775807)),
            expected_hex
        );

        // text "hello" -> 68656c6c6f (from ascii entry)
        let ascii_ref = types.get("ascii").unwrap();
        let expected_hex = ascii_ref.get("serialized_hex").unwrap().as_str().unwrap();
        assert_eq!(hex::encode(serialize_cql_text("hello")), expected_hex);

        // int -1 -> ffffffff
        let neg_ref = types.get("int_negative").unwrap();
        let expected_hex = neg_ref.get("serialized_hex").unwrap().as_str().unwrap();
        assert_eq!(hex::encode(serialize_cql_int(-1)), expected_hex);

        // int 0 -> 00000000
        let zero_ref = types.get("int_zero").unwrap();
        let expected_hex = zero_ref.get("serialized_hex").unwrap().as_str().unwrap();
        assert_eq!(hex::encode(serialize_cql_int(0)), expected_hex);

        // empty text -> ""
        let empty_ref = types.get("empty_text").unwrap();
        let expected_hex = empty_ref.get("serialized_hex").unwrap().as_str().unwrap();
        assert_eq!(hex::encode(serialize_cql_text("")), expected_hex);
    }

    // ── Proptest roundtrip tests ───────────────────────────────────────

    proptest! {
        #[test]
        fn prop_int_roundtrip(v in any::<i32>()) {
            let bytes = serialize_cql_int(v);
            let decoded = deserialize_cql_int(&bytes).unwrap();
            prop_assert_eq!(v, decoded);
        }

        #[test]
        fn prop_bigint_roundtrip(v in any::<i64>()) {
            let bytes = serialize_cql_bigint(v);
            let decoded = deserialize_cql_bigint(&bytes).unwrap();
            prop_assert_eq!(v, decoded);
        }

        #[test]
        fn prop_smallint_roundtrip(v in any::<i16>()) {
            let bytes = serialize_cql_smallint(v);
            let decoded = deserialize_cql_smallint(&bytes).unwrap();
            prop_assert_eq!(v, decoded);
        }

        #[test]
        fn prop_tinyint_roundtrip(v in any::<i8>()) {
            let bytes = serialize_cql_tinyint(v);
            let decoded = deserialize_cql_tinyint(&bytes).unwrap();
            prop_assert_eq!(v, decoded);
        }

        #[test]
        fn prop_boolean_roundtrip(v in any::<bool>()) {
            let bytes = serialize_cql_boolean(v);
            let decoded = deserialize_cql_boolean(&bytes).unwrap();
            prop_assert_eq!(v, decoded);
        }
    }
}
