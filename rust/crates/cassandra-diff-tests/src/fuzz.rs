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
//!
//! ## Usage
//!
//! ```rust,ignore
//! use proptest::prelude::*;
//! use cassandra_diff_tests::fuzz;
//!
//! proptest! {
//!     #[test]
//!     fn frame_header_roundtrip(header in fuzz::arb_frame_header()) {
//!         let encoded = fuzz::encode_header(&header);
//!         let decoded = cassandra_diff_tests::comparators::protocol::parse_header(&encoded);
//!         prop_assert_eq!(decoded.unwrap(), header);
//!     }
//! }
//! ```

use crate::comparators::protocol::FrameHeader;

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

// TODO(phase-3): Add proptest strategies when proptest is integrated into test suite:
// - arb_frame_header() -> Strategy<FrameHeader>
// - arb_error_code() -> Strategy<i32>
// - arb_cql_value() -> Strategy<CqlValue>
// - arb_query() -> Strategy<String>
//
// TODO(harry-integration): When the Rust node supports enough CQL, wire
// Harry (ci/harry_simulation.sh) to run against both Java and Rust and
// compare outcomes via this crate's comparators.
//
// TODO(fql-replay): When FQL fixtures become available, add a replay
// function that reads a Chronicle-queue FQL file, replays the queries
// against both nodes, and diffs the results.

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
}
