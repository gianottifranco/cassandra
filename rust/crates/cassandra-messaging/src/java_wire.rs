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

//! Java internode message header codec.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.net.Message.Serializer`
//! - `org.apache.cassandra.net.Verb`
//! - `org.apache.cassandra.utils.vint.VIntCoding`

use crate::{Message, MessageHeader, Verb, frame::MAX_PAYLOAD_SIZE};

/// Header fields carried by Java's internode message serializer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaMessageHeader {
    pub id: u64,
    pub verb: Verb,
    pub created_at_millis_low: i32,
    pub expires_in_millis: u64,
    pub flags: u32,
    pub params_count: u32,
    pub payload_size: u32,
}

/// Opaque Java message parameter in the `Message.Serializer` header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaMessageParam {
    pub param_type: u64,
    pub value: Vec<u8>,
}

/// Raw Java-layout message: decoded header plus opaque payload bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct JavaMessage {
    pub header: JavaMessageHeader,
    pub params: Vec<JavaMessageParam>,
    pub payload: Vec<u8>,
}

#[derive(Debug, thiserror::Error, PartialEq, Eq)]
pub enum JavaWireError {
    #[error("unexpected end of Java internode message")]
    UnexpectedEof,

    #[error("invalid Java internode message: {0}")]
    InvalidData(String),

    #[error("unsupported Java verb id: {0}")]
    UnsupportedVerb(i32),

    #[error("Rust verb {0:?} has no Java wire id mapping")]
    UnsupportedRustVerb(Verb),
}

pub struct JavaMessageCodec;

impl JavaMessageCodec {
    /// Encode an opaque-payload message using Java's header layout.
    ///
    /// This intentionally does not serialize typed Java payload objects. The
    /// caller provides payload bytes that already match the selected verb.
    pub fn encode_raw(message: &Message, expires_in_millis: u64) -> Result<Vec<u8>, JavaWireError> {
        Self::encode_raw_with_params(message, expires_in_millis, &[])
    }

    /// Encode a Java-layout message with opaque header parameters.
    pub fn encode_raw_with_params(
        message: &Message,
        expires_in_millis: u64,
        params: &[JavaMessageParam],
    ) -> Result<Vec<u8>, JavaWireError> {
        if message.payload.len() > MAX_PAYLOAD_SIZE as usize {
            return Err(JavaWireError::InvalidData(format!(
                "payload size exceeds maximum: {} > {}",
                message.payload.len(),
                MAX_PAYLOAD_SIZE
            )));
        }
        let mut out = Vec::new();
        write_unsigned_vint(message.header.message_id, &mut out);
        write_i32(&mut out, message.header.creation_timestamp as i32);
        write_unsigned_vint(expires_in_millis, &mut out);
        write_unsigned_vint(java_verb_id(message.header.verb)? as u64, &mut out);
        write_unsigned_vint(message.header.flags as u64, &mut out);
        write_unsigned_vint(params.len() as u64, &mut out);
        for param in params {
            write_unsigned_vint(param.param_type, &mut out);
            write_unsigned_vint(param.value.len() as u64, &mut out);
            out.extend_from_slice(&param.value);
        }
        write_unsigned_vint(message.payload.len() as u64, &mut out);
        out.extend_from_slice(&message.payload);
        Ok(out)
    }

    /// Decode a Java-layout message while leaving the payload opaque.
    pub fn decode_raw(input: &[u8]) -> Result<JavaMessage, JavaWireError> {
        let mut cursor = input;
        let id = read_unsigned_vint(&mut cursor)?;
        let created_at_millis_low = read_i32(&mut cursor)?;
        let expires_in_millis = read_unsigned_vint(&mut cursor)?;
        let java_verb = read_unsigned_vint(&mut cursor)?;
        let verb = rust_verb_from_java_id(checked_i32(java_verb, "verb id")?)?;
        let flags = checked_u32(read_unsigned_vint(&mut cursor)?, "flags")?;
        let params_count = checked_u32(read_unsigned_vint(&mut cursor)?, "params count")?;
        let params = read_params(&mut cursor, params_count)?;
        let payload_size = checked_u32(read_unsigned_vint(&mut cursor)?, "payload size")?;
        if payload_size > MAX_PAYLOAD_SIZE {
            return Err(JavaWireError::InvalidData(format!(
                "payload size exceeds maximum: {} > {}",
                payload_size, MAX_PAYLOAD_SIZE
            )));
        }
        let payload = read_exact(&mut cursor, payload_size as usize)?.to_vec();
        ensure_consumed(cursor)?;

        Ok(JavaMessage {
            header: JavaMessageHeader {
                id,
                verb,
                created_at_millis_low,
                expires_in_millis,
                flags,
                params_count,
                payload_size,
            },
            params,
            payload,
        })
    }

    pub fn to_message(decoded: JavaMessage) -> Message {
        Message {
            header: MessageHeader {
                verb: decoded.header.verb,
                message_id: decoded.header.id,
                flags: decoded.header.flags,
                creation_timestamp: decoded.header.created_at_millis_low as i64,
                payload_size: decoded.header.payload_size,
            },
            payload: decoded.payload,
            forwarding: None,
            expires_at_nanos: None,
        }
    }
}

pub fn java_verb_id(verb: Verb) -> Result<i32, JavaWireError> {
    match verb {
        Verb::Mutation => Ok(0),
        Verb::MutationResponse => Ok(60),
        Verb::Hint => Ok(1),
        Verb::HintResponse => Ok(61),
        Verb::ReadRepair => Ok(2),
        Verb::ReadRepairResponse => Ok(62),
        Verb::ReadData | Verb::ReadDigest => Ok(3),
        Verb::ReadDataResponse | Verb::ReadDigestResponse => Ok(63),
        Verb::BatchStore => Ok(5),
        Verb::BatchStoreResponse => Ok(65),
        Verb::BatchRemove => Ok(6),
        Verb::GossipDigestSyn => Ok(14),
        Verb::GossipDigestAck => Ok(15),
        Verb::GossipDigestAck2 => Ok(16),
        Verb::SchemaPush => Ok(18),
        Verb::SchemaPull => Ok(28),
        Verb::SchemaResponse => Ok(88),
        Verb::GossipShutdown => Ok(29),
        Verb::Ping => Ok(31),
        Verb::Pong => Ok(91),
        Verb::RequestFailure => Ok(99),
        other => Err(JavaWireError::UnsupportedRustVerb(other)),
    }
}

pub fn rust_verb_from_java_id(id: i32) -> Result<Verb, JavaWireError> {
    match id {
        0 => Ok(Verb::Mutation),
        60 => Ok(Verb::MutationResponse),
        1 => Ok(Verb::Hint),
        61 => Ok(Verb::HintResponse),
        2 => Ok(Verb::ReadRepair),
        62 => Ok(Verb::ReadRepairResponse),
        3 => Ok(Verb::ReadData),
        63 => Ok(Verb::ReadDataResponse),
        5 => Ok(Verb::BatchStore),
        65 => Ok(Verb::BatchStoreResponse),
        6 => Ok(Verb::BatchRemove),
        14 => Ok(Verb::GossipDigestSyn),
        15 => Ok(Verb::GossipDigestAck),
        16 => Ok(Verb::GossipDigestAck2),
        18 => Ok(Verb::SchemaPush),
        28 => Ok(Verb::SchemaPull),
        88 => Ok(Verb::SchemaResponse),
        29 => Ok(Verb::GossipShutdown),
        31 => Ok(Verb::Ping),
        91 => Ok(Verb::Pong),
        99 => Ok(Verb::RequestFailure),
        other => Err(JavaWireError::UnsupportedVerb(other)),
    }
}

pub fn write_unsigned_vint(value: u64, out: &mut Vec<u8>) {
    let size = compute_unsigned_vint_size(value);
    if size == 1 {
        out.push(value as u8);
    } else if size < 9 {
        let shift = (8 - size) * 8;
        let extra_bytes = size - 1;
        let mask = (encode_extra_bytes_to_read(extra_bytes) as u64) << 56;
        let register = (value << shift) | mask;
        out.extend_from_slice(&register.to_be_bytes()[..size]);
    } else {
        out.push(0xff);
        out.extend_from_slice(&value.to_be_bytes());
    }
}

pub fn read_unsigned_vint(input: &mut &[u8]) -> Result<u64, JavaWireError> {
    let first = read_u8(input)?;
    if first & 0x80 == 0 {
        return Ok(first as u64);
    }
    let extra = first.leading_ones() as usize;
    if extra > 8 {
        return Err(JavaWireError::InvalidData(format!(
            "invalid vint lead byte {first:#04x}"
        )));
    }
    let mut value = (first & first_byte_value_mask(extra)) as u64;
    for _ in 0..extra {
        value = (value << 8) | read_u8(input)? as u64;
    }
    Ok(value)
}

pub fn compute_unsigned_vint_size(value: u64) -> usize {
    let magnitude = (value | 1).leading_zeros();
    ((639 - magnitude * 9) >> 6) as usize
}

fn first_byte_value_mask(extra_bytes_to_read: usize) -> u8 {
    0xff >> extra_bytes_to_read
}

fn encode_extra_bytes_to_read(extra_bytes_to_read: usize) -> u8 {
    !first_byte_value_mask(extra_bytes_to_read)
}

fn read_params(input: &mut &[u8], count: u32) -> Result<Vec<JavaMessageParam>, JavaWireError> {
    let mut params = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let param_type = read_unsigned_vint(input)?;
        let length = checked_usize(read_unsigned_vint(input)?, "param length")?;
        let value = read_exact(input, length)?.to_vec();
        params.push(JavaMessageParam { param_type, value });
    }
    Ok(params)
}

fn checked_i32(value: u64, field: &str) -> Result<i32, JavaWireError> {
    i32::try_from(value)
        .map_err(|_| JavaWireError::InvalidData(format!("{field} out of range: {value}")))
}

fn checked_u32(value: u64, field: &str) -> Result<u32, JavaWireError> {
    u32::try_from(value)
        .map_err(|_| JavaWireError::InvalidData(format!("{field} out of range: {value}")))
}

fn checked_usize(value: u64, field: &str) -> Result<usize, JavaWireError> {
    usize::try_from(value)
        .map_err(|_| JavaWireError::InvalidData(format!("{field} out of range: {value}")))
}

fn write_i32(out: &mut Vec<u8>, value: i32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn read_i32(input: &mut &[u8]) -> Result<i32, JavaWireError> {
    let bytes = read_exact(input, 4)?;
    Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn read_u8(input: &mut &[u8]) -> Result<u8, JavaWireError> {
    Ok(*read_exact(input, 1)?
        .first()
        .ok_or(JavaWireError::UnexpectedEof)?)
}

fn read_exact<'a>(input: &mut &'a [u8], len: usize) -> Result<&'a [u8], JavaWireError> {
    if input.len() < len {
        return Err(JavaWireError::UnexpectedEof);
    }
    let (head, tail) = input.split_at(len);
    *input = tail;
    Ok(head)
}

fn ensure_consumed(input: &[u8]) -> Result<(), JavaWireError> {
    if input.is_empty() {
        Ok(())
    } else {
        Err(JavaWireError::InvalidData(format!(
            "{} trailing bytes",
            input.len()
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn unsigned_vint_matches_java_encoding_boundaries() {
        let cases = [
            (0, vec![0x00]),
            (127, vec![0x7f]),
            (128, vec![0x80, 0x80]),
            (16_383, vec![0xbf, 0xff]),
            (16_384, vec![0xc0, 0x40, 0x00]),
        ];

        for (value, expected) in cases {
            let mut bytes = Vec::new();
            write_unsigned_vint(value, &mut bytes);
            assert_eq!(bytes, expected);
            assert_eq!(read_unsigned_vint(&mut bytes.as_slice()).unwrap(), value);
        }
    }

    #[test]
    fn maps_selected_rust_verbs_to_java_ids() {
        assert_eq!(java_verb_id(Verb::GossipDigestSyn).unwrap(), 14);
        assert_eq!(java_verb_id(Verb::GossipDigestAck).unwrap(), 15);
        assert_eq!(java_verb_id(Verb::Ping).unwrap(), 31);
        assert_eq!(rust_verb_from_java_id(91).unwrap(), Verb::Pong);
    }

    #[test]
    fn encodes_and_decodes_java_raw_message_without_params() {
        let mut message = Message::request(Verb::GossipDigestSyn, 42, b"payload".to_vec());
        message.header.creation_timestamp = 0x0102_0304;

        let bytes = JavaMessageCodec::encode_raw(&message, 5000).unwrap();
        assert_eq!(bytes[0], 42);
        assert_eq!(&bytes[1..5], &[1, 2, 3, 4]);
        assert_eq!(bytes[5..7], [0x93, 0x88]);
        assert_eq!(bytes[7], 14);
        assert_eq!(bytes[8], 0);
        assert_eq!(bytes[9], 0);
        assert_eq!(bytes[10], 7);

        let decoded = JavaMessageCodec::decode_raw(&bytes).unwrap();
        assert_eq!(decoded.header.id, 42);
        assert_eq!(decoded.header.verb, Verb::GossipDigestSyn);
        assert_eq!(decoded.header.expires_in_millis, 5000);
        assert_eq!(decoded.payload, b"payload");
    }

    #[test]
    fn skips_unknown_params_by_length() {
        let mut bytes = Vec::new();
        write_unsigned_vint(1, &mut bytes);
        write_i32(&mut bytes, 2);
        write_unsigned_vint(3, &mut bytes);
        write_unsigned_vint(14, &mut bytes);
        write_unsigned_vint(0, &mut bytes);
        write_unsigned_vint(1, &mut bytes);
        write_unsigned_vint(123, &mut bytes);
        write_unsigned_vint(2, &mut bytes);
        bytes.extend_from_slice(&[9, 9]);
        write_unsigned_vint(1, &mut bytes);
        bytes.push(7);

        let decoded = JavaMessageCodec::decode_raw(&bytes).unwrap();
        assert_eq!(decoded.header.params_count, 1);
        assert_eq!(
            decoded.params,
            vec![JavaMessageParam {
                param_type: 123,
                value: vec![9, 9],
            }]
        );
        assert_eq!(decoded.payload, vec![7]);
    }

    #[test]
    fn encodes_and_decodes_raw_message_with_params() {
        let message = Message::request(Verb::Ping, 7, b"ping".to_vec());
        let params = vec![
            JavaMessageParam {
                param_type: 1,
                value: vec![0xaa, 0xbb],
            },
            JavaMessageParam {
                param_type: 130,
                value: b"trace".to_vec(),
            },
        ];

        let bytes = JavaMessageCodec::encode_raw_with_params(&message, 1234, &params).unwrap();
        let decoded = JavaMessageCodec::decode_raw(&bytes).unwrap();

        assert_eq!(decoded.header.id, 7);
        assert_eq!(decoded.header.verb, Verb::Ping);
        assert_eq!(decoded.header.params_count, 2);
        assert_eq!(decoded.params, params);
        assert_eq!(decoded.payload, b"ping");
    }

    #[test]
    fn rejects_oversized_payloads() {
        let message = Message::request(Verb::Ping, 7, vec![0u8; MAX_PAYLOAD_SIZE as usize + 1]);
        assert!(matches!(
            JavaMessageCodec::encode_raw(&message, 1234),
            Err(JavaWireError::InvalidData(msg)) if msg.contains("payload size exceeds maximum")
        ));

        let mut bytes = Vec::new();
        write_unsigned_vint(1, &mut bytes);
        write_i32(&mut bytes, 2);
        write_unsigned_vint(3, &mut bytes);
        write_unsigned_vint(31, &mut bytes);
        write_unsigned_vint(0, &mut bytes);
        write_unsigned_vint(0, &mut bytes);
        write_unsigned_vint(MAX_PAYLOAD_SIZE as u64 + 1, &mut bytes);

        assert!(matches!(
            JavaMessageCodec::decode_raw(&bytes),
            Err(JavaWireError::InvalidData(msg)) if msg.contains("payload size exceeds maximum")
        ));
    }
}
