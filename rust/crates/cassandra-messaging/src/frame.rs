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

//! Internode message framing and codec.
//!
//! Messages are length-prefixed and carry a header with verb, message ID,
//! and flags. The body is opaque bytes (JSON serialized for now; binary
//! codec planned for hot-path verbs).
//!
//! ## Wire format
//!
//! ```text
//! ┌──────────┬──────────┬──────────┬──────────┬──────────┬──────────┐
//! │ length   │ verb_id  │ msg_id   │ flags    │ ts_epoch │ body ... │
//! │ (u32)    │ (i32)    │ (u64)    │ (u32)    │ (i64)    │          │
//! └──────────┴──────────┴──────────┴──────────┴──────────┴──────────┘
//! ```

use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::verb::Verb;

/// Header size in bytes: length(4) + verb(4) + msg_id(8) + flags(4) + timestamp(8) = 28
pub const HEADER_SIZE: usize = 28;

/// Maximum message payload size (64 MiB).
pub const MAX_PAYLOAD_SIZE: u32 = 64 * 1024 * 1024;

/// Header flags.
pub mod flags {
    /// Message is a response to a prior request.
    pub const RESPONSE: u32 = 0x01;
    /// Message is a failure response.
    pub const FAILURE: u32 = 0x02;
    /// Body is compressed (reserved for future use).
    pub const COMPRESSED: u32 = 0x04;
}

/// Internode message header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MessageHeader {
    /// Message verb (type).
    pub verb: Verb,
    /// Unique message ID (for correlating responses).
    pub message_id: u64,
    /// Flags (response, failure, compressed, etc.).
    pub flags: u32,
    /// Creation timestamp (epoch millis on the sender).
    pub creation_timestamp: i64,
    /// Payload size in bytes.
    pub payload_size: u32,
}

/// A complete internode message: header + opaque payload.
#[derive(Debug, Clone)]
pub struct Message {
    pub header: MessageHeader,
    pub payload: Vec<u8>,
}

impl Message {
    /// Create a new request message.
    pub fn request(verb: Verb, message_id: u64, payload: Vec<u8>) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        Self {
            header: MessageHeader {
                verb,
                message_id,
                flags: 0,
                creation_timestamp: now,
                payload_size: payload.len() as u32,
            },
            payload,
        }
    }

    /// Create a response message for the given request.
    pub fn response(request_id: u64, verb: Verb, payload: Vec<u8>) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        Self {
            header: MessageHeader {
                verb,
                message_id: request_id,
                flags: flags::RESPONSE,
                creation_timestamp: now,
                payload_size: payload.len() as u32,
            },
            payload,
        }
    }

    /// Create a failure response.
    pub fn failure(request_id: u64, payload: Vec<u8>) -> Self {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as i64;

        Self {
            header: MessageHeader {
                verb: Verb::RequestFailure,
                message_id: request_id,
                flags: flags::RESPONSE | flags::FAILURE,
                creation_timestamp: now,
                payload_size: payload.len() as u32,
            },
            payload,
        }
    }

    /// Returns `true` if this is a response message.
    pub fn is_response(&self) -> bool {
        self.header.flags & flags::RESPONSE != 0
    }

    /// Returns `true` if this is a failure response.
    pub fn is_failure(&self) -> bool {
        self.header.flags & flags::FAILURE != 0
    }
}

/// Tokio codec for encoding/decoding `Message` on TCP connections.
///
/// Uses length-prefixed framing. Thread-safe for single connection use
/// (one encoder + one decoder per connection half).
#[derive(Debug, Default)]
pub struct MessageCodec;

impl Decoder for MessageCodec {
    type Item = Message;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Need at least the length field
        if src.len() < 4 {
            return Ok(None);
        }

        // Peek at the total length (includes header + body, excluding the length field itself)
        let total_len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;

        if total_len > MAX_PAYLOAD_SIZE as usize + HEADER_SIZE {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Message too large: {total_len} bytes"),
            ));
        }

        // Wait for the full message
        if src.len() < 4 + total_len {
            src.reserve(4 + total_len - src.len());
            return Ok(None);
        }

        // Consume the length prefix
        src.advance(4);

        // Read header fields
        let verb_id = src.get_i32();
        let message_id = src.get_u64();
        let msg_flags = src.get_u32();
        let creation_timestamp = src.get_i64();

        let verb = Verb::from_id(verb_id).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("Unknown verb ID: {verb_id}"),
            )
        })?;

        let payload_size = total_len - (HEADER_SIZE - 4); // subtract header fields we already read
        let payload = src.split_to(payload_size).to_vec();

        Ok(Some(Message {
            header: MessageHeader {
                verb,
                message_id,
                flags: msg_flags,
                creation_timestamp,
                payload_size: payload_size as u32,
            },
            payload,
        }))
    }
}

impl Encoder<Message> for MessageCodec {
    type Error = std::io::Error;

    fn encode(&mut self, msg: Message, dst: &mut BytesMut) -> Result<(), Self::Error> {
        let header_fields_size = HEADER_SIZE - 4; // verb + msg_id + flags + timestamp
        let total_len = header_fields_size + msg.payload.len();

        if msg.payload.len() > MAX_PAYLOAD_SIZE as usize {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "Payload exceeds maximum size",
            ));
        }

        dst.reserve(4 + total_len);

        // Length prefix
        dst.put_u32(total_len as u32);

        // Header fields
        dst.put_i32(msg.header.verb.id());
        dst.put_u64(msg.header.message_id);
        dst.put_u32(msg.header.flags);
        dst.put_i64(msg.header.creation_timestamp);

        // Payload
        dst.put_slice(&msg.payload);

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_round_trip() {
        let mut codec = MessageCodec;
        let msg = Message::request(Verb::Mutation, 42, b"test payload".to_vec());

        let mut buf = BytesMut::new();
        codec.encode(msg.clone(), &mut buf).unwrap();

        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.header.verb, Verb::Mutation);
        assert_eq!(decoded.header.message_id, 42);
        assert_eq!(decoded.payload, b"test payload");
        assert!(!decoded.is_response());
    }

    #[test]
    fn response_flag() {
        let msg = Message::response(99, Verb::MutationResponse, vec![1, 2, 3]);
        assert!(msg.is_response());
        assert!(!msg.is_failure());
    }

    #[test]
    fn failure_flag() {
        let msg = Message::failure(99, b"error details".to_vec());
        assert!(msg.is_response());
        assert!(msg.is_failure());
    }

    #[test]
    fn partial_frame() {
        let mut codec = MessageCodec;
        let msg = Message::request(Verb::Ping, 1, b"hello".to_vec());

        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).unwrap();

        // Split the buffer in half to simulate partial read
        let second_half = buf.split_off(buf.len() / 2);

        // First decode should return None (incomplete)
        assert!(codec.decode(&mut buf).unwrap().is_none());

        // Re-combine and decode
        buf.extend_from_slice(&second_half);
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.header.verb, Verb::Ping);
    }

    #[test]
    fn empty_payload() {
        let mut codec = MessageCodec;
        let msg = Message::request(Verb::Pong, 7, Vec::new());

        let mut buf = BytesMut::new();
        codec.encode(msg, &mut buf).unwrap();

        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert!(decoded.payload.is_empty());
    }

    #[test]
    fn multiple_messages() {
        let mut codec = MessageCodec;
        let mut buf = BytesMut::new();

        for i in 0..5 {
            let msg = Message::request(Verb::Mutation, i, format!("msg-{i}").into_bytes());
            codec.encode(msg, &mut buf).unwrap();
        }

        for i in 0..5 {
            let decoded = codec.decode(&mut buf).unwrap().unwrap();
            assert_eq!(decoded.header.message_id, i);
            assert_eq!(decoded.payload, format!("msg-{i}").into_bytes());
        }

        assert!(codec.decode(&mut buf).unwrap().is_none());
    }
}
