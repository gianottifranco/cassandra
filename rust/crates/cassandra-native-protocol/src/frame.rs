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

//! CQL binary protocol frame header and codec.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.transport.Frame`
//! - `org.apache.cassandra.transport.FrameDecoder`
//!
//! ## Wire format
//!
//! ```text
//! 0         8        16        24       32
//! +---------+---------+---------+---------+
//! | version |  flags  |     stream id     |
//! +---------+---------+---------+---------+
//! | opcode  |       length (4 bytes)      |
//! +---------+---------+---------+---------+
//! |              body (length bytes)      |
//! +---------------------------------------+
//! ```
//!
//! Header is 9 bytes. Version high bit indicates response (0x80).

use bytes::{Buf, BufMut, Bytes, BytesMut};
use std::io;
use tokio_util::codec::{Decoder, Encoder};

/// Protocol versions we support.
pub const PROTOCOL_V4: u8 = 0x04;
pub const PROTOCOL_V5: u8 = 0x05;

/// The response direction bit in the version byte.
pub const RESPONSE_FLAG: u8 = 0x80;

/// Frame header flags.
pub mod flags {
    pub const COMPRESSION: u8 = 0x01;
    pub const TRACING: u8 = 0x02;
    pub const CUSTOM_PAYLOAD: u8 = 0x04;
    pub const WARNING: u8 = 0x08;
    pub const USE_BETA: u8 = 0x10;
}

/// Opcodes for request and response messages.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Opcode {
    Error = 0x00,
    Startup = 0x01,
    Ready = 0x02,
    Authenticate = 0x03,
    Options = 0x05,
    Supported = 0x06,
    Query = 0x07,
    Result = 0x08,
    Prepare = 0x09,
    Execute = 0x0A,
    Register = 0x0B,
    Event = 0x0C,
    Batch = 0x0D,
    AuthChallenge = 0x0E,
    AuthResponse = 0x0F,
    AuthSuccess = 0x10,
}

impl Opcode {
    pub fn from_u8(v: u8) -> io::Result<Self> {
        match v {
            0x00 => Ok(Opcode::Error),
            0x01 => Ok(Opcode::Startup),
            0x02 => Ok(Opcode::Ready),
            0x03 => Ok(Opcode::Authenticate),
            0x05 => Ok(Opcode::Options),
            0x06 => Ok(Opcode::Supported),
            0x07 => Ok(Opcode::Query),
            0x08 => Ok(Opcode::Result),
            0x09 => Ok(Opcode::Prepare),
            0x0A => Ok(Opcode::Execute),
            0x0B => Ok(Opcode::Register),
            0x0C => Ok(Opcode::Event),
            0x0D => Ok(Opcode::Batch),
            0x0E => Ok(Opcode::AuthChallenge),
            0x0F => Ok(Opcode::AuthResponse),
            0x10 => Ok(Opcode::AuthSuccess),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown opcode: 0x{:02X}", v),
            )),
        }
    }

    pub fn is_request(self) -> bool {
        matches!(
            self,
            Opcode::Startup
                | Opcode::Options
                | Opcode::Query
                | Opcode::Prepare
                | Opcode::Execute
                | Opcode::Register
                | Opcode::Batch
                | Opcode::AuthResponse
        )
    }
}

/// Parsed frame header (9 bytes).
#[derive(Debug, Clone, Copy)]
pub struct FrameHeader {
    pub version: u8,
    pub flags: u8,
    pub stream_id: i16,
    pub opcode: Opcode,
    pub length: u32,
}

impl FrameHeader {
    pub const SIZE: usize = 9;

    pub fn is_response(&self) -> bool {
        self.version & RESPONSE_FLAG != 0
    }

    pub fn protocol_version(&self) -> u8 {
        self.version & 0x7F
    }

    /// Encode the header into a BytesMut buffer.
    pub fn encode(&self, buf: &mut BytesMut) {
        buf.put_u8(self.version);
        buf.put_u8(self.flags);
        buf.put_i16(self.stream_id);
        buf.put_u8(self.opcode as u8);
        buf.put_u32(self.length);
    }

    /// Decode a header from 9 bytes.
    pub fn decode(data: &[u8]) -> io::Result<Self> {
        if data.len() < Self::SIZE {
            return Err(io::Error::new(
                io::ErrorKind::UnexpectedEof,
                "frame header too short",
            ));
        }
        let version = data[0];
        let flags = data[1];
        let stream_id = i16::from_be_bytes([data[2], data[3]]);
        let opcode = Opcode::from_u8(data[4])?;
        let length = u32::from_be_bytes([data[5], data[6], data[7], data[8]]);
        Ok(FrameHeader {
            version,
            flags,
            stream_id,
            opcode,
            length,
        })
    }
}

/// A complete CQL protocol frame (header + body).
#[derive(Debug, Clone)]
pub struct Frame {
    pub header: FrameHeader,
    pub body: Bytes,
}

/// Maximum frame body size (256 MB, matching Java).
const MAX_FRAME_LENGTH: u32 = 256 * 1024 * 1024;

/// Tokio codec for reading/writing CQL protocol frames.
///
/// Handles frame boundary detection. Compression/decompression is done
/// at a higher layer after decoding the frame.
pub struct FrameCodec;

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Self::Item>, Self::Error> {
        // Need at least the header.
        if src.len() < FrameHeader::SIZE {
            return Ok(None);
        }

        // Peek at header without consuming.
        let header = FrameHeader::decode(&src[..FrameHeader::SIZE])?;

        if header.length > MAX_FRAME_LENGTH {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("frame too large: {} bytes", header.length),
            ));
        }

        let total = FrameHeader::SIZE + header.length as usize;
        if src.len() < total {
            // Reserve space for the remaining data.
            src.reserve(total - src.len());
            return Ok(None);
        }

        // Consume header bytes.
        src.advance(FrameHeader::SIZE);
        // Consume body bytes.
        let body = src.split_to(header.length as usize).freeze();

        Ok(Some(Frame { header, body }))
    }
}

impl Encoder<Frame> for FrameCodec {
    type Error = io::Error;

    fn encode(&mut self, frame: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        dst.reserve(FrameHeader::SIZE + frame.body.len());
        frame.header.encode(dst);
        dst.extend_from_slice(&frame.body);
        Ok(())
    }
}

/// Helper: build a response frame.
pub fn response_frame(
    version: u8,
    stream_id: i16,
    opcode: Opcode,
    flags: u8,
    body: Bytes,
) -> Frame {
    let header = FrameHeader {
        version: version | RESPONSE_FLAG,
        flags,
        stream_id,
        opcode,
        length: body.len() as u32,
    };
    Frame { header, body }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_roundtrip() {
        let hdr = FrameHeader {
            version: PROTOCOL_V4,
            flags: 0,
            stream_id: 1,
            opcode: Opcode::Query,
            length: 42,
        };
        let mut buf = BytesMut::new();
        hdr.encode(&mut buf);
        assert_eq!(buf.len(), FrameHeader::SIZE);

        let decoded = FrameHeader::decode(&buf).unwrap();
        assert_eq!(decoded.version, PROTOCOL_V4);
        assert_eq!(decoded.stream_id, 1);
        assert_eq!(decoded.opcode, Opcode::Query);
        assert_eq!(decoded.length, 42);
    }

    #[test]
    fn frame_codec_decode() {
        let body_data = b"test body data";
        let hdr = FrameHeader {
            version: PROTOCOL_V4,
            flags: 0,
            stream_id: 0,
            opcode: Opcode::Startup,
            length: body_data.len() as u32,
        };
        let mut buf = BytesMut::new();
        hdr.encode(&mut buf);
        buf.extend_from_slice(body_data);

        let mut codec = FrameCodec;
        let frame = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame.header.opcode, Opcode::Startup);
        assert_eq!(&frame.body[..], body_data);
        assert!(buf.is_empty());
    }

    #[test]
    fn frame_codec_partial() {
        let mut buf = BytesMut::new();
        // Only partial header
        buf.extend_from_slice(&[0x04, 0x00, 0x00]);
        let mut codec = FrameCodec;
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn frame_codec_roundtrip() {
        let body = Bytes::from_static(b"hello");
        let frame = response_frame(PROTOCOL_V4, 5, Opcode::Result, 0, body.clone());

        let mut buf = BytesMut::new();
        let mut codec = FrameCodec;
        codec.encode(frame, &mut buf).unwrap();

        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.header.stream_id, 5);
        assert!(decoded.header.is_response());
        assert_eq!(decoded.header.opcode, Opcode::Result);
        assert_eq!(decoded.body, body);
    }

    #[test]
    fn opcode_is_request() {
        assert!(Opcode::Query.is_request());
        assert!(Opcode::Startup.is_request());
        assert!(!Opcode::Ready.is_request());
        assert!(!Opcode::Result.is_request());
    }

    #[test]
    fn golden_startup_header() {
        // A STARTUP request from a v4 client, stream 0, no flags, body len 22.
        let bytes = [0x04, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x16];
        let hdr = FrameHeader::decode(&bytes).unwrap();
        assert_eq!(hdr.protocol_version(), 4);
        assert!(!hdr.is_response());
        assert_eq!(hdr.opcode, Opcode::Startup);
        assert_eq!(hdr.length, 22);
    }
}
