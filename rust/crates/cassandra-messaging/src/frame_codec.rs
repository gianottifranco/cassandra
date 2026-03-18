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

//! Frame codec with CRC and LZ4 support for internode messaging.
//!
//! Three frame modes matching Java:
//! - **CRC**: 6-byte header (3-byte length+selfContained, 3-byte CRC24),
//!   payload, 4-byte CRC32 trailer
//! - **LZ4**: 8-byte header (compressed+uncompressed lengths, CRC24),
//!   compressed payload, 4-byte CRC32 trailer
//! - **Unprotected**: 4-byte length, no CRC
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.net.FrameEncoder`
//! - `org.apache.cassandra.net.FrameDecoder`

use bytes::{Buf, BufMut, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use crate::crc::{crc24, crc32c};

/// Maximum frame payload size (just under 128 KiB, fits in 17-bit CRC header field).
pub const MAX_FRAME_PAYLOAD: usize = 0x1FFFF; // 131071 bytes

/// Frame mode for encoding/decoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameMode {
    /// CRC-protected frame: header CRC-24 + payload CRC-32.
    Crc,
    /// LZ4-compressed frame with CRC protection.
    Lz4,
    /// Unprotected frame: length-prefixed, no integrity checks.
    Unprotected,
}

/// A decoded frame payload.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// Whether this frame is self-contained (single message).
    pub self_contained: bool,
    /// The frame payload bytes.
    pub payload: Vec<u8>,
}

/// Codec for encoding/decoding CRC/LZ4/Unprotected frames.
///
/// ## Java Oracle
///
/// - `org.apache.cassandra.net.FrameEncoderCrc`
/// - `org.apache.cassandra.net.FrameDecoderCrc`
/// - `org.apache.cassandra.net.FrameEncoderLZ4`
#[derive(Debug, Clone)]
pub struct FrameCodec {
    mode: FrameMode,
}

impl FrameCodec {
    pub fn new(mode: FrameMode) -> Self {
        Self { mode }
    }

    pub fn mode(&self) -> FrameMode {
        self.mode
    }
}

// ─── CRC frame layout ───────────────────────────────────────────────────
// Header (6 bytes): [length_and_flags: 3 bytes] [header_crc24: 3 bytes]
//   length_and_flags: bits 0-16 = payload length, bit 17 = self_contained
// Payload: [payload_bytes...]
// Trailer (4 bytes): [payload_crc32: 4 bytes]

// ─── LZ4 frame layout ───────────────────────────────────────────────────
// Header (8 bytes): [compressed_len: 2 bytes] [uncompressed_len: 2 bytes]
//                   [self_contained: 1 byte] [header_crc24: 3 bytes]
// Payload: [lz4_compressed_bytes...]
// Trailer (4 bytes): [compressed_payload_crc32: 4 bytes]

// ─── Unprotected frame layout ───────────────────────────────────────────
// Header (4 bytes): [length: 4 bytes]
// Payload: [payload_bytes...]

impl Decoder for FrameCodec {
    type Item = Frame;
    type Error = std::io::Error;

    fn decode(&mut self, src: &mut BytesMut) -> Result<Option<Frame>, Self::Error> {
        match self.mode {
            FrameMode::Crc => decode_crc(src),
            FrameMode::Lz4 => decode_lz4(src),
            FrameMode::Unprotected => decode_unprotected(src),
        }
    }
}

impl Encoder<Frame> for FrameCodec {
    type Error = std::io::Error;

    fn encode(&mut self, frame: Frame, dst: &mut BytesMut) -> Result<(), Self::Error> {
        if frame.payload.len() > MAX_FRAME_PAYLOAD {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!(
                    "Frame payload too large: {} > {MAX_FRAME_PAYLOAD}",
                    frame.payload.len()
                ),
            ));
        }
        match self.mode {
            FrameMode::Crc => encode_crc(&frame, dst),
            FrameMode::Lz4 => encode_lz4(&frame, dst),
            FrameMode::Unprotected => encode_unprotected(&frame, dst),
        }
    }
}

// ─── CRC encode/decode ──────────────────────────────────────────────────

fn encode_crc(frame: &Frame, dst: &mut BytesMut) -> Result<(), std::io::Error> {
    let len = frame.payload.len() as u32;
    let flags = if frame.self_contained { 1u32 } else { 0 };
    let length_and_flags = len | (flags << 17);

    // Header: 3 bytes length_and_flags + 3 bytes CRC-24
    let header_bytes = [
        (length_and_flags & 0xFF) as u8,
        ((length_and_flags >> 8) & 0xFF) as u8,
        ((length_and_flags >> 16) & 0xFF) as u8,
    ];
    let hdr_crc = crc24(&header_bytes);

    dst.reserve(6 + frame.payload.len() + 4);
    dst.put_slice(&header_bytes);
    dst.put_u8((hdr_crc & 0xFF) as u8);
    dst.put_u8(((hdr_crc >> 8) & 0xFF) as u8);
    dst.put_u8(((hdr_crc >> 16) & 0xFF) as u8);

    // Payload
    dst.put_slice(&frame.payload);

    // Trailer: CRC-32C of payload
    let payload_crc = crc32c(&frame.payload);
    dst.put_u32_le(payload_crc);

    Ok(())
}

fn decode_crc(src: &mut BytesMut) -> Result<Option<Frame>, std::io::Error> {
    if src.len() < 6 {
        return Ok(None);
    }

    // Peek at header
    let header_bytes = [src[0], src[1], src[2]];
    let stored_crc = (src[3] as u32) | ((src[4] as u32) << 8) | ((src[5] as u32) << 16);
    let computed_crc = crc24(&header_bytes);
    if stored_crc != computed_crc {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "CRC frame header CRC-24 mismatch",
        ));
    }

    let length_and_flags = (header_bytes[0] as u32)
        | ((header_bytes[1] as u32) << 8)
        | ((header_bytes[2] as u32) << 16);
    let payload_len = (length_and_flags & 0x1FFFF) as usize;
    let self_contained = (length_and_flags >> 17) & 1 != 0;

    if payload_len > MAX_FRAME_PAYLOAD {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "CRC frame payload too large",
        ));
    }

    let total = 6 + payload_len + 4;
    if src.len() < total {
        src.reserve(total - src.len());
        return Ok(None);
    }

    // Consume header
    src.advance(6);

    // Read payload
    let payload = src.split_to(payload_len).to_vec();

    // Read and verify CRC-32C trailer
    let stored_crc32 = src.get_u32_le();
    let computed_crc32 = crc32c(&payload);
    if stored_crc32 != computed_crc32 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "CRC frame payload CRC-32 mismatch",
        ));
    }

    Ok(Some(Frame {
        self_contained,
        payload,
    }))
}

// ─── LZ4 encode/decode ─────────────────────────────────────────────────

fn encode_lz4(frame: &Frame, dst: &mut BytesMut) -> Result<(), std::io::Error> {
    let uncompressed_len = frame.payload.len();
    let compressed = lz4_flex::compress_prepend_size(&frame.payload);
    let compressed_len = compressed.len();

    if compressed_len > u16::MAX as usize || uncompressed_len > u16::MAX as usize {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Frame too large for LZ4 header",
        ));
    }

    // Header: 2+2+1 = 5 bytes data + 3 bytes CRC-24
    let mut header_data = [0u8; 5];
    header_data[0] = (compressed_len & 0xFF) as u8;
    header_data[1] = ((compressed_len >> 8) & 0xFF) as u8;
    header_data[2] = (uncompressed_len & 0xFF) as u8;
    header_data[3] = ((uncompressed_len >> 8) & 0xFF) as u8;
    header_data[4] = if frame.self_contained { 1 } else { 0 };

    let hdr_crc = crc24(&header_data);

    dst.reserve(8 + compressed_len + 4);
    dst.put_slice(&header_data);
    dst.put_u8((hdr_crc & 0xFF) as u8);
    dst.put_u8(((hdr_crc >> 8) & 0xFF) as u8);
    dst.put_u8(((hdr_crc >> 16) & 0xFF) as u8);

    // Compressed payload
    dst.put_slice(&compressed);

    // Trailer: CRC-32C of compressed payload
    let payload_crc = crc32c(&compressed);
    dst.put_u32_le(payload_crc);

    Ok(())
}

fn decode_lz4(src: &mut BytesMut) -> Result<Option<Frame>, std::io::Error> {
    if src.len() < 8 {
        return Ok(None);
    }

    // Peek at header
    let header_data = [src[0], src[1], src[2], src[3], src[4]];
    let stored_crc = (src[5] as u32) | ((src[6] as u32) << 8) | ((src[7] as u32) << 16);
    let computed_crc = crc24(&header_data);
    if stored_crc != computed_crc {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "LZ4 frame header CRC-24 mismatch",
        ));
    }

    let compressed_len = (header_data[0] as usize) | ((header_data[1] as usize) << 8);
    let _uncompressed_len = (header_data[2] as usize) | ((header_data[3] as usize) << 8);
    let self_contained = header_data[4] != 0;

    let total = 8 + compressed_len + 4;
    if src.len() < total {
        src.reserve(total - src.len());
        return Ok(None);
    }

    // Consume header
    src.advance(8);

    // Read compressed payload
    let compressed = src.split_to(compressed_len).to_vec();

    // Verify CRC-32C of compressed data
    let stored_crc32 = src.get_u32_le();
    let computed_crc32 = crc32c(&compressed);
    if stored_crc32 != computed_crc32 {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "LZ4 frame payload CRC-32 mismatch",
        ));
    }

    // Decompress
    let payload = lz4_flex::decompress_size_prepended(&compressed).map_err(|e| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("LZ4 decompression failed: {e}"),
        )
    })?;

    Ok(Some(Frame {
        self_contained,
        payload,
    }))
}

// ─── Unprotected encode/decode ──────────────────────────────────────────

fn encode_unprotected(frame: &Frame, dst: &mut BytesMut) -> Result<(), std::io::Error> {
    dst.reserve(4 + frame.payload.len());
    dst.put_u32(frame.payload.len() as u32);
    dst.put_slice(&frame.payload);
    Ok(())
}

fn decode_unprotected(src: &mut BytesMut) -> Result<Option<Frame>, std::io::Error> {
    if src.len() < 4 {
        return Ok(None);
    }

    let len = u32::from_be_bytes([src[0], src[1], src[2], src[3]]) as usize;
    if len > MAX_FRAME_PAYLOAD {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "Unprotected frame payload too large",
        ));
    }

    if src.len() < 4 + len {
        src.reserve(4 + len - src.len());
        return Ok(None);
    }

    src.advance(4);
    let payload = src.split_to(len).to_vec();

    Ok(Some(Frame {
        self_contained: true,
        payload,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip(mode: FrameMode, payload: &[u8], self_contained: bool) {
        let mut codec = FrameCodec::new(mode);
        let frame = Frame {
            self_contained,
            payload: payload.to_vec(),
        };

        let mut buf = BytesMut::new();
        codec.encode(frame.clone(), &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.payload, payload);
        if mode != FrameMode::Unprotected {
            assert_eq!(decoded.self_contained, self_contained);
        }
    }

    #[test]
    fn crc_round_trip_empty() {
        round_trip(FrameMode::Crc, &[], true);
    }

    #[test]
    fn crc_round_trip_data() {
        round_trip(FrameMode::Crc, b"hello frame", true);
    }

    #[test]
    fn crc_round_trip_not_self_contained() {
        round_trip(FrameMode::Crc, b"multi", false);
    }

    #[test]
    fn lz4_round_trip_empty() {
        round_trip(FrameMode::Lz4, &[], true);
    }

    #[test]
    fn lz4_round_trip_data() {
        round_trip(FrameMode::Lz4, b"hello compressed frame", true);
    }

    #[test]
    fn lz4_round_trip_large() {
        let data = vec![0xAB; 4096];
        round_trip(FrameMode::Lz4, &data, true);
    }

    #[test]
    fn unprotected_round_trip() {
        round_trip(FrameMode::Unprotected, b"no crc", true);
    }

    #[test]
    fn crc_corrupt_header_detected() {
        let mut codec = FrameCodec::new(FrameMode::Crc);
        let frame = Frame {
            self_contained: true,
            payload: b"test".to_vec(),
        };

        let mut buf = BytesMut::new();
        codec.encode(frame, &mut buf).unwrap();

        // Corrupt header byte
        buf[0] ^= 0xFF;
        let result = codec.decode(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn crc_corrupt_payload_detected() {
        let mut codec = FrameCodec::new(FrameMode::Crc);
        let frame = Frame {
            self_contained: true,
            payload: b"test data".to_vec(),
        };

        let mut buf = BytesMut::new();
        codec.encode(frame, &mut buf).unwrap();

        // Corrupt a payload byte (after 6-byte header)
        buf[7] ^= 0xFF;
        let result = codec.decode(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn lz4_corrupt_header_detected() {
        let mut codec = FrameCodec::new(FrameMode::Lz4);
        let frame = Frame {
            self_contained: true,
            payload: b"test".to_vec(),
        };

        let mut buf = BytesMut::new();
        codec.encode(frame, &mut buf).unwrap();

        // Corrupt header byte
        buf[0] ^= 0xFF;
        let result = codec.decode(&mut buf);
        assert!(result.is_err());
    }

    #[test]
    fn partial_frame_returns_none() {
        let mut codec = FrameCodec::new(FrameMode::Crc);
        let frame = Frame {
            self_contained: true,
            payload: b"test".to_vec(),
        };

        let mut buf = BytesMut::new();
        codec.encode(frame, &mut buf).unwrap();

        let full = buf.split();
        let mut partial = BytesMut::from(&full[..5]);
        assert!(codec.decode(&mut partial).unwrap().is_none());
    }

    #[test]
    fn multiple_frames() {
        let mut codec = FrameCodec::new(FrameMode::Crc);
        let mut buf = BytesMut::new();

        for i in 0..3 {
            let frame = Frame {
                self_contained: true,
                payload: format!("frame-{i}").into_bytes(),
            };
            codec.encode(frame, &mut buf).unwrap();
        }

        for i in 0..3 {
            let decoded = codec.decode(&mut buf).unwrap().unwrap();
            assert_eq!(decoded.payload, format!("frame-{i}").into_bytes());
        }
        assert!(codec.decode(&mut buf).unwrap().is_none());
    }

    #[test]
    fn payload_too_large_rejected() {
        let mut codec = FrameCodec::new(FrameMode::Crc);
        let frame = Frame {
            self_contained: true,
            payload: vec![0; MAX_FRAME_PAYLOAD + 1],
        };

        let mut buf = BytesMut::new();
        assert!(codec.encode(frame, &mut buf).is_err());
    }
}
