// Licensed under Apache License, Version 2.0.

//! Integration tests for the native transport server.
//!
//! These tests verify the CQL protocol frame codec round-trips and
//! pipelining behaviour using raw bytes over the wire format.

use bytes::{Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use cassandra_native_protocol::frame::{Frame, FrameCodec, FrameHeader, Opcode};

// ─── Helpers ──────────────────────────────────────────────────────────────

/// Build an OPTIONS request frame (opcode 0x05, empty body).
fn options_frame(stream_id: i16) -> Vec<u8> {
    let header = FrameHeader {
        version: 4,
        flags: 0,
        stream_id,
        opcode: Opcode::Options,
        length: 0,
    };
    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    buf.to_vec()
}

/// Build a STARTUP request frame (opcode 0x01).
fn startup_frame(stream_id: i16) -> Vec<u8> {
    // Body: string map with CQL_VERSION = "3.4.7"
    let key = b"CQL_VERSION";
    let val = b"3.4.7";
    // map count (2) + key len (2) + key + val len (2) + val
    let body_len = 2 + 2 + key.len() + 2 + val.len();

    let header = FrameHeader {
        version: 4,
        flags: 0,
        stream_id,
        opcode: Opcode::Startup,
        length: body_len as u32,
    };

    let mut buf = BytesMut::new();
    header.encode(&mut buf);
    // map count
    buf.extend_from_slice(&1u16.to_be_bytes());
    // key
    buf.extend_from_slice(&(key.len() as u16).to_be_bytes());
    buf.extend_from_slice(key);
    // value
    buf.extend_from_slice(&(val.len() as u16).to_be_bytes());
    buf.extend_from_slice(val);
    buf.to_vec()
}

// ─── Tests ────────────────────────────────────────────────────────────────

/// Verify that we can construct and encode an OPTIONS frame, then decode it.
#[tokio::test]
async fn options_frame_codec_roundtrip() {
    let raw = options_frame(42);
    let mut buf = BytesMut::from(&raw[..]);
    let mut codec = FrameCodec;
    let frame = codec.decode(&mut buf).unwrap().expect("should decode");
    assert_eq!(frame.header.opcode, Opcode::Options);
    assert_eq!(frame.header.stream_id, 42);
    assert!(frame.body.is_empty());
}

#[tokio::test]
async fn startup_frame_codec_roundtrip() {
    let raw = startup_frame(1);
    let mut buf = BytesMut::from(&raw[..]);
    let mut codec = FrameCodec;
    let frame = codec.decode(&mut buf).unwrap().expect("should decode");
    assert_eq!(frame.header.opcode, Opcode::Startup);
    assert_eq!(frame.header.stream_id, 1);
    assert!(!frame.body.is_empty());
}

/// Verify that the FrameCodec correctly handles partial reads.
#[tokio::test]
async fn codec_handles_partial_frames() {
    let raw = options_frame(0);
    let mut codec = FrameCodec;

    // Feed only first 5 bytes (incomplete header)
    let mut buf = BytesMut::from(&raw[..5]);
    assert!(codec.decode(&mut buf).unwrap().is_none());

    // Feed the rest
    buf.extend_from_slice(&raw[5..]);
    let frame = codec.decode(&mut buf).unwrap().expect("should complete");
    assert_eq!(frame.header.opcode, Opcode::Options);
}

/// Verify that encoding a response frame produces valid wire bytes.
#[tokio::test]
async fn response_frame_encode() {
    let frame = Frame {
        header: FrameHeader {
            version: 0x84, // v4 response
            flags: 0,
            stream_id: 7,
            opcode: Opcode::Ready,
            length: 0,
        },
        body: Bytes::new(),
    };
    let mut codec = FrameCodec;
    let mut buf = BytesMut::new();
    codec.encode(frame, &mut buf).unwrap();

    // Should produce exactly 9 bytes (header only)
    assert_eq!(buf.len(), 9);
    assert_eq!(buf[0], 0x84); // response version
    assert_eq!(buf[4], 0x02); // READY opcode
}

/// Verify multiple frames can be pipelined into one buffer.
#[tokio::test]
async fn multiple_frames_in_buffer() {
    let mut combined = Vec::new();
    combined.extend_from_slice(&options_frame(1));
    combined.extend_from_slice(&options_frame(2));
    combined.extend_from_slice(&startup_frame(3));

    let mut buf = BytesMut::from(&combined[..]);
    let mut codec = FrameCodec;

    let f1 = codec.decode(&mut buf).unwrap().expect("frame 1");
    assert_eq!(f1.header.stream_id, 1);
    assert_eq!(f1.header.opcode, Opcode::Options);

    let f2 = codec.decode(&mut buf).unwrap().expect("frame 2");
    assert_eq!(f2.header.stream_id, 2);
    assert_eq!(f2.header.opcode, Opcode::Options);

    let f3 = codec.decode(&mut buf).unwrap().expect("frame 3");
    assert_eq!(f3.header.stream_id, 3);
    assert_eq!(f3.header.opcode, Opcode::Startup);

    // Buffer should be empty now
    assert!(codec.decode(&mut buf).unwrap().is_none());
}

/// Verify frame header fields survive encode→decode round-trip.
#[tokio::test]
async fn header_roundtrip_preserves_fields() {
    let frame = Frame {
        header: FrameHeader {
            version: 4,
            flags: 0x02, // TRACING
            stream_id: 32767,
            opcode: Opcode::Query,
            length: 5,
        },
        body: Bytes::from_static(b"hello"),
    };

    let mut codec = FrameCodec;
    let mut buf = BytesMut::new();
    codec.encode(frame, &mut buf).unwrap();

    let decoded = codec.decode(&mut buf).unwrap().expect("decode");
    assert_eq!(decoded.header.version, 4);
    assert_eq!(decoded.header.flags, 0x02);
    assert_eq!(decoded.header.stream_id, 32767);
    assert_eq!(decoded.header.opcode, Opcode::Query);
    assert_eq!(decoded.body.len(), 5);
    assert_eq!(&decoded.body[..], b"hello");
}

/// Structural test: verify opcode discriminant values.
#[test]
fn opcodes_are_correct() {
    assert_eq!(Opcode::Startup as u8, 0x01);
    assert_eq!(Opcode::Ready as u8, 0x02);
    assert_eq!(Opcode::Options as u8, 0x05);
    assert_eq!(Opcode::Supported as u8, 0x06);
    assert_eq!(Opcode::Query as u8, 0x07);
    assert_eq!(Opcode::Result as u8, 0x08);
    assert_eq!(Opcode::AuthResponse as u8, 0x0F);
}
