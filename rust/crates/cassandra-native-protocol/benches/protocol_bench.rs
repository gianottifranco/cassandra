// Licensed under Apache License, Version 2.0.

//! Native protocol codec benchmarks.
//!
//! Measures frame encode/decode performance against budgets from ADR-015.
//!
//! ## Running
//!
//! ```bash
//! cargo bench -p cassandra-native-protocol --bench protocol_bench
//! ```

use criterion::{Criterion, black_box, criterion_group, criterion_main};

use bytes::{Bytes, BytesMut};
use tokio_util::codec::{Decoder, Encoder};

use cassandra_native_protocol::frame::{
    Frame, FrameCodec, FrameHeader, Opcode, PROTOCOL_V4, RESPONSE_FLAG, response_frame,
};

fn make_query_body() -> Bytes {
    Bytes::from_static(b"SELECT * FROM system.local WHERE key = 'local'")
}

fn make_query_frame() -> Frame {
    let body = make_query_body();
    Frame {
        header: FrameHeader {
            version: PROTOCOL_V4,
            flags: 0,
            stream_id: 1,
            opcode: Opcode::Query,
            length: body.len() as u32,
        },
        body,
    }
}

fn make_result_frame(row_count: usize) -> Frame {
    let mut body = Vec::with_capacity(row_count * 64 + 16);
    // Result kind: Rows (0x0002)
    body.extend_from_slice(&[0x00, 0x00, 0x00, 0x02]);
    // Flags: 0x0001 (global_tables_spec)
    body.extend_from_slice(&[0x00, 0x00, 0x00, 0x01]);
    // Column count
    body.extend_from_slice(&(1u32).to_be_bytes());
    // Rows count
    body.extend_from_slice(&(row_count as u32).to_be_bytes());
    for i in 0..row_count {
        let val = format!("value-{i:06}");
        body.extend_from_slice(&(val.len() as u32).to_be_bytes());
        body.extend_from_slice(val.as_bytes());
    }

    response_frame(PROTOCOL_V4, 1, Opcode::Result, 0, Bytes::from(body))
}

fn bench_frame_encode(c: &mut Criterion) {
    let frame = make_query_frame();

    c.bench_function("frame_encode_query", |b| {
        b.iter(|| {
            let mut codec = FrameCodec;
            let mut dst = BytesMut::with_capacity(256);
            codec.encode(black_box(frame.clone()), &mut dst).unwrap();
            black_box(dst);
        });
    });
}

fn bench_frame_decode(c: &mut Criterion) {
    let frame = make_query_frame();
    let mut codec = FrameCodec;
    let mut encoded = BytesMut::with_capacity(256);
    codec.encode(frame, &mut encoded).unwrap();
    let wire = encoded.freeze();

    c.bench_function("frame_decode_query", |b| {
        b.iter(|| {
            let mut src = BytesMut::from(wire.as_ref());
            let mut codec = FrameCodec;
            let decoded = codec.decode(black_box(&mut src)).unwrap();
            black_box(decoded);
        });
    });
}

fn bench_result_encode(c: &mut Criterion) {
    let frame = make_result_frame(100);

    c.bench_function("frame_encode_result_100rows", |b| {
        b.iter(|| {
            let mut codec = FrameCodec;
            let mut dst = BytesMut::with_capacity(16 * 1024);
            codec.encode(black_box(frame.clone()), &mut dst).unwrap();
            black_box(dst);
        });
    });
}

fn bench_result_decode(c: &mut Criterion) {
    let frame = make_result_frame(100);
    let mut codec = FrameCodec;
    let mut encoded = BytesMut::with_capacity(16 * 1024);
    codec.encode(frame, &mut encoded).unwrap();
    let wire = encoded.freeze();

    c.bench_function("frame_decode_result_100rows", |b| {
        b.iter(|| {
            let mut src = BytesMut::from(wire.as_ref());
            let mut codec = FrameCodec;
            let decoded = codec.decode(black_box(&mut src)).unwrap();
            black_box(decoded);
        });
    });
}

fn bench_header_decode(c: &mut Criterion) {
    let bytes = [0x04, 0x00, 0x00, 0x01, 0x07, 0x00, 0x00, 0x00, 0x2A];

    c.bench_function("frame_header_decode", |b| {
        b.iter(|| {
            let hdr = FrameHeader::decode(black_box(&bytes)).unwrap();
            black_box(hdr);
        });
    });
}

criterion_group!(
    benches,
    bench_frame_encode,
    bench_frame_decode,
    bench_result_encode,
    bench_result_decode,
    bench_header_decode,
);
criterion_main!(benches);
