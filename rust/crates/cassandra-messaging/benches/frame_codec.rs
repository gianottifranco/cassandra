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

//! Benchmarks for frame codec, CRC, and message throughput.

use bytes::BytesMut;
use criterion::{Criterion, black_box, criterion_group, criterion_main};
use tokio_util::codec::{Decoder, Encoder};

use cassandra_messaging::crc::{crc24, crc32c};
use cassandra_messaging::frame_codec::{Frame, FrameCodec, FrameMode};

fn bench_crc24(c: &mut Criterion) {
    let data = vec![0xAB; 1024];
    c.bench_function("crc24_1kb", |b| {
        b.iter(|| crc24(black_box(&data)))
    });
}

fn bench_crc32c(c: &mut Criterion) {
    let data = vec![0xAB; 1024];
    c.bench_function("crc32c_1kb", |b| {
        b.iter(|| crc32c(black_box(&data)))
    });

    let large_data = vec![0xCD; 64 * 1024];
    c.bench_function("crc32c_64kb", |b| {
        b.iter(|| crc32c(black_box(&large_data)))
    });
}

fn bench_frame_encode_decode_crc(c: &mut Criterion) {
    let payload = vec![0x42; 4096];
    let frame = Frame {
        self_contained: true,
        payload: payload.clone(),
    };

    c.bench_function("frame_crc_encode_4kb", |b| {
        b.iter(|| {
            let mut codec = FrameCodec::new(FrameMode::Crc);
            let mut buf = BytesMut::with_capacity(8192);
            codec.encode(black_box(frame.clone()), &mut buf).unwrap();
        })
    });

    c.bench_function("frame_crc_decode_4kb", |b| {
        let mut codec = FrameCodec::new(FrameMode::Crc);
        let mut encoded = BytesMut::new();
        codec.encode(frame.clone(), &mut encoded).unwrap();
        let encoded_bytes = encoded.freeze();

        b.iter(|| {
            let mut codec = FrameCodec::new(FrameMode::Crc);
            let mut buf = BytesMut::from(encoded_bytes.as_ref());
            codec.decode(black_box(&mut buf)).unwrap()
        })
    });
}

fn bench_frame_encode_decode_lz4(c: &mut Criterion) {
    let payload = vec![0x42; 4096];
    let frame = Frame {
        self_contained: true,
        payload: payload.clone(),
    };

    c.bench_function("frame_lz4_encode_4kb", |b| {
        b.iter(|| {
            let mut codec = FrameCodec::new(FrameMode::Lz4);
            let mut buf = BytesMut::with_capacity(8192);
            codec.encode(black_box(frame.clone()), &mut buf).unwrap();
        })
    });

    c.bench_function("frame_lz4_decode_4kb", |b| {
        let mut codec = FrameCodec::new(FrameMode::Lz4);
        let mut encoded = BytesMut::new();
        codec.encode(frame.clone(), &mut encoded).unwrap();
        let encoded_bytes = encoded.freeze();

        b.iter(|| {
            let mut codec = FrameCodec::new(FrameMode::Lz4);
            let mut buf = BytesMut::from(encoded_bytes.as_ref());
            codec.decode(black_box(&mut buf)).unwrap()
        })
    });
}

fn bench_frame_encode_decode_unprotected(c: &mut Criterion) {
    let payload = vec![0x42; 4096];
    let frame = Frame {
        self_contained: true,
        payload: payload.clone(),
    };

    c.bench_function("frame_unprotected_encode_4kb", |b| {
        b.iter(|| {
            let mut codec = FrameCodec::new(FrameMode::Unprotected);
            let mut buf = BytesMut::with_capacity(8192);
            codec.encode(black_box(frame.clone()), &mut buf).unwrap();
        })
    });
}

criterion_group!(
    benches,
    bench_crc24,
    bench_crc32c,
    bench_frame_encode_decode_crc,
    bench_frame_encode_decode_lz4,
    bench_frame_encode_decode_unprotected,
);
criterion_main!(benches);
