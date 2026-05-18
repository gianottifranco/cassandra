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

//! Integration tests for persistent internode messaging.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use cassandra_messaging::Verb;
use cassandra_messaging::connection_type::ConnectionType;
use cassandra_messaging::crc::{crc24, crc32c, crc32c_update};
use cassandra_messaging::forwarding::ForwardingInfo;
use cassandra_messaging::frame::Message;
use cassandra_messaging::frame_codec::{Frame, FrameCodec, FrameMode};
use cassandra_messaging::handshake;
use cassandra_messaging::outbound_connections::OutboundConnections;
use cassandra_messaging::outbound_queue::OutboundMessageQueue;
use cassandra_messaging::resource_limits::{EndpointLimits, Limit};

use bytes::BytesMut;
use tokio_util::codec::{Decoder, Encoder};

// ─── CRC integrity tests ───────────────────────────────────────────────

#[test]
fn crc32c_known_vector_123456789() {
    assert_eq!(crc32c(b"123456789"), 0xE306_9283);
}

#[test]
fn crc32c_incremental_matches_full() {
    let data = b"hello world, this is a longer test string for CRC";
    let full = crc32c(data);
    let partial = crc32c(&data[..20]);
    let updated = crc32c_update(partial, &data[20..]);
    assert_eq!(updated, full);
}

#[test]
fn crc24_fits_in_24_bits() {
    for i in 0..256 {
        let crc = crc24(&[i as u8; 64]);
        assert!(crc <= 0xFF_FFFF, "CRC-24 overflow for input {i}");
    }
}

// ─── Frame codec: CRC round-trip ────────────────────────────────────────

#[test]
fn frame_crc_round_trip_various_sizes() {
    for size in [0, 1, 100, 1024, 4096, 65535] {
        let mut codec = FrameCodec::new(FrameMode::Crc);
        let payload = vec![0xAB; size];
        let frame = Frame {
            self_contained: true,
            payload: payload.clone(),
        };

        let mut buf = BytesMut::new();
        codec.encode(frame, &mut buf).unwrap();
        let decoded = codec.decode(&mut buf).unwrap().unwrap();
        assert_eq!(decoded.payload.len(), size);
        assert_eq!(decoded.payload, payload);
    }
}

#[test]
fn frame_crc_corrupt_detected() {
    let mut codec = FrameCodec::new(FrameMode::Crc);
    let frame = Frame {
        self_contained: true,
        payload: b"integrity test".to_vec(),
    };

    let mut buf = BytesMut::new();
    codec.encode(frame, &mut buf).unwrap();

    // Corrupt payload byte (after 6-byte header)
    if buf.len() > 7 {
        buf[7] ^= 0xFF;
    }

    let result = codec.decode(&mut buf);
    assert!(result.is_err(), "Corrupted CRC frame should be rejected");
}

// ─── Frame codec: LZ4 round-trip ───────────────────────────────────────

#[test]
fn frame_lz4_round_trip() {
    let mut codec = FrameCodec::new(FrameMode::Lz4);
    let payload = b"compressible data compressible data compressible data".to_vec();
    let frame = Frame {
        self_contained: true,
        payload: payload.clone(),
    };

    let mut buf = BytesMut::new();
    codec.encode(frame, &mut buf).unwrap();
    let decoded = codec.decode(&mut buf).unwrap().unwrap();
    assert_eq!(decoded.payload, payload);
}

#[test]
fn frame_lz4_compression_reduces_size() {
    let mut codec = FrameCodec::new(FrameMode::Lz4);
    // Highly compressible data
    let payload = vec![0x42; 4096];
    let frame = Frame {
        self_contained: true,
        payload: payload.clone(),
    };

    let mut buf = BytesMut::new();
    codec.encode(frame, &mut buf).unwrap();

    // Compressed frame should be smaller than uncompressed
    // (8 header + compressed + 4 trailer) < (4096 + overhead)
    assert!(
        buf.len() < payload.len(),
        "LZ4 should compress repetitive data"
    );
}

// ─── Three-channel routing ──────────────────────────────────────────────

#[test]
fn three_channel_classification() {
    // Urgent
    assert_eq!(
        ConnectionType::classify(Verb::Ping, 0),
        ConnectionType::Urgent
    );
    assert_eq!(
        ConnectionType::classify(Verb::GossipDigestSyn, 100),
        ConnectionType::Urgent
    );

    // Small
    assert_eq!(
        ConnectionType::classify(Verb::Mutation, 1000),
        ConnectionType::Small
    );
    assert_eq!(
        ConnectionType::classify(Verb::ReadData, 512),
        ConnectionType::Small
    );

    // Large
    assert_eq!(
        ConnectionType::classify(Verb::StreamData, 100_000),
        ConnectionType::Large
    );
    assert_eq!(
        ConnectionType::classify(Verb::Mutation, 65 * 1024),
        ConnectionType::Large
    );
}

#[test]
fn outbound_connections_routing() {
    let conns = OutboundConnections::new(
        "127.0.0.1:7000".parse().unwrap(),
        "127.0.0.1:0".parse().unwrap(),
    );

    // Urgent
    assert!(conns.send(
        Message::request(Verb::Ping, 1, Vec::new()),
        Duration::from_secs(5),
    ));

    // Small
    assert!(conns.send(
        Message::request(Verb::Mutation, 2, b"payload".to_vec()),
        Duration::from_secs(5),
    ));

    // Large
    assert!(conns.send(
        Message::request(Verb::StreamData, 3, vec![0; 65 * 1024]),
        Duration::from_secs(5),
    ));
}

// ─── Handshake protocol ────────────────────────────────────────────────

#[tokio::test]
async fn handshake_full_flow() {
    let (client, server) = tokio::io::duplex(4096);
    let mut client = client;
    let mut server = server;

    let sender_addr: SocketAddr = "192.168.1.100:7000".parse().unwrap();

    let (client_result, server_result) = tokio::join!(
        handshake::perform_outbound_handshake(
            &mut client,
            ConnectionType::Small,
            true,
            true,
            sender_addr,
        ),
        handshake::accept_inbound_handshake(&mut server),
    );

    let client_hs = client_result.unwrap();
    let (server_hs, peer_addr) = server_result.unwrap();

    assert_eq!(client_hs.version, server_hs.version);
    assert_eq!(client_hs.connection_type, ConnectionType::Small);
    assert_eq!(server_hs.connection_type, ConnectionType::Small);
    assert!(client_hs.compression);
    assert!(client_hs.crc_framing);
    assert_eq!(peer_addr, sender_addr);
}

#[tokio::test]
async fn handshake_all_connection_types() {
    for conn_type in [
        ConnectionType::Urgent,
        ConnectionType::Small,
        ConnectionType::Large,
    ] {
        let (mut client, mut server) = tokio::io::duplex(4096);
        let addr: SocketAddr = "10.0.0.1:7000".parse().unwrap();

        let (c, s) = tokio::join!(
            handshake::perform_outbound_handshake(&mut client, conn_type, false, false, addr,),
            handshake::accept_inbound_handshake(&mut server),
        );

        let ch = c.unwrap();
        let (sh, _) = s.unwrap();
        assert_eq!(ch.connection_type, conn_type);
        assert_eq!(sh.connection_type, conn_type);
    }
}

// ─── Resource limits backpressure ───────────────────────────────────────

#[test]
fn resource_limits_three_tier_backpressure() {
    let global = Arc::new(Limit::new(1000));
    let limits = EndpointLimits::with_defaults(Arc::clone(&global));

    // Allocate within limits
    let p1 = limits.try_allocate(500).expect("should succeed");
    assert_eq!(p1.bytes(), 500);
    assert_eq!(global.allocated(), 500);

    let _p2 = limits.try_allocate(400).expect("should succeed");
    assert_eq!(global.allocated(), 900);

    // Global limit reached
    assert!(limits.try_allocate(200).is_none());

    // Release and retry
    drop(p1);
    assert_eq!(global.allocated(), 400);
    let _p3 = limits
        .try_allocate(200)
        .expect("should succeed after release");
}

// ─── Message expiration ─────────────────────────────────────────────────

#[test]
fn message_queue_expiration() {
    let (mut queue, producer) = OutboundMessageQueue::new();

    let expired = std::time::Instant::now() - Duration::from_millis(1);
    let fresh = std::time::Instant::now() + Duration::from_secs(60);

    // Enqueue a mix of expired and fresh messages
    producer.enqueue(Message::request(Verb::Mutation, 1, Vec::new()), expired);
    producer.enqueue(Message::request(Verb::Mutation, 2, Vec::new()), expired);
    producer.enqueue(Message::request(Verb::Mutation, 3, Vec::new()), fresh);

    // poll_next should skip expired, return fresh
    let msg = queue.poll_next().unwrap();
    assert_eq!(msg.message.header.message_id, 3);

    // No more messages
    assert!(queue.poll_next().is_none());

    let snap = queue.metrics.snapshot();
    assert_eq!(snap.expired, 2);
    assert_eq!(snap.sent, 1);
}

#[test]
fn message_expiration_flag() {
    // Message with past expiration
    let past_nanos = 1i64; // epoch + 1ns, definitely in the past
    let msg = Message::request(Verb::Ping, 1, Vec::new()).with_expiration(past_nanos);
    assert!(msg.is_expired());

    // Message with far-future expiration
    let future_nanos = i64::MAX;
    let msg = Message::request(Verb::Ping, 2, Vec::new()).with_expiration(future_nanos);
    assert!(!msg.is_expired());

    // Message without expiration never expires
    let msg = Message::request(Verb::Ping, 3, Vec::new());
    assert!(!msg.is_expired());
}

// ─── Forwarding info ───────────────────────────────────────────────────

#[test]
fn forwarding_info_propagation() {
    let origin: SocketAddr = "10.0.0.1:7000".parse().unwrap();
    let forwarder: SocketAddr = "10.0.0.2:7000".parse().unwrap();

    let mut info = ForwardingInfo::single(origin, 42);
    info.add_hop(forwarder, 99);

    // Serialize and deserialize
    let bytes = info.to_bytes();
    let decoded = ForwardingInfo::from_bytes(&bytes).unwrap();
    assert_eq!(decoded.hop_count(), 2);
    assert_eq!(decoded.origin().unwrap().from, origin);
    assert_eq!(decoded.last_forwarder().unwrap().from, forwarder);

    // Attach to message
    let msg = Message::request(Verb::Mutation, 1, b"data".to_vec()).with_forwarding_info(decoded);
    assert!(msg.is_forwarded());
    assert!(msg.forwarding.is_some());
}

// ─── Service persistent routing ─────────────────────────────────────────

#[test]
fn service_persistent_send() {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let svc = cassandra_messaging::MessagingService::new(addr);

    // get_outbound creates connections lazily
    let conns = svc.get_outbound("10.0.0.1:7000".parse().unwrap());
    assert_eq!(
        conns.remote(),
        "10.0.0.1:7000".parse::<SocketAddr>().unwrap()
    );

    // Second call returns the same instance
    let conns2 = svc.get_outbound("10.0.0.1:7000".parse().unwrap());
    assert_eq!(conns2.remote(), conns.remote());
}

#[test]
fn service_close_all_outbound() {
    let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
    let svc = cassandra_messaging::MessagingService::new(addr);

    let _ = svc.get_outbound("10.0.0.1:7000".parse().unwrap());
    let _ = svc.get_outbound("10.0.0.2:7000".parse().unwrap());

    svc.close_all_outbound(); // Should not panic
}

// ─── Metrics expansion ─────────────────────────────────────────────────

#[test]
fn metrics_all_verbs_registered() {
    let metrics = cassandra_messaging::MessagingMetrics::new();
    let snaps = metrics.all_verb_snapshots();
    assert!(snaps.len() >= 44);

    // All these verbs should be registered
    for verb in [
        Verb::StreamInit,
        Verb::StreamData,
        Verb::TcmCommit,
        Verb::RepairRequest,
        Verb::TopologyChange,
        Verb::GossipShutdown,
    ] {
        metrics.verb(verb).record_sent();
    }
}

#[test]
fn metrics_per_connection_type() {
    let metrics = cassandra_messaging::MessagingMetrics::new();
    metrics.record_bytes_sent(100, ConnectionType::Urgent);
    metrics.record_bytes_sent(200, ConnectionType::Small);
    metrics.record_bytes_received(300, ConnectionType::Large);

    assert_eq!(
        metrics
            .bytes_sent
            .load(std::sync::atomic::Ordering::Relaxed),
        300
    );
    assert_eq!(
        metrics
            .bytes_received
            .load(std::sync::atomic::Ordering::Relaxed),
        300
    );
}
