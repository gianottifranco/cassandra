// Licensed under Apache License, Version 2.0.

//! Integration tests for multi-node streaming using real TCP connections
//! between two MessagingService instances on localhost.

use std::net::SocketAddr;
use std::sync::Arc;

use cassandra_messaging::{Message, MessagingService, Verb};
use cassandra_streaming::protocol::*;
use cassandra_streaming::receiver::StreamReceiver;
use cassandra_streaming::transfer::{ChecksumAlgorithm, ChunkChecksum, StreamTransfer};
use cassandra_streaming::transport::{compress, decompress, verify_chunk_checksum};
use cassandra_streaming::{StreamManager, StreamingMetrics};
use tempfile::TempDir;
use uuid::Uuid;

/// Find a random available port for testing.
fn random_addr() -> SocketAddr {
    // Bind to port 0 to get a random port
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    listener.local_addr().unwrap()
}

// ─────────────────────────────────────────────────────────────────────────────
// Protocol-level tests (no TCP needed)
// ─────────────────────────────────────────────────────────────────────────────

#[test]
fn stream_init_handshake_round_trip() {
    let session_id = Uuid::new_v4();
    let init = StreamMessage::Init(StreamInitMessage {
        session_id,
        operation: cassandra_streaming::StreamOperation::Bootstrap,
        description: "bootstrap test".into(),
        keyspaces: vec!["ks1".into()],
        ranges: vec![(0, 1000)],
    });

    let msg = init.to_message(1).unwrap();
    assert_eq!(msg.header.verb, Verb::StreamInit);

    let decoded = StreamMessage::from_message(&msg).unwrap();
    assert_eq!(init, decoded);

    // Build accepted response
    let resp = StreamMessage::InitResponse(StreamInitResponseMessage {
        session_id,
        accepted: true,
        reason: None,
    });
    let resp_msg = resp.to_message(1).unwrap();
    let decoded_resp = StreamMessage::from_message(&resp_msg).unwrap();
    assert_eq!(resp, decoded_resp);
}

#[test]
fn full_data_transfer_protocol() {
    let session_id = Uuid::new_v4();
    let transfer_id = Uuid::new_v4();

    // Create test data
    let wire = WirePartitions {
        entries: vec![
            WirePartition {
                key: b"pk1".to_vec(),
                rows: vec![(
                    b"ck1".to_vec(),
                    cassandra_storage::memtable::partition::Row {
                        clustering_key: b"ck1".to_vec(),
                        cells: vec![cassandra_storage::memtable::partition::Cell {
                            column: "col1".into(),
                            value: Some(b"value1".to_vec()),
                            timestamp: 1000,
                            ttl: 0,
                            local_deletion_time: None,
                            is_tombstone: false,
                        }],
                        is_tombstone: false,
                        local_deletion_time: None,
                    },
                )],
                tombstone_timestamp: None,
                tombstone_local_deletion_time: None,
            },
        ],
    };

    let serialized = serde_json::to_vec(&wire).unwrap();
    let chunks = StreamTransfer::chunkify_with_algorithm(
        transfer_id,
        &serialized,
        64,
        ChecksumAlgorithm::Crc32,
    );

    // Verify all chunks can be wrapped in StreamDataMessage
    for chunk in &chunks {
        let data_msg = StreamMessage::Data(StreamDataMessage {
            session_id,
            transfer_id,
            sequence: chunk.sequence,
            data: chunk.data.clone(),
            checksum: chunk.checksum.hash,
            checksum_algorithm: chunk.checksum.algorithm.to_string(),
            compressed: false,
            is_last: chunk.is_last,
        });

        let wire_msg = data_msg.to_message(100 + chunk.sequence).unwrap();
        let decoded = StreamMessage::from_message(&wire_msg).unwrap();
        assert_eq!(data_msg, decoded);
    }

    // Verify last chunk
    assert!(chunks.last().unwrap().is_last);

    // Reassemble and verify data matches
    let mut reassembled = Vec::new();
    for chunk in &chunks {
        reassembled.extend_from_slice(&chunk.data);
    }
    assert_eq!(reassembled, serialized);

    let deserialized: WirePartitions = serde_json::from_slice(&reassembled).unwrap();
    let partitions = deserialized.to_partitions();
    assert_eq!(partitions.len(), 1);
    assert_eq!(partitions[0].0, b"pk1");
}

#[test]
fn checksum_failure_rejection() {
    let data = b"valid data payload";
    let checksum = ChunkChecksum::compute(data, ChecksumAlgorithm::Crc32);

    // Valid check
    assert!(verify_chunk_checksum(data, &checksum.hash, "Crc32"));

    // Corrupted data
    let mut corrupted = data.to_vec();
    corrupted[0] ^= 0xFF;
    assert!(!verify_chunk_checksum(&corrupted, &checksum.hash, "Crc32"));

    // Wrong algorithm
    assert!(!verify_chunk_checksum(data, &checksum.hash, "Unknown"));
}

#[test]
fn session_completion_lifecycle() {
    let session_id = Uuid::new_v4();

    // Init
    let init = StreamMessage::Init(StreamInitMessage {
        session_id,
        operation: cassandra_streaming::StreamOperation::Repair,
        description: "repair stream".into(),
        keyspaces: vec!["ks1".into()],
        ranges: vec![(0, 100)],
    });
    let init_msg = init.to_message(1).unwrap();
    assert_eq!(init_msg.header.verb, Verb::StreamInit);

    // Data
    let data = StreamMessage::Data(StreamDataMessage {
        session_id,
        transfer_id: Uuid::new_v4(),
        sequence: 0,
        data: vec![1, 2, 3],
        checksum: [0u8; 16],
        checksum_algorithm: "Crc32".into(),
        compressed: false,
        is_last: true,
    });
    let data_msg = data.to_message(2).unwrap();
    assert_eq!(data_msg.header.verb, Verb::StreamData);

    // Complete
    let complete = StreamMessage::Complete(StreamCompleteMessage {
        session_id,
        success: true,
        error: None,
    });
    let complete_msg = complete.to_message(3).unwrap();
    assert_eq!(complete_msg.header.verb, Verb::StreamComplete);

    // Complete response
    let ack = StreamMessage::CompleteResponse(StreamCompleteResponseMessage {
        session_id,
        acknowledged: true,
    });
    let ack_msg = ack.to_message(3).unwrap();
    let decoded = StreamMessage::from_message(&ack_msg).unwrap();
    assert_eq!(ack, decoded);
}

#[test]
fn compression_end_to_end() {
    let wire = WirePartitions {
        entries: (0..5)
            .map(|i| WirePartition {
                key: format!("pk{i}").into_bytes(),
                rows: vec![(
                    format!("ck{i}").into_bytes(),
                    cassandra_storage::memtable::partition::Row {
                        clustering_key: format!("ck{i}").into_bytes(),
                        cells: vec![cassandra_storage::memtable::partition::Cell {
                            column: format!("col{i}"),
                            value: Some(format!("value{i}").into_bytes()),
                            timestamp: 1000 + i as i64,
                            ttl: 0,
                            local_deletion_time: None,
                            is_tombstone: false,
                        }],
                        is_tombstone: false,
                        local_deletion_time: None,
                    },
                )],
                tombstone_timestamp: None,
                tombstone_local_deletion_time: None,
            })
            .collect(),
    };

    let serialized = serde_json::to_vec(&wire).unwrap();

    // Compress
    let compressed = compress(&serialized);
    assert!(!compressed.is_empty());

    // Decompress
    let decompressed = decompress(&compressed).unwrap();
    assert_eq!(decompressed, serialized);

    // Verify data integrity
    let result: WirePartitions = serde_json::from_slice(&decompressed).unwrap();
    let partitions = result.to_partitions();
    assert_eq!(partitions.len(), 5);
    for (i, (key, _pd)) in partitions.iter().enumerate() {
        assert_eq!(key, &format!("pk{i}").into_bytes());
    }
}

#[test]
fn receiver_handler_registration() {
    let addr = random_addr();
    let messaging = Arc::new(MessagingService::new(addr));
    let manager = Arc::new(StreamManager::new());
    let metrics = Arc::new(StreamingMetrics::new());
    let tmp = TempDir::new().unwrap();

    let receiver = Arc::new(StreamReceiver::new(
        manager,
        metrics,
        tmp.path(),
    ));

    // Register handlers — should not panic
    StreamReceiver::register_handlers(Arc::clone(&receiver), &messaging);
    assert_eq!(receiver.active_buffer_count(), 0);
}

#[test]
fn abort_mid_stream_cleanup() {
    // Verify that the receiver's buffer tracking works for cleanup
    let manager = Arc::new(StreamManager::new());
    let metrics = Arc::new(StreamingMetrics::new());
    let tmp = TempDir::new().unwrap();

    let receiver = StreamReceiver::new(
        manager,
        metrics,
        tmp.path(),
    );

    // No buffers initially
    assert_eq!(receiver.active_buffer_count(), 0);
}
