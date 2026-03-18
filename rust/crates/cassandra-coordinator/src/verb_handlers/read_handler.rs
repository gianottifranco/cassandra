// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
// SPDX-License-Identifier: Apache-2.0

//! Replica-side read verb handlers for data and digest reads.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.db.ReadCommandVerbHandler`
//!
//! `ReadDataVerbHandler` reads from local storage and returns a full data
//! response. `ReadDigestVerbHandler` computes a digest hash and returns it.

use cassandra_messaging::frame::Message;
use cassandra_messaging::verb::Verb;
use tracing::{debug, warn};

// ─── Request / Response Types ───────────────────────────────────

/// Read request payload sent from coordinator to replica.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReadDataRequest {
    /// Keyspace to read from.
    pub keyspace: String,
    /// Table to read from.
    pub table: String,
    /// Serialized partition key.
    pub partition_key: Vec<u8>,
}

/// Wire-format partition result for serialization over messaging.
///
/// This is a simplified version of `PartitionResult` that can be
/// serialized/deserialized over the wire. The full `PartitionResult`
/// contains storage-layer types that aren't serializable.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct WirePartitionResult {
    /// Partition key.
    pub partition_key: Vec<u8>,
    /// Serialized row data (opaque bytes from storage layer).
    pub data: Vec<u8>,
    /// Number of live rows.
    pub live_row_count: usize,
    /// Whether this partition was truncated by per-partition limit.
    pub was_truncated: bool,
}

/// Read data response payload.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReadDataResponsePayload {
    /// Partition results from the local read.
    pub partitions: Vec<WirePartitionResult>,
    /// Number of tombstones encountered.
    pub tombstones_read: u32,
    /// Whether the read was short (more data available).
    pub is_short_read: bool,
}

/// Read digest request payload.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReadDigestRequest {
    /// Keyspace to read from.
    pub keyspace: String,
    /// Table to read from.
    pub table: String,
    /// Serialized partition key.
    pub partition_key: Vec<u8>,
}

/// Read digest response payload.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReadDigestResponsePayload {
    /// The digest hash of the data.
    pub digest: [u8; 8],
}

// ─── ReadData Handler ───────────────────────────────────────────

/// Replica-side handler for `Verb::ReadData` messages.
///
/// Reads data from the local storage engine (stubbed) and returns
/// a full data response.
pub struct ReadDataVerbHandler;

impl ReadDataVerbHandler {
    /// Handle an incoming read data request.
    pub fn handle(msg: Message) -> Option<Message> {
        let request: ReadDataRequest = match serde_json::from_slice(&msg.payload) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "Failed to deserialize read data request");
                return Some(Message::failure(
                    msg.header.message_id,
                    format!("Deserialization error: {e}").into_bytes(),
                ));
            }
        };

        debug!(
            keyspace = %request.keyspace,
            table = %request.table,
            "Reading data locally"
        );

        // In a full implementation, this would read from the local storage engine.
        // For now, return an empty data response.
        let response = ReadDataResponsePayload {
            partitions: Vec::new(),
            tombstones_read: 0,
            is_short_read: false,
        };
        let payload = serde_json::to_vec(&response).unwrap_or_default();

        Some(Message::response(
            msg.header.message_id,
            Verb::ReadDataResponse,
            payload,
        ))
    }
}

// ─── ReadDigest Handler ─────────────────────────────────────────

/// Replica-side handler for `Verb::ReadDigest` messages.
///
/// Reads data from the local storage engine (stubbed), computes a
/// digest hash, and returns it.
pub struct ReadDigestVerbHandler;

impl ReadDigestVerbHandler {
    /// Handle an incoming read digest request.
    pub fn handle(msg: Message) -> Option<Message> {
        let request: ReadDigestRequest = match serde_json::from_slice(&msg.payload) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "Failed to deserialize read digest request");
                return Some(Message::failure(
                    msg.header.message_id,
                    format!("Deserialization error: {e}").into_bytes(),
                ));
            }
        };

        debug!(
            keyspace = %request.keyspace,
            table = %request.table,
            "Computing digest locally"
        );

        // In a full implementation, this would read data and compute a real digest.
        // For now, return a zeroed digest (empty partition).
        let response = ReadDigestResponsePayload { digest: [0u8; 8] };
        let payload = serde_json::to_vec(&response).unwrap_or_default();

        Some(Message::response(
            msg.header.message_id,
            Verb::ReadDigestResponse,
            payload,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn read_data_request_roundtrip() {
        let request = ReadDataRequest {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            partition_key: vec![1, 2, 3],
        };
        let bytes = serde_json::to_vec(&request).unwrap();
        let decoded: ReadDataRequest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.keyspace, "ks");
        assert_eq!(decoded.partition_key, vec![1, 2, 3]);
    }

    #[test]
    fn read_digest_request_roundtrip() {
        let request = ReadDigestRequest {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            partition_key: vec![4, 5],
        };
        let bytes = serde_json::to_vec(&request).unwrap();
        let decoded: ReadDigestRequest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.table, "tbl");
    }

    #[test]
    fn wire_partition_result_roundtrip() {
        let pr = WirePartitionResult {
            partition_key: vec![1, 2],
            data: vec![3, 4, 5],
            live_row_count: 10,
            was_truncated: false,
        };
        let bytes = serde_json::to_vec(&pr).unwrap();
        let decoded: WirePartitionResult = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.partition_key, vec![1, 2]);
        assert_eq!(decoded.live_row_count, 10);
    }

    #[test]
    fn handle_read_data_valid() {
        let request = ReadDataRequest {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            partition_key: vec![1],
        };
        let payload = serde_json::to_vec(&request).unwrap();
        let msg = Message::request(Verb::ReadData, 10, payload);

        let response = ReadDataVerbHandler::handle(msg);
        assert!(response.is_some());
        let resp = response.unwrap();
        assert_eq!(resp.header.verb, Verb::ReadDataResponse);
        assert!(resp.is_response());

        let body: ReadDataResponsePayload = serde_json::from_slice(&resp.payload).unwrap();
        assert!(body.partitions.is_empty());
        assert_eq!(body.tombstones_read, 0);
    }

    #[test]
    fn handle_read_data_invalid() {
        let msg = Message::request(Verb::ReadData, 10, b"bad".to_vec());
        let response = ReadDataVerbHandler::handle(msg);
        assert!(response.is_some());
        assert!(response.unwrap().is_failure());
    }

    #[test]
    fn handle_read_digest_valid() {
        let request = ReadDigestRequest {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            partition_key: vec![1],
        };
        let payload = serde_json::to_vec(&request).unwrap();
        let msg = Message::request(Verb::ReadDigest, 20, payload);

        let response = ReadDigestVerbHandler::handle(msg);
        assert!(response.is_some());
        let resp = response.unwrap();
        assert_eq!(resp.header.verb, Verb::ReadDigestResponse);

        let body: ReadDigestResponsePayload = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(body.digest, [0u8; 8]);
    }

    #[test]
    fn handle_read_digest_invalid() {
        let msg = Message::request(Verb::ReadDigest, 20, b"bad".to_vec());
        let response = ReadDigestVerbHandler::handle(msg);
        assert!(response.is_some());
        assert!(response.unwrap().is_failure());
    }
}
