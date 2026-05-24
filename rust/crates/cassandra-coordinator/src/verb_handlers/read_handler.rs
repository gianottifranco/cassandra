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
use cassandra_storage::engine::StorageEngine;
use tracing::{debug, warn};

use crate::read::{DataResponse, Digest};

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
    /// Continue after this clustering key (exclusive).
    pub start_after: Option<Vec<u8>>,
    /// Maximum live rows to return for this request.
    pub row_limit: Option<usize>,
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
    /// Digest of the same local data response.
    pub digest: [u8; 8],
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
/// Reads data from the local storage engine and returns a full data response.
pub struct ReadDataVerbHandler;

impl ReadDataVerbHandler {
    /// Handle an incoming read data request when no local storage engine is configured.
    pub fn handle(msg: Message) -> Option<Message> {
        Some(Message::failure(
            msg.header.message_id,
            b"Local storage engine not configured for ReadData".to_vec(),
        ))
    }

    /// Handle an incoming read data request using the local storage engine.
    pub fn handle_with_storage(msg: Message, storage: &StorageEngine) -> Option<Message> {
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

        let response = read_data_response(storage, &request);
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
/// Reads data from the local storage engine, computes a digest hash, and returns it.
pub struct ReadDigestVerbHandler;

impl ReadDigestVerbHandler {
    /// Handle an incoming read digest request when no local storage engine is configured.
    pub fn handle(msg: Message) -> Option<Message> {
        Some(Message::failure(
            msg.header.message_id,
            b"Local storage engine not configured for ReadDigest".to_vec(),
        ))
    }

    /// Handle an incoming read digest request using the local storage engine.
    pub fn handle_with_storage(msg: Message, storage: &StorageEngine) -> Option<Message> {
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

        let response = read_digest_response(storage, &request);
        let payload = serde_json::to_vec(&response).unwrap_or_default();

        Some(Message::response(
            msg.header.message_id,
            Verb::ReadDigestResponse,
            payload,
        ))
    }
}

fn read_data_response(
    storage: &StorageEngine,
    request: &ReadDataRequest,
) -> ReadDataResponsePayload {
    let now_seconds = current_unix_seconds();
    let data =
        match storage.read_partition(&request.keyspace, &request.table, &request.partition_key) {
            Some(data) => data,
            None => {
                return ReadDataResponsePayload {
                    partitions: Vec::new(),
                    digest: Digest::empty().0,
                    tombstones_read: 0,
                    is_short_read: false,
                };
            }
        };

    let (bounded_data, is_short_read) =
        apply_bounds_to_partition(data, request.start_after.as_deref(), request.row_limit);
    let digest = Digest::from_partition(&bounded_data).0;
    let mut response =
        DataResponse::from_partition(request.partition_key.clone(), bounded_data, now_seconds);
    response.is_short_read = is_short_read;
    ReadDataResponsePayload {
        partitions: response
            .partitions
            .into_iter()
            .map(|partition| WirePartitionResult {
                partition_key: partition.partition_key,
                data: partition
                    .data
                    .as_ref()
                    .map(encode_partition_data)
                    .unwrap_or_default(),
                live_row_count: partition.live_row_count,
                was_truncated: partition.was_truncated,
            })
            .collect(),
        digest,
        tombstones_read: response.tombstones_read,
        is_short_read: response.is_short_read,
    }
}

fn apply_bounds_to_partition(
    data: cassandra_storage::memtable::partition::PartitionData,
    start_after: Option<&[u8]>,
    row_limit: Option<usize>,
) -> (cassandra_storage::memtable::partition::PartitionData, bool) {
    let mut bounded = cassandra_storage::memtable::partition::PartitionData::new();
    if let (Some(ts), Some(ldt)) = (data.tombstone_timestamp, data.tombstone_local_deletion_time) {
        bounded.set_tombstone(ts, ldt);
    }

    let mut rows = data
        .rows
        .into_iter()
        .filter(|(clustering_key, _)| {
            start_after
                .map(|start| clustering_key.as_slice() > start)
                .unwrap_or(true)
        })
        .collect::<Vec<_>>();
    let mut is_short_read = false;
    if let Some(limit) = row_limit {
        if rows.len() > limit {
            rows.truncate(limit);
            is_short_read = true;
        }
    }
    for (_, row) in rows {
        bounded.apply_row(row);
    }

    (bounded, is_short_read)
}

fn read_digest_response(
    storage: &StorageEngine,
    request: &ReadDigestRequest,
) -> ReadDigestResponsePayload {
    match storage.read_partition(&request.keyspace, &request.table, &request.partition_key) {
        Some(data) => ReadDigestResponsePayload {
            digest: Digest::from_partition(&data).0,
        },
        None => ReadDigestResponsePayload {
            digest: Digest::empty().0,
        },
    }
}

fn current_unix_seconds() -> i32 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i32
}

fn encode_partition_data(data: &cassandra_storage::memtable::partition::PartitionData) -> Vec<u8> {
    let mut out = Vec::new();
    write_opt_i64(&mut out, data.tombstone_timestamp);
    write_opt_i32(&mut out, data.tombstone_local_deletion_time);
    write_u32(&mut out, data.rows.len());

    for (clustering_key, row) in &data.rows {
        write_bytes(&mut out, clustering_key);
        out.push(u8::from(row.is_tombstone));
        write_opt_i32(&mut out, row.local_deletion_time);
        write_u32(&mut out, row.cells.len());

        for cell in &row.cells {
            write_bytes(&mut out, cell.column.as_bytes());
            match &cell.value {
                Some(value) => {
                    out.push(1);
                    write_bytes(&mut out, value);
                }
                None => out.push(0),
            }
            out.extend_from_slice(&cell.timestamp.to_be_bytes());
            out.extend_from_slice(&cell.ttl.to_be_bytes());
            write_opt_i32(&mut out, cell.local_deletion_time);
            out.push(u8::from(cell.is_tombstone));
        }
    }

    out
}

fn write_u32(out: &mut Vec<u8>, value: usize) {
    out.extend_from_slice(&(value as u32).to_be_bytes());
}

fn write_bytes(out: &mut Vec<u8>, value: &[u8]) {
    write_u32(out, value.len());
    out.extend_from_slice(value);
}

fn write_opt_i64(out: &mut Vec<u8>, value: Option<i64>) {
    match value {
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&value.to_be_bytes());
        }
        None => out.push(0),
    }
}

fn write_opt_i32(out: &mut Vec<u8>, value: Option<i32>) {
    match value {
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&value.to_be_bytes());
        }
        None => out.push(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_storage::commitlog::{CellMutation, CommitLogConfig, Mutation, MutationRow};
    use cassandra_storage::engine::EngineConfig;
    use tempfile::TempDir;

    fn test_storage() -> (StorageEngine, TempDir) {
        let temp = TempDir::new().unwrap();
        let storage = StorageEngine::open(EngineConfig {
            data_directories: vec![temp.path().join("data")],
            commitlog: CommitLogConfig {
                directory: temp.path().join("commitlog"),
                ..CommitLogConfig::default()
            },
            ..EngineConfig::default()
        })
        .unwrap();
        (storage, temp)
    }

    fn write_cell(storage: &StorageEngine, keyspace: &str, table: &str, pk: &[u8], value: &[u8]) {
        storage
            .apply_mutation(&Mutation {
                keyspace: keyspace.to_string(),
                table: table.to_string(),
                partition_key: pk.to_vec(),
                rows: vec![MutationRow {
                    clustering_key: Vec::new(),
                    cells: vec![CellMutation {
                        column: "v".to_string(),
                        value: Some(value.to_vec()),
                        timestamp: 1_000,
                        ttl: 0,
                        local_deletion_time: None,
                        is_tombstone: false,
                    }],
                    is_tombstone: false,
                    local_deletion_time: None,
                }],
                timestamp: 1_000,
                cdc_enabled: false,
                static_cells: Vec::new(),
                partition_tombstone: None,
                range_tombstones: Vec::new(),
            })
            .unwrap();
    }

    #[test]
    fn read_data_request_roundtrip() {
        let request = ReadDataRequest {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            partition_key: vec![1, 2, 3],
            start_after: None,
            row_limit: None,
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
    fn handle_read_data_without_storage_fails() {
        let request = ReadDataRequest {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            partition_key: vec![1],
            start_after: None,
            row_limit: None,
        };
        let payload = serde_json::to_vec(&request).unwrap();
        let msg = Message::request(Verb::ReadData, 10, payload);

        let response = ReadDataVerbHandler::handle(msg);
        assert!(response.is_some());
        let resp = response.unwrap();
        assert!(resp.is_failure());
    }

    #[test]
    fn handle_read_data_invalid() {
        let msg = Message::request(Verb::ReadData, 10, b"bad".to_vec());
        let response = ReadDataVerbHandler::handle(msg);
        assert!(response.is_some());
        assert!(response.unwrap().is_failure());
    }

    #[test]
    fn handle_read_data_reads_local_storage() {
        let (storage, _temp) = test_storage();
        write_cell(&storage, "ks", "tbl", b"pk1", b"value1");
        let request = ReadDataRequest {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            partition_key: b"pk1".to_vec(),
            start_after: None,
            row_limit: None,
        };
        let payload = serde_json::to_vec(&request).unwrap();
        let msg = Message::request(Verb::ReadData, 10, payload);

        let response = ReadDataVerbHandler::handle_with_storage(msg, &storage);
        assert!(response.is_some());
        let resp = response.unwrap();
        assert_eq!(resp.header.verb, Verb::ReadDataResponse);
        assert!(resp.is_response());

        let body: ReadDataResponsePayload = serde_json::from_slice(&resp.payload).unwrap();
        assert_eq!(body.partitions.len(), 1);
        assert_eq!(body.partitions[0].partition_key, b"pk1".to_vec());
        assert_eq!(body.partitions[0].live_row_count, 1);
        assert!(!body.partitions[0].data.is_empty());
        assert_eq!(body.tombstones_read, 0);
    }

    #[test]
    fn handle_read_digest_without_storage_fails() {
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
        assert!(resp.is_failure());
    }

    #[test]
    fn handle_read_digest_reads_local_storage() {
        let (storage, _temp) = test_storage();
        write_cell(&storage, "ks", "tbl", b"pk1", b"value1");
        let request = ReadDigestRequest {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            partition_key: b"pk1".to_vec(),
        };
        let payload = serde_json::to_vec(&request).unwrap();
        let msg = Message::request(Verb::ReadDigest, 20, payload);

        let response = ReadDigestVerbHandler::handle_with_storage(msg, &storage);
        assert!(response.is_some());
        let resp = response.unwrap();
        assert_eq!(resp.header.verb, Verb::ReadDigestResponse);

        let body: ReadDigestResponsePayload = serde_json::from_slice(&resp.payload).unwrap();
        assert_ne!(body.digest, [0u8; 8]);
    }

    #[test]
    fn handle_read_digest_invalid() {
        let msg = Message::request(Verb::ReadDigest, 20, b"bad".to_vec());
        let response = ReadDigestVerbHandler::handle(msg);
        assert!(response.is_some());
        assert!(response.unwrap().is_failure());
    }

    #[test]
    fn handle_read_data_applies_start_after_and_row_limit() {
        let (storage, _temp) = test_storage();
        storage
            .apply_mutation(&Mutation {
                keyspace: "ks".to_string(),
                table: "tbl".to_string(),
                partition_key: b"pk1".to_vec(),
                rows: vec![
                    MutationRow {
                        clustering_key: b"a".to_vec(),
                        cells: vec![CellMutation {
                            column: "v".to_string(),
                            value: Some(b"1".to_vec()),
                            timestamp: 1_000,
                            ttl: 0,
                            local_deletion_time: None,
                            is_tombstone: false,
                        }],
                        is_tombstone: false,
                        local_deletion_time: None,
                    },
                    MutationRow {
                        clustering_key: b"b".to_vec(),
                        cells: vec![CellMutation {
                            column: "v".to_string(),
                            value: Some(b"2".to_vec()),
                            timestamp: 1_001,
                            ttl: 0,
                            local_deletion_time: None,
                            is_tombstone: false,
                        }],
                        is_tombstone: false,
                        local_deletion_time: None,
                    },
                    MutationRow {
                        clustering_key: b"c".to_vec(),
                        cells: vec![CellMutation {
                            column: "v".to_string(),
                            value: Some(b"3".to_vec()),
                            timestamp: 1_002,
                            ttl: 0,
                            local_deletion_time: None,
                            is_tombstone: false,
                        }],
                        is_tombstone: false,
                        local_deletion_time: None,
                    },
                ],
                timestamp: 1_000,
                cdc_enabled: false,
                static_cells: Vec::new(),
                partition_tombstone: None,
                range_tombstones: Vec::new(),
            })
            .unwrap();

        let request = ReadDataRequest {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            partition_key: b"pk1".to_vec(),
            start_after: Some(b"a".to_vec()),
            row_limit: Some(1),
        };
        let msg = Message::request(Verb::ReadData, 10, serde_json::to_vec(&request).unwrap());
        let response = ReadDataVerbHandler::handle_with_storage(msg, &storage).unwrap();
        let body: ReadDataResponsePayload = serde_json::from_slice(&response.payload).unwrap();

        assert_eq!(body.partitions.len(), 1);
        assert_eq!(body.partitions[0].live_row_count, 1);
        assert!(body.is_short_read);
    }
}
