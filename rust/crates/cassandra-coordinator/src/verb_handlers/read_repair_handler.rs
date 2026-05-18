// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
// SPDX-License-Identifier: Apache-2.0

//! Replica-side read repair verb handler.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.service.reads.repair.ReadRepairHandler`
//!
//! Receives `Verb::ReadRepair`, deserializes the repair mutation,
//! applies it locally, and returns `Verb::ReadRepairResponse`.

use cassandra_messaging::frame::Message;
use cassandra_messaging::verb::Verb;
use cassandra_storage::engine::StorageEngine;
use tracing::{debug, warn};

use super::mutation_handler::to_storage_mutation;
use crate::write::{CellMutation, CoordinatedMutation, MutationRow};

// ─── Payload Types ──────────────────────────────────────────────

/// Read repair request: a mutation to bring this replica up to date.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReadRepairRequest {
    /// The repair mutation to apply.
    pub mutation: CoordinatedMutation,
}

/// Read repair response.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct ReadRepairResponse {
    /// Whether the repair was applied successfully.
    pub success: bool,
}

// ─── Handler ────────────────────────────────────────────────────

/// Replica-side handler for `Verb::ReadRepair` messages.
pub struct ReadRepairVerbHandler;

impl ReadRepairVerbHandler {
    /// Handle an incoming read repair message when no local storage engine is configured.
    pub fn handle(msg: Message) -> Option<Message> {
        Some(Message::failure(
            msg.header.message_id,
            b"Local storage engine not configured for ReadRepair".to_vec(),
        ))
    }

    /// Handle an incoming read repair message using the local storage engine.
    pub fn handle_with_storage(msg: Message, storage: &StorageEngine) -> Option<Message> {
        let request: ReadRepairRequest = match decode_read_repair_request(&msg.payload) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "Failed to deserialize read repair request");
                return Some(Message::failure(
                    msg.header.message_id,
                    format!("Deserialization error: {e}").into_bytes(),
                ));
            }
        };

        debug!(
            keyspace = %request.mutation.keyspace,
            table = %request.mutation.table,
            "Applying read repair locally"
        );

        let storage_mutation = to_storage_mutation(request.mutation);
        if let Err(e) = storage.apply_mutation(&storage_mutation) {
            return Some(Message::failure(
                msg.header.message_id,
                format!("Storage apply error: {e}").into_bytes(),
            ));
        }

        let response = ReadRepairResponse { success: true };
        let payload = serde_json::to_vec(&response).unwrap_or_default();

        Some(Message::response(
            msg.header.message_id,
            Verb::ReadRepairResponse,
            payload,
        ))
    }
}

fn decode_read_repair_request(payload: &[u8]) -> Result<ReadRepairRequest, String> {
    serde_json::from_slice(payload)
        .map_err(|json_err| json_err.to_string())
        .or_else(|json_err| {
            decode_staged_repair_payload(payload).map_err(|binary_err| {
                format!("json error: {json_err}; staged repair payload error: {binary_err}")
            })
        })
}

fn decode_staged_repair_payload(payload: &[u8]) -> Result<ReadRepairRequest, String> {
    let mut cursor = payload;
    let keyspace = read_utf8_field(&mut cursor, "keyspace")?;
    let table = read_utf8_field(&mut cursor, "table")?;
    let partition_key = read_len_prefixed_bytes(&mut cursor, "partition key")?;
    let row_count = read_u32(&mut cursor, "row count")? as usize;
    let mut rows = Vec::with_capacity(row_count);
    let mut mutation_timestamp = 0;

    for _ in 0..row_count {
        let clustering_key = read_len_prefixed_bytes(&mut cursor, "clustering key")?;
        let cell_count = read_u32(&mut cursor, "cell count")? as usize;
        let mut cells = Vec::with_capacity(cell_count);
        for _ in 0..cell_count {
            let column = read_utf8_field(&mut cursor, "column")?;
            let value = read_len_prefixed_bytes(&mut cursor, "value")?;
            let timestamp = read_i64(&mut cursor, "timestamp")?;
            mutation_timestamp = mutation_timestamp.max(timestamp);
            cells.push(CellMutation {
                column,
                is_tombstone: value.is_empty(),
                value: if value.is_empty() { None } else { Some(value) },
                timestamp,
                ttl: 0,
                collection_op: None,
            });
        }
        rows.push(MutationRow {
            clustering_key,
            cells,
            is_tombstone: false,
            range_tombstone: None,
        });
    }

    if !cursor.is_empty() {
        return Err(format!("{} trailing bytes", cursor.len()));
    }

    Ok(ReadRepairRequest {
        mutation: CoordinatedMutation::simple(
            keyspace,
            table,
            partition_key,
            rows,
            mutation_timestamp,
        ),
    })
}

fn read_utf8_field(input: &mut &[u8], field: &str) -> Result<String, String> {
    let bytes = read_len_prefixed_bytes(input, field)?;
    String::from_utf8(bytes).map_err(|err| format!("{field} is not UTF-8: {err}"))
}

fn read_len_prefixed_bytes(input: &mut &[u8], field: &str) -> Result<Vec<u8>, String> {
    let len = read_u32(input, field)? as usize;
    if input.len() < len {
        return Err(format!(
            "{field} length {len} exceeds remaining payload {}",
            input.len()
        ));
    }
    let (head, tail) = input.split_at(len);
    *input = tail;
    Ok(head.to_vec())
}

fn read_u32(input: &mut &[u8], field: &str) -> Result<u32, String> {
    if input.len() < 4 {
        return Err(format!("{field} is truncated"));
    }
    let (head, tail) = input.split_at(4);
    *input = tail;
    Ok(u32::from_be_bytes(head.try_into().unwrap()))
}

fn read_i64(input: &mut &[u8], field: &str) -> Result<i64, String> {
    if input.len() < 8 {
        return Err(format!("{field} is truncated"));
    }
    let (head, tail) = input.split_at(8);
    *input = tail;
    Ok(i64::from_be_bytes(head.try_into().unwrap()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::{CellMutation, CoordinatedMutation, MutationRow};
    use cassandra_storage::commitlog::CommitLogConfig;
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

    fn make_repair_request() -> ReadRepairRequest {
        ReadRepairRequest {
            mutation: CoordinatedMutation::simple(
                "ks".to_string(),
                "tbl".to_string(),
                vec![1, 2],
                vec![MutationRow {
                    clustering_key: vec![3],
                    cells: vec![CellMutation {
                        column: "v".to_string(),
                        value: Some(b"repaired".to_vec()),
                        timestamp: 2000,
                        ttl: 0,
                        is_tombstone: false,
                        collection_op: None,
                    }],
                    is_tombstone: false,
                    range_tombstone: None,
                }],
                2000,
            ),
        }
    }

    #[test]
    fn read_repair_request_roundtrip() {
        let req = make_repair_request();
        let bytes = serde_json::to_vec(&req).unwrap();
        let decoded: ReadRepairRequest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.mutation.keyspace, "ks");
        assert_eq!(decoded.mutation.timestamp, 2000);
    }

    #[test]
    fn handle_valid_read_repair_without_storage_fails() {
        let req = make_repair_request();
        let payload = serde_json::to_vec(&req).unwrap();
        let msg = Message::request(Verb::ReadRepair, 70, payload);

        let response = ReadRepairVerbHandler::handle(msg);
        assert!(response.is_some());
        let resp = response.unwrap();
        assert!(resp.is_failure());
    }

    #[test]
    fn handle_valid_read_repair_applies_to_storage() {
        let (storage, _temp) = test_storage();
        let req = make_repair_request();
        let payload = serde_json::to_vec(&req).unwrap();
        let msg = Message::request(Verb::ReadRepair, 70, payload);

        let response = ReadRepairVerbHandler::handle_with_storage(msg, &storage);
        assert!(response.is_some());
        let resp = response.unwrap();
        assert_eq!(resp.header.verb, Verb::ReadRepairResponse);
        assert!(resp.is_response());

        let body: ReadRepairResponse = serde_json::from_slice(&resp.payload).unwrap();
        assert!(body.success);
        let partition = storage.read_partition("ks", "tbl", &[1, 2]).unwrap();
        let row = partition.rows.get(&vec![3]).unwrap();
        assert_eq!(row.cells[0].value.as_deref(), Some(b"repaired".as_slice()));
    }

    #[test]
    fn handle_staged_read_repair_payload_applies_to_storage() {
        let (storage, _temp) = test_storage();
        let mut payload = Vec::new();
        write_len_prefixed(&mut payload, b"ks");
        write_len_prefixed(&mut payload, b"tbl");
        write_len_prefixed(&mut payload, &[1, 2]);
        payload.extend_from_slice(&1u32.to_be_bytes());
        write_len_prefixed(&mut payload, &[3]);
        payload.extend_from_slice(&1u32.to_be_bytes());
        write_len_prefixed(&mut payload, b"v");
        write_len_prefixed(&mut payload, b"repaired");
        payload.extend_from_slice(&2000i64.to_be_bytes());
        let msg = Message::request(Verb::ReadRepair, 70, payload);

        let response = ReadRepairVerbHandler::handle_with_storage(msg, &storage);
        assert!(response.is_some());
        let resp = response.unwrap();
        assert_eq!(resp.header.verb, Verb::ReadRepairResponse);
        assert!(resp.is_response());

        let body: ReadRepairResponse = serde_json::from_slice(&resp.payload).unwrap();
        assert!(body.success);
        let partition = storage.read_partition("ks", "tbl", &[1, 2]).unwrap();
        let row = partition.rows.get(&vec![3]).unwrap();
        assert_eq!(row.cells[0].value.as_deref(), Some(b"repaired".as_slice()));
    }

    #[test]
    fn handle_invalid_read_repair() {
        let msg = Message::request(Verb::ReadRepair, 70, b"bad".to_vec());
        let response = ReadRepairVerbHandler::handle(msg);
        assert!(response.is_some());
        assert!(response.unwrap().is_failure());
    }

    fn write_len_prefixed(out: &mut Vec<u8>, bytes: &[u8]) {
        out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        out.extend_from_slice(bytes);
    }
}
