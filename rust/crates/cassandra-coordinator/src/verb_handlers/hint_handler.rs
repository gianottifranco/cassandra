// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
// SPDX-License-Identifier: Apache-2.0

//! Replica-side hint verb handler.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.hints.HintVerbHandler`
//!
//! Receives `Verb::Hint`, deserializes the hint (containing a mutation
//! for this node), applies it locally, and returns `Verb::HintResponse`.

use cassandra_messaging::frame::Message;
use cassandra_messaging::verb::Verb;
use cassandra_storage::engine::StorageEngine;
use tracing::{debug, warn};

use super::mutation_handler::to_storage_mutation;
use crate::write::CoordinatedMutation;

// ─── Payload Types ──────────────────────────────────────────────

/// Hint delivery request: a mutation that was stored as a hint and
/// is now being delivered to the recovered replica.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HintRequest {
    /// The original mutation to apply.
    pub mutation: CoordinatedMutation,
    /// Hint ID for deduplication.
    pub hint_id: u64,
    /// When the hint was created (epoch millis).
    pub created_at: i64,
}

/// Hint delivery response.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct HintResponsePayload {
    /// Whether the hint was applied successfully.
    pub success: bool,
}

// ─── Handler ────────────────────────────────────────────────────

/// Replica-side handler for `Verb::Hint` messages.
pub struct HintVerbHandler;

impl HintVerbHandler {
    /// Handle an incoming hint delivery message when no local storage engine is configured.
    pub fn handle(msg: Message) -> Option<Message> {
        Some(Message::failure(
            msg.header.message_id,
            b"Local storage engine not configured for Hint".to_vec(),
        ))
    }

    /// Handle an incoming hint delivery message using the local storage engine.
    pub fn handle_with_storage(msg: Message, storage: &StorageEngine) -> Option<Message> {
        let request: HintRequest = match serde_json::from_slice(&msg.payload) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "Failed to deserialize hint request");
                return Some(Message::failure(
                    msg.header.message_id,
                    format!("Deserialization error: {e}").into_bytes(),
                ));
            }
        };

        debug!(
            hint_id = request.hint_id,
            keyspace = %request.mutation.keyspace,
            table = %request.mutation.table,
            "Applying hint locally"
        );

        let storage_mutation = to_storage_mutation(request.mutation);
        if let Err(e) = storage.apply_mutation(&storage_mutation) {
            return Some(Message::failure(
                msg.header.message_id,
                format!("Storage apply error: {e}").into_bytes(),
            ));
        }

        let response = HintResponsePayload { success: true };
        let payload = serde_json::to_vec(&response).unwrap_or_default();

        Some(Message::response(
            msg.header.message_id,
            Verb::HintResponse,
            payload,
        ))
    }
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

    fn make_hint_request() -> HintRequest {
        HintRequest {
            mutation: CoordinatedMutation::simple(
                "ks".to_string(),
                "tbl".to_string(),
                vec![1],
                vec![MutationRow {
                    clustering_key: vec![2],
                    cells: vec![CellMutation {
                        column: "v".to_string(),
                        value: Some(b"hinted".to_vec()),
                        timestamp: 1000,
                        ttl: 0,
                        is_tombstone: false,
                        collection_op: None,
                    }],
                    is_tombstone: false,
                    range_tombstone: None,
                }],
                1000,
            ),
            hint_id: 42,
            created_at: 1_700_000_000_000,
        }
    }

    #[test]
    fn hint_request_roundtrip() {
        let req = make_hint_request();
        let bytes = serde_json::to_vec(&req).unwrap();
        let decoded: HintRequest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.hint_id, 42);
        assert_eq!(decoded.mutation.keyspace, "ks");
    }

    #[test]
    fn handle_valid_hint_without_storage_fails() {
        let req = make_hint_request();
        let payload = serde_json::to_vec(&req).unwrap();
        let msg = Message::request(Verb::Hint, 100, payload);

        let response = HintVerbHandler::handle(msg);
        assert!(response.is_some());
        let resp = response.unwrap();
        assert!(resp.is_failure());
    }

    #[test]
    fn handle_valid_hint_applies_to_storage() {
        let (storage, _temp) = test_storage();
        let req = make_hint_request();
        let payload = serde_json::to_vec(&req).unwrap();
        let msg = Message::request(Verb::Hint, 100, payload);

        let response = HintVerbHandler::handle_with_storage(msg, &storage);
        assert!(response.is_some());
        let resp = response.unwrap();
        assert_eq!(resp.header.verb, Verb::HintResponse);
        assert!(resp.is_response());

        let body: HintResponsePayload = serde_json::from_slice(&resp.payload).unwrap();
        assert!(body.success);
        let partition = storage.read_partition("ks", "tbl", &[1]).unwrap();
        let row = partition.rows.get(&vec![2]).unwrap();
        assert_eq!(row.cells[0].value.as_deref(), Some(b"hinted".as_slice()));
    }

    #[test]
    fn handle_invalid_hint() {
        let msg = Message::request(Verb::Hint, 100, b"garbage".to_vec());
        let response = HintVerbHandler::handle(msg);
        assert!(response.is_some());
        assert!(response.unwrap().is_failure());
    }
}
