// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
// SPDX-License-Identifier: Apache-2.0

//! Replica-side batchlog verb handlers.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.batchlog.BatchStoreVerbHandler`
//! - `org.apache.cassandra.batchlog.BatchRemoveVerbHandler`
//!
//! `BatchStoreVerbHandler` stores a batch log entry on the replica.
//! `BatchRemoveVerbHandler` removes a batch log entry by UUID after
//! the batch has been fully applied.

use cassandra_messaging::frame::Message;
use cassandra_messaging::verb::Verb;
use tracing::{debug, warn};
use uuid::Uuid;

use crate::batch::{BatchEntry, BatchLogManager, BatchType};
use crate::write::CoordinatedMutation;

// ─── Payload Types ──────────────────────────────────────────────

/// Batch store request: store these mutations in the batchlog.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BatchStoreRequest {
    /// Unique batch ID.
    pub id: Uuid,
    /// Batch type (Logged, Unlogged, Counter).
    pub batch_type: BatchType,
    /// All mutations in the batch.
    pub mutations: Vec<CoordinatedMutation>,
    /// Creation timestamp (epoch millis).
    pub created_at: i64,
}

/// Batch store response.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BatchStoreResponse {
    /// Whether the batch was stored successfully.
    pub success: bool,
}

/// Batch remove request: remove a completed batch from the batchlog.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BatchRemoveRequest {
    /// Batch ID to remove.
    pub id: Uuid,
}

/// Batch remove response.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct BatchRemoveResponse {
    /// Whether the batch was found and removed.
    pub removed: bool,
}

// ─── BatchStore Handler ─────────────────────────────────────────

/// Replica-side handler for `Verb::BatchStore` messages.
pub struct BatchStoreVerbHandler;

impl BatchStoreVerbHandler {
    /// Handle an incoming batch store request when no local batchlog manager is configured.
    pub fn handle(msg: Message) -> Option<Message> {
        Some(Message::failure(
            msg.header.message_id,
            b"Local batchlog manager not configured for BatchStore".to_vec(),
        ))
    }

    /// Handle an incoming batch store request using the local batchlog manager.
    pub fn handle_with_batchlog(msg: Message, batchlog: &BatchLogManager) -> Option<Message> {
        let request: BatchStoreRequest = match serde_json::from_slice(&msg.payload) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "Failed to deserialize batch store request");
                return Some(Message::failure(
                    msg.header.message_id,
                    format!("Deserialization error: {e}").into_bytes(),
                ));
            }
        };

        debug!(
            batch_id = %request.id,
            batch_type = %request.batch_type,
            mutations = request.mutations.len(),
            "Storing batch log entry locally"
        );

        batchlog.store_entry(BatchEntry {
            id: request.id,
            batch_type: request.batch_type,
            mutations: request.mutations,
            created_at: request.created_at,
            version: 1,
        });
        let response = BatchStoreResponse { success: true };
        let payload = serde_json::to_vec(&response).unwrap_or_default();

        Some(Message::response(
            msg.header.message_id,
            Verb::BatchStoreResponse,
            payload,
        ))
    }
}

// ─── BatchRemove Handler ────────────────────────────────────────

/// Replica-side handler for `Verb::BatchRemove` messages.
pub struct BatchRemoveVerbHandler;

impl BatchRemoveVerbHandler {
    /// Handle an incoming batch remove request when no local batchlog manager is configured.
    pub fn handle(msg: Message) -> Option<Message> {
        Some(Message::failure(
            msg.header.message_id,
            b"Local batchlog manager not configured for BatchRemove".to_vec(),
        ))
    }

    /// Handle an incoming batch remove request using the local batchlog manager.
    pub fn handle_with_batchlog(msg: Message, batchlog: &BatchLogManager) -> Option<Message> {
        let request: BatchRemoveRequest = match serde_json::from_slice(&msg.payload) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "Failed to deserialize batch remove request");
                return Some(Message::failure(
                    msg.header.message_id,
                    format!("Deserialization error: {e}").into_bytes(),
                ));
            }
        };

        debug!(
            batch_id = %request.id,
            "Removing batch log entry locally"
        );

        let response = BatchRemoveResponse {
            removed: batchlog.remove(&request.id).is_some(),
        };
        let payload = serde_json::to_vec(&response).unwrap_or_default();

        // BatchRemove has no dedicated response verb; use the request ID
        // to correlate. The coordinator treats any non-failure response as success.
        Some(Message::response(
            msg.header.message_id,
            Verb::BatchStoreResponse,
            payload,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_batch_store_request() -> BatchStoreRequest {
        BatchStoreRequest {
            id: Uuid::new_v4(),
            batch_type: BatchType::Logged,
            mutations: vec![CoordinatedMutation::simple(
                "ks".to_string(),
                "tbl".to_string(),
                vec![1],
                vec![],
                1000,
            )],
            created_at: 1_700_000_000_000,
        }
    }

    #[test]
    fn batch_store_request_roundtrip() {
        let req = make_batch_store_request();
        let bytes = serde_json::to_vec(&req).unwrap();
        let decoded: BatchStoreRequest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.batch_type, BatchType::Logged);
        assert_eq!(decoded.mutations.len(), 1);
    }

    #[test]
    fn batch_remove_request_roundtrip() {
        let id = Uuid::new_v4();
        let req = BatchRemoveRequest { id };
        let bytes = serde_json::to_vec(&req).unwrap();
        let decoded: BatchRemoveRequest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.id, id);
    }

    #[test]
    fn handle_batch_store_without_batchlog_fails() {
        let req = make_batch_store_request();
        let payload = serde_json::to_vec(&req).unwrap();
        let msg = Message::request(Verb::BatchStore, 50, payload);

        let response = BatchStoreVerbHandler::handle(msg);
        assert!(response.is_some());
        let resp = response.unwrap();
        assert!(resp.is_failure());
    }

    #[test]
    fn handle_batch_store_persists_to_batchlog() {
        let batchlog = BatchLogManager::new();
        let req = make_batch_store_request();
        let id = req.id;
        let payload = serde_json::to_vec(&req).unwrap();
        let msg = Message::request(Verb::BatchStore, 50, payload);

        let response = BatchStoreVerbHandler::handle_with_batchlog(msg, &batchlog);
        assert!(response.is_some());
        let resp = response.unwrap();
        assert_eq!(resp.header.verb, Verb::BatchStoreResponse);
        assert!(resp.is_response());

        let body: BatchStoreResponse = serde_json::from_slice(&resp.payload).unwrap();
        assert!(body.success);
        let stored = batchlog.get(&id).unwrap();
        assert_eq!(stored.id, id);
        assert_eq!(stored.batch_type, BatchType::Logged);
        assert_eq!(stored.mutations.len(), 1);
    }

    #[test]
    fn handle_batch_store_invalid() {
        let msg = Message::request(Verb::BatchStore, 50, b"bad".to_vec());
        let response = BatchStoreVerbHandler::handle(msg);
        assert!(response.is_some());
        assert!(response.unwrap().is_failure());
    }

    #[test]
    fn handle_batch_remove_without_batchlog_fails() {
        let req = BatchRemoveRequest { id: Uuid::new_v4() };
        let payload = serde_json::to_vec(&req).unwrap();
        let msg = Message::request(Verb::BatchRemove, 60, payload);

        let response = BatchRemoveVerbHandler::handle(msg);
        assert!(response.is_some());
        let resp = response.unwrap();
        assert!(resp.is_failure());
    }

    #[test]
    fn handle_batch_remove_deletes_from_batchlog() {
        let batchlog = BatchLogManager::new();
        let id = batchlog.store(
            BatchType::Logged,
            vec![CoordinatedMutation::simple(
                "ks".to_string(),
                "tbl".to_string(),
                vec![1],
                vec![],
                1000,
            )],
        );
        let req = BatchRemoveRequest { id };
        let payload = serde_json::to_vec(&req).unwrap();
        let msg = Message::request(Verb::BatchRemove, 60, payload);

        let response = BatchRemoveVerbHandler::handle_with_batchlog(msg, &batchlog);
        assert!(response.is_some());
        let resp = response.unwrap();
        assert!(resp.is_response());

        let body: BatchRemoveResponse = serde_json::from_slice(&resp.payload).unwrap();
        assert!(body.removed);
        assert!(batchlog.get(&id).is_none());
    }

    #[test]
    fn handle_batch_remove_reports_missing_entry() {
        let batchlog = BatchLogManager::new();
        let req = BatchRemoveRequest { id: Uuid::new_v4() };
        let payload = serde_json::to_vec(&req).unwrap();
        let msg = Message::request(Verb::BatchRemove, 60, payload);

        let response = BatchRemoveVerbHandler::handle_with_batchlog(msg, &batchlog);
        let resp = response.unwrap();
        let body: BatchRemoveResponse = serde_json::from_slice(&resp.payload).unwrap();
        assert!(!body.removed);
    }

    #[test]
    fn handle_batch_remove_invalid() {
        let msg = Message::request(Verb::BatchRemove, 60, b"bad".to_vec());
        let response = BatchRemoveVerbHandler::handle(msg);
        assert!(response.is_some());
        assert!(response.unwrap().is_failure());
    }
}
