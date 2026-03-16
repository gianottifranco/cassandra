// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
// SPDX-License-Identifier: Apache-2.0

//! Replica-side mutation verb handler.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.db.MutationVerbHandler`
//!
//! Receives `Verb::Mutation`, deserializes `CoordinatedMutation` from
//! the payload, applies locally, and returns `Verb::MutationResponse`.

use cassandra_messaging::frame::Message;
use cassandra_messaging::verb::Verb;
use tracing::{debug, warn};

use crate::write::CoordinatedMutation;

// ─── Mutation Payload ───────────────────────────────────────────

/// Serialized mutation request sent between coordinator and replicas.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MutationRequest {
    /// The mutation to apply.
    pub mutation: CoordinatedMutation,
}

/// Response from a successful mutation apply.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct MutationResponse {
    /// Whether the mutation was applied successfully.
    pub success: bool,
}

// ─── Handler ────────────────────────────────────────────────────

/// Replica-side handler for `Verb::Mutation` messages.
///
/// Deserializes the mutation, applies it locally (logs success),
/// and returns a `Verb::MutationResponse`.
pub struct MutationVerbHandler;

impl MutationVerbHandler {
    /// Handle an incoming mutation message.
    pub fn handle(msg: Message) -> Option<Message> {
        let request: MutationRequest = match serde_json::from_slice(&msg.payload) {
            Ok(r) => r,
            Err(e) => {
                warn!(error = %e, "Failed to deserialize mutation request");
                return Some(Message::failure(
                    msg.header.message_id,
                    format!("Deserialization error: {e}").into_bytes(),
                ));
            }
        };

        debug!(
            keyspace = %request.mutation.keyspace,
            table = %request.mutation.table,
            "Applying mutation locally"
        );

        // In a full implementation, this would write to the local storage engine.
        // For now, we log and return success.
        let response = MutationResponse { success: true };
        let payload = serde_json::to_vec(&response).unwrap_or_default();

        Some(Message::response(
            msg.header.message_id,
            Verb::MutationResponse,
            payload,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::{CoordinatedMutation, MutationRow, CellMutation};

    fn make_mutation() -> CoordinatedMutation {
        CoordinatedMutation::simple(
            "test_ks".to_string(),
            "test_table".to_string(),
            vec![1, 2, 3],
            vec![MutationRow {
                clustering_key: vec![10],
                cells: vec![CellMutation {
                    column: "col1".to_string(),
                    value: Some(b"value1".to_vec()),
                    timestamp: 1000,
                    ttl: 0,
                    is_tombstone: false,
                    collection_op: None,
                }],
                is_tombstone: false,
                range_tombstone: None,
            }],
            1000,
        )
    }

    #[test]
    fn mutation_request_roundtrip() {
        let request = MutationRequest {
            mutation: make_mutation(),
        };
        let bytes = serde_json::to_vec(&request).unwrap();
        let decoded: MutationRequest = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(decoded.mutation.keyspace, "test_ks");
        assert_eq!(decoded.mutation.table, "test_table");
    }

    #[test]
    fn handle_valid_mutation() {
        let request = MutationRequest {
            mutation: make_mutation(),
        };
        let payload = serde_json::to_vec(&request).unwrap();
        let msg = Message::request(Verb::Mutation, 42, payload);

        let response = MutationVerbHandler::handle(msg);
        assert!(response.is_some());
        let resp = response.unwrap();
        assert_eq!(resp.header.verb, Verb::MutationResponse);
        assert!(resp.is_response());

        let resp_body: MutationResponse = serde_json::from_slice(&resp.payload).unwrap();
        assert!(resp_body.success);
    }

    #[test]
    fn handle_invalid_payload() {
        let msg = Message::request(Verb::Mutation, 42, b"not json".to_vec());
        let response = MutationVerbHandler::handle(msg);
        assert!(response.is_some());
        let resp = response.unwrap();
        assert!(resp.is_failure());
    }
}
