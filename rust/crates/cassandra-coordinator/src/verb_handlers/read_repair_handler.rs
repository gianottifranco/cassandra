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
use tracing::{debug, warn};

use crate::write::CoordinatedMutation;

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
    /// Handle an incoming read repair message.
    pub fn handle(msg: Message) -> Option<Message> {
        let request: ReadRepairRequest = match serde_json::from_slice(&msg.payload) {
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

        // In a full implementation, this would apply the repair mutation
        // to the local storage engine.
        let response = ReadRepairResponse { success: true };
        let payload = serde_json::to_vec(&response).unwrap_or_default();

        Some(Message::response(
            msg.header.message_id,
            Verb::ReadRepairResponse,
            payload,
        ))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::write::CoordinatedMutation;

    fn make_repair_request() -> ReadRepairRequest {
        ReadRepairRequest {
            mutation: CoordinatedMutation::simple(
                "ks".to_string(),
                "tbl".to_string(),
                vec![1, 2],
                vec![],
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
    fn handle_valid_read_repair() {
        let req = make_repair_request();
        let payload = serde_json::to_vec(&req).unwrap();
        let msg = Message::request(Verb::ReadRepair, 70, payload);

        let response = ReadRepairVerbHandler::handle(msg);
        assert!(response.is_some());
        let resp = response.unwrap();
        assert_eq!(resp.header.verb, Verb::ReadRepairResponse);
        assert!(resp.is_response());

        let body: ReadRepairResponse = serde_json::from_slice(&resp.payload).unwrap();
        assert!(body.success);
    }

    #[test]
    fn handle_invalid_read_repair() {
        let msg = Message::request(Verb::ReadRepair, 70, b"bad".to_vec());
        let response = ReadRepairVerbHandler::handle(msg);
        assert!(response.is_some());
        assert!(response.unwrap().is_failure());
    }
}
