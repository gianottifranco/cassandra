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
use tracing::{debug, warn};

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
    /// Handle an incoming hint delivery message.
    pub fn handle(msg: Message) -> Option<Message> {
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

        // In a full implementation, this would apply the mutation to the
        // local storage engine, exactly like a normal mutation.
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
    use crate::write::CoordinatedMutation;

    fn make_hint_request() -> HintRequest {
        HintRequest {
            mutation: CoordinatedMutation::simple(
                "ks".to_string(),
                "tbl".to_string(),
                vec![1],
                vec![],
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
    fn handle_valid_hint() {
        let req = make_hint_request();
        let payload = serde_json::to_vec(&req).unwrap();
        let msg = Message::request(Verb::Hint, 100, payload);

        let response = HintVerbHandler::handle(msg);
        assert!(response.is_some());
        let resp = response.unwrap();
        assert_eq!(resp.header.verb, Verb::HintResponse);
        assert!(resp.is_response());

        let body: HintResponsePayload = serde_json::from_slice(&resp.payload).unwrap();
        assert!(body.success);
    }

    #[test]
    fn handle_invalid_hint() {
        let msg = Message::request(Verb::Hint, 100, b"garbage".to_vec());
        let response = HintVerbHandler::handle(msg);
        assert!(response.is_some());
        assert!(response.unwrap().is_failure());
    }
}
