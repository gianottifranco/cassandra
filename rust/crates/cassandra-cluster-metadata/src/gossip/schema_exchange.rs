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

//! Schema exchange: push/pull schema state between nodes.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.schema.MigrationManager`
//! - `org.apache.cassandra.net.SchemaPullVerbHandler`
//! - `org.apache.cassandra.net.SchemaPushVerbHandler`

use std::sync::Arc;
use std::time::Duration;

use cassandra_messaging::service::MessageHandler;
use cassandra_messaging::verb::Verb;
use cassandra_messaging::{Message, MessagingService};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::node::Endpoint;

/// A schema snapshot exchanged between nodes.
///
/// Contains the serialized schema definition. In a full implementation,
/// this would include keyspace/table/type/function/aggregate definitions.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaSnapshot {
    /// Schema version UUID string.
    pub version: String,
    /// Serialized schema data (JSON for now).
    pub data: Vec<u8>,
}

/// Trait for providing the local schema snapshot.
pub trait SchemaProvider: Send + Sync {
    /// Return the current schema snapshot.
    fn current_schema(&self) -> SchemaSnapshot;
}

/// Trait for applying a received schema snapshot.
pub trait SchemaApplier: Send + Sync {
    /// Apply a schema snapshot received from a peer.
    fn apply_schema(&self, snapshot: SchemaSnapshot) -> Result<(), String>;
}

/// Create a handler for SchemaPull requests.
///
/// When a peer requests our schema, we serialize and return it.
pub fn make_schema_pull_handler(provider: Arc<dyn SchemaProvider>) -> MessageHandler {
    Arc::new(move |msg: Message| {
        let snapshot = provider.current_schema();
        let payload = match serde_json::to_vec(&snapshot) {
            Ok(p) => p,
            Err(e) => {
                warn!(error = %e, "Failed to serialize schema snapshot");
                return Some(Message::failure(
                    msg.header.message_id,
                    format!("Schema serialization failed: {}", e).into_bytes(),
                ));
            }
        };

        Some(Message::response(
            msg.header.message_id,
            Verb::SchemaResponse,
            payload,
        ))
    })
}

/// Create a handler for SchemaPush messages.
///
/// When a peer pushes a schema change, we apply it via the `SchemaApplier`.
pub fn make_schema_push_handler(applier: Arc<dyn SchemaApplier>) -> MessageHandler {
    Arc::new(move |msg: Message| {
        let snapshot: SchemaSnapshot = match serde_json::from_slice(&msg.payload) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "Failed to deserialize pushed schema");
                return Some(Message::failure(
                    msg.header.message_id,
                    format!("Bad schema payload: {}", e).into_bytes(),
                ));
            }
        };

        info!(version = snapshot.version, "Applying pushed schema");
        match applier.apply_schema(snapshot) {
            Ok(()) => None, // Success, no response needed for push
            Err(e) => {
                warn!(error = e, "Failed to apply pushed schema");
                Some(Message::failure(
                    msg.header.message_id,
                    format!("Schema apply failed: {}", e).into_bytes(),
                ))
            }
        }
    })
}

/// Register schema exchange handlers on the messaging service.
pub fn register_schema_handlers(
    provider: Arc<dyn SchemaProvider>,
    applier: Arc<dyn SchemaApplier>,
    messaging: &MessagingService,
) {
    messaging.register_handler(Verb::SchemaPull, make_schema_pull_handler(provider));
    messaging.register_handler(Verb::SchemaPush, make_schema_push_handler(applier));
}

/// Pull schema from a remote peer.
///
/// Sends a SchemaPull request and waits for the SchemaResponse.
pub async fn pull_schema_from(
    peer: Endpoint,
    messaging: &MessagingService,
    timeout: Duration,
) -> Result<SchemaSnapshot, SchemaExchangeError> {
    let msg_id = messaging.next_id();
    let pull_msg = Message::request(Verb::SchemaPull, msg_id, Vec::new());

    let response = messaging
        .send_and_wait(peer.addr(), pull_msg, timeout)
        .await
        .map_err(|e| SchemaExchangeError::Network(e.to_string()))?;

    if response.is_failure() {
        return Err(SchemaExchangeError::PeerError(
            String::from_utf8_lossy(&response.payload).to_string(),
        ));
    }

    serde_json::from_slice(&response.payload)
        .map_err(|e| SchemaExchangeError::Deserialization(e.to_string()))
}

/// Push schema to a remote peer.
pub async fn push_schema_to(
    peer: Endpoint,
    schema: &SchemaSnapshot,
    messaging: &MessagingService,
) -> Result<(), SchemaExchangeError> {
    let payload = serde_json::to_vec(schema)
        .map_err(|e| SchemaExchangeError::Deserialization(e.to_string()))?;

    let msg_id = messaging.next_id();
    let push_msg = Message::request(Verb::SchemaPush, msg_id, payload);

    messaging
        .send(peer.addr(), push_msg)
        .await
        .map_err(|e| SchemaExchangeError::Network(e.to_string()))?;

    debug!(peer = %peer, "Schema pushed to peer");
    Ok(())
}

/// Errors from schema exchange operations.
#[derive(Debug, thiserror::Error)]
pub enum SchemaExchangeError {
    #[error("Network error: {0}")]
    Network(String),

    #[error("Deserialization error: {0}")]
    Deserialization(String),

    #[error("Peer returned error: {0}")]
    PeerError(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Mutex;

    struct TestProvider {
        snapshot: SchemaSnapshot,
    }

    impl SchemaProvider for TestProvider {
        fn current_schema(&self) -> SchemaSnapshot {
            self.snapshot.clone()
        }
    }

    struct TestApplier {
        applied: Mutex<Vec<SchemaSnapshot>>,
    }

    impl TestApplier {
        fn new() -> Self {
            Self {
                applied: Mutex::new(Vec::new()),
            }
        }

        fn applied(&self) -> Vec<SchemaSnapshot> {
            self.applied.lock().unwrap().clone()
        }
    }

    impl SchemaApplier for TestApplier {
        fn apply_schema(&self, snapshot: SchemaSnapshot) -> Result<(), String> {
            self.applied.lock().unwrap().push(snapshot);
            Ok(())
        }
    }

    #[test]
    fn pull_handler_returns_schema() {
        let provider = Arc::new(TestProvider {
            snapshot: SchemaSnapshot {
                version: "v1".to_string(),
                data: b"test-schema".to_vec(),
            },
        });

        let handler = make_schema_pull_handler(provider);
        let msg = Message::request(Verb::SchemaPull, 1, Vec::new());

        let response = handler(msg).unwrap();
        assert_eq!(response.header.verb, Verb::SchemaResponse);
        assert!(response.is_response());

        let snapshot: SchemaSnapshot = serde_json::from_slice(&response.payload).unwrap();
        assert_eq!(snapshot.version, "v1");
        assert_eq!(snapshot.data, b"test-schema");
    }

    #[test]
    fn push_handler_calls_applier() {
        let applier = Arc::new(TestApplier::new());
        let handler = make_schema_push_handler(applier.clone());

        let snapshot = SchemaSnapshot {
            version: "v2".to_string(),
            data: b"new-schema".to_vec(),
        };
        let payload = serde_json::to_vec(&snapshot).unwrap();
        let msg = Message::request(Verb::SchemaPush, 1, payload);

        let response = handler(msg);
        assert!(response.is_none()); // Push doesn't respond on success

        let applied = applier.applied();
        assert_eq!(applied.len(), 1);
        assert_eq!(applied[0].version, "v2");
    }

    #[test]
    fn push_handler_malformed_payload() {
        let applier = Arc::new(TestApplier::new());
        let handler = make_schema_push_handler(applier);

        let msg = Message::request(Verb::SchemaPush, 1, b"garbage".to_vec());
        let response = handler(msg);
        assert!(response.is_some());
        assert!(response.unwrap().is_failure());
    }

    #[test]
    fn schema_snapshot_round_trip() {
        let snapshot = SchemaSnapshot {
            version: "abc-123".to_string(),
            data: vec![1, 2, 3, 4, 5],
        };
        let json = serde_json::to_vec(&snapshot).unwrap();
        let decoded: SchemaSnapshot = serde_json::from_slice(&json).unwrap();
        assert_eq!(decoded.version, snapshot.version);
        assert_eq!(decoded.data, snapshot.data);
    }
}
