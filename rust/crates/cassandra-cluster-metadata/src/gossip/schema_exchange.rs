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

use std::collections::VecDeque;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use cassandra_messaging::service::MessageHandler;
use cassandra_messaging::verb::Verb;
use cassandra_messaging::{Message, MessagingService};
use cassandra_schema::{
    DistributedSchema, RemoteSchemaMigration, SchemaMigration, SchemaMigrationError,
    SchemaMigrationJournalRecord, SchemaMigrationJournalStatus, append_migration_record,
    load_migration_journal,
};
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
    /// Serialized schema data encoded as JSON.
    pub data: Vec<u8>,
}

/// Payload carried by `SchemaPush`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SchemaPushPayload {
    Snapshot {
        snapshot: SchemaSnapshot,
    },
    Migration {
        migration: Box<RemoteSchemaMigration>,
    },
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

    /// Apply a schema migration received from a peer.
    fn apply_migration(&self, migration: RemoteSchemaMigration) -> Result<(), String> {
        Err(format!(
            "schema migration push is not supported by this applier: source={}",
            migration.source
        ))
    }
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
        let apply_result = match decode_schema_push_payload(&msg.payload) {
            Ok(SchemaPushPayload::Snapshot { snapshot }) => {
                info!(
                    version = snapshot.version,
                    "Applying pushed schema snapshot"
                );
                applier.apply_schema(snapshot)
            }
            Ok(SchemaPushPayload::Migration { migration }) => {
                info!(
                    source = %migration.source,
                    base_version = %migration.base_version,
                    "Applying pushed schema migration"
                );
                return match applier.apply_migration(*migration) {
                    Ok(()) => Some(Message::response(
                        msg.header.message_id,
                        Verb::SchemaResponse,
                        Vec::new(),
                    )),
                    Err(e) => {
                        warn!(error = e, "Failed to apply pushed schema");
                        Some(Message::failure(
                            msg.header.message_id,
                            format!("Schema apply failed: {}", e).into_bytes(),
                        ))
                    }
                };
            }
            Err(e) => {
                warn!(error = %e, "Failed to deserialize pushed schema");
                return Some(Message::failure(
                    msg.header.message_id,
                    format!("Bad schema payload: {}", e).into_bytes(),
                ));
            }
        };

        match apply_result {
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

fn decode_schema_push_payload(payload: &[u8]) -> Result<SchemaPushPayload, serde_json::Error> {
    match serde_json::from_slice::<SchemaPushPayload>(payload) {
        Ok(push) => Ok(push),
        Err(push_error) => serde_json::from_slice::<SchemaSnapshot>(payload)
            .map(|snapshot| SchemaPushPayload::Snapshot { snapshot })
            .map_err(|_| push_error),
    }
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

/// Push a single schema migration to a remote peer.
pub async fn push_schema_migration_to(
    peer: Endpoint,
    migration: &RemoteSchemaMigration,
    messaging: &MessagingService,
) -> Result<(), SchemaExchangeError> {
    let payload = serde_json::to_vec(&SchemaPushPayload::Migration {
        migration: Box::new(migration.clone()),
    })
    .map_err(|e| SchemaExchangeError::Deserialization(e.to_string()))?;

    let msg_id = messaging.next_id();
    let push_msg = Message::request(Verb::SchemaPush, msg_id, payload);

    messaging
        .send(peer.addr(), push_msg)
        .await
        .map_err(|e| SchemaExchangeError::Network(e.to_string()))?;

    debug!(
        peer = %peer,
        source = %migration.source,
        base_version = %migration.base_version,
        "Schema migration pushed to peer"
    );
    Ok(())
}

/// Push a single schema migration to a remote peer and wait for its ACK.
pub async fn push_schema_migration_to_with_ack(
    peer: Endpoint,
    migration: &RemoteSchemaMigration,
    messaging: &MessagingService,
    timeout: Duration,
) -> Result<(), SchemaExchangeError> {
    let payload = serde_json::to_vec(&SchemaPushPayload::Migration {
        migration: Box::new(migration.clone()),
    })
    .map_err(|e| SchemaExchangeError::Deserialization(e.to_string()))?;

    let msg_id = messaging.next_id();
    let push_msg = Message::request(Verb::SchemaPush, msg_id, payload);
    let response = messaging
        .send_and_wait(peer.addr(), push_msg, timeout)
        .await
        .map_err(|e| SchemaExchangeError::Network(e.to_string()))?;

    if response.is_failure() {
        return Err(SchemaExchangeError::PeerError(
            String::from_utf8_lossy(&response.payload).to_string(),
        ));
    }

    debug!(
        peer = %peer,
        source = %migration.source,
        base_version = %migration.base_version,
        "Schema migration acknowledged by peer"
    );
    Ok(())
}

/// Per-peer result of a schema migration fan-out.
#[derive(Debug, Clone)]
pub struct SchemaMigrationPushResult {
    pub peer: Endpoint,
    pub attempts: usize,
    pub result: Result<(), SchemaExchangeError>,
}

impl SchemaMigrationPushResult {
    pub fn acknowledged(&self) -> bool {
        self.result.is_ok()
    }
}

/// Retry policy for schema migration fan-out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SchemaMigrationFanoutConfig {
    pub timeout: Duration,
    pub max_attempts: usize,
    pub retry_backoff: Duration,
}

impl SchemaMigrationFanoutConfig {
    pub fn single_attempt(timeout: Duration) -> Self {
        Self {
            timeout,
            max_attempts: 1,
            retry_backoff: Duration::ZERO,
        }
    }

    fn attempts(&self) -> usize {
        self.max_attempts.max(1)
    }
}

impl Default for SchemaMigrationFanoutConfig {
    fn default() -> Self {
        Self {
            timeout: Duration::from_secs(30),
            max_attempts: 3,
            retry_backoff: Duration::from_millis(250),
        }
    }
}

/// Summary of a schema migration fan-out.
#[derive(Debug, Clone)]
pub struct SchemaMigrationFanoutReport {
    pub results: Vec<SchemaMigrationPushResult>,
}

impl SchemaMigrationFanoutReport {
    pub fn acknowledged_count(&self) -> usize {
        self.results
            .iter()
            .filter(|result| result.acknowledged())
            .count()
    }

    pub fn failed_count(&self) -> usize {
        self.results.len() - self.acknowledged_count()
    }

    pub fn all_acknowledged(&self) -> bool {
        self.failed_count() == 0
    }

    pub fn total_attempts(&self) -> usize {
        self.results.iter().map(|result| result.attempts).sum()
    }

    pub fn failed_peers(&self) -> Vec<Endpoint> {
        self.results
            .iter()
            .filter(|result| !result.acknowledged())
            .map(|result| result.peer)
            .collect()
    }
}

/// Push a schema migration to multiple peers and collect ACK/failure status.
pub async fn push_schema_migration_to_peers(
    peers: &[Endpoint],
    migration: &RemoteSchemaMigration,
    messaging: &MessagingService,
    timeout: Duration,
) -> SchemaMigrationFanoutReport {
    push_schema_migration_to_peers_with_retry(
        peers,
        migration,
        messaging,
        SchemaMigrationFanoutConfig::single_attempt(timeout),
    )
    .await
}

/// Push a schema migration to multiple peers with retry/backoff.
pub async fn push_schema_migration_to_peers_with_retry(
    peers: &[Endpoint],
    migration: &RemoteSchemaMigration,
    messaging: &MessagingService,
    config: SchemaMigrationFanoutConfig,
) -> SchemaMigrationFanoutReport {
    let mut results = Vec::with_capacity(peers.len());
    for peer in peers {
        let mut attempts = 0;
        let mut result = Err(SchemaExchangeError::Network(
            "schema migration was not attempted".to_string(),
        ));

        for attempt in 1..=config.attempts() {
            attempts = attempt;
            result = push_schema_migration_to_with_ack(*peer, migration, messaging, config.timeout)
                .await;
            if result.is_ok() || attempt == config.attempts() {
                break;
            }
            if !config.retry_backoff.is_zero() {
                tokio::time::sleep(config.retry_backoff).await;
            }
        }

        results.push(SchemaMigrationPushResult {
            peer: *peer,
            attempts,
            result,
        });
    }
    SchemaMigrationFanoutReport { results }
}

/// Report for a locally applied and cluster-pushed schema migration.
#[derive(Debug, Clone)]
pub struct SchemaMigrationCoordinationReport {
    pub migration: RemoteSchemaMigration,
    pub local_record: SchemaMigrationJournalRecord,
    pub fanout: SchemaMigrationFanoutReport,
}

impl SchemaMigrationCoordinationReport {
    pub fn fully_acknowledged(&self) -> bool {
        self.local_record.status == SchemaMigrationJournalStatus::Applied
            && self.fanout.all_acknowledged()
    }
}

/// Errors from coordinating a schema migration locally and across peers.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SchemaMigrationCoordinationError {
    #[error("local schema migration failed: {0:?}")]
    Local(SchemaMigrationError),

    #[error("schema migration journal failed: {0}")]
    Journal(String),
}

/// Apply a schema migration locally, persist the decision, then push to peers.
pub async fn coordinate_schema_migration<P: AsRef<Path>>(
    schema: &mut DistributedSchema,
    source: impl Into<String>,
    migration: SchemaMigration,
    peers: &[Endpoint],
    messaging: &MessagingService,
    journal_path: P,
    fanout_config: SchemaMigrationFanoutConfig,
) -> Result<SchemaMigrationCoordinationReport, SchemaMigrationCoordinationError> {
    let journal_path = journal_path.as_ref();
    let remote = schema.prepare_remote_migration(source, migration);
    let sequence = next_journal_sequence(journal_path)?;
    let local_version = schema.current_version();

    let local_result = schema
        .apply_remote_migration(remote.clone())
        .map_err(SchemaMigrationCoordinationError::Local)?;
    let local_record = SchemaMigrationJournalRecord {
        sequence,
        local_version,
        resulting_version: Some(local_result.result.new_version),
        migration: remote.clone(),
        status: SchemaMigrationJournalStatus::Applied,
    };
    append_migration_record(journal_path, &local_record)
        .map_err(|error| SchemaMigrationCoordinationError::Journal(error.to_string()))?;

    let fanout =
        push_schema_migration_to_peers_with_retry(peers, &remote, messaging, fanout_config).await;

    Ok(SchemaMigrationCoordinationReport {
        migration: remote,
        local_record,
        fanout,
    })
}

fn next_journal_sequence(path: &Path) -> Result<u64, SchemaMigrationCoordinationError> {
    let records = load_migration_journal(path)
        .map_err(|error| SchemaMigrationCoordinationError::Journal(error.to_string()))?;
    Ok(records
        .iter()
        .map(|record| record.sequence)
        .max()
        .unwrap_or(0)
        + 1)
}

/// FIFO scheduler for schema migrations.
#[derive(Debug, Clone)]
pub struct SchemaMigrationScheduler {
    source: String,
    peers: Vec<Endpoint>,
    journal_path: PathBuf,
    fanout_config: SchemaMigrationFanoutConfig,
    queue: VecDeque<SchemaMigration>,
    completed: Vec<SchemaMigrationCoordinationReport>,
}

impl SchemaMigrationScheduler {
    pub fn new(
        source: impl Into<String>,
        peers: Vec<Endpoint>,
        journal_path: impl Into<PathBuf>,
        fanout_config: SchemaMigrationFanoutConfig,
    ) -> Self {
        Self {
            source: source.into(),
            peers,
            journal_path: journal_path.into(),
            fanout_config,
            queue: VecDeque::new(),
            completed: Vec::new(),
        }
    }

    pub fn enqueue(&mut self, migration: SchemaMigration) {
        self.queue.push_back(migration);
    }

    pub fn pending_len(&self) -> usize {
        self.queue.len()
    }

    pub fn completed(&self) -> &[SchemaMigrationCoordinationReport] {
        &self.completed
    }

    pub async fn run_next(
        &mut self,
        schema: &mut DistributedSchema,
        messaging: &MessagingService,
    ) -> Result<Option<&SchemaMigrationCoordinationReport>, SchemaMigrationCoordinationError> {
        let Some(migration) = self.queue.pop_front() else {
            return Ok(None);
        };

        let report = coordinate_schema_migration(
            schema,
            self.source.clone(),
            migration,
            &self.peers,
            messaging,
            &self.journal_path,
            self.fanout_config,
        )
        .await?;
        self.completed.push(report);
        Ok(self.completed.last())
    }

    pub async fn drain(
        &mut self,
        schema: &mut DistributedSchema,
        messaging: &MessagingService,
    ) -> Result<&[SchemaMigrationCoordinationReport], SchemaMigrationCoordinationError> {
        while self.pending_len() > 0 {
            self.run_next(schema, messaging).await?;
        }
        Ok(self.completed())
    }
}

/// Errors from schema exchange operations.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
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
    use cassandra_schema::{
        DistributedSchema, KeyspaceMetadata, KeyspaceParams, RemoteSchemaMigration, SchemaCatalog,
        SchemaMigration, load_migration_journal,
    };
    use std::sync::Mutex;
    use uuid::Uuid;

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
        migrations: Mutex<Vec<RemoteSchemaMigration>>,
    }

    impl TestApplier {
        fn new() -> Self {
            Self {
                applied: Mutex::new(Vec::new()),
                migrations: Mutex::new(Vec::new()),
            }
        }

        fn applied(&self) -> Vec<SchemaSnapshot> {
            self.applied.lock().unwrap().clone()
        }

        fn migrations(&self) -> Vec<RemoteSchemaMigration> {
            self.migrations.lock().unwrap().clone()
        }
    }

    impl SchemaApplier for TestApplier {
        fn apply_schema(&self, snapshot: SchemaSnapshot) -> Result<(), String> {
            self.applied.lock().unwrap().push(snapshot);
            Ok(())
        }

        fn apply_migration(&self, migration: RemoteSchemaMigration) -> Result<(), String> {
            self.migrations.lock().unwrap().push(migration);
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
    fn push_handler_accepts_typed_snapshot_payload() {
        let applier = Arc::new(TestApplier::new());
        let handler = make_schema_push_handler(applier.clone());

        let payload = serde_json::to_vec(&SchemaPushPayload::Snapshot {
            snapshot: SchemaSnapshot {
                version: "v3".to_string(),
                data: b"typed-schema".to_vec(),
            },
        })
        .unwrap();
        let msg = Message::request(Verb::SchemaPush, 1, payload);

        let response = handler(msg);
        assert!(response.is_none());
        assert_eq!(applier.applied()[0].version, "v3");
    }

    #[test]
    fn push_handler_applies_remote_migration_payload() {
        let applier = Arc::new(TestApplier::new());
        let handler = make_schema_push_handler(applier.clone());
        let base_version = Uuid::new_v4();
        let migration = RemoteSchemaMigration::new(
            "node1",
            base_version,
            SchemaMigration::CreateKeyspace(KeyspaceMetadata::new("ks", KeyspaceParams::default())),
        );

        let payload = serde_json::to_vec(&SchemaPushPayload::Migration {
            migration: Box::new(migration.clone()),
        })
        .unwrap();
        let msg = Message::request(Verb::SchemaPush, 1, payload);

        let response = handler(msg).unwrap();
        assert_eq!(response.header.verb, Verb::SchemaResponse);
        assert!(response.is_response());
        assert!(!response.is_failure());
        assert_eq!(applier.migrations(), vec![migration]);
    }

    #[test]
    fn fanout_report_counts_acknowledged_and_failed_peers() {
        let peer1 = Endpoint::new("127.0.0.1:7001".parse().unwrap());
        let peer2 = Endpoint::new("127.0.0.1:7002".parse().unwrap());
        let report = SchemaMigrationFanoutReport {
            results: vec![
                SchemaMigrationPushResult {
                    peer: peer1,
                    attempts: 1,
                    result: Ok(()),
                },
                SchemaMigrationPushResult {
                    peer: peer2,
                    attempts: 3,
                    result: Err(SchemaExchangeError::PeerError("stale schema".to_string())),
                },
            ],
        };

        assert_eq!(report.acknowledged_count(), 1);
        assert_eq!(report.failed_count(), 1);
        assert_eq!(report.total_attempts(), 4);
        assert_eq!(report.failed_peers(), vec![peer2]);
        assert!(!report.all_acknowledged());
        assert!(report.results[0].acknowledged());
        assert!(!report.results[1].acknowledged());
    }

    #[test]
    fn fanout_config_normalizes_zero_attempts() {
        let config = SchemaMigrationFanoutConfig {
            timeout: Duration::from_secs(1),
            max_attempts: 0,
            retry_backoff: Duration::ZERO,
        };

        assert_eq!(config.attempts(), 1);
        assert_eq!(
            SchemaMigrationFanoutConfig::single_attempt(Duration::from_secs(2)),
            SchemaMigrationFanoutConfig {
                timeout: Duration::from_secs(2),
                max_attempts: 1,
                retry_backoff: Duration::ZERO,
            }
        );
    }

    #[tokio::test]
    async fn coordinate_schema_migration_applies_journals_and_reports_empty_fanout() {
        let mut schema = DistributedSchema::new(SchemaCatalog::new());
        let messaging = MessagingService::new("127.0.0.1:0".parse().unwrap());
        let dir = tempfile::tempdir().unwrap();
        let journal = dir.path().join("schema_migrations.jsonl");

        let report = coordinate_schema_migration(
            &mut schema,
            "node1",
            SchemaMigration::CreateKeyspace(KeyspaceMetadata::new("ks", KeyspaceParams::default())),
            &[],
            &messaging,
            &journal,
            SchemaMigrationFanoutConfig::single_attempt(Duration::from_millis(1)),
        )
        .await
        .unwrap();

        assert!(report.fully_acknowledged());
        assert_eq!(report.local_record.sequence, 1);
        assert_eq!(report.fanout.acknowledged_count(), 0);
        assert!(schema.snapshot().keyspace("ks").is_some());

        let records = load_migration_journal(&journal).unwrap();
        assert_eq!(records, vec![report.local_record]);
    }

    #[tokio::test]
    async fn coordinate_schema_migration_continues_journal_sequence() {
        let mut schema = DistributedSchema::new(SchemaCatalog::new());
        let messaging = MessagingService::new("127.0.0.1:0".parse().unwrap());
        let dir = tempfile::tempdir().unwrap();
        let journal = dir.path().join("schema_migrations.jsonl");

        let first = coordinate_schema_migration(
            &mut schema,
            "node1",
            SchemaMigration::CreateKeyspace(KeyspaceMetadata::new(
                "ks1",
                KeyspaceParams::default(),
            )),
            &[],
            &messaging,
            &journal,
            SchemaMigrationFanoutConfig::single_attempt(Duration::from_millis(1)),
        )
        .await
        .unwrap();
        let second = coordinate_schema_migration(
            &mut schema,
            "node1",
            SchemaMigration::CreateKeyspace(KeyspaceMetadata::new(
                "ks2",
                KeyspaceParams::default(),
            )),
            &[],
            &messaging,
            &journal,
            SchemaMigrationFanoutConfig::single_attempt(Duration::from_millis(1)),
        )
        .await
        .unwrap();

        assert_eq!(first.local_record.sequence, 1);
        assert_eq!(second.local_record.sequence, 2);
        assert_eq!(load_migration_journal(&journal).unwrap().len(), 2);
    }

    #[tokio::test]
    async fn scheduler_runs_migrations_in_fifo_order_and_records_reports() {
        let mut schema = DistributedSchema::new(SchemaCatalog::new());
        let messaging = MessagingService::new("127.0.0.1:0".parse().unwrap());
        let dir = tempfile::tempdir().unwrap();
        let journal = dir.path().join("schema_migrations.jsonl");
        let mut scheduler = SchemaMigrationScheduler::new(
            "node1",
            Vec::new(),
            journal.clone(),
            SchemaMigrationFanoutConfig::single_attempt(Duration::from_millis(1)),
        );

        scheduler.enqueue(SchemaMigration::CreateKeyspace(KeyspaceMetadata::new(
            "ks1",
            KeyspaceParams::default(),
        )));
        scheduler.enqueue(SchemaMigration::CreateKeyspace(KeyspaceMetadata::new(
            "ks2",
            KeyspaceParams::default(),
        )));

        assert_eq!(scheduler.pending_len(), 2);
        let first = scheduler.run_next(&mut schema, &messaging).await.unwrap();
        assert_eq!(first.unwrap().local_record.sequence, 1);
        assert_eq!(scheduler.pending_len(), 1);

        let completed = scheduler.drain(&mut schema, &messaging).await.unwrap();
        assert_eq!(completed.len(), 2);
        assert_eq!(completed[1].local_record.sequence, 2);
        assert_eq!(scheduler.pending_len(), 0);
        assert!(schema.snapshot().keyspace("ks1").is_some());
        assert!(schema.snapshot().keyspace("ks2").is_some());
        assert_eq!(load_migration_journal(&journal).unwrap().len(), 2);
    }

    #[test]
    fn push_handler_returns_failure_when_migration_applier_is_missing() {
        struct SnapshotOnlyApplier;

        impl SchemaApplier for SnapshotOnlyApplier {
            fn apply_schema(&self, _snapshot: SchemaSnapshot) -> Result<(), String> {
                Ok(())
            }
        }

        let handler = make_schema_push_handler(Arc::new(SnapshotOnlyApplier));
        let migration = RemoteSchemaMigration::new(
            "node1",
            Uuid::new_v4(),
            SchemaMigration::CreateKeyspace(KeyspaceMetadata::new("ks", KeyspaceParams::default())),
        );
        let payload = serde_json::to_vec(&SchemaPushPayload::Migration {
            migration: Box::new(migration),
        })
        .unwrap();
        let msg = Message::request(Verb::SchemaPush, 1, payload);

        let response = handler(msg).unwrap();
        assert!(response.is_failure());
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

    #[test]
    fn schema_push_payload_round_trip() {
        let migration = RemoteSchemaMigration::new(
            "node1",
            Uuid::new_v4(),
            SchemaMigration::DropKeyspace("ks".to_string()),
        );
        let payload = SchemaPushPayload::Migration {
            migration: Box::new(migration.clone()),
        };
        let json = serde_json::to_vec(&payload).unwrap();
        let decoded: SchemaPushPayload = serde_json::from_slice(&json).unwrap();

        match decoded {
            SchemaPushPayload::Migration { migration: decoded } => {
                assert_eq!(*decoded, migration);
            }
            SchemaPushPayload::Snapshot { .. } => panic!("expected migration payload"),
        }
    }
}
