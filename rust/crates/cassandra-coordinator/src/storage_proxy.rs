// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.
// SPDX-License-Identifier: Apache-2.0

//! StorageProxy: unified facade for read/write coordination.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.service.StorageProxy`
//!
//! ## Architecture
//!
//! StorageProxy is the single entry point for all data-path operations.
//! It holds references to the write coordinator, read coordinator,
//! messaging service, hint store, and batch log manager, and delegates
//! to each as appropriate.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, warn};

use cassandra_cluster_metadata::{Endpoint, ReplicationStrategy, Snitch};
use cassandra_messaging::{Message, MessagingService, Verb};

use cassandra_schema::table::TransactionalMode;

use crate::batch::BatchLogManager;
use crate::consensus::ConsensusRouter;
use crate::consistency::ConsistencyLevel;
use crate::hints::HintStore;
use crate::paxos::coordinator::CasResult;
use crate::read::{CoordinatedRead, ReadCoordinator, ReadError, ReadResult};
use crate::verb_handlers::mutation_handler::{MutationRequest, MutationResponse};
use crate::verb_handlers::read_handler::{
    ReadDataRequest, ReadDataResponsePayload, ReadDigestRequest, ReadDigestResponsePayload,
};
use crate::write::{CoordinatedMutation, WriteCoordinator, WriteError, WriteResult};

// ─── Configuration ──────────────────────────────────────────────

/// Configuration for the StorageProxy.
#[derive(Debug, Clone)]
pub struct StorageProxyConfig {
    /// Write timeout (default: 2s).
    pub write_timeout: Duration,
    /// Read timeout (default: 5s).
    pub read_timeout: Duration,
    /// Local listen address for messaging.
    pub listen_address: SocketAddr,
    /// Local datacenter name.
    pub local_datacenter: String,
    /// Whether this node is currently bootstrapping.
    pub is_bootstrapping: bool,
    /// Maximum concurrent reads.
    pub max_concurrent_reads: usize,
    /// Maximum concurrent writes.
    pub max_concurrent_writes: usize,
}

impl Default for StorageProxyConfig {
    fn default() -> Self {
        Self {
            write_timeout: Duration::from_secs(2),
            read_timeout: Duration::from_secs(5),
            listen_address: "127.0.0.1:7000".parse().unwrap(),
            local_datacenter: "datacenter1".to_string(),
            is_bootstrapping: false,
            max_concurrent_reads: 1024,
            max_concurrent_writes: 1024,
        }
    }
}

// ─── StorageProxy ───────────────────────────────────────────────

/// Unified facade for coordinated reads and writes.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.service.StorageProxy`
pub struct StorageProxy {
    /// Write coordinator for mutation routing.
    write_coordinator: Arc<WriteCoordinator>,
    /// Read coordinator for read routing.
    read_coordinator: Arc<ReadCoordinator>,
    /// Inter-node messaging service.
    messaging: Arc<MessagingService>,
    /// Hint store for down replicas.
    hint_store: Arc<HintStore>,
    /// Batch log manager for logged batches.
    batch_log: Arc<BatchLogManager>,
    /// Consensus router for CAS/LWT operations.
    consensus_router: Option<Arc<ConsensusRouter>>,
    /// Configuration.
    config: StorageProxyConfig,
}

impl StorageProxy {
    /// Create a new StorageProxy with all dependencies.
    pub fn new(
        write_coordinator: Arc<WriteCoordinator>,
        read_coordinator: Arc<ReadCoordinator>,
        messaging: Arc<MessagingService>,
        hint_store: Arc<HintStore>,
        batch_log: Arc<BatchLogManager>,
        config: StorageProxyConfig,
    ) -> Self {
        Self {
            write_coordinator,
            read_coordinator,
            messaging,
            hint_store,
            batch_log,
            consensus_router: None,
            config,
        }
    }

    /// Set the consensus router for CAS operations.
    pub fn set_consensus_router(&mut self, router: Arc<ConsensusRouter>) {
        self.consensus_router = Some(router);
    }

    /// Access the consensus router (if configured).
    pub fn consensus_router(&self) -> Option<&Arc<ConsensusRouter>> {
        self.consensus_router.as_ref()
    }

    /// Access the write coordinator.
    pub fn write_coordinator(&self) -> &Arc<WriteCoordinator> {
        &self.write_coordinator
    }

    /// Access the read coordinator.
    pub fn read_coordinator(&self) -> &Arc<ReadCoordinator> {
        &self.read_coordinator
    }

    /// Access the messaging service.
    pub fn messaging(&self) -> &Arc<MessagingService> {
        &self.messaging
    }

    /// Access the hint store.
    pub fn hint_store(&self) -> &Arc<HintStore> {
        &self.hint_store
    }

    /// Access the batch log manager.
    pub fn batch_log(&self) -> &Arc<BatchLogManager> {
        &self.batch_log
    }

    /// The proxy configuration.
    pub fn config(&self) -> &StorageProxyConfig {
        &self.config
    }

    /// Coordinate a write mutation across replicas.
    ///
    /// Delegates to `WriteCoordinator::coordinate_write()`, then sends
    /// `Verb::Mutation` messages to remote replicas via the messaging
    /// service, and stores hints for any replicas that fail.
    ///
    /// ## Java Oracle
    ///
    /// `StorageProxy.mutate()`
    pub async fn mutate(
        &self,
        mutation: CoordinatedMutation,
        cl: ConsistencyLevel,
        strategy: &dyn ReplicationStrategy,
        snitch: &dyn Snitch,
    ) -> Result<WriteResult, WriteError> {
        debug!(
            keyspace = %mutation.keyspace,
            table = %mutation.table,
            cl = ?cl,
            "StorageProxy.mutate"
        );

        // Compute the write plan.
        let plan = self
            .write_coordinator
            .compute_write_plan(&mutation, cl, strategy, snitch)?;

        // Send mutations to live replicas via messaging, using local dispatch
        // for the local endpoint and request/response transport for remotes.
        let mut send_failures: Vec<Endpoint> = Vec::new();
        let mut send_successes = 0usize;

        for replica in &plan.live_replicas {
            match self.send_mutation_to_replica(*replica, &mutation).await {
                Ok(()) => send_successes += 1,
                Err(e) => {
                    warn!(replica = %replica, error = %e, "Mutation failed");
                    send_failures.push(*replica);
                }
            }
        }

        // Store hints for dead replicas and failed remote sends.
        let mut hints_stored = 0usize;
        for dead in &plan.dead_replicas {
            if self.hint_store.store_hint(*dead, mutation.clone()) {
                hints_stored += 1;
            }
        }
        for failed in &send_failures {
            if self.hint_store.store_hint(*failed, mutation.clone()) {
                hints_stored += 1;
            }
        }

        self.write_coordinator.complete_write_plan(
            &mutation,
            &plan,
            send_successes,
            hints_stored,
            plan.live_replicas.clone(),
        )
    }

    /// Coordinate a write using async WriteResponseHandler (WU-01).
    ///
    /// Uses `coordinate_write_async()` to create a handler, fans out
    /// mutations via messaging, records acks, and awaits CL satisfaction.
    ///
    /// ## Java Oracle
    ///
    /// `StorageProxy.mutate()` with `performWrite()` + `WriteResponseHandler`
    pub async fn mutate_async(
        &self,
        mutation: CoordinatedMutation,
        cl: ConsistencyLevel,
        strategy: &dyn ReplicationStrategy,
        snitch: &dyn Snitch,
    ) -> Result<WriteResult, WriteError> {
        debug!(
            keyspace = %mutation.keyspace,
            table = %mutation.table,
            cl = ?cl,
            "StorageProxy.mutate_async"
        );

        // Get the plan and handler from WriteCoordinator
        let (plan, handler) = self
            .write_coordinator
            .coordinate_write_async(&mutation, cl, strategy, snitch)?;

        // Fan out mutations to remote live replicas via messaging
        let local_endpoint = Endpoint::new(self.config.listen_address);

        for replica in &plan.live_replicas {
            if *replica == local_endpoint {
                let payload = serde_json::to_vec(&MutationRequest {
                    mutation: mutation.clone(),
                })
                .unwrap_or_default();
                let msg = Message::request(Verb::Mutation, self.messaging.next_id(), payload);
                match self.messaging.dispatch(msg) {
                    Some(response) if mutation_response_succeeded(&response) => {
                        handler.on_response(replica);
                    }
                    Some(response) => {
                        warn!(
                            replica = %replica,
                            verb = %response.header.verb,
                            "Local mutation handler failed"
                        );
                        handler.on_failure(
                            *replica,
                            crate::write_response_handler::RequestFailureReason::Unknown,
                        );
                        if self.hint_store.store_hint(*replica, mutation.clone()) {
                            handler.on_hint_stored();
                        }
                    }
                    None => {
                        handler.on_failure(
                            *replica,
                            crate::write_response_handler::RequestFailureReason::Unknown,
                        );
                    }
                }
                continue;
            }
            let payload = serde_json::to_vec(&MutationRequest {
                mutation: mutation.clone(),
            })
            .unwrap_or_default();
            let msg = Message::request(Verb::Mutation, self.messaging.next_id(), payload);
            let handler_clone = Arc::clone(&handler);
            let replica_ep = *replica;
            let messaging = Arc::clone(&self.messaging);
            let timeout = self.config.write_timeout;
            let hint_store = Arc::clone(&self.hint_store);
            let mutation_clone = mutation.clone();

            // Send asynchronously and record ack/failure
            tokio::spawn(async move {
                match messaging.send_and_wait(replica_ep.0, msg, timeout).await {
                    Ok(response) if mutation_response_succeeded(&response) => {
                        handler_clone.on_response(&replica_ep);
                    }
                    Ok(response) => {
                        warn!(
                            replica = %replica_ep,
                            verb = %response.header.verb,
                            "Remote mutation handler failed"
                        );
                        handler_clone.on_failure(
                            replica_ep,
                            crate::write_response_handler::RequestFailureReason::Unknown,
                        );
                        if hint_store.store_hint(replica_ep, mutation_clone) {
                            handler_clone.on_hint_stored();
                        }
                    }
                    Err(e) => {
                        warn!(replica = %replica_ep, error = %e, "Remote mutation failed");
                        handler_clone.on_failure(
                            replica_ep,
                            crate::write_response_handler::RequestFailureReason::Unknown,
                        );
                        // Store hint for failed replica
                        if hint_store.store_hint(replica_ep, mutation_clone) {
                            handler_clone.on_hint_stored();
                        }
                    }
                }
            });
        }

        // Await CL satisfaction
        handler.await_completion().await
    }

    async fn send_mutation_to_replica(
        &self,
        replica: Endpoint,
        mutation: &CoordinatedMutation,
    ) -> Result<(), String> {
        let payload = serde_json::to_vec(&MutationRequest {
            mutation: mutation.clone(),
        })
        .map_err(|e| format!("Serialize mutation request: {e}"))?;
        let msg = Message::request(Verb::Mutation, self.messaging.next_id(), payload);

        let response = if replica.addr() == self.config.listen_address {
            self.messaging
                .dispatch(msg)
                .ok_or_else(|| format!("No local mutation handler for {replica}"))?
        } else {
            self.messaging
                .send_and_wait(replica.addr(), msg, self.config.write_timeout)
                .await
                .map_err(|e| e.to_string())?
        };

        if mutation_response_succeeded(&response) {
            Ok(())
        } else {
            Err(format!(
                "Mutation response from {replica} was {}",
                response.header.verb
            ))
        }
    }

    /// Coordinate a read from replicas.
    ///
    /// Sends `Verb::ReadData` / `Verb::ReadDigest` messages to replicas
    /// via the messaging service, collects responses, handles digest
    /// mismatch, and triggers read repair when needed.
    ///
    /// ## Java Oracle
    ///
    /// `StorageProxy.fetchRows()`
    pub async fn fetch_rows(
        &self,
        read: &CoordinatedRead,
        cl: ConsistencyLevel,
        strategy: &dyn ReplicationStrategy,
        snitch: &dyn Snitch,
    ) -> Result<ReadResult, ReadError> {
        debug!(
            keyspace = %read.keyspace,
            table = %read.table,
            cl = ?cl,
            "StorageProxy.fetch_rows"
        );

        // Delegate to the read coordinator for replica selection, availability,
        // CL calculation, speculative policy bookkeeping, and tombstone limits.
        let planned = self
            .read_coordinator
            .plan_read(read, cl, strategy, snitch)?;

        self.fetch_rows_via_messaging(read, planned).await
    }

    /// Fetch rows asynchronously with replica messaging.
    ///
    /// ## Java Oracle
    ///
    /// `StorageProxy.fetchRows()` -> `AbstractReadExecutor.execute()`
    pub async fn fetch_rows_async(
        &self,
        read: &CoordinatedRead,
        cl: ConsistencyLevel,
        strategy: &dyn ReplicationStrategy,
        snitch: &dyn Snitch,
    ) -> Result<ReadResult, ReadError> {
        debug!(
            keyspace = %read.keyspace,
            table = %read.table,
            cl = ?cl,
            "StorageProxy.fetch_rows_async"
        );

        self.fetch_rows(read, cl, strategy, snitch).await
    }

    async fn fetch_rows_via_messaging(
        &self,
        read: &CoordinatedRead,
        planned: ReadResult,
    ) -> Result<ReadResult, ReadError> {
        let Some(data_replica) = planned.contacted_replicas.first().copied() else {
            return Ok(planned);
        };

        let data_request = ReadDataRequest {
            keyspace: read.keyspace.clone(),
            table: read.table.clone(),
            partition_key: read.partition_key.clone(),
        };
        let data_msg = Message::request(
            Verb::ReadData,
            self.messaging.next_id(),
            serde_json::to_vec(&data_request)
                .map_err(|e| ReadError::Internal(format!("Serialize ReadData request: {e}")))?,
        );
        let data_response = self.send_read_message(data_replica, data_msg).await?;
        if data_response.header.verb != Verb::ReadDataResponse {
            return Err(ReadError::Internal(format!(
                "ReadData from {data_replica} returned {}",
                data_response.header.verb
            )));
        }
        let data_payload: ReadDataResponsePayload = serde_json::from_slice(&data_response.payload)
            .map_err(|e| {
                ReadError::Internal(format!(
                    "ReadData response from {data_replica} invalid: {e}"
                ))
            })?;

        let mut responses_received = 1usize;
        let mut contacted = vec![data_replica];
        let mut digest_mismatches = 0usize;

        for digest_replica in planned
            .contacted_replicas
            .iter()
            .copied()
            .skip(1)
            .take(planned.responses_required.saturating_sub(1))
        {
            let digest_request = ReadDigestRequest {
                keyspace: read.keyspace.clone(),
                table: read.table.clone(),
                partition_key: read.partition_key.clone(),
            };
            let digest_msg = Message::request(
                Verb::ReadDigest,
                self.messaging.next_id(),
                serde_json::to_vec(&digest_request).map_err(|e| {
                    ReadError::Internal(format!("Serialize ReadDigest request: {e}"))
                })?,
            );
            let digest_response = self.send_read_message(digest_replica, digest_msg).await?;
            if digest_response.header.verb != Verb::ReadDigestResponse {
                return Err(ReadError::Internal(format!(
                    "ReadDigest from {digest_replica} returned {}",
                    digest_response.header.verb
                )));
            }
            let digest_payload: ReadDigestResponsePayload =
                serde_json::from_slice(&digest_response.payload).map_err(|e| {
                    ReadError::Internal(format!(
                        "ReadDigest response from {digest_replica} invalid: {e}"
                    ))
                })?;
            responses_received += 1;
            contacted.push(digest_replica);
            if digest_payload.digest != data_payload.digest {
                digest_mismatches += 1;
            }
        }

        if digest_mismatches > 0 {
            return Err(ReadError::DigestMismatch {
                replicas_mismatched: digest_mismatches,
            });
        }

        let data = data_payload
            .partitions
            .first()
            .map(|partition| partition.data.clone());
        Ok(ReadResult {
            data,
            partitions: Vec::new(),
            responses_received,
            responses_required: planned.responses_required,
            read_repair_triggered: false,
            digest_mismatch_resolved: false,
            contacted_replicas: contacted,
            warnings: planned.warnings,
            paging_state: planned.paging_state,
            speculative_retry_used: planned.speculative_retry_used,
            tombstones_read: data_payload.tombstones_read,
        })
    }

    async fn send_read_message(
        &self,
        endpoint: Endpoint,
        msg: Message,
    ) -> Result<Message, ReadError> {
        if endpoint.addr() == self.config.listen_address {
            return self.messaging.dispatch(msg).ok_or_else(|| {
                ReadError::Internal(format!("No local read handler for {endpoint}"))
            });
        }
        self.messaging
            .send_and_wait(endpoint.addr(), msg, self.config.read_timeout)
            .await
            .map_err(|e| ReadError::Internal(format!("Read request to {endpoint} failed: {e}")))
    }

    // ─── CAS / LWT ──────────────────────────────────────────────────

    /// Execute a Compare-And-Set (CAS) operation.
    ///
    /// Resolves the `TransactionalMode` from the table metadata and delegates
    /// to the `ConsensusRouter`.
    ///
    /// ## Java Oracle
    ///
    /// `StorageProxy.cas()`
    #[allow(clippy::too_many_arguments)]
    pub async fn cas<R, F1, F2>(
        &self,
        keyspace: &str,
        table: &str,
        mode: TransactionalMode,
        partition_key: &[u8],
        mutation: Vec<u8>,
        read_current_fn: F1,
        condition_fn: F2,
    ) -> Result<CasResult, Box<dyn std::error::Error + Send + Sync>>
    where
        R: std::future::Future<Output = Option<Vec<u8>>> + Send + 'static,
        F1: Fn() -> R + Send + Sync,
        F2: Fn(Option<&[u8]>) -> bool + Send + Sync,
    {
        let router = self
            .consensus_router
            .as_ref()
            .ok_or("Consensus router not configured — CAS operations unavailable")?;

        debug!(
            keyspace,
            table,
            mode = ?mode,
            "StorageProxy.cas"
        );

        router
            .execute_cas(
                keyspace,
                table,
                mode,
                partition_key,
                mutation,
                read_current_fn,
                condition_fn,
            )
            .await
    }

    /// Execute a CAS operation explicitly via Paxos.
    ///
    /// ## Java Oracle
    ///
    /// `StorageProxy.casPaxos()`
    #[allow(clippy::too_many_arguments)]
    pub async fn cas_paxos<R, F1, F2>(
        &self,
        keyspace: &str,
        table: &str,
        partition_key: &[u8],
        mutation: Vec<u8>,
        read_current_fn: F1,
        condition_fn: F2,
    ) -> Result<CasResult, Box<dyn std::error::Error + Send + Sync>>
    where
        R: std::future::Future<Output = Option<Vec<u8>>> + Send + 'static,
        F1: Fn() -> R + Send + Sync,
        F2: Fn(Option<&[u8]>) -> bool + Send + Sync,
    {
        self.cas(
            keyspace,
            table,
            TransactionalMode::Paxos,
            partition_key,
            mutation,
            read_current_fn,
            condition_fn,
        )
        .await
    }

    /// Execute a CAS operation explicitly via Accord.
    ///
    /// ## Java Oracle
    ///
    /// `StorageProxy.casAccord()`
    pub async fn cas_accord(
        &self,
        keyspace: &str,
        mutations: Vec<Vec<u8>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let router = self
            .consensus_router
            .as_ref()
            .ok_or("Consensus router not configured — CAS operations unavailable")?;

        debug!(keyspace, "StorageProxy.cas_accord");

        router.execute_accord_transaction(keyspace, mutations).await
    }

    /// Whether this node is currently bootstrapping.
    pub fn is_bootstrapping(&self) -> bool {
        self.config.is_bootstrapping
    }

    /// Set the bootstrapping state.
    pub fn set_bootstrapping(&mut self, bootstrapping: bool) {
        self.config.is_bootstrapping = bootstrapping;
    }
}

fn mutation_response_succeeded(response: &Message) -> bool {
    if response.header.verb != Verb::MutationResponse {
        return false;
    }
    serde_json::from_slice::<MutationResponse>(&response.payload)
        .map(|body| body.success)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::BatchLogManager;
    use crate::hints::HintStore;
    use crate::verb_handlers::registration::register_all_verb_handlers_with_storage;
    use crate::write::{CellMutation, MutationRow};
    use cassandra_cluster_metadata::node::{NodeId, NodeInfo};
    use cassandra_cluster_metadata::{ClusterMetadata, SimpleSnitch, SimpleStrategy};
    use cassandra_common::Token;
    use cassandra_storage::commitlog::{
        CellMutation as StorageCellMutation, CommitLogConfig, Mutation as StorageMutation,
        MutationRow as StorageMutationRow,
    };
    use cassandra_storage::engine::{EngineConfig, StorageEngine};
    use tempfile::TempDir;

    fn make_proxy() -> StorageProxy {
        let ep = Endpoint::new("127.0.0.1:7000".parse().unwrap());
        let node = NodeInfo::new(
            NodeId::random(),
            ep,
            "dc1",
            "rack1",
            vec![Token::from_raw(0)],
        );
        let cluster = Arc::new(ClusterMetadata::new(node));
        let hint_store = Arc::new(HintStore::new(1000));
        let write_coord = Arc::new(WriteCoordinator::new(
            Arc::clone(&cluster),
            ep,
            Arc::clone(&hint_store),
        ));
        let read_coord = Arc::new(ReadCoordinator::new(Arc::clone(&cluster), ep));
        let messaging = Arc::new(MessagingService::new("127.0.0.1:7000".parse().unwrap()));
        let batch_log = Arc::new(BatchLogManager::new());

        StorageProxy::new(
            write_coord,
            read_coord,
            messaging,
            hint_store,
            batch_log,
            StorageProxyConfig::default(),
        )
    }

    #[test]
    fn construction_and_accessors() {
        let proxy = make_proxy();
        assert!(!proxy.is_bootstrapping());
        assert_eq!(proxy.config().write_timeout, Duration::from_secs(2));
        assert_eq!(proxy.config().read_timeout, Duration::from_secs(5));
        assert_eq!(proxy.config().local_datacenter, "datacenter1");
    }

    #[test]
    fn set_bootstrapping() {
        let mut proxy = make_proxy();
        proxy.set_bootstrapping(true);
        assert!(proxy.is_bootstrapping());
        proxy.set_bootstrapping(false);
        assert!(!proxy.is_bootstrapping());
    }

    #[test]
    fn config_default() {
        let config = StorageProxyConfig::default();
        assert_eq!(config.write_timeout, Duration::from_secs(2));
        assert_eq!(config.read_timeout, Duration::from_secs(5));
        assert!(!config.is_bootstrapping);
        assert_eq!(config.max_concurrent_reads, 1024);
        assert_eq!(config.max_concurrent_writes, 1024);
    }

    #[test]
    fn accessor_types() {
        let proxy = make_proxy();
        // Ensure all accessors return the correct Arc types.
        let _wc: &Arc<WriteCoordinator> = proxy.write_coordinator();
        let _rc: &Arc<ReadCoordinator> = proxy.read_coordinator();
        let _ms: &Arc<MessagingService> = proxy.messaging();
        let _hs: &Arc<HintStore> = proxy.hint_store();
        let _bl: &Arc<BatchLogManager> = proxy.batch_log();
    }

    #[tokio::test]
    async fn fetch_rows_uses_storage_backed_read_verb_handler() {
        let proxy = make_proxy();
        let temp = TempDir::new().unwrap();
        let storage = Arc::new(
            StorageEngine::open(EngineConfig {
                data_directories: vec![temp.path().join("data")],
                commitlog: CommitLogConfig {
                    directory: temp.path().join("commitlog"),
                    ..CommitLogConfig::default()
                },
                ..EngineConfig::default()
            })
            .unwrap(),
        );
        storage
            .apply_mutation(&StorageMutation {
                keyspace: "ks".to_string(),
                table: "users".to_string(),
                partition_key: b"pk1".to_vec(),
                rows: vec![StorageMutationRow {
                    clustering_key: Vec::new(),
                    cells: vec![StorageCellMutation {
                        column: "name".to_string(),
                        value: Some(b"alice".to_vec()),
                        timestamp: 1,
                        ttl: 0,
                        local_deletion_time: None,
                        is_tombstone: false,
                    }],
                    is_tombstone: false,
                    local_deletion_time: None,
                }],
                timestamp: 1,
                cdc_enabled: false,
                static_cells: Vec::new(),
                partition_tombstone: None,
                range_tombstones: Vec::new(),
            })
            .unwrap();
        register_all_verb_handlers_with_storage(proxy.messaging(), Arc::clone(&storage));

        let result = proxy
            .fetch_rows(
                &CoordinatedRead {
                    keyspace: "ks".to_string(),
                    table: "users".to_string(),
                    partition_key: b"pk1".to_vec(),
                },
                ConsistencyLevel::One,
                &SimpleStrategy::new(1),
                &SimpleSnitch,
            )
            .await
            .unwrap();

        assert_eq!(result.responses_received, 1);
        assert!(result.data.as_ref().is_some_and(|data| !data.is_empty()));
        assert_eq!(result.contacted_replicas.len(), 1);

        let async_result = proxy
            .fetch_rows_async(
                &CoordinatedRead {
                    keyspace: "ks".to_string(),
                    table: "users".to_string(),
                    partition_key: b"pk1".to_vec(),
                },
                ConsistencyLevel::One,
                &SimpleStrategy::new(1),
                &SimpleSnitch,
            )
            .await
            .unwrap();
        assert!(
            async_result
                .data
                .as_ref()
                .is_some_and(|data| !data.is_empty())
        );
    }

    #[tokio::test]
    async fn mutate_uses_storage_backed_local_mutation_handler() {
        let proxy = make_proxy();
        let temp = TempDir::new().unwrap();
        let storage = Arc::new(
            StorageEngine::open(EngineConfig {
                data_directories: vec![temp.path().join("data")],
                commitlog: CommitLogConfig {
                    directory: temp.path().join("commitlog"),
                    ..CommitLogConfig::default()
                },
                ..EngineConfig::default()
            })
            .unwrap(),
        );
        register_all_verb_handlers_with_storage(proxy.messaging(), Arc::clone(&storage));

        let result = proxy
            .mutate(
                CoordinatedMutation::simple(
                    "ks".to_string(),
                    "users".to_string(),
                    b"pk2".to_vec(),
                    vec![MutationRow {
                        clustering_key: Vec::new(),
                        cells: vec![CellMutation {
                            column: "name".to_string(),
                            value: Some(b"bob".to_vec()),
                            timestamp: 1,
                            ttl: 0,
                            is_tombstone: false,
                            collection_op: None,
                        }],
                        is_tombstone: false,
                        range_tombstone: None,
                    }],
                    1,
                ),
                ConsistencyLevel::One,
                &SimpleStrategy::new(1),
                &SimpleSnitch,
            )
            .await
            .unwrap();

        assert_eq!(result.acks_received, 1);
        let partition = storage.read_partition("ks", "users", b"pk2").unwrap();
        let row = partition.rows.values().next().unwrap();
        assert_eq!(row.cells[0].value.as_deref(), Some(b"bob".as_slice()));
    }
}
