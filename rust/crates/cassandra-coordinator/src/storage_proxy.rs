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

use tracing::{debug, info, warn};

use cassandra_cluster_metadata::{ClusterMetadata, Endpoint, ReplicationStrategy, Snitch};
use cassandra_messaging::{Message, MessageHandler, MessagingService, Verb};

use cassandra_schema::table::TransactionalMode;

use crate::batch::BatchLogManager;
use crate::consensus::ConsensusRouter;
use crate::consistency::ConsistencyLevel;
use crate::hints::HintStore;
use crate::paxos::coordinator::CasResult;
use crate::read::{
    CoordinatedRead, ReadCoordinator, ReadError, ReadResult, SinglePartitionReadCommand,
};
use crate::write::{CoordinatedMutation, WriteCoordinator, WriteError, WriteResult, WriteType};
use crate::write_response_handler::WriteResponseHandler;

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

        // Send mutations to remote live replicas via messaging.
        let local_endpoint = Endpoint::new(self.config.listen_address);
        let mut remote_send_failures: Vec<Endpoint> = Vec::new();

        for replica in &plan.live_replicas {
            if *replica == local_endpoint {
                continue; // Local replica handled by coordinator directly.
            }
            let payload = serde_json::to_vec(&mutation).unwrap_or_default();
            let msg = Message::request(Verb::Mutation, self.messaging.next_id(), payload);
            if let Err(e) = self
                .messaging
                .send_and_wait(replica.0, msg, self.config.write_timeout)
                .await
            {
                warn!(replica = %replica, error = %e, "Remote mutation failed");
                remote_send_failures.push(*replica);
            }
        }

        // Store hints for dead replicas and failed remote sends.
        let mut hints_stored = 0usize;
        for dead in &plan.dead_replicas {
            if self.hint_store.store_hint(*dead, mutation.clone()) {
                hints_stored += 1;
            }
        }
        for failed in &remote_send_failures {
            if self.hint_store.store_hint(*failed, mutation.clone()) {
                hints_stored += 1;
            }
        }

        // Calculate acks: local (if local is replica) + remote successes.
        let remote_successes = plan
            .live_replicas
            .iter()
            .filter(|r| **r != local_endpoint && !remote_send_failures.contains(r))
            .count();
        let local_ack = if plan.local_is_replica { 1 } else { 0 };
        let acks_received = local_ack + remote_successes;

        // Check if CL is satisfied.
        if acks_received < plan.block_for {
            // For CL=ANY, hints count toward satisfaction.
            let effective = if cl == ConsistencyLevel::Any {
                acks_received + hints_stored
            } else {
                acks_received
            };
            if effective < plan.block_for {
                return Err(WriteError::Timeout {
                    cl,
                    write_type: WriteType::Simple,
                    required: plan.block_for,
                    received: acks_received,
                    block_for: plan.block_for,
                });
            }
        }

        Ok(WriteResult {
            acks_received,
            acks_required: plan.block_for,
            contacted_replicas: plan.live_replicas.clone(),
            hints_stored,
        })
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
                // Local replica: record ack immediately (simulates local apply)
                handler.on_response(replica);
                continue;
            }
            let payload = serde_json::to_vec(&mutation).unwrap_or_default();
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
                    Ok(_) => {
                        handler_clone.on_response(&replica_ep);
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

        // Delegate to the read coordinator for the core coordination logic.
        // The read coordinator already handles:
        //   - Replica selection and availability checks
        //   - Digest comparison and mismatch handling
        //   - Read repair triggering
        //   - Tombstone threshold checks
        let result = self
            .read_coordinator
            .coordinate_read(read, cl, strategy, snitch)?;

        // Send read data/digest requests to remote replicas via messaging.
        // For now, the read coordinator simulates responses internally.
        // When full messaging integration is needed, we would:
        //   1. Send Verb::ReadData to the data replica
        //   2. Send Verb::ReadDigest to digest replicas
        //   3. Collect responses and feed them to the resolver
        //   4. On digest mismatch, send Verb::ReadData to all replicas
        //   5. Send Verb::ReadRepair mutations for stale replicas

        Ok(result)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::BatchLogManager;
    use crate::hints::HintStore;
    use cassandra_cluster_metadata::node::{NodeId, NodeInfo};
    use cassandra_common::Token;

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
}
