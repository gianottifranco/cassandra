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

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tracing::{debug, warn};

use cassandra_cluster_metadata::{Endpoint, ReplicationStrategy, Snitch};
use cassandra_messaging::{Message, MessagingService, Verb};

use cassandra_schema::table::TransactionalMode;
use cassandra_storage::memtable::partition::{Cell, PartitionData, Row};

use crate::batch::BatchLogManager;
use crate::consensus::ConsensusRouter;
use crate::consistency::ConsistencyLevel;
use crate::hints::HintStore;
use crate::paxos::coordinator::CasResult;
use crate::read::{
    CoordinatedRead, DataResponse, Digest, PartitionResult, ReadCoordinator, ReadError, ReadResult,
};
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

        self.fetch_rows_via_messaging(read, cl, planned).await
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
        cl: ConsistencyLevel,
        planned: ReadResult,
    ) -> Result<ReadResult, ReadError> {
        if planned.contacted_replicas.is_empty() {
            return Ok(planned);
        };

        let mut failure_map = HashMap::new();
        let mut num_failures = 0usize;
        let mut data_replica = None;
        let mut data_payload = None;

        for candidate in planned.contacted_replicas.iter().copied() {
            match self.fetch_full_data_payload(read, candidate).await {
                Ok(payload) => {
                    data_replica = Some(candidate);
                    data_payload = Some(payload);
                    break;
                }
                Err(err) => {
                    warn!(
                        replica = %candidate,
                        error = %err,
                        "ReadData request failed; trying next read candidate"
                    );
                    num_failures += 1;
                    failure_map.insert(candidate, 0);
                }
            }
        }

        let Some(data_replica) = data_replica else {
            return Err(ReadError::ReadFailure {
                cl,
                required: planned.responses_required,
                received: 0,
                num_failures,
                data_present: false,
                failure_map,
            });
        };
        let data_payload = data_payload.unwrap();

        let mut responses_received = 1usize;
        let mut contacted = vec![data_replica];
        let mut digest_mismatches = 0usize;
        let mut digest_responses = Vec::new();
        let digest_candidates: Vec<Endpoint> = planned
            .contacted_replicas
            .iter()
            .copied()
            .filter(|replica| *replica != data_replica && !failure_map.contains_key(replica))
            .collect();

        for digest_replica in digest_candidates {
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
            let digest_response = match self.send_read_message(digest_replica, digest_msg).await {
                Ok(response) => response,
                Err(err) => {
                    warn!(
                        replica = %digest_replica,
                        error = %err,
                        "ReadDigest request failed; trying next read candidate"
                    );
                    num_failures += 1;
                    failure_map.insert(digest_replica, 0);
                    continue;
                }
            };
            if digest_response.header.verb != Verb::ReadDigestResponse {
                warn!(
                    replica = %digest_replica,
                    verb = %digest_response.header.verb,
                    "ReadDigest request returned unexpected verb; trying next read candidate"
                );
                num_failures += 1;
                failure_map.insert(digest_replica, 0);
                continue;
            }
            let digest_payload: ReadDigestResponsePayload =
                serde_json::from_slice(&digest_response.payload).map_err(|e| {
                    ReadError::Internal(format!(
                        "ReadDigest response from {digest_replica} invalid: {e}"
                    ))
                })?;
            responses_received += 1;
            contacted.push(digest_replica);
            digest_responses.push(Digest(digest_payload.digest));
            if digest_payload.digest != data_payload.digest {
                digest_mismatches += 1;
            }
            if responses_received >= planned.responses_required && !planned.speculative_retry_used {
                break;
            }
        }

        if responses_received < planned.responses_required {
            return Err(ReadError::ReadFailure {
                cl,
                required: planned.responses_required,
                received: responses_received,
                num_failures,
                data_present: true,
                failure_map,
            });
        }

        if digest_mismatches > 0 {
            let data_response = read_data_payload_to_response(&data_payload)?;
            let mut full_responses = vec![data_response.clone()];
            for replica in contacted.iter().copied().skip(1) {
                let payload = self.fetch_full_data_payload(read, replica).await?;
                full_responses.push(read_data_payload_to_response(&payload)?);
            }

            let resolved = self.read_coordinator.resolve_replica_responses(
                read,
                &contacted,
                data_response,
                digest_responses,
                full_responses,
                now_in_seconds(),
            )?;
            if resolved.read_repair.pending_count() > 0 {
                let mut read_repair = resolved.read_repair;
                let queued = self
                    .read_coordinator
                    .read_repair_scheduler()
                    .enqueue_from_handler(&mut read_repair);
                debug!(queued, "Queued read repairs after digest mismatch");
            }

            let data = resolved
                .data
                .partitions
                .first()
                .and_then(|partition| partition.data.as_ref())
                .map(encode_result_partition_data);
            return Ok(ReadResult {
                data,
                partitions: resolved.data.partitions,
                responses_received,
                responses_required: planned.responses_required,
                read_repair_triggered: true,
                digest_mismatch_resolved: true,
                contacted_replicas: contacted,
                warnings: resolved.warnings,
                paging_state: planned.paging_state,
                speculative_retry_used: planned.speculative_retry_used,
                tombstones_read: resolved.tombstones_read,
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

    async fn fetch_full_data_payload(
        &self,
        read: &CoordinatedRead,
        endpoint: Endpoint,
    ) -> Result<ReadDataResponsePayload, ReadError> {
        let request = ReadDataRequest {
            keyspace: read.keyspace.clone(),
            table: read.table.clone(),
            partition_key: read.partition_key.clone(),
        };
        let msg = Message::request(
            Verb::ReadData,
            self.messaging.next_id(),
            serde_json::to_vec(&request)
                .map_err(|e| ReadError::Internal(format!("Serialize ReadData request: {e}")))?,
        );
        let response = self.send_read_message(endpoint, msg).await?;
        if response.header.verb != Verb::ReadDataResponse {
            return Err(ReadError::Internal(format!(
                "ReadData from {endpoint} returned {}",
                response.header.verb
            )));
        }
        serde_json::from_slice(&response.payload).map_err(|e| {
            ReadError::Internal(format!("ReadData response from {endpoint} invalid: {e}"))
        })
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

fn read_data_payload_to_response(
    payload: &ReadDataResponsePayload,
) -> Result<DataResponse, ReadError> {
    let mut partitions = Vec::with_capacity(payload.partitions.len());
    for partition in &payload.partitions {
        let data = if partition.data.is_empty() {
            None
        } else {
            Some(decode_partition_data(&partition.data)?)
        };
        partitions.push(PartitionResult {
            partition_key: partition.partition_key.clone(),
            data,
            live_row_count: partition.live_row_count,
            was_truncated: partition.was_truncated,
        });
    }

    Ok(DataResponse {
        partitions,
        tombstones_read: payload.tombstones_read,
        is_short_read: payload.is_short_read,
    })
}

fn decode_partition_data(bytes: &[u8]) -> Result<PartitionData, ReadError> {
    let mut input = bytes;
    let tombstone_timestamp = read_opt_i64(&mut input, "partition tombstone timestamp")?;
    let tombstone_local_deletion_time =
        read_opt_i32(&mut input, "partition tombstone local deletion time")?;
    let row_count = read_u32(&mut input, "row count")? as usize;

    let mut data = PartitionData {
        rows: Default::default(),
        tombstone_timestamp,
        tombstone_local_deletion_time,
    };

    for _ in 0..row_count {
        let clustering_key = read_len_prefixed_bytes(&mut input, "clustering key")?;
        let is_tombstone = read_bool(&mut input, "row tombstone")?;
        let local_deletion_time = read_opt_i32(&mut input, "row local deletion time")?;
        let cell_count = read_u32(&mut input, "cell count")? as usize;
        let mut cells = Vec::with_capacity(cell_count);

        for _ in 0..cell_count {
            let column = String::from_utf8(read_len_prefixed_bytes(&mut input, "column")?)
                .map_err(|e| ReadError::Internal(format!("ReadData column not UTF-8: {e}")))?;
            let has_value = read_bool(&mut input, "cell value presence")?;
            let value = if has_value {
                Some(read_len_prefixed_bytes(&mut input, "cell value")?)
            } else {
                None
            };
            let timestamp = read_i64(&mut input, "cell timestamp")?;
            let ttl = read_i32(&mut input, "cell ttl")?;
            let cell_local_deletion_time = read_opt_i32(&mut input, "cell local deletion time")?;
            let cell_is_tombstone = read_bool(&mut input, "cell tombstone")?;
            cells.push(Cell {
                column,
                value,
                timestamp,
                ttl,
                local_deletion_time: cell_local_deletion_time,
                is_tombstone: cell_is_tombstone,
            });
        }

        data.rows.insert(
            clustering_key.clone(),
            Row {
                clustering_key,
                cells,
                is_tombstone,
                local_deletion_time,
            },
        );
    }

    if !input.is_empty() {
        return Err(ReadError::Internal(format!(
            "ReadData partition payload has {} trailing bytes",
            input.len()
        )));
    }

    Ok(data)
}

fn encode_result_partition_data(data: &PartitionData) -> Vec<u8> {
    let mut out = Vec::new();
    write_opt_i64(&mut out, data.tombstone_timestamp);
    write_opt_i32(&mut out, data.tombstone_local_deletion_time);
    write_u32(&mut out, data.rows.len());
    for (clustering_key, row) in &data.rows {
        write_bytes(&mut out, clustering_key);
        out.push(u8::from(row.is_tombstone));
        write_opt_i32(&mut out, row.local_deletion_time);
        write_u32(&mut out, row.cells.len());
        for cell in &row.cells {
            write_bytes(&mut out, cell.column.as_bytes());
            match &cell.value {
                Some(value) => {
                    out.push(1);
                    write_bytes(&mut out, value);
                }
                None => out.push(0),
            }
            out.extend_from_slice(&cell.timestamp.to_be_bytes());
            out.extend_from_slice(&cell.ttl.to_be_bytes());
            write_opt_i32(&mut out, cell.local_deletion_time);
            out.push(u8::from(cell.is_tombstone));
        }
    }
    out
}

fn now_in_seconds() -> i32 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs() as i32
}

fn read_u32(input: &mut &[u8], field: &str) -> Result<u32, ReadError> {
    if input.len() < 4 {
        return Err(ReadError::Internal(format!("{field} is truncated")));
    }
    let (head, tail) = input.split_at(4);
    *input = tail;
    Ok(u32::from_be_bytes(head.try_into().unwrap()))
}

fn read_i64(input: &mut &[u8], field: &str) -> Result<i64, ReadError> {
    if input.len() < 8 {
        return Err(ReadError::Internal(format!("{field} is truncated")));
    }
    let (head, tail) = input.split_at(8);
    *input = tail;
    Ok(i64::from_be_bytes(head.try_into().unwrap()))
}

fn read_i32(input: &mut &[u8], field: &str) -> Result<i32, ReadError> {
    if input.len() < 4 {
        return Err(ReadError::Internal(format!("{field} is truncated")));
    }
    let (head, tail) = input.split_at(4);
    *input = tail;
    Ok(i32::from_be_bytes(head.try_into().unwrap()))
}

fn read_bool(input: &mut &[u8], field: &str) -> Result<bool, ReadError> {
    if input.is_empty() {
        return Err(ReadError::Internal(format!("{field} is truncated")));
    }
    let value = input[0];
    *input = &input[1..];
    match value {
        0 => Ok(false),
        1 => Ok(true),
        other => Err(ReadError::Internal(format!(
            "{field} has invalid boolean value {other}"
        ))),
    }
}

fn read_len_prefixed_bytes(input: &mut &[u8], field: &str) -> Result<Vec<u8>, ReadError> {
    let len = read_u32(input, field)? as usize;
    if input.len() < len {
        return Err(ReadError::Internal(format!(
            "{field} length {len} exceeds remaining payload {}",
            input.len()
        )));
    }
    let (head, tail) = input.split_at(len);
    *input = tail;
    Ok(head.to_vec())
}

fn read_opt_i64(input: &mut &[u8], field: &str) -> Result<Option<i64>, ReadError> {
    Ok(if read_bool(input, field)? {
        Some(read_i64(input, field)?)
    } else {
        None
    })
}

fn read_opt_i32(input: &mut &[u8], field: &str) -> Result<Option<i32>, ReadError> {
    Ok(if read_bool(input, field)? {
        Some(read_i32(input, field)?)
    } else {
        None
    })
}

fn write_u32(out: &mut Vec<u8>, value: usize) {
    out.extend_from_slice(&(value as u32).to_be_bytes());
}

fn write_bytes(out: &mut Vec<u8>, value: &[u8]) {
    write_u32(out, value.len());
    out.extend_from_slice(value);
}

fn write_opt_i64(out: &mut Vec<u8>, value: Option<i64>) {
    match value {
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&value.to_be_bytes());
        }
        None => out.push(0),
    }
}

fn write_opt_i32(out: &mut Vec<u8>, value: Option<i32>) {
    match value {
        Some(value) => {
            out.push(1);
            out.extend_from_slice(&value.to_be_bytes());
        }
        None => out.push(0),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::batch::BatchLogManager;
    use crate::hints::HintStore;
    use crate::read::SpeculativeRetryPolicy;
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
    use std::net::TcpListener;
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

    fn free_addr() -> SocketAddr {
        TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
    }

    fn open_test_storage(temp: &TempDir, name: &str) -> Arc<StorageEngine> {
        Arc::new(
            StorageEngine::open(EngineConfig {
                data_directories: vec![temp.path().join(name).join("data")],
                commitlog: CommitLogConfig {
                    directory: temp.path().join(name).join("commitlog"),
                    ..CommitLogConfig::default()
                },
                ..EngineConfig::default()
            })
            .unwrap(),
        )
    }

    fn apply_test_value(
        storage: &StorageEngine,
        partition_key: &[u8],
        value: &[u8],
        timestamp: i64,
    ) {
        storage
            .apply_mutation(&StorageMutation {
                keyspace: "ks".to_string(),
                table: "users".to_string(),
                partition_key: partition_key.to_vec(),
                rows: vec![StorageMutationRow {
                    clustering_key: Vec::new(),
                    cells: vec![StorageCellMutation {
                        column: "name".to_string(),
                        value: Some(value.to_vec()),
                        timestamp,
                        ttl: 0,
                        local_deletion_time: None,
                        is_tombstone: false,
                    }],
                    is_tombstone: false,
                    local_deletion_time: None,
                }],
                timestamp,
                cdc_enabled: false,
                static_cells: Vec::new(),
                partition_tombstone: None,
                range_tombstones: Vec::new(),
            })
            .unwrap();
    }

    fn make_two_node_proxy(
        local_addr: SocketAddr,
        remote_addr: SocketAddr,
        partition_key: &[u8],
    ) -> StorageProxy {
        let key_token = Token::from_partition_key(partition_key);
        let remote_token = if key_token.value() == i64::MAX {
            Token::from_raw(i64::MIN)
        } else {
            Token::from_raw(key_token.value() + 1)
        };
        let local_ep = Endpoint::new(local_addr);
        let remote_ep = Endpoint::new(remote_addr);
        let local_node = NodeInfo::new(NodeId::random(), local_ep, "dc1", "rack1", vec![key_token]);
        let remote_node = NodeInfo::new(
            NodeId::random(),
            remote_ep,
            "dc1",
            "rack1",
            vec![remote_token],
        );
        let cluster = Arc::new(ClusterMetadata::new(local_node));
        cluster.update_node(remote_node);
        let hint_store = Arc::new(HintStore::new(1000));
        let write_coord = Arc::new(WriteCoordinator::new(
            Arc::clone(&cluster),
            local_ep,
            Arc::clone(&hint_store),
        ));
        let read_coord = Arc::new(ReadCoordinator::new(Arc::clone(&cluster), local_ep));
        let messaging = Arc::new(MessagingService::new(local_addr));
        let batch_log = Arc::new(BatchLogManager::new());

        StorageProxy::new(
            write_coord,
            read_coord,
            messaging,
            hint_store,
            batch_log,
            StorageProxyConfig {
                listen_address: local_addr,
                read_timeout: Duration::from_secs(1),
                ..StorageProxyConfig::default()
            },
        )
    }

    fn make_three_node_proxy(
        local_addr: SocketAddr,
        remote_one_addr: SocketAddr,
        remote_two_addr: SocketAddr,
        partition_key: &[u8],
        speculative_retry: SpeculativeRetryPolicy,
    ) -> StorageProxy {
        let key_token = Token::from_partition_key(partition_key);
        let local_ep = Endpoint::new(local_addr);
        let remote_one_ep = Endpoint::new(remote_one_addr);
        let remote_two_ep = Endpoint::new(remote_two_addr);
        let local_node = NodeInfo::new(NodeId::random(), local_ep, "dc1", "rack1", vec![key_token]);
        let remote_one_node = NodeInfo::new(
            NodeId::random(),
            remote_one_ep,
            "dc1",
            "rack1",
            vec![Token::from_raw(key_token.value().saturating_add(1))],
        );
        let remote_two_node = NodeInfo::new(
            NodeId::random(),
            remote_two_ep,
            "dc1",
            "rack1",
            vec![Token::from_raw(key_token.value().saturating_add(2))],
        );
        let cluster = Arc::new(ClusterMetadata::new(local_node));
        cluster.update_node(remote_one_node);
        cluster.update_node(remote_two_node);
        let hint_store = Arc::new(HintStore::new(1000));
        let write_coord = Arc::new(WriteCoordinator::new(
            Arc::clone(&cluster),
            local_ep,
            Arc::clone(&hint_store),
        ));
        let read_coord = Arc::new(
            ReadCoordinator::new(Arc::clone(&cluster), local_ep)
                .with_speculative_retry(speculative_retry),
        );
        let messaging = Arc::new(MessagingService::new(local_addr));
        let batch_log = Arc::new(BatchLogManager::new());

        StorageProxy::new(
            write_coord,
            read_coord,
            messaging,
            hint_store,
            batch_log,
            StorageProxyConfig {
                listen_address: local_addr,
                read_timeout: Duration::from_millis(150),
                ..StorageProxyConfig::default()
            },
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
        let storage = open_test_storage(&temp, "local");
        apply_test_value(&storage, b"pk1", b"alice", 1);
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
    async fn fetch_rows_resolves_digest_mismatch_and_queues_read_repair() {
        let partition_key = b"pk-digest-mismatch";
        let local_addr = free_addr();
        let remote_addr = free_addr();
        let proxy = make_two_node_proxy(local_addr, remote_addr, partition_key);
        let temp = TempDir::new().unwrap();
        let local_storage = open_test_storage(&temp, "local");
        let remote_storage = open_test_storage(&temp, "remote");
        apply_test_value(&local_storage, partition_key, b"stale", 1);
        apply_test_value(&remote_storage, partition_key, b"fresh", 2);
        register_all_verb_handlers_with_storage(proxy.messaging(), Arc::clone(&local_storage));

        let remote_service = Arc::new(MessagingService::new(remote_addr));
        register_all_verb_handlers_with_storage(&remote_service, Arc::clone(&remote_storage));
        Arc::clone(&remote_service).start_listener().await.unwrap();

        let result = proxy
            .fetch_rows(
                &CoordinatedRead {
                    keyspace: "ks".to_string(),
                    table: "users".to_string(),
                    partition_key: partition_key.to_vec(),
                },
                ConsistencyLevel::Quorum,
                &SimpleStrategy::new(2),
                &SimpleSnitch,
            )
            .await
            .unwrap();

        assert!(result.digest_mismatch_resolved);
        assert!(result.read_repair_triggered);
        assert_eq!(result.responses_received, 2);
        assert_eq!(result.contacted_replicas.len(), 2);
        assert_eq!(
            proxy
                .read_coordinator()
                .read_repair_scheduler()
                .pending_count(),
            1
        );

        let returned = result.data.as_deref().unwrap();
        let partition = decode_partition_data(returned).unwrap();
        let row = partition.rows.values().next().unwrap();
        assert_eq!(row.cells[0].value.as_deref(), Some(b"fresh".as_slice()));
    }

    #[tokio::test]
    async fn fetch_rows_speculative_always_contacts_extra_digest_replica() {
        let partition_key = b"pk-speculative-always";
        let local_addr = free_addr();
        let remote_one_addr = free_addr();
        let remote_two_addr = free_addr();
        let proxy = make_three_node_proxy(
            local_addr,
            remote_one_addr,
            remote_two_addr,
            partition_key,
            SpeculativeRetryPolicy::Always,
        );
        let temp = TempDir::new().unwrap();
        let local_storage = open_test_storage(&temp, "local");
        let remote_one_storage = open_test_storage(&temp, "remote-one");
        let remote_two_storage = open_test_storage(&temp, "remote-two");
        apply_test_value(&local_storage, partition_key, b"same", 1);
        apply_test_value(&remote_one_storage, partition_key, b"same", 1);
        apply_test_value(&remote_two_storage, partition_key, b"same", 1);
        register_all_verb_handlers_with_storage(proxy.messaging(), Arc::clone(&local_storage));

        let remote_one_service = Arc::new(MessagingService::new(remote_one_addr));
        register_all_verb_handlers_with_storage(
            &remote_one_service,
            Arc::clone(&remote_one_storage),
        );
        Arc::clone(&remote_one_service)
            .start_listener()
            .await
            .unwrap();

        let remote_two_service = Arc::new(MessagingService::new(remote_two_addr));
        register_all_verb_handlers_with_storage(
            &remote_two_service,
            Arc::clone(&remote_two_storage),
        );
        Arc::clone(&remote_two_service)
            .start_listener()
            .await
            .unwrap();

        let result = proxy
            .fetch_rows(
                &CoordinatedRead {
                    keyspace: "ks".to_string(),
                    table: "users".to_string(),
                    partition_key: partition_key.to_vec(),
                },
                ConsistencyLevel::Quorum,
                &SimpleStrategy::new(3),
                &SimpleSnitch,
            )
            .await
            .unwrap();

        assert!(result.speculative_retry_used);
        assert_eq!(result.responses_required, 2);
        assert_eq!(result.responses_received, 3);
        assert_eq!(result.contacted_replicas.len(), 3);
        assert!(!result.digest_mismatch_resolved);
    }

    #[tokio::test]
    async fn fetch_rows_uses_extra_read_candidate_when_digest_replica_fails() {
        let partition_key = b"pk-digest-fallback";
        let local_addr = free_addr();
        let failed_digest_addr = free_addr();
        let fallback_addr = free_addr();
        let proxy = make_three_node_proxy(
            local_addr,
            failed_digest_addr,
            fallback_addr,
            partition_key,
            SpeculativeRetryPolicy::FixedDelay(Duration::from_millis(1)),
        );
        let temp = TempDir::new().unwrap();
        let local_storage = open_test_storage(&temp, "local");
        let fallback_storage = open_test_storage(&temp, "fallback");
        apply_test_value(&local_storage, partition_key, b"same", 1);
        apply_test_value(&fallback_storage, partition_key, b"same", 1);
        register_all_verb_handlers_with_storage(proxy.messaging(), Arc::clone(&local_storage));

        let fallback_service = Arc::new(MessagingService::new(fallback_addr));
        register_all_verb_handlers_with_storage(&fallback_service, Arc::clone(&fallback_storage));
        Arc::clone(&fallback_service)
            .start_listener()
            .await
            .unwrap();

        let result = proxy
            .fetch_rows(
                &CoordinatedRead {
                    keyspace: "ks".to_string(),
                    table: "users".to_string(),
                    partition_key: partition_key.to_vec(),
                },
                ConsistencyLevel::Quorum,
                &SimpleStrategy::new(3),
                &SimpleSnitch,
            )
            .await
            .unwrap();

        assert!(!result.speculative_retry_used);
        assert_eq!(result.responses_required, 2);
        assert_eq!(result.responses_received, 2);
        assert_eq!(
            result.contacted_replicas,
            vec![Endpoint::new(local_addr), Endpoint::new(fallback_addr)]
        );
    }

    #[tokio::test]
    async fn mutate_uses_storage_backed_local_mutation_handler() {
        let proxy = make_proxy();
        let temp = TempDir::new().unwrap();
        let storage = open_test_storage(&temp, "local");
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
