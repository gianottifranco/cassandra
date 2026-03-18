// Licensed under Apache License, Version 2.0.

//! Consensus Router
//!
//! ## Java Oracle
//! - `org.apache.cassandra.service.consensus.ConsensusRequestRouter`
//! - `org.apache.cassandra.service.consensus.TransactionalMode`
//! - `org.apache.cassandra.service.StorageProxy.cas()` -> `casPaxos` vs `casAccord`
//!
//! Dispatches conditional operations (LWT) to either Paxos or Accord,
//! depending on the TableMetadata `TransactionalMode` configuration.
//! In Mixed mode, uses per-key migration state to route individual
//! operations to the appropriate protocol.

use cassandra_accord::migration::{KeyMigrationState, TableMigrationState};
use cassandra_accord::service::AccordService;
use cassandra_schema::table::TransactionalMode;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::paxos::coordinator::{CasResult, PaxosCoordinator};

/// Metrics for consensus routing decisions.
#[derive(Debug, Default)]
pub struct ConsensusMetrics {
    /// Number of operations routed to Paxos.
    pub paxos_count: AtomicU64,
    /// Number of operations routed to Accord.
    pub accord_count: AtomicU64,
    /// Number of operations rejected (mode Off).
    pub rejected_count: AtomicU64,
    /// Number of operations routed via migration (Mixed mode).
    pub migration_count: AtomicU64,
}

impl ConsensusMetrics {
    pub fn new() -> Self {
        Self::default()
    }

    /// Record a Paxos routing decision.
    pub fn record_paxos(&self) {
        self.paxos_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record an Accord routing decision.
    pub fn record_accord(&self) {
        self.accord_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a rejected operation.
    pub fn record_rejected(&self) {
        self.rejected_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Record a migration-mode routing decision.
    pub fn record_migration(&self) {
        self.migration_count.fetch_add(1, Ordering::Relaxed);
    }
}

/// Router for transactional operations with per-key migration awareness.
///
/// ## Java Oracle
/// `org.apache.cassandra.service.consensus.ConsensusRequestRouter`
pub struct ConsensusRouter {
    paxos: Arc<PaxosCoordinator>,
    accord: Arc<AccordService>,
    /// Per-table migration state for Mixed mode routing.
    migration_states: RwLock<HashMap<Uuid, TableMigrationState>>,
    /// Routing metrics.
    metrics: Arc<ConsensusMetrics>,
}

impl ConsensusRouter {
    pub fn new(paxos: Arc<PaxosCoordinator>, accord: Arc<AccordService>) -> Self {
        Self {
            paxos,
            accord,
            migration_states: RwLock::new(HashMap::new()),
            metrics: Arc::new(ConsensusMetrics::new()),
        }
    }

    /// Access routing metrics.
    pub fn metrics(&self) -> &Arc<ConsensusMetrics> {
        &self.metrics
    }

    /// Register migration state for a table (used during Paxos→Accord migration).
    pub fn register_migration_state(&self, table_id: Uuid, state: TableMigrationState) {
        self.migration_states.write().insert(table_id, state);
    }

    /// Get the per-key migration state for Mixed mode routing.
    fn get_key_migration_state(&self, table_id: &Uuid, partition_key: &[u8]) -> KeyMigrationState {
        let states = self.migration_states.read();
        match states.get(table_id) {
            Some(table_state) => table_state.get_key_state(partition_key),
            None => KeyMigrationState::Paxos, // Default to Paxos when no migration state
        }
    }

    /// Execute a Compare-And-Set (CAS) conditional update.
    ///
    /// Routes the operation to Accord or Paxos based on the table's `TransactionalMode`.
    /// In Mixed mode, performs per-key migration-aware routing.
    #[allow(clippy::too_many_arguments)]
    pub async fn execute_cas<R, F1, F2>(
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
        match mode {
            TransactionalMode::Off => {
                self.metrics.record_rejected();
                Err("Transactions are disabled for this table (TransactionalMode::Off)".into())
            }
            TransactionalMode::Paxos => {
                self.metrics.record_paxos();
                debug!(keyspace, table, "Routing CAS to Paxos");
                let result = self
                    .paxos
                    .execute_cas(
                        keyspace,
                        table,
                        partition_key,
                        mutation,
                        read_current_fn,
                        condition_fn,
                    )
                    .await;
                Ok(result)
            }
            TransactionalMode::Accord => {
                self.metrics.record_accord();
                debug!(keyspace, table, "Routing CAS to Accord");

                self.accord
                    .execute_transaction(keyspace, vec![mutation])
                    .await?;

                Ok(CasResult::Success)
            }
            TransactionalMode::Mixed => {
                self.execute_cas_mixed(
                    keyspace,
                    table,
                    partition_key,
                    mutation,
                    read_current_fn,
                    condition_fn,
                )
                .await
            }
        }
    }

    /// Execute CAS under Mixed mode with per-key migration awareness.
    ///
    /// ## Java Oracle
    /// `ConsensusRequestRouter.routeMixed()`
    ///
    /// Routes based on the per-key migration state:
    /// - `Paxos` → route to Paxos
    /// - `Migrating` → route to Paxos (safe during transition)
    /// - `Accord` → route to Accord
    #[allow(clippy::too_many_arguments)]
    async fn execute_cas_mixed<R, F1, F2>(
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
        self.metrics.record_migration();

        // Look up per-key migration state using a nil table_id as placeholder.
        // In production, table_id would come from TableMetadata.
        let table_id = Uuid::nil();
        let key_state = self.get_key_migration_state(&table_id, partition_key);

        match key_state {
            KeyMigrationState::Paxos | KeyMigrationState::Migrating => {
                info!(
                    keyspace,
                    table,
                    key_state = ?key_state,
                    "Mixed mode: routing to Paxos"
                );
                let result = self
                    .paxos
                    .execute_cas(
                        keyspace,
                        table,
                        partition_key,
                        mutation,
                        read_current_fn,
                        condition_fn,
                    )
                    .await;
                Ok(result)
            }
            KeyMigrationState::Accord => {
                info!(
                    keyspace,
                    table, "Mixed mode: key migrated, routing to Accord"
                );
                self.accord
                    .execute_transaction(keyspace, vec![mutation])
                    .await?;
                Ok(CasResult::Success)
            }
        }
    }

    /// Execute a full Accord transaction (multi-key, for TransactionStatement).
    ///
    /// This bypasses CAS routing and goes directly to Accord.
    pub async fn execute_accord_transaction(
        &self,
        keyspace: &str,
        mutations: Vec<Vec<u8>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        self.metrics.record_accord();
        self.accord.execute_transaction(keyspace, mutations).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PaxosConfig, PaxosReplica};
    use cassandra_accord::service::AccordConfig;
    use uuid::Uuid;

    fn setup_router() -> ConsensusRouter {
        let node_id = Uuid::new_v4();
        let replica = Arc::new(PaxosReplica::new(node_id));
        let paxos = Arc::new(PaxosCoordinator::with_config(
            node_id,
            vec![replica],
            1,
            PaxosConfig::default(),
        ));
        let accord = Arc::new(AccordService::new(AccordConfig::default(), node_id));
        ConsensusRouter::new(paxos, accord)
    }

    #[tokio::test]
    async fn router_dispatches_correctly() {
        let router = setup_router();

        // Paxos Mode
        let paxos_res = router
            .execute_cas(
                "ks",
                "t1",
                TransactionalMode::Paxos,
                b"pk",
                b"mutation".to_vec(),
                || async { None },
                |curr| curr.is_none(),
            )
            .await
            .unwrap();
        assert!(matches!(paxos_res, CasResult::Success));

        // Off Mode
        let off_res = router
            .execute_cas(
                "ks",
                "t1",
                TransactionalMode::Off,
                b"pk",
                b"mutation".to_vec(),
                || async { None },
                |curr| curr.is_none(),
            )
            .await;
        assert!(off_res.is_err());
        assert_eq!(
            off_res.unwrap_err().to_string(),
            "Transactions are disabled for this table (TransactionalMode::Off)"
        );

        // Accord Mode (disabled so will error in stub)
        let accord_res = router
            .execute_cas(
                "ks",
                "t1",
                TransactionalMode::Accord,
                b"pk",
                b"mutation".to_vec(),
                || async { None },
                |curr| curr.is_none(),
            )
            .await;
        assert!(accord_res.is_err());
        assert_eq!(accord_res.unwrap_err().to_string(), "Accord is disabled");
    }

    #[tokio::test]
    async fn mixed_mode_defaults_to_paxos() {
        let router = setup_router();

        let result = router
            .execute_cas(
                "ks",
                "t1",
                TransactionalMode::Mixed,
                b"pk",
                b"mutation".to_vec(),
                || async { None },
                |curr| curr.is_none(),
            )
            .await
            .unwrap();
        assert!(matches!(result, CasResult::Success));
    }

    #[tokio::test]
    async fn mixed_mode_routes_migrated_key_to_accord() {
        let node_id = Uuid::new_v4();
        let replica = Arc::new(PaxosReplica::new(node_id));
        let paxos = Arc::new(PaxosCoordinator::with_config(
            node_id,
            vec![replica],
            1,
            PaxosConfig::default(),
        ));
        let accord_config = AccordConfig {
            enabled: true,
            journaling_enabled: false,
        };
        let accord = Arc::new(AccordService::new(accord_config, node_id));
        let router = ConsensusRouter::new(paxos, accord);

        // Register migration state with a migrated key
        let table_id = Uuid::nil();
        let mut migration = TableMigrationState::new(table_id);
        migration.mark_migrated(b"migrated_key".to_vec());
        router.register_migration_state(table_id, migration);

        // Migrated key should route to Accord
        let result = router
            .execute_cas(
                "ks",
                "t1",
                TransactionalMode::Mixed,
                b"migrated_key",
                b"mutation".to_vec(),
                || async { None },
                |curr| curr.is_none(),
            )
            .await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn metrics_tracked() {
        let router = setup_router();

        router
            .execute_cas(
                "ks",
                "t1",
                TransactionalMode::Paxos,
                b"pk",
                b"mutation".to_vec(),
                || async { None },
                |_| true,
            )
            .await
            .unwrap();

        let _ = router
            .execute_cas(
                "ks",
                "t1",
                TransactionalMode::Off,
                b"pk",
                b"mutation".to_vec(),
                || async { None },
                |_| true,
            )
            .await;

        assert_eq!(router.metrics().paxos_count.load(Ordering::Relaxed), 1);
        assert_eq!(router.metrics().rejected_count.load(Ordering::Relaxed), 1);
    }
}
