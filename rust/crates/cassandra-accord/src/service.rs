// Licensed under Apache License, Version 2.0.

//! Accord service -- coordinator integration for Accord transactions.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.service.accord.AccordService`
//!
//! ## Protocol
//! 1. **PreAccept** -- Register transaction with keys, get initial timestamp
//! 2. **Accept** -- Resolve conflicts, agree on execution timestamp
//! 3. **Commit** -- Durably decide the transaction
//! 4. **Apply** -- Execute the committed mutation

use std::sync::Arc;

use tracing::{debug, info};
use uuid::Uuid;

use crate::command_store::CommandStore;
use crate::error::{AccordError, AccordResult};
use crate::executor::AccordExecutor;
use crate::journal::AccordJournal;
use crate::types::{CommandStatus, Keys, Timestamp, Txn, TxnId};

/// A globally unique transaction identifier used by Accord (legacy alias).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct AccordTxnId(pub Uuid);

/// Configuration for the Accord service.
#[derive(Debug, Clone)]
pub struct AccordConfig {
    pub enabled: bool,
    pub journaling_enabled: bool,
}

impl Default for AccordConfig {
    fn default() -> Self {
        Self {
            enabled: false,
            journaling_enabled: true,
        }
    }
}

/// The Accord Service coordinates Accord transactions.
///
/// Wraps CommandStore, Journal, and Executor to drive the 4-phase protocol.
pub struct AccordService {
    config: AccordConfig,
    pub node_id: Uuid,
    command_store: Arc<CommandStore>,
    journal: Arc<AccordJournal>,
    executor: Arc<AccordExecutor>,
}

impl AccordService {
    /// Initialize the Accord service with full infrastructure.
    pub fn new(config: AccordConfig, node_id: Uuid) -> Self {
        let command_store = Arc::new(CommandStore::new());
        let journal = Arc::new(AccordJournal::new(config.journaling_enabled));
        let executor = Arc::new(AccordExecutor::new(
            Arc::clone(&command_store),
            Arc::clone(&journal),
        ));

        if config.enabled {
            info!(%node_id, "Accord service initialized");
        } else {
            debug!("Accord service initialized (disabled)");
        }

        Self {
            config,
            node_id,
            command_store,
            journal,
            executor,
        }
    }

    /// Check if Accord is enabled globally.
    pub fn is_enabled(&self) -> bool {
        self.config.enabled
    }

    /// Access the command store.
    pub fn command_store(&self) -> &Arc<CommandStore> {
        &self.command_store
    }

    /// Access the journal.
    pub fn journal(&self) -> &Arc<AccordJournal> {
        &self.journal
    }

    /// Execute a transaction via the 4-phase Accord protocol.
    ///
    /// ## Protocol Phases
    /// 1. PreAccept -- register with initial timestamp
    /// 2. Accept -- resolve conflicts (simplified: use initial timestamp)
    /// 3. Commit -- durably decide
    /// 4. Apply -- execute mutation via executor
    pub async fn execute_transaction(
        &self,
        keyspace: &str,
        mutations: Vec<Vec<u8>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if !self.config.enabled {
            return Err("Accord is disabled".into());
        }

        let txn_id = TxnId::new(self.node_id);
        let execute_at = Timestamp::now();

        debug!(%txn_id, "Starting Accord transaction");

        // Build the transaction
        let mutation = mutations.into_iter().flatten().collect::<Vec<u8>>();
        let txn = Txn {
            keys: Keys::single(vec![]),
            mutation,
            keyspace: keyspace.to_string(),
        };

        // Phase 1: PreAccept
        self.command_store
            .pre_accept(txn_id, txn, execute_at)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                e.to_string().into()
            })?;
        self.journal
            .write(txn_id, CommandStatus::PreAccepted, execute_at, vec![])
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                e.to_string().into()
            })?;

        // Phase 2: Accept (simplified -- no conflict resolution in stub)
        self.command_store
            .accept(txn_id, execute_at)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                e.to_string().into()
            })?;
        self.journal
            .write(txn_id, CommandStatus::Accepted, execute_at, vec![])
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                e.to_string().into()
            })?;

        // Phase 3: Commit
        self.command_store
            .commit(txn_id, execute_at)
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                e.to_string().into()
            })?;
        self.journal
            .write(txn_id, CommandStatus::Committed, execute_at, vec![])
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                e.to_string().into()
            })?;

        // Phase 4: Apply
        self.executor
            .execute(txn_id)
            .await
            .map_err(|e| -> Box<dyn std::error::Error + Send + Sync> {
                e.to_string().into()
            })?;

        info!(%txn_id, "Accord transaction completed");
        Ok(())
    }

    /// Execute a transaction and return the result bytes.
    pub async fn execute_transaction_returning(
        &self,
        keyspace: &str,
        mutations: Vec<Vec<u8>>,
    ) -> AccordResult<Vec<u8>> {
        if !self.config.enabled {
            return Err(AccordError::Disabled);
        }

        let txn_id = TxnId::new(self.node_id);
        let execute_at = Timestamp::now();

        let mutation = mutations.into_iter().flatten().collect::<Vec<u8>>();
        let txn = Txn {
            keys: Keys::single(vec![]),
            mutation,
            keyspace: keyspace.to_string(),
        };

        // 4-phase protocol
        self.command_store.pre_accept(txn_id, txn, execute_at)?;
        self.command_store.accept(txn_id, execute_at)?;
        self.command_store.commit(txn_id, execute_at)?;
        self.executor.execute(txn_id).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn service_disabled_by_default() {
        let svc = AccordService::new(AccordConfig::default(), Uuid::new_v4());
        assert!(!svc.is_enabled());
    }

    #[test]
    fn service_enabled() {
        let config = AccordConfig {
            enabled: true,
            journaling_enabled: true,
        };
        let svc = AccordService::new(config, Uuid::new_v4());
        assert!(svc.is_enabled());
    }

    #[tokio::test]
    async fn execute_transaction_when_disabled() {
        let svc = AccordService::new(AccordConfig::default(), Uuid::new_v4());
        let result = svc
            .execute_transaction("ks", vec![b"data".to_vec()])
            .await;
        assert!(result.is_err());
        assert_eq!(result.unwrap_err().to_string(), "Accord is disabled");
    }

    #[tokio::test]
    async fn execute_transaction_full_protocol() {
        let config = AccordConfig {
            enabled: true,
            journaling_enabled: true,
        };
        let svc = AccordService::new(config, Uuid::new_v4());
        let result = svc
            .execute_transaction("ks", vec![b"INSERT data".to_vec()])
            .await;
        assert!(result.is_ok());

        // Verify journal has entries (PreAccepted, Accepted, Committed, Applied from executor)
        assert!(svc.journal().len() >= 3);
    }

    #[tokio::test]
    async fn execute_transaction_returning() {
        let config = AccordConfig {
            enabled: true,
            journaling_enabled: false,
        };
        let svc = AccordService::new(config, Uuid::new_v4());
        let result = svc
            .execute_transaction_returning("ks", vec![b"mutation".to_vec()])
            .await;
        assert!(result.is_ok());
    }
}
