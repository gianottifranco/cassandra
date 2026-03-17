// Licensed under Apache License, Version 2.0.

//! Accord transaction executor.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.service.accord.AccordExecutor`

use std::sync::Arc;

use tracing::{debug, info};

use crate::command_store::CommandStore;
use crate::error::{AccordError, AccordResult};
use crate::journal::AccordJournal;
use crate::types::{CommandStatus, TxnId};

/// Executes committed Accord transactions.
///
/// Takes committed transactions from the CommandStore and applies
/// their mutations to the underlying storage.
pub struct AccordExecutor {
    command_store: Arc<CommandStore>,
    journal: Arc<AccordJournal>,
}

impl AccordExecutor {
    pub fn new(command_store: Arc<CommandStore>, journal: Arc<AccordJournal>) -> Self {
        Self {
            command_store,
            journal,
        }
    }

    /// Execute a committed transaction.
    ///
    /// Transitions the command from Committed -> Applied.
    pub async fn execute(&self, txn_id: TxnId) -> AccordResult<Vec<u8>> {
        let entry = self.command_store.get(&txn_id)
            .ok_or_else(|| AccordError::Internal(format!("Unknown txn: {txn_id}")))?;

        if entry.status != CommandStatus::Committed {
            return Err(AccordError::InvalidTransition {
                from: entry.status,
                to: CommandStatus::Applied,
            });
        }

        debug!(%txn_id, "Executing committed transaction");

        // Apply the mutation (in production, this writes to the storage engine)
        let result = entry.txn.mutation.clone();

        // Mark as applied
        self.command_store.apply(txn_id)?;

        // Journal the applied status
        self.journal.write(
            txn_id,
            CommandStatus::Applied,
            entry.execute_at,
            vec![],
        )?;

        info!(%txn_id, "Transaction applied successfully");
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Keys, Timestamp, Txn};

    fn test_txn_id() -> TxnId {
        TxnId::with_timestamp(100, uuid::Uuid::nil(), 0)
    }

    fn test_txn() -> Txn {
        Txn {
            keys: Keys::single(b"key1".to_vec()),
            mutation: b"INSERT data".to_vec(),
            keyspace: "ks".to_string(),
        }
    }

    #[tokio::test]
    async fn execute_committed_txn() {
        let store = Arc::new(CommandStore::new());
        let journal = Arc::new(AccordJournal::new(true));
        let executor = AccordExecutor::new(store.clone(), journal);

        let txn_id = test_txn_id();
        let ts = Timestamp(100);

        store.pre_accept(txn_id, test_txn(), ts).unwrap();
        store.accept(txn_id, ts).unwrap();
        store.commit(txn_id, ts).unwrap();

        let result = executor.execute(txn_id).await.unwrap();
        assert_eq!(result, b"INSERT data");
        assert_eq!(store.get_status(&txn_id), Some(CommandStatus::Applied));
    }

    #[tokio::test]
    async fn cannot_execute_non_committed() {
        let store = Arc::new(CommandStore::new());
        let journal = Arc::new(AccordJournal::new(true));
        let executor = AccordExecutor::new(store.clone(), journal);

        let txn_id = test_txn_id();
        store.pre_accept(txn_id, test_txn(), Timestamp(100)).unwrap();

        let result = executor.execute(txn_id).await;
        assert!(result.is_err());
    }
}
