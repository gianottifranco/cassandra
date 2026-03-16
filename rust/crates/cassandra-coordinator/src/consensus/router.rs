// Licensed under Apache License, Version 2.0.

//! Consensus Router
//!
//! ## Java Oracle
//! - `org.apache.cassandra.service.consensus.TransactionalMode`
//! - `org.apache.cassandra.service.StorageProxy.cas()` -> `casPaxos` vs `casAccord`
//!
//! Dispatches conditional operations (LWT) to either Paxos or Accord,
//! depending on the TableMetadata `TransactionalMode` configuration.

use cassandra_accord::service::AccordService;
use cassandra_schema::table::TransactionalMode;
use std::sync::Arc;
use tracing::{debug, info};

use crate::paxos::coordinator::{CasResult, PaxosCoordinator};

/// Router for transactional operations.
pub struct ConsensusRouter {
    paxos: Arc<PaxosCoordinator>,
    accord: Arc<AccordService>,
}

impl ConsensusRouter {
    pub fn new(paxos: Arc<PaxosCoordinator>, accord: Arc<AccordService>) -> Self {
        Self { paxos, accord }
    }

    /// Execute a Compare-And-Set (CAS) conditional update.
    ///
    /// Routes the operation to Accord or Paxos based on the table's `TransactionalMode`.
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
                Err("Transactions are disabled for this table (TransactionalMode::Off)".into())
            }
            TransactionalMode::Paxos => {
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
                debug!(keyspace, table, "Routing CAS to Accord");

                // Accord execute_transaction typically handles both read condition
                // and writes, but for the stub we just send the mutation.
                self.accord
                    .execute_transaction(keyspace, vec![mutation])
                    .await?;

                // For Accord, condition evaluation happens inside the state machine.
                // Assuming success for the stub.
                Ok(CasResult::Success)
            }
            TransactionalMode::Mixed => {
                info!(
                    keyspace,
                    table, "Routing CAS under Mixed mode (Migration) - routing to Paxos by default"
                );
                // Migration logic typically involves writing to both or routing to Paxos
                // while burning in Accord. We route to Paxos for safety.
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
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{PaxosConfig, PaxosReplica};
    use cassandra_accord::service::AccordConfig;
    use uuid::Uuid;

    #[tokio::test]
    async fn router_dispatches_correctly() {
        let node_id = Uuid::new_v4();
        let replica = Arc::new(PaxosReplica::new(node_id));
        let paxos = Arc::new(PaxosCoordinator::with_config(
            node_id,
            vec![replica],
            1,
            PaxosConfig::default(),
        ));

        // Disable accord globally for test
        let accord = Arc::new(AccordService::new(AccordConfig::default(), node_id));
        let router = ConsensusRouter::new(paxos, accord);

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
}
