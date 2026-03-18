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

//! Paxos repair: completes accepted-but-not-committed proposals.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.service.paxos.PaxosRepair`
//! - `org.apache.cassandra.service.paxos.AbstractPaxosRepair`

use std::sync::Arc;
use tracing::{debug, info, warn};
use uuid::Uuid;

use super::coordinator::{CasResult, PaxosCoordinator};
use super::state::PaxosState;
use super::storage::PaxosStorage;

/// Scans for and completes accepted-but-not-committed Paxos proposals.
///
/// On startup or during anti-entropy repair, the repair module finds
/// in-progress proposals in `system.paxos` and drives them to completion.
pub struct PaxosRepair {
    storage: Arc<PaxosStorage>,
    coordinator: Arc<PaxosCoordinator>,
}

impl PaxosRepair {
    pub fn new(storage: Arc<PaxosStorage>, coordinator: Arc<PaxosCoordinator>) -> Self {
        Self {
            storage,
            coordinator,
        }
    }

    /// Scan for uncommitted proposals and complete them.
    ///
    /// Returns the number of proposals successfully completed.
    pub async fn repair_uncommitted(&self, known_keys: &[Vec<u8>], cf_id: Uuid) -> usize {
        let uncommitted = self.storage.load_all_uncommitted(known_keys, cf_id);
        let total = uncommitted.len();

        if total == 0 {
            debug!("Paxos repair: no uncommitted proposals found");
            return 0;
        }

        info!(count = total, "Paxos repair: found uncommitted proposals");

        let mut completed = 0;
        for (key, state) in &uncommitted {
            match self.repair_single(key, state).await {
                Ok(true) => {
                    completed += 1;
                    debug!(key_len = key.len(), "Paxos repair: completed proposal");
                }
                Ok(false) => {
                    debug!(
                        key_len = key.len(),
                        "Paxos repair: proposal already resolved"
                    );
                }
                Err(e) => {
                    warn!(
                        key_len = key.len(),
                        error = %e,
                        "Paxos repair: failed to complete proposal"
                    );
                }
            }
        }

        info!(completed, total, "Paxos repair finished");
        completed
    }

    /// Attempt to complete a single uncommitted proposal.
    async fn repair_single(
        &self,
        partition_key: &[u8],
        state: &PaxosState,
    ) -> Result<bool, Box<dyn std::error::Error + Send + Sync>> {
        let proposal = match state.in_progress_proposal() {
            Some(p) => p,
            None => return Ok(false),
        };

        // Drive the proposal to completion by running a CAS round
        // that adopts the in-progress value (the coordinator handles adoption).
        let result = self
            .coordinator
            .execute_cas(
                "", // keyspace not needed for repair
                "", // table not needed for repair
                partition_key,
                proposal.mutation.clone(),
                || async { None },
                |_| true, // Always accept during repair
            )
            .await;

        match result {
            CasResult::Success => Ok(true),
            _ => Ok(false),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::paxos::coordinator::{PaxosConfig, PaxosReplica};

    fn node_a() -> Uuid {
        Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap()
    }

    #[tokio::test]
    async fn repair_with_no_uncommitted() {
        let replica = Arc::new(PaxosReplica::new(node_a()));
        let coordinator = Arc::new(PaxosCoordinator::with_config(
            node_a(),
            vec![replica],
            1,
            PaxosConfig {
                use_jitter: false,
                ..Default::default()
            },
        ));

        let dir = tempfile::TempDir::new().unwrap();
        let config = cassandra_storage::engine::EngineConfig {
            data_directories: vec![dir.path().to_path_buf()],
            ..Default::default()
        };
        let engine = Arc::new(cassandra_storage::engine::StorageEngine::open(config).unwrap());
        let storage = Arc::new(PaxosStorage::new(engine));

        let repair = PaxosRepair::new(storage, coordinator);
        let completed = repair.repair_uncommitted(&[], Uuid::nil()).await;
        assert_eq!(completed, 0);
    }
}
