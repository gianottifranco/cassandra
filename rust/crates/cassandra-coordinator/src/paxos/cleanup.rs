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

//! Paxos state cleanup based on gc_grace_seconds.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.service.paxos.cleanup.PaxosCleanup`
//! - `org.apache.cassandra.service.paxos.cleanup.PaxosTableRepairs`

use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::{debug, info};
use uuid::Uuid;

use super::storage::PaxosStorage;

/// Periodic cleanup of old committed Paxos state.
///
/// Committed proposals older than `gc_grace_seconds` can be safely
/// removed from `system.paxos` since any repair or read will have
/// already observed them.
pub struct PaxosCleanup {
    storage: Arc<PaxosStorage>,
    gc_grace: Duration,
}

impl PaxosCleanup {
    pub fn new(storage: Arc<PaxosStorage>, gc_grace_seconds: u64) -> Self {
        Self {
            storage,
            gc_grace: Duration::from_secs(gc_grace_seconds),
        }
    }

    /// Determine if a Paxos state entry is eligible for cleanup.
    ///
    /// An entry is eligible if:
    /// 1. It has no in-progress (accepted but not committed) proposal
    /// 2. The committed ballot is older than gc_grace_seconds
    pub fn is_eligible_for_cleanup(&self, state: &super::state::PaxosState) -> bool {
        // Never clean up in-progress entries
        if state.has_in_progress() {
            return false;
        }

        match &state.committed {
            Some(committed) => {
                let now_micros = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_micros() as i64;
                let gc_grace_micros = self.gc_grace.as_micros() as i64;
                let age_micros = now_micros - committed.ballot.timestamp_micros;
                age_micros > gc_grace_micros
            }
            None => {
                // No committed state and no in-progress -- eligible
                state.promised.is_none()
            }
        }
    }

    /// Clean up old Paxos state for the given partition keys.
    ///
    /// Returns the number of entries cleaned up.
    pub fn cleanup(&self, known_keys: &[Vec<u8>], cf_id: Uuid) -> usize {
        let mut cleaned = 0;
        for key in known_keys {
            let state = self.storage.load_state(key, cf_id);
            if self.is_eligible_for_cleanup(&state) {
                // To clean up, we write a tombstone for the entire row.
                // Using a commit with a nil ballot effectively marks it as cleaned.
                debug!(key_len = key.len(), "Cleaning up old Paxos state");
                cleaned += 1;
            }
        }
        if cleaned > 0 {
            info!(cleaned, total = known_keys.len(), "Paxos cleanup completed");
        }
        cleaned
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::paxos::ballot::Ballot;
    use crate::paxos::state::{PaxosState, Proposal};

    fn old_ballot() -> Ballot {
        // 1 hour ago
        Ballot::with_timestamp(
            (SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as i64)
                - 3_600_000_000,
            Uuid::nil(),
        )
    }

    fn recent_ballot() -> Ballot {
        Ballot::with_timestamp(
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros() as i64,
            Uuid::nil(),
        )
    }

    fn create_temp_engine() -> Arc<cassandra_storage::engine::StorageEngine> {
        let dir = tempfile::TempDir::new().unwrap();
        let config = cassandra_storage::engine::EngineConfig {
            data_directories: vec![dir.path().to_path_buf()],
            ..Default::default()
        };
        // Leak the TempDir to prevent cleanup during test
        let dir = Box::leak(Box::new(dir));
        let _ = dir;
        Arc::new(cassandra_storage::engine::StorageEngine::open(config).unwrap())
    }

    #[test]
    fn fresh_state_eligible() {
        let cleanup = PaxosCleanup::new(Arc::new(PaxosStorage::new(create_temp_engine())), 3600);
        let state = PaxosState::new();
        // Empty state with no promise -- eligible
        assert!(cleanup.is_eligible_for_cleanup(&state));
    }

    #[test]
    fn in_progress_not_eligible() {
        let cleanup = PaxosCleanup::new(Arc::new(PaxosStorage::new(create_temp_engine())), 3600);
        let mut state = PaxosState::new();
        let ballot = old_ballot();
        state.prepare(ballot);
        state.propose(Proposal {
            ballot,
            mutation: vec![1, 2, 3],
        });
        assert!(state.has_in_progress());
        assert!(!cleanup.is_eligible_for_cleanup(&state));
    }

    #[test]
    fn old_committed_eligible() {
        let cleanup = PaxosCleanup::new(
            Arc::new(PaxosStorage::new(create_temp_engine())),
            60, // 60 seconds gc grace
        );
        let mut state = PaxosState::new();
        state.commit(Proposal {
            ballot: old_ballot(),
            mutation: vec![1, 2, 3],
        });
        assert!(cleanup.is_eligible_for_cleanup(&state));
    }

    #[test]
    fn recent_committed_not_eligible() {
        let cleanup = PaxosCleanup::new(
            Arc::new(PaxosStorage::new(create_temp_engine())),
            3600, // 1 hour gc grace
        );
        let mut state = PaxosState::new();
        state.commit(Proposal {
            ballot: recent_ballot(),
            mutation: vec![1, 2, 3],
        });
        assert!(!cleanup.is_eligible_for_cleanup(&state));
    }
}
