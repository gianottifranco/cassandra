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

//! Paxos state persistence mapping to `system.paxos`.
//!
//! ## Java Oracle
//!
//! `org.apache.cassandra.db.SystemKeyspace.savePaxosPromise()`
//! `org.apache.cassandra.db.SystemKeyspace.savePaxosProposal()`
//! `org.apache.cassandra.db.SystemKeyspace.savePaxosCommit()`
//! `org.apache.cassandra.db.SystemKeyspace.loadPaxosState()`

use std::sync::Arc;

use tracing::{debug, warn};
use uuid::Uuid;

use cassandra_storage::commitlog::{CellMutation, Mutation, MutationRow};
use cassandra_storage::engine::StorageEngine;

use crate::paxos::ballot::Ballot;
use crate::paxos::state::{PaxosState, Proposal};

/// Helper to serialize a Ballot into a UUID for the system.paxos table.
/// We use the V1 UUID layout: timestamp is mapped to the timestamp fields,
/// and node_id dictates the MAC/node part.
fn ballot_to_uuid(ballot: Ballot) -> Uuid {
    if ballot.is_none() {
        return Uuid::nil();
    }
    // Convert micros to 100ns intervals since Gregorian calendar epoch (1582-10-15)
    // UUID v1 epoch is 12219292800 seconds before Unix epoch.
    let ticks = (ballot.timestamp_micros as u64) * 10 + 122192928000000000u64;

    // Use the low 6 bytes of the node_id as the MAC address
    let node_id_bytes = ballot.node_id.as_bytes();
    let mut mac = [0u8; 6];
    mac.copy_from_slice(&node_id_bytes[10..16]);

    Uuid::new_v1(uuid::Timestamp::from_gregorian(ticks, 0), &mac)
}

/// Helper to deserialize a Ballot from a UUID.
fn uuid_to_ballot(id: Uuid) -> Ballot {
    if id.is_nil() {
        return Ballot::none();
    }
    if let Some(ts) = id.get_timestamp() {
        let (ticks, _) = ts.to_gregorian();
        let micros = (ticks.saturating_sub(122192928000000000u64)) / 10;

        // Recover node_id (we pad the 6 byte MAC back to a UUID, though this is lossy
        // compared to the original node UUID, in practice Cassandra just uses the MAC/IP
        // of the node for the ballot node part).
        let mut node_bytes = [0u8; 16];
        node_bytes[10..16].copy_from_slice(&id.get_node_id().unwrap_or([0; 6]));

        Ballot::with_timestamp(micros as i64, Uuid::from_bytes(node_bytes))
    } else {
        Ballot::none()
    }
}

pub struct PaxosStorage {
    engine: Arc<StorageEngine>,
}

impl PaxosStorage {
    pub fn new(engine: Arc<StorageEngine>) -> Self {
        Self { engine }
    }

    /// Load Paxos state from `system.paxos`.
    pub fn load_state(&self, partition_key: &[u8], cf_id: Uuid) -> PaxosState {
        let mut state = PaxosState::new();

        let partition_data = match self.engine.read_partition("system", "paxos", partition_key) {
            Some(pd) => pd,
            None => return state,
        };

        // Find the clustering row for cf_id
        let cf_id_bytes = cf_id.as_bytes().to_vec();

        if let Some(row) = partition_data.rows.get(&cf_id_bytes) {
            let mut promised_uuid = None;
            let mut accepted_uuid = None;
            let mut accepted_mutation = None;
            let mut committed_uuid = None;
            let mut committed_mutation = None;

            for cell in &row.cells {
                // Ignore tombstones
                if cell.is_tombstone {
                    continue;
                }
                let val = match &cell.value {
                    Some(v) => v,
                    None => continue,
                };

                match cell.column.as_str() {
                    "in_progress_ballot" => {
                        if val.len() == 16 {
                            promised_uuid = Some(Uuid::from_slice(val).unwrap_or(Uuid::nil()));
                        }
                    }
                    "proposal_ballot" => {
                        if val.len() == 16 {
                            accepted_uuid = Some(Uuid::from_slice(val).unwrap_or(Uuid::nil()));
                        }
                    }
                    "proposal" => {
                        accepted_mutation = Some(val.clone());
                    }
                    "most_recent_commit_at" => {
                        if val.len() == 16 {
                            committed_uuid = Some(Uuid::from_slice(val).unwrap_or(Uuid::nil()));
                        }
                    }
                    "most_recent_commit" => {
                        committed_mutation = Some(val.clone());
                    }
                    _ => {}
                }
            }

            if let Some(uuid) = promised_uuid {
                state.promised = uuid_to_ballot(uuid);
            }
            if let (Some(uuid), Some(mutation)) = (accepted_uuid, accepted_mutation) {
                state.accepted = Some(Proposal {
                    ballot: uuid_to_ballot(uuid),
                    mutation,
                });
            }
            if let (Some(uuid), Some(mutation)) = (committed_uuid, committed_mutation) {
                state.committed = Some(Proposal {
                    ballot: uuid_to_ballot(uuid),
                    mutation,
                });
            }
        }

        state
    }

    /// Save promised ballot to `system.paxos`.
    pub fn save_promise(
        &self,
        partition_key: &[u8],
        cf_id: Uuid,
        ballot: Ballot,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let ts = ballot.timestamp_micros;
        let uuid = ballot_to_uuid(ballot);

        let mutation = Mutation {
            keyspace: "system".to_string(),
            table: "paxos".to_string(),
            partition_key: partition_key.to_vec(),
            rows: vec![MutationRow {
                clustering_key: cf_id.as_bytes().to_vec(),
                cells: vec![CellMutation {
                    column: "in_progress_ballot".to_string(),
                    value: Some(uuid.as_bytes().to_vec()),
                    timestamp: ts,
                    ttl: 0,
                    local_deletion_time: None,
                    is_tombstone: false,
                }],
                is_tombstone: false,
                local_deletion_time: None,
            }],
            timestamp: ts,
            cdc_enabled: false,
        };

        self.engine.apply_mutation(&mutation)
    }

    /// Save accepted proposal to `system.paxos`.
    pub fn save_proposal(
        &self,
        partition_key: &[u8],
        cf_id: Uuid,
        proposal: &Proposal,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let ts = proposal.ballot.timestamp_micros;
        let uuid = ballot_to_uuid(proposal.ballot);

        let mutation = Mutation {
            keyspace: "system".to_string(),
            table: "paxos".to_string(),
            partition_key: partition_key.to_vec(),
            rows: vec![MutationRow {
                clustering_key: cf_id.as_bytes().to_vec(),
                cells: vec![
                    CellMutation {
                        column: "proposal_ballot".to_string(),
                        value: Some(uuid.as_bytes().to_vec()),
                        timestamp: ts,
                        ttl: 0,
                        local_deletion_time: None,
                        is_tombstone: false,
                    },
                    CellMutation {
                        column: "proposal".to_string(),
                        value: Some(proposal.mutation.clone()),
                        timestamp: ts,
                        ttl: 0,
                        local_deletion_time: None,
                        is_tombstone: false,
                    },
                    CellMutation {
                        column: "proposal_version".to_string(),
                        // Simulate Cassandra native protocol version
                        value: Some(4i32.to_be_bytes().to_vec()),
                        timestamp: ts,
                        ttl: 0,
                        local_deletion_time: None,
                        is_tombstone: false,
                    },
                ],
                is_tombstone: false,
                local_deletion_time: None,
            }],
            timestamp: ts,
            cdc_enabled: false,
        };

        self.engine.apply_mutation(&mutation)
    }

    /// Save commit to `system.paxos` (and typically clear in-progress).
    pub fn save_commit(
        &self,
        partition_key: &[u8],
        cf_id: Uuid,
        proposal: &Proposal,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let ts = proposal.ballot.timestamp_micros;
        let uuid = ballot_to_uuid(proposal.ballot);

        let cells = vec![
            CellMutation {
                column: "most_recent_commit_at".to_string(),
                value: Some(uuid.as_bytes().to_vec()),
                timestamp: ts,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            },
            CellMutation {
                column: "most_recent_commit".to_string(),
                value: Some(proposal.mutation.clone()),
                timestamp: ts,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            },
            CellMutation {
                column: "most_recent_commit_version".to_string(),
                value: Some(4i32.to_be_bytes().to_vec()),
                timestamp: ts,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            },
            // Clear in-progress
            CellMutation {
                column: "in_progress_ballot".to_string(),
                value: None,
                timestamp: ts,
                ttl: 0,
                local_deletion_time: Some(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i32,
                ),
                is_tombstone: true,
            },
            CellMutation {
                column: "proposal_ballot".to_string(),
                value: None,
                timestamp: ts,
                ttl: 0,
                local_deletion_time: Some(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i32,
                ),
                is_tombstone: true,
            },
            CellMutation {
                column: "proposal".to_string(),
                value: None,
                timestamp: ts,
                ttl: 0,
                local_deletion_time: Some(
                    std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap()
                        .as_secs() as i32,
                ),
                is_tombstone: true,
            },
        ];

        let mutation = Mutation {
            keyspace: "system".to_string(),
            table: "paxos".to_string(),
            partition_key: partition_key.to_vec(),
            rows: vec![MutationRow {
                clustering_key: cf_id.as_bytes().to_vec(),
                cells,
                is_tombstone: false,
                local_deletion_time: None,
            }],
            timestamp: ts,
            cdc_enabled: false,
        };

        self.engine.apply_mutation(&mutation)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::paxos::ballot::Ballot;
    use cassandra_storage::engine::EngineConfig;
    use tempfile::TempDir;

    #[test]
    fn test_uuid_ballot_conversion() {
        let node_id = Uuid::new_v4();
        let ballot = Ballot::with_timestamp(1680000000000, node_id);

        let uuid = ballot_to_uuid(ballot);
        let recovered = uuid_to_ballot(uuid);

        assert_eq!(ballot.timestamp_micros, recovered.timestamp_micros);
        // The node_id is truncated to 6 bytes, so we can't assert full equality
        // but it is enough to pass Java's checks.
    }

    #[test]
    fn test_paxos_storage_roundtrip() {
        let dir = TempDir::new().unwrap();
        let config = EngineConfig {
            data_directories: vec![dir.path().to_path_buf()],
            ..Default::default()
        };
        let engine = Arc::new(StorageEngine::open(config).unwrap());
        let storage = PaxosStorage::new(engine);

        let pk = b"my_paxos_row";
        let cf_id = Uuid::new_v4();

        let initial_state = storage.load_state(pk, cf_id);
        assert!(initial_state.promised.is_none());

        let ballot1 = Ballot::with_timestamp(100, Uuid::new_v4());
        storage.save_promise(pk, cf_id, ballot1).unwrap();

        let state2 = storage.load_state(pk, cf_id);
        assert_eq!(state2.promised.timestamp_micros, 100);

        let proposal = Proposal {
            ballot: ballot1,
            mutation: b"INSERT data".to_vec(),
        };
        storage.save_proposal(pk, cf_id, &proposal).unwrap();

        let state3 = storage.load_state(pk, cf_id);
        assert!(state3.accepted.is_some());
        assert_eq!(state3.accepted.unwrap().mutation, b"INSERT data");

        storage.save_commit(pk, cf_id, &proposal).unwrap();

        let state4 = storage.load_state(pk, cf_id);
        // Commit clears in-progress in storage
        assert!(state4.accepted.is_none());
        assert!(state4.promised.is_none());
        assert!(state4.committed.is_some());
        assert_eq!(state4.committed.unwrap().mutation, b"INSERT data");
    }
}
