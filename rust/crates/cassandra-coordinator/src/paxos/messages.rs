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

//! Paxos inter-node message types.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.service.paxos.PrepareRequest`
//! - `org.apache.cassandra.service.paxos.PrepareResponse`
//! - `org.apache.cassandra.service.paxos.ProposeRequest`
//! - `org.apache.cassandra.service.paxos.Commit`

use serde::{Deserialize, Serialize};

use super::ballot::Ballot;
use super::state::Proposal;

/// Prepare request (Phase 1a) — sent by the proposer to all replicas.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaxosPrepare {
    /// The partition key for which this Paxos round applies.
    pub partition_key: Vec<u8>,
    /// Keyspace name.
    pub keyspace: String,
    /// Table name.
    pub table: String,
    /// The ballot being proposed.
    pub ballot: Ballot,
}

/// Promise response (Phase 1b) — returned by a replica to the proposer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaxosPromise {
    /// Whether the ballot was promised.
    pub promised: bool,
    /// The ballot that was promised (or the higher one that blocked it).
    pub ballot: Ballot,
    /// Any in-progress accepted proposal that the new proposer must adopt.
    pub in_progress: Option<Proposal>,
    /// The most recently committed proposal (for up-to-date read).
    pub most_recent_commit: Option<Proposal>,
}

/// Propose request (Phase 2a) — sent by the proposer after getting a quorum
/// of promises.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaxosPropose {
    /// The partition key.
    pub partition_key: Vec<u8>,
    /// Keyspace name.
    pub keyspace: String,
    /// Table name.
    pub table: String,
    /// The proposal to accept.
    pub proposal: Proposal,
}

/// Accept response (Phase 2b) — returned by a replica.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaxosAccept {
    /// Whether the proposal was accepted.
    pub accepted: bool,
    /// The current promised ballot on this replica.
    pub ballot: Ballot,
}

/// Commit message (Phase 3) — sent to all replicas after quorum accepts.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PaxosCommit {
    /// The partition key.
    pub partition_key: Vec<u8>,
    /// Keyspace name.
    pub keyspace: String,
    /// Table name.
    pub table: String,
    /// The decided proposal to durably commit.
    pub proposal: Proposal,
}

#[cfg(test)]
mod tests {
    use super::*;
    use uuid::Uuid;

    #[test]
    fn prepare_serializes() {
        let msg = PaxosPrepare {
            partition_key: b"pk1".to_vec(),
            keyspace: "ks".to_string(),
            table: "t1".to_string(),
            ballot: Ballot::with_timestamp(100, Uuid::nil()),
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: PaxosPrepare = serde_json::from_str(&json).unwrap();
        assert_eq!(back.partition_key, b"pk1");
        assert_eq!(back.ballot.timestamp_micros, 100);
    }

    #[test]
    fn propose_serializes() {
        let msg = PaxosPropose {
            partition_key: b"pk1".to_vec(),
            keyspace: "ks".to_string(),
            table: "t1".to_string(),
            proposal: Proposal {
                ballot: Ballot::with_timestamp(100, Uuid::nil()),
                mutation: b"INSERT data".to_vec(),
            },
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: PaxosPropose = serde_json::from_str(&json).unwrap();
        assert_eq!(back.proposal.mutation, b"INSERT data");
    }

    #[test]
    fn commit_serializes() {
        let msg = PaxosCommit {
            partition_key: b"pk".to_vec(),
            keyspace: "ks".to_string(),
            table: "t".to_string(),
            proposal: Proposal {
                ballot: Ballot::with_timestamp(200, Uuid::nil()),
                mutation: b"value".to_vec(),
            },
        };
        let json = serde_json::to_string(&msg).unwrap();
        let back: PaxosCommit = serde_json::from_str(&json).unwrap();
        assert_eq!(back.proposal.ballot.timestamp_micros, 200);
    }
}
