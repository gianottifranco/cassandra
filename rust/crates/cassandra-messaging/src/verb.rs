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

//! Inter-node message verbs.
//!
//! Each internode message carries a verb that identifies the message type
//! and routes it to the appropriate handler.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.net.Verb`
//! - `org.apache.cassandra.net.MessagingService`

use serde::{Deserialize, Serialize};

/// Message verb: identifies the type of internode message.
///
/// Modeled after Java's `Verb` enum. Each verb has a unique ID for
/// wire-format encoding.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(i32)]
pub enum Verb {
    // ── Mutation verbs ──────────────────────────────────────────
    /// Write mutation to a replica.
    Mutation = 0,
    /// Response to a mutation request.
    MutationResponse = 1,

    // ── Read verbs ──────────────────────────────────────────────
    /// Read data (full partition/rows) from a replica.
    ReadData = 2,
    /// Response with read data.
    ReadDataResponse = 3,
    /// Read digest (hash of data) from a replica.
    ReadDigest = 4,
    /// Response with digest.
    ReadDigestResponse = 5,

    // ── Gossip verbs ────────────────────────────────────────────
    /// Gossip SYN message.
    GossipDigestSyn = 6,
    /// Gossip ACK message.
    GossipDigestAck = 7,
    /// Gossip ACK2 message.
    GossipDigestAck2 = 8,

    // ── Hints ───────────────────────────────────────────────────
    /// Deliver a hint to the target node.
    Hint = 9,
    /// Response to a hint delivery.
    HintResponse = 10,

    // ── Batch ───────────────────────────────────────────────────
    /// Store a batch log entry.
    BatchStore = 11,
    /// Response to batch store.
    BatchStoreResponse = 12,
    /// Remove a batch log entry.
    BatchRemove = 13,

    // ── Read repair ─────────────────────────────────────────────
    /// Read repair mutation.
    ReadRepair = 14,
    /// Response to read repair.
    ReadRepairResponse = 15,

    // ── Schema ──────────────────────────────────────────────────
    /// Schema push (DDL change propagation).
    SchemaPush = 16,
    /// Schema pull (request schema from peer).
    SchemaPull = 17,
    /// Schema response.
    SchemaResponse = 18,

    // ── Ping ────────────────────────────────────────────────────
    /// Lightweight ping for liveness checking.
    Ping = 19,
    /// Pong response.
    Pong = 20,

    // ── Streaming ───────────────────────────────────────────────
    /// Initialize a streaming session.
    StreamInit = 100,
    /// Response to stream init.
    StreamInitResponse = 101,
    /// Transfer a data chunk.
    StreamData = 102,
    /// Response/ack for a data chunk.
    StreamDataResponse = 103,
    /// Signal stream completion.
    StreamComplete = 104,
    /// Response to stream complete.
    StreamCompleteResponse = 105,

    // ── Repair ──────────────────────────────────────────────────
    /// Initiate a repair session.
    RepairRequest = 110,
    /// Response to repair request.
    RepairResponse = 111,
    /// Request Merkle tree for a range.
    MerkleTreeRequest = 112,
    /// Merkle tree response.
    MerkleTreeResponse = 113,
    /// Request anti-compaction after repair.
    AntiCompactionRequest = 114,
    /// Anti-compaction response.
    AntiCompactionResponse = 115,

    // ── Topology ────────────────────────────────────────────────
    /// Notify peers of a topology change.
    TopologyChange = 120,
    /// Ack for topology change.
    TopologyChangeResponse = 121,
    /// Bootstrap data request.
    BootstrapRequest = 122,
    /// Bootstrap data response.
    BootstrapResponse = 123,

    // ── TCM (Transactional Cluster Metadata) ────────────────────
    /// Commit a metadata transformation to the TCM log.
    TcmCommit = 200,
    /// Response to TCM commit.
    TcmCommitResponse = 201,
    /// Fetch metadata log entries since an epoch.
    TcmFetch = 202,
    /// Response with metadata log entries.
    TcmFetchResponse = 203,
    /// Notify peers of a new TCM epoch.
    TcmNotify = 204,

    // ── Internal ────────────────────────────────────────────────
    /// Request failure response (generic error).
    RequestFailure = 99,
}

impl Verb {
    /// The wire-format ID for this verb.
    pub fn id(self) -> i32 {
        self as i32
    }

    /// Decode a verb from its wire-format ID.
    pub fn from_id(id: i32) -> Option<Self> {
        match id {
            0 => Some(Self::Mutation),
            1 => Some(Self::MutationResponse),
            2 => Some(Self::ReadData),
            3 => Some(Self::ReadDataResponse),
            4 => Some(Self::ReadDigest),
            5 => Some(Self::ReadDigestResponse),
            6 => Some(Self::GossipDigestSyn),
            7 => Some(Self::GossipDigestAck),
            8 => Some(Self::GossipDigestAck2),
            9 => Some(Self::Hint),
            10 => Some(Self::HintResponse),
            11 => Some(Self::BatchStore),
            12 => Some(Self::BatchStoreResponse),
            13 => Some(Self::BatchRemove),
            14 => Some(Self::ReadRepair),
            15 => Some(Self::ReadRepairResponse),
            16 => Some(Self::SchemaPush),
            17 => Some(Self::SchemaPull),
            18 => Some(Self::SchemaResponse),
            19 => Some(Self::Ping),
            20 => Some(Self::Pong),
            99 => Some(Self::RequestFailure),
            100 => Some(Self::StreamInit),
            101 => Some(Self::StreamInitResponse),
            102 => Some(Self::StreamData),
            103 => Some(Self::StreamDataResponse),
            104 => Some(Self::StreamComplete),
            105 => Some(Self::StreamCompleteResponse),
            110 => Some(Self::RepairRequest),
            111 => Some(Self::RepairResponse),
            112 => Some(Self::MerkleTreeRequest),
            113 => Some(Self::MerkleTreeResponse),
            114 => Some(Self::AntiCompactionRequest),
            115 => Some(Self::AntiCompactionResponse),
            120 => Some(Self::TopologyChange),
            121 => Some(Self::TopologyChangeResponse),
            122 => Some(Self::BootstrapRequest),
            123 => Some(Self::BootstrapResponse),
            200 => Some(Self::TcmCommit),
            201 => Some(Self::TcmCommitResponse),
            202 => Some(Self::TcmFetch),
            203 => Some(Self::TcmFetchResponse),
            204 => Some(Self::TcmNotify),
            _ => None,
        }
    }

    /// Returns `true` if this verb expects a response.
    pub fn is_request(self) -> bool {
        matches!(
            self,
            Self::Mutation
                | Self::ReadData
                | Self::ReadDigest
                | Self::GossipDigestSyn
                | Self::GossipDigestAck
                | Self::Hint
                | Self::BatchStore
                | Self::BatchRemove
                | Self::ReadRepair
                | Self::SchemaPush
                | Self::SchemaPull
                | Self::Ping
                | Self::StreamInit
                | Self::StreamData
                | Self::StreamComplete
                | Self::RepairRequest
                | Self::MerkleTreeRequest
                | Self::AntiCompactionRequest
                | Self::TopologyChange
                | Self::BootstrapRequest
                | Self::TcmCommit
                | Self::TcmFetch
                | Self::TcmNotify
        )
    }

    /// Returns the response verb corresponding to this request verb.
    pub fn response_verb(self) -> Option<Self> {
        match self {
            Self::Mutation => Some(Self::MutationResponse),
            Self::ReadData => Some(Self::ReadDataResponse),
            Self::ReadDigest => Some(Self::ReadDigestResponse),
            Self::GossipDigestSyn => Some(Self::GossipDigestAck),
            Self::GossipDigestAck => Some(Self::GossipDigestAck2),
            Self::Hint => Some(Self::HintResponse),
            Self::BatchStore => Some(Self::BatchStoreResponse),
            Self::ReadRepair => Some(Self::ReadRepairResponse),
            Self::Ping => Some(Self::Pong),
            Self::SchemaPull => Some(Self::SchemaResponse),
            Self::StreamInit => Some(Self::StreamInitResponse),
            Self::StreamData => Some(Self::StreamDataResponse),
            Self::StreamComplete => Some(Self::StreamCompleteResponse),
            Self::RepairRequest => Some(Self::RepairResponse),
            Self::MerkleTreeRequest => Some(Self::MerkleTreeResponse),
            Self::AntiCompactionRequest => Some(Self::AntiCompactionResponse),
            Self::TopologyChange => Some(Self::TopologyChangeResponse),
            Self::BootstrapRequest => Some(Self::BootstrapResponse),
            Self::TcmCommit => Some(Self::TcmCommitResponse),
            Self::TcmFetch => Some(Self::TcmFetchResponse),
            _ => None,
        }
    }

    /// Human-readable name.
    pub fn name(self) -> &'static str {
        match self {
            Self::Mutation => "MUTATION",
            Self::MutationResponse => "MUTATION_RSP",
            Self::ReadData => "READ_DATA",
            Self::ReadDataResponse => "READ_DATA_RSP",
            Self::ReadDigest => "READ_DIGEST",
            Self::ReadDigestResponse => "READ_DIGEST_RSP",
            Self::GossipDigestSyn => "GOSSIP_SYN",
            Self::GossipDigestAck => "GOSSIP_ACK",
            Self::GossipDigestAck2 => "GOSSIP_ACK2",
            Self::Hint => "HINT",
            Self::HintResponse => "HINT_RSP",
            Self::BatchStore => "BATCH_STORE",
            Self::BatchStoreResponse => "BATCH_STORE_RSP",
            Self::BatchRemove => "BATCH_REMOVE",
            Self::ReadRepair => "READ_REPAIR",
            Self::ReadRepairResponse => "READ_REPAIR_RSP",
            Self::SchemaPush => "SCHEMA_PUSH",
            Self::SchemaPull => "SCHEMA_PULL",
            Self::SchemaResponse => "SCHEMA_RSP",
            Self::Ping => "PING",
            Self::Pong => "PONG",
            Self::StreamInit => "STREAM_INIT",
            Self::StreamInitResponse => "STREAM_INIT_RSP",
            Self::StreamData => "STREAM_DATA",
            Self::StreamDataResponse => "STREAM_DATA_RSP",
            Self::StreamComplete => "STREAM_COMPLETE",
            Self::StreamCompleteResponse => "STREAM_COMPLETE_RSP",
            Self::RepairRequest => "REPAIR_REQ",
            Self::RepairResponse => "REPAIR_RSP",
            Self::MerkleTreeRequest => "MERKLE_TREE_REQ",
            Self::MerkleTreeResponse => "MERKLE_TREE_RSP",
            Self::AntiCompactionRequest => "ANTI_COMPACTION_REQ",
            Self::AntiCompactionResponse => "ANTI_COMPACTION_RSP",
            Self::TopologyChange => "TOPOLOGY_CHANGE",
            Self::TopologyChangeResponse => "TOPOLOGY_CHANGE_RSP",
            Self::BootstrapRequest => "BOOTSTRAP_REQ",
            Self::BootstrapResponse => "BOOTSTRAP_RSP",
            Self::TcmCommit => "TCM_COMMIT",
            Self::TcmCommitResponse => "TCM_COMMIT_RSP",
            Self::TcmFetch => "TCM_FETCH",
            Self::TcmFetchResponse => "TCM_FETCH_RSP",
            Self::TcmNotify => "TCM_NOTIFY",
            Self::RequestFailure => "REQUEST_FAILURE",
        }
    }
}

impl std::fmt::Display for Verb {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self.name())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn all_verbs() -> Vec<Verb> {
        vec![
            Verb::Mutation,
            Verb::MutationResponse,
            Verb::ReadData,
            Verb::ReadDataResponse,
            Verb::ReadDigest,
            Verb::ReadDigestResponse,
            Verb::GossipDigestSyn,
            Verb::GossipDigestAck,
            Verb::GossipDigestAck2,
            Verb::Hint,
            Verb::HintResponse,
            Verb::BatchStore,
            Verb::BatchStoreResponse,
            Verb::BatchRemove,
            Verb::ReadRepair,
            Verb::ReadRepairResponse,
            Verb::SchemaPush,
            Verb::SchemaPull,
            Verb::SchemaResponse,
            Verb::Ping,
            Verb::Pong,
            Verb::StreamInit,
            Verb::StreamInitResponse,
            Verb::StreamData,
            Verb::StreamDataResponse,
            Verb::StreamComplete,
            Verb::StreamCompleteResponse,
            Verb::RepairRequest,
            Verb::RepairResponse,
            Verb::MerkleTreeRequest,
            Verb::MerkleTreeResponse,
            Verb::AntiCompactionRequest,
            Verb::AntiCompactionResponse,
            Verb::TopologyChange,
            Verb::TopologyChangeResponse,
            Verb::BootstrapRequest,
            Verb::BootstrapResponse,
            Verb::TcmCommit,
            Verb::TcmCommitResponse,
            Verb::TcmFetch,
            Verb::TcmFetchResponse,
            Verb::TcmNotify,
            Verb::RequestFailure,
        ]
    }

    #[test]
    fn all_verbs_round_trip() {
        for verb in all_verbs() {
            let id = verb.id();
            assert_eq!(
                Verb::from_id(id),
                Some(verb),
                "Round-trip failed for {verb}"
            );
        }
    }

    #[test]
    fn unique_ids() {
        let verbs = all_verbs();
        let mut ids: Vec<i32> = verbs.iter().map(|v| v.id()).collect();
        ids.sort();
        ids.dedup();
        assert_eq!(ids.len(), verbs.len(), "Duplicate verb IDs detected");
    }

    #[test]
    fn request_response_pairs() {
        assert_eq!(Verb::Mutation.response_verb(), Some(Verb::MutationResponse));
        assert_eq!(Verb::ReadData.response_verb(), Some(Verb::ReadDataResponse));
        assert_eq!(Verb::Ping.response_verb(), Some(Verb::Pong));
        assert_eq!(Verb::Pong.response_verb(), None);
        // New streaming/repair/topology pairs
        assert_eq!(
            Verb::StreamInit.response_verb(),
            Some(Verb::StreamInitResponse)
        );
        assert_eq!(
            Verb::StreamData.response_verb(),
            Some(Verb::StreamDataResponse)
        );
        assert_eq!(
            Verb::StreamComplete.response_verb(),
            Some(Verb::StreamCompleteResponse)
        );
        assert_eq!(
            Verb::RepairRequest.response_verb(),
            Some(Verb::RepairResponse)
        );
        assert_eq!(
            Verb::MerkleTreeRequest.response_verb(),
            Some(Verb::MerkleTreeResponse)
        );
        assert_eq!(
            Verb::AntiCompactionRequest.response_verb(),
            Some(Verb::AntiCompactionResponse)
        );
        assert_eq!(
            Verb::TopologyChange.response_verb(),
            Some(Verb::TopologyChangeResponse)
        );
        assert_eq!(
            Verb::BootstrapRequest.response_verb(),
            Some(Verb::BootstrapResponse)
        );
    }

    #[test]
    fn unknown_id_returns_none() {
        assert_eq!(Verb::from_id(999), None);
        assert_eq!(Verb::from_id(-1), None);
    }

    #[test]
    fn streaming_verbs_are_requests() {
        assert!(Verb::StreamInit.is_request());
        assert!(Verb::StreamData.is_request());
        assert!(Verb::StreamComplete.is_request());
        assert!(!Verb::StreamInitResponse.is_request());
    }

    #[test]
    fn repair_verbs_are_requests() {
        assert!(Verb::RepairRequest.is_request());
        assert!(Verb::MerkleTreeRequest.is_request());
        assert!(Verb::AntiCompactionRequest.is_request());
        assert!(!Verb::RepairResponse.is_request());
    }

    #[test]
    fn topology_verbs_are_requests() {
        assert!(Verb::TopologyChange.is_request());
        assert!(Verb::BootstrapRequest.is_request());
        assert!(!Verb::TopologyChangeResponse.is_request());
    }
}
