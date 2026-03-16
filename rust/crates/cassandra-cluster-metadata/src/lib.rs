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

//! # cassandra-cluster-metadata
//!
//! Cluster membership, token ring management, replication strategies,
//! gossip protocol, snitches, and failure detection.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.gms` — gossip, failure detection
//! - `org.apache.cassandra.locator` — snitches, replication strategies
//! - `org.apache.cassandra.dht` — partitioners, token ring
//!
//! ## Architecture
//!
//! - [`node`] — endpoint identity and lifecycle state
//! - [`ring`] — token ring (token→endpoint mapping)
//! - [`snitch`] — datacenter/rack topology
//! - [`replication`] — SimpleStrategy, NetworkTopologyStrategy, LocalStrategy, EverywhereStrategy
//! - [`gossip`] — state propagation, heartbeats, failure detection
//! - [`cluster`] — immutable cluster snapshot (Arc-swapped)
//! - [`tcm`] — Transactional Cluster Metadata (epoch-based metadata log)

pub mod node;
pub mod ring;
pub mod snitch;
pub mod replication;
pub mod gossip;
pub mod cluster;
pub mod topology;
pub mod tcm;

// Re-exports for ergonomic usage.
pub use node::{Endpoint, NodeId, NodeInfo, NodeState};
pub use ring::TokenRing;
pub use snitch::{
    Snitch, SimpleSnitch, PropertyFileSnitch, GossipingPropertyFileSnitch,
    DynamicEndpointSnitch, RackInferringSnitch, Ec2Snitch, create_snitch,
};
pub use replication::{
    ReplicationStrategy, SimpleStrategy, NetworkTopologyStrategy,
    LocalStrategy, EverywhereStrategy, TransientReplicationStrategy,
    create_strategy,
};
pub use gossip::{
    ApplicationState, EndpointState, Gossiper, HeartbeatState, SeedProvider, VersionedValue,
};
pub use gossip::failure_detector::FailureDetector;
pub use gossip::messages::{GossipDigest, GossipDigestSyn, GossipDigestAck, GossipDigestAck2};
pub use cluster::{ClusterMetadata, ClusterSnapshot};
pub use topology::{TopologyCoordinator, TopologyOperation, TopologyState, TopologyError, StreamPlanDescriptor, StreamRangeRequest};
pub use tcm::{Epoch, MetadataLog, NodeDirectory, Placement, LockedRanges, TcmMetadata, TcmError, Transformation};
pub use tcm::bridge::{ControlPlaneMode, ControlPlaneBridge};

