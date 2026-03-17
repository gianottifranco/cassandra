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

pub mod cloud_metadata;
pub mod cluster;
pub mod gossip;
pub mod node;
pub mod range_streamer;
pub mod replica_collection;
pub mod replica_plan;
pub mod replication;
pub mod ring;
pub mod snitch;
pub mod tcm;
pub mod token_allocator;
pub mod topology;

// Re-exports for ergonomic usage.
pub use cluster::{ClusterMetadata, ClusterSnapshot};
pub use gossip::failure_detector::FailureDetector;
pub use gossip::messages::{GossipDigest, GossipDigestAck, GossipDigestAck2, GossipDigestSyn};
pub use gossip::metrics::{GossipMetrics, GossipMetricsSnapshot};
pub use gossip::service::{GossipService, GossipServiceConfig};
pub use gossip::subscribers::{EndpointStateChangeSubscriber, GossipEvent, SubscriberRegistry};
pub use gossip::{
    ApplicationState, EndpointState, Gossiper, HeartbeatState, SeedProvider, VersionedValue,
};
pub use node::{Endpoint, NodeId, NodeInfo, NodeState};
pub use range_streamer::{FetchReplica, RangeStreamer, SourceFilter};
pub use replica_collection::{
    EndpointsForRange, EndpointsForToken, RangesAtEndpoint, ReplicationFactor,
};
pub use replica_plan::{ReplicaPlanForRangeRead, ReplicaPlanForRead, ReplicaPlanForWrite};
pub use replication::{
    EverywhereStrategy, LocalStrategy, NetworkTopologyStrategy, OldNetworkTopologyStrategy,
    Replica, ReplicationStrategy, SimpleStrategy, TransientReplicationStrategy, create_strategy,
};
pub use ring::TokenRing;
pub use snitch::{
    AlibabaCloudSnitch, AzureSnitch, CloudstackSnitch, DynamicEndpointSnitch, Ec2Snitch,
    GossipingPropertyFileSnitch, PropertyFileSnitch, RackInferringSnitch, SimpleSnitch, Snitch,
    create_snitch,
};
pub use token_allocator::{TokenAllocator, create_token_allocator};
pub use tcm::bridge::{ControlPlaneBridge, ControlPlaneMode};
pub use tcm::{
    Epoch, LockedRanges, MetadataLog, NodeDirectory, Placement, TcmError, TcmMetadata,
    Transformation,
};
pub use topology::{
    StreamPlanDescriptor, StreamRangeRequest, TopologyCoordinator, TopologyError,
    TopologyOperation, TopologyState,
};
