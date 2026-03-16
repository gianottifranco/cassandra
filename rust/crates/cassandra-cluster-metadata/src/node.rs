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

//! Node identity and state types.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.locator.InetAddressAndPort`
//! - `org.apache.cassandra.gms.ApplicationState`
//! - `org.apache.cassandra.gms.VersionedValue` (STATUS values)

use std::fmt;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use cassandra_common::Token;

/// Unique identifier for a node in the cluster.
///
/// Maps to Java's `host_id` (UUID stored in system.local).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub Uuid);

impl NodeId {
    /// Generate a new random node ID.
    pub fn random() -> Self {
        Self(Uuid::new_v4())
    }

    /// Create from an existing UUID.
    pub const fn from_uuid(uuid: Uuid) -> Self {
        Self(uuid)
    }
}

impl fmt::Display for NodeId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

/// Network endpoint for a node.
///
/// Wraps a `SocketAddr` with semantic meaning: this is the listen address
/// for internode communication (default port 7000 in Java Cassandra).
#[derive(Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct Endpoint(pub SocketAddr);

impl Endpoint {
    pub fn new(addr: SocketAddr) -> Self {
        Self(addr)
    }

    pub fn addr(&self) -> SocketAddr {
        self.0
    }
}

impl fmt::Debug for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Endpoint({})", self.0)
    }
}

impl fmt::Display for Endpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<SocketAddr> for Endpoint {
    fn from(addr: SocketAddr) -> Self {
        Self(addr)
    }
}

/// Lifecycle state of a node in the cluster.
///
/// Matches Java's status values from `VersionedValue.STATUS`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum NodeState {
    /// Node is joining the ring (bootstrapping).
    Joining,
    /// Node is fully operational.
    Normal,
    /// Node is leaving the ring (decommission in progress).
    Leaving,
    /// Node is moving to a new token range.
    Moving,
    /// Node has been marked dead by the failure detector.
    Dead,
    /// Node has fully left the ring (decommission complete).
    Left,
    /// Node is replacing a dead node.
    Replacing,
}

impl NodeState {
    /// Returns `true` if the node participates in reads/writes.
    pub fn is_live(&self) -> bool {
        matches!(self, Self::Normal)
    }

    /// Returns `true` if the node owns token ranges (may still serve reads).
    pub fn owns_tokens(&self) -> bool {
        matches!(self, Self::Normal | Self::Leaving | Self::Moving)
    }

    /// Returns `true` if the node has been removed from the cluster.
    pub fn is_removed(&self) -> bool {
        matches!(self, Self::Left)
    }

    /// Returns `true` if the node is in a transitional state.
    pub fn is_transitioning(&self) -> bool {
        matches!(
            self,
            Self::Joining | Self::Leaving | Self::Moving | Self::Replacing
        )
    }
}

impl fmt::Display for NodeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Joining => write!(f, "JOINING"),
            Self::Normal => write!(f, "NORMAL"),
            Self::Leaving => write!(f, "LEAVING"),
            Self::Moving => write!(f, "MOVING"),
            Self::Dead => write!(f, "DEAD"),
            Self::Left => write!(f, "LEFT"),
            Self::Replacing => write!(f, "REPLACING"),
        }
    }
}

/// Comprehensive information about a node in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeInfo {
    /// Unique host ID.
    pub host_id: NodeId,
    /// Network endpoint for internode communication.
    pub endpoint: Endpoint,
    /// Datacenter name.
    pub datacenter: String,
    /// Rack name within the datacenter.
    pub rack: String,
    /// Tokens owned by this node on the ring.
    pub tokens: Vec<Token>,
    /// Current lifecycle state.
    pub state: NodeState,
    /// Reported load in bytes (from gossip).
    pub load_bytes: u64,
    /// Schema version UUID (for schema agreement).
    pub schema_version: Option<Uuid>,
}

impl NodeInfo {
    /// Create a new NodeInfo with minimal required fields.
    pub fn new(
        host_id: NodeId,
        endpoint: Endpoint,
        datacenter: impl Into<String>,
        rack: impl Into<String>,
        tokens: Vec<Token>,
    ) -> Self {
        Self {
            host_id,
            endpoint,
            datacenter: datacenter.into(),
            rack: rack.into(),
            tokens,
            state: NodeState::Normal,
            load_bytes: 0,
            schema_version: None,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddrV4};

    fn test_endpoint(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(127, 0, 0, 1),
            port,
        )))
    }

    #[test]
    fn node_id_random_unique() {
        let a = NodeId::random();
        let b = NodeId::random();
        assert_ne!(a, b);
    }

    #[test]
    fn node_state_is_live() {
        assert!(NodeState::Normal.is_live());
        assert!(!NodeState::Joining.is_live());
        assert!(!NodeState::Leaving.is_live());
        assert!(!NodeState::Dead.is_live());
        assert!(!NodeState::Left.is_live());
        assert!(!NodeState::Replacing.is_live());
    }

    #[test]
    fn node_state_owns_tokens() {
        assert!(NodeState::Normal.owns_tokens());
        assert!(NodeState::Leaving.owns_tokens());
        assert!(NodeState::Moving.owns_tokens());
        assert!(!NodeState::Joining.owns_tokens());
        assert!(!NodeState::Dead.owns_tokens());
        assert!(!NodeState::Left.owns_tokens());
    }

    #[test]
    fn node_state_is_transitioning() {
        assert!(NodeState::Joining.is_transitioning());
        assert!(NodeState::Leaving.is_transitioning());
        assert!(NodeState::Moving.is_transitioning());
        assert!(NodeState::Replacing.is_transitioning());
        assert!(!NodeState::Normal.is_transitioning());
        assert!(!NodeState::Dead.is_transitioning());
        assert!(!NodeState::Left.is_transitioning());
    }

    #[test]
    fn node_state_is_removed() {
        assert!(NodeState::Left.is_removed());
        assert!(!NodeState::Normal.is_removed());
        assert!(!NodeState::Dead.is_removed());
    }

    #[test]
    fn endpoint_display() {
        let ep = test_endpoint(7000);
        assert_eq!(ep.to_string(), "127.0.0.1:7000");
    }

    #[test]
    fn node_info_creation() {
        let node = NodeInfo::new(
            NodeId::random(),
            test_endpoint(7000),
            "dc1",
            "rack1",
            vec![Token::from_raw(0)],
        );
        assert_eq!(node.datacenter, "dc1");
        assert_eq!(node.rack, "rack1");
        assert_eq!(node.state, NodeState::Normal);
    }
}
