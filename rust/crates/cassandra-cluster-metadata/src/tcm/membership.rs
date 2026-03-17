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

//! Membership directory for Transactional Cluster Metadata.
//!
//! Wraps the base [`NodeDirectory`] with per-node version tracking,
//! network addresses, datacenter/rack location, and removal history.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.tcm.membership.Directory`
//! - `org.apache.cassandra.tcm.membership.NodeVersion`
//! - `org.apache.cassandra.tcm.membership.NodeAddresses`
//! - `org.apache.cassandra.tcm.membership.Location`

use std::collections::HashMap;
use std::net::SocketAddr;

use serde::{Deserialize, Serialize};

use crate::node::{NodeId, NodeInfo};
use crate::tcm::NodeDirectory;

// ─────────────────────────────────────────────────────────────────────────────
// NodeVersion
// ─────────────────────────────────────────────────────────────────────────────

/// Tracks the metadata version for a specific node.
///
/// Each time the node's metadata is updated (e.g. state change, token
/// assignment), the version is incremented so peers can detect staleness.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodeVersion {
    /// The node this version belongs to.
    pub node_id: NodeId,
    /// Monotonically increasing version counter.
    pub version: u64,
}

impl NodeVersion {
    /// Create a new `NodeVersion` starting at the given version.
    pub fn new(node_id: NodeId, version: u64) -> Self {
        Self { node_id, version }
    }

    /// Increment the version counter by one.
    pub fn increment(&mut self) {
        self.version += 1;
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// NodeAddresses
// ─────────────────────────────────────────────────────────────────────────────

/// Network addresses advertised by a node for different protocols.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NodeAddresses {
    /// The node these addresses belong to.
    pub node_id: NodeId,
    /// CQL native transport address (default port 9042).
    pub native_transport: Option<SocketAddr>,
    /// Internode storage address (default port 7000).
    pub storage: Option<SocketAddr>,
    /// JMX management address.
    pub jmx: Option<SocketAddr>,
}

// ─────────────────────────────────────────────────────────────────────────────
// Location
// ─────────────────────────────────────────────────────────────────────────────

/// Datacenter and rack placement for a node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Location {
    /// Datacenter name.
    pub datacenter: String,
    /// Rack name within the datacenter.
    pub rack: String,
}

impl Location {
    /// Create a new location from datacenter and rack names.
    pub fn new(datacenter: impl Into<String>, rack: impl Into<String>) -> Self {
        Self {
            datacenter: datacenter.into(),
            rack: rack.into(),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// MembershipDirectory
// ─────────────────────────────────────────────────────────────────────────────

/// Extended directory that augments [`NodeDirectory`] with version tracking,
/// addresses, location information, and removal history.
///
/// This is the Rust equivalent of Java's `membership.Directory`.
#[derive(Debug, Clone)]
pub struct MembershipDirectory {
    /// Core node identity and state directory.
    directory: NodeDirectory,
    /// Per-node metadata version counters.
    versions: HashMap<NodeId, NodeVersion>,
    /// Per-node network addresses.
    addresses: HashMap<NodeId, NodeAddresses>,
    /// Per-node datacenter/rack location.
    locations: HashMap<NodeId, Location>,
    /// Nodes that have been unregistered (decommissioned).
    removed_nodes: Vec<NodeId>,
}

impl MembershipDirectory {
    /// Create an empty membership directory.
    pub fn new() -> Self {
        Self {
            directory: NodeDirectory::new(),
            versions: HashMap::new(),
            addresses: HashMap::new(),
            locations: HashMap::new(),
            removed_nodes: Vec::new(),
        }
    }

    /// Register a node with its full metadata.
    ///
    /// Inserts the node into the inner [`NodeDirectory`], creates a
    /// [`NodeVersion`] starting at 1, and stores the addresses and
    /// location derived from the node info.
    pub fn register(&mut self, info: NodeInfo, addresses: NodeAddresses) {
        let id = info.host_id;
        let location = Location::new(info.datacenter.clone(), info.rack.clone());

        self.directory.register(info);
        self.versions.insert(id, NodeVersion::new(id, 1));
        self.addresses.insert(id, addresses);
        self.locations.insert(id, location);
    }

    /// Unregister a node, moving it to the removed list.
    ///
    /// Returns the [`NodeInfo`] if the node was found, or `None` otherwise.
    /// The node's version, addresses, and location are removed, and its
    /// ID is appended to the removal history.
    pub fn unregister(&mut self, id: &NodeId) -> Option<NodeInfo> {
        let info = self.directory.unregister(id);
        if info.is_some() {
            self.versions.remove(id);
            self.addresses.remove(id);
            self.locations.remove(id);
            self.removed_nodes.push(*id);
        }
        info
    }

    /// Get the metadata version for a node.
    pub fn get_version(&self, id: &NodeId) -> Option<&NodeVersion> {
        self.versions.get(id)
    }

    /// Get the network addresses for a node.
    pub fn get_addresses(&self, id: &NodeId) -> Option<&NodeAddresses> {
        self.addresses.get(id)
    }

    /// Get the datacenter/rack location for a node.
    pub fn get_location(&self, id: &NodeId) -> Option<&Location> {
        self.locations.get(id)
    }

    /// Check whether a node has been removed (unregistered).
    pub fn is_removed(&self, id: &NodeId) -> bool {
        self.removed_nodes.contains(id)
    }

    /// Slice of all removed node IDs, in removal order.
    pub fn removed_nodes(&self) -> &[NodeId] {
        &self.removed_nodes
    }

    /// Immutable reference to the inner node directory.
    pub fn directory(&self) -> &NodeDirectory {
        &self.directory
    }

    /// Mutable reference to the inner node directory.
    pub fn directory_mut(&mut self) -> &mut NodeDirectory {
        &mut self.directory
    }
}

impl Default for MembershipDirectory {
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Tests
// ─────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::Endpoint;
    use cassandra_common::Token;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use uuid::Uuid;

    fn node_id(n: u128) -> NodeId {
        NodeId::from_uuid(Uuid::from_u128(n))
    }

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    fn make_info(id: NodeId, port: u16) -> NodeInfo {
        NodeInfo::new(id, ep(port), "dc1", "rack1", vec![Token::from_raw(0)])
    }

    fn make_addresses(id: NodeId, port: u16) -> NodeAddresses {
        let base = Ipv4Addr::new(127, 0, 0, 1);
        NodeAddresses {
            node_id: id,
            native_transport: Some(SocketAddr::new(IpAddr::V4(base), port)),
            storage: Some(SocketAddr::new(IpAddr::V4(base), port + 1000)),
            jmx: Some(SocketAddr::new(IpAddr::V4(base), port + 2000)),
        }
    }

    #[test]
    fn register_stores_full_metadata() {
        let mut dir = MembershipDirectory::new();
        let id = node_id(1);
        let info = make_info(id, 9042);
        let addrs = make_addresses(id, 9042);

        dir.register(info, addrs);

        // Node is in the inner directory
        assert!(dir.directory().get(&id).is_some());

        // Version starts at 1
        let ver = dir.get_version(&id).unwrap();
        assert_eq!(ver.version, 1);
        assert_eq!(ver.node_id, id);

        // Addresses stored
        let a = dir.get_addresses(&id).unwrap();
        assert_eq!(a.native_transport.unwrap().port(), 9042);
        assert_eq!(a.storage.unwrap().port(), 10042);
        assert_eq!(a.jmx.unwrap().port(), 11042);

        // Location derived from NodeInfo
        let loc = dir.get_location(&id).unwrap();
        assert_eq!(loc.datacenter, "dc1");
        assert_eq!(loc.rack, "rack1");
    }

    #[test]
    fn unregister_moves_to_removed_list() {
        let mut dir = MembershipDirectory::new();
        let id = node_id(1);
        dir.register(make_info(id, 9042), make_addresses(id, 9042));

        let removed_info = dir.unregister(&id);
        assert!(removed_info.is_some());
        assert_eq!(removed_info.unwrap().host_id, id);

        // Metadata cleaned up
        assert!(dir.get_version(&id).is_none());
        assert!(dir.get_addresses(&id).is_none());
        assert!(dir.get_location(&id).is_none());

        // Inner directory no longer has the node
        assert!(dir.directory().get(&id).is_none());

        // Tracked in removed history
        assert!(dir.is_removed(&id));
        assert_eq!(dir.removed_nodes(), &[id]);
    }

    #[test]
    fn unregister_nonexistent_returns_none() {
        let mut dir = MembershipDirectory::new();
        assert!(dir.unregister(&node_id(99)).is_none());
        assert!(dir.removed_nodes().is_empty());
    }

    #[test]
    fn version_address_location_lookup() {
        let mut dir = MembershipDirectory::new();
        let id_a = node_id(1);
        let id_b = node_id(2);

        let info_b = NodeInfo::new(id_b, ep(9043), "dc2", "rack2", vec![Token::from_raw(100)]);
        dir.register(make_info(id_a, 9042), make_addresses(id_a, 9042));
        dir.register(info_b, make_addresses(id_b, 9043));

        // Lookup by different IDs
        assert_eq!(dir.get_version(&id_a).unwrap().version, 1);
        assert_eq!(dir.get_location(&id_b).unwrap().datacenter, "dc2");
        assert_eq!(dir.get_location(&id_b).unwrap().rack, "rack2");

        // Missing node returns None
        assert!(dir.get_version(&node_id(99)).is_none());
        assert!(dir.get_addresses(&node_id(99)).is_none());
        assert!(dir.get_location(&node_id(99)).is_none());
    }

    #[test]
    fn is_removed_false_for_active_node() {
        let mut dir = MembershipDirectory::new();
        let id = node_id(1);
        dir.register(make_info(id, 9042), make_addresses(id, 9042));

        assert!(!dir.is_removed(&id));
        assert!(!dir.is_removed(&node_id(99)));
    }

    #[test]
    fn removed_nodes_preserves_order() {
        let mut dir = MembershipDirectory::new();
        let id_a = node_id(1);
        let id_b = node_id(2);
        let id_c = node_id(3);

        dir.register(make_info(id_a, 9042), make_addresses(id_a, 9042));
        dir.register(make_info(id_b, 9043), make_addresses(id_b, 9043));
        dir.register(make_info(id_c, 9044), make_addresses(id_c, 9044));

        dir.unregister(&id_b);
        dir.unregister(&id_a);

        assert_eq!(dir.removed_nodes(), &[id_b, id_a]);
        assert!(dir.is_removed(&id_a));
        assert!(dir.is_removed(&id_b));
        assert!(!dir.is_removed(&id_c));
    }

    #[test]
    fn directory_mut_allows_inner_modification() {
        let mut dir = MembershipDirectory::new();
        let id = node_id(1);
        dir.register(make_info(id, 9042), make_addresses(id, 9042));

        // Mutate inner directory through accessor
        let inner = dir.directory_mut();
        let info = inner.get_mut(&id).unwrap();
        info.load_bytes = 1024;

        assert_eq!(dir.directory().get(&id).unwrap().load_bytes, 1024);
    }

    #[test]
    fn node_version_increment() {
        let mut ver = NodeVersion::new(node_id(1), 1);
        assert_eq!(ver.version, 1);

        ver.increment();
        assert_eq!(ver.version, 2);

        ver.increment();
        assert_eq!(ver.version, 3);
    }

    #[test]
    fn location_equality() {
        let a = Location::new("dc1", "rack1");
        let b = Location::new("dc1", "rack1");
        let c = Location::new("dc1", "rack2");

        assert_eq!(a, b);
        assert_ne!(a, c);
    }
}
