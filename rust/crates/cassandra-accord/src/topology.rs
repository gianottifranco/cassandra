// Licensed under Apache License, Version 2.0.

//! Accord topology mapping from Cassandra TCM.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.service.accord.AccordTopology`

use std::collections::HashMap;
use uuid::Uuid;

/// Maps Cassandra cluster topology to Accord topology.
///
/// In Accord, nodes are identified by integer IDs. This struct
/// maintains the bidirectional mapping between Cassandra node UUIDs
/// and Accord node IDs.
pub struct AccordTopology {
    /// Cassandra UUID -> Accord node ID.
    node_to_accord: HashMap<Uuid, u32>,
    /// Accord node ID -> Cassandra UUID.
    accord_to_node: HashMap<u32, Uuid>,
    /// Next available Accord node ID.
    next_id: u32,
    /// Current topology epoch.
    epoch: u64,
}

impl AccordTopology {
    pub fn new() -> Self {
        Self {
            node_to_accord: HashMap::new(),
            accord_to_node: HashMap::new(),
            next_id: 0,
            epoch: 0,
        }
    }

    /// Register a Cassandra node and get its Accord ID.
    pub fn register_node(&mut self, node_id: Uuid) -> u32 {
        if let Some(&id) = self.node_to_accord.get(&node_id) {
            return id;
        }
        let id = self.next_id;
        self.next_id += 1;
        self.node_to_accord.insert(node_id, id);
        self.accord_to_node.insert(id, node_id);
        id
    }

    /// Look up a Cassandra node UUID by Accord ID.
    pub fn get_node(&self, accord_id: u32) -> Option<Uuid> {
        self.accord_to_node.get(&accord_id).copied()
    }

    /// Look up an Accord ID by Cassandra node UUID.
    pub fn get_accord_id(&self, node_id: &Uuid) -> Option<u32> {
        self.node_to_accord.get(node_id).copied()
    }

    /// Number of registered nodes.
    pub fn node_count(&self) -> usize {
        self.node_to_accord.len()
    }

    /// Current topology epoch.
    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// Advance the epoch (e.g., on topology change).
    pub fn advance_epoch(&mut self) -> u64 {
        self.epoch += 1;
        self.epoch
    }
}

impl Default for AccordTopology {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn register_and_lookup() {
        let mut topo = AccordTopology::new();
        let node = Uuid::new_v4();
        let id = topo.register_node(node);
        assert_eq!(topo.get_node(id), Some(node));
        assert_eq!(topo.get_accord_id(&node), Some(id));
    }

    #[test]
    fn register_idempotent() {
        let mut topo = AccordTopology::new();
        let node = Uuid::new_v4();
        let id1 = topo.register_node(node);
        let id2 = topo.register_node(node);
        assert_eq!(id1, id2);
        assert_eq!(topo.node_count(), 1);
    }

    #[test]
    fn multiple_nodes() {
        let mut topo = AccordTopology::new();
        let n1 = Uuid::new_v4();
        let n2 = Uuid::new_v4();
        let id1 = topo.register_node(n1);
        let id2 = topo.register_node(n2);
        assert_ne!(id1, id2);
        assert_eq!(topo.node_count(), 2);
    }

    #[test]
    fn epoch_advances() {
        let mut topo = AccordTopology::new();
        assert_eq!(topo.epoch(), 0);
        assert_eq!(topo.advance_epoch(), 1);
        assert_eq!(topo.advance_epoch(), 2);
    }
}
