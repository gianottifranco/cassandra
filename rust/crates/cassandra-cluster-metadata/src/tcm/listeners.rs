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

//! Listener infrastructure for metadata change notifications.
//!
//! Components register [`ChangeListener`] implementations with the
//! [`ListenerRegistry`] to be notified when cluster metadata changes.
//! Each listener can filter which [`MetadataChangeEvent`] variants it
//! cares about.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.tcm.listeners.ChangeListener`

use std::sync::Arc;

use parking_lot::RwLock;

use crate::node::NodeId;
use crate::tcm::{Epoch, Transformation};

// ─────────────────────────────────────────────────────────────────────────────
// MetadataChangeEvent
// ─────────────────────────────────────────────────────────────────────────────

/// An event emitted when cluster metadata changes.
#[derive(Debug, Clone)]
pub enum MetadataChangeEvent {
    /// A schema change was committed.
    SchemaChanged { epoch: Epoch, description: String },
    /// A topology change (node register/unregister/state change) was committed.
    TopologyChanged {
        epoch: Epoch,
        node_id: NodeId,
        transformation: Transformation,
    },
    /// Replica placements were recomputed.
    PlacementChanged { epoch: Epoch },
    /// A metadata snapshot was taken.
    SnapshotTaken { epoch: Epoch },
    /// The metadata log was truncated.
    LogTruncated { epoch: Epoch },
    /// The epoch advanced (catch-all for any epoch bump).
    EpochAdvanced { old: Epoch, new: Epoch },
}

// ─────────────────────────────────────────────────────────────────────────────
// ChangeListener
// ─────────────────────────────────────────────────────────────────────────────

/// Trait for components that want to be notified of metadata changes.
pub trait ChangeListener: Send + Sync {
    /// Called when a metadata change event occurs.
    fn on_change(&self, event: &MetadataChangeEvent);

    /// A human-readable name for this listener (used for unregistration).
    fn name(&self) -> &str;
}

// ─────────────────────────────────────────────────────────────────────────────
// ListenerRegistry
// ─────────────────────────────────────────────────────────────────────────────

/// Thread-safe registry of [`ChangeListener`] implementations.
///
/// Listeners are notified in registration order when [`notify`](Self::notify)
/// is called.
pub struct ListenerRegistry {
    listeners: RwLock<Vec<Arc<dyn ChangeListener>>>,
}

impl ListenerRegistry {
    /// Create an empty registry.
    pub fn new() -> Self {
        Self {
            listeners: RwLock::new(Vec::new()),
        }
    }

    /// Register a new listener.
    pub fn register(&self, listener: Arc<dyn ChangeListener>) {
        self.listeners.write().push(listener);
    }

    /// Remove a listener by name.
    pub fn unregister(&self, name: &str) {
        self.listeners.write().retain(|l| l.name() != name);
    }

    /// Notify all registered listeners of an event.
    pub fn notify(&self, event: &MetadataChangeEvent) {
        let listeners = self.listeners.read();
        for listener in listeners.iter() {
            listener.on_change(event);
        }
    }

    /// Number of currently registered listeners.
    pub fn listener_count(&self) -> usize {
        self.listeners.read().len()
    }
}

impl Default for ListenerRegistry {
    fn default() -> Self {
        Self::new()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SchemaListener
// ─────────────────────────────────────────────────────────────────────────────

/// A listener that captures only [`MetadataChangeEvent::SchemaChanged`] events.
pub struct SchemaListener {
    changes: RwLock<Vec<(Epoch, String)>>,
}

impl SchemaListener {
    pub fn new() -> Self {
        Self {
            changes: RwLock::new(Vec::new()),
        }
    }

    /// Return all captured schema changes as `(epoch, description)` pairs.
    pub fn changes(&self) -> Vec<(Epoch, String)> {
        self.changes.read().clone()
    }
}

impl Default for SchemaListener {
    fn default() -> Self {
        Self::new()
    }
}

impl ChangeListener for SchemaListener {
    fn on_change(&self, event: &MetadataChangeEvent) {
        if let MetadataChangeEvent::SchemaChanged { epoch, description } = event {
            self.changes.write().push((*epoch, description.clone()));
        }
    }

    fn name(&self) -> &str {
        "SchemaListener"
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// PlacementChangeListener
// ─────────────────────────────────────────────────────────────────────────────

/// A listener that captures [`MetadataChangeEvent::PlacementChanged`] and
/// [`MetadataChangeEvent::TopologyChanged`] events.
pub struct PlacementChangeListener {
    epochs: RwLock<Vec<Epoch>>,
}

impl PlacementChangeListener {
    pub fn new() -> Self {
        Self {
            epochs: RwLock::new(Vec::new()),
        }
    }

    /// Return all epochs at which placement-affecting changes occurred.
    pub fn changed_epochs(&self) -> Vec<Epoch> {
        self.epochs.read().clone()
    }
}

impl Default for PlacementChangeListener {
    fn default() -> Self {
        Self::new()
    }
}

impl ChangeListener for PlacementChangeListener {
    fn on_change(&self, event: &MetadataChangeEvent) {
        match event {
            MetadataChangeEvent::PlacementChanged { epoch }
            | MetadataChangeEvent::TopologyChanged { epoch, .. } => {
                self.epochs.write().push(*epoch);
            }
            _ => {}
        }
    }

    fn name(&self) -> &str {
        "PlacementChangeListener"
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// SnapshotListener
// ─────────────────────────────────────────────────────────────────────────────

/// A listener that captures [`MetadataChangeEvent::SnapshotTaken`] events.
pub struct SnapshotListener {
    snapshots: RwLock<Vec<Epoch>>,
}

impl SnapshotListener {
    pub fn new() -> Self {
        Self {
            snapshots: RwLock::new(Vec::new()),
        }
    }

    /// Return all epochs at which snapshots were taken.
    pub fn snapshot_epochs(&self) -> Vec<Epoch> {
        self.snapshots.read().clone()
    }
}

impl Default for SnapshotListener {
    fn default() -> Self {
        Self::new()
    }
}

impl ChangeListener for SnapshotListener {
    fn on_change(&self, event: &MetadataChangeEvent) {
        if let MetadataChangeEvent::SnapshotTaken { epoch } = event {
            self.snapshots.write().push(*epoch);
        }
    }

    fn name(&self) -> &str {
        "SnapshotListener"
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// LogListener
// ─────────────────────────────────────────────────────────────────────────────

/// A listener that captures **all** metadata change events.
pub struct LogListener {
    events: RwLock<Vec<MetadataChangeEvent>>,
}

impl LogListener {
    pub fn new() -> Self {
        Self {
            events: RwLock::new(Vec::new()),
        }
    }

    /// Return all captured events.
    pub fn all_events(&self) -> Vec<MetadataChangeEvent> {
        self.events.read().clone()
    }
}

impl Default for LogListener {
    fn default() -> Self {
        Self::new()
    }
}

impl ChangeListener for LogListener {
    fn on_change(&self, event: &MetadataChangeEvent) {
        self.events.write().push(event.clone());
    }

    fn name(&self) -> &str {
        "LogListener"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::node::Endpoint;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};
    use uuid::Uuid;

    fn node_id(n: u128) -> NodeId {
        NodeId::from_uuid(Uuid::from_u128(n))
    }

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(127, 0, 0, 1),
            port,
        )))
    }

    #[test]
    fn register_listener_and_receive_events() {
        let registry = ListenerRegistry::new();
        let schema = Arc::new(SchemaListener::new());

        registry.register(schema.clone());
        assert_eq!(registry.listener_count(), 1);

        let event = MetadataChangeEvent::SchemaChanged {
            epoch: Epoch::FIRST,
            description: "create table foo".into(),
        };
        registry.notify(&event);

        let changes = schema.changes();
        assert_eq!(changes.len(), 1);
        assert_eq!(changes[0].0, Epoch::FIRST);
        assert_eq!(changes[0].1, "create table foo");
    }

    #[test]
    fn multiple_listeners_all_notified() {
        let registry = ListenerRegistry::new();
        let schema = Arc::new(SchemaListener::new());
        let log = Arc::new(LogListener::new());

        registry.register(schema.clone());
        registry.register(log.clone());
        assert_eq!(registry.listener_count(), 2);

        let event = MetadataChangeEvent::SchemaChanged {
            epoch: Epoch::FIRST,
            description: "alter keyspace".into(),
        };
        registry.notify(&event);

        assert_eq!(schema.changes().len(), 1);
        assert_eq!(log.all_events().len(), 1);
    }

    #[test]
    fn filter_dispatching_schema_listener_only_gets_schema_events() {
        let registry = ListenerRegistry::new();
        let schema = Arc::new(SchemaListener::new());

        registry.register(schema.clone());

        // Schema event — should be captured.
        registry.notify(&MetadataChangeEvent::SchemaChanged {
            epoch: Epoch::FIRST,
            description: "create table".into(),
        });

        // Topology event — should NOT be captured by SchemaListener.
        registry.notify(&MetadataChangeEvent::TopologyChanged {
            epoch: Epoch(2),
            node_id: node_id(1),
            transformation: Transformation::Register {
                node_id: node_id(1),
                endpoint: ep(7001),
                dc: "dc1".into(),
                rack: "rack1".into(),
            },
        });

        // Placement event — should NOT be captured by SchemaListener.
        registry.notify(&MetadataChangeEvent::PlacementChanged { epoch: Epoch(3) });

        // Snapshot event — should NOT be captured by SchemaListener.
        registry.notify(&MetadataChangeEvent::SnapshotTaken { epoch: Epoch(4) });

        // Only the schema event should have been captured.
        assert_eq!(schema.changes().len(), 1);
        assert_eq!(schema.changes()[0].1, "create table");
    }

    #[test]
    fn placement_listener_captures_placement_and_topology() {
        let registry = ListenerRegistry::new();
        let placement = Arc::new(PlacementChangeListener::new());

        registry.register(placement.clone());

        registry.notify(&MetadataChangeEvent::PlacementChanged {
            epoch: Epoch::FIRST,
        });
        registry.notify(&MetadataChangeEvent::TopologyChanged {
            epoch: Epoch(2),
            node_id: node_id(1),
            transformation: Transformation::Register {
                node_id: node_id(1),
                endpoint: ep(7001),
                dc: "dc1".into(),
                rack: "rack1".into(),
            },
        });
        registry.notify(&MetadataChangeEvent::SchemaChanged {
            epoch: Epoch(3),
            description: "ignored".into(),
        });

        let epochs = placement.changed_epochs();
        assert_eq!(epochs.len(), 2);
        assert_eq!(epochs[0], Epoch::FIRST);
        assert_eq!(epochs[1], Epoch(2));
    }

    #[test]
    fn snapshot_listener_captures_only_snapshots() {
        let registry = ListenerRegistry::new();
        let snap = Arc::new(SnapshotListener::new());

        registry.register(snap.clone());

        registry.notify(&MetadataChangeEvent::SnapshotTaken { epoch: Epoch(5) });
        registry.notify(&MetadataChangeEvent::SchemaChanged {
            epoch: Epoch(6),
            description: "ignored".into(),
        });
        registry.notify(&MetadataChangeEvent::SnapshotTaken { epoch: Epoch(10) });

        let epochs = snap.snapshot_epochs();
        assert_eq!(epochs.len(), 2);
        assert_eq!(epochs[0], Epoch(5));
        assert_eq!(epochs[1], Epoch(10));
    }

    #[test]
    fn log_listener_captures_all_events() {
        let registry = ListenerRegistry::new();
        let log = Arc::new(LogListener::new());

        registry.register(log.clone());

        registry.notify(&MetadataChangeEvent::SchemaChanged {
            epoch: Epoch::FIRST,
            description: "a".into(),
        });
        registry.notify(&MetadataChangeEvent::PlacementChanged { epoch: Epoch(2) });
        registry.notify(&MetadataChangeEvent::SnapshotTaken { epoch: Epoch(3) });
        registry.notify(&MetadataChangeEvent::LogTruncated { epoch: Epoch(4) });
        registry.notify(&MetadataChangeEvent::EpochAdvanced {
            old: Epoch(4),
            new: Epoch(5),
        });

        assert_eq!(log.all_events().len(), 5);
    }

    #[test]
    fn unregister_removes_listener() {
        let registry = ListenerRegistry::new();
        let schema = Arc::new(SchemaListener::new());
        let log = Arc::new(LogListener::new());

        registry.register(schema.clone());
        registry.register(log.clone());
        assert_eq!(registry.listener_count(), 2);

        registry.unregister("SchemaListener");
        assert_eq!(registry.listener_count(), 1);

        // After unregistration, schema listener should not receive events.
        registry.notify(&MetadataChangeEvent::SchemaChanged {
            epoch: Epoch::FIRST,
            description: "should not appear".into(),
        });

        assert!(schema.changes().is_empty());
        assert_eq!(log.all_events().len(), 1);
    }
}
