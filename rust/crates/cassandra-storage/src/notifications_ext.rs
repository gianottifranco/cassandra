// Licensed under Apache License, Version 2.0.

//! Extended storage event notifications.
//!
//! Provides additional event types beyond the core [`super::notifications`]
//! module, covering memtable flushes, schema changes, bootstrap lifecycle,
//! and snapshot operations. Uses the same publish/subscribe pattern with
//! panic isolation.

use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

// ─── Events ──────────────────────────────────────────────────────────────────

/// Extended storage events published through the [`ExtendedEventBus`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum ExtendedStorageEvent {
    /// A memtable flush has started.
    MemtableFlushStarted { keyspace: String, table: String },
    /// The schema for a keyspace changed.
    SchemaChanged {
        keyspace: String,
        change_type: String,
    },
    /// A node bootstrap has started.
    BootstrapStarted { node_id: String },
    /// A node bootstrap completed.
    BootstrapCompleted { node_id: String, duration_ms: u64 },
    /// A snapshot was created.
    SnapshotCreated { keyspace: String, tag: String },
    /// A snapshot was deleted.
    SnapshotDeleted { keyspace: String, tag: String },
}

// ─── Listener trait ──────────────────────────────────────────────────────────

/// Trait for components that want to receive extended storage events.
pub trait ExtendedEventListener: Send + Sync {
    /// Called when an extended storage event is published.
    fn on_extended_event(&self, event: &ExtendedStorageEvent);

    /// Unique identifier for this listener (used for unsubscribe).
    fn listener_id(&self) -> &str;
}

// ─── Event bus ───────────────────────────────────────────────────────────────

/// Internal bookkeeping entry for a registered extended listener.
struct ExtendedListenerEntry {
    id: String,
    listener: Arc<dyn ExtendedEventListener>,
}

/// Publish/subscribe bus for [`ExtendedStorageEvent`]s.
///
/// Thread-safe. Listeners are called synchronously in registration order.
/// A panicking listener is caught and logged — it does not prevent other
/// listeners from receiving the event.
pub struct ExtendedEventBus {
    listeners: RwLock<Vec<ExtendedListenerEntry>>,
}

impl ExtendedEventBus {
    /// Create a new, empty extended event bus.
    pub fn new() -> Self {
        Self {
            listeners: RwLock::new(Vec::new()),
        }
    }

    /// Register a listener. It will receive all future events.
    pub fn subscribe(&self, listener: Arc<dyn ExtendedEventListener>) {
        let id = listener.listener_id().to_string();
        let mut listeners = self.listeners.write();
        listeners.push(ExtendedListenerEntry { id, listener });
    }

    /// Remove a listener by its ID. Returns `true` if a listener was found
    /// and removed.
    pub fn unsubscribe(&self, listener_id: &str) -> bool {
        let mut listeners = self.listeners.write();
        let before = listeners.len();
        listeners.retain(|entry| entry.id != listener_id);
        listeners.len() < before
    }

    /// Dispatch an event to all registered listeners.
    ///
    /// Each listener call is wrapped in [`std::panic::catch_unwind`] so that
    /// a panicking listener does not prevent delivery to subsequent listeners.
    pub fn publish(&self, event: &ExtendedStorageEvent) {
        let listeners = self.listeners.read();
        for entry in listeners.iter() {
            let listener = Arc::clone(&entry.listener);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                listener.on_extended_event(event);
            }));
            if let Err(panic_info) = result {
                let msg = if let Some(s) = panic_info.downcast_ref::<&str>() {
                    (*s).to_string()
                } else if let Some(s) = panic_info.downcast_ref::<String>() {
                    s.clone()
                } else {
                    "unknown panic".to_string()
                };
                tracing::error!(
                    listener_id = %entry.id,
                    panic = %msg,
                    "Listener panicked while handling extended storage event"
                );
            }
        }
    }

    /// Number of currently registered listeners.
    pub fn listener_count(&self) -> usize {
        self.listeners.read().len()
    }
}

impl Default for ExtendedEventBus {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test listener that records all received events.
    struct RecordingListener {
        id: String,
        events: Arc<RwLock<Vec<ExtendedStorageEvent>>>,
    }

    impl RecordingListener {
        fn new(id: &str) -> (Arc<Self>, Arc<RwLock<Vec<ExtendedStorageEvent>>>) {
            let events = Arc::new(RwLock::new(Vec::new()));
            let listener = Arc::new(Self {
                id: id.to_string(),
                events: Arc::clone(&events),
            });
            (listener, events)
        }
    }

    impl ExtendedEventListener for RecordingListener {
        fn on_extended_event(&self, event: &ExtendedStorageEvent) {
            self.events.write().push(event.clone());
        }

        fn listener_id(&self) -> &str {
            &self.id
        }
    }

    /// Listener that always panics, for isolation testing.
    struct PanickingListener {
        id: String,
    }

    impl ExtendedEventListener for PanickingListener {
        fn on_extended_event(&self, _event: &ExtendedStorageEvent) {
            panic!("intentional panic in test listener");
        }

        fn listener_id(&self) -> &str {
            &self.id
        }
    }

    #[test]
    fn subscribe_and_publish_reaches_listener() {
        let bus = ExtendedEventBus::new();
        let (listener, events) = RecordingListener::new("test-1");

        bus.subscribe(listener);

        let event = ExtendedStorageEvent::MemtableFlushStarted {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
        };
        bus.publish(&event);

        let recorded = events.read();
        assert_eq!(recorded.len(), 1);
        match &recorded[0] {
            ExtendedStorageEvent::MemtableFlushStarted { keyspace, table } => {
                assert_eq!(keyspace, "ks");
                assert_eq!(table, "tbl");
            }
            other => panic!("unexpected event: {:?}", other),
        }
    }

    #[test]
    fn multiple_listeners_all_receive_events() {
        let bus = ExtendedEventBus::new();
        let (l1, events1) = RecordingListener::new("l1");
        let (l2, events2) = RecordingListener::new("l2");

        bus.subscribe(l1);
        bus.subscribe(l2);

        let event = ExtendedStorageEvent::BootstrapCompleted {
            node_id: "node-1".to_string(),
            duration_ms: 5000,
        };
        bus.publish(&event);

        assert_eq!(events1.read().len(), 1);
        assert_eq!(events2.read().len(), 1);
    }

    #[test]
    fn unsubscribe_stops_delivery() {
        let bus = ExtendedEventBus::new();
        let (listener, events) = RecordingListener::new("removable");

        bus.subscribe(listener);
        assert_eq!(bus.listener_count(), 1);

        let removed = bus.unsubscribe("removable");
        assert!(removed);
        assert_eq!(bus.listener_count(), 0);

        let event = ExtendedStorageEvent::SchemaChanged {
            keyspace: "ks".to_string(),
            change_type: "CREATE_TABLE".to_string(),
        };
        bus.publish(&event);

        assert!(events.read().is_empty());

        // Unsubscribing a non-existent listener returns false.
        assert!(!bus.unsubscribe("does-not-exist"));
    }

    #[test]
    fn panicking_listener_does_not_affect_others() {
        let bus = ExtendedEventBus::new();

        // Register a panicking listener first.
        let panicker = Arc::new(PanickingListener {
            id: "panicker".to_string(),
        });
        bus.subscribe(panicker);

        // Register a well-behaved listener after.
        let (good, events) = RecordingListener::new("good");
        bus.subscribe(good);

        let event = ExtendedStorageEvent::BootstrapStarted {
            node_id: "node-1".to_string(),
        };
        bus.publish(&event);

        // The good listener should still receive the event.
        let recorded = events.read();
        assert_eq!(recorded.len(), 1);
    }

    #[test]
    fn event_serialization_roundtrip() {
        let events = vec![
            ExtendedStorageEvent::MemtableFlushStarted {
                keyspace: "ks1".to_string(),
                table: "tbl1".to_string(),
            },
            ExtendedStorageEvent::SchemaChanged {
                keyspace: "ks2".to_string(),
                change_type: "DROP_TABLE".to_string(),
            },
            ExtendedStorageEvent::BootstrapStarted {
                node_id: "node-a".to_string(),
            },
            ExtendedStorageEvent::BootstrapCompleted {
                node_id: "node-a".to_string(),
                duration_ms: 12345,
            },
            ExtendedStorageEvent::SnapshotCreated {
                keyspace: "ks3".to_string(),
                tag: "daily-backup".to_string(),
            },
            ExtendedStorageEvent::SnapshotDeleted {
                keyspace: "ks3".to_string(),
                tag: "daily-backup".to_string(),
            },
        ];

        for event in &events {
            let json = serde_json::to_string(event).unwrap();
            let deserialized: ExtendedStorageEvent = serde_json::from_str(&json).unwrap();
            // Verify the roundtrip produces identical JSON.
            let json2 = serde_json::to_string(&deserialized).unwrap();
            assert_eq!(json, json2);
        }
    }
}
