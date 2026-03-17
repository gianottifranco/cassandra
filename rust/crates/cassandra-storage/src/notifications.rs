// Licensed under Apache License, Version 2.0.

//! Storage event notifications.
//!
//! Provides a publish/subscribe event bus for storage-level events such as
//! SSTable additions, compaction completions, and flush notifications.
//! Listeners are invoked synchronously; panics are isolated so a single
//! misbehaving listener cannot take down the bus.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.notifications.SSTableAddedNotification`
//! - `org.apache.cassandra.notifications.SSTableListChangedNotification`
//! - `org.apache.cassandra.notifications.INotification`
//! - `org.apache.cassandra.notifications.INotificationConsumer`

use std::sync::Arc;

use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::sstable::format::SSTableId;

// ─── Events ──────────────────────────────────────────────────────────────────

/// Storage events published through the [`StorageEventBus`].
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageEvent {
    /// A new SSTable was added (e.g. after flush or streaming).
    SSTableAdded {
        id: SSTableId,
        keyspace: String,
        table: String,
    },
    /// An SSTable was removed (e.g. after compaction).
    SSTableRemoved {
        id: SSTableId,
        keyspace: String,
        table: String,
    },
    /// SSTables were atomically replaced (compaction output).
    SSTableReplaced {
        removed: Vec<SSTableId>,
        added: Vec<SSTableId>,
        keyspace: String,
        table: String,
    },
    /// A memtable flush completed.
    FlushCompleted {
        keyspace: String,
        table: String,
        sstable_id: SSTableId,
        bytes_flushed: u64,
    },
    /// A compaction task started.
    CompactionStarted {
        id: Uuid,
        input_sstables: Vec<SSTableId>,
    },
    /// A compaction task finished.
    CompactionCompleted {
        id: Uuid,
        input_sstables: Vec<SSTableId>,
        output_sstables: Vec<SSTableId>,
        bytes_written: u64,
    },
    /// Repair session status changed.
    RepairStatusChanged {
        session_id: Uuid,
        status: String,
    },
    /// A table truncation completed.
    TruncateCompleted {
        keyspace: String,
        table: String,
    },
}

// ─── Listener trait ──────────────────────────────────────────────────────────

/// Trait for components that want to receive storage events.
pub trait StorageEventListener: Send + Sync {
    /// Called when a storage event is published.
    fn on_event(&self, event: &StorageEvent);

    /// Unique identifier for this listener (used for unsubscribe).
    fn listener_id(&self) -> &str;
}

// ─── Event bus ───────────────────────────────────────────────────────────────

/// Internal bookkeeping entry for a registered listener.
struct ListenerEntry {
    id: String,
    listener: Arc<dyn StorageEventListener>,
}

/// Publish/subscribe bus for [`StorageEvent`]s.
///
/// Thread-safe. Listeners are called synchronously in registration order.
/// A panicking listener is caught and logged — it does not prevent other
/// listeners from receiving the event.
pub struct StorageEventBus {
    listeners: RwLock<Vec<ListenerEntry>>,
}

impl StorageEventBus {
    /// Create a new, empty event bus.
    pub fn new() -> Self {
        Self {
            listeners: RwLock::new(Vec::new()),
        }
    }

    /// Register a listener. It will receive all future events.
    pub fn subscribe(&self, listener: Arc<dyn StorageEventListener>) {
        let id = listener.listener_id().to_string();
        let mut listeners = self.listeners.write();
        listeners.push(ListenerEntry { id, listener });
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
    pub fn publish(&self, event: &StorageEvent) {
        let listeners = self.listeners.read();
        for entry in listeners.iter() {
            let listener = Arc::clone(&entry.listener);
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                listener.on_event(event);
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
                    "Listener panicked while handling storage event"
                );
            }
        }
    }

    /// Number of currently registered listeners.
    pub fn listener_count(&self) -> usize {
        self.listeners.read().len()
    }
}

impl Default for StorageEventBus {
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
        events: Arc<RwLock<Vec<StorageEvent>>>,
    }

    impl RecordingListener {
        fn new(id: &str) -> (Arc<Self>, Arc<RwLock<Vec<StorageEvent>>>) {
            let events = Arc::new(RwLock::new(Vec::new()));
            let listener = Arc::new(Self {
                id: id.to_string(),
                events: Arc::clone(&events),
            });
            (listener, events)
        }
    }

    impl StorageEventListener for RecordingListener {
        fn on_event(&self, event: &StorageEvent) {
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

    impl StorageEventListener for PanickingListener {
        fn on_event(&self, _event: &StorageEvent) {
            panic!("intentional panic in test listener");
        }

        fn listener_id(&self) -> &str {
            &self.id
        }
    }

    #[test]
    fn subscribe_and_publish_reaches_listener() {
        let bus = StorageEventBus::new();
        let (listener, events) = RecordingListener::new("test-1");

        bus.subscribe(listener);

        let event = StorageEvent::SSTableAdded {
            id: 42,
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
        };
        bus.publish(&event);

        let recorded = events.read();
        assert_eq!(recorded.len(), 1);
        match &recorded[0] {
            StorageEvent::SSTableAdded { id, .. } => assert_eq!(*id, 42),
            other => panic!("unexpected event: {:?}", other),
        }
    }

    #[test]
    fn multiple_listeners_all_receive_events() {
        let bus = StorageEventBus::new();
        let (l1, events1) = RecordingListener::new("l1");
        let (l2, events2) = RecordingListener::new("l2");

        bus.subscribe(l1);
        bus.subscribe(l2);

        let event = StorageEvent::FlushCompleted {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
            sstable_id: 7,
            bytes_flushed: 1024,
        };
        bus.publish(&event);

        assert_eq!(events1.read().len(), 1);
        assert_eq!(events2.read().len(), 1);
    }

    #[test]
    fn unsubscribe_stops_delivery() {
        let bus = StorageEventBus::new();
        let (listener, events) = RecordingListener::new("removable");

        bus.subscribe(listener);
        assert_eq!(bus.listener_count(), 1);

        let removed = bus.unsubscribe("removable");
        assert!(removed);
        assert_eq!(bus.listener_count(), 0);

        let event = StorageEvent::TruncateCompleted {
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
        };
        bus.publish(&event);

        assert!(events.read().is_empty());

        // Unsubscribing a non-existent listener returns false.
        assert!(!bus.unsubscribe("does-not-exist"));
    }

    #[test]
    fn panicking_listener_does_not_affect_others() {
        let bus = StorageEventBus::new();

        // Register a panicking listener first.
        let panicker = Arc::new(PanickingListener {
            id: "panicker".to_string(),
        });
        bus.subscribe(panicker);

        // Register a well-behaved listener after.
        let (good, events) = RecordingListener::new("good");
        bus.subscribe(good);

        let event = StorageEvent::CompactionStarted {
            id: Uuid::nil(),
            input_sstables: vec![1, 2, 3],
        };
        bus.publish(&event);

        // The good listener should still receive the event.
        let recorded = events.read();
        assert_eq!(recorded.len(), 1);
    }

    #[test]
    fn event_serialization_roundtrip() {
        let events = vec![
            StorageEvent::SSTableAdded {
                id: 1,
                keyspace: "ks1".to_string(),
                table: "tbl1".to_string(),
            },
            StorageEvent::CompactionCompleted {
                id: Uuid::nil(),
                input_sstables: vec![1, 2],
                output_sstables: vec![3],
                bytes_written: 4096,
            },
            StorageEvent::RepairStatusChanged {
                session_id: Uuid::nil(),
                status: "running".to_string(),
            },
        ];

        for event in &events {
            let json = serde_json::to_string(event).unwrap();
            let deserialized: StorageEvent = serde_json::from_str(&json).unwrap();
            // Verify the roundtrip produces identical JSON.
            let json2 = serde_json::to_string(&deserialized).unwrap();
            assert_eq!(json, json2);
        }
    }
}
