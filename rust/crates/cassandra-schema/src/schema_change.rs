// Licensed under Apache License, Version 2.0.

//! Schema change events, listeners, and notifier.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.schema.SchemaChangeListener`
//! - `org.apache.cassandra.schema.Schema` (listener management)

use parking_lot::RwLock;
use std::sync::Arc;

/// A schema change event describing what changed.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SchemaChangeEvent {
    KeyspaceCreated(String),
    KeyspaceDropped(String),
    KeyspaceAltered(String),
    TableCreated { keyspace: String, table: String },
    TableDropped { keyspace: String, table: String },
    TableAltered { keyspace: String, table: String },
    ViewCreated { keyspace: String, view: String },
    ViewDropped { keyspace: String, view: String },
    TypeCreated { keyspace: String, type_name: String },
    TypeDropped { keyspace: String, type_name: String },
    FunctionCreated { keyspace: String, function: String },
    FunctionDropped { keyspace: String, function: String },
    AggregateCreated { keyspace: String, aggregate: String },
    AggregateDropped { keyspace: String, aggregate: String },
    TriggerCreated { keyspace: String, table: String, trigger: String },
    TriggerDropped { keyspace: String, table: String, trigger: String },
    IndexCreated { keyspace: String, table: String, index: String },
    IndexDropped { keyspace: String, table: String, index: String },
}

/// Trait for listeners that react to schema changes.
pub trait SchemaChangeListener: Send + Sync {
    /// Called when a schema change event occurs.
    fn on_change(&self, event: &SchemaChangeEvent);
}

/// Manages schema change listeners and dispatches events.
pub struct SchemaChangeNotifier {
    listeners: Arc<RwLock<Vec<Arc<dyn SchemaChangeListener>>>>,
}

impl SchemaChangeNotifier {
    /// Create a new notifier with no listeners.
    pub fn new() -> Self {
        Self {
            listeners: Arc::new(RwLock::new(Vec::new())),
        }
    }

    /// Register a listener.
    pub fn register(&self, listener: Arc<dyn SchemaChangeListener>) {
        self.listeners.write().push(listener);
    }

    /// Number of registered listeners.
    pub fn listener_count(&self) -> usize {
        self.listeners.read().len()
    }

    /// Notify all listeners of an event.
    pub fn notify(&self, event: &SchemaChangeEvent) {
        let listeners = self.listeners.read();
        for listener in listeners.iter() {
            listener.on_change(event);
        }
    }
}

impl Default for SchemaChangeNotifier {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for SchemaChangeNotifier {
    fn clone(&self) -> Self {
        Self {
            listeners: Arc::clone(&self.listeners),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};

    struct CountingListener {
        count: AtomicUsize,
    }

    impl SchemaChangeListener for CountingListener {
        fn on_change(&self, _event: &SchemaChangeEvent) {
            self.count.fetch_add(1, Ordering::Relaxed);
        }
    }

    #[test]
    fn register_and_notify() {
        let notifier = SchemaChangeNotifier::new();
        let listener = Arc::new(CountingListener {
            count: AtomicUsize::new(0),
        });

        notifier.register(listener.clone());
        assert_eq!(notifier.listener_count(), 1);

        notifier.notify(&SchemaChangeEvent::KeyspaceCreated("ks".into()));
        assert_eq!(listener.count.load(Ordering::Relaxed), 1);

        notifier.notify(&SchemaChangeEvent::TableCreated {
            keyspace: "ks".into(),
            table: "t1".into(),
        });
        assert_eq!(listener.count.load(Ordering::Relaxed), 2);
    }

    #[test]
    fn multiple_listeners() {
        let notifier = SchemaChangeNotifier::new();
        let l1 = Arc::new(CountingListener {
            count: AtomicUsize::new(0),
        });
        let l2 = Arc::new(CountingListener {
            count: AtomicUsize::new(0),
        });

        notifier.register(l1.clone());
        notifier.register(l2.clone());

        notifier.notify(&SchemaChangeEvent::ViewCreated {
            keyspace: "ks".into(),
            view: "v1".into(),
        });

        assert_eq!(l1.count.load(Ordering::Relaxed), 1);
        assert_eq!(l2.count.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn no_listeners_no_panic() {
        let notifier = SchemaChangeNotifier::new();
        notifier.notify(&SchemaChangeEvent::KeyspaceDropped("ks".into()));
    }
}
