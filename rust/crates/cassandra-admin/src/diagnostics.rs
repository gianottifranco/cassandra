// Licensed under Apache License, Version 2.0.

//! Diagnostic event service.
//!
//! Provides a publish/subscribe mechanism for cluster-wide diagnostic events
//! such as compaction progress, schema changes, topology updates, and GC
//! pauses. Events are dispatched synchronously to registered listeners with
//! panic isolation, and a bounded ring buffer keeps the most recent events
//! for retrospective inspection.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.diag.DiagnosticEventService`
//! - `org.apache.cassandra.diag.DiagnosticEvent`

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::sync::Arc;

use parking_lot::{Mutex, RwLock};
use serde::{Deserialize, Serialize};

/// Maximum number of events retained in the history ring buffer.
const HISTORY_CAPACITY: usize = 500;

// ─── Event types ─────────────────────────────────────────────────────────────

/// Categories of diagnostic events emitted by the node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiagnosticEventType {
    CompactionStarted,
    CompactionCompleted,
    FlushStarted,
    FlushCompleted,
    SchemaChanged,
    BootstrapStarted,
    BootstrapCompleted,
    StreamingStarted,
    StreamingCompleted,
    TopologyChanged,
    GCPause,
    SlowQuery,
    LargePartition,
}

impl fmt::Display for DiagnosticEventType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let label = match self {
            Self::CompactionStarted => "CompactionStarted",
            Self::CompactionCompleted => "CompactionCompleted",
            Self::FlushStarted => "FlushStarted",
            Self::FlushCompleted => "FlushCompleted",
            Self::SchemaChanged => "SchemaChanged",
            Self::BootstrapStarted => "BootstrapStarted",
            Self::BootstrapCompleted => "BootstrapCompleted",
            Self::StreamingStarted => "StreamingStarted",
            Self::StreamingCompleted => "StreamingCompleted",
            Self::TopologyChanged => "TopologyChanged",
            Self::GCPause => "GCPause",
            Self::SlowQuery => "SlowQuery",
            Self::LargePartition => "LargePartition",
        };
        f.write_str(label)
    }
}

// ─── Event ───────────────────────────────────────────────────────────────────

/// A single diagnostic event with metadata.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DiagnosticEvent {
    pub event_type: DiagnosticEventType,
    /// Epoch milliseconds when the event was created.
    pub timestamp_ms: u64,
    pub description: String,
    /// Optional key-value metadata associated with the event.
    pub attributes: HashMap<String, String>,
}

impl DiagnosticEvent {
    /// Create a new event stamped with the current time.
    pub fn now(event_type: DiagnosticEventType, description: impl Into<String>) -> Self {
        let timestamp_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_millis() as u64;
        Self {
            event_type,
            timestamp_ms,
            description: description.into(),
            attributes: HashMap::new(),
        }
    }
}

// ─── Listener trait ──────────────────────────────────────────────────────────

/// Trait for components that want to receive diagnostic events.
pub trait DiagnosticEventListener: Send + Sync {
    /// Called when a diagnostic event is published.
    fn on_diagnostic_event(&self, event: &DiagnosticEvent);

    /// Unique identifier for this listener (used for unsubscribe).
    fn listener_id(&self) -> &str;
}

// ─── Service ─────────────────────────────────────────────────────────────────

/// Internal bookkeeping entry for a registered listener.
struct ListenerEntry {
    id: String,
    listener: Arc<dyn DiagnosticEventListener>,
}

/// Publish/subscribe service for [`DiagnosticEvent`]s with a history ring buffer.
///
/// Thread-safe. Listeners are called synchronously in registration order.
/// A panicking listener is caught so it does not prevent delivery to others.
pub struct DiagnosticEventService {
    listeners: RwLock<Vec<ListenerEntry>>,
    history: Mutex<VecDeque<DiagnosticEvent>>,
}

impl DiagnosticEventService {
    /// Create a new service with an empty listener list and history buffer.
    pub fn new() -> Self {
        Self {
            listeners: RwLock::new(Vec::new()),
            history: Mutex::new(VecDeque::with_capacity(HISTORY_CAPACITY)),
        }
    }

    /// Register a listener. It will receive all future events.
    pub fn subscribe(&self, listener: Arc<dyn DiagnosticEventListener>) {
        let id = listener.listener_id().to_string();
        let mut listeners = self.listeners.write();
        listeners.push(ListenerEntry { id, listener });
    }

    /// Remove a listener by its ID. Returns `true` if found and removed.
    pub fn unsubscribe(&self, listener_id: &str) -> bool {
        let mut listeners = self.listeners.write();
        let before = listeners.len();
        listeners.retain(|entry| entry.id != listener_id);
        listeners.len() < before
    }

    /// Dispatch an event to all listeners, then append it to the history buffer.
    ///
    /// Each listener call is wrapped in [`std::panic::catch_unwind`] so that
    /// a panicking listener does not prevent delivery to subsequent listeners.
    pub fn publish(&self, event: DiagnosticEvent) {
        {
            let listeners = self.listeners.read();
            for entry in listeners.iter() {
                let listener = Arc::clone(&entry.listener);
                let result =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        listener.on_diagnostic_event(&event);
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
                        "Listener panicked while handling diagnostic event"
                    );
                }
            }
        }

        // Append to ring buffer.
        let mut history = self.history.lock();
        if history.len() == HISTORY_CAPACITY {
            history.pop_front();
        }
        history.push_back(event);
    }

    /// Return the most recent events (up to `limit`), newest last.
    pub fn recent_events(&self, limit: usize) -> Vec<DiagnosticEvent> {
        let history = self.history.lock();
        let skip = history.len().saturating_sub(limit);
        history.iter().skip(skip).cloned().collect()
    }

    /// Number of currently registered listeners.
    pub fn listener_count(&self) -> usize {
        self.listeners.read().len()
    }
}

impl Default for DiagnosticEventService {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct RecordingListener {
        id: String,
        events: Arc<RwLock<Vec<DiagnosticEvent>>>,
    }

    impl RecordingListener {
        fn new(id: &str) -> (Arc<Self>, Arc<RwLock<Vec<DiagnosticEvent>>>) {
            let events = Arc::new(RwLock::new(Vec::new()));
            let listener = Arc::new(Self {
                id: id.to_string(),
                events: Arc::clone(&events),
            });
            (listener, events)
        }
    }

    impl DiagnosticEventListener for RecordingListener {
        fn on_diagnostic_event(&self, event: &DiagnosticEvent) {
            self.events.write().push(event.clone());
        }

        fn listener_id(&self) -> &str {
            &self.id
        }
    }

    struct PanickingListener {
        id: String,
    }

    impl DiagnosticEventListener for PanickingListener {
        fn on_diagnostic_event(&self, _event: &DiagnosticEvent) {
            panic!("intentional panic in test listener");
        }

        fn listener_id(&self) -> &str {
            &self.id
        }
    }

    fn make_event(ty: DiagnosticEventType) -> DiagnosticEvent {
        DiagnosticEvent::now(ty, format!("{ty} event"))
    }

    #[test]
    fn subscribe_and_publish_reaches_listener() {
        let svc = DiagnosticEventService::new();
        let (listener, events) = RecordingListener::new("r1");
        svc.subscribe(listener);

        svc.publish(make_event(DiagnosticEventType::CompactionStarted));

        let recorded = events.read();
        assert_eq!(recorded.len(), 1);
        assert_eq!(recorded[0].event_type, DiagnosticEventType::CompactionStarted);
    }

    #[test]
    fn multiple_listeners_all_receive_events() {
        let svc = DiagnosticEventService::new();
        let (l1, e1) = RecordingListener::new("l1");
        let (l2, e2) = RecordingListener::new("l2");
        svc.subscribe(l1);
        svc.subscribe(l2);

        svc.publish(make_event(DiagnosticEventType::SchemaChanged));

        assert_eq!(e1.read().len(), 1);
        assert_eq!(e2.read().len(), 1);
    }

    #[test]
    fn unsubscribe_stops_delivery() {
        let svc = DiagnosticEventService::new();
        let (listener, events) = RecordingListener::new("removable");
        svc.subscribe(listener);
        assert_eq!(svc.listener_count(), 1);

        assert!(svc.unsubscribe("removable"));
        assert_eq!(svc.listener_count(), 0);

        svc.publish(make_event(DiagnosticEventType::GCPause));
        assert!(events.read().is_empty());

        assert!(!svc.unsubscribe("does-not-exist"));
    }

    #[test]
    fn panicking_listener_does_not_affect_others() {
        let svc = DiagnosticEventService::new();
        let panicker = Arc::new(PanickingListener {
            id: "panicker".to_string(),
        });
        svc.subscribe(panicker);

        let (good, events) = RecordingListener::new("good");
        svc.subscribe(good);

        svc.publish(make_event(DiagnosticEventType::SlowQuery));

        assert_eq!(events.read().len(), 1);
    }

    #[test]
    fn recent_events_returns_latest() {
        let svc = DiagnosticEventService::new();
        svc.publish(make_event(DiagnosticEventType::FlushStarted));
        svc.publish(make_event(DiagnosticEventType::FlushCompleted));
        svc.publish(make_event(DiagnosticEventType::TopologyChanged));

        let recent = svc.recent_events(2);
        assert_eq!(recent.len(), 2);
        assert_eq!(recent[0].event_type, DiagnosticEventType::FlushCompleted);
        assert_eq!(recent[1].event_type, DiagnosticEventType::TopologyChanged);

        // Asking for more than available returns all.
        assert_eq!(svc.recent_events(100).len(), 3);
    }

    #[test]
    fn history_ring_buffer_evicts_oldest() {
        let svc = DiagnosticEventService::new();
        for _ in 0..HISTORY_CAPACITY + 10 {
            svc.publish(make_event(DiagnosticEventType::GCPause));
        }
        assert_eq!(svc.recent_events(HISTORY_CAPACITY + 100).len(), HISTORY_CAPACITY);
    }

    #[test]
    fn event_display_impl() {
        assert_eq!(DiagnosticEventType::CompactionStarted.to_string(), "CompactionStarted");
        assert_eq!(DiagnosticEventType::LargePartition.to_string(), "LargePartition");
    }

    #[test]
    fn event_serialization_roundtrip() {
        let event = DiagnosticEvent {
            event_type: DiagnosticEventType::StreamingCompleted,
            timestamp_ms: 1700000000000,
            description: "streaming done".to_string(),
            attributes: HashMap::from([("host".to_string(), "node-1".to_string())]),
        };
        let json = serde_json::to_string(&event).unwrap();
        let deserialized: DiagnosticEvent = serde_json::from_str(&json).unwrap();
        let json2 = serde_json::to_string(&deserialized).unwrap();
        assert_eq!(json, json2);
    }
}
