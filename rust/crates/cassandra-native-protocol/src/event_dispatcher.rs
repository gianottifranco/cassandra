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

//! Event dispatcher for server-push events (TOPOLOGY_CHANGE, STATUS_CHANGE, SCHEMA_CHANGE).
//!
//! ## Java Oracle
//! - `org.apache.cassandra.transport.Event`
//! - `org.apache.cassandra.service.StorageService` (event emission points)

use std::sync::atomic::{AtomicU64, Ordering};
use tokio::sync::broadcast;

use crate::frame::Frame;
use crate::message::EventMessage;
use crate::response;

/// Capacity of the broadcast channel for event push.
const EVENT_CHANNEL_CAPACITY: usize = 256;

/// Server-wide event dispatcher.
///
/// Connections register for events via REGISTER; the dispatcher broadcasts
/// event frames to all subscribers. Uses a `tokio::sync::broadcast` channel
/// so slow receivers are dropped (matching Java behavior of dropping events
/// for unresponsive clients rather than blocking the event thread).
pub struct EventDispatcher {
    sender: broadcast::Sender<EventFrame>,
    /// Monotonic counter of total events dispatched (metrics).
    dispatched_count: AtomicU64,
}

/// An event frame ready for transmission, tagged with the event type
/// for filtering by the receiver.
#[derive(Clone, Debug)]
pub struct EventFrame {
    /// The event type string ("TOPOLOGY_CHANGE", "STATUS_CHANGE", "SCHEMA_CHANGE").
    pub event_type: String,
    /// Pre-encoded frame bytes ready for the wire.
    pub frame: Frame,
}

impl EventDispatcher {
    /// Create a new event dispatcher.
    pub fn new() -> Self {
        let (sender, _) = broadcast::channel(EVENT_CHANNEL_CAPACITY);
        Self {
            sender,
            dispatched_count: AtomicU64::new(0),
        }
    }

    /// Subscribe to events. Returns a receiver for event frames.
    pub fn subscribe(&self) -> broadcast::Receiver<EventFrame> {
        self.sender.subscribe()
    }

    /// Broadcast an event to all registered connections.
    ///
    /// The event is encoded as a protocol frame at version 4 on stream -1
    /// (the standard event stream ID).
    pub fn dispatch(&self, event: EventMessage, version: u8) {
        let event_type = match &event {
            EventMessage::TopologyChange { .. } => "TOPOLOGY_CHANGE",
            EventMessage::StatusChange { .. } => "STATUS_CHANGE",
            EventMessage::SchemaChange(_) => "SCHEMA_CHANGE",
        };

        let frame = response::encode_response(
            &crate::message::Message::Event(event),
            version,
            -1, // Event stream ID per protocol spec.
        );

        let ef = EventFrame {
            event_type: event_type.to_string(),
            frame,
        };

        // Ignore send errors (no receivers connected).
        let _ = self.sender.send(ef);
        self.dispatched_count.fetch_add(1, Ordering::Relaxed);
    }

    /// Total events dispatched since creation (for metrics).
    pub fn dispatched_count(&self) -> u64 {
        self.dispatched_count.load(Ordering::Relaxed)
    }

    /// Number of active subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.sender.receiver_count()
    }
}

impl Default for EventDispatcher {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::message::{EventMessage, SchemaChange};
    use std::net::{IpAddr, Ipv4Addr};

    #[tokio::test]
    async fn dispatch_topology_change() {
        let dispatcher = EventDispatcher::new();
        let mut rx = dispatcher.subscribe();

        dispatcher.dispatch(
            EventMessage::TopologyChange {
                change: "NEW_NODE".to_string(),
                addr: (IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 9042),
            },
            4,
        );

        let ef = rx.recv().await.unwrap();
        assert_eq!(ef.event_type, "TOPOLOGY_CHANGE");
        assert_eq!(dispatcher.dispatched_count(), 1);
    }

    #[tokio::test]
    async fn dispatch_schema_change() {
        let dispatcher = EventDispatcher::new();
        let mut rx = dispatcher.subscribe();

        dispatcher.dispatch(
            EventMessage::SchemaChange(SchemaChange {
                change_type: "CREATED".to_string(),
                target: "TABLE".to_string(),
                keyspace: "test_ks".to_string(),
                name: Some("users".to_string()),
                arg_types: None,
            }),
            4,
        );

        let ef = rx.recv().await.unwrap();
        assert_eq!(ef.event_type, "SCHEMA_CHANGE");
    }

    #[test]
    fn no_subscribers_no_error() {
        let dispatcher = EventDispatcher::new();
        // Should not panic.
        dispatcher.dispatch(
            EventMessage::StatusChange {
                change: "UP".to_string(),
                addr: (IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), 9042),
            },
            4,
        );
        assert_eq!(dispatcher.dispatched_count(), 1);
    }

    #[test]
    fn subscriber_count() {
        let dispatcher = EventDispatcher::new();
        assert_eq!(dispatcher.subscriber_count(), 0);
        let _rx1 = dispatcher.subscribe();
        assert_eq!(dispatcher.subscriber_count(), 1);
        let _rx2 = dispatcher.subscribe();
        assert_eq!(dispatcher.subscriber_count(), 2);
    }
}
