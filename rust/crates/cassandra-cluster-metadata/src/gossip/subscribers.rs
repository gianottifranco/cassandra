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

//! Gossip event subscriber system.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.gms.IEndpointStateChangeSubscriber`

use std::sync::Arc;

use parking_lot::RwLock;
use tracing::warn;

use crate::gossip::{ApplicationState, EndpointState, VersionedValue};
use crate::node::Endpoint;

/// Events that can be delivered to gossip subscribers.
#[derive(Debug, Clone)]
pub enum GossipEvent {
    /// A new node has joined the cluster.
    Join {
        endpoint: Endpoint,
        state: EndpointState,
    },
    /// A previously dead node is now alive.
    Alive {
        endpoint: Endpoint,
        state: EndpointState,
    },
    /// A node has been marked dead by the failure detector.
    Dead {
        endpoint: Endpoint,
        state: EndpointState,
    },
    /// A node has been removed from the cluster.
    Removed {
        endpoint: Endpoint,
        state: EndpointState,
    },
    /// A node has restarted (generation change).
    Restart {
        endpoint: Endpoint,
        state: EndpointState,
    },
    /// An application state on a node has changed.
    StateChanged {
        endpoint: Endpoint,
        key: ApplicationState,
        value: VersionedValue,
    },
}

/// Trait for receiving gossip state change notifications.
///
/// Subscribers are notified of cluster membership and state changes.
/// Implementations should be non-blocking — long-running work should
/// be dispatched to a background task.
pub trait EndpointStateChangeSubscriber: Send + Sync {
    /// Called when a new endpoint joins the cluster.
    fn on_join(&self, endpoint: Endpoint, state: &EndpointState);

    /// Called when a previously dead endpoint becomes alive.
    fn on_alive(&self, endpoint: Endpoint, state: &EndpointState);

    /// Called when an endpoint is marked dead.
    fn on_dead(&self, endpoint: Endpoint, state: &EndpointState);

    /// Called when an endpoint is removed from the cluster.
    fn on_removed(&self, endpoint: Endpoint, state: &EndpointState);

    /// Called when an endpoint restarts (generation change).
    fn on_restart(&self, endpoint: Endpoint, state: &EndpointState);

    /// Called when a specific application state changes on an endpoint.
    fn on_state_changed(&self, endpoint: Endpoint, key: ApplicationState, value: &VersionedValue);
}

/// Registry that manages gossip event subscribers and dispatches notifications.
pub struct SubscriberRegistry {
    subscribers: RwLock<Vec<Arc<dyn EndpointStateChangeSubscriber>>>,
}

impl SubscriberRegistry {
    /// Create a new empty registry.
    pub fn new() -> Self {
        Self {
            subscribers: RwLock::new(Vec::new()),
        }
    }

    /// Register a subscriber.
    pub fn register(&self, subscriber: Arc<dyn EndpointStateChangeSubscriber>) {
        self.subscribers.write().push(subscriber);
    }

    /// Number of registered subscribers.
    pub fn subscriber_count(&self) -> usize {
        self.subscribers.read().len()
    }

    /// Notify all subscribers of an event. Panics in individual subscribers
    /// are caught and logged — they do not prevent other subscribers from
    /// receiving the notification.
    pub fn notify(&self, event: &GossipEvent) {
        let subs = self.subscribers.read();
        for sub in subs.iter() {
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| match event {
                GossipEvent::Join { endpoint, state } => sub.on_join(*endpoint, state),
                GossipEvent::Alive { endpoint, state } => sub.on_alive(*endpoint, state),
                GossipEvent::Dead { endpoint, state } => sub.on_dead(*endpoint, state),
                GossipEvent::Removed { endpoint, state } => sub.on_removed(*endpoint, state),
                GossipEvent::Restart { endpoint, state } => sub.on_restart(*endpoint, state),
                GossipEvent::StateChanged {
                    endpoint,
                    key,
                    value,
                } => sub.on_state_changed(*endpoint, *key, value),
            }));

            if let Err(e) = result {
                warn!(error = ?e, "Gossip subscriber panicked during notification");
            }
        }
    }
}

impl Default for SubscriberRegistry {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::SocketAddr;
    use std::sync::Mutex;

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    /// A test subscriber that records all events.
    struct RecordingSubscriber {
        events: Mutex<Vec<String>>,
    }

    impl RecordingSubscriber {
        fn new() -> Self {
            Self {
                events: Mutex::new(Vec::new()),
            }
        }

        fn events(&self) -> Vec<String> {
            self.events.lock().unwrap().clone()
        }
    }

    impl EndpointStateChangeSubscriber for RecordingSubscriber {
        fn on_join(&self, endpoint: Endpoint, _state: &EndpointState) {
            self.events
                .lock()
                .unwrap()
                .push(format!("join:{}", endpoint));
        }

        fn on_alive(&self, endpoint: Endpoint, _state: &EndpointState) {
            self.events
                .lock()
                .unwrap()
                .push(format!("alive:{}", endpoint));
        }

        fn on_dead(&self, endpoint: Endpoint, _state: &EndpointState) {
            self.events
                .lock()
                .unwrap()
                .push(format!("dead:{}", endpoint));
        }

        fn on_removed(&self, endpoint: Endpoint, _state: &EndpointState) {
            self.events
                .lock()
                .unwrap()
                .push(format!("removed:{}", endpoint));
        }

        fn on_restart(&self, endpoint: Endpoint, _state: &EndpointState) {
            self.events
                .lock()
                .unwrap()
                .push(format!("restart:{}", endpoint));
        }

        fn on_state_changed(
            &self,
            endpoint: Endpoint,
            key: ApplicationState,
            _value: &VersionedValue,
        ) {
            self.events
                .lock()
                .unwrap()
                .push(format!("state_changed:{}:{:?}", endpoint, key));
        }
    }

    /// A subscriber that panics on every callback.
    struct PanickingSubscriber;

    impl EndpointStateChangeSubscriber for PanickingSubscriber {
        fn on_join(&self, _: Endpoint, _: &EndpointState) {
            panic!("intentional test panic");
        }
        fn on_alive(&self, _: Endpoint, _: &EndpointState) {
            panic!("intentional test panic");
        }
        fn on_dead(&self, _: Endpoint, _: &EndpointState) {
            panic!("intentional test panic");
        }
        fn on_removed(&self, _: Endpoint, _: &EndpointState) {
            panic!("intentional test panic");
        }
        fn on_restart(&self, _: Endpoint, _: &EndpointState) {
            panic!("intentional test panic");
        }
        fn on_state_changed(&self, _: Endpoint, _: ApplicationState, _: &VersionedValue) {
            panic!("intentional test panic");
        }
    }

    #[test]
    fn register_and_notify_join() {
        let registry = SubscriberRegistry::new();
        let sub = Arc::new(RecordingSubscriber::new());
        registry.register(sub.clone());

        assert_eq!(registry.subscriber_count(), 1);

        let state = EndpointState::new(1);
        registry.notify(&GossipEvent::Join {
            endpoint: ep(7001),
            state,
        });

        let events = sub.events();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0], "join:127.0.0.1:7001");
    }

    #[test]
    fn multiple_subscribers() {
        let registry = SubscriberRegistry::new();
        let sub1 = Arc::new(RecordingSubscriber::new());
        let sub2 = Arc::new(RecordingSubscriber::new());
        registry.register(sub1.clone());
        registry.register(sub2.clone());

        let state = EndpointState::new(1);
        registry.notify(&GossipEvent::Dead {
            endpoint: ep(7002),
            state,
        });

        assert_eq!(sub1.events().len(), 1);
        assert_eq!(sub2.events().len(), 1);
        assert_eq!(sub1.events()[0], "dead:127.0.0.1:7002");
    }

    #[test]
    fn empty_registry_noop() {
        let registry = SubscriberRegistry::new();
        // Should not panic
        registry.notify(&GossipEvent::Alive {
            endpoint: ep(7001),
            state: EndpointState::new(1),
        });
        assert_eq!(registry.subscriber_count(), 0);
    }

    #[test]
    fn panic_isolation() {
        let registry = SubscriberRegistry::new();
        let panicker = Arc::new(PanickingSubscriber);
        let recorder = Arc::new(RecordingSubscriber::new());

        // Register panicking subscriber FIRST, recorder SECOND
        registry.register(panicker);
        registry.register(recorder.clone());

        let state = EndpointState::new(1);
        registry.notify(&GossipEvent::Join {
            endpoint: ep(7001),
            state,
        });

        // Recorder should still have received the event despite panicker
        assert_eq!(recorder.events().len(), 1);
    }

    #[test]
    fn all_event_types() {
        let registry = SubscriberRegistry::new();
        let sub = Arc::new(RecordingSubscriber::new());
        registry.register(sub.clone());

        let state = EndpointState::new(1);
        registry.notify(&GossipEvent::Join {
            endpoint: ep(7001),
            state: state.clone(),
        });
        registry.notify(&GossipEvent::Alive {
            endpoint: ep(7001),
            state: state.clone(),
        });
        registry.notify(&GossipEvent::Dead {
            endpoint: ep(7001),
            state: state.clone(),
        });
        registry.notify(&GossipEvent::Removed {
            endpoint: ep(7001),
            state: state.clone(),
        });
        registry.notify(&GossipEvent::Restart {
            endpoint: ep(7001),
            state: state.clone(),
        });
        registry.notify(&GossipEvent::StateChanged {
            endpoint: ep(7001),
            key: ApplicationState::Status,
            value: VersionedValue::new(1, "NORMAL"),
        });

        let events = sub.events();
        assert_eq!(events.len(), 6);
        assert!(events[0].starts_with("join:"));
        assert!(events[1].starts_with("alive:"));
        assert!(events[2].starts_with("dead:"));
        assert!(events[3].starts_with("removed:"));
        assert!(events[4].starts_with("restart:"));
        assert!(events[5].starts_with("state_changed:"));
    }
}
