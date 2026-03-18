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

//! Gossip shutdown protocol: graceful cluster departure.
//!
//! When a node is shutting down, it announces its departure to all
//! live peers so they can immediately mark it dead rather than waiting
//! for the failure detector to time out.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.gms.Gossiper.stop()`
//! - `org.apache.cassandra.net.GossipShutdownVerbHandler`

use std::sync::Arc;
use std::time::Duration;

use cassandra_messaging::service::MessageHandler;
use cassandra_messaging::verb::Verb;
use cassandra_messaging::{Message, MessagingService};
use serde::{Deserialize, Serialize};
use tracing::{debug, info, warn};

use crate::gossip::Gossiper;
use crate::node::Endpoint;

/// Message sent when a node is shutting down.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipShutdownMessage {
    /// The endpoint that is shutting down.
    pub endpoint: Endpoint,
}

/// Create a handler for incoming gossip shutdown messages.
///
/// When a peer announces shutdown, we quarantine it and mark it dead
/// immediately — no need to wait for the failure detector.
pub fn make_shutdown_handler(gossiper: Arc<Gossiper>) -> MessageHandler {
    Arc::new(move |msg: Message| {
        let shutdown_msg: GossipShutdownMessage = match serde_json::from_slice(&msg.payload) {
            Ok(m) => m,
            Err(e) => {
                warn!(error = %e, "Failed to deserialize GossipShutdownMessage");
                return None;
            }
        };

        info!(endpoint = %shutdown_msg.endpoint, "Received gossip shutdown announcement");

        // Quarantine the departing node
        gossiper.quarantine(shutdown_msg.endpoint);

        // Mark it dead immediately
        gossiper.convict(&shutdown_msg.endpoint);

        None // No response needed
    })
}

/// Register the gossip shutdown handler on the messaging service.
pub fn register_shutdown_handler(gossiper: Arc<Gossiper>, messaging: &MessagingService) {
    messaging.register_handler(Verb::GossipShutdown, make_shutdown_handler(gossiper));
}

/// Announce shutdown to all live peers, then stop the gossiper.
///
/// Sets local status to SHUTDOWN, broadcasts shutdown message to all
/// live endpoints, waits briefly for delivery, then calls `gossiper.shutdown()`.
pub async fn announce_shutdown(gossiper: &Gossiper, messaging: &MessagingService) {
    // Set local status to indicate shutdown
    gossiper.set_local_state(
        crate::gossip::ApplicationState::Status,
        "SHUTDOWN".to_string(),
    );

    let shutdown_msg = GossipShutdownMessage {
        endpoint: gossiper.local_endpoint(),
    };

    let payload = match serde_json::to_vec(&shutdown_msg) {
        Ok(p) => p,
        Err(e) => {
            warn!(error = %e, "Failed to serialize shutdown message");
            gossiper.shutdown();
            return;
        }
    };

    // Send shutdown announcement to all live peers
    let live = gossiper.live_endpoints();
    for peer in &live {
        let msg_id = messaging.next_id();
        let msg = Message::request(Verb::GossipShutdown, msg_id, payload.clone());
        if let Err(e) = messaging.send(peer.addr(), msg).await {
            debug!(peer = %peer, error = %e, "Failed to send shutdown to peer");
        }
    }

    // Brief wait for messages to be delivered
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Stop the gossiper
    gossiper.shutdown();

    info!(
        endpoint = %gossiper.local_endpoint(),
        peers_notified = live.len(),
        "Gossip shutdown complete"
    );
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gossip::SeedProvider;
    use std::net::SocketAddr;

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    #[test]
    fn shutdown_handler_quarantines_sender() {
        let gossiper = Arc::new(Gossiper::new(
            ep(7001),
            SeedProvider::new(vec![ep(7002)]),
            1,
        ));

        // Simulate ep(7002) being live
        gossiper.live_endpoints.write().push(ep(7002));

        let handler = make_shutdown_handler(Arc::clone(&gossiper));

        let shutdown_msg = GossipShutdownMessage { endpoint: ep(7002) };
        let payload = serde_json::to_vec(&shutdown_msg).unwrap();
        let msg = Message::request(Verb::GossipShutdown, 1, payload);

        handler(msg);

        // ep(7002) should be quarantined and dead
        assert!(gossiper.is_quarantined(&ep(7002)));
        assert!(gossiper.dead_endpoints().contains(&ep(7002)));
        assert!(!gossiper.live_endpoints().contains(&ep(7002)));
    }

    #[tokio::test]
    async fn announce_shutdown_sets_status_and_stops() {
        let gossiper = Gossiper::new(ep(7001), SeedProvider::new(vec![]), 1);
        let messaging = MessagingService::new("127.0.0.1:0".parse().unwrap());

        assert!(gossiper.is_running());

        announce_shutdown(&gossiper, &messaging).await;

        assert!(!gossiper.is_running());

        // Status should be LEFT (set by gossiper.shutdown())
        let state = gossiper.get_endpoint_state(&ep(7001)).unwrap();
        assert_eq!(state.status(), Some("LEFT"));
    }

    #[test]
    fn shutdown_message_serialization_round_trip() {
        let msg = GossipShutdownMessage { endpoint: ep(7001) };
        let json = serde_json::to_vec(&msg).unwrap();
        let decoded: GossipShutdownMessage = serde_json::from_slice(&json).unwrap();
        assert_eq!(decoded.endpoint, ep(7001));
    }
}
