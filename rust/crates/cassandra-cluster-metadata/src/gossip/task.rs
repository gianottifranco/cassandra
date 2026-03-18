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

//! Periodic gossip task: runs the SYN/ACK/ACK2 loop every second.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.gms.Gossiper.GossipTask`

use std::sync::Arc;
use std::time::Duration;

use cassandra_messaging::verb::Verb;
use cassandra_messaging::{Message, MessagingService};
use tracing::{debug, warn};

use crate::gossip::Gossiper;

/// Configuration for the periodic gossip task.
pub struct GossipTaskConfig {
    /// Interval between gossip rounds.
    pub interval: Duration,
    /// Timeout for individual gossip message exchanges.
    pub message_timeout: Duration,
}

impl Default for GossipTaskConfig {
    fn default() -> Self {
        Self {
            interval: Duration::from_secs(1),
            message_timeout: Duration::from_secs(1),
        }
    }
}

/// The periodic gossip task.
///
/// Each round: increment heartbeat, pick target, SYN→ACK→ACK2 exchange,
/// health assessment, quarantine cleanup, and dead endpoint probing.
pub struct GossipTask {
    gossiper: Arc<Gossiper>,
    messaging: Arc<MessagingService>,
    config: GossipTaskConfig,
}

impl GossipTask {
    /// Create a new gossip task.
    pub fn new(
        gossiper: Arc<Gossiper>,
        messaging: Arc<MessagingService>,
        config: GossipTaskConfig,
    ) -> Self {
        Self {
            gossiper,
            messaging,
            config,
        }
    }

    /// Spawn the periodic gossip task. Returns a `JoinHandle` for the
    /// background tokio task.
    pub fn spawn(self: Arc<Self>) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut interval = tokio::time::interval(self.config.interval);
            loop {
                interval.tick().await;

                if !self.gossiper.is_running() {
                    debug!("Gossiper stopped, exiting gossip task");
                    break;
                }

                self.do_one_round().await;
            }
        })
    }

    /// Execute a single gossip round. Exposed for testability.
    pub async fn do_one_round(&self) {
        // Step 1: Increment local heartbeat
        self.gossiper.increment_heartbeat();

        // Step 2: Pick a gossip target
        if let Some(target) = self.gossiper.pick_gossip_target() {
            // Step 3: Build and send SYN
            let syn = self.gossiper.make_gossip_digest_syn();
            let syn_payload = match serde_json::to_vec(&syn) {
                Ok(p) => p,
                Err(e) => {
                    warn!(error = %e, "Failed to serialize SYN");
                    return;
                }
            };

            let msg_id = self.messaging.next_id();
            let syn_msg = Message::request(Verb::GossipDigestSyn, msg_id, syn_payload);

            // Step 4: Send SYN and wait for ACK
            match self
                .messaging
                .send_and_wait(target.addr(), syn_msg, self.config.message_timeout)
                .await
            {
                Ok(ack_msg) => {
                    // Step 5: Process ACK → produce ACK2
                    if let Ok(ack) = serde_json::from_slice(&ack_msg.payload) {
                        let ack2 = self.gossiper.handle_ack(&ack);

                        // Step 6: Send ACK2 (fire-and-forget)
                        if let Ok(ack2_payload) = serde_json::to_vec(&ack2) {
                            let ack2_msg = Message::request(
                                Verb::GossipDigestAck2,
                                self.messaging.next_id(),
                                ack2_payload,
                            );
                            if let Err(e) = self.messaging.send(target.addr(), ack2_msg).await {
                                debug!(target = %target, error = %e, "Failed to send ACK2");
                            }
                        }
                    } else {
                        warn!(target = %target, "Failed to deserialize ACK response");
                    }
                }
                Err(e) => {
                    debug!(target = %target, error = %e, "Gossip SYN to target failed");
                }
            }
        }

        // Step 7: Assess endpoint health
        self.gossiper.assess_endpoint_health();

        // Step 8: Cleanup expired quarantine entries
        self.gossiper.cleanup_quarantine();

        // Step 9: Maybe probe dead endpoints
        if let Some(dead_target) = self.gossiper.maybe_probe_dead_endpoints() {
            let syn = self.gossiper.make_gossip_digest_syn();
            if let Ok(syn_payload) = serde_json::to_vec(&syn) {
                let msg_id = self.messaging.next_id();
                let syn_msg = Message::request(Verb::GossipDigestSyn, msg_id, syn_payload);
                let _ = self.messaging.send(dead_target.addr(), syn_msg).await;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gossip::handlers::register_gossip_handlers;
    use crate::gossip::{ApplicationState, SeedProvider};
    use crate::node::Endpoint;
    use std::net::SocketAddr;

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    #[tokio::test]
    async fn do_one_round_increments_heartbeat() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let gossiper = Arc::new(Gossiper::new(ep(7001), SeedProvider::new(vec![]), 1));
        let messaging = Arc::new(MessagingService::new(addr));

        let initial_version = gossiper
            .get_endpoint_state(&ep(7001))
            .unwrap()
            .heartbeat
            .version;

        let task = GossipTask::new(
            Arc::clone(&gossiper),
            messaging,
            GossipTaskConfig::default(),
        );

        task.do_one_round().await;

        let new_version = gossiper
            .get_endpoint_state(&ep(7001))
            .unwrap()
            .heartbeat
            .version;

        assert_eq!(new_version, initial_version + 1);
    }

    #[tokio::test]
    async fn stopping_gossiper_stops_loop() {
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        let gossiper = Arc::new(Gossiper::new(ep(7001), SeedProvider::new(vec![]), 1));
        let messaging = Arc::new(MessagingService::new(addr));

        let task = Arc::new(GossipTask::new(
            Arc::clone(&gossiper),
            messaging,
            GossipTaskConfig {
                interval: Duration::from_millis(50),
                message_timeout: Duration::from_millis(100),
            },
        ));

        let handle = task.clone().spawn();

        // Let it run a couple of rounds
        tokio::time::sleep(Duration::from_millis(150)).await;

        // Stop the gossiper
        gossiper.shutdown();

        // The task should exit
        let result = tokio::time::timeout(Duration::from_secs(1), handle).await;
        assert!(result.is_ok(), "Task should have stopped within timeout");
    }

    #[tokio::test]
    async fn do_one_round_with_live_target() {
        // Start a "seed" node with handlers
        let seed_gossiper = Arc::new(Gossiper::new(
            ep(0), // will be replaced by actual addr
            SeedProvider::new(vec![]),
            1,
        ));
        seed_gossiper.set_local_state(ApplicationState::Datacenter, "dc1".to_string());

        let seed_messaging = Arc::new(MessagingService::new("127.0.0.1:0".parse().unwrap()));
        register_gossip_handlers(Arc::clone(&seed_gossiper), &seed_messaging);

        // Start the seed's listener
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let seed_addr = listener.local_addr().unwrap();

        let svc = Arc::clone(&seed_messaging);
        tokio::spawn(async move {
            loop {
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let svc2 = Arc::clone(&svc);
                        tokio::spawn(async move {
                            let _ = svc2.dispatch_on_stream(stream).await;
                        });
                    }
                    Err(_) => break,
                }
            }
        });

        tokio::time::sleep(Duration::from_millis(50)).await;

        // Create the joiner node that gossips to the seed
        let joiner = Arc::new(Gossiper::new(
            ep(0),
            SeedProvider::new(vec![Endpoint::new(seed_addr)]),
            1,
        ));
        joiner.set_local_state(ApplicationState::Status, "JOINING".to_string());

        let joiner_messaging = Arc::new(MessagingService::new("127.0.0.1:0".parse().unwrap()));

        let task = GossipTask::new(
            Arc::clone(&joiner),
            joiner_messaging,
            GossipTaskConfig {
                interval: Duration::from_secs(1),
                message_timeout: Duration::from_secs(2),
            },
        );

        // Run one round — should succeed even if target doesn't respond perfectly
        task.do_one_round().await;

        // Heartbeat should have incremented
        let state = joiner.get_endpoint_state(&ep(0)).unwrap();
        assert!(state.heartbeat.version >= 1);
    }
}
