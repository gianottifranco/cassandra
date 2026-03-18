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

//! Shadow round: network-integrated cluster discovery before bootstrap.
//!
//! Sends empty-digest SYN to all seeds and collects ACK responses
//! WITHOUT modifying the live/dead endpoint lists. This gives a new
//! node a view of the cluster before announcing itself.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.gms.Gossiper.doShadowRound()`

use std::collections::HashMap;
use std::time::Duration;

use cassandra_messaging::verb::Verb;
use cassandra_messaging::{Message, MessagingService};
use tracing::{debug, info, warn};

use crate::gossip::messages::{GossipDigestAck, GossipDigestSyn};
use crate::gossip::{EndpointState, Gossiper};
use crate::node::Endpoint;

/// Result of a shadow round.
#[derive(Debug)]
pub struct ShadowRoundResult {
    /// Endpoints discovered from seed ACK responses.
    pub discovered_endpoints: Vec<Endpoint>,
    /// Endpoint states collected from seeds (read-only snapshot).
    pub endpoint_states: HashMap<Endpoint, EndpointState>,
    /// Number of seeds that responded.
    pub seeds_responded: usize,
    /// Total number of seeds contacted.
    pub seeds_total: usize,
}

/// Shadow round configuration.
pub struct ShadowRoundConfig {
    /// Timeout for each seed's SYN→ACK exchange.
    pub per_seed_timeout: Duration,
    /// Minimum number of seed responses required.
    pub min_seeds: usize,
}

impl Default for ShadowRoundConfig {
    fn default() -> Self {
        Self {
            per_seed_timeout: Duration::from_secs(5),
            min_seeds: 1,
        }
    }
}

/// Execute a shadow round: discover the cluster topology from seeds.
///
/// Sends an empty-digest SYN to each seed and collects ACK responses.
/// Does NOT update the gossiper's live/dead lists — the result is a
/// read-only snapshot for the caller to inspect.
pub async fn execute_shadow_round(
    gossiper: &Gossiper,
    messaging: &MessagingService,
    config: &ShadowRoundConfig,
) -> Result<ShadowRoundResult, ShadowRoundError> {
    let seeds: Vec<Endpoint> = gossiper
        .seeds()
        .seeds()
        .iter()
        .filter(|s| **s != gossiper.local_endpoint())
        .copied()
        .collect();

    if seeds.is_empty() {
        return Err(ShadowRoundError::NoSeeds);
    }

    let seeds_total = seeds.len();

    // Build a SYN with empty digests (we know nothing yet).
    let syn = GossipDigestSyn {
        cluster_id: gossiper.cluster_name().to_string(),
        digests: vec![],
    };

    let syn_payload = serde_json::to_vec(&syn)
        .map_err(|e| ShadowRoundError::Internal(format!("Failed to serialize SYN: {}", e)))?;

    let mut all_states: HashMap<Endpoint, EndpointState> = HashMap::new();
    let mut seeds_responded = 0;

    for seed in &seeds {
        let msg_id = messaging.next_id();
        let syn_msg = Message::request(Verb::GossipDigestSyn, msg_id, syn_payload.clone());

        match messaging
            .send_and_wait(seed.addr(), syn_msg, config.per_seed_timeout)
            .await
        {
            Ok(ack_msg) => {
                if ack_msg.is_failure() {
                    warn!(seed = %seed, "Seed returned failure response");
                    continue;
                }

                match serde_json::from_slice::<GossipDigestAck>(&ack_msg.payload) {
                    Ok(ack) => {
                        seeds_responded += 1;
                        info!(seed = %seed, states = ack.updated_states.len(), "Shadow round: seed responded");

                        for (ep, state) in ack.updated_states {
                            all_states.insert(ep, state);
                        }
                    }
                    Err(e) => {
                        warn!(seed = %seed, error = %e, "Failed to parse shadow round ACK");
                    }
                }
            }
            Err(e) => {
                debug!(seed = %seed, error = %e, "Shadow round: seed unreachable");
            }
        }
    }

    if seeds_responded < config.min_seeds {
        return Err(ShadowRoundError::InsufficientResponses {
            responded: seeds_responded,
            required: config.min_seeds,
        });
    }

    let discovered_endpoints: Vec<Endpoint> = all_states.keys().copied().collect();

    Ok(ShadowRoundResult {
        discovered_endpoints,
        endpoint_states: all_states,
        seeds_responded,
        seeds_total,
    })
}

/// Errors from the shadow round.
#[derive(Debug, thiserror::Error)]
pub enum ShadowRoundError {
    #[error("No seeds configured")]
    NoSeeds,

    #[error("Insufficient seed responses: {responded}/{required}")]
    InsufficientResponses { responded: usize, required: usize },

    #[error("Internal error: {0}")]
    Internal(String),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gossip::handlers::register_gossip_handlers;
    use crate::gossip::{ApplicationState, SeedProvider};
    use std::net::SocketAddr;
    use std::sync::Arc;

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    #[tokio::test]
    async fn shadow_round_no_seeds() {
        let gossiper = Gossiper::new(ep(7001), SeedProvider::new(vec![]), 1);
        let messaging = MessagingService::new("127.0.0.1:0".parse().unwrap());

        let result =
            execute_shadow_round(&gossiper, &messaging, &ShadowRoundConfig::default()).await;

        assert!(matches!(result, Err(ShadowRoundError::NoSeeds)));
    }

    #[tokio::test]
    async fn shadow_round_timeout_when_seed_unreachable() {
        // Seed on a port that nothing listens on
        let gossiper = Gossiper::new(ep(7001), SeedProvider::new(vec![ep(19999)]), 1);
        let messaging = MessagingService::new("127.0.0.1:0".parse().unwrap());

        let config = ShadowRoundConfig {
            per_seed_timeout: Duration::from_millis(200),
            min_seeds: 1,
        };

        let result = execute_shadow_round(&gossiper, &messaging, &config).await;

        assert!(matches!(
            result,
            Err(ShadowRoundError::InsufficientResponses { .. })
        ));
    }

    #[tokio::test]
    async fn shadow_round_does_not_modify_live_dead() {
        // Set up a seed with a listener
        let seed_gossiper = Arc::new(Gossiper::new(ep(0), SeedProvider::new(vec![]), 1));
        seed_gossiper.set_local_state(ApplicationState::Datacenter, "dc1".to_string());
        seed_gossiper.set_local_state(ApplicationState::Status, "NORMAL".to_string());

        let seed_messaging = Arc::new(MessagingService::new("127.0.0.1:0".parse().unwrap()));
        register_gossip_handlers(Arc::clone(&seed_gossiper), &seed_messaging);

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

        // Joiner performs shadow round
        let joiner = Gossiper::new(ep(0), SeedProvider::new(vec![Endpoint::new(seed_addr)]), 1);
        let joiner_messaging = MessagingService::new("127.0.0.1:0".parse().unwrap());

        let config = ShadowRoundConfig {
            per_seed_timeout: Duration::from_secs(2),
            min_seeds: 1,
        };

        // Verify live/dead are empty before
        assert!(joiner.live_endpoints().is_empty());
        assert!(joiner.dead_endpoints().is_empty());

        let _result = execute_shadow_round(&joiner, &joiner_messaging, &config).await;

        // Live/dead should still be empty (shadow round is read-only)
        assert!(joiner.live_endpoints().is_empty());
        assert!(joiner.dead_endpoints().is_empty());
    }
}
