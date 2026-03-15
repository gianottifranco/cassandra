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

//! Gossip message types for the SYN → ACK → ACK2 protocol.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.gms.GossipDigestSyn`
//! - `org.apache.cassandra.gms.GossipDigestAck`
//! - `org.apache.cassandra.gms.GossipDigestAck2`

use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::gossip::EndpointState;
use crate::node::Endpoint;

/// A gossip digest summarizes what we know about an endpoint.
///
/// Used during gossiping: "I know endpoint X at generation G, max version V."
/// If the peer has a higher version, it will send us the delta.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipDigest {
    pub endpoint: Endpoint,
    pub generation: i64,
    pub max_version: i64,
}

/// SYN message: initiates a gossip round.
///
/// Contains a list of digests for all endpoints this node knows about.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipDigestSyn {
    /// Cluster identifier (must match for nodes to gossip).
    pub cluster_id: String,
    /// Digests for all known endpoints.
    pub digests: Vec<GossipDigest>,
}

/// ACK message: response to a SYN.
///
/// Contains:
/// 1. Digests for states we need from the sender (we're behind)
/// 2. Full state for endpoints where the sender is behind
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipDigestAck {
    /// Digests for endpoints where we need updates from the sender.
    pub stale_digests: Vec<GossipDigest>,
    /// States where we have newer data than the sender.
    pub updated_states: HashMap<Endpoint, EndpointState>,
}

/// ACK2 message: final leg of the gossip exchange.
///
/// Contains state updates for the endpoints the ACK receiver requested.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GossipDigestAck2 {
    /// States for endpoints the ACK receiver was behind on.
    pub updated_states: HashMap<Endpoint, EndpointState>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    #[test]
    fn digest_creation() {
        let d = GossipDigest {
            endpoint: ep(7001),
            generation: 1,
            max_version: 42,
        };
        assert_eq!(d.endpoint, ep(7001));
        assert_eq!(d.generation, 1);
        assert_eq!(d.max_version, 42);
    }

    #[test]
    fn syn_serialization_round_trip() {
        let syn = GossipDigestSyn {
            cluster_id: "test-cluster".to_string(),
            digests: vec![GossipDigest {
                endpoint: ep(7001),
                generation: 1,
                max_version: 10,
            }],
        };

        let json = serde_json::to_string(&syn).unwrap();
        let decoded: GossipDigestSyn = serde_json::from_str(&json).unwrap();

        assert_eq!(decoded.cluster_id, "test-cluster");
        assert_eq!(decoded.digests.len(), 1);
        assert_eq!(decoded.digests[0].endpoint, ep(7001));
    }

    #[test]
    fn ack_with_states() {
        let mut states = HashMap::new();
        states.insert(ep(7001), EndpointState::new(1));

        let ack = GossipDigestAck {
            stale_digests: vec![],
            updated_states: states,
        };

        assert_eq!(ack.updated_states.len(), 1);
    }
}
