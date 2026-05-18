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

//! Gossip verb handlers: SYN, ACK, ACK2.
//!
//! Creates `MessageHandler` closures that bridge the `MessagingService`
//! dispatch layer to the `Gossiper` protocol methods.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.gms.GossipDigestSynVerbHandler`
//! - `org.apache.cassandra.gms.GossipDigestAckVerbHandler`
//! - `org.apache.cassandra.gms.GossipDigestAck2VerbHandler`

use std::sync::Arc;

use cassandra_messaging::service::MessageHandler;
use cassandra_messaging::verb::Verb;
use cassandra_messaging::{Message, MessagingService};
use tracing::{debug, warn};

use crate::gossip::Gossiper;
use crate::gossip::messages::{
    GossipDigestAck, GossipDigestAck2, GossipDigestSyn, JavaGossipCodec,
};

/// Create a SYN handler for the gossiper.
///
/// Validates cluster name, deserializes the SYN, calls `gossiper.handle_syn()`,
/// and returns the serialized ACK as a response message.
pub fn make_syn_handler(gossiper: Arc<Gossiper>) -> MessageHandler {
    Arc::new(move |msg: Message| {
        let syn: GossipDigestSyn = match JavaGossipCodec::decode_syn(&msg.payload) {
            Ok(s) => s,
            Err(e) => {
                warn!(error = %e, "Failed to deserialize GossipDigestSyn");
                return Some(Message::failure(
                    msg.header.message_id,
                    format!("Bad SYN payload: {}", e).into_bytes(),
                ));
            }
        };

        // Validate cluster name
        if syn.cluster_id != gossiper.cluster_name() {
            warn!(
                expected = gossiper.cluster_name(),
                received = syn.cluster_id,
                "Cluster name mismatch in gossip SYN"
            );
            return Some(Message::failure(
                msg.header.message_id,
                format!(
                    "Cluster name mismatch: expected '{}', got '{}'",
                    gossiper.cluster_name(),
                    syn.cluster_id
                )
                .into_bytes(),
            ));
        }

        let ack = gossiper.handle_syn(&syn);
        let payload = match JavaGossipCodec::encode_ack(&ack) {
            Ok(payload) => payload,
            Err(e) => {
                warn!(error = %e, "Failed to serialize GossipDigestAck");
                return Some(Message::failure(
                    msg.header.message_id,
                    format!("Bad ACK payload: {e}").into_bytes(),
                ));
            }
        };
        Some(Message::response(
            msg.header.message_id,
            Verb::GossipDigestAck,
            payload,
        ))
    })
}

/// Create an ACK handler for the gossiper.
///
/// Deserializes the ACK, calls `gossiper.handle_ack()`, and returns
/// the serialized ACK2 as a response message.
pub fn make_ack_handler(gossiper: Arc<Gossiper>) -> MessageHandler {
    Arc::new(move |msg: Message| {
        let ack: GossipDigestAck = match JavaGossipCodec::decode_ack(&msg.payload) {
            Ok(a) => a,
            Err(e) => {
                warn!(error = %e, "Failed to deserialize GossipDigestAck");
                return Some(Message::failure(
                    msg.header.message_id,
                    format!("Bad ACK payload: {}", e).into_bytes(),
                ));
            }
        };

        let ack2 = gossiper.handle_ack(&ack);
        let payload = match JavaGossipCodec::encode_ack2(&ack2) {
            Ok(payload) => payload,
            Err(e) => {
                warn!(error = %e, "Failed to serialize GossipDigestAck2");
                return Some(Message::failure(
                    msg.header.message_id,
                    format!("Bad ACK2 payload: {e}").into_bytes(),
                ));
            }
        };
        Some(Message::response(
            msg.header.message_id,
            Verb::GossipDigestAck2,
            payload,
        ))
    })
}

/// Create an ACK2 handler for the gossiper.
///
/// Deserializes the ACK2 and calls `gossiper.handle_ack2()`.
/// ACK2 is the terminal message — no response needed.
pub fn make_ack2_handler(gossiper: Arc<Gossiper>) -> MessageHandler {
    Arc::new(move |msg: Message| {
        let ack2: GossipDigestAck2 = match JavaGossipCodec::decode_ack2(&msg.payload) {
            Ok(a) => a,
            Err(e) => {
                warn!(error = %e, "Failed to deserialize GossipDigestAck2");
                return None;
            }
        };

        gossiper.handle_ack2(&ack2);
        debug!("Processed ACK2");
        None
    })
}

/// Register all gossip verb handlers on the messaging service.
pub fn register_gossip_handlers(gossiper: Arc<Gossiper>, messaging: &MessagingService) {
    messaging.register_handler(
        Verb::GossipDigestSyn,
        make_syn_handler(Arc::clone(&gossiper)),
    );
    messaging.register_handler(
        Verb::GossipDigestAck,
        make_ack_handler(Arc::clone(&gossiper)),
    );
    messaging.register_handler(Verb::GossipDigestAck2, make_ack2_handler(gossiper));
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::gossip::{ApplicationState, SeedProvider};
    use crate::node::Endpoint;
    use std::net::SocketAddr;

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    #[test]
    fn syn_handler_produces_ack() {
        let gossiper = Arc::new(Gossiper::new(
            ep(7001),
            SeedProvider::new(vec![ep(7002)]),
            1,
        ));
        gossiper.set_local_state(ApplicationState::Status, "NORMAL".to_string());

        let handler = make_syn_handler(Arc::clone(&gossiper));

        // Build a SYN from a "remote" gossiper
        let remote = Gossiper::new(ep(7002), SeedProvider::new(vec![ep(7001)]), 1);
        remote.set_local_state(ApplicationState::Datacenter, "dc1".to_string());
        let syn = remote.make_gossip_digest_syn();

        let syn_payload = JavaGossipCodec::encode_syn(&syn).unwrap();
        let msg = Message::request(Verb::GossipDigestSyn, 1, syn_payload);

        let response = handler(msg);
        assert!(response.is_some());
        let resp = response.unwrap();
        assert_eq!(resp.header.verb, Verb::GossipDigestAck);
        assert!(resp.is_response());

        // Verify the Java-layout ACK payload deserializes.
        let _ack = JavaGossipCodec::decode_ack(&resp.payload).unwrap();
    }

    #[test]
    fn full_syn_ack_ack2_round_trip() {
        let g1 = Arc::new(Gossiper::new(
            ep(7001),
            SeedProvider::new(vec![ep(7002)]),
            1,
        ));
        let g2 = Arc::new(Gossiper::new(
            ep(7002),
            SeedProvider::new(vec![ep(7001)]),
            1,
        ));

        g1.set_local_state(ApplicationState::Datacenter, "dc1".to_string());
        g2.set_local_state(ApplicationState::Datacenter, "dc2".to_string());

        let syn_handler = make_syn_handler(Arc::clone(&g2));
        let ack_handler = make_ack_handler(Arc::clone(&g1));

        // Step 1: G1 builds SYN, dispatches through G2's handler
        let syn = g1.make_gossip_digest_syn();
        let syn_msg = Message::request(
            Verb::GossipDigestSyn,
            1,
            JavaGossipCodec::encode_syn(&syn).unwrap(),
        );
        let ack_msg = syn_handler(syn_msg).unwrap();

        // Step 2: G1 receives ACK, dispatches through its ACK handler
        let ack_msg_as_request =
            Message::request(Verb::GossipDigestAck, 2, ack_msg.payload.clone());
        let ack2_msg = ack_handler(ack_msg_as_request).unwrap();

        // Step 3: G2 receives ACK2
        let ack2_handler = make_ack2_handler(Arc::clone(&g2));
        let ack2_msg_as_request =
            Message::request(Verb::GossipDigestAck2, 3, ack2_msg.payload.clone());
        ack2_handler(ack2_msg_as_request);

        // Both should now know about each other
        assert_eq!(g1.known_endpoint_count(), 2);
        assert_eq!(g2.known_endpoint_count(), 2);
    }

    #[test]
    fn malformed_payload_returns_failure() {
        let gossiper = Arc::new(Gossiper::new(ep(7001), SeedProvider::new(vec![]), 1));

        let handler = make_syn_handler(gossiper);
        let msg = Message::request(Verb::GossipDigestSyn, 1, b"not json".to_vec());

        let response = handler(msg);
        assert!(response.is_some());
        let resp = response.unwrap();
        assert!(resp.is_failure());
    }

    #[test]
    fn cluster_name_mismatch_rejected() {
        let gossiper = Arc::new(Gossiper::with_cluster_name(
            ep(7001),
            SeedProvider::new(vec![]),
            1,
            "my-cluster",
        ));

        let handler = make_syn_handler(gossiper);

        let syn = GossipDigestSyn {
            cluster_id: "wrong-cluster".to_string(),
            digests: vec![],
        };
        let msg = Message::request(
            Verb::GossipDigestSyn,
            1,
            JavaGossipCodec::encode_syn(&syn).unwrap(),
        );

        let response = handler(msg);
        assert!(response.is_some());
        let resp = response.unwrap();
        assert!(resp.is_failure());
        let body = String::from_utf8_lossy(&resp.payload);
        assert!(body.contains("mismatch"));
    }
}
