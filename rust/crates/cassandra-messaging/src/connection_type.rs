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

//! Connection type classification for three-channel internode messaging.
//!
//! Each peer connection uses three channels (urgent/small/large) to
//! prevent head-of-line blocking. Messages are classified by verb and
//! payload size.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.net.ConnectionType`
//! - `org.apache.cassandra.net.OutboundConnections`

use crate::verb::Verb;

/// Threshold for classifying a message as "large" (64 KiB).
pub const LARGE_THRESHOLD: usize = 64 * 1024;

/// Connection type for three-channel internode messaging.
///
/// Matches Java's `ConnectionType` enum used by `OutboundConnections`
/// to route messages to the appropriate channel.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ConnectionType {
    /// Urgent channel: Ping, Pong, Gossip messages.
    /// These are time-sensitive and must not be blocked.
    Urgent,
    /// Small channel: most request/response messages under 64 KiB.
    Small,
    /// Large channel: messages with payloads >= 64 KiB (streaming, etc.).
    Large,
}

impl ConnectionType {
    /// Classify a message by verb and payload size.
    ///
    /// - Urgent: Ping, Pong, GossipDigestSyn/Ack/Ack2, GossipShutdown
    /// - Large: payload >= 64 KiB
    /// - Small: everything else
    pub fn classify(verb: Verb, payload_size: usize) -> Self {
        if is_urgent(verb) {
            ConnectionType::Urgent
        } else if payload_size >= LARGE_THRESHOLD {
            ConnectionType::Large
        } else {
            ConnectionType::Small
        }
    }
}

impl std::fmt::Display for ConnectionType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConnectionType::Urgent => write!(f, "URGENT"),
            ConnectionType::Small => write!(f, "SMALL"),
            ConnectionType::Large => write!(f, "LARGE"),
        }
    }
}

fn is_urgent(verb: Verb) -> bool {
    matches!(
        verb,
        Verb::Ping
            | Verb::Pong
            | Verb::GossipDigestSyn
            | Verb::GossipDigestAck
            | Verb::GossipDigestAck2
            | Verb::GossipShutdown
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ping_is_urgent() {
        assert_eq!(ConnectionType::classify(Verb::Ping, 0), ConnectionType::Urgent);
    }

    #[test]
    fn pong_is_urgent() {
        assert_eq!(ConnectionType::classify(Verb::Pong, 0), ConnectionType::Urgent);
    }

    #[test]
    fn gossip_syn_is_urgent() {
        assert_eq!(
            ConnectionType::classify(Verb::GossipDigestSyn, 100),
            ConnectionType::Urgent
        );
    }

    #[test]
    fn gossip_ack_is_urgent() {
        assert_eq!(
            ConnectionType::classify(Verb::GossipDigestAck, 100),
            ConnectionType::Urgent
        );
    }

    #[test]
    fn gossip_ack2_is_urgent() {
        assert_eq!(
            ConnectionType::classify(Verb::GossipDigestAck2, 100),
            ConnectionType::Urgent
        );
    }

    #[test]
    fn gossip_shutdown_is_urgent() {
        assert_eq!(
            ConnectionType::classify(Verb::GossipShutdown, 0),
            ConnectionType::Urgent
        );
    }

    #[test]
    fn mutation_small() {
        assert_eq!(
            ConnectionType::classify(Verb::Mutation, 1000),
            ConnectionType::Small
        );
    }

    #[test]
    fn mutation_large() {
        assert_eq!(
            ConnectionType::classify(Verb::Mutation, LARGE_THRESHOLD),
            ConnectionType::Large
        );
    }

    #[test]
    fn stream_data_large() {
        assert_eq!(
            ConnectionType::classify(Verb::StreamData, 100_000),
            ConnectionType::Large
        );
    }

    #[test]
    fn read_data_small() {
        assert_eq!(
            ConnectionType::classify(Verb::ReadData, 512),
            ConnectionType::Small
        );
    }

    #[test]
    fn urgent_verbs_ignore_payload_size() {
        // Even with a large payload, gossip is still urgent
        assert_eq!(
            ConnectionType::classify(Verb::GossipDigestSyn, 100_000),
            ConnectionType::Urgent
        );
    }

    #[test]
    fn display_impl() {
        assert_eq!(format!("{}", ConnectionType::Urgent), "URGENT");
        assert_eq!(format!("{}", ConnectionType::Small), "SMALL");
        assert_eq!(format!("{}", ConnectionType::Large), "LARGE");
    }
}
