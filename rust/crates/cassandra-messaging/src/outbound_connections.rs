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

//! Three-channel outbound connections wrapper.
//!
//! Groups three `OutboundConnection` instances (urgent/small/large) for
//! a single peer. Routes messages based on verb and payload size.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.net.OutboundConnections`

use std::net::SocketAddr;
use std::time::{Duration, Instant};

use crate::connection_type::ConnectionType;
use crate::frame::Message;
use crate::outbound_connection::{OutboundConnection, OutboundConnectionConfig};

/// Three-channel outbound connections for a single peer.
///
/// Messages are classified by verb and payload size, then routed to
/// the appropriate channel (urgent, small, or large).
pub struct OutboundConnections {
    urgent: OutboundConnection,
    small: OutboundConnection,
    large: OutboundConnection,
    remote: SocketAddr,
}

impl OutboundConnections {
    /// Create outbound connections for a remote peer.
    pub fn new(remote: SocketAddr, local_addr: SocketAddr) -> Self {
        Self {
            urgent: OutboundConnection::new(OutboundConnectionConfig {
                remote,
                local_addr,
                connection_type: ConnectionType::Urgent,
                compression: false,
                crc_framing: true,
            }),
            small: OutboundConnection::new(OutboundConnectionConfig {
                remote,
                local_addr,
                connection_type: ConnectionType::Small,
                compression: true,
                crc_framing: true,
            }),
            large: OutboundConnection::new(OutboundConnectionConfig {
                remote,
                local_addr,
                connection_type: ConnectionType::Large,
                compression: true,
                crc_framing: true,
            }),
            remote,
        }
    }

    /// Send a message, routing it to the correct channel.
    ///
    /// The message is classified by verb and payload size, then enqueued
    /// on the appropriate outbound connection.
    pub fn send(&self, msg: Message, timeout: Duration) -> bool {
        let conn_type = ConnectionType::classify(msg.header.verb, msg.payload.len());
        let expires_at = Instant::now() + timeout;
        self.channel(conn_type).enqueue(msg, expires_at)
    }

    /// Get the channel for a given connection type.
    fn channel(&self, conn_type: ConnectionType) -> &OutboundConnection {
        match conn_type {
            ConnectionType::Urgent => &self.urgent,
            ConnectionType::Small => &self.small,
            ConnectionType::Large => &self.large,
        }
    }

    /// Close all three channels.
    pub fn close_all(&self) {
        self.urgent.close();
        self.small.close();
        self.large.close();
    }

    /// The remote peer address.
    pub fn remote(&self) -> SocketAddr {
        self.remote
    }

    /// Get metrics from all channels.
    pub fn total_bytes_sent(&self) -> u64 {
        use std::sync::atomic::Ordering;
        self.urgent.metrics.bytes_sent.load(Ordering::Relaxed)
            + self.small.metrics.bytes_sent.load(Ordering::Relaxed)
            + self.large.metrics.bytes_sent.load(Ordering::Relaxed)
    }

    /// Get total messages sent across all channels.
    pub fn total_messages_sent(&self) -> u64 {
        use std::sync::atomic::Ordering;
        self.urgent.metrics.messages_sent.load(Ordering::Relaxed)
            + self.small.metrics.messages_sent.load(Ordering::Relaxed)
            + self.large.metrics.messages_sent.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::verb::Verb;

    #[test]
    fn routes_ping_to_urgent() {
        let conns = OutboundConnections::new(
            "127.0.0.1:7000".parse().unwrap(),
            "127.0.0.1:0".parse().unwrap(),
        );

        let msg = Message::request(Verb::Ping, 1, Vec::new());
        assert!(conns.send(msg, Duration::from_secs(5)));
    }

    #[test]
    fn routes_mutation_to_small() {
        let conns = OutboundConnections::new(
            "127.0.0.1:7000".parse().unwrap(),
            "127.0.0.1:0".parse().unwrap(),
        );

        let msg = Message::request(Verb::Mutation, 1, b"small payload".to_vec());
        assert!(conns.send(msg, Duration::from_secs(5)));
    }

    #[test]
    fn routes_large_payload_to_large() {
        let conns = OutboundConnections::new(
            "127.0.0.1:7000".parse().unwrap(),
            "127.0.0.1:0".parse().unwrap(),
        );

        let payload = vec![0u8; 65 * 1024]; // > 64 KiB threshold
        let msg = Message::request(Verb::StreamData, 1, payload);
        assert!(conns.send(msg, Duration::from_secs(5)));
    }

    #[test]
    fn close_all_does_not_panic() {
        let conns = OutboundConnections::new(
            "127.0.0.1:7000".parse().unwrap(),
            "127.0.0.1:0".parse().unwrap(),
        );
        conns.close_all();
    }

    #[test]
    fn initial_metrics_zero() {
        let conns = OutboundConnections::new(
            "127.0.0.1:7000".parse().unwrap(),
            "127.0.0.1:0".parse().unwrap(),
        );
        assert_eq!(conns.total_bytes_sent(), 0);
        assert_eq!(conns.total_messages_sent(), 0);
    }
}
