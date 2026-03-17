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

//! Forwarding info for internode request forwarding.
//!
//! When a coordinator forwards a request to another node, it attaches
//! forwarding metadata so the target can send the response back through
//! the correct path.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.net.ForwardingInfo`
//! - `org.apache.cassandra.net.Message` (forwarding fields)

use std::net::SocketAddr;

/// A single hop in the forwarding chain.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardingHop {
    /// The address of the node that forwarded the message.
    pub from: SocketAddr,
    /// The message ID on the forwarding node (for response correlation).
    pub message_id: u64,
}

/// Forwarding metadata attached to forwarded messages.
///
/// Contains the chain of hops the message has taken, allowing the
/// final target to send the response back through the original path.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ForwardingInfo {
    /// The chain of forwarding hops (most recent last).
    pub hops: Vec<ForwardingHop>,
}

impl ForwardingInfo {
    /// Create forwarding info with a single hop.
    pub fn single(from: SocketAddr, message_id: u64) -> Self {
        Self {
            hops: vec![ForwardingHop { from, message_id }],
        }
    }

    /// Add a forwarding hop.
    pub fn add_hop(&mut self, from: SocketAddr, message_id: u64) {
        self.hops.push(ForwardingHop { from, message_id });
    }

    /// The original sender (first hop in the chain).
    pub fn origin(&self) -> Option<&ForwardingHop> {
        self.hops.first()
    }

    /// The most recent forwarder (last hop in the chain).
    pub fn last_forwarder(&self) -> Option<&ForwardingHop> {
        self.hops.last()
    }

    /// Number of hops in the forwarding chain.
    pub fn hop_count(&self) -> usize {
        self.hops.len()
    }

    /// Encode to bytes for wire transfer.
    ///
    /// Format: [hop_count: u16] [foreach hop: ip_version(u8) + addr + port(u16) + msg_id(u64)]
    pub fn to_bytes(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(2 + self.hops.len() * 15);
        buf.extend_from_slice(&(self.hops.len() as u16).to_be_bytes());

        for hop in &self.hops {
            match hop.from {
                SocketAddr::V4(addr) => {
                    buf.push(4); // IPv4
                    buf.extend_from_slice(&addr.ip().octets());
                    buf.extend_from_slice(&addr.port().to_be_bytes());
                }
                SocketAddr::V6(addr) => {
                    buf.push(6); // IPv6
                    buf.extend_from_slice(&addr.ip().octets());
                    buf.extend_from_slice(&addr.port().to_be_bytes());
                }
            }
            buf.extend_from_slice(&hop.message_id.to_be_bytes());
        }
        buf
    }

    /// Decode from bytes.
    pub fn from_bytes(data: &[u8]) -> Option<Self> {
        if data.len() < 2 {
            return None;
        }
        let hop_count = u16::from_be_bytes([data[0], data[1]]) as usize;
        let mut offset = 2;
        let mut hops = Vec::with_capacity(hop_count);

        for _ in 0..hop_count {
            if offset >= data.len() {
                return None;
            }
            let ip_version = data[offset];
            offset += 1;

            let addr = match ip_version {
                4 => {
                    if offset + 6 > data.len() {
                        return None;
                    }
                    let ip = std::net::Ipv4Addr::new(
                        data[offset],
                        data[offset + 1],
                        data[offset + 2],
                        data[offset + 3],
                    );
                    let port =
                        u16::from_be_bytes([data[offset + 4], data[offset + 5]]);
                    offset += 6;
                    SocketAddr::from((ip, port))
                }
                6 => {
                    if offset + 18 > data.len() {
                        return None;
                    }
                    let mut octets = [0u8; 16];
                    octets.copy_from_slice(&data[offset..offset + 16]);
                    let ip = std::net::Ipv6Addr::from(octets);
                    let port = u16::from_be_bytes([
                        data[offset + 16],
                        data[offset + 17],
                    ]);
                    offset += 18;
                    SocketAddr::from((ip, port))
                }
                _ => return None,
            };

            if offset + 8 > data.len() {
                return None;
            }
            let message_id = u64::from_be_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
                data[offset + 4],
                data[offset + 5],
                data[offset + 6],
                data[offset + 7],
            ]);
            offset += 8;

            hops.push(ForwardingHop { from: addr, message_id });
        }

        Some(Self { hops })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_hop_round_trip() {
        let addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let info = ForwardingInfo::single(addr, 42);

        let bytes = info.to_bytes();
        let decoded = ForwardingInfo::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, info);
    }

    #[test]
    fn multi_hop_round_trip() {
        let mut info = ForwardingInfo::single("10.0.0.1:7000".parse().unwrap(), 1);
        info.add_hop("10.0.0.2:7000".parse().unwrap(), 2);
        info.add_hop("10.0.0.3:7000".parse().unwrap(), 3);

        let bytes = info.to_bytes();
        let decoded = ForwardingInfo::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, info);
        assert_eq!(decoded.hop_count(), 3);
    }

    #[test]
    fn ipv6_round_trip() {
        let addr: SocketAddr = "[::1]:7000".parse().unwrap();
        let info = ForwardingInfo::single(addr, 99);

        let bytes = info.to_bytes();
        let decoded = ForwardingInfo::from_bytes(&bytes).unwrap();
        assert_eq!(decoded, info);
    }

    #[test]
    fn origin_and_last_forwarder() {
        let mut info = ForwardingInfo::single("10.0.0.1:7000".parse().unwrap(), 1);
        info.add_hop("10.0.0.2:7000".parse().unwrap(), 2);

        let origin = info.origin().unwrap();
        assert_eq!(origin.from, "10.0.0.1:7000".parse::<SocketAddr>().unwrap());
        assert_eq!(origin.message_id, 1);

        let last = info.last_forwarder().unwrap();
        assert_eq!(last.from, "10.0.0.2:7000".parse::<SocketAddr>().unwrap());
        assert_eq!(last.message_id, 2);
    }

    #[test]
    fn empty_bytes_returns_none() {
        assert!(ForwardingInfo::from_bytes(&[]).is_none());
    }

    #[test]
    fn truncated_bytes_returns_none() {
        assert!(ForwardingInfo::from_bytes(&[0, 1, 4]).is_none());
    }

    #[test]
    fn invalid_ip_version_returns_none() {
        // hop_count=1, ip_version=9 (invalid)
        assert!(ForwardingInfo::from_bytes(&[0, 1, 9]).is_none());
    }
}
