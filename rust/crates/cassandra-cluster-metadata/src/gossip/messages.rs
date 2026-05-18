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
use std::fmt;
use std::net::{IpAddr, Ipv4Addr, Ipv6Addr, SocketAddr};

use serde::{Deserialize, Serialize};

use crate::gossip::{ApplicationState, EndpointState, HeartbeatState, VersionedValue};
use crate::node::Endpoint;

const DEFAULT_PARTITIONER: &str = "org.apache.cassandra.dht.Murmur3Partitioner";
const MIN_IPV4_ENDPOINT_BYTES: usize = 1 + 4 + 2;
const MIN_DIGEST_BYTES: usize = MIN_IPV4_ENDPOINT_BYTES + 4 + 4;
const MIN_ENDPOINT_STATE_BYTES: usize = 4 + 4 + 4;
const MIN_STATE_MAP_ENTRY_BYTES: usize = MIN_IPV4_ENDPOINT_BYTES + MIN_ENDPOINT_STATE_BYTES;
const MIN_APPLICATION_STATE_BYTES: usize = 4 + 2 + 4;

/// A gossip digest summarizes what we know about an endpoint.
///
/// Used during gossiping: "I know endpoint X at generation G, max version V."
/// If the peer has a higher version, it will send us the delta.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
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

/// Binary gossip payload codec following the Java `org.apache.cassandra.gms`
/// serializer layout for modern address-and-port messaging versions.
///
/// Gossip strings are encoded with Java `DataOutput.writeUTF` modified UTF-8,
/// not ordinary UTF-8.
pub struct JavaGossipCodec;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum GossipWireError {
    UnexpectedEof,
    InvalidData(String),
    UnsupportedApplicationState(i32),
}

impl fmt::Display for GossipWireError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnexpectedEof => write!(f, "unexpected end of gossip payload"),
            Self::InvalidData(msg) => write!(f, "invalid gossip payload: {msg}"),
            Self::UnsupportedApplicationState(state) => {
                write!(f, "unsupported gossip application state ordinal: {state}")
            }
        }
    }
}

impl std::error::Error for GossipWireError {}

impl JavaGossipCodec {
    pub fn encode_digest(digest: &GossipDigest) -> Result<Vec<u8>, GossipWireError> {
        let mut out = Vec::new();
        write_digest(&mut out, digest)?;
        Ok(out)
    }

    pub fn decode_digest(input: &[u8]) -> Result<GossipDigest, GossipWireError> {
        let mut cursor = input;
        let digest = read_digest(&mut cursor)?;
        ensure_consumed(cursor)?;
        Ok(digest)
    }

    pub fn encode_syn(syn: &GossipDigestSyn) -> Result<Vec<u8>, GossipWireError> {
        let mut out = Vec::new();
        write_java_utf(&mut out, &syn.cluster_id)?;
        write_java_utf(&mut out, DEFAULT_PARTITIONER)?;
        write_digest_list(&mut out, &syn.digests)?;
        Ok(out)
    }

    pub fn decode_syn(input: &[u8]) -> Result<GossipDigestSyn, GossipWireError> {
        let mut cursor = input;
        let cluster_id = read_java_utf(&mut cursor)?;
        let _partitioner = read_java_utf(&mut cursor)?;
        let digests = read_digest_list(&mut cursor)?;
        ensure_consumed(cursor)?;
        Ok(GossipDigestSyn {
            cluster_id,
            digests,
        })
    }

    pub fn encode_ack(ack: &GossipDigestAck) -> Result<Vec<u8>, GossipWireError> {
        let mut out = Vec::new();
        write_digest_list(&mut out, &ack.stale_digests)?;
        write_state_map(&mut out, &ack.updated_states)?;
        Ok(out)
    }

    pub fn decode_ack(input: &[u8]) -> Result<GossipDigestAck, GossipWireError> {
        let mut cursor = input;
        let stale_digests = read_digest_list(&mut cursor)?;
        let updated_states = read_state_map(&mut cursor)?;
        ensure_consumed(cursor)?;
        Ok(GossipDigestAck {
            stale_digests,
            updated_states,
        })
    }

    pub fn encode_ack2(ack2: &GossipDigestAck2) -> Result<Vec<u8>, GossipWireError> {
        let mut out = Vec::new();
        write_state_map(&mut out, &ack2.updated_states)?;
        Ok(out)
    }

    pub fn decode_ack2(input: &[u8]) -> Result<GossipDigestAck2, GossipWireError> {
        let mut cursor = input;
        let updated_states = read_state_map(&mut cursor)?;
        ensure_consumed(cursor)?;
        Ok(GossipDigestAck2 { updated_states })
    }
}

fn write_digest(out: &mut Vec<u8>, digest: &GossipDigest) -> Result<(), GossipWireError> {
    write_endpoint(out, digest.endpoint)?;
    write_i32(out, checked_i32(digest.generation, "generation")?);
    write_i32(out, checked_i32(digest.max_version, "max version")?);
    Ok(())
}

fn read_digest(input: &mut &[u8]) -> Result<GossipDigest, GossipWireError> {
    let endpoint = read_endpoint(input)?;
    let generation = read_i32(input)? as i64;
    let max_version = read_i32(input)? as i64;
    Ok(GossipDigest {
        endpoint,
        generation,
        max_version,
    })
}

fn write_digest_list(out: &mut Vec<u8>, digests: &[GossipDigest]) -> Result<(), GossipWireError> {
    write_i32(out, checked_i32(digests.len() as i64, "digest count")?);
    for digest in digests {
        write_digest(out, digest)?;
    }
    Ok(())
}

fn read_digest_list(input: &mut &[u8]) -> Result<Vec<GossipDigest>, GossipWireError> {
    let count = read_count(input, "digest count", MIN_DIGEST_BYTES)?;
    let mut digests = Vec::with_capacity(count);
    for _ in 0..count {
        digests.push(read_digest(input)?);
    }
    Ok(digests)
}

fn write_state_map(
    out: &mut Vec<u8>,
    states: &HashMap<Endpoint, EndpointState>,
) -> Result<(), GossipWireError> {
    write_i32(
        out,
        checked_i32(states.len() as i64, "endpoint state count")?,
    );
    let mut entries = states.iter().collect::<Vec<_>>();
    entries.sort_by_key(|(endpoint, _)| endpoint.addr());
    for (endpoint, state) in entries {
        write_endpoint(out, *endpoint)?;
        write_endpoint_state(out, state)?;
    }
    Ok(())
}

fn read_state_map(input: &mut &[u8]) -> Result<HashMap<Endpoint, EndpointState>, GossipWireError> {
    let count = read_count(input, "endpoint state count", MIN_STATE_MAP_ENTRY_BYTES)?;
    let mut states = HashMap::with_capacity(count);
    for _ in 0..count {
        let endpoint = read_endpoint(input)?;
        let state = read_endpoint_state(input)?;
        states.insert(endpoint, state);
    }
    Ok(states)
}

fn write_endpoint_state(out: &mut Vec<u8>, state: &EndpointState) -> Result<(), GossipWireError> {
    write_i32(
        out,
        checked_i32(state.heartbeat.generation, "heartbeat generation")?,
    );
    write_i32(
        out,
        checked_i32(state.heartbeat.version, "heartbeat version")?,
    );

    write_i32(
        out,
        checked_i32(
            state.application_states.len() as i64,
            "application state count",
        )?,
    );
    let mut app_states = state.application_states.iter().collect::<Vec<_>>();
    app_states.sort_by_key(|(state, _)| java_application_state_ordinal(state).unwrap_or(i32::MAX));
    for (app_state, value) in app_states {
        write_i32(out, java_application_state_ordinal(app_state)?);
        write_java_utf(out, &value.value)?;
        write_i32(out, checked_i32(value.version, "versioned value version")?);
    }
    Ok(())
}

fn read_endpoint_state(input: &mut &[u8]) -> Result<EndpointState, GossipWireError> {
    let generation = read_i32(input)? as i64;
    let version = read_i32(input)? as i64;
    let count = read_count(
        input,
        "application state count",
        MIN_APPLICATION_STATE_BYTES,
    )?;
    let mut application_states = std::collections::BTreeMap::new();
    for _ in 0..count {
        let ordinal = read_i32(input)?;
        let key = application_state_from_java_ordinal(ordinal)?;
        let value = read_java_utf(input)?;
        let value_version = read_i32(input)? as i64;
        application_states.insert(key, VersionedValue::new(value_version, value));
    }
    Ok(EndpointState {
        heartbeat: HeartbeatState {
            generation,
            version,
        },
        application_states,
        update_timestamp: None,
    })
}

fn write_endpoint(out: &mut Vec<u8>, endpoint: Endpoint) -> Result<(), GossipWireError> {
    match endpoint.addr() {
        SocketAddr::V4(addr) => {
            out.push(6);
            out.extend_from_slice(&addr.ip().octets());
            write_u16(out, addr.port());
        }
        SocketAddr::V6(addr) => {
            out.push(18);
            out.extend_from_slice(&addr.ip().octets());
            write_u16(out, addr.port());
        }
    }
    Ok(())
}

fn read_endpoint(input: &mut &[u8]) -> Result<Endpoint, GossipWireError> {
    let size = read_u8(input)?;
    let addr = match size {
        6 => {
            let bytes = read_exact(input, 4)?;
            let port = read_u16(input)?;
            SocketAddr::new(
                IpAddr::V4(Ipv4Addr::new(bytes[0], bytes[1], bytes[2], bytes[3])),
                port,
            )
        }
        18 => {
            let bytes = read_exact(input, 16)?;
            let port = read_u16(input)?;
            let mut octets = [0u8; 16];
            octets.copy_from_slice(bytes);
            SocketAddr::new(IpAddr::V6(Ipv6Addr::from(octets)), port)
        }
        other => {
            return Err(GossipWireError::InvalidData(format!(
                "unsupported endpoint size {other}"
            )));
        }
    };
    Ok(Endpoint::new(addr))
}

fn java_application_state_ordinal(state: &ApplicationState) -> Result<i32, GossipWireError> {
    match state {
        ApplicationState::Status => Ok(0),
        ApplicationState::Load => Ok(1),
        ApplicationState::SchemaVersion => Ok(2),
        ApplicationState::Datacenter => Ok(3),
        ApplicationState::Rack => Ok(4),
        ApplicationState::ReleaseVersion => Ok(5),
        ApplicationState::RemovalCoordinator => Ok(6),
        ApplicationState::InternalIp => Ok(7),
        ApplicationState::RpcAddress => Ok(8),
        ApplicationState::X11Padding => Ok(9),
        ApplicationState::Severity => Ok(10),
        ApplicationState::NetVersion => Ok(11),
        ApplicationState::HostId => Ok(12),
        ApplicationState::Tokens => Ok(13),
        ApplicationState::RpcReady => Ok(14),
        ApplicationState::InternalAddressAndPort => Ok(15),
        ApplicationState::NativeAddressAndPort => Ok(16),
        ApplicationState::StatusWithPort => Ok(17),
        ApplicationState::SstableVersions => Ok(18),
        ApplicationState::DiskUsage => Ok(19),
        ApplicationState::IndexStatus => Ok(20),
        ApplicationState::PaddingX1 => Ok(21),
        ApplicationState::PaddingX2 => Ok(22),
        ApplicationState::PaddingX3 => Ok(23),
        ApplicationState::PaddingX4 => Ok(24),
        ApplicationState::PaddingX5 => Ok(25),
        ApplicationState::PaddingX6 => Ok(26),
        ApplicationState::PaddingX7 => Ok(27),
        ApplicationState::PaddingX8 => Ok(28),
        ApplicationState::PaddingX9 => Ok(29),
        ApplicationState::PaddingX10 => Ok(30),
        ApplicationState::TcmEpoch => Err(GossipWireError::UnsupportedApplicationState(-1)),
    }
}

fn application_state_from_java_ordinal(ordinal: i32) -> Result<ApplicationState, GossipWireError> {
    match ordinal {
        0 => Ok(ApplicationState::Status),
        1 => Ok(ApplicationState::Load),
        2 => Ok(ApplicationState::SchemaVersion),
        3 => Ok(ApplicationState::Datacenter),
        4 => Ok(ApplicationState::Rack),
        5 => Ok(ApplicationState::ReleaseVersion),
        6 => Ok(ApplicationState::RemovalCoordinator),
        7 => Ok(ApplicationState::InternalIp),
        8 => Ok(ApplicationState::RpcAddress),
        9 => Ok(ApplicationState::X11Padding),
        10 => Ok(ApplicationState::Severity),
        11 => Ok(ApplicationState::NetVersion),
        12 => Ok(ApplicationState::HostId),
        13 => Ok(ApplicationState::Tokens),
        14 => Ok(ApplicationState::RpcReady),
        15 => Ok(ApplicationState::InternalAddressAndPort),
        16 => Ok(ApplicationState::NativeAddressAndPort),
        17 => Ok(ApplicationState::StatusWithPort),
        18 => Ok(ApplicationState::SstableVersions),
        19 => Ok(ApplicationState::DiskUsage),
        20 => Ok(ApplicationState::IndexStatus),
        21 => Ok(ApplicationState::PaddingX1),
        22 => Ok(ApplicationState::PaddingX2),
        23 => Ok(ApplicationState::PaddingX3),
        24 => Ok(ApplicationState::PaddingX4),
        25 => Ok(ApplicationState::PaddingX5),
        26 => Ok(ApplicationState::PaddingX6),
        27 => Ok(ApplicationState::PaddingX7),
        28 => Ok(ApplicationState::PaddingX8),
        29 => Ok(ApplicationState::PaddingX9),
        30 => Ok(ApplicationState::PaddingX10),
        other => Err(GossipWireError::UnsupportedApplicationState(other)),
    }
}

fn checked_i32(value: i64, field: &str) -> Result<i32, GossipWireError> {
    i32::try_from(value).map_err(|_| {
        GossipWireError::InvalidData(format!("{field} does not fit Java int: {value}"))
    })
}

fn read_count(
    input: &mut &[u8],
    field: &str,
    min_entry_bytes: usize,
) -> Result<usize, GossipWireError> {
    let count = read_i32(input)?;
    if count < 0 {
        return Err(GossipWireError::InvalidData(format!(
            "{field} is negative: {count}"
        )));
    }
    let count = count as usize;
    if min_entry_bytes > 0 && count > input.len() / min_entry_bytes {
        return Err(GossipWireError::InvalidData(format!(
            "{field} exceeds remaining payload: {count} entries for {} bytes",
            input.len()
        )));
    }
    Ok(count)
}

fn write_java_utf(out: &mut Vec<u8>, value: &str) -> Result<(), GossipWireError> {
    let mut bytes = Vec::new();
    for unit in value.encode_utf16() {
        match unit {
            0x0001..=0x007f => bytes.push(unit as u8),
            0x0000..=0x07ff => {
                bytes.push((0xc0 | ((unit >> 6) & 0x1f)) as u8);
                bytes.push((0x80 | (unit & 0x3f)) as u8);
            }
            _ => {
                bytes.push((0xe0 | ((unit >> 12) & 0x0f)) as u8);
                bytes.push((0x80 | ((unit >> 6) & 0x3f)) as u8);
                bytes.push((0x80 | (unit & 0x3f)) as u8);
            }
        }
    }

    if bytes.len() > u16::MAX as usize {
        return Err(GossipWireError::InvalidData(format!(
            "UTF value too long: {} bytes",
            bytes.len()
        )));
    }
    write_u16(out, bytes.len() as u16);
    out.extend_from_slice(&bytes);
    Ok(())
}

fn read_java_utf(input: &mut &[u8]) -> Result<String, GossipWireError> {
    let len = read_u16(input)? as usize;
    let bytes = read_exact(input, len)?;
    let mut units = Vec::with_capacity(len);
    let mut index = 0;
    while index < bytes.len() {
        let first = bytes[index];
        match first >> 4 {
            0x0..=0x7 => {
                units.push(first as u16);
                index += 1;
            }
            0xc | 0xd => {
                if index + 1 >= bytes.len() {
                    return Err(GossipWireError::InvalidData(
                        "truncated modified UTF-8 sequence".to_string(),
                    ));
                }
                let second = bytes[index + 1];
                if (second & 0xc0) != 0x80 {
                    return Err(GossipWireError::InvalidData(
                        "invalid modified UTF-8 continuation byte".to_string(),
                    ));
                }
                units.push((((first & 0x1f) as u16) << 6) | ((second & 0x3f) as u16));
                index += 2;
            }
            0xe => {
                if index + 2 >= bytes.len() {
                    return Err(GossipWireError::InvalidData(
                        "truncated modified UTF-8 sequence".to_string(),
                    ));
                }
                let second = bytes[index + 1];
                let third = bytes[index + 2];
                if (second & 0xc0) != 0x80 || (third & 0xc0) != 0x80 {
                    return Err(GossipWireError::InvalidData(
                        "invalid modified UTF-8 continuation byte".to_string(),
                    ));
                }
                units.push(
                    (((first & 0x0f) as u16) << 12)
                        | (((second & 0x3f) as u16) << 6)
                        | ((third & 0x3f) as u16),
                );
                index += 3;
            }
            _ => {
                return Err(GossipWireError::InvalidData(format!(
                    "invalid modified UTF-8 leading byte: {first:#04x}"
                )));
            }
        }
    }

    String::from_utf16(&units)
        .map_err(|err| GossipWireError::InvalidData(format!("invalid UTF value: {err}")))
}

fn write_i32(out: &mut Vec<u8>, value: i32) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn write_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

fn read_i32(input: &mut &[u8]) -> Result<i32, GossipWireError> {
    let bytes = read_exact(input, 4)?;
    Ok(i32::from_be_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]))
}

fn read_u16(input: &mut &[u8]) -> Result<u16, GossipWireError> {
    let bytes = read_exact(input, 2)?;
    Ok(u16::from_be_bytes([bytes[0], bytes[1]]))
}

fn read_u8(input: &mut &[u8]) -> Result<u8, GossipWireError> {
    Ok(*read_exact(input, 1)?
        .first()
        .ok_or(GossipWireError::UnexpectedEof)?)
}

fn read_exact<'a>(input: &mut &'a [u8], len: usize) -> Result<&'a [u8], GossipWireError> {
    if input.len() < len {
        return Err(GossipWireError::UnexpectedEof);
    }
    let (head, tail) = input.split_at(len);
    *input = tail;
    Ok(head)
}

fn ensure_consumed(input: &[u8]) -> Result<(), GossipWireError> {
    if input.is_empty() {
        Ok(())
    } else {
        Err(GossipWireError::InvalidData(format!(
            "{} trailing bytes",
            input.len()
        )))
    }
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

    #[test]
    fn java_digest_binary_matches_address_and_port_layout() {
        let digest = GossipDigest {
            endpoint: ep(7001),
            generation: 1,
            max_version: 10,
        };

        let bytes = JavaGossipCodec::encode_digest(&digest).unwrap();
        assert_eq!(
            bytes,
            vec![6, 127, 0, 0, 1, 0x1b, 0x59, 0, 0, 0, 1, 0, 0, 0, 10]
        );
        assert_eq!(JavaGossipCodec::decode_digest(&bytes).unwrap(), digest);
    }

    #[test]
    fn java_syn_binary_round_trip() {
        let syn = GossipDigestSyn {
            cluster_id: "test-cluster".to_string(),
            digests: vec![GossipDigest {
                endpoint: ep(7001),
                generation: 1,
                max_version: 10,
            }],
        };

        let bytes = JavaGossipCodec::encode_syn(&syn).unwrap();
        assert!(bytes.starts_with(&[0, 12, b't', b'e', b's', b't']));
        let decoded = JavaGossipCodec::decode_syn(&bytes).unwrap();
        assert_eq!(decoded.cluster_id, syn.cluster_id);
        assert_eq!(decoded.digests, syn.digests);
    }

    #[test]
    fn java_ack_and_ack2_binary_round_trip_endpoint_states() {
        let mut state = EndpointState::new(7);
        state.heartbeat.version = 8;
        state.set_state(
            ApplicationState::StatusWithPort,
            VersionedValue::new(9, "NORMAL,7001"),
        );
        state.set_state(ApplicationState::Datacenter, VersionedValue::new(10, "dc1"));
        state.set_state(ApplicationState::RpcReady, VersionedValue::new(11, "true"));
        state.set_state(
            ApplicationState::InternalAddressAndPort,
            VersionedValue::new(12, "127.0.0.1:7001"),
        );

        let mut states = HashMap::new();
        states.insert(ep(7001), state);

        let ack = GossipDigestAck {
            stale_digests: vec![GossipDigest {
                endpoint: ep(7002),
                generation: 2,
                max_version: 3,
            }],
            updated_states: states.clone(),
        };
        let decoded_ack =
            JavaGossipCodec::decode_ack(&JavaGossipCodec::encode_ack(&ack).unwrap()).unwrap();
        assert_eq!(decoded_ack.stale_digests, ack.stale_digests);
        let decoded_state = decoded_ack.updated_states.get(&ep(7001)).unwrap();
        assert_eq!(decoded_state.heartbeat.generation, 7);
        assert_eq!(
            decoded_state
                .get_state(&ApplicationState::StatusWithPort)
                .unwrap()
                .value,
            "NORMAL,7001"
        );
        assert_eq!(
            decoded_state
                .get_state(&ApplicationState::RpcReady)
                .unwrap()
                .value,
            "true"
        );

        let ack2 = GossipDigestAck2 {
            updated_states: states,
        };
        let decoded_ack2 =
            JavaGossipCodec::decode_ack2(&JavaGossipCodec::encode_ack2(&ack2).unwrap()).unwrap();
        assert!(decoded_ack2.updated_states.contains_key(&ep(7001)));
    }

    #[test]
    fn java_utf_uses_modified_utf8_for_gossip_strings() {
        let value = "dc\0\u{1f600}";
        let mut bytes = Vec::new();
        write_java_utf(&mut bytes, value).unwrap();

        assert_eq!(
            bytes,
            vec![
                0, 10, b'd', b'c', 0xc0, 0x80, 0xed, 0xa0, 0xbd, 0xed, 0xb8, 0x80
            ]
        );

        assert_eq!(read_java_utf(&mut bytes.as_slice()).unwrap(), value);
    }

    #[test]
    fn java_utf_rejects_malformed_modified_utf8() {
        let truncated_three_byte = [0, 2, 0xed, 0xa0];
        assert!(matches!(
            read_java_utf(&mut truncated_three_byte.as_slice()),
            Err(GossipWireError::InvalidData(_))
        ));

        let invalid_continuation = [0, 2, 0xc0, b'a'];
        assert!(matches!(
            read_java_utf(&mut invalid_continuation.as_slice()),
            Err(GossipWireError::InvalidData(_))
        ));

        let unpaired_surrogate = [0, 3, 0xed, 0xa0, 0xbd];
        assert!(matches!(
            read_java_utf(&mut unpaired_surrogate.as_slice()),
            Err(GossipWireError::InvalidData(_))
        ));
    }

    #[test]
    fn java_gossip_rejects_impossible_counts_before_allocation() {
        let mut digest_list = Vec::new();
        write_i32(&mut digest_list, i32::MAX);
        assert!(matches!(
            read_digest_list(&mut digest_list.as_slice()),
            Err(GossipWireError::InvalidData(_))
        ));

        let mut state_map = Vec::new();
        write_i32(&mut state_map, i32::MAX);
        assert!(matches!(
            read_state_map(&mut state_map.as_slice()),
            Err(GossipWireError::InvalidData(_))
        ));

        let mut endpoint_state = Vec::new();
        write_i32(&mut endpoint_state, 1);
        write_i32(&mut endpoint_state, 2);
        write_i32(&mut endpoint_state, i32::MAX);
        assert!(matches!(
            read_endpoint_state(&mut endpoint_state.as_slice()),
            Err(GossipWireError::InvalidData(_))
        ));
    }

    #[test]
    fn java_application_state_ordinals_cover_current_java_enum() {
        let states = [
            (ApplicationState::Status, 0),
            (ApplicationState::Load, 1),
            (ApplicationState::SchemaVersion, 2),
            (ApplicationState::Datacenter, 3),
            (ApplicationState::Rack, 4),
            (ApplicationState::ReleaseVersion, 5),
            (ApplicationState::RemovalCoordinator, 6),
            (ApplicationState::InternalIp, 7),
            (ApplicationState::RpcAddress, 8),
            (ApplicationState::X11Padding, 9),
            (ApplicationState::Severity, 10),
            (ApplicationState::NetVersion, 11),
            (ApplicationState::HostId, 12),
            (ApplicationState::Tokens, 13),
            (ApplicationState::RpcReady, 14),
            (ApplicationState::InternalAddressAndPort, 15),
            (ApplicationState::NativeAddressAndPort, 16),
            (ApplicationState::StatusWithPort, 17),
            (ApplicationState::SstableVersions, 18),
            (ApplicationState::DiskUsage, 19),
            (ApplicationState::IndexStatus, 20),
            (ApplicationState::PaddingX1, 21),
            (ApplicationState::PaddingX2, 22),
            (ApplicationState::PaddingX3, 23),
            (ApplicationState::PaddingX4, 24),
            (ApplicationState::PaddingX5, 25),
            (ApplicationState::PaddingX6, 26),
            (ApplicationState::PaddingX7, 27),
            (ApplicationState::PaddingX8, 28),
            (ApplicationState::PaddingX9, 29),
            (ApplicationState::PaddingX10, 30),
        ];

        for (state, ordinal) in states {
            assert_eq!(java_application_state_ordinal(&state).unwrap(), ordinal);
            assert_eq!(application_state_from_java_ordinal(ordinal).unwrap(), state);
        }
    }

    #[test]
    fn java_gossip_rejects_unknown_state_ordinal() {
        let mut bytes = Vec::new();
        write_i32(&mut bytes, 1);
        write_i32(&mut bytes, 2);
        write_i32(&mut bytes, 1);
        write_i32(&mut bytes, 99);
        write_java_utf(&mut bytes, "value").unwrap();
        write_i32(&mut bytes, 3);

        assert_eq!(
            read_endpoint_state(&mut bytes.as_slice()).unwrap_err(),
            GossipWireError::UnsupportedApplicationState(99)
        );
    }
}
