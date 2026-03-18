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

//! Modern 2-phase internode handshake protocol.
//!
//! ## Protocol
//!
//! 1. **Initiate**: sender writes PROTOCOL_MAGIC + flags (connection_type,
//!    compression, framing, versions) + sender SocketAddr + CRC32
//! 2. **Accept**: receiver writes max_version + use_version + CRC32
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.net.OutboundConnectionInitiator`
//! - `org.apache.cassandra.net.InboundConnectionInitiator`
//! - `org.apache.cassandra.net.HandshakeProtocol`

use std::net::SocketAddr;

use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

use crate::connection_type::ConnectionType;
use crate::crc::crc32c;
use crate::frame::{CURRENT_MESSAGING_VERSION, MIN_MESSAGING_VERSION};

/// Protocol magic bytes identifying a Cassandra internode connection.
pub const PROTOCOL_MAGIC: u32 = 0xCA55_AD12;

/// Handshake flags bit layout.
mod flag_bits {
    /// Bits 0-1: connection type (0=Urgent, 1=Small, 2=Large).
    pub const CONN_TYPE_MASK: u32 = 0x03;
    /// Bit 2: LZ4 compression enabled.
    pub const COMPRESSION_LZ4: u32 = 0x04;
    /// Bit 3: CRC framing enabled.
    pub const FRAMING_CRC: u32 = 0x08;
    /// Bits 8-15: sender's max version.
    pub const VERSION_SHIFT: u32 = 8;
    /// Bits 16-23: sender's min version.
    pub const MIN_VERSION_SHIFT: u32 = 16;
}

/// Errors during handshake.
#[derive(Debug, thiserror::Error)]
pub enum HandshakeError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Bad protocol magic: expected {PROTOCOL_MAGIC:#010X}, got {got:#010X}")]
    BadMagic { got: u32 },

    #[error("CRC mismatch in handshake")]
    CrcMismatch,

    #[error(
        "Version mismatch: local [{min_local}..{max_local}], remote [{min_remote}..{max_remote}]"
    )]
    VersionMismatch {
        min_local: i32,
        max_local: i32,
        min_remote: i32,
        max_remote: i32,
    },

    #[error("Invalid handshake data")]
    InvalidData,
}

/// Result of a successful outbound handshake.
#[derive(Debug, Clone)]
pub struct HandshakeResult {
    /// Negotiated protocol version.
    pub version: i32,
    /// Connection type for this channel.
    pub connection_type: ConnectionType,
    /// Whether LZ4 compression is enabled.
    pub compression: bool,
    /// Whether CRC framing is enabled.
    pub crc_framing: bool,
}

/// Initiate message sent by the outbound side.
#[derive(Debug, Clone)]
pub struct InitiateMessage {
    pub connection_type: ConnectionType,
    pub compression: bool,
    pub crc_framing: bool,
    pub max_version: i32,
    pub min_version: i32,
    pub sender_addr: SocketAddr,
}

/// Accept message sent by the inbound side.
#[derive(Debug, Clone)]
pub struct AcceptMessage {
    pub max_version: i32,
    pub use_version: i32,
}

/// Perform the outbound side of the handshake.
///
/// Writes the Initiate message and reads the Accept response.
pub async fn perform_outbound_handshake<S>(
    stream: &mut S,
    connection_type: ConnectionType,
    compression: bool,
    crc_framing: bool,
    sender_addr: SocketAddr,
) -> Result<HandshakeResult, HandshakeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Build flags
    let conn_type_bits = match connection_type {
        ConnectionType::Urgent => 0u32,
        ConnectionType::Small => 1u32,
        ConnectionType::Large => 2u32,
    };
    let mut flags = conn_type_bits & flag_bits::CONN_TYPE_MASK;
    if compression {
        flags |= flag_bits::COMPRESSION_LZ4;
    }
    if crc_framing {
        flags |= flag_bits::FRAMING_CRC;
    }
    flags |= (CURRENT_MESSAGING_VERSION as u32) << flag_bits::VERSION_SHIFT;
    flags |= (MIN_MESSAGING_VERSION as u32) << flag_bits::MIN_VERSION_SHIFT;

    // Encode sender address
    let addr_bytes = encode_addr(&sender_addr);

    // Build initiate payload: magic(4) + flags(4) + addr_len(2) + addr + crc32(4)
    let mut payload = Vec::with_capacity(4 + 4 + 2 + addr_bytes.len());
    payload.extend_from_slice(&PROTOCOL_MAGIC.to_be_bytes());
    payload.extend_from_slice(&flags.to_be_bytes());
    payload.extend_from_slice(&(addr_bytes.len() as u16).to_be_bytes());
    payload.extend_from_slice(&addr_bytes);

    let crc = crc32c(&payload);
    payload.extend_from_slice(&crc.to_be_bytes());

    stream.write_all(&payload).await?;
    stream.flush().await?;

    // Read accept: max_version(4) + use_version(4) + crc32(4)
    let mut accept_buf = [0u8; 12];
    stream.read_exact(&mut accept_buf).await?;

    let accept_crc = crc32c(&accept_buf[..8]);
    let stored_crc =
        u32::from_be_bytes([accept_buf[8], accept_buf[9], accept_buf[10], accept_buf[11]]);
    if accept_crc != stored_crc {
        return Err(HandshakeError::CrcMismatch);
    }

    let max_version =
        i32::from_be_bytes([accept_buf[0], accept_buf[1], accept_buf[2], accept_buf[3]]);
    let use_version =
        i32::from_be_bytes([accept_buf[4], accept_buf[5], accept_buf[6], accept_buf[7]]);

    if use_version < MIN_MESSAGING_VERSION || use_version > CURRENT_MESSAGING_VERSION {
        return Err(HandshakeError::VersionMismatch {
            min_local: MIN_MESSAGING_VERSION,
            max_local: CURRENT_MESSAGING_VERSION,
            min_remote: use_version,
            max_remote: max_version,
        });
    }

    Ok(HandshakeResult {
        version: use_version,
        connection_type,
        compression,
        crc_framing,
    })
}

/// Accept the inbound side of the handshake.
///
/// Reads the Initiate message and writes the Accept response.
pub async fn accept_inbound_handshake<S>(
    stream: &mut S,
) -> Result<(HandshakeResult, SocketAddr), HandshakeError>
where
    S: AsyncRead + AsyncWrite + Unpin,
{
    // Read magic(4) + flags(4)
    let mut header = [0u8; 8];
    stream.read_exact(&mut header).await?;

    let magic = u32::from_be_bytes([header[0], header[1], header[2], header[3]]);
    if magic != PROTOCOL_MAGIC {
        return Err(HandshakeError::BadMagic { got: magic });
    }

    let flags = u32::from_be_bytes([header[4], header[5], header[6], header[7]]);

    // Read addr_len(2)
    let mut addr_len_buf = [0u8; 2];
    stream.read_exact(&mut addr_len_buf).await?;
    let addr_len = u16::from_be_bytes(addr_len_buf) as usize;

    // Read addr bytes
    let mut addr_bytes = vec![0u8; addr_len];
    stream.read_exact(&mut addr_bytes).await?;

    // Read CRC32(4)
    let mut crc_buf = [0u8; 4];
    stream.read_exact(&mut crc_buf).await?;
    let stored_crc = u32::from_be_bytes(crc_buf);

    // Verify CRC over magic + flags + addr_len + addr
    let mut check_data = Vec::with_capacity(8 + 2 + addr_len);
    check_data.extend_from_slice(&header);
    check_data.extend_from_slice(&addr_len_buf);
    check_data.extend_from_slice(&addr_bytes);
    let computed_crc = crc32c(&check_data);
    if stored_crc != computed_crc {
        return Err(HandshakeError::CrcMismatch);
    }

    // Parse flags
    let conn_type = match flags & flag_bits::CONN_TYPE_MASK {
        0 => ConnectionType::Urgent,
        1 => ConnectionType::Small,
        2 => ConnectionType::Large,
        _ => return Err(HandshakeError::InvalidData),
    };
    let compression = flags & flag_bits::COMPRESSION_LZ4 != 0;
    let crc_framing = flags & flag_bits::FRAMING_CRC != 0;
    let remote_max = ((flags >> flag_bits::VERSION_SHIFT) & 0xFF) as i32;
    let remote_min = ((flags >> flag_bits::MIN_VERSION_SHIFT) & 0xFF) as i32;

    // Parse sender address
    let sender_addr = decode_addr(&addr_bytes).ok_or(HandshakeError::InvalidData)?;

    // Negotiate version
    let use_version = CURRENT_MESSAGING_VERSION.min(remote_max);
    if use_version < MIN_MESSAGING_VERSION || use_version < remote_min {
        return Err(HandshakeError::VersionMismatch {
            min_local: MIN_MESSAGING_VERSION,
            max_local: CURRENT_MESSAGING_VERSION,
            min_remote: remote_min,
            max_remote: remote_max,
        });
    }

    // Send accept: max_version(4) + use_version(4) + crc32(4)
    let mut accept = [0u8; 12];
    accept[0..4].copy_from_slice(&CURRENT_MESSAGING_VERSION.to_be_bytes());
    accept[4..8].copy_from_slice(&use_version.to_be_bytes());
    let accept_crc = crc32c(&accept[..8]);
    accept[8..12].copy_from_slice(&accept_crc.to_be_bytes());

    stream.write_all(&accept).await?;
    stream.flush().await?;

    Ok((
        HandshakeResult {
            version: use_version,
            connection_type: conn_type,
            compression,
            crc_framing,
        },
        sender_addr,
    ))
}

fn encode_addr(addr: &SocketAddr) -> Vec<u8> {
    match addr {
        SocketAddr::V4(a) => {
            let mut buf = Vec::with_capacity(7);
            buf.push(4);
            buf.extend_from_slice(&a.ip().octets());
            buf.extend_from_slice(&a.port().to_be_bytes());
            buf
        }
        SocketAddr::V6(a) => {
            let mut buf = Vec::with_capacity(19);
            buf.push(6);
            buf.extend_from_slice(&a.ip().octets());
            buf.extend_from_slice(&a.port().to_be_bytes());
            buf
        }
    }
}

fn decode_addr(data: &[u8]) -> Option<SocketAddr> {
    if data.is_empty() {
        return None;
    }
    match data[0] {
        4 if data.len() >= 7 => {
            let ip = std::net::Ipv4Addr::new(data[1], data[2], data[3], data[4]);
            let port = u16::from_be_bytes([data[5], data[6]]);
            Some(SocketAddr::from((ip, port)))
        }
        6 if data.len() >= 19 => {
            let mut octets = [0u8; 16];
            octets.copy_from_slice(&data[1..17]);
            let ip = std::net::Ipv6Addr::from(octets);
            let port = u16::from_be_bytes([data[17], data[18]]);
            Some(SocketAddr::from((ip, port)))
        }
        _ => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn handshake_round_trip() {
        let (mut client, mut server) = tokio::io::duplex(1024);

        let sender_addr: SocketAddr = "192.168.1.1:7000".parse().unwrap();

        let (client_result, server_result) = tokio::join!(
            perform_outbound_handshake(&mut client, ConnectionType::Small, true, true, sender_addr,),
            accept_inbound_handshake(&mut server),
        );

        let client_hs = client_result.unwrap();
        let (server_hs, peer_addr) = server_result.unwrap();

        assert_eq!(client_hs.version, CURRENT_MESSAGING_VERSION);
        assert_eq!(server_hs.version, CURRENT_MESSAGING_VERSION);
        assert_eq!(client_hs.connection_type, ConnectionType::Small);
        assert_eq!(server_hs.connection_type, ConnectionType::Small);
        assert!(client_hs.compression);
        assert!(server_hs.compression);
        assert!(client_hs.crc_framing);
        assert!(server_hs.crc_framing);
        assert_eq!(peer_addr, sender_addr);
    }

    #[tokio::test]
    async fn handshake_urgent_no_compression() {
        let (client, server) = tokio::io::duplex(1024);
        let mut client = client;
        let mut server = server;

        let sender_addr: SocketAddr = "10.0.0.5:9042".parse().unwrap();

        let (client_result, server_result) = tokio::join!(
            perform_outbound_handshake(
                &mut client,
                ConnectionType::Urgent,
                false,
                false,
                sender_addr,
            ),
            accept_inbound_handshake(&mut server),
        );

        let client_hs = client_result.unwrap();
        let (server_hs, peer_addr) = server_result.unwrap();

        assert_eq!(client_hs.connection_type, ConnectionType::Urgent);
        assert_eq!(server_hs.connection_type, ConnectionType::Urgent);
        assert!(!client_hs.compression);
        assert!(!server_hs.compression);
        assert_eq!(peer_addr, sender_addr);
    }

    #[tokio::test]
    async fn handshake_bad_magic_rejected() {
        let (mut client, mut server) = tokio::io::duplex(1024);

        // Write bad magic manually
        let bad_magic = 0xDEADBEEFu32;
        client.write_all(&bad_magic.to_be_bytes()).await.unwrap();
        client.write_all(&[0u8; 4]).await.unwrap(); // flags
        client.write_all(&[0u8; 2]).await.unwrap(); // addr_len = 0
        client.write_all(&[0u8; 4]).await.unwrap(); // crc (wrong but won't matter)

        let result = accept_inbound_handshake(&mut server).await;
        assert!(matches!(result, Err(HandshakeError::BadMagic { .. })));
    }

    #[test]
    fn addr_encode_decode_v4() {
        let addr: SocketAddr = "127.0.0.1:7000".parse().unwrap();
        let bytes = encode_addr(&addr);
        let decoded = decode_addr(&bytes).unwrap();
        assert_eq!(decoded, addr);
    }

    #[test]
    fn addr_encode_decode_v6() {
        let addr: SocketAddr = "[::1]:7000".parse().unwrap();
        let bytes = encode_addr(&addr);
        let decoded = decode_addr(&bytes).unwrap();
        assert_eq!(decoded, addr);
    }
}
