// Licensed under Apache License, Version 2.0.

//! # Stream Transport Layer
//!
//! Wraps [`MessagingService`] for streaming-specific send operations with
//! LZ4 compression, CRC32 verification, and rate limiting.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.streaming.async.StreamingInboundHandler`
//! - `org.apache.cassandra.streaming.async.NettyStreamingMessageSender`

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use cassandra_messaging::{MessagingError, MessagingService};

use crate::protocol::*;
use crate::transfer::{ChunkChecksum, StreamRateLimiter};

const INIT_TIMEOUT: Duration = Duration::from_secs(30);
const COMPLETE_TIMEOUT: Duration = Duration::from_secs(60);

/// Errors from the transport layer.
#[derive(Debug, thiserror::Error)]
pub enum StreamTransportError {
    #[error("messaging error: {0}")]
    Messaging(#[from] MessagingError),

    #[error("protocol error: {0}")]
    Protocol(#[from] StreamProtocolError),

    #[error("checksum verification failed for chunk seq={0}")]
    ChecksumMismatch(u64),

    #[error("stream init rejected: {0}")]
    InitRejected(String),

    #[error("decompression error: {0}")]
    Decompression(String),
}

/// Streaming transport wrapping the messaging service.
pub struct StreamTransport {
    messaging: Arc<MessagingService>,
    rate_limiter: StreamRateLimiter,
}

impl StreamTransport {
    pub fn new(messaging: Arc<MessagingService>, rate_limiter: StreamRateLimiter) -> Self {
        Self {
            messaging,
            rate_limiter,
        }
    }

    /// Send a StreamInit and wait for the response.
    pub async fn send_init(
        &self,
        peer: SocketAddr,
        msg: StreamInitMessage,
    ) -> Result<StreamInitResponseMessage, StreamTransportError> {
        let msg_id = self.messaging.next_id();
        let wire = StreamMessage::Init(msg).to_message(msg_id)?;
        let resp = self
            .messaging
            .send_and_wait(peer, wire, INIT_TIMEOUT)
            .await?;
        match StreamMessage::from_message(&resp)? {
            StreamMessage::InitResponse(r) => Ok(r),
            other => Err(StreamTransportError::Protocol(
                StreamProtocolError::UnexpectedVerb(other.verb()),
            )),
        }
    }

    /// Send a data chunk (fire-and-forget on the large channel).
    /// Applies rate limiting before sending.
    pub async fn send_data(
        &self,
        peer: SocketAddr,
        msg: StreamDataMessage,
    ) -> Result<(), StreamTransportError> {
        let wait = self.rate_limiter.acquire(msg.data.len() as u64);
        if !wait.is_zero() {
            tokio::time::sleep(wait).await;
        }
        let msg_id = self.messaging.next_id();
        let wire = StreamMessage::Data(msg).to_message(msg_id)?;
        self.messaging.send(peer, wire).await?;
        Ok(())
    }

    /// Send a StreamComplete and wait for acknowledgement.
    pub async fn send_complete(
        &self,
        peer: SocketAddr,
        msg: StreamCompleteMessage,
    ) -> Result<StreamCompleteResponseMessage, StreamTransportError> {
        let msg_id = self.messaging.next_id();
        let wire = StreamMessage::Complete(msg).to_message(msg_id)?;
        let resp = self
            .messaging
            .send_and_wait(peer, wire, COMPLETE_TIMEOUT)
            .await?;
        match StreamMessage::from_message(&resp)? {
            StreamMessage::CompleteResponse(r) => Ok(r),
            other => Err(StreamTransportError::Protocol(
                StreamProtocolError::UnexpectedVerb(other.verb()),
            )),
        }
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// LZ4 compression helpers
// ─────────────────────────────────────────────────────────────────────────────

/// Compress data with LZ4 (prepends decompressed size).
pub fn compress(data: &[u8]) -> Vec<u8> {
    lz4_flex::compress_prepend_size(data)
}

/// Decompress LZ4 data.
pub fn decompress(data: &[u8]) -> Result<Vec<u8>, StreamTransportError> {
    lz4_flex::decompress_size_prepended(data)
        .map_err(|e| StreamTransportError::Decompression(e.to_string()))
}

/// Verify a data chunk's checksum against its payload.
pub fn verify_chunk_checksum(data: &[u8], checksum: &[u8; 16], algorithm: &str) -> bool {
    let algo = match algorithm {
        "Crc32" => crate::transfer::ChecksumAlgorithm::Crc32,
        "Md5" => crate::transfer::ChecksumAlgorithm::Md5,
        _ => return false,
    };
    let expected = ChunkChecksum {
        algorithm: algo,
        hash: *checksum,
    };
    expected.verify(data)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn compression_round_trip() {
        let data = b"hello world streaming data that should compress well \
                     hello world streaming data that should compress well";
        let compressed = compress(data);
        let decompressed = decompress(&compressed).unwrap();
        assert_eq!(&decompressed, data);
    }

    #[test]
    fn compression_empty() {
        let compressed = compress(b"");
        let decompressed = decompress(&compressed).unwrap();
        assert!(decompressed.is_empty());
    }

    #[test]
    fn decompression_invalid_data() {
        let result = decompress(b"not valid lz4");
        assert!(result.is_err());
    }

    #[test]
    fn checksum_verify_crc32() {
        let data = b"test data for checksum";
        let checksum = ChunkChecksum::compute(data, crate::transfer::ChecksumAlgorithm::Crc32);
        assert!(verify_chunk_checksum(data, &checksum.hash, "Crc32"));
        assert!(!verify_chunk_checksum(
            b"wrong data",
            &checksum.hash,
            "Crc32"
        ));
    }

    #[test]
    fn rate_limiter_sleep_calculation() {
        let limiter = StreamRateLimiter::new(1000); // 1000 bytes/sec
        // First acquire should be immediate (bucket starts full)
        let wait = limiter.acquire(500);
        assert!(wait.is_zero() || wait < Duration::from_millis(1));
    }
}
