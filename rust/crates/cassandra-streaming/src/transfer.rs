// Licensed under Apache License, Version 2.0.

//! Chunk-based data transfer with checksums and flow control.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.streaming.StreamTransferTask`
//! - `org.apache.cassandra.streaming.OutgoingDataMessage`
//! - `org.apache.cassandra.streaming.IncomingDataMessage`

use std::fmt;

use md5::{Digest, Md5};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use cassandra_common::Token;

/// Default chunk size for streaming transfers: 64 KiB.
pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;

/// Maximum concurrent transfers per session (flow control).
pub const MAX_CONCURRENT_TRANSFERS: usize = 4;

/// Maximum retry attempts per chunk.
pub const MAX_CHUNK_RETRIES: u32 = 3;

/// State of an individual transfer.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum TransferState {
    /// Transfer is pending, not yet started.
    Pending,
    /// Transfer is actively sending/receiving chunks.
    InProgress,
    /// Transfer completed successfully.
    Complete,
    /// Transfer failed after retries.
    Failed,
    /// Transfer was cancelled.
    Cancelled,
}

impl fmt::Display for TransferState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Pending => write!(f, "PENDING"),
            Self::InProgress => write!(f, "IN_PROGRESS"),
            Self::Complete => write!(f, "COMPLETE"),
            Self::Failed => write!(f, "FAILED"),
            Self::Cancelled => write!(f, "CANCELLED"),
        }
    }
}

/// MD5 checksum for a data chunk or an entire file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkChecksum {
    /// MD5 hash bytes.
    pub hash: [u8; 16],
}

impl ChunkChecksum {
    /// Compute checksum from raw data.
    pub fn compute(data: &[u8]) -> Self {
        let mut hasher = Md5::new();
        hasher.update(data);
        let result = hasher.finalize();
        let mut hash = [0u8; 16];
        hash.copy_from_slice(&result);
        Self { hash }
    }

    /// Verify that data matches this checksum.
    pub fn verify(&self, data: &[u8]) -> bool {
        let computed = Self::compute(data);
        computed.hash == self.hash
    }
}

impl fmt::Display for ChunkChecksum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in &self.hash {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// A single data chunk being transferred.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataChunk {
    /// Transfer this chunk belongs to.
    pub transfer_id: Uuid,
    /// Sequence number within the transfer (0-indexed).
    pub sequence: u64,
    /// Raw data bytes.
    pub data: Vec<u8>,
    /// Checksum for verification.
    pub checksum: ChunkChecksum,
    /// Whether this is the last chunk of the transfer.
    pub is_last: bool,
}

/// Describes a single transfer of data for a keyspace/table range.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamTransfer {
    /// Unique transfer identifier.
    pub id: Uuid,
    /// Keyspace being transferred.
    pub keyspace: String,
    /// Table being transferred.
    pub table: String,
    /// Token ranges covered by this transfer.
    pub ranges: Vec<(Token, Token)>,
    /// Current state of the transfer.
    pub state: TransferState,
    /// Chunk size for this transfer.
    pub chunk_size: usize,
    /// Total bytes to transfer (may be estimated).
    pub total_bytes: u64,
    /// Bytes transferred so far.
    pub bytes_transferred: u64,
    /// Number of chunks sent/received.
    pub chunks_transferred: u64,
    /// Number of retries so far.
    pub retry_count: u32,
}

impl StreamTransfer {
    /// Create a new transfer descriptor.
    pub fn new(keyspace: String, table: String, ranges: Vec<(Token, Token)>) -> Self {
        Self {
            id: Uuid::new_v4(),
            keyspace,
            table,
            ranges,
            state: TransferState::Pending,
            chunk_size: DEFAULT_CHUNK_SIZE,
            total_bytes: 0,
            bytes_transferred: 0,
            chunks_transferred: 0,
            retry_count: 0,
        }
    }

    /// Split data into chunks with checksums.
    pub fn chunkify(transfer_id: Uuid, data: &[u8], chunk_size: usize) -> Vec<DataChunk> {
        if data.is_empty() {
            return vec![DataChunk {
                transfer_id,
                sequence: 0,
                data: Vec::new(),
                checksum: ChunkChecksum::compute(&[]),
                is_last: true,
            }];
        }

        let mut chunks = Vec::new();
        let total_chunks = (data.len() + chunk_size - 1) / chunk_size;

        for (i, chunk_data) in data.chunks(chunk_size).enumerate() {
            chunks.push(DataChunk {
                transfer_id,
                sequence: i as u64,
                data: chunk_data.to_vec(),
                checksum: ChunkChecksum::compute(chunk_data),
                is_last: i == total_chunks - 1,
            });
        }

        chunks
    }

    /// Record bytes transferred and update progress.
    pub fn record_progress(&mut self, bytes: u64) {
        self.bytes_transferred += bytes;
        self.chunks_transferred += 1;
    }

    /// Fraction of transfer completed (0.0 to 1.0).
    pub fn progress(&self) -> f64 {
        if self.total_bytes == 0 {
            return if self.state == TransferState::Complete {
                1.0
            } else {
                0.0
            };
        }
        self.bytes_transferred as f64 / self.total_bytes as f64
    }

    /// Whether another retry is allowed.
    pub fn can_retry(&self) -> bool {
        self.retry_count < MAX_CHUNK_RETRIES
    }

    /// Increment retry counter.
    pub fn record_retry(&mut self) {
        self.retry_count += 1;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksum_compute_verify() {
        let data = b"hello streaming world";
        let checksum = ChunkChecksum::compute(data);
        assert!(checksum.verify(data));
        assert!(!checksum.verify(b"corrupted data"));
    }

    #[test]
    fn checksum_empty() {
        let c1 = ChunkChecksum::compute(&[]);
        let c2 = ChunkChecksum::compute(&[]);
        assert_eq!(c1, c2);
    }

    #[test]
    fn checksum_display() {
        let checksum = ChunkChecksum::compute(b"test");
        let display = format!("{checksum}");
        assert_eq!(display.len(), 32); // MD5 = 16 bytes = 32 hex chars
    }

    #[test]
    fn chunkify_small_data() {
        let data = b"hello";
        let chunks = StreamTransfer::chunkify(Uuid::new_v4(), data, 64);
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].is_last);
        assert_eq!(chunks[0].data, b"hello");
        assert!(chunks[0].checksum.verify(b"hello"));
    }

    #[test]
    fn chunkify_splits_evenly() {
        let data = vec![0u8; 256];
        let chunks = StreamTransfer::chunkify(Uuid::new_v4(), &data, 64);
        assert_eq!(chunks.len(), 4);
        assert!(!chunks[0].is_last);
        assert!(!chunks[1].is_last);
        assert!(!chunks[2].is_last);
        assert!(chunks[3].is_last);
        for (i, chunk) in chunks.iter().enumerate() {
            assert_eq!(chunk.sequence, i as u64);
            assert_eq!(chunk.data.len(), 64);
        }
    }

    #[test]
    fn chunkify_uneven_last() {
        let data = vec![42u8; 100];
        let chunks = StreamTransfer::chunkify(Uuid::new_v4(), &data, 64);
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].data.len(), 64);
        assert_eq!(chunks[1].data.len(), 36);
        assert!(chunks[1].is_last);
    }

    #[test]
    fn chunkify_empty() {
        let chunks = StreamTransfer::chunkify(Uuid::new_v4(), &[], 64);
        assert_eq!(chunks.len(), 1);
        assert!(chunks[0].is_last);
        assert!(chunks[0].data.is_empty());
    }

    #[test]
    fn transfer_progress() {
        let mut t = StreamTransfer::new("ks".into(), "t".into(), vec![]);
        t.total_bytes = 1000;
        assert_eq!(t.progress(), 0.0);

        t.record_progress(500);
        assert!((t.progress() - 0.5).abs() < f64::EPSILON);

        t.state = TransferState::Complete;
        t.record_progress(500);
        assert!((t.progress() - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn retry_limit() {
        let mut t = StreamTransfer::new("ks".into(), "t".into(), vec![]);
        assert!(t.can_retry());
        t.record_retry();
        t.record_retry();
        t.record_retry();
        assert!(!t.can_retry());
    }

    #[test]
    fn transfer_state_display() {
        assert_eq!(TransferState::Pending.to_string(), "PENDING");
        assert_eq!(TransferState::InProgress.to_string(), "IN_PROGRESS");
        assert_eq!(TransferState::Complete.to_string(), "COMPLETE");
        assert_eq!(TransferState::Failed.to_string(), "FAILED");
        assert_eq!(TransferState::Cancelled.to_string(), "CANCELLED");
    }
}
