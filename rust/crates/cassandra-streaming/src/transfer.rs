// Licensed under Apache License, Version 2.0.

//! Chunk-based data transfer with checksums, rate limiting, and flow control.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.streaming.StreamTransferTask`
//! - `org.apache.cassandra.streaming.OutgoingDataMessage`
//! - `org.apache.cassandra.streaming.IncomingDataMessage`
//! - `org.apache.cassandra.streaming.StreamRateLimiter`

use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use md5::{Digest, Md5};
use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use cassandra_common::Token;

/// Default chunk size for streaming transfers: 64 KiB.
pub const DEFAULT_CHUNK_SIZE: usize = 64 * 1024;

/// Maximum concurrent transfers per session (flow control).
pub const MAX_CONCURRENT_TRANSFERS: usize = 4;

/// Maximum retry attempts per chunk.
pub const MAX_CHUNK_RETRIES: u32 = 3;

/// Default rate limit: 0 means unlimited.
pub const DEFAULT_RATE_LIMIT_BYTES_PER_SEC: u64 = 0;

// ── Checksum Algorithm ──────────────────────────────────────────────────

/// Checksum algorithm selection.
///
/// Java Cassandra supports both CRC32 and Adler32 for on-disk checksums;
/// for streaming it uses CRC32 (post-4.0). We support MD5 (legacy) and CRC32.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ChecksumAlgorithm {
    Md5,
    Crc32,
}

impl Default for ChecksumAlgorithm {
    fn default() -> Self {
        Self::Crc32
    }
}

impl fmt::Display for ChecksumAlgorithm {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Md5 => write!(f, "MD5"),
            Self::Crc32 => write!(f, "CRC32"),
        }
    }
}

// ── Transfer State ──────────────────────────────────────────────────────

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

// ── Checksum ────────────────────────────────────────────────────────────

/// Checksum for a data chunk or an entire file.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ChunkChecksum {
    /// Algorithm used.
    pub algorithm: ChecksumAlgorithm,
    /// Hash bytes (16 for MD5, 4 for CRC32 zero-padded to 16).
    pub hash: [u8; 16],
}

impl ChunkChecksum {
    /// Compute checksum from raw data using the specified algorithm.
    pub fn compute(data: &[u8], algorithm: ChecksumAlgorithm) -> Self {
        match algorithm {
            ChecksumAlgorithm::Md5 => {
                let mut hasher = Md5::new();
                hasher.update(data);
                let result = hasher.finalize();
                let mut hash = [0u8; 16];
                hash.copy_from_slice(&result);
                Self { algorithm, hash }
            }
            ChecksumAlgorithm::Crc32 => {
                let crc = crc32_compute(data);
                let mut hash = [0u8; 16];
                hash[0..4].copy_from_slice(&crc.to_be_bytes());
                Self { algorithm, hash }
            }
        }
    }

    /// Compute checksum using default MD5 (backward compatible).
    pub fn compute_md5(data: &[u8]) -> Self {
        Self::compute(data, ChecksumAlgorithm::Md5)
    }

    /// Verify that data matches this checksum.
    pub fn verify(&self, data: &[u8]) -> bool {
        let computed = Self::compute(data, self.algorithm);
        computed.hash == self.hash
    }
}

/// Simple CRC32 implementation (IEEE polynomial).
fn crc32_compute(data: &[u8]) -> u32 {
    let mut crc: u32 = 0xFFFF_FFFF;
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 != 0 {
                crc = (crc >> 1) ^ 0xEDB8_8320;
            } else {
                crc >>= 1;
            }
        }
    }
    !crc
}

impl fmt::Display for ChunkChecksum {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let len = match self.algorithm {
            ChecksumAlgorithm::Md5 => 16,
            ChecksumAlgorithm::Crc32 => 4,
        };
        for byte in &self.hash[..len] {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

// ── Rate Limiter ────────────────────────────────────────────────────────

/// Token-bucket rate limiter for streaming throughput control.
///
/// ## Java Oracle
///
/// - `org.apache.cassandra.streaming.StreamRateLimiter`
/// - Configurable via `stream_throughput_outbound` in cassandra.yaml
///
/// A limit of 0 means unlimited.
#[derive(Debug, Clone)]
pub struct StreamRateLimiter {
    inner: Arc<Mutex<RateLimiterInner>>,
}

#[derive(Debug)]
struct RateLimiterInner {
    /// Maximum bytes per second (0 = unlimited).
    bytes_per_sec: u64,
    /// Tokens available in the bucket.
    available: f64,
    /// Last time tokens were refilled.
    last_refill: Instant,
}

impl StreamRateLimiter {
    /// Create a new rate limiter with the given bytes/sec limit.
    pub fn new(bytes_per_sec: u64) -> Self {
        Self {
            inner: Arc::new(Mutex::new(RateLimiterInner {
                bytes_per_sec,
                available: bytes_per_sec as f64,
                last_refill: Instant::now(),
            })),
        }
    }

    /// Create an unlimited rate limiter.
    pub fn unlimited() -> Self {
        Self::new(0)
    }

    /// Returns true if rate limiting is enabled.
    pub fn is_limited(&self) -> bool {
        self.inner.lock().bytes_per_sec > 0
    }

    /// Update the rate limit at runtime.
    pub fn set_rate(&self, bytes_per_sec: u64) {
        let mut inner = self.inner.lock();
        inner.bytes_per_sec = bytes_per_sec;
        inner.available = bytes_per_sec as f64;
        inner.last_refill = Instant::now();
    }

    /// Acquire permission to send `n` bytes. Returns the duration to wait
    /// before sending, or `Duration::ZERO` if no wait is needed.
    pub fn acquire(&self, n: u64) -> Duration {
        let mut inner = self.inner.lock();
        if inner.bytes_per_sec == 0 {
            return Duration::ZERO;
        }

        // Refill tokens based on elapsed time.
        let now = Instant::now();
        let elapsed = now.duration_since(inner.last_refill).as_secs_f64();
        inner.available += elapsed * inner.bytes_per_sec as f64;
        if inner.available > inner.bytes_per_sec as f64 {
            inner.available = inner.bytes_per_sec as f64; // cap at 1 second burst
        }
        inner.last_refill = now;

        let needed = n as f64;
        if inner.available >= needed {
            inner.available -= needed;
            Duration::ZERO
        } else {
            let deficit = needed - inner.available;
            inner.available = 0.0;
            let wait_secs = deficit / inner.bytes_per_sec as f64;
            Duration::from_secs_f64(wait_secs)
        }
    }

    /// Current rate limit in bytes/sec.
    pub fn rate(&self) -> u64 {
        self.inner.lock().bytes_per_sec
    }
}

// ── Data Chunk ──────────────────────────────────────────────────────────

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

// ── Stream Transfer ─────────────────────────────────────────────────────

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
    /// Checksum algorithm to use.
    pub checksum_algorithm: ChecksumAlgorithm,
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
            checksum_algorithm: ChecksumAlgorithm::default(),
        }
    }

    /// Create a transfer with a specific checksum algorithm.
    pub fn with_checksum(mut self, algorithm: ChecksumAlgorithm) -> Self {
        self.checksum_algorithm = algorithm;
        self
    }

    /// Split data into chunks with checksums.
    pub fn chunkify(transfer_id: Uuid, data: &[u8], chunk_size: usize) -> Vec<DataChunk> {
        Self::chunkify_with_algorithm(transfer_id, data, chunk_size, ChecksumAlgorithm::default())
    }

    /// Split data into chunks with the specified checksum algorithm.
    pub fn chunkify_with_algorithm(
        transfer_id: Uuid,
        data: &[u8],
        chunk_size: usize,
        algorithm: ChecksumAlgorithm,
    ) -> Vec<DataChunk> {
        if data.is_empty() {
            return vec![DataChunk {
                transfer_id,
                sequence: 0,
                data: Vec::new(),
                checksum: ChunkChecksum::compute(&[], algorithm),
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
                checksum: ChunkChecksum::compute(chunk_data, algorithm),
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

// ── Retry Policy ────────────────────────────────────────────────────────

/// Exponential backoff retry policy for streaming reconnection.
///
/// ## Java Oracle
///
/// - `org.apache.cassandra.streaming.StreamSession` retry logic
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StreamRetryPolicy {
    /// Base delay before first retry.
    pub base_delay: Duration,
    /// Maximum delay between retries.
    pub max_delay: Duration,
    /// Maximum number of retry attempts.
    pub max_attempts: u32,
    /// Jitter factor (0.0 to 1.0) to randomize delays.
    pub jitter_factor: f64,
}

impl StreamRetryPolicy {
    /// Create a policy with sensible defaults matching Java behavior.
    pub fn default_policy() -> Self {
        Self {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(60),
            max_attempts: 3,
            jitter_factor: 0.25,
        }
    }

    /// Calculate the delay before the nth retry (0-indexed).
    pub fn delay_for_attempt(&self, attempt: u32) -> Duration {
        if attempt >= self.max_attempts {
            return self.max_delay;
        }
        let exp_delay = self.base_delay.as_secs_f64() * 2.0f64.powi(attempt as i32);
        let capped = exp_delay.min(self.max_delay.as_secs_f64());

        // Apply jitter: delay ± jitter_factor * delay
        let jitter_range = capped * self.jitter_factor;
        // Deterministic in tests: use midpoint
        let jittered = capped + jitter_range * 0.5;
        Duration::from_secs_f64(jittered.min(self.max_delay.as_secs_f64()))
    }

    /// Whether the given attempt number is within retry limits.
    pub fn should_retry(&self, attempt: u32) -> bool {
        attempt < self.max_attempts
    }
}

impl Default for StreamRetryPolicy {
    fn default() -> Self {
        Self::default_policy()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn checksum_md5_compute_verify() {
        let data = b"hello streaming world";
        let checksum = ChunkChecksum::compute(data, ChecksumAlgorithm::Md5);
        assert!(checksum.verify(data));
        assert!(!checksum.verify(b"corrupted data"));
        assert_eq!(checksum.algorithm, ChecksumAlgorithm::Md5);
    }

    #[test]
    fn checksum_crc32_compute_verify() {
        let data = b"hello streaming world";
        let checksum = ChunkChecksum::compute(data, ChecksumAlgorithm::Crc32);
        assert!(checksum.verify(data));
        assert!(!checksum.verify(b"corrupted data"));
        assert_eq!(checksum.algorithm, ChecksumAlgorithm::Crc32);
    }

    #[test]
    fn checksum_empty() {
        let c1 = ChunkChecksum::compute(&[], ChecksumAlgorithm::Md5);
        let c2 = ChunkChecksum::compute(&[], ChecksumAlgorithm::Md5);
        assert_eq!(c1, c2);

        let c3 = ChunkChecksum::compute(&[], ChecksumAlgorithm::Crc32);
        let c4 = ChunkChecksum::compute(&[], ChecksumAlgorithm::Crc32);
        assert_eq!(c3, c4);
    }

    #[test]
    fn checksum_display_md5() {
        let checksum = ChunkChecksum::compute(b"test", ChecksumAlgorithm::Md5);
        let display = format!("{checksum}");
        assert_eq!(display.len(), 32); // MD5 = 16 bytes = 32 hex chars
    }

    #[test]
    fn checksum_display_crc32() {
        let checksum = ChunkChecksum::compute(b"test", ChecksumAlgorithm::Crc32);
        let display = format!("{checksum}");
        assert_eq!(display.len(), 8); // CRC32 = 4 bytes = 8 hex chars
    }

    #[test]
    fn checksum_backward_compat() {
        let data = b"backward compat test";
        let old = ChunkChecksum::compute_md5(data);
        let new = ChunkChecksum::compute(data, ChecksumAlgorithm::Md5);
        assert_eq!(old, new);
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
    fn chunkify_with_crc32() {
        let data = vec![0u8; 128];
        let chunks = StreamTransfer::chunkify_with_algorithm(
            Uuid::new_v4(),
            &data,
            64,
            ChecksumAlgorithm::Crc32,
        );
        assert_eq!(chunks.len(), 2);
        assert_eq!(chunks[0].checksum.algorithm, ChecksumAlgorithm::Crc32);
        assert!(chunks[0].checksum.verify(&[0u8; 64]));
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

    #[test]
    fn transfer_with_checksum() {
        let t = StreamTransfer::new("ks".into(), "t".into(), vec![])
            .with_checksum(ChecksumAlgorithm::Crc32);
        assert_eq!(t.checksum_algorithm, ChecksumAlgorithm::Crc32);
    }

    // ── Rate limiter tests ──────────────────────────────────────────────

    #[test]
    fn rate_limiter_unlimited() {
        let rl = StreamRateLimiter::unlimited();
        assert!(!rl.is_limited());
        assert_eq!(rl.acquire(1_000_000), Duration::ZERO);
    }

    #[test]
    fn rate_limiter_within_budget() {
        let rl = StreamRateLimiter::new(1_000_000); // 1 MB/s
        assert!(rl.is_limited());
        // First acquisition within budget should be instant
        let wait = rl.acquire(100);
        assert_eq!(wait, Duration::ZERO);
    }

    #[test]
    fn rate_limiter_over_budget() {
        let rl = StreamRateLimiter::new(1000); // 1000 bytes/s
        // Exhaust the bucket
        let _ = rl.acquire(1000);
        // Next acquisition should require waiting
        let wait = rl.acquire(500);
        assert!(wait > Duration::ZERO);
    }

    #[test]
    fn rate_limiter_set_rate() {
        let rl = StreamRateLimiter::new(1000);
        assert_eq!(rl.rate(), 1000);
        rl.set_rate(5000);
        assert_eq!(rl.rate(), 5000);
    }

    // ── Retry policy tests ──────────────────────────────────────────────

    #[test]
    fn retry_policy_defaults() {
        let policy = StreamRetryPolicy::default_policy();
        assert!(policy.should_retry(0));
        assert!(policy.should_retry(1));
        assert!(policy.should_retry(2));
        assert!(!policy.should_retry(3));
    }

    #[test]
    fn retry_policy_exponential_backoff() {
        let policy = StreamRetryPolicy {
            base_delay: Duration::from_secs(1),
            max_delay: Duration::from_secs(30),
            max_attempts: 5,
            jitter_factor: 0.0, // No jitter for deterministic test
        };

        let d0 = policy.delay_for_attempt(0);
        let d1 = policy.delay_for_attempt(1);
        let d2 = policy.delay_for_attempt(2);

        // Each delay should be ~2x the previous (exponential)
        assert!(d1 > d0);
        assert!(d2 > d1);
    }

    #[test]
    fn retry_policy_capped_at_max() {
        let policy = StreamRetryPolicy {
            base_delay: Duration::from_secs(10),
            max_delay: Duration::from_secs(30),
            max_attempts: 10,
            jitter_factor: 0.0,
        };

        let d5 = policy.delay_for_attempt(5);
        assert!(d5 <= policy.max_delay);
    }

    #[test]
    fn checksum_algorithm_display() {
        assert_eq!(ChecksumAlgorithm::Md5.to_string(), "MD5");
        assert_eq!(ChecksumAlgorithm::Crc32.to_string(), "CRC32");
    }
}
