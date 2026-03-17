// Licensed under Apache License, Version 2.0.

//! Thread-safe buffer pool for reusing heap-allocated byte buffers.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.io.util.BufferPool`

use std::sync::atomic::{AtomicU64, Ordering};

use crossbeam::queue::ArrayQueue;

// ---------------------------------------------------------------------------
// Metrics
// ---------------------------------------------------------------------------

/// Snapshot of pool counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BufferPoolMetrics {
    /// Total fresh allocations (cache miss).
    pub allocations: u64,
    /// Buffers served from the cache.
    pub cache_hits: u64,
    /// Buffers dropped because the pool was full or the buffer had the wrong
    /// capacity.
    pub evictions: u64,
}

// ---------------------------------------------------------------------------
// BufferPool
// ---------------------------------------------------------------------------

/// A lock-free pool of reusable `Vec<u8>` buffers.
///
/// Each buffer has a fixed capacity equal to `chunk_size`.  Buffers whose
/// capacity does not match are silently discarded on [`release`](Self::release).
pub struct BufferPool {
    pool: ArrayQueue<Vec<u8>>,
    chunk_size: usize,
    allocations: AtomicU64,
    cache_hits: AtomicU64,
    evictions: AtomicU64,
}

impl BufferPool {
    /// Creates a new pool.
    ///
    /// * `chunk_size` – capacity of each buffer (bytes).
    /// * `max_cached` – upper bound on pooled buffers.
    pub fn new(chunk_size: usize, max_cached: usize) -> Self {
        Self {
            pool: ArrayQueue::new(max_cached),
            chunk_size,
            allocations: AtomicU64::new(0),
            cache_hits: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }

    /// Obtains a zeroed buffer with capacity `chunk_size`.
    ///
    /// Returns a cached buffer when one is available, otherwise allocates.
    pub fn acquire(&self) -> Vec<u8> {
        if let Some(mut buf) = self.pool.pop() {
            self.cache_hits.fetch_add(1, Ordering::Relaxed);
            buf.clear();
            buf
        } else {
            self.allocations.fetch_add(1, Ordering::Relaxed);
            Vec::with_capacity(self.chunk_size)
        }
    }

    /// Returns a buffer to the pool for later reuse.
    ///
    /// The buffer is silently dropped when:
    /// - its capacity differs from `chunk_size`, or
    /// - the pool is already full.
    pub fn release(&self, buf: Vec<u8>) {
        if buf.capacity() != self.chunk_size {
            self.evictions.fetch_add(1, Ordering::Relaxed);
            return;
        }
        if self.pool.push(buf).is_err() {
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Returns a point-in-time snapshot of the pool metrics.
    pub fn metrics(&self) -> BufferPoolMetrics {
        BufferPoolMetrics {
            allocations: self.allocations.load(Ordering::Relaxed),
            cache_hits: self.cache_hits.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }

    /// The capacity every pooled buffer is expected to have.
    pub fn chunk_size(&self) -> usize {
        self.chunk_size
    }
}

impl Default for BufferPool {
    fn default() -> Self {
        Self::new(65_536, 1024)
    }
}

// We cannot derive Debug because `ArrayQueue` does not implement it.
impl std::fmt::Debug for BufferPool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BufferPool")
            .field("chunk_size", &self.chunk_size)
            .field("cached", &self.pool.len())
            .field("metrics", &self.metrics())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_acquire_returns_buffer_with_correct_capacity() {
        let pool = BufferPool::new(4096, 8);
        let buf = pool.acquire();
        assert_eq!(buf.capacity(), 4096);
        assert!(buf.is_empty());
    }

    #[test]
    fn test_acquire_release_cycle() {
        let pool = BufferPool::new(128, 4);
        let mut buf = pool.acquire();
        buf.extend_from_slice(&[1u8; 64]);
        pool.release(buf);

        // Re-acquired buffer must be cleared.
        let buf2 = pool.acquire();
        assert!(buf2.is_empty());
        assert_eq!(buf2.capacity(), 128);
    }

    #[test]
    fn test_metrics_allocation_and_hit() {
        let pool = BufferPool::new(256, 4);

        // First acquire → fresh allocation.
        let buf = pool.acquire();
        assert_eq!(pool.metrics().allocations, 1);
        assert_eq!(pool.metrics().cache_hits, 0);

        pool.release(buf);

        // Second acquire → cache hit.
        let _buf2 = pool.acquire();
        assert_eq!(pool.metrics().allocations, 1);
        assert_eq!(pool.metrics().cache_hits, 1);
    }

    #[test]
    fn test_pool_exhaustion_evicts() {
        let pool = BufferPool::new(64, 2);
        let b1 = pool.acquire();
        let b2 = pool.acquire();
        let b3 = pool.acquire();

        pool.release(b1);
        pool.release(b2);
        // Pool is full – b3 should be evicted.
        pool.release(b3);

        assert_eq!(pool.metrics().evictions, 1);
    }

    #[test]
    fn test_release_wrong_sized_buffer_is_evicted() {
        let pool = BufferPool::new(128, 4);
        let wrong = Vec::with_capacity(256);
        pool.release(wrong);
        assert_eq!(pool.metrics().evictions, 1);
    }

    #[test]
    fn test_default_values() {
        let pool = BufferPool::default();
        assert_eq!(pool.chunk_size(), 65_536);
        // Allocations start at zero.
        assert_eq!(pool.metrics().allocations, 0);
    }
}
