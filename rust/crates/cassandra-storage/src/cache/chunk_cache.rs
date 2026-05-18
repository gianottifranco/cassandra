// Licensed under Apache License, Version 2.0.

//! Content-addressable file chunk cache for SSTable reads.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cache.ChunkCache`
//!
//! Caches raw byte chunks from SSTable data files, keyed by
//! `(SSTableId, file_offset)`. Unlike the key cache (entry-count LRU),
//! the chunk cache enforces a **byte budget** — evicting LRU entries
//! until the total cached bytes fit within `max_size_bytes`.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use lru::LruCache;
use parking_lot::Mutex;

use crate::sstable::format::SSTableId;

use super::CacheStats;

/// Composite key: SSTable generation + file offset of the chunk.
pub type ChunkCacheKey = (SSTableId, u64);

/// Configuration for the chunk cache.
#[derive(Debug, Clone)]
pub struct ChunkCacheConfig {
    /// Maximum total bytes of cached chunk data.
    pub max_size_bytes: usize,
}

impl Default for ChunkCacheConfig {
    fn default() -> Self {
        Self {
            max_size_bytes: 32 * 1024 * 1024, // 32 MB
        }
    }
}

/// Inner mutable state protected by a mutex.
struct Inner {
    lru: LruCache<ChunkCacheKey, Arc<Vec<u8>>>,
    current_bytes: usize,
    max_size_bytes: usize,
}

/// Thread-safe, byte-budget LRU chunk cache.
///
/// Wraps an `lru::LruCache` but manages eviction by total byte size
/// rather than entry count. Safe to share across reader threads via `Arc`.
pub struct ChunkCache {
    inner: Mutex<Inner>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

impl ChunkCache {
    /// Create a new chunk cache with the given configuration.
    pub fn new(config: ChunkCacheConfig) -> Arc<Self> {
        // Use an unbounded LruCache since we manage eviction by byte
        // budget, not entry count.
        let lru = LruCache::unbounded();
        Arc::new(Self {
            inner: Mutex::new(Inner {
                lru,
                current_bytes: 0,
                max_size_bytes: config.max_size_bytes,
            }),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        })
    }

    /// Look up a cached chunk. Updates LRU order on hit.
    pub fn get(&self, sstable_id: SSTableId, offset: u64) -> Option<Arc<Vec<u8>>> {
        let key = (sstable_id, offset);
        let mut inner = self.inner.lock();
        match inner.lru.get(&key) {
            Some(data) => {
                let data = Arc::clone(data);
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(data)
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Insert a chunk into the cache, evicting LRU entries as needed to
    /// stay within the byte budget.
    ///
    /// If the chunk itself exceeds `max_size_bytes`, it is not cached.
    pub fn put(&self, sstable_id: SSTableId, offset: u64, data: Arc<Vec<u8>>) {
        let chunk_len = data.len();
        let key = (sstable_id, offset);
        let mut inner = self.inner.lock();

        // Skip caching if a single chunk exceeds the entire budget.
        if chunk_len > inner.max_size_bytes {
            return;
        }

        // If key already exists, remove the old entry's bytes first.
        if let Some(old) = inner.lru.pop(&key) {
            inner.current_bytes = inner.current_bytes.saturating_sub(old.len());
        }

        // Evict LRU entries until there is room.
        while inner.current_bytes + chunk_len > inner.max_size_bytes {
            if let Some((_evicted_key, evicted_val)) = inner.lru.pop_lru() {
                inner.current_bytes = inner.current_bytes.saturating_sub(evicted_val.len());
                self.evictions.fetch_add(1, Ordering::Relaxed);
            } else {
                break;
            }
        }

        inner.lru.put(key, data);
        inner.current_bytes += chunk_len;
    }

    /// Remove all cached entries for a given SSTable (e.g. after compaction).
    pub fn invalidate_sstable(&self, sstable_id: SSTableId) {
        let mut inner = self.inner.lock();
        // Collect keys to remove (cannot mutate while iterating).
        let keys_to_remove: Vec<ChunkCacheKey> = inner
            .lru
            .iter()
            .filter(|&(&k, _)| k.0 == sstable_id)
            .map(|(&k, _)| k)
            .collect();

        for key in keys_to_remove {
            if let Some(val) = inner.lru.pop(&key) {
                inner.current_bytes = inner.current_bytes.saturating_sub(val.len());
            }
        }
    }

    /// Clear all entries and reset byte tracking.
    pub fn clear(&self) {
        let mut inner = self.inner.lock();
        inner.lru.clear();
        inner.current_bytes = 0;
    }

    /// Snapshot of cache statistics.
    ///
    /// The `size` field reports current byte usage (not entry count).
    pub fn stats(&self) -> CacheStats {
        let inner = self.inner.lock();
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            size: inner.current_bytes,
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }

    /// Current total bytes of cached chunk data.
    pub fn size_bytes(&self) -> usize {
        self.inner.lock().current_bytes
    }

    /// Number of cached entries.
    pub fn entry_count(&self) -> usize {
        self.inner.lock().lru.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_get_put() {
        let cache = ChunkCache::new(ChunkCacheConfig {
            max_size_bytes: 1024,
        });
        let data = Arc::new(vec![1u8, 2, 3, 4]);
        cache.put(1, 0, Arc::clone(&data));

        let result = cache.get(1, 0);
        assert!(result.is_some());
        assert_eq!(*result.unwrap(), vec![1u8, 2, 3, 4]);
    }

    #[test]
    fn cache_miss() {
        let cache = ChunkCache::new(ChunkCacheConfig {
            max_size_bytes: 1024,
        });
        assert!(cache.get(1, 0).is_none());
        assert!(cache.get(42, 999).is_none());
    }

    #[test]
    fn byte_budget_eviction() {
        let cache = ChunkCache::new(ChunkCacheConfig {
            max_size_bytes: 100,
        });

        // Put a 60-byte chunk for SSTable 1
        let chunk_a = Arc::new(vec![0xAA; 60]);
        cache.put(1, 0, chunk_a);
        assert_eq!(cache.size_bytes(), 60);
        assert_eq!(cache.entry_count(), 1);

        // Put another 60-byte chunk for SSTable 2 — should evict first
        let chunk_b = Arc::new(vec![0xBB; 60]);
        cache.put(2, 0, chunk_b);
        assert_eq!(cache.size_bytes(), 60);
        assert_eq!(cache.entry_count(), 1);

        // First chunk should be evicted
        assert!(cache.get(1, 0).is_none());
        // Second chunk should be present
        assert!(cache.get(2, 0).is_some());
    }

    #[test]
    fn invalidate_sstable() {
        let cache = ChunkCache::new(ChunkCacheConfig {
            max_size_bytes: 1024,
        });

        // Put chunks for two SSTables
        cache.put(1, 0, Arc::new(vec![1; 10]));
        cache.put(1, 100, Arc::new(vec![2; 10]));
        cache.put(2, 0, Arc::new(vec![3; 10]));

        assert_eq!(cache.entry_count(), 3);
        assert_eq!(cache.size_bytes(), 30);

        // Invalidate SSTable 1
        cache.invalidate_sstable(1);

        assert_eq!(cache.entry_count(), 1);
        assert_eq!(cache.size_bytes(), 10);
        assert!(cache.get(1, 0).is_none());
        assert!(cache.get(1, 100).is_none());
        assert!(cache.get(2, 0).is_some());
    }

    #[test]
    fn stats_tracking() {
        let cache = ChunkCache::new(ChunkCacheConfig {
            max_size_bytes: 100,
        });

        // Put a 30-byte chunk
        cache.put(1, 0, Arc::new(vec![0; 30]));

        // One hit
        cache.get(1, 0);
        // One miss
        cache.get(1, 999);

        // Put a 80-byte chunk — should evict the first (30 + 80 > 100)
        cache.put(2, 0, Arc::new(vec![0; 80]));

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.evictions, 1);
        assert_eq!(stats.size, 80); // byte usage, not entry count
    }

    #[test]
    fn large_chunk_exceeds_budget() {
        let cache = ChunkCache::new(ChunkCacheConfig {
            max_size_bytes: 100,
        });

        // Pre-populate with a small chunk
        cache.put(1, 0, Arc::new(vec![0; 50]));

        // Try to insert a chunk larger than the budget
        cache.put(2, 0, Arc::new(vec![0; 200]));

        // The oversized chunk should not be cached
        assert!(cache.get(2, 0).is_none());

        // The existing chunk should still be present (not evicted for nothing)
        assert!(cache.get(1, 0).is_some());
        assert_eq!(cache.size_bytes(), 50);
    }

    #[test]
    fn clear_resets_bytes() {
        let cache = ChunkCache::new(ChunkCacheConfig {
            max_size_bytes: 1024,
        });

        cache.put(1, 0, Arc::new(vec![0; 100]));
        cache.put(1, 100, Arc::new(vec![0; 200]));
        assert_eq!(cache.size_bytes(), 300);
        assert_eq!(cache.entry_count(), 2);

        cache.clear();

        assert_eq!(cache.size_bytes(), 0);
        assert_eq!(cache.entry_count(), 0);
        assert!(cache.get(1, 0).is_none());
    }
}
