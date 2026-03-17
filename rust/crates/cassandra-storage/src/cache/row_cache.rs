// Licensed under Apache License, Version 2.0.

//! Row cache: caches full partition data by `(table_hash, partition_key)`.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.cache.RowCacheKey`
//! - `org.apache.cassandra.cache.IRowCacheEntry`

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use lru::LruCache;
use parking_lot::Mutex;

use crate::cache::CacheStats;
use crate::memtable::partition::PartitionData;

/// Cache key: `(table_hash, partition_key)`.
pub type RowCacheKey = (u64, Vec<u8>);

/// Configuration for the row cache.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct RowCacheConfig {
    /// Maximum number of cached partitions. 0 means disabled.
    pub max_entries: usize,
}

impl Default for RowCacheConfig {
    fn default() -> Self {
        Self { max_entries: 0 }
    }
}

/// Thread-safe LRU row cache for full partition data.
pub struct RowCache {
    cache: Mutex<LruCache<RowCacheKey, PartitionData>>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    enabled: bool,
}

impl RowCache {
    /// Create a new `RowCache` wrapped in an `Arc`.
    ///
    /// When `max_entries` is 0 the cache is disabled and all operations
    /// are no-ops (gets always return `None`).
    pub fn new(config: RowCacheConfig) -> Arc<Self> {
        let enabled = config.max_entries > 0;
        let capacity = NonZeroUsize::new(config.max_entries)
            .unwrap_or(NonZeroUsize::new(1).unwrap());

        Arc::new(Self {
            cache: Mutex::new(LruCache::new(capacity)),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            enabled,
        })
    }

    /// Retrieve a cached partition. Returns `None` on miss or if cache is
    /// disabled. Updates hit/miss counters.
    pub fn get(&self, table_hash: u64, partition_key: &[u8]) -> Option<PartitionData> {
        if !self.enabled {
            self.misses.fetch_add(1, Ordering::Relaxed);
            return None;
        }

        let key = (table_hash, partition_key.to_vec());
        let mut guard = self.cache.lock();
        match guard.get(&key) {
            Some(data) => {
                let data = data.clone();
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(data)
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Insert (or update) a partition in the cache. If the cache is at
    /// capacity the least-recently-used entry is evicted.
    pub fn put(&self, table_hash: u64, partition_key: Vec<u8>, data: PartitionData) {
        if !self.enabled {
            return;
        }

        let key = (table_hash, partition_key);
        let mut guard = self.cache.lock();
        if let Some((_evicted_key, _evicted_val)) = guard.push(key, data) {
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Remove a specific partition from the cache.
    pub fn invalidate_partition(&self, table_hash: u64, partition_key: &[u8]) {
        if !self.enabled {
            return;
        }
        let key = (table_hash, partition_key.to_vec());
        let mut guard = self.cache.lock();
        guard.pop(&key);
    }

    /// Remove **all** entries belonging to the given table.
    pub fn invalidate_table(&self, table_hash: u64) {
        if !self.enabled {
            return;
        }
        let mut guard = self.cache.lock();
        let keys_to_remove: Vec<RowCacheKey> = guard
            .iter()
            .filter(|((th, _), _)| *th == table_hash)
            .map(|(k, _)| k.clone())
            .collect();
        for key in keys_to_remove {
            guard.pop(&key);
        }
    }

    /// Clear all entries from the cache.
    pub fn clear(&self) {
        let mut guard = self.cache.lock();
        guard.clear();
    }

    /// Return a snapshot of the cache statistics.
    pub fn stats(&self) -> CacheStats {
        let guard = self.cache.lock();
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            size: guard.len(),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }

    /// Return the current number of cached entries.
    pub fn size(&self) -> usize {
        self.cache.lock().len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_cache(max: usize) -> Arc<RowCache> {
        RowCache::new(RowCacheConfig { max_entries: max })
    }

    #[test]
    fn basic_get_put() {
        let cache = make_cache(16);
        let pd = PartitionData::new();
        cache.put(1, b"pk1".to_vec(), pd.clone());

        let got = cache.get(1, b"pk1").expect("should be cached");
        assert_eq!(got.rows.len(), pd.rows.len());
    }

    #[test]
    fn cache_miss() {
        let cache = make_cache(16);
        assert!(cache.get(1, b"pk_missing").is_none());

        let stats = cache.stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 0);
    }

    #[test]
    fn lru_eviction() {
        let cache = make_cache(2);
        cache.put(1, b"a".to_vec(), PartitionData::new());
        cache.put(1, b"b".to_vec(), PartitionData::new());
        // This should evict "a"
        cache.put(1, b"c".to_vec(), PartitionData::new());

        assert!(cache.get(1, b"a").is_none(), "oldest entry should be evicted");
        assert!(cache.get(1, b"b").is_some());
        assert!(cache.get(1, b"c").is_some());

        let stats = cache.stats();
        assert_eq!(stats.evictions, 1);
    }

    #[test]
    fn invalidate_partition() {
        let cache = make_cache(16);
        cache.put(1, b"pk1".to_vec(), PartitionData::new());
        cache.invalidate_partition(1, b"pk1");
        assert!(cache.get(1, b"pk1").is_none());
    }

    #[test]
    fn invalidate_table() {
        let cache = make_cache(16);
        cache.put(1, b"a".to_vec(), PartitionData::new());
        cache.put(1, b"b".to_vec(), PartitionData::new());
        cache.put(2, b"c".to_vec(), PartitionData::new());

        cache.invalidate_table(1);

        assert!(cache.get(1, b"a").is_none());
        assert!(cache.get(1, b"b").is_none());
        assert!(cache.get(2, b"c").is_some(), "table 2 entries should remain");
    }

    #[test]
    fn stats_tracking() {
        let cache = make_cache(2);
        // 1 miss
        cache.get(1, b"x");
        // 2 puts, then 1 eviction on the 3rd
        cache.put(1, b"a".to_vec(), PartitionData::new());
        cache.put(1, b"b".to_vec(), PartitionData::new());
        cache.put(1, b"c".to_vec(), PartitionData::new()); // evicts "a"
        // 1 hit
        cache.get(1, b"c");

        let stats = cache.stats();
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.evictions, 1);
        assert_eq!(stats.size, 2);
    }

    #[test]
    fn update_existing() {
        let cache = make_cache(16);
        cache.put(1, b"pk1".to_vec(), PartitionData::new());
        cache.put(1, b"pk1".to_vec(), PartitionData::new());

        assert_eq!(cache.size(), 1, "updating same key should not increase size");
    }
}
