// Licensed under Apache License, Version 2.0.

//! Counter cell value cache.
//!
//! Caches the latest counter context bytes so that short-read repairs and
//! counter-increment paths can skip disk I/O for hot counters.
//!
//! Java Oracle: `org.apache.cassandra.cache.CounterCacheKey`

use std::num::NonZeroUsize;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use lru::LruCache;
use parking_lot::Mutex;

use super::CacheStats;

/// Cache key: `(table_hash, partition_key, cell_path)`.
pub type CounterCacheKey = (u64, Vec<u8>, Vec<u8>);

/// Configuration for [`CounterCache`].
#[derive(Debug, Clone)]
pub struct CounterCacheConfig {
    /// Maximum number of entries the cache will hold before evicting.
    pub max_entries: usize,
}

impl Default for CounterCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 50_000,
        }
    }
}

/// Thread-safe LRU cache for counter cell values.
///
/// The cache maps `(table_hash, partition_key, cell_path)` to the serialized
/// counter context bytes (`Vec<u8>`).
pub struct CounterCache {
    inner: Mutex<LruCache<CounterCacheKey, Vec<u8>>>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

impl CounterCache {
    /// Create a new `CounterCache` wrapped in an [`Arc`].
    pub fn new(config: CounterCacheConfig) -> Arc<Self> {
        let cap = NonZeroUsize::new(config.max_entries).expect("max_entries must be > 0");
        Arc::new(Self {
            inner: Mutex::new(LruCache::new(cap)),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        })
    }

    /// Look up a counter value. Updates hit/miss statistics.
    pub fn get(
        &self,
        table_hash: u64,
        partition_key: &[u8],
        cell_path: &[u8],
    ) -> Option<Vec<u8>> {
        let key = (table_hash, partition_key.to_vec(), cell_path.to_vec());
        let mut cache = self.inner.lock();
        match cache.get(&key) {
            Some(v) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(v.clone())
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Insert or update a counter value. Tracks evictions when the cache is full.
    pub fn put(
        &self,
        table_hash: u64,
        partition_key: Vec<u8>,
        cell_path: Vec<u8>,
        value: Vec<u8>,
    ) {
        let key = (table_hash, partition_key, cell_path);
        let mut cache = self.inner.lock();
        let was_full = cache.len() == cache.cap().get();
        let evicted = cache.push(key, value);
        if was_full && evicted.is_some() {
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Remove a specific entry from the cache.
    pub fn invalidate(&self, table_hash: u64, partition_key: &[u8], cell_path: &[u8]) {
        let key = (table_hash, partition_key.to_vec(), cell_path.to_vec());
        let mut cache = self.inner.lock();
        cache.pop(&key);
    }

    /// Remove **all** entries belonging to `table_hash`.
    pub fn invalidate_table(&self, table_hash: u64) {
        let mut cache = self.inner.lock();
        let keys_to_remove: Vec<CounterCacheKey> = cache
            .iter()
            .filter(|((th, _, _), _)| *th == table_hash)
            .map(|(k, _)| k.clone())
            .collect();
        for k in keys_to_remove {
            cache.pop(&k);
        }
    }

    /// Remove all entries.
    pub fn clear(&self) {
        let mut cache = self.inner.lock();
        cache.clear();
    }

    /// Return a snapshot of cache statistics.
    pub fn stats(&self) -> CacheStats {
        let cache = self.inner.lock();
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            size: cache.len(),
            evictions: self.evictions.load(Ordering::Relaxed),
        }
    }

    /// Current number of entries.
    pub fn size(&self) -> usize {
        self.inner.lock().len()
    }

    /// Drain all entries for persistence.
    pub fn entries(&self) -> Vec<(CounterCacheKey, Vec<u8>)> {
        let cache = self.inner.lock();
        cache.iter().map(|(k, v)| (k.clone(), v.clone())).collect()
    }

    /// Bulk-insert entries (e.g. loaded from disk on startup).
    pub fn load_entries(&self, entries: Vec<(CounterCacheKey, Vec<u8>)>) {
        let mut cache = self.inner.lock();
        for ((th, pk, cp), value) in entries {
            cache.push((th, pk, cp), value);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small_cache(cap: usize) -> Arc<CounterCache> {
        CounterCache::new(CounterCacheConfig {
            max_entries: cap,
        })
    }

    #[test]
    fn basic_get_put() {
        let cache = small_cache(10);
        cache.put(1, b"pk1".to_vec(), b"cp1".to_vec(), b"val1".to_vec());
        let val = cache.get(1, b"pk1", b"cp1");
        assert_eq!(val, Some(b"val1".to_vec()));
    }

    #[test]
    fn cache_miss() {
        let cache = small_cache(10);
        assert_eq!(cache.get(1, b"pk1", b"cp1"), None);
    }

    #[test]
    fn lru_eviction() {
        let cache = small_cache(2);
        cache.put(1, b"pk1".to_vec(), b"cp1".to_vec(), b"v1".to_vec());
        cache.put(2, b"pk2".to_vec(), b"cp2".to_vec(), b"v2".to_vec());
        // This should evict (1, pk1, cp1)
        cache.put(3, b"pk3".to_vec(), b"cp3".to_vec(), b"v3".to_vec());

        assert_eq!(cache.get(1, b"pk1", b"cp1"), None, "oldest entry should be evicted");
        assert_eq!(cache.get(2, b"pk2", b"cp2"), Some(b"v2".to_vec()));
        assert_eq!(cache.get(3, b"pk3", b"cp3"), Some(b"v3".to_vec()));
    }

    #[test]
    fn invalidate_entry() {
        let cache = small_cache(10);
        cache.put(1, b"pk1".to_vec(), b"cp1".to_vec(), b"val".to_vec());
        cache.invalidate(1, b"pk1", b"cp1");
        assert_eq!(cache.get(1, b"pk1", b"cp1"), None);
    }

    #[test]
    fn invalidate_table() {
        let cache = small_cache(10);
        cache.put(1, b"pk1".to_vec(), b"cp1".to_vec(), b"v1".to_vec());
        cache.put(1, b"pk2".to_vec(), b"cp2".to_vec(), b"v2".to_vec());
        cache.put(2, b"pk3".to_vec(), b"cp3".to_vec(), b"v3".to_vec());

        cache.invalidate_table(1);

        assert_eq!(cache.get(1, b"pk1", b"cp1"), None);
        assert_eq!(cache.get(1, b"pk2", b"cp2"), None);
        assert_eq!(cache.get(2, b"pk3", b"cp3"), Some(b"v3".to_vec()));
    }

    #[test]
    fn stats_tracking() {
        let cache = small_cache(2);
        // 1 miss
        cache.get(1, b"pk1", b"cp1");
        // 2 puts, then 1 eviction
        cache.put(1, b"pk1".to_vec(), b"cp1".to_vec(), b"v1".to_vec());
        cache.put(2, b"pk2".to_vec(), b"cp2".to_vec(), b"v2".to_vec());
        cache.put(3, b"pk3".to_vec(), b"cp3".to_vec(), b"v3".to_vec()); // evicts pk1
        // 1 hit
        cache.get(2, b"pk2", b"cp2");
        // 1 miss (pk1 was evicted)
        cache.get(1, b"pk1", b"cp1");

        let s = cache.stats();
        assert_eq!(s.hits, 1);
        assert_eq!(s.misses, 2);
        assert_eq!(s.evictions, 1);
        assert_eq!(s.size, 2);
        assert!((s.hit_rate() - 1.0 / 3.0).abs() < 1e-9);
    }

    #[test]
    fn entries_and_load() {
        let cache = small_cache(10);
        cache.put(1, b"pk1".to_vec(), b"cp1".to_vec(), b"v1".to_vec());
        cache.put(2, b"pk2".to_vec(), b"cp2".to_vec(), b"v2".to_vec());

        let entries = cache.entries();
        assert_eq!(entries.len(), 2);

        cache.clear();
        assert_eq!(cache.size(), 0);

        cache.load_entries(entries);
        assert_eq!(cache.size(), 2);
        assert_eq!(cache.get(1, b"pk1", b"cp1"), Some(b"v1".to_vec()));
        assert_eq!(cache.get(2, b"pk2", b"cp2"), Some(b"v2".to_vec()));
    }
}
