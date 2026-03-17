// Licensed under Apache License, Version 2.0.

//! Shared key cache for SSTable partition lookups.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.cache.KeyCacheKey`
//! - `org.apache.cassandra.cache.AutoSavingCache`
//!
//! Maps `(SSTableId, partition_key)` → data file offset so repeated reads
//! skip the index binary search. The cache is LRU-evicted and safe to share
//! across reader threads via `Arc<KeyCache>`.

use std::collections::{HashMap, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::Mutex;

use super::format::SSTableId;

/// Composite key: SSTable generation + raw partition key bytes.
pub type KeyCacheKey = (SSTableId, Vec<u8>);

/// Configuration for the key cache.
#[derive(Debug, Clone)]
pub struct KeyCacheConfig {
    pub max_entries: usize,
}

impl Default for KeyCacheConfig {
    fn default() -> Self {
        Self {
            max_entries: 1_000_000,
        }
    }
}

/// Hit / miss / size statistics.
#[derive(Debug, Clone)]
pub struct KeyCacheStats {
    pub hits: u64,
    pub misses: u64,
    pub size: usize,
}

/// Thread-safe LRU key cache backed by `HashMap` + `VecDeque`.
pub struct KeyCache {
    inner: Mutex<LruMap>,
    hits: AtomicU64,
    misses: AtomicU64,
}

/// Manual LRU: HashMap for O(1) lookup, VecDeque for eviction order.
struct LruMap {
    map: HashMap<KeyCacheKey, u64>,
    order: VecDeque<KeyCacheKey>,
    max_entries: usize,
}

impl LruMap {
    fn new(max_entries: usize) -> Self {
        Self {
            map: HashMap::new(),
            order: VecDeque::new(),
            max_entries,
        }
    }

    fn get(&mut self, key: &KeyCacheKey) -> Option<u64> {
        let value = self.map.get(key).copied()?;
        // Move to back (most recently used)
        self.order.retain(|k| k != key);
        self.order.push_back(key.clone());
        Some(value)
    }

    fn put(&mut self, key: KeyCacheKey, value: u64) {
        if self.map.contains_key(&key) {
            self.order.retain(|k| k != &key);
        } else if self.map.len() >= self.max_entries {
            // Evict least recently used (front)
            if let Some(evicted) = self.order.pop_front() {
                self.map.remove(&evicted);
            }
        }
        self.map.insert(key.clone(), value);
        self.order.push_back(key);
    }

    fn invalidate_sstable(&mut self, sstable_id: SSTableId) {
        self.order.retain(|k| k.0 != sstable_id);
        self.map.retain(|k, _| k.0 != sstable_id);
    }

    fn len(&self) -> usize {
        self.map.len()
    }
}

impl KeyCache {
    /// Create a new key cache with the given configuration.
    pub fn new(config: KeyCacheConfig) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(LruMap::new(config.max_entries)),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
        })
    }

    /// Look up a cached data offset. Updates LRU order on hit.
    pub fn get(&self, sstable_id: SSTableId, key: &[u8]) -> Option<u64> {
        let cache_key = (sstable_id, key.to_vec());
        let mut inner = self.inner.lock();
        match inner.get(&cache_key) {
            Some(offset) => {
                self.hits.fetch_add(1, Ordering::Relaxed);
                Some(offset)
            }
            None => {
                self.misses.fetch_add(1, Ordering::Relaxed);
                None
            }
        }
    }

    /// Insert or update a cache entry.
    pub fn put(&self, sstable_id: SSTableId, key: Vec<u8>, offset: u64) {
        let cache_key = (sstable_id, key);
        self.inner.lock().put(cache_key, offset);
    }

    /// Remove all entries for a given SSTable (e.g. after compaction).
    pub fn invalidate_sstable(&self, sstable_id: SSTableId) {
        self.inner.lock().invalidate_sstable(sstable_id);
    }

    /// Snapshot of cache statistics.
    pub fn stats(&self) -> KeyCacheStats {
        let inner = self.inner.lock();
        KeyCacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            size: inner.len(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn basic_get_put() {
        let cache = KeyCache::new(KeyCacheConfig { max_entries: 100 });
        cache.put(1, b"pk1".to_vec(), 1000);
        assert_eq!(cache.get(1, b"pk1"), Some(1000));
        assert_eq!(cache.get(1, b"pk2"), None);
        assert_eq!(cache.get(2, b"pk1"), None);
    }

    #[test]
    fn lru_eviction() {
        let cache = KeyCache::new(KeyCacheConfig { max_entries: 3 });
        cache.put(1, b"a".to_vec(), 10);
        cache.put(1, b"b".to_vec(), 20);
        cache.put(1, b"c".to_vec(), 30);

        // Access "a" to make it recently used
        assert_eq!(cache.get(1, b"a"), Some(10));

        // Insert "d" — should evict "b" (least recently used)
        cache.put(1, b"d".to_vec(), 40);

        assert_eq!(cache.get(1, b"a"), Some(10));
        assert_eq!(cache.get(1, b"b"), None); // evicted
        assert_eq!(cache.get(1, b"c"), Some(30));
        assert_eq!(cache.get(1, b"d"), Some(40));
    }

    #[test]
    fn invalidate_sstable() {
        let cache = KeyCache::new(KeyCacheConfig { max_entries: 100 });
        cache.put(1, b"a".to_vec(), 10);
        cache.put(1, b"b".to_vec(), 20);
        cache.put(2, b"a".to_vec(), 30);

        cache.invalidate_sstable(1);

        assert_eq!(cache.get(1, b"a"), None);
        assert_eq!(cache.get(1, b"b"), None);
        assert_eq!(cache.get(2, b"a"), Some(30));
    }

    #[test]
    fn stats_tracking() {
        let cache = KeyCache::new(KeyCacheConfig { max_entries: 100 });
        cache.put(1, b"pk".to_vec(), 42);

        // One hit, one miss
        cache.get(1, b"pk");
        cache.get(1, b"missing");

        let stats = cache.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.misses, 1);
        assert_eq!(stats.size, 1);
    }

    #[test]
    fn concurrent_access() {
        let cache = KeyCache::new(KeyCacheConfig { max_entries: 1000 });

        let handles: Vec<_> = (0..8u64)
            .map(|t| {
                let cache = Arc::clone(&cache);
                std::thread::spawn(move || {
                    for i in 0..100u64 {
                        cache.put(t, format!("key_{i}").into_bytes(), i * 100);
                    }
                    for i in 0..100u64 {
                        let _ = cache.get(t, format!("key_{i}").as_bytes());
                    }
                })
            })
            .collect();

        for h in handles {
            h.join().unwrap();
        }

        let stats = cache.stats();
        assert_eq!(stats.size, 800); // 8 threads × 100 keys
        assert_eq!(stats.hits + stats.misses, 800);
    }

    #[test]
    fn update_existing_key() {
        let cache = KeyCache::new(KeyCacheConfig { max_entries: 100 });
        cache.put(1, b"pk".to_vec(), 100);
        cache.put(1, b"pk".to_vec(), 200);
        assert_eq!(cache.get(1, b"pk"), Some(200));
        assert_eq!(cache.stats().size, 1);
    }
}
