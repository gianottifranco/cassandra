// Licensed under Apache License, Version 2.0.

//! Cache service: coordinates all cache instances.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.service.CacheService`

use std::sync::Arc;

use crate::sstable::format::SSTableId;
use crate::sstable::key_cache::KeyCache;

use super::CacheStats;
use super::chunk_cache::ChunkCache;
use super::counter_cache::CounterCache;
use super::row_cache::RowCache;

/// Unified coordinator holding references to all cache instances.
pub struct CacheService {
    key_cache: Arc<KeyCache>,
    row_cache: Option<Arc<RowCache>>,
    counter_cache: Option<Arc<CounterCache>>,
    chunk_cache: Option<Arc<ChunkCache>>,
}

impl CacheService {
    /// Create a new cache service with the given cache instances.
    pub fn new(
        key_cache: Arc<KeyCache>,
        row_cache: Option<Arc<RowCache>>,
        counter_cache: Option<Arc<CounterCache>>,
        chunk_cache: Option<Arc<ChunkCache>>,
    ) -> Self {
        Self {
            key_cache,
            row_cache,
            counter_cache,
            chunk_cache,
        }
    }

    /// Reference to the key cache (always present).
    pub fn key_cache(&self) -> &Arc<KeyCache> {
        &self.key_cache
    }

    /// Reference to the row cache, if configured.
    pub fn row_cache(&self) -> Option<&Arc<RowCache>> {
        self.row_cache.as_ref()
    }

    /// Reference to the counter cache, if configured.
    pub fn counter_cache(&self) -> Option<&Arc<CounterCache>> {
        self.counter_cache.as_ref()
    }

    /// Reference to the chunk cache, if configured.
    pub fn chunk_cache(&self) -> Option<&Arc<ChunkCache>> {
        self.chunk_cache.as_ref()
    }

    /// Collect statistics from all active caches.
    pub fn all_stats(&self) -> Vec<(&str, CacheStats)> {
        let mut result = Vec::new();

        let ks = self.key_cache.stats();
        result.push((
            "KeyCache",
            CacheStats {
                hits: ks.hits,
                misses: ks.misses,
                size: ks.size,
                evictions: ks.evictions,
            },
        ));

        if let Some(rc) = &self.row_cache {
            result.push(("RowCache", rc.stats()));
        }
        if let Some(cc) = &self.counter_cache {
            result.push(("CounterCache", cc.stats()));
        }
        if let Some(ch) = &self.chunk_cache {
            result.push(("ChunkCache", ch.stats()));
        }

        result
    }

    /// Invalidate all cache entries associated with a given SSTable.
    pub fn invalidate_sstable(&self, sstable_id: SSTableId) {
        self.key_cache.invalidate_sstable(sstable_id);
        if let Some(ch) = &self.chunk_cache {
            ch.invalidate_sstable(sstable_id);
        }
    }

    /// Invalidate all cache entries associated with a given table.
    pub fn invalidate_table(&self, table_hash: u64) {
        if let Some(rc) = &self.row_cache {
            rc.invalidate_table(table_hash);
        }
        if let Some(cc) = &self.counter_cache {
            cc.invalidate_table(table_hash);
        }
    }

    /// Clear every cache managed by this service.
    pub fn clear_all(&self) {
        self.key_cache.clear();
        if let Some(rc) = &self.row_cache {
            rc.clear();
        }
        if let Some(cc) = &self.counter_cache {
            cc.clear();
        }
        if let Some(ch) = &self.chunk_cache {
            ch.clear();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::chunk_cache::ChunkCacheConfig;
    use crate::cache::counter_cache::CounterCacheConfig;
    use crate::cache::row_cache::RowCacheConfig;
    use crate::memtable::partition::PartitionData;
    use crate::sstable::key_cache::KeyCacheConfig;

    fn make_key_cache() -> Arc<KeyCache> {
        KeyCache::new(KeyCacheConfig { max_entries: 100 })
    }

    fn make_row_cache() -> Arc<RowCache> {
        RowCache::new(RowCacheConfig { max_entries: 100 })
    }

    fn make_counter_cache() -> Arc<CounterCache> {
        CounterCache::new(CounterCacheConfig { max_entries: 100 })
    }

    fn make_chunk_cache() -> Arc<ChunkCache> {
        ChunkCache::new(ChunkCacheConfig {
            max_size_bytes: 1024,
        })
    }

    #[test]
    fn service_with_all_caches() {
        let svc = CacheService::new(
            make_key_cache(),
            Some(make_row_cache()),
            Some(make_counter_cache()),
            Some(make_chunk_cache()),
        );

        let stats = svc.all_stats();
        assert_eq!(stats.len(), 4);
        assert_eq!(stats[0].0, "KeyCache");
        assert_eq!(stats[1].0, "RowCache");
        assert_eq!(stats[2].0, "CounterCache");
        assert_eq!(stats[3].0, "ChunkCache");
    }

    #[test]
    fn service_with_only_key_cache() {
        let svc = CacheService::new(make_key_cache(), None, None, None);

        let stats = svc.all_stats();
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].0, "KeyCache");

        assert!(svc.row_cache().is_none());
        assert!(svc.counter_cache().is_none());
        assert!(svc.chunk_cache().is_none());
    }

    #[test]
    fn invalidate_sstable_propagates() {
        let kc = make_key_cache();
        let ch = make_chunk_cache();

        kc.put(10, b"pk1".to_vec(), 100);
        kc.put(10, b"pk2".to_vec(), 200);
        kc.put(20, b"pk3".to_vec(), 300);
        ch.put(10, 0, Arc::new(b"chunk_a".to_vec()));
        ch.put(10, 64, Arc::new(b"chunk_b".to_vec()));
        ch.put(20, 0, Arc::new(b"chunk_c".to_vec()));

        let svc = CacheService::new(kc.clone(), None, None, Some(ch.clone()));
        svc.invalidate_sstable(10);

        assert_eq!(kc.get(10, b"pk1"), None);
        assert_eq!(kc.get(10, b"pk2"), None);
        assert!(ch.get(10, 0).is_none());
        assert!(ch.get(10, 64).is_none());

        assert_eq!(kc.get(20, b"pk3"), Some(300));
        assert!(ch.get(20, 0).is_some());
    }

    #[test]
    fn invalidate_table_propagates() {
        let rc = make_row_cache();
        let cc = make_counter_cache();

        rc.put(42, b"pk1".to_vec(), PartitionData::new());
        rc.put(42, b"pk2".to_vec(), PartitionData::new());
        rc.put(99, b"pk3".to_vec(), PartitionData::new());
        cc.put(42, b"pk1".to_vec(), b"col1".to_vec(), b"val1".to_vec());
        cc.put(99, b"pk2".to_vec(), b"col2".to_vec(), b"val2".to_vec());

        let svc = CacheService::new(make_key_cache(), Some(rc.clone()), Some(cc.clone()), None);
        svc.invalidate_table(42);

        assert!(rc.get(42, b"pk1").is_none());
        assert!(rc.get(42, b"pk2").is_none());
        assert!(cc.get(42, b"pk1", b"col1").is_none());

        assert!(rc.get(99, b"pk3").is_some());
        assert!(cc.get(99, b"pk2", b"col2").is_some());
    }

    #[test]
    fn clear_all() {
        let kc = make_key_cache();
        let rc = make_row_cache();
        let cc = make_counter_cache();
        let ch = make_chunk_cache();

        kc.put(1, b"pk".to_vec(), 100);
        rc.put(1, b"pk".to_vec(), PartitionData::new());
        cc.put(1, b"pk".to_vec(), b"col".to_vec(), b"val".to_vec());
        ch.put(1, 0, Arc::new(b"chunk".to_vec()));

        let svc = CacheService::new(
            kc.clone(),
            Some(rc.clone()),
            Some(cc.clone()),
            Some(ch.clone()),
        );
        svc.clear_all();

        assert_eq!(kc.get(1, b"pk"), None);
        assert!(rc.get(1, b"pk").is_none());
        assert!(cc.get(1, b"pk", b"col").is_none());
        assert!(ch.get(1, 0).is_none());
    }
}
