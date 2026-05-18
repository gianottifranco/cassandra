// Licensed under Apache License, Version 2.0.

//! Cache invalidation via storage event bus.
//!
//! Automatically invalidates cache entries when SSTables are removed,
//! compaction completes, tables are truncated, or flushes occur.

use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::cache::chunk_cache::ChunkCache;
use crate::cache::counter_cache::CounterCache;
use crate::cache::row_cache::RowCache;
use crate::notifications::{StorageEvent, StorageEventListener};
use crate::sstable::format::SSTableId;
use crate::sstable::key_cache::KeyCache;

/// Listener that invalidates caches in response to storage events.
pub struct CacheInvalidationListener {
    key_cache: Arc<KeyCache>,
    row_cache: Option<Arc<RowCache>>,
    counter_cache: Option<Arc<CounterCache>>,
    chunk_cache: Option<Arc<ChunkCache>>,
}

impl CacheInvalidationListener {
    /// Create a new invalidation listener wired to the given caches.
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

    /// Invalidate key cache and chunk cache entries for a single SSTable.
    fn invalidate_sstable(&self, id: SSTableId) {
        self.key_cache.invalidate_sstable(id);
        if let Some(ref cc) = self.chunk_cache {
            cc.invalidate_sstable(id);
        }
    }

    /// Compute a table hash from keyspace and table name.
    fn table_hash(keyspace: &str, table: &str) -> u64 {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        keyspace.hash(&mut hasher);
        table.hash(&mut hasher);
        hasher.finish()
    }
}

impl StorageEventListener for CacheInvalidationListener {
    fn on_event(&self, event: &StorageEvent) {
        match event {
            StorageEvent::SSTableRemoved { id, .. } => {
                self.invalidate_sstable(*id);
            }
            StorageEvent::SSTableReplaced { removed, .. } => {
                for id in removed {
                    self.invalidate_sstable(*id);
                }
            }
            StorageEvent::CompactionCompleted { input_sstables, .. } => {
                for id in input_sstables {
                    self.invalidate_sstable(*id);
                }
            }
            StorageEvent::TruncateCompleted { keyspace, table } => {
                let h = Self::table_hash(keyspace, table);
                if let Some(ref rc) = self.row_cache {
                    rc.invalidate_table(h);
                }
                if let Some(ref cc) = self.counter_cache {
                    cc.invalidate_table(h);
                }
            }
            StorageEvent::FlushCompleted {
                keyspace, table, ..
            } => {
                let h = Self::table_hash(keyspace, table);
                if let Some(ref rc) = self.row_cache {
                    rc.invalidate_table(h);
                }
            }
            // SSTableAdded, CompactionStarted, RepairStatusChanged — no-op.
            _ => {}
        }
    }

    fn listener_id(&self) -> &str {
        "cache-invalidation"
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

    fn make_listener() -> (
        CacheInvalidationListener,
        Arc<KeyCache>,
        Arc<RowCache>,
        Arc<CounterCache>,
        Arc<ChunkCache>,
    ) {
        let key_cache = KeyCache::new(KeyCacheConfig { max_entries: 1000 });
        let row_cache = RowCache::new(RowCacheConfig { max_entries: 1000 });
        let counter_cache = CounterCache::new(CounterCacheConfig { max_entries: 1000 });
        let chunk_cache = ChunkCache::new(ChunkCacheConfig {
            max_size_bytes: 1024 * 1024,
        });

        let listener = CacheInvalidationListener::new(
            Arc::clone(&key_cache),
            Some(Arc::clone(&row_cache)),
            Some(Arc::clone(&counter_cache)),
            Some(Arc::clone(&chunk_cache)),
        );

        (listener, key_cache, row_cache, counter_cache, chunk_cache)
    }

    #[test]
    fn sstable_removed_invalidates_key_and_chunk() {
        let (listener, key_cache, _row_cache, _counter_cache, chunk_cache) = make_listener();

        key_cache.put(42, b"pk1".to_vec(), 100);
        key_cache.put(42, b"pk2".to_vec(), 200);
        chunk_cache.put(42, 0, Arc::new(vec![1, 2, 3]));
        chunk_cache.put(42, 4096, Arc::new(vec![4, 5, 6]));

        key_cache.put(99, b"pk1".to_vec(), 300);
        chunk_cache.put(99, 0, Arc::new(vec![7, 8, 9]));

        let event = StorageEvent::SSTableRemoved {
            id: 42,
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
        };
        listener.on_event(&event);

        assert!(key_cache.get(42, b"pk1").is_none());
        assert!(key_cache.get(42, b"pk2").is_none());
        assert!(chunk_cache.get(42, 0).is_none());
        assert!(chunk_cache.get(42, 4096).is_none());

        assert!(key_cache.get(99, b"pk1").is_some());
        assert!(chunk_cache.get(99, 0).is_some());
    }

    #[test]
    fn sstable_replaced_invalidates_removed_ids() {
        let (listener, key_cache, _row_cache, _counter_cache, chunk_cache) = make_listener();

        for id in [1u64, 2, 3] {
            key_cache.put(id, b"pk".to_vec(), id * 100);
            chunk_cache.put(id, 0, Arc::new(vec![id as u8]));
        }

        let event = StorageEvent::SSTableReplaced {
            removed: vec![1, 2],
            added: vec![4],
            keyspace: "ks".to_string(),
            table: "tbl".to_string(),
        };
        listener.on_event(&event);

        assert!(key_cache.get(1, b"pk").is_none());
        assert!(key_cache.get(2, b"pk").is_none());
        assert!(chunk_cache.get(1, 0).is_none());
        assert!(chunk_cache.get(2, 0).is_none());

        assert!(key_cache.get(3, b"pk").is_some());
        assert!(chunk_cache.get(3, 0).is_some());
    }

    #[test]
    fn compaction_completed_invalidates_inputs() {
        let (listener, key_cache, _row_cache, _counter_cache, chunk_cache) = make_listener();

        for id in [10u64, 20] {
            key_cache.put(id, b"pk".to_vec(), id * 10);
            chunk_cache.put(id, 0, Arc::new(vec![id as u8]));
        }

        let event = StorageEvent::CompactionCompleted {
            id: uuid::Uuid::nil(),
            input_sstables: vec![10, 20],
            output_sstables: vec![30],
            bytes_written: 8192,
        };
        listener.on_event(&event);

        assert!(key_cache.get(10, b"pk").is_none());
        assert!(key_cache.get(20, b"pk").is_none());
    }

    #[test]
    fn truncate_invalidates_row_and_counter() {
        let (listener, _key_cache, row_cache, counter_cache, _chunk_cache) = make_listener();

        let h = CacheInvalidationListener::table_hash("ks1", "tbl1");
        row_cache.put(h, b"row1".to_vec(), PartitionData::new());
        row_cache.put(h, b"row2".to_vec(), PartitionData::new());
        counter_cache.put(h, b"pk1".to_vec(), b"col1".to_vec(), b"val1".to_vec());

        let h2 = CacheInvalidationListener::table_hash("ks2", "tbl2");
        row_cache.put(h2, b"row1".to_vec(), PartitionData::new());
        counter_cache.put(h2, b"pk1".to_vec(), b"col1".to_vec(), b"val2".to_vec());

        let event = StorageEvent::TruncateCompleted {
            keyspace: "ks1".to_string(),
            table: "tbl1".to_string(),
        };
        listener.on_event(&event);

        assert!(row_cache.get(h, b"row1").is_none());
        assert!(row_cache.get(h, b"row2").is_none());
        assert!(counter_cache.get(h, b"pk1", b"col1").is_none());

        assert!(row_cache.get(h2, b"row1").is_some());
        assert!(counter_cache.get(h2, b"pk1", b"col1").is_some());
    }

    #[test]
    fn flush_invalidates_row_cache() {
        let (listener, _key_cache, row_cache, counter_cache, _chunk_cache) = make_listener();

        let h = CacheInvalidationListener::table_hash("ks1", "tbl1");
        row_cache.put(h, b"row1".to_vec(), PartitionData::new());
        counter_cache.put(h, b"pk1".to_vec(), b"col1".to_vec(), b"val".to_vec());

        let event = StorageEvent::FlushCompleted {
            keyspace: "ks1".to_string(),
            table: "tbl1".to_string(),
            sstable_id: 5,
            bytes_flushed: 2048,
        };
        listener.on_event(&event);

        assert!(row_cache.get(h, b"row1").is_none());
        // Counter cache NOT invalidated by flush
        assert!(counter_cache.get(h, b"pk1", b"col1").is_some());
    }

    #[test]
    fn unrelated_events_do_nothing() {
        let (listener, key_cache, row_cache, _counter_cache, chunk_cache) = make_listener();

        key_cache.put(1, b"pk".to_vec(), 100);
        let h = CacheInvalidationListener::table_hash("ks", "tbl");
        row_cache.put(h, b"row".to_vec(), PartitionData::new());
        chunk_cache.put(1, 0, Arc::new(vec![1, 2]));

        let events = vec![
            StorageEvent::SSTableAdded {
                id: 999,
                keyspace: "ks".to_string(),
                table: "tbl".to_string(),
            },
            StorageEvent::CompactionStarted {
                id: uuid::Uuid::nil(),
                input_sstables: vec![1],
            },
            StorageEvent::RepairStatusChanged {
                session_id: uuid::Uuid::nil(),
                status: "running".to_string(),
            },
        ];

        for event in &events {
            listener.on_event(event);
        }

        assert!(key_cache.get(1, b"pk").is_some());
        assert!(row_cache.get(h, b"row").is_some());
        assert!(chunk_cache.get(1, 0).is_some());
    }
}
