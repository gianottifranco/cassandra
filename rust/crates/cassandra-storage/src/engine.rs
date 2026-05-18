// Licensed under Apache License, Version 2.0.

//! Storage engine: coordinates commit log, memtables, SSTables, and compaction.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.ColumnFamilyStore`
//! - `org.apache.cassandra.db.Keyspace`
//! - `org.apache.cassandra.service.StorageService` (flush/compact)
//!
//! ## Architecture
//!
//! ```text
//!  Write → CommitLog → Memtable ──flush──→ SSTable ──compact──→ SSTable
//!                                                        ↓
//!  Read  → merge(Memtable, SSTables*)             CDC / Backup
//! ```
//!
//! The engine manages the full lifecycle of data from write to compaction.
//! It supports multiple SSTable formats (Big, BTI), all compaction strategies
//! (STCS, LCS, TWCS, UCS), CDC, snapshots, and incremental backups.

use std::collections::hash_map::DefaultHasher;
use std::collections::{BTreeSet, HashMap};
use std::fs;
use std::hash::{Hash, Hasher};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use tracing::{debug, error, info, warn};

use crate::backup::{self, IncrementalBackupConfig};
use crate::cache::row_cache::RowCache;
use crate::commitlog::{CommitLog, CommitLogConfig, Mutation};
use crate::compaction::{
    CompactionMetrics, CompactionStrategy, CompactionStrategyType, SSTableMetadata,
    create_strategy, create_strategy_with_options, merge_partitions, split_compaction_output,
};
use crate::index::{IndexDefinition, IndexManager, IndexStatus, IndexType, SecondaryIndex};
#[cfg(feature = "materialized-views")]
use crate::materialized_views::{MaterializedViewDefinition, ViewManager};
use crate::memtable::partition::{Cell, PartitionData, Row};
use crate::memtable::{MemtableManager, MemtableType};
use crate::sstable::format::{SSTableDescriptor, SSTableFormat, SSTableId};
use crate::sstable::key_cache::{KeyCache, KeyCacheConfig};
use crate::sstable::{BtiReader, BtiWriter, SSTableReader, SSTableWriter};

// ─── Configuration ─────────────────────────────────────────────────────────

/// Storage engine configuration.
#[derive(Debug, Clone)]
pub struct EngineConfig {
    /// Root data directories.
    pub data_directories: Vec<PathBuf>,
    /// Commit log configuration.
    pub commitlog: CommitLogConfig,
    /// Memory threshold to trigger memtable flush (bytes).
    pub memtable_flush_threshold: usize,
    /// GC grace period (seconds).
    pub gc_grace_seconds: i32,
    /// Compaction strategy type.
    pub compaction_strategy_type: CompactionStrategyType,
    /// Java table compaction option map for the selected strategy.
    pub compaction_options: HashMap<String, String>,
    /// SSTable format to use for new SSTables.
    pub sstable_format: SSTableFormat,
    /// Memtable type.
    pub memtable_type: MemtableType,
    /// Incremental backup configuration.
    pub incremental_backup: IncrementalBackupConfig,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            data_directories: vec![PathBuf::from("data")],
            commitlog: CommitLogConfig::default(),
            memtable_flush_threshold: 128 * 1024 * 1024,
            gc_grace_seconds: 864_000, // 10 days
            compaction_strategy_type: CompactionStrategyType::default(),
            compaction_options: HashMap::new(),
            sstable_format: SSTableFormat::default(),
            memtable_type: MemtableType::default(),
            incremental_backup: IncrementalBackupConfig::default(),
        }
    }
}

impl EngineConfig {
    /// Return a Java-style compaction writer output cap, if configured.
    pub fn compaction_output_max_size_bytes(&self) -> Option<u64> {
        if let Some(size_mb) = self
            .compaction_options
            .get("sstable_size_in_mb")
            .and_then(|value| value.trim().parse::<u64>().ok())
        {
            return Some(size_mb.saturating_mul(1024 * 1024));
        }

        self.compaction_options
            .get("target_sstable_size")
            .and_then(|value| parse_human_size_bytes(value).ok())
    }
}

fn parse_human_size_bytes(value: &str) -> Result<u64, String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return Err("size must not be empty".to_string());
    }

    let split_at = trimmed
        .find(|ch: char| !(ch.is_ascii_digit() || ch == '.'))
        .unwrap_or(trimmed.len());
    let (number, suffix) = trimmed.split_at(split_at);
    let amount = number
        .parse::<f64>()
        .map_err(|_| format!("{value} is not a valid size"))?;
    if amount < 0.0 || !amount.is_finite() {
        return Err(format!("{value} is not a valid size"));
    }

    let multiplier = match suffix.trim().to_ascii_lowercase().as_str() {
        "" | "b" => 1_f64,
        "k" | "kb" | "kib" => 1024_f64,
        "m" | "mb" | "mib" => 1024_f64.powi(2),
        "g" | "gb" | "gib" => 1024_f64.powi(3),
        "t" | "tb" | "tib" => 1024_f64.powi(4),
        "p" | "pb" | "pib" => 1024_f64.powi(5),
        other => return Err(format!("{other} is not a supported size suffix")),
    };

    let bytes = (amount * multiplier).ceil();
    if bytes > u64::MAX as f64 {
        return Err(format!("{value} is out of range"));
    }
    Ok(bytes as u64)
}

// ─── Engine Stats ──────────────────────────────────────────────────────────

/// Engine-level statistics.
#[derive(Debug, Clone)]
pub struct EngineStats {
    pub sstable_count: usize,
    pub total_data_bytes: u64,
    pub flushes_completed: u64,
    pub compactions_completed: u64,
    pub memtable_memory_bytes: usize,
}

// ─── SSTable Handle ────────────────────────────────────────────────────────

/// Abstraction over Big and BTI readers.
enum SSTableHandle {
    Big(SSTableReader),
    Bti(BtiReader),
}

impl SSTableHandle {
    fn descriptor(&self) -> &SSTableDescriptor {
        match self {
            SSTableHandle::Big(r) => r.descriptor(),
            SSTableHandle::Bti(r) => r.descriptor(),
        }
    }

    fn belongs_to(&self, keyspace: &str, table: &str) -> bool {
        let descriptor = self.descriptor();
        descriptor.keyspace == keyspace && descriptor.table == table
    }

    fn generation(&self) -> SSTableId {
        match self {
            SSTableHandle::Big(r) => r.generation(),
            SSTableHandle::Bti(r) => r.generation(),
        }
    }

    fn might_contain_key(&self, key: &[u8]) -> bool {
        match self {
            SSTableHandle::Big(r) => r.might_contain_key(key),
            SSTableHandle::Bti(r) => r.might_contain_key(key),
        }
    }

    fn get_partition(&self, key: &[u8]) -> std::io::Result<Option<PartitionData>> {
        match self {
            SSTableHandle::Big(r) => r.get_partition(key),
            SSTableHandle::Bti(r) => r.get_partition(key),
        }
    }

    fn iter_partitions(&self) -> std::io::Result<Vec<(Vec<u8>, PartitionData)>> {
        match self {
            SSTableHandle::Big(r) => r.iter_partitions(),
            SSTableHandle::Bti(r) => r.iter_partitions(),
        }
    }

    fn data_size(&self) -> u64 {
        match self {
            SSTableHandle::Big(r) => r.stats().map_or(0, |s| s.data_size),
            SSTableHandle::Bti(r) => r.stats().map_or(0, |s| s.data_size),
        }
    }

    fn partition_count(&self) -> u64 {
        match self {
            SSTableHandle::Big(r) => r.stats().map_or(0, |s| s.partition_count),
            SSTableHandle::Bti(r) => r.stats().map_or(0, |s| s.partition_count),
        }
    }

    fn keyspace(&self) -> String {
        match self {
            SSTableHandle::Big(r) => r.descriptor().keyspace.clone(),
            SSTableHandle::Bti(r) => r.descriptor().keyspace.clone(),
        }
    }

    fn table(&self) -> String {
        match self {
            SSTableHandle::Big(r) => r.descriptor().table.clone(),
            SSTableHandle::Bti(r) => r.descriptor().table.clone(),
        }
    }

    fn cf_name(&self) -> String {
        format!("{}.{}", self.keyspace(), self.table())
    }

    fn min_timestamp(&self) -> i64 {
        match self {
            SSTableHandle::Big(r) => r.stats().map_or(0, |s| s.min_timestamp),
            SSTableHandle::Bti(r) => r.stats().map_or(0, |s| s.min_timestamp),
        }
    }

    fn max_timestamp(&self) -> i64 {
        match self {
            SSTableHandle::Big(r) => r.stats().map_or(0, |s| s.max_timestamp),
            SSTableHandle::Bti(r) => r.stats().map_or(0, |s| s.max_timestamp),
        }
    }

    fn descriptor_component_files(&self) -> Vec<PathBuf> {
        match self {
            SSTableHandle::Big(r) => {
                let desc = r.descriptor();
                desc.expected_components()
                    .iter()
                    .map(|c| desc.component_path(*c))
                    .filter(|p| p.exists())
                    .collect()
            }
            SSTableHandle::Bti(r) => {
                let desc = r.descriptor();
                desc.expected_components()
                    .iter()
                    .map(|c| desc.component_path(*c))
                    .filter(|p| p.exists())
                    .collect()
            }
        }
    }
}

// ─── StorageEngine ─────────────────────────────────────────────────────────

/// The main storage engine.
pub struct StorageEngine {
    config: EngineConfig,
    commitlog: CommitLog,
    memtable_manager: MemtableManager,
    /// Loaded SSTables.
    sstables: RwLock<Vec<SSTableHandle>>,
    /// Next SSTable generation number.
    next_generation: AtomicU64,
    /// Compaction strategy.
    compaction_strategy: Box<dyn CompactionStrategy>,
    /// Flush counter.
    flushes_completed: AtomicU64,
    /// Compaction metrics.
    pub compaction_metrics: CompactionMetrics,
    /// Index managers per column family.
    pub index_managers: RwLock<std::collections::HashMap<String, std::sync::Arc<IndexManager>>>,
    /// Materialized view manager.
    #[cfg(feature = "materialized-views")]
    pub view_manager: std::sync::Arc<ViewManager>,
    /// Shared key cache for SSTable reads.
    key_cache: Option<Arc<KeyCache>>,
    /// Row cache for partition data.
    row_cache: Option<Arc<RowCache>>,
}

impl StorageEngine {
    /// Open the storage engine with the given config.
    pub fn open(config: EngineConfig) -> Result<Self, Box<dyn std::error::Error>> {
        // Ensure data directories exist
        for dir in &config.data_directories {
            fs::create_dir_all(dir)?;
        }

        // Open commit log
        let commitlog = CommitLog::open(config.commitlog.clone())?;

        // Create memtable manager
        let memtable_manager =
            MemtableManager::with_type(config.memtable_flush_threshold, config.memtable_type);

        // Create compaction strategy
        let compaction_strategy = if config.compaction_options.is_empty() {
            create_strategy(config.compaction_strategy_type)
        } else {
            create_strategy_with_options(
                config.compaction_strategy_type,
                &config.compaction_options,
            )
            .map_err(|err| format!("invalid compaction options: {err}"))?
        };

        // Create key cache
        let key_cache = KeyCache::new(KeyCacheConfig::default());

        // Load existing SSTables
        let (sstables, max_gen) =
            Self::load_existing_sstables(&config.data_directories, Some(&key_cache))?;

        info!(
            sstables = sstables.len(),
            max_gen,
            format = ?config.sstable_format,
            strategy = ?config.compaction_strategy_type,
            "Storage engine opened"
        );

        Ok(Self {
            config,
            commitlog,
            memtable_manager,
            sstables: RwLock::new(sstables),
            next_generation: AtomicU64::new(max_gen + 1),
            compaction_strategy,
            flushes_completed: AtomicU64::new(0),
            compaction_metrics: CompactionMetrics::default(),
            index_managers: RwLock::new(std::collections::HashMap::new()),
            #[cfg(feature = "materialized-views")]
            view_manager: std::sync::Arc::new(ViewManager::new()),
            key_cache: Some(key_cache),
            row_cache: None,
        })
    }

    /// Set the row cache for this engine.
    pub fn set_row_cache(&mut self, cache: Arc<RowCache>) {
        self.row_cache = Some(cache);
    }

    /// Get a reference to the key cache.
    pub fn key_cache(&self) -> Option<&Arc<KeyCache>> {
        self.key_cache.as_ref()
    }

    /// Get a reference to the row cache.
    pub fn row_cache(&self) -> Option<&Arc<RowCache>> {
        self.row_cache.as_ref()
    }

    /// Compute a table hash from keyspace and table name.
    fn table_hash(keyspace: &str, table: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        keyspace.hash(&mut hasher);
        table.hash(&mut hasher);
        hasher.finish()
    }

    /// Apply a mutation (write path).
    pub fn apply_mutation(&self, mutation: &Mutation) -> Result<(), Box<dyn std::error::Error>> {
        // 1. Write to commit log
        let (seg_id, _offset) = self.commitlog.append(mutation)?;

        // 2. Apply to memtable
        let cf_name = format!("{}.{}", mutation.keyspace, mutation.table);
        let memtable = self.memtable_manager.get_or_create(&cf_name, seg_id);
        memtable.update_commitlog_upper_bound(seg_id);

        for mrow in &mutation.rows {
            let row = Row {
                clustering_key: mrow.clustering_key.clone(),
                cells: mrow
                    .cells
                    .iter()
                    .map(|c| Cell {
                        column: c.column.clone(),
                        value: c.value.clone(),
                        timestamp: c.timestamp,
                        ttl: c.ttl,
                        local_deletion_time: c.local_deletion_time,
                        is_tombstone: c.is_tombstone,
                    })
                    .collect(),
                is_tombstone: mrow.is_tombstone,
                local_deletion_time: mrow.local_deletion_time,
            };

            // Notify secondary indexes
            let idx_mgrs = self.index_managers.read();
            if let Some(idx_mgr) = idx_mgrs.get(&cf_name) {
                for cell in &row.cells {
                    if let Some(val) = &cell.value {
                        if !cell.is_tombstone {
                            let _ = idx_mgr.on_write(
                                &mutation.partition_key,
                                &row.clustering_key,
                                &cell.column,
                                val,
                            );
                        }
                    }
                }
            }

            memtable.apply(mutation.partition_key.clone(), row);
        }

        // 2b. Apply static cells as a row with empty clustering key.
        if !mutation.static_cells.is_empty() {
            let static_row = Row {
                clustering_key: Vec::new(), // static row has no clustering key
                cells: mutation
                    .static_cells
                    .iter()
                    .map(|c| Cell {
                        column: c.column.clone(),
                        value: c.value.clone(),
                        timestamp: c.timestamp,
                        ttl: c.ttl,
                        local_deletion_time: c.local_deletion_time,
                        is_tombstone: c.is_tombstone,
                    })
                    .collect(),
                is_tombstone: false,
                local_deletion_time: None,
            };
            memtable.apply(mutation.partition_key.clone(), static_row);
        }

        // 2c. Apply partition-level tombstone.
        if let Some(ref pt) = mutation.partition_tombstone {
            memtable.set_partition_tombstone(
                mutation.partition_key.clone(),
                pt.timestamp,
                pt.local_deletion_time,
            );
        }

        // 2d. Apply range tombstones as tombstone rows covering the range start.
        for rt in &mutation.range_tombstones {
            let tombstone_row = Row {
                clustering_key: rt.start.clone(),
                cells: Vec::new(),
                is_tombstone: true,
                local_deletion_time: Some(rt.local_deletion_time),
            };
            memtable.apply(mutation.partition_key.clone(), tombstone_row);
        }

        // 3. Generate and apply materialized view mutations
        #[cfg(feature = "materialized-views")]
        if self
            .view_manager
            .has_views_for(&mutation.keyspace, &mutation.table)
        {
            for mrow in &mutation.rows {
                let mut cols_map = std::collections::HashMap::new();
                for cell in &mrow.cells {
                    cols_map.insert(cell.column.clone(), cell.value.clone());
                }
                let view_muts = self.view_manager.generate_view_updates(
                    &mutation.keyspace,
                    &mutation.table,
                    &mutation.partition_key,
                    &cols_map,
                    mrow.cells.first().map_or(0, |c| c.timestamp),
                    mrow.is_tombstone,
                );

                for view_mut in view_muts.mutations {
                    let mut cells = Vec::new();
                    let timestamp = view_mut.timestamp;
                    for (col_name, col_value) in view_mut.columns {
                        cells.push(crate::commitlog::CellMutation {
                            column: col_name,
                            value: col_value,
                            timestamp,
                            ttl: 0,
                            local_deletion_time: None,
                            is_tombstone: view_mut.is_delete,
                        });
                    }
                    let m = Mutation {
                        keyspace: view_mut.keyspace,
                        table: view_mut.view_table,
                        partition_key: view_mut.partition_key,
                        rows: vec![crate::commitlog::MutationRow {
                            clustering_key: mrow.clustering_key.clone(),
                            cells,
                            is_tombstone: view_mut.is_delete,
                            local_deletion_time: None,
                        }],
                        timestamp,
                        cdc_enabled: false,
                        static_cells: Vec::new(),
                        partition_tombstone: None,
                        range_tombstones: Vec::new(),
                    };
                    let _ = self.apply_mutation(&m);
                }
            }
        }

        // 4. Invalidate row cache for this partition
        if let Some(ref cache) = self.row_cache {
            let th = Self::table_hash(&mutation.keyspace, &mutation.table);
            cache.invalidate_partition(th, &mutation.partition_key);
        }

        // 5. Check backpressure
        if self.memtable_manager.should_flush() {
            debug!("Backpressure: inline flush triggered");
            self.flush_cf(&cf_name)?;
        }

        Ok(())
    }

    /// Read a partition (read path): merge memtable + SSTables.
    pub fn read_partition(
        &self,
        keyspace: &str,
        table: &str,
        partition_key: &[u8],
    ) -> Option<PartitionData> {
        let cf_name = format!("{keyspace}.{table}");
        let th = Self::table_hash(keyspace, table);

        // Read from memtable
        let memtable = self.memtable_manager.get_or_create(&cf_name, 0);
        let mut result = memtable.get_partition(partition_key);

        // If memtable had no data, try row cache before going to SSTables
        if result.is_none() {
            if let Some(ref cache) = self.row_cache {
                if let Some(cached) = cache.get(th, partition_key) {
                    return Some(cached);
                }
            }
        }

        // Read from SSTables (newest first)
        let sstables = self.sstables.read();
        for sst in sstables.iter().rev() {
            if !sst.belongs_to(keyspace, table) {
                continue;
            }
            if !sst.might_contain_key(partition_key) {
                continue;
            }
            if let Ok(Some(sst_partition)) = sst.get_partition(partition_key) {
                match result {
                    Some(ref mut existing) => {
                        // Merge SSTable data into existing
                        for (_ck, row) in sst_partition.rows {
                            existing.apply_row(row);
                        }
                        if let Some(ts) = sst_partition.tombstone_timestamp {
                            if let Some(ldt) = sst_partition.tombstone_local_deletion_time {
                                existing.set_tombstone(ts, ldt);
                            }
                        }
                    }
                    None => result = Some(sst_partition),
                }
            }
        }

        // Populate row cache on SSTable read (only when memtable had no data)
        if let Some(ref data) = result {
            if let Some(ref cache) = self.row_cache {
                cache.put(th, partition_key.to_vec(), data.clone());
            }
        }

        result
    }

    /// Scan all partitions in a table by merging active memtable entries and SSTables.
    ///
    /// ## Java Oracle
    /// `org.apache.cassandra.db.ColumnFamilyStore.getRangeSlice()`
    pub fn scan_all_partitions(
        &self,
        keyspace: &str,
        table: &str,
    ) -> Vec<(Vec<u8>, PartitionData)> {
        let cf_name = format!("{keyspace}.{table}");
        let mut keys = BTreeSet::new();

        for (key, _) in self.memtable_manager.active_partitions(&cf_name) {
            keys.insert(key);
        }

        let sstables = self.sstables.read();
        for sst in sstables
            .iter()
            .filter(|sst| sst.belongs_to(keyspace, table))
        {
            if let Ok(partitions) = sst.iter_partitions() {
                for (key, _) in partitions {
                    keys.insert(key);
                }
            }
        }
        drop(sstables);

        keys.into_iter()
            .filter_map(|key| {
                self.read_partition(keyspace, table, &key)
                    .map(|partition| (key, partition))
            })
            .collect()
    }

    /// Truncate a table by discarding its active memtable, loaded SSTables,
    /// secondary-index contents, and table-scoped cache entries.
    pub fn truncate_table(
        &self,
        keyspace: &str,
        table: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let cf_name = format!("{keyspace}.{table}");

        if let Some(old_memtable) = self
            .memtable_manager
            .switch_memtable(&cf_name, self.commitlog.current_segment_id())
        {
            self.memtable_manager.flush_complete(old_memtable.id);
        }

        let table_hash = Self::table_hash(keyspace, table);
        if let Some(ref cache) = self.row_cache {
            cache.invalidate_table(table_hash);
        }

        if let Some(idx_mgr) = self.index_managers.read().get(&cf_name).cloned() {
            idx_mgr.truncate_all()?;
        }

        let mut removed_generations = Vec::new();
        let mut component_files = Vec::new();
        {
            let mut sstables = self.sstables.write();
            let existing = std::mem::take(&mut *sstables);
            let mut retained = Vec::with_capacity(existing.len());

            for sstable in existing {
                if sstable.keyspace() == keyspace && sstable.table() == table {
                    removed_generations.push(sstable.generation());
                    component_files.extend(sstable.descriptor_component_files());
                } else {
                    retained.push(sstable);
                }
            }

            *sstables = retained;
        }

        if let Some(ref key_cache) = self.key_cache {
            for generation in &removed_generations {
                key_cache.invalidate_sstable(*generation);
            }
        }

        for path in component_files {
            match fs::remove_file(&path) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::NotFound => {}
                Err(err) => return Err(Box::new(err)),
            }
        }

        info!(
            keyspace,
            table,
            sstables_removed = removed_generations.len(),
            "Truncated table"
        );
        Ok(())
    }

    /// Search an exact-match index (2i or SAI).
    pub fn search_index(
        &self,
        keyspace: &str,
        table: &str,
        index_name: &str,
        term: &[u8],
    ) -> Result<Vec<PartitionData>, Box<dyn std::error::Error>> {
        Ok(self
            .search_index_with_keys(keyspace, table, index_name, term)?
            .into_iter()
            .map(|(_, pd)| pd)
            .collect())
    }

    /// Search an exact-match index and preserve the matching partition key.
    pub fn search_index_with_keys(
        &self,
        keyspace: &str,
        table: &str,
        index_name: &str,
        term: &[u8],
    ) -> Result<Vec<(Vec<u8>, PartitionData)>, Box<dyn std::error::Error>> {
        let cf_name = format!("{}.{}", keyspace, table);
        let mgrs = self.index_managers.read();
        let mgr = mgrs.get(&cf_name).ok_or("Index manager not found")?;

        let entries = mgr.search(index_name, term)?;

        let mut results = Vec::new();
        // Resolve raw partition data
        // In production this returns Iterators to avoid holding whole partitions in RAM.
        for entry in entries {
            if let Some(pd) = self.read_partition(keyspace, table, &entry.partition_key) {
                results.push((entry.partition_key, pd));
            }
        }
        Ok(results)
    }

    /// Search a vector index using kNN.
    pub fn search_vector_index(
        &self,
        keyspace: &str,
        table: &str,
        index_name: &str,
        vector: &[u8],
        top_k: usize,
    ) -> Result<Vec<(PartitionData, f32)>, Box<dyn std::error::Error>> {
        Ok(self
            .search_vector_index_with_keys(keyspace, table, index_name, vector, top_k)?
            .into_iter()
            .map(|(_, pd, score)| (pd, score))
            .collect())
    }

    /// Search a vector index and preserve the matching partition key.
    pub fn search_vector_index_with_keys(
        &self,
        keyspace: &str,
        table: &str,
        index_name: &str,
        vector: &[u8],
        top_k: usize,
    ) -> Result<Vec<(Vec<u8>, PartitionData, f32)>, Box<dyn std::error::Error>> {
        let cf_name = format!("{}.{}", keyspace, table);
        let mgrs = self.index_managers.read();
        let mgr = mgrs.get(&cf_name).ok_or("Index manager not found")?;

        let entries = mgr.search_vector(index_name, vector, top_k)?;

        let mut results = Vec::new();
        for (entry, score) in entries {
            if let Some(pd) = self.read_partition(keyspace, table, &entry.partition_key) {
                results.push((entry.partition_key, pd, score));
            }
        }
        Ok(results)
    }

    /// Flush a column family's memtable to disk.
    pub fn flush_cf(&self, cf_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        let old_memtable = match self
            .memtable_manager
            .switch_memtable(cf_name, self.commitlog.current_segment_id())
        {
            Some(mt) => mt,
            None => return Ok(()),
        };

        let partitions = old_memtable.iter_partitions();
        if partitions.is_empty() {
            self.memtable_manager.flush_complete(old_memtable.id);
            return Ok(());
        }

        let generation = self.next_generation.fetch_add(1, Ordering::SeqCst);
        let data_dir = &self.config.data_directories[0];

        let parts: Vec<&str> = cf_name.splitn(2, '.').collect();
        let (ks, tbl) = if parts.len() == 2 {
            (parts[0], parts[1])
        } else {
            (cf_name, "unknown")
        };

        let mut descriptor = SSTableDescriptor::new(data_dir, ks, tbl, generation);
        descriptor.format = self.config.sstable_format;

        // Give SAI indexes a chance to build segments
        if let Some(idx_mgr) = self.index_managers.read().get(cf_name).cloned() {
            if let Err(e) = idx_mgr.build_sai_segments(generation, &partitions) {
                error!(cf = cf_name, error = %e, "Failed to build SAI segments during flush");
            }
        }

        // Write SSTable using the configured format
        match self.config.sstable_format {
            SSTableFormat::Big => {
                let writer = SSTableWriter::new(descriptor.clone());
                writer.write(&partitions)?;
                let mut reader = SSTableReader::open(descriptor)?;
                if let Some(ref cache) = self.key_cache {
                    reader = reader.with_key_cache(Arc::clone(cache));
                }
                let handle = SSTableHandle::Big(reader);
                let files = handle.descriptor_component_files();
                self.maybe_backup(&files);
                self.sstables.write().push(handle);
            }
            SSTableFormat::Bti => {
                let writer = BtiWriter::new(descriptor.clone());
                writer.write(&partitions)?;
                let reader = BtiReader::open(descriptor)?;
                let handle = SSTableHandle::Bti(reader);
                let files = handle.descriptor_component_files();
                self.maybe_backup(&files);
                self.sstables.write().push(handle);
            }
        }

        // Mark flush complete
        self.memtable_manager.flush_complete(old_memtable.id);
        self.flushes_completed.fetch_add(1, Ordering::Relaxed);

        // Record truncation point
        let upper = old_memtable.commitlog_upper_bound.load(Ordering::Relaxed);
        self.commitlog.mark_cf_flushed(cf_name, upper, 0);

        // Discard old commit log segments
        if let Some(lowest) = self.memtable_manager.lowest_commitlog_bound() {
            if lowest > 1 {
                let _ = self.commitlog.discard_completed_segments(lowest - 1);
            }
        }

        info!(
            cf = cf_name,
            generation,
            partitions = partitions.len(),
            format = ?self.config.sstable_format,
            "Flush complete"
        );

        Ok(())
    }

    /// Flush all column families.
    pub fn flush_all(&self) -> Result<(), Box<dyn std::error::Error>> {
        let cf_names = self.memtable_manager.active_cf_names();
        for cf in &cf_names {
            self.flush_cf(cf)?;
        }
        Ok(())
    }

    /// Run compaction if needed.
    pub fn maybe_compact(&self) -> Result<bool, Box<dyn std::error::Error>> {
        let sstables = self.sstables.read();
        let metadata: Vec<SSTableMetadata> = sstables
            .iter()
            .map(|sst| SSTableMetadata {
                id: sst.generation(),
                data_size: sst.data_size(),
                partition_count: sst.partition_count(),
                min_timestamp: sst.min_timestamp(),
                max_timestamp: sst.max_timestamp(),
            })
            .collect();
        drop(sstables);

        let groups = self.compaction_strategy.pick_compaction(&metadata);
        if groups.is_empty() {
            return Ok(false);
        }

        for group_ids in &groups {
            self.compact_group(group_ids)?;
        }

        Ok(true)
    }

    /// Create a named snapshot.
    pub fn snapshot(
        &self,
        name: &str,
        keyspace: &str,
        table: &str,
        schema_cql: Option<&str>,
    ) -> Result<backup::SnapshotManifest, Box<dyn std::error::Error>> {
        let data_dir = &self.config.data_directories[0];
        let sstables = self.sstables.read();

        let all_files: Vec<PathBuf> = sstables
            .iter()
            .flat_map(|sst| sst.descriptor_component_files())
            .collect();

        let manifest =
            backup::create_snapshot(name, data_dir, keyspace, table, &all_files, schema_cql)?;

        Ok(manifest)
    }

    /// List snapshots.
    pub fn list_snapshots(
        &self,
    ) -> Result<Vec<backup::SnapshotManifest>, Box<dyn std::error::Error>> {
        let data_dir = &self.config.data_directories[0];
        Ok(backup::list_snapshots(data_dir)?)
    }

    /// Delete a snapshot.
    pub fn delete_snapshot(&self, name: &str) -> Result<(), Box<dyn std::error::Error>> {
        let data_dir = &self.config.data_directories[0];
        Ok(backup::delete_snapshot(data_dir, name)?)
    }

    /// Replay commitlog for recovery.
    pub fn replay_commitlog(&self) -> Result<usize, Box<dyn std::error::Error>> {
        let result = self.commitlog.replay()?;
        let count = result.mutations.len();

        for mutation in result.mutations {
            let cf_name = format!("{}.{}", mutation.keyspace, mutation.table);
            let memtable = self.memtable_manager.get_or_create(&cf_name, 0);

            for mrow in &mutation.rows {
                let row = Row {
                    clustering_key: mrow.clustering_key.clone(),
                    cells: mrow
                        .cells
                        .iter()
                        .map(|c| Cell {
                            column: c.column.clone(),
                            value: c.value.clone(),
                            timestamp: c.timestamp,
                            ttl: c.ttl,
                            local_deletion_time: c.local_deletion_time,
                            is_tombstone: c.is_tombstone,
                        })
                        .collect(),
                    is_tombstone: mrow.is_tombstone,
                    local_deletion_time: mrow.local_deletion_time,
                };
                memtable.apply(mutation.partition_key.clone(), row);
            }
        }

        info!(mutations = count, "Commitlog replay complete");
        Ok(count)
    }

    /// Force sync the commit log.
    pub fn sync_commitlog(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.commitlog.sync()?;
        Ok(())
    }

    /// Get engine statistics.
    pub fn stats(&self) -> EngineStats {
        let sstables = self.sstables.read();
        let total_bytes: u64 = sstables.iter().map(|s| s.data_size()).sum();

        EngineStats {
            sstable_count: sstables.len(),
            total_data_bytes: total_bytes,
            flushes_completed: self.flushes_completed.load(Ordering::Relaxed),
            compactions_completed: self
                .compaction_metrics
                .compactions_completed
                .load(Ordering::Relaxed),
            memtable_memory_bytes: self.memtable_manager.total_memory_usage(),
        }
    }

    /// Add a new index and rebuild it from existing data.
    pub fn rebuild_index(
        &self,
        cf_name: &str,
        definition: IndexDefinition,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let index_name = definition.name.clone();

        // Ensure index manager exists.
        let idx_mgr = {
            let mut mgrs = self.index_managers.write();
            let entry = mgrs
                .entry(cf_name.to_string())
                .or_insert_with(|| std::sync::Arc::new(IndexManager::new()));
            entry.clone()
        };

        if idx_mgr.has_index(&index_name) {
            idx_mgr.unregister(&index_name);
        }
        idx_mgr.set_status(&index_name, IndexStatus::Building);

        let index: Box<dyn SecondaryIndex> = match definition.index_type {
            IndexType::Legacy => {
                Box::new(crate::index::legacy::LegacyIndex::new(definition.clone()))
            }
            #[cfg(feature = "sasi")]
            IndexType::Sasi => Box::new(crate::index::sasi::SasiIndex::new(definition.clone())),
            #[cfg(not(feature = "sasi"))]
            IndexType::Sasi => {
                return Err(Box::new(std::io::Error::new(
                    std::io::ErrorKind::Unsupported,
                    "SASI index not enabled in build metrics",
                )));
            }
            IndexType::Sai => Box::new(crate::index::sai::SaiIndex::new(definition.clone())),
        };

        info!(cf = cf_name, index = index_name, "Starting index rebuild");

        let mut count = 0;
        let (keyspace, table) = cf_name
            .split_once('.')
            .ok_or_else(|| format!("Invalid column family name '{}'", cf_name))?;
        for (pk, partition) in self.scan_all_partitions(keyspace, table) {
            for (ck, row) in partition.rows {
                for cell in row.cells {
                    if !cell.is_tombstone && cell.column == definition.column {
                        if let Some(val) = &cell.value {
                            let entry = crate::index::IndexEntry {
                                term: val.clone(),
                                partition_key: pk.clone(),
                                clustering_key: ck.clone(),
                            };
                            index.insert(&entry)?;
                            count += 1;
                        }
                    }
                }
            }
        }

        idx_mgr.register(index);
        idx_mgr.set_status(&index_name, IndexStatus::QueryReady);

        info!(
            cf = cf_name,
            index = index_name,
            entries = count,
            "Finished index rebuild"
        );
        Ok(())
    }

    /// Build a materialized view by backfilling all existing base table data.
    #[cfg(feature = "materialized-views")]
    pub fn build_view(
        &self,
        def: MaterializedViewDefinition,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let view_name = def.name.clone();
        let ks = def.keyspace.clone();
        let base_table = def.base_table.clone();

        // 1. Register the view
        self.view_manager.register(def.clone())?;

        info!(view = %view_name, base_table = %base_table, "Starting materialized view backfill");

        let mut count = 0;
        for (pk, partition) in self.scan_all_partitions(&ks, &base_table) {
            for (ck, row) in partition.rows {
                let mut cols_map = std::collections::HashMap::new();
                let mut ts = 0;
                let mut tombstone = row.is_tombstone;
                for cell in row.cells {
                    ts = ts.max(cell.timestamp);
                    if cell.is_tombstone {
                        tombstone = true;
                    }
                    cols_map.insert(cell.column, cell.value);
                }

                let view_muts = self.view_manager.generate_view_updates(
                    &ks,
                    &base_table,
                    &pk,
                    &cols_map,
                    ts,
                    tombstone,
                );

                for view_mut in view_muts.mutations {
                    let mut cells = Vec::new();
                    for (col_name, col_value) in view_mut.columns {
                        cells.push(crate::commitlog::CellMutation {
                            column: col_name,
                            value: col_value,
                            timestamp: ts,
                            ttl: 0,
                            local_deletion_time: None,
                            is_tombstone: view_mut.is_delete,
                        });
                    }
                    let m = Mutation {
                        keyspace: view_mut.keyspace,
                        table: view_mut.view_table,
                        partition_key: view_mut.partition_key,
                        rows: vec![crate::commitlog::MutationRow {
                            clustering_key: ck.clone(),
                            cells,
                            is_tombstone: view_mut.is_delete,
                            local_deletion_time: None,
                        }],
                        timestamp: ts,
                        cdc_enabled: false,
                        static_cells: Vec::new(),
                        partition_tombstone: None,
                        range_tombstones: Vec::new(),
                    };
                    let _ = self.apply_mutation(&m);
                    count += 1;
                }
            }
        }

        info!(view = %view_name, mutations_applied = count, "Finished materialized view backfill");
        Ok(())
    }

    // ─── Internal helpers ──────────────────────────────────────────────

    fn compact_group(&self, group_ids: &[SSTableId]) -> Result<(), Box<dyn std::error::Error>> {
        let sstables = self.sstables.read();

        // Collect partitions from SSTables in the group
        let mut sources = Vec::new();
        let mut total_read_bytes = 0u64;
        let mut group_keyspace = String::from("compacted");
        let mut group_table = String::from("data");
        let mut group_cf_name = String::from("compacted.data");

        for sst in sstables.iter() {
            if group_ids.contains(&sst.generation()) {
                if sources.is_empty() {
                    group_keyspace = sst.keyspace();
                    group_table = sst.table();
                    group_cf_name = sst.cf_name();
                }
                let partitions = sst.iter_partitions()?;
                total_read_bytes += sst.data_size();
                sources.push(partitions);
            }
        }
        drop(sstables);

        if sources.is_empty() {
            return Ok(());
        }

        let now_seconds = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i32;

        let merged = merge_partitions(sources, self.config.gc_grace_seconds, now_seconds);

        if merged.is_empty() {
            // All data was tombstoned or expired
            let mut sstables = self.sstables.write();
            sstables.retain(|sst| !group_ids.contains(&sst.generation()));
            return Ok(());
        }

        let output_batches =
            split_compaction_output(&merged, self.config.compaction_output_max_size_bytes());
        let data_dir = &self.config.data_directories[0];
        let mut new_handles = Vec::new();
        let mut written_bytes = 0_u64;
        let mut output_generations = Vec::new();

        for batch in output_batches {
            let generation = self.next_generation.fetch_add(1, Ordering::SeqCst);
            output_generations.push(generation);
            let mut descriptor =
                SSTableDescriptor::new(data_dir, &group_keyspace, &group_table, generation);
            descriptor.format = self.config.sstable_format;

            // Give SAI indexes a chance to build segments
            if let Some(idx_mgr) = self.index_managers.read().get(&group_cf_name).cloned() {
                if let Err(e) = idx_mgr.build_sai_segments(generation, &batch) {
                    error!(cf = group_cf_name, error = %e, "Failed to build SAI segments during compaction");
                }
            }

            written_bytes += match self.config.sstable_format {
                SSTableFormat::Big => {
                    let writer = SSTableWriter::new(descriptor.clone());
                    let stats = writer.write(&batch)?;
                    stats.data_size
                }
                SSTableFormat::Bti => {
                    let writer = BtiWriter::new(descriptor.clone());
                    let stats = writer.write(&batch)?;
                    stats.data_size
                }
            };

            let new_handle = match self.config.sstable_format {
                SSTableFormat::Big => {
                    let mut reader = SSTableReader::open(descriptor)?;
                    if let Some(ref cache) = self.key_cache {
                        reader = reader.with_key_cache(Arc::clone(cache));
                    }
                    SSTableHandle::Big(reader)
                }
                SSTableFormat::Bti => SSTableHandle::Bti(BtiReader::open(descriptor)?),
            };
            new_handles.push(new_handle);
        }

        {
            let mut sstables = self.sstables.write();
            sstables.retain(|sst| !group_ids.contains(&sst.generation()));
            sstables.extend(new_handles);
        }

        // Update metrics
        self.compaction_metrics
            .compactions_completed
            .fetch_add(1, Ordering::Relaxed);
        self.compaction_metrics
            .bytes_read
            .fetch_add(total_read_bytes, Ordering::Relaxed);
        self.compaction_metrics
            .bytes_written
            .fetch_add(written_bytes, Ordering::Relaxed);
        self.compaction_metrics
            .sstables_compacted
            .fetch_add(group_ids.len() as u64, Ordering::Relaxed);

        info!(
            group = ?group_ids,
            generations = ?output_generations,
            merged_partitions = merged.len(),
            "Compaction complete"
        );

        Ok(())
    }

    fn maybe_backup(&self, files: &[PathBuf]) {
        if self.config.incremental_backup.enabled {
            if let Err(e) = backup::backup_sstable(&self.config.incremental_backup.directory, files)
            {
                warn!(error = %e, "Incremental backup failed");
            }
        }
    }

    fn load_existing_sstables(
        data_dirs: &[PathBuf],
        key_cache: Option<&Arc<KeyCache>>,
    ) -> Result<(Vec<SSTableHandle>, u64), Box<dyn std::error::Error>> {
        let mut handles = Vec::new();
        let mut max_gen: u64 = 0;

        for data_dir in data_dirs {
            if !data_dir.exists() {
                continue;
            }
            let mut toc_files = Vec::new();
            collect_toc_files(data_dir, &mut toc_files)?;
            for toc_path in toc_files {
                let Some(fname) = toc_path.file_name().and_then(|name| name.to_str()) else {
                    continue;
                };
                let Some(component_dir) = toc_path.parent() else {
                    continue;
                };

                // Parse descriptor from TOC filename
                if let Some((desc, generation)) = parse_toc_filename(fname, component_dir) {
                    max_gen = max_gen.max(generation);
                    match desc.format {
                        SSTableFormat::Big => match SSTableReader::open(desc) {
                            Ok(reader) => {
                                let reader = if let Some(cache) = key_cache {
                                    reader.with_key_cache(Arc::clone(cache))
                                } else {
                                    reader
                                };
                                handles.push(SSTableHandle::Big(reader));
                            }
                            Err(e) => {
                                warn!(file = %fname, error = %e, "Failed to open Big SSTable")
                            }
                        },
                        SSTableFormat::Bti => match BtiReader::open(desc) {
                            Ok(reader) => handles.push(SSTableHandle::Bti(reader)),
                            Err(e) => {
                                warn!(file = %fname, error = %e, "Failed to open BTI SSTable")
                            }
                        },
                    }
                }
            }
        }

        handles.sort_by_key(|sst| sst.generation());
        Ok((handles, max_gen))
    }
}

fn collect_toc_files(dir: &Path, out: &mut Vec<PathBuf>) -> std::io::Result<()> {
    for entry in fs::read_dir(dir)? {
        let entry = entry?;
        let file_type = entry.file_type()?;
        let path = entry.path();

        if file_type.is_dir() {
            let name = entry.file_name();
            if matches!(name.to_str(), Some("snapshots" | "backups")) {
                continue;
            }
            collect_toc_files(&path, out)?;
        } else if file_type.is_file()
            && path
                .file_name()
                .and_then(|name| name.to_str())
                .is_some_and(|name| name.ends_with("-TOC.txt"))
        {
            out.push(path);
        }
    }
    Ok(())
}

/// Parse a TOC filename to extract SSTable descriptor.
fn parse_toc_filename(fname: &str, dir: &Path) -> Option<(SSTableDescriptor, u64)> {
    // Format: {ks}-{table}-{format}-{generation}-TOC.txt
    let base = fname.strip_suffix("-TOC.txt")?;
    let parts: Vec<&str> = base.rsplitn(3, '-').collect();
    if parts.len() < 3 {
        return None;
    }
    let generation: u64 = parts[0].parse().ok()?;
    let format_str = parts[1];
    let ks_table = parts[2];

    let format = match format_str {
        "big" => SSTableFormat::Big,
        "bti" => SSTableFormat::Bti,
        _ => return None,
    };

    // Split ks-table: take the first part as ks, rest as table
    let kst_parts: Vec<&str> = ks_table.splitn(2, '-').collect();
    if kst_parts.len() < 2 {
        return None;
    }

    let mut desc = SSTableDescriptor::new(dir, kst_parts[0], kst_parts[1], generation);
    desc.format = format;
    Some((desc, generation))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::commitlog::{CellMutation, MutationRow};
    use tempfile::TempDir;

    fn test_engine_config(dir: &Path) -> EngineConfig {
        EngineConfig {
            data_directories: vec![dir.to_path_buf()],
            commitlog: CommitLogConfig {
                directory: dir.join("commitlog"),
                max_segment_size: 4096,
                ..CommitLogConfig::default()
            },
            memtable_flush_threshold: 1024 * 1024,
            gc_grace_seconds: 0,
            ..EngineConfig::default()
        }
    }

    fn test_mutation(ks: &str, tbl: &str, pk: &[u8], col: &str, val: &[u8]) -> Mutation {
        Mutation {
            keyspace: ks.to_string(),
            table: tbl.to_string(),
            partition_key: pk.to_vec(),
            rows: vec![MutationRow {
                clustering_key: vec![],
                cells: vec![CellMutation {
                    column: col.to_string(),
                    value: Some(val.to_vec()),
                    timestamp: 1000,
                    ttl: 0,
                    local_deletion_time: None,
                    is_tombstone: false,
                }],
                is_tombstone: false,
                local_deletion_time: None,
            }],
            timestamp: 1000,
            cdc_enabled: false,
            static_cells: Vec::new(),
            partition_tombstone: None,
            range_tombstones: Vec::new(),
        }
    }

    #[test]
    fn open_uses_java_compaction_options() {
        let dir = TempDir::new().unwrap();
        let mut config = test_engine_config(dir.path());
        config.compaction_strategy_type = CompactionStrategyType::TimeWindow;
        config.compaction_options = HashMap::from([
            ("compaction_window_unit".to_string(), "HOURS".to_string()),
            ("compaction_window_size".to_string(), "2".to_string()),
            ("min_threshold".to_string(), "2".to_string()),
        ]);

        let engine = StorageEngine::open(config).unwrap();
        assert_eq!(
            engine.config.compaction_strategy_type,
            CompactionStrategyType::TimeWindow
        );
        assert_eq!(
            engine
                .config
                .compaction_options
                .get("compaction_window_unit")
                .map(String::as_str),
            Some("HOURS")
        );
    }

    #[test]
    fn open_rejects_invalid_compaction_options() {
        let dir = TempDir::new().unwrap();
        let mut config = test_engine_config(dir.path());
        config.compaction_options = HashMap::from([
            ("bucket_low".to_string(), "2.0".to_string()),
            ("bucket_high".to_string(), "1.0".to_string()),
        ]);

        let err = match StorageEngine::open(config) {
            Ok(_) => panic!("invalid compaction options should fail"),
            Err(err) => err,
        };
        assert!(err.to_string().contains("invalid compaction options"));
    }

    #[test]
    fn compaction_output_max_size_reads_java_options() {
        let mut config = EngineConfig {
            compaction_options: HashMap::from([(
                "sstable_size_in_mb".to_string(),
                "2".to_string(),
            )]),
            ..EngineConfig::default()
        };
        assert_eq!(
            config.compaction_output_max_size_bytes(),
            Some(2 * 1024 * 1024)
        );

        config.compaction_options =
            HashMap::from([("target_sstable_size".to_string(), "3MiB".to_string())]);
        assert_eq!(
            config.compaction_output_max_size_bytes(),
            Some(3 * 1024 * 1024)
        );
    }

    #[test]
    fn compaction_splits_outputs_by_java_size_option() {
        let dir = TempDir::new().unwrap();
        let mut config = test_engine_config(dir.path());
        config.compaction_options = HashMap::from([
            ("min_threshold".to_string(), "2".to_string()),
            ("max_threshold".to_string(), "2".to_string()),
            ("target_sstable_size".to_string(), "1KiB".to_string()),
        ]);
        let engine = StorageEngine::open(config).unwrap();

        engine
            .apply_mutation(&test_mutation("ks", "t1", b"pk1", "name", &vec![1; 2048]))
            .unwrap();
        engine.flush_cf("ks.t1").unwrap();
        engine
            .apply_mutation(&test_mutation("ks", "t1", b"pk2", "name", &vec![2; 2048]))
            .unwrap();
        engine.flush_cf("ks.t1").unwrap();
        assert_eq!(engine.stats().sstable_count, 2);

        assert!(engine.maybe_compact().unwrap());
        let stats = engine.stats();
        assert_eq!(stats.compactions_completed, 1);
        assert_eq!(
            stats.sstable_count, 2,
            "two oversized partitions should be split into two output SSTables"
        );
        assert!(engine.read_partition("ks", "t1", b"pk1").is_some());
        assert!(engine.read_partition("ks", "t1", b"pk2").is_some());
    }

    #[test]
    fn write_read_roundtrip() {
        let dir = TempDir::new().unwrap();
        let config = test_engine_config(dir.path());
        let engine = StorageEngine::open(config).unwrap();

        let m = test_mutation("ks", "t1", b"pk1", "name", b"alice");
        engine.apply_mutation(&m).unwrap();

        let result = engine.read_partition("ks", "t1", b"pk1");
        assert!(result.is_some());
        let pd = result.unwrap();
        let row = pd.rows.values().next().unwrap();
        assert_eq!(row.cells[0].value.as_deref(), Some(b"alice".as_slice()));
    }

    #[test]
    fn flush_and_read() {
        let dir = TempDir::new().unwrap();
        let config = test_engine_config(dir.path());
        let engine = StorageEngine::open(config).unwrap();

        let m = test_mutation("ks", "t1", b"pk1", "name", b"bob");
        engine.apply_mutation(&m).unwrap();
        engine.flush_cf("ks.t1").unwrap();

        let stats = engine.stats();
        assert_eq!(stats.sstable_count, 1);
        assert_eq!(stats.flushes_completed, 1);

        let result = engine.read_partition("ks", "t1", b"pk1");
        assert!(result.is_some());
    }

    #[test]
    fn scan_all_partitions_merges_memtable_and_sstables() {
        let dir = TempDir::new().unwrap();
        let config = test_engine_config(dir.path());
        let engine = StorageEngine::open(config).unwrap();

        engine
            .apply_mutation(&test_mutation("ks", "t1", b"pk1", "name", b"flushed"))
            .unwrap();
        engine.flush_cf("ks.t1").unwrap();
        engine
            .apply_mutation(&test_mutation("ks", "t1", b"pk2", "name", b"memtable"))
            .unwrap();

        let partitions = engine.scan_all_partitions("ks", "t1");
        let keys: Vec<Vec<u8>> = partitions.into_iter().map(|(key, _)| key).collect();
        assert_eq!(keys, vec![b"pk1".to_vec(), b"pk2".to_vec()]);
    }

    #[test]
    fn read_partition_ignores_sstables_from_other_tables() {
        let dir = TempDir::new().unwrap();
        let config = test_engine_config(dir.path());
        let engine = StorageEngine::open(config).unwrap();

        engine
            .apply_mutation(&test_mutation("ks", "t1", b"same_pk", "name", b"table1"))
            .unwrap();
        engine.flush_cf("ks.t1").unwrap();

        assert!(engine.read_partition("ks", "t2", b"same_pk").is_none());
        assert!(engine.scan_all_partitions("ks", "t2").is_empty());
    }

    #[test]
    fn reopen_loads_sstables_from_nested_table_directories() {
        let dir = TempDir::new().unwrap();
        let config = test_engine_config(dir.path());
        {
            let engine = StorageEngine::open(config.clone()).unwrap();
            engine
                .apply_mutation(&test_mutation("ks", "t1", b"pk1", "name", b"nested"))
                .unwrap();
            engine.flush_cf("ks.t1").unwrap();
        }

        let nested = dir.path().join("ks").join("t1-abc123");
        fs::create_dir_all(&nested).unwrap();
        for entry in fs::read_dir(dir.path()).unwrap() {
            let entry = entry.unwrap();
            if entry.file_type().unwrap().is_file() {
                let file_name = entry.file_name();
                fs::rename(entry.path(), nested.join(file_name)).unwrap();
            }
        }

        let reopened = StorageEngine::open(config).unwrap();
        assert_eq!(reopened.stats().sstable_count, 1);
        let partition = reopened.read_partition("ks", "t1", b"pk1").unwrap();
        let row = partition.rows.values().next().unwrap();
        assert_eq!(row.cells[0].value.as_deref(), Some(b"nested".as_slice()));
    }

    #[test]
    fn rebuild_sai_index_from_active_memtable() {
        let dir = TempDir::new().unwrap();
        let config = test_engine_config(dir.path());
        let engine = StorageEngine::open(config).unwrap();

        engine
            .apply_mutation(&test_mutation("ks", "t1", b"pk1", "name", b"alice"))
            .unwrap();

        engine
            .rebuild_index(
                "ks.t1",
                IndexDefinition {
                    name: "idx_name".to_string(),
                    keyspace: "ks".to_string(),
                    table: "t1".to_string(),
                    column: "name".to_string(),
                    index_type: IndexType::Sai,
                    options: Default::default(),
                },
            )
            .unwrap();

        let results = engine
            .search_index("ks", "t1", "idx_name", b"alice")
            .unwrap();
        assert_eq!(results.len(), 1);
        let row = results[0].rows.values().next().unwrap();
        assert_eq!(row.cells[0].value.as_deref(), Some(b"alice".as_slice()));
    }

    #[test]
    fn rebuild_legacy_index_scans_only_target_table() {
        let dir = TempDir::new().unwrap();
        let config = test_engine_config(dir.path());
        let engine = StorageEngine::open(config).unwrap();

        engine
            .apply_mutation(&test_mutation("ks", "t1", b"same_pk", "name", b"alice"))
            .unwrap();
        engine.flush_cf("ks.t1").unwrap();
        engine
            .apply_mutation(&test_mutation("ks", "t2", b"same_pk", "name", b"bob"))
            .unwrap();
        engine.flush_cf("ks.t2").unwrap();

        engine
            .rebuild_index(
                "ks.t1",
                IndexDefinition {
                    name: "idx_name_legacy".to_string(),
                    keyspace: "ks".to_string(),
                    table: "t1".to_string(),
                    column: "name".to_string(),
                    index_type: IndexType::Legacy,
                    options: Default::default(),
                },
            )
            .unwrap();

        assert_eq!(
            engine
                .search_index("ks", "t1", "idx_name_legacy", b"alice")
                .unwrap()
                .len(),
            1
        );
        assert!(
            engine
                .search_index("ks", "t1", "idx_name_legacy", b"bob")
                .unwrap()
                .is_empty()
        );
    }

    #[test]
    fn truncate_table_discards_memtable_and_sstables() {
        let dir = TempDir::new().unwrap();
        let config = test_engine_config(dir.path());
        let engine = StorageEngine::open(config).unwrap();

        engine
            .apply_mutation(&test_mutation("ks", "t1", b"pk1", "name", b"flushed"))
            .unwrap();
        engine.flush_cf("ks.t1").unwrap();
        engine
            .apply_mutation(&test_mutation("ks", "t1", b"pk2", "name", b"memtable"))
            .unwrap();

        assert!(engine.read_partition("ks", "t1", b"pk1").is_some());
        assert!(engine.read_partition("ks", "t1", b"pk2").is_some());

        engine.truncate_table("ks", "t1").unwrap();

        assert!(engine.read_partition("ks", "t1", b"pk1").is_none());
        assert!(engine.read_partition("ks", "t1", b"pk2").is_none());
        assert_eq!(engine.stats().sstable_count, 0);
        assert!(engine.scan_all_partitions("ks", "t1").is_empty());
    }

    #[test]
    fn flush_all_cfs() {
        let dir = TempDir::new().unwrap();
        let config = test_engine_config(dir.path());
        let engine = StorageEngine::open(config).unwrap();

        engine
            .apply_mutation(&test_mutation("ks", "t1", b"pk1", "n", b"v1"))
            .unwrap();
        engine
            .apply_mutation(&test_mutation("ks", "t2", b"pk1", "n", b"v2"))
            .unwrap();
        engine.flush_all().unwrap();

        let stats = engine.stats();
        assert_eq!(stats.sstable_count, 2);
    }

    #[test]
    fn snapshot_lifecycle() {
        let dir = TempDir::new().unwrap();
        let config = test_engine_config(dir.path());
        let engine = StorageEngine::open(config).unwrap();

        engine
            .apply_mutation(&test_mutation("ks", "t1", b"pk1", "n", b"v1"))
            .unwrap();
        engine.flush_cf("ks.t1").unwrap();

        let manifest = engine
            .snapshot("test_snap", "ks", "t1", Some("CREATE TABLE t1 ...;"))
            .unwrap();
        assert_eq!(manifest.name, "test_snap");

        let snaps = engine.list_snapshots().unwrap();
        assert_eq!(snaps.len(), 1);

        engine.delete_snapshot("test_snap").unwrap();
        assert!(engine.list_snapshots().unwrap().is_empty());
    }

    #[test]
    fn commitlog_replay() {
        let dir = TempDir::new().unwrap();
        let config = test_engine_config(dir.path());

        // Write and sync
        {
            let engine = StorageEngine::open(config.clone()).unwrap();
            engine
                .apply_mutation(&test_mutation("ks", "t1", b"pk1", "n", b"v1"))
                .unwrap();
            engine
                .apply_mutation(&test_mutation("ks", "t1", b"pk2", "n", b"v2"))
                .unwrap();
            engine.sync_commitlog().unwrap();
        }

        // Reopen and replay
        let engine = StorageEngine::open(config).unwrap();
        let count = engine.replay_commitlog().unwrap();
        assert_eq!(count, 2);
    }

    #[test]
    fn bti_format_engine() {
        let dir = TempDir::new().unwrap();
        let mut config = test_engine_config(dir.path());
        config.sstable_format = SSTableFormat::Bti;

        let engine = StorageEngine::open(config).unwrap();
        engine
            .apply_mutation(&test_mutation("ks", "t1", b"pk1", "n", b"bti_val"))
            .unwrap();
        engine.flush_cf("ks.t1").unwrap();

        let result = engine.read_partition("ks", "t1", b"pk1");
        assert!(result.is_some());
        let pd = result.unwrap();
        let row = pd.rows.values().next().unwrap();
        assert_eq!(row.cells[0].value.as_deref(), Some(b"bti_val".as_slice()));
    }

    #[test]
    fn trie_memtable_engine() {
        let dir = TempDir::new().unwrap();
        let mut config = test_engine_config(dir.path());
        config.memtable_type = MemtableType::Trie;

        let engine = StorageEngine::open(config).unwrap();
        engine
            .apply_mutation(&test_mutation("ks", "t1", b"pk1", "n", b"trie_val"))
            .unwrap();

        let result = engine.read_partition("ks", "t1", b"pk1");
        assert!(result.is_some());
    }

    #[test]
    fn engine_stats() {
        let dir = TempDir::new().unwrap();
        let config = test_engine_config(dir.path());
        let engine = StorageEngine::open(config).unwrap();

        let stats = engine.stats();
        assert_eq!(stats.sstable_count, 0);
        assert_eq!(stats.flushes_completed, 0);
    }

    #[test]
    fn parse_toc_roundtrip() {
        let dir = Path::new("/data");
        let (desc, generation) = parse_toc_filename("ks-t1-big-42-TOC.txt", dir).unwrap();
        assert_eq!(generation, 42);
        assert_eq!(desc.keyspace, "ks");
        assert_eq!(desc.table, "t1");
        assert_eq!(desc.format, SSTableFormat::Big);

        let (desc2, generation2) = parse_toc_filename("ks-t1-bti-99-TOC.txt", dir).unwrap();
        assert_eq!(generation2, 99);
        assert_eq!(desc2.format, SSTableFormat::Bti);
    }

    // ── WU-15: Static cells in Mutation ──

    #[test]
    fn apply_mutation_with_static_cells() {
        let dir = TempDir::new().unwrap();
        let config = test_engine_config(dir.path());
        let engine = StorageEngine::open(config).unwrap();

        let mutation = Mutation {
            keyspace: "ks".to_string(),
            table: "t1".to_string(),
            partition_key: b"pk1".to_vec(),
            rows: vec![MutationRow {
                clustering_key: b"ck1".to_vec(),
                cells: vec![CellMutation {
                    column: "regular_col".to_string(),
                    value: Some(b"regular_val".to_vec()),
                    timestamp: 1000,
                    ttl: 0,
                    local_deletion_time: None,
                    is_tombstone: false,
                }],
                is_tombstone: false,
                local_deletion_time: None,
            }],
            timestamp: 1000,
            cdc_enabled: false,
            static_cells: vec![CellMutation {
                column: "static_col".to_string(),
                value: Some(b"static_val".to_vec()),
                timestamp: 1000,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            partition_tombstone: None,
            range_tombstones: Vec::new(),
        };

        engine.apply_mutation(&mutation).unwrap();

        let pd = engine.read_partition("ks", "t1", b"pk1").unwrap();
        // Should have 2 rows: one regular (ck1) and one static (empty ck)
        assert!(pd.rows.contains_key(&b"ck1".to_vec()));
        assert!(pd.rows.contains_key(&Vec::<u8>::new()));
        let static_row = pd.rows.get(&Vec::<u8>::new()).unwrap();
        assert_eq!(static_row.cells.len(), 1);
        assert_eq!(static_row.cells[0].column, "static_col");
    }

    // ── WU-17: Partition tombstone in Mutation ──

    #[test]
    fn apply_mutation_partition_tombstone() {
        let dir = TempDir::new().unwrap();
        let config = test_engine_config(dir.path());
        let engine = StorageEngine::open(config).unwrap();

        // First insert a row
        let insert = test_mutation("ks", "t1", b"pk1", "col", b"val");
        engine.apply_mutation(&insert).unwrap();

        // Verify the row exists
        let pd = engine.read_partition("ks", "t1", b"pk1").unwrap();
        assert!(!pd.rows.is_empty());

        // Now apply a partition tombstone
        let tombstone = Mutation {
            keyspace: "ks".to_string(),
            table: "t1".to_string(),
            partition_key: b"pk1".to_vec(),
            rows: Vec::new(),
            timestamp: 2000,
            cdc_enabled: false,
            static_cells: Vec::new(),
            partition_tombstone: Some(crate::commitlog::TombstoneMarker {
                timestamp: 2000,
                local_deletion_time: 2,
            }),
            range_tombstones: Vec::new(),
        };

        engine.apply_mutation(&tombstone).unwrap();

        // Verify partition tombstone was set
        let pd = engine.read_partition("ks", "t1", b"pk1").unwrap();
        assert_eq!(pd.tombstone_timestamp, Some(2000));
        assert_eq!(pd.tombstone_local_deletion_time, Some(2));
    }

    // ── WU-17: Range tombstone in Mutation ──

    #[test]
    fn apply_mutation_range_tombstone() {
        let dir = TempDir::new().unwrap();
        let config = test_engine_config(dir.path());
        let engine = StorageEngine::open(config).unwrap();

        // Insert two rows
        let mut m1 = test_mutation("ks", "t1", b"pk1", "col", b"val1");
        m1.rows[0].clustering_key = b"ck_a".to_vec();
        engine.apply_mutation(&m1).unwrap();

        let mut m2 = test_mutation("ks", "t1", b"pk1", "col", b"val2");
        m2.rows[0].clustering_key = b"ck_b".to_vec();
        engine.apply_mutation(&m2).unwrap();

        // Apply a range tombstone covering ck_a
        let range_ts = Mutation {
            keyspace: "ks".to_string(),
            table: "t1".to_string(),
            partition_key: b"pk1".to_vec(),
            rows: Vec::new(),
            timestamp: 3000,
            cdc_enabled: false,
            static_cells: Vec::new(),
            partition_tombstone: None,
            range_tombstones: vec![crate::commitlog::RangeTombstoneMarker {
                start: b"ck_a".to_vec(),
                end: b"ck_a".to_vec(),
                timestamp: 3000,
                local_deletion_time: 3,
            }],
        };

        engine.apply_mutation(&range_ts).unwrap();

        // The range tombstone creates a tombstone row at ck_a
        let pd = engine.read_partition("ks", "t1", b"pk1").unwrap();
        let row_a = pd.rows.get(&b"ck_a".to_vec()).unwrap();
        assert!(row_a.is_tombstone);
        // ck_b should still exist and not be a tombstone
        let row_b = pd.rows.get(&b"ck_b".to_vec()).unwrap();
        assert!(!row_b.is_tombstone);
    }
}
