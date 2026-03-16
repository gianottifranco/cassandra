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

use std::fs;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};

use parking_lot::RwLock;
use tracing::{debug, info, warn};

use crate::backup::{self, IncrementalBackupConfig};
use crate::commitlog::{
    CommitLog, CommitLogConfig, Mutation,
};
use crate::compaction::{
    CompactionMetrics, CompactionStrategy, CompactionStrategyType,
    SSTableMetadata, create_strategy, merge_partitions,
};
use crate::memtable::{MemtableManager, MemtableType};
use crate::memtable::partition::{Cell, PartitionData, Row};
use crate::sstable::format::{SSTableDescriptor, SSTableFormat, SSTableId};
use crate::sstable::{SSTableReader, SSTableWriter, BtiReader, BtiWriter};

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
            sstable_format: SSTableFormat::default(),
            memtable_type: MemtableType::default(),
            incremental_backup: IncrementalBackupConfig::default(),
        }
    }
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
        let compaction_strategy = create_strategy(config.compaction_strategy_type);

        // Load existing SSTables
        let (sstables, max_gen) = Self::load_existing_sstables(&config.data_directories)?;

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
        })
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
            memtable.apply(mutation.partition_key.clone(), row);
        }

        // 3. Check backpressure
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

        // Read from memtable
        let memtable = self.memtable_manager.get_or_create(&cf_name, 0);
        let mut result = memtable.get_partition(partition_key);

        // Read from SSTables (newest first)
        let sstables = self.sstables.read();
        for sst in sstables.iter().rev() {
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

        result
    }

    /// Flush a column family's memtable to disk.
    pub fn flush_cf(&self, cf_name: &str) -> Result<(), Box<dyn std::error::Error>> {
        let old_memtable = match self.memtable_manager.switch_memtable(
            cf_name,
            self.commitlog.current_segment_id(),
        ) {
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

        // Write SSTable using the configured format
        match self.config.sstable_format {
            SSTableFormat::Big => {
                let writer = SSTableWriter::new(descriptor.clone());
                writer.write(&partitions)?;
                let reader = SSTableReader::open(descriptor)?;
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
        let upper = old_memtable
            .commitlog_upper_bound
            .load(Ordering::Relaxed);
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

        let manifest = backup::create_snapshot(
            name, data_dir, keyspace, table, &all_files, schema_cql,
        )?;

        Ok(manifest)
    }

    /// List snapshots.
    pub fn list_snapshots(&self) -> Result<Vec<backup::SnapshotManifest>, Box<dyn std::error::Error>> {
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

    // ─── Internal helpers ──────────────────────────────────────────────

    fn compact_group(&self, group_ids: &[SSTableId]) -> Result<(), Box<dyn std::error::Error>> {
        let sstables = self.sstables.read();

        // Collect partitions from SSTables in the group
        let mut sources = Vec::new();
        let mut total_read_bytes = 0u64;

        for sst in sstables.iter() {
            if group_ids.contains(&sst.generation()) {
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

        // Write new SSTable
        let generation = self.next_generation.fetch_add(1, Ordering::SeqCst);
        let data_dir = &self.config.data_directories[0];
        let mut descriptor = SSTableDescriptor::new(data_dir, "compacted", "data", generation);
        descriptor.format = self.config.sstable_format;

        let written_bytes = match self.config.sstable_format {
            SSTableFormat::Big => {
                let writer = SSTableWriter::new(descriptor.clone());
                let stats = writer.write(&merged)?;
                stats.data_size
            }
            SSTableFormat::Bti => {
                let writer = BtiWriter::new(descriptor.clone());
                let stats = writer.write(&merged)?;
                stats.data_size
            }
        };

        // Open the new SSTable and replace old ones
        let new_handle = match self.config.sstable_format {
            SSTableFormat::Big => SSTableHandle::Big(SSTableReader::open(descriptor)?),
            SSTableFormat::Bti => SSTableHandle::Bti(BtiReader::open(descriptor)?),
        };

        {
            let mut sstables = self.sstables.write();
            sstables.retain(|sst| !group_ids.contains(&sst.generation()));
            sstables.push(new_handle);
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
            generation,
            merged_partitions = merged.len(),
            "Compaction complete"
        );

        Ok(())
    }

    fn maybe_backup(&self, files: &[PathBuf]) {
        if self.config.incremental_backup.enabled {
            if let Err(e) = backup::backup_sstable(
                &self.config.incremental_backup.directory,
                files,
            ) {
                warn!(error = %e, "Incremental backup failed");
            }
        }
    }

    fn load_existing_sstables(
        data_dirs: &[PathBuf],
    ) -> Result<(Vec<SSTableHandle>, u64), Box<dyn std::error::Error>> {
        let mut handles = Vec::new();
        let mut max_gen: u64 = 0;

        for data_dir in data_dirs {
            if !data_dir.exists() {
                continue;
            }
            for entry in fs::read_dir(data_dir)? {
                let entry = entry?;
                let fname = entry.file_name();
                let fname = fname.to_string_lossy();
                if fname.ends_with("-TOC.txt") {
                    // Parse descriptor from TOC filename
                    if let Some((desc, generation)) = parse_toc_filename(&fname, data_dir) {
                        max_gen = max_gen.max(generation);
                        match desc.format {
                            SSTableFormat::Big => {
                                match SSTableReader::open(desc) {
                                    Ok(reader) => handles.push(SSTableHandle::Big(reader)),
                                    Err(e) => warn!(file = %fname, error = %e, "Failed to open Big SSTable"),
                                }
                            }
                            SSTableFormat::Bti => {
                                match BtiReader::open(desc) {
                                    Ok(reader) => handles.push(SSTableHandle::Bti(reader)),
                                    Err(e) => warn!(file = %fname, error = %e, "Failed to open BTI SSTable"),
                                }
                            }
                        }
                    }
                }
            }
        }

        handles.sort_by_key(|sst| sst.generation());
        Ok((handles, max_gen))
    }
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
    use crate::commitlog::{MutationRow, CellMutation};
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
        }
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
    fn flush_all_cfs() {
        let dir = TempDir::new().unwrap();
        let config = test_engine_config(dir.path());
        let engine = StorageEngine::open(config).unwrap();

        engine.apply_mutation(&test_mutation("ks", "t1", b"pk1", "n", b"v1")).unwrap();
        engine.apply_mutation(&test_mutation("ks", "t2", b"pk1", "n", b"v2")).unwrap();
        engine.flush_all().unwrap();

        let stats = engine.stats();
        assert_eq!(stats.sstable_count, 2);
    }

    #[test]
    fn snapshot_lifecycle() {
        let dir = TempDir::new().unwrap();
        let config = test_engine_config(dir.path());
        let engine = StorageEngine::open(config).unwrap();

        engine.apply_mutation(&test_mutation("ks", "t1", b"pk1", "n", b"v1")).unwrap();
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
            engine.apply_mutation(&test_mutation("ks", "t1", b"pk1", "n", b"v1")).unwrap();
            engine.apply_mutation(&test_mutation("ks", "t1", b"pk2", "n", b"v2")).unwrap();
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
        engine.apply_mutation(&test_mutation("ks", "t1", b"pk1", "n", b"bti_val")).unwrap();
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
        engine.apply_mutation(&test_mutation("ks", "t1", b"pk1", "n", b"trie_val")).unwrap();

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
}
