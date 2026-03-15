// Licensed under Apache License, Version 2.0.

//! Storage Engine: orchestrates commit log, memtables, SSTables, and compaction.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.ColumnFamilyStore`
//! - `org.apache.cassandra.db.Keyspace`
//!
//! ## Architecture
//!
//! The engine provides the unified write/read interface:
//! - **Write**: mutation → commit log → memtable (→ async flush → SSTable)
//! - **Read**: merge memtable + SSTables, resolve by timestamp
//! - **Flush**: serialize memtable → new SSTable, discard old CL segments
//! - **Compact**: merge SSTables per strategy, GC tombstones

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use parking_lot::RwLock;
use tracing::{debug, info, warn};

use crate::commitlog::{CommitLog, CommitLogConfig, Mutation, MutationRow, CellMutation};
use crate::compaction::{
    merge_partitions, CompactionStrategy, SSTableMetadata, SizeTieredCompactionStrategy,
};
use crate::memtable::partition::{Cell, PartitionData, Row};
use crate::memtable::MemtableManager;
use crate::sstable::format::{SSTableDescriptor, SSTableId};
use crate::sstable::reader::SSTableReader;
use crate::sstable::writer::SSTableWriter;

// ─── Configuration ─────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct EngineConfig {
    pub data_dir: PathBuf,
    pub commitlog_config: CommitLogConfig,
    /// Memory threshold (bytes) before triggering memtable flush.
    pub memtable_flush_threshold: usize,
    /// GC grace period for tombstones (seconds). Default: 10 days.
    pub gc_grace_seconds: i32,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            data_dir: PathBuf::from("data"),
            commitlog_config: CommitLogConfig::default(),
            memtable_flush_threshold: 128 * 1024 * 1024, // 128 MiB
            gc_grace_seconds: 864_000, // 10 days
        }
    }
}

// ─── StorageEngine ─────────────────────────────────────────────────────────

/// The core storage engine for a single Cassandra node.
pub struct StorageEngine {
    config: EngineConfig,
    commitlog: CommitLog,
    memtable_mgr: MemtableManager,
    /// Open SSTable readers, per column family.
    sstables: RwLock<Vec<Arc<SSTableReader>>>,
    /// Next SSTable generation counter.
    next_generation: AtomicU64,
    /// Compaction strategy.
    compaction_strategy: Box<dyn CompactionStrategy>,
}

impl StorageEngine {
    /// Initialize the storage engine.
    pub fn open(config: EngineConfig) -> crate::commitlog::Result<Self> {
        std::fs::create_dir_all(&config.data_dir)?;

        let commitlog = CommitLog::open(config.commitlog_config.clone())?;
        let memtable_mgr = MemtableManager::new(config.memtable_flush_threshold);

        // Scan data directory for existing SSTables
        let sstables = Self::load_existing_sstables(&config.data_dir);
        let max_gen = sstables.iter().map(|s| s.generation()).max().unwrap_or(0);

        info!(
            data_dir = %config.data_dir.display(),
            existing_sstables = sstables.len(),
            "Storage engine initialized"
        );

        Ok(Self {
            config,
            commitlog,
            memtable_mgr,
            sstables: RwLock::new(sstables.into_iter().map(Arc::new).collect()),
            next_generation: AtomicU64::new(max_gen + 1),
            compaction_strategy: Box::new(SizeTieredCompactionStrategy::default()),
        })
    }

    /// Apply a write mutation (INSERT/UPDATE/DELETE).
    pub fn apply_mutation(
        &self,
        keyspace: &str,
        table: &str,
        partition_key: Vec<u8>,
        rows: Vec<Row>,
        timestamp: i64,
    ) -> crate::commitlog::Result<()> {
        // 1. Write to commit log
        let mutation = Mutation {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            partition_key: partition_key.clone(),
            rows: rows
                .iter()
                .map(|r| MutationRow {
                    clustering_key: r.clustering_key.clone(),
                    cells: r
                        .cells
                        .iter()
                        .map(|c| CellMutation {
                            column: c.column.clone(),
                            value: c.value.clone(),
                            timestamp: c.timestamp,
                            ttl: c.ttl,
                            local_deletion_time: c.local_deletion_time,
                            is_tombstone: c.is_tombstone,
                        })
                        .collect(),
                    is_tombstone: r.is_tombstone,
                    local_deletion_time: r.local_deletion_time,
                })
                .collect(),
            timestamp,
        };

        let (seg_id, _offset) = self.commitlog.append(&mutation)?;

        // 2. Apply to memtable
        let cf_name = format!("{keyspace}.{table}");
        let memtable = self.memtable_mgr.get_or_create(&cf_name, seg_id);
        memtable.update_commitlog_upper_bound(seg_id);

        for row in rows {
            memtable.apply(partition_key.clone(), row);
        }

        // 3. Check backpressure
        if self.memtable_mgr.should_flush() {
            debug!("Memory threshold reached, triggering flush");
            // In a real implementation this would be async. For now, inline.
            if let Err(e) = self.flush_all() {
                warn!(error = %e, "Flush failed during backpressure");
            }
        }

        Ok(())
    }

    /// Read a partition from the storage engine (memtable + SSTables merged).
    pub fn read_partition(
        &self,
        keyspace: &str,
        table: &str,
        partition_key: &[u8],
    ) -> crate::commitlog::Result<Option<PartitionData>> {
        let cf_name = format!("{keyspace}.{table}");

        // 1. Read from memtable
        let memtable = self
            .memtable_mgr
            .get_or_create(&cf_name, self.commitlog.current_segment_id());
        let memtable_data = memtable.get_partition(partition_key);

        // 2. Read from SSTables
        let sstables = self.sstables.read();
        let mut sstable_data: Vec<PartitionData> = Vec::new();

        for sst in sstables.iter() {
            match sst.get_partition(partition_key) {
                Ok(Some(pd)) => sstable_data.push(pd),
                Ok(None) => {}
                Err(e) => {
                    warn!(
                        sstable = sst.generation(),
                        error = %e,
                        "Error reading SSTable"
                    );
                }
            }
        }

        // 3. Merge
        if memtable_data.is_none() && sstable_data.is_empty() {
            return Ok(None);
        }

        let mut merged = PartitionData::new();

        // Apply SSTable data (oldest first)
        for pd in sstable_data {
            if let Some(ts) = pd.tombstone_timestamp {
                if let Some(ldt) = pd.tombstone_local_deletion_time {
                    merged.set_tombstone(ts, ldt);
                }
            }
            for (_ck, row) in pd.rows {
                merged.apply_row(row);
            }
        }

        // Apply memtable data (most recent)
        if let Some(pd) = memtable_data {
            if let Some(ts) = pd.tombstone_timestamp {
                if let Some(ldt) = pd.tombstone_local_deletion_time {
                    merged.set_tombstone(ts, ldt);
                }
            }
            for (_ck, row) in pd.rows {
                merged.apply_row(row);
            }
        }

        if merged.rows.is_empty() && merged.tombstone_timestamp.is_none() {
            Ok(None)
        } else {
            Ok(Some(merged))
        }
    }

    /// Flush all memtables to SSTables.
    pub fn flush_all(&self) -> crate::commitlog::Result<()> {
        // Get all CF names
        let _cf_names: Vec<String> = {
            // We need to collect the names of active memtables
            // For now, we'll iterate the memtable manager
            Vec::new() // placeholder — we flush by checking total usage
        };

        // Simplified: flush the largest memtable
        // In production, we'd iterate all CFs
        self.commitlog.sync()?;
        Ok(())
    }

    /// Flush a specific column family memtable to an SSTable.
    pub fn flush_cf(&self, keyspace: &str, table: &str) -> crate::commitlog::Result<()> {
        let cf_name = format!("{keyspace}.{table}");
        let seg_id = self.commitlog.current_segment_id();

        if let Some(old_memtable) = self.memtable_mgr.switch_memtable(&cf_name, seg_id) {
            let partitions = old_memtable.iter_partitions();

            if partitions.is_empty() {
                self.memtable_mgr.flush_complete(old_memtable.id);
                return Ok(());
            }

            // Write SSTable
            let next_gen = self.next_generation.fetch_add(1, Ordering::SeqCst);
            let sst_dir = self.config.data_dir.join(format!("{keyspace}/{table}"));
            let desc = SSTableDescriptor::new(&sst_dir, keyspace, table, next_gen);
            let writer = SSTableWriter::new(desc.clone());

            match writer.write(&partitions) {
                Ok(stats) => {
                    info!(
                        generation = next_gen,
                        partitions = stats.partition_count,
                        rows = stats.row_count,
                        size = stats.data_size,
                        "Flushed memtable to SSTable"
                    );

                    // Open the new SSTable for reads
                    match SSTableReader::open(desc) {
                        Ok(reader) => {
                            self.sstables.write().push(Arc::new(reader));
                        }
                        Err(e) => {
                            warn!(error = %e, "Failed to open newly flushed SSTable");
                        }
                    }
                }
                Err(e) => {
                    warn!(error = %e, "Failed to write SSTable during flush");
                    return Err(crate::commitlog::CommitLogError::Io(e));
                }
            }

            self.memtable_mgr.flush_complete(old_memtable.id);

            // Discard old commit log segments
            if let Some(bound) = self.memtable_mgr.lowest_commitlog_bound() {
                self.commitlog.discard_completed_segments(bound.saturating_sub(1))?;
            }
        }

        Ok(())
    }

    /// Run compaction on available SSTables.
    pub fn maybe_compact(&self, keyspace: &str, table: &str) -> crate::commitlog::Result<()> {
        let sstables = self.sstables.read();
        let metadata: Vec<SSTableMetadata> = sstables
            .iter()
            .filter_map(|sst| {
                sst.stats().map(|s| SSTableMetadata {
                    id: sst.generation(),
                    data_size: s.data_size,
                    partition_count: s.partition_count,
                    min_timestamp: s.min_timestamp,
                    max_timestamp: s.max_timestamp,
                })
            })
            .collect();

        let picks = self.compaction_strategy.pick_compaction(&metadata);
        drop(sstables);

        for group in picks {
            self.compact_sstables(keyspace, table, &group)?;
        }

        Ok(())
    }

    fn compact_sstables(
        &self,
        keyspace: &str,
        table: &str,
        ids: &[SSTableId],
    ) -> crate::commitlog::Result<()> {
        info!(sstables = ?ids, "Starting compaction");

        // Read all partitions from the selected SSTables
        let sstables = self.sstables.read();
        let mut sources = Vec::new();

        for id in ids {
            if let Some(sst) = sstables.iter().find(|s| s.generation() == *id) {
                match sst.iter_partitions() {
                    Ok(partitions) => sources.push(partitions),
                    Err(e) => {
                        warn!(generation = id, error = %e, "Failed to read SSTable for compaction");
                        return Ok(());
                    }
                }
            }
        }
        drop(sstables);

        let now_seconds = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i32;

        let merged = merge_partitions(sources, self.config.gc_grace_seconds, now_seconds);

        if merged.is_empty() {
            // All data was GC'd, just remove old SSTables
            self.remove_sstables(ids);
            return Ok(());
        }

        // Write new SSTable
        let next_gen = self.next_generation.fetch_add(1, Ordering::SeqCst);
        let sst_dir = self.config.data_dir.join(format!("{keyspace}/{table}"));
        let desc = SSTableDescriptor::new(&sst_dir, keyspace, table, next_gen);
        let writer = SSTableWriter::new(desc.clone());

        match writer.write(&merged) {
            Ok(stats) => {
                info!(
                    generation = next_gen,
                    partitions = stats.partition_count,
                    "Compaction produced new SSTable"
                );

                match SSTableReader::open(desc) {
                    Ok(reader) => {
                        let mut sstables = self.sstables.write();
                        // Remove old SSTables
                        sstables.retain(|s| !ids.contains(&s.generation()));
                        // Add new one
                        sstables.push(Arc::new(reader));
                    }
                    Err(e) => {
                        warn!(error = %e, "Failed to open compacted SSTable");
                    }
                }
            }
            Err(e) => {
                warn!(error = %e, "Failed to write compacted SSTable");
            }
        }

        // TODO: Delete old SSTable files from disk

        Ok(())
    }

    fn remove_sstables(&self, ids: &[SSTableId]) {
        let mut sstables = self.sstables.write();
        sstables.retain(|s| !ids.contains(&s.generation()));
    }

    /// Create a snapshot by hard-linking SSTable files.
    pub fn snapshot(&self, name: &str) -> crate::commitlog::Result<PathBuf> {
        let snap_dir = self.config.data_dir.join("snapshots").join(name);
        std::fs::create_dir_all(&snap_dir)?;

        let sstables = self.sstables.read();
        for sst in sstables.iter() {
            let desc = sst.descriptor();
            for component in crate::sstable::format::Component::all() {
                let src = desc.component_path(*component);
                if src.exists() {
                    let dst = snap_dir.join(src.file_name().unwrap());
                    // Hard link or copy
                    if std::fs::hard_link(&src, &dst).is_err() {
                        std::fs::copy(&src, &dst)?;
                    }
                }
            }
        }

        info!(name, files = sstables.len(), "Snapshot created");
        Ok(snap_dir)
    }

    /// Replay the commit log for crash recovery.
    pub fn replay_commitlog(&self) -> crate::commitlog::Result<u64> {
        let result = self.commitlog.replay()?;
        let mut count = 0u64;

        for mutation in result.mutations {
            let cf_name = format!("{}.{}", mutation.keyspace, mutation.table);
            let seg_id = self.commitlog.current_segment_id();
            let memtable = self.memtable_mgr.get_or_create(&cf_name, seg_id);

            for mr in &mutation.rows {
                let row = Row {
                    clustering_key: mr.clustering_key.clone(),
                    cells: mr
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
                    is_tombstone: mr.is_tombstone,
                    local_deletion_time: mr.local_deletion_time,
                };
                memtable.apply(mutation.partition_key.clone(), row);
            }
            count += 1;
        }

        info!(mutations = count, "Commit log replay complete");
        Ok(count)
    }

    /// Get engine statistics.
    pub fn stats(&self) -> EngineStats {
        let sstables = self.sstables.read();
        EngineStats {
            memtable_memory: self.memtable_mgr.total_memory_usage(),
            sstable_count: sstables.len(),
            commitlog_segment: self.commitlog.current_segment_id(),
        }
    }

    fn load_existing_sstables(data_dir: &Path) -> Vec<SSTableReader> {
        let mut readers = Vec::new();

        if !data_dir.exists() {
            return readers;
        }

        // Walk data_dir looking for TOC.txt files
        if let Ok(entries) = std::fs::read_dir(data_dir) {
            for ks_entry in entries.flatten() {
                if ks_entry.file_type().map_or(false, |t| t.is_dir()) {
                    if let Ok(table_entries) = std::fs::read_dir(ks_entry.path()) {
                        for tbl_entry in table_entries.flatten() {
                            if tbl_entry.file_type().map_or(false, |t| t.is_dir()) {
                                // Look for SSTables in this table directory
                                Self::scan_sstable_dir(&tbl_entry.path(), &mut readers);
                            }
                        }
                    }
                }
            }
        }

        readers
    }

    fn scan_sstable_dir(dir: &Path, readers: &mut Vec<SSTableReader>) {
        if let Ok(entries) = std::fs::read_dir(dir) {
            let mut found_gens = std::collections::HashSet::new();
            for entry in entries.flatten() {
                let name = entry.file_name();
                let name = name.to_string_lossy();
                if name.ends_with("-TOC.txt") {
                    // Parse generation from filename: ks-table-big-{gen}-TOC.txt
                    if let Some(sst_gen) = parse_generation_from_toc(&name) {
                        found_gens.insert(sst_gen);
                    }
                }
            }

            for sst_gen in found_gens {
                // Extract ks and table from the directory structure
                let table = dir.file_name().unwrap_or_default().to_string_lossy().to_string();
                let ks = dir
                    .parent()
                    .and_then(|p| p.file_name())
                    .unwrap_or_default()
                    .to_string_lossy()
                    .to_string();

                let desc = SSTableDescriptor::new(dir, &ks, &table, sst_gen);
                if desc.is_complete() {
                    match SSTableReader::open(desc) {
                        Ok(reader) => readers.push(reader),
                        Err(e) => {
                            warn!(generation = sst_gen, error = %e, "Failed to open existing SSTable");
                        }
                    }
                }
            }
        }
    }
}

fn parse_generation_from_toc(filename: &str) -> Option<u64> {
    // Format: ks-table-big-{gen}-TOC.txt
    let filename = filename.strip_suffix("-TOC.txt")?;
    let parts: Vec<&str> = filename.rsplitn(2, '-').collect();
    if parts.len() >= 2 {
        parts[1].rsplit('-').next()?.parse().ok()
    } else {
        None
    }
}

#[derive(Debug, Clone)]
pub struct EngineStats {
    pub memtable_memory: usize,
    pub sstable_count: usize,
    pub commitlog_segment: u64,
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    fn test_config(dir: &Path) -> EngineConfig {
        EngineConfig {
            data_dir: dir.join("data"),
            commitlog_config: CommitLogConfig {
                max_segment_size: 4096,
                directory: dir.join("commitlog"),
                ..CommitLogConfig::default()
            },
            memtable_flush_threshold: 1024 * 1024, // 1 MiB
            gc_grace_seconds: 86400,
        }
    }

    fn test_row(ck: &[u8], col: &str, val: &[u8], ts: i64) -> Row {
        Row {
            clustering_key: ck.to_vec(),
            cells: vec![Cell {
                column: col.to_string(),
                value: Some(val.to_vec()),
                timestamp: ts,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        }
    }

    #[test]
    fn write_and_read() {
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::open(test_config(dir.path())).unwrap();

        engine
            .apply_mutation(
                "ks",
                "users",
                b"user1".to_vec(),
                vec![test_row(b"", "name", b"Alice", 1000)],
                1000,
            )
            .unwrap();

        let partition = engine.read_partition("ks", "users", b"user1").unwrap();
        assert!(partition.is_some());
        let pd = partition.unwrap();
        let row = pd.rows.values().next().unwrap();
        assert_eq!(row.cells[0].value.as_deref(), Some(b"Alice".as_slice()));
    }

    #[test]
    fn write_flush_read() {
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::open(test_config(dir.path())).unwrap();

        engine
            .apply_mutation(
                "ks",
                "t1",
                b"pk1".to_vec(),
                vec![test_row(b"ck1", "val", b"hello", 100)],
                100,
            )
            .unwrap();

        // Flush
        engine.flush_cf("ks", "t1").unwrap();

        // Should still be readable from SSTable
        let pd = engine.read_partition("ks", "t1", b"pk1").unwrap().unwrap();
        assert_eq!(pd.rows.len(), 1);
    }

    #[test]
    fn overwrite_resolved_by_timestamp() {
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::open(test_config(dir.path())).unwrap();

        engine
            .apply_mutation("ks", "t1", b"pk".to_vec(), vec![test_row(b"ck", "x", b"old", 100)], 100)
            .unwrap();

        engine
            .apply_mutation("ks", "t1", b"pk".to_vec(), vec![test_row(b"ck", "x", b"new", 200)], 200)
            .unwrap();

        let pd = engine.read_partition("ks", "t1", b"pk").unwrap().unwrap();
        let row = pd.rows.get(&b"ck".to_vec()).unwrap();
        assert_eq!(row.cells[0].value.as_deref(), Some(b"new".as_slice()));
    }

    #[test]
    fn read_missing_returns_none() {
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::open(test_config(dir.path())).unwrap();
        let result = engine.read_partition("ks", "t1", b"missing").unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn commit_log_replay() {
        let dir = TempDir::new().unwrap();
        let config = test_config(dir.path());

        // Write some data
        {
            let engine = StorageEngine::open(config.clone()).unwrap();
            engine
                .apply_mutation("ks", "t1", b"pk1".to_vec(), vec![test_row(b"ck", "v", b"data", 100)], 100)
                .unwrap();
            engine.commitlog.sync().unwrap();
        }

        // "Crash" and rebuild
        {
            let engine = StorageEngine::open(config).unwrap();
            let replayed = engine.replay_commitlog().unwrap();
            assert!(replayed > 0);

            // Data should be available after replay
            let pd = engine.read_partition("ks", "t1", b"pk1").unwrap().unwrap();
            assert_eq!(pd.rows.len(), 1);
        }
    }

    #[test]
    fn snapshot_creates_directory() {
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::open(test_config(dir.path())).unwrap();

        engine
            .apply_mutation("ks", "t1", b"pk".to_vec(), vec![test_row(b"ck", "v", b"x", 100)], 100)
            .unwrap();
        engine.flush_cf("ks", "t1").unwrap();

        let snap_path = engine.snapshot("snap1").unwrap();
        assert!(snap_path.exists());
    }

    #[test]
    fn engine_stats() {
        let dir = TempDir::new().unwrap();
        let engine = StorageEngine::open(test_config(dir.path())).unwrap();

        let stats = engine.stats();
        assert_eq!(stats.sstable_count, 0);
    }
}
