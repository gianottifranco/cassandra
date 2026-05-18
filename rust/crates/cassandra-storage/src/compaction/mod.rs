// Licensed under Apache License, Version 2.0.

//! Compaction: merge multiple SSTables, resolve conflicts, expire tombstones.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy`
//! - `org.apache.cassandra.db.compaction.LeveledCompactionStrategy`
//! - `org.apache.cassandra.db.compaction.TimeWindowCompactionStrategy`
//! - `org.apache.cassandra.db.compaction.UnifiedCompactionStrategy`
//! - `org.apache.cassandra.db.compaction.CompactionManager`
//! - `org.apache.cassandra.db.compaction.CompactionIterator`
//!
//! ## Strategies
//!
//! | Strategy | Module  | Status       | Description                          |
//! |----------|---------|-------------|--------------------------------------|
//! | STCS     | (here)  | Functional  | Size-tiered: groups by similar size  |
//! | LCS      | lcs     | Functional  | Leveled: non-overlapping levels      |
//! | TWCS     | twcs    | Functional  | Time-window: groups by time window   |
//! | UCS      | ucs     | Experimental| Unified: adaptive tiered/leveled     |

pub mod active;
pub mod anticompaction;
pub mod controller;
pub mod errors;
pub mod iterator;
pub mod journal;
pub mod lcs;
pub mod leveled_manifest;
pub mod lifecycle;
pub mod logger;
pub mod manager;
pub mod pending_repair;
pub mod task;
pub mod twcs;
pub mod ucs;
pub mod validation;

use std::collections::{BTreeMap, HashMap};
use std::sync::atomic::{AtomicU64, Ordering};

use crate::memtable::partition::{PartitionData, Row};
use crate::sstable::format::SSTableId;

// ─── Strategy type enum ────────────────────────────────────────────────────

/// Compaction strategy selector.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize, Default)]
pub enum CompactionStrategyType {
    #[default]
    SizeTiered,
    Leveled,
    TimeWindow,
    Unified,
}

impl CompactionStrategyType {
    /// Parse the Java compaction strategy class name stored in table metadata.
    pub fn from_java_class(class_name: &str) -> Result<Self, String> {
        let simple_name = class_name.rsplit('.').next().unwrap_or(class_name);
        match simple_name {
            "SizeTieredCompactionStrategy" => Ok(Self::SizeTiered),
            "LeveledCompactionStrategy" => Ok(Self::Leveled),
            "TimeWindowCompactionStrategy" => Ok(Self::TimeWindow),
            "UnifiedCompactionStrategy" => Ok(Self::Unified),
            other => Err(format!("unsupported compaction strategy class {other}")),
        }
    }
}

/// Create a compaction strategy from the type enum.
pub fn create_strategy(strategy_type: CompactionStrategyType) -> Box<dyn CompactionStrategy> {
    match strategy_type {
        CompactionStrategyType::SizeTiered => Box::new(SizeTieredCompactionStrategy::default()),
        CompactionStrategyType::Leveled => Box::new(lcs::LeveledCompactionStrategy::default()),
        CompactionStrategyType::TimeWindow => {
            Box::new(twcs::TimeWindowCompactionStrategy::default())
        }
        CompactionStrategyType::Unified => Box::new(ucs::UnifiedCompactionStrategy::default()),
    }
}

/// Create a specific compaction strategy from Java table metadata options.
pub fn create_strategy_with_options(
    strategy_type: CompactionStrategyType,
    options: &HashMap<String, String>,
) -> Result<Box<dyn CompactionStrategy>, String> {
    match strategy_type {
        CompactionStrategyType::SizeTiered => Ok(Box::new(
            SizeTieredCompactionStrategy::from_options(options)?,
        )),
        CompactionStrategyType::Leveled => Ok(Box::new(
            lcs::LeveledCompactionStrategy::from_options(options)?,
        )),
        CompactionStrategyType::TimeWindow => Ok(Box::new(
            twcs::TimeWindowCompactionStrategy::from_options(options)?,
        )),
        CompactionStrategyType::Unified => Ok(Box::new(
            ucs::UnifiedCompactionStrategy::from_options(options)?,
        )),
    }
}

/// Create a compaction strategy from Java table metadata options.
pub fn create_strategy_from_options(
    options: &HashMap<String, String>,
) -> Result<Box<dyn CompactionStrategy>, String> {
    let strategy_type = options
        .get("class")
        .map(|class_name| CompactionStrategyType::from_java_class(class_name))
        .transpose()?
        .unwrap_or_default();

    create_strategy_with_options(strategy_type, options)
}

// ─── Compaction Strategy trait ─────────────────────────────────────────────

/// Metadata about an SSTable for compaction decisions.
#[derive(Debug, Clone)]
pub struct SSTableMetadata {
    pub id: SSTableId,
    pub data_size: u64,
    pub partition_count: u64,
    pub min_timestamp: i64,
    pub max_timestamp: i64,
}

/// Trait for compaction strategies.
pub trait CompactionStrategy: Send + Sync {
    /// Given current SSTables, return groups that should be compacted.
    fn pick_compaction(&self, sstables: &[SSTableMetadata]) -> Vec<Vec<SSTableId>>;
}

// ─── STCS ──────────────────────────────────────────────────────────────────

/// Size-Tiered Compaction Strategy.
#[derive(Debug, Clone)]
pub struct SizeTieredCompactionStrategy {
    /// Minimum number of SSTables in a bucket to trigger compaction.
    pub min_threshold: usize,
    /// Maximum number of SSTables to compact at once.
    pub max_threshold: usize,
    /// Bucket size ratio: SSTables within this ratio of each other
    /// are grouped together.
    pub bucket_low: f64,
    pub bucket_high: f64,
    /// Java `min_sstable_size`, retained for option parity.
    pub min_sstable_size: u64,
}

impl Default for SizeTieredCompactionStrategy {
    fn default() -> Self {
        Self {
            min_threshold: 4,
            max_threshold: 32,
            bucket_low: 0.5,
            bucket_high: 1.5,
            min_sstable_size: 50 * 1024 * 1024,
        }
    }
}

impl SizeTieredCompactionStrategy {
    pub fn from_options(options: &HashMap<String, String>) -> Result<Self, String> {
        let mut strategy = Self::default();
        if let Some(value) = options.get("min_threshold") {
            strategy.min_threshold = parse_positive_usize(value, "min_threshold")?;
        }
        if let Some(value) = options.get("max_threshold") {
            strategy.max_threshold = parse_positive_usize(value, "max_threshold")?;
        }
        if strategy.max_threshold < strategy.min_threshold {
            return Err("max_threshold must be greater than or equal to min_threshold".to_string());
        }
        if let Some(value) = options.get("bucket_low") {
            strategy.bucket_low = parse_positive_f64(value, "bucket_low")?;
        }
        if let Some(value) = options.get("bucket_high") {
            strategy.bucket_high = parse_positive_f64(value, "bucket_high")?;
        }
        if strategy.bucket_high <= strategy.bucket_low {
            return Err("bucket_high must be greater than bucket_low".to_string());
        }
        if let Some(value) = options.get("min_sstable_size") {
            strategy.min_sstable_size = parse_nonnegative_u64(value, "min_sstable_size")?;
        }
        Ok(strategy)
    }
}

impl CompactionStrategy for SizeTieredCompactionStrategy {
    fn pick_compaction(&self, sstables: &[SSTableMetadata]) -> Vec<Vec<SSTableId>> {
        if sstables.len() < self.min_threshold {
            return vec![];
        }

        let mut sorted: Vec<_> = sstables.to_vec();
        sorted.sort_by_key(|s| s.data_size);

        let mut buckets: Vec<Vec<&SSTableMetadata>> = Vec::new();

        for sst in &sorted {
            let mut placed = false;
            for bucket in &mut buckets {
                let avg_size: f64 =
                    bucket.iter().map(|s| s.data_size as f64).sum::<f64>() / bucket.len() as f64;
                let ratio = sst.data_size as f64 / avg_size;
                if ratio >= self.bucket_low && ratio <= self.bucket_high {
                    bucket.push(sst);
                    placed = true;
                    break;
                }
            }
            if !placed {
                buckets.push(vec![sst]);
            }
        }

        buckets
            .into_iter()
            .filter(|b| b.len() >= self.min_threshold)
            .map(|mut b| {
                b.truncate(self.max_threshold);
                b.iter().map(|s| s.id).collect()
            })
            .collect()
    }
}

pub(crate) fn parse_bool(value: &str, option: &str) -> Result<bool, String> {
    match value.trim().to_ascii_lowercase().as_str() {
        "true" => Ok(true),
        "false" => Ok(false),
        _ => Err(format!(
            "{option} should either be true or false, not {value}"
        )),
    }
}

pub(crate) fn parse_i64(value: &str, option: &str) -> Result<i64, String> {
    value
        .trim()
        .parse::<i64>()
        .map_err(|_| format!("{value} is not a parsable integer for {option}"))
}

pub(crate) fn parse_positive_usize(value: &str, option: &str) -> Result<usize, String> {
    let parsed = parse_i64(value, option)?;
    if parsed <= 0 {
        return Err(format!("{option} must be positive: {parsed}"));
    }
    Ok(parsed as usize)
}

pub(crate) fn parse_positive_u32(value: &str, option: &str) -> Result<u32, String> {
    let parsed = parse_positive_usize(value, option)?;
    if parsed > u32::MAX as usize {
        return Err(format!("{option} is out of range: {parsed}"));
    }
    Ok(parsed as u32)
}

pub(crate) fn parse_nonnegative_u64(value: &str, option: &str) -> Result<u64, String> {
    let parsed = parse_i64(value, option)?;
    if parsed < 0 {
        return Err(format!("{option} must be non-negative: {parsed}"));
    }
    Ok(parsed as u64)
}

pub(crate) fn parse_positive_f64(value: &str, option: &str) -> Result<f64, String> {
    let parsed = value
        .trim()
        .parse::<f64>()
        .map_err(|_| format!("{value} is not a parsable float for {option}"))?;
    if !parsed.is_finite() || parsed <= 0.0 {
        return Err(format!("{option} must be positive: {value}"));
    }
    Ok(parsed)
}

// ─── Expired SSTable Detection ─────────────────────────────────────────────

/// Check if an SSTable can be dropped entirely because all its data
/// has exceeded gc_grace (all timestamps + gc_grace < now).
pub fn is_fully_expired(sst: &SSTableMetadata, gc_grace_seconds: i32, now_seconds: i64) -> bool {
    let gc_grace_micros = gc_grace_seconds as i64 * 1_000_000;
    sst.max_timestamp + gc_grace_micros < now_seconds * 1_000_000
}

/// Detect SSTables composed entirely of tombstones that are past gc_grace.
/// Returns IDs of SSTables that can be dropped without compaction.
pub fn find_fully_expired(
    sstables: &[SSTableMetadata],
    gc_grace_seconds: i32,
    now_seconds: i64,
) -> Vec<SSTableId> {
    sstables
        .iter()
        .filter(|sst| is_fully_expired(sst, gc_grace_seconds, now_seconds))
        .map(|sst| sst.id)
        .collect()
}

/// Type alias for a collection of partitions with their keys.
pub type PartitionVec = Vec<(Vec<u8>, PartitionData)>;

/// Estimate a compacted partition's serialized size for output splitting.
///
/// This mirrors the SSTable rewriter accounting closely enough for deciding
/// when a Java-style size-capped compaction writer should rotate outputs.
pub fn estimate_compaction_output_size(key: &[u8], data: &PartitionData) -> u64 {
    let mut size = key.len() as u64 + 4;
    for row in data.rows.values() {
        size += row.clustering_key.len() as u64 + 10;
        for cell in &row.cells {
            size += cell.column.len() as u64 + 16;
            if let Some(ref value) = cell.value {
                size += value.len() as u64 + 4;
            }
            if cell.local_deletion_time.is_some() {
                size += 4;
            }
        }
        if row.local_deletion_time.is_some() {
            size += 4;
        }
    }
    size + 1
}

/// Split compacted partitions into output SSTable batches.
pub fn split_compaction_output(
    partitions: &[(Vec<u8>, PartitionData)],
    max_sstable_size: Option<u64>,
) -> Vec<PartitionVec> {
    let Some(max_size) = max_sstable_size.filter(|size| *size > 0) else {
        return if partitions.is_empty() {
            Vec::new()
        } else {
            vec![partitions.to_vec()]
        };
    };

    let mut batches = Vec::new();
    let mut current = Vec::new();
    let mut current_size = 0_u64;

    for (key, data) in partitions {
        let partition_size = estimate_compaction_output_size(key, data);
        if !current.is_empty() && current_size + partition_size > max_size {
            batches.push(current);
            current = Vec::new();
            current_size = 0;
        }
        current_size += partition_size;
        current.push((key.clone(), data.clone()));
    }

    if !current.is_empty() {
        batches.push(current);
    }

    batches
}

/// Token-range based split for anticompaction (repair).
/// Splits a set of partitions into two groups based on a range predicate.
pub fn anticompact_partitions<F>(
    partitions: Vec<(Vec<u8>, PartitionData)>,
    in_range: F,
) -> (PartitionVec, PartitionVec)
where
    F: Fn(&[u8]) -> bool,
{
    let mut inside = Vec::new();
    let mut outside = Vec::new();
    for (pk, pd) in partitions {
        if in_range(&pk) {
            inside.push((pk, pd));
        } else {
            outside.push((pk, pd));
        }
    }
    (inside, outside)
}

// ─── Compaction Metrics ────────────────────────────────────────────────────

/// Operational metrics for compaction.
#[derive(Debug, Default)]
pub struct CompactionMetrics {
    pub compactions_completed: AtomicU64,
    pub bytes_read: AtomicU64,
    pub bytes_written: AtomicU64,
    pub sstables_compacted: AtomicU64,
    pub tombstones_dropped: AtomicU64,
    pub expired_sstables_dropped: AtomicU64,
}

impl CompactionMetrics {
    pub fn snapshot(&self) -> CompactionMetricsSnapshot {
        CompactionMetricsSnapshot {
            compactions_completed: self.compactions_completed.load(Ordering::Relaxed),
            bytes_read: self.bytes_read.load(Ordering::Relaxed),
            bytes_written: self.bytes_written.load(Ordering::Relaxed),
            sstables_compacted: self.sstables_compacted.load(Ordering::Relaxed),
            tombstones_dropped: self.tombstones_dropped.load(Ordering::Relaxed),
            expired_sstables_dropped: self.expired_sstables_dropped.load(Ordering::Relaxed),
        }
    }
}

#[derive(Debug, Clone)]
pub struct CompactionMetricsSnapshot {
    pub compactions_completed: u64,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub sstables_compacted: u64,
    pub tombstones_dropped: u64,
    pub expired_sstables_dropped: u64,
}

// ─── Merge logic ───────────────────────────────────────────────────────────

/// Merge multiple sets of partitions (from different SSTables) into one.
/// Resolves cell conflicts by timestamp (last-write-wins).
/// Expires tombstones past gc_grace_seconds.
pub fn merge_partitions(
    sources: Vec<Vec<(Vec<u8>, PartitionData)>>,
    gc_grace_seconds: i32,
    now_seconds: i32,
) -> Vec<(Vec<u8>, PartitionData)> {
    let mut merged: BTreeMap<Vec<u8>, PartitionData> = BTreeMap::new();

    for source in sources {
        for (pk, partition) in source {
            let entry = merged.entry(pk).or_default();

            if let Some(ts) = partition.tombstone_timestamp {
                if let Some(ldt) = partition.tombstone_local_deletion_time {
                    entry.set_tombstone(ts, ldt);
                }
            }

            for (_ck, row) in partition.rows {
                entry.apply_row(row);
            }
        }
    }

    let gc_cutoff = now_seconds - gc_grace_seconds;

    let mut result: Vec<(Vec<u8>, PartitionData)> = Vec::new();

    for (pk, mut partition) in merged {
        if let Some(ldt) = partition.tombstone_local_deletion_time {
            if ldt <= gc_cutoff {
                partition.tombstone_timestamp = None;
                partition.tombstone_local_deletion_time = None;
            }
        }

        let mut purged_rows: BTreeMap<Vec<u8>, Row> = BTreeMap::new();

        for (ck, mut row) in partition.rows {
            if row.is_tombstone {
                if let Some(ldt) = row.local_deletion_time {
                    if ldt <= gc_cutoff {
                        continue;
                    }
                }
            }

            row.cells.retain(|cell| {
                if cell.is_tombstone {
                    if let Some(ldt) = cell.local_deletion_time {
                        return ldt > gc_cutoff;
                    }
                }
                if cell.ttl > 0 {
                    if let Some(ldt) = cell.local_deletion_time {
                        if now_seconds >= ldt && ldt <= gc_cutoff {
                            return false;
                        }
                    }
                }
                true
            });

            if !row.cells.is_empty() || row.is_tombstone {
                purged_rows.insert(ck, row);
            }
        }

        partition.rows = purged_rows;

        if !partition.rows.is_empty() || partition.tombstone_timestamp.is_some() {
            result.push((pk, partition));
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::partition::{Cell, PartitionData, Row};

    fn make_cell(col: &str, val: &[u8], ts: i64) -> Cell {
        Cell {
            column: col.to_string(),
            value: Some(val.to_vec()),
            timestamp: ts,
            ttl: 0,
            local_deletion_time: None,
            is_tombstone: false,
        }
    }

    fn make_tombstone_cell(col: &str, ts: i64, ldt: i32) -> Cell {
        Cell {
            column: col.to_string(),
            value: None,
            timestamp: ts,
            ttl: 0,
            local_deletion_time: Some(ldt),
            is_tombstone: true,
        }
    }

    fn make_row(ck: &[u8], cells: Vec<Cell>) -> Row {
        Row {
            clustering_key: ck.to_vec(),
            cells,
            is_tombstone: false,
            local_deletion_time: None,
        }
    }

    fn make_partition(rows: Vec<Row>) -> PartitionData {
        let mut pd = PartitionData::new();
        for row in rows {
            pd.apply_row(row);
        }
        pd
    }

    #[test]
    fn stcs_picks_correct_buckets() {
        let stcs = SizeTieredCompactionStrategy {
            min_threshold: 2,
            ..Default::default()
        };

        let sstables = vec![
            SSTableMetadata {
                id: 1,
                data_size: 100,
                partition_count: 10,
                min_timestamp: 0,
                max_timestamp: 100,
            },
            SSTableMetadata {
                id: 2,
                data_size: 110,
                partition_count: 10,
                min_timestamp: 0,
                max_timestamp: 100,
            },
            SSTableMetadata {
                id: 3,
                data_size: 105,
                partition_count: 10,
                min_timestamp: 0,
                max_timestamp: 100,
            },
            SSTableMetadata {
                id: 4,
                data_size: 10000,
                partition_count: 10,
                min_timestamp: 0,
                max_timestamp: 100,
            },
        ];

        let picks = stcs.pick_compaction(&sstables);
        assert!(!picks.is_empty());
        let first_group = &picks[0];
        assert!(first_group.len() >= 2);
        assert!(!first_group.contains(&4));
    }

    #[test]
    fn stcs_below_threshold() {
        let stcs = SizeTieredCompactionStrategy::default();
        let sstables = vec![SSTableMetadata {
            id: 1,
            data_size: 100,
            partition_count: 10,
            min_timestamp: 0,
            max_timestamp: 100,
        }];
        let picks = stcs.pick_compaction(&sstables);
        assert!(picks.is_empty());
    }

    #[test]
    fn merge_last_write_wins() {
        let source1 = vec![(
            b"pk1".to_vec(),
            make_partition(vec![make_row(b"ck1", vec![make_cell("name", b"old", 100)])]),
        )];
        let source2 = vec![(
            b"pk1".to_vec(),
            make_partition(vec![make_row(b"ck1", vec![make_cell("name", b"new", 200)])]),
        )];

        let merged = merge_partitions(vec![source1, source2], 86400, 1000);
        assert_eq!(merged.len(), 1);
        let row = merged[0].1.rows.get(&b"ck1".to_vec()).unwrap();
        assert_eq!(row.cells[0].value.as_deref(), Some(b"new".as_slice()));
    }

    #[test]
    fn merge_tombstone_gc() {
        let source = vec![(
            b"pk1".to_vec(),
            make_partition(vec![Row {
                clustering_key: b"ck1".to_vec(),
                cells: vec![make_tombstone_cell("name", 100, 100)],
                is_tombstone: false,
                local_deletion_time: None,
            }]),
        )];

        let merged = merge_partitions(vec![source], 1000, 2000);
        assert!(merged.is_empty());
    }

    #[test]
    fn merge_preserves_live_tombstones() {
        let source = vec![(
            b"pk1".to_vec(),
            make_partition(vec![Row {
                clustering_key: b"ck1".to_vec(),
                cells: vec![make_tombstone_cell("name", 100, 900)],
                is_tombstone: false,
                local_deletion_time: None,
            }]),
        )];

        let merged = merge_partitions(vec![source], 1000, 1000);
        assert_eq!(merged.len(), 1);
    }

    #[test]
    fn merge_multiple_sources_sorted_output() {
        let source1 = vec![
            (
                b"a".to_vec(),
                make_partition(vec![make_row(b"ck", vec![make_cell("x", b"1", 100)])]),
            ),
            (
                b"c".to_vec(),
                make_partition(vec![make_row(b"ck", vec![make_cell("x", b"3", 100)])]),
            ),
        ];
        let source2 = vec![(
            b"b".to_vec(),
            make_partition(vec![make_row(b"ck", vec![make_cell("x", b"2", 100)])]),
        )];

        let merged = merge_partitions(vec![source1, source2], 86400, 1000);
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[0].0, b"a");
        assert_eq!(merged[1].0, b"b");
        assert_eq!(merged[2].0, b"c");
    }

    #[test]
    fn compaction_output_split_preserves_order() {
        let partitions = vec![
            (
                b"a".to_vec(),
                make_partition(vec![make_row(b"ck", vec![make_cell("x", b"1111", 1)])]),
            ),
            (
                b"b".to_vec(),
                make_partition(vec![make_row(b"ck", vec![make_cell("x", b"2222", 1)])]),
            ),
            (
                b"c".to_vec(),
                make_partition(vec![make_row(b"ck", vec![make_cell("x", b"3333", 1)])]),
            ),
        ];
        let first_size = estimate_compaction_output_size(&partitions[0].0, &partitions[0].1);
        let batches = split_compaction_output(&partitions, Some(first_size + 1));

        assert_eq!(batches.len(), 3);
        assert_eq!(batches[0][0].0, b"a");
        assert_eq!(batches[1][0].0, b"b");
        assert_eq!(batches[2][0].0, b"c");
    }

    #[test]
    fn strategy_factory() {
        let stcs = create_strategy(CompactionStrategyType::SizeTiered);
        let lcs = create_strategy(CompactionStrategyType::Leveled);
        let twcs = create_strategy(CompactionStrategyType::TimeWindow);

        // Smoke test: all should handle empty input
        assert!(stcs.pick_compaction(&[]).is_empty());
        assert!(lcs.pick_compaction(&[]).is_empty());
        assert!(twcs.pick_compaction(&[]).is_empty());
    }

    #[test]
    fn parses_java_strategy_class_names() {
        assert_eq!(
            CompactionStrategyType::from_java_class("SizeTieredCompactionStrategy").unwrap(),
            CompactionStrategyType::SizeTiered
        );
        assert_eq!(
            CompactionStrategyType::from_java_class(
                "org.apache.cassandra.db.compaction.LeveledCompactionStrategy"
            )
            .unwrap(),
            CompactionStrategyType::Leveled
        );
        assert_eq!(
            CompactionStrategyType::from_java_class("TimeWindowCompactionStrategy").unwrap(),
            CompactionStrategyType::TimeWindow
        );
        assert_eq!(
            CompactionStrategyType::from_java_class(
                "org.apache.cassandra.db.compaction.UnifiedCompactionStrategy"
            )
            .unwrap(),
            CompactionStrategyType::Unified
        );
        assert!(CompactionStrategyType::from_java_class("UnknownStrategy").is_err());
    }

    #[test]
    fn parses_stcs_java_options() {
        let options = HashMap::from([
            ("min_threshold".to_string(), "2".to_string()),
            ("max_threshold".to_string(), "9".to_string()),
            ("bucket_low".to_string(), "0.4".to_string()),
            ("bucket_high".to_string(), "1.7".to_string()),
            ("min_sstable_size".to_string(), "1048576".to_string()),
        ]);
        let stcs = SizeTieredCompactionStrategy::from_options(&options).unwrap();
        assert_eq!(stcs.min_threshold, 2);
        assert_eq!(stcs.max_threshold, 9);
        assert!((stcs.bucket_low - 0.4).abs() < 0.001);
        assert!((stcs.bucket_high - 1.7).abs() < 0.001);
        assert_eq!(stcs.min_sstable_size, 1_048_576);
    }

    #[test]
    fn strategy_factory_accepts_java_options() {
        let options = HashMap::from([
            (
                "class".to_string(),
                "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy".to_string(),
            ),
            ("min_threshold".to_string(), "2".to_string()),
        ]);
        let strategy = create_strategy_from_options(&options).unwrap();

        let sstables: Vec<SSTableMetadata> = (1..=2)
            .map(|id| SSTableMetadata {
                id,
                data_size: 100,
                partition_count: 10,
                min_timestamp: 0,
                max_timestamp: 100,
            })
            .collect();
        assert_eq!(strategy.pick_compaction(&sstables), vec![vec![1, 2]]);
    }

    #[test]
    fn strategy_factory_accepts_unified_java_options_without_feature_gate() {
        let options = HashMap::from([
            (
                "class".to_string(),
                "org.apache.cassandra.db.compaction.UnifiedCompactionStrategy".to_string(),
            ),
            ("scaling_parameters".to_string(), "T4, L10".to_string()),
            ("target_sstable_size".to_string(), "128MiB".to_string()),
            ("min_threshold".to_string(), "2".to_string()),
            ("max_threshold".to_string(), "4".to_string()),
        ]);
        let strategy = create_strategy_from_options(&options).unwrap();

        let sstables: Vec<SSTableMetadata> = (1..=4)
            .map(|id| SSTableMetadata {
                id,
                data_size: 128 * 1024 * 1024,
                partition_count: 1024,
                min_timestamp: 0,
                max_timestamp: 100,
            })
            .collect();
        assert_eq!(strategy.pick_compaction(&sstables), vec![vec![1, 2, 3, 4]]);
    }

    #[test]
    fn anticompaction_splits() {
        let partitions = vec![
            (
                b"a".to_vec(),
                make_partition(vec![make_row(b"ck", vec![make_cell("x", b"1", 100)])]),
            ),
            (
                b"b".to_vec(),
                make_partition(vec![make_row(b"ck", vec![make_cell("x", b"2", 100)])]),
            ),
            (
                b"c".to_vec(),
                make_partition(vec![make_row(b"ck", vec![make_cell("x", b"3", 100)])]),
            ),
        ];

        let (inside, outside) = anticompact_partitions(partitions, |pk| pk == b"b");
        assert_eq!(inside.len(), 1);
        assert_eq!(inside[0].0, b"b");
        assert_eq!(outside.len(), 2);
    }

    #[test]
    fn expired_sstable_detection() {
        let sst = SSTableMetadata {
            id: 1,
            data_size: 100,
            partition_count: 10,
            min_timestamp: 0,
            max_timestamp: 100_000_000, // 100 seconds in micros
        };
        // gc_grace = 86400, now = 200_000 seconds
        assert!(is_fully_expired(&sst, 86400, 200_000));
        // gc_grace = 86400, now = 100 seconds
        assert!(!is_fully_expired(&sst, 86400, 100));
    }
}
