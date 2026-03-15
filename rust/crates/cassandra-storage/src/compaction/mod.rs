// Licensed under Apache License, Version 2.0.

//! Compaction: merge multiple SSTables, resolve conflicts, expire tombstones.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy`
//! - `org.apache.cassandra.db.compaction.CompactionManager`
//! - `org.apache.cassandra.db.compaction.CompactionIterator`
//!
//! ## Strategy: Size-Tiered (STCS)
//!
//! Groups SSTables by similar size into buckets. When a bucket reaches
//! a threshold (default 4), all SSTables in that bucket are compacted
//! together into a single new SSTable.

use std::collections::BTreeMap;

use crate::memtable::partition::{PartitionData, Row};
use crate::sstable::format::SSTableId;

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
}

impl Default for SizeTieredCompactionStrategy {
    fn default() -> Self {
        Self {
            min_threshold: 4,
            max_threshold: 32,
            bucket_low: 0.5,
            bucket_high: 1.5,
        }
    }
}

impl CompactionStrategy for SizeTieredCompactionStrategy {
    fn pick_compaction(&self, sstables: &[SSTableMetadata]) -> Vec<Vec<SSTableId>> {
        if sstables.len() < self.min_threshold {
            return vec![];
        }

        // Sort by size
        let mut sorted: Vec<_> = sstables.to_vec();
        sorted.sort_by_key(|s| s.data_size);

        // Group into size buckets
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

        // Return buckets that meet the threshold
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

// ─── Merge logic ───────────────────────────────────────────────────────────

/// Merge multiple sets of partitions (from different SSTables) into one.
/// Resolves cell conflicts by timestamp (last-write-wins).
/// Expires tombstones past gc_grace_seconds.
pub fn merge_partitions(
    sources: Vec<Vec<(Vec<u8>, PartitionData)>>,
    gc_grace_seconds: i32,
    now_seconds: i32,
) -> Vec<(Vec<u8>, PartitionData)> {
    // Collect all partitions into a merged map
    let mut merged: BTreeMap<Vec<u8>, PartitionData> = BTreeMap::new();

    for source in sources {
        for (pk, partition) in source {
            let entry = merged.entry(pk).or_insert_with(PartitionData::new);

            // Merge partition-level tombstones
            if let Some(ts) = partition.tombstone_timestamp {
                if let Some(ldt) = partition.tombstone_local_deletion_time {
                    entry.set_tombstone(ts, ldt);
                }
            }

            // Merge rows
            for (_ck, row) in partition.rows {
                entry.apply_row(row);
            }
        }
    }

    // Purge expired tombstones and TTLed cells
    let gc_cutoff = now_seconds - gc_grace_seconds;

    let mut result: Vec<(Vec<u8>, PartitionData)> = Vec::new();

    for (pk, mut partition) in merged {
        // Check if partition tombstone can be GC'd
        if let Some(ldt) = partition.tombstone_local_deletion_time {
            if ldt <= gc_cutoff {
                partition.tombstone_timestamp = None;
                partition.tombstone_local_deletion_time = None;
            }
        }

        // Filter rows
        let mut purged_rows: BTreeMap<Vec<u8>, Row> = BTreeMap::new();

        for (ck, mut row) in partition.rows {
            // GC row tombstones
            if row.is_tombstone {
                if let Some(ldt) = row.local_deletion_time {
                    if ldt <= gc_cutoff {
                        continue; // Drop this row entirely
                    }
                }
            }

            // Filter cells: remove GC'd tombstones and expired TTL cells
            row.cells.retain(|cell| {
                if cell.is_tombstone {
                    if let Some(ldt) = cell.local_deletion_time {
                        return ldt > gc_cutoff;
                    }
                }
                if cell.ttl > 0 {
                    if let Some(ldt) = cell.local_deletion_time {
                        // If TTL expired AND past gc_grace, drop it
                        if now_seconds >= ldt && ldt <= gc_cutoff {
                            return false;
                        }
                    }
                }
                true
            });

            // Keep the row if it still has cells or is a live tombstone
            if !row.cells.is_empty() || row.is_tombstone {
                purged_rows.insert(ck, row);
            }
        }

        partition.rows = purged_rows;

        // Keep partition if it has any content
        if !partition.rows.is_empty()
            || partition.tombstone_timestamp.is_some()
        {
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
            SSTableMetadata { id: 1, data_size: 100, partition_count: 10, min_timestamp: 0, max_timestamp: 100 },
            SSTableMetadata { id: 2, data_size: 110, partition_count: 10, min_timestamp: 0, max_timestamp: 100 },
            SSTableMetadata { id: 3, data_size: 105, partition_count: 10, min_timestamp: 0, max_timestamp: 100 },
            SSTableMetadata { id: 4, data_size: 10000, partition_count: 10, min_timestamp: 0, max_timestamp: 100 },
        ];

        let picks = stcs.pick_compaction(&sstables);
        assert!(!picks.is_empty());
        // The first 3 should be grouped (similar size), the 4th is alone
        let first_group = &picks[0];
        assert!(first_group.len() >= 2);
        assert!(!first_group.contains(&4));
    }

    #[test]
    fn stcs_below_threshold() {
        let stcs = SizeTieredCompactionStrategy::default();
        let sstables = vec![
            SSTableMetadata { id: 1, data_size: 100, partition_count: 10, min_timestamp: 0, max_timestamp: 100 },
        ];
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

        // GC grace = 1000s, now = 2000 → tombstone at ldt=100 is well past gc_grace
        let merged = merge_partitions(vec![source], 1000, 2000);
        // Tombstone should be GC'd, row should be empty → partition dropped
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

        // GC grace = 1000s, now = 1000 → tombstone at ldt=900 is NOT past gc_cutoff (1000-1000=0)
        let merged = merge_partitions(vec![source], 1000, 1000);
        assert_eq!(merged.len(), 1);
    }

    #[test]
    fn merge_multiple_sources_sorted_output() {
        let source1 = vec![
            (b"a".to_vec(), make_partition(vec![make_row(b"ck", vec![make_cell("x", b"1", 100)])])),
            (b"c".to_vec(), make_partition(vec![make_row(b"ck", vec![make_cell("x", b"3", 100)])])),
        ];
        let source2 = vec![
            (b"b".to_vec(), make_partition(vec![make_row(b"ck", vec![make_cell("x", b"2", 100)])])),
        ];

        let merged = merge_partitions(vec![source1, source2], 86400, 1000);
        assert_eq!(merged.len(), 3);
        assert_eq!(merged[0].0, b"a");
        assert_eq!(merged[1].0, b"b");
        assert_eq!(merged[2].0, b"c");
    }
}
