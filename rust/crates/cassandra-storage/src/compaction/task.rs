// Licensed under Apache License, Version 2.0.

//! Compaction task hierarchy: context, result, and task trait with concrete
//! implementations for regular compaction, cleanup, scrub, tombstone, and
//! SSTable upgrade operations.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.compaction.CompactionTask`
//! - `org.apache.cassandra.db.compaction.AbstractCompactionTask`
//! - `org.apache.cassandra.db.compaction.CompactionIterator`

use serde::{Deserialize, Serialize};

use crate::compaction::active::CancellationToken;
use crate::compaction::errors::{CompactionError, CompactionReason, CompactionType};
use crate::compaction::merge_partitions;
use crate::memtable::partition::PartitionData;
use crate::sstable::format::SSTableId;

// ─── CompactionContext ──────────────────────────────────────────────────────

/// Input parameters that accompany every compaction execution.
#[derive(Debug, Clone)]
pub struct CompactionContext {
    pub input_sstables: Vec<SSTableId>,
    pub compaction_type: CompactionType,
    pub reason: CompactionReason,
    pub gc_grace_seconds: i32,
    pub now_seconds: i32,
    pub cancel_token: CancellationToken,
}

// ─── CompactionResult ───────────────────────────────────────────────────────

/// Statistics produced after a compaction completes.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct CompactionResult {
    pub input_sstable_count: usize,
    pub output_sstable_count: usize,
    pub bytes_read: u64,
    pub bytes_written: u64,
    pub partitions_merged: u64,
    pub tombstones_dropped: u64,
    pub duration_ms: u64,
}

// ─── CompactionTask trait ───────────────────────────────────────────────────

/// A compaction task that can be executed against a set of partition sources.
pub trait CompactionTask: Send + Sync {
    /// Human-readable name for this task kind.
    fn name(&self) -> &str;

    /// The compaction type produced by this task.
    fn compaction_type(&self) -> CompactionType;

    /// Execute the compaction over the given partition sources.
    ///
    /// Each inner `Vec` represents the partitions from one SSTable. Returns
    /// the merged output partitions together with result statistics.
    fn execute(
        &self,
        ctx: &CompactionContext,
        partitions: Vec<Vec<(Vec<u8>, PartitionData)>>,
    ) -> Result<(Vec<(Vec<u8>, PartitionData)>, CompactionResult), CompactionError>;
}

// ─── Helpers ────────────────────────────────────────────────────────────────

/// Check the cancellation token and return `CompactionError::Cancelled` if set.
fn check_cancelled(ctx: &CompactionContext) -> Result<(), CompactionError> {
    if ctx.cancel_token.is_cancelled() {
        return Err(CompactionError::Cancelled);
    }
    Ok(())
}

/// Count total partitions across all sources.
fn count_input_partitions(sources: &[Vec<(Vec<u8>, PartitionData)>]) -> usize {
    sources.iter().map(|s| s.len()).sum()
}

// ─── RegularCompactionTask ──────────────────────────────────────────────────

/// Standard merge compaction using `merge_partitions`.
pub struct RegularCompactionTask;

impl CompactionTask for RegularCompactionTask {
    fn name(&self) -> &str {
        "Regular Compaction"
    }

    fn compaction_type(&self) -> CompactionType {
        CompactionType::Compaction
    }

    fn execute(
        &self,
        ctx: &CompactionContext,
        partitions: Vec<Vec<(Vec<u8>, PartitionData)>>,
    ) -> Result<(Vec<(Vec<u8>, PartitionData)>, CompactionResult), CompactionError> {
        check_cancelled(ctx)?;

        let input_count = partitions.len();
        let input_partitions = count_input_partitions(&partitions);

        let merged = merge_partitions(partitions, ctx.gc_grace_seconds, ctx.now_seconds);

        check_cancelled(ctx)?;

        let result = CompactionResult {
            input_sstable_count: input_count,
            output_sstable_count: 1,
            partitions_merged: input_partitions as u64,
            ..Default::default()
        };

        Ok((merged, result))
    }
}

// ─── CleanupTask ────────────────────────────────────────────────────────────

/// Filters partitions to retain only those whose keys fall within locally
/// owned token ranges.
pub struct CleanupTask {
    /// Each tuple is an inclusive (start, end) key range.
    owned_ranges: Vec<(Vec<u8>, Vec<u8>)>,
}

impl CleanupTask {
    pub fn new(owned_ranges: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        Self { owned_ranges }
    }

    fn key_in_range(&self, key: &[u8]) -> bool {
        self.owned_ranges
            .iter()
            .any(|(start, end)| key >= start.as_slice() && key <= end.as_slice())
    }
}

impl CompactionTask for CleanupTask {
    fn name(&self) -> &str {
        "Cleanup"
    }

    fn compaction_type(&self) -> CompactionType {
        CompactionType::Cleanup
    }

    fn execute(
        &self,
        ctx: &CompactionContext,
        partitions: Vec<Vec<(Vec<u8>, PartitionData)>>,
    ) -> Result<(Vec<(Vec<u8>, PartitionData)>, CompactionResult), CompactionError> {
        check_cancelled(ctx)?;

        let input_count = partitions.len();
        let input_partitions = count_input_partitions(&partitions);

        // Flatten then filter by owned ranges.
        let output: Vec<(Vec<u8>, PartitionData)> = partitions
            .into_iter()
            .flatten()
            .filter(|(key, _)| self.key_in_range(key))
            .collect();

        let result = CompactionResult {
            input_sstable_count: input_count,
            output_sstable_count: 1,
            partitions_merged: input_partitions as u64,
            ..Default::default()
        };

        Ok((output, result))
    }
}

// ─── ScrubTask ──────────────────────────────────────────────────────────────

/// Validates partitions and drops corrupt ones (those with empty keys).
pub struct ScrubTask;

impl CompactionTask for ScrubTask {
    fn name(&self) -> &str {
        "Scrub"
    }

    fn compaction_type(&self) -> CompactionType {
        CompactionType::Scrub
    }

    fn execute(
        &self,
        ctx: &CompactionContext,
        partitions: Vec<Vec<(Vec<u8>, PartitionData)>>,
    ) -> Result<(Vec<(Vec<u8>, PartitionData)>, CompactionResult), CompactionError> {
        check_cancelled(ctx)?;

        let input_count = partitions.len();
        let input_partitions = count_input_partitions(&partitions);

        // Keep only partitions with non-empty keys (valid).
        let output: Vec<(Vec<u8>, PartitionData)> = partitions
            .into_iter()
            .flatten()
            .filter(|(key, _)| !key.is_empty())
            .collect();

        let dropped = input_partitions.saturating_sub(output.len());

        if dropped > 0 {
            tracing::warn!(
                dropped_partitions = dropped,
                "scrub dropped corrupt partitions with empty keys"
            );
        }

        let result = CompactionResult {
            input_sstable_count: input_count,
            output_sstable_count: 1,
            partitions_merged: input_partitions as u64,
            ..Default::default()
        };

        Ok((output, result))
    }
}

// ─── TombstoneCompactionTask ────────────────────────────────────────────────

/// Focuses on tombstone-heavy SSTables, aggressively dropping expired
/// tombstones past `gc_grace_seconds` and tracking the count.
pub struct TombstoneCompactionTask;

impl CompactionTask for TombstoneCompactionTask {
    fn name(&self) -> &str {
        "Tombstone Compaction"
    }

    fn compaction_type(&self) -> CompactionType {
        CompactionType::Tombstone
    }

    fn execute(
        &self,
        ctx: &CompactionContext,
        partitions: Vec<Vec<(Vec<u8>, PartitionData)>>,
    ) -> Result<(Vec<(Vec<u8>, PartitionData)>, CompactionResult), CompactionError> {
        check_cancelled(ctx)?;

        let input_count = partitions.len();
        let input_partitions = count_input_partitions(&partitions);

        // Count tombstones before merge.
        let tombstones_before: u64 = partitions
            .iter()
            .flatten()
            .map(|(_, pd)| count_tombstones(pd))
            .sum();

        let merged = merge_partitions(partitions, ctx.gc_grace_seconds, ctx.now_seconds);

        check_cancelled(ctx)?;

        // Count tombstones after merge.
        let tombstones_after: u64 = merged.iter().map(|(_, pd)| count_tombstones(pd)).sum();

        let tombstones_dropped = tombstones_before.saturating_sub(tombstones_after);

        let result = CompactionResult {
            input_sstable_count: input_count,
            output_sstable_count: 1,
            partitions_merged: input_partitions as u64,
            tombstones_dropped,
            ..Default::default()
        };

        Ok((merged, result))
    }
}

/// Count all tombstones (partition-level, row-level, cell-level) in a
/// `PartitionData`.
fn count_tombstones(pd: &PartitionData) -> u64 {
    let mut count: u64 = 0;
    if pd.tombstone_timestamp.is_some() {
        count += 1;
    }
    for row in pd.rows.values() {
        if row.is_tombstone {
            count += 1;
        }
        for cell in &row.cells {
            if cell.is_tombstone {
                count += 1;
            }
        }
    }
    count
}

// ─── UpgradeSSTableTask ─────────────────────────────────────────────────────

/// Pass-through (no-op rewrite) used to upgrade SSTable format versions.
pub struct UpgradeSSTableTask;

impl CompactionTask for UpgradeSSTableTask {
    fn name(&self) -> &str {
        "Upgrade SSTable"
    }

    fn compaction_type(&self) -> CompactionType {
        CompactionType::Upgrade
    }

    fn execute(
        &self,
        ctx: &CompactionContext,
        partitions: Vec<Vec<(Vec<u8>, PartitionData)>>,
    ) -> Result<(Vec<(Vec<u8>, PartitionData)>, CompactionResult), CompactionError> {
        check_cancelled(ctx)?;

        let input_count = partitions.len();
        let input_partitions = count_input_partitions(&partitions);

        // No-op: just flatten without any merge or filtering.
        let output: Vec<(Vec<u8>, PartitionData)> = partitions.into_iter().flatten().collect();

        let result = CompactionResult {
            input_sstable_count: input_count,
            output_sstable_count: 1,
            partitions_merged: input_partitions as u64,
            ..Default::default()
        };

        Ok((output, result))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::memtable::partition::{Cell, Row};

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

    fn make_ctx() -> CompactionContext {
        CompactionContext {
            input_sstables: vec![1, 2],
            compaction_type: CompactionType::Compaction,
            reason: CompactionReason::Normal,
            gc_grace_seconds: 86400,
            now_seconds: 1000,
            cancel_token: CancellationToken::new(),
        }
    }

    // ── RegularCompactionTask ────────────────────────────────────────────

    #[test]
    fn regular_merges_two_sources() {
        let source1 = vec![(
            b"pk1".to_vec(),
            make_partition(vec![make_row(b"ck1", vec![make_cell("c", b"old", 100)])]),
        )];
        let source2 = vec![(
            b"pk1".to_vec(),
            make_partition(vec![make_row(b"ck1", vec![make_cell("c", b"new", 200)])]),
        )];

        let ctx = make_ctx();
        let task = RegularCompactionTask;
        let (merged, result) = task.execute(&ctx, vec![source1, source2]).unwrap();

        assert_eq!(merged.len(), 1);
        let row = merged[0].1.rows.get(&b"ck1".to_vec()).unwrap();
        assert_eq!(row.cells[0].value.as_deref(), Some(b"new".as_slice()));
        assert_eq!(result.input_sstable_count, 2);
        assert_eq!(result.partitions_merged, 2);
    }

    // ── TombstoneCompactionTask ──────────────────────────────────────────

    #[test]
    fn tombstone_task_drops_expired_tombstones() {
        // gc_grace = 1000, now = 2000 => gc_cutoff = 1000
        // Tombstone with ldt=100 is well below cutoff => dropped.
        let source = vec![(
            b"pk1".to_vec(),
            make_partition(vec![Row {
                clustering_key: b"ck1".to_vec(),
                cells: vec![make_tombstone_cell("c", 100, 100)],
                is_tombstone: false,
                local_deletion_time: None,
            }]),
        )];

        let mut ctx = make_ctx();
        ctx.gc_grace_seconds = 1000;
        ctx.now_seconds = 2000;
        ctx.compaction_type = CompactionType::Tombstone;

        let task = TombstoneCompactionTask;
        let (merged, result) = task.execute(&ctx, vec![source]).unwrap();

        // The tombstone should have been GC'd.
        assert!(merged.is_empty());
        assert_eq!(result.tombstones_dropped, 1);
    }

    #[test]
    fn tombstone_task_preserves_live_tombstones() {
        // gc_cutoff = now - gc_grace = 1000 - 1000 = 0
        // Tombstone with ldt=900 > gc_cutoff => preserved.
        let source = vec![(
            b"pk1".to_vec(),
            make_partition(vec![Row {
                clustering_key: b"ck1".to_vec(),
                cells: vec![make_tombstone_cell("c", 100, 900)],
                is_tombstone: false,
                local_deletion_time: None,
            }]),
        )];

        let mut ctx = make_ctx();
        ctx.gc_grace_seconds = 1000;
        ctx.now_seconds = 1000;
        ctx.compaction_type = CompactionType::Tombstone;

        let task = TombstoneCompactionTask;
        let (merged, result) = task.execute(&ctx, vec![source]).unwrap();

        assert_eq!(merged.len(), 1);
        assert_eq!(result.tombstones_dropped, 0);
    }

    // ── Cancellation ────────────────────────────────────────────────────

    #[test]
    fn cancellation_returns_cancelled_error() {
        let ctx = make_ctx();
        ctx.cancel_token.cancel();

        let task = RegularCompactionTask;
        let result = task.execute(&ctx, vec![]);
        assert!(result.is_err());
        assert!(matches!(result.unwrap_err(), CompactionError::Cancelled));
    }

    // ── CompactionResult defaults ───────────────────────────────────────

    #[test]
    fn compaction_result_default_is_zero() {
        let r = CompactionResult::default();
        assert_eq!(r.input_sstable_count, 0);
        assert_eq!(r.output_sstable_count, 0);
        assert_eq!(r.bytes_read, 0);
        assert_eq!(r.bytes_written, 0);
        assert_eq!(r.partitions_merged, 0);
        assert_eq!(r.tombstones_dropped, 0);
        assert_eq!(r.duration_ms, 0);
    }

    #[test]
    fn compaction_result_serde_roundtrip() {
        let r = CompactionResult {
            input_sstable_count: 3,
            output_sstable_count: 1,
            bytes_read: 1024,
            bytes_written: 512,
            partitions_merged: 100,
            tombstones_dropped: 5,
            duration_ms: 42,
        };
        let json = serde_json::to_string(&r).unwrap();
        let back: CompactionResult = serde_json::from_str(&json).unwrap();
        assert_eq!(back.bytes_read, 1024);
        assert_eq!(back.tombstones_dropped, 5);
    }

    // ── CleanupTask ─────────────────────────────────────────────────────

    #[test]
    fn cleanup_filters_by_owned_ranges() {
        let source = vec![
            (
                b"a".to_vec(),
                make_partition(vec![make_row(b"ck", vec![make_cell("c", b"1", 100)])]),
            ),
            (
                b"m".to_vec(),
                make_partition(vec![make_row(b"ck", vec![make_cell("c", b"2", 100)])]),
            ),
            (
                b"z".to_vec(),
                make_partition(vec![make_row(b"ck", vec![make_cell("c", b"3", 100)])]),
            ),
        ];

        let task = CleanupTask::new(vec![(b"a".to_vec(), b"n".to_vec())]);
        let ctx = make_ctx();
        let (output, _result) = task.execute(&ctx, vec![source]).unwrap();

        // "a" and "m" are in range [a, n], "z" is out.
        assert_eq!(output.len(), 2);
    }

    // ── ScrubTask ───────────────────────────────────────────────────────

    #[test]
    fn scrub_drops_empty_key_partitions() {
        let source = vec![
            (
                b"valid".to_vec(),
                make_partition(vec![make_row(b"ck", vec![make_cell("c", b"1", 100)])]),
            ),
            (
                b"".to_vec(), // empty key = corrupt
                make_partition(vec![make_row(b"ck", vec![make_cell("c", b"2", 100)])]),
            ),
        ];

        let task = ScrubTask;
        let ctx = make_ctx();
        let (output, _result) = task.execute(&ctx, vec![source]).unwrap();

        assert_eq!(output.len(), 1);
        assert_eq!(output[0].0, b"valid");
    }

    // ── UpgradeSSTableTask ──────────────────────────────────────────────

    #[test]
    fn upgrade_is_passthrough() {
        let source = vec![(
            b"pk1".to_vec(),
            make_partition(vec![make_row(b"ck", vec![make_cell("c", b"v", 100)])]),
        )];

        let task = UpgradeSSTableTask;
        let ctx = make_ctx();
        let (output, result) = task.execute(&ctx, vec![source]).unwrap();

        assert_eq!(output.len(), 1);
        assert_eq!(result.input_sstable_count, 1);
    }
}
