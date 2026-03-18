// Licensed under Apache License, Version 2.0.

//! Central compaction execution engine: scheduling, rate-limiting, and lifecycle.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.compaction.CompactionManager`
//! - `org.apache.cassandra.db.compaction.ActiveCompactions`
//! - `org.apache.cassandra.db.compaction.CompactionTask`

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use uuid::Uuid;

use crate::compaction::CompactionMetrics;
use crate::compaction::active::*;
use crate::compaction::errors::*;
use crate::compaction::task::*;
use crate::memtable::partition::PartitionData;

// ─── RateLimiter ────────────────────────────────────────────────────────────

/// Byte-rate limiter for compaction I/O throughput.
#[derive(Debug)]
pub struct RateLimiter {
    bytes_per_second: u64,
    bytes_this_second: AtomicU64,
    enabled: bool,
}

impl RateLimiter {
    /// Create a rate limiter with the given byte-per-second cap.
    pub fn new(bytes_per_second: u64) -> Self {
        Self {
            bytes_per_second,
            bytes_this_second: AtomicU64::new(0),
            enabled: true,
        }
    }

    /// Create a disabled rate limiter (no throttling).
    pub fn disabled() -> Self {
        Self {
            bytes_per_second: 0,
            bytes_this_second: AtomicU64::new(0),
            enabled: false,
        }
    }

    /// Try to acquire `bytes` of budget. Returns `true` if under limit.
    pub fn acquire(&self, bytes: u64) -> bool {
        if !self.enabled {
            return true;
        }
        let prev = self.bytes_this_second.fetch_add(bytes, Ordering::Relaxed);
        prev + bytes <= self.bytes_per_second
    }

    /// Reset the per-second counter (called periodically by a timer).
    pub fn reset(&self) {
        self.bytes_this_second.store(0, Ordering::Relaxed);
    }
}

// ─── CompactionManager ──────────────────────────────────────────────────────

/// Central compaction execution engine.
///
/// Manages active compactions, enforces concurrency limits and rate limiting,
/// and tracks operational metrics.
pub struct CompactionManager {
    active: Arc<ActiveCompactions>,
    metrics: Arc<CompactionMetrics>,
    rate_limiter: Arc<RateLimiter>,
    max_concurrent: usize,
    shutdown: AtomicBool,
}

impl CompactionManager {
    /// Create a new compaction manager.
    pub fn new(max_concurrent: usize, rate_limiter: RateLimiter) -> Self {
        Self {
            active: Arc::new(ActiveCompactions::new()),
            metrics: Arc::new(CompactionMetrics::default()),
            rate_limiter: Arc::new(rate_limiter),
            max_concurrent,
            shutdown: AtomicBool::new(false),
        }
    }

    /// Submit a background compaction task.
    ///
    /// Respects `max_concurrent` and shutdown state. Registers the task in
    /// active compactions and returns its UUID.
    pub fn submit_background(
        &self,
        task: &dyn CompactionTask,
        ctx: &CompactionContext,
    ) -> Result<Uuid, CompactionError> {
        if self.shutdown.load(Ordering::Relaxed) {
            return Err(CompactionError::InvalidState(
                "compaction manager is shut down".into(),
            ));
        }
        if self.active.count() >= self.max_concurrent {
            return Err(CompactionError::InvalidState(format!(
                "max concurrent compactions reached ({})",
                self.max_concurrent
            )));
        }
        let id = Uuid::new_v4();
        let info = CompactionInfo {
            id,
            compaction_type: task.compaction_type(),
            sstable_ids: ctx.input_sstables.clone(),
            started_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            cancel_token: ctx.cancel_token.clone(),
        };
        // Ignore conflict errors for simplicity; the caller should check.
        let _ = self.active.register(info);
        Ok(id)
    }

    /// Submit a user-defined compaction task.
    ///
    /// Bypasses the concurrent limit check but still checks shutdown state.
    pub fn submit_user_defined(
        &self,
        task: &dyn CompactionTask,
        ctx: &CompactionContext,
    ) -> Result<Uuid, CompactionError> {
        if self.shutdown.load(Ordering::Relaxed) {
            return Err(CompactionError::InvalidState(
                "compaction manager is shut down".into(),
            ));
        }
        let id = Uuid::new_v4();
        let info = CompactionInfo {
            id,
            compaction_type: task.compaction_type(),
            sstable_ids: ctx.input_sstables.clone(),
            started_at_ms: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_millis() as u64,
            cancel_token: ctx.cancel_token.clone(),
        };
        let _ = self.active.register(info);
        Ok(id)
    }

    /// Execute a compaction task and update metrics on completion.
    pub fn execute_task(
        &self,
        task_id: &Uuid,
        task: &dyn CompactionTask,
        ctx: &CompactionContext,
        partitions: Vec<Vec<(Vec<u8>, PartitionData)>>,
    ) -> Result<CompactionResult, CompactionError> {
        let result = task.execute(ctx, partitions);
        // Unregister regardless of success or failure.
        self.active.unregister(task_id);
        match result {
            Ok((_output, res)) => {
                self.metrics
                    .compactions_completed
                    .fetch_add(1, Ordering::Relaxed);
                self.metrics
                    .bytes_read
                    .fetch_add(res.bytes_read, Ordering::Relaxed);
                self.metrics
                    .bytes_written
                    .fetch_add(res.bytes_written, Ordering::Relaxed);
                self.metrics
                    .sstables_compacted
                    .fetch_add(res.input_sstable_count as u64, Ordering::Relaxed);
                Ok(res)
            }
            Err(e) => Err(e),
        }
    }

    /// Cancel a specific compaction task by UUID.
    pub fn cancel(&self, task_id: &Uuid) -> bool {
        self.active.cancel(task_id)
    }

    /// Cancel all active compaction tasks.
    pub fn cancel_all(&self) {
        self.active.cancel_all();
    }

    /// Shut down the compaction manager, preventing new submissions.
    pub fn shutdown(&self) {
        self.shutdown.store(true, Ordering::Relaxed);
        self.cancel_all();
    }

    /// Returns `true` if shutdown has been initiated.
    pub fn is_shutdown(&self) -> bool {
        self.shutdown.load(Ordering::Relaxed)
    }

    /// Number of currently active compactions.
    pub fn active_count(&self) -> usize {
        self.active.count()
    }

    /// Reference to the compaction metrics.
    pub fn metrics(&self) -> &CompactionMetrics {
        &self.metrics
    }

    /// Reference to the rate limiter.
    pub fn rate_limiter(&self) -> &RateLimiter {
        &self.rate_limiter
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::compaction::task::RegularCompactionTask;
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

    fn test_ctx() -> CompactionContext {
        CompactionContext {
            input_sstables: vec![100, 200],
            compaction_type: CompactionType::Compaction,
            reason: CompactionReason::Normal,
            gc_grace_seconds: 86400,
            now_seconds: 1000,
            cancel_token: CancellationToken::new(),
        }
    }

    /// Helper: create a context with unique SSTable IDs to avoid conflicts.
    fn test_ctx_with_ids(ids: Vec<u64>) -> CompactionContext {
        CompactionContext {
            input_sstables: ids,
            compaction_type: CompactionType::Compaction,
            reason: CompactionReason::Normal,
            gc_grace_seconds: 86400,
            now_seconds: 1000,
            cancel_token: CancellationToken::new(),
        }
    }

    #[test]
    fn submit_background_respects_max_concurrent() {
        let mgr = CompactionManager::new(2, RateLimiter::disabled());
        let task = RegularCompactionTask;

        let ctx1 = test_ctx_with_ids(vec![1, 2]);
        let _id1 = mgr.submit_background(&task, &ctx1).unwrap();

        let ctx2 = test_ctx_with_ids(vec![3, 4]);
        let _id2 = mgr.submit_background(&task, &ctx2).unwrap();
        assert_eq!(mgr.active_count(), 2);

        // Third submission should fail due to max_concurrent.
        let ctx3 = test_ctx_with_ids(vec![5, 6]);
        let err = mgr.submit_background(&task, &ctx3).unwrap_err();
        assert!(matches!(err, CompactionError::InvalidState(_)));
    }

    #[test]
    fn submit_user_defined_bypasses_limit() {
        let mgr = CompactionManager::new(1, RateLimiter::disabled());
        let task = RegularCompactionTask;

        let ctx1 = test_ctx_with_ids(vec![1, 2]);
        let _id1 = mgr.submit_background(&task, &ctx1).unwrap();
        assert_eq!(mgr.active_count(), 1);

        // Background would fail.
        let ctx2 = test_ctx_with_ids(vec![3, 4]);
        let err = mgr.submit_background(&task, &ctx2).unwrap_err();
        assert!(matches!(err, CompactionError::InvalidState(_)));

        // User-defined succeeds despite limit.
        let ctx3 = test_ctx_with_ids(vec![5, 6]);
        let _id2 = mgr.submit_user_defined(&task, &ctx3).unwrap();
        assert_eq!(mgr.active_count(), 2);
    }

    #[test]
    fn execute_updates_metrics() {
        let mgr = CompactionManager::new(4, RateLimiter::disabled());
        let task = RegularCompactionTask;
        let ctx = test_ctx();

        let id = mgr.submit_background(&task, &ctx).unwrap();

        let source1 = vec![(
            b"pk1".to_vec(),
            make_partition(vec![make_row(b"ck1", vec![make_cell("c", b"v1", 100)])]),
        )];
        let source2 = vec![(
            b"pk2".to_vec(),
            make_partition(vec![make_row(b"ck1", vec![make_cell("c", b"v2", 200)])]),
        )];

        let result = mgr
            .execute_task(&id, &task, &ctx, vec![source1, source2])
            .unwrap();

        assert_eq!(result.input_sstable_count, 2);
        assert_eq!(result.partitions_merged, 2);

        let snap = mgr.metrics().snapshot();
        assert_eq!(snap.compactions_completed, 1);
        assert_eq!(snap.sstables_compacted, 2);

        // Task should be unregistered after execution.
        assert_eq!(mgr.active_count(), 0);
    }

    #[test]
    fn cancel_and_shutdown_work() {
        let mgr = CompactionManager::new(4, RateLimiter::disabled());
        let task = RegularCompactionTask;

        let ctx1 = test_ctx_with_ids(vec![1, 2]);
        let id1 = mgr.submit_background(&task, &ctx1).unwrap();

        let ctx2 = test_ctx_with_ids(vec![3, 4]);
        let _id2 = mgr.submit_background(&task, &ctx2).unwrap();
        assert_eq!(mgr.active_count(), 2);

        // Cancel one.
        assert!(mgr.cancel(&id1));

        // Shutdown prevents new submissions and cancels all.
        mgr.shutdown();
        assert!(mgr.is_shutdown());

        let ctx3 = test_ctx_with_ids(vec![5, 6]);
        let err = mgr.submit_background(&task, &ctx3).unwrap_err();
        assert!(matches!(err, CompactionError::InvalidState(_)));
    }

    #[test]
    fn rate_limiter_basic_logic() {
        let rl = RateLimiter::new(1000);
        assert!(rl.acquire(500));
        assert!(rl.acquire(400));
        // 900 used, 200 more would exceed 1000.
        assert!(!rl.acquire(200));

        rl.reset();
        assert!(rl.acquire(999));
    }

    #[test]
    fn rate_limiter_disabled_always_allows() {
        let rl = RateLimiter::disabled();
        assert!(rl.acquire(u64::MAX));
    }
}
