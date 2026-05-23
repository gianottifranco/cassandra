// Licensed under Apache License, Version 2.0.

//! Integration tests for the compaction lifecycle: LifecycleTransaction + Journal
//! + SSTableTracker + StorageEventBus working end-to-end.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.lifecycle.LifecycleTransaction`
//! - `org.apache.cassandra.db.lifecycle.Tracker`
//! - `org.apache.cassandra.notifications.SSTableListChangedNotification`

use std::collections::BTreeSet;
use std::sync::Arc;

use parking_lot::RwLock;
use uuid::Uuid;

use cassandra_storage::compaction::controller::CompactionController;
use cassandra_storage::compaction::errors::{CompactionType, OperationProgress};
use cassandra_storage::compaction::iterator::{CompactionIterator, VecSource};
use cassandra_storage::compaction::journal::{Journal, JournalEntry};
use cassandra_storage::compaction::lifecycle::LifecycleTransaction;
use cassandra_storage::compaction::logger::{CompactionEvent, CompactionLogger};
use cassandra_storage::compaction::manager::{CompactionManager, RateLimiter};
use cassandra_storage::compaction::pending_repair::PendingRepairManager;
use cassandra_storage::compaction::{CompactionStrategyType, SSTableMetadata};
use cassandra_storage::memtable::partition::{Cell, PartitionData, Row};
use cassandra_storage::notifications::{StorageEvent, StorageEventBus, StorageEventListener};
use cassandra_storage::sstable::tracker::SSTableTracker;

// ─── Helpers ──────────────────────────────────────────────────────────────

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

/// Test listener that records events.
struct RecordingListener {
    id: String,
    events: Arc<RwLock<Vec<StorageEvent>>>,
}

impl StorageEventListener for RecordingListener {
    fn on_event(&self, event: &StorageEvent) {
        self.events.write().push(event.clone());
    }

    fn listener_id(&self) -> &str {
        &self.id
    }
}

// ─── Tests ────────────────────────────────────────────────────────────────

/// Full lifecycle: create txn → stage SSTables → commit → journal records →
/// tracker updated → event published.
#[test]
fn full_lifecycle_transaction_with_journal_tracker_and_events() {
    let dir = tempfile::tempdir().unwrap();
    let journal_path = dir.path().join("sstable_mutations.journal");

    // 1. Set up tracker with initial SSTables (IDs 1, 2, 3).
    let mut initial = BTreeSet::new();
    initial.insert(1u64);
    initial.insert(2u64);
    initial.insert(3u64);
    let tracker = SSTableTracker::from_ids(initial);

    // 2. Set up event bus with recording listener.
    let events = Arc::new(RwLock::new(Vec::new()));
    let bus = StorageEventBus::new();
    let listener = Arc::new(RecordingListener {
        id: "test".to_string(),
        events: events.clone(),
    });
    bus.subscribe(listener);

    // 3. Create lifecycle transaction.
    let mut txn = LifecycleTransaction::new(dir.path()).unwrap();

    // 4. Create journal and log the replacement.
    let mut journal = Journal::create(&journal_path).unwrap();

    // Stage new SSTable (ID 10) — marks it as being added.
    txn.stage(10).unwrap();
    // Obsolete old SSTables (IDs 1, 2) — marks them for removal.
    txn.obsolete(1).unwrap();
    txn.obsolete(2).unwrap();

    // Journal the replacement operation.
    journal
        .append(JournalEntry::Replace {
            removed: vec![1, 2],
            added: vec![10],
        })
        .unwrap();

    // 5. Commit the transaction.
    txn.commit().unwrap();

    // 6. Apply to tracker (atomic replacement).
    tracker.apply_replacement(&[1, 2], &[10]);

    // 7. Publish event.
    bus.publish(&StorageEvent::SSTableReplaced {
        removed: vec![1, 2],
        added: vec![10],
        keyspace: "test_ks".to_string(),
        table: "test_table".to_string(),
    });

    // ── Verify ──

    // Tracker has correct SSTables: {3, 10}
    let view = tracker.view();
    assert_eq!(view.len(), 2);
    assert!(view.contains(&3));
    assert!(view.contains(&10));
    assert!(!view.contains(&1));
    assert!(!view.contains(&2));

    // Journal replays correctly.
    let replayed = Journal::replay(&journal_path).unwrap();
    assert_eq!(replayed.len(), 1);
    match &replayed[0] {
        JournalEntry::Replace { removed, added } => {
            assert_eq!(removed, &vec![1u64, 2]);
            assert_eq!(added, &vec![10u64]);
        }
        other => panic!("Expected Replace, got {:?}", other),
    }

    // Event was received.
    let recorded = events.read();
    assert_eq!(recorded.len(), 1);
    match &recorded[0] {
        StorageEvent::SSTableReplaced {
            removed,
            added,
            keyspace,
            table,
        } => {
            assert_eq!(removed, &vec![1u64, 2]);
            assert_eq!(added, &vec![10u64]);
            assert_eq!(keyspace, "test_ks");
            assert_eq!(table, "test_table");
        }
        other => panic!("Expected SSTableReplaced, got {:?}", other),
    }
}

/// CompactionIterator merges partitions from multiple sources correctly.
#[test]
fn compaction_iterator_merge_integration() {
    let cancel = cassandra_storage::compaction::iterator::CancellationToken::new();
    let source1 = VecSource::new(
        vec![
            (
                b"a".to_vec(),
                make_partition(vec![make_row(b"ck1", vec![make_cell("x", b"old", 100)])]),
            ),
            (
                b"c".to_vec(),
                make_partition(vec![make_row(b"ck1", vec![make_cell("x", b"3", 200)])]),
            ),
        ],
        0,
    );
    let source2 = VecSource::new(
        vec![
            (
                b"a".to_vec(),
                make_partition(vec![make_row(b"ck1", vec![make_cell("x", b"new", 200)])]),
            ),
            (
                b"b".to_vec(),
                make_partition(vec![make_row(b"ck1", vec![make_cell("x", b"2", 100)])]),
            ),
        ],
        1,
    );

    let iter = CompactionIterator::new(
        vec![Box::new(source1), Box::new(source2)],
        86400,
        1000,
        cancel,
    );

    let result = iter.merge_all().unwrap();

    // Should produce 3 partitions in sorted order: a, b, c
    assert_eq!(result.len(), 3);
    assert_eq!(result[0].0, b"a");
    assert_eq!(result[1].0, b"b");
    assert_eq!(result[2].0, b"c");

    // "a" should have the newer value (ts=200)
    let row_a = result[0].1.rows.get(b"ck1".as_slice()).unwrap();
    assert_eq!(row_a.cells[0].value.as_deref(), Some(b"new".as_slice()));
}

/// Active compactions + manager interaction.
#[test]
fn active_compactions_and_manager_integration() {
    let manager = CompactionManager::new(2, RateLimiter::disabled());

    // Manager starts with no active compactions.
    assert_eq!(manager.active_count(), 0);
    assert!(!manager.is_shutdown());

    // Shutdown prevents further operations.
    manager.shutdown();
    assert!(manager.is_shutdown());
}

/// Controller produces task specs from strategy.
#[test]
fn controller_produces_task_specs() {
    let controller = CompactionController::new(CompactionStrategyType::SizeTiered);

    let sstables = (0..5)
        .map(|i| SSTableMetadata {
            id: i,
            data_size: 100,
            partition_count: 10,
            min_timestamp: 0,
            max_timestamp: 100,
        })
        .collect::<Vec<_>>();

    let tasks = controller.get_next_background_tasks(&sstables);
    // With 5 similar-sized SSTables and min_threshold=4, STCS should pick a group.
    assert!(!tasks.is_empty());
    assert!(tasks[0].sstable_ids.len() >= 4);
}

/// CompactionLogger writes and reads events.
#[test]
fn logger_writes_and_reads_events() {
    let dir = tempfile::tempdir().unwrap();
    let log_path = dir.path().join("compaction.log");

    let logger = CompactionLogger::new(Some(log_path.clone()), false);

    let id = Uuid::new_v4();
    logger
        .log_event(&CompactionEvent::Started {
            id,
            compaction_type: CompactionType::Compaction,
            sstable_ids: vec![1, 2, 3],
            timestamp_ms: 1000,
        })
        .unwrap();

    logger
        .log_event(&CompactionEvent::Completed {
            id,
            input_sstables: 3,
            output_sstables: 1,
            bytes_read: 1024,
            bytes_written: 512,
            duration_ms: 100,
            timestamp_ms: 1100,
        })
        .unwrap();

    let events = CompactionLogger::read_events(&log_path).unwrap();
    assert_eq!(events.len(), 2);
}

/// PendingRepairManager tracks repair sessions.
#[test]
fn pending_repair_lifecycle() {
    let manager = PendingRepairManager::new();
    let session = Uuid::new_v4();

    manager.register_pending(session).unwrap();
    manager.add_sstable(&session, 10).unwrap();
    manager.add_sstable(&session, 20).unwrap();

    assert!(manager.is_pending(&10));
    assert!(manager.is_pending(&20));

    let finalized = manager.finalize(&session).unwrap();
    assert_eq!(finalized.len(), 2);
    assert!(!manager.is_pending(&10));
}

/// OperationProgress tracks compaction progress.
#[test]
fn operation_progress_tracking() {
    let progress = OperationProgress::new(1000, 100);
    assert_eq!(progress.progress_pct(), 0.0);
    assert!(!progress.is_complete());

    progress
        .bytes_processed
        .store(500, std::sync::atomic::Ordering::Relaxed);
    progress
        .partitions_processed
        .store(50, std::sync::atomic::Ordering::Relaxed);
    assert!((progress.progress_pct() - 50.0).abs() < 0.1);

    progress
        .bytes_processed
        .store(1000, std::sync::atomic::Ordering::Relaxed);
    assert!(progress.is_complete());
}

/// Tracker snapshot isolation: view doesn't change after modifications.
#[test]
fn tracker_snapshot_isolation() {
    let tracker = SSTableTracker::new();
    tracker.add(1);
    tracker.add(2);

    let snapshot = tracker.view();
    assert_eq!(snapshot.len(), 2);

    // Modify tracker after taking snapshot.
    tracker.add(3);
    tracker.remove(&1);

    // Snapshot is unchanged.
    assert_eq!(snapshot.len(), 2);
    assert!(snapshot.contains(&1));
    assert!(snapshot.contains(&2));

    // Current view reflects changes.
    let current = tracker.view();
    assert_eq!(current.len(), 2);
    assert!(current.contains(&2));
    assert!(current.contains(&3));
}
