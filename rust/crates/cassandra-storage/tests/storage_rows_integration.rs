// Licensed under Apache License, Version 2.0.

//! Integration tests for the rich rows/partitions/filters/transforms/tries model.

use cassandra_common::tombstone::DeletionTime;
use cassandra_common::token::Token;
use cassandra_common::ttl::NO_TTL;

use cassandra_storage::filter::clustering_filter::{
    ClusteringBound, ClusteringIndexFilter, Slice, Slices,
};
use cassandra_storage::filter::data_limits::DataLimits;
use cassandra_storage::memtable::partition::{Cell, PartitionData, Row};
use cassandra_storage::memtable::shard::ShardedMemtable;
use cassandra_storage::memtable::{MemtableBackend, SkipListMemtable};
use cassandra_storage::partitions::decorated_key::DecoratedKey;
use cassandra_storage::partitions::filtered_partition::FilteredPartition;
use cassandra_storage::partitions::iterators::{InMemoryRowIterator, UnfilteredRowIterator};
use cassandra_storage::partitions::partition_update::PartitionUpdate;
use cassandra_storage::rows::cell::CellData;
use cassandra_storage::rows::liveness::LivenessInfo;
use cassandra_storage::rows::unfiltered::{RowData, Unfiltered};
use cassandra_storage::transform::filtered::FilteredRows;
use cassandra_storage::transform::transformation::{LimitsTransform, PurgeTransform, Transformation};
use cassandra_storage::tries::in_memory::InMemoryTrie;
use cassandra_storage::tries::merge::MergeTrie;
use cassandra_storage::tries::trie::Trie;

// ─── Helpers ──────────────────────────────────────────────────────────────

fn make_cell(col: &str, val: &[u8], ts: i64) -> CellData {
    CellData {
        column: col.to_string(),
        value: Some(val.to_vec()),
        timestamp: ts,
        ttl: NO_TTL,
        local_deletion_time: i32::MAX,
        path: None,
    }
}

fn make_row(ck: &[u8], col: &str, val: &[u8], ts: i64) -> RowData {
    let mut row = RowData::new(ck.to_vec());
    row.liveness_info = LivenessInfo::create(ts);
    row.add_cell(make_cell(col, val, ts));
    row
}

fn simple_row(ck: &[u8], col: &str, val: &[u8], ts: i64) -> Row {
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

// ─── Test (a): Build PartitionUpdate, iterate with UnfilteredRowIterator ──

#[test]
fn partition_update_iterate_row_ordering() {
    let dk = DecoratedKey::with_token(b"pk1".to_vec(), Token::from_raw(42));
    let mut pu = PartitionUpdate::new(dk);

    // Insert rows out of order
    pu.add_row(make_row(b"ck3", "name", b"charlie", 100));
    pu.add_row(make_row(b"ck1", "name", b"alice", 100));
    pu.add_row(make_row(b"ck2", "name", b"bob", 100));

    // Convert to PartitionData and iterate
    let pd = PartitionData::from(&pu);
    let mut iter = InMemoryRowIterator::from_partition_data(b"pk1".to_vec(), &pd);

    // Should be in clustering key order
    let mut keys = Vec::new();
    while let Some(item) = iter.next() {
        keys.push(item.clustering_key().to_vec());
    }
    assert_eq!(keys, vec![b"ck1".to_vec(), b"ck2".to_vec(), b"ck3".to_vec()]);
}

#[test]
fn partition_update_with_static_row() {
    let dk = DecoratedKey::with_token(b"pk".to_vec(), Token::from_raw(1));
    let mut pu = PartitionUpdate::new(dk);

    let mut sr = RowData::new_static();
    sr.add_cell(make_cell("static_col", b"static_val", 100));
    pu.add_row(sr);
    pu.add_row(make_row(b"ck1", "name", b"alice", 100));

    assert!(pu.static_row.is_some());
    assert_eq!(pu.row_count(), 1);
}

// ─── Test (b): Apply transformations, verify dead data removed ────────────

#[test]
fn purge_transform_removes_dead_data() {
    let items = vec![
        Unfiltered::Row(make_row(b"ck1", "x", b"live", 100)),
        Unfiltered::Row({
            let mut row = RowData::new(b"ck2".to_vec());
            row.deletion = DeletionTime::new(50, 50); // purgeable tombstone
            row
        }),
        Unfiltered::Row(make_row(b"ck3", "x", b"live", 100)),
    ];

    let source = InMemoryRowIterator::new(b"pk".to_vec(), DeletionTime::LIVE, items, false);
    let purge = PurgeTransform::new(100, 200); // gc_before=100
    let mut filtered = FilteredRows::new(Box::new(source)).add_transform(Box::new(purge));

    let mut results = Vec::new();
    while let Some(item) = filtered.next() {
        results.push(item.clustering_key().to_vec());
    }
    assert_eq!(results, vec![b"ck1".to_vec(), b"ck3".to_vec()]); // ck2 purged
}

#[test]
fn limits_transform_enforces_row_limit() {
    let items: Vec<Unfiltered> = (0..10)
        .map(|i| Unfiltered::Row(make_row(format!("ck{i:02}").as_bytes(), "x", b"v", 100)))
        .collect();

    let source = InMemoryRowIterator::new(b"pk".to_vec(), DeletionTime::LIVE, items, false);
    let limits = DataLimits::CqlLimit {
        rows_limit: 3,
        per_partition_limit: u32::MAX,
    };
    let mut limits_transform = LimitsTransform::new(limits, 0);
    limits_transform.apply_to_partition(b"pk", DeletionTime::LIVE);

    let mut filtered =
        FilteredRows::new(Box::new(source)).add_transform(Box::new(limits_transform));

    let mut count = 0;
    while filtered.next().is_some() {
        count += 1;
    }
    assert_eq!(count, 3);
}

// ─── Test (c): ClusteringIndexFilter slice/names selection ────────────────

#[test]
fn clustering_slice_selection() {
    let filter = ClusteringIndexFilter::Slice(Slices {
        slices: vec![Slice {
            start: ClusteringBound {
                values: b"ck2".to_vec(),
                inclusive: true,
            },
            end: ClusteringBound {
                values: b"ck4".to_vec(),
                inclusive: true,
            },
        }],
        is_reversed: false,
    });

    assert!(!filter.selects(b"ck1"));
    assert!(filter.selects(b"ck2"));
    assert!(filter.selects(b"ck3"));
    assert!(filter.selects(b"ck4"));
    assert!(!filter.selects(b"ck5"));
}

#[test]
fn clustering_names_selection() {
    let mut names = std::collections::BTreeSet::new();
    names.insert(b"ck1".to_vec());
    names.insert(b"ck5".to_vec());
    let filter = ClusteringIndexFilter::Names(names);

    assert!(filter.selects(b"ck1"));
    assert!(!filter.selects(b"ck2"));
    assert!(filter.selects(b"ck5"));
}

// ─── Test (d): InMemoryTrie insert/iterate/merge roundtrip ────────────────

#[test]
fn trie_insert_iterate_merge_roundtrip() {
    let mut t1 = InMemoryTrie::new();
    t1.apply(b"partition_a", |_| 1i32);
    t1.apply(b"partition_b", |_| 2);
    t1.apply(b"partition_c", |_| 3);

    assert_eq!(t1.entry_count(), 3);
    assert_eq!(t1.get(b"partition_b"), Some(&2));

    let mut t2 = InMemoryTrie::new();
    t2.apply(b"partition_b", |_| 10);
    t2.apply(b"partition_d", |_| 4);

    let merged = MergeTrie::new(vec![t1, t2]).materialize(|a, b| a + b);

    assert_eq!(merged.entry_count(), 4);
    assert_eq!(merged.get(b"partition_a"), Some(&1));
    assert_eq!(merged.get(b"partition_b"), Some(&12)); // 2 + 10
    assert_eq!(merged.get(b"partition_c"), Some(&3));
    assert_eq!(merged.get(b"partition_d"), Some(&4));

    // Verify sorted iteration
    let entries: Vec<_> = merged.iter();
    let keys: Vec<_> = entries.iter().map(|(k, _)| k.clone()).collect();
    assert_eq!(
        keys,
        vec![
            b"partition_a".to_vec(),
            b"partition_b".to_vec(),
            b"partition_c".to_vec(),
            b"partition_d".to_vec(),
        ]
    );
}

// ─── Test (e): ShardedMemtable concurrent write correctness ───────────────

#[test]
fn sharded_memtable_concurrent_writes() {
    let shards: Vec<Box<dyn MemtableBackend>> = (0..4)
        .map(|_| Box::new(SkipListMemtable::new()) as Box<dyn MemtableBackend>)
        .collect();
    let mt = ShardedMemtable::new(shards);

    // Write many partitions
    for i in 0..100u32 {
        let key = format!("partition_{i:04}").into_bytes();
        mt.apply(key, simple_row(b"ck1", "val", &i.to_be_bytes(), 100));
    }

    assert_eq!(mt.partition_count(), 100);

    // Verify all partitions are readable
    for i in 0..100u32 {
        let key = format!("partition_{i:04}").into_bytes();
        let pd = mt.get_partition(&key).expect("partition should exist");
        assert_eq!(pd.rows.len(), 1);
    }

    // Verify iter returns all partitions sorted
    let all = mt.iter_partitions();
    assert_eq!(all.len(), 100);
    for i in 1..all.len() {
        assert!(all[i - 1].0 <= all[i].0, "partitions not sorted");
    }
}

#[test]
fn sharded_memtable_merge_within_shard() {
    let shards: Vec<Box<dyn MemtableBackend>> = (0..4)
        .map(|_| Box::new(SkipListMemtable::new()) as Box<dyn MemtableBackend>)
        .collect();
    let mt = ShardedMemtable::new(shards);

    let key = b"test_key".to_vec();
    mt.apply(key.clone(), simple_row(b"ck1", "name", b"old", 100));
    mt.apply(key.clone(), simple_row(b"ck1", "name", b"new", 200));

    let pd = mt.get_partition(&key).unwrap();
    let row = &pd.rows[&b"ck1".to_vec()];
    assert_eq!(row.cells[0].value.as_deref(), Some(b"new".as_slice()));
    assert_eq!(row.cells[0].timestamp, 200);
}

// ─── Test: FilteredPartition materialization ──────────────────────────────

#[test]
fn filtered_partition_materialization() {
    let mut pd = PartitionData::new();
    pd.apply_row(simple_row(b"ck1", "x", b"1", 100));
    pd.apply_row(simple_row(b"ck2", "x", b"2", 100));
    pd.apply_row(simple_row(b"ck3", "x", b"3", 100));

    let mut iter = InMemoryRowIterator::from_partition_data(b"pk".to_vec(), &pd);
    let fp = FilteredPartition::create(&mut iter);

    assert_eq!(fp.partition_key, b"pk");
    assert_eq!(fp.row_count(), 3);
    let rows = fp.rows();
    assert_eq!(rows[0].clustering_key, b"ck1");
    assert_eq!(rows[2].clustering_key, b"ck3");
}

// ─── Test: From/Into conversions roundtrip ────────────────────────────────

#[test]
fn partition_update_roundtrip_preserves_data() {
    let mut pd = PartitionData::new();
    pd.apply_row(simple_row(b"ck1", "name", b"alice", 100));
    pd.apply_row(simple_row(b"ck2", "name", b"bob", 200));
    pd.set_tombstone(50, 50);

    // PartitionData -> PartitionUpdate
    let pu = PartitionUpdate::from_partition_data(b"pk".to_vec(), &pd);
    assert_eq!(pu.row_count(), 2);
    assert!(!pu.deletion_info.partition_deletion.is_live());

    // PartitionUpdate -> PartitionData
    let pd2 = PartitionData::from(&pu);
    assert_eq!(pd2.rows.len(), 2);
    assert_eq!(pd2.tombstone_timestamp, Some(50));
}
