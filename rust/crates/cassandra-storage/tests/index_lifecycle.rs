// Licensed under Apache License, Version 2.0.

//! Integration tests for the full secondary index lifecycle.
//!
//! Tests cover: create → insert → query → rebuild → delete → drop.

use std::collections::HashMap;
use std::sync::Arc;

use cassandra_storage::index::lifecycle::SecondaryIndexManager;
use cassandra_storage::index::{IndexDefinition, IndexManager, IndexStatus, IndexType};
use cassandra_storage::memtable::partition::{Cell, PartitionData, Row};

fn make_def(name: &str, column: &str) -> IndexDefinition {
    IndexDefinition {
        name: name.into(),
        keyspace: "ks".into(),
        table: "tbl".into(),
        column: column.into(),
        index_type: IndexType::Legacy,
        options: HashMap::new(),
    }
}

fn make_row(ck: &[u8], column: &str, value: &[u8]) -> Row {
    Row {
        clustering_key: ck.to_vec(),
        cells: vec![Cell {
            column: column.into(),
            value: Some(value.to_vec()),
            timestamp: 1,
            ttl: 0,
            local_deletion_time: None,
            is_tombstone: false,
        }],
        is_tombstone: false,
        local_deletion_time: None,
    }
}

/// Test 1: Create Legacy index → insert rows → query by indexed value → verify results
#[test]
fn create_insert_query() {
    let mgr = Arc::new(IndexManager::new());
    let sim = SecondaryIndexManager::new(mgr.clone());

    let def = make_def("email_idx", "email");
    sim.add_index(def).unwrap();

    // Build the index with initial data
    let mut pd = PartitionData::new();
    pd.apply_row(make_row(b"ck1", "email", b"alice@example.com"));
    pd.apply_row(make_row(b"ck2", "email", b"bob@example.com"));

    let partitions = vec![(b"pk1".to_vec(), pd)];
    sim.rebuild_index("email_idx", &partitions).unwrap();

    // Query
    let results = mgr.search("email_idx", b"alice@example.com").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].partition_key, b"pk1");
    assert_eq!(results[0].clustering_key, b"ck1");

    // Query for bob
    let results = mgr.search("email_idx", b"bob@example.com").unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].clustering_key, b"ck2");

    // Query for non-existent
    let results = mgr.search("email_idx", b"charlie@example.com").unwrap();
    assert!(results.is_empty());
}

/// Test 2: Create index → insert → delete row → verify index updated
#[test]
fn insert_then_delete_updates_index() {
    let mgr = Arc::new(IndexManager::new());
    let sim = SecondaryIndexManager::new(mgr.clone());

    let def = make_def("email_idx", "email");
    sim.add_index(def).unwrap();

    // Rebuild makes it queryable
    let mut pd = PartitionData::new();
    pd.apply_row(make_row(b"ck1", "email", b"alice@example.com"));
    sim.rebuild_index("email_idx", &[(b"pk1".to_vec(), pd)])
        .unwrap();

    // Verify present
    let results = mgr.search("email_idx", b"alice@example.com").unwrap();
    assert_eq!(results.len(), 1);

    // Delete via index manager notification
    mgr.on_delete(b"pk1", b"ck1", "email", b"alice@example.com")
        .unwrap();

    // Verify gone
    let results = mgr.search("email_idx", b"alice@example.com").unwrap();
    assert!(results.is_empty());
}

/// Test 3: Create index → rebuild from data → verify search works
#[test]
fn rebuild_repopulates_index() {
    let mgr = Arc::new(IndexManager::new());
    let sim = SecondaryIndexManager::new(mgr.clone());

    let def = make_def("city_idx", "city");
    sim.add_index(def).unwrap();

    // Initially building, not queryable
    assert_eq!(mgr.get_status("city_idx"), Some(IndexStatus::Building));

    // Rebuild with data
    let mut pd1 = PartitionData::new();
    pd1.apply_row(make_row(b"ck1", "city", b"NYC"));
    let mut pd2 = PartitionData::new();
    pd2.apply_row(make_row(b"ck2", "city", b"SF"));
    pd2.apply_row(make_row(b"ck3", "city", b"NYC"));

    let partitions = vec![(b"pk1".to_vec(), pd1), (b"pk2".to_vec(), pd2)];
    sim.rebuild_index("city_idx", &partitions).unwrap();

    // Now queryable
    assert_eq!(mgr.get_status("city_idx"), Some(IndexStatus::QueryReady));

    let nyc_results = mgr.search("city_idx", b"NYC").unwrap();
    assert_eq!(nyc_results.len(), 2);

    let sf_results = mgr.search("city_idx", b"SF").unwrap();
    assert_eq!(sf_results.len(), 1);
}

/// Test 4: Drop index → verify search fails
#[test]
fn drop_index_removes_search_capability() {
    let mgr = Arc::new(IndexManager::new());
    let sim = SecondaryIndexManager::new(mgr.clone());

    let def = make_def("name_idx", "name");
    sim.add_index(def).unwrap();

    let mut pd = PartitionData::new();
    pd.apply_row(make_row(b"ck1", "name", b"Alice"));
    sim.rebuild_index("name_idx", &[(b"pk1".to_vec(), pd)])
        .unwrap();

    // Can search
    let results = mgr.search("name_idx", b"Alice").unwrap();
    assert_eq!(results.len(), 1);

    // Drop
    sim.remove_index("name_idx").unwrap();
    assert!(!mgr.has_index("name_idx"));
    assert_eq!(mgr.count(), 0);

    // Search fails with NotFound
    let err = mgr.search("name_idx", b"Alice").unwrap_err();
    assert!(err.to_string().contains("not found"));
}

/// Test 5: Status tracking — index not queryable during build
#[test]
fn not_queryable_during_build() {
    let mgr = Arc::new(IndexManager::new());
    let sim = SecondaryIndexManager::new(mgr.clone());

    let def = make_def("status_idx", "col");
    sim.add_index(def).unwrap();

    // Status is Building after add
    assert_eq!(mgr.get_status("status_idx"), Some(IndexStatus::Building));
    assert!(!mgr.is_queryable("status_idx"));

    // Insert data directly — it still goes in, but search returns empty
    mgr.on_write(b"pk1", b"ck1", "col", b"val").unwrap();
    let results = mgr.search("status_idx", b"val").unwrap();
    assert!(results.is_empty(), "should be empty during build");

    // Mark as queryable
    mgr.set_status("status_idx", IndexStatus::QueryReady);
    let results = mgr.search("status_idx", b"val").unwrap();
    assert_eq!(results.len(), 1, "should be visible when query-ready");
}

/// Test: Metadata conversion round-trip
#[test]
fn metadata_conversion_round_trip() {
    use cassandra_schema::index::{IndexKind, IndexMetadata};

    let mut opts = HashMap::new();
    opts.insert("target".into(), "email".into());
    let meta = IndexMetadata::new("id1".into(), "email_idx".into(), IndexKind::Keys, opts);

    let def = IndexDefinition::from_metadata(&meta, "ks", "users");
    assert_eq!(def.name, "email_idx");
    assert_eq!(def.keyspace, "ks");
    assert_eq!(def.table, "users");
    assert_eq!(def.column, "email");
    assert_eq!(def.index_type, IndexType::Legacy);

    let back = def.to_metadata();
    assert_eq!(back.name, "email_idx");
    assert_eq!(back.kind, IndexKind::Keys);
    assert_eq!(back.target_column().unwrap(), "email");
}
