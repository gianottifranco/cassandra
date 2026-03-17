// Licensed under Apache License, Version 2.0.

//! Integration tests for schema agreement with all metadata types.

use cassandra_schema::catalog::{SchemaCatalog, SchemaSnapshot};
use cassandra_schema::distributed_schema::DistributedSchema;
use cassandra_schema::keyspace::{KeyspaceMetadata, KeyspaceParams};
use cassandra_schema::schema_agreement::{
    check_schema_agreement, compute_schema_version, SchemaAgreementStatus,
};
use cassandra_schema::schema_change::{SchemaChangeEvent, SchemaChangeListener, SchemaChangeNotifier};
use cassandra_schema::trigger::TriggerDefinition;
use cassandra_schema::user_function::{UserAggregate, UserFunction};
use cassandra_schema::user_type::UserType;
use cassandra_schema::view::ViewMetadata;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

fn base_keyspace() -> KeyspaceMetadata {
    KeyspaceMetadata::new("test_ks", KeyspaceParams::default())
}

#[test]
fn schema_with_views_types_functions_deterministic() {
    let ks = base_keyspace()
        .with_view(
            ViewMetadata::new("v1", "test_ks", "t1")
                .with_where_clause("x IS NOT NULL")
                .with_column("x"),
        )
        .with_type(
            UserType::new("test_ks", "address")
                .with_field("street", "text")
                .with_field("city", "text"),
        )
        .with_function(
            UserFunction::new("test_ks", "double_val", "int", "java", "return x * 2;")
                .with_arg("x", "int"),
        );

    let mut snap1 = SchemaSnapshot::empty();
    snap1.keyspaces.insert("test_ks".to_string(), ks.clone());
    let v1 = compute_schema_version(&snap1);

    let mut snap2 = SchemaSnapshot::empty();
    snap2.keyspaces.insert("test_ks".to_string(), ks);
    let v2 = compute_schema_version(&snap2);

    assert_eq!(v1, v2, "Same schema should produce same version");
}

#[test]
fn two_nodes_same_schema_agree() {
    let ks = base_keyspace()
        .with_view(ViewMetadata::new("v1", "test_ks", "t1"));

    let mut snap = SchemaSnapshot::empty();
    snap.keyspaces.insert("test_ks".to_string(), ks);
    let version = compute_schema_version(&snap);

    let peers = vec![
        ("node1".to_string(), version),
        ("node2".to_string(), version),
    ];
    assert_eq!(
        check_schema_agreement(version, &peers),
        SchemaAgreementStatus::Agreed(version)
    );
}

#[test]
fn adding_view_changes_version() {
    let ks1 = base_keyspace();
    let mut snap1 = SchemaSnapshot::empty();
    snap1.keyspaces.insert("test_ks".to_string(), ks1);
    let v1 = compute_schema_version(&snap1);

    let ks2 = base_keyspace().with_view(ViewMetadata::new("v1", "test_ks", "t1"));
    let mut snap2 = SchemaSnapshot::empty();
    snap2.keyspaces.insert("test_ks".to_string(), ks2);
    let v2 = compute_schema_version(&snap2);

    assert_ne!(v1, v2, "Adding view should change version");

    // Two nodes disagree
    let peers = vec![("node2".to_string(), v2)];
    match check_schema_agreement(v1, &peers) {
        SchemaAgreementStatus::Disagreed { .. } => {} // expected
        other => panic!("Expected disagreement, got {:?}", other),
    }
}

#[test]
fn adding_type_changes_version() {
    let ks1 = base_keyspace();
    let mut snap1 = SchemaSnapshot::empty();
    snap1.keyspaces.insert("test_ks".to_string(), ks1);
    let v1 = compute_schema_version(&snap1);

    let ks2 = base_keyspace().with_type(UserType::new("test_ks", "addr").with_field("x", "int"));
    let mut snap2 = SchemaSnapshot::empty();
    snap2.keyspaces.insert("test_ks".to_string(), ks2);
    let v2 = compute_schema_version(&snap2);

    assert_ne!(v1, v2, "Adding type should change version");
}

#[test]
fn adding_function_changes_version() {
    let ks1 = base_keyspace();
    let mut snap1 = SchemaSnapshot::empty();
    snap1.keyspaces.insert("test_ks".to_string(), ks1);
    let v1 = compute_schema_version(&snap1);

    let ks2 = base_keyspace().with_function(
        UserFunction::new("test_ks", "f1", "int", "java", "return 1;"),
    );
    let mut snap2 = SchemaSnapshot::empty();
    snap2.keyspaces.insert("test_ks".to_string(), ks2);
    let v2 = compute_schema_version(&snap2);

    assert_ne!(v1, v2, "Adding function should change version");
}

#[test]
fn adding_aggregate_changes_version() {
    let ks1 = base_keyspace();
    let mut snap1 = SchemaSnapshot::empty();
    snap1.keyspaces.insert("test_ks".to_string(), ks1);
    let v1 = compute_schema_version(&snap1);

    let ks2 = base_keyspace().with_aggregate(
        UserAggregate::new("test_ks", "avg1", "int", "sfn").with_arg_type("int"),
    );
    let mut snap2 = SchemaSnapshot::empty();
    snap2.keyspaces.insert("test_ks".to_string(), ks2);
    let v2 = compute_schema_version(&snap2);

    assert_ne!(v1, v2, "Adding aggregate should change version");
}

#[test]
fn trigger_changes_version() {
    use cassandra_schema::column::ColumnMetadata;
    use cassandra_schema::table::TableMetadataBuilder;
    use cassandra_types::CqlType;

    let table = TableMetadataBuilder::new("test_ks", "t1")
        .add_column(ColumnMetadata::partition_key("id", 0, CqlType::Int))
        .build();
    let ks1 = base_keyspace().with_table(table.clone());
    let mut snap1 = SchemaSnapshot::empty();
    snap1.keyspaces.insert("test_ks".to_string(), ks1);
    let v1 = compute_schema_version(&snap1);

    let table_with_trigger = table.with_trigger(TriggerDefinition::new("t1", "com.Foo"));
    let ks2 = base_keyspace().with_table(table_with_trigger);
    let mut snap2 = SchemaSnapshot::empty();
    snap2.keyspaces.insert("test_ks".to_string(), ks2);
    let v2 = compute_schema_version(&snap2);

    assert_ne!(v1, v2, "Adding trigger should change version");
}

#[test]
fn distributed_schema_version_updates_on_mutations() {
    let catalog = SchemaCatalog::new();
    let mut ds = DistributedSchema::new(catalog);
    let v1 = ds.current_version();

    ds.apply_keyspace(
        base_keyspace().with_view(ViewMetadata::new("v1", "test_ks", "t1")),
        SchemaChangeEvent::KeyspaceCreated("test_ks".into()),
    );
    let v2 = ds.current_version();
    assert_ne!(v1, v2);

    // Verify snapshot reflects the change
    assert!(ds.snapshot().keyspace("test_ks").is_some());
    assert!(ds.snapshot().keyspace("test_ks").unwrap().view("v1").is_some());
}

struct CountingListener {
    count: AtomicUsize,
}

impl SchemaChangeListener for CountingListener {
    fn on_change(&self, _event: &SchemaChangeEvent) {
        self.count.fetch_add(1, Ordering::Relaxed);
    }
}

#[test]
fn distributed_schema_notifies_on_mutations() {
    let catalog = SchemaCatalog::new();
    let notifier = SchemaChangeNotifier::new();
    let listener = Arc::new(CountingListener {
        count: AtomicUsize::new(0),
    });
    notifier.register(listener.clone());

    let mut ds = DistributedSchema::with_notifier(catalog, notifier);

    ds.apply_keyspace(
        base_keyspace(),
        SchemaChangeEvent::KeyspaceCreated("test_ks".into()),
    );
    assert_eq!(listener.count.load(Ordering::Relaxed), 1);

    ds.remove_keyspace("test_ks");
    assert_eq!(listener.count.load(Ordering::Relaxed), 2);
}
