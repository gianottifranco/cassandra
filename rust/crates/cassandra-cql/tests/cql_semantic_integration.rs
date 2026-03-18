// Licensed under Apache License, Version 2.0.

//! Integration tests for the CQL semantic layer.
//!
//! Tests the parse → plan pipeline with restriction validation,
//! function evaluation, selector evaluation, and condition evaluation.

use cassandra_cql::ast::*;
use cassandra_cql::conditions::ConditionEvaluator;
use cassandra_cql::functions::FunctionRegistry;
use cassandra_cql::parser;
use cassandra_cql::planner;
use cassandra_cql::selection::selector_eval::SelectorEvaluator;
use cassandra_schema::column::{ClusteringOrder, ColumnMetadata};
use cassandra_schema::table::TableMetadataBuilder;
use cassandra_schema::{KeyspaceMetadata, KeyspaceParams, SchemaSnapshot};
use cassandra_types::CqlType;
use std::collections::HashMap;

fn test_schema() -> SchemaSnapshot {
    let table = TableMetadataBuilder::new("test_ks", "users")
        .add_column(ColumnMetadata::partition_key("user_id", 0, CqlType::Uuid))
        .add_column(ColumnMetadata::clustering(
            "created_at",
            0,
            CqlType::Timestamp,
            ClusteringOrder::Desc,
        ))
        .add_column(ColumnMetadata::regular("name", CqlType::Varchar))
        .add_column(ColumnMetadata::regular("age", CqlType::Int))
        .add_column(ColumnMetadata::regular(
            "tags",
            CqlType::Set(Box::new(CqlType::Varchar), false),
        ))
        .build();

    let ks = KeyspaceMetadata::new("test_ks", KeyspaceParams::default()).with_table(table);

    let mut snapshot = SchemaSnapshot::empty();
    snapshot.keyspaces.insert("test_ks".to_string(), ks);
    snapshot
}

// ── Restriction validation tests ──────────────────────────────────────

#[test]
fn invalid_where_missing_pk() {
    let schema = test_schema();
    let stmt = parser::parse("SELECT * FROM test_ks.users WHERE name = 'Alice'").unwrap();
    let result = planner::plan(&stmt, &schema, None);
    assert!(
        result.is_err(),
        "should reject WHERE without PK restriction"
    );
}

#[test]
fn invalid_where_bad_operator_for_type() {
    let schema = test_schema();
    // CONTAINS on a non-collection column
    let stmt =
        parser::parse("SELECT * FROM test_ks.users WHERE user_id = 123 AND name CONTAINS 'x'")
            .unwrap();
    let result = planner::plan(&stmt, &schema, None);
    assert!(
        result.is_err(),
        "should reject CONTAINS on non-collection column"
    );
}

#[test]
fn needs_allow_filtering() {
    let schema = test_schema();
    // Non-key column restriction without ALLOW FILTERING
    let stmt =
        parser::parse("SELECT * FROM test_ks.users WHERE user_id = 123 AND age > 21").unwrap();
    let result = planner::plan(&stmt, &schema, None);
    assert!(
        result.is_err(),
        "should reject non-key restriction without ALLOW FILTERING"
    );
}

#[test]
fn allow_filtering_accepted() {
    let schema = test_schema();
    let stmt = parser::parse(
        "SELECT * FROM test_ks.users WHERE user_id = 123 AND age > 21 ALLOW FILTERING",
    )
    .unwrap();
    let result = planner::plan(&stmt, &schema, None);
    assert!(result.is_ok(), "should accept with ALLOW FILTERING");

    if let planner::QueryPlan::Select(plan) = result.unwrap() {
        assert!(plan.restrictions.as_ref().unwrap().needs_filtering);
    } else {
        panic!("expected Select plan");
    }
}

#[test]
fn valid_pk_eq_plan() {
    let schema = test_schema();
    let stmt = parser::parse("SELECT * FROM test_ks.users WHERE user_id = 123").unwrap();
    let result = planner::plan(&stmt, &schema, None).unwrap();
    if let planner::QueryPlan::Select(plan) = result {
        let restrictions = plan.restrictions.unwrap();
        assert_eq!(restrictions.partition_key_restrictions.len(), 1);
        assert!(!restrictions.needs_filtering);
    } else {
        panic!("expected Select plan");
    }
}

#[test]
fn contains_on_collection_accepted() {
    let schema = test_schema();
    let stmt = parser::parse(
        "SELECT * FROM test_ks.users WHERE user_id = 123 AND tags CONTAINS 'admin' ALLOW FILTERING",
    )
    .unwrap();
    let result = planner::plan(&stmt, &schema, None);
    assert!(result.is_ok());
}

#[test]
fn undefined_column_rejected() {
    let schema = test_schema();
    let stmt =
        parser::parse("SELECT * FROM test_ks.users WHERE user_id = 123 AND nonexistent = 'x'")
            .unwrap();
    let result = planner::plan(&stmt, &schema, None);
    assert!(result.is_err());
}

#[test]
fn select_invalid_column_rejected() {
    let schema = test_schema();
    let stmt = parser::parse("SELECT nonexistent FROM test_ks.users").unwrap();
    let result = planner::plan(&stmt, &schema, None);
    assert!(result.is_err(), "should reject SELECT of undefined column");
}

#[test]
fn insert_column_value_mismatch() {
    let schema = test_schema();
    let stmt = parser::parse("INSERT INTO test_ks.users (user_id, name) VALUES (123)").unwrap();
    let result = planner::plan(&stmt, &schema, None);
    assert!(
        result.is_err(),
        "should reject INSERT with mismatched column/value counts"
    );
}

// ── Function evaluation tests ─────────────────────────────────────────

#[test]
fn now_returns_timeuuid() {
    let registry = FunctionRegistry::with_builtins();
    let func = registry.resolve_by_name("now").unwrap();
    let result = func.execute(&[]).unwrap().unwrap();
    assert_eq!(result.len(), 16);
    // Version should be 1
    let version = (result[6] >> 4) & 0x0F;
    assert_eq!(version, 1);
}

#[test]
fn cast_int_to_text_via_registry() {
    let registry = FunctionRegistry::with_builtins();
    let func = registry.resolve("cast", &[CqlType::Int]).unwrap();
    let bytes = 42i32.to_be_bytes();
    let result = func.execute(&[Some(&bytes)]).unwrap().unwrap();
    assert_eq!(String::from_utf8(result).unwrap(), "42");
}

#[test]
fn count_aggregate() {
    use cassandra_cql::functions::aggregates::{AggregateFunction, CountStarAggregate};

    let agg = CountStarAggregate;
    let mut state = agg.init_state();
    for _ in 0..10 {
        state = agg.accumulate(state.as_deref(), Some(&[0])).unwrap();
    }
    let result = agg.finalize(state.as_deref()).unwrap().unwrap();
    let count = i64::from_be_bytes(result.try_into().unwrap());
    assert_eq!(count, 10);
}

// ── Selector evaluation tests ─────────────────────────────────────────

#[test]
fn selector_eval_column() {
    let registry = FunctionRegistry::new();
    let eval = SelectorEvaluator::new(&registry);

    let mut columns = HashMap::new();
    columns.insert("name".to_string(), 0);
    columns.insert("age".to_string(), 1);

    let row = vec![Some(b"Alice".to_vec()), Some(30i32.to_be_bytes().to_vec())];

    let result = eval.evaluate(&Selector::Column("name".into()), &columns, &row, None);
    assert_eq!(result, Some(b"Alice".to_vec()));
}

#[test]
fn selector_eval_alias() {
    let registry = FunctionRegistry::new();
    let eval = SelectorEvaluator::new(&registry);

    let mut columns = HashMap::new();
    columns.insert("name".to_string(), 0);
    let row = vec![Some(b"Bob".to_vec())];

    let sel = Selector::Alias {
        selector: Box::new(Selector::Column("name".into())),
        alias: "user_name".into(),
    };
    let result = eval.evaluate(&sel, &columns, &row, None);
    assert_eq!(result, Some(b"Bob".to_vec()));
}

#[test]
fn selector_eval_function() {
    let registry = FunctionRegistry::with_builtins();
    let eval = SelectorEvaluator::new(&registry);
    let columns = HashMap::new();
    let row = vec![];

    let sel = Selector::Function("now".into(), vec![]);
    let result = eval.evaluate(&sel, &columns, &row, None);
    assert!(result.is_some());
}

// ── Condition evaluation tests ────────────────────────────────────────

#[test]
fn condition_if_exists_true() {
    let result = ConditionEvaluator::eval_if_exists(true);
    assert!(result.applied);
}

#[test]
fn condition_if_exists_false() {
    let result = ConditionEvaluator::eval_if_exists(false);
    assert!(!result.applied);
}

#[test]
fn condition_column_eq_match() {
    let mut columns = HashMap::new();
    columns.insert("age".to_string(), 0);
    let mut types = HashMap::new();
    types.insert("age".to_string(), CqlType::Bigint);

    let row = vec![Some(30i64.to_be_bytes().to_vec())];
    let conditions = vec![Relation {
        column: "age".to_string(),
        op: RelationOp::Eq,
        value: Term::Literal(Literal::Integer(30)),
    }];

    let result = ConditionEvaluator::eval_conditions(&conditions, &columns, &types, Some(&row));
    assert!(result.applied);
}

#[test]
fn condition_column_eq_no_match() {
    let mut columns = HashMap::new();
    columns.insert("age".to_string(), 0);
    let mut types = HashMap::new();
    types.insert("age".to_string(), CqlType::Bigint);

    let row = vec![Some(30i64.to_be_bytes().to_vec())];
    let conditions = vec![Relation {
        column: "age".to_string(),
        op: RelationOp::Eq,
        value: Term::Literal(Literal::Integer(99)),
    }];

    let result = ConditionEvaluator::eval_conditions(&conditions, &columns, &types, Some(&row));
    assert!(!result.applied);
    assert!(result.current_values.is_some());
}

// ── Parser tests for new statement types ──────────────────────────────

#[test]
fn parse_alter_type_add_field() {
    let stmt = parser::parse("ALTER TYPE test_ks.address ADD zip_code text").unwrap();
    match stmt {
        Statement::AlterType(at) => {
            assert_eq!(at.name, "address");
            assert!(matches!(at.operation, AlterTypeOp::AddField(..)));
        }
        _ => panic!("expected AlterType"),
    }
}

#[test]
fn parse_alter_type_rename() {
    let stmt = parser::parse("ALTER TYPE test_ks.address RENAME street TO road").unwrap();
    match stmt {
        Statement::AlterType(at) => {
            assert!(matches!(at.operation, AlterTypeOp::RenameField(..)));
        }
        _ => panic!("expected AlterType"),
    }
}

#[test]
fn parse_alter_materialized_view() {
    let stmt =
        parser::parse("ALTER MATERIALIZED VIEW test_ks.my_view WITH gc_grace_seconds = 3600")
            .unwrap();
    match stmt {
        Statement::AlterMaterializedView(amv) => {
            assert_eq!(amv.name, "my_view");
            assert!(amv.options.contains_key("gc_grace_seconds"));
        }
        _ => panic!("expected AlterMaterializedView"),
    }
}

#[test]
fn parse_describe_cluster() {
    let stmt = parser::parse("DESCRIBE CLUSTER").unwrap();
    match stmt {
        Statement::Describe(d) => {
            assert!(matches!(d.target, DescribeTarget::Cluster));
        }
        _ => panic!("expected Describe"),
    }
}

#[test]
fn parse_describe_keyspace() {
    let stmt = parser::parse("DESCRIBE KEYSPACE test_ks").unwrap();
    match stmt {
        Statement::Describe(d) => {
            assert!(matches!(d.target, DescribeTarget::Keyspace(ref k) if k == "test_ks"));
        }
        _ => panic!("expected Describe"),
    }
}

#[test]
fn parse_describe_table() {
    let stmt = parser::parse("DESCRIBE TABLE test_ks.users").unwrap();
    match stmt {
        Statement::Describe(d) => match d.target {
            DescribeTarget::Table(ks, name) => {
                assert_eq!(ks, Some("test_ks".to_string()));
                assert_eq!(name, "users");
            }
            _ => panic!("expected Table target"),
        },
        _ => panic!("expected Describe"),
    }
}

#[test]
fn describe_is_plannable() {
    let schema = test_schema();
    let stmt = parser::parse("DESCRIBE CLUSTER").unwrap();
    let result = planner::plan(&stmt, &schema, None);
    assert!(result.is_ok(), "DESCRIBE should be plannable");
    assert!(matches!(result.unwrap(), planner::QueryPlan::Describe(_)));
}
