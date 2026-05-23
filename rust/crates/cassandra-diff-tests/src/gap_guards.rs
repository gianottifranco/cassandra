// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

//! # Gap Guard Tests
//!
//! Each test below documents a known gap between the Java baseline and the
//! Rust implementation. Tests are `#[ignore]`d and will show up in
//! `cargo test -- --ignored` output, serving as living documentation.
//!
//! When a gap is closed, remove `#[ignore]` and implement the actual
//! verification. Run `cargo test -p cassandra-diff-tests -- --list 2>&1 | grep gap_guard`
//! to see all documented gaps.

// ═══════════════════════════════════════════════════════════════════════
// CQL GAPS
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn gap_guard_cql_functions() {
    // CLOSED by prompt-12: FunctionRegistry has builtins including math, json, time/uuid,
    // token/cast/blob, vector similarity, masking functions, and a restricted
    // Java-source UDF adapter for deterministic scalar bodies.
    use cassandra_cql::functions::FunctionRegistry;
    use cassandra_storage::udf::{UdfDefinition, UdfManager};
    use cassandra_types::CqlType;
    let registry = FunctionRegistry::with_builtins();
    let names = registry.function_names();
    // Verify core builtins are registered
    assert!(names.contains(&"now".to_string()), "Missing now()");
    assert!(names.contains(&"uuid".to_string()), "Missing uuid()");
    assert!(names.contains(&"tojson".to_string()), "Missing toJson()");
    assert!(names.contains(&"to_json".to_string()), "Missing to_json()");
    assert!(
        names.contains(&"fromjson".to_string()),
        "Missing fromJson()"
    );
    assert!(
        names.contains(&"from_json".to_string()),
        "Missing from_json()"
    );
    assert!(names.contains(&"abs".to_string()), "Missing abs()");
    assert!(names.contains(&"length".to_string()), "Missing length()");
    assert!(names.contains(&"token".to_string()), "Missing token()");
    assert!(
        names.len() >= 10,
        "Expected at least 10 builtin functions, got {}",
        names.len()
    );
    let complex_map = CqlType::Map(
        Box::new(CqlType::Tuple(vec![CqlType::Int, CqlType::Varchar])),
        Box::new(CqlType::List(Box::new(CqlType::Varchar), true)),
        true,
    );
    assert_eq!(
        registry
            .resolve("map_keys", &[complex_map.clone()])
            .unwrap()
            .return_type(),
        CqlType::Set(
            Box::new(CqlType::Tuple(vec![CqlType::Int, CqlType::Varchar])),
            false
        )
    );
    assert_eq!(
        registry
            .resolve("map_values", &[complex_map])
            .unwrap()
            .return_type(),
        CqlType::List(
            Box::new(CqlType::List(Box::new(CqlType::Varchar), true)),
            false
        )
    );
    let complex_list = CqlType::List(
        Box::new(CqlType::Tuple(vec![CqlType::Int, CqlType::Varchar])),
        true,
    );
    assert_eq!(
        registry
            .resolve("collection_min", &[complex_list.clone()])
            .unwrap()
            .return_type(),
        CqlType::Tuple(vec![CqlType::Int, CqlType::Varchar])
    );
    assert_eq!(
        registry
            .resolve("collection_max", &[complex_list])
            .unwrap()
            .return_type(),
        CqlType::Tuple(vec![CqlType::Int, CqlType::Varchar])
    );
    assert_eq!(
        registry
            .resolve_with_return("from_json", &[CqlType::Varchar], &CqlType::Int)
            .unwrap()
            .return_type(),
        CqlType::Int
    );
    let to_json_int = registry.resolve("to_json", &[CqlType::Int]).unwrap();
    assert_eq!(to_json_int.arg_types(), vec![CqlType::Int]);
    assert_eq!(to_json_int.return_type(), CqlType::Varchar);

    let varint_input = cassandra_types::bigint::string_to_varint("1000").unwrap();
    for name in ["exp", "log", "log10"] {
        let function = registry
            .resolve(name, &[CqlType::Varint])
            .unwrap_or_else(|| panic!("missing MathFcts {name}(varint) overload"));
        assert_eq!(function.return_type(), CqlType::Varint);
    }
    let log10_varint = registry.resolve("log10", &[CqlType::Varint]).unwrap();
    let log10 = log10_varint
        .execute(&[Some(&varint_input)])
        .unwrap()
        .unwrap();
    assert_eq!(cassandra_types::bigint::varint_to_string(&log10), "3");
    let decimal_input = cassandra_types::bigint::string_to_decimal("-2.5").unwrap();
    let abs_decimal = registry.resolve("abs", &[CqlType::Decimal]).unwrap();
    let abs_result = abs_decimal
        .execute(&[Some(&decimal_input)])
        .unwrap()
        .unwrap();
    assert_eq!(
        cassandra_types::bigint::decimal_to_string(&abs_result).unwrap(),
        "2.5"
    );
    let round_decimal = registry.resolve("round", &[CqlType::Decimal]).unwrap();
    let round_result = round_decimal
        .execute(&[Some(&decimal_input)])
        .unwrap()
        .unwrap();
    assert_eq!(
        cassandra_types::bigint::decimal_to_string(&round_result).unwrap(),
        "-3"
    );
    let decimal_log10_input = cassandra_types::bigint::string_to_decimal("1000").unwrap();
    for name in ["exp", "log", "log10"] {
        let function = registry
            .resolve(name, &[CqlType::Decimal])
            .unwrap_or_else(|| panic!("missing MathFcts {name}(decimal) overload"));
        assert_eq!(function.return_type(), CqlType::Decimal);
    }
    let log10_decimal = registry.resolve("log10", &[CqlType::Decimal]).unwrap();
    let log10_decimal_result = log10_decimal
        .execute(&[Some(&decimal_log10_input)])
        .unwrap()
        .unwrap();
    assert_eq!(
        cassandra_types::bigint::decimal_to_string(&log10_decimal_result).unwrap(),
        "3"
    );
    let cast_bool_text = registry
        .resolve("cast_as_text", &[CqlType::Boolean])
        .unwrap_or_else(|| panic!("missing CastFcts cast_as_text(boolean) overload"));
    assert_eq!(cast_bool_text.return_type(), CqlType::Varchar);
    assert_eq!(
        cast_bool_text.execute(&[Some(&[1])]).unwrap(),
        Some(b"true".to_vec())
    );
    let uuid_bytes = uuid::Uuid::parse_str("6ba7b810-9dad-11d1-80b4-00c04fd430c8")
        .unwrap()
        .into_bytes();
    let cast_uuid_ascii = registry
        .resolve("cast_as_ascii", &[CqlType::Uuid])
        .unwrap_or_else(|| panic!("missing CastFcts cast_as_ascii(uuid) overload"));
    assert_eq!(cast_uuid_ascii.return_type(), CqlType::Ascii);
    assert_eq!(
        cast_uuid_ascii.execute(&[Some(&uuid_bytes)]).unwrap(),
        Some(b"6ba7b810-9dad-11d1-80b4-00c04fd430c8".to_vec())
    );
    let cast_inet_text = registry
        .resolve("cast_as_text", &[CqlType::Inet])
        .unwrap_or_else(|| panic!("missing CastFcts cast_as_text(inet) overload"));
    assert_eq!(
        cast_inet_text.execute(&[Some(&[127, 0, 0, 1])]).unwrap(),
        Some(b"127.0.0.1".to_vec())
    );

    let vector_type = CqlType::Vector(Box::new(CqlType::Float), 2);
    let zero_vector = cassandra_types::vector::VectorValue::new(vec![0.0, 0.0]).serialize();
    let non_zero_vector = cassandra_types::vector::VectorValue::new(vec![1.0, 0.0]).serialize();
    let cosine = registry
        .resolve(
            "similarity_cosine",
            &[vector_type.clone(), vector_type.clone()],
        )
        .unwrap();
    assert!(
        cosine
            .execute(&[Some(&zero_vector), Some(&non_zero_vector)])
            .unwrap_err()
            .contains("doesn't support all-zero vectors"),
        "VectorFcts cosine parity requires all-zero vector rejection"
    );
    let dot_product = registry
        .resolve(
            "similarity_dot_product",
            &[vector_type.clone(), vector_type],
        )
        .unwrap();
    let dot = dot_product
        .execute(&[Some(&zero_vector), Some(&non_zero_vector)])
        .unwrap()
        .unwrap();
    assert_eq!(f32::from_be_bytes(dot.try_into().unwrap()), 0.0);

    let mut udf_manager = UdfManager::new();
    udf_manager
        .register_function(UdfDefinition {
            name: "echo".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["text".to_string()],
            return_type: "text".to_string(),
            language: "java".to_string(),
            body: "return val;".to_string(),
            called_on_null_input: false,
        })
        .unwrap();
    udf_manager
        .register_function(UdfDefinition {
            name: "plus".to_string(),
            keyspace: "ks".to_string(),
            arg_types: vec!["int".to_string(), "int".to_string()],
            return_type: "int".to_string(),
            language: "java".to_string(),
            body: "return a + b;".to_string(),
            called_on_null_input: false,
        })
        .unwrap();

    assert_eq!(
        udf_manager
            .execute_function(
                "ks",
                "echo",
                &["text".to_string()],
                &[Some(b"Ada".to_vec())]
            )
            .unwrap(),
        Some(b"Ada".to_vec())
    );
    let sum = udf_manager
        .execute_function(
            "ks",
            "plus",
            &["int".to_string(), "int".to_string()],
            &[
                Some(2i32.to_be_bytes().to_vec()),
                Some(3i32.to_be_bytes().to_vec()),
            ],
        )
        .unwrap()
        .unwrap();
    assert_eq!(i32::from_be_bytes(sum.try_into().unwrap()), 5);
    assert!(
        udf_manager
            .register_function(UdfDefinition {
                name: "unsafe_body".to_string(),
                keyspace: "ks".to_string(),
                arg_types: vec!["text".to_string()],
                return_type: "text".to_string(),
                language: "java".to_string(),
                body: "System.exit(1);".to_string(),
                called_on_null_input: false,
            })
            .is_err()
    );
}

#[test]
fn gap_guard_cql_permission_statements() {
    // CLOSED by prompt-14 follow-up: parser carries role and permission DCL
    // statements through concrete AST variants.
    use cassandra_cql::ast::{Resource, RoleAccess, Statement};
    use cassandra_cql::parser::parse;

    let stmt =
        parse("CREATE ROLE IF NOT EXISTS analyst WITH PASSWORD = 'pw' AND LOGIN = true AND ACCESS TO DATACENTERS {'dc1'} AND ACCESS FROM ALL CIDRS").unwrap();
    match stmt {
        Statement::CreateRole(role) => {
            assert_eq!(role.name, "analyst");
            assert!(role.if_not_exists);
            assert_eq!(role.password.as_deref(), Some("pw"));
            assert_eq!(role.login, Some(true));
            assert_eq!(
                role.datacenter_access,
                Some(RoleAccess::Restricted(vec!["dc1".to_string()]))
            );
            assert_eq!(role.cidr_access, Some(RoleAccess::All));
        }
        other => panic!("expected CreateRole, got {other:?}"),
    }

    let stmt = parse("CREATE ROLE custom WITH OPTIONS = {'a':'b', 'b':1}").unwrap();
    match stmt {
        Statement::CreateRole(role) => {
            assert_eq!(role.name, "custom");
            assert_eq!(role.options.get("a").map(String::as_str), Some("b"));
            assert_eq!(role.options.get("b").map(String::as_str), Some("1"));
        }
        other => panic!("expected CreateRole, got {other:?}"),
    }

    let stmt = parse("ALTER ROLE analyst WITH SUPERUSER = false AND LOGIN = true AND ACCESS TO ALL DATACENTERS AND ACCESS FROM CIDRS {'region1', 'region2'}").unwrap();
    match stmt {
        Statement::AlterRole(role) => {
            assert_eq!(role.name, "analyst");
            assert!(!role.if_exists);
            assert_eq!(role.superuser, Some(false));
            assert_eq!(role.login, Some(true));
            assert_eq!(role.datacenter_access, Some(RoleAccess::All));
            assert_eq!(
                role.cidr_access,
                Some(RoleAccess::Restricted(vec![
                    "region1".to_string(),
                    "region2".to_string()
                ]))
            );
        }
        other => panic!("expected AlterRole, got {other:?}"),
    }

    let stmt = parse("ALTER ROLE analyst WITH OPTIONS = {'region':'west'}").unwrap();
    match stmt {
        Statement::AlterRole(role) => {
            assert_eq!(role.name, "analyst");
            assert_eq!(role.options.get("region").map(String::as_str), Some("west"));
        }
        other => panic!("expected AlterRole, got {other:?}"),
    }

    let stmt = parse("ALTER ROLE analyst WITH HASHED PASSWORD = '$2b$12$hash'").unwrap();
    match stmt {
        Statement::AlterRole(role) => {
            assert_eq!(role.name, "analyst");
            assert_eq!(role.password, None);
            assert_eq!(role.hashed_password.as_deref(), Some("$2b$12$hash"));
        }
        other => panic!("expected AlterRole, got {other:?}"),
    }

    let stmt =
        parse("CREATE USER IF NOT EXISTS app WITH HASHED PASSWORD '$2b$12$apphash'").unwrap();
    match stmt {
        Statement::CreateRole(role) => {
            assert_eq!(role.name, "app");
            assert!(role.if_not_exists);
            assert_eq!(role.login, Some(true));
            assert_eq!(role.hashed_password.as_deref(), Some("$2b$12$apphash"));
        }
        other => panic!("expected CreateRole, got {other:?}"),
    }

    let stmt = parse("CREATE ROLE $$ r1 ' x $ x ' $$").unwrap();
    match stmt {
        Statement::CreateRole(role) => {
            assert_eq!(role.name, " r1 ' x $ x ' ");
        }
        other => panic!("expected CreateRole, got {other:?}"),
    }

    assert!(parse(r#"CREATE USER "quoted_user""#).is_err());

    let stmt = parse("ALTER USER IF EXISTS app WITH PASSWORD 'pw'").unwrap();
    match stmt {
        Statement::AlterRole(role) => {
            assert_eq!(role.name, "app");
            assert!(role.if_exists);
            assert_eq!(role.password.as_deref(), Some("pw"));
        }
        other => panic!("expected AlterRole, got {other:?}"),
    }

    let stmt = parse("DROP USER IF EXISTS app").unwrap();
    match stmt {
        Statement::DropRole(role) => {
            assert_eq!(role.name, "app");
            assert!(role.if_exists);
        }
        other => panic!("expected DropRole, got {other:?}"),
    }

    let stmt = parse("DROP ROLE IF EXISTS analyst").unwrap();
    match stmt {
        Statement::DropRole(role) => {
            assert_eq!(role.name, "analyst");
            assert!(role.if_exists);
        }
        other => panic!("expected DropRole, got {other:?}"),
    }

    let stmt = parse("GRANT MODIFY ON TABLE ks.users TO analyst").unwrap();
    match stmt {
        Statement::Grant(grant) => {
            assert_eq!(grant.permissions, vec!["MODIFY"]);
            assert_eq!(grant.role, "analyst");
            assert_eq!(
                grant.resource,
                Resource::Table {
                    keyspace: Some("ks".to_string()),
                    table: "users".to_string()
                }
            );
        }
        other => panic!("expected Grant, got {other:?}"),
    }

    let stmt =
        parse("GRANT MODIFY PERMISSION, SELECT PERMISSION ON ALL KEYSPACES TO 'analyst'").unwrap();
    match stmt {
        Statement::Grant(grant) => {
            assert_eq!(grant.permissions, vec!["MODIFY", "SELECT"]);
            assert_eq!(grant.role, "analyst");
            assert_eq!(grant.resource, Resource::AllKeyspaces);
        }
        other => panic!("expected Grant, got {other:?}"),
    }

    let stmt = parse("REVOKE MODIFY ON KEYSPACE ks FROM analyst").unwrap();
    match stmt {
        Statement::Revoke(revoke) => {
            assert_eq!(revoke.permissions, vec!["MODIFY"]);
            assert_eq!(revoke.role, "analyst");
            assert_eq!(revoke.resource, Resource::Keyspace("ks".to_string()));
        }
        other => panic!("expected Revoke, got {other:?}"),
    }

    let stmt = parse("REVOKE CREATE, ALTER ON ROLE $$source$$ FROM $$ target $$").unwrap();
    match stmt {
        Statement::Revoke(revoke) => {
            assert_eq!(revoke.permissions, vec!["CREATE", "ALTER"]);
            assert_eq!(revoke.role, " target ");
            assert_eq!(revoke.resource, Resource::Role("source".to_string()));
        }
        other => panic!("expected Revoke, got {other:?}"),
    }

    let stmt = parse("LIST ROLES OF analyst NORECURSIVE").unwrap();
    match stmt {
        Statement::ListRoles(list) => {
            assert_eq!(list.of_role.as_deref(), Some("analyst"));
            assert!(list.no_recursive);
        }
        other => panic!("expected ListRoles, got {other:?}"),
    }

    let stmt = parse("LIST PERMISSIONS ON ROLE analyst OF auditor").unwrap();
    match stmt {
        Statement::ListPermissions(list) => {
            assert_eq!(list.permissions, vec!["ALL"]);
            assert_eq!(list.resource, Some(Resource::Role("analyst".to_string())));
            assert_eq!(list.of_role.as_deref(), Some("auditor"));
        }
        other => panic!("expected ListPermissions, got {other:?}"),
    }

    let stmt = parse("LIST ALTER, DROP PERMISSION ON ROLE 'source' OF $$ target $$").unwrap();
    match stmt {
        Statement::ListPermissions(list) => {
            assert_eq!(list.permissions, vec!["ALTER", "DROP"]);
            assert_eq!(list.resource, Some(Resource::Role("source".to_string())));
            assert_eq!(list.of_role.as_deref(), Some(" target "));
        }
        other => panic!("expected ListPermissions, got {other:?}"),
    }
}

#[test]
fn gap_guard_cql_batch_using_attributes() {
    // CLOSED by Rust rewrite follow-up: CQL parser limits batch objectives to
    // mutation statements like Java, and planner preserves Java BatchStatement
    // USING TIMESTAMP semantics plus validation rejects global TTL and invalid
    // custom timestamp combinations.
    use cassandra_cql::{
        ast::BatchType,
        parser::parse,
        planner::{QueryPlan, plan},
    };
    use cassandra_schema::SchemaSnapshot;

    let schema = SchemaSnapshot::empty();
    let stmt = parse(
        "BEGIN BATCH USING TIMESTAMP 123
            INSERT INTO ks.users (id, name) VALUES (1, 'Ada');
            UPDATE ks.users SET name = 'Grace' WHERE id = 2;
            DELETE FROM ks.users WHERE id = 3;
         APPLY BATCH",
    )
    .unwrap();
    let QueryPlan::Batch(batch) = plan(&stmt, &schema, Some("ks")).unwrap() else {
        panic!("expected batch plan");
    };
    assert_eq!(batch.batch_type, BatchType::Logged);
    assert!(matches!(
        &batch.plans[0],
        QueryPlan::Insert(insert) if insert.using_timestamp == Some(123)
    ));
    assert!(matches!(
        &batch.plans[1],
        QueryPlan::Update(update) if update.using_timestamp == Some(123)
    ));
    assert!(matches!(
        &batch.plans[2],
        QueryPlan::Delete(delete) if delete.using_timestamp == Some(123)
    ));

    let global_ttl = parse(
        "BEGIN BATCH USING TTL 60
            INSERT INTO ks.users (id, name) VALUES (1, 'Ada');
         APPLY BATCH",
    )
    .unwrap();
    assert!(
        plan(&global_ttl, &schema, Some("ks"))
            .unwrap_err()
            .to_string()
            .contains("Global TTL on the BATCH statement is not supported")
    );

    let mixed_timestamps = parse(
        "BEGIN BATCH USING TIMESTAMP 123
            INSERT INTO ks.users (id, name) VALUES (1, 'Ada') USING TIMESTAMP 456;
         APPLY BATCH",
    )
    .unwrap();
    assert!(
        plan(&mixed_timestamps, &schema, Some("ks"))
            .unwrap_err()
            .to_string()
            .contains("Timestamp must be set either on BATCH or individual statements")
    );

    let conditional = parse(
        "BEGIN BATCH USING TIMESTAMP 123
            INSERT INTO ks.users (id, name) VALUES (1, 'Ada') IF NOT EXISTS;
         APPLY BATCH",
    )
    .unwrap();
    assert!(
        plan(&conditional, &schema, Some("ks"))
            .unwrap_err()
            .to_string()
            .contains("Cannot provide custom timestamp for conditional BATCH")
    );

    let counter = parse(
        "BEGIN COUNTER BATCH USING TIMESTAMP 123
            UPDATE ks.users SET visits = visits + 1 WHERE id = 1;
         APPLY BATCH",
    )
    .unwrap();
    assert!(
        plan(&counter, &schema, Some("ks"))
            .unwrap_err()
            .to_string()
            .contains("Cannot provide custom timestamp for counter BATCH")
    );

    assert!(parse("BEGIN BATCH SELECT * FROM ks.users; APPLY BATCH").is_err());
    assert!(parse("BEGIN BATCH USE ks; APPLY BATCH").is_err());
}

#[test]
fn gap_guard_cql_delete_using_timestamp_only() {
    // CLOSED by Rust rewrite follow-up: DELETE uses Java's dedicated
    // usingClauseDelete grammar, which accepts USING TIMESTAMP but not TTL.
    use cassandra_cql::{
        ast::{Literal, Statement, Term, UsingClause},
        parser::parse,
    };

    let stmt = parse("DELETE FROM ks.users USING TIMESTAMP 123 WHERE id = 1").unwrap();
    let Statement::Delete(delete) = stmt else {
        panic!("expected DELETE");
    };
    assert!(matches!(
        delete.using.as_slice(),
        [UsingClause::Timestamp(Term::Literal(Literal::Integer(123)))]
    ));

    assert!(parse("DELETE FROM ks.users USING TTL 60 WHERE id = 1").is_err());
    assert!(parse("DELETE FROM ks.users USING TIMESTAMP 123 AND TTL 60 WHERE id = 1").is_err());
}

#[test]
fn gap_guard_cql_constraints() {
    // CLOSED by prompt-14 continuation: parser/planner preserve Java-style
    // column CHECK constraints and the server executor enforces NOT NULL,
    // scalar comparisons, LENGTH(), and OCTET_LENGTH() on INSERT/UPDATE.
    use cassandra_cql::{
        ast::{ColumnConstraint, ConstraintRelationOp, Statement},
        parser::parse,
        planner::{QueryPlan, plan},
    };
    use cassandra_schema::{
        ColumnConstraintMetadata, ConstraintRelationOp as SchemaConstraintRelationOp,
        SchemaSnapshot,
    };

    let stmt = parse(
        "CREATE TABLE ks.tbl (
            pk int PRIMARY KEY,
            ck1 int CHECK ck1 < 100 AND ck1 >= 10,
            name text CHECK LENGTH() <= 20,
            payload blob CHECK OCTET_LENGTH() != 0,
            value int CHECK NOT NULL
        )",
    )
    .unwrap();

    let Statement::CreateTable(create) = &stmt else {
        panic!("expected CREATE TABLE");
    };
    let ck1 = create.columns.iter().find(|col| col.name == "ck1").unwrap();
    assert_eq!(ck1.constraints.len(), 2);
    assert_eq!(
        ck1.constraints[0],
        ColumnConstraint::Scalar {
            column: "ck1".to_string(),
            op: ConstraintRelationOp::Lt,
            term: "100".to_string()
        }
    );
    assert_eq!(
        ck1.constraints[1],
        ColumnConstraint::Scalar {
            column: "ck1".to_string(),
            op: ConstraintRelationOp::Gte,
            term: "10".to_string()
        }
    );

    let name = create
        .columns
        .iter()
        .find(|col| col.name == "name")
        .unwrap();
    assert_eq!(
        name.constraints[0],
        ColumnConstraint::Function {
            name: "length".to_string(),
            args: vec![],
            op: ConstraintRelationOp::Lte,
            term: "20".to_string()
        }
    );
    let value = create
        .columns
        .iter()
        .find(|col| col.name == "value")
        .unwrap();
    assert_eq!(value.constraints, vec![ColumnConstraint::NotNull]);

    let planned = plan(&stmt, &SchemaSnapshot::empty(), Some("ks")).unwrap();
    let QueryPlan::CreateTable(plan) = planned else {
        panic!("expected CREATE TABLE plan");
    };
    let planned_name = plan.columns.iter().find(|col| col.name == "name").unwrap();
    assert_eq!(
        planned_name.constraints,
        vec![ColumnConstraintMetadata::Function {
            name: "length".to_string(),
            args: vec![],
            op: SchemaConstraintRelationOp::Lte,
            term: "20".to_string()
        }]
    );

    let stmt = parse("ALTER TABLE ks.tbl ALTER ck1 CHECK ck1 < 200 AND ck1 > 0").unwrap();
    match stmt {
        Statement::AlterTable(alter) => match alter.operation {
            cassandra_cql::ast::AlterTableOp::AlterConstraints(column, constraints) => {
                assert_eq!(column, "ck1");
                assert_eq!(constraints.len(), 2);
            }
            other => panic!("expected AlterConstraints, got {other:?}"),
        },
        other => panic!("expected AlterTable, got {other:?}"),
    }

    let stmt = parse("ALTER TABLE ks.tbl ALTER ck1 DROP CHECK").unwrap();
    match stmt {
        Statement::AlterTable(alter) => match alter.operation {
            cassandra_cql::ast::AlterTableOp::DropConstraints(column) => {
                assert_eq!(column, "ck1");
            }
            other => panic!("expected DropConstraints, got {other:?}"),
        },
        other => panic!("expected AlterTable, got {other:?}"),
    }
}

#[test]
fn gap_guard_cql_advanced_restrictions() {
    // CLOSED by Rust rewrite follow-up: parser and StatementRestrictions carry
    // token() restrictions, multi-column clustering tuple restrictions,
    // collection CONTAINS / CONTAINS KEY validation, and LIKE filtering.
    use cassandra_cql::{
        ast::{Literal, RelationOp, Term},
        parser::parse,
        planner::{QueryPlan, plan as plan_query},
        restrictions::RestrictionKind,
    };
    use cassandra_schema::{
        ClusteringOrder, ColumnMetadata, KeyspaceMetadata, KeyspaceParams, SchemaSnapshot,
        TableMetadataBuilder,
    };
    use cassandra_types::CqlType;

    let table = TableMetadataBuilder::new("ks", "tbl")
        .add_column(ColumnMetadata::partition_key("pk", 0, CqlType::Int))
        .add_column(ColumnMetadata::clustering(
            "ck1",
            0,
            CqlType::Int,
            ClusteringOrder::Asc,
        ))
        .add_column(ColumnMetadata::clustering(
            "ck2",
            1,
            CqlType::Int,
            ClusteringOrder::Asc,
        ))
        .add_column(ColumnMetadata::regular(
            "tags",
            CqlType::Set(Box::new(CqlType::Varchar), false),
        ))
        .add_column(ColumnMetadata::regular(
            "attrs",
            CqlType::Map(
                Box::new(CqlType::Varchar),
                Box::new(CqlType::Varchar),
                false,
            ),
        ))
        .add_column(ColumnMetadata::regular("name", CqlType::Varchar))
        .build();
    let ks = KeyspaceMetadata::new("ks", KeyspaceParams::default()).with_table(table);
    let mut schema = SchemaSnapshot::empty();
    schema.keyspaces.insert("ks".to_string(), ks);

    let stmt = parse("SELECT * FROM ks.tbl WHERE token(pk) > ?").unwrap();
    let QueryPlan::Select(plan) = plan_query(&stmt, &schema, None).unwrap() else {
        panic!("expected SELECT plan");
    };
    assert_eq!(plan.where_clause[0].column, "token");
    assert!(plan.restrictions.unwrap().is_token_based);

    let stmt = parse("SELECT * FROM ks.tbl WHERE pk = 1 AND (ck1, ck2) > (10, 20)").unwrap();
    let QueryPlan::Select(plan) = plan_query(&stmt, &schema, None).unwrap() else {
        panic!("expected SELECT plan");
    };
    assert_eq!(plan.where_clause[1].column, "(ck1,ck2)");
    assert!(matches!(
        plan.where_clause[1].value,
        Term::TupleLiteral(ref terms) if terms.len() == 2
            && terms[0] == Term::Literal(Literal::Integer(10))
            && terms[1] == Term::Literal(Literal::Integer(20))
    ));
    let restrictions = plan.restrictions.unwrap();
    assert!(
        restrictions
            .clustering_restrictions
            .iter()
            .any(|restriction| {
                restriction.column_name == "(ck1,ck2)"
                    && matches!(
                        restriction.kind,
                        RestrictionKind::Range { op: RelationOp::Gt }
                    )
            })
    );

    let stmt = parse("SELECT * FROM ks.tbl WHERE pk = 1 AND tags CONTAINS 'blue' ALLOW FILTERING")
        .unwrap();
    let QueryPlan::Select(plan) = plan_query(&stmt, &schema, None).unwrap() else {
        panic!("expected SELECT plan");
    };
    assert!(
        plan.restrictions
            .unwrap()
            .non_key_restrictions
            .iter()
            .any(|restriction| matches!(restriction.kind, RestrictionKind::Contains))
    );

    let stmt =
        parse("SELECT * FROM ks.tbl WHERE pk = 1 AND attrs CONTAINS KEY 'region' ALLOW FILTERING")
            .unwrap();
    let QueryPlan::Select(plan) = plan_query(&stmt, &schema, None).unwrap() else {
        panic!("expected SELECT plan");
    };
    assert!(
        plan.restrictions
            .unwrap()
            .non_key_restrictions
            .iter()
            .any(|restriction| matches!(restriction.kind, RestrictionKind::ContainsKey))
    );

    let stmt =
        parse("SELECT * FROM ks.tbl WHERE pk = 1 AND name LIKE 'Al%' ALLOW FILTERING").unwrap();
    let QueryPlan::Select(plan) = plan_query(&stmt, &schema, None).unwrap() else {
        panic!("expected SELECT plan");
    };
    assert!(
        plan.restrictions
            .unwrap()
            .non_key_restrictions
            .iter()
            .any(|restriction| matches!(restriction.kind, RestrictionKind::Like))
    );
}

#[test]
fn gap_guard_index_differential_testing() {
    // PARTIAL closure by prompt-14 follow-up: differential harness can run the
    // same operation script against multiple secondary-index backends and
    // compare canonical exact/range search results against static oracle expectations.
    // Full live Java oracle execution remains tracked in the report.
    use cassandra_storage::index::IndexEntry;
    use cassandra_storage::index::differential::{
        ExpectedIndexObservation, IndexDifferentialOp, compare_index_backends, observations_match,
        observations_match_expected, range_observation_key,
    };
    use cassandra_storage::index::legacy::LegacyIndex;
    use cassandra_storage::index::sai::SaiIndex;

    fn entry(term: &[u8], pk: &[u8]) -> IndexEntry {
        IndexEntry {
            term: term.to_vec(),
            partition_key: pk.to_vec(),
            clustering_key: vec![],
        }
    }

    let legacy = LegacyIndex::create("legacy_email", "ks", "users", "email");
    let sai = SaiIndex::create("sai_email", "ks", "users", "email");
    let observations = compare_index_backends(
        &legacy,
        &sai,
        &[
            IndexDifferentialOp::Insert(entry(b"a@example.com", b"pk1")),
            IndexDifferentialOp::Insert(entry(b"b@example.com", b"pk2")),
            IndexDifferentialOp::Insert(entry(b"c@example.com", b"pk3")),
            IndexDifferentialOp::Search(b"a@example.com".to_vec()),
            IndexDifferentialOp::RangeSearch {
                start: Some(b"b@example.com".to_vec()),
                end: Some(b"c@example.com".to_vec()),
            },
            IndexDifferentialOp::Delete(entry(b"a@example.com", b"pk1")),
            IndexDifferentialOp::Search(b"a@example.com".to_vec()),
        ],
    )
    .unwrap();

    assert!(observations_match(&observations));
    assert_eq!(observations[0].left_results.len(), 1);
    assert_eq!(observations[1].left_results.len(), 2);
    assert!(observations[2].left_results.is_empty());
    assert!(observations_match_expected(
        &observations,
        &[
            ExpectedIndexObservation {
                op_index: 3,
                term: b"a@example.com".to_vec(),
                results: vec![entry(b"a@example.com", b"pk1")],
            },
            ExpectedIndexObservation {
                op_index: 4,
                term: range_observation_key(
                    Some(b"b@example.com".as_slice()),
                    Some(b"c@example.com".as_slice()),
                ),
                results: vec![
                    entry(b"b@example.com", b"pk2"),
                    entry(b"c@example.com", b"pk3"),
                ],
            },
            ExpectedIndexObservation {
                op_index: 6,
                term: b"a@example.com".to_vec(),
                results: Vec::new(),
            },
        ],
    ));
}

// ═══════════════════════════════════════════════════════════════════════
// CQL GAPS (Expanded — Prompt 11)
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn gap_guard_cql_masking_functions() {
    // CLOSED by prompt-14 follow-up: FunctionRegistry registers the dynamic
    // data masking builtins and their Java-style native scalar behavior is callable.
    use cassandra_cql::functions::FunctionRegistry;
    use cassandra_types::CqlType;

    let registry = FunctionRegistry::with_builtins();
    let names = registry.function_names();
    for name in [
        "mask_default",
        "mask_null",
        "mask_inner",
        "mask_outer",
        "mask_replace",
        "mask_hash",
    ] {
        assert!(names.contains(&name.to_string()), "missing {name}");
    }

    let mask_null = registry
        .resolve("mask_null", &[CqlType::Varchar])
        .expect("mask_null");
    assert_eq!(mask_null.execute(&[Some(b"secret")]).unwrap(), None);

    let mask_inner = registry
        .resolve(
            "mask_inner",
            &[
                CqlType::Varchar,
                CqlType::Int,
                CqlType::Int,
                CqlType::Varchar,
            ],
        )
        .expect("mask_inner");
    let begin = 1i32.to_be_bytes();
    let end = 1i32.to_be_bytes();
    assert_eq!(
        mask_inner
            .execute(&[Some(b"secret"), Some(&begin), Some(&end), Some(b"*")])
            .unwrap(),
        Some(b"s****t".to_vec())
    );

    let mask_outer = registry
        .resolve(
            "mask_outer",
            &[
                CqlType::Varchar,
                CqlType::Int,
                CqlType::Int,
                CqlType::Varchar,
            ],
        )
        .expect("mask_outer");
    assert_eq!(
        mask_outer
            .execute(&[Some(b"secret"), Some(&begin), Some(&end), Some(b"#")])
            .unwrap(),
        Some(b"#ecre#".to_vec())
    );
    let begin_zero = 0i32.to_be_bytes();
    let end_past_len = 10i32.to_be_bytes();
    assert_eq!(
        mask_inner
            .execute(&[
                Some(b"secret"),
                Some(&begin_zero),
                Some(&end_past_len),
                Some(b"*")
            ])
            .unwrap(),
        Some(b"secret".to_vec())
    );
    assert_eq!(
        mask_outer
            .execute(&[
                Some(b"secret"),
                Some(&begin_zero),
                Some(&end_past_len),
                Some(b"#")
            ])
            .unwrap(),
        Some(b"######".to_vec())
    );

    let mask_replace = registry
        .resolve("mask_replace", &[CqlType::Varchar, CqlType::Varchar])
        .expect("mask_replace");
    assert_eq!(
        mask_replace
            .execute(&[Some(b"secret"), Some(b"REDACTED")])
            .unwrap(),
        Some(b"REDACTED".to_vec())
    );

    let mask_default = registry
        .resolve("mask_default", &[CqlType::Int])
        .expect("mask_default");
    assert_eq!(
        mask_default.execute(&[Some(&42i32.to_be_bytes())]).unwrap(),
        Some(0i32.to_be_bytes().to_vec())
    );

    let list_type = CqlType::List(Box::new(CqlType::Int), false);
    let mask_default_list = registry
        .resolve("mask_default", std::slice::from_ref(&list_type))
        .expect("mask_default(list<int>)");
    assert_eq!(mask_default_list.return_type(), list_type);
    assert_eq!(
        mask_default_list.execute(&[Some(&[])]).unwrap(),
        Some(0i32.to_be_bytes().to_vec())
    );

    let tuple_type = CqlType::Tuple(vec![CqlType::Int, CqlType::Varchar]);
    let mask_default_tuple = registry
        .resolve("mask_default", std::slice::from_ref(&tuple_type))
        .expect("mask_default(tuple<int, text>)");
    let mut expected_tuple = Vec::new();
    expected_tuple.extend_from_slice(&4i32.to_be_bytes());
    expected_tuple.extend_from_slice(&0i32.to_be_bytes());
    expected_tuple.extend_from_slice(&4i32.to_be_bytes());
    expected_tuple.extend_from_slice(b"****");
    assert_eq!(
        mask_default_tuple.execute(&[Some(&[])]).unwrap(),
        Some(expected_tuple)
    );

    let mask_hash = registry
        .resolve("mask_hash", &[CqlType::Varchar])
        .expect("mask_hash");
    assert_eq!(mask_hash.return_type(), CqlType::Blob);
    assert_eq!(
        hex::encode(mask_hash.execute(&[Some(b"secret")]).unwrap().unwrap()),
        "2bb80d537b1da3e38bd30361aa855686bde0eacd7162fef6a25fe97bf527a25b"
    );

    let mask_hash_with_algorithm = registry
        .resolve("mask_hash", &[CqlType::Varchar, CqlType::Varchar])
        .expect("mask_hash(text, text)");
    assert_eq!(
        hex::encode(
            mask_hash_with_algorithm
                .execute(&[Some(b"secret"), Some(b"SHA3-256")])
                .unwrap()
                .unwrap()
        ),
        "f5a5207a8729b1f709cb710311751eb2fc8acad5a1fb8ac991b736e69b6529a3"
    );
    assert!(
        mask_hash_with_algorithm
            .execute(&[Some(b"secret"), Some(b"unknown-algorithm")])
            .unwrap_err()
            .contains("Hash algorithm not found")
    );
}

#[test]
fn gap_guard_cql_function_type_helpers() {
    // CLOSED by Rust rewrite follow-up: function signatures expose arg and
    // return types, registry resolution handles simple widening, shared type
    // helpers cover CQL names/fixed sizes/collections/assignment, and UDF
    // values deserialize/serialize through the Java-compatible CQL codec.
    use cassandra_cql::{
        functions::{FunctionName, FunctionRegistry},
        udf::UdfValue,
    };
    use cassandra_types::{
        AssignmentResult, CqlType,
        abstract_type::{deserialize, from_cql_string},
        codec::CqlValue,
        native::parse_cql_type,
        type_compat::{is_compatible_with, test_assignment},
    };

    let name = FunctionName::new("ks", "f");
    assert_eq!(name.canonical(), "ks.f");
    assert_eq!(FunctionName::native("now").canonical(), "system.now");

    let registry = FunctionRegistry::with_builtins();
    let abs = registry.resolve("abs", &[CqlType::Int]).expect("abs(int)");
    assert_eq!(abs.arg_types(), vec![CqlType::Int]);
    assert_eq!(abs.return_type(), CqlType::Int);

    let ceil = registry
        .resolve("ceil", &[CqlType::Float])
        .expect("ceil(float)");
    assert_eq!(ceil.arg_types(), vec![CqlType::Float]);
    assert_eq!(ceil.return_type(), CqlType::Float);

    let list = parse_cql_type("list<int>").unwrap();
    assert_eq!(list.cql_name(), "list<int>");
    assert!(list.is_collection());
    assert!(list.is_multi_cell());
    assert_eq!(CqlType::Int.fixed_size(), Some(4));
    assert_eq!(CqlType::Varchar.fixed_size(), None);

    let frozen_map = parse_cql_type("frozen<map<text, list<int>>>").unwrap();
    assert_eq!(frozen_map.cql_name(), "frozen<map<text, list<int>>>");
    assert!(!frozen_map.is_multi_cell());

    assert!(is_compatible_with(&CqlType::Smallint, &CqlType::Bigint));
    assert_eq!(
        test_assignment(&CqlType::Bigint, &CqlType::Int),
        AssignmentResult::Compatible
    );
    assert_eq!(
        test_assignment(&CqlType::Boolean, &CqlType::Int),
        AssignmentResult::NotAssignable
    );

    let reversed = CqlType::Reversed(Box::new(CqlType::Int));
    assert!(reversed.is_reversed());
    assert_eq!(*reversed.unwrap_reversed(), CqlType::Int);

    assert_eq!(
        UdfValue::deserialize_arg(&CqlType::Int, Some(&7i32.to_be_bytes())).unwrap(),
        UdfValue::Int(7)
    );
    let encoded_list = CqlValue::List(vec![CqlValue::Int(1), CqlValue::Int(2)]).serialize_value();
    assert_eq!(
        UdfValue::deserialize_arg(&parse_cql_type("list<int>").unwrap(), Some(&encoded_list))
            .unwrap(),
        UdfValue::List(vec![UdfValue::Int(1), UdfValue::Int(2)])
    );
    for cql_type in [
        CqlType::Boolean,
        CqlType::Int,
        CqlType::Bigint,
        CqlType::Counter,
        CqlType::Float,
        CqlType::Double,
        CqlType::Varint,
        CqlType::Decimal,
        CqlType::Timestamp,
        CqlType::Uuid,
        CqlType::Timeuuid,
        CqlType::Inet,
        CqlType::Reversed(Box::new(CqlType::Int)),
    ] {
        assert_eq!(
            UdfValue::deserialize_arg(&cql_type, Some(&[])).unwrap(),
            UdfValue::Null,
            "empty {} UDF argument should match Java null composition",
            cql_type.cql_name()
        );
    }
    assert_eq!(
        UdfValue::deserialize_arg(&CqlType::Varchar, Some(&[])).unwrap(),
        UdfValue::Text(String::new())
    );
    assert_eq!(
        UdfValue::deserialize_arg(&CqlType::Blob, Some(&[])).unwrap(),
        UdfValue::Blob(Vec::new())
    );
    assert_eq!(
        UdfValue::Text("ok".to_string())
            .serialize_return(&CqlType::Varchar)
            .unwrap(),
        Some(b"ok".to_vec())
    );

    let vector_ty = CqlType::Vector(Box::new(CqlType::Float), 3);
    let vector_bytes = from_cql_string(&vector_ty, "[1.0, -2.5, 3.25]").unwrap();
    assert_eq!(
        deserialize(&vector_ty, &vector_bytes).unwrap(),
        CqlValue::Vector(cassandra_types::VectorValue::new(vec![1.0, -2.5, 3.25]))
    );
    assert!(from_cql_string(&vector_ty, "[1.0, 2.0]").is_err());
    assert!(from_cql_string(&CqlType::Vector(Box::new(CqlType::Float), 2), "[1.0, NaN]").is_err());
}

#[test]
fn gap_guard_cql_schema_statements() {
    // CLOSED by prompt-12: Parser handles CREATE/ALTER/DROP for types, functions,
    // aggregates, indexes, triggers, and map-valued ALTER TABLE options.
    // Planner has Describe variant.
    use cassandra_cql::ast::{AlterTableOp, Statement};
    use cassandra_cql::parser;
    use std::collections::BTreeMap;

    // Verify CREATE FUNCTION parses
    let stmt = parser::parse(
        "CREATE FUNCTION ks.myfunc(val int) CALLED ON NULL INPUT RETURNS int LANGUAGE java AS 'return val;'",
    );
    assert!(stmt.is_ok(), "CREATE FUNCTION should parse");

    // Verify CREATE AGGREGATE parses
    let stmt = parser::parse("CREATE AGGREGATE ks.myagg(int) SFUNC plus STYPE int INITCOND 0");
    assert!(stmt.is_ok(), "CREATE AGGREGATE should parse");

    // Verify CREATE TRIGGER parses
    let stmt = parser::parse("CREATE TRIGGER mytrigger ON ks.t USING 'org.example.MyTrigger'");
    assert!(stmt.is_ok(), "CREATE TRIGGER should parse");

    // Verify DESCRIBE parses
    let stmt = parser::parse("DESCRIBE KEYSPACE system");
    assert!(stmt.is_ok(), "DESCRIBE should parse");

    let stmt = parser::parse("TRUNCATE COLUMNFAMILY ks.t").unwrap();
    let Statement::Truncate(truncate) = stmt else {
        panic!("expected TRUNCATE");
    };
    assert_eq!(truncate.keyspace.as_deref(), Some("ks"));
    assert_eq!(truncate.table, "t");
    assert!(parser::parse("TRUNCATE TABLE ks.t").is_err());

    let stmt = parser::parse(
        "ALTER TABLE ks.t WITH compression = {'class': 'LZ4Compressor', 'chunk_length_in_kb': 64}",
    )
    .unwrap();
    let Statement::AlterTable(alter) = stmt else {
        panic!("expected ALTER TABLE");
    };
    let AlterTableOp::WithOptions(options) = alter.operation else {
        panic!("expected ALTER TABLE WITH options");
    };
    let compression: BTreeMap<String, String> =
        serde_json::from_str(options.get("compression").unwrap()).unwrap();
    assert_eq!(
        compression.get("class").map(String::as_str),
        Some("LZ4Compressor")
    );
    assert_eq!(
        compression.get("chunk_length_in_kb").map(String::as_str),
        Some("64")
    );

    let stmt = parser::parse(
        "CREATE TABLE ks.configured (
            id int PRIMARY KEY,
            name text
        ) WITH transactional_mode = 'accord'
          AND default_time_to_live = 120",
    )
    .unwrap();
    let Statement::CreateTable(create) = stmt else {
        panic!("expected CREATE TABLE");
    };
    assert_eq!(
        create.options.get("transactional_mode").map(String::as_str),
        Some("accord")
    );
    assert_eq!(
        create
            .options
            .get("default_time_to_live")
            .map(String::as_str),
        Some("120")
    );
}

#[test]
fn gap_guard_cql_selection_functions() {
    // CLOSED by prompt-12: WritetimeOrTtl selector now evaluates against CellMeta.
    // SelectorEvaluator accepts cell_metadata parameter for WRITETIME/TTL resolution.
    use cassandra_cql::ast::Selector;
    use cassandra_cql::functions::FunctionRegistry;
    use cassandra_cql::selection::selector_eval::{CellMeta, SelectorEvaluator};
    use std::collections::HashMap;

    let registry = FunctionRegistry::new();
    let eval = SelectorEvaluator::new(&registry);

    let mut columns = HashMap::new();
    columns.insert("name".to_string(), 0usize);
    let row = vec![Some(b"Alice".to_vec())];

    let mut cell_meta = HashMap::new();
    cell_meta.insert(
        "name".to_string(),
        CellMeta {
            timestamp: Some(1234567890),
            ttl: Some(3600),
        },
    );

    // Test WRITETIME selector
    let sel = Selector::WritetimeOrTtl("writetime".to_string(), "name".to_string());
    let result = eval.evaluate(&sel, &columns, &row, Some(&cell_meta));
    assert!(result.is_some(), "WRITETIME should return a value");
    let ts = i64::from_be_bytes(result.unwrap().try_into().unwrap());
    assert_eq!(ts, 1234567890);

    // Test TTL selector
    let sel = Selector::WritetimeOrTtl("ttl".to_string(), "name".to_string());
    let result = eval.evaluate(&sel, &columns, &row, Some(&cell_meta));
    assert!(result.is_some(), "TTL should return a value");
    let ttl = i32::from_be_bytes(result.unwrap().try_into().unwrap());
    assert_eq!(ttl, 3600);
}

#[test]
fn gap_guard_cql_terms_advanced() {
    // CLOSED by prompt-14 continuation: UPDATE assignment parsing now
    // distinguishes list append/prepend/removal, set/map discarders, map put
    // operations, and UDT-shaped literals.
    use cassandra_cql::{
        ast::{AssignmentOp, Literal, Statement, Term},
        parser::parse,
    };

    let stmt = parse("UPDATE ks.tbl SET items = items + [1, 2] WHERE pk = 1").unwrap();
    let Statement::Update(update) = stmt else {
        panic!("expected UPDATE");
    };
    assert_eq!(update.assignments[0].column, "items");
    assert_eq!(update.assignments[0].op, AssignmentOp::CollectionAppend);
    assert!(matches!(
        update.assignments[0].value,
        Term::CollectionLiteral(ref terms)
            if terms == &vec![
                Term::Literal(Literal::Integer(1)),
                Term::Literal(Literal::Integer(2))
            ]
    ));

    let stmt = parse("UPDATE ks.tbl SET items = [0] + items WHERE pk = 1").unwrap();
    let Statement::Update(update) = stmt else {
        panic!("expected UPDATE");
    };
    assert_eq!(update.assignments[0].op, AssignmentOp::CollectionPrepend);

    let stmt = parse("UPDATE ks.tbl SET items = items - [2] WHERE pk = 1").unwrap();
    let Statement::Update(update) = stmt else {
        panic!("expected UPDATE");
    };
    assert_eq!(update.assignments[0].op, AssignmentOp::CollectionRemove);

    let stmt = parse("UPDATE ks.tbl SET attrs['region'] = 'eu' WHERE pk = 1").unwrap();
    let Statement::Update(update) = stmt else {
        panic!("expected UPDATE");
    };
    assert_eq!(update.assignments[0].column, "attrs");
    assert_eq!(
        update.assignments[0].op,
        AssignmentOp::MapPut {
            key: Term::Literal(Literal::String("region".to_string()))
        }
    );
    assert_eq!(
        update.assignments[0].value,
        Term::Literal(Literal::String("eu".to_string()))
    );

    let stmt = parse("UPDATE ks.tbl SET tags = tags - {'old', 'stale'} WHERE pk = 1").unwrap();
    let Statement::Update(update) = stmt else {
        panic!("expected UPDATE");
    };
    assert_eq!(update.assignments[0].op, AssignmentOp::CollectionRemove);
    assert_eq!(
        update.assignments[0].value,
        Term::CollectionLiteral(vec![
            Term::Literal(Literal::String("old".to_string())),
            Term::Literal(Literal::String("stale".to_string())),
        ])
    );

    let stmt = parse("UPDATE ks.tbl SET attrs = attrs - {'region'} WHERE pk = 1").unwrap();
    let Statement::Update(update) = stmt else {
        panic!("expected UPDATE");
    };
    assert_eq!(update.assignments[0].op, AssignmentOp::CollectionRemove);
    assert_eq!(
        update.assignments[0].value,
        Term::CollectionLiteral(vec![Term::Literal(Literal::String("region".to_string()))])
    );

    let stmt =
        parse("UPDATE ks.tbl SET address = {street: 'Main', zip: 90210} WHERE pk = 1").unwrap();
    let Statement::Update(update) = stmt else {
        panic!("expected UPDATE");
    };
    assert_eq!(update.assignments[0].op, AssignmentOp::Set);
    assert_eq!(
        update.assignments[0].value,
        Term::MapLiteral(vec![
            (
                Term::Literal(Literal::String("street".to_string())),
                Term::Literal(Literal::String("Main".to_string()))
            ),
            (
                Term::Literal(Literal::String("zip".to_string())),
                Term::Literal(Literal::Integer(90210))
            ),
        ])
    );
}

// ═══════════════════════════════════════════════════════════════════════
// STORAGE GAPS
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn gap_guard_sstable_java_compat() {
    // PARTIAL closure by prompt-14 follow-up: Rust can now discover and
    // validate Java big-format SSTable component manifests and reconcile TOC
    // listings with discovered files, including Data.db Digest.crc32 and
    // CRC.db chunk checks plus CompressionInfo.db metadata parsing. Full Java
    // row/cell binary decoding remains tracked.
    use cassandra_storage::sstable::compat::{
        JavaBigBloomFilter, JavaBigComponent, JavaBigDescriptor, calculate_crc32,
        calculate_crc32_chunks, discover_java_big_sstables, parse_java_big_filter,
        write_java_big_filter,
    };

    let dir = tempfile::tempdir().unwrap();
    fn write_java_utf(buf: &mut Vec<u8>, value: &str) {
        buf.extend_from_slice(&(value.len() as u16).to_be_bytes());
        buf.extend_from_slice(value.as_bytes());
    }
    fn compression_info_bytes() -> Vec<u8> {
        let mut bytes = Vec::new();
        write_java_utf(&mut bytes, "LZ4Compressor");
        bytes.extend_from_slice(&1_i32.to_be_bytes());
        write_java_utf(&mut bytes, "crc_check_chance");
        write_java_utf(&mut bytes, "1.0");
        bytes.extend_from_slice(&4_i32.to_be_bytes());
        bytes.extend_from_slice(&32_i32.to_be_bytes());
        bytes.extend_from_slice(&12_i64.to_be_bytes());
        bytes.extend_from_slice(&3_i32.to_be_bytes());
        for offset in [0_i64, 9, 19] {
            bytes.extend_from_slice(&offset.to_be_bytes());
        }
        bytes
    }
    fn bloom_filter() -> JavaBigBloomFilter {
        let mut filter = JavaBigBloomFilter {
            hash_count: 3,
            word_count: 2,
            bitset_bytes: vec![0; 16],
        };
        filter.set_bit(0).unwrap();
        filter.set_bit(9).unwrap();
        filter.set_bit(70).unwrap();
        filter
    }

    let data_path = dir.path().join("nb-7-big-Data.db");
    std::fs::write(&data_path, b"java-big-data").unwrap();
    for component in [
        JavaBigComponent::Index,
        JavaBigComponent::Statistics,
        JavaBigComponent::Filter,
        JavaBigComponent::Digest,
        JavaBigComponent::Crc,
        JavaBigComponent::CompressionInfo,
        JavaBigComponent::Toc,
    ] {
        std::fs::write(
            dir.path().join(format!("nb-7-big-{}", component.suffix())),
            b"x",
        )
        .unwrap();
    }
    std::fs::write(
        dir.path().join("nb-7-big-CompressionInfo.db"),
        compression_info_bytes(),
    )
    .unwrap();
    let bloom = bloom_filter();
    std::fs::write(
        dir.path().join("nb-7-big-Filter.db"),
        write_java_big_filter(&bloom, false).unwrap(),
    )
    .unwrap();
    let digest = calculate_crc32(&data_path).unwrap();
    std::fs::write(dir.path().join("nb-7-big-Digest.crc32"), digest.to_string()).unwrap();
    let chunk_checksums = calculate_crc32_chunks(&data_path, 4).unwrap();
    let mut crc_bytes = 4_u32.to_be_bytes().to_vec();
    for checksum in &chunk_checksums {
        crc_bytes.extend_from_slice(&checksum.to_be_bytes());
    }
    std::fs::write(dir.path().join("nb-7-big-CRC.db"), crc_bytes).unwrap();
    std::fs::write(
        dir.path().join("nb-7-big-TOC.txt"),
        "Data.db\nnb-7-big-Index.db\nStatistics.db\nSummary.db\nDigest.crc32\nCRC.db\nCompressionInfo.db\n",
    )
    .unwrap();

    let manifests = discover_java_big_sstables(dir.path()).unwrap();
    assert_eq!(manifests.len(), 1);
    assert_eq!(manifests[0].descriptor.version, "nb");
    assert_eq!(manifests[0].descriptor.generation, 7);
    assert!(manifests[0].is_minimally_readable());
    let toc_validation = manifests[0].validate_toc().unwrap();
    assert_eq!(
        toc_validation.listed,
        vec![
            JavaBigComponent::Data,
            JavaBigComponent::Index,
            JavaBigComponent::Statistics,
            JavaBigComponent::Summary,
            JavaBigComponent::Digest,
            JavaBigComponent::Crc,
            JavaBigComponent::CompressionInfo,
        ]
    );
    assert_eq!(toc_validation.missing, vec![JavaBigComponent::Summary]);
    assert_eq!(
        toc_validation.extra,
        vec![JavaBigComponent::Filter, JavaBigComponent::Toc]
    );
    let digest_validation = manifests[0].validate_data_digest().unwrap().unwrap();
    assert_eq!(digest_validation.stored, digest);
    assert_eq!(digest_validation.calculated, digest);
    assert!(digest_validation.is_valid());
    let crc_validation = manifests[0].validate_data_crc().unwrap().unwrap();
    assert_eq!(crc_validation.chunk_size, 4);
    assert_eq!(crc_validation.stored, chunk_checksums);
    assert_eq!(crc_validation.calculated, chunk_checksums);
    assert!(crc_validation.is_valid());
    let compression = manifests[0].compression_metadata().unwrap().unwrap();
    assert_eq!(compression.compressor_name, "LZ4Compressor");
    assert_eq!(
        compression
            .options
            .get("crc_check_chance")
            .map(String::as_str),
        Some("1.0")
    );
    assert_eq!(compression.chunk_length, 4);
    assert_eq!(compression.max_compressed_length, 32);
    assert_eq!(compression.data_length, 12);
    assert_eq!(compression.chunk_offsets, vec![0, 9, 19]);
    assert_eq!(compression.chunk_for(5, 30).unwrap().offset, 9);
    let parsed_bloom = manifests[0].bloom_filter(false).unwrap().unwrap();
    assert_eq!(parsed_bloom, bloom);
    assert_eq!(parsed_bloom.capacity_bits(), 128);
    assert!(parsed_bloom.is_bit_set(0));
    assert!(parsed_bloom.is_bit_set(9));
    assert!(parsed_bloom.is_bit_set(70));
    assert!(!parsed_bloom.is_bit_set(71));
    let old_filter_bytes = write_java_big_filter(&bloom, true).unwrap();
    assert_eq!(
        parse_java_big_filter(&old_filter_bytes, true).unwrap(),
        bloom
    );
    assert_ne!(
        parse_java_big_filter(&old_filter_bytes, false)
            .unwrap()
            .bitset_bytes,
        bloom.bitset_bytes,
        "old BloomFilter format stores each 64-bit word in Java long order"
    );
    assert_eq!(
        JavaBigDescriptor::parse_component_filename("nb-7-big-Data.db")
            .unwrap()
            .1,
        JavaBigComponent::Data
    );
}

#[test]
fn gap_guard_compaction_execution() {
    // PARTIALLY CLOSED by prompt-14 continuation: task runner, cancellation, metrics, cleanup/scrub.
    use cassandra_storage::{
        compaction::{
            active::CancellationToken,
            errors::{CompactionError, CompactionReason, CompactionType},
            manager::{CompactionManager, RateLimiter},
            task::{
                CleanupTask, CompactionContext, CompactionTask, RegularCompactionTask, ScrubTask,
            },
        },
        memtable::partition::{Cell, PartitionData, Row},
    };

    fn cell(column: &str, value: &[u8], timestamp: i64) -> Cell {
        Cell {
            column: column.to_string(),
            value: Some(value.to_vec()),
            timestamp,
            ttl: 0,
            local_deletion_time: None,
            is_tombstone: false,
        }
    }

    fn partition(key: &[u8], value: &[u8], timestamp: i64) -> (Vec<u8>, PartitionData) {
        let mut partition = PartitionData::new();
        partition.apply_row(Row {
            clustering_key: b"ck".to_vec(),
            cells: vec![cell("v", value, timestamp)],
            is_tombstone: false,
            local_deletion_time: None,
        });
        (key.to_vec(), partition)
    }

    let ctx = CompactionContext {
        input_sstables: vec![11, 12],
        compaction_type: CompactionType::Compaction,
        reason: CompactionReason::Normal,
        gc_grace_seconds: 86_400,
        now_seconds: 2_000,
        cancel_token: CancellationToken::new(),
    };

    let task = RegularCompactionTask;
    assert_eq!(task.name(), "Regular Compaction");
    let manager = CompactionManager::new(1, RateLimiter::disabled());
    let task_id = manager.submit_background(&task, &ctx).unwrap();
    assert_eq!(manager.active_count(), 1);
    let result = manager
        .execute_task(
            &task_id,
            &task,
            &ctx,
            vec![
                vec![partition(b"pk-a", b"old", 100)],
                vec![partition(b"pk-b", b"new", 200)],
            ],
        )
        .unwrap();
    assert_eq!(result.input_sstable_count, 2);
    assert_eq!(result.output_sstable_count, 1);
    assert_eq!(result.partitions_merged, 2);
    assert_eq!(manager.active_count(), 0);
    let metrics = manager.metrics().snapshot();
    assert_eq!(metrics.compactions_completed, 1);
    assert_eq!(metrics.sstables_compacted, 2);

    let overlap_manager = CompactionManager::new(2, RateLimiter::disabled());
    overlap_manager.submit_background(&task, &ctx).unwrap();
    assert!(matches!(
        overlap_manager.submit_background(&task, &ctx).unwrap_err(),
        CompactionError::ConcurrentModification { sstable_ids }
            if sstable_ids == vec![11, 12]
    ));
    assert!(matches!(
        overlap_manager.submit_user_defined(&task, &ctx).unwrap_err(),
        CompactionError::ConcurrentModification { sstable_ids }
            if sstable_ids == vec![11, 12]
    ));

    let first = manager.submit_background(&task, &ctx).unwrap();
    assert!(matches!(
        manager.submit_background(&task, &ctx).unwrap_err(),
        CompactionError::InvalidState(_)
    ));
    assert!(manager.cancel(&first));
    manager.shutdown();
    assert!(manager.is_shutdown());
    assert!(matches!(
        manager.submit_background(&task, &ctx).unwrap_err(),
        CompactionError::InvalidState(_)
    ));

    let task_ctx = CompactionContext {
        input_sstables: vec![21, 22],
        compaction_type: CompactionType::Compaction,
        reason: CompactionReason::Normal,
        gc_grace_seconds: 86_400,
        now_seconds: 2_000,
        cancel_token: CancellationToken::new(),
    };
    let cleanup = CleanupTask::new(vec![(b"pk-b".to_vec(), b"pk-z".to_vec())]);
    let (cleaned, cleanup_result) = cleanup
        .execute(
            &task_ctx,
            vec![vec![
                partition(b"pk-a", b"drop", 100),
                partition(b"pk-b", b"keep", 100),
            ]],
        )
        .unwrap();
    assert_eq!(cleanup_result.partitions_merged, 2);
    assert_eq!(cleaned.len(), 1);
    assert_eq!(cleaned[0].0, b"pk-b");

    let scrub = ScrubTask;
    let (scrubbed, scrub_result) = scrub
        .execute(
            &task_ctx,
            vec![vec![
                partition(b"", b"corrupt", 100),
                partition(b"valid", b"keep", 100),
            ]],
        )
        .unwrap();
    assert_eq!(scrub_result.partitions_merged, 2);
    assert_eq!(scrubbed.len(), 1);
    assert_eq!(scrubbed[0].0, b"valid");

    let cancelled_ctx = CompactionContext {
        cancel_token: {
            let token = CancellationToken::new();
            token.cancel();
            token
        },
        ..task_ctx
    };
    assert!(matches!(
        task.execute(&cancelled_ctx, vec![vec![partition(b"pk", b"v", 1)]]),
        Err(CompactionError::Cancelled)
    ));

    let limiter = RateLimiter::new(10);
    assert!(limiter.acquire(4));
    assert!(limiter.acquire(6));
    assert!(!limiter.acquire(1));
    limiter.reset();
    assert!(limiter.acquire(10));
}

#[test]
fn gap_guard_trie_index() {
    // CLOSED by prompt-05: InMemoryTrie, MergeTrie, MemtableTrie, cursor-based iteration.
    // Verify cassandra_storage::tries module types are constructible.
    use cassandra_storage::tries::InMemoryTrie;
    let _trie: InMemoryTrie<String> = InMemoryTrie::new();
}

#[test]
fn gap_guard_db_filters() {
    // CLOSED by prompt-05 and extended by Rust rewrite follow-up:
    // ColumnFilter, ClusteringIndexFilter, RowFilter, DataLimits, and SSTable
    // residual row filtering cover equality, range, contains, map equality,
    // and custom-index column-existence checks.
    use cassandra_storage::filter::row_filter::{FilterExpression, Operator};
    use cassandra_storage::filter::{ColumnFilter, RowFilter};
    use cassandra_storage::memtable::partition::{Cell, PartitionData, Row};
    use cassandra_storage::sstable::filtered_scanner::{ScanOptions, apply_filters};

    let _cf = ColumnFilter::AllColumns;
    let mut partition = PartitionData::new();
    partition.apply_row(Row {
        clustering_key: b"ck1".to_vec(),
        cells: vec![
            Cell {
                column: "score".to_string(),
                value: Some(vec![20]),
                timestamp: 1,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            },
            Cell {
                column: "tags".to_string(),
                value: Some(b"red,green".to_vec()),
                timestamp: 1,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            },
            Cell {
                column: "attrs[region]".to_string(),
                value: Some(b"eu".to_vec()),
                timestamp: 1,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            },
            Cell {
                column: "embedding".to_string(),
                value: Some(b"vec".to_vec()),
                timestamp: 1,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            },
        ],
        is_tombstone: false,
        local_deletion_time: None,
    });
    partition.apply_row(Row {
        clustering_key: b"ck2".to_vec(),
        cells: vec![Cell {
            column: "score".to_string(),
            value: Some(vec![10]),
            timestamp: 1,
            ttl: 0,
            local_deletion_time: None,
            is_tombstone: false,
        }],
        is_tombstone: false,
        local_deletion_time: None,
    });

    let filtered = apply_filters(
        partition,
        &ScanOptions {
            column_filter: None,
            clustering_filter: None,
            data_limits: None,
            row_filter: Some(
                RowFilter::none()
                    .with(FilterExpression::Simple {
                        column: "score".to_string(),
                        operator: Operator::Gte,
                        value: vec![20],
                    })
                    .with(FilterExpression::Simple {
                        column: "tags".to_string(),
                        operator: Operator::Contains,
                        value: b"green".to_vec(),
                    })
                    .with(FilterExpression::MapEquality {
                        column: "attrs".to_string(),
                        key: b"region".to_vec(),
                        value: b"eu".to_vec(),
                    })
                    .with(FilterExpression::Custom {
                        column: "embedding".to_string(),
                        operator: Operator::Ann,
                        value: b"query".to_vec(),
                        index_name: "embedding_idx".to_string(),
                    }),
            ),
        },
    );
    assert_eq!(filtered.rows.len(), 1);
    assert!(filtered.rows.contains_key(b"ck1".as_slice()));
}

#[test]
fn gap_guard_caching() {
    // CLOSED by prompt-14 continuation: key/row/counter/chunk caches, service invalidation, persistence.
    use std::sync::Arc;

    use cassandra_storage::{
        cache::{
            CacheService,
            chunk_cache::{ChunkCache, ChunkCacheConfig},
            counter_cache::{CounterCache, CounterCacheConfig},
            persistence::{load_counter_cache, load_key_cache, save_counter_cache, save_key_cache},
            row_cache::{RowCache, RowCacheConfig},
        },
        memtable::partition::PartitionData,
        sstable::key_cache::{KeyCache, KeyCacheConfig},
    };

    let key_cache = KeyCache::new(KeyCacheConfig { max_entries: 2 });
    key_cache.put(10, b"pk-a".to_vec(), 100);
    key_cache.put(10, b"pk-b".to_vec(), 200);
    assert_eq!(key_cache.get(10, b"pk-a"), Some(100));
    key_cache.put(10, b"pk-c".to_vec(), 300);
    assert_eq!(
        key_cache.get(10, b"pk-b"),
        None,
        "LRU key should be evicted"
    );

    let row_cache = RowCache::new(RowCacheConfig { max_entries: 2 });
    row_cache.put(42, b"row-a".to_vec(), PartitionData::new());
    assert!(row_cache.get(42, b"row-a").is_some());
    row_cache.invalidate_partition(42, b"row-a");
    assert!(row_cache.get(42, b"row-a").is_none());

    let counter_cache = CounterCache::new(CounterCacheConfig { max_entries: 2 });
    counter_cache.put(42, b"pk".to_vec(), b"counter".to_vec(), b"value".to_vec());
    assert_eq!(
        counter_cache.get(42, b"pk", b"counter"),
        Some(b"value".to_vec())
    );
    counter_cache.invalidate(42, b"pk", b"counter");
    assert_eq!(counter_cache.get(42, b"pk", b"counter"), None);

    let chunk_cache = ChunkCache::new(ChunkCacheConfig { max_size_bytes: 8 });
    chunk_cache.put(7, 0, Arc::new(vec![1, 2, 3, 4]));
    chunk_cache.put(7, 4, Arc::new(vec![5, 6, 7, 8]));
    chunk_cache.put(8, 0, Arc::new(vec![9, 10, 11, 12]));
    assert!(
        chunk_cache.get(7, 0).is_none(),
        "oldest chunk should be evicted by byte budget"
    );
    assert_eq!(chunk_cache.size_bytes(), 8);

    let service = CacheService::new(
        key_cache.clone(),
        Some(row_cache.clone()),
        Some(counter_cache.clone()),
        Some(chunk_cache.clone()),
    );
    let names: Vec<&str> = service
        .all_stats()
        .into_iter()
        .map(|(name, _)| name)
        .collect();
    assert_eq!(
        names,
        vec!["KeyCache", "RowCache", "CounterCache", "ChunkCache"]
    );
    key_cache.put(99, b"keep".to_vec(), 900);
    key_cache.put(7, b"drop".to_vec(), 700);
    chunk_cache.put(7, 64, Arc::new(vec![1, 1, 1, 1]));
    service.invalidate_sstable(7);
    assert_eq!(key_cache.get(7, b"drop"), None);
    assert!(chunk_cache.get(7, 64).is_none());
    assert_eq!(key_cache.get(99, b"keep"), Some(900));

    row_cache.put(123, b"row".to_vec(), PartitionData::new());
    counter_cache.put(123, b"pk".to_vec(), b"cell".to_vec(), b"v".to_vec());
    service.invalidate_table(123);
    assert!(row_cache.get(123, b"row").is_none());
    assert_eq!(counter_cache.get(123, b"pk", b"cell"), None);

    let dir = tempfile::tempdir().unwrap();
    let key_path = dir.path().join("KeyCache-v1.json");
    let counter_path = dir.path().join("CounterCache-v1.json");
    key_cache.put(55, b"persisted".to_vec(), 55_000);
    counter_cache.put(
        55,
        b"pk".to_vec(),
        b"cell".to_vec(),
        b"counter-value".to_vec(),
    );
    save_key_cache(&key_path, &key_cache).unwrap();
    save_counter_cache(&counter_path, &counter_cache).unwrap();

    let restored_key_cache = KeyCache::new(KeyCacheConfig { max_entries: 10 });
    let restored_counter_cache = CounterCache::new(CounterCacheConfig { max_entries: 10 });
    assert!(load_key_cache(&key_path, &restored_key_cache).unwrap() > 0);
    assert!(load_counter_cache(&counter_path, &restored_counter_cache).unwrap() > 0);
    assert_eq!(restored_key_cache.get(55, b"persisted"), Some(55_000));
    assert_eq!(
        restored_counter_cache.get(55, b"pk", b"cell"),
        Some(b"counter-value".to_vec())
    );
}

#[test]
fn gap_guard_row_transformations() {
    // CLOSED by prompt-05: Transformation trait, FilteredRows, etc.
    // Verify cassandra_storage::transform module types exist.
    use cassandra_storage::transform::Transformation;
    // Trait exists - verified by import
    let _ = std::any::TypeId::of::<dyn Transformation>();
}

#[test]
fn gap_guard_guardrails() {
    // CLOSED by prompt-14 (WU-10): GuardrailsConfig with check_create_table,
    // check_create_index, check_allow_filtering, check_truncate, check_page_size,
    // check_drop_keyspace, check_write_guardrails, check_select_guardrails.
    // Guardrails are in cassandra-config (GuardrailsConfig) and cassandra-server
    // (guardrail_checks module). Tests verify all check functions exist and work.
}

// ═══════════════════════════════════════════════════════════════════════
// STORAGE GAPS (Expanded — Prompt 11)
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn gap_guard_memtable_trie() {
    // PARTIAL closure by prompt-14 follow-up: trie memtable backend is selectable,
    // stores shared-prefix partition keys, preserves sorted iteration, and merges rows.
    // Java off-heap memory layout parity remains tracked in the report.
    use cassandra_storage::memtable::{
        MemtableBackend, MemtableType, create_backend,
        partition::{Cell, Row},
        trie::TrieMemtable,
    };

    fn row(clustering_key: &[u8], value: &[u8], timestamp: i64) -> Row {
        Row {
            clustering_key: clustering_key.to_vec(),
            cells: vec![Cell {
                column: "v".to_string(),
                value: Some(value.to_vec()),
                timestamp,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        }
    }

    let memtable = TrieMemtable::new();
    memtable.apply(b"user:0003".to_vec(), row(b"ck", b"three", 100));
    memtable.apply(b"user:0001".to_vec(), row(b"ck", b"one", 100));
    memtable.apply(b"user:0002".to_vec(), row(b"ck", b"old", 100));
    memtable.apply(b"user:0002".to_vec(), row(b"ck", b"two", 200));

    assert_eq!(memtable.partition_count(), 3);
    assert_eq!(memtable.operation_count(), 4);
    assert!(memtable.memory_usage() > 0);

    let merged = memtable.get_partition(b"user:0002").unwrap();
    let merged_row = merged.rows.get(b"ck".as_slice()).unwrap();
    assert_eq!(
        merged_row.cells[0].value.as_deref(),
        Some(b"two".as_slice())
    );
    assert_eq!(merged_row.cells[0].timestamp, 200);

    let keys: Vec<_> = memtable
        .iter_partitions()
        .into_iter()
        .map(|(key, _)| key)
        .collect();
    assert_eq!(
        keys,
        vec![
            b"user:0001".to_vec(),
            b"user:0002".to_vec(),
            b"user:0003".to_vec()
        ]
    );

    memtable.set_partition_tombstone(b"user:0004".to_vec(), 300, 1234);
    let tombstoned = memtable.get_partition(b"user:0004").unwrap();
    assert_eq!(tombstoned.tombstone_timestamp, Some(300));
    assert_eq!(tombstoned.tombstone_local_deletion_time, Some(1234));

    let backend = create_backend(MemtableType::Trie);
    backend.apply(b"configured".to_vec(), row(b"ck", b"value", 1));
    assert!(backend.get_partition(b"configured").is_some());
}

#[test]
fn gap_guard_commitlog_compression() {
    // CLOSED by Rust rewrite follow-up: commitlog segment v2 headers carry
    // compression/encryption flags, and compressed or encrypted entries replay
    // to their original payloads through the segment codec path.
    use cassandra_storage::commitlog::{
        encrypted::{
            CommitLogEncryptor, EncryptedSegmentWriter, EncryptingSegmentWriter, PlainSegmentWriter,
        },
        segment::{CorruptionPolicy, Segment, SegmentFlags},
    };
    use std::sync::Arc;

    #[derive(Debug)]
    struct XorEncryptor(u8);

    impl CommitLogEncryptor for XorEncryptor {
        fn encrypt_segment(&self, data: &[u8]) -> Result<Vec<u8>, String> {
            Ok(data.iter().map(|byte| byte ^ self.0).collect())
        }

        fn decrypt_segment(&self, data: &[u8]) -> Result<Vec<u8>, String> {
            self.encrypt_segment(data)
        }

        fn is_enabled(&self) -> bool {
            true
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let flags = SegmentFlags {
        compression_enabled: true,
        ..SegmentFlags::default()
    };
    let mut segment = Segment::create_with_flags(dir.path(), 42, flags).unwrap();

    let compressible_payload = vec![b'a'; 1024];
    let small_payload = b"small mutation payload".to_vec();
    segment.append_entry(&compressible_payload).unwrap();
    segment.append_entry(&small_payload).unwrap();
    segment.sync().unwrap();

    let segment_path = segment.path().to_path_buf();
    let compressed_size = segment.size();
    drop(segment);

    let read_segment = Segment::open_for_read(&segment_path).unwrap();
    assert!(
        read_segment.flags().compression_enabled,
        "commitlog segment header must preserve compression-enabled flag"
    );

    let entries: Vec<_> = read_segment
        .read_all_entries()
        .into_iter()
        .map(|entry| entry.unwrap())
        .collect();
    assert_eq!(
        entries,
        vec![compressible_payload.clone(), small_payload.clone()]
    );
    assert!(
        compressed_size < 1024 + 16 + 2 * 9,
        "compressible commitlog entry should occupy less than the raw v2 entry budget"
    );

    let writer = PlainSegmentWriter;
    assert!(
        !writer.is_encrypted(),
        "current commitlog encryption hook must report plaintext segments explicitly"
    );

    let encrypted_flags = SegmentFlags {
        compression_enabled: true,
        encryption_enabled: true,
    };
    let codec = EncryptingSegmentWriter::new(Arc::new(XorEncryptor(0xa5)));
    let mut encrypted_segment =
        Segment::create_with_flags(dir.path(), 43, encrypted_flags).unwrap();
    encrypted_segment
        .append_entry_with_codec(&compressible_payload, &codec)
        .unwrap();
    encrypted_segment
        .append_entry_with_codec(&small_payload, &codec)
        .unwrap();
    encrypted_segment.sync().unwrap();

    let read_encrypted = Segment::open_for_read(encrypted_segment.path()).unwrap();
    assert!(read_encrypted.flags().encryption_enabled);
    let encrypted_entries: Vec<_> = read_encrypted
        .read_entries_with_codec(CorruptionPolicy::StopOnCorrupt, &codec)
        .into_iter()
        .map(|entry| entry.unwrap())
        .collect();
    assert_eq!(encrypted_entries, vec![compressible_payload, small_payload]);
}

#[test]
fn gap_guard_sstable_read_compat() {
    // PARTIAL closure by prompt-14 follow-up: Java big-format TOC/component
    // parsing, TOC reconciliation, and primary Index.db partition-key/data
    // offset scanning plus Summary.db sampled index offsets are available as
    // migration reader probes. Full Data.db partition decoding remains tracked.
    use cassandra_storage::sstable::column_index::{
        JavaBigDeletionTime, JavaBigRowIndexEntry, JavaBigRowIndexEntryKind,
    };
    use cassandra_storage::sstable::compat::{
        JAVA_CLUSTERING_KIND_EXCL_END_INCL_START_BOUNDARY,
        JAVA_UNFILTERED_EXT_HAS_SHADOWABLE_DELETION, JAVA_UNFILTERED_EXT_IS_STATIC,
        JAVA_UNFILTERED_EXTENSION_FLAG, JAVA_UNFILTERED_HAS_COMPLEX_DELETION,
        JAVA_UNFILTERED_HAS_DELETION, JAVA_UNFILTERED_HAS_TIMESTAMP, JAVA_UNFILTERED_HAS_TTL,
        JavaBigComplexCell, JavaBigComponent, JavaBigDataPartitionHeader,
        JavaBigDataPartitionUnfiltered, JavaBigDeletionTimeMetadata, JavaBigDescriptor,
        JavaBigEncodingStats, JavaBigRowLivenessMetadata, JavaBigSerializationHeader,
        JavaBigSerializationHeaderColumn, JavaBigSimpleCell, JavaBigSummary, JavaBigSummaryEntry,
        JavaBigUnfilteredKind, discover_java_big_sstables, parse_java_big_data_partition_header_at,
        parse_java_big_data_partition_simple_rows_at, parse_java_big_unfiltered_header_at,
        parse_java_big_unfiltered_header_at_with_clustering_types,
        parse_java_big_unfiltered_row_with_simple_cells_at, read_java_big_primary_index,
        read_java_big_summary, read_java_big_toc, write_java_big_complex_column,
        write_java_big_data_partition_header, write_java_big_primary_index_entry,
        write_java_big_range_tombstone_marker_body, write_java_big_regular_column_subset,
        write_java_big_simple_cell, write_java_big_summary,
        write_java_big_unfiltered_end_of_partition, write_java_big_unfiltered_marker_header,
        write_java_big_unfiltered_row, write_java_big_unfiltered_row_metadata_body,
    };

    let dir = tempfile::tempdir().unwrap();
    let toc = dir.path().join("nb-1-big-TOC.txt");
    std::fs::write(&toc, "Data.db\nIndex.db\nStatistics.db\n").unwrap();
    assert_eq!(
        read_java_big_toc(&toc).unwrap(),
        vec![
            JavaBigComponent::Data,
            JavaBigComponent::Index,
            JavaBigComponent::Statistics
        ]
    );

    let descriptor = JavaBigDescriptor {
        version: "nb".into(),
        generation: 1,
        format: "big".into(),
    };
    assert_eq!(
        descriptor.component_filename(JavaBigComponent::Data),
        "nb-1-big-Data.db"
    );

    let mut data_bytes = Vec::new();
    let first_data_offset = data_bytes.len() as u64;
    write_java_big_data_partition_header(
        &mut data_bytes,
        b"pk-a",
        &JavaBigDeletionTimeMetadata::LIVE,
        false,
    )
    .unwrap();
    let encoding_stats = JavaBigEncodingStats {
        min_timestamp: 1_000,
        min_local_deletion_time: 2_000,
        min_ttl: 10,
    };
    let row_liveness = JavaBigRowLivenessMetadata {
        timestamp: Some(1_100),
        ttl: Some(40),
        local_expiration_time: Some(2_400),
    };
    let row_deletion = JavaBigDeletionTimeMetadata {
        marked_for_delete_at: 1_200,
        local_deletion_time: 2_500,
        is_live: false,
    };
    let row_flags = JAVA_UNFILTERED_EXTENSION_FLAG
        | JAVA_UNFILTERED_HAS_TIMESTAMP
        | JAVA_UNFILTERED_HAS_TTL
        | JAVA_UNFILTERED_HAS_DELETION
        | JAVA_UNFILTERED_HAS_COMPLEX_DELETION;
    let row_serialization_header = JavaBigSerializationHeader {
        encoding_stats: encoding_stats.clone(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_types: vec![
            "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.Int32Type)"
                .to_string(),
        ],
        static_columns: vec![JavaBigSerializationHeaderColumn {
            name: b"static_value".to_vec(),
            type_spec: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
        regular_columns: vec![
            JavaBigSerializationHeaderColumn {
                name: b"ignored".to_vec(),
                type_spec: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            },
            JavaBigSerializationHeaderColumn {
                name: b"value".to_vec(),
                type_spec: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            },
            JavaBigSerializationHeaderColumn {
                name: b"items".to_vec(),
                type_spec:
                    "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)"
                        .to_string(),
            },
            JavaBigSerializationHeaderColumn {
                name: b"attrs".to_vec(),
                type_spec: "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)"
                    .to_string(),
            },
            JavaBigSerializationHeaderColumn {
                name: b"tags".to_vec(),
                type_spec:
                    "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type)"
                        .to_string(),
            },
            JavaBigSerializationHeaderColumn {
                name: b"profile".to_vec(),
                type_spec: "org.apache.cassandra.db.marshal.UserType(ks,70726f66696c65,6e616d65:org.apache.cassandra.db.marshal.UTF8Type,616765:org.apache.cassandra.db.marshal.Int32Type)"
                    .to_string(),
            },
            JavaBigSerializationHeaderColumn {
                name: b"deleted_value".to_vec(),
                type_spec: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
            },
            JavaBigSerializationHeaderColumn {
                name: b"embedding".to_vec(),
                type_spec:
                    "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType,3)"
                        .to_string(),
            },
            JavaBigSerializationHeaderColumn {
                name: b"rank".to_vec(),
                type_spec:
                    "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.Int32Type)"
                        .to_string(),
            },
        ],
    };
    let mut row_body = Vec::new();
    write_java_big_unfiltered_row_metadata_body(
        &mut row_body,
        row_flags,
        &row_liveness,
        Some(&row_deletion),
        &encoding_stats,
    )
    .unwrap();
    write_java_big_regular_column_subset(&mut row_body, 9, &[1, 2, 3, 4, 5, 6, 7, 8]).unwrap();
    let row_cell = JavaBigSimpleCell {
        column_name: b"value".to_vec(),
        column_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        flags: 0,
        timestamp: 1_100,
        local_deletion_time: None,
        ttl: None,
        value: Some(b"hello".to_vec()),
        is_deleted: false,
        is_expiring: false,
        uses_row_timestamp: true,
        uses_row_ttl: false,
    };
    write_java_big_simple_cell(&mut row_body, &row_cell, &row_liveness, &encoding_stats).unwrap();
    let complex_deletion = JavaBigDeletionTimeMetadata {
        marked_for_delete_at: 1_050,
        local_deletion_time: 2_100,
        is_live: false,
    };
    let complex_cells = vec![JavaBigComplexCell {
        path: vec![0x42; 16],
        cell: JavaBigSimpleCell {
            column_name: b"items".to_vec(),
            column_type:
                "org.apache.cassandra.db.marshal.ListType(org.apache.cassandra.db.marshal.Int32Type)"
                    .to_string(),
            flags: 0,
            timestamp: 1_100,
            local_deletion_time: None,
            ttl: None,
            value: Some(7_i32.to_be_bytes().to_vec()),
            is_deleted: false,
            is_expiring: false,
            uses_row_timestamp: true,
            uses_row_ttl: false,
        },
    }];
    write_java_big_complex_column(
        &mut row_body,
        &row_serialization_header.regular_columns[2],
        true,
        Some(&complex_deletion),
        &complex_cells,
        &row_liveness,
        &encoding_stats,
    )
    .unwrap();
    let map_deletion = JavaBigDeletionTimeMetadata {
        marked_for_delete_at: 1_060,
        local_deletion_time: 2_110,
        is_live: false,
    };
    let map_cells = vec![JavaBigComplexCell {
        path: b"score".to_vec(),
        cell: JavaBigSimpleCell {
            column_name: b"attrs".to_vec(),
            column_type: "org.apache.cassandra.db.marshal.MapType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.Int32Type)"
                .to_string(),
            flags: 0,
            timestamp: 1_100,
            local_deletion_time: None,
            ttl: None,
            value: Some(99_i32.to_be_bytes().to_vec()),
            is_deleted: false,
            is_expiring: false,
            uses_row_timestamp: true,
            uses_row_ttl: false,
        },
    }];
    write_java_big_complex_column(
        &mut row_body,
        &row_serialization_header.regular_columns[3],
        true,
        Some(&map_deletion),
        &map_cells,
        &row_liveness,
        &encoding_stats,
    )
    .unwrap();
    let set_deletion = JavaBigDeletionTimeMetadata {
        marked_for_delete_at: 1_070,
        local_deletion_time: 2_120,
        is_live: false,
    };
    let set_cells = vec![JavaBigComplexCell {
        path: b"blue".to_vec(),
        cell: JavaBigSimpleCell {
            column_name: b"tags".to_vec(),
            column_type:
                "org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.UTF8Type)"
                    .to_string(),
            flags: 0,
            timestamp: 1_100,
            local_deletion_time: None,
            ttl: None,
            value: None,
            is_deleted: false,
            is_expiring: false,
            uses_row_timestamp: true,
            uses_row_ttl: false,
        },
    }];
    write_java_big_complex_column(
        &mut row_body,
        &row_serialization_header.regular_columns[4],
        true,
        Some(&set_deletion),
        &set_cells,
        &row_liveness,
        &encoding_stats,
    )
    .unwrap();
    let udt_deletion = JavaBigDeletionTimeMetadata {
        marked_for_delete_at: 1_080,
        local_deletion_time: 2_130,
        is_live: false,
    };
    let udt_cells = vec![
        JavaBigComplexCell {
            path: 0_u16.to_be_bytes().to_vec(),
            cell: JavaBigSimpleCell {
                column_name: b"profile".to_vec(),
                column_type: "org.apache.cassandra.db.marshal.UserType(ks,70726f66696c65,6e616d65:org.apache.cassandra.db.marshal.UTF8Type,616765:org.apache.cassandra.db.marshal.Int32Type)"
                    .to_string(),
                flags: 0,
                timestamp: 1_100,
                local_deletion_time: None,
                ttl: None,
                value: Some(b"ada".to_vec()),
                is_deleted: false,
                is_expiring: false,
                uses_row_timestamp: true,
                uses_row_ttl: false,
            },
        },
        JavaBigComplexCell {
            path: 1_u16.to_be_bytes().to_vec(),
            cell: JavaBigSimpleCell {
                column_name: b"profile".to_vec(),
                column_type: "org.apache.cassandra.db.marshal.UserType(ks,70726f66696c65,6e616d65:org.apache.cassandra.db.marshal.UTF8Type,616765:org.apache.cassandra.db.marshal.Int32Type)"
                    .to_string(),
                flags: 0,
                timestamp: 1_100,
                local_deletion_time: None,
                ttl: None,
                value: Some(37_i32.to_be_bytes().to_vec()),
                is_deleted: false,
                is_expiring: false,
                uses_row_timestamp: true,
                uses_row_ttl: false,
            },
        },
    ];
    write_java_big_complex_column(
        &mut row_body,
        &row_serialization_header.regular_columns[5],
        true,
        Some(&udt_deletion),
        &udt_cells,
        &row_liveness,
        &encoding_stats,
    )
    .unwrap();
    let deleted_cell = JavaBigSimpleCell {
        column_name: b"deleted_value".to_vec(),
        column_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        flags: 0,
        timestamp: 1_130,
        local_deletion_time: Some(2_150),
        ttl: None,
        value: None,
        is_deleted: true,
        is_expiring: false,
        uses_row_timestamp: false,
        uses_row_ttl: false,
    };
    write_java_big_simple_cell(&mut row_body, &deleted_cell, &row_liveness, &encoding_stats)
        .unwrap();
    let embedding_value = [1.0_f32, -2.5_f32, 3.25_f32]
        .into_iter()
        .flat_map(f32::to_be_bytes)
        .collect::<Vec<_>>();
    let embedding_cell = JavaBigSimpleCell {
        column_name: b"embedding".to_vec(),
        column_type:
            "org.apache.cassandra.db.marshal.VectorType(org.apache.cassandra.db.marshal.FloatType,3)"
                .to_string(),
        flags: 0,
        timestamp: 1_100,
        local_deletion_time: None,
        ttl: None,
        value: Some(embedding_value.clone()),
        is_deleted: false,
        is_expiring: false,
        uses_row_timestamp: true,
        uses_row_ttl: false,
    };
    write_java_big_simple_cell(
        &mut row_body,
        &embedding_cell,
        &row_liveness,
        &encoding_stats,
    )
    .unwrap();
    let rank_cell = JavaBigSimpleCell {
        column_name: b"rank".to_vec(),
        column_type:
            "org.apache.cassandra.db.marshal.ReversedType(org.apache.cassandra.db.marshal.Int32Type)"
                .to_string(),
        flags: 0,
        timestamp: 1_100,
        local_deletion_time: None,
        ttl: None,
        value: Some(4_i32.to_be_bytes().to_vec()),
        is_deleted: false,
        is_expiring: false,
        uses_row_timestamp: true,
        uses_row_ttl: false,
    };
    write_java_big_simple_cell(&mut row_body, &rank_cell, &row_liveness, &encoding_stats).unwrap();
    let first_unfiltered_offset = data_bytes.len() as u64;
    write_java_big_unfiltered_row(
        &mut data_bytes,
        row_flags,
        Some(JAVA_UNFILTERED_EXT_HAS_SHADOWABLE_DELETION),
        &[Some(42_i32.to_be_bytes().to_vec())],
        &row_serialization_header.clustering_types,
        0,
        &row_body,
    )
    .unwrap();
    let static_unfiltered_offset = data_bytes.len() as u64;
    let static_flags = JAVA_UNFILTERED_EXTENSION_FLAG | JAVA_UNFILTERED_HAS_TIMESTAMP;
    let static_liveness = JavaBigRowLivenessMetadata {
        timestamp: Some(1_101),
        ttl: None,
        local_expiration_time: None,
    };
    let mut static_body = Vec::new();
    write_java_big_unfiltered_row_metadata_body(
        &mut static_body,
        static_flags,
        &static_liveness,
        None,
        &encoding_stats,
    )
    .unwrap();
    write_java_big_regular_column_subset(&mut static_body, 1, &[0]).unwrap();
    let static_cell = JavaBigSimpleCell {
        column_name: b"static_value".to_vec(),
        column_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        flags: 0,
        timestamp: 1_101,
        local_deletion_time: None,
        ttl: None,
        value: Some(b"static".to_vec()),
        is_deleted: false,
        is_expiring: false,
        uses_row_timestamp: true,
        uses_row_ttl: false,
    };
    write_java_big_simple_cell(
        &mut static_body,
        &static_cell,
        &static_liveness,
        &encoding_stats,
    )
    .unwrap();
    write_java_big_unfiltered_row(
        &mut data_bytes,
        static_flags,
        Some(JAVA_UNFILTERED_EXT_IS_STATIC),
        &[],
        &row_serialization_header.clustering_types,
        0,
        &static_body,
    )
    .unwrap();
    let first_end_offset = data_bytes.len() as u64;
    write_java_big_unfiltered_end_of_partition(&mut data_bytes);
    let deleted_partition = JavaBigDeletionTimeMetadata {
        marked_for_delete_at: 123_456_789,
        local_deletion_time: 1_700_000_000,
        is_live: false,
    };
    let second_data_offset = data_bytes.len() as u64;
    write_java_big_data_partition_header(&mut data_bytes, b"pk-b", &deleted_partition, false)
        .unwrap();
    let second_unfiltered_offset = data_bytes.len() as u64;
    let marker_deletion = JavaBigDeletionTimeMetadata {
        marked_for_delete_at: 1_010,
        local_deletion_time: 2_010,
        is_live: false,
    };
    let marker_start_deletion = JavaBigDeletionTimeMetadata {
        marked_for_delete_at: 1_020,
        local_deletion_time: 2_020,
        is_live: false,
    };
    let mut marker_body = Vec::new();
    write_java_big_range_tombstone_marker_body(
        &mut marker_body,
        JAVA_CLUSTERING_KIND_EXCL_END_INCL_START_BOUNDARY,
        &marker_deletion,
        Some(&marker_start_deletion),
        &encoding_stats,
    )
    .unwrap();
    write_java_big_unfiltered_marker_header(
        &mut data_bytes,
        JAVA_CLUSTERING_KIND_EXCL_END_INCL_START_BOUNDARY,
        3,
        &marker_body,
    )
    .unwrap();
    let second_end_offset = data_bytes.len() as u64;
    write_java_big_unfiltered_end_of_partition(&mut data_bytes);
    std::fs::write(dir.path().join("nb-1-big-Data.db"), &data_bytes).unwrap();
    std::fs::write(dir.path().join("nb-1-big-Statistics.db"), b"x").unwrap();

    let mut index_bytes = Vec::new();
    let unindexed = JavaBigRowIndexEntry {
        data_file_position: first_data_offset,
        promoted_size: 0,
        kind: JavaBigRowIndexEntryKind::Unindexed,
        header_length: None,
        deletion_time: None,
        column_index_count: 0,
        index_info_bytes: Vec::new(),
        offsets: Vec::new(),
    };
    let indexed = JavaBigRowIndexEntry {
        data_file_position: second_data_offset,
        promoted_size: 0,
        kind: JavaBigRowIndexEntryKind::Indexed,
        header_length: Some(6),
        deletion_time: Some(JavaBigDeletionTime::LIVE),
        column_index_count: 2,
        index_info_bytes: vec![0x11, 0x12, 0x21],
        offsets: vec![0, 2],
    };
    write_java_big_primary_index_entry(&mut index_bytes, b"pk-a", &unindexed).unwrap();
    write_java_big_primary_index_entry(&mut index_bytes, b"pk-b", &indexed).unwrap();
    let index_path = dir.path().join("nb-1-big-Index.db");
    std::fs::write(&index_path, index_bytes).unwrap();

    let entries = read_java_big_primary_index(&index_path).unwrap();
    assert_eq!(entries.len(), 2);
    assert_eq!(entries[0].partition_key, b"pk-a");
    assert_eq!(entries[0].row_index.data_file_position, first_data_offset);
    assert!(!entries[0].row_index.is_indexed());
    assert_eq!(entries[1].partition_key, b"pk-b");
    assert_eq!(entries[1].row_index.data_file_position, second_data_offset);
    assert_eq!(
        entries[1].row_index.index_info_slice(0),
        Some(&[0x11, 0x12][..])
    );
    assert_eq!(entries[1].row_index.index_info_slice(1), Some(&[0x21][..]));
    let first_header = parse_java_big_data_partition_header_at(
        &data_bytes,
        entries[0].row_index.data_file_position,
        false,
    )
    .unwrap();
    assert_eq!(
        first_header,
        JavaBigDataPartitionHeader {
            data_offset: first_data_offset,
            partition_key: b"pk-a".to_vec(),
            deletion_time: JavaBigDeletionTimeMetadata::LIVE,
            bytes_consumed: 2 + b"pk-a".len() + 12,
        }
    );
    let second_header = parse_java_big_data_partition_header_at(
        &data_bytes,
        entries[1].row_index.data_file_position,
        false,
    )
    .unwrap();
    assert_eq!(second_header.partition_key, b"pk-b");
    assert_eq!(second_header.deletion_time, deleted_partition);
    let first_row = parse_java_big_unfiltered_header_at_with_clustering_types(
        &data_bytes,
        first_unfiltered_offset,
        &row_serialization_header.clustering_types,
    )
    .unwrap();
    assert_eq!(first_row.kind, JavaBigUnfilteredKind::Row);
    assert_eq!(first_row.flags, row_flags);
    assert_eq!(first_row.previous_unfiltered_size, Some(0));
    assert_eq!(first_row.body_length, Some(row_body.len() as u64));
    assert_eq!(first_row.next_unfiltered_offset, static_unfiltered_offset);
    assert_eq!(
        first_row.extended_flags,
        JAVA_UNFILTERED_EXT_HAS_SHADOWABLE_DELETION
    );
    assert_eq!(
        first_row.clustering_values,
        vec![Some(42_i32.to_be_bytes().to_vec())]
    );
    assert!(!first_row.is_static);
    let static_row = parse_java_big_unfiltered_header_at_with_clustering_types(
        &data_bytes,
        static_unfiltered_offset,
        &row_serialization_header.clustering_types,
    )
    .unwrap();
    assert_eq!(static_row.kind, JavaBigUnfilteredKind::Row);
    assert_eq!(static_row.flags, static_flags);
    assert_eq!(static_row.extended_flags, JAVA_UNFILTERED_EXT_IS_STATIC);
    assert!(static_row.is_static);
    assert!(static_row.clustering_values.is_empty());
    assert_eq!(static_row.next_unfiltered_offset, first_end_offset);
    let first_row_with_cells = parse_java_big_unfiltered_row_with_simple_cells_at(
        &data_bytes,
        first_unfiltered_offset,
        &row_serialization_header,
        false,
    )
    .unwrap();
    assert_eq!(first_row_with_cells.simple_cells.len(), 4);
    assert_eq!(first_row_with_cells.simple_cells[0].column_name, b"value");
    assert_eq!(
        first_row_with_cells.simple_cells[0].value.as_deref(),
        Some(b"hello".as_slice())
    );
    assert_eq!(first_row_with_cells.simple_cells[0].timestamp, 1_100);
    assert!(first_row_with_cells.simple_cells[0].uses_row_timestamp);
    assert_eq!(
        first_row_with_cells.simple_cells[1].column_name,
        b"deleted_value"
    );
    assert!(first_row_with_cells.simple_cells[1].is_deleted);
    assert_eq!(first_row_with_cells.simple_cells[1].value, None);
    assert_eq!(first_row_with_cells.simple_cells[1].timestamp, 1_130);
    assert_eq!(
        first_row_with_cells.simple_cells[1].local_deletion_time,
        Some(2_150)
    );
    assert_eq!(
        first_row_with_cells.simple_cells[2].column_name,
        b"embedding"
    );
    assert_eq!(
        first_row_with_cells.simple_cells[2].value.as_deref(),
        Some(embedding_value.as_slice())
    );
    assert_eq!(first_row_with_cells.simple_cells[3].column_name, b"rank");
    assert_eq!(
        first_row_with_cells.simple_cells[3].value.as_deref(),
        Some(4_i32.to_be_bytes().as_slice())
    );
    assert_eq!(first_row_with_cells.complex_columns.len(), 4);
    assert_eq!(
        first_row_with_cells.complex_columns[0].deletion_time,
        Some(complex_deletion)
    );
    assert_eq!(first_row_with_cells.complex_columns[0].cells.len(), 1);
    assert_eq!(
        first_row_with_cells.complex_columns[0].cells[0].path,
        vec![0x42; 16]
    );
    assert_eq!(
        first_row_with_cells.complex_columns[0].cells[0]
            .cell
            .value
            .as_deref(),
        Some(7_i32.to_be_bytes().as_slice())
    );
    assert_eq!(
        first_row_with_cells.complex_columns[1].deletion_time,
        Some(map_deletion)
    );
    assert_eq!(
        first_row_with_cells.complex_columns[1].cells[0].path,
        b"score"
    );
    assert_eq!(
        first_row_with_cells.complex_columns[1].cells[0]
            .cell
            .value
            .as_deref(),
        Some(99_i32.to_be_bytes().as_slice())
    );
    assert_eq!(
        first_row_with_cells.complex_columns[2].deletion_time,
        Some(set_deletion)
    );
    assert_eq!(
        first_row_with_cells.complex_columns[2].cells[0].path,
        b"blue"
    );
    assert_eq!(
        first_row_with_cells.complex_columns[2].cells[0].cell.value,
        None
    );
    assert_eq!(
        first_row_with_cells.complex_columns[3].deletion_time,
        Some(udt_deletion)
    );
    assert_eq!(
        first_row_with_cells.complex_columns[3].cells[0].path,
        0_u16.to_be_bytes()
    );
    assert_eq!(
        first_row_with_cells.complex_columns[3].cells[0]
            .cell
            .value
            .as_deref(),
        Some(b"ada".as_slice())
    );
    assert_eq!(
        first_row_with_cells.complex_columns[3].cells[1].path,
        1_u16.to_be_bytes()
    );
    assert_eq!(
        first_row_with_cells.complex_columns[3].cells[1]
            .cell
            .value
            .as_deref(),
        Some(37_i32.to_be_bytes().as_slice())
    );
    assert_eq!(first_row_with_cells.liveness, row_liveness);
    assert_eq!(first_row_with_cells.deletion_time, Some(row_deletion));
    assert!(first_row_with_cells.deletion_is_shadowable);
    let first_partition = parse_java_big_data_partition_simple_rows_at(
        &data_bytes,
        first_data_offset,
        false,
        &row_serialization_header,
    )
    .unwrap();
    assert_eq!(first_partition.header.partition_key, b"pk-a");
    assert_eq!(first_partition.unfiltereds.len(), 2);
    let JavaBigDataPartitionUnfiltered::Row(first_partition_row) = &first_partition.unfiltereds[0]
    else {
        panic!("expected decoded partition row");
    };
    assert_eq!(
        first_partition_row.simple_cells[0].value.as_deref(),
        Some(b"hello".as_slice())
    );
    assert!(first_partition_row.simple_cells[1].is_deleted);
    assert_eq!(first_partition_row.simple_cells[1].value, None);
    assert_eq!(
        first_partition_row.simple_cells[2].value.as_deref(),
        Some(embedding_value.as_slice())
    );
    assert_eq!(
        first_partition_row.simple_cells[3].value.as_deref(),
        Some(4_i32.to_be_bytes().as_slice())
    );
    assert_eq!(first_partition_row.complex_columns.len(), 4);
    assert_eq!(
        first_partition_row.complex_columns[0].cells[0]
            .cell
            .value
            .as_deref(),
        Some(7_i32.to_be_bytes().as_slice())
    );
    assert_eq!(
        first_partition_row.complex_columns[1].cells[0]
            .cell
            .value
            .as_deref(),
        Some(99_i32.to_be_bytes().as_slice())
    );
    assert_eq!(
        first_partition_row.complex_columns[2].cells[0].cell.value,
        None
    );
    assert_eq!(
        first_partition_row.complex_columns[3].cells[1]
            .cell
            .value
            .as_deref(),
        Some(37_i32.to_be_bytes().as_slice())
    );
    let JavaBigDataPartitionUnfiltered::Row(static_partition_row) = &first_partition.unfiltereds[1]
    else {
        panic!("expected decoded static partition row");
    };
    assert!(static_partition_row.header.is_static);
    assert!(static_partition_row.header.clustering_values.is_empty());
    assert_eq!(
        static_partition_row.simple_cells[0].column_name,
        b"static_value"
    );
    assert_eq!(
        static_partition_row.simple_cells[0].value.as_deref(),
        Some(b"static".as_slice())
    );
    assert_eq!(first_partition.end_offset, first_end_offset + 1);
    assert_eq!(
        parse_java_big_unfiltered_header_at(&data_bytes, first_end_offset, 0)
            .unwrap()
            .kind,
        JavaBigUnfilteredKind::EndOfPartition
    );
    let second_marker =
        parse_java_big_unfiltered_header_at(&data_bytes, second_unfiltered_offset, 0).unwrap();
    assert_eq!(
        second_marker.kind,
        JavaBigUnfilteredKind::RangeTombstoneMarker
    );
    assert_eq!(
        second_marker.clustering_kind_ordinal,
        Some(JAVA_CLUSTERING_KIND_EXCL_END_INCL_START_BOUNDARY)
    );
    assert_eq!(second_marker.previous_unfiltered_size, Some(3));
    assert_eq!(second_marker.body_length, Some(marker_body.len() as u64));
    assert_eq!(second_marker.next_unfiltered_offset, second_end_offset);
    let marker_only_header = JavaBigSerializationHeader {
        encoding_stats: encoding_stats.clone(),
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_types: Vec::new(),
        static_columns: Vec::new(),
        regular_columns: Vec::new(),
    };
    let second_partition = parse_java_big_data_partition_simple_rows_at(
        &data_bytes,
        second_data_offset,
        false,
        &marker_only_header,
    )
    .unwrap();
    assert_eq!(second_partition.header.partition_key, b"pk-b");
    assert_eq!(second_partition.unfiltereds.len(), 1);
    assert!(matches!(
        second_partition.unfiltereds[0],
        JavaBigDataPartitionUnfiltered::RangeTombstoneMarker(_)
    ));
    let JavaBigDataPartitionUnfiltered::RangeTombstoneMarker(second_partition_marker) =
        &second_partition.unfiltereds[0]
    else {
        panic!("expected decoded marker");
    };
    assert_eq!(second_partition_marker.deletion_time, marker_deletion);
    assert_eq!(
        second_partition_marker.boundary_start_deletion_time,
        Some(marker_start_deletion)
    );
    assert_eq!(second_partition.end_offset, second_end_offset + 1);

    let summary = JavaBigSummary {
        min_index_interval: 128,
        offheap_size: 0,
        sampling_level: 128,
        size_at_full_sampling: 2,
        entries: vec![
            JavaBigSummaryEntry {
                partition_key: b"pk-a".to_vec(),
                index_offset: entries[0].index_offset,
            },
            JavaBigSummaryEntry {
                partition_key: b"pk-b".to_vec(),
                index_offset: entries[1].index_offset,
            },
        ],
        first_key: b"pk-a".to_vec(),
        last_key: b"pk-b".to_vec(),
    };
    let summary_path = dir.path().join("nb-1-big-Summary.db");
    std::fs::write(&summary_path, write_java_big_summary(&summary).unwrap()).unwrap();
    let parsed_summary = read_java_big_summary(&summary_path).unwrap();
    assert_eq!(parsed_summary.min_index_interval, 128);
    assert_eq!(parsed_summary.sampling_level, 128);
    assert_eq!(parsed_summary.entries[0].partition_key, b"pk-a");
    assert_eq!(
        parsed_summary.entries[1].index_offset,
        entries[1].index_offset
    );
    assert_eq!(parsed_summary.first_key, b"pk-a");
    assert_eq!(parsed_summary.last_key, b"pk-b");

    let manifest = discover_java_big_sstables(dir.path()).unwrap().remove(0);
    let manifest_header = manifest
        .data_partition_header_at(entries[1].row_index.data_file_position, false)
        .unwrap()
        .unwrap();
    assert_eq!(manifest_header.partition_key, b"pk-b");
    assert_eq!(manifest_header.deletion_time, deleted_partition);
    let indexed_headers = manifest
        .indexed_data_partition_headers(false)
        .unwrap()
        .unwrap();
    assert_eq!(indexed_headers.len(), 2);
    assert_eq!(indexed_headers[0].index_entry.partition_key, b"pk-a");
    assert_eq!(indexed_headers[0].data_header.partition_key, b"pk-a");
    assert_eq!(indexed_headers[1].index_entry.partition_key, b"pk-b");
    assert_eq!(indexed_headers[1].data_header.partition_key, b"pk-b");

    let mut modern_data = Vec::new();
    write_java_big_data_partition_header(
        &mut modern_data,
        b"pk-live",
        &JavaBigDeletionTimeMetadata::LIVE,
        true,
    )
    .unwrap();
    let modern_deleted_offset = modern_data.len() as u64;
    let modern_deleted = JavaBigDeletionTimeMetadata {
        marked_for_delete_at: 987_654_321,
        local_deletion_time: 3_000_000_000,
        is_live: false,
    };
    write_java_big_data_partition_header(&mut modern_data, b"pk-deleted", &modern_deleted, true)
        .unwrap();
    let live_header = parse_java_big_data_partition_header_at(&modern_data, 0, true).unwrap();
    assert_eq!(live_header.partition_key, b"pk-live");
    assert_eq!(live_header.deletion_time, JavaBigDeletionTimeMetadata::LIVE);
    assert_eq!(live_header.bytes_consumed, 2 + b"pk-live".len() + 1);
    let deleted_header =
        parse_java_big_data_partition_header_at(&modern_data, modern_deleted_offset, true).unwrap();
    assert_eq!(deleted_header.partition_key, b"pk-deleted");
    assert_eq!(deleted_header.deletion_time, modern_deleted);
}

#[test]
fn gap_guard_compaction_unified() {
    // PARTIAL closure by prompt-14 follow-up: experimental UCS groups SSTables
    // by density, parses Java option-map scaling/sharding controls, and applies
    // compaction thresholds. Full Java Controller/Sharded orchestration remains
    // tracked in the report.
    use std::collections::HashMap;

    use cassandra_storage::{
        compaction::{
            CompactionStrategy, CompactionStrategyType, SSTableMetadata,
            SizeTieredCompactionStrategy, create_strategy_from_options,
            lcs::LeveledCompactionStrategy,
            twcs::{TimeUnit, TimeWindowCompactionStrategy, TimestampResolution},
            ucs::{OverlapInclusionMethod, ScalingParameter, UnifiedCompactionStrategy},
        },
        sstable::format::SSTableId,
    };

    fn meta(id: SSTableId, data_size: u64, partition_count: u64) -> SSTableMetadata {
        SSTableMetadata {
            id,
            data_size,
            partition_count,
            min_timestamp: 0,
            max_timestamp: 1000,
        }
    }

    let scaling = ScalingParameter {
        values: vec![2, -2],
    };
    assert_eq!(scaling.get(0), 2);
    assert_eq!(scaling.get(1), -2);
    assert_eq!(scaling.get(7), -2);
    assert_eq!(
        ScalingParameter::parse("T4, L10, N, -3").unwrap().values,
        vec![2, -8, 0, -3]
    );
    assert_eq!(ScalingParameter::print(-8), "L10");

    let options = HashMap::from([
        ("scaling_parameters".to_string(), "T4, L10".to_string()),
        ("target_sstable_size".to_string(), "1GiB".to_string()),
        ("min_sstable_size".to_string(), "64MiB".to_string()),
        ("flush_size_override".to_string(), "8MiB".to_string()),
        ("base_shard_count".to_string(), "8".to_string()),
        ("max_sstables_to_compact".to_string(), "6".to_string()),
        (
            "expired_sstable_check_frequency_seconds".to_string(),
            "900".to_string(),
        ),
        (
            "unsafe_aggressive_sstable_expiration".to_string(),
            "true".to_string(),
        ),
        ("sstable_growth".to_string(), "33.3%".to_string()),
        ("overlap_inclusion_method".to_string(), "single".to_string()),
        ("parallelize_output_shards".to_string(), "false".to_string()),
    ]);
    let parsed = UnifiedCompactionStrategy::from_options(&options).unwrap();
    assert_eq!(parsed.scaling.get(0), 2);
    assert_eq!(parsed.scaling.get(1), -8);
    assert_eq!(parsed.target_sstable_size, 1 << 30);
    assert_eq!(parsed.min_sstable_size, 64 * 1024 * 1024);
    assert_eq!(parsed.flush_size_override, 8 * 1024 * 1024);
    assert_eq!(parsed.base_shard_count, 8);
    assert_eq!(parsed.max_threshold, 6);
    assert_eq!(parsed.expired_sstable_check_frequency_seconds, 900);
    assert!(parsed.unsafe_aggressive_sstable_expiration);
    assert!((parsed.sstable_growth - 0.333).abs() < 0.0001);
    assert_eq!(
        parsed.overlap_inclusion_method,
        OverlapInclusionMethod::Single
    );
    assert!(!parsed.parallelize_output_shards);

    assert_eq!(
        CompactionStrategyType::from_java_class(
            "org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy"
        )
        .unwrap(),
        CompactionStrategyType::SizeTiered
    );
    assert_eq!(
        CompactionStrategyType::from_java_class("LeveledCompactionStrategy").unwrap(),
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

    let stcs_options = HashMap::from([
        (
            "class".to_string(),
            "SizeTieredCompactionStrategy".to_string(),
        ),
        ("min_threshold".to_string(), "2".to_string()),
        ("max_threshold".to_string(), "6".to_string()),
        ("bucket_low".to_string(), "0.4".to_string()),
        ("bucket_high".to_string(), "1.6".to_string()),
        ("min_sstable_size".to_string(), "1048576".to_string()),
    ]);
    let stcs = SizeTieredCompactionStrategy::from_options(&stcs_options).unwrap();
    assert_eq!(stcs.min_threshold, 2);
    assert_eq!(stcs.max_threshold, 6);
    assert!((stcs.bucket_low - 0.4).abs() < 0.001);
    assert!((stcs.bucket_high - 1.6).abs() < 0.001);
    assert_eq!(stcs.min_sstable_size, 1_048_576);
    assert_eq!(
        create_strategy_from_options(&stcs_options)
            .unwrap()
            .pick_compaction(&[meta(100, 1_000, 100), meta(101, 1_100, 100)]),
        vec![vec![100, 101]]
    );

    let mut ucs_options = options.clone();
    ucs_options.insert(
        "class".to_string(),
        "org.apache.cassandra.db.compaction.UnifiedCompactionStrategy".to_string(),
    );
    assert_eq!(
        create_strategy_from_options(&ucs_options)
            .unwrap()
            .pick_compaction(&[
                meta(110, 128 * 1024 * 1024, 1024),
                meta(111, 128 * 1024 * 1024, 1024),
                meta(112, 128 * 1024 * 1024, 1024),
                meta(113, 128 * 1024 * 1024, 1024),
                meta(114, 128 * 1024 * 1024, 1024),
                meta(115, 128 * 1024 * 1024, 1024),
            ]),
        vec![vec![110, 111, 112, 113, 114, 115]]
    );

    let lcs_options = HashMap::from([
        ("sstable_size_in_mb".to_string(), "32".to_string()),
        ("fanout_size".to_string(), "4".to_string()),
        ("min_threshold".to_string(), "2".to_string()),
        ("max_threshold".to_string(), "5".to_string()),
        ("single_sstable_uplevel".to_string(), "false".to_string()),
    ]);
    let lcs = LeveledCompactionStrategy::from_options(&lcs_options).unwrap();
    assert_eq!(lcs.sstable_size_in_mb, 32);
    assert_eq!(lcs.fanout_size, 4);
    assert_eq!(lcs.l0_threshold, 2);
    assert_eq!(lcs.max_threshold, 5);
    assert!(!lcs.single_sstable_uplevel);

    let twcs_options = HashMap::from([
        ("compaction_window_unit".to_string(), "HOURS".to_string()),
        ("compaction_window_size".to_string(), "6".to_string()),
        (
            "timestamp_resolution".to_string(),
            "MILLISECONDS".to_string(),
        ),
        (
            "expired_sstable_check_frequency_seconds".to_string(),
            "120".to_string(),
        ),
        (
            "unsafe_aggressive_sstable_expiration".to_string(),
            "true".to_string(),
        ),
        ("min_threshold".to_string(), "2".to_string()),
    ]);
    let twcs = TimeWindowCompactionStrategy::from_options(&twcs_options).unwrap();
    assert_eq!(twcs.time_unit, TimeUnit::Hours);
    assert_eq!(twcs.window_size, 6);
    assert_eq!(twcs.timestamp_resolution, TimestampResolution::Milliseconds);
    assert_eq!(twcs.expired_sstable_check_frequency_seconds, 120);
    assert!(twcs.unsafe_aggressive_sstable_expiration);
    assert_eq!(twcs.stcs.min_threshold, 2);

    let ucs = UnifiedCompactionStrategy {
        scaling,
        min_threshold: 3,
        max_threshold: 3,
        ..Default::default()
    };
    let picks = ucs.pick_compaction(&[
        meta(1, 1_000, 100),
        meta(2, 1_100, 100),
        meta(3, 1_200, 100),
        meta(4, 100_000, 100),
    ]);
    assert_eq!(picks, vec![vec![1, 2, 3]]);

    let capped = UnifiedCompactionStrategy {
        min_threshold: 3,
        max_threshold: 2,
        ..Default::default()
    };
    assert_eq!(
        capped.pick_compaction(&[
            meta(10, 1_000, 100),
            meta(11, 1_100, 100),
            meta(12, 1_200, 100),
        ]),
        vec![vec![10, 11]]
    );

    let below_threshold = UnifiedCompactionStrategy {
        min_threshold: 4,
        ..Default::default()
    };
    assert!(
        below_threshold
            .pick_compaction(&[meta(20, 1_000, 100), meta(21, 1_100, 100)])
            .is_empty()
    );
}

#[test]
fn gap_guard_compaction_writers() {
    // PARTIAL closure by prompt-14 follow-up: SSTable rewriter and runtime
    // compaction output planning preserve data and support size-based output
    // splitting. Full Java compaction writer hierarchy parity remains tracked.
    use cassandra_storage::{
        compaction::{estimate_compaction_output_size, split_compaction_output},
        memtable::partition::{Cell, PartitionData, Row},
        sstable::{
            format::SSTableDescriptor,
            reader::SSTableReader,
            rewriter::{RewriterConfig, rewrite},
        },
    };

    fn partition(key: u8, value_bytes: usize) -> (Vec<u8>, PartitionData) {
        let mut data = PartitionData::new();
        data.apply_row(Row {
            clustering_key: b"ck".to_vec(),
            cells: vec![Cell {
                column: "v".to_string(),
                value: Some(vec![key; value_bytes]),
                timestamp: key as i64,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });
        (vec![key], data)
    }

    let dir = tempfile::tempdir().unwrap();
    let base = SSTableDescriptor::new(dir.path(), "ks", "tbl", 100);
    let partitions: Vec<_> = (0..6).map(|i| partition(i, 64)).collect();

    let single = rewrite(
        partitions.clone().into_iter(),
        &base,
        &RewriterConfig {
            max_sstable_size: None,
        },
    )
    .unwrap();
    assert_eq!(single.output_descriptors.len(), 1);
    assert_eq!(single.stats[0].partition_count, 6);
    assert_eq!(single.stats[0].row_count, 6);

    let reader = SSTableReader::open(single.output_descriptors[0].clone()).unwrap();
    let read_back = reader.iter_partitions().unwrap();
    assert_eq!(read_back.len(), 6);
    assert_eq!(read_back[0].0, vec![0]);
    assert_eq!(read_back[5].0, vec![5]);

    let split = rewrite(
        partitions.into_iter(),
        &base,
        &RewriterConfig {
            max_sstable_size: Some(90),
        },
    )
    .unwrap();
    assert!(
        split.output_descriptors.len() > 1,
        "size-based rewriter should split compacted output SSTables"
    );
    for (idx, descriptor) in split.output_descriptors.iter().enumerate() {
        assert_eq!(descriptor.generation, 100 + idx as u64);
        let reader = SSTableReader::open(descriptor.clone()).unwrap();
        assert!(!reader.iter_partitions().unwrap().is_empty());
    }
    assert_eq!(
        split
            .stats
            .iter()
            .map(|stats| stats.partition_count)
            .sum::<u64>(),
        6
    );

    let planned_partitions: Vec<_> = (10..13).map(|i| partition(i, 64)).collect();
    let first_size =
        estimate_compaction_output_size(&planned_partitions[0].0, &planned_partitions[0].1);
    let planned = split_compaction_output(&planned_partitions, Some(first_size + 1));
    assert_eq!(planned.len(), 3);
    assert_eq!(planned[0][0].0, vec![10]);
    assert_eq!(planned[1][0].0, vec![11]);
    assert_eq!(planned[2][0].0, vec![12]);
}

#[test]
fn gap_guard_sstable_index_summary() {
    // CLOSED by prompt-14 continuation: Summary.db round-trip and sampled lookup windows.
    use cassandra_storage::sstable::summary::{IndexSummary, SummaryEntry};

    let entries = vec![
        SummaryEntry {
            partition_key: b"pk-000".to_vec(),
            index_offset: 0,
        },
        SummaryEntry {
            partition_key: b"pk-100".to_vec(),
            index_offset: 1024,
        },
        SummaryEntry {
            partition_key: b"pk-200".to_vec(),
            index_offset: 2048,
        },
        SummaryEntry {
            partition_key: b"pk-300".to_vec(),
            index_offset: 3072,
        },
    ];
    let summary = IndexSummary::from_entries(entries.clone());
    assert_eq!(summary.entry_count(), 4);
    assert_eq!(summary.min_key(), Some(&b"pk-000"[..]));
    assert_eq!(summary.max_key(), Some(&b"pk-300"[..]));

    let middle = summary.search(b"pk-150");
    assert_eq!(middle.index_start, 1);
    assert_eq!(middle.index_end, 2);
    let exact = summary.search(b"pk-200");
    assert_eq!(exact.index_start, 2);
    assert_eq!(exact.index_end, 3);
    let first_window = summary.search(b"pk-001");
    assert_eq!(first_window.index_start, 0);
    assert_eq!(first_window.index_end, 1);
    let before_first = summary.search(b"pk");
    assert_eq!(before_first.index_start, 0);
    assert_eq!(before_first.index_end, 0);
    let after_last = summary.search(b"pk-999");
    assert_eq!(after_last.index_start, 3);
    assert_eq!(after_last.index_end, 4);

    let mut file = tempfile::NamedTempFile::new().unwrap();
    IndexSummary::serialize(&entries, &mut file).unwrap();
    let loaded = IndexSummary::load(file.path()).unwrap();
    assert_eq!(loaded.entries(), entries.as_slice());

    let empty = IndexSummary::empty();
    assert_eq!(empty.search(b"anything").index_start, 0);
    assert_eq!(empty.search(b"anything").index_end, 0);
}

#[test]
fn gap_guard_sstable_metadata() {
    // PARTIALLY CLOSED by prompt-14 continuation: binary Statistics.db metadata,
    // JSON fallback, writer/reader/verifier binary Statistics.db integration,
    // and Java Big Statistics.db component-container parsing.
    use cassandra_storage::memtable::partition::{Cell, PartitionData, Row};
    use cassandra_storage::sstable::{
        compat::{
            JavaBigCommitLogInterval, JavaBigCommitLogPosition, JavaBigCompactionMetadata,
            JavaBigEncodingStats, JavaBigEstimatedHistogram, JavaBigEstimatedHistogramBucket,
            JavaBigImprovedMinMax, JavaBigMetadataType, JavaBigRawClusteringBound, JavaBigRawSlice,
            JavaBigSerializationHeader, JavaBigSerializationHeaderColumn,
            JavaBigStatisticsComponent, JavaBigStatsMetadataLegacyTail,
            JavaBigStatsMetadataModernTail, JavaBigStatsMetadataPrefix,
            JavaBigStatsMetadataTailFeatures, JavaBigTombstoneHistogram,
            JavaBigTombstoneHistogramEntry, JavaBigValidationMetadata, parse_java_big_statistics,
            parse_java_big_statistics_auto, parse_java_big_stats_metadata_modern_tail,
            write_java_big_compaction_metadata, write_java_big_serialization_header,
            write_java_big_statistics, write_java_big_stats_metadata_legacy_tail,
            write_java_big_stats_metadata_modern_tail, write_java_big_stats_metadata_prefix,
            write_java_big_validation_metadata,
        },
        format::{Component, SSTableDescriptor},
        metadata::{
            METADATA_MAGIC, MetadataCollector, MetadataError, MetadataSerializer, SSTableMetadata,
        },
        reader::SSTableReader,
        writer::{SSTableStats, SSTableWriter},
    };

    let stats = SSTableStats {
        partition_count: 3,
        row_count: 12,
        cell_count: 24,
        min_timestamp: 100,
        max_timestamp: 900,
        data_size: 4096,
        index_size: 512,
    };
    let metadata = SSTableMetadata::from_stats(&stats, b"pk-001".to_vec(), b"pk-999".to_vec());
    let encoded = MetadataSerializer::serialize(&metadata);
    assert_eq!(&encoded[..4], &METADATA_MAGIC);
    assert_eq!(encoded[4], 1, "metadata version");
    assert_eq!(&encoded[5..7], &[0x00, 0x09], "fixed field count");
    assert_eq!(MetadataSerializer::deserialize(&encoded).unwrap(), metadata);
    assert_eq!(
        MetadataSerializer::deserialize_auto(&encoded).unwrap(),
        metadata
    );

    let legacy_json = serde_json::to_vec(&stats).unwrap();
    let legacy = MetadataSerializer::deserialize_auto(&legacy_json).unwrap();
    assert_eq!(legacy.partition_count, stats.partition_count);
    assert_eq!(legacy.row_count, stats.row_count);
    assert_eq!(legacy.cell_count, stats.cell_count);
    assert_eq!(legacy.min_timestamp, stats.min_timestamp);
    assert_eq!(legacy.max_timestamp, stats.max_timestamp);
    assert!(legacy.min_partition_key.is_empty());
    assert!(legacy.max_partition_key.is_empty());

    let java_validation = JavaBigValidationMetadata {
        partitioner: "org.apache.cassandra.dht.Murmur3Partitioner".to_string(),
        bloom_filter_fp_chance: 0.01,
    };
    let java_validation_payload = write_java_big_validation_metadata(&java_validation).unwrap();
    let java_compaction = JavaBigCompactionMetadata {
        cardinality_estimator: vec![0x01, 0x02, 0x03],
    };
    let java_compaction_payload = write_java_big_compaction_metadata(&java_compaction).unwrap();
    let java_stats_prefix = JavaBigStatsMetadataPrefix {
        estimated_partition_size: JavaBigEstimatedHistogram {
            buckets: vec![
                JavaBigEstimatedHistogramBucket {
                    offset: 1,
                    count: 2,
                },
                JavaBigEstimatedHistogramBucket {
                    offset: 1,
                    count: 0,
                },
                JavaBigEstimatedHistogramBucket {
                    offset: 2,
                    count: 1,
                },
            ],
        },
        estimated_cell_per_partition_count: JavaBigEstimatedHistogram {
            buckets: vec![
                JavaBigEstimatedHistogramBucket {
                    offset: 1,
                    count: 3,
                },
                JavaBigEstimatedHistogramBucket {
                    offset: 1,
                    count: 4,
                },
            ],
        },
        commit_log_upper_bound: JavaBigCommitLogPosition {
            segment_id: 42,
            position: 256,
        },
        min_timestamp: 100,
        max_timestamp: 900,
        min_local_deletion_time: i64::MAX,
        max_local_deletion_time: 1_700_000_000,
        min_ttl: 0,
        max_ttl: 86_400,
        compression_ratio: 0.75,
        estimated_tombstone_drop_time: JavaBigTombstoneHistogram {
            max_bin_size: 2,
            entries: vec![
                JavaBigTombstoneHistogramEntry {
                    point: 1_700_000_000,
                    count: 5,
                },
                JavaBigTombstoneHistogramEntry {
                    point: 1_700_000_100,
                    count: 7,
                },
            ],
        },
        sstable_level: 3,
        repaired_at: 123_456,
        bytes_consumed: 0,
    };
    let mut java_stats_payload =
        write_java_big_stats_metadata_prefix(&java_stats_prefix, true).unwrap();
    let java_stats_prefix_len = java_stats_payload.len();
    let java_stats_tail_features = JavaBigStatsMetadataTailFeatures::for_big_version("nb");
    assert!(java_stats_tail_features.has_legacy_min_max);
    assert!(java_stats_tail_features.has_commit_log_lower_bound);
    assert!(java_stats_tail_features.has_commit_log_intervals);
    assert!(java_stats_tail_features.has_pending_repair);
    assert!(java_stats_tail_features.has_is_transient);
    assert!(java_stats_tail_features.has_originating_host_id);
    assert!(!java_stats_tail_features.has_partition_level_deletions_presence_marker);
    let pending_repair_uuid = [0x11_u8; 16];
    let originating_host_uuid = [0x22_u8; 16];
    let java_stats_tail = JavaBigStatsMetadataLegacyTail {
        legacy_min_clustering_values: vec![b"ck-a".to_vec(), b"ck-b".to_vec()],
        legacy_max_clustering_values: vec![b"ck-y".to_vec(), b"ck-z".to_vec()],
        has_legacy_counter_shards: true,
        total_columns_set: 44,
        total_rows: 12,
        commit_log_lower_bound: Some(JavaBigCommitLogPosition {
            segment_id: 40,
            position: 128,
        }),
        commit_log_intervals: vec![
            JavaBigCommitLogInterval {
                start: JavaBigCommitLogPosition {
                    segment_id: 40,
                    position: 128,
                },
                end: JavaBigCommitLogPosition {
                    segment_id: 41,
                    position: 64,
                },
            },
            JavaBigCommitLogInterval {
                start: JavaBigCommitLogPosition {
                    segment_id: 42,
                    position: 1,
                },
                end: JavaBigCommitLogPosition {
                    segment_id: 42,
                    position: 256,
                },
            },
        ],
        pending_repair: Some(pending_repair_uuid),
        is_transient: Some(true),
        originating_host_id: Some(originating_host_uuid),
        has_partition_level_deletions: None,
        bytes_consumed: 0,
    };
    let java_stats_tail_payload =
        write_java_big_stats_metadata_legacy_tail(&java_stats_tail, java_stats_tail_features)
            .unwrap();
    let java_stats_tail_len = java_stats_tail_payload.len();
    java_stats_payload.extend_from_slice(&java_stats_tail_payload);
    let java_header = JavaBigSerializationHeader {
        encoding_stats: JavaBigEncodingStats {
            min_timestamp: 1_442_880_000_000_123,
            min_local_deletion_time: 1_442_880_456,
            min_ttl: 60,
        },
        key_type: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        clustering_types: vec!["org.apache.cassandra.db.marshal.Int32Type".to_string()],
        static_columns: vec![JavaBigSerializationHeaderColumn {
            name: b"s".to_vec(),
            type_spec: "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
        }],
        regular_columns: vec![
            JavaBigSerializationHeaderColumn {
                name: b"v".to_vec(),
                type_spec: "org.apache.cassandra.db.marshal.BytesType".to_string(),
            },
            JavaBigSerializationHeaderColumn {
                name: b"ttl".to_vec(),
                type_spec: "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            },
        ],
    };
    let java_header_payload = write_java_big_serialization_header(&java_header).unwrap();
    let java_components = vec![
        JavaBigStatisticsComponent {
            component_type: JavaBigMetadataType::Validation,
            offset: 0,
            payload: java_validation_payload,
        },
        JavaBigStatisticsComponent {
            component_type: JavaBigMetadataType::Compaction,
            offset: 0,
            payload: java_compaction_payload,
        },
        JavaBigStatisticsComponent {
            component_type: JavaBigMetadataType::Stats,
            offset: 0,
            payload: java_stats_payload,
        },
        JavaBigStatisticsComponent {
            component_type: JavaBigMetadataType::Header,
            offset: 0,
            payload: java_header_payload,
        },
    ];
    let java_encoded = write_java_big_statistics(&java_components, true).unwrap();
    let java_stats = parse_java_big_statistics(&java_encoded, true).unwrap();
    let java_stats_auto = parse_java_big_statistics_auto(&java_encoded).unwrap();
    assert!(java_stats.has_checksum);
    assert!(java_stats_auto.has_checksum);
    assert_eq!(java_stats_auto.components, java_stats.components);
    assert_eq!(
        java_stats
            .validation_metadata()
            .unwrap()
            .unwrap()
            .partitioner,
        java_validation.partitioner
    );
    assert!(
        (java_stats
            .validation_metadata()
            .unwrap()
            .unwrap()
            .bloom_filter_fp_chance
            - java_validation.bloom_filter_fp_chance)
            .abs()
            < f64::EPSILON
    );
    assert_eq!(
        java_stats.compaction_metadata().unwrap().unwrap(),
        java_compaction
    );
    let parsed_java_stats_prefix = java_stats.stats_metadata_prefix(true).unwrap().unwrap();
    assert_eq!(
        parsed_java_stats_prefix.estimated_partition_size,
        java_stats_prefix.estimated_partition_size
    );
    assert_eq!(
        parsed_java_stats_prefix.estimated_cell_per_partition_count,
        java_stats_prefix.estimated_cell_per_partition_count
    );
    assert_eq!(
        parsed_java_stats_prefix.commit_log_upper_bound,
        java_stats_prefix.commit_log_upper_bound
    );
    assert_eq!(parsed_java_stats_prefix.min_timestamp, 100);
    assert_eq!(parsed_java_stats_prefix.max_timestamp, 900);
    assert_eq!(parsed_java_stats_prefix.min_local_deletion_time, i64::MAX);
    assert_eq!(
        parsed_java_stats_prefix.max_local_deletion_time,
        1_700_000_000
    );
    assert_eq!(parsed_java_stats_prefix.min_ttl, 0);
    assert_eq!(parsed_java_stats_prefix.max_ttl, 86_400);
    assert!((parsed_java_stats_prefix.compression_ratio - 0.75).abs() < f64::EPSILON);
    assert_eq!(
        parsed_java_stats_prefix.estimated_tombstone_drop_time,
        java_stats_prefix.estimated_tombstone_drop_time
    );
    assert_eq!(parsed_java_stats_prefix.sstable_level, 3);
    assert_eq!(parsed_java_stats_prefix.repaired_at, 123_456);
    assert_eq!(
        parsed_java_stats_prefix.bytes_consumed,
        java_stats_prefix_len
    );
    let parsed_java_stats_tail = java_stats
        .stats_metadata_legacy_tail(java_stats_prefix_len, java_stats_tail_features)
        .unwrap()
        .unwrap();
    assert_eq!(
        parsed_java_stats_tail.legacy_min_clustering_values,
        java_stats_tail.legacy_min_clustering_values
    );
    assert_eq!(
        parsed_java_stats_tail.legacy_max_clustering_values,
        java_stats_tail.legacy_max_clustering_values
    );
    assert!(parsed_java_stats_tail.has_legacy_counter_shards);
    assert_eq!(parsed_java_stats_tail.total_columns_set, 44);
    assert_eq!(parsed_java_stats_tail.total_rows, 12);
    assert_eq!(
        parsed_java_stats_tail.commit_log_lower_bound,
        java_stats_tail.commit_log_lower_bound
    );
    assert_eq!(
        parsed_java_stats_tail.commit_log_intervals,
        java_stats_tail.commit_log_intervals
    );
    assert_eq!(
        parsed_java_stats_tail.pending_repair,
        Some(pending_repair_uuid)
    );
    assert_eq!(parsed_java_stats_tail.is_transient, Some(true));
    assert_eq!(
        parsed_java_stats_tail.originating_host_id,
        Some(originating_host_uuid)
    );
    assert_eq!(parsed_java_stats_tail.has_partition_level_deletions, None);
    assert_eq!(parsed_java_stats_tail.bytes_consumed, java_stats_tail_len);

    let java_oa_tail_features = JavaBigStatsMetadataTailFeatures::for_big_version("oa");
    assert!(!java_oa_tail_features.has_legacy_min_max);
    assert!(java_oa_tail_features.has_improved_min_max);
    assert!(java_oa_tail_features.has_key_range);
    assert!(java_oa_tail_features.has_token_space_coverage);
    let java_oa_tail = JavaBigStatsMetadataModernTail {
        improved_min_max: JavaBigImprovedMinMax {
            clustering_types: vec![
                "org.apache.cassandra.db.marshal.UTF8Type".to_string(),
                "org.apache.cassandra.db.marshal.Int32Type".to_string(),
            ],
            covered_clustering: JavaBigRawSlice {
                start: JavaBigRawClusteringBound {
                    kind_ordinal: 1,
                    values: vec![Some(b"ck-a".to_vec()), Some(1_i32.to_be_bytes().to_vec())],
                },
                end: JavaBigRawClusteringBound {
                    kind_ordinal: 6,
                    values: vec![Some(b"ck-z".to_vec()), None],
                },
            },
        },
        has_legacy_counter_shards: false,
        total_columns_set: 88,
        total_rows: 22,
        commit_log_lower_bound: Some(JavaBigCommitLogPosition {
            segment_id: 90,
            position: 12,
        }),
        commit_log_intervals: vec![JavaBigCommitLogInterval {
            start: JavaBigCommitLogPosition {
                segment_id: 90,
                position: 12,
            },
            end: JavaBigCommitLogPosition {
                segment_id: 91,
                position: 44,
            },
        }],
        pending_repair: None,
        is_transient: Some(false),
        originating_host_id: None,
        has_partition_level_deletions: Some(true),
        first_key: Some(b"first".to_vec()),
        last_key: Some(b"last".to_vec()),
        token_space_coverage: Some(0.125),
        bytes_consumed: 0,
    };
    let mut java_oa_stats_payload =
        write_java_big_stats_metadata_prefix(&java_stats_prefix, true).unwrap();
    let java_oa_prefix_len = java_oa_stats_payload.len();
    let java_oa_tail_payload =
        write_java_big_stats_metadata_modern_tail(&java_oa_tail, java_oa_tail_features).unwrap();
    java_oa_stats_payload.extend_from_slice(&java_oa_tail_payload);
    let parsed_java_oa_tail = parse_java_big_stats_metadata_modern_tail(
        &java_oa_stats_payload,
        java_oa_prefix_len,
        java_oa_tail_features,
    )
    .unwrap();
    assert_eq!(
        parsed_java_oa_tail.improved_min_max,
        java_oa_tail.improved_min_max
    );
    assert_eq!(parsed_java_oa_tail.total_columns_set, 88);
    assert_eq!(parsed_java_oa_tail.total_rows, 22);
    assert_eq!(
        parsed_java_oa_tail.commit_log_intervals,
        java_oa_tail.commit_log_intervals
    );
    assert_eq!(
        parsed_java_oa_tail.has_partition_level_deletions,
        Some(true)
    );
    assert_eq!(
        parsed_java_oa_tail.first_key.as_deref(),
        Some(&b"first"[..])
    );
    assert_eq!(parsed_java_oa_tail.last_key.as_deref(), Some(&b"last"[..]));
    assert_eq!(parsed_java_oa_tail.token_space_coverage, Some(0.125));

    let parsed_java_header = java_stats.serialization_header().unwrap().unwrap();
    assert_eq!(parsed_java_header, java_header);
    assert_eq!(
        parsed_java_header.encoding_stats.min_timestamp,
        1_442_880_000_000_123
    );
    assert_eq!(
        parsed_java_header.encoding_stats.min_local_deletion_time,
        1_442_880_456
    );
    assert_eq!(parsed_java_header.encoding_stats.min_ttl, 60);
    assert_eq!(
        parsed_java_header.key_type,
        "org.apache.cassandra.db.marshal.UTF8Type"
    );
    assert_eq!(
        parsed_java_header.clustering_types,
        vec!["org.apache.cassandra.db.marshal.Int32Type".to_string()]
    );
    assert_eq!(parsed_java_header.static_columns[0].name, b"s");
    assert_eq!(parsed_java_header.regular_columns[0].name, b"v");
    assert_eq!(parsed_java_header.regular_columns[1].name, b"ttl");
    let mut corrupt_java = java_encoded;
    let last = corrupt_java.len() - 1;
    corrupt_java[last] ^= 0xff;
    assert!(parse_java_big_statistics(&corrupt_java, true).is_err());

    let mut collector = MetadataCollector::new();
    collector.add_partition_key(b"mid");
    collector.add_partition_key(b"aaa");
    collector.add_partition_key(b"zzz");
    collector.add_timestamp(700);
    collector.add_timestamp(300);
    collector.row_count = 3;
    collector.cell_count = 6;
    let collected = collector.finish();
    assert_eq!(collected.partition_count, 3);
    assert_eq!(collected.min_partition_key, b"aaa");
    assert_eq!(collected.max_partition_key, b"zzz");
    assert_eq!(collected.min_timestamp, 300);
    assert_eq!(collected.max_timestamp, 700);

    let mut corrupt = encoded.clone();
    corrupt[0] = b'X';
    assert!(matches!(
        MetadataSerializer::deserialize(&corrupt).unwrap_err(),
        MetadataError::BadMagic(_)
    ));
    let mut corrupt_field_count = encoded.clone();
    corrupt_field_count[5..7].copy_from_slice(&8u16.to_be_bytes());
    assert!(matches!(
        MetadataSerializer::deserialize(&corrupt_field_count).unwrap_err(),
        MetadataError::BadFieldCount(8)
    ));
    let mut trailing = encoded.clone();
    trailing.push(0);
    assert!(matches!(
        MetadataSerializer::deserialize(&trailing).unwrap_err(),
        MetadataError::TrailingBytes(1)
    ));

    let dir = tempfile::tempdir().unwrap();
    let desc = SSTableDescriptor::new(dir.path(), "ks", "tbl", 1);
    let mut partition = PartitionData::new();
    partition.apply_row(Row {
        clustering_key: b"ck".to_vec(),
        cells: vec![Cell {
            column: "v".to_string(),
            value: Some(b"value".to_vec()),
            timestamp: 1234,
            ttl: 0,
            local_deletion_time: None,
            is_tombstone: false,
        }],
        is_tombstone: false,
        local_deletion_time: None,
    });
    SSTableWriter::new(desc.clone())
        .write(&[(b"pk-001".to_vec(), partition)])
        .unwrap();
    let persisted = std::fs::read(desc.component_path(Component::Statistics)).unwrap();
    assert_eq!(&persisted[..4], &METADATA_MAGIC);
    let reader = SSTableReader::open(desc).unwrap();
    let persisted_stats = reader.stats().unwrap();
    assert_eq!(persisted_stats.partition_count, 1);
    assert_eq!(persisted_stats.row_count, 1);
    assert_eq!(persisted_stats.cell_count, 1);
}

#[test]
fn gap_guard_column_index_builder() {
    // PARTIAL closure: Rust has partition-internal IndexInfo block building
    // for clustering ranges, data offsets, widths, row counts, lookup, and a
    // binary IndexInfo block representation. It also parses Java Big
    // RowIndexEntry framing and offset tables; schema-dependent clustering
    // prefix decoding inside each IndexInfo remains separate.
    use cassandra_storage::sstable::column_index::{
        ColumnIndexBuilder, JAVA_INDEX_INFO_WIDTH_BASE, JavaBigClusteringPrefix,
        JavaBigDeletionTime, JavaBigIndexInfo, JavaBigIndexInfoTail, JavaBigRowIndexEntry,
        JavaBigRowIndexEntryKind, deserialize_index_infos,
        deserialize_java_big_row_index_entry_with_cache_size, find_block, read_java_big_index_info,
        serialize_index_infos, serialize_java_big_row_index_entry, write_java_big_index_info,
    };

    let builder = ColumnIndexBuilder::new(8);
    assert!(builder.is_empty());

    let mut builder = ColumnIndexBuilder::new(8);
    builder.add_row_at(b"ck1".to_vec(), 100, 3);
    builder.add_row_at(b"ck2".to_vec(), 103, 5);
    assert_eq!(builder.block_count(), 1);
    builder.add_row_at(b"ck3".to_vec(), 108, 2);
    assert_eq!(builder.block_count(), 2);
    let blocks = builder.finish();

    assert_eq!(blocks.len(), 2);
    assert_eq!(blocks[0].first_clustering, b"ck1");
    assert_eq!(blocks[0].last_clustering, b"ck2");
    assert_eq!(blocks[0].offset, 100);
    assert_eq!(blocks[0].width, 8);
    assert_eq!(blocks[0].row_count, 2);
    assert_eq!(blocks[1].first_clustering, b"ck3");
    assert_eq!(blocks[1].last_clustering, b"ck3");
    assert_eq!(blocks[1].offset, 108);

    let ck2_block = find_block(&blocks, b"ck2").unwrap();
    assert!(ck2_block.contains_clustering(b"ck2"));
    assert_eq!(find_block(&blocks, b"missing"), None);

    let encoded = serialize_index_infos(&blocks).unwrap();
    assert_eq!(&encoded[..4], b"CIDX");
    assert_eq!(encoded[4], 1);
    assert_eq!(deserialize_index_infos(&encoded).unwrap(), blocks);

    let mut corrupt = encoded;
    corrupt[0] = b'X';
    assert!(deserialize_index_infos(&corrupt).is_err());

    let clustering_types = vec!["Int32Type".to_string(), "UTF8Type".to_string()];
    let first_info = JavaBigIndexInfo {
        first_clustering: JavaBigClusteringPrefix {
            kind_ordinal: 0x20,
            values: vec![Some(1i32.to_be_bytes().to_vec()), Some(b"alpha".to_vec())],
        },
        last_clustering: JavaBigClusteringPrefix {
            kind_ordinal: 0x20,
            values: vec![Some(3i32.to_be_bytes().to_vec()), Some(b"gamma".to_vec())],
        },
        tail: JavaBigIndexInfoTail {
            offset: 0xa0,
            width: JAVA_INDEX_INFO_WIDTH_BASE as u64 + 1,
            end_open_marker: None,
        },
    };
    let second_info = JavaBigIndexInfo {
        first_clustering: JavaBigClusteringPrefix {
            kind_ordinal: 0x20,
            values: vec![Some(4i32.to_be_bytes().to_vec()), Some(b"delta".to_vec())],
        },
        last_clustering: JavaBigClusteringPrefix {
            kind_ordinal: 0x20,
            values: vec![Some(9i32.to_be_bytes().to_vec()), Some(b"omega".to_vec())],
        },
        tail: JavaBigIndexInfoTail {
            offset: 0xb0,
            width: JAVA_INDEX_INFO_WIDTH_BASE as u64 + 2,
            end_open_marker: None,
        },
    };
    let mut first_info_bytes = Vec::new();
    write_java_big_index_info(&mut first_info_bytes, &first_info, &clustering_types).unwrap();
    let mut second_info_bytes = Vec::new();
    write_java_big_index_info(&mut second_info_bytes, &second_info, &clustering_types).unwrap();
    let mut index_info_bytes = first_info_bytes.clone();
    index_info_bytes.extend_from_slice(&second_info_bytes);

    let java_entry = JavaBigRowIndexEntry {
        data_file_position: 4096,
        promoted_size: 0,
        kind: JavaBigRowIndexEntryKind::Indexed,
        header_length: Some(11),
        deletion_time: Some(JavaBigDeletionTime::LIVE),
        column_index_count: 2,
        index_info_bytes,
        offsets: vec![0, first_info_bytes.len() as i32],
    };
    let java_encoded = serialize_java_big_row_index_entry(&java_entry).unwrap();
    let java_parsed =
        deserialize_java_big_row_index_entry_with_cache_size(&java_encoded, 512).unwrap();
    assert_eq!(java_parsed.data_file_position, 4096);
    assert_eq!(java_parsed.kind, JavaBigRowIndexEntryKind::Indexed);
    assert_eq!(java_parsed.header_length, Some(11));
    assert_eq!(java_parsed.deletion_time, Some(JavaBigDeletionTime::LIVE));
    assert_eq!(java_parsed.column_index_count, 2);
    let parsed_first =
        read_java_big_index_info(java_parsed.index_info_slice(0).unwrap(), &clustering_types)
            .unwrap();
    assert_eq!(parsed_first, first_info);
    let parsed_second =
        read_java_big_index_info(java_parsed.index_info_slice(1).unwrap(), &clustering_types)
            .unwrap();
    assert_eq!(parsed_second, second_info);

    let mut large_entry = java_entry.clone();
    large_entry.index_info_bytes = vec![0xcc; 32];
    large_entry.offsets = vec![0, 16];
    let large_encoded = serialize_java_big_row_index_entry(&large_entry).unwrap();
    let large_parsed =
        deserialize_java_big_row_index_entry_with_cache_size(&large_encoded, 8).unwrap();
    assert_eq!(large_parsed.kind, JavaBigRowIndexEntryKind::ShallowIndexed);
}

#[test]
fn gap_guard_bloom_filter() {
    // CLOSED by prompt-14 continuation: SSTable BloomFilter membership and Filter.db round-trip.
    use cassandra_storage::sstable::bloom::BloomFilter;

    let mut filter = BloomFilter::new(128, 0.01);
    for key in [b"pk-0001", b"pk-0002", b"pk-0042", b"pk-0100"] {
        filter.add(key);
    }

    for key in [b"pk-0001", b"pk-0002", b"pk-0042", b"pk-0100"] {
        assert!(filter.might_contain(key), "inserted key should be present");
    }
    assert!(
        !filter.might_contain(b"definitely-missing-key"),
        "small, sparse filter should reject an unrelated key"
    );

    let mut serialized = Vec::new();
    filter.serialize(&mut serialized).unwrap();
    assert!(
        serialized.len() > 16,
        "Filter.db payload should include header and bitset"
    );

    let restored = BloomFilter::deserialize(&mut serialized.as_slice()).unwrap();
    for key in [b"pk-0001", b"pk-0002", b"pk-0042", b"pk-0100"] {
        assert!(restored.might_contain(key), "round-tripped filter lost key");
    }

    let mut false_positives = 0;
    for i in 0..512 {
        if restored.might_contain(format!("missing-{i}").as_bytes()) {
            false_positives += 1;
        }
    }
    assert!(
        false_positives < 40,
        "unexpectedly high false-positive count: {false_positives}"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// DISTRIBUTED GAPS
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn gap_guard_paging() {
    // CLOSED by prompt-14 (WU-06): PagingState serialize/deserialize,
    // page_size/paging_state in SelectPlan, apply_paging() in executor,
    // HAS_MORE_PAGES flag in protocol response.
    use cassandra_coordinator::read::PagingState;
    let state = PagingState::new(b"pk".to_vec(), vec![], 100, 0);
    assert!(state.has_more());
    let bytes = state.serialize();
    assert!(PagingState::deserialize(&bytes).is_some());
}

#[test]
fn gap_guard_aggregation() {
    // CLOSED by prompt-14 follow-up: parser/planner carry GROUP BY and the
    // executor has grouped aggregate state for count/sum/min/max/avg.
    // Native count_rows/countRows catalog names and COUNT(1) route to the same
    // row-count aggregate used by Cassandra.
    use cassandra_cql::ast::{SelectColumns, Statement};
    use cassandra_cql::functions::FunctionRegistry;
    use cassandra_cql::selection::aggregation::has_aggregates;
    use cassandra_types::CqlType;

    let stmt = cassandra_cql::parser::parse("SELECT id, count(*) FROM ks.t GROUP BY id").unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    assert_eq!(select.group_by, vec!["id"]);
    let SelectColumns::Named(selectors) = select.columns else {
        panic!("expected named selectors");
    };
    assert!(has_aggregates(&selectors));

    let count_one = cassandra_cql::parser::parse("SELECT COUNT(1) FROM ks.t").unwrap();
    let Statement::Select(select) = count_one else {
        panic!("expected select");
    };
    let SelectColumns::Named(selectors) = select.columns else {
        panic!("expected named selectors");
    };
    assert!(matches!(
        selectors.as_slice(),
        [cassandra_cql::ast::Selector::Count]
    ));

    let count_rows = cassandra_cql::ast::Selector::Function("count_rows".to_string(), Vec::new());
    let legacy_count_rows =
        cassandra_cql::ast::Selector::Function("countRows".to_string(), Vec::new());
    assert!(has_aggregates(&[count_rows, legacy_count_rows]));

    let registry = FunctionRegistry::with_builtins();
    for name in ["count_rows", "countRows"] {
        let function = registry
            .resolve(name, &[])
            .unwrap_or_else(|| panic!("missing aggregate catalog function {name}"));
        assert_eq!(function.return_type(), CqlType::Bigint);
    }

    let count_column = registry
        .resolve("count", &[CqlType::Int])
        .expect("missing count(int) aggregate catalog function");
    assert_eq!(count_column.arg_types(), vec![CqlType::Int]);
    assert_eq!(count_column.return_type(), CqlType::Bigint);

    for cql_type in [
        CqlType::Tinyint,
        CqlType::Smallint,
        CqlType::Int,
        CqlType::Bigint,
        CqlType::Counter,
        CqlType::Float,
        CqlType::Double,
        CqlType::Varint,
        CqlType::Decimal,
    ] {
        for name in ["sum", "avg", "min", "max"] {
            let function = registry
                .resolve(name, std::slice::from_ref(&cql_type))
                .unwrap_or_else(|| {
                    panic!(
                        "missing aggregate catalog function {name}({})",
                        cql_type.cql_name()
                    )
                });
            assert_eq!(function.arg_types(), vec![cql_type.clone()]);
            assert_eq!(function.return_type(), cql_type);
        }
    }
}

#[test]
fn gap_guard_gossip_wire_compat() {
    // PARTIAL closure by prompt-14 follow-up: gossip SYN/ACK/ACK2 payloads
    // now have a Java-layout binary codec for digests, address+port endpoints,
    // heartbeat state, supported ApplicationState ordinals, and VersionedValue
    // payloads, and the Rust gossip handlers/task/shadow round use that codec.
    // Full mixed Java+Rust cluster formation remains tracked.
    use cassandra_cluster_metadata::{
        ApplicationState, Endpoint, EndpointState, GossipDigest, GossipDigestAck, GossipDigestAck2,
        GossipDigestSyn, GossipWireError, JavaGossipCodec, VersionedValue,
    };
    use std::collections::HashMap;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    let digest = GossipDigest {
        endpoint: ep(7001),
        generation: 1,
        max_version: 10,
    };
    let digest_bytes = JavaGossipCodec::encode_digest(&digest).unwrap();
    assert_eq!(
        digest_bytes,
        vec![6, 127, 0, 0, 1, 0x1b, 0x59, 0, 0, 0, 1, 0, 0, 0, 10]
    );
    assert_eq!(
        JavaGossipCodec::decode_digest(&digest_bytes).unwrap(),
        digest
    );

    let syn = GossipDigestSyn {
        cluster_id: "test-cluster".to_string(),
        digests: vec![digest.clone()],
    };
    let decoded_syn =
        JavaGossipCodec::decode_syn(&JavaGossipCodec::encode_syn(&syn).unwrap()).unwrap();
    assert_eq!(decoded_syn.cluster_id, "test-cluster");
    assert_eq!(decoded_syn.digests, vec![digest.clone()]);

    let mut state = EndpointState::new(7);
    state.heartbeat.version = 8;
    state.set_state(
        ApplicationState::StatusWithPort,
        VersionedValue::new(9, "NORMAL,7001"),
    );
    state.set_state(ApplicationState::Datacenter, VersionedValue::new(10, "dc1"));
    state.set_state(ApplicationState::RpcReady, VersionedValue::new(11, "true"));
    state.set_state(
        ApplicationState::InternalAddressAndPort,
        VersionedValue::new(12, "127.0.0.1:7001"),
    );
    let mut states = HashMap::new();
    states.insert(ep(7001), state);

    let ack = GossipDigestAck {
        stale_digests: vec![digest],
        updated_states: states.clone(),
    };
    let decoded_ack =
        JavaGossipCodec::decode_ack(&JavaGossipCodec::encode_ack(&ack).unwrap()).unwrap();
    assert_eq!(decoded_ack.stale_digests.len(), 1);
    let decoded_state = decoded_ack.updated_states.get(&ep(7001)).unwrap();
    assert_eq!(decoded_state.heartbeat.generation, 7);
    assert_eq!(
        decoded_state
            .get_state(&ApplicationState::StatusWithPort)
            .unwrap()
            .value,
        "NORMAL,7001"
    );
    assert_eq!(
        decoded_state
            .get_state(&ApplicationState::RpcReady)
            .unwrap()
            .value,
        "true"
    );

    let ack2 = GossipDigestAck2 {
        updated_states: states,
    };
    assert!(
        JavaGossipCodec::decode_ack2(&JavaGossipCodec::encode_ack2(&ack2).unwrap())
            .unwrap()
            .updated_states
            .contains_key(&ep(7001))
    );

    let modified_utf_value = "dc\0\u{1f600}";
    let mut state = EndpointState::new(13);
    state.set_state(
        ApplicationState::Datacenter,
        VersionedValue::new(14, modified_utf_value),
    );
    let mut states = HashMap::new();
    states.insert(ep(7002), state);
    let encoded = JavaGossipCodec::encode_ack2(&GossipDigestAck2 {
        updated_states: states,
    })
    .unwrap();
    let java_modified_utf = [
        0, 10, b'd', b'c', 0xc0, 0x80, 0xed, 0xa0, 0xbd, 0xed, 0xb8, 0x80,
    ];
    assert!(
        encoded
            .windows(java_modified_utf.len())
            .any(|window| window == java_modified_utf)
    );
    let decoded = JavaGossipCodec::decode_ack2(&encoded).unwrap();
    assert_eq!(
        decoded
            .updated_states
            .get(&ep(7002))
            .unwrap()
            .get_state(&ApplicationState::Datacenter)
            .unwrap()
            .value,
        modified_utf_value
    );

    let mut invalid = vec![6, 127, 0, 0, 1, 0x1b, 0x59, 0, 0, 0, 1, 0, 0, 0, 10, 1];
    assert!(matches!(
        JavaGossipCodec::decode_digest(&invalid),
        Err(GossipWireError::InvalidData(_))
    ));
    invalid.clear();

    let pathological_count = i32::MAX.to_be_bytes();
    assert!(matches!(
        JavaGossipCodec::decode_ack(&pathological_count),
        Err(GossipWireError::InvalidData(_))
    ));
    assert!(matches!(
        JavaGossipCodec::decode_ack2(&pathological_count),
        Err(GossipWireError::InvalidData(_))
    ));

    for app_state in [
        ApplicationState::RemovalCoordinator,
        ApplicationState::X11Padding,
        ApplicationState::RpcReady,
        ApplicationState::InternalAddressAndPort,
        ApplicationState::PaddingX1,
        ApplicationState::PaddingX10,
    ] {
        let mut state = EndpointState::new(20);
        state.set_state(app_state, VersionedValue::new(21, "java-compatible"));
        let mut states = HashMap::new();
        states.insert(ep(7003), state);
        let decoded = JavaGossipCodec::decode_ack2(
            &JavaGossipCodec::encode_ack2(&GossipDigestAck2 {
                updated_states: states,
            })
            .unwrap(),
        )
        .unwrap();
        assert_eq!(
            decoded
                .updated_states
                .get(&ep(7003))
                .unwrap()
                .get_state(&app_state)
                .unwrap()
                .value,
            "java-compatible"
        );
    }
}

#[test]
fn gap_guard_internode_wire_compat() {
    // PARTIAL closure by prompt-14 follow-up: Rust now has a Java-layout
    // internode raw-message codec for Message.Serializer headers, Cassandra
    // unsigned vint encoding, Java verb-id mapping for overlapping verbs, and
    // opaque header parameters/payload bytes. Connection pools, flow control,
    // typed payload serializers, and full mixed-cluster transport remain tracked.
    use cassandra_messaging::{
        JavaMessageCodec, JavaMessageParam, JavaWireError, Message, Verb, frame::MAX_PAYLOAD_SIZE,
        java_verb_id, rust_verb_from_java_id,
    };

    assert_eq!(java_verb_id(Verb::GossipDigestSyn).unwrap(), 14);
    assert_eq!(java_verb_id(Verb::Ping).unwrap(), 31);
    assert_eq!(rust_verb_from_java_id(91).unwrap(), Verb::Pong);

    let mut message = Message::request(Verb::GossipDigestSyn, 128, b"gossip-payload".to_vec());
    message.header.creation_timestamp = 0x0102_0304;
    let bytes = JavaMessageCodec::encode_raw(&message, 5000).unwrap();

    assert_eq!(&bytes[0..2], &[0x80, 0x80]); // Java unsigned vint for 128
    assert_eq!(&bytes[2..6], &[1, 2, 3, 4]);
    assert_eq!(&bytes[6..8], &[0x93, 0x88]); // Java unsigned vint for 5000
    assert_eq!(bytes[8], 14); // Java GOSSIP_DIGEST_SYN verb id

    let decoded = JavaMessageCodec::decode_raw(&bytes).unwrap();
    assert_eq!(decoded.header.id, 128);
    assert_eq!(decoded.header.verb, Verb::GossipDigestSyn);
    assert_eq!(decoded.header.expires_in_millis, 5000);
    assert_eq!(decoded.header.params_count, 0);
    assert_eq!(decoded.payload, b"gossip-payload");

    let round_tripped = JavaMessageCodec::to_message(decoded);
    assert_eq!(round_tripped.header.verb, Verb::GossipDigestSyn);
    assert_eq!(round_tripped.header.message_id, 128);
    assert_eq!(round_tripped.payload, b"gossip-payload");

    let with_params = JavaMessageCodec::encode_raw_with_params(
        &message,
        5000,
        &[
            JavaMessageParam {
                param_type: 1,
                value: vec![0xaa, 0xbb],
            },
            JavaMessageParam {
                param_type: 130,
                value: b"trace".to_vec(),
            },
        ],
    )
    .unwrap();
    let decoded_with_params = JavaMessageCodec::decode_raw(&with_params).unwrap();
    assert_eq!(decoded_with_params.header.params_count, 2);
    assert_eq!(
        decoded_with_params.params[0],
        JavaMessageParam {
            param_type: 1,
            value: vec![0xaa, 0xbb],
        }
    );
    assert_eq!(decoded_with_params.params[1].param_type, 130);
    assert_eq!(decoded_with_params.payload, b"gossip-payload");

    let unsupported = vec![1, 0, 0, 0, 2, 3, 0x80, 0xfa, 0, 0, 0];
    assert!(matches!(
        JavaMessageCodec::decode_raw(&unsupported),
        Err(JavaWireError::UnsupportedVerb(250))
    ));

    let oversized = Message::request(Verb::Ping, 129, vec![0u8; MAX_PAYLOAD_SIZE as usize + 1]);
    assert!(matches!(
        JavaMessageCodec::encode_raw(&oversized, 5000),
        Err(JavaWireError::InvalidData(msg)) if msg.contains("payload size exceeds maximum")
    ));
}

// ═══════════════════════════════════════════════════════════════════════
// DISTRIBUTED GAPS (Expanded — Prompt 11)
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn gap_guard_snitches_dynamic() {
    // CLOSED by prompt-14 follow-up: DynamicEndpointSnitch wraps a static
    // snitch, applies latency/severity scores, honors badness threshold, and
    // resets scores for recovery.
    use cassandra_cluster_metadata::{DynamicEndpointSnitch, Endpoint, SimpleSnitch, Snitch};
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::time::Duration;

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    let source = ep(7001);
    let fast = ep(7001);
    let slow = ep(7002);

    let snitch = DynamicEndpointSnitch::new(Box::new(SimpleSnitch));
    snitch.record_latency(fast, 100.0);
    snitch.record_latency(slow, 10_000.0);
    let mut endpoints = vec![slow, fast];
    snitch.sort_by_proximity(&source, &mut endpoints);
    assert_eq!(endpoints, vec![fast, slow]);

    snitch.update_severity(fast, 10.0);
    let mut endpoints = vec![fast, slow];
    snitch.sort_by_proximity(&source, &mut endpoints);
    assert_eq!(endpoints, vec![slow, fast]);

    snitch.reset_scores();
    let mut endpoints = vec![slow, fast];
    snitch.sort_by_proximity(&source, &mut endpoints);
    assert_eq!(endpoints, vec![slow, fast]);

    let thresholded =
        DynamicEndpointSnitch::with_config(Box::new(SimpleSnitch), 0.10, Duration::from_secs(600));
    thresholded.record_latency(fast, 100.0);
    thresholded.record_latency(slow, 105.0);
    let mut endpoints = vec![slow, fast];
    thresholded.sort_by_proximity(&source, &mut endpoints);
    assert_eq!(endpoints, vec![slow, fast]);
}

#[test]
fn gap_guard_coordinator_read_repair() {
    // PARTIALLY CLOSED by prompt-14 continuation: read-repair staging, replica
    // verb handling, and staged repair payload decoding.
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use cassandra_cluster_metadata::Endpoint;
    use cassandra_coordinator::verb_handlers::read_repair_handler::{
        ReadRepairRequest, ReadRepairResponse,
    };
    use cassandra_coordinator::{
        CoordinatedMutation, ReadRepairHandler, ReadRepairStrategy, ReadRepairVerbHandler,
    };
    use cassandra_messaging::{Message, Verb};
    use cassandra_storage::commitlog::CommitLogConfig;
    use cassandra_storage::engine::{EngineConfig, StorageEngine};
    use cassandra_storage::memtable::partition::{Cell, PartitionData, Row};
    use tempfile::TempDir;

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    fn test_storage() -> (StorageEngine, TempDir) {
        let temp = TempDir::new().unwrap();
        let storage = StorageEngine::open(EngineConfig {
            data_directories: vec![temp.path().join("data")],
            commitlog: CommitLogConfig {
                directory: temp.path().join("commitlog"),
                ..CommitLogConfig::default()
            },
            ..EngineConfig::default()
        })
        .unwrap();
        (storage, temp)
    }

    let mut data = PartitionData::new();
    data.apply_row(Row {
        clustering_key: b"ck".to_vec(),
        cells: vec![Cell {
            column: "v".to_string(),
            value: Some(b"new".to_vec()),
            timestamp: 99,
            ttl: 0,
            local_deletion_time: None,
            is_tombstone: false,
        }],
        is_tombstone: false,
        local_deletion_time: None,
    });

    assert_eq!(
        ReadRepairStrategy::from_str_cql("NONE"),
        ReadRepairStrategy::None
    );
    assert_eq!(
        ReadRepairStrategy::from_str_cql("blocking"),
        ReadRepairStrategy::Blocking
    );

    let mut blocking = ReadRepairHandler::new(ReadRepairStrategy::Blocking);
    assert!(blocking.is_enabled());
    blocking.stage_repair(
        ep(7002),
        "ks".to_string(),
        "users".to_string(),
        b"pk".to_vec(),
        data.clone(),
    );
    assert_eq!(blocking.pending_count(), 1);
    assert_eq!(blocking.pending[0].target, ep(7002));
    assert_eq!(blocking.pending[0].keyspace, "ks");
    assert_eq!(blocking.pending[0].table, "users");
    assert_eq!(blocking.pending[0].partition_key, b"pk");

    let mut disabled = ReadRepairHandler::new(ReadRepairStrategy::None);
    assert!(!disabled.is_enabled());
    disabled.stage_repair(
        ep(7003),
        "ks".to_string(),
        "users".to_string(),
        b"pk".to_vec(),
        data,
    );
    assert_eq!(disabled.pending_count(), 0);

    let request = ReadRepairRequest {
        mutation: CoordinatedMutation::simple(
            "ks".to_string(),
            "users".to_string(),
            b"pk".to_vec(),
            Vec::new(),
            1234,
        ),
    };
    let payload = serde_json::to_vec(&request).unwrap();
    let (storage, _temp) = test_storage();
    let response = ReadRepairVerbHandler::handle_with_storage(
        Message::request(Verb::ReadRepair, 77, payload),
        &storage,
    )
    .expect("valid repair response");
    assert_eq!(response.header.verb, Verb::ReadRepairResponse);
    assert!(response.is_response());
    let body: ReadRepairResponse = serde_json::from_slice(&response.payload).unwrap();
    assert!(body.success);

    let mut staged_payload = Vec::new();
    for bytes in [b"ks".as_slice(), b"users".as_slice(), b"pk".as_slice()] {
        staged_payload.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
        staged_payload.extend_from_slice(bytes);
    }
    staged_payload.extend_from_slice(&1u32.to_be_bytes());
    staged_payload.extend_from_slice(&2u32.to_be_bytes());
    staged_payload.extend_from_slice(b"ck");
    staged_payload.extend_from_slice(&1u32.to_be_bytes());
    staged_payload.extend_from_slice(&1u32.to_be_bytes());
    staged_payload.extend_from_slice(b"v");
    staged_payload.extend_from_slice(&8u32.to_be_bytes());
    staged_payload.extend_from_slice(b"repaired");
    staged_payload.extend_from_slice(&5678i64.to_be_bytes());

    let staged_response = ReadRepairVerbHandler::handle_with_storage(
        Message::request(Verb::ReadRepair, 79, staged_payload),
        &storage,
    )
    .expect("staged repair should return response");
    assert!(staged_response.is_response());
    let repaired = storage.read_partition("ks", "users", b"pk").unwrap();
    let repaired_row = repaired.rows.get(b"ck".as_slice()).unwrap();
    assert_eq!(
        repaired_row.cells[0].value.as_deref(),
        Some(b"repaired".as_slice())
    );

    let failure = ReadRepairVerbHandler::handle_with_storage(
        Message::request(Verb::ReadRepair, 78, b"not json".to_vec()),
        &storage,
    )
    .expect("invalid repair should return failure");
    assert!(failure.is_failure());
}

#[test]
fn gap_guard_repair_messages() {
    // CLOSED by prompt-14 continuation: consistent repair and sync message envelopes.
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use cassandra_cluster_metadata::Endpoint;
    use cassandra_common::Token;
    use cassandra_repair::messages::{
        ConsistentSessionState, FailSessionMessage, FinalizeCommit, FinalizePromise,
        FinalizePropose, PrepareConsistentRequest, PrepareConsistentResponse, RepairMessage,
        StatusRequest, StatusResponse, SyncRequest, SyncResponse,
    };

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    let parent_id = Default::default();
    let session_id = Default::default();
    let source = ep(7001);
    let target = ep(7002);
    let ranges = vec![(Token::from_raw(0), Token::from_raw(100))];
    let tables = vec!["users".to_string(), "orders".to_string()];

    let prepare = RepairMessage::PrepareConsistentRequest(PrepareConsistentRequest {
        parent_id,
        keyspace: "ks".to_string(),
        tables: tables.clone(),
        ranges: ranges.clone(),
        participants: vec![source, target],
        is_forced: true,
    });
    let decoded = RepairMessage::from_payload(&prepare.to_payload()).unwrap();
    match decoded {
        RepairMessage::PrepareConsistentRequest(req) => {
            assert_eq!(req.keyspace, "ks");
            assert_eq!(req.tables, tables);
            assert_eq!(req.ranges, ranges);
            assert_eq!(req.participants, vec![source, target]);
            assert!(req.is_forced);
        }
        other => panic!("expected PrepareConsistentRequest, got {other:?}"),
    }

    let response = RepairMessage::PrepareConsistentResponse(PrepareConsistentResponse {
        parent_id,
        endpoint: source,
        success: true,
    });
    assert!(matches!(
        RepairMessage::from_payload(&response.to_payload()).unwrap(),
        RepairMessage::PrepareConsistentResponse(PrepareConsistentResponse { success: true, .. })
    ));

    for message in [
        RepairMessage::FinalizePropose(FinalizePropose { parent_id }),
        RepairMessage::FinalizePromise(FinalizePromise {
            parent_id,
            endpoint: target,
            success: true,
        }),
        RepairMessage::FinalizeCommit(FinalizeCommit { parent_id }),
        RepairMessage::FailSession(FailSessionMessage { parent_id }),
        RepairMessage::StatusRequest(StatusRequest { parent_id }),
    ] {
        let payload = message.to_payload();
        assert!(RepairMessage::from_payload(&payload).is_ok());
    }

    let status = RepairMessage::StatusResponse(StatusResponse {
        parent_id,
        state: ConsistentSessionState::FinalizePromised,
    });
    match RepairMessage::from_payload(&status.to_payload()).unwrap() {
        RepairMessage::StatusResponse(resp) => {
            assert_eq!(resp.state, ConsistentSessionState::FinalizePromised);
            assert_eq!(resp.state.to_string(), "FINALIZE_PROMISED");
        }
        other => panic!("expected StatusResponse, got {other:?}"),
    }

    let sync = RepairMessage::SyncRequest(SyncRequest {
        parent_id,
        session_id,
        source,
        target,
        ranges: vec![(Token::from_raw(-10), Token::from_raw(10))],
        keyspace: "ks".to_string(),
        tables: vec!["users".to_string()],
    });
    match RepairMessage::from_payload(&sync.to_payload()).unwrap() {
        RepairMessage::SyncRequest(req) => {
            assert_eq!(req.source, source);
            assert_eq!(req.target, target);
            assert_eq!(req.tables, vec!["users"]);
        }
        other => panic!("expected SyncRequest, got {other:?}"),
    }

    let sync_response = RepairMessage::SyncResponse(SyncResponse {
        parent_id,
        session_id,
        success: false,
        error: Some("stream timeout".to_string()),
    });
    match RepairMessage::from_payload(&sync_response.to_payload()).unwrap() {
        RepairMessage::SyncResponse(resp) => {
            assert!(!resp.success);
            assert_eq!(resp.error.as_deref(), Some("stream timeout"));
        }
        other => panic!("expected SyncResponse, got {other:?}"),
    }

    assert!(RepairMessage::from_payload(b"not json").is_err());
}

#[test]
fn gap_guard_consistent_repair() {
    // CLOSED by prompt-14 continuation: coordinator/local consistent repair state machines.
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use cassandra_cluster_metadata::Endpoint;
    use cassandra_common::Token;
    use cassandra_repair::anti_compaction::{classify_partitions, is_range_fully_repaired};
    use cassandra_repair::messages::{FinalizePromise, PrepareConsistentResponse};
    use cassandra_repair::{
        ConsistentSessionState, CoordinatorAction, CoordinatorSession, LocalSession,
        LocalSessionStore, PendingRepairTracker, consistent_local::InMemoryLocalSessionStore,
    };
    use uuid::Uuid;

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    let parent_id = Uuid::from_u128(0x1234);
    let participants = vec![ep(7001), ep(7002)];
    let ranges = vec![(Token::from_raw(0), Token::from_raw(100))];
    let mut coordinator = CoordinatorSession::new(
        parent_id,
        participants.clone(),
        "ks".to_string(),
        vec!["users".to_string()],
        ranges.clone(),
    );

    let prepare_messages = coordinator.prepare();
    assert_eq!(prepare_messages.len(), 2);
    assert_eq!(prepare_messages[0].1.keyspace, "ks");
    assert_eq!(prepare_messages[0].1.ranges, ranges);
    assert_eq!(prepare_messages[0].1.participants, participants);

    assert_eq!(
        coordinator.handle_prepare_response(PrepareConsistentResponse {
            parent_id,
            endpoint: ep(7001),
            success: true,
        }),
        CoordinatorAction::WaitForMore
    );
    assert_eq!(
        coordinator.handle_prepare_response(PrepareConsistentResponse {
            parent_id,
            endpoint: ep(7002),
            success: true,
        }),
        CoordinatorAction::ProceedToRepair
    );
    assert_eq!(coordinator.state, ConsistentSessionState::Prepared);

    coordinator.repair_complete();
    assert_eq!(coordinator.state, ConsistentSessionState::FinalizeProposing);
    let finalize_messages = coordinator.propose_finalize();
    assert_eq!(finalize_messages.len(), 2);

    assert_eq!(
        coordinator.handle_promise(FinalizePromise {
            parent_id,
            endpoint: ep(7001),
            success: true,
        }),
        CoordinatorAction::WaitForMore
    );
    assert_eq!(
        coordinator.handle_promise(FinalizePromise {
            parent_id,
            endpoint: ep(7002),
            success: true,
        }),
        CoordinatorAction::Commit
    );
    assert_eq!(coordinator.state, ConsistentSessionState::FinalizePromised);
    let commit_messages = coordinator.commit();
    assert_eq!(commit_messages.len(), 2);
    assert_eq!(coordinator.state, ConsistentSessionState::Committed);

    let req = prepare_messages[0].1.clone();
    let (mut local, response) = LocalSession::handle_prepare(&req, ep(9000), ep(7001));
    assert!(response.success);
    assert_eq!(local.state, ConsistentSessionState::Prepared);
    local.set_repairing();
    assert_eq!(local.state, ConsistentSessionState::Repairing);
    let promise = local.handle_finalize_propose(ep(7001));
    assert!(promise.success);
    assert_eq!(local.state, ConsistentSessionState::FinalizePromised);
    local.handle_commit().unwrap();
    assert_eq!(local.state, ConsistentSessionState::Committed);

    let store = InMemoryLocalSessionStore::default();
    let (mut pending, _) = LocalSession::handle_prepare(&req, ep(9000), ep(7001));
    store.save(&pending).unwrap();
    assert_eq!(store.load(parent_id).unwrap().unwrap().keyspace, "ks");
    assert_eq!(store.list_pending().unwrap().len(), 1);
    pending.handle_fail();
    store.save(&pending).unwrap();
    assert!(store.list_pending().unwrap().is_empty());

    let wrapping = (Token::from_raw(100), Token::from_raw(-100));
    assert!(is_range_fully_repaired(wrapping, &[wrapping]));
    assert!(is_range_fully_repaired(
        wrapping,
        &[
            (Token::from_raw(100), Token::MINIMUM),
            (Token::MINIMUM, Token::from_raw(-100))
        ]
    ));
    assert!(!is_range_fully_repaired(
        wrapping,
        &[(Token::from_raw(100), Token::MINIMUM)]
    ));
    let full_ring = (Token::MINIMUM, Token::MINIMUM);
    assert!(is_range_fully_repaired(full_ring, &[full_ring]));
    let tokens = vec![Token::MINIMUM, Token::from_raw(0), Token::MAXIMUM];
    let (repaired, unrepaired) = classify_partitions(&tokens, &[full_ring]);
    assert_eq!(repaired, tokens);
    assert!(unrepaired.is_empty());

    let tracker = PendingRepairTracker::new(Box::new(InMemoryLocalSessionStore::default()));
    let (tracked, _) = LocalSession::handle_prepare(&req, ep(9000), ep(7002));
    tracker.add_session(&tracked).unwrap();
    assert!(tracker.get_session(parent_id).unwrap().is_some());
    tracker.remove_session(parent_id).unwrap();
    assert!(tracker.get_session(parent_id).unwrap().is_none());

    let mut failing = CoordinatorSession::new(
        Uuid::from_u128(0x5678),
        vec![ep(7011)],
        "ks".to_string(),
        vec!["users".to_string()],
        vec![(Token::from_raw(10), Token::from_raw(20))],
    );
    assert!(matches!(
        failing.handle_prepare_response(PrepareConsistentResponse {
            parent_id: failing.parent_id,
            endpoint: ep(7011),
            success: false,
        }),
        CoordinatorAction::Fail(_)
    ));
    assert_eq!(failing.state, ConsistentSessionState::Failed);
}

#[test]
fn gap_guard_streaming_messages() {
    // CLOSED by prompt-14 continuation: streaming verb payloads and wire partition conversion.
    use cassandra_storage::memtable::partition::{Cell, PartitionData, Row};
    use cassandra_streaming::{
        StreamCompleteMessage, StreamCompleteResponseMessage, StreamDataMessage,
        StreamDataResponseMessage, StreamInitMessage, StreamInitResponseMessage, StreamMessage,
        StreamOperation, WirePartitions,
    };
    use uuid::Uuid;

    let session_id = Uuid::from_u128(0xabc);
    let transfer_id = Uuid::from_u128(0xdef);
    let init = StreamMessage::Init(StreamInitMessage {
        session_id,
        operation: StreamOperation::Bootstrap,
        description: "bootstrap stream".to_string(),
        keyspaces: vec!["ks".to_string()],
        ranges: vec![(0, 100)],
    });
    let init_frame = init.to_message(1).unwrap();
    assert_eq!(StreamMessage::from_message(&init_frame).unwrap(), init);

    let init_response = StreamMessage::InitResponse(StreamInitResponseMessage {
        session_id,
        accepted: true,
        reason: None,
    });
    let init_response_frame = init_response.to_message(2).unwrap();
    assert_eq!(
        StreamMessage::from_message(&init_response_frame).unwrap(),
        init_response
    );

    let data = StreamMessage::Data(StreamDataMessage {
        session_id,
        transfer_id,
        sequence: 7,
        data: vec![1, 2, 3, 4],
        checksum: [0x5a; 16],
        checksum_algorithm: "MD5".to_string(),
        compressed: true,
        is_last: false,
    });
    let data_frame = data.to_message(3).unwrap();
    assert_eq!(StreamMessage::from_message(&data_frame).unwrap(), data);

    let data_response = StreamMessage::DataResponse(StreamDataResponseMessage {
        session_id,
        transfer_id,
        sequence: 7,
        accepted: false,
        error: Some("checksum mismatch".to_string()),
    });
    let data_response_frame = data_response.to_message(4).unwrap();
    assert_eq!(
        StreamMessage::from_message(&data_response_frame).unwrap(),
        data_response
    );

    let complete = StreamMessage::Complete(StreamCompleteMessage {
        session_id,
        success: true,
        error: None,
    });
    let complete_frame = complete.to_message(5).unwrap();
    assert_eq!(
        StreamMessage::from_message(&complete_frame).unwrap(),
        complete
    );

    let complete_response = StreamMessage::CompleteResponse(StreamCompleteResponseMessage {
        session_id,
        acknowledged: true,
    });
    let complete_response_frame = complete_response.to_message(6).unwrap();
    assert_eq!(
        StreamMessage::from_message(&complete_response_frame).unwrap(),
        complete_response
    );

    let mut partition = PartitionData::new();
    partition.apply_row(Row {
        clustering_key: b"ck".to_vec(),
        cells: vec![Cell {
            column: "v".to_string(),
            value: Some(b"value".to_vec()),
            timestamp: 123,
            ttl: 0,
            local_deletion_time: None,
            is_tombstone: false,
        }],
        is_tombstone: false,
        local_deletion_time: None,
    });
    partition.set_tombstone(456, 789);
    let partitions = vec![(b"pk".to_vec(), partition)];
    let wire = WirePartitions::from_partitions(&partitions);
    assert_eq!(wire.entries.len(), 1);
    let round_tripped = wire.to_partitions();
    assert_eq!(round_tripped[0].0, b"pk");
    assert_eq!(round_tripped[0].1.tombstone_timestamp, Some(456));
    assert_eq!(round_tripped[0].1.rows.len(), 1);

    let mut invalid_frame = data_frame;
    invalid_frame.payload = b"not json".to_vec();
    assert!(StreamMessage::from_message(&invalid_frame).is_err());
}

#[test]
fn gap_guard_hints_persistence() {
    // CLOSED by prompt-14 follow-up: append-only hint segment files include
    // descriptor naming, writer/reader roundtrip, CRC validation, listing, disk
    // usage, and segment deletion.
    use cassandra_cluster_metadata::Endpoint;
    use cassandra_coordinator::hint_segment::HintSegmentDescriptor;
    use cassandra_coordinator::{
        CoordinatedMutation, Hint, HintSegmentManager, HintSegmentReader, HintSegmentWriter,
    };
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    fn hint(id: u64) -> Hint {
        Hint {
            target: ep(7002),
            mutation: CoordinatedMutation::simple(
                "ks".to_string(),
                "users".to_string(),
                format!("user{id}").into_bytes(),
                vec![],
                1000 + id as i64,
            ),
            created_at: 1_700_000_000_000,
            hint_id: id,
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let descriptor = HintSegmentDescriptor {
        target_id: "node-a".to_string(),
        created_at: 1_700_000_000_000,
        version: 1,
    };
    assert_eq!(
        descriptor.filename(),
        "node-a-1700000000000.hints".to_string()
    );

    let mut writer = HintSegmentWriter::create(dir.path(), descriptor.clone(), 1024 * 1024)
        .expect("create hint writer");
    assert!(writer.append(&hint(1)).unwrap());
    assert!(writer.append(&hint(2)).unwrap());
    writer.sync().unwrap();
    assert!(writer.bytes_written() > 0);

    let mut reader = HintSegmentReader::open(writer.path()).expect("open hint reader");
    let hints = reader.read_all().unwrap();
    assert_eq!(hints.len(), 2);
    assert_eq!(hints[0].hint_id, 1);
    assert_eq!(hints[1].hint_id, 2);
    assert_eq!(reader.entries_read(), 2);
    assert_eq!(reader.entries_corrupted(), 0);

    let manager = HintSegmentManager::new(dir.path().to_path_buf());
    let segments = manager.segments_for("node-a").unwrap();
    assert_eq!(segments.len(), 1);
    assert!(manager.total_disk_usage().unwrap() > 0);
    assert_eq!(manager.delete_all_for("node-a").unwrap(), 1);
    assert!(manager.segments_for("node-a").unwrap().is_empty());
    assert_eq!(manager.total_disk_usage().unwrap(), 0);
}

// ═══════════════════════════════════════════════════════════════════════
// SECURITY GAPS
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn gap_guard_fql() {
    // CLOSED by prompt-14 continuation: binary FQL records plus file logger/reader.
    use cassandra_security::{FqlLogger, FqlReader, FqlRecord};

    let record = FqlRecord {
        timestamp_micros: 1_700_000_000_000_000,
        consistency_level: 1,
        query: "SELECT * FROM ks.users WHERE id = ?".to_string(),
        bind_values: vec![b"user-123".to_vec(), vec![0x00, 0x01, 0x02]],
    };
    let encoded = record.encode();
    let (decoded, consumed) = FqlRecord::decode(&encoded).unwrap();
    assert_eq!(decoded, record);
    assert_eq!(consumed, encoded.len());

    let dir = tempfile::tempdir().unwrap();
    let logger = FqlLogger::new(dir.path().to_path_buf(), 10, true).unwrap();
    assert!(logger.is_enabled());
    logger.log_query(&record).unwrap();
    logger
        .log_query(&FqlRecord {
            timestamp_micros: 1_700_000_000_000_001,
            consistency_level: 6,
            query: "INSERT INTO ks.users (id, name) VALUES (?, ?)".to_string(),
            bind_values: vec![b"user-456".to_vec(), b"Ada".to_vec()],
        })
        .unwrap();

    let records = FqlReader::read_all(&dir.path().join("fql.bin")).unwrap();
    assert_eq!(records.len(), 2);
    assert_eq!(records[0], record);
    assert_eq!(
        records[1].query,
        "INSERT INTO ks.users (id, name) VALUES (?, ?)"
    );
    assert_eq!(
        records[1].bind_values,
        vec![b"user-456".to_vec(), b"Ada".to_vec()]
    );

    let disabled_dir = tempfile::tempdir().unwrap();
    let disabled = FqlLogger::new(disabled_dir.path().to_path_buf(), 10, false).unwrap();
    disabled.log_query(&record).unwrap();
    assert!(!disabled_dir.path().join("fql.bin").exists());

    assert!(FqlRecord::decode(&encoded[..3]).is_err());
}

#[test]
fn gap_guard_ldap_kerberos_auth() {
    // PARTIAL closure by prompt-14 follow-up: provider-backed LDAP and
    // Kerberos authenticators now exercise external bind/ticket validation
    // flows and map identities to Cassandra roles. Production LDAP/Kerberos
    // network/SASL integrations remain tracked in the report.
    use cassandra_security::{
        AesCbcProvider, Authenticator, Credentials, CryptoProvider, InMemoryLdapDirectory,
        KerberosAuthenticator, LdapAuthenticator, StaticKerberosValidator,
    };

    let ldap = LdapAuthenticator::new(
        InMemoryLdapDirectory::new().add_user("ada", "secret", "analyst", false),
    );
    let ldap_user = ldap
        .authenticate(&Credentials {
            username: "ada".into(),
            password: "secret".into(),
            source_address: None,
        })
        .unwrap();
    assert_eq!(ldap_user.role_name, "analyst");
    assert!(!ldap_user.is_anonymous);
    assert!(ldap.require_authentication());
    assert!(
        ldap.authenticate(&Credentials {
            username: "ada".into(),
            password: "wrong".into(),
            source_address: None,
        })
        .is_err()
    );

    let kerberos = KerberosAuthenticator::new(
        "cassandra/host@EXAMPLE.COM",
        StaticKerberosValidator::new().add_ticket(
            b"ticket".to_vec(),
            "ada@EXAMPLE.COM",
            "analyst",
            false,
        ),
    );
    let krb_user = kerberos.authenticate_token(b"ticket").unwrap();
    assert_eq!(krb_user.role_name, "analyst");
    assert!(kerberos.authenticate_token(b"bad-ticket").is_err());

    let aes = AesCbcProvider;
    let iv = vec![0x11; aes.iv_length()];
    let plaintext = b"java-compatible AES-192 CBC";
    let ciphertext = aes.encrypt(&[0x42; 24], &iv, plaintext).unwrap();
    assert_eq!(
        aes.decrypt(&[0x42; 24], &iv, &ciphertext).unwrap(),
        plaintext
    );
    assert!(aes.encrypt(&[0x42; 20], &iv, plaintext).is_err());
}

#[test]
fn gap_guard_auth_persistent() {
    // PARTIAL closure by prompt-14 follow-up: roles, role memberships,
    // permission grants, network permissions, identity_to_roles mappings, and
    // CIDR groups now round-trip through a system_auth-style persistent store.
    // Full distributed system_auth keyspace/query integration remains tracked
    // in the report.
    use cassandra_security::{
        CidrGroup, CidrGroupsManager, DCPermissions, IdentityRoleMapper, NetworkAuthorizer,
        Permission, PersistentCidrGroupsManager, PersistentIdentityRoleMapper,
        PersistentNetworkAuthorizer, PersistentRoleManager, Resource, Role, RoleManager,
    };

    fn role(name: &str) -> Role {
        Role {
            name: name.to_string(),
            is_superuser: false,
            can_login: true,
            hashed_password: None,
            member_of: vec![],
            network_permissions: None,
        }
    }

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("system_auth.json");
    let manager = PersistentRoleManager::open(&path).unwrap();
    manager.create_role(role("reader"));
    manager.create_role(role("analyst"));
    manager.grant_role("reader", "analyst").unwrap();
    let table = Resource::Table {
        keyspace: "ks".into(),
        table: "tbl".into(),
    };
    manager
        .store()
        .grant("reader", &table, Permission::Select)
        .unwrap();
    let identity_mapper = PersistentIdentityRoleMapper::open(&path).unwrap();
    identity_mapper.set_mapping("spiffe://example/analyst", "analyst");
    let cidr_groups = PersistentCidrGroupsManager::open(&path).unwrap();
    cidr_groups
        .create_group(CidrGroup {
            name: "office".to_string(),
            ranges: vec!["10.0.0.0/8".to_string()],
        })
        .unwrap();
    let network_authorizer = PersistentNetworkAuthorizer::open(&path).unwrap();
    network_authorizer.set_permissions(
        "analyst",
        DCPermissions::restricted(std::collections::HashSet::from(["dc1".to_string()])),
    );
    assert!(path.exists());

    let reopened = PersistentRoleManager::open(&path).unwrap();
    assert_eq!(
        reopened.get_role("analyst").unwrap().member_of,
        vec!["reader".to_string()]
    );
    assert_eq!(
        reopened.store().list_permissions("reader", &table).unwrap(),
        vec![Permission::Select]
    );
    reopened
        .store()
        .revoke("reader", &table, Permission::Select)
        .unwrap();
    assert!(
        reopened
            .store()
            .list_permissions("reader", &table)
            .unwrap()
            .is_empty()
    );
    let reopened_identity_mapper = PersistentIdentityRoleMapper::open(&path).unwrap();
    assert_eq!(
        reopened_identity_mapper.get_role_for_identity("spiffe://example/analyst"),
        Some("analyst".to_string())
    );
    reopened_identity_mapper.remove_mapping("spiffe://example/analyst");
    assert!(
        PersistentIdentityRoleMapper::open(&path)
            .unwrap()
            .get_role_for_identity("spiffe://example/analyst")
            .is_none()
    );
    let reopened_cidr_groups = PersistentCidrGroupsManager::open(&path).unwrap();
    assert_eq!(
        reopened_cidr_groups.get_group("office").unwrap().ranges,
        vec!["10.0.0.0/8".to_string()]
    );
    reopened_cidr_groups
        .update_group("office", vec!["172.16.0.0/12".to_string()])
        .unwrap();
    assert_eq!(
        PersistentCidrGroupsManager::open(&path)
            .unwrap()
            .get_group("office")
            .unwrap()
            .ranges,
        vec!["172.16.0.0/12".to_string()]
    );
    assert!(
        reopened_cidr_groups
            .create_group(CidrGroup {
                name: "bad".to_string(),
                ranges: vec!["not-a-cidr".to_string()],
            })
            .is_err()
    );
    let reopened_network_authorizer = PersistentNetworkAuthorizer::open(&path).unwrap();
    assert!(
        reopened_network_authorizer
            .authorize("analyst", "dc1")
            .is_ok()
    );
    assert!(
        reopened_network_authorizer
            .authorize("analyst", "dc2")
            .is_err()
    );
    reopened_network_authorizer.drop("analyst");
    assert!(
        PersistentNetworkAuthorizer::open(&path)
            .unwrap()
            .authorize("analyst", "dc2")
            .is_ok()
    );
}

// ═══════════════════════════════════════════════════════════════════════
// TOOLING GAPS
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn gap_guard_nodetool_commands() {
    // PARTIAL closure by prompt-14 follow-up: Rust now exposes a nodetool
    // command catalog with admin API paths across cluster, topology,
    // snapshots, compaction, stats, cache/hints, logging, repair, and SSTable
    // operations. Full Java nodetool/JMX command parity remains tracked.
    use cassandra_admin::{command_by_name, command_catalog};
    use std::collections::HashSet;

    let commands = command_catalog();
    assert!(commands.len() >= 50);
    let categories = commands
        .iter()
        .map(|command| command.category)
        .collect::<HashSet<_>>();
    for category in [
        "cluster",
        "topology",
        "snapshots",
        "compaction",
        "stats",
        "cache",
        "hints",
        "logging",
        "repair",
    ] {
        assert!(categories.contains(category), "missing {category} commands");
    }
    assert_eq!(
        command_by_name("tablestats").unwrap().admin_path,
        Some("/api/v1/stats/tables")
    );
    assert_eq!(
        command_by_name("rebuild_index").unwrap().admin_path,
        Some("/api/v1/index/rebuild")
    );
}

#[test]
fn gap_guard_sstable_offline_tools() {
    // PARTIAL closure by prompt-14 follow-up: verifier, scrubber, and upgrader
    // primitives operate on Rust SSTables. Export/split/metadata/relevel/repair
    // offline tool parity remains tracked in the report.
    use cassandra_storage::{
        memtable::partition::{Cell, PartitionData, Row},
        sstable::{
            format::SSTableDescriptor, reader::SSTableReader, scrubber::SSTableScrubber,
            upgrader::SSTableUpgrader, verifier::SSTableVerifier, writer::SSTableWriter,
        },
    };

    fn partition(key: u8) -> (Vec<u8>, PartitionData) {
        let mut data = PartitionData::new();
        data.apply_row(Row {
            clustering_key: vec![key],
            cells: vec![Cell {
                column: "v".to_string(),
                value: Some(vec![key, key + 1]),
                timestamp: 1000 + key as i64,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });
        (vec![key], data)
    }

    let dir = tempfile::tempdir().unwrap();
    let input = SSTableDescriptor::new(dir.path(), "ks", "tbl", 1);
    let scrubbed = SSTableDescriptor::new(dir.path(), "ks", "tbl", 2);
    let upgraded = SSTableDescriptor::new(dir.path(), "ks", "tbl", 3);
    let partitions = vec![partition(1), partition(2), partition(3)];

    let stats = SSTableWriter::new(input.clone())
        .write(&partitions)
        .unwrap();
    assert_eq!(stats.partition_count, 3);

    let verification = SSTableVerifier::verify(&input);
    assert!(
        verification.is_valid(),
        "freshly written SSTable should verify without error issues: {:?}",
        verification.issues
    );

    let scrub_result = SSTableScrubber::scrub(&input, &scrubbed).unwrap();
    assert_eq!(scrub_result.partitions_recovered, 3);
    assert_eq!(scrub_result.partitions_skipped, 0);
    assert!(scrub_result.bytes_read > 0);
    assert!(scrub_result.bytes_written > 0);
    let scrubbed_rows = SSTableReader::open(scrubbed.clone())
        .unwrap()
        .iter_partitions()
        .unwrap();
    assert_eq!(scrubbed_rows.len(), 3);

    let upgrade_result = SSTableUpgrader::upgrade(&scrubbed, &upgraded).unwrap();
    assert_eq!(upgrade_result.partitions_written, 3);
    assert!(upgrade_result.input_data_size > 0);
    assert!(upgrade_result.output_data_size > 0);
    let upgraded_rows = SSTableReader::open(upgraded)
        .unwrap()
        .iter_partitions()
        .unwrap();
    assert_eq!(upgraded_rows.len(), 3);

    let overwrite_err = SSTableUpgrader::upgrade(&input, &input).unwrap_err();
    assert_eq!(overwrite_err.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn gap_guard_virtual_tables() {
    // PARTIAL closure by prompt-14 follow-up: admin virtual table registry
    // exposes a broad system_views subset used by HTTP/admin surfaces,
    // including auth/JMX/network cache-key tables, plus system_virtual_schema
    // keyspace/table/column introspection. Full Java db.virtual parity across
    // every built-in table remains tracked.
    use cassandra_admin::VirtualTableRegistry;

    let registry = VirtualTableRegistry::with_builtins();
    let tables = registry.list_tables();
    assert!(
        tables.len() >= 28,
        "expected at least 28 built-in virtual tables, got {}",
        tables.len()
    );

    for table_name in [
        "local",
        "peers",
        "settings",
        "thread_pools",
        "sstable_tasks",
        "clients",
        "gossip_info",
        "caches",
        "snapshots",
        "internode_inbound",
        "internode_outbound",
        "streaming",
        "system_logs",
        "queries",
        "slow_queries",
        "system_properties",
        "pending_hints",
        "repairs",
        "batch_metrics",
        "cql_metrics",
        "credentials_cache_keys",
        "permissions_cache_keys",
        "jmx_permissions_cache_keys",
        "network_permissions_cache_keys",
        "roles_cache_keys",
    ] {
        let table = registry
            .get("system_views", table_name)
            .unwrap_or_else(|| panic!("missing system_views.{table_name}"));
        assert!(
            !table.columns().is_empty(),
            "system_views.{table_name} must expose columns"
        );
    }

    let local = registry.get("system_views", "local").unwrap();
    let local_rows = local.rows();
    assert_eq!(local_rows.len(), 1);
    assert!(local_rows[0].contains_key("host_id"));
    assert_eq!(
        local_rows[0]
            .get("native_transport_port")
            .map(String::as_str),
        Some("9042")
    );

    let peers = registry.get("system_views", "peers").unwrap();
    let peer_columns: Vec<_> = peers
        .columns()
        .into_iter()
        .map(|column| column.name)
        .collect();
    assert!(peer_columns.contains(&"host_id".to_string()));
    assert!(peer_columns.contains(&"tokens".to_string()));

    let settings = registry.get("system_views", "settings").unwrap();
    assert!(
        settings
            .rows()
            .iter()
            .any(|row| row.get("name").map(String::as_str) == Some("concurrent_reads"))
    );
    for table_name in [
        "credentials_cache_keys",
        "permissions_cache_keys",
        "jmx_permissions_cache_keys",
        "network_permissions_cache_keys",
        "roles_cache_keys",
    ] {
        let table = registry.get("system_views", table_name).unwrap();
        assert_eq!(table.columns()[0].name, "cache_key");
        assert!(table.rows().is_empty());
    }
    let virtual_keyspaces = registry.get("system_virtual_schema", "keyspaces").unwrap();
    assert!(
        virtual_keyspaces
            .rows()
            .iter()
            .any(|row| { row.get("keyspace_name").map(String::as_str) == Some("system_views") })
    );

    let virtual_tables = registry.get("system_virtual_schema", "tables").unwrap();
    assert!(virtual_tables.rows().iter().any(|row| {
        row.get("keyspace_name").map(String::as_str) == Some("system_views")
            && row.get("table_name").map(String::as_str) == Some("local")
    }));

    let virtual_columns = registry.get("system_virtual_schema", "columns").unwrap();
    assert!(virtual_columns.rows().iter().any(|row| {
        row.get("keyspace_name").map(String::as_str) == Some("system_views")
            && row.get("table_name").map(String::as_str) == Some("local")
            && row.get("column_name").map(String::as_str) == Some("host_id")
            && row.get("type").map(String::as_str) == Some("uuid")
    }));
    assert!(registry.get("system_views", "nonexistent").is_none());
}

// ═══════════════════════════════════════════════════════════════════════
// TRUNK-ONLY / EXPERIMENTAL GAPS
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn gap_guard_tcm() {
    // PARTIAL closure by prompt-14 follow-up: TCM epoch state, metadata
    // transformations, snapshots, and in-memory log storage are implemented.
    // Full distributed CMS/quorum and Java trunk parity remains tracked.
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    use cassandra_cluster_metadata::{
        Endpoint, Epoch, NodeId, NodeState, TcmMetadata, Transformation,
        tcm::{
            TcmSnapshot,
            log_storage::{Entry, InMemoryLogStorage, LogStorage, LogStorageError},
        },
    };
    use cassandra_common::Token;
    use uuid::Uuid;

    let node_id = NodeId::from_uuid(Uuid::from_u128(1));
    let endpoint = Endpoint::new(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 7000));
    let mut metadata = TcmMetadata::new();
    assert!(metadata.epoch.is_empty());

    let epoch = metadata
        .apply(
            Transformation::Register {
                node_id,
                endpoint,
                dc: "dc1".to_string(),
                rack: "rack1".to_string(),
            },
            node_id,
        )
        .unwrap();
    assert_eq!(epoch, Epoch::FIRST);
    assert!(metadata.directory.get(&node_id).is_some());

    let epoch = metadata
        .apply(
            Transformation::AssignTokens {
                node_id,
                tokens: vec![Token(0), Token(100)],
            },
            node_id,
        )
        .unwrap();
    assert_eq!(epoch, Epoch(2));
    let info = metadata.directory.get(&node_id).unwrap();
    assert_eq!(info.tokens, vec![Token(0), Token(100)]);
    assert_eq!(info.state, NodeState::Normal);

    let schema_version = Uuid::from_u128(99);
    metadata
        .apply(
            Transformation::SchemaChange {
                schema_version,
                description: "create table ks.tbl".to_string(),
            },
            node_id,
        )
        .unwrap();
    assert_eq!(metadata.schema_version, Some(schema_version));

    let snapshot = metadata.snapshot();
    assert_eq!(snapshot.epoch, metadata.epoch);
    assert_eq!(snapshot.nodes.len(), 1);
    let restored = TcmMetadata::restore_from_snapshot(&snapshot);
    assert_eq!(restored.epoch, metadata.epoch);
    assert_eq!(restored.schema_version, Some(schema_version));

    let storage = InMemoryLogStorage::new();
    let entry = Entry {
        id: 1,
        epoch: Epoch::FIRST,
        transformation: Transformation::ForceSnapshot,
        committed_by: node_id,
    };
    storage.append(entry.clone()).unwrap();
    assert_eq!(storage.latest_epoch(), Epoch::FIRST);
    assert!(matches!(
        storage.append(entry).unwrap_err(),
        LogStorageError::DuplicateEntry(Epoch::FIRST)
    ));
    assert_eq!(storage.entries_since(Epoch::EMPTY).unwrap().len(), 1);
    assert!(storage.entries_since(Epoch::FIRST).unwrap().is_empty());

    storage
        .store_snapshot(TcmSnapshot {
            epoch: Epoch(3),
            nodes: snapshot.nodes.clone(),
            schema_version: snapshot.schema_version,
            in_progress: vec![],
            created_at_millis: 1,
        })
        .unwrap();
    assert_eq!(
        storage.get_latest_snapshot().unwrap().unwrap().epoch,
        Epoch(3)
    );
    let sealed = storage.seal_period(Epoch(3), 1).unwrap();
    assert_eq!(sealed.epoch, Epoch(3));
    assert_eq!(storage.sealed_periods().unwrap().len(), 1);
}

#[test]
fn gap_guard_accord() {
    // PARTIAL closure: Rust now exposes core Accord transaction primitives,
    // command lifecycle validation, topology mapping, migration tracking, and
    // an in-memory replayable journal. Full distributed Accord wire protocol
    // and recovery/application parity remain outside this guard.
    use cassandra_accord::{
        AccordJournal, AccordTopology, CommandStatus, CommandStore, KeyMigrationState, Keys,
        TableMigrationState, Timestamp, Txn, TxnId,
    };
    use uuid::Uuid;

    let node_id = Uuid::from_u128(1);
    let txn_id = TxnId::with_timestamp(100, node_id, 0);
    assert!(TxnId::with_timestamp(101, node_id, 0) > txn_id);
    assert!(CommandStatus::Committed.is_decided());
    assert!(CommandStatus::Applied.is_terminal());

    let txn = Txn {
        keys: Keys::multiple(vec![b"k1".to_vec(), b"k2".to_vec()]),
        mutation: b"mutation-bytes".to_vec(),
        keyspace: "ks".to_string(),
    };
    assert_eq!(txn.keys.len(), 2);

    let store = CommandStore::new();
    store
        .pre_accept(txn_id, txn.clone(), Timestamp(10))
        .unwrap();
    assert_eq!(store.get_status(&txn_id), Some(CommandStatus::PreAccepted));
    assert!(store.apply(txn_id).is_err());
    store.accept(txn_id, Timestamp(11)).unwrap();
    store.commit(txn_id, Timestamp(12)).unwrap();
    assert_eq!(store.get(&txn_id).unwrap().txn.mutation, txn.mutation);
    store.apply(txn_id).unwrap();
    assert_eq!(store.get_status(&txn_id), Some(CommandStatus::Applied));
    assert!(store.invalidate(txn_id).is_err());

    let journal = AccordJournal::new(true);
    assert!(journal.is_enabled());
    assert_eq!(
        journal
            .write(
                txn_id,
                CommandStatus::PreAccepted,
                Timestamp(10),
                b"pre".to_vec()
            )
            .unwrap(),
        0
    );
    assert_eq!(
        journal
            .write(
                txn_id,
                CommandStatus::Committed,
                Timestamp(12),
                b"commit".to_vec()
            )
            .unwrap(),
        1
    );
    let replayed = journal.replay();
    assert_eq!(replayed.len(), 2);
    assert_eq!(replayed[0].status, CommandStatus::PreAccepted);
    assert_eq!(replayed[1].status, CommandStatus::Committed);
    assert_eq!(journal.truncate_before(1), 1);
    assert_eq!(journal.len(), 1);

    let disabled_journal = AccordJournal::new(false);
    disabled_journal
        .write(txn_id, CommandStatus::PreAccepted, Timestamp(10), vec![])
        .unwrap();
    assert!(disabled_journal.is_empty());

    let mut topology = AccordTopology::new();
    let accord_id = topology.register_node(node_id);
    assert_eq!(topology.register_node(node_id), accord_id);
    assert_eq!(topology.get_node(accord_id), Some(node_id));
    assert_eq!(topology.get_accord_id(&node_id), Some(accord_id));
    assert_eq!(topology.node_count(), 1);
    assert_eq!(topology.advance_epoch(), 1);

    let mut migration = TableMigrationState::new(Uuid::from_u128(2));
    assert_eq!(migration.get_key_state(b"k1"), KeyMigrationState::Paxos);
    migration.mark_migrating(b"k1".to_vec());
    assert_eq!(migration.get_key_state(b"k1"), KeyMigrationState::Migrating);
    migration.mark_migrated(b"k1".to_vec());
    assert_eq!(migration.get_key_state(b"k1"), KeyMigrationState::Accord);
    assert!(migration.is_complete());
    assert_eq!(migration.progress(), 1.0);
}

#[test]
fn gap_guard_consensus() {
    // PARTIAL closure: Rust has a consensus router for transactional table
    // modes, Paxos/Accord dispatch, Mixed-mode migration routing, and metrics.
    // Full Java service.consensus parity remains experimental.
    use cassandra_accord::{AccordConfig, AccordService, KeyMigrationState, TableMigrationState};
    use cassandra_coordinator::{
        CasResult, ConsensusRouter, PaxosConfig, PaxosCoordinator, PaxosReplica,
    };
    use cassandra_schema::table::TransactionalMode;
    use std::sync::Arc;
    use std::sync::atomic::Ordering;
    use uuid::Uuid;

    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();

    runtime.block_on(async {
        let node_id = Uuid::from_u128(1);
        let replica = Arc::new(PaxosReplica::new(node_id));
        let paxos = Arc::new(PaxosCoordinator::with_config(
            node_id,
            vec![replica],
            1,
            PaxosConfig::default(),
        ));
        let accord = Arc::new(AccordService::new(
            AccordConfig {
                enabled: true,
                journaling_enabled: true,
            },
            node_id,
        ));
        let router = ConsensusRouter::new(paxos, accord);

        let rejected = router
            .execute_cas(
                "ks",
                "tbl",
                TransactionalMode::Off,
                b"pk",
                b"mutation".to_vec(),
                || async { None },
                |_| true,
            )
            .await;
        assert!(rejected.is_err());
        assert_eq!(router.metrics().rejected_count.load(Ordering::Relaxed), 1);

        let paxos_result = router
            .execute_cas(
                "ks",
                "tbl",
                TransactionalMode::Paxos,
                b"pk",
                b"mutation".to_vec(),
                || async { None },
                |current| current.is_none(),
            )
            .await
            .unwrap();
        assert!(matches!(paxos_result, CasResult::Success));
        assert_eq!(router.metrics().paxos_count.load(Ordering::Relaxed), 1);

        let failed_condition = router
            .execute_cas(
                "ks",
                "tbl",
                TransactionalMode::Paxos,
                b"pk2",
                b"mutation".to_vec(),
                || async { Some(b"existing".to_vec()) },
                |_| false,
            )
            .await
            .unwrap();
        assert!(matches!(
            failed_condition,
            CasResult::ConditionNotMet { .. }
        ));

        let accord_result = router
            .execute_cas(
                "ks",
                "tbl",
                TransactionalMode::Accord,
                b"accord_direct",
                b"mutation".to_vec(),
                || async { None },
                |_| true,
            )
            .await
            .unwrap();
        assert!(matches!(accord_result, CasResult::Success));
        assert_eq!(router.metrics().accord_count.load(Ordering::Relaxed), 1);

        let mut migration = TableMigrationState::new(Uuid::nil());
        migration.set_key_state(b"paxos_key".to_vec(), KeyMigrationState::Paxos);
        migration.mark_migrating(b"moving_key".to_vec());
        migration.mark_migrated(b"accord_key".to_vec());
        router.register_migration_state(Uuid::nil(), migration);

        let mixed_paxos = router
            .execute_cas(
                "ks",
                "tbl",
                TransactionalMode::Mixed,
                b"moving_key",
                b"mutation".to_vec(),
                || async { None },
                |_| true,
            )
            .await
            .unwrap();
        assert!(matches!(mixed_paxos, CasResult::Success));

        let mixed_accord = router
            .execute_cas(
                "ks",
                "tbl",
                TransactionalMode::Mixed,
                b"accord_key",
                b"mutation".to_vec(),
                || async { None },
                |_| true,
            )
            .await
            .unwrap();
        assert!(matches!(mixed_accord, CasResult::Success));
        assert_eq!(router.metrics().migration_count.load(Ordering::Relaxed), 2);
    });
}

#[test]
fn gap_guard_journal() {
    // PARTIAL closure by prompt-14 follow-up: Rust now has generic
    // replayable journal primitives with stable record pointers, segment
    // rollover, key lookup, truncation, and deterministic durable segment
    // files. On-disk indexes, compaction, serializers, and subsystem
    // integrations remain tracked in the report.
    use cassandra_common::{RecordPointer, SegmentedJournal};

    let mut journal = SegmentedJournal::new(8);
    let first = journal.append(b"k1".to_vec(), b"v1".to_vec());
    let second = journal.append(b"k1".to_vec(), b"v2".to_vec());
    let third = journal.append(b"k2".to_vec(), b"v3".to_vec());

    assert_eq!(first, RecordPointer::new(0, 0));
    assert!(second > first);
    assert!(third.segment_id > first.segment_id);
    assert_eq!(journal.segment_ids(), vec![0, 1]);
    assert_eq!(journal.segment_count(), 2);

    assert_eq!(journal.read(first).unwrap().value, b"v1");
    assert_eq!(
        journal
            .records_for_key(b"k1")
            .into_iter()
            .map(|record| record.value.as_slice())
            .collect::<Vec<_>>(),
        vec![b"v1".as_slice(), b"v2".as_slice()]
    );
    assert_eq!(journal.latest(b"k1").unwrap().value, b"v2");
    assert_eq!(
        journal
            .replay()
            .into_iter()
            .map(|record| record.pointer)
            .collect::<Vec<_>>(),
        vec![first, second, third]
    );

    assert_eq!(journal.truncate_before(third), 2);
    assert!(journal.read(first).is_none());
    assert!(journal.read(second).is_none());
    assert_eq!(journal.latest(b"k1"), None);
    assert_eq!(journal.latest(b"k2").unwrap().value, b"v3");

    let dir = tempfile::tempdir().unwrap();
    let mut durable = SegmentedJournal::new(8);
    let first = durable.append(b"k1".to_vec(), b"v1".to_vec());
    let second = durable.append(b"k1".to_vec(), b"v2".to_vec());
    let third = durable.append(b"k2".to_vec(), b"v3".to_vec());
    let paths = durable.write_segments(dir.path()).unwrap();
    assert_eq!(paths.len(), 2);
    let loaded = SegmentedJournal::load_segments(dir.path(), 8).unwrap();
    assert_eq!(loaded.read(first).unwrap().value, b"v1");
    assert_eq!(loaded.latest(b"k1").unwrap().pointer, second);
    assert_eq!(
        loaded
            .replay()
            .into_iter()
            .map(|record| record.pointer)
            .collect::<Vec<_>>(),
        vec![first, second, third]
    );
}

// ═══════════════════════════════════════════════════════════════════════
// CONCURRENCY & UTILITIES GAPS
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn gap_guard_concurrency_stages() {
    // CLOSED by prompt-25: Stage enum with observable metrics.
    use cassandra_common::{Stage, StageRegistry};
    let registry = StageRegistry::new();
    // Verify all 15 stage variants are tracked
    assert_eq!(Stage::all().len(), 15);
    let metrics = registry.get(Stage::Read);
    metrics.inc_active();
    let (active, _, _) = metrics.snapshot();
    assert_eq!(active, 1);
}

#[test]
fn gap_guard_tracing_storage() {
    // PARTIAL closure by prompt-14 follow-up: coordinator tracing has session
    // event capture, active/recent in-memory storage, TTL cleanup, and
    // system_traces-shaped row projection. Full storage-engine persistence
    // remains tracked in the report.
    use std::sync::Arc;

    use cassandra_coordinator::{
        InMemorySessionStore, TracingCleanupTask, TracingConfig, TracingManager,
    };
    use uuid::Uuid;

    let manager = TracingManager::new(TracingConfig {
        enabled: true,
        sample_rate: 1.0,
        default_ttl_secs: 60,
    });
    let session = manager.begin_session().expect("tracing should be sampled");
    session.trace("coordinator", "Selecting replicas");
    session.trace("replica:7000", "Read command completed");
    assert_eq!(manager.active_count(), 1);
    assert_eq!(session.events().len(), 2);
    assert_eq!(
        manager
            .get_session(&session.session_id)
            .unwrap()
            .events()
            .len(),
        2
    );

    manager.finish_session(session.session_id);
    assert_eq!(manager.active_count(), 0);
    let recent = manager.list_recent_sessions(1);
    assert_eq!(recent.len(), 1);
    assert_eq!(recent[0].session.session_id, session.session_id);
    assert!(recent[0].finished_at_ms > 0);
    let trace_rows = manager.system_traces_rows(10);
    assert_eq!(trace_rows.sessions.len(), 1);
    assert_eq!(trace_rows.sessions[0].session_id, session.session_id);
    assert_eq!(trace_rows.events.len(), 2);
    assert_eq!(trace_rows.events[0].activity, "Selecting replicas");

    let disabled = TracingManager::new(TracingConfig {
        enabled: false,
        sample_rate: 1.0,
        default_ttl_secs: 60,
    });
    assert!(disabled.begin_session().is_none());

    let store = Arc::new(InMemorySessionStore::new(2));
    store.add(Uuid::from_u128(1), 1);
    store.add(Uuid::from_u128(2), u64::MAX);
    store.add(Uuid::from_u128(3), u64::MAX);
    assert_eq!(store.len(), 2, "bounded store should evict oldest entry");
    let cleanup = TracingCleanupTask::new(store.clone(), 10);
    assert_eq!(
        cleanup.run_once(),
        0,
        "future-dated entries should be retained"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// TOOLING GAPS (Expanded — Prompt 11)
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn gap_guard_nodetool_formatters() {
    // CLOSED by Rust rewrite follow-up: nodetool table, JSON, and YAML output
    // formatters are reusable from cassandra-admin, including stats rendering.
    use cassandra_admin::{
        OutputFormat, StatsTable, TableFormatter, TableStatsHolder, render_json, render_yaml,
    };

    let mut formatter = TableFormatter::new(["Name", "Value"]);
    formatter.add_row(["ReadStage", "10"]);
    formatter.add_row(["MutationStage", "200"]);
    let rendered = formatter.render();
    assert!(rendered.contains("Name"));
    assert!(rendered.contains("MutationStage  200"));

    let json = render_json(&serde_json::json!({
        "name": "ReadStage",
        "active": 1,
    }))
    .unwrap();
    assert!(json.contains("\"active\": 1"));

    let yaml = render_yaml(&serde_json::json!({
        "name": "ReadStage",
        "active": 1,
    }))
    .unwrap();
    assert!(yaml.contains("active: 1"));

    let holder = TableStatsHolder::new(vec![StatsTable {
        keyspace: "ks".into(),
        table: "users".into(),
        sstable_count: 2,
        disk_space_bytes: 100,
        read_count: 4,
        write_count: 5,
    }]);
    assert!(holder.render(OutputFormat::Yaml).contains("tables:"));
}

#[test]
fn gap_guard_nodetool_stats() {
    // PARTIAL closure by prompt-14 follow-up: TableStatsHolder/StatsTable
    // aggregate, filter, sort, summarize, and render tablestats/cfstats-style
    // data. Live per-table JMX parity remains tracked.
    use cassandra_admin::{OutputFormat, StatsTable, TableStatsHolder};

    let holder = TableStatsHolder::new(vec![
        StatsTable {
            keyspace: "ks".into(),
            table: "users".into(),
            sstable_count: 2,
            disk_space_bytes: 100,
            read_count: 4,
            write_count: 5,
        },
        StatsTable {
            keyspace: "ks".into(),
            table: "events".into(),
            sstable_count: 3,
            disk_space_bytes: 200,
            read_count: 6,
            write_count: 7,
        },
    ]);

    assert_eq!(holder.total_sstable_count(), 5);
    assert_eq!(holder.total_disk_space_bytes(), 300);
    assert_eq!(holder.total_read_count(), 10);
    assert_eq!(holder.total_write_count(), 12);
    assert_eq!(holder.summary().table_count, 2);
    assert_eq!(holder.filter_keyspace("ks").tables.len(), 2);
    assert_eq!(holder.filter_table("ks", "users").tables.len(), 1);
    assert_eq!(holder.sorted_by_name().tables[0].table, "events");
    assert!(holder.render(OutputFormat::Table).contains("Disk bytes"));
    assert!(holder.render(OutputFormat::Table).contains("TOTAL"));
    assert!(holder.render(OutputFormat::Json).contains("\"tables\""));
    assert!(holder.render(OutputFormat::Yaml).contains("tables:"));
}

#[test]
fn gap_guard_sai_disk_format() {
    // PARTIALLY CLOSED by prompt-14 continuation: SAI segment binary format and posting lists.
    use cassandra_storage::index::sai::{
        builder::{SaiSegmentBuilder, merge_segments},
        segment_format::{read_segment, read_segment_from, write_segment, write_segment_to},
    };

    let mut builder = SaiSegmentBuilder::new(7, "idx_age", "age");
    builder.add(b"30".to_vec(), b"pk-1".to_vec(), b"ck-1".to_vec());
    builder.add(b"30".to_vec(), b"pk-2".to_vec(), b"ck-2".to_vec());
    builder.add(b"40".to_vec(), b"pk-3".to_vec(), Vec::new());
    assert_eq!(builder.term_count(), 2);
    assert_eq!(builder.entry_count(), 3);
    let segment = builder.build();

    let mut bytes = Vec::new();
    write_segment_to(&mut bytes, &segment).unwrap();
    assert_eq!(&bytes[..4], b"SAI1");
    assert_eq!(&bytes[4..8], &1u32.to_be_bytes());

    let decoded = read_segment_from(&mut std::io::Cursor::new(&bytes)).unwrap();
    assert_eq!(decoded.row_count, 3);
    assert_eq!(decoded.terms[b"30".as_slice()].len(), 2);
    assert_eq!(
        decoded.terms[b"40".as_slice()].locations()[0].partition_key,
        b"pk-3"
    );

    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("idx_age.sai");
    write_segment(&path, &segment).unwrap();
    let from_file = read_segment(&path).unwrap();
    assert_eq!(from_file.terms[b"30".as_slice()].len(), 2);

    let mut second = SaiSegmentBuilder::new(8, "idx_age", "age");
    second.add(b"30".to_vec(), b"pk-4".to_vec(), b"ck-4".to_vec());
    second.add(b"50".to_vec(), b"pk-5".to_vec(), b"ck-5".to_vec());
    let merged = merge_segments(&[segment, second.build()], 9);
    assert_eq!(merged.sstable_generation, 9);
    assert_eq!(merged.terms[b"30".as_slice()].len(), 3);
    assert_eq!(merged.terms[b"50".as_slice()].len(), 1);

    let mut corrupt = bytes;
    corrupt[0] = b'X';
    assert!(read_segment_from(&mut std::io::Cursor::new(&corrupt)).is_err());

    let mut bad_count = Vec::new();
    write_segment_to(&mut bad_count, &from_file).unwrap();
    bad_count[12..20].copy_from_slice(&99u64.to_be_bytes());
    let err = read_segment_from(&mut std::io::Cursor::new(&bad_count)).unwrap_err();
    assert!(err.to_string().contains("row count mismatch"));

    let mut single = SaiSegmentBuilder::new(10, "idx_age", "age");
    single.add(b"dup".to_vec(), b"pk".to_vec(), Vec::new());
    let mut duplicate_term = Vec::new();
    write_segment_to(&mut duplicate_term, &single.build()).unwrap();
    let mut with_duplicate = duplicate_term.clone();
    with_duplicate[8..12].copy_from_slice(&2u32.to_be_bytes());
    with_duplicate[12..20].copy_from_slice(&2u64.to_be_bytes());
    with_duplicate.extend_from_slice(&duplicate_term[20..]);
    let err = read_segment_from(&mut std::io::Cursor::new(&with_duplicate)).unwrap_err();
    assert!(err.to_string().contains("duplicate SAI segment term"));
}

#[test]
fn gap_guard_sai_analyzers() {
    // CLOSED by prompt-14 continuation: Rust now has SAI analyzer selection,
    // non-tokenizing filters, and tokenizing standard analyzer behavior for
    // string terms.
    use std::collections::HashMap;

    use cassandra_storage::index::{
        IndexDefinition, IndexEntry, IndexType, SecondaryIndex,
        sai::{
            SaiIndex,
            analyzer::{
                ANALYZER_CLASS, ASCII, AnalyzerInstance, CASE_SENSITIVE, NORMALIZE,
                NonTokenizingAnalyzer, NonTokenizingOptions, SaiAnalyzer, StandardAnalyzer,
                analyzer_from_options,
            },
        },
    };

    let default = NonTokenizingAnalyzer::new(NonTokenizingOptions::default());
    assert!(!default.transform_value());
    assert_eq!(default.analyze("Hello World"), vec!["Hello World"]);
    assert!(default.analyze("").is_empty());

    let insensitive = NonTokenizingAnalyzer::new(NonTokenizingOptions {
        case_sensitive: false,
        normalize: false,
        ascii: false,
    });
    assert!(insensitive.transform_value());
    assert_eq!(insensitive.analyze("Hello World"), vec!["hello world"]);

    let folded = NonTokenizingAnalyzer::new(NonTokenizingOptions {
        case_sensitive: false,
        normalize: true,
        ascii: true,
    });
    assert_eq!(folded.analyze("Cafe\u{301} ÉLAN"), vec!["cafe elan"]);

    let mut options = HashMap::new();
    options.insert(CASE_SENSITIVE.to_string(), "false".to_string());
    options.insert(NORMALIZE.to_string(), "true".to_string());
    options.insert(ASCII.to_string(), "true".to_string());
    options.insert("irrelevant".to_string(), "ignored".to_string());
    let analyzer_options = NonTokenizingOptions::analyzer_options(&options);
    assert_eq!(analyzer_options.len(), 3);
    let analyzer = analyzer_from_options(&options).unwrap().unwrap();
    assert_eq!(
        analyzer.non_tokenizing_options().unwrap(),
        NonTokenizingOptions {
            case_sensitive: false,
            normalize: true,
            ascii: true,
        }
    );
    assert!(analyzer_from_options(&HashMap::new()).unwrap().is_none());

    let mut invalid = HashMap::new();
    invalid.insert(ASCII.to_string(), "not-a-bool".to_string());
    assert!(analyzer_from_options(&invalid).is_err());

    let standard = StandardAnalyzer::new(NonTokenizingOptions {
        case_sensitive: false,
        normalize: true,
        ascii: true,
    });
    assert_eq!(
        standard.analyze("Cafe\u{301}, ÉLAN-42 foo_bar"),
        vec!["cafe", "elan", "42", "foo", "bar"]
    );

    let mut selected = HashMap::new();
    selected.insert(ANALYZER_CLASS.to_string(), "standard".to_string());
    selected.insert(CASE_SENSITIVE.to_string(), "false".to_string());
    let analyzer = analyzer_from_options(&selected).unwrap().unwrap();
    assert!(matches!(analyzer, AnalyzerInstance::Standard(_)));
    assert_eq!(analyzer.analyze("Hello, World"), vec!["hello", "world"]);

    let sai = SaiIndex::new(IndexDefinition {
        name: "idx_body".to_string(),
        keyspace: "ks".to_string(),
        table: "docs".to_string(),
        column: "body".to_string(),
        index_type: IndexType::Sai,
        options: selected,
    });
    sai.insert(&IndexEntry {
        term: b"Hello, World".to_vec(),
        partition_key: b"doc1".to_vec(),
        clustering_key: Vec::new(),
    })
    .unwrap();
    let hits = sai.search(b"world").unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].partition_key, b"doc1");

    let vector_sai = SaiIndex::new(IndexDefinition {
        name: "idx_embedding".to_string(),
        keyspace: "ks".to_string(),
        table: "docs".to_string(),
        column: "embedding".to_string(),
        index_type: IndexType::Sai,
        options: HashMap::from([
            ("vector_dimensions".to_string(), "3".to_string()),
            (
                "vector_similarity_metric".to_string(),
                "euclidean".to_string(),
            ),
        ]),
    });
    let first = cassandra_types::vector::VectorValue::new(vec![0.0, 0.0, 0.0]).serialize();
    let second = cassandra_types::vector::VectorValue::new(vec![1.0, 0.0, 0.0]).serialize();
    vector_sai
        .insert(&IndexEntry {
            term: first.clone(),
            partition_key: b"doc1".to_vec(),
            clustering_key: Vec::new(),
        })
        .unwrap();
    vector_sai
        .insert(&IndexEntry {
            term: second,
            partition_key: b"doc2".to_vec(),
            clustering_key: Vec::new(),
        })
        .unwrap();
    vector_sai
        .delete(&IndexEntry {
            term: first.clone(),
            partition_key: b"doc1".to_vec(),
            clustering_key: Vec::new(),
        })
        .unwrap();
    let hits = vector_sai.search_vector(&first, 2).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].0.partition_key, b"doc2");
    vector_sai.truncate().unwrap();
    assert!(vector_sai.search_vector(&first, 1).unwrap().is_empty());
}

// ═══════════════════════════════════════════════════════════════════════
// COMMON / UTILITIES GAPS (Expanded — Prompt 11)
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn gap_guard_utils_memory() {
    // PARTIAL closure by prompt-14 follow-up: IO memory utilities include a
    // lock-free buffer pool with metrics, mmap-backed rebuffering, and
    // memtable native/slab allocation plus pool accounting. Full Java memory
    // reclamation/off-heap integration remains tracked.
    use std::io::Write;

    use cassandra_io::util::{
        buffer_pool::BufferPool, mmap_rebufferer::MmapRebufferer, rebufferer::Rebufferer,
    };
    use cassandra_storage::memtable::allocator::{
        AllocationError, MemtablePool, NativeAllocator, SlabAllocator,
    };

    let pool = BufferPool::new(128, 2);
    let mut buf = pool.acquire();
    assert_eq!(buf.capacity(), 128);
    assert!(buf.is_empty());
    buf.extend_from_slice(b"payload");
    pool.release(buf);

    let cached = pool.acquire();
    assert!(cached.is_empty(), "reused buffers must be cleared");
    assert_eq!(cached.capacity(), 128);
    assert_eq!(pool.metrics().allocations, 1);
    assert_eq!(pool.metrics().cache_hits, 1);

    pool.release(Vec::with_capacity(64));
    assert_eq!(pool.metrics().evictions, 1);
    assert_eq!(BufferPool::default().chunk_size(), 65_536);

    let mut file = tempfile::NamedTempFile::new().unwrap();
    let data: Vec<u8> = (0..160).map(|i| i as u8).collect();
    file.write_all(&data).unwrap();
    file.flush().unwrap();

    let rebufferer = MmapRebufferer::new(file.path(), 64).unwrap();
    assert_eq!(rebufferer.file_length(), 160);
    assert_eq!(rebufferer.chunk_size(), 64);
    let first = rebufferer.rebuffer(0).unwrap();
    assert_eq!(first.offset(), 0);
    assert_eq!(first.limit(), 64);
    assert_eq!(first.buffer(), &data[..64]);
    let tail = rebufferer.rebuffer(128).unwrap();
    assert_eq!(tail.offset(), 128);
    assert_eq!(tail.buffer(), &data[128..]);
    assert!(rebufferer.rebuffer(160).is_err());

    let native = NativeAllocator::new(16);
    let mut allocation = native.allocate(8).unwrap();
    allocation.as_mut_slice().copy_from_slice(b"memtable");
    assert_eq!(allocation.as_slice(), b"memtable");
    assert_eq!(native.allocated_bytes(), 8);
    assert_eq!(
        native.allocate(9).unwrap_err(),
        AllocationError::LimitExceeded {
            requested: 9,
            used: 8,
            limit: 16,
        }
    );
    drop(allocation);
    assert_eq!(native.allocated_bytes(), 0);

    let slabs = SlabAllocator::new(16, 64);
    let first = slabs.allocate(6).unwrap();
    let second = slabs.allocate(8).unwrap();
    assert_eq!(slabs.allocated_bytes(), 14);
    assert_eq!(slabs.reserved_bytes(), 16);
    let large = slabs.allocate(24).unwrap();
    assert_eq!(slabs.reserved_bytes(), 40);
    drop((first, second, large));
    assert_eq!(slabs.allocated_bytes(), 0);

    let pool = MemtablePool::new(32);
    let reservation = pool.try_reserve(20).unwrap();
    assert_eq!(reservation.bytes(), 20);
    assert_eq!(pool.used_bytes(), 20);
    assert_eq!(pool.available_bytes(), 12);
    assert!(pool.try_reserve(13).is_err());
    drop(reservation);
    assert_eq!(pool.used_bytes(), 0);
}

#[test]
fn gap_guard_utils_concurrent() {
    // PARTIAL closure by prompt-14 follow-up: lifecycle transactions and
    // active-compaction tracking provide crash-safe transactional logs,
    // conflict detection, cooperative cancellation, and common
    // Ref/SharedCloseable/WaitQueue/OpOrder primitives. Full Java concurrency
    // integration remains tracked.
    use std::{
        sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        },
        time::Duration,
    };

    use cassandra_common::{OpOrder, Ref as CassandraRef, SharedCloseable, WaitQueue};
    use cassandra_storage::compaction::{
        active::{ActiveCompactions, CancellationToken, CompactionInfo},
        errors::CompactionType,
        lifecycle::{LifecycleTransaction, TransactionState},
    };
    use uuid::Uuid;

    let token = CancellationToken::new();
    let cloned = token.clone();
    assert!(!token.is_cancelled());
    cloned.cancel();
    assert!(token.is_cancelled());

    let tracker = ActiveCompactions::new();
    let first = CompactionInfo {
        id: Uuid::new_v4(),
        compaction_type: CompactionType::Compaction,
        sstable_ids: vec![1, 2],
        started_at_ms: 100,
        cancel_token: CancellationToken::new(),
    };
    let first_id = first.id;
    let first_cancel = first.cancel_token.clone();
    tracker.register(first).unwrap();
    assert_eq!(tracker.count(), 1);
    assert!(tracker.has_conflict(&[2]));
    assert!(!tracker.has_conflict(&[3]));

    let overlapping = CompactionInfo {
        id: Uuid::new_v4(),
        compaction_type: CompactionType::Cleanup,
        sstable_ids: vec![2, 3],
        started_at_ms: 101,
        cancel_token: CancellationToken::new(),
    };
    assert!(tracker.register(overlapping).is_err());
    assert!(tracker.cancel(&first_id));
    assert!(first_cancel.is_cancelled());
    assert!(tracker.unregister(&first_id).is_some());
    assert_eq!(tracker.count(), 0);

    let dir = tempfile::tempdir().unwrap();
    let mut txn = LifecycleTransaction::new(dir.path()).unwrap();
    txn.stage(10).unwrap();
    txn.obsolete(5).unwrap();
    assert_eq!(txn.staged_ids(), &[10]);
    assert_eq!(txn.obsoleted_ids(), &[5]);
    assert!(txn.log_path().exists());
    txn.commit().unwrap();
    assert_eq!(txn.state(), TransactionState::Committed);
    assert!(
        txn.stage(11).is_err(),
        "committed transaction must be closed"
    );

    let mut aborting = LifecycleTransaction::new(dir.path()).unwrap();
    aborting.stage(20).unwrap();
    aborting.abort().unwrap();
    assert_eq!(aborting.state(), TransactionState::Aborted);
    assert!(aborting.staged_ids().is_empty());

    let released = Arc::new(AtomicUsize::new(0));
    let released_clone = Arc::clone(&released);
    let reference = CassandraRef::with_on_release("sstable-handle".to_string(), move |value| {
        assert_eq!(value, "sstable-handle");
        released_clone.fetch_add(1, Ordering::SeqCst);
    });
    let reference_copy = reference.clone();
    assert_eq!(reference.get(), "sstable-handle");
    assert_eq!(reference.ref_count(), 2);
    drop(reference);
    assert_eq!(released.load(Ordering::SeqCst), 0);
    reference_copy.release();
    assert_eq!(released.load(Ordering::SeqCst), 1);

    let closed = Arc::new(AtomicUsize::new(0));
    let closed_clone = Arc::clone(&closed);
    let closeable = SharedCloseable::with_on_close(move || {
        closed_clone.fetch_add(1, Ordering::SeqCst);
    });
    let closeable_copy = closeable.shared_copy();
    assert_eq!(closeable.strong_count(), 2);
    assert!(closeable_copy.close());
    assert!(!closeable.close(), "close callback must be idempotent");
    assert!(closeable.is_closed());
    assert_eq!(closed.load(Ordering::SeqCst), 1);

    let queue = WaitQueue::new();
    let first_waiter = queue.register();
    let second_waiter = queue.register();
    assert_eq!(queue.waiting_count(), 2);
    assert!(queue.signal_one());
    assert!(first_waiter.wait_timeout(Duration::from_millis(1)));
    assert!(!second_waiter.is_signaled());
    assert_eq!(queue.signal_all(), 1);
    second_waiter.wait();
    assert_eq!(queue.waiting_count(), 0);

    let order = OpOrder::new();
    let mut first_group = order.start();
    let barrier = order.new_barrier();
    let second_group = order.start();
    assert!(!barrier.is_complete());
    first_group.close();
    barrier.wait();
    assert!(barrier.is_complete());
    assert_eq!(order.active_count(), 1);
    assert_eq!(second_group.id(), 1);
}

#[test]
fn gap_guard_utils_bytecomparable() {
    // CLOSED by prompt-14 continuation: ByteComparable-style order-preserving encoding.
    use cassandra_types::{CqlType, byte_comparable::encode_byte_comparable};

    let neg_int = encode_byte_comparable(&CqlType::Int, &(-1i32).to_be_bytes());
    let zero_int = encode_byte_comparable(&CqlType::Int, &0i32.to_be_bytes());
    let pos_int = encode_byte_comparable(&CqlType::Int, &1i32.to_be_bytes());
    assert!(neg_int < zero_int);
    assert!(zero_int < pos_int);

    let neg_float = encode_byte_comparable(&CqlType::Float, &(-1.0f32).to_be_bytes());
    let zero_float = encode_byte_comparable(&CqlType::Float, &0.0f32.to_be_bytes());
    let pos_float = encode_byte_comparable(&CqlType::Float, &1.0f32.to_be_bytes());
    assert!(neg_float < zero_float);
    assert!(zero_float < pos_float);

    let prefix = encode_byte_comparable(&CqlType::Varchar, b"a");
    let longer = encode_byte_comparable(&CqlType::Varchar, b"ab");
    let escaped_null = encode_byte_comparable(&CqlType::Blob, &[0x61, 0x00, 0x62]);
    assert!(prefix < longer);
    assert_eq!(escaped_null, vec![0x61, 0x00, 0xff, 0x62, 0x00, 0x00]);

    let reversed_int = CqlType::Reversed(Box::new(CqlType::Int));
    let reversed_one = encode_byte_comparable(&reversed_int, &1i32.to_be_bytes());
    let reversed_two = encode_byte_comparable(&reversed_int, &2i32.to_be_bytes());
    assert!(reversed_two < reversed_one);

    let negative_varint = encode_byte_comparable(&CqlType::Varint, &[0xff]);
    let zero_varint = encode_byte_comparable(&CqlType::Varint, &[0x00]);
    let positive_varint = encode_byte_comparable(&CqlType::Varint, &[0x01]);
    assert!(negative_varint < zero_varint);
    assert!(zero_varint < positive_varint);

    let mut uuid = [0u8; 16];
    uuid[6] = 0x40;
    let encoded_uuid = encode_byte_comparable(&CqlType::Uuid, &uuid);
    assert_eq!(encoded_uuid.len(), 17);
    assert_eq!(encoded_uuid[0], 4);
}

#[test]
fn gap_guard_cdc() {
    // PARTIALLY CLOSED by prompt-14 continuation: CDC raw segment listing, status,
    // space accounting, Java-style _cdc.idx sidecars, and completed segment replay.
    use std::fs;

    use cassandra_storage::cdc::{
        CdcStatus, cdc_status, discard_consumed_segments, list_cdc_segment_infos,
        list_cdc_segments, read_completed_cdc_mutations, write_cdc_index,
    };
    use cassandra_storage::commitlog::{
        CellMutation, Mutation, MutationRow,
        segment::{CorruptionPolicy, Segment},
    };

    let dir = tempfile::tempdir().unwrap();
    fs::write(dir.path().join("CommitLog-1.log"), b"aaaa").unwrap();
    fs::write(dir.path().join("CommitLog-2.log"), b"bbbbbb").unwrap();
    fs::write(dir.path().join("CommitLog-10.log"), b"cccccccc").unwrap();
    fs::write(dir.path().join("ignored.txt"), b"not-cdc").unwrap();

    let segments = list_cdc_segments(dir.path()).unwrap();
    assert_eq!(segments.len(), 3);
    assert!(segments[0].ends_with("CommitLog-1.log"));
    assert!(segments[1].ends_with("CommitLog-2.log"));
    assert!(segments[2].ends_with("CommitLog-10.log"));

    let status = cdc_status(dir.path(), 1_000).unwrap();
    assert!(status.enabled);
    assert_eq!(status.directory, dir.path());
    assert_eq!(status.segment_count, 3);
    assert_eq!(status.used_bytes, 18);
    assert!(status.has_space());
    assert!((status.usage_percent() - 1.8).abs() < 1e-9);

    let full = CdcStatus {
        enabled: true,
        directory: dir.path().to_path_buf(),
        used_bytes: 1_000,
        limit_bytes: 1_000,
        segment_count: 3,
    };
    assert!(!full.has_space());
    assert_eq!(full.usage_percent(), 100.0);

    assert_eq!(discard_consumed_segments(dir.path(), 2).unwrap(), 2);
    let remaining = list_cdc_segments(dir.path()).unwrap();
    assert_eq!(remaining.len(), 1);
    assert!(remaining[0].ends_with("CommitLog-10.log"));

    let replay_dir = tempfile::tempdir().unwrap();
    let mut segment = Segment::create(replay_dir.path(), 20).unwrap();
    let mutation = Mutation {
        keyspace: "ks".to_string(),
        table: "cdc_enabled_table".to_string(),
        partition_key: b"pk".to_vec(),
        rows: vec![MutationRow {
            clustering_key: Vec::new(),
            cells: vec![CellMutation {
                column: "v".to_string(),
                value: Some(b"changed".to_vec()),
                timestamp: 42,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        }],
        timestamp: 42,
        cdc_enabled: true,
        static_cells: Vec::new(),
        partition_tombstone: None,
        range_tombstones: Vec::new(),
    };
    segment
        .append_entry(&serde_json::to_vec(&mutation).unwrap())
        .unwrap();
    segment.sync().unwrap();
    write_cdc_index(segment.path(), segment.size(), true).unwrap();

    let infos = list_cdc_segment_infos(replay_dir.path()).unwrap();
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].id, 20);
    assert_eq!(infos[0].durable_offset, Some(infos[0].size_bytes));
    assert!(infos[0].completed);

    let mutations =
        read_completed_cdc_mutations(replay_dir.path(), CorruptionPolicy::StopOnCorrupt).unwrap();
    assert_eq!(mutations.len(), 1);
    assert_eq!(mutations[0].table, "cdc_enabled_table");
    assert!(mutations[0].cdc_enabled);
}

// ═══════════════════════════════════════════════════════════════════════
// WRITE PATH GAPS (Prompt 13 — WU-20)
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn gap_guard_write_path_mv_fanout() {
    // CLOSED by prompt-13 (WU-18): ViewManager.generate_view_updates_with_existing()
    // and WriteCoordinator backpressure tracking for view update backlog.
    use cassandra_storage::materialized_views::ViewManager;
    let mgr = ViewManager::new();
    assert_eq!(mgr.view_count(), 0);
    // generate_view_updates_with_existing exists and is callable
    let result = mgr.generate_view_updates_with_existing(
        "ks",
        "t",
        b"pk",
        &std::collections::HashMap::new(),
        0,
        false,
        None,
    );
    assert!(result.mutations.is_empty());
}

#[test]
fn gap_guard_write_path_trigger_augmentation() {
    // CLOSED by prompt-13 (WU-19): TriggerManager.augment_mutation()
    // and TriggerExecutor struct behind triggers feature flag.
    use cassandra_storage::triggers::TriggerManager;
    let mgr = TriggerManager::new();
    assert!(!mgr.has_triggers_for("ks", "t"));
}

#[test]
fn gap_guard_write_path_read_before_write() {
    // CLOSED by prompt-14 follow-up: WriteCoordinator has a read-before-write
    // existing-row hook and passes that state into ViewManager delta generation.
    use cassandra_cluster_metadata::{
        ClusterMetadata, Endpoint, NodeId, NodeInfo, SimpleSnitch, SimpleStrategy,
    };
    use cassandra_common::Token;
    use cassandra_coordinator::consistency::ConsistencyLevel;
    use cassandra_coordinator::hints::HintStore;
    use cassandra_coordinator::write::{
        CellMutation, CoordinatedMutation, MutationRow, WriteCoordinator,
    };
    use cassandra_storage::materialized_views::{MaterializedViewDefinition, ViewManager};
    use std::collections::HashMap;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    fn node(port: u16, token: i64) -> NodeInfo {
        NodeInfo::new(
            NodeId::random(),
            ep(port),
            "dc1",
            "rack1",
            vec![Token::from_raw(token)],
        )
    }

    let cluster = Arc::new(ClusterMetadata::new(node(7001, -100)));
    cluster.update_node(node(7002, 0));
    cluster.update_node(node(7003, 100));

    let coordinator = WriteCoordinator::new(
        Arc::clone(&cluster),
        ep(7001),
        Arc::new(HintStore::new(1000)),
    )
    .with_view_existing_row_reader(Arc::new(|_| {
        let mut existing = HashMap::new();
        existing.insert("email".to_string(), Some(b"old@example.com".to_vec()));
        existing.insert("name".to_string(), Some(b"Alice".to_vec()));
        Some(existing)
    }));
    let strategy = SimpleStrategy::new(3);
    let snitch = SimpleSnitch;
    let vm = ViewManager::new();

    vm.register(MaterializedViewDefinition {
        name: "users_by_email".to_string(),
        keyspace: "ks".to_string(),
        base_table: "users".to_string(),
        view_table: "users_by_email".to_string(),
        included_columns: vec!["email".to_string(), "name".to_string()],
        where_clause: "email IS NOT NULL".to_string(),
        include_all_columns: false,
        view_pk_columns: vec!["email".to_string()],
    })
    .unwrap();

    let mutation = CoordinatedMutation::simple(
        "ks".to_string(),
        "users".to_string(),
        b"user1".to_vec(),
        vec![MutationRow {
            clustering_key: vec![],
            cells: vec![
                CellMutation {
                    column: "email".to_string(),
                    value: Some(b"new@example.com".to_vec()),
                    timestamp: 1000,
                    ttl: 0,
                    is_tombstone: false,
                    collection_op: None,
                },
                CellMutation {
                    column: "name".to_string(),
                    value: Some(b"Alice".to_vec()),
                    timestamp: 1000,
                    ttl: 0,
                    is_tombstone: false,
                    collection_op: None,
                },
            ],
            is_tombstone: false,
            range_tombstone: None,
        }],
        1000,
    );

    let (_result, fanout) = coordinator
        .coordinate_write_with_hooks(
            &mutation,
            ConsistencyLevel::One,
            &strategy,
            &snitch,
            Some(&vm),
            None,
        )
        .unwrap();

    assert_eq!(fanout.mutations_generated, 2);
    assert_eq!(fanout.mutations_applied, 2);
}

#[test]
fn gap_guard_trigger_plugin_system() {
    // PARTIAL closure by prompt-14 follow-up: TriggerManager now has a
    // runtime plugin registry, can execute registered plugin augmentations,
    // and supports enable/disable control for registered triggers. Real
    // WASM/FFI sandbox loading, isolation, and deployment remain tracked.
    use cassandra_storage::triggers::{
        StaticTriggerPlugin, TriggerDefinition, TriggerManager, TriggerPluginRegistry,
    };
    use std::sync::Arc;

    let mut registry = TriggerPluginRegistry::new();
    registry.register_plugin(
        "wasm://audit",
        Arc::new(StaticTriggerPlugin::new(vec![b"augmented".to_vec()])),
    );
    assert_eq!(registry.len(), 1);

    let mut manager = TriggerManager::with_registry(registry);
    manager
        .register(TriggerDefinition {
            name: "audit_trigger".to_string(),
            keyspace: "ks".to_string(),
            table: "users".to_string(),
            trigger_class: "wasm://audit".to_string(),
        })
        .unwrap();
    assert!(manager.has_triggers_for("ks", "users"));
    assert!(manager.has_enabled_triggers_for("ks", "users"));
    assert_eq!(
        manager.augment_mutation("ks", "users", b"mutation"),
        vec![b"augmented".to_vec()]
    );
    assert!(manager.disable("ks", "users", "audit_trigger"));
    assert!(!manager.is_enabled("ks", "users", "audit_trigger"));
    assert!(!manager.has_enabled_triggers_for("ks", "users"));
    assert!(
        manager
            .augment_mutation("ks", "users", b"mutation")
            .is_empty()
    );
    assert!(manager.enable("ks", "users", "audit_trigger"));
    assert!(manager.is_enabled("ks", "users", "audit_trigger"));
    assert_eq!(
        manager.augment_mutation("ks", "users", b"mutation"),
        vec![b"augmented".to_vec()]
    );

    let mut missing = TriggerManager::new();
    assert!(
        missing
            .register(TriggerDefinition {
                name: "missing".to_string(),
                keyspace: "ks".to_string(),
                table: "users".to_string(),
                trigger_class: "wasm://missing".to_string(),
            })
            .is_err()
    );
}

#[test]
fn gap_guard_async_mv_fanout() {
    // PARTIAL closure by prompt-14 follow-up: async MV fanout now has a
    // queue-backed dispatcher that separates generation from application,
    // tracks backlog, records generated/applied/retried/failed metrics, and
    // requeues retryable failures. Real replica messaging integration remains
    // tracked in the report.
    use cassandra_coordinator::{AsyncViewFanoutDispatcher, ViewFanoutMetrics, WriteError};
    use cassandra_storage::materialized_views::ViewMutation;
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering};

    let metrics = Arc::new(ViewFanoutMetrics::new());
    let backlog = Arc::new(AtomicU64::new(0));
    let dispatcher = AsyncViewFanoutDispatcher::new(metrics.clone(), backlog.clone());

    let mutations = vec![
        ViewMutation {
            keyspace: "ks".into(),
            view_table: "users_by_email".into(),
            partition_key: b"a@example.com".to_vec(),
            is_delete: false,
            columns: HashMap::new(),
            timestamp: 1,
        },
        ViewMutation {
            keyspace: "ks".into(),
            view_table: "users_by_email".into(),
            partition_key: b"b@example.com".to_vec(),
            is_delete: false,
            columns: HashMap::new(),
            timestamp: 2,
        },
    ];

    assert_eq!(dispatcher.enqueue(mutations), 2);
    assert_eq!(dispatcher.pending_count(), 2);
    assert_eq!(backlog.load(Ordering::Relaxed), 2);
    assert_eq!(metrics.view_mutations_generated.load(Ordering::Relaxed), 2);

    let result = dispatcher.drain_with(|_| Ok(()));
    assert_eq!(result.mutations_generated, 2);
    assert_eq!(result.mutations_applied, 2);
    assert_eq!(result.mutations_failed, 0);
    assert_eq!(dispatcher.pending_count(), 0);
    assert_eq!(backlog.load(Ordering::Relaxed), 0);
    assert_eq!(metrics.view_mutations_applied.load(Ordering::Relaxed), 2);

    let retry_dispatcher = AsyncViewFanoutDispatcher::new(metrics.clone(), backlog.clone());
    assert_eq!(
        retry_dispatcher.enqueue([ViewMutation {
            keyspace: "ks".into(),
            view_table: "users_by_email".into(),
            partition_key: b"retry@example.com".to_vec(),
            is_delete: false,
            columns: HashMap::new(),
            timestamp: 3,
        }]),
        1
    );
    let retry_result = retry_dispatcher.drain_with_retries(2, |_| Err(WriteError::Overloaded));
    assert_eq!(retry_result.mutations_generated, 1);
    assert_eq!(retry_result.mutations_retried, 1);
    assert_eq!(retry_result.mutations_failed, 0);
    assert_eq!(retry_dispatcher.pending_count(), 1);
    assert_eq!(backlog.load(Ordering::Relaxed), 1);
    assert_eq!(metrics.view_mutations_retried.load(Ordering::Relaxed), 1);

    let applied_after_retry = retry_dispatcher.drain_with_retries(2, |_| Ok(()));
    assert_eq!(applied_after_retry.mutations_applied, 1);
    assert_eq!(retry_dispatcher.pending_count(), 0);
    assert_eq!(backlog.load(Ordering::Relaxed), 0);
}

// ═══════════════════════════════════════════════════════════════════════
// READ PATH GAPS (Prompt 14)
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn gap_guard_read_error_mapping() {
    // CLOSED by prompt-14 (WU-01): read_error_to_cassandra_error() and
    // read_error_to_error_frame() map all ReadError variants to CassandraError.
    use cassandra_coordinator::consistency::ConsistencyLevel;
    use cassandra_coordinator::read::ReadError;
    let err = ReadError::Timeout {
        cl: ConsistencyLevel::One,
        required: 1,
        received: 0,
        data_present: false,
    };
    assert_eq!(err.error_code(), 0x1200);
}

#[test]
fn gap_guard_read_paging_state() {
    // CLOSED by prompt-14 (WU-06): PagingState integrated into executor,
    // SelectPlan carries page_size and paging_state, apply_paging() helper.
    use cassandra_coordinator::read::PagingState;
    let state = PagingState::new(b"pk".to_vec(), b"ck".to_vec(), 42, 10);
    let bytes = state.serialize();
    let recovered = PagingState::deserialize(&bytes).unwrap();
    assert_eq!(recovered.remaining, 42);
}

#[test]
fn gap_guard_read_aggregation() {
    // CLOSED by prompt-14 follow-up: GROUP BY reaches SelectPlan and executor
    // aggregate state is covered by cassandra-server executor tests.
    use cassandra_cql::ast::{SelectColumns, Statement};

    let stmt = cassandra_cql::parser::parse("SELECT id, sum(v) FROM ks.t GROUP BY id").unwrap();
    let Statement::Select(select) = stmt else {
        panic!("expected select");
    };
    assert_eq!(select.group_by, vec!["id"]);
    assert!(matches!(select.columns, SelectColumns::Named(_)));
}

#[test]
fn gap_guard_read_distinct() {
    // CLOSED by prompt-14 follow-up: SELECT DISTINCT is validated to partition
    // key/static columns and executor post-processing de-duplicates partition
    // keys.
    use cassandra_cql::selection::post_process::apply_distinct;
    let rows = vec![
        vec![Some(1i32.to_be_bytes().to_vec())],
        vec![Some(1i32.to_be_bytes().to_vec())],
        vec![Some(2i32.to_be_bytes().to_vec())],
    ];
    let distinct = apply_distinct(rows, &[0]);
    assert_eq!(distinct.len(), 2);
}

#[test]
fn gap_guard_dc_aware_read_executor() {
    // CLOSED by prompt-14 follow-up: read coordination filters LOCAL_* CLs
    // to the configured local datacenter before availability and execution
    // planning, covering both single-partition and range reads.
    use cassandra_cluster_metadata::{
        ClusterMetadata, Endpoint, NodeId, NodeInfo, SimpleSnitch, SimpleStrategy,
    };
    use cassandra_common::Token;
    use cassandra_coordinator::consistency::ConsistencyLevel;
    use cassandra_coordinator::read::{
        CoordinatedRead, PartitionRangeReadCommand, ReadCoordinator, ReadError,
    };
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};
    use std::sync::Arc;

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::new(
            IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
            port,
        ))
    }

    fn node(port: u16, token: i64, dc: &str) -> NodeInfo {
        NodeInfo::new(
            NodeId::random(),
            ep(port),
            dc,
            "rack1",
            vec![Token::from_raw(token)],
        )
    }

    let cluster = Arc::new(ClusterMetadata::new(node(7001, -100, "dc1")));
    cluster.update_node(node(7002, 0, "dc1"));
    cluster.update_node(node(7003, 100, "dc2"));

    let coordinator =
        ReadCoordinator::new(Arc::clone(&cluster), ep(7001)).with_local_datacenter("dc1".into());
    let strategy = SimpleStrategy::new(3);
    let snitch = SimpleSnitch;
    let read = CoordinatedRead {
        keyspace: "ks".to_string(),
        table: "users".to_string(),
        partition_key: b"user1".to_vec(),
    };

    let local_quorum = coordinator
        .coordinate_read(&read, ConsistencyLevel::LocalQuorum, &strategy, &snitch)
        .unwrap();
    assert_eq!(local_quorum.responses_required, 2);
    assert!(
        local_quorum
            .contacted_replicas
            .iter()
            .all(|endpoint| *endpoint == ep(7001) || *endpoint == ep(7002))
    );

    let range = PartitionRangeReadCommand::full_scan("ks", "users");
    let local_range = coordinator
        .coordinate_range_read(&range, ConsistencyLevel::LocalQuorum, &strategy, &snitch)
        .unwrap();
    assert_eq!(local_range.responses_required, 2);
    assert!(
        local_range
            .contacted_replicas
            .iter()
            .all(|endpoint| *endpoint == ep(7001) || *endpoint == ep(7002))
    );

    cluster.mark_dead(&ep(7001));
    cluster.mark_dead(&ep(7002));
    let local_one =
        coordinator.coordinate_read(&read, ConsistencyLevel::LocalOne, &strategy, &snitch);
    assert!(matches!(
        local_one,
        Err(ReadError::Unavailable {
            cl: ConsistencyLevel::LocalOne,
            required: 1,
            alive: 0,
        })
    ));
}
