// Licensed under Apache License, Version 2.0.

//! Schema comparison between Java and Rust Cassandra catalogs.
//!
//! Parses CQL `DESCRIBE SCHEMA` output from Java and compares it
//! against the target Rust schema to identify differences that
//! could cause migration issues.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.schema.SchemaKeyspace`
//! - `org.apache.cassandra.cql3.statements.schema.*`

use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

// ─── Schema Representation ────────────────────────────────────────────────

/// Simplified schema representation for comparison purposes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaSnapshot {
    /// Source identifier (e.g., "java-4.1.3" or "rust-0.1.0").
    pub source: String,
    /// Keyspaces and their tables.
    pub keyspaces: BTreeMap<String, KeyspaceSchema>,
    /// User-defined types.
    pub types: Vec<TypeSchema>,
    /// User-defined functions.
    pub functions: Vec<FunctionSchema>,
    /// User-defined aggregates.
    pub aggregates: Vec<AggregateSchema>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyspaceSchema {
    pub name: String,
    pub replication: BTreeMap<String, String>,
    pub durable_writes: bool,
    pub tables: BTreeMap<String, TableSchema>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableSchema {
    pub name: String,
    pub columns: Vec<ColumnSchema>,
    pub partition_key: Vec<String>,
    pub clustering_key: Vec<String>,
    pub options: BTreeMap<String, String>,
    pub indexes: Vec<IndexSchema>,
    pub materialized_views: Vec<String>,
    /// CDC enabled for this table.
    pub cdc: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ColumnSchema {
    pub name: String,
    pub cql_type: String,
    pub kind: ColumnKind,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub enum ColumnKind {
    PartitionKey,
    Clustering,
    Regular,
    Static,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexSchema {
    pub name: String,
    pub target_column: String,
    pub kind: String,
    pub options: BTreeMap<String, String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeSchema {
    pub keyspace: String,
    pub name: String,
    pub fields: Vec<(String, String)>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FunctionSchema {
    pub keyspace: String,
    pub name: String,
    pub argument_types: Vec<String>,
    pub return_type: String,
    pub language: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AggregateSchema {
    pub keyspace: String,
    pub name: String,
    pub argument_types: Vec<String>,
    pub state_func: String,
    pub final_func: Option<String>,
    pub state_type: String,
    pub init_cond: Option<String>,
}

// ─── Diff Report ──────────────────────────────────────────────────────────

/// Complete schema diff report.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SchemaDiffReport {
    pub source_label: String,
    pub target_label: String,
    pub keyspace_diffs: Vec<KeyspaceDiff>,
    pub type_diffs: Vec<TypeDiff>,
    pub total_differences: usize,
    pub severity: DiffSeverity,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub enum DiffSeverity {
    /// No differences found.
    Identical,
    /// Minor differences (options, metadata) that don't affect data.
    Minor,
    /// Significant differences (columns, types) that affect data layout.
    Major,
    /// Critical differences (missing tables, incompatible types).
    Critical,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KeyspaceDiff {
    pub keyspace: String,
    pub kind: DiffKind,
    pub table_diffs: Vec<TableDiff>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TableDiff {
    pub table: String,
    pub kind: DiffKind,
    pub column_diffs: Vec<ColumnDiff>,
    pub option_diffs: Vec<OptionDiff>,
    pub index_diffs: Vec<IndexDiff>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ColumnDiff {
    pub column: String,
    pub kind: DiffKind,
    pub detail: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OptionDiff {
    pub option: String,
    pub source_value: Option<String>,
    pub target_value: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IndexDiff {
    pub index: String,
    pub kind: DiffKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TypeDiff {
    pub type_name: String,
    pub keyspace: String,
    pub kind: DiffKind,
    pub detail: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum DiffKind {
    Added,
    Removed,
    Modified,
    Unchanged,
}

// ─── Diff Logic ───────────────────────────────────────────────────────────

/// Compare two schema snapshots and produce a diff report.
pub fn diff_schemas(source: &SchemaSnapshot, target: &SchemaSnapshot) -> SchemaDiffReport {
    let mut keyspace_diffs = Vec::new();
    let mut total = 0usize;

    let all_ks: BTreeSet<&String> = source
        .keyspaces
        .keys()
        .chain(target.keyspaces.keys())
        .collect();

    for ks_name in all_ks {
        let src_ks = source.keyspaces.get(ks_name);
        let tgt_ks = target.keyspaces.get(ks_name);

        match (src_ks, tgt_ks) {
            (Some(_), None) => {
                total += 1;
                keyspace_diffs.push(KeyspaceDiff {
                    keyspace: ks_name.to_string(),
                    kind: DiffKind::Removed,
                    table_diffs: vec![],
                });
            }
            (None, Some(_)) => {
                total += 1;
                keyspace_diffs.push(KeyspaceDiff {
                    keyspace: ks_name.to_string(),
                    kind: DiffKind::Added,
                    table_diffs: vec![],
                });
            }
            (Some(src), Some(tgt)) => {
                let table_diffs = diff_tables(&src.tables, &tgt.tables);
                let diff_count: usize = table_diffs.len();
                total += diff_count;
                if diff_count > 0 {
                    keyspace_diffs.push(KeyspaceDiff {
                        keyspace: ks_name.to_string(),
                        kind: DiffKind::Modified,
                        table_diffs,
                    });
                }
            }
            (None, None) => unreachable!(),
        }
    }

    // Type diffs
    let type_diffs = diff_types(&source.types, &target.types);
    total += type_diffs.len();

    let severity = if total == 0 {
        DiffSeverity::Identical
    } else if keyspace_diffs.iter().any(|d| d.kind == DiffKind::Removed) {
        DiffSeverity::Critical
    } else if keyspace_diffs.iter().any(|d| {
        d.table_diffs
            .iter()
            .any(|t| t.kind == DiffKind::Removed || !t.column_diffs.is_empty())
    }) {
        DiffSeverity::Major
    } else {
        DiffSeverity::Minor
    };

    SchemaDiffReport {
        source_label: source.source.clone(),
        target_label: target.source.clone(),
        keyspace_diffs,
        type_diffs,
        total_differences: total,
        severity,
    }
}

fn diff_tables(
    source: &BTreeMap<String, TableSchema>,
    target: &BTreeMap<String, TableSchema>,
) -> Vec<TableDiff> {
    let mut diffs = Vec::new();
    let all_tables: BTreeSet<&String> = source.keys().chain(target.keys()).collect();

    for table_name in all_tables {
        match (source.get(table_name), target.get(table_name)) {
            (Some(_), None) => {
                diffs.push(TableDiff {
                    table: table_name.to_string(),
                    kind: DiffKind::Removed,
                    column_diffs: vec![],
                    option_diffs: vec![],
                    index_diffs: vec![],
                });
            }
            (None, Some(_)) => {
                diffs.push(TableDiff {
                    table: table_name.to_string(),
                    kind: DiffKind::Added,
                    column_diffs: vec![],
                    option_diffs: vec![],
                    index_diffs: vec![],
                });
            }
            (Some(src), Some(tgt)) => {
                let column_diffs = diff_columns(&src.columns, &tgt.columns);
                let option_diffs = diff_options(&src.options, &tgt.options);
                let index_diffs = diff_indexes(&src.indexes, &tgt.indexes);

                if !column_diffs.is_empty() || !option_diffs.is_empty() || !index_diffs.is_empty() {
                    diffs.push(TableDiff {
                        table: table_name.to_string(),
                        kind: DiffKind::Modified,
                        column_diffs,
                        option_diffs,
                        index_diffs,
                    });
                }
            }
            (None, None) => unreachable!(),
        }
    }

    diffs
}

fn diff_columns(source: &[ColumnSchema], target: &[ColumnSchema]) -> Vec<ColumnDiff> {
    let mut diffs = Vec::new();
    let src_map: BTreeMap<&str, &ColumnSchema> =
        source.iter().map(|c| (c.name.as_str(), c)).collect();
    let tgt_map: BTreeMap<&str, &ColumnSchema> =
        target.iter().map(|c| (c.name.as_str(), c)).collect();

    let all_cols: BTreeSet<&str> = src_map
        .keys()
        .copied()
        .chain(tgt_map.keys().copied())
        .collect();

    for col in all_cols {
        match (src_map.get(col), tgt_map.get(col)) {
            (Some(_), None) => {
                diffs.push(ColumnDiff {
                    column: col.to_string(),
                    kind: DiffKind::Removed,
                    detail: "Column exists in source but not target".into(),
                });
            }
            (None, Some(_)) => {
                diffs.push(ColumnDiff {
                    column: col.to_string(),
                    kind: DiffKind::Added,
                    detail: "Column exists in target but not source".into(),
                });
            }
            (Some(s), Some(t)) => {
                if s != t {
                    let detail = if s.cql_type != t.cql_type {
                        format!("Type changed: {} → {}", s.cql_type, t.cql_type)
                    } else {
                        format!("Kind changed: {:?} → {:?}", s.kind, t.kind)
                    };
                    diffs.push(ColumnDiff {
                        column: col.to_string(),
                        kind: DiffKind::Modified,
                        detail,
                    });
                }
            }
            (None, None) => unreachable!(),
        }
    }

    diffs
}

fn diff_options(
    source: &BTreeMap<String, String>,
    target: &BTreeMap<String, String>,
) -> Vec<OptionDiff> {
    let mut diffs = Vec::new();
    let all_opts: BTreeSet<&String> = source.keys().chain(target.keys()).collect();

    for opt in all_opts {
        let s = source.get(opt);
        let t = target.get(opt);
        if s != t {
            diffs.push(OptionDiff {
                option: opt.clone(),
                source_value: s.cloned(),
                target_value: t.cloned(),
            });
        }
    }

    diffs
}

fn diff_indexes(source: &[IndexSchema], target: &[IndexSchema]) -> Vec<IndexDiff> {
    let mut diffs = Vec::new();
    let src_names: BTreeSet<&str> = source.iter().map(|i| i.name.as_str()).collect();
    let tgt_names: BTreeSet<&str> = target.iter().map(|i| i.name.as_str()).collect();

    for name in src_names.difference(&tgt_names) {
        diffs.push(IndexDiff {
            index: name.to_string(),
            kind: DiffKind::Removed,
        });
    }
    for name in tgt_names.difference(&src_names) {
        diffs.push(IndexDiff {
            index: name.to_string(),
            kind: DiffKind::Added,
        });
    }

    diffs
}

fn diff_types(source: &[TypeSchema], target: &[TypeSchema]) -> Vec<TypeDiff> {
    let mut diffs = Vec::new();
    let src_map: BTreeMap<(&str, &str), &TypeSchema> = source
        .iter()
        .map(|t| ((t.keyspace.as_str(), t.name.as_str()), t))
        .collect();
    let tgt_map: BTreeMap<(&str, &str), &TypeSchema> = target
        .iter()
        .map(|t| ((t.keyspace.as_str(), t.name.as_str()), t))
        .collect();

    for key in src_map.keys() {
        if !tgt_map.contains_key(key) {
            diffs.push(TypeDiff {
                type_name: key.1.to_string(),
                keyspace: key.0.to_string(),
                kind: DiffKind::Removed,
                detail: "Type exists in source but not target".into(),
            });
        }
    }
    for key in tgt_map.keys() {
        if !src_map.contains_key(key) {
            diffs.push(TypeDiff {
                type_name: key.1.to_string(),
                keyspace: key.0.to_string(),
                kind: DiffKind::Added,
                detail: "Type exists in target but not source".into(),
            });
        }
    }

    diffs
}

/// Parse a simplified CQL schema string into a `SchemaSnapshot`.
///
/// This parser handles the output of `DESCRIBE SCHEMA` at a high level,
/// extracting keyspaces, tables, columns, and basic options.
pub fn parse_cql_schema(cql: &str, source_label: &str) -> SchemaSnapshot {
    let mut snapshot = SchemaSnapshot {
        source: source_label.to_string(),
        keyspaces: BTreeMap::new(),
        types: Vec::new(),
        functions: Vec::new(),
        aggregates: Vec::new(),
    };

    let mut current_ks: Option<String> = None;
    let mut current_table: Option<String> = None;
    let mut current_columns: Vec<ColumnSchema> = Vec::new();
    let mut current_pk: Vec<String> = Vec::new();

    for line in cql.lines() {
        let trimmed = line.trim();

        // CREATE KEYSPACE
        if trimmed.starts_with("CREATE KEYSPACE") {
            if let Some(ks_name) = extract_name_after(trimmed, "CREATE KEYSPACE") {
                let ks_name = ks_name.trim_end_matches(";").to_string();
                current_ks = Some(ks_name.clone());
                snapshot
                    .keyspaces
                    .entry(ks_name.clone())
                    .or_insert_with(|| KeyspaceSchema {
                        name: ks_name,
                        replication: BTreeMap::new(),
                        durable_writes: true,
                        tables: BTreeMap::new(),
                    });
            }
        }

        // CREATE TABLE
        if trimmed.starts_with("CREATE TABLE") {
            // Flush previous table
            flush_table(
                &mut snapshot,
                &current_ks,
                &current_table,
                &current_columns,
                &current_pk,
            );
            current_columns.clear();
            current_pk.clear();

            if let Some(full_name) = extract_name_after(trimmed, "CREATE TABLE") {
                let parts: Vec<&str> = full_name.split('.').collect();
                if parts.len() == 2 {
                    current_ks = Some(parts[0].to_string());
                    current_table = Some(parts[1].trim_end_matches('(').trim().to_string());
                } else {
                    current_table = Some(full_name.trim_end_matches('(').trim().to_string());
                }
            }
        }

        // PRIMARY KEY
        if trimmed.starts_with("PRIMARY KEY") {
            if let Some(pk) = extract_primary_key(trimmed) {
                current_pk = pk;
            }
        }

        // Column definitions (inside CREATE TABLE)
        if current_table.is_some()
            && !trimmed.starts_with("CREATE")
            && !trimmed.starts_with("PRIMARY")
            && !trimmed.starts_with(")")
            && !trimmed.starts_with("WITH")
            && !trimmed.is_empty()
            && trimmed.contains(' ')
        {
            let parts: Vec<&str> = trimmed.splitn(2, ' ').collect();
            if parts.len() == 2 {
                let col_name = parts[0].trim_matches('"');
                let col_type = parts[1].trim_end_matches(',').trim();
                if !col_type.is_empty() && col_name != "PRIMARY" {
                    current_columns.push(ColumnSchema {
                        name: col_name.to_string(),
                        cql_type: col_type.to_string(),
                        kind: ColumnKind::Regular,
                    });
                }
            }
        }

        // End of CREATE TABLE
        if current_table.is_some() && trimmed.starts_with(");") {
            flush_table(
                &mut snapshot,
                &current_ks,
                &current_table,
                &current_columns,
                &current_pk,
            );
            current_table = None;
            current_columns.clear();
            current_pk.clear();
        }
    }

    // Flush any remaining table
    flush_table(
        &mut snapshot,
        &current_ks,
        &current_table,
        &current_columns,
        &current_pk,
    );

    snapshot
}

fn flush_table(
    snapshot: &mut SchemaSnapshot,
    current_ks: &Option<String>,
    current_table: &Option<String>,
    columns: &[ColumnSchema],
    pk: &[String],
) {
    if let (Some(ks), Some(table)) = (current_ks, current_table) {
        if columns.is_empty() {
            return;
        }

        let mut cols = columns.to_vec();
        for col in &mut cols {
            if pk.first().map(|s| s.as_str()) == Some(&col.name) {
                col.kind = ColumnKind::PartitionKey;
            } else if pk.contains(&col.name) {
                col.kind = ColumnKind::Clustering;
            }
        }

        let ks_schema = snapshot
            .keyspaces
            .entry(ks.clone())
            .or_insert_with(|| KeyspaceSchema {
                name: ks.clone(),
                replication: BTreeMap::new(),
                durable_writes: true,
                tables: BTreeMap::new(),
            });

        ks_schema.tables.insert(
            table.clone(),
            TableSchema {
                name: table.clone(),
                columns: cols,
                partition_key: pk.first().cloned().into_iter().collect(),
                clustering_key: pk.iter().skip(1).cloned().collect(),
                options: BTreeMap::new(),
                indexes: Vec::new(),
                materialized_views: Vec::new(),
                cdc: false,
            },
        );
    }
}

fn extract_name_after(line: &str, prefix: &str) -> Option<String> {
    let rest = line.strip_prefix(prefix)?.trim();
    // Handle "IF NOT EXISTS" clause
    let rest = if rest.starts_with("IF NOT EXISTS") {
        rest.strip_prefix("IF NOT EXISTS")?.trim()
    } else {
        rest
    };
    let name = rest
        .split_whitespace()
        .next()?
        .trim_matches('"')
        .trim_end_matches('(')
        .trim_end_matches(';');
    Some(name.to_string())
}

fn extract_primary_key(line: &str) -> Option<Vec<String>> {
    let start = line.find('(')?;
    let end = line.rfind(')')?;
    let inner = &line[start + 1..end];
    // Handle composite: ((pk1, pk2), ck1, ck2)
    let keys: Vec<String> = inner
        .replace('(', "")
        .replace(')', "")
        .split(',')
        .map(|s| s.trim().trim_matches('"').to_string())
        .filter(|s| !s.is_empty())
        .collect();
    Some(keys)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_schema(
        source: &str,
        ks: &str,
        tables: Vec<(&str, Vec<(&str, &str)>)>,
    ) -> SchemaSnapshot {
        let mut keyspaces = BTreeMap::new();
        let mut table_map = BTreeMap::new();

        for (tname, cols) in tables {
            let columns: Vec<ColumnSchema> = cols
                .into_iter()
                .enumerate()
                .map(|(i, (name, typ))| ColumnSchema {
                    name: name.to_string(),
                    cql_type: typ.to_string(),
                    kind: if i == 0 {
                        ColumnKind::PartitionKey
                    } else {
                        ColumnKind::Regular
                    },
                })
                .collect();
            table_map.insert(
                tname.to_string(),
                TableSchema {
                    name: tname.to_string(),
                    columns,
                    partition_key: vec![],
                    clustering_key: vec![],
                    options: BTreeMap::new(),
                    indexes: Vec::new(),
                    materialized_views: Vec::new(),
                    cdc: false,
                },
            );
        }

        keyspaces.insert(
            ks.to_string(),
            KeyspaceSchema {
                name: ks.to_string(),
                replication: BTreeMap::new(),
                durable_writes: true,
                tables: table_map,
            },
        );

        SchemaSnapshot {
            source: source.to_string(),
            keyspaces,
            types: Vec::new(),
            functions: Vec::new(),
            aggregates: Vec::new(),
        }
    }

    #[test]
    fn identical_schemas() {
        let a = make_schema(
            "java",
            "ks",
            vec![("t1", vec![("id", "int"), ("name", "text")])],
        );
        let b = make_schema(
            "rust",
            "ks",
            vec![("t1", vec![("id", "int"), ("name", "text")])],
        );
        let report = diff_schemas(&a, &b);
        assert_eq!(report.severity, DiffSeverity::Identical);
        assert_eq!(report.total_differences, 0);
    }

    #[test]
    fn missing_table() {
        let a = make_schema(
            "java",
            "ks",
            vec![("t1", vec![("id", "int")]), ("t2", vec![("id", "int")])],
        );
        let b = make_schema("rust", "ks", vec![("t1", vec![("id", "int")])]);
        let report = diff_schemas(&a, &b);
        assert!(report.total_differences > 0);
        assert!(report.severity >= DiffSeverity::Major);
    }

    #[test]
    fn added_column() {
        let a = make_schema("java", "ks", vec![("t1", vec![("id", "int")])]);
        let b = make_schema(
            "rust",
            "ks",
            vec![("t1", vec![("id", "int"), ("extra", "text")])],
        );
        let report = diff_schemas(&a, &b);
        assert!(report.total_differences > 0);
    }

    #[test]
    fn type_change() {
        let a = make_schema(
            "java",
            "ks",
            vec![("t1", vec![("id", "int"), ("val", "text")])],
        );
        let b = make_schema(
            "rust",
            "ks",
            vec![("t1", vec![("id", "int"), ("val", "blob")])],
        );
        let report = diff_schemas(&a, &b);
        assert!(report.total_differences > 0);
        let col_diffs = &report.keyspace_diffs[0].table_diffs[0].column_diffs;
        assert!(
            col_diffs
                .iter()
                .any(|d| d.column == "val" && d.kind == DiffKind::Modified)
        );
    }

    #[test]
    fn parse_simple_cql() {
        let cql = r#"
CREATE KEYSPACE myks WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};

CREATE TABLE myks.users (
    id int,
    name text,
    email text,
    PRIMARY KEY (id)
);
"#;
        let schema = parse_cql_schema(cql, "java-4.1");
        assert!(schema.keyspaces.contains_key("myks"));
        let users = &schema.keyspaces["myks"].tables["users"];
        assert_eq!(users.columns.len(), 3);
    }

    #[test]
    fn missing_keyspace_is_critical() {
        let a = make_schema("java", "ks1", vec![("t1", vec![("id", "int")])]);
        let b = SchemaSnapshot {
            source: "rust".into(),
            keyspaces: BTreeMap::new(),
            types: Vec::new(),
            functions: Vec::new(),
            aggregates: Vec::new(),
        };
        let report = diff_schemas(&a, &b);
        assert_eq!(report.severity, DiffSeverity::Critical);
    }

    #[test]
    fn diff_report_serializable() {
        let a = make_schema("java", "ks", vec![("t1", vec![("id", "int")])]);
        let b = make_schema(
            "rust",
            "ks",
            vec![("t1", vec![("id", "int"), ("v", "text")])],
        );
        let report = diff_schemas(&a, &b);
        let json = serde_json::to_string_pretty(&report).unwrap();
        assert!(json.contains("total_differences"));
    }
}
