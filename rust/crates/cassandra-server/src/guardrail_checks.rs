// Licensed under Apache License, Version 2.0.

//! Guardrails enforcement at the CQL query execution layer.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.guardrails.Guardrails`
//! - `org.apache.cassandra.cql3.statements.CreateTableStatement` (guardrail hooks)
//!
//! Wires guardrail checks into query execution paths:
//! - CREATE TABLE → tables/columns limits
//! - CREATE INDEX → index limits
//! - SELECT ALLOW FILTERING → feature flag
//! - TRUNCATE → feature flag
//! - Page size → threshold check

use cassandra_config::{GuardrailAction, GuardrailViolation, GuardrailsConfig};
use tracing::warn;

/// Result of a guardrail check.
#[derive(Debug)]
pub enum GuardrailResult {
    /// Operation is allowed.
    Allowed,
    /// Operation is allowed but generated a warning.
    Warned(String),
    /// Operation is rejected.
    Rejected(String),
}

impl GuardrailResult {
    pub fn is_rejected(&self) -> bool {
        matches!(self, GuardrailResult::Rejected(_))
    }
}

/// Check guardrails for CREATE TABLE.
pub fn check_create_table(
    config: &GuardrailsConfig,
    tables_in_keyspace: i64,
    column_count: i64,
) -> GuardrailResult {
    // Check tables per keyspace
    if let Some(v) = check_threshold(config, "tables_per_keyspace", tables_in_keyspace + 1) {
        if v.action == GuardrailAction::Fail {
            return GuardrailResult::Rejected(v.message);
        }
        warn!("{}", v);
    }

    // Check columns per table
    if let Some(v) = check_threshold(config, "columns_per_table", column_count) {
        if v.action == GuardrailAction::Fail {
            return GuardrailResult::Rejected(v.message);
        }
        warn!("{}", v);
        return GuardrailResult::Warned(v.message);
    }

    GuardrailResult::Allowed
}

/// Check guardrails for CREATE INDEX.
pub fn check_create_index(config: &GuardrailsConfig, indexes_on_table: i64) -> GuardrailResult {
    if let Some(v) = check_threshold(config, "secondary_indexes_per_table", indexes_on_table + 1) {
        if v.action == GuardrailAction::Fail {
            return GuardrailResult::Rejected(v.message);
        }
        warn!("{}", v);
        return GuardrailResult::Warned(v.message);
    }
    GuardrailResult::Allowed
}

/// Check guardrails for ALLOW FILTERING.
pub fn check_allow_filtering(config: &GuardrailsConfig) -> GuardrailResult {
    check_feature(config, "allow_filtering", "ALLOW FILTERING")
}

/// Check guardrails for TRUNCATE.
pub fn check_truncate(config: &GuardrailsConfig) -> GuardrailResult {
    check_feature(config, "truncate", "TRUNCATE")
}

/// Check page size guardrail.
pub fn check_page_size(config: &GuardrailsConfig, page_size: i64) -> GuardrailResult {
    if let Some(v) = check_threshold(config, "page_size", page_size) {
        if v.action == GuardrailAction::Fail {
            return GuardrailResult::Rejected(v.message);
        }
        warn!("{}", v);
        return GuardrailResult::Warned(v.message);
    }
    GuardrailResult::Allowed
}

/// Check DROP KEYSPACE guardrail.
pub fn check_drop_keyspace(config: &GuardrailsConfig) -> GuardrailResult {
    check_feature(config, "drop_keyspace", "DROP KEYSPACE")
}

fn check_threshold(
    config: &GuardrailsConfig,
    name: &str,
    value: i64,
) -> Option<GuardrailViolation> {
    let action = config.check_threshold(name, value);
    match action {
        GuardrailAction::Disabled => None,
        _ => Some(GuardrailViolation {
            guardrail_name: name.to_string(),
            message: format!(
                "{} = {} exceeds {} threshold",
                name,
                value,
                if action == GuardrailAction::Warn {
                    "warn"
                } else {
                    "fail"
                }
            ),
            action,
        }),
    }
}

fn check_feature(config: &GuardrailsConfig, feature: &str, display: &str) -> GuardrailResult {
    match config.check_feature(feature) {
        GuardrailAction::Fail => {
            GuardrailResult::Rejected(format!("{display} is not allowed by guardrails"))
        }
        _ => GuardrailResult::Allowed,
    }
}

// ─── Read Guardrails (WU-10) ──────────────────────────────────────

/// Check read-path guardrails at the CQL query execution layer.
///
/// Validates:
/// - ALLOW FILTERING enabled when used
/// - SELECT column count
///
/// ## Java Oracle
/// `org.apache.cassandra.db.guardrails.Guardrails` — read-path hooks
pub fn check_select_guardrails(
    config: &GuardrailsConfig,
    allow_filtering: bool,
    column_count: i64,
) -> GuardrailResult {
    // Check ALLOW FILTERING
    if allow_filtering {
        let result = check_allow_filtering(config);
        if result.is_rejected() {
            return result;
        }
    }

    // Check columns per query (if threshold configured)
    if let Some(v) = check_threshold(config, "columns_per_query", column_count) {
        if v.action == GuardrailAction::Fail {
            return GuardrailResult::Rejected(v.message);
        }
        return GuardrailResult::Warned(v.message);
    }

    GuardrailResult::Allowed
}

// ─── Write Guardrails (WU-04) ─────────────────────────────────────

/// Check write-path guardrails at the server layer.
///
/// Wraps coordinator-level `WriteGuardrails` for use in the CQL query
/// execution layer. Returns a `GuardrailResult` suitable for warning
/// or rejecting before the write reaches the coordinator.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.db.guardrails.Guardrails` — write-path hooks
pub fn check_write_guardrails(
    guardrails: &cassandra_coordinator::WriteGuardrails,
    mutation: &cassandra_coordinator::CoordinatedMutation,
) -> GuardrailResult {
    // Check mutation size
    let size = mutation.estimated_size();
    if size > guardrails.max_mutation_size {
        return GuardrailResult::Rejected(format!(
            "Mutation of {} bytes exceeds maximum of {} bytes",
            size, guardrails.max_mutation_size
        ));
    }

    // Check partition size warning
    if size > guardrails.partition_size_warn {
        return GuardrailResult::Warned(format!(
            "Mutation of {} bytes exceeds partition size warn threshold of {} bytes",
            size, guardrails.partition_size_warn
        ));
    }

    // Check tombstone count
    let tombstone_count = count_tombstones(mutation);
    if tombstone_count > guardrails.tombstone_warn_threshold {
        return GuardrailResult::Warned(format!(
            "Mutation contains {} tombstones, exceeds warn threshold of {}",
            tombstone_count, guardrails.tombstone_warn_threshold
        ));
    }

    // Check collection sizes
    let max_collection = max_collection_size(mutation);
    if max_collection > guardrails.collection_size_warn {
        return GuardrailResult::Warned(format!(
            "Collection of {} elements exceeds warn threshold of {}",
            max_collection, guardrails.collection_size_warn
        ));
    }

    // Check columns per query
    let column_count = count_columns(mutation);
    if column_count > guardrails.columns_per_query_warn {
        return GuardrailResult::Warned(format!(
            "Query touches {} columns, exceeds warn threshold of {}",
            column_count, guardrails.columns_per_query_warn
        ));
    }

    GuardrailResult::Allowed
}

fn count_tombstones(mutation: &cassandra_coordinator::CoordinatedMutation) -> usize {
    let mut count = 0;
    if mutation.partition_tombstone.is_some() {
        count += 1;
    }
    for row in &mutation.rows {
        if row.is_tombstone {
            count += 1;
        }
        if row.range_tombstone.is_some() {
            count += 1;
        }
        for cell in &row.cells {
            if cell.is_tombstone {
                count += 1;
            }
        }
    }
    for cell in &mutation.static_cells {
        if cell.is_tombstone {
            count += 1;
        }
    }
    count
}

fn max_collection_size(mutation: &cassandra_coordinator::CoordinatedMutation) -> usize {
    let all_cells = mutation
        .rows
        .iter()
        .flat_map(|r| r.cells.iter())
        .chain(mutation.static_cells.iter());
    let mut max_size = 0usize;
    for cell in all_cells {
        if let Some(ref op) = cell.collection_op {
            let sz = match op {
                cassandra_coordinator::CollectionOp::Append(elems)
                | cassandra_coordinator::CollectionOp::Remove(elems) => elems.len(),
                cassandra_coordinator::CollectionOp::MapPut(entries) => entries.len(),
            };
            if sz > max_size {
                max_size = sz;
            }
        }
    }
    max_size
}

fn count_columns(mutation: &cassandra_coordinator::CoordinatedMutation) -> usize {
    let mut columns = std::collections::HashSet::new();
    for row in &mutation.rows {
        for cell in &row.cells {
            columns.insert(cell.column.as_str());
        }
    }
    for cell in &mutation.static_cells {
        columns.insert(cell.column.as_str());
    }
    columns.len()
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_config::ThresholdGuardrail;

    #[test]
    fn create_table_allowed() {
        let config = GuardrailsConfig::default();
        let result = check_create_table(&config, 5, 10);
        assert!(!result.is_rejected());
    }

    #[test]
    fn create_table_rejected_too_many_tables() {
        let mut config = GuardrailsConfig::default();
        config.tables_per_keyspace = ThresholdGuardrail {
            warn_threshold: Some(5),
            fail_threshold: Some(10),
        };
        let result = check_create_table(&config, 10, 5);
        assert!(result.is_rejected());
    }

    #[test]
    fn create_table_warned_columns() {
        let mut config = GuardrailsConfig::default();
        config.columns_per_table = ThresholdGuardrail {
            warn_threshold: Some(20),
            fail_threshold: Some(100),
        };
        let result = check_create_table(&config, 1, 25);
        assert!(matches!(result, GuardrailResult::Warned(_)));
    }

    #[test]
    fn create_index_rejected() {
        let mut config = GuardrailsConfig::default();
        config.secondary_indexes_per_table = ThresholdGuardrail {
            warn_threshold: Some(3),
            fail_threshold: Some(5),
        };
        let result = check_create_index(&config, 5);
        assert!(result.is_rejected());
    }

    #[test]
    fn allow_filtering_disabled() {
        let mut config = GuardrailsConfig::default();
        config.allow_filtering_enabled = false;
        let result = check_allow_filtering(&config);
        assert!(result.is_rejected());
    }

    #[test]
    fn truncate_disabled() {
        let mut config = GuardrailsConfig::default();
        config.truncate_enabled = false;
        let result = check_truncate(&config);
        assert!(result.is_rejected());
    }

    #[test]
    fn page_size_check() {
        let mut config = GuardrailsConfig::default();
        config.page_size = ThresholdGuardrail {
            warn_threshold: Some(1000),
            fail_threshold: Some(5000),
        };
        assert!(!check_page_size(&config, 500).is_rejected());
        assert!(matches!(
            check_page_size(&config, 1000),
            GuardrailResult::Warned(_)
        ));
        assert!(check_page_size(&config, 5000).is_rejected());
    }

    #[test]
    fn drop_keyspace_allowed_by_default() {
        let config = GuardrailsConfig::default();
        assert!(!check_drop_keyspace(&config).is_rejected());
    }

    #[test]
    fn drop_keyspace_disabled() {
        let mut config = GuardrailsConfig::default();
        config.drop_keyspace_enabled = false;
        assert!(check_drop_keyspace(&config).is_rejected());
    }

    // ── WU-04: Write guardrails at server layer ──────────────────

    fn test_mutation() -> cassandra_coordinator::CoordinatedMutation {
        cassandra_coordinator::CoordinatedMutation::simple(
            "ks".to_string(),
            "tbl".to_string(),
            b"pk1".to_vec(),
            vec![cassandra_coordinator::MutationRow {
                clustering_key: vec![],
                cells: vec![cassandra_coordinator::CellMutation {
                    column: "col1".to_string(),
                    value: Some(b"val".to_vec()),
                    timestamp: 1000,
                    ttl: 0,
                    is_tombstone: false,
                    collection_op: None,
                }],
                is_tombstone: false,
                range_tombstone: None,
            }],
            1000,
        )
    }

    #[test]
    fn write_guardrail_allowed() {
        let guardrails = cassandra_coordinator::WriteGuardrails::default();
        let result = check_write_guardrails(&guardrails, &test_mutation());
        assert!(!result.is_rejected());
        assert!(matches!(result, GuardrailResult::Allowed));
    }

    #[test]
    fn write_guardrail_rejects_oversized_mutation() {
        let guardrails = cassandra_coordinator::WriteGuardrails {
            max_mutation_size: 10, // very small
            ..Default::default()
        };
        let result = check_write_guardrails(&guardrails, &test_mutation());
        assert!(result.is_rejected());
    }

    #[test]
    fn write_guardrail_warns_tombstones() {
        let guardrails = cassandra_coordinator::WriteGuardrails {
            tombstone_warn_threshold: 0,
            ..Default::default()
        };
        let mut m = test_mutation();
        m.rows[0].is_tombstone = true;
        let result = check_write_guardrails(&guardrails, &m);
        assert!(matches!(result, GuardrailResult::Warned(_)));
    }

    #[test]
    fn write_guardrail_warns_columns() {
        let guardrails = cassandra_coordinator::WriteGuardrails {
            columns_per_query_warn: 0,
            ..Default::default()
        };
        let result = check_write_guardrails(&guardrails, &test_mutation());
        assert!(matches!(result, GuardrailResult::Warned(_)));
    }

    #[test]
    fn write_guardrail_warns_collection_size() {
        let guardrails = cassandra_coordinator::WriteGuardrails {
            collection_size_warn: 1,
            ..Default::default()
        };
        let mut m = test_mutation();
        m.rows[0].cells[0].collection_op = Some(cassandra_coordinator::CollectionOp::Append(vec![
            b"a".to_vec(),
            b"b".to_vec(),
        ]));
        let result = check_write_guardrails(&guardrails, &m);
        assert!(matches!(result, GuardrailResult::Warned(_)));
    }

    // ── WU-10: Read guardrails ───────────────────────────────────────

    #[test]
    fn select_guardrail_allowed() {
        let config = GuardrailsConfig::default();
        let result = check_select_guardrails(&config, false, 5);
        assert!(!result.is_rejected());
    }

    #[test]
    fn select_guardrail_rejects_allow_filtering() {
        let mut config = GuardrailsConfig::default();
        config.allow_filtering_enabled = false;
        let result = check_select_guardrails(&config, true, 5);
        assert!(result.is_rejected());
    }

    #[test]
    fn select_guardrail_allows_when_no_filtering() {
        let mut config = GuardrailsConfig::default();
        config.allow_filtering_enabled = false;
        // allow_filtering=false in the query, so guardrail should not trigger
        let result = check_select_guardrails(&config, false, 5);
        assert!(!result.is_rejected());
    }
}
