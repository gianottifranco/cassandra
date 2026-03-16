// Licensed under Apache License, Version 2.0.

//! Guardrails: configuration-driven limits and controls.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.guardrails.Guardrails`
//! - `org.apache.cassandra.db.guardrails.GuardrailsConfig`
//!
//! Guardrails enforce operational limits on CQL operations to prevent
//! accidental damage (e.g., unbounded partition growth, too many columns).
//! Each guardrail can be in one of three modes: Disabled, Warn, or Fail.

use serde::{Deserialize, Serialize};

/// The action a guardrail takes when triggered.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GuardrailAction {
    /// No limit enforced.
    Disabled,
    /// Log a warning but allow the operation.
    Warn,
    /// Reject the operation with an error.
    Fail,
}

impl Default for GuardrailAction {
    fn default() -> Self {
        Self::Disabled
    }
}

/// A threshold guardrail with optional warn and fail values.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ThresholdGuardrail {
    /// Threshold that triggers a warning. `None` = disabled.
    pub warn_threshold: Option<i64>,
    /// Threshold that triggers a failure. `None` = disabled.
    pub fail_threshold: Option<i64>,
}

impl Default for ThresholdGuardrail {
    fn default() -> Self {
        Self {
            warn_threshold: None,
            fail_threshold: None,
        }
    }
}

impl ThresholdGuardrail {
    /// Check a value against this guardrail.
    pub fn check(&self, value: i64) -> GuardrailAction {
        if let Some(fail) = self.fail_threshold {
            if value >= fail {
                return GuardrailAction::Fail;
            }
        }
        if let Some(warn) = self.warn_threshold {
            if value >= warn {
                return GuardrailAction::Warn;
            }
        }
        GuardrailAction::Disabled
    }
}

/// Full set of guardrails configuration.
///
/// Matches the Java `GuardrailsConfig` / `Guardrails` classes.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GuardrailsConfig {
    // ─── Table / Keyspace Limits ─────────────────────────────────────────
    /// Maximum number of tables per keyspace.
    pub tables_per_keyspace: ThresholdGuardrail,
    /// Maximum number of columns per table.
    pub columns_per_table: ThresholdGuardrail,
    /// Maximum number of secondary indexes per table.
    pub secondary_indexes_per_table: ThresholdGuardrail,
    /// Maximum number of materialized views per table.
    pub materialized_views_per_table: ThresholdGuardrail,
    /// Maximum number of fields in a UDT.
    pub fields_per_udt: ThresholdGuardrail,

    // ─── Collection Limits ───────────────────────────────────────────────
    /// Maximum number of items in a collection (list, set, map).
    pub collection_size: ThresholdGuardrail,
    /// Maximum size (in bytes) of a single collection element.
    pub item_size: ThresholdGuardrail,

    // ─── Partition Limits ────────────────────────────────────────────────
    /// Maximum partition size in bytes.
    pub partition_size: ThresholdGuardrail,
    /// Maximum number of rows per partition.
    pub partition_tombstones: ThresholdGuardrail,

    // ─── Query Limits ────────────────────────────────────────────────────
    /// Maximum page size in rows.
    pub page_size: ThresholdGuardrail,
    /// Maximum number of IN restrictions in a query.
    pub in_select_cartesian_product: ThresholdGuardrail,

    // ─── Feature Flags ───────────────────────────────────────────────────
    /// Whether ALLOW FILTERING is permitted.
    pub allow_filtering_enabled: bool,
    /// Whether compact storage is permitted for new tables.
    pub compact_tables_enabled: bool,
    /// Whether user-defined aggregates are permitted.
    pub user_aggregates_enabled: bool,
    /// Whether group-by queries are permitted.
    pub group_by_enabled: bool,
    /// Whether TRUNCATE is permitted.
    pub truncate_enabled: bool,
    /// Whether DROP KEYSPACE of non-empty keyspaces is permitted.
    pub drop_keyspace_enabled: bool,
    /// Whether uncompressed SSTables are permitted.
    pub uncompressed_tables_enabled: bool,

    // ─── Read/Write Consistency ──────────────────────────────────────────
    /// Minimum read consistency level allowed.
    pub minimum_replication_factor: ThresholdGuardrail,
}

impl Default for GuardrailsConfig {
    fn default() -> Self {
        Self {
            tables_per_keyspace: ThresholdGuardrail::default(),
            columns_per_table: ThresholdGuardrail::default(),
            secondary_indexes_per_table: ThresholdGuardrail::default(),
            materialized_views_per_table: ThresholdGuardrail::default(),
            fields_per_udt: ThresholdGuardrail::default(),
            collection_size: ThresholdGuardrail::default(),
            item_size: ThresholdGuardrail::default(),
            partition_size: ThresholdGuardrail::default(),
            partition_tombstones: ThresholdGuardrail::default(),
            page_size: ThresholdGuardrail::default(),
            in_select_cartesian_product: ThresholdGuardrail::default(),
            allow_filtering_enabled: true,
            compact_tables_enabled: false,
            user_aggregates_enabled: true,
            group_by_enabled: true,
            truncate_enabled: true,
            drop_keyspace_enabled: true,
            uncompressed_tables_enabled: true,
            minimum_replication_factor: ThresholdGuardrail::default(),
        }
    }
}

impl GuardrailsConfig {
    /// Check a guardrail by name for a given value.
    /// Returns the action to take.
    pub fn check_threshold(&self, name: &str, value: i64) -> GuardrailAction {
        match name {
            "tables_per_keyspace" => self.tables_per_keyspace.check(value),
            "columns_per_table" => self.columns_per_table.check(value),
            "secondary_indexes_per_table" => self.secondary_indexes_per_table.check(value),
            "materialized_views_per_table" => self.materialized_views_per_table.check(value),
            "fields_per_udt" => self.fields_per_udt.check(value),
            "collection_size" => self.collection_size.check(value),
            "item_size" => self.item_size.check(value),
            "partition_size" => self.partition_size.check(value),
            "partition_tombstones" => self.partition_tombstones.check(value),
            "page_size" => self.page_size.check(value),
            "in_select_cartesian_product" => self.in_select_cartesian_product.check(value),
            "minimum_replication_factor" => self.minimum_replication_factor.check(value),
            _ => GuardrailAction::Disabled,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_guardrails_are_disabled() {
        let g = GuardrailsConfig::default();
        assert_eq!(g.tables_per_keyspace.check(9999), GuardrailAction::Disabled);
    }

    #[test]
    fn threshold_warn() {
        let t = ThresholdGuardrail {
            warn_threshold: Some(100),
            fail_threshold: Some(500),
        };
        assert_eq!(t.check(50), GuardrailAction::Disabled);
        assert_eq!(t.check(100), GuardrailAction::Warn);
        assert_eq!(t.check(250), GuardrailAction::Warn);
        assert_eq!(t.check(500), GuardrailAction::Fail);
        assert_eq!(t.check(1000), GuardrailAction::Fail);
    }

    #[test]
    fn threshold_only_warn() {
        let t = ThresholdGuardrail {
            warn_threshold: Some(10),
            fail_threshold: None,
        };
        assert_eq!(t.check(5), GuardrailAction::Disabled);
        assert_eq!(t.check(10), GuardrailAction::Warn);
        assert_eq!(t.check(9999), GuardrailAction::Warn);
    }

    #[test]
    fn threshold_only_fail() {
        let t = ThresholdGuardrail {
            warn_threshold: None,
            fail_threshold: Some(100),
        };
        assert_eq!(t.check(50), GuardrailAction::Disabled);
        assert_eq!(t.check(100), GuardrailAction::Fail);
    }

    #[test]
    fn check_by_name() {
        let mut g = GuardrailsConfig::default();
        g.columns_per_table = ThresholdGuardrail {
            warn_threshold: Some(20),
            fail_threshold: Some(100),
        };
        assert_eq!(g.check_threshold("columns_per_table", 10), GuardrailAction::Disabled);
        assert_eq!(g.check_threshold("columns_per_table", 20), GuardrailAction::Warn);
        assert_eq!(g.check_threshold("columns_per_table", 100), GuardrailAction::Fail);
        assert_eq!(g.check_threshold("unknown", 100), GuardrailAction::Disabled);
    }

    #[test]
    fn feature_flags_defaults() {
        let g = GuardrailsConfig::default();
        assert!(g.allow_filtering_enabled);
        assert!(!g.compact_tables_enabled);
        assert!(g.truncate_enabled);
    }

    #[test]
    fn serialization_roundtrip() {
        let g = GuardrailsConfig::default();
        let json = serde_json::to_string(&g).unwrap();
        let g2: GuardrailsConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(g2.allow_filtering_enabled, g.allow_filtering_enabled);
    }
}
