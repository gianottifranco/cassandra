// Licensed under Apache License, Version 2.0.

#![allow(clippy::field_reassign_with_default)]

//! Guardrails: configuration-driven limits and controls.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.guardrails.Guardrails`
//! - `org.apache.cassandra.db.guardrails.GuardrailsConfig`
//!
//! Guardrails enforce operational limits on CQL operations to prevent
//! accidental damage (e.g., unbounded partition growth, too many columns).
//! Each guardrail can be in one of three modes: Disabled, Warn, or Fail.

use crate::units::{DataSize, Duration};
use serde::{Deserialize, Serialize};
use std::fmt;

/// The action a guardrail takes when triggered.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum GuardrailAction {
    /// No limit enforced.
    #[default]
    Disabled,
    /// Log a warning but allow the operation.
    Warn,
    /// Reject the operation with an error.
    Fail,
}

/// A threshold guardrail with optional warn and fail values.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ThresholdGuardrail {
    /// Threshold that triggers a warning. `None` = disabled.
    pub warn_threshold: Option<i64>,
    /// Threshold that triggers a failure. `None` = disabled.
    pub fail_threshold: Option<i64>,
}

/// A data-size threshold guardrail with optional warn and fail values.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct DataSizeThresholdGuardrail {
    /// Threshold that triggers a warning. `None` = disabled.
    pub warn_threshold: Option<DataSize>,
    /// Threshold that triggers a failure. `None` = disabled.
    pub fail_threshold: Option<DataSize>,
}

impl DataSizeThresholdGuardrail {
    /// Check a size against this guardrail.
    pub fn check(&self, value: DataSize) -> GuardrailAction {
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
#[serde(default)]
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

    /// Upstream flat thresholds from `cassandra.yaml`.
    pub keyspaces_warn_threshold: i64,
    pub keyspaces_fail_threshold: i64,
    pub tables_warn_threshold: i64,
    pub tables_fail_threshold: i64,
    pub columns_per_table_warn_threshold: i64,
    pub columns_per_table_fail_threshold: i64,
    pub secondary_indexes_per_table_warn_threshold: i64,
    pub secondary_indexes_per_table_fail_threshold: i64,
    pub materialized_views_per_table_warn_threshold: i64,
    pub materialized_views_per_table_fail_threshold: i64,
    pub page_size_warn_threshold: i64,
    pub page_size_fail_threshold: i64,
    pub partition_keys_in_select_warn_threshold: i64,
    pub partition_keys_in_select_fail_threshold: i64,
    pub in_select_cartesian_product_warn_threshold: i64,
    pub in_select_cartesian_product_fail_threshold: i64,
    pub partition_tombstones_warn_threshold: i64,
    pub partition_tombstones_fail_threshold: i64,
    pub items_per_collection_warn_threshold: i64,
    pub items_per_collection_fail_threshold: i64,
    pub fields_per_udt_warn_threshold: i64,
    pub fields_per_udt_fail_threshold: i64,
    pub vector_dimensions_warn_threshold: i64,
    pub vector_dimensions_fail_threshold: i64,
    pub data_disk_usage_percentage_warn_threshold: i64,
    pub data_disk_usage_percentage_fail_threshold: i64,
    pub minimum_replication_factor_warn_threshold: i64,
    pub minimum_replication_factor_fail_threshold: i64,
    pub maximum_replication_factor_warn_threshold: i64,
    pub maximum_replication_factor_fail_threshold: i64,

    // ─── Collection Limits ───────────────────────────────────────────────
    /// Maximum number of items in a collection (list, set, map).
    pub collection_size: ThresholdGuardrail,
    /// Maximum size (in bytes) of a single collection element.
    pub item_size: ThresholdGuardrail,

    pub partition_size_warn_threshold: Option<DataSize>,
    pub partition_size_fail_threshold: Option<DataSize>,
    pub column_value_size_warn_threshold: Option<DataSize>,
    pub column_value_size_fail_threshold: Option<DataSize>,
    pub column_ascii_value_size_warn_threshold: Option<DataSize>,
    pub column_ascii_value_size_fail_threshold: Option<DataSize>,
    pub column_blob_value_size_warn_threshold: Option<DataSize>,
    pub column_blob_value_size_fail_threshold: Option<DataSize>,
    pub column_text_and_varchar_value_size_warn_threshold: Option<DataSize>,
    pub column_text_and_varchar_value_size_fail_threshold: Option<DataSize>,
    pub collection_size_warn_threshold: Option<DataSize>,
    pub collection_size_fail_threshold: Option<DataSize>,
    pub collection_map_size_warn_threshold: Option<DataSize>,
    pub collection_map_size_fail_threshold: Option<DataSize>,
    pub collection_set_size_warn_threshold: Option<DataSize>,
    pub collection_set_size_fail_threshold: Option<DataSize>,
    pub collection_list_size_warn_threshold: Option<DataSize>,
    pub collection_list_size_fail_threshold: Option<DataSize>,
    pub data_disk_usage_max_disk_size: Option<DataSize>,
    pub sai_string_term_size_warn_threshold: Option<DataSize>,
    pub sai_string_term_size_fail_threshold: Option<DataSize>,
    pub sai_frozen_term_size_warn_threshold: Option<DataSize>,
    pub sai_frozen_term_size_fail_threshold: Option<DataSize>,
    pub sai_vector_term_size_warn_threshold: Option<DataSize>,
    pub sai_vector_term_size_fail_threshold: Option<DataSize>,

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
    pub secondary_indexes_enabled: bool,
    pub user_timestamps_enabled: bool,
    pub drop_truncate_table_enabled: bool,
    pub bulk_load_enabled: bool,
    pub read_before_write_list_operations_enabled: bool,
    pub simplestrategy_enabled: bool,
    pub alter_table_enabled: bool,
    pub data_disk_usage_keyspace_wide_protection_enabled: bool,
    pub default_secondary_index_enabled: bool,
    pub zero_ttl_on_twcs_enabled: bool,
    pub zero_ttl_on_twcs_warned: bool,
    pub non_partition_restricted_index_query_enabled: bool,
    pub unset_training_min_frequency_warned: bool,
    pub unset_training_min_frequency_enabled: bool,

    // ─── Read/Write Consistency ──────────────────────────────────────────
    /// Minimum read consistency level allowed.
    pub minimum_replication_factor: ThresholdGuardrail,
    pub table_properties_warned: Vec<String>,
    pub table_properties_ignored: Vec<String>,
    pub table_properties_disallowed: Vec<String>,
    pub keyspace_properties_warned: Vec<String>,
    pub keyspace_properties_ignored: Vec<String>,
    pub keyspace_properties_disallowed: Vec<String>,
    pub read_consistency_levels_warned: Vec<String>,
    pub read_consistency_levels_disallowed: Vec<String>,
    pub write_consistency_levels_warned: Vec<String>,
    pub write_consistency_levels_disallowed: Vec<String>,
    pub maximum_timestamp_warn_threshold: Option<Duration>,
    pub maximum_timestamp_fail_threshold: Option<Duration>,
    pub minimum_timestamp_warn_threshold: Option<Duration>,
    pub minimum_timestamp_fail_threshold: Option<Duration>,
    pub role_name_policy: Option<RoleNamePolicyConfig>,
    pub role_name_policy_reconfiguration_enabled: bool,
    pub password_policy: Option<PasswordPolicyConfig>,
    pub password_policy_reconfiguration_enabled: bool,
    pub sai_sstable_indexes_per_query_warn_threshold: i64,
    pub sai_sstable_indexes_per_query_fail_threshold: i64,
    pub default_secondary_index: String,
}

/// Role-name validator/generator policy.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct RoleNamePolicyConfig {
    pub validator_class_name: Option<String>,
    pub generator_class_name: Option<String>,
    pub min_generated_name_size: Option<u32>,
}

/// Password validator/generator policy configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(default)]
pub struct PasswordPolicyConfig {
    pub validator_class_name: Option<String>,
    pub generator_class_name: Option<String>,
    pub characteristic_warn: i64,
    pub characteristic_fail: i64,
    pub max_length: u32,
    pub length_warn: i64,
    pub length_fail: i64,
    pub upper_case_warn: i64,
    pub upper_case_fail: i64,
    pub lower_case_warn: i64,
    pub lower_case_fail: i64,
    pub digit_warn: i64,
    pub digit_fail: i64,
    pub special_warn: i64,
    pub special_fail: i64,
    pub illegal_sequence_length: u32,
    pub dictionary: Option<String>,
    pub detailed_messages: bool,
}

impl Default for PasswordPolicyConfig {
    fn default() -> Self {
        Self {
            validator_class_name: None,
            generator_class_name: None,
            characteristic_warn: 3,
            characteristic_fail: 2,
            max_length: 1000,
            length_warn: 12,
            length_fail: 8,
            upper_case_warn: 2,
            upper_case_fail: 1,
            lower_case_warn: 2,
            lower_case_fail: 1,
            digit_warn: 2,
            digit_fail: 1,
            special_warn: 2,
            special_fail: 1,
            illegal_sequence_length: 5,
            dictionary: None,
            detailed_messages: true,
        }
    }
}

impl Default for GuardrailsConfig {
    fn default() -> Self {
        Self {
            tables_per_keyspace: ThresholdGuardrail::default(),
            columns_per_table: ThresholdGuardrail::default(),
            secondary_indexes_per_table: ThresholdGuardrail::default(),
            materialized_views_per_table: ThresholdGuardrail::default(),
            fields_per_udt: ThresholdGuardrail::default(),
            keyspaces_warn_threshold: -1,
            keyspaces_fail_threshold: -1,
            tables_warn_threshold: -1,
            tables_fail_threshold: -1,
            columns_per_table_warn_threshold: -1,
            columns_per_table_fail_threshold: -1,
            secondary_indexes_per_table_warn_threshold: -1,
            secondary_indexes_per_table_fail_threshold: -1,
            materialized_views_per_table_warn_threshold: -1,
            materialized_views_per_table_fail_threshold: -1,
            page_size_warn_threshold: -1,
            page_size_fail_threshold: -1,
            partition_keys_in_select_warn_threshold: -1,
            partition_keys_in_select_fail_threshold: -1,
            in_select_cartesian_product_warn_threshold: -1,
            in_select_cartesian_product_fail_threshold: -1,
            partition_tombstones_warn_threshold: -1,
            partition_tombstones_fail_threshold: -1,
            items_per_collection_warn_threshold: -1,
            items_per_collection_fail_threshold: -1,
            fields_per_udt_warn_threshold: -1,
            fields_per_udt_fail_threshold: -1,
            vector_dimensions_warn_threshold: -1,
            vector_dimensions_fail_threshold: -1,
            data_disk_usage_percentage_warn_threshold: -1,
            data_disk_usage_percentage_fail_threshold: -1,
            minimum_replication_factor_warn_threshold: -1,
            minimum_replication_factor_fail_threshold: -1,
            maximum_replication_factor_warn_threshold: -1,
            maximum_replication_factor_fail_threshold: -1,
            collection_size: ThresholdGuardrail::default(),
            item_size: ThresholdGuardrail::default(),
            partition_size_warn_threshold: None,
            partition_size_fail_threshold: None,
            column_value_size_warn_threshold: None,
            column_value_size_fail_threshold: None,
            column_ascii_value_size_warn_threshold: None,
            column_ascii_value_size_fail_threshold: None,
            column_blob_value_size_warn_threshold: None,
            column_blob_value_size_fail_threshold: None,
            column_text_and_varchar_value_size_warn_threshold: None,
            column_text_and_varchar_value_size_fail_threshold: None,
            collection_size_warn_threshold: None,
            collection_size_fail_threshold: None,
            collection_map_size_warn_threshold: None,
            collection_map_size_fail_threshold: None,
            collection_set_size_warn_threshold: None,
            collection_set_size_fail_threshold: None,
            collection_list_size_warn_threshold: None,
            collection_list_size_fail_threshold: None,
            data_disk_usage_max_disk_size: None,
            sai_string_term_size_warn_threshold: Some(DataSize::from_kibibytes(1)),
            sai_string_term_size_fail_threshold: Some(DataSize::from_kibibytes(8)),
            sai_frozen_term_size_warn_threshold: Some(DataSize::from_kibibytes(1)),
            sai_frozen_term_size_fail_threshold: Some(DataSize::from_kibibytes(8)),
            sai_vector_term_size_warn_threshold: Some(DataSize::from_kibibytes(16)),
            sai_vector_term_size_fail_threshold: Some(DataSize::from_kibibytes(32)),
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
            secondary_indexes_enabled: true,
            user_timestamps_enabled: true,
            drop_truncate_table_enabled: true,
            bulk_load_enabled: true,
            read_before_write_list_operations_enabled: true,
            simplestrategy_enabled: true,
            alter_table_enabled: true,
            data_disk_usage_keyspace_wide_protection_enabled: false,
            default_secondary_index_enabled: true,
            zero_ttl_on_twcs_enabled: true,
            zero_ttl_on_twcs_warned: true,
            non_partition_restricted_index_query_enabled: true,
            unset_training_min_frequency_warned: true,
            unset_training_min_frequency_enabled: true,
            minimum_replication_factor: ThresholdGuardrail::default(),
            table_properties_warned: Vec::new(),
            table_properties_ignored: Vec::new(),
            table_properties_disallowed: Vec::new(),
            keyspace_properties_warned: Vec::new(),
            keyspace_properties_ignored: Vec::new(),
            keyspace_properties_disallowed: Vec::new(),
            read_consistency_levels_warned: Vec::new(),
            read_consistency_levels_disallowed: Vec::new(),
            write_consistency_levels_warned: Vec::new(),
            write_consistency_levels_disallowed: Vec::new(),
            maximum_timestamp_warn_threshold: None,
            maximum_timestamp_fail_threshold: None,
            minimum_timestamp_warn_threshold: None,
            minimum_timestamp_fail_threshold: None,
            role_name_policy: None,
            role_name_policy_reconfiguration_enabled: true,
            password_policy: None,
            password_policy_reconfiguration_enabled: true,
            sai_sstable_indexes_per_query_warn_threshold: 32,
            sai_sstable_indexes_per_query_fail_threshold: -1,
            default_secondary_index: "legacy_local_table".to_string(),
        }
    }
}

impl GuardrailsConfig {
    /// Check a guardrail by name for a given value.
    /// Returns the action to take.
    pub fn check_threshold(&self, name: &str, value: i64) -> GuardrailAction {
        match name {
            "keyspaces" => check_flat_threshold(
                self.keyspaces_warn_threshold,
                self.keyspaces_fail_threshold,
                value,
            ),
            "tables" | "tables_per_keyspace" => prefer_structured_threshold(
                &self.tables_per_keyspace,
                self.tables_warn_threshold,
                self.tables_fail_threshold,
                value,
            ),
            "columns_per_table" => prefer_structured_threshold(
                &self.columns_per_table,
                self.columns_per_table_warn_threshold,
                self.columns_per_table_fail_threshold,
                value,
            ),
            "secondary_indexes_per_table" => prefer_structured_threshold(
                &self.secondary_indexes_per_table,
                self.secondary_indexes_per_table_warn_threshold,
                self.secondary_indexes_per_table_fail_threshold,
                value,
            ),
            "materialized_views_per_table" => prefer_structured_threshold(
                &self.materialized_views_per_table,
                self.materialized_views_per_table_warn_threshold,
                self.materialized_views_per_table_fail_threshold,
                value,
            ),
            "fields_per_udt" => prefer_structured_threshold(
                &self.fields_per_udt,
                self.fields_per_udt_warn_threshold,
                self.fields_per_udt_fail_threshold,
                value,
            ),
            "collection_size" => self.collection_size.check(value),
            "item_size" => self.item_size.check(value),
            "partition_size" => self.partition_size.check(value),
            "partition_tombstones" => prefer_structured_threshold(
                &self.partition_tombstones,
                self.partition_tombstones_warn_threshold,
                self.partition_tombstones_fail_threshold,
                value,
            ),
            "page_size" => prefer_structured_threshold(
                &self.page_size,
                self.page_size_warn_threshold,
                self.page_size_fail_threshold,
                value,
            ),
            "partition_keys_in_select" => check_flat_threshold(
                self.partition_keys_in_select_warn_threshold,
                self.partition_keys_in_select_fail_threshold,
                value,
            ),
            "in_select_cartesian_product" => prefer_structured_threshold(
                &self.in_select_cartesian_product,
                self.in_select_cartesian_product_warn_threshold,
                self.in_select_cartesian_product_fail_threshold,
                value,
            ),
            "items_per_collection" => check_flat_threshold(
                self.items_per_collection_warn_threshold,
                self.items_per_collection_fail_threshold,
                value,
            ),
            "vector_dimensions" => check_flat_threshold(
                self.vector_dimensions_warn_threshold,
                self.vector_dimensions_fail_threshold,
                value,
            ),
            "data_disk_usage_percentage" => check_flat_threshold(
                self.data_disk_usage_percentage_warn_threshold,
                self.data_disk_usage_percentage_fail_threshold,
                value,
            ),
            "minimum_replication_factor" => prefer_structured_threshold(
                &self.minimum_replication_factor,
                self.minimum_replication_factor_warn_threshold,
                self.minimum_replication_factor_fail_threshold,
                value,
            ),
            "maximum_replication_factor" => check_flat_threshold(
                self.maximum_replication_factor_warn_threshold,
                self.maximum_replication_factor_fail_threshold,
                value,
            ),
            "sai_sstable_indexes_per_query" => check_flat_threshold(
                self.sai_sstable_indexes_per_query_warn_threshold,
                self.sai_sstable_indexes_per_query_fail_threshold,
                value,
            ),
            _ => GuardrailAction::Disabled,
        }
    }

    /// Check a data-size guardrail by upstream name.
    pub fn check_size_threshold(&self, name: &str, value: DataSize) -> GuardrailAction {
        match name {
            "partition_size" => check_flat_size_threshold(
                self.partition_size_warn_threshold,
                self.partition_size_fail_threshold,
                value,
            ),
            "column_value_size" => check_flat_size_threshold(
                self.column_value_size_warn_threshold,
                self.column_value_size_fail_threshold,
                value,
            ),
            "column_ascii_value_size" => check_flat_size_threshold(
                self.column_ascii_value_size_warn_threshold,
                self.column_ascii_value_size_fail_threshold,
                value,
            ),
            "column_blob_value_size" => check_flat_size_threshold(
                self.column_blob_value_size_warn_threshold,
                self.column_blob_value_size_fail_threshold,
                value,
            ),
            "column_text_and_varchar_value_size" => check_flat_size_threshold(
                self.column_text_and_varchar_value_size_warn_threshold,
                self.column_text_and_varchar_value_size_fail_threshold,
                value,
            ),
            "collection_size" => check_flat_size_threshold(
                self.collection_size_warn_threshold,
                self.collection_size_fail_threshold,
                value,
            ),
            "collection_map_size" => check_flat_size_threshold(
                self.collection_map_size_warn_threshold,
                self.collection_map_size_fail_threshold,
                value,
            ),
            "collection_set_size" => check_flat_size_threshold(
                self.collection_set_size_warn_threshold,
                self.collection_set_size_fail_threshold,
                value,
            ),
            "collection_list_size" => check_flat_size_threshold(
                self.collection_list_size_warn_threshold,
                self.collection_list_size_fail_threshold,
                value,
            ),
            "sai_string_term_size" => check_flat_size_threshold(
                self.sai_string_term_size_warn_threshold,
                self.sai_string_term_size_fail_threshold,
                value,
            ),
            "sai_frozen_term_size" => check_flat_size_threshold(
                self.sai_frozen_term_size_warn_threshold,
                self.sai_frozen_term_size_fail_threshold,
                value,
            ),
            "sai_vector_term_size" => check_flat_size_threshold(
                self.sai_vector_term_size_warn_threshold,
                self.sai_vector_term_size_fail_threshold,
                value,
            ),
            _ => GuardrailAction::Disabled,
        }
    }

    /// Check a feature-flag guardrail by name.
    pub fn check_feature(&self, name: &str) -> GuardrailAction {
        let enabled = match name {
            "allow_filtering" => self.allow_filtering_enabled,
            "compact_tables" => self.compact_tables_enabled,
            "user_aggregates" => self.user_aggregates_enabled,
            "group_by" => self.group_by_enabled,
            "truncate" => self.truncate_enabled && self.drop_truncate_table_enabled,
            "drop_truncate_table" => self.drop_truncate_table_enabled,
            "drop_keyspace" => self.drop_keyspace_enabled,
            "uncompressed_tables" => self.uncompressed_tables_enabled,
            "secondary_indexes" => self.secondary_indexes_enabled,
            "user_timestamps" => self.user_timestamps_enabled,
            "bulk_load" => self.bulk_load_enabled,
            "read_before_write_list_operations" => self.read_before_write_list_operations_enabled,
            "simplestrategy" => self.simplestrategy_enabled,
            "alter_table" => self.alter_table_enabled,
            "default_secondary_index" => self.default_secondary_index_enabled,
            "zero_ttl_on_twcs" => self.zero_ttl_on_twcs_enabled,
            "non_partition_restricted_index_query" => {
                self.non_partition_restricted_index_query_enabled
            }
            "unset_training_min_frequency" => self.unset_training_min_frequency_enabled,
            _ => return GuardrailAction::Disabled,
        };
        if enabled {
            GuardrailAction::Disabled
        } else {
            GuardrailAction::Fail
        }
    }
}

fn prefer_structured_threshold(
    structured: &ThresholdGuardrail,
    warn: i64,
    fail: i64,
    value: i64,
) -> GuardrailAction {
    let action = structured.check(value);
    if action == GuardrailAction::Disabled {
        check_flat_threshold(warn, fail, value)
    } else {
        action
    }
}

fn check_flat_threshold(warn: i64, fail: i64, value: i64) -> GuardrailAction {
    if fail >= 0 && value >= fail {
        GuardrailAction::Fail
    } else if warn >= 0 && value >= warn {
        GuardrailAction::Warn
    } else {
        GuardrailAction::Disabled
    }
}

fn check_flat_size_threshold(
    warn: Option<DataSize>,
    fail: Option<DataSize>,
    value: DataSize,
) -> GuardrailAction {
    if let Some(fail) = fail {
        if value >= fail {
            return GuardrailAction::Fail;
        }
    }
    if let Some(warn) = warn {
        if value >= warn {
            return GuardrailAction::Warn;
        }
    }
    GuardrailAction::Disabled
}

// ─── Guardrail Enforcement Framework ──────────────────────────────────────

/// A violation produced when a guardrail check fails.
#[derive(Debug, Clone)]
pub struct GuardrailViolation {
    pub guardrail_name: String,
    pub message: String,
    pub action: GuardrailAction,
}

impl fmt::Display for GuardrailViolation {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let level = match self.action {
            GuardrailAction::Warn => "WARNING",
            GuardrailAction::Fail => "REJECTED",
            GuardrailAction::Disabled => "OK",
        };
        write!(f, "[{}] {}: {}", level, self.guardrail_name, self.message)
    }
}

impl std::error::Error for GuardrailViolation {}

/// Trait for guardrail implementations.
pub trait Guardrail: Send + Sync {
    /// The name of this guardrail (for error messages and logging).
    fn name(&self) -> &str;

    /// Check the guardrail, returning a violation if triggered.
    fn check(&self, config: &GuardrailsConfig) -> Option<GuardrailViolation>;
}

/// A guardrail that checks whether a feature is enabled.
pub struct EnableFlagGuardrail {
    pub feature_name: &'static str,
    pub display_name: &'static str,
}

impl Guardrail for EnableFlagGuardrail {
    fn name(&self) -> &str {
        self.display_name
    }

    fn check(&self, config: &GuardrailsConfig) -> Option<GuardrailViolation> {
        match config.check_feature(self.feature_name) {
            GuardrailAction::Fail => Some(GuardrailViolation {
                guardrail_name: self.display_name.to_string(),
                message: format!("{} is not allowed", self.display_name),
                action: GuardrailAction::Fail,
            }),
            _ => None,
        }
    }
}

/// A guardrail that checks a numeric value against a threshold.
pub struct ValuesGuardrail<F: Fn() -> i64 + Send + Sync> {
    pub threshold_name: &'static str,
    pub value_fn: F,
}

impl<F: Fn() -> i64 + Send + Sync> Guardrail for ValuesGuardrail<F> {
    fn name(&self) -> &str {
        self.threshold_name
    }

    fn check(&self, config: &GuardrailsConfig) -> Option<GuardrailViolation> {
        let value = (self.value_fn)();
        let action = config.check_threshold(self.threshold_name, value);
        match action {
            GuardrailAction::Disabled => None,
            _ => Some(GuardrailViolation {
                guardrail_name: self.threshold_name.to_string(),
                message: format!(
                    "{} = {} exceeds {} threshold",
                    self.threshold_name,
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
}

/// Password policy guardrail.
pub struct PasswordPolicyGuardrail {
    pub min_length: usize,
    pub require_uppercase: bool,
    pub require_digit: bool,
}

impl Default for PasswordPolicyGuardrail {
    fn default() -> Self {
        Self {
            min_length: 8,
            require_uppercase: true,
            require_digit: true,
        }
    }
}

impl PasswordPolicyGuardrail {
    /// Check a password against the policy.
    pub fn check_password(&self, password: &str) -> Option<GuardrailViolation> {
        if password.len() < self.min_length {
            return Some(GuardrailViolation {
                guardrail_name: "password_policy".to_string(),
                message: format!("password must be at least {} characters", self.min_length),
                action: GuardrailAction::Fail,
            });
        }
        if self.require_uppercase && !password.chars().any(|c| c.is_uppercase()) {
            return Some(GuardrailViolation {
                guardrail_name: "password_policy".to_string(),
                message: "password must contain at least one uppercase letter".to_string(),
                action: GuardrailAction::Fail,
            });
        }
        if self.require_digit && !password.chars().any(|c| c.is_ascii_digit()) {
            return Some(GuardrailViolation {
                guardrail_name: "password_policy".to_string(),
                message: "password must contain at least one digit".to_string(),
                action: GuardrailAction::Fail,
            });
        }
        None
    }
}

/// Registry for all active guardrails.
pub struct GuardrailRegistry {
    guardrails: Vec<Box<dyn Guardrail>>,
}

impl GuardrailRegistry {
    pub fn new() -> Self {
        Self {
            guardrails: Vec::new(),
        }
    }

    /// Register a guardrail.
    pub fn register(&mut self, guardrail: Box<dyn Guardrail>) {
        self.guardrails.push(guardrail);
    }

    /// Run all registered guardrails, returning any violations.
    pub fn check_all(&self, config: &GuardrailsConfig) -> Vec<GuardrailViolation> {
        self.guardrails
            .iter()
            .filter_map(|g| g.check(config))
            .collect()
    }

    /// Check all guardrails and return `Err` if any are `Fail`.
    pub fn enforce(
        &self,
        config: &GuardrailsConfig,
    ) -> Result<Vec<GuardrailViolation>, GuardrailViolation> {
        let violations = self.check_all(config);
        if let Some(fail) = violations
            .iter()
            .find(|v| v.action == GuardrailAction::Fail)
        {
            return Err(fail.clone());
        }
        // Return warnings only
        Ok(violations)
    }
}

impl Default for GuardrailRegistry {
    fn default() -> Self {
        Self::new()
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
        assert_eq!(
            g.check_threshold("columns_per_table", 10),
            GuardrailAction::Disabled
        );
        assert_eq!(
            g.check_threshold("columns_per_table", 20),
            GuardrailAction::Warn
        );
        assert_eq!(
            g.check_threshold("columns_per_table", 100),
            GuardrailAction::Fail
        );
        assert_eq!(g.check_threshold("unknown", 100), GuardrailAction::Disabled);
    }

    #[test]
    fn feature_flags_defaults() {
        let g = GuardrailsConfig::default();
        assert!(g.allow_filtering_enabled);
        assert!(!g.compact_tables_enabled);
        assert!(g.truncate_enabled);
        assert!(g.secondary_indexes_enabled);
        assert!(g.user_timestamps_enabled);
        assert_eq!(g.sai_sstable_indexes_per_query_warn_threshold, 32);
        assert_eq!(
            g.sai_string_term_size_warn_threshold.unwrap().kibibytes(),
            1
        );
    }

    #[test]
    fn serialization_roundtrip() {
        let g = GuardrailsConfig::default();
        let json = serde_json::to_string(&g).unwrap();
        let g2: GuardrailsConfig = serde_json::from_str(&json).unwrap();
        assert_eq!(g2.allow_filtering_enabled, g.allow_filtering_enabled);
    }

    #[test]
    fn deserialize_upstream_flat_guardrail_keys() {
        let yaml = r#"
keyspaces_warn_threshold: 3
keyspaces_fail_threshold: 5
tables_warn_threshold: 10
tables_fail_threshold: 12
secondary_indexes_enabled: false
table_properties_warned: [comment]
table_properties_ignored: [compaction]
table_properties_disallowed: [gc_grace_seconds]
keyspace_properties_warned: [durable_writes]
user_timestamps_enabled: false
maximum_timestamp_warn_threshold: 24h
maximum_timestamp_fail_threshold: 48h
drop_truncate_table_enabled: false
bulk_load_enabled: false
partition_keys_in_select_warn_threshold: 4
partition_keys_in_select_fail_threshold: 8
partition_size_warn_threshold: 100MiB
partition_size_fail_threshold: 1GiB
column_value_size_warn_threshold: 1MiB
column_value_size_fail_threshold: 2MiB
collection_map_size_warn_threshold: 512KiB
collection_map_size_fail_threshold: 1MiB
items_per_collection_warn_threshold: 100
items_per_collection_fail_threshold: 200
simplestrategy_enabled: false
vector_dimensions_warn_threshold: 1024
vector_dimensions_fail_threshold: 2048
data_disk_usage_percentage_warn_threshold: 80
data_disk_usage_percentage_fail_threshold: 90
data_disk_usage_max_disk_size: 2TiB
data_disk_usage_keyspace_wide_protection_enabled: true
minimum_replication_factor_warn_threshold: 2
minimum_replication_factor_fail_threshold: 1
maximum_replication_factor_warn_threshold: 5
maximum_replication_factor_fail_threshold: 7
role_name_policy:
  generator_class_name: UUIDRoleNameGenerator
  min_generated_name_size: 10
role_name_policy_reconfiguration_enabled: false
password_policy:
  validator_class_name: CassandraPasswordValidator
  generator_class_name: CassandraPasswordGenerator
  characteristic_warn: 3
  characteristic_fail: 2
  length_warn: 12
  length_fail: 8
  detailed_messages: false
password_policy_reconfiguration_enabled: false
zero_ttl_on_twcs_enabled: false
zero_ttl_on_twcs_warned: false
non_partition_restricted_index_query_enabled: false
sai_sstable_indexes_per_query_warn_threshold: 16
sai_sstable_indexes_per_query_fail_threshold: 32
sai_string_term_size_warn_threshold: 2KiB
sai_string_term_size_fail_threshold: 4KiB
sai_vector_term_size_warn_threshold: 8KiB
sai_vector_term_size_fail_threshold: 16KiB
default_secondary_index: sai
default_secondary_index_enabled: false
unset_training_min_frequency_warned: false
unset_training_min_frequency_enabled: false
"#;
        let g: GuardrailsConfig = serde_yaml::from_str(yaml).unwrap();

        assert_eq!(g.check_threshold("keyspaces", 4), GuardrailAction::Warn);
        assert_eq!(g.check_threshold("keyspaces", 5), GuardrailAction::Fail);
        assert_eq!(g.check_threshold("tables", 11), GuardrailAction::Warn);
        assert_eq!(
            g.check_threshold("partition_keys_in_select", 8),
            GuardrailAction::Fail
        );
        assert_eq!(
            g.check_size_threshold("partition_size", DataSize::from_mebibytes(100)),
            GuardrailAction::Warn
        );
        assert_eq!(
            g.check_size_threshold("partition_size", DataSize::from_gibibytes(1)),
            GuardrailAction::Fail
        );
        assert_eq!(
            g.check_size_threshold("collection_map_size", DataSize::from_kibibytes(512)),
            GuardrailAction::Warn
        );
        assert_eq!(g.check_feature("secondary_indexes"), GuardrailAction::Fail);
        assert_eq!(g.check_feature("user_timestamps"), GuardrailAction::Fail);
        assert_eq!(g.check_feature("truncate"), GuardrailAction::Fail);
        assert_eq!(g.table_properties_warned, vec!["comment".to_string()]);
        assert_eq!(
            g.keyspace_properties_warned,
            vec!["durable_writes".to_string()]
        );
        assert_eq!(
            g.maximum_timestamp_warn_threshold.unwrap().millis(),
            86_400_000
        );
        assert_eq!(
            g.data_disk_usage_max_disk_size.unwrap().mebibytes(),
            2_097_152
        );
        assert_eq!(
            g.role_name_policy
                .as_ref()
                .unwrap()
                .generator_class_name
                .as_deref(),
            Some("UUIDRoleNameGenerator")
        );
        assert_eq!(
            g.password_policy
                .as_ref()
                .unwrap()
                .validator_class_name
                .as_deref(),
            Some("CassandraPasswordValidator")
        );
        assert!(!g.password_policy.as_ref().unwrap().detailed_messages);
        assert_eq!(
            g.check_size_threshold("sai_string_term_size", DataSize::from_kibibytes(4)),
            GuardrailAction::Fail
        );
        assert_eq!(g.default_secondary_index, "sai");
        assert_eq!(
            g.check_feature("default_secondary_index"),
            GuardrailAction::Fail
        );
        assert_eq!(
            g.check_feature("unset_training_min_frequency"),
            GuardrailAction::Fail
        );
    }

    #[test]
    fn check_feature_disabled() {
        let mut g = GuardrailsConfig::default();
        g.truncate_enabled = false;
        assert_eq!(g.check_feature("truncate"), GuardrailAction::Fail);
        assert_eq!(
            g.check_feature("allow_filtering"),
            GuardrailAction::Disabled
        );
    }

    #[test]
    fn enable_flag_guardrail() {
        let guard = EnableFlagGuardrail {
            feature_name: "truncate",
            display_name: "TRUNCATE",
        };
        let mut config = GuardrailsConfig::default();
        assert!(guard.check(&config).is_none());

        config.truncate_enabled = false;
        let v = guard.check(&config).unwrap();
        assert_eq!(v.action, GuardrailAction::Fail);
    }

    #[test]
    fn values_guardrail() {
        let guard = ValuesGuardrail {
            threshold_name: "columns_per_table",
            value_fn: || 50,
        };
        let mut config = GuardrailsConfig::default();
        config.columns_per_table = ThresholdGuardrail {
            warn_threshold: Some(20),
            fail_threshold: Some(100),
        };
        let v = guard.check(&config).unwrap();
        assert_eq!(v.action, GuardrailAction::Warn);
    }

    #[test]
    fn password_policy_basic() {
        let policy = PasswordPolicyGuardrail::default();
        assert!(policy.check_password("short").is_some());
        assert!(policy.check_password("alllowercase1").is_some());
        assert!(policy.check_password("NoDigitsHere").is_some());
        assert!(policy.check_password("Valid1Pass").is_none());
    }

    #[test]
    fn guardrail_registry() {
        let mut registry = GuardrailRegistry::new();
        registry.register(Box::new(EnableFlagGuardrail {
            feature_name: "truncate",
            display_name: "TRUNCATE",
        }));

        let config = GuardrailsConfig::default();
        let violations = registry.check_all(&config);
        assert!(violations.is_empty());

        let mut config2 = GuardrailsConfig::default();
        config2.truncate_enabled = false;
        let violations = registry.check_all(&config2);
        assert_eq!(violations.len(), 1);
        assert!(registry.enforce(&config2).is_err());
    }

    #[test]
    fn guardrail_violation_display() {
        let v = GuardrailViolation {
            guardrail_name: "test".to_string(),
            message: "exceeded".to_string(),
            action: GuardrailAction::Warn,
        };
        assert!(v.to_string().contains("WARNING"));
    }
}
