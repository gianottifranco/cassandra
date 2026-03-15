// Licensed under Apache License, Version 2.0.

//! Cassandra configuration structure.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.config.Config`
//! - `conf/cassandra.yaml`

use serde::Deserialize;
use crate::units::{DataSize, Duration};

/// Core Cassandra configuration loaded from `cassandra.yaml`.
///
/// Fields are named to match the YAML keys. Optional fields use `Option<T>`
/// and will fall back to defaults if not specified.
#[derive(Debug, Clone, Deserialize)]
pub struct CassandraConfig {
    // ── Cluster identity ──
    #[serde(default = "defaults::cluster_name")]
    pub cluster_name: String,

    #[serde(default = "defaults::num_tokens")]
    pub num_tokens: u32,

    #[serde(default)]
    pub allocate_tokens_for_local_replication_factor: Option<u32>,

    // ── Partitioner ──
    #[serde(default = "defaults::partitioner")]
    pub partitioner: String,

    // ── Storage directories ──
    #[serde(default)]
    pub data_file_directories: Option<Vec<String>>,

    #[serde(default)]
    pub commitlog_directory: Option<String>,

    #[serde(default)]
    pub hints_directory: Option<String>,

    #[serde(default)]
    pub saved_caches_directory: Option<String>,

    #[serde(default)]
    pub cdc_raw_directory: Option<String>,

    // ── Networking ──
    #[serde(default)]
    pub listen_address: Option<String>,

    #[serde(default)]
    pub listen_interface: Option<String>,

    #[serde(default)]
    pub rpc_address: Option<String>,

    #[serde(default = "defaults::storage_port")]
    pub storage_port: u16,

    #[serde(default = "defaults::ssl_storage_port")]
    pub ssl_storage_port: u16,

    #[serde(default = "defaults::native_transport_port")]
    pub native_transport_port: u16,

    // ── Thread pools ──
    #[serde(default = "defaults::concurrent_reads")]
    pub concurrent_reads: u32,

    #[serde(default = "defaults::concurrent_writes")]
    pub concurrent_writes: u32,

    #[serde(default = "defaults::concurrent_counter_writes")]
    pub concurrent_counter_writes: u32,

    #[serde(default = "defaults::concurrent_materialized_view_writes")]
    pub concurrent_materialized_view_writes: u32,

    // ── Hints ──
    #[serde(default = "defaults::hinted_handoff_enabled")]
    pub hinted_handoff_enabled: bool,

    #[serde(default)]
    pub max_hint_window: Option<Duration>,

    #[serde(default)]
    pub hinted_handoff_throttle: Option<DataSize>,

    #[serde(default = "defaults::max_hints_delivery_threads")]
    pub max_hints_delivery_threads: u32,

    // ── Commitlog ──
    #[serde(default = "defaults::commitlog_sync")]
    pub commitlog_sync: String,

    #[serde(default)]
    pub commitlog_sync_period: Option<Duration>,

    #[serde(default)]
    pub commitlog_segment_size: Option<DataSize>,

    #[serde(default)]
    pub commitlog_disk_access_mode: Option<String>,

    // ── Disk failure ──
    #[serde(default = "defaults::disk_failure_policy")]
    pub disk_failure_policy: String,

    #[serde(default = "defaults::commit_failure_policy")]
    pub commit_failure_policy: String,

    // ── Caches ──
    #[serde(default)]
    pub key_cache_size: Option<DataSize>,

    #[serde(default)]
    pub key_cache_save_period: Option<Duration>,

    #[serde(default)]
    pub row_cache_size: Option<DataSize>,

    // ── CDC ──
    #[serde(default)]
    pub cdc_enabled: Option<bool>,

    // ── Auth ──
    #[serde(default)]
    pub authenticator: Option<serde_yaml::Value>,

    #[serde(default)]
    pub authorizer: Option<serde_yaml::Value>,

    // ── Seed provider ──
    #[serde(default)]
    pub seed_provider: Option<serde_yaml::Value>,

    // ── Misc ──
    #[serde(default)]
    pub prepared_statements_cache_size: Option<DataSize>,

    // Catch-all for unrecognized fields (forwards compatibility)
    #[serde(flatten)]
    pub extra: std::collections::HashMap<String, serde_yaml::Value>,
}

/// Default values matching Java's Config class.
pub mod defaults {
    pub fn cluster_name() -> String { "Test Cluster".to_string() }
    pub fn num_tokens() -> u32 { 16 }
    pub fn partitioner() -> String { "org.apache.cassandra.dht.Murmur3Partitioner".to_string() }
    pub fn storage_port() -> u16 { 7000 }
    pub fn ssl_storage_port() -> u16 { 7001 }
    pub fn native_transport_port() -> u16 { 9042 }
    pub fn concurrent_reads() -> u32 { 32 }
    pub fn concurrent_writes() -> u32 { 32 }
    pub fn concurrent_counter_writes() -> u32 { 32 }
    pub fn concurrent_materialized_view_writes() -> u32 { 32 }
    pub fn hinted_handoff_enabled() -> bool { true }
    pub fn max_hints_delivery_threads() -> u32 { 2 }
    pub fn commitlog_sync() -> String { "periodic".to_string() }
    pub fn disk_failure_policy() -> String { "stop".to_string() }
    pub fn commit_failure_policy() -> String { "stop".to_string() }
}

impl Default for CassandraConfig {
    fn default() -> Self {
        // Deserialize from empty YAML to get all defaults
        serde_yaml::from_str("{}").expect("defaults should parse")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config() {
        let cfg = CassandraConfig::default();
        assert_eq!(cfg.cluster_name, "Test Cluster");
        assert_eq!(cfg.num_tokens, 16);
        assert_eq!(cfg.native_transport_port, 9042);
        assert_eq!(cfg.concurrent_reads, 32);
    }

    #[test]
    fn deserialize_minimal() {
        let yaml = r#"
cluster_name: MyCluster
num_tokens: 256
"#;
        let cfg: CassandraConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.cluster_name, "MyCluster");
        assert_eq!(cfg.num_tokens, 256);
        assert_eq!(cfg.native_transport_port, 9042); // default
    }

    #[test]
    fn extra_fields_captured() {
        let yaml = r#"
cluster_name: Test
some_unknown_field: 42
"#;
        let cfg: CassandraConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.extra.contains_key("some_unknown_field"));
    }
}
