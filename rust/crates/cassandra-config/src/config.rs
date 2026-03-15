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
    pub authenticator: Option<AuthenticatorConfig>,

    #[serde(default)]
    pub authorizer: Option<AuthorizerConfig>,

    #[serde(default)]
    pub role_manager: Option<String>,

    // ── TLS / Encryption ──
    #[serde(default)]
    pub client_encryption_options: Option<EncryptionOptions>,

    #[serde(default)]
    pub server_encryption_options: Option<EncryptionOptions>,

    // ── Audit Logging ──
    #[serde(default)]
    pub audit_logging_options: Option<AuditLoggingConfig>,

    // ── Full Query Logging ──
    #[serde(default)]
    pub full_query_logging_options: Option<FqlConfig>,

    // ── Admin HTTP ──
    #[serde(default = "defaults::admin_port")]
    pub admin_port: u16,

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

// ─── Auth Config ───────────────────────────────────────────────────────────

/// Authenticator configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct AuthenticatorConfig {
    /// Class name or short name: "AllowAllAuthenticator", "PasswordAuthenticator"
    pub class_name: String,
    /// Additional parameters.
    #[serde(default)]
    pub parameters: std::collections::HashMap<String, String>,
}

/// Authorizer configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct AuthorizerConfig {
    /// Class name: "AllowAllAuthorizer", "CassandraAuthorizer"
    pub class_name: String,
    #[serde(default)]
    pub parameters: std::collections::HashMap<String, String>,
}

// ─── Encryption Config ─────────────────────────────────────────────────────

/// TLS/SSL encryption options matching Java's `EncryptionOptions`.
#[derive(Debug, Clone, Deserialize)]
pub struct EncryptionOptions {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub optional: bool,
    #[serde(default)]
    pub keystore: Option<String>,
    #[serde(default)]
    pub keystore_password: Option<String>,
    #[serde(default)]
    pub truststore: Option<String>,
    #[serde(default)]
    pub truststore_password: Option<String>,
    /// PEM certificate path (Rust-native alternative to JKS).
    #[serde(default)]
    pub certificate: Option<String>,
    /// PEM private key path.
    #[serde(default)]
    pub certificate_key: Option<String>,
    /// CA certificate path.
    #[serde(default)]
    pub ca_certificate: Option<String>,
    #[serde(default)]
    pub require_client_auth: bool,
    #[serde(default)]
    pub protocol: Option<String>,
    #[serde(default)]
    pub cipher_suites: Option<Vec<String>>,
}

// ─── Audit Config ──────────────────────────────────────────────────────────

/// Audit logging configuration in cassandra.yaml.
#[derive(Debug, Clone, Deserialize)]
pub struct AuditLoggingConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "defaults::audit_logger")]
    pub logger: String,
    #[serde(default)]
    pub audit_logs_dir: Option<String>,
    #[serde(default)]
    pub included_keyspaces: Option<String>,
    #[serde(default)]
    pub excluded_keyspaces: Option<String>,
    #[serde(default)]
    pub included_categories: Option<String>,
    #[serde(default)]
    pub excluded_categories: Option<String>,
    #[serde(default)]
    pub roll_cycle: Option<String>,
    #[serde(default)]
    pub max_log_size: Option<u64>,
}

/// FQL configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct FqlConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub log_dir: Option<String>,
    #[serde(default)]
    pub max_log_size_mb: Option<u64>,
    #[serde(default)]
    pub block: Option<bool>,
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
    pub fn admin_port() -> u16 { 9090 }
    pub fn audit_logger() -> String { "FileAuditLogger".to_string() }
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
        assert_eq!(cfg.admin_port, 9090);
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

    #[test]
    fn deserialize_with_encryption() {
        let yaml = r#"
cluster_name: SecureCluster
client_encryption_options:
  enabled: true
  certificate: "/etc/certs/node.crt"
  certificate_key: "/etc/certs/node.key"
  ca_certificate: "/etc/certs/ca.crt"
  require_client_auth: true
"#;
        let cfg: CassandraConfig = serde_yaml::from_str(yaml).unwrap();
        let enc = cfg.client_encryption_options.unwrap();
        assert!(enc.enabled);
        assert_eq!(enc.certificate.unwrap(), "/etc/certs/node.crt");
        assert!(enc.require_client_auth);
    }

    #[test]
    fn deserialize_with_authenticator() {
        let yaml = r#"
authenticator:
  class_name: PasswordAuthenticator
"#;
        let cfg: CassandraConfig = serde_yaml::from_str(yaml).unwrap();
        let auth = cfg.authenticator.unwrap();
        assert_eq!(auth.class_name, "PasswordAuthenticator");
    }

    #[test]
    fn deserialize_with_audit() {
        let yaml = r#"
audit_logging_options:
  enabled: true
  logger: FileAuditLogger
  audit_logs_dir: "/var/log/cassandra/audit"
"#;
        let cfg: CassandraConfig = serde_yaml::from_str(yaml).unwrap();
        let audit = cfg.audit_logging_options.unwrap();
        assert!(audit.enabled);
        assert_eq!(audit.audit_logs_dir.unwrap(), "/var/log/cassandra/audit");
    }

    #[test]
    fn deserialize_with_fql() {
        let yaml = r#"
full_query_logging_options:
  enabled: true
  log_dir: "/var/log/cassandra/fql"
  max_log_size_mb: 500
"#;
        let cfg: CassandraConfig = serde_yaml::from_str(yaml).unwrap();
        let fql = cfg.full_query_logging_options.unwrap();
        assert!(fql.enabled);
        assert_eq!(fql.max_log_size_mb.unwrap(), 500);
    }
}
