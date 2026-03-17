// Licensed under Apache License, Version 2.0.

//! Cassandra configuration structure.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.config.Config`
//! - `conf/cassandra.yaml`

use crate::units::{DataSize, Duration};
use serde::Deserialize;

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

    // ── Native transport limits ──
    /// Maximum number of concurrent client connections (global).
    #[serde(default = "defaults::native_transport_max_concurrent_connections")]
    pub native_transport_max_concurrent_connections: i32,

    /// Maximum number of concurrent client connections per source IP.
    #[serde(default = "defaults::native_transport_max_concurrent_connections_per_ip")]
    pub native_transport_max_concurrent_connections_per_ip: i32,

    /// Maximum allowed frame size in bytes.
    #[serde(default = "defaults::native_transport_max_frame_size")]
    pub native_transport_max_frame_size: u64,

    /// Maximum bytes of request data allowed in-flight across all connections.
    #[serde(default = "defaults::native_transport_max_request_data_in_flight")]
    pub native_transport_max_request_data_in_flight: u64,

    /// Whether to enable native transport rate limiting.
    #[serde(default)]
    pub native_transport_rate_limiting_enabled: bool,

    /// Maximum requests per second when rate limiting is enabled.
    #[serde(default = "defaults::native_transport_max_requests_per_second")]
    pub native_transport_max_requests_per_second: u32,

    /// Idle timeout in seconds for native transport connections. 0 = disabled.
    #[serde(default)]
    pub native_transport_idle_timeout_seconds: u64,

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

    #[serde(default)]
    pub counter_cache_size: Option<DataSize>,

    #[serde(default)]
    pub counter_cache_save_period: Option<Duration>,

    #[serde(default)]
    pub chunk_cache_size: Option<DataSize>,

    // ── CDC ──
    #[serde(default)]
    pub cdc_enabled: Option<bool>,

    // ── Auth ──
    #[serde(default)]
    pub authenticator: Option<AuthenticatorConfig>,

    #[serde(default)]
    pub authorizer: Option<AuthorizerConfig>,

    #[serde(default)]
    pub role_manager: Option<RoleManagerConfig>,

    #[serde(default)]
    pub network_authorizer: Option<NetworkAuthorizerConfig>,

    #[serde(default)]
    pub internode_authenticator: Option<InternodeAuthenticatorConfig>,

    #[serde(default)]
    pub cidr_authorizer: Option<CidrAuthorizerConfig>,

    // ── Auth Cache ──
    /// Validity period for permissions cache in milliseconds.
    #[serde(default = "defaults::permissions_validity_ms")]
    pub permissions_validity_in_ms: u64,

    /// Max entries in the permissions cache.
    #[serde(default = "defaults::permissions_cache_max_entries")]
    pub permissions_cache_max_entries: usize,

    /// Validity period for roles cache in milliseconds.
    #[serde(default = "defaults::roles_validity_ms")]
    pub roles_validity_in_ms: u64,

    /// Max entries in the roles cache.
    #[serde(default = "defaults::roles_cache_max_entries")]
    pub roles_cache_max_entries: usize,

    /// Validity period for credentials cache in milliseconds.
    #[serde(default = "defaults::credentials_validity_ms")]
    pub credentials_validity_in_ms: u64,

    /// Max entries in the credentials cache.
    #[serde(default = "defaults::credentials_cache_max_entries")]
    pub credentials_cache_max_entries: usize,

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

/// Role manager configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct RoleManagerConfig {
    /// Class name: "CassandraRoleManager", "InMemoryRoleManager"
    pub class_name: String,
    #[serde(default)]
    pub parameters: std::collections::HashMap<String, String>,
}

/// Network authorizer configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct NetworkAuthorizerConfig {
    /// Class name: "AllowAllNetworkAuthorizer", "CassandraNetworkAuthorizer"
    pub class_name: String,
    #[serde(default)]
    pub parameters: std::collections::HashMap<String, String>,
}

/// Internode authenticator configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct InternodeAuthenticatorConfig {
    /// Class name: "AllowAllInternodeAuthenticator", "MutualTlsInternodeAuthenticator"
    pub class_name: String,
    #[serde(default)]
    pub parameters: std::collections::HashMap<String, String>,
}

/// CIDR authorizer configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct CidrAuthorizerConfig {
    /// Whether CIDR-based authorization is enabled.
    #[serde(default)]
    pub enabled: bool,
    /// Whether to deny by default when no CIDR rules exist.
    #[serde(default)]
    pub deny_by_default: bool,
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
    pub fn cluster_name() -> String {
        "Test Cluster".to_string()
    }
    pub fn num_tokens() -> u32 {
        16
    }
    pub fn partitioner() -> String {
        "org.apache.cassandra.dht.Murmur3Partitioner".to_string()
    }
    pub fn storage_port() -> u16 {
        7000
    }
    pub fn ssl_storage_port() -> u16 {
        7001
    }
    pub fn native_transport_port() -> u16 {
        9042
    }
    pub fn concurrent_reads() -> u32 {
        32
    }
    pub fn concurrent_writes() -> u32 {
        32
    }
    pub fn concurrent_counter_writes() -> u32 {
        32
    }
    pub fn concurrent_materialized_view_writes() -> u32 {
        32
    }
    pub fn hinted_handoff_enabled() -> bool {
        true
    }
    pub fn max_hints_delivery_threads() -> u32 {
        2
    }
    pub fn commitlog_sync() -> String {
        "periodic".to_string()
    }
    pub fn disk_failure_policy() -> String {
        "stop".to_string()
    }
    pub fn commit_failure_policy() -> String {
        "stop".to_string()
    }
    pub fn admin_port() -> u16 {
        9090
    }
    pub fn audit_logger() -> String {
        "FileAuditLogger".to_string()
    }
    /// -1 means unlimited (Java default).
    pub fn native_transport_max_concurrent_connections() -> i32 {
        -1
    }
    /// -1 means unlimited (Java default).
    pub fn native_transport_max_concurrent_connections_per_ip() -> i32 {
        -1
    }
    /// 256 MiB (Java default).
    pub fn native_transport_max_frame_size() -> u64 {
        256 * 1024 * 1024
    }
    /// ~1/2 of heap; we use a fixed 512 MiB default.
    pub fn native_transport_max_request_data_in_flight() -> u64 {
        512 * 1024 * 1024
    }
    pub fn native_transport_max_requests_per_second() -> u32 {
        25_000
    }
    /// Default permissions cache validity: 2000ms (Java default).
    pub fn permissions_validity_ms() -> u64 {
        2000
    }
    pub fn permissions_cache_max_entries() -> usize {
        1000
    }
    /// Default roles cache validity: 2000ms (Java default).
    pub fn roles_validity_ms() -> u64 {
        2000
    }
    pub fn roles_cache_max_entries() -> usize {
        1000
    }
    /// Default credentials cache validity: 2000ms (Java default).
    pub fn credentials_validity_ms() -> u64 {
        2000
    }
    pub fn credentials_cache_max_entries() -> usize {
        1000
    }
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
    fn native_transport_defaults() {
        let cfg = CassandraConfig::default();
        assert_eq!(cfg.native_transport_max_concurrent_connections, -1);
        assert_eq!(cfg.native_transport_max_concurrent_connections_per_ip, -1);
        assert_eq!(cfg.native_transport_max_frame_size, 256 * 1024 * 1024);
        assert_eq!(cfg.native_transport_max_request_data_in_flight, 512 * 1024 * 1024);
        assert!(!cfg.native_transport_rate_limiting_enabled);
        assert_eq!(cfg.native_transport_max_requests_per_second, 25_000);
        assert_eq!(cfg.native_transport_idle_timeout_seconds, 0);
    }

    #[test]
    fn deserialize_native_transport_fields() {
        let yaml = r#"
native_transport_max_concurrent_connections: 1024
native_transport_max_concurrent_connections_per_ip: 64
native_transport_max_frame_size: 16777216
native_transport_max_request_data_in_flight: 268435456
native_transport_rate_limiting_enabled: true
native_transport_max_requests_per_second: 10000
native_transport_idle_timeout_seconds: 300
"#;
        let cfg: CassandraConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.native_transport_max_concurrent_connections, 1024);
        assert_eq!(cfg.native_transport_max_concurrent_connections_per_ip, 64);
        assert_eq!(cfg.native_transport_max_frame_size, 16_777_216);
        assert_eq!(cfg.native_transport_max_request_data_in_flight, 268_435_456);
        assert!(cfg.native_transport_rate_limiting_enabled);
        assert_eq!(cfg.native_transport_max_requests_per_second, 10_000);
        assert_eq!(cfg.native_transport_idle_timeout_seconds, 300);
    }

    #[test]
    fn cache_config_defaults() {
        let cfg = CassandraConfig::default();
        assert!(cfg.key_cache_size.is_none());
        assert!(cfg.key_cache_save_period.is_none());
        assert!(cfg.row_cache_size.is_none());
        assert!(cfg.counter_cache_size.is_none());
        assert!(cfg.counter_cache_save_period.is_none());
        assert!(cfg.chunk_cache_size.is_none());
    }

    #[test]
    fn deserialize_cache_config() {
        let yaml = r#"
counter_cache_size: "50MiB"
counter_cache_save_period: "7200s"
chunk_cache_size: "32MiB"
"#;
        let cfg: CassandraConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(cfg.counter_cache_size.is_some());
        assert!(cfg.counter_cache_save_period.is_some());
        assert!(cfg.chunk_cache_size.is_some());
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
