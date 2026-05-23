// Licensed under Apache License, Version 2.0.

//! Cassandra configuration structure.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.config.Config`
//! - `conf/cassandra.yaml`

use crate::units::{DataRateSpec, DataSize, Duration};
use serde::{Deserialize, Deserializer};
use std::collections::HashMap;

fn duration_millis_from_yaml<'de, D>(deserializer: D) -> Result<u64, D::Error>
where
    D: Deserializer<'de>,
{
    Ok(Duration::deserialize(deserializer)?.millis())
}

fn audit_logger_from_yaml<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: Deserializer<'de>,
{
    let value = serde_yaml::Value::deserialize(deserializer)?;
    match value {
        serde_yaml::Value::String(logger) => Ok(logger),
        serde_yaml::Value::Sequence(loggers) => loggers
            .into_iter()
            .find_map(|logger| match logger {
                serde_yaml::Value::Mapping(map) => map
                    .get(serde_yaml::Value::String("class_name".to_string()))
                    .and_then(serde_yaml::Value::as_str)
                    .map(str::to_string),
                _ => None,
            })
            .ok_or_else(|| serde::de::Error::custom("audit logger list requires class_name")),
        serde_yaml::Value::Mapping(map) => map
            .get(serde_yaml::Value::String("class_name".to_string()))
            .and_then(serde_yaml::Value::as_str)
            .map(str::to_string)
            .ok_or_else(|| serde::de::Error::custom("audit logger map requires class_name")),
        serde_yaml::Value::Null => Ok(defaults::audit_logger()),
        other => Err(serde::de::Error::custom(format!(
            "unsupported audit logger value: {other:?}"
        ))),
    }
}

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

    #[serde(default = "defaults::auto_bootstrap")]
    pub auto_bootstrap: bool,

    #[serde(default)]
    pub initial_token: Option<String>,

    // ── Partitioner ──
    #[serde(default = "defaults::partitioner")]
    pub partitioner: String,

    #[serde(default = "defaults::endpoint_snitch")]
    pub endpoint_snitch: String,

    // ── Storage directories ──
    #[serde(default)]
    pub data_file_directories: Option<Vec<String>>,

    #[serde(default)]
    pub local_system_data_file_directory: Option<String>,

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
    pub listen_interface_prefer_ipv6: bool,

    #[serde(default)]
    pub broadcast_address: Option<String>,

    #[serde(default)]
    pub listen_on_broadcast_address: bool,

    #[serde(default)]
    pub rpc_address: Option<String>,

    #[serde(default)]
    pub rpc_interface: Option<String>,

    #[serde(default)]
    pub rpc_interface_prefer_ipv6: bool,

    #[serde(default)]
    pub broadcast_rpc_address: Option<String>,

    #[serde(default = "defaults::rpc_keepalive")]
    pub rpc_keepalive: bool,

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

    #[serde(default)]
    pub native_transport_flush_in_batches_legacy: bool,

    /// Idle timeout in seconds for native transport connections. 0 = disabled.
    #[serde(default)]
    pub native_transport_idle_timeout_seconds: u64,

    /// Upstream duration-form idle timeout. 0 = disabled.
    #[serde(default = "defaults::native_transport_idle_timeout")]
    pub native_transport_idle_timeout: Duration,

    #[serde(default = "defaults::native_transport_allow_older_protocols")]
    pub native_transport_allow_older_protocols: bool,

    #[serde(default = "defaults::start_native_transport")]
    pub start_native_transport: bool,

    #[serde(default = "defaults::native_transport_max_threads")]
    pub native_transport_max_threads: u32,

    #[serde(default = "defaults::native_transport_max_auth_threads")]
    pub native_transport_max_auth_threads: u32,

    // ── Request timeouts ──
    #[serde(default = "defaults::request_timeout")]
    pub request_timeout: Duration,

    #[serde(default = "defaults::read_request_timeout")]
    pub read_request_timeout: Duration,

    #[serde(default = "defaults::range_request_timeout")]
    pub range_request_timeout: Duration,

    #[serde(default = "defaults::write_request_timeout")]
    pub write_request_timeout: Duration,

    #[serde(default = "defaults::counter_write_request_timeout")]
    pub counter_write_request_timeout: Duration,

    #[serde(default = "defaults::cas_contention_timeout")]
    pub cas_contention_timeout: Duration,

    #[serde(default = "defaults::truncate_request_timeout")]
    pub truncate_request_timeout: Duration,

    #[serde(default = "defaults::repair_request_timeout")]
    pub repair_request_timeout: Duration,

    #[serde(default = "defaults::slow_query_log_timeout")]
    pub slow_query_log_timeout: Duration,

    #[serde(default = "defaults::internode_timeout")]
    pub internode_timeout: bool,

    // ── Thread pools ──
    #[serde(default = "defaults::concurrent_reads")]
    pub concurrent_reads: u32,

    #[serde(default = "defaults::concurrent_writes")]
    pub concurrent_writes: u32,

    #[serde(default = "defaults::concurrent_counter_writes")]
    pub concurrent_counter_writes: u32,

    #[serde(default = "defaults::concurrent_materialized_view_writes")]
    pub concurrent_materialized_view_writes: u32,

    #[serde(default = "defaults::concurrent_materialized_view_builders")]
    pub concurrent_materialized_view_builders: u32,

    // ── Hints ──
    #[serde(default = "defaults::hinted_handoff_enabled")]
    pub hinted_handoff_enabled: bool,

    #[serde(default)]
    pub hinted_handoff_disabled_datacenters: Option<Vec<String>>,

    #[serde(default)]
    pub max_hint_window: Option<Duration>,

    #[serde(default)]
    pub hinted_handoff_throttle: Option<DataSize>,

    #[serde(default = "defaults::max_hints_delivery_threads")]
    pub max_hints_delivery_threads: u32,

    #[serde(default = "defaults::hints_flush_period")]
    pub hints_flush_period: Duration,

    #[serde(default = "defaults::max_hints_file_size")]
    pub max_hints_file_size: DataSize,

    #[serde(default = "defaults::zero_data_size")]
    pub max_hints_size_per_host: DataSize,

    #[serde(default)]
    pub auto_hints_cleanup_enabled: bool,

    #[serde(default = "defaults::transfer_hints_on_decommission")]
    pub transfer_hints_on_decommission: bool,

    #[serde(default)]
    pub hints_compression: Option<Vec<ClassWithListParameters>>,

    #[serde(default)]
    pub heap_dump_path: Option<String>,

    #[serde(default)]
    pub dump_heap_on_uncaught_exception: bool,

    #[serde(default = "defaults::hint_window_persistent_enabled")]
    pub hint_window_persistent_enabled: bool,

    #[serde(default = "defaults::batchlog_replay_throttle")]
    pub batchlog_replay_throttle: DataSize,

    #[serde(default = "defaults::batchlog_endpoint_strategy")]
    pub batchlog_endpoint_strategy: String,

    // ── Commitlog ──
    #[serde(default = "defaults::commitlog_sync")]
    pub commitlog_sync: String,

    #[serde(default)]
    pub commitlog_sync_period: Option<Duration>,

    #[serde(default)]
    pub commitlog_segment_size: Option<DataSize>,

    #[serde(default)]
    pub commitlog_total_space: Option<DataSize>,

    #[serde(default)]
    pub commitlog_disk_access_mode: Option<String>,

    #[serde(default)]
    pub disk_access_mode: Option<String>,

    #[serde(default)]
    pub commitlog_compression: Option<Vec<ClassWithListParameters>>,

    #[serde(default = "defaults::compaction_read_disk_access_mode")]
    pub compaction_read_disk_access_mode: String,

    #[serde(default = "defaults::flush_compression")]
    pub flush_compression: String,

    #[serde(default)]
    pub commitlog_sync_group_window: Option<Duration>,

    #[serde(default)]
    pub periodic_commitlog_sync_lag_block: Option<Duration>,

    // ── Memtable / file cache ──
    #[serde(default)]
    pub memtable: Option<MemtableConfig>,

    #[serde(default)]
    pub memtable_heap_space: Option<DataSize>,

    #[serde(default)]
    pub memtable_offheap_space: Option<DataSize>,

    #[serde(default)]
    pub memtable_cleanup_threshold: Option<f64>,

    #[serde(default = "defaults::memtable_allocation_type")]
    pub memtable_allocation_type: String,

    #[serde(default)]
    pub memtable_flush_writers: u32,

    #[serde(default)]
    pub file_cache_size: Option<DataSize>,

    #[serde(default)]
    pub networking_cache_size: Option<DataSize>,

    #[serde(default)]
    pub file_cache_enabled: bool,

    #[serde(default = "defaults::buffer_pool_use_heap_if_exhausted")]
    pub buffer_pool_use_heap_if_exhausted: bool,

    #[serde(default = "defaults::disk_optimization_strategy")]
    pub disk_optimization_strategy: String,

    // ── Streaming / compaction throughput ──
    #[serde(default = "defaults::streaming_connections_per_host")]
    pub streaming_connections_per_host: u32,

    #[serde(default = "defaults::stream_throughput_outbound")]
    pub stream_throughput_outbound: DataRateSpec,

    #[serde(default = "defaults::inter_dc_stream_throughput_outbound")]
    pub inter_dc_stream_throughput_outbound: DataRateSpec,

    #[serde(default = "defaults::entire_sstable_stream_throughput_outbound")]
    pub entire_sstable_stream_throughput_outbound: DataRateSpec,

    #[serde(default = "defaults::entire_sstable_inter_dc_stream_throughput_outbound")]
    pub entire_sstable_inter_dc_stream_throughput_outbound: DataRateSpec,

    #[serde(default = "defaults::stream_entire_sstables")]
    pub stream_entire_sstables: bool,

    #[serde(default = "defaults::compaction_throughput")]
    pub compaction_throughput: DataRateSpec,

    #[serde(default)]
    pub concurrent_compactors: Option<u32>,

    #[serde(default)]
    pub concurrent_validations: Option<u32>,

    #[serde(default)]
    pub repair_session_space: Option<DataSize>,

    #[serde(default)]
    pub concurrent_merkle_tree_requests: u32,

    #[serde(default)]
    pub trickle_fsync: bool,

    #[serde(default = "defaults::trickle_fsync_interval")]
    pub trickle_fsync_interval: DataSize,

    // ── Internode messaging / streaming state ──
    #[serde(default = "defaults::internode_socket_buffer_size")]
    pub internode_socket_send_buffer_size: DataSize,

    #[serde(default = "defaults::internode_socket_buffer_size")]
    pub internode_socket_receive_buffer_size: DataSize,

    #[serde(default = "defaults::internode_application_queue_capacity")]
    pub internode_application_send_queue_capacity: DataSize,

    #[serde(default = "defaults::internode_application_queue_reserve_endpoint_capacity")]
    pub internode_application_send_queue_reserve_endpoint_capacity: DataSize,

    #[serde(default = "defaults::internode_application_queue_reserve_global_capacity")]
    pub internode_application_send_queue_reserve_global_capacity: DataSize,

    #[serde(default = "defaults::internode_application_queue_capacity")]
    pub internode_application_receive_queue_capacity: DataSize,

    #[serde(default = "defaults::internode_application_queue_reserve_endpoint_capacity")]
    pub internode_application_receive_queue_reserve_endpoint_capacity: DataSize,

    #[serde(default = "defaults::internode_application_queue_reserve_global_capacity")]
    pub internode_application_receive_queue_reserve_global_capacity: DataSize,

    #[serde(default = "defaults::internode_tcp_connect_timeout")]
    pub internode_tcp_connect_timeout: Duration,

    #[serde(default = "defaults::internode_tcp_user_timeout")]
    pub internode_tcp_user_timeout: Duration,

    #[serde(default = "defaults::internode_streaming_tcp_user_timeout")]
    pub internode_streaming_tcp_user_timeout: Duration,

    #[serde(default = "defaults::streaming_keep_alive_period")]
    pub streaming_keep_alive_period: Duration,

    #[serde(default = "defaults::streaming_state_expires")]
    pub streaming_state_expires: Duration,

    #[serde(default = "defaults::streaming_state_size")]
    pub streaming_state_size: DataSize,

    #[serde(default = "defaults::streaming_stats_enabled")]
    pub streaming_stats_enabled: bool,

    // ── SSTable / snapshot storage ──
    #[serde(default)]
    pub incremental_backups: bool,

    #[serde(default)]
    pub snapshot_before_compaction: bool,

    #[serde(default = "defaults::auto_snapshot")]
    pub auto_snapshot: bool,

    #[serde(default)]
    pub auto_snapshot_ttl: Option<Duration>,

    #[serde(default)]
    pub snapshot_links_per_second: u64,

    #[serde(default)]
    pub sstable: Option<SstableConfig>,

    #[serde(default)]
    pub column_index_size: Option<DataSize>,

    #[serde(default = "defaults::column_index_cache_size")]
    pub column_index_cache_size: DataSize,

    #[serde(default)]
    pub default_compaction: Option<ParameterizedClass>,

    #[serde(default = "defaults::sstable_preemptive_open_interval")]
    pub sstable_preemptive_open_interval: Option<DataSize>,

    #[serde(default)]
    pub uuid_sstable_identifiers_enabled: bool,

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
    pub key_cache_keys_to_save: Option<u32>,

    #[serde(default)]
    pub row_cache_class_name: Option<String>,

    #[serde(default)]
    pub row_cache_size: Option<DataSize>,

    #[serde(default)]
    pub row_cache_save_period: Option<Duration>,

    #[serde(default)]
    pub row_cache_keys_to_save: Option<u32>,

    #[serde(default)]
    pub counter_cache_size: Option<DataSize>,

    #[serde(default)]
    pub counter_cache_save_period: Option<Duration>,

    #[serde(default)]
    pub counter_cache_keys_to_save: Option<u32>,

    #[serde(default)]
    pub chunk_cache_size: Option<DataSize>,

    #[serde(default = "defaults::cache_load_timeout")]
    pub cache_load_timeout: Duration,

    // ── CDC ──
    #[serde(default)]
    pub cdc_enabled: Option<bool>,

    #[serde(default = "defaults::cdc_block_writes")]
    pub cdc_block_writes: bool,

    #[serde(default = "defaults::cdc_on_repair_enabled")]
    pub cdc_on_repair_enabled: bool,

    #[serde(default = "defaults::cdc_total_space")]
    pub cdc_total_space: DataSize,

    #[serde(default = "defaults::cdc_free_space_check_interval")]
    pub cdc_free_space_check_interval: Duration,

    // ── SSTable index summary ──
    #[serde(default)]
    pub index_summary_capacity: Option<DataSize>,

    #[serde(default = "defaults::index_summary_resize_interval")]
    pub index_summary_resize_interval: Option<Duration>,

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

    #[serde(default)]
    pub traverse_auth_from_root: bool,

    // ── Auth Cache ──
    /// Validity period for permissions cache in milliseconds.
    #[serde(
        default = "defaults::permissions_validity_ms",
        alias = "permissions_validity",
        deserialize_with = "duration_millis_from_yaml"
    )]
    pub permissions_validity_in_ms: u64,

    /// Max entries in the permissions cache.
    #[serde(default = "defaults::permissions_cache_max_entries")]
    pub permissions_cache_max_entries: usize,

    #[serde(default)]
    pub permissions_update_interval: Option<Duration>,

    #[serde(default)]
    pub permissions_cache_active_update: bool,

    /// Validity period for roles cache in milliseconds.
    #[serde(
        default = "defaults::roles_validity_ms",
        alias = "roles_validity",
        deserialize_with = "duration_millis_from_yaml"
    )]
    pub roles_validity_in_ms: u64,

    /// Max entries in the roles cache.
    #[serde(default = "defaults::roles_cache_max_entries")]
    pub roles_cache_max_entries: usize,

    #[serde(default)]
    pub roles_update_interval: Option<Duration>,

    #[serde(default)]
    pub roles_cache_active_update: bool,

    /// Validity period for credentials cache in milliseconds.
    #[serde(
        default = "defaults::credentials_validity_ms",
        alias = "credentials_validity",
        deserialize_with = "duration_millis_from_yaml"
    )]
    pub credentials_validity_in_ms: u64,

    /// Max entries in the credentials cache.
    #[serde(default = "defaults::credentials_cache_max_entries")]
    pub credentials_cache_max_entries: usize,

    #[serde(default)]
    pub credentials_update_interval: Option<Duration>,

    #[serde(default)]
    pub credentials_cache_active_update: bool,

    // ── TLS / Encryption ──
    #[serde(default)]
    pub crypto_provider: Option<Vec<ClassWithListParameters>>,

    #[serde(default)]
    pub client_encryption_options: Option<EncryptionOptions>,

    #[serde(default)]
    pub server_encryption_options: Option<EncryptionOptions>,

    #[serde(default)]
    pub transparent_data_encryption_options: Option<TransparentDataEncryptionConfig>,

    // ── Internode / tracing runtime ──
    #[serde(default = "defaults::internode_compression")]
    pub internode_compression: String,

    #[serde(default)]
    pub inter_dc_tcp_nodelay: bool,

    #[serde(default = "defaults::trace_type_query_ttl")]
    pub trace_type_query_ttl: Duration,

    #[serde(default = "defaults::trace_type_repair_ttl")]
    pub trace_type_repair_ttl: Duration,

    // ── Audit Logging ──
    #[serde(default)]
    pub audit_logging_options: Option<AuditLoggingConfig>,

    // ── Full Query Logging ──
    #[serde(default)]
    pub full_query_logging_options: Option<FqlConfig>,

    // ── JMX ──
    #[serde(default)]
    pub jmx_server_options: Option<JmxServerOptionsConfig>,

    // ── Safety thresholds / feature flags ──
    #[serde(default = "defaults::corrupted_tombstone_strategy")]
    pub corrupted_tombstone_strategy: String,

    #[serde(default)]
    pub gc_log_threshold: Option<Duration>,

    #[serde(default)]
    pub gc_warn_threshold: Option<Duration>,

    #[serde(default)]
    pub gc_concurrent_phase_log_threshold: Option<Duration>,

    #[serde(default)]
    pub gc_concurrent_phase_warn_threshold: Option<Duration>,

    #[serde(default = "defaults::max_value_size")]
    pub max_value_size: DataSize,

    #[serde(default = "defaults::default_keyspace_rf")]
    pub default_keyspace_rf: u32,

    #[serde(default)]
    pub ideal_consistency_level: Option<String>,

    #[serde(default)]
    pub automatic_sstable_upgrade: bool,

    #[serde(default = "defaults::max_concurrent_automatic_sstable_upgrades")]
    pub max_concurrent_automatic_sstable_upgrades: u32,

    #[serde(default = "defaults::sstables_per_read_log_threshold")]
    pub sstables_per_read_log_threshold: u32,

    #[serde(default = "defaults::tombstone_warn_threshold")]
    pub tombstone_warn_threshold: u32,

    #[serde(default = "defaults::tombstone_failure_threshold")]
    pub tombstone_failure_threshold: u32,

    #[serde(default)]
    pub replica_filtering_protection: ReplicaFilteringProtectionConfig,

    #[serde(default = "defaults::batch_size_warn_threshold")]
    pub batch_size_warn_threshold: DataSize,

    #[serde(default = "defaults::batch_size_fail_threshold")]
    pub batch_size_fail_threshold: DataSize,

    #[serde(default)]
    pub read_thresholds_enabled: bool,

    #[serde(default)]
    pub coordinator_read_size_warn_threshold: Option<DataSize>,

    #[serde(default)]
    pub coordinator_read_size_fail_threshold: Option<DataSize>,

    #[serde(default)]
    pub local_read_size_warn_threshold: Option<DataSize>,

    #[serde(default)]
    pub local_read_size_fail_threshold: Option<DataSize>,

    #[serde(default)]
    pub row_index_read_size_warn_threshold: Option<DataSize>,

    #[serde(default)]
    pub row_index_read_size_fail_threshold: Option<DataSize>,

    #[serde(default = "defaults::unlogged_batch_across_partitions_warn_threshold")]
    pub unlogged_batch_across_partitions_warn_threshold: u32,

    #[serde(default)]
    pub diagnostic_events_enabled: bool,

    #[serde(default)]
    pub repaired_data_tracking_for_range_reads_enabled: bool,

    #[serde(default)]
    pub repaired_data_tracking_for_partition_reads_enabled: bool,

    #[serde(default)]
    pub report_unconfirmed_repaired_data_mismatches: bool,

    #[serde(default)]
    pub auth_read_consistency_level: Option<String>,

    #[serde(default)]
    pub auth_write_consistency_level: Option<String>,

    #[serde(default)]
    pub auth_cache_warming_enabled: bool,

    #[serde(default)]
    pub dynamic_data_masking_enabled: bool,

    #[serde(default)]
    pub user_defined_functions_enabled: bool,

    #[serde(default = "defaults::triggers_policy")]
    pub triggers_policy: String,

    #[serde(default)]
    pub materialized_views_enabled: bool,

    #[serde(default = "defaults::materialized_views_on_repair_enabled")]
    pub materialized_views_on_repair_enabled: bool,

    #[serde(default)]
    pub sasi_indexes_enabled: bool,

    #[serde(default)]
    pub transient_replication_enabled: bool,

    #[serde(default)]
    pub drop_compact_storage_enabled: bool,

    #[serde(default = "defaults::use_statements_enabled")]
    pub use_statements_enabled: bool,

    #[serde(default)]
    pub client_error_reporting_exclusions: Option<ClientErrorReportingExclusions>,

    #[serde(default = "defaults::max_comment_length")]
    pub max_comment_length: u32,

    #[serde(default = "defaults::max_security_label_length")]
    pub max_security_label_length: u32,

    #[serde(default = "defaults::storage_compatibility_mode")]
    pub storage_compatibility_mode: String,

    #[serde(default)]
    pub startup_checks: Option<StartupChecksConfig>,

    #[serde(default)]
    pub accord: Option<AccordRuntimeConfig>,

    #[serde(default)]
    pub reject_repair_compaction_threshold: Option<u32>,

    #[serde(default)]
    pub repair_disk_headroom_reject_ratio: Option<f64>,

    #[serde(default)]
    pub incremental_repair_disk_headroom_reject_ratio: Option<f64>,

    #[serde(default)]
    pub auto_repair: Option<AutoRepairConfig>,

    #[serde(default = "defaults::compression_dictionary_refresh_interval")]
    pub compression_dictionary_refresh_interval: Duration,

    #[serde(default = "defaults::compression_dictionary_refresh_initial_delay")]
    pub compression_dictionary_refresh_initial_delay: Duration,

    #[serde(default = "defaults::compression_dictionary_cache_size")]
    pub compression_dictionary_cache_size: u32,

    #[serde(default = "defaults::compression_dictionary_cache_expire")]
    pub compression_dictionary_cache_expire: Duration,

    // ── Failure detection / dynamic snitch ──
    #[serde(default = "defaults::phi_convict_threshold")]
    pub phi_convict_threshold: f64,

    #[serde(default = "defaults::dynamic_snitch")]
    pub dynamic_snitch: bool,

    #[serde(default = "defaults::dynamic_snitch_update_interval")]
    pub dynamic_snitch_update_interval: Duration,

    #[serde(default = "defaults::dynamic_snitch_reset_interval")]
    pub dynamic_snitch_reset_interval: Duration,

    #[serde(default = "defaults::dynamic_snitch_badness_threshold")]
    pub dynamic_snitch_badness_threshold: f64,

    #[serde(default)]
    pub initial_location_provider: Option<String>,

    #[serde(default)]
    pub node_proximity: Option<String>,

    #[serde(default)]
    pub addresses_config: Option<String>,

    #[serde(default)]
    pub prefer_local_connections: bool,

    #[serde(default = "defaults::failure_detector")]
    pub failure_detector: String,

    // ── Partition denylist ──
    #[serde(default)]
    pub partition_denylist_enabled: bool,

    #[serde(default = "defaults::denylist_writes_enabled")]
    pub denylist_writes_enabled: bool,

    #[serde(default = "defaults::denylist_reads_enabled")]
    pub denylist_reads_enabled: bool,

    #[serde(default = "defaults::denylist_range_reads_enabled")]
    pub denylist_range_reads_enabled: bool,

    #[serde(default = "defaults::denylist_refresh")]
    pub denylist_refresh: Duration,

    #[serde(default = "defaults::denylist_initial_load_retry")]
    pub denylist_initial_load_retry: Duration,

    #[serde(default = "defaults::denylist_max_keys_per_table")]
    pub denylist_max_keys_per_table: u32,

    #[serde(default = "defaults::denylist_max_keys_total")]
    pub denylist_max_keys_total: u32,

    #[serde(default = "defaults::denylist_consistency_level")]
    pub denylist_consistency_level: String,

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
    pub extra: HashMap<String, serde_yaml::Value>,
}

// ─── Memtable Config ──────────────────────────────────────────────────────

/// Named memtable configurations from the top-level `memtable` YAML section.
#[derive(Debug, Clone, Deserialize)]
pub struct MemtableConfig {
    #[serde(default)]
    pub configurations: HashMap<String, MemtableConfiguration>,
}

/// Java-compatible inheriting memtable class descriptor.
#[derive(Debug, Clone, Deserialize)]
pub struct MemtableConfiguration {
    #[serde(default)]
    pub inherits: Option<String>,
    #[serde(default)]
    pub class_name: Option<String>,
    #[serde(default)]
    pub parameters: HashMap<String, serde_yaml::Value>,
}

// ─── Storage Config ───────────────────────────────────────────────────────

/// Java-style parameterized class descriptor used by compaction and formats.
#[derive(Debug, Clone, Deserialize)]
pub struct ParameterizedClass {
    pub class_name: String,
    #[serde(default)]
    pub parameters: HashMap<String, serde_yaml::Value>,
}

/// Java-style class descriptor where parameters are represented as a YAML list
/// of maps, as used by crypto_provider and key_provider sections.
#[derive(Debug, Clone, Deserialize)]
pub struct ClassWithListParameters {
    pub class_name: String,
    #[serde(default)]
    pub parameters: Vec<HashMap<String, serde_yaml::Value>>,
}

/// SSTable format configuration from the top-level `sstable` section.
#[derive(Debug, Clone, Deserialize)]
pub struct SstableConfig {
    #[serde(default = "defaults::sstable_selected_format")]
    pub selected_format: String,
    #[serde(default)]
    pub format: HashMap<String, HashMap<String, serde_yaml::Value>>,
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
    pub ssl_context_factory: Option<ParameterizedClass>,
    #[serde(default)]
    pub keystore: Option<String>,
    #[serde(default)]
    pub keystore_password: Option<String>,
    #[serde(default)]
    pub keystore_password_file: Option<String>,
    #[serde(default)]
    pub truststore: Option<String>,
    #[serde(default)]
    pub truststore_password: Option<String>,
    #[serde(default)]
    pub truststore_password_file: Option<String>,
    #[serde(default)]
    pub outbound_keystore: Option<String>,
    #[serde(default)]
    pub outbound_keystore_password: Option<String>,
    #[serde(default)]
    pub outbound_keystore_password_file: Option<String>,
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
    pub require_endpoint_verification: bool,
    #[serde(default)]
    pub protocol: Option<String>,
    #[serde(default)]
    pub accepted_protocols: Option<Vec<String>>,
    #[serde(default)]
    pub algorithm: Option<String>,
    #[serde(default)]
    pub store_type: Option<String>,
    #[serde(default)]
    pub cipher_suites: Option<Vec<String>>,
    #[serde(default = "defaults::internode_encryption")]
    pub internode_encryption: String,
    #[serde(default)]
    pub legacy_ssl_storage_port_enabled: bool,
    #[serde(default)]
    pub max_certificate_validity_period: Option<Duration>,
    #[serde(default)]
    pub certificate_validity_warn_threshold: Option<Duration>,
}

/// Transparent data encryption settings.
#[derive(Debug, Clone, Deserialize)]
pub struct TransparentDataEncryptionConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default = "defaults::tde_chunk_length_kb")]
    pub chunk_length_kb: u32,
    #[serde(default)]
    pub cipher: Option<String>,
    #[serde(default)]
    pub key_alias: Option<String>,
    #[serde(default)]
    pub iv_length: Option<u32>,
    #[serde(default)]
    pub key_provider: Vec<ClassWithListParameters>,
}

/// Replica filtering protection thresholds.
#[derive(Debug, Clone, Deserialize)]
pub struct ReplicaFilteringProtectionConfig {
    #[serde(default = "defaults::replica_filtering_cached_rows_warn_threshold")]
    pub cached_rows_warn_threshold: u32,
    #[serde(default = "defaults::replica_filtering_cached_rows_fail_threshold")]
    pub cached_rows_fail_threshold: u32,
}

impl Default for ReplicaFilteringProtectionConfig {
    fn default() -> Self {
        serde_yaml::from_str("{}").expect("replica filtering defaults should parse")
    }
}

/// Native protocol client-error metric exclusions.
#[derive(Debug, Clone, Deserialize)]
pub struct ClientErrorReportingExclusions {
    #[serde(default)]
    pub subnets: Vec<String>,
}

// ─── Audit Config ──────────────────────────────────────────────────────────

/// Audit logging configuration in cassandra.yaml.
#[derive(Debug, Clone, Deserialize)]
pub struct AuditLoggingConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(
        default = "defaults::audit_logger",
        deserialize_with = "audit_logger_from_yaml"
    )]
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
    pub included_users: Option<String>,
    #[serde(default)]
    pub excluded_users: Option<String>,
    #[serde(default)]
    pub roll_cycle: Option<String>,
    #[serde(default)]
    pub block: Option<bool>,
    #[serde(default)]
    pub max_queue_weight: Option<u64>,
    #[serde(default)]
    pub max_log_size: Option<u64>,
    #[serde(default)]
    pub archive_command: Option<String>,
    #[serde(default)]
    pub max_archive_retries: Option<u32>,
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
    pub roll_cycle: Option<String>,
    #[serde(default)]
    pub block: Option<bool>,
    #[serde(default)]
    pub max_queue_weight: Option<u64>,
    #[serde(default)]
    pub max_log_size: Option<u64>,
    #[serde(default)]
    pub archive_command: Option<String>,
    #[serde(default)]
    pub allow_nodetool_archive_command: bool,
    #[serde(default)]
    pub max_archive_retries: Option<u32>,
}

/// JMX server configuration from `jmx_server_options`.
#[derive(Debug, Clone, Deserialize)]
pub struct JmxServerOptionsConfig {
    #[serde(default = "defaults::enabled")]
    pub enabled: bool,
    #[serde(default)]
    pub remote: bool,
    #[serde(default = "defaults::jmx_port")]
    pub jmx_port: u16,
    #[serde(default)]
    pub rmi_port: Option<u16>,
    #[serde(default)]
    pub jmx_encryption_options: Option<EncryptionOptions>,
    #[serde(default)]
    pub authenticate: bool,
    #[serde(default)]
    pub password_file: Option<String>,
    #[serde(default)]
    pub access_file: Option<String>,
    #[serde(default)]
    pub login_config_name: Option<String>,
    #[serde(default)]
    pub login_config_file: Option<String>,
    #[serde(default)]
    pub authorizer: Option<String>,
}

/// Startup checks configuration from `startup_checks`.
#[derive(Debug, Clone, Deserialize)]
pub struct StartupChecksConfig {
    #[serde(default)]
    pub check_filesystem_ownership: Option<FilesystemOwnershipCheckConfig>,
    #[serde(default)]
    pub check_data_resurrection: Option<DataResurrectionCheckConfig>,
    #[serde(flatten)]
    pub extra: HashMap<String, serde_yaml::Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct FilesystemOwnershipCheckConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub ownership_token: Option<String>,
    #[serde(default = "defaults::filesystem_ownership_filename")]
    pub ownership_filename: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct DataResurrectionCheckConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub heartbeat_file: Option<String>,
    #[serde(default)]
    pub excluded_keyspaces: Option<String>,
    #[serde(default)]
    pub excluded_tables: Option<String>,
}

/// Accord node-level runtime configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct AccordRuntimeConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub journal_directory: Option<String>,
    #[serde(default = "defaults::negative_one")]
    pub queue_shard_count: i32,
    #[serde(default = "defaults::negative_one")]
    pub command_store_shard_count: i32,
    #[serde(default = "defaults::accord_recover_delay")]
    pub recover_delay: Duration,
    #[serde(default = "defaults::accord_fast_path_update_delay")]
    pub fast_path_update_delay: Duration,
}

/// Auto repair scheduler configuration.
#[derive(Debug, Clone, Deserialize)]
pub struct AutoRepairConfig {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub repair_type_overrides: HashMap<String, AutoRepairTypeOverride>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct AutoRepairTypeOverride {
    #[serde(default)]
    pub enabled: bool,
    #[serde(default)]
    pub min_repair_interval: Option<Duration>,
    #[serde(default)]
    pub token_range_splitter: Option<ParameterizedClass>,
}

/// Default values matching Java's Config class.
pub mod defaults {
    pub fn cluster_name() -> String {
        "Test Cluster".to_string()
    }
    pub fn num_tokens() -> u32 {
        16
    }
    pub fn auto_bootstrap() -> bool {
        true
    }
    pub fn partitioner() -> String {
        "org.apache.cassandra.dht.Murmur3Partitioner".to_string()
    }
    pub fn endpoint_snitch() -> String {
        "SimpleSnitch".to_string()
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
    pub fn concurrent_materialized_view_builders() -> u32 {
        1
    }
    pub fn hinted_handoff_enabled() -> bool {
        true
    }
    pub fn max_hints_delivery_threads() -> u32 {
        2
    }
    pub fn hints_flush_period() -> super::Duration {
        super::Duration::from_seconds(10)
    }
    pub fn max_hints_file_size() -> super::DataSize {
        super::DataSize::from_mebibytes(128)
    }
    pub fn zero_data_size() -> super::DataSize {
        super::DataSize::ZERO
    }
    pub fn transfer_hints_on_decommission() -> bool {
        true
    }
    pub fn hint_window_persistent_enabled() -> bool {
        true
    }
    pub fn batchlog_replay_throttle() -> super::DataSize {
        super::DataSize::from_kibibytes(1024)
    }
    pub fn batchlog_endpoint_strategy() -> String {
        "random_remote".to_string()
    }
    pub fn commitlog_sync() -> String {
        "periodic".to_string()
    }
    pub fn compaction_read_disk_access_mode() -> String {
        "auto".to_string()
    }
    pub fn flush_compression() -> String {
        "fast".to_string()
    }
    pub fn cache_load_timeout() -> super::Duration {
        super::Duration::from_seconds(30)
    }
    pub fn cdc_block_writes() -> bool {
        true
    }
    pub fn cdc_on_repair_enabled() -> bool {
        true
    }
    pub fn corrupted_tombstone_strategy() -> String {
        "disabled".to_string()
    }
    pub fn max_value_size() -> super::DataSize {
        super::DataSize::from_mebibytes(256)
    }
    pub fn default_keyspace_rf() -> u32 {
        1
    }
    pub fn max_concurrent_automatic_sstable_upgrades() -> u32 {
        1
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
    pub fn enabled() -> bool {
        true
    }
    pub fn jmx_port() -> u16 {
        7199
    }
    pub fn filesystem_ownership_filename() -> String {
        ".cassandra_fs_ownership".to_string()
    }
    pub fn negative_one() -> i32 {
        -1
    }
    pub fn accord_recover_delay() -> super::Duration {
        super::Duration::from_seconds(1)
    }
    pub fn accord_fast_path_update_delay() -> super::Duration {
        super::Duration::from_seconds(5)
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
    pub fn start_native_transport() -> bool {
        true
    }
    pub fn native_transport_max_threads() -> u32 {
        128
    }
    pub fn native_transport_max_auth_threads() -> u32 {
        4
    }
    pub fn native_transport_allow_older_protocols() -> bool {
        true
    }
    pub fn native_transport_idle_timeout() -> super::Duration {
        super::Duration::ZERO
    }
    pub fn rpc_keepalive() -> bool {
        true
    }
    pub fn request_timeout() -> super::Duration {
        super::Duration::from_seconds(10)
    }
    pub fn read_request_timeout() -> super::Duration {
        super::Duration::from_seconds(5)
    }
    pub fn range_request_timeout() -> super::Duration {
        super::Duration::from_seconds(10)
    }
    pub fn write_request_timeout() -> super::Duration {
        super::Duration::from_seconds(2)
    }
    pub fn counter_write_request_timeout() -> super::Duration {
        super::Duration::from_seconds(5)
    }
    pub fn cas_contention_timeout() -> super::Duration {
        super::Duration::from_millis(1000)
    }
    pub fn truncate_request_timeout() -> super::Duration {
        super::Duration::from_seconds(60)
    }
    pub fn repair_request_timeout() -> super::Duration {
        super::Duration::from_seconds(120)
    }
    pub fn slow_query_log_timeout() -> super::Duration {
        super::Duration::from_millis(500)
    }
    pub fn internode_timeout() -> bool {
        true
    }
    pub fn streaming_connections_per_host() -> u32 {
        1
    }
    pub fn stream_throughput_outbound() -> super::DataRateSpec {
        super::DataRateSpec::from_mebibytes_per_second(24)
    }
    pub fn inter_dc_stream_throughput_outbound() -> super::DataRateSpec {
        super::DataRateSpec::from_mebibytes_per_second(24)
    }
    pub fn entire_sstable_stream_throughput_outbound() -> super::DataRateSpec {
        super::DataRateSpec::from_mebibytes_per_second(24)
    }
    pub fn entire_sstable_inter_dc_stream_throughput_outbound() -> super::DataRateSpec {
        super::DataRateSpec::from_mebibytes_per_second(24)
    }
    pub fn stream_entire_sstables() -> bool {
        true
    }
    pub fn compaction_throughput() -> super::DataRateSpec {
        super::DataRateSpec::from_mebibytes_per_second(64)
    }
    pub fn memtable_allocation_type() -> String {
        "heap_buffers".to_string()
    }
    pub fn buffer_pool_use_heap_if_exhausted() -> bool {
        true
    }
    pub fn disk_optimization_strategy() -> String {
        "ssd".to_string()
    }
    pub fn trickle_fsync_interval() -> super::DataSize {
        super::DataSize::from_kibibytes(10_240)
    }
    pub fn internode_socket_buffer_size() -> super::DataSize {
        super::DataSize::ZERO
    }
    pub fn internode_application_queue_capacity() -> super::DataSize {
        super::DataSize::from_mebibytes(4)
    }
    pub fn internode_application_queue_reserve_endpoint_capacity() -> super::DataSize {
        super::DataSize::from_mebibytes(128)
    }
    pub fn internode_application_queue_reserve_global_capacity() -> super::DataSize {
        super::DataSize::from_mebibytes(512)
    }
    pub fn internode_tcp_connect_timeout() -> super::Duration {
        super::Duration::from_seconds(2)
    }
    pub fn internode_tcp_user_timeout() -> super::Duration {
        super::Duration::from_seconds(30)
    }
    pub fn internode_streaming_tcp_user_timeout() -> super::Duration {
        super::Duration::from_seconds(300)
    }
    pub fn streaming_keep_alive_period() -> super::Duration {
        super::Duration::from_seconds(300)
    }
    pub fn streaming_state_expires() -> super::Duration {
        super::Duration::from_days(3)
    }
    pub fn streaming_state_size() -> super::DataSize {
        super::DataSize::from_mebibytes(40)
    }
    pub fn streaming_stats_enabled() -> bool {
        true
    }
    pub fn cdc_total_space() -> super::DataSize {
        super::DataSize::from_mebibytes(4_096)
    }
    pub fn cdc_free_space_check_interval() -> super::Duration {
        super::Duration::from_millis(250)
    }
    pub fn index_summary_resize_interval() -> Option<super::Duration> {
        Some(super::Duration::from_minutes(60))
    }
    pub fn auto_snapshot() -> bool {
        true
    }
    pub fn column_index_cache_size() -> super::DataSize {
        super::DataSize::from_kibibytes(2)
    }
    pub fn sstable_preemptive_open_interval() -> Option<super::DataSize> {
        Some(super::DataSize::from_mebibytes(50))
    }
    pub fn sstable_selected_format() -> String {
        "big".to_string()
    }
    pub fn phi_convict_threshold() -> f64 {
        8.0
    }
    pub fn dynamic_snitch() -> bool {
        true
    }
    pub fn dynamic_snitch_update_interval() -> super::Duration {
        super::Duration::from_millis(100)
    }
    pub fn dynamic_snitch_reset_interval() -> super::Duration {
        super::Duration::from_minutes(10)
    }
    pub fn dynamic_snitch_badness_threshold() -> f64 {
        0.1
    }
    pub fn failure_detector() -> String {
        "FailureDetector".to_string()
    }
    pub fn denylist_writes_enabled() -> bool {
        true
    }
    pub fn denylist_reads_enabled() -> bool {
        true
    }
    pub fn denylist_range_reads_enabled() -> bool {
        true
    }
    pub fn denylist_refresh() -> super::Duration {
        super::Duration::from_seconds(600)
    }
    pub fn denylist_initial_load_retry() -> super::Duration {
        super::Duration::from_seconds(5)
    }
    pub fn denylist_max_keys_per_table() -> u32 {
        1_000
    }
    pub fn denylist_max_keys_total() -> u32 {
        10_000
    }
    pub fn denylist_consistency_level() -> String {
        "QUORUM".to_string()
    }
    pub fn internode_encryption() -> String {
        "none".to_string()
    }
    pub fn internode_compression() -> String {
        "dc".to_string()
    }
    pub fn trace_type_query_ttl() -> super::Duration {
        super::Duration::from_days(1)
    }
    pub fn trace_type_repair_ttl() -> super::Duration {
        super::Duration::from_days(7)
    }
    pub fn tde_chunk_length_kb() -> u32 {
        64
    }
    pub fn sstables_per_read_log_threshold() -> u32 {
        100
    }
    pub fn tombstone_warn_threshold() -> u32 {
        1_000
    }
    pub fn tombstone_failure_threshold() -> u32 {
        100_000
    }
    pub fn replica_filtering_cached_rows_warn_threshold() -> u32 {
        2_000
    }
    pub fn replica_filtering_cached_rows_fail_threshold() -> u32 {
        32_000
    }
    pub fn batch_size_warn_threshold() -> super::DataSize {
        super::DataSize::from_kibibytes(5)
    }
    pub fn batch_size_fail_threshold() -> super::DataSize {
        super::DataSize::from_kibibytes(50)
    }
    pub fn unlogged_batch_across_partitions_warn_threshold() -> u32 {
        10
    }
    pub fn materialized_views_on_repair_enabled() -> bool {
        true
    }
    pub fn use_statements_enabled() -> bool {
        true
    }
    pub fn triggers_policy() -> String {
        "enabled".to_string()
    }
    pub fn max_comment_length() -> u32 {
        128
    }
    pub fn max_security_label_length() -> u32 {
        48
    }
    pub fn storage_compatibility_mode() -> String {
        "NONE".to_string()
    }
    pub fn compression_dictionary_refresh_interval() -> super::Duration {
        super::Duration::from_hours(1)
    }
    pub fn compression_dictionary_refresh_initial_delay() -> super::Duration {
        super::Duration::from_seconds(10)
    }
    pub fn compression_dictionary_cache_size() -> u32 {
        10
    }
    pub fn compression_dictionary_cache_expire() -> super::Duration {
        super::Duration::from_hours(24)
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
        assert!(cfg.start_native_transport);
        assert_eq!(cfg.native_transport_max_threads, 128);
        assert_eq!(cfg.native_transport_max_auth_threads, 4);
        assert!(cfg.native_transport_allow_older_protocols);
        assert_eq!(cfg.concurrent_materialized_view_builders, 1);
        assert_eq!(cfg.hints_flush_period.millis(), 10_000);
        assert_eq!(cfg.max_hints_file_size.mebibytes(), 128);
        assert_eq!(cfg.batchlog_replay_throttle.kibibytes(), 1024);
        assert_eq!(cfg.internode_compression, "dc");
        assert_eq!(cfg.trace_type_query_ttl.millis(), 86_400_000);
        assert_eq!(cfg.trace_type_repair_ttl.millis(), 604_800_000);
        assert_eq!(cfg.tombstone_warn_threshold, 1_000);
        assert_eq!(cfg.tombstone_failure_threshold, 100_000);
        assert_eq!(cfg.batch_size_warn_threshold.kibibytes(), 5);
        assert_eq!(cfg.batch_size_fail_threshold.kibibytes(), 50);
        assert!(!cfg.read_thresholds_enabled);
        assert!(cfg.coordinator_read_size_warn_threshold.is_none());
        assert!(cfg.coordinator_read_size_fail_threshold.is_none());
        assert!(cfg.local_read_size_warn_threshold.is_none());
        assert!(cfg.local_read_size_fail_threshold.is_none());
        assert!(cfg.row_index_read_size_warn_threshold.is_none());
        assert!(cfg.row_index_read_size_fail_threshold.is_none());
        assert_eq!(cfg.corrupted_tombstone_strategy, "disabled");
        assert_eq!(cfg.max_value_size.mebibytes(), 256);
        assert_eq!(cfg.default_keyspace_rf, 1);
        assert!(!cfg.automatic_sstable_upgrade);
        assert_eq!(cfg.max_concurrent_automatic_sstable_upgrades, 1);
        assert!(cfg.materialized_views_on_repair_enabled);
        assert!(cfg.use_statements_enabled);
        assert_eq!(cfg.storage_compatibility_mode, "NONE");
        assert!(cfg.jmx_server_options.is_none());
        assert!(cfg.startup_checks.is_none());
        assert!(cfg.accord.is_none());
        assert!(cfg.auto_repair.is_none());
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
crypto_provider:
  - class_name: org.apache.cassandra.security.DefaultCryptoProvider
    parameters:
      - fail_on_missing_provider: "false"
client_encryption_options:
  enabled: true
  optional: true
  ssl_context_factory:
    class_name: org.apache.cassandra.security.DefaultSslContextFactory
    parameters:
      require_endpoint_verification: "false"
  keystore: "/etc/certs/node.keystore"
  keystore_password_file: "/etc/certs/node.keystore.pass"
  truststore: "/etc/certs/truststore"
  truststore_password_file: "/etc/certs/truststore.pass"
  certificate: "/etc/certs/node.crt"
  certificate_key: "/etc/certs/node.key"
  ca_certificate: "/etc/certs/ca.crt"
  require_client_auth: true
  require_endpoint_verification: true
  protocol: TLS
  accepted_protocols: [TLSv1.2, TLSv1.3]
  algorithm: SunX509
  store_type: JKS
  cipher_suites: [TLS_AES_128_GCM_SHA256]
  max_certificate_validity_period: "365d"
  certificate_validity_warn_threshold: "10d"
server_encryption_options:
  internode_encryption: all
  legacy_ssl_storage_port_enabled: true
  outbound_keystore: "/etc/certs/outbound.keystore"
  outbound_keystore_password_file: "/etc/certs/outbound.keystore.pass"
  require_endpoint_verification: true
transparent_data_encryption_options:
  enabled: true
  chunk_length_kb: 64
  cipher: AES/CBC/PKCS5Padding
  key_alias: testing:1
  iv_length: 16
  key_provider:
    - class_name: org.apache.cassandra.security.JKSKeyProvider
      parameters:
        - keystore: conf/.keystore
          keystore_password: cassandra
"#;
        let cfg: CassandraConfig = serde_yaml::from_str(yaml).unwrap();
        let crypto_provider = cfg.crypto_provider.unwrap();
        assert_eq!(
            crypto_provider[0].class_name,
            "org.apache.cassandra.security.DefaultCryptoProvider"
        );
        let enc = cfg.client_encryption_options.unwrap();
        assert!(enc.enabled);
        assert!(enc.optional);
        let factory = enc.ssl_context_factory.unwrap();
        assert_eq!(
            factory.class_name,
            "org.apache.cassandra.security.DefaultSslContextFactory"
        );
        assert_eq!(
            factory.parameters.get("require_endpoint_verification"),
            Some(&serde_yaml::Value::String("false".to_string()))
        );
        assert_eq!(enc.keystore.unwrap(), "/etc/certs/node.keystore");
        assert_eq!(
            enc.keystore_password_file.unwrap(),
            "/etc/certs/node.keystore.pass"
        );
        assert_eq!(enc.truststore.unwrap(), "/etc/certs/truststore");
        assert_eq!(
            enc.truststore_password_file.unwrap(),
            "/etc/certs/truststore.pass"
        );
        assert_eq!(enc.certificate.unwrap(), "/etc/certs/node.crt");
        assert!(enc.require_client_auth);
        assert!(enc.require_endpoint_verification);
        assert_eq!(enc.protocol.unwrap(), "TLS");
        assert_eq!(
            enc.accepted_protocols.unwrap(),
            vec!["TLSv1.2".to_string(), "TLSv1.3".to_string()]
        );
        assert_eq!(enc.algorithm.unwrap(), "SunX509");
        assert_eq!(enc.store_type.unwrap(), "JKS");
        assert_eq!(
            enc.cipher_suites.unwrap(),
            vec!["TLS_AES_128_GCM_SHA256".to_string()]
        );
        assert_eq!(
            enc.max_certificate_validity_period.unwrap().millis(),
            31_536_000_000
        );
        assert_eq!(
            enc.certificate_validity_warn_threshold.unwrap().millis(),
            864_000_000
        );

        let server_enc = cfg.server_encryption_options.unwrap();
        assert_eq!(server_enc.internode_encryption, "all");
        assert!(server_enc.legacy_ssl_storage_port_enabled);
        assert_eq!(
            server_enc.outbound_keystore.unwrap(),
            "/etc/certs/outbound.keystore"
        );
        assert_eq!(
            server_enc.outbound_keystore_password_file.unwrap(),
            "/etc/certs/outbound.keystore.pass"
        );
        assert!(server_enc.require_endpoint_verification);

        let tde = cfg.transparent_data_encryption_options.unwrap();
        assert!(tde.enabled);
        assert_eq!(tde.chunk_length_kb, 64);
        assert_eq!(tde.cipher.as_deref(), Some("AES/CBC/PKCS5Padding"));
        assert_eq!(tde.key_alias.as_deref(), Some("testing:1"));
        assert_eq!(tde.iv_length, Some(16));
        assert_eq!(
            tde.key_provider[0].class_name,
            "org.apache.cassandra.security.JKSKeyProvider"
        );
    }

    #[test]
    fn deserialize_with_upstream_runtime_extensions() {
        let yaml = r#"
hints_flush_period: 12s
max_hints_file_size: 256MiB
max_hints_size_per_host: 1GiB
auto_hints_cleanup_enabled: true
transfer_hints_on_decommission: false
hints_compression:
  - class_name: LZ4Compressor
heap_dump_path: /var/lib/cassandra/heapdump
dump_heap_on_uncaught_exception: true
hint_window_persistent_enabled: false
batchlog_replay_throttle: 2048KiB
batchlog_endpoint_strategy: dynamic_remote
local_system_data_file_directory: /var/lib/cassandra/system
disk_access_mode: mmap_index_only
commitlog_disk_access_mode: direct
commitlog_compression:
  - class_name: LZ4Compressor
compaction_read_disk_access_mode: direct
flush_compression: table
commitlog_sync_group_window: 1500ms
periodic_commitlog_sync_lag_block: 2s
roles_validity: 3s
roles_update_interval: 30s
roles_cache_active_update: true
permissions_validity: 4s
permissions_update_interval: 40s
permissions_cache_active_update: true
credentials_validity: 5s
credentials_update_interval: 50s
credentials_cache_active_update: true
traverse_auth_from_root: true
key_cache_keys_to_save: 100
row_cache_class_name: org.apache.cassandra.cache.OHCProvider
row_cache_save_period: 10s
row_cache_keys_to_save: 200
counter_cache_keys_to_save: 300
cache_load_timeout: 31s
cdc_block_writes: false
cdc_on_repair_enabled: false
networking_cache_size: 64MiB
file_cache_enabled: true
native_transport_flush_in_batches_legacy: true
corrupted_tombstone_strategy: warn
gc_log_threshold: 200ms
gc_warn_threshold: 1000ms
gc_concurrent_phase_log_threshold: 1000ms
gc_concurrent_phase_warn_threshold: 2000ms
max_value_size: 128MiB
default_keyspace_rf: 3
ideal_consistency_level: EACH_QUORUM
automatic_sstable_upgrade: true
max_concurrent_automatic_sstable_upgrades: 2
internode_compression: all
inter_dc_tcp_nodelay: true
trace_type_query_ttl: 2d
trace_type_repair_ttl: 8d
user_defined_functions_enabled: true
triggers_policy: disabled
sstables_per_read_log_threshold: 101
tombstone_warn_threshold: 2000
tombstone_failure_threshold: 200000
replica_filtering_protection:
  cached_rows_warn_threshold: 3000
  cached_rows_fail_threshold: 40000
batch_size_warn_threshold: 6KiB
batch_size_fail_threshold: 60KiB
read_thresholds_enabled: true
coordinator_read_size_warn_threshold: 1MiB
coordinator_read_size_fail_threshold: 2MiB
local_read_size_warn_threshold: 512KiB
local_read_size_fail_threshold: 4MiB
row_index_read_size_warn_threshold: 256KiB
row_index_read_size_fail_threshold: 1MiB
unlogged_batch_across_partitions_warn_threshold: 12
diagnostic_events_enabled: true
repaired_data_tracking_for_range_reads_enabled: true
repaired_data_tracking_for_partition_reads_enabled: true
report_unconfirmed_repaired_data_mismatches: true
auth_read_consistency_level: LOCAL_QUORUM
auth_write_consistency_level: EACH_QUORUM
auth_cache_warming_enabled: true
dynamic_data_masking_enabled: true
materialized_views_enabled: true
materialized_views_on_repair_enabled: false
sasi_indexes_enabled: true
transient_replication_enabled: true
drop_compact_storage_enabled: true
use_statements_enabled: false
client_error_reporting_exclusions:
  subnets: [127.0.0.1, 10.0.0.0/8]
max_comment_length: 256
max_security_label_length: 96
storage_compatibility_mode: UPGRADING
compression_dictionary_refresh_interval: 2h
compression_dictionary_refresh_initial_delay: 20s
compression_dictionary_cache_size: 20
compression_dictionary_cache_expire: 48h
jmx_server_options:
  enabled: true
  remote: true
  jmx_port: 7199
  rmi_port: 7299
  jmx_encryption_options:
    enabled: true
    keystore: conf/jmx.keystore
  authenticate: true
  password_file: /etc/cassandra/jmxremote.password
  access_file: /etc/cassandra/jmxremote.access
  login_config_name: CassandraLogin
  login_config_file: conf/cassandra-jaas.config
  authorizer: org.apache.cassandra.auth.jmx.AuthorizationProxy
startup_checks:
  check_filesystem_ownership:
    enabled: true
    ownership_token: sometoken
    ownership_filename: .cassandra_fs_ownership
  check_data_resurrection:
    enabled: true
    heartbeat_file: /var/lib/cassandra/data/cassandra-heartbeat
    excluded_keyspaces: system,system_schema
    excluded_tables: ks.tbl
accord:
  enabled: true
  journal_directory: /var/lib/cassandra/accord
  queue_shard_count: 4
  command_store_shard_count: 8
  recover_delay: 2s
  fast_path_update_delay: 6s
reject_repair_compaction_threshold: 1024
repair_disk_headroom_reject_ratio: 0.2
incremental_repair_disk_headroom_reject_ratio: 0.1
auto_repair:
  enabled: true
  repair_type_overrides:
    full:
      enabled: true
      min_repair_interval: 24h
      token_range_splitter:
        class_name: org.apache.cassandra.repair.autorepair.RepairTokenRangeSplitter
        parameters:
          bytes_per_assignment: 50GiB
"#;
        let cfg: CassandraConfig = serde_yaml::from_str(yaml).unwrap();
        assert_eq!(cfg.hints_flush_period.millis(), 12_000);
        assert_eq!(cfg.max_hints_file_size.mebibytes(), 256);
        assert_eq!(cfg.max_hints_size_per_host.mebibytes(), 1024);
        assert!(cfg.auto_hints_cleanup_enabled);
        assert!(!cfg.transfer_hints_on_decommission);
        assert_eq!(
            cfg.hints_compression.unwrap()[0].class_name,
            "LZ4Compressor"
        );
        assert_eq!(
            cfg.heap_dump_path.as_deref(),
            Some("/var/lib/cassandra/heapdump")
        );
        assert!(cfg.dump_heap_on_uncaught_exception);
        assert!(!cfg.hint_window_persistent_enabled);
        assert_eq!(cfg.batchlog_replay_throttle.kibibytes(), 2048);
        assert_eq!(cfg.batchlog_endpoint_strategy, "dynamic_remote");
        assert_eq!(
            cfg.local_system_data_file_directory.as_deref(),
            Some("/var/lib/cassandra/system")
        );
        assert_eq!(cfg.disk_access_mode.as_deref(), Some("mmap_index_only"));
        assert_eq!(cfg.commitlog_disk_access_mode.as_deref(), Some("direct"));
        assert_eq!(
            cfg.commitlog_compression.unwrap()[0].class_name,
            "LZ4Compressor"
        );
        assert_eq!(cfg.compaction_read_disk_access_mode, "direct");
        assert_eq!(cfg.flush_compression, "table");
        assert_eq!(cfg.commitlog_sync_group_window.unwrap().millis(), 1_500);
        assert_eq!(
            cfg.periodic_commitlog_sync_lag_block.unwrap().millis(),
            2_000
        );
        assert_eq!(cfg.roles_validity_in_ms, 3_000);
        assert_eq!(cfg.roles_update_interval.unwrap().millis(), 30_000);
        assert!(cfg.roles_cache_active_update);
        assert_eq!(cfg.permissions_validity_in_ms, 4_000);
        assert_eq!(cfg.permissions_update_interval.unwrap().millis(), 40_000);
        assert!(cfg.permissions_cache_active_update);
        assert_eq!(cfg.credentials_validity_in_ms, 5_000);
        assert_eq!(cfg.credentials_update_interval.unwrap().millis(), 50_000);
        assert!(cfg.credentials_cache_active_update);
        assert!(cfg.traverse_auth_from_root);
        assert_eq!(cfg.key_cache_keys_to_save, Some(100));
        assert_eq!(
            cfg.row_cache_class_name.as_deref(),
            Some("org.apache.cassandra.cache.OHCProvider")
        );
        assert_eq!(cfg.row_cache_save_period.unwrap().millis(), 10_000);
        assert_eq!(cfg.row_cache_keys_to_save, Some(200));
        assert_eq!(cfg.counter_cache_keys_to_save, Some(300));
        assert_eq!(cfg.cache_load_timeout.millis(), 31_000);
        assert!(!cfg.cdc_block_writes);
        assert!(!cfg.cdc_on_repair_enabled);
        assert_eq!(cfg.networking_cache_size.unwrap().mebibytes(), 64);
        assert!(cfg.file_cache_enabled);
        assert!(cfg.native_transport_flush_in_batches_legacy);
        assert_eq!(cfg.corrupted_tombstone_strategy, "warn");
        assert_eq!(cfg.gc_log_threshold.unwrap().millis(), 200);
        assert_eq!(cfg.gc_warn_threshold.unwrap().millis(), 1_000);
        assert_eq!(
            cfg.gc_concurrent_phase_log_threshold.unwrap().millis(),
            1_000
        );
        assert_eq!(
            cfg.gc_concurrent_phase_warn_threshold.unwrap().millis(),
            2_000
        );
        assert_eq!(cfg.max_value_size.mebibytes(), 128);
        assert_eq!(cfg.default_keyspace_rf, 3);
        assert_eq!(cfg.ideal_consistency_level.as_deref(), Some("EACH_QUORUM"));
        assert!(cfg.automatic_sstable_upgrade);
        assert_eq!(cfg.max_concurrent_automatic_sstable_upgrades, 2);
        assert_eq!(cfg.internode_compression, "all");
        assert!(cfg.inter_dc_tcp_nodelay);
        assert_eq!(cfg.trace_type_query_ttl.millis(), 172_800_000);
        assert_eq!(cfg.trace_type_repair_ttl.millis(), 691_200_000);
        assert!(cfg.user_defined_functions_enabled);
        assert_eq!(cfg.triggers_policy, "disabled");
        assert_eq!(cfg.sstables_per_read_log_threshold, 101);
        assert_eq!(cfg.tombstone_warn_threshold, 2_000);
        assert_eq!(cfg.tombstone_failure_threshold, 200_000);
        assert_eq!(
            cfg.replica_filtering_protection.cached_rows_warn_threshold,
            3_000
        );
        assert_eq!(
            cfg.replica_filtering_protection.cached_rows_fail_threshold,
            40_000
        );
        assert_eq!(cfg.batch_size_warn_threshold.kibibytes(), 6);
        assert_eq!(cfg.batch_size_fail_threshold.kibibytes(), 60);
        assert!(cfg.read_thresholds_enabled);
        assert_eq!(
            cfg.coordinator_read_size_warn_threshold
                .unwrap()
                .mebibytes(),
            1
        );
        assert_eq!(
            cfg.coordinator_read_size_fail_threshold
                .unwrap()
                .mebibytes(),
            2
        );
        assert_eq!(cfg.local_read_size_warn_threshold.unwrap().kibibytes(), 512);
        assert_eq!(cfg.local_read_size_fail_threshold.unwrap().mebibytes(), 4);
        assert_eq!(
            cfg.row_index_read_size_warn_threshold.unwrap().kibibytes(),
            256
        );
        assert_eq!(
            cfg.row_index_read_size_fail_threshold.unwrap().mebibytes(),
            1
        );
        assert_eq!(cfg.unlogged_batch_across_partitions_warn_threshold, 12);
        assert!(cfg.diagnostic_events_enabled);
        assert!(cfg.repaired_data_tracking_for_range_reads_enabled);
        assert!(cfg.repaired_data_tracking_for_partition_reads_enabled);
        assert!(cfg.report_unconfirmed_repaired_data_mismatches);
        assert_eq!(
            cfg.auth_read_consistency_level.as_deref(),
            Some("LOCAL_QUORUM")
        );
        assert_eq!(
            cfg.auth_write_consistency_level.as_deref(),
            Some("EACH_QUORUM")
        );
        assert!(cfg.auth_cache_warming_enabled);
        assert!(cfg.dynamic_data_masking_enabled);
        assert!(cfg.materialized_views_enabled);
        assert!(!cfg.materialized_views_on_repair_enabled);
        assert!(cfg.sasi_indexes_enabled);
        assert!(cfg.transient_replication_enabled);
        assert!(cfg.drop_compact_storage_enabled);
        assert!(!cfg.use_statements_enabled);
        assert_eq!(
            cfg.client_error_reporting_exclusions.unwrap().subnets,
            vec!["127.0.0.1".to_string(), "10.0.0.0/8".to_string()]
        );
        assert_eq!(cfg.max_comment_length, 256);
        assert_eq!(cfg.max_security_label_length, 96);
        assert_eq!(cfg.storage_compatibility_mode, "UPGRADING");
        assert_eq!(
            cfg.compression_dictionary_refresh_interval.millis(),
            7_200_000
        );
        assert_eq!(
            cfg.compression_dictionary_refresh_initial_delay.millis(),
            20_000
        );
        assert_eq!(cfg.compression_dictionary_cache_size, 20);
        assert_eq!(
            cfg.compression_dictionary_cache_expire.millis(),
            172_800_000
        );
        let jmx = cfg.jmx_server_options.unwrap();
        assert!(jmx.enabled);
        assert!(jmx.remote);
        assert_eq!(jmx.jmx_port, 7199);
        assert_eq!(jmx.rmi_port, Some(7299));
        assert!(jmx.jmx_encryption_options.unwrap().enabled);
        assert!(jmx.authenticate);
        assert_eq!(
            jmx.password_file.as_deref(),
            Some("/etc/cassandra/jmxremote.password")
        );
        assert_eq!(jmx.login_config_name.as_deref(), Some("CassandraLogin"));
        assert_eq!(
            jmx.authorizer.as_deref(),
            Some("org.apache.cassandra.auth.jmx.AuthorizationProxy")
        );
        let startup = cfg.startup_checks.unwrap();
        let ownership = startup.check_filesystem_ownership.unwrap();
        assert!(ownership.enabled);
        assert_eq!(ownership.ownership_token.as_deref(), Some("sometoken"));
        assert_eq!(ownership.ownership_filename, ".cassandra_fs_ownership");
        let resurrection = startup.check_data_resurrection.unwrap();
        assert!(resurrection.enabled);
        assert_eq!(
            resurrection.heartbeat_file.as_deref(),
            Some("/var/lib/cassandra/data/cassandra-heartbeat")
        );
        assert_eq!(
            resurrection.excluded_keyspaces.as_deref(),
            Some("system,system_schema")
        );
        let accord = cfg.accord.unwrap();
        assert!(accord.enabled);
        assert_eq!(
            accord.journal_directory.as_deref(),
            Some("/var/lib/cassandra/accord")
        );
        assert_eq!(accord.queue_shard_count, 4);
        assert_eq!(accord.command_store_shard_count, 8);
        assert_eq!(accord.recover_delay.millis(), 2_000);
        assert_eq!(accord.fast_path_update_delay.millis(), 6_000);
        assert_eq!(cfg.reject_repair_compaction_threshold, Some(1024));
        assert_eq!(cfg.repair_disk_headroom_reject_ratio, Some(0.2));
        assert_eq!(cfg.incremental_repair_disk_headroom_reject_ratio, Some(0.1));
        let auto_repair = cfg.auto_repair.unwrap();
        assert!(auto_repair.enabled);
        let full = auto_repair.repair_type_overrides.get("full").unwrap();
        assert!(full.enabled);
        assert_eq!(full.min_repair_interval.unwrap().millis(), 86_400_000);
        assert_eq!(
            full.token_range_splitter.as_ref().unwrap().class_name,
            "org.apache.cassandra.repair.autorepair.RepairTokenRangeSplitter"
        );
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
        assert_eq!(audit.logger, "FileAuditLogger");
        assert_eq!(audit.audit_logs_dir.unwrap(), "/var/log/cassandra/audit");
    }

    #[test]
    fn deserialize_with_audit_logger_class_list() {
        let yaml = r#"
audit_logging_options:
  enabled: true
  logger:
    - class_name: BinAuditLogger
      parameters:
        - field_separator: "|"
  included_users: alice
  excluded_users: bob
  block: true
  max_queue_weight: 1024
  archive_command: "/bin/true %path"
  max_archive_retries: 3
"#;
        let cfg: CassandraConfig = serde_yaml::from_str(yaml).unwrap();
        let audit = cfg.audit_logging_options.unwrap();
        assert_eq!(audit.logger, "BinAuditLogger");
        assert_eq!(audit.included_users.as_deref(), Some("alice"));
        assert_eq!(audit.excluded_users.as_deref(), Some("bob"));
        assert_eq!(audit.block, Some(true));
        assert_eq!(audit.max_queue_weight, Some(1024));
        assert_eq!(audit.archive_command.as_deref(), Some("/bin/true %path"));
        assert_eq!(audit.max_archive_retries, Some(3));
    }

    #[test]
    fn native_transport_defaults() {
        let cfg = CassandraConfig::default();
        assert_eq!(cfg.native_transport_max_concurrent_connections, -1);
        assert_eq!(cfg.native_transport_max_concurrent_connections_per_ip, -1);
        assert_eq!(cfg.native_transport_max_frame_size, 256 * 1024 * 1024);
        assert_eq!(
            cfg.native_transport_max_request_data_in_flight,
            512 * 1024 * 1024
        );
        assert!(!cfg.native_transport_rate_limiting_enabled);
        assert_eq!(cfg.native_transport_max_requests_per_second, 25_000);
        assert_eq!(cfg.native_transport_idle_timeout_seconds, 0);
        assert_eq!(cfg.native_transport_idle_timeout.millis(), 0);
        assert!(cfg.start_native_transport);
        assert_eq!(cfg.native_transport_max_threads, 128);
        assert_eq!(cfg.native_transport_max_auth_threads, 4);
        assert!(cfg.native_transport_allow_older_protocols);
    }

    #[test]
    fn upstream_runtime_defaults() {
        let cfg = CassandraConfig::default();
        assert!(cfg.auto_bootstrap);
        assert!(cfg.initial_token.is_none());
        assert_eq!(cfg.endpoint_snitch, "SimpleSnitch");
        assert!(cfg.listen_address.is_none());
        assert!(cfg.listen_interface.is_none());
        assert!(!cfg.listen_interface_prefer_ipv6);
        assert!(cfg.broadcast_address.is_none());
        assert!(!cfg.listen_on_broadcast_address);
        assert!(cfg.rpc_address.is_none());
        assert!(cfg.rpc_interface.is_none());
        assert!(!cfg.rpc_interface_prefer_ipv6);
        assert!(cfg.broadcast_rpc_address.is_none());
        assert!(cfg.rpc_keepalive);
        assert_eq!(cfg.request_timeout.millis(), 10_000);
        assert_eq!(cfg.read_request_timeout.millis(), 5_000);
        assert_eq!(cfg.range_request_timeout.millis(), 10_000);
        assert_eq!(cfg.write_request_timeout.millis(), 2_000);
        assert_eq!(cfg.counter_write_request_timeout.millis(), 5_000);
        assert_eq!(cfg.cas_contention_timeout.millis(), 1_000);
        assert_eq!(cfg.truncate_request_timeout.millis(), 60_000);
        assert_eq!(cfg.repair_request_timeout.millis(), 120_000);
        assert_eq!(cfg.slow_query_log_timeout.millis(), 500);
        assert!(cfg.internode_timeout);
        assert_eq!(cfg.streaming_connections_per_host, 1);
        assert_eq!(cfg.stream_throughput_outbound.mebibytes_per_second(), 24);
        assert_eq!(
            cfg.inter_dc_stream_throughput_outbound
                .mebibytes_per_second(),
            24
        );
        assert_eq!(
            cfg.entire_sstable_stream_throughput_outbound
                .mebibytes_per_second(),
            24
        );
        assert_eq!(
            cfg.entire_sstable_inter_dc_stream_throughput_outbound
                .mebibytes_per_second(),
            24
        );
        assert!(cfg.stream_entire_sstables);
        assert_eq!(cfg.compaction_throughput.mebibytes_per_second(), 64);
        assert!(cfg.concurrent_compactors.is_none());
        assert!(cfg.concurrent_validations.is_none());
        assert_eq!(cfg.concurrent_materialized_view_builders, 1);
        assert!(cfg.repair_session_space.is_none());
        assert_eq!(cfg.concurrent_merkle_tree_requests, 0);
        assert_eq!(cfg.memtable_flush_writers, 0);
        assert!(cfg.memtable_heap_space.is_none());
        assert!(cfg.memtable_offheap_space.is_none());
        assert!(cfg.memtable_cleanup_threshold.is_none());
        assert_eq!(cfg.memtable_allocation_type, "heap_buffers");
        assert!(cfg.file_cache_size.is_none());
        assert!(cfg.buffer_pool_use_heap_if_exhausted);
        assert_eq!(cfg.disk_optimization_strategy, "ssd");
        assert!(!cfg.trickle_fsync);
        assert_eq!(cfg.trickle_fsync_interval.kibibytes(), 10_240);
        assert_eq!(cfg.internode_socket_send_buffer_size.bytes(), 0);
        assert_eq!(cfg.internode_socket_receive_buffer_size.bytes(), 0);
        assert_eq!(cfg.internode_application_send_queue_capacity.mebibytes(), 4);
        assert_eq!(
            cfg.internode_application_send_queue_reserve_endpoint_capacity
                .mebibytes(),
            128
        );
        assert_eq!(
            cfg.internode_application_send_queue_reserve_global_capacity
                .mebibytes(),
            512
        );
        assert_eq!(
            cfg.internode_application_receive_queue_capacity.mebibytes(),
            4
        );
        assert_eq!(cfg.internode_tcp_connect_timeout.millis(), 2_000);
        assert_eq!(cfg.internode_tcp_user_timeout.millis(), 30_000);
        assert_eq!(cfg.internode_streaming_tcp_user_timeout.millis(), 300_000);
        assert_eq!(cfg.streaming_keep_alive_period.millis(), 300_000);
        assert_eq!(cfg.streaming_state_expires.millis(), 259_200_000);
        assert_eq!(cfg.streaming_state_size.mebibytes(), 40);
        assert!(cfg.streaming_stats_enabled);
        assert_eq!(cfg.cdc_total_space.mebibytes(), 4_096);
        assert_eq!(cfg.cdc_free_space_check_interval.millis(), 250);
        assert!(cfg.index_summary_capacity.is_none());
        assert_eq!(
            cfg.index_summary_resize_interval.unwrap().millis(),
            3_600_000
        );
        assert!(!cfg.incremental_backups);
        assert!(!cfg.snapshot_before_compaction);
        assert!(cfg.auto_snapshot);
        assert!(cfg.auto_snapshot_ttl.is_none());
        assert_eq!(cfg.snapshot_links_per_second, 0);
        assert!(cfg.sstable.is_none());
        assert!(cfg.column_index_size.is_none());
        assert_eq!(cfg.column_index_cache_size.kibibytes(), 2);
        assert!(cfg.default_compaction.is_none());
        assert_eq!(
            cfg.sstable_preemptive_open_interval.unwrap().mebibytes(),
            50
        );
        assert!(!cfg.uuid_sstable_identifiers_enabled);
        assert_eq!(cfg.phi_convict_threshold, 8.0);
        assert!(cfg.dynamic_snitch);
        assert_eq!(cfg.dynamic_snitch_update_interval.millis(), 100);
        assert_eq!(cfg.dynamic_snitch_reset_interval.millis(), 600_000);
        assert_eq!(cfg.dynamic_snitch_badness_threshold, 0.1);
        assert!(cfg.initial_location_provider.is_none());
        assert!(cfg.node_proximity.is_none());
        assert!(cfg.addresses_config.is_none());
        assert!(!cfg.prefer_local_connections);
        assert_eq!(cfg.failure_detector, "FailureDetector");
        assert!(!cfg.partition_denylist_enabled);
        assert!(cfg.denylist_writes_enabled);
        assert!(cfg.denylist_reads_enabled);
        assert!(cfg.denylist_range_reads_enabled);
        assert_eq!(cfg.denylist_refresh.millis(), 600_000);
        assert_eq!(cfg.denylist_initial_load_retry.millis(), 5_000);
        assert_eq!(cfg.denylist_max_keys_per_table, 1_000);
        assert_eq!(cfg.denylist_max_keys_total, 10_000);
        assert_eq!(cfg.denylist_consistency_level, "QUORUM");
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
    fn deserialize_upstream_runtime_fields() {
        let yaml = r#"
auto_bootstrap: false
initial_token: "1,2,3"
endpoint_snitch: GossipingPropertyFileSnitch
start_native_transport: false
native_transport_max_threads: 256
native_transport_max_auth_threads: 8
native_transport_allow_older_protocols: false
native_transport_idle_timeout: "60s"
listen_address: "10.0.0.10"
listen_interface_prefer_ipv6: true
broadcast_address: "10.0.0.11"
listen_on_broadcast_address: true
rpc_address: "0.0.0.0"
rpc_interface_prefer_ipv6: true
broadcast_rpc_address: "10.0.0.12"
rpc_keepalive: false
request_timeout: "12s"
read_request_timeout: "6s"
range_request_timeout: "11s"
write_request_timeout: "3s"
counter_write_request_timeout: "7s"
cas_contention_timeout: "1500ms"
truncate_request_timeout: "70s"
repair_request_timeout: "130s"
slow_query_log_timeout: "750ms"
internode_timeout: false
internode_socket_send_buffer_size: "4096B"
internode_socket_receive_buffer_size: "8192B"
internode_application_send_queue_capacity: "8MiB"
internode_application_send_queue_reserve_endpoint_capacity: "64MiB"
internode_application_send_queue_reserve_global_capacity: "256MiB"
internode_application_receive_queue_capacity: "9MiB"
internode_application_receive_queue_reserve_endpoint_capacity: "65MiB"
internode_application_receive_queue_reserve_global_capacity: "257MiB"
internode_tcp_connect_timeout: "3s"
internode_tcp_user_timeout: "0ms"
internode_streaming_tcp_user_timeout: "10m"
streaming_keep_alive_period: "0s"
streaming_state_expires: "4d"
streaming_state_size: "80MiB"
streaming_stats_enabled: false
streaming_connections_per_host: 4
stream_throughput_outbound: "48MiB/s"
inter_dc_stream_throughput_outbound: "12MiB/s"
entire_sstable_stream_throughput_outbound: "20MiB/s"
entire_sstable_inter_dc_stream_throughput_outbound: "10MiB/s"
stream_entire_sstables: false
compaction_throughput: "32MiB/s"
concurrent_compactors: 6
concurrent_validations: 2
concurrent_materialized_view_builders: 3
repair_session_space: "64MiB"
concurrent_merkle_tree_requests: 5
memtable:
  configurations:
    skiplist:
      class_name: SkipListMemtable
    trie:
      class_name: TrieMemtable
    default:
      inherits: skiplist
memtable_heap_space: "512MiB"
memtable_offheap_space: "256MiB"
memtable_cleanup_threshold: 0.25
memtable_allocation_type: offheap_objects
memtable_flush_writers: 2
file_cache_size: "128MiB"
buffer_pool_use_heap_if_exhausted: false
disk_optimization_strategy: spinning
trickle_fsync: true
trickle_fsync_interval: "2048KiB"
cdc_total_space: "1024MiB"
cdc_free_space_check_interval: "500ms"
index_summary_capacity: "64MiB"
index_summary_resize_interval: "30m"
incremental_backups: true
snapshot_before_compaction: true
auto_snapshot: false
auto_snapshot_ttl: "30d"
snapshot_links_per_second: 123
sstable:
  selected_format: bti
  format:
    bti:
      mode: fast
column_index_size: "16KiB"
column_index_cache_size: "8KiB"
default_compaction:
  class_name: SizeTieredCompactionStrategy
  parameters:
    min_threshold: 4
    max_threshold: 32
sstable_preemptive_open_interval: "25MiB"
uuid_sstable_identifiers_enabled: true
phi_convict_threshold: 10.5
dynamic_snitch: false
dynamic_snitch_update_interval: "250ms"
dynamic_snitch_reset_interval: "15m"
dynamic_snitch_badness_threshold: 0.25
initial_location_provider: RackDCFileLocationProvider
node_proximity: NetworkTopologyProximity
addresses_config: Ec2MultiRegionAddressConfig
prefer_local_connections: true
failure_detector: DeadlineFailureDetector
partition_denylist_enabled: true
denylist_writes_enabled: false
denylist_reads_enabled: false
denylist_range_reads_enabled: false
denylist_refresh: "700s"
denylist_initial_load_retry: "6s"
denylist_max_keys_per_table: 2000
denylist_max_keys_total: 20000
denylist_consistency_level: LOCAL_QUORUM
"#;
        let cfg: CassandraConfig = serde_yaml::from_str(yaml).unwrap();
        assert!(!cfg.auto_bootstrap);
        assert_eq!(cfg.initial_token.as_deref(), Some("1,2,3"));
        assert_eq!(cfg.endpoint_snitch, "GossipingPropertyFileSnitch");
        assert!(!cfg.start_native_transport);
        assert_eq!(cfg.native_transport_max_threads, 256);
        assert_eq!(cfg.native_transport_max_auth_threads, 8);
        assert!(!cfg.native_transport_allow_older_protocols);
        assert_eq!(cfg.native_transport_idle_timeout.millis(), 60_000);
        assert_eq!(cfg.listen_address.as_deref(), Some("10.0.0.10"));
        assert!(cfg.listen_interface_prefer_ipv6);
        assert_eq!(cfg.broadcast_address.as_deref(), Some("10.0.0.11"));
        assert!(cfg.listen_on_broadcast_address);
        assert_eq!(cfg.rpc_address.as_deref(), Some("0.0.0.0"));
        assert!(cfg.rpc_interface_prefer_ipv6);
        assert_eq!(cfg.broadcast_rpc_address.as_deref(), Some("10.0.0.12"));
        assert!(!cfg.rpc_keepalive);
        assert_eq!(cfg.request_timeout.millis(), 12_000);
        assert_eq!(cfg.read_request_timeout.millis(), 6_000);
        assert_eq!(cfg.range_request_timeout.millis(), 11_000);
        assert_eq!(cfg.write_request_timeout.millis(), 3_000);
        assert_eq!(cfg.counter_write_request_timeout.millis(), 7_000);
        assert_eq!(cfg.cas_contention_timeout.millis(), 1_500);
        assert_eq!(cfg.truncate_request_timeout.millis(), 70_000);
        assert_eq!(cfg.repair_request_timeout.millis(), 130_000);
        assert_eq!(cfg.slow_query_log_timeout.millis(), 750);
        assert!(!cfg.internode_timeout);
        assert_eq!(cfg.internode_socket_send_buffer_size.bytes(), 4096);
        assert_eq!(cfg.internode_socket_receive_buffer_size.bytes(), 8192);
        assert_eq!(cfg.internode_application_send_queue_capacity.mebibytes(), 8);
        assert_eq!(
            cfg.internode_application_send_queue_reserve_endpoint_capacity
                .mebibytes(),
            64
        );
        assert_eq!(
            cfg.internode_application_send_queue_reserve_global_capacity
                .mebibytes(),
            256
        );
        assert_eq!(
            cfg.internode_application_receive_queue_capacity.mebibytes(),
            9
        );
        assert_eq!(
            cfg.internode_application_receive_queue_reserve_endpoint_capacity
                .mebibytes(),
            65
        );
        assert_eq!(
            cfg.internode_application_receive_queue_reserve_global_capacity
                .mebibytes(),
            257
        );
        assert_eq!(cfg.internode_tcp_connect_timeout.millis(), 3_000);
        assert_eq!(cfg.internode_tcp_user_timeout.millis(), 0);
        assert_eq!(cfg.internode_streaming_tcp_user_timeout.millis(), 600_000);
        assert_eq!(cfg.streaming_keep_alive_period.millis(), 0);
        assert_eq!(cfg.streaming_state_expires.millis(), 345_600_000);
        assert_eq!(cfg.streaming_state_size.mebibytes(), 80);
        assert!(!cfg.streaming_stats_enabled);
        assert_eq!(cfg.streaming_connections_per_host, 4);
        assert_eq!(cfg.stream_throughput_outbound.mebibytes_per_second(), 48);
        assert_eq!(
            cfg.inter_dc_stream_throughput_outbound
                .mebibytes_per_second(),
            12
        );
        assert_eq!(
            cfg.entire_sstable_stream_throughput_outbound
                .mebibytes_per_second(),
            20
        );
        assert_eq!(
            cfg.entire_sstable_inter_dc_stream_throughput_outbound
                .mebibytes_per_second(),
            10
        );
        assert!(!cfg.stream_entire_sstables);
        assert_eq!(cfg.compaction_throughput.mebibytes_per_second(), 32);
        assert_eq!(cfg.concurrent_compactors, Some(6));
        assert_eq!(cfg.concurrent_validations, Some(2));
        assert_eq!(cfg.concurrent_materialized_view_builders, 3);
        assert_eq!(cfg.repair_session_space.unwrap().mebibytes(), 64);
        assert_eq!(cfg.concurrent_merkle_tree_requests, 5);
        let memtable = cfg.memtable.as_ref().unwrap();
        assert_eq!(
            memtable
                .configurations
                .get("trie")
                .unwrap()
                .class_name
                .as_deref(),
            Some("TrieMemtable")
        );
        assert_eq!(
            memtable
                .configurations
                .get("default")
                .unwrap()
                .inherits
                .as_deref(),
            Some("skiplist")
        );
        assert_eq!(cfg.memtable_heap_space.unwrap().mebibytes(), 512);
        assert_eq!(cfg.memtable_offheap_space.unwrap().mebibytes(), 256);
        assert_eq!(cfg.memtable_cleanup_threshold, Some(0.25));
        assert_eq!(cfg.memtable_allocation_type, "offheap_objects");
        assert_eq!(cfg.memtable_flush_writers, 2);
        assert_eq!(cfg.file_cache_size.unwrap().mebibytes(), 128);
        assert!(!cfg.buffer_pool_use_heap_if_exhausted);
        assert_eq!(cfg.disk_optimization_strategy, "spinning");
        assert!(cfg.trickle_fsync);
        assert_eq!(cfg.trickle_fsync_interval.kibibytes(), 2_048);
        assert_eq!(cfg.cdc_total_space.mebibytes(), 1_024);
        assert_eq!(cfg.cdc_free_space_check_interval.millis(), 500);
        assert_eq!(cfg.index_summary_capacity.unwrap().mebibytes(), 64);
        assert_eq!(
            cfg.index_summary_resize_interval.unwrap().millis(),
            1_800_000
        );
        assert!(cfg.incremental_backups);
        assert!(cfg.snapshot_before_compaction);
        assert!(!cfg.auto_snapshot);
        assert_eq!(cfg.auto_snapshot_ttl.unwrap().millis(), 2_592_000_000);
        assert_eq!(cfg.snapshot_links_per_second, 123);
        let sstable = cfg.sstable.as_ref().unwrap();
        assert_eq!(sstable.selected_format, "bti");
        assert!(sstable.format.contains_key("bti"));
        assert_eq!(cfg.column_index_size.unwrap().kibibytes(), 16);
        assert_eq!(cfg.column_index_cache_size.kibibytes(), 8);
        let default_compaction = cfg.default_compaction.as_ref().unwrap();
        assert_eq!(
            default_compaction.class_name,
            "SizeTieredCompactionStrategy"
        );
        assert!(default_compaction.parameters.contains_key("min_threshold"));
        assert_eq!(
            cfg.sstable_preemptive_open_interval.unwrap().mebibytes(),
            25
        );
        assert!(cfg.uuid_sstable_identifiers_enabled);
        assert_eq!(cfg.phi_convict_threshold, 10.5);
        assert!(!cfg.dynamic_snitch);
        assert_eq!(cfg.dynamic_snitch_update_interval.millis(), 250);
        assert_eq!(cfg.dynamic_snitch_reset_interval.millis(), 900_000);
        assert_eq!(cfg.dynamic_snitch_badness_threshold, 0.25);
        assert_eq!(
            cfg.initial_location_provider.as_deref(),
            Some("RackDCFileLocationProvider")
        );
        assert_eq!(
            cfg.node_proximity.as_deref(),
            Some("NetworkTopologyProximity")
        );
        assert_eq!(
            cfg.addresses_config.as_deref(),
            Some("Ec2MultiRegionAddressConfig")
        );
        assert!(cfg.prefer_local_connections);
        assert_eq!(cfg.failure_detector, "DeadlineFailureDetector");
        assert!(cfg.partition_denylist_enabled);
        assert!(!cfg.denylist_writes_enabled);
        assert!(!cfg.denylist_reads_enabled);
        assert!(!cfg.denylist_range_reads_enabled);
        assert_eq!(cfg.denylist_refresh.millis(), 700_000);
        assert_eq!(cfg.denylist_initial_load_retry.millis(), 6_000);
        assert_eq!(cfg.denylist_max_keys_per_table, 2_000);
        assert_eq!(cfg.denylist_max_keys_total, 20_000);
        assert_eq!(cfg.denylist_consistency_level, "LOCAL_QUORUM");
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
