// Licensed under Apache License, Version 2.0.

//! Environment variable overrides for configuration.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.config.CassandraRelevantProperties`
//!
//! Defines env → config field mappings with precedence: env > YAML > default.

use crate::config::CassandraConfig;

/// A single env-var → config-field mapping.
struct PropertyMapping {
    env_var: &'static str,
    apply: fn(&mut CassandraConfig, &str),
}

fn default_encryption_options() -> crate::config::EncryptionOptions {
    serde_yaml::from_str("{}").expect("empty encryption options should parse")
}

fn default_jmx_server_options() -> crate::config::JmxServerOptionsConfig {
    serde_yaml::from_str("{}").expect("empty jmx server options should parse")
}

fn default_accord_runtime() -> crate::config::AccordRuntimeConfig {
    serde_yaml::from_str("{}").expect("empty accord runtime config should parse")
}

fn default_auto_repair() -> crate::config::AutoRepairConfig {
    serde_yaml::from_str("{}").expect("empty auto repair config should parse")
}

/// All recognised env-var overrides.
///
/// Convention: `CASSANDRA_` prefix, uppercase, underscores.
const MAPPINGS: &[PropertyMapping] = &[
    // ── Cluster identity ──
    PropertyMapping {
        env_var: "CASSANDRA_CLUSTER_NAME",
        apply: |c, v| c.cluster_name = v.to_string(),
    },
    PropertyMapping {
        env_var: "CASSANDRA_NUM_TOKENS",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.num_tokens = n;
            }
        },
    },
    // ── Networking ──
    PropertyMapping {
        env_var: "CASSANDRA_LISTEN_ADDRESS",
        apply: |c, v| c.listen_address = Some(v.to_string()),
    },
    PropertyMapping {
        env_var: "CASSANDRA_LISTEN_INTERFACE",
        apply: |c, v| c.listen_interface = Some(v.to_string()),
    },
    PropertyMapping {
        env_var: "CASSANDRA_BROADCAST_ADDRESS",
        apply: |c, v| c.broadcast_address = Some(v.to_string()),
    },
    PropertyMapping {
        env_var: "CASSANDRA_RPC_ADDRESS",
        apply: |c, v| c.rpc_address = Some(v.to_string()),
    },
    PropertyMapping {
        env_var: "CASSANDRA_RPC_INTERFACE",
        apply: |c, v| c.rpc_interface = Some(v.to_string()),
    },
    PropertyMapping {
        env_var: "CASSANDRA_BROADCAST_RPC_ADDRESS",
        apply: |c, v| c.broadcast_rpc_address = Some(v.to_string()),
    },
    PropertyMapping {
        env_var: "CASSANDRA_NATIVE_TRANSPORT_PORT",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.native_transport_port = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_STORAGE_PORT",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.storage_port = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_SSL_STORAGE_PORT",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.ssl_storage_port = n;
            }
        },
    },
    // ── Directories ──
    PropertyMapping {
        env_var: "CASSANDRA_DATA_FILE_DIRECTORIES",
        apply: |c, v| {
            c.data_file_directories = Some(v.split(',').map(|s| s.trim().to_string()).collect());
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_COMMITLOG_DIRECTORY",
        apply: |c, v| c.commitlog_directory = Some(v.to_string()),
    },
    PropertyMapping {
        env_var: "CASSANDRA_HINTS_DIRECTORY",
        apply: |c, v| c.hints_directory = Some(v.to_string()),
    },
    PropertyMapping {
        env_var: "CASSANDRA_SAVED_CACHES_DIRECTORY",
        apply: |c, v| c.saved_caches_directory = Some(v.to_string()),
    },
    PropertyMapping {
        env_var: "CASSANDRA_CDC_RAW_DIRECTORY",
        apply: |c, v| c.cdc_raw_directory = Some(v.to_string()),
    },
    PropertyMapping {
        env_var: "CASSANDRA_LOCAL_SYSTEM_DATA_FILE_DIRECTORY",
        apply: |c, v| c.local_system_data_file_directory = Some(v.to_string()),
    },
    PropertyMapping {
        env_var: "CASSANDRA_CDC_BLOCK_WRITES",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.cdc_block_writes = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_CDC_ON_REPAIR_ENABLED",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.cdc_on_repair_enabled = b;
            }
        },
    },
    // ── Thread pools ──
    PropertyMapping {
        env_var: "CASSANDRA_CONCURRENT_READS",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.concurrent_reads = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_CONCURRENT_WRITES",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.concurrent_writes = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_CONCURRENT_COUNTER_WRITES",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.concurrent_counter_writes = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_CONCURRENT_MV_WRITES",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.concurrent_materialized_view_writes = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_CONCURRENT_MV_BUILDERS",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.concurrent_materialized_view_builders = n;
            }
        },
    },
    // ── Hints ──
    PropertyMapping {
        env_var: "CASSANDRA_HINTED_HANDOFF_ENABLED",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.hinted_handoff_enabled = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_MAX_HINTS_DELIVERY_THREADS",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.max_hints_delivery_threads = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_TRANSFER_HINTS_ON_DECOMMISSION",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.transfer_hints_on_decommission = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_HINT_WINDOW_PERSISTENT_ENABLED",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.hint_window_persistent_enabled = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_BATCHLOG_ENDPOINT_STRATEGY",
        apply: |c, v| c.batchlog_endpoint_strategy = v.to_string(),
    },
    PropertyMapping {
        env_var: "CASSANDRA_HEAP_DUMP_PATH",
        apply: |c, v| c.heap_dump_path = Some(v.to_string()),
    },
    PropertyMapping {
        env_var: "CASSANDRA_DUMP_HEAP_ON_UNCAUGHT_EXCEPTION",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.dump_heap_on_uncaught_exception = b;
            }
        },
    },
    // ── Commitlog ──
    PropertyMapping {
        env_var: "CASSANDRA_COMMITLOG_SYNC",
        apply: |c, v| c.commitlog_sync = v.to_string(),
    },
    PropertyMapping {
        env_var: "CASSANDRA_COMMITLOG_TOTAL_SPACE",
        apply: |c, v| {
            if let Ok(size) = v.parse() {
                c.commitlog_total_space = Some(size);
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_DISK_ACCESS_MODE",
        apply: |c, v| c.disk_access_mode = Some(v.to_string()),
    },
    PropertyMapping {
        env_var: "CASSANDRA_COMPACTION_READ_DISK_ACCESS_MODE",
        apply: |c, v| c.compaction_read_disk_access_mode = v.to_string(),
    },
    PropertyMapping {
        env_var: "CASSANDRA_FLUSH_COMPRESSION",
        apply: |c, v| c.flush_compression = v.to_string(),
    },
    // ── Native transport limits ──
    PropertyMapping {
        env_var: "CASSANDRA_NATIVE_TRANSPORT_MAX_CONCURRENT_CONNECTIONS",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.native_transport_max_concurrent_connections = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_NATIVE_TRANSPORT_MAX_FRAME_SIZE",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.native_transport_max_frame_size = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_START_NATIVE_TRANSPORT",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.start_native_transport = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_NATIVE_TRANSPORT_MAX_THREADS",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.native_transport_max_threads = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_NATIVE_TRANSPORT_MAX_AUTH_THREADS",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.native_transport_max_auth_threads = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_NATIVE_TRANSPORT_ALLOW_OLDER_PROTOCOLS",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.native_transport_allow_older_protocols = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_NATIVE_TRANSPORT_IDLE_TIMEOUT",
        apply: |c, v| {
            if let Ok(duration) = v.parse() {
                c.native_transport_idle_timeout = duration;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_NATIVE_TRANSPORT_MAX_REQUESTS_PER_SECOND",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.native_transport_max_requests_per_second = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_NATIVE_TRANSPORT_RATE_LIMITING",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.native_transport_rate_limiting_enabled = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_NATIVE_TRANSPORT_FLUSH_IN_BATCHES_LEGACY",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.native_transport_flush_in_batches_legacy = b;
            }
        },
    },
    // ── Internode messaging / streaming state ──
    PropertyMapping {
        env_var: "CASSANDRA_INTERNODE_TIMEOUT",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.internode_timeout = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_INTERNODE_TCP_CONNECT_TIMEOUT",
        apply: |c, v| {
            if let Ok(duration) = v.parse() {
                c.internode_tcp_connect_timeout = duration;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_INTERNODE_APPLICATION_SEND_QUEUE_CAPACITY",
        apply: |c, v| {
            if let Ok(size) = v.parse() {
                c.internode_application_send_queue_capacity = size;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_STREAMING_KEEP_ALIVE_PERIOD",
        apply: |c, v| {
            if let Ok(duration) = v.parse() {
                c.streaming_keep_alive_period = duration;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_STREAMING_STATS_ENABLED",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.streaming_stats_enabled = b;
            }
        },
    },
    // ── Topology / denylist ──
    PropertyMapping {
        env_var: "CASSANDRA_INITIAL_LOCATION_PROVIDER",
        apply: |c, v| c.initial_location_provider = Some(v.to_string()),
    },
    PropertyMapping {
        env_var: "CASSANDRA_NODE_PROXIMITY",
        apply: |c, v| c.node_proximity = Some(v.to_string()),
    },
    PropertyMapping {
        env_var: "CASSANDRA_ADDRESSES_CONFIG",
        apply: |c, v| c.addresses_config = Some(v.to_string()),
    },
    PropertyMapping {
        env_var: "CASSANDRA_PREFER_LOCAL_CONNECTIONS",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.prefer_local_connections = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_FAILURE_DETECTOR",
        apply: |c, v| c.failure_detector = v.to_string(),
    },
    PropertyMapping {
        env_var: "CASSANDRA_INTERNODE_COMPRESSION",
        apply: |c, v| c.internode_compression = v.to_string(),
    },
    PropertyMapping {
        env_var: "CASSANDRA_INTER_DC_TCP_NODELAY",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.inter_dc_tcp_nodelay = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_PARTITION_DENYLIST_ENABLED",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.partition_denylist_enabled = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_DENYLIST_CONSISTENCY_LEVEL",
        apply: |c, v| c.denylist_consistency_level = v.to_string(),
    },
    // ── TLS / encryption ──
    PropertyMapping {
        env_var: "CASSANDRA_CLIENT_ENCRYPTION_ENABLED",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.client_encryption_options
                    .get_or_insert_with(default_encryption_options)
                    .enabled = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_SERVER_INTERNODE_ENCRYPTION",
        apply: |c, v| {
            c.server_encryption_options
                .get_or_insert_with(default_encryption_options)
                .internode_encryption = v.to_string();
        },
    },
    // ── Memtable / compaction runtime ──
    PropertyMapping {
        env_var: "CASSANDRA_MEMTABLE_HEAP_SPACE",
        apply: |c, v| {
            if let Ok(size) = v.parse() {
                c.memtable_heap_space = Some(size);
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_MEMTABLE_OFFHEAP_SPACE",
        apply: |c, v| {
            if let Ok(size) = v.parse() {
                c.memtable_offheap_space = Some(size);
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_MEMTABLE_ALLOCATION_TYPE",
        apply: |c, v| c.memtable_allocation_type = v.to_string(),
    },
    PropertyMapping {
        env_var: "CASSANDRA_MEMTABLE_FLUSH_WRITERS",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.memtable_flush_writers = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_CONCURRENT_COMPACTORS",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.concurrent_compactors = Some(n);
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_CONCURRENT_VALIDATIONS",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.concurrent_validations = Some(n);
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_NETWORKING_CACHE_SIZE",
        apply: |c, v| {
            if let Ok(size) = v.parse() {
                c.networking_cache_size = Some(size);
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_FILE_CACHE_ENABLED",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.file_cache_enabled = b;
            }
        },
    },
    // ── SSTable / snapshot storage ──
    PropertyMapping {
        env_var: "CASSANDRA_INCREMENTAL_BACKUPS",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.incremental_backups = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_AUTO_SNAPSHOT",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.auto_snapshot = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_SNAPSHOT_BEFORE_COMPACTION",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.snapshot_before_compaction = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_COLUMN_INDEX_CACHE_SIZE",
        apply: |c, v| {
            if let Ok(size) = v.parse() {
                c.column_index_cache_size = size;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_STREAM_ENTIRE_SSTABLES",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.stream_entire_sstables = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_MATERIALIZED_VIEWS_ENABLED",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.materialized_views_enabled = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_MATERIALIZED_VIEWS_ON_REPAIR_ENABLED",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.materialized_views_on_repair_enabled = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_SASI_INDEXES_ENABLED",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.sasi_indexes_enabled = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_TRANSIENT_REPLICATION_ENABLED",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.transient_replication_enabled = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_STORAGE_COMPATIBILITY_MODE",
        apply: |c, v| c.storage_compatibility_mode = v.to_string(),
    },
    PropertyMapping {
        env_var: "CASSANDRA_CORRUPTED_TOMBSTONE_STRATEGY",
        apply: |c, v| c.corrupted_tombstone_strategy = v.to_string(),
    },
    PropertyMapping {
        env_var: "CASSANDRA_DEFAULT_KEYSPACE_RF",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.default_keyspace_rf = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_AUTH_READ_CONSISTENCY_LEVEL",
        apply: |c, v| c.auth_read_consistency_level = Some(v.to_string()),
    },
    PropertyMapping {
        env_var: "CASSANDRA_AUTH_WRITE_CONSISTENCY_LEVEL",
        apply: |c, v| c.auth_write_consistency_level = Some(v.to_string()),
    },
    PropertyMapping {
        env_var: "CASSANDRA_AUTH_CACHE_WARMING_ENABLED",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.auth_cache_warming_enabled = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_DYNAMIC_DATA_MASKING_ENABLED",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.dynamic_data_masking_enabled = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_AUTOMATIC_SSTABLE_UPGRADE",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.automatic_sstable_upgrade = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_TOMBSTONE_WARN_THRESHOLD",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.tombstone_warn_threshold = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_TOMBSTONE_FAILURE_THRESHOLD",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.tombstone_failure_threshold = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_BATCH_SIZE_WARN_THRESHOLD",
        apply: |c, v| {
            if let Ok(size) = v.parse() {
                c.batch_size_warn_threshold = size;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_BATCH_SIZE_FAIL_THRESHOLD",
        apply: |c, v| {
            if let Ok(size) = v.parse() {
                c.batch_size_fail_threshold = size;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_READ_THRESHOLDS_ENABLED",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.read_thresholds_enabled = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_COORDINATOR_READ_SIZE_WARN_THRESHOLD",
        apply: |c, v| {
            if let Ok(size) = v.parse() {
                c.coordinator_read_size_warn_threshold = Some(size);
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_COORDINATOR_READ_SIZE_FAIL_THRESHOLD",
        apply: |c, v| {
            if let Ok(size) = v.parse() {
                c.coordinator_read_size_fail_threshold = Some(size);
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_LOCAL_READ_SIZE_WARN_THRESHOLD",
        apply: |c, v| {
            if let Ok(size) = v.parse() {
                c.local_read_size_warn_threshold = Some(size);
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_LOCAL_READ_SIZE_FAIL_THRESHOLD",
        apply: |c, v| {
            if let Ok(size) = v.parse() {
                c.local_read_size_fail_threshold = Some(size);
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_ROW_INDEX_READ_SIZE_WARN_THRESHOLD",
        apply: |c, v| {
            if let Ok(size) = v.parse() {
                c.row_index_read_size_warn_threshold = Some(size);
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_ROW_INDEX_READ_SIZE_FAIL_THRESHOLD",
        apply: |c, v| {
            if let Ok(size) = v.parse() {
                c.row_index_read_size_fail_threshold = Some(size);
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_USE_STATEMENTS_ENABLED",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.use_statements_enabled = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_JMX_ENABLED",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.jmx_server_options
                    .get_or_insert_with(default_jmx_server_options)
                    .enabled = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_JMX_PORT",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.jmx_server_options
                    .get_or_insert_with(default_jmx_server_options)
                    .jmx_port = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_ACCORD_ENABLED",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.accord.get_or_insert_with(default_accord_runtime).enabled = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_AUTO_REPAIR_ENABLED",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.auto_repair
                    .get_or_insert_with(default_auto_repair)
                    .enabled = b;
            }
        },
    },
    // ── Failure policies ──
    PropertyMapping {
        env_var: "CASSANDRA_DISK_FAILURE_POLICY",
        apply: |c, v| c.disk_failure_policy = v.to_string(),
    },
    PropertyMapping {
        env_var: "CASSANDRA_COMMIT_FAILURE_POLICY",
        apply: |c, v| c.commit_failure_policy = v.to_string(),
    },
    // ── Auth cache ──
    PropertyMapping {
        env_var: "CASSANDRA_PERMISSIONS_VALIDITY_MS",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.permissions_validity_in_ms = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_ROLES_VALIDITY_MS",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.roles_validity_in_ms = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_CREDENTIALS_VALIDITY_MS",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.credentials_validity_in_ms = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_TRAVERSE_AUTH_FROM_ROOT",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.traverse_auth_from_root = b;
            }
        },
    },
    // ── Admin ──
    PropertyMapping {
        env_var: "CASSANDRA_ADMIN_PORT",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.admin_port = n;
            }
        },
    },
];

/// Apply environment variable overrides to a config.
///
/// For each recognised `CASSANDRA_*` env var that is set, its value
/// overrides the corresponding config field. Unknown env vars are ignored.
pub fn apply_overrides(config: &mut CassandraConfig) {
    for mapping in MAPPINGS {
        if let Ok(val) = std::env::var(mapping.env_var) {
            tracing::debug!(env = mapping.env_var, value = %val, "env override applied");
            (mapping.apply)(config, &val);
        }
    }
}

/// List all recognised property env-var names (for diagnostics).
pub fn known_properties() -> Vec<&'static str> {
    MAPPINGS.iter().map(|m| m.env_var).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CassandraConfig;

    #[test]
    fn apply_cluster_name_override() {
        let mut cfg = CassandraConfig::default();
        apply_known_mapping(&mut cfg, "CASSANDRA_CLUSTER_NAME", "EnvCluster");
        assert_eq!(cfg.cluster_name, "EnvCluster");
    }

    #[test]
    fn apply_port_override() {
        let mut cfg = CassandraConfig::default();
        apply_known_mapping(&mut cfg, "CASSANDRA_NATIVE_TRANSPORT_PORT", "19042");
        assert_eq!(cfg.native_transport_port, 19042);
    }

    #[test]
    fn invalid_port_ignored() {
        let mut cfg = CassandraConfig::default();
        apply_known_mapping(&mut cfg, "CASSANDRA_NATIVE_TRANSPORT_PORT", "not_a_number");
        assert_eq!(cfg.native_transport_port, 9042); // unchanged
    }

    #[test]
    fn data_dirs_comma_separated() {
        let mut cfg = CassandraConfig::default();
        apply_known_mapping(
            &mut cfg,
            "CASSANDRA_DATA_FILE_DIRECTORIES",
            "/data1, /data2, /data3",
        );
        let dirs = cfg.data_file_directories.unwrap();
        assert_eq!(dirs, vec!["/data1", "/data2", "/data3"]);
    }

    #[test]
    fn known_properties_not_empty() {
        let props = known_properties();
        assert!(props.len() >= 20);
        assert!(props.contains(&"CASSANDRA_CLUSTER_NAME"));
        assert!(props.contains(&"CASSANDRA_MEMTABLE_HEAP_SPACE"));
        assert!(props.contains(&"CASSANDRA_NATIVE_TRANSPORT_MAX_THREADS"));
        assert!(props.contains(&"CASSANDRA_BROADCAST_RPC_ADDRESS"));
        assert!(props.contains(&"CASSANDRA_STREAM_ENTIRE_SSTABLES"));
        assert!(props.contains(&"CASSANDRA_INTERNODE_TIMEOUT"));
        assert!(props.contains(&"CASSANDRA_PARTITION_DENYLIST_ENABLED"));
        assert!(props.contains(&"CASSANDRA_CLIENT_ENCRYPTION_ENABLED"));
        assert!(props.contains(&"CASSANDRA_SERVER_INTERNODE_ENCRYPTION"));
        assert!(props.contains(&"CASSANDRA_INTERNODE_COMPRESSION"));
        assert!(props.contains(&"CASSANDRA_STORAGE_COMPATIBILITY_MODE"));
        assert!(props.contains(&"CASSANDRA_BATCHLOG_ENDPOINT_STRATEGY"));
        assert!(props.contains(&"CASSANDRA_CDC_BLOCK_WRITES"));
        assert!(props.contains(&"CASSANDRA_TRAVERSE_AUTH_FROM_ROOT"));
        assert!(props.contains(&"CASSANDRA_FILE_CACHE_ENABLED"));
        assert!(props.contains(&"CASSANDRA_AUTH_CACHE_WARMING_ENABLED"));
        assert!(props.contains(&"CASSANDRA_USE_STATEMENTS_ENABLED"));
        assert!(props.contains(&"CASSANDRA_READ_THRESHOLDS_ENABLED"));
        assert!(props.contains(&"CASSANDRA_JMX_PORT"));
        assert!(props.contains(&"CASSANDRA_ACCORD_ENABLED"));
        assert!(props.contains(&"CASSANDRA_AUTO_REPAIR_ENABLED"));
    }

    #[test]
    fn apply_upstream_runtime_overrides() {
        let mut cfg = CassandraConfig::default();
        for (env_var, value) in [
            ("CASSANDRA_START_NATIVE_TRANSPORT", "false"),
            ("CASSANDRA_NATIVE_TRANSPORT_MAX_THREADS", "64"),
            ("CASSANDRA_NATIVE_TRANSPORT_ALLOW_OLDER_PROTOCOLS", "false"),
            ("CASSANDRA_NATIVE_TRANSPORT_IDLE_TIMEOUT", "45s"),
            ("CASSANDRA_BROADCAST_RPC_ADDRESS", "10.0.0.12"),
            ("CASSANDRA_INTERNODE_TIMEOUT", "false"),
            ("CASSANDRA_INTERNODE_TCP_CONNECT_TIMEOUT", "5s"),
            (
                "CASSANDRA_INTERNODE_APPLICATION_SEND_QUEUE_CAPACITY",
                "16MiB",
            ),
            ("CASSANDRA_STREAMING_KEEP_ALIVE_PERIOD", "30s"),
            ("CASSANDRA_STREAMING_STATS_ENABLED", "false"),
            (
                "CASSANDRA_INITIAL_LOCATION_PROVIDER",
                "RackDCFileLocationProvider",
            ),
            ("CASSANDRA_NODE_PROXIMITY", "NetworkTopologyProximity"),
            ("CASSANDRA_ADDRESSES_CONFIG", "Ec2MultiRegionAddressConfig"),
            ("CASSANDRA_PREFER_LOCAL_CONNECTIONS", "true"),
            ("CASSANDRA_FAILURE_DETECTOR", "DeadlineFailureDetector"),
            ("CASSANDRA_INTERNODE_COMPRESSION", "all"),
            ("CASSANDRA_INTER_DC_TCP_NODELAY", "true"),
            ("CASSANDRA_PARTITION_DENYLIST_ENABLED", "true"),
            ("CASSANDRA_DENYLIST_CONSISTENCY_LEVEL", "LOCAL_QUORUM"),
            ("CASSANDRA_CLIENT_ENCRYPTION_ENABLED", "true"),
            ("CASSANDRA_SERVER_INTERNODE_ENCRYPTION", "all"),
            ("CASSANDRA_MEMTABLE_HEAP_SPACE", "128MiB"),
            ("CASSANDRA_MEMTABLE_ALLOCATION_TYPE", "offheap_buffers"),
            ("CASSANDRA_CONCURRENT_COMPACTORS", "3"),
            ("CASSANDRA_INCREMENTAL_BACKUPS", "true"),
            ("CASSANDRA_STREAM_ENTIRE_SSTABLES", "false"),
            ("CASSANDRA_COLUMN_INDEX_CACHE_SIZE", "16KiB"),
            ("CASSANDRA_MATERIALIZED_VIEWS_ENABLED", "true"),
            ("CASSANDRA_SASI_INDEXES_ENABLED", "true"),
            ("CASSANDRA_TRANSIENT_REPLICATION_ENABLED", "true"),
            ("CASSANDRA_STORAGE_COMPATIBILITY_MODE", "UPGRADING"),
            ("CASSANDRA_TOMBSTONE_WARN_THRESHOLD", "2000"),
            ("CASSANDRA_TOMBSTONE_FAILURE_THRESHOLD", "200000"),
            ("CASSANDRA_BATCH_SIZE_WARN_THRESHOLD", "6KiB"),
            ("CASSANDRA_BATCH_SIZE_FAIL_THRESHOLD", "60KiB"),
            ("CASSANDRA_READ_THRESHOLDS_ENABLED", "true"),
            ("CASSANDRA_COORDINATOR_READ_SIZE_WARN_THRESHOLD", "1MiB"),
            ("CASSANDRA_COORDINATOR_READ_SIZE_FAIL_THRESHOLD", "2MiB"),
            ("CASSANDRA_LOCAL_READ_SIZE_WARN_THRESHOLD", "512KiB"),
            ("CASSANDRA_LOCAL_READ_SIZE_FAIL_THRESHOLD", "4MiB"),
            ("CASSANDRA_ROW_INDEX_READ_SIZE_WARN_THRESHOLD", "256KiB"),
            ("CASSANDRA_ROW_INDEX_READ_SIZE_FAIL_THRESHOLD", "1MiB"),
            ("CASSANDRA_TRANSFER_HINTS_ON_DECOMMISSION", "false"),
            ("CASSANDRA_HINT_WINDOW_PERSISTENT_ENABLED", "false"),
            ("CASSANDRA_BATCHLOG_ENDPOINT_STRATEGY", "dynamic"),
            ("CASSANDRA_HEAP_DUMP_PATH", "/tmp/heapdump"),
            ("CASSANDRA_DUMP_HEAP_ON_UNCAUGHT_EXCEPTION", "true"),
            ("CASSANDRA_LOCAL_SYSTEM_DATA_FILE_DIRECTORY", "/data/system"),
            ("CASSANDRA_CDC_BLOCK_WRITES", "false"),
            ("CASSANDRA_CDC_ON_REPAIR_ENABLED", "false"),
            ("CASSANDRA_DISK_ACCESS_MODE", "mmap"),
            ("CASSANDRA_COMPACTION_READ_DISK_ACCESS_MODE", "direct"),
            ("CASSANDRA_FLUSH_COMPRESSION", "table"),
            ("CASSANDRA_TRAVERSE_AUTH_FROM_ROOT", "true"),
            ("CASSANDRA_NETWORKING_CACHE_SIZE", "64MiB"),
            ("CASSANDRA_FILE_CACHE_ENABLED", "true"),
            ("CASSANDRA_NATIVE_TRANSPORT_FLUSH_IN_BATCHES_LEGACY", "true"),
            ("CASSANDRA_CORRUPTED_TOMBSTONE_STRATEGY", "warn"),
            ("CASSANDRA_DEFAULT_KEYSPACE_RF", "3"),
            ("CASSANDRA_AUTH_READ_CONSISTENCY_LEVEL", "LOCAL_QUORUM"),
            ("CASSANDRA_AUTH_WRITE_CONSISTENCY_LEVEL", "EACH_QUORUM"),
            ("CASSANDRA_AUTH_CACHE_WARMING_ENABLED", "true"),
            ("CASSANDRA_DYNAMIC_DATA_MASKING_ENABLED", "true"),
            ("CASSANDRA_AUTOMATIC_SSTABLE_UPGRADE", "true"),
            ("CASSANDRA_MATERIALIZED_VIEWS_ON_REPAIR_ENABLED", "false"),
            ("CASSANDRA_USE_STATEMENTS_ENABLED", "false"),
            ("CASSANDRA_JMX_ENABLED", "true"),
            ("CASSANDRA_JMX_PORT", "7299"),
            ("CASSANDRA_ACCORD_ENABLED", "true"),
            ("CASSANDRA_AUTO_REPAIR_ENABLED", "true"),
        ] {
            apply_known_mapping(&mut cfg, env_var, value);
        }

        assert!(!cfg.start_native_transport);
        assert_eq!(cfg.native_transport_max_threads, 64);
        assert!(!cfg.native_transport_allow_older_protocols);
        assert_eq!(cfg.native_transport_idle_timeout.millis(), 45_000);
        assert_eq!(cfg.broadcast_rpc_address.as_deref(), Some("10.0.0.12"));
        assert!(!cfg.internode_timeout);
        assert_eq!(cfg.internode_tcp_connect_timeout.millis(), 5_000);
        assert_eq!(
            cfg.internode_application_send_queue_capacity.mebibytes(),
            16
        );
        assert_eq!(cfg.streaming_keep_alive_period.millis(), 30_000);
        assert!(!cfg.streaming_stats_enabled);
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
        assert_eq!(cfg.internode_compression, "all");
        assert!(cfg.inter_dc_tcp_nodelay);
        assert!(cfg.partition_denylist_enabled);
        assert_eq!(cfg.denylist_consistency_level, "LOCAL_QUORUM");
        assert!(cfg.client_encryption_options.unwrap().enabled);
        assert_eq!(
            cfg.server_encryption_options.unwrap().internode_encryption,
            "all"
        );
        assert_eq!(cfg.memtable_heap_space.unwrap().mebibytes(), 128);
        assert_eq!(cfg.memtable_allocation_type, "offheap_buffers");
        assert_eq!(cfg.concurrent_compactors, Some(3));
        assert!(cfg.incremental_backups);
        assert!(!cfg.stream_entire_sstables);
        assert_eq!(cfg.column_index_cache_size.kibibytes(), 16);
        assert!(cfg.materialized_views_enabled);
        assert!(cfg.sasi_indexes_enabled);
        assert!(cfg.transient_replication_enabled);
        assert_eq!(cfg.storage_compatibility_mode, "UPGRADING");
        assert_eq!(cfg.tombstone_warn_threshold, 2_000);
        assert_eq!(cfg.tombstone_failure_threshold, 200_000);
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
        assert!(!cfg.transfer_hints_on_decommission);
        assert!(!cfg.hint_window_persistent_enabled);
        assert_eq!(cfg.batchlog_endpoint_strategy, "dynamic");
        assert_eq!(cfg.heap_dump_path.as_deref(), Some("/tmp/heapdump"));
        assert!(cfg.dump_heap_on_uncaught_exception);
        assert_eq!(
            cfg.local_system_data_file_directory.as_deref(),
            Some("/data/system")
        );
        assert!(!cfg.cdc_block_writes);
        assert!(!cfg.cdc_on_repair_enabled);
        assert_eq!(cfg.disk_access_mode.as_deref(), Some("mmap"));
        assert_eq!(cfg.compaction_read_disk_access_mode, "direct");
        assert_eq!(cfg.flush_compression, "table");
        assert!(cfg.traverse_auth_from_root);
        assert_eq!(cfg.networking_cache_size.unwrap().mebibytes(), 64);
        assert!(cfg.file_cache_enabled);
        assert!(cfg.native_transport_flush_in_batches_legacy);
        assert_eq!(cfg.corrupted_tombstone_strategy, "warn");
        assert_eq!(cfg.default_keyspace_rf, 3);
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
        assert!(cfg.automatic_sstable_upgrade);
        assert!(!cfg.materialized_views_on_repair_enabled);
        assert!(!cfg.use_statements_enabled);
        assert!(cfg.jmx_server_options.as_ref().unwrap().enabled);
        assert_eq!(cfg.jmx_server_options.unwrap().jmx_port, 7299);
        assert!(cfg.accord.unwrap().enabled);
        assert!(cfg.auto_repair.unwrap().enabled);
    }

    #[test]
    fn unset_env_vars_leave_defaults() {
        for name in known_properties() {
            unsafe { std::env::remove_var(name) };
        }
        let mut cfg = CassandraConfig::default();
        let original = CassandraConfig::default();
        apply_overrides(&mut cfg);
        assert_eq!(cfg.cluster_name, original.cluster_name);
        assert_eq!(cfg.native_transport_port, original.native_transport_port);
    }

    fn apply_known_mapping(config: &mut CassandraConfig, env_var: &str, value: &str) {
        let mapping = MAPPINGS.iter().find(|mapping| mapping.env_var == env_var);
        assert!(mapping.is_some(), "missing mapping for {env_var}");
        (mapping.unwrap().apply)(config, value);
    }
}
