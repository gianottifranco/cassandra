// Licensed under Apache License, Version 2.0.

//! Configuration validation.

use crate::config::CassandraConfig;

/// A configuration validation error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConfigError {
    pub field: String,
    pub message: String,
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "config error [{}]: {}", self.field, self.message)
    }
}
impl std::error::Error for ConfigError {}

/// Validate a configuration, returning all errors found.
pub fn validate(config: &CassandraConfig) -> Vec<ConfigError> {
    let mut errors = Vec::new();

    // Cluster name must not be empty
    if config.cluster_name.is_empty() {
        errors.push(ConfigError {
            field: "cluster_name".into(),
            message: "cluster_name must not be empty".into(),
        });
    }

    // num_tokens must be >= 1
    if config.num_tokens == 0 {
        errors.push(ConfigError {
            field: "num_tokens".into(),
            message: "num_tokens must be at least 1".into(),
        });
    }

    if config.endpoint_snitch.trim().is_empty() {
        errors.push(ConfigError {
            field: "endpoint_snitch".into(),
            message: "endpoint_snitch must not be empty".into(),
        });
    }

    if config.listen_address.is_some() && config.listen_interface.is_some() {
        errors.push(ConfigError {
            field: "listen_address".into(),
            message: "set listen_address OR listen_interface, not both".into(),
        });
    }

    if config.rpc_address.is_some() && config.rpc_interface.is_some() {
        errors.push(ConfigError {
            field: "rpc_address".into(),
            message: "set rpc_address OR rpc_interface, not both".into(),
        });
    }

    if is_wildcard_address(config.listen_address.as_deref()) {
        errors.push(ConfigError {
            field: "listen_address".into(),
            message: "listen_address must not be 0.0.0.0".into(),
        });
    }

    if is_wildcard_address(config.broadcast_address.as_deref()) {
        errors.push(ConfigError {
            field: "broadcast_address".into(),
            message: "broadcast_address must not be 0.0.0.0".into(),
        });
    }

    if config.listen_on_broadcast_address && config.broadcast_address.is_none() {
        errors.push(ConfigError {
            field: "listen_on_broadcast_address".into(),
            message: "listen_on_broadcast_address requires broadcast_address".into(),
        });
    }

    if is_wildcard_address(config.broadcast_rpc_address.as_deref()) {
        errors.push(ConfigError {
            field: "broadcast_rpc_address".into(),
            message: "broadcast_rpc_address must not be 0.0.0.0".into(),
        });
    }

    if is_wildcard_address(config.rpc_address.as_deref()) && config.broadcast_rpc_address.is_none()
    {
        errors.push(ConfigError {
            field: "broadcast_rpc_address".into(),
            message: "broadcast_rpc_address is required when rpc_address is 0.0.0.0".into(),
        });
    }

    // Port range checks
    if config.native_transport_port == 0 {
        errors.push(ConfigError {
            field: "native_transport_port".into(),
            message: "native_transport_port must be > 0".into(),
        });
    }
    if config.storage_port == 0 {
        errors.push(ConfigError {
            field: "storage_port".into(),
            message: "storage_port must be > 0".into(),
        });
    }

    // Concurrent must be > 0
    for (name, val) in [
        ("concurrent_reads", config.concurrent_reads),
        ("concurrent_writes", config.concurrent_writes),
        (
            "concurrent_counter_writes",
            config.concurrent_counter_writes,
        ),
    ] {
        if val == 0 {
            errors.push(ConfigError {
                field: name.into(),
                message: format!("{} must be > 0", name),
            });
        }
    }

    // commitlog_sync must be valid
    match config.commitlog_sync.as_str() {
        "periodic" | "batch" | "group" => {}
        other => {
            errors.push(ConfigError {
                field: "commitlog_sync".into(),
                message: format!(
                    "invalid commitlog_sync mode: '{}' (expected periodic|batch|group)",
                    other
                ),
            });
        }
    }

    match config.commit_failure_policy.as_str() {
        "die" | "stop" | "stop_commit" | "ignore" => {}
        other => {
            errors.push(ConfigError {
                field: "commit_failure_policy".into(),
                message: format!("invalid commit_failure_policy: '{}'", other),
            });
        }
    }

    // disk_failure_policy validation
    match config.disk_failure_policy.as_str() {
        "die" | "stop_paranoid" | "stop" | "best_effort" | "ignore" => {}
        other => {
            errors.push(ConfigError {
                field: "disk_failure_policy".into(),
                message: format!("invalid disk_failure_policy: '{}'", other),
            });
        }
    }

    // commitlog_segment_size cross-check (must be positive if set)
    if let Some(ref seg_size) = config.commitlog_segment_size {
        if seg_size.bytes() == 0 {
            errors.push(ConfigError {
                field: "commitlog_segment_size".into(),
                message: "commitlog_segment_size must be > 0".into(),
            });
        }
    }

    for (name, size) in [
        ("commitlog_total_space", config.commitlog_total_space),
        ("memtable_heap_space", config.memtable_heap_space),
        ("memtable_offheap_space", config.memtable_offheap_space),
        ("file_cache_size", config.file_cache_size),
        ("networking_cache_size", config.networking_cache_size),
        ("max_value_size", Some(config.max_value_size)),
        ("max_hints_file_size", Some(config.max_hints_file_size)),
        (
            "batchlog_replay_throttle",
            Some(config.batchlog_replay_throttle),
        ),
        ("cdc_total_space", Some(config.cdc_total_space)),
        ("index_summary_capacity", config.index_summary_capacity),
        ("column_index_size", config.column_index_size),
        (
            "column_index_cache_size",
            Some(config.column_index_cache_size),
        ),
        (
            "sstable_preemptive_open_interval",
            config.sstable_preemptive_open_interval,
        ),
        (
            "batch_size_warn_threshold",
            Some(config.batch_size_warn_threshold),
        ),
        (
            "batch_size_fail_threshold",
            Some(config.batch_size_fail_threshold),
        ),
        (
            "coordinator_read_size_warn_threshold",
            config.coordinator_read_size_warn_threshold,
        ),
        (
            "coordinator_read_size_fail_threshold",
            config.coordinator_read_size_fail_threshold,
        ),
        (
            "local_read_size_warn_threshold",
            config.local_read_size_warn_threshold,
        ),
        (
            "local_read_size_fail_threshold",
            config.local_read_size_fail_threshold,
        ),
        (
            "row_index_read_size_warn_threshold",
            config.row_index_read_size_warn_threshold,
        ),
        (
            "row_index_read_size_fail_threshold",
            config.row_index_read_size_fail_threshold,
        ),
    ] {
        if let Some(size) = size {
            if size.bytes() == 0 {
                errors.push(ConfigError {
                    field: name.into(),
                    message: format!("{name} must be > 0"),
                });
            }
        }
    }

    for (name, timeout) in [
        ("request_timeout", config.request_timeout),
        ("read_request_timeout", config.read_request_timeout),
        ("range_request_timeout", config.range_request_timeout),
        ("write_request_timeout", config.write_request_timeout),
        (
            "counter_write_request_timeout",
            config.counter_write_request_timeout,
        ),
        ("cas_contention_timeout", config.cas_contention_timeout),
        ("truncate_request_timeout", config.truncate_request_timeout),
        ("repair_request_timeout", config.repair_request_timeout),
        ("hints_flush_period", config.hints_flush_period),
        ("trace_type_query_ttl", config.trace_type_query_ttl),
        ("trace_type_repair_ttl", config.trace_type_repair_ttl),
        (
            "compression_dictionary_refresh_interval",
            config.compression_dictionary_refresh_interval,
        ),
        (
            "compression_dictionary_refresh_initial_delay",
            config.compression_dictionary_refresh_initial_delay,
        ),
        (
            "compression_dictionary_cache_expire",
            config.compression_dictionary_cache_expire,
        ),
    ] {
        if timeout.millis() == 0 {
            errors.push(ConfigError {
                field: name.into(),
                message: format!("{name} must be > 0"),
            });
        }
    }

    for (name, timeout) in [
        (
            "commitlog_sync_group_window",
            config.commitlog_sync_group_window,
        ),
        (
            "periodic_commitlog_sync_lag_block",
            config.periodic_commitlog_sync_lag_block,
        ),
    ] {
        if let Some(timeout) = timeout {
            if timeout.millis() == 0 {
                errors.push(ConfigError {
                    field: name.into(),
                    message: format!("{name} must be > 0 when set"),
                });
            }
        }
    }

    if config.batch_size_fail_threshold < config.batch_size_warn_threshold {
        errors.push(ConfigError {
            field: "batch_size_fail_threshold".into(),
            message: "batch_size_fail_threshold must be >= batch_size_warn_threshold".into(),
        });
    }

    validate_optional_size_threshold_pair(
        "coordinator_read_size_warn_threshold",
        config.coordinator_read_size_warn_threshold,
        "coordinator_read_size_fail_threshold",
        config.coordinator_read_size_fail_threshold,
        &mut errors,
    );
    validate_optional_size_threshold_pair(
        "local_read_size_warn_threshold",
        config.local_read_size_warn_threshold,
        "local_read_size_fail_threshold",
        config.local_read_size_fail_threshold,
        &mut errors,
    );
    validate_optional_size_threshold_pair(
        "row_index_read_size_warn_threshold",
        config.row_index_read_size_warn_threshold,
        "row_index_read_size_fail_threshold",
        config.row_index_read_size_fail_threshold,
        &mut errors,
    );

    if config.tombstone_failure_threshold < config.tombstone_warn_threshold {
        errors.push(ConfigError {
            field: "tombstone_failure_threshold".into(),
            message: "tombstone_failure_threshold must be >= tombstone_warn_threshold".into(),
        });
    }

    if config
        .replica_filtering_protection
        .cached_rows_fail_threshold
        < config
            .replica_filtering_protection
            .cached_rows_warn_threshold
    {
        errors.push(ConfigError {
            field: "replica_filtering_protection.cached_rows_fail_threshold".into(),
            message: "cached_rows_fail_threshold must be >= cached_rows_warn_threshold".into(),
        });
    }

    if config.compression_dictionary_cache_size == 0 {
        errors.push(ConfigError {
            field: "compression_dictionary_cache_size".into(),
            message: "compression_dictionary_cache_size must be > 0".into(),
        });
    }

    for (name, value) in [
        (
            "sstables_per_read_log_threshold",
            config.sstables_per_read_log_threshold,
        ),
        (
            "unlogged_batch_across_partitions_warn_threshold",
            config.unlogged_batch_across_partitions_warn_threshold,
        ),
        ("max_comment_length", config.max_comment_length),
        (
            "max_security_label_length",
            config.max_security_label_length,
        ),
        ("default_keyspace_rf", config.default_keyspace_rf),
        (
            "max_concurrent_automatic_sstable_upgrades",
            config.max_concurrent_automatic_sstable_upgrades,
        ),
    ] {
        if value == 0 {
            errors.push(ConfigError {
                field: name.into(),
                message: format!("{name} must be > 0"),
            });
        }
    }

    if config.max_value_size.bytes() >= 2 * 1024 * 1024 * 1024 {
        errors.push(ConfigError {
            field: "max_value_size".into(),
            message: "max_value_size must be less than 2GiB".into(),
        });
    }

    match config.corrupted_tombstone_strategy.as_str() {
        "disabled" | "warn" | "exception" => {}
        other => {
            errors.push(ConfigError {
                field: "corrupted_tombstone_strategy".into(),
                message: format!("invalid corrupted_tombstone_strategy: '{}'", other),
            });
        }
    }

    for (name, level) in [
        (
            "ideal_consistency_level",
            config.ideal_consistency_level.as_deref(),
        ),
        (
            "auth_read_consistency_level",
            config.auth_read_consistency_level.as_deref(),
        ),
        (
            "auth_write_consistency_level",
            config.auth_write_consistency_level.as_deref(),
        ),
    ] {
        if let Some(level) = level {
            if !is_valid_consistency_level(level) {
                errors.push(ConfigError {
                    field: name.into(),
                    message: format!("invalid {name}: '{}'", level),
                });
            }
        }
    }

    if let Some(exclusions) = &config.client_error_reporting_exclusions {
        for (index, subnet) in exclusions.subnets.iter().enumerate() {
            if subnet.trim().is_empty() {
                errors.push(ConfigError {
                    field: format!("client_error_reporting_exclusions.subnets[{index}]"),
                    message: "client error reporting subnet must not be empty".into(),
                });
            }
        }
    }

    match config.internode_compression.as_str() {
        "all" | "dc" | "none" => {}
        other => {
            errors.push(ConfigError {
                field: "internode_compression".into(),
                message: format!("invalid internode_compression: '{}'", other),
            });
        }
    }

    match config.batchlog_endpoint_strategy.as_str() {
        "random_remote" | "prefer_local" | "dynamic_remote" | "dynamic" => {}
        other => {
            errors.push(ConfigError {
                field: "batchlog_endpoint_strategy".into(),
                message: format!("invalid batchlog_endpoint_strategy: '{}'", other),
            });
        }
    }

    if let Some(mode) = &config.disk_access_mode {
        match mode.as_str() {
            "auto" | "standard" | "mmap" | "mmap_index_only" => {}
            other => {
                errors.push(ConfigError {
                    field: "disk_access_mode".into(),
                    message: format!("invalid disk_access_mode: '{}'", other),
                });
            }
        }
    }

    if let Some(mode) = &config.commitlog_disk_access_mode {
        match mode.as_str() {
            "auto" | "legacy" | "mmap" | "direct" | "standard" => {}
            other => {
                errors.push(ConfigError {
                    field: "commitlog_disk_access_mode".into(),
                    message: format!("invalid commitlog_disk_access_mode: '{}'", other),
                });
            }
        }
    }

    match config.compaction_read_disk_access_mode.as_str() {
        "auto" | "direct" => {}
        other => {
            errors.push(ConfigError {
                field: "compaction_read_disk_access_mode".into(),
                message: format!("invalid compaction_read_disk_access_mode: '{}'", other),
            });
        }
    }

    match config.flush_compression.as_str() {
        "none" | "fast" | "table" => {}
        other => {
            errors.push(ConfigError {
                field: "flush_compression".into(),
                message: format!("invalid flush_compression: '{}'", other),
            });
        }
    }

    match config.triggers_policy.as_str() {
        "enabled" | "disabled" | "forbidden" => {}
        other => {
            errors.push(ConfigError {
                field: "triggers_policy".into(),
                message: format!("invalid triggers_policy: '{}'", other),
            });
        }
    }

    match config.storage_compatibility_mode.as_str() {
        "CASSANDRA_4" | "UPGRADING" | "NONE" => {}
        other => {
            errors.push(ConfigError {
                field: "storage_compatibility_mode".into(),
                message: format!("invalid storage_compatibility_mode: '{}'", other),
            });
        }
    }

    if let Some(providers) = &config.crypto_provider {
        validate_class_list("crypto_provider", providers, &mut errors);
    }

    if let Some(compressors) = &config.hints_compression {
        validate_class_list("hints_compression", compressors, &mut errors);
    }

    if let Some(compressors) = &config.commitlog_compression {
        validate_class_list("commitlog_compression", compressors, &mut errors);
    }

    if let Some(tde) = &config.transparent_data_encryption_options {
        validate_transparent_data_encryption_options(tde, &mut errors);
    }

    validate_jmx_server_options(config.jmx_server_options.as_ref(), &mut errors);
    validate_startup_checks(config.startup_checks.as_ref(), &mut errors);
    validate_accord_runtime(config.accord.as_ref(), &mut errors);
    validate_auto_repair(config.auto_repair.as_ref(), &mut errors);

    if let Some(threshold) = config.reject_repair_compaction_threshold {
        if threshold == 0 {
            errors.push(ConfigError {
                field: "reject_repair_compaction_threshold".into(),
                message: "reject_repair_compaction_threshold must be > 0 when set".into(),
            });
        }
    }

    for (name, ratio) in [
        (
            "repair_disk_headroom_reject_ratio",
            config.repair_disk_headroom_reject_ratio,
        ),
        (
            "incremental_repair_disk_headroom_reject_ratio",
            config.incremental_repair_disk_headroom_reject_ratio,
        ),
    ] {
        if let Some(ratio) = ratio {
            if !(0.0..=1.0).contains(&ratio) || !ratio.is_finite() {
                errors.push(ConfigError {
                    field: name.into(),
                    message: format!("{name} must be finite and between 0.0 and 1.0"),
                });
            }
        }
    }

    if config.internode_tcp_connect_timeout.millis() == 0 {
        errors.push(ConfigError {
            field: "internode_tcp_connect_timeout".into(),
            message: "internode_tcp_connect_timeout must be > 0".into(),
        });
    }

    for (name, size) in [
        (
            "internode_application_send_queue_capacity",
            config.internode_application_send_queue_capacity,
        ),
        (
            "internode_application_send_queue_reserve_endpoint_capacity",
            config.internode_application_send_queue_reserve_endpoint_capacity,
        ),
        (
            "internode_application_send_queue_reserve_global_capacity",
            config.internode_application_send_queue_reserve_global_capacity,
        ),
        (
            "internode_application_receive_queue_capacity",
            config.internode_application_receive_queue_capacity,
        ),
        (
            "internode_application_receive_queue_reserve_endpoint_capacity",
            config.internode_application_receive_queue_reserve_endpoint_capacity,
        ),
        (
            "internode_application_receive_queue_reserve_global_capacity",
            config.internode_application_receive_queue_reserve_global_capacity,
        ),
        ("streaming_state_size", config.streaming_state_size),
    ] {
        if size.bytes() == 0 {
            errors.push(ConfigError {
                field: name.into(),
                message: format!("{name} must be > 0"),
            });
        }
    }

    if config.streaming_state_expires.millis() == 0 {
        errors.push(ConfigError {
            field: "streaming_state_expires".into(),
            message: "streaming_state_expires must be > 0".into(),
        });
    }

    if config.streaming_connections_per_host == 0 {
        errors.push(ConfigError {
            field: "streaming_connections_per_host".into(),
            message: "streaming_connections_per_host must be > 0".into(),
        });
    }

    if config.native_transport_max_threads == 0 {
        errors.push(ConfigError {
            field: "native_transport_max_threads".into(),
            message: "native_transport_max_threads must be > 0".into(),
        });
    }

    if config.concurrent_materialized_view_builders == 0 {
        errors.push(ConfigError {
            field: "concurrent_materialized_view_builders".into(),
            message: "concurrent_materialized_view_builders must be > 0".into(),
        });
    }

    if let Some(compactors) = config.concurrent_compactors {
        if compactors == 0 {
            errors.push(ConfigError {
                field: "concurrent_compactors".into(),
                message: "concurrent_compactors must be > 0 when set".into(),
            });
        }
    }

    match config.memtable_allocation_type.as_str() {
        "unslabbed_heap_buffers"
        | "unslabbed_heap_buffers_logged"
        | "heap_buffers"
        | "offheap_buffers"
        | "offheap_objects" => {}
        other => {
            errors.push(ConfigError {
                field: "memtable_allocation_type".into(),
                message: format!("invalid memtable_allocation_type: '{}'", other),
            });
        }
    }

    if let Some(threshold) = config.memtable_cleanup_threshold {
        if !(0.01..=0.99).contains(&threshold) || !threshold.is_finite() {
            errors.push(ConfigError {
                field: "memtable_cleanup_threshold".into(),
                message: "memtable_cleanup_threshold must be finite and between 0.01 and 0.99"
                    .into(),
            });
        }
    }

    match config.disk_optimization_strategy.as_str() {
        "ssd" | "spinning" => {}
        other => {
            errors.push(ConfigError {
                field: "disk_optimization_strategy".into(),
                message: format!("invalid disk_optimization_strategy: '{}'", other),
            });
        }
    }

    if config.trickle_fsync_interval.bytes() == 0 {
        errors.push(ConfigError {
            field: "trickle_fsync_interval".into(),
            message: "trickle_fsync_interval must be > 0".into(),
        });
    }

    if config.cdc_free_space_check_interval.millis() == 0 {
        errors.push(ConfigError {
            field: "cdc_free_space_check_interval".into(),
            message: "cdc_free_space_check_interval must be > 0".into(),
        });
    }

    if let Some(interval) = config.index_summary_resize_interval {
        if interval.millis() == 0 {
            errors.push(ConfigError {
                field: "index_summary_resize_interval".into(),
                message: "index_summary_resize_interval must be > 0 when set".into(),
            });
        }
    }

    if let Some(ttl) = config.auto_snapshot_ttl {
        if ttl.millis() == 0 {
            errors.push(ConfigError {
                field: "auto_snapshot_ttl".into(),
                message: "auto_snapshot_ttl must be > 0 when set".into(),
            });
        }
    }

    if let Some(repair_session_space) = config.repair_session_space {
        if repair_session_space.mebibytes() < 1 {
            errors.push(ConfigError {
                field: "repair_session_space".into(),
                message: "repair_session_space must be at least 1MiB".into(),
            });
        }
    }

    if config.phi_convict_threshold <= 0.0 || !config.phi_convict_threshold.is_finite() {
        errors.push(ConfigError {
            field: "phi_convict_threshold".into(),
            message: "phi_convict_threshold must be a finite value > 0".into(),
        });
    }

    if config.dynamic_snitch_badness_threshold < 0.0
        || !config.dynamic_snitch_badness_threshold.is_finite()
    {
        errors.push(ConfigError {
            field: "dynamic_snitch_badness_threshold".into(),
            message: "dynamic_snitch_badness_threshold must be finite and >= 0".into(),
        });
    }

    if config.failure_detector.trim().is_empty() {
        errors.push(ConfigError {
            field: "failure_detector".into(),
            message: "failure_detector must not be empty".into(),
        });
    }

    if config.initial_location_provider.is_some() ^ config.node_proximity.is_some() {
        errors.push(ConfigError {
            field: "initial_location_provider".into(),
            message: "initial_location_provider and node_proximity must be specified together"
                .into(),
        });
    }

    if config.addresses_config.is_some()
        && (config.initial_location_provider.is_none() || config.node_proximity.is_none())
    {
        errors.push(ConfigError {
            field: "addresses_config".into(),
            message: "addresses_config requires initial_location_provider and node_proximity"
                .into(),
        });
    }

    if config.denylist_refresh.millis() == 0 {
        errors.push(ConfigError {
            field: "denylist_refresh".into(),
            message: "denylist_refresh must be > 0".into(),
        });
    }

    if config.denylist_initial_load_retry.millis() == 0 {
        errors.push(ConfigError {
            field: "denylist_initial_load_retry".into(),
            message: "denylist_initial_load_retry must be > 0".into(),
        });
    }

    if config.denylist_max_keys_per_table == 0 {
        errors.push(ConfigError {
            field: "denylist_max_keys_per_table".into(),
            message: "denylist_max_keys_per_table must be > 0".into(),
        });
    }

    if config.denylist_max_keys_total == 0 {
        errors.push(ConfigError {
            field: "denylist_max_keys_total".into(),
            message: "denylist_max_keys_total must be > 0".into(),
        });
    }

    match config.denylist_consistency_level.as_str() {
        "ANY" | "ONE" | "TWO" | "THREE" | "QUORUM" | "ALL" | "LOCAL_QUORUM" | "EACH_QUORUM"
        | "SERIAL" | "LOCAL_SERIAL" | "LOCAL_ONE" => {}
        other => {
            errors.push(ConfigError {
                field: "denylist_consistency_level".into(),
                message: format!("invalid denylist_consistency_level: '{}'", other),
            });
        }
    }

    validate_encryption_options(
        "client_encryption_options",
        config.client_encryption_options.as_ref(),
        &mut errors,
    );
    validate_encryption_options(
        "server_encryption_options",
        config.server_encryption_options.as_ref(),
        &mut errors,
    );

    errors
}

fn is_wildcard_address(value: Option<&str>) -> bool {
    matches!(value.map(str::trim), Some("0.0.0.0") | Some("::"))
}

fn is_valid_consistency_level(value: &str) -> bool {
    matches!(
        value,
        "ANY"
            | "ONE"
            | "TWO"
            | "THREE"
            | "QUORUM"
            | "ALL"
            | "LOCAL_QUORUM"
            | "EACH_QUORUM"
            | "SERIAL"
            | "LOCAL_SERIAL"
            | "LOCAL_ONE"
    )
}

fn validate_encryption_options(
    field: &str,
    options: Option<&crate::config::EncryptionOptions>,
    errors: &mut Vec<ConfigError>,
) {
    let Some(options) = options else {
        return;
    };

    if let Some(factory) = &options.ssl_context_factory {
        if factory.class_name.trim().is_empty() {
            errors.push(ConfigError {
                field: format!("{field}.ssl_context_factory.class_name"),
                message: "ssl_context_factory.class_name must not be empty".into(),
            });
        }
    }

    match options.internode_encryption.as_str() {
        "none" | "dc" | "rack" | "all" => {}
        other => {
            errors.push(ConfigError {
                field: format!("{field}.internode_encryption"),
                message: format!("invalid internode_encryption: '{}'", other),
            });
        }
    }

    if let Some(validity) = options.max_certificate_validity_period {
        if validity.millis() == 0 {
            errors.push(ConfigError {
                field: format!("{field}.max_certificate_validity_period"),
                message: "max_certificate_validity_period must be > 0 when set".into(),
            });
        }
    }

    if let Some(warn) = options.certificate_validity_warn_threshold {
        if warn.millis() == 0 {
            errors.push(ConfigError {
                field: format!("{field}.certificate_validity_warn_threshold"),
                message: "certificate_validity_warn_threshold must be > 0 when set".into(),
            });
        }
        if let Some(validity) = options.max_certificate_validity_period {
            if warn > validity {
                errors.push(ConfigError {
                    field: format!("{field}.certificate_validity_warn_threshold"),
                    message:
                        "certificate_validity_warn_threshold must not exceed max_certificate_validity_period"
                            .into(),
                });
            }
        }
    }
}

fn validate_optional_size_threshold_pair(
    warn_name: &str,
    warn: Option<crate::units::DataSize>,
    fail_name: &str,
    fail: Option<crate::units::DataSize>,
    errors: &mut Vec<ConfigError>,
) {
    if let (Some(warn), Some(fail)) = (warn, fail) {
        if fail < warn {
            errors.push(ConfigError {
                field: fail_name.into(),
                message: format!("{fail_name} must be >= {warn_name}"),
            });
        }
    }
}

fn validate_class_list(
    field: &str,
    classes: &[crate::config::ClassWithListParameters],
    errors: &mut Vec<ConfigError>,
) {
    if classes.is_empty() {
        errors.push(ConfigError {
            field: field.into(),
            message: format!("{field} must not be empty when set"),
        });
    }

    for (index, class) in classes.iter().enumerate() {
        if class.class_name.trim().is_empty() {
            errors.push(ConfigError {
                field: format!("{field}[{index}].class_name"),
                message: "class_name must not be empty".into(),
            });
        }
    }
}

fn validate_transparent_data_encryption_options(
    options: &crate::config::TransparentDataEncryptionConfig,
    errors: &mut Vec<ConfigError>,
) {
    if options.chunk_length_kb == 0 {
        errors.push(ConfigError {
            field: "transparent_data_encryption_options.chunk_length_kb".into(),
            message: "chunk_length_kb must be > 0".into(),
        });
    }

    if options.enabled || !options.key_provider.is_empty() {
        validate_class_list(
            "transparent_data_encryption_options.key_provider",
            &options.key_provider,
            errors,
        );
    }
}

fn validate_jmx_server_options(
    options: Option<&crate::config::JmxServerOptionsConfig>,
    errors: &mut Vec<ConfigError>,
) {
    let Some(options) = options else {
        return;
    };

    if options.jmx_port == 0 {
        errors.push(ConfigError {
            field: "jmx_server_options.jmx_port".into(),
            message: "jmx_port must be > 0".into(),
        });
    }

    for (field, value) in [
        ("password_file", options.password_file.as_deref()),
        ("access_file", options.access_file.as_deref()),
        ("login_config_name", options.login_config_name.as_deref()),
        ("login_config_file", options.login_config_file.as_deref()),
        ("authorizer", options.authorizer.as_deref()),
    ] {
        if matches!(value, Some(v) if v.trim().is_empty()) {
            errors.push(ConfigError {
                field: format!("jmx_server_options.{field}"),
                message: format!("{field} must not be empty when set"),
            });
        }
    }

    validate_encryption_options(
        "jmx_server_options.jmx_encryption_options",
        options.jmx_encryption_options.as_ref(),
        errors,
    );
}

fn validate_startup_checks(
    checks: Option<&crate::config::StartupChecksConfig>,
    errors: &mut Vec<ConfigError>,
) {
    let Some(checks) = checks else {
        return;
    };

    if let Some(ownership) = &checks.check_filesystem_ownership {
        if ownership.enabled {
            if matches!(ownership.ownership_token.as_deref(), Some(token) if token.trim().is_empty())
            {
                errors.push(ConfigError {
                    field: "startup_checks.check_filesystem_ownership.ownership_token".into(),
                    message: "ownership_token must not be empty when set".into(),
                });
            }
            if ownership.ownership_filename.trim().is_empty() {
                errors.push(ConfigError {
                    field: "startup_checks.check_filesystem_ownership.ownership_filename".into(),
                    message: "ownership_filename must not be empty".into(),
                });
            }
        }
    }

    if let Some(resurrection) = &checks.check_data_resurrection {
        if resurrection.enabled
            && matches!(resurrection.heartbeat_file.as_deref(), Some(path) if path.trim().is_empty())
        {
            errors.push(ConfigError {
                field: "startup_checks.check_data_resurrection.heartbeat_file".into(),
                message: "heartbeat_file must not be empty when set".into(),
            });
        }
    }
}

fn validate_accord_runtime(
    accord: Option<&crate::config::AccordRuntimeConfig>,
    errors: &mut Vec<ConfigError>,
) {
    let Some(accord) = accord else {
        return;
    };

    for (field, count) in [
        ("queue_shard_count", accord.queue_shard_count),
        (
            "command_store_shard_count",
            accord.command_store_shard_count,
        ),
    ] {
        if count < -1 || count == 0 {
            errors.push(ConfigError {
                field: format!("accord.{field}"),
                message: format!("{field} must be -1 or a positive shard count"),
            });
        }
    }

    for (field, duration) in [
        ("recover_delay", accord.recover_delay),
        ("fast_path_update_delay", accord.fast_path_update_delay),
    ] {
        if duration.millis() == 0 {
            errors.push(ConfigError {
                field: format!("accord.{field}"),
                message: format!("{field} must be > 0"),
            });
        }
    }
}

fn validate_auto_repair(
    auto_repair: Option<&crate::config::AutoRepairConfig>,
    errors: &mut Vec<ConfigError>,
) {
    let Some(auto_repair) = auto_repair else {
        return;
    };

    for (repair_type, config) in &auto_repair.repair_type_overrides {
        let field_prefix = format!("auto_repair.repair_type_overrides.{repair_type}");
        if repair_type.trim().is_empty() {
            errors.push(ConfigError {
                field: "auto_repair.repair_type_overrides".into(),
                message: "repair type name must not be empty".into(),
            });
        }
        if let Some(interval) = config.min_repair_interval {
            if interval.millis() == 0 {
                errors.push(ConfigError {
                    field: format!("{field_prefix}.min_repair_interval"),
                    message: "min_repair_interval must be > 0 when set".into(),
                });
            }
        }
        if let Some(splitter) = &config.token_range_splitter {
            if splitter.class_name.trim().is_empty() {
                errors.push(ConfigError {
                    field: format!("{field_prefix}.token_range_splitter.class_name"),
                    message: "class_name must not be empty".into(),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn default_config_valid() {
        let cfg = CassandraConfig::default();
        let errors = validate(&cfg);
        assert!(errors.is_empty(), "default config has errors: {:?}", errors);
    }

    #[test]
    fn empty_cluster_name() {
        let yaml = r#"cluster_name: """#;
        let cfg: CassandraConfig = serde_yaml::from_str(yaml).unwrap();
        let errors = validate(&cfg);
        assert!(errors.iter().any(|e| e.field == "cluster_name"));
    }

    #[test]
    fn invalid_commitlog_sync() {
        let yaml = r#"commitlog_sync: "invalid""#;
        let cfg: CassandraConfig = serde_yaml::from_str(yaml).unwrap();
        let errors = validate(&cfg);
        assert!(errors.iter().any(|e| e.field == "commitlog_sync"));
    }

    #[test]
    fn zero_concurrent_reads() {
        let yaml = r#"concurrent_reads: 0"#;
        let cfg: CassandraConfig = serde_yaml::from_str(yaml).unwrap();
        let errors = validate(&cfg);
        assert!(errors.iter().any(|e| e.field == "concurrent_reads"));
    }

    #[test]
    fn invalid_timeout_and_snitch_fields() {
        let yaml = r#"
endpoint_snitch: ""
read_request_timeout: 0
streaming_connections_per_host: 0
phi_convict_threshold: 0.0
dynamic_snitch_badness_threshold: -1.0
"#;
        let cfg: CassandraConfig = serde_yaml::from_str(yaml).unwrap();
        let errors = validate(&cfg);
        for field in [
            "endpoint_snitch",
            "read_request_timeout",
            "streaming_connections_per_host",
            "phi_convict_threshold",
            "dynamic_snitch_badness_threshold",
        ] {
            assert!(
                errors.iter().any(|e| e.field == field),
                "missing validation error for {field}: {errors:?}"
            );
        }
    }

    #[test]
    fn invalid_upstream_runtime_fields() {
        let yaml = r#"
commit_failure_policy: "explode"
commitlog_total_space: 0
native_transport_max_threads: 0
concurrent_materialized_view_builders: 0
concurrent_compactors: 0
memtable_allocation_type: invalid
memtable_cleanup_threshold: 1.5
disk_optimization_strategy: tape
trickle_fsync_interval: 0
cdc_free_space_check_interval: 0
index_summary_resize_interval: 0
repair_session_space: "512KiB"
column_index_cache_size: 0
sstable_preemptive_open_interval: 0
auto_snapshot_ttl: 0
repair_request_timeout: 0
internode_tcp_connect_timeout: 0
internode_application_send_queue_capacity: 0
internode_application_send_queue_reserve_endpoint_capacity: 0
internode_application_send_queue_reserve_global_capacity: 0
internode_application_receive_queue_capacity: 0
internode_application_receive_queue_reserve_endpoint_capacity: 0
internode_application_receive_queue_reserve_global_capacity: 0
streaming_state_expires: 0
streaming_state_size: 0
failure_detector: ""
initial_location_provider: RackDCFileLocationProvider
addresses_config: Ec2MultiRegionAddressConfig
denylist_refresh: 0
denylist_initial_load_retry: 0
denylist_max_keys_per_table: 0
denylist_max_keys_total: 0
denylist_consistency_level: SOMETIMES
hints_flush_period: 0
max_hints_file_size: 0
batchlog_replay_throttle: 0
batchlog_endpoint_strategy: nearest
disk_access_mode: tape
commitlog_disk_access_mode: buffered
compaction_read_disk_access_mode: mmap
flush_compression: slow
commitlog_sync_group_window: 0
periodic_commitlog_sync_lag_block: 0
internode_compression: sometimes
trace_type_query_ttl: 0
trace_type_repair_ttl: 0
triggers_policy: maybe
sstables_per_read_log_threshold: 0
tombstone_warn_threshold: 100
tombstone_failure_threshold: 10
replica_filtering_protection:
  cached_rows_warn_threshold: 20
  cached_rows_fail_threshold: 10
batch_size_warn_threshold: 10KiB
batch_size_fail_threshold: 1KiB
coordinator_read_size_warn_threshold: 2MiB
coordinator_read_size_fail_threshold: 1MiB
local_read_size_warn_threshold: 2MiB
local_read_size_fail_threshold: 1MiB
row_index_read_size_warn_threshold: 2MiB
row_index_read_size_fail_threshold: 1MiB
networking_cache_size: 0
corrupted_tombstone_strategy: explode
max_value_size: 2GiB
default_keyspace_rf: 0
ideal_consistency_level: SOMETIMES
max_concurrent_automatic_sstable_upgrades: 0
auth_read_consistency_level: USUALLY
auth_write_consistency_level: NEVER
client_error_reporting_exclusions:
  subnets: [""]
storage_compatibility_mode: LEGACY
compression_dictionary_refresh_interval: 0
compression_dictionary_refresh_initial_delay: 0
compression_dictionary_cache_size: 0
compression_dictionary_cache_expire: 0
crypto_provider:
  - class_name: ""
hints_compression:
  - class_name: ""
commitlog_compression:
  - class_name: ""
transparent_data_encryption_options:
  enabled: true
  chunk_length_kb: 0
  key_provider:
    - class_name: ""
jmx_server_options:
  jmx_port: 0
  password_file: ""
  jmx_encryption_options:
    ssl_context_factory:
      class_name: ""
    internode_encryption: invalid
startup_checks:
  check_filesystem_ownership:
    enabled: true
    ownership_token: ""
    ownership_filename: ""
  check_data_resurrection:
    enabled: true
    heartbeat_file: ""
accord:
  queue_shard_count: 0
  command_store_shard_count: -2
  recover_delay: 0
  fast_path_update_delay: 0
reject_repair_compaction_threshold: 0
repair_disk_headroom_reject_ratio: 1.5
incremental_repair_disk_headroom_reject_ratio: -0.1
auto_repair:
  repair_type_overrides:
    full:
      min_repair_interval: 0
      token_range_splitter:
        class_name: ""
client_encryption_options:
  ssl_context_factory:
    class_name: ""
  internode_encryption: maybe
  max_certificate_validity_period: 0
  certificate_validity_warn_threshold: 1d
server_encryption_options:
  internode_encryption: invalid
  max_certificate_validity_period: 10d
  certificate_validity_warn_threshold: 11d
"#;
        let cfg: CassandraConfig = serde_yaml::from_str(yaml).unwrap();
        let errors = validate(&cfg);
        for field in [
            "commit_failure_policy",
            "commitlog_total_space",
            "native_transport_max_threads",
            "concurrent_materialized_view_builders",
            "concurrent_compactors",
            "memtable_allocation_type",
            "memtable_cleanup_threshold",
            "disk_optimization_strategy",
            "trickle_fsync_interval",
            "cdc_free_space_check_interval",
            "index_summary_resize_interval",
            "repair_session_space",
            "column_index_cache_size",
            "sstable_preemptive_open_interval",
            "auto_snapshot_ttl",
            "repair_request_timeout",
            "internode_tcp_connect_timeout",
            "internode_application_send_queue_capacity",
            "internode_application_send_queue_reserve_endpoint_capacity",
            "internode_application_send_queue_reserve_global_capacity",
            "internode_application_receive_queue_capacity",
            "internode_application_receive_queue_reserve_endpoint_capacity",
            "internode_application_receive_queue_reserve_global_capacity",
            "streaming_state_expires",
            "streaming_state_size",
            "failure_detector",
            "initial_location_provider",
            "addresses_config",
            "denylist_refresh",
            "denylist_initial_load_retry",
            "denylist_max_keys_per_table",
            "denylist_max_keys_total",
            "denylist_consistency_level",
            "hints_flush_period",
            "max_hints_file_size",
            "batchlog_replay_throttle",
            "batchlog_endpoint_strategy",
            "disk_access_mode",
            "commitlog_disk_access_mode",
            "compaction_read_disk_access_mode",
            "flush_compression",
            "commitlog_sync_group_window",
            "periodic_commitlog_sync_lag_block",
            "internode_compression",
            "trace_type_query_ttl",
            "trace_type_repair_ttl",
            "triggers_policy",
            "sstables_per_read_log_threshold",
            "tombstone_failure_threshold",
            "replica_filtering_protection.cached_rows_fail_threshold",
            "batch_size_fail_threshold",
            "coordinator_read_size_fail_threshold",
            "local_read_size_fail_threshold",
            "row_index_read_size_fail_threshold",
            "networking_cache_size",
            "corrupted_tombstone_strategy",
            "max_value_size",
            "default_keyspace_rf",
            "ideal_consistency_level",
            "max_concurrent_automatic_sstable_upgrades",
            "auth_read_consistency_level",
            "auth_write_consistency_level",
            "client_error_reporting_exclusions.subnets[0]",
            "storage_compatibility_mode",
            "compression_dictionary_refresh_interval",
            "compression_dictionary_refresh_initial_delay",
            "compression_dictionary_cache_size",
            "compression_dictionary_cache_expire",
            "crypto_provider[0].class_name",
            "hints_compression[0].class_name",
            "commitlog_compression[0].class_name",
            "transparent_data_encryption_options.chunk_length_kb",
            "transparent_data_encryption_options.key_provider[0].class_name",
            "jmx_server_options.jmx_port",
            "jmx_server_options.password_file",
            "jmx_server_options.jmx_encryption_options.ssl_context_factory.class_name",
            "jmx_server_options.jmx_encryption_options.internode_encryption",
            "startup_checks.check_filesystem_ownership.ownership_token",
            "startup_checks.check_filesystem_ownership.ownership_filename",
            "startup_checks.check_data_resurrection.heartbeat_file",
            "accord.queue_shard_count",
            "accord.command_store_shard_count",
            "accord.recover_delay",
            "accord.fast_path_update_delay",
            "reject_repair_compaction_threshold",
            "repair_disk_headroom_reject_ratio",
            "incremental_repair_disk_headroom_reject_ratio",
            "auto_repair.repair_type_overrides.full.min_repair_interval",
            "auto_repair.repair_type_overrides.full.token_range_splitter.class_name",
            "client_encryption_options.ssl_context_factory.class_name",
            "client_encryption_options.internode_encryption",
            "client_encryption_options.max_certificate_validity_period",
            "client_encryption_options.certificate_validity_warn_threshold",
            "server_encryption_options.internode_encryption",
            "server_encryption_options.certificate_validity_warn_threshold",
        ] {
            assert!(
                errors.iter().any(|e| e.field == field),
                "missing validation error for {field}: {errors:?}"
            );
        }
    }

    #[test]
    fn network_cross_field_validation() {
        let yaml = r#"
listen_address: "0.0.0.0"
listen_interface: eth0
broadcast_address: "0.0.0.0"
listen_on_broadcast_address: true
rpc_address: "0.0.0.0"
rpc_interface: eth1
broadcast_rpc_address: "0.0.0.0"
"#;
        let cfg: CassandraConfig = serde_yaml::from_str(yaml).unwrap();
        let errors = validate(&cfg);
        for field in [
            "listen_address",
            "broadcast_address",
            "rpc_address",
            "broadcast_rpc_address",
        ] {
            assert!(
                errors.iter().any(|e| e.field == field),
                "missing validation error for {field}: {errors:?}"
            );
        }
    }

    #[test]
    fn rpc_wildcard_requires_broadcast_rpc_address() {
        let yaml = r#"rpc_address: "0.0.0.0""#;
        let cfg: CassandraConfig = serde_yaml::from_str(yaml).unwrap();
        let errors = validate(&cfg);
        assert!(errors.iter().any(|e| e.field == "broadcast_rpc_address"));

        let yaml = r#"
rpc_address: "0.0.0.0"
broadcast_rpc_address: "10.0.0.12"
"#;
        let cfg: CassandraConfig = serde_yaml::from_str(yaml).unwrap();
        let errors = validate(&cfg);
        assert!(
            !errors.iter().any(|e| e.field == "broadcast_rpc_address"),
            "unexpected broadcast_rpc_address error: {errors:?}"
        );
    }

    #[test]
    fn zero_native_transport_auth_threads_is_valid() {
        let yaml = r#"native_transport_max_auth_threads: 0"#;
        let cfg: CassandraConfig = serde_yaml::from_str(yaml).unwrap();
        let errors = validate(&cfg);
        assert!(
            !errors
                .iter()
                .any(|e| e.field == "native_transport_max_auth_threads"),
            "auth thread zero should use main executor: {errors:?}"
        );
    }
}
