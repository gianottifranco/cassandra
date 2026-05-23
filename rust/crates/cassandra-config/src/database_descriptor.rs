// Licensed under Apache License, Version 2.0.

#![allow(clippy::field_reassign_with_default)]

//! Runtime configuration singleton.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.config.DatabaseDescriptor`
//!
//! Thread-safe holder for the active `CassandraConfig` and `GuardrailsConfig`,
//! providing typed accessor methods and consistent snapshot reads.

use crate::config::CassandraConfig;
use crate::guardrails::GuardrailsConfig;
use crate::units::{DataRateSpec, DataSize, Duration};
use parking_lot::RwLock;
use std::sync::Arc;

/// Thread-safe runtime configuration holder.
///
/// Wraps the active config behind `Arc<RwLock<_>>` so subsystems can share
/// a single source of truth that supports hot-reload via atomic swap.
#[derive(Clone)]
pub struct DatabaseDescriptor {
    config: Arc<RwLock<CassandraConfig>>,
    guardrails: Arc<RwLock<GuardrailsConfig>>,
}

/// An immutable snapshot of both configs for consistent reads.
pub struct ConfigSnapshot {
    pub config: CassandraConfig,
    pub guardrails: GuardrailsConfig,
}

impl DatabaseDescriptor {
    /// Create a new runtime descriptor from loaded configs.
    pub fn new(config: CassandraConfig, guardrails: GuardrailsConfig) -> Self {
        Self {
            config: Arc::new(RwLock::new(config)),
            guardrails: Arc::new(RwLock::new(guardrails)),
        }
    }

    /// Take a consistent snapshot of both configs.
    pub fn snapshot(&self) -> ConfigSnapshot {
        let config = self.config.read().clone();
        let guardrails = self.guardrails.read().clone();
        ConfigSnapshot { config, guardrails }
    }

    /// Replace the active config (used by hot-reload).
    pub fn swap_config(&self, new_config: CassandraConfig) {
        *self.config.write() = new_config;
    }

    /// Replace the active guardrails config (used by hot-reload).
    pub fn swap_guardrails(&self, new_guardrails: GuardrailsConfig) {
        *self.guardrails.write() = new_guardrails;
    }

    /// Get a shared reference to the inner config lock (for subsystems).
    pub fn config_arc(&self) -> &Arc<RwLock<CassandraConfig>> {
        &self.config
    }

    /// Get a shared reference to the inner guardrails lock.
    pub fn guardrails_arc(&self) -> &Arc<RwLock<GuardrailsConfig>> {
        &self.guardrails
    }

    // ── Cluster identity ─────────────────────────────────────────────────

    pub fn cluster_name(&self) -> String {
        self.config.read().cluster_name.clone()
    }

    pub fn num_tokens(&self) -> u32 {
        self.config.read().num_tokens
    }

    pub fn partitioner(&self) -> String {
        self.config.read().partitioner.clone()
    }

    pub fn auto_bootstrap(&self) -> bool {
        self.config.read().auto_bootstrap
    }

    pub fn initial_token(&self) -> Option<String> {
        self.config.read().initial_token.clone()
    }

    pub fn endpoint_snitch(&self) -> String {
        self.config.read().endpoint_snitch.clone()
    }

    // ── Networking ───────────────────────────────────────────────────────

    pub fn listen_address(&self) -> Option<String> {
        self.config.read().listen_address.clone()
    }

    pub fn listen_interface(&self) -> Option<String> {
        self.config.read().listen_interface.clone()
    }

    pub fn listen_interface_prefer_ipv6(&self) -> bool {
        self.config.read().listen_interface_prefer_ipv6
    }

    pub fn broadcast_address(&self) -> Option<String> {
        self.config.read().broadcast_address.clone()
    }

    pub fn listen_on_broadcast_address(&self) -> bool {
        self.config.read().listen_on_broadcast_address
    }

    pub fn rpc_address(&self) -> Option<String> {
        self.config.read().rpc_address.clone()
    }

    pub fn rpc_interface(&self) -> Option<String> {
        self.config.read().rpc_interface.clone()
    }

    pub fn rpc_interface_prefer_ipv6(&self) -> bool {
        self.config.read().rpc_interface_prefer_ipv6
    }

    pub fn broadcast_rpc_address(&self) -> Option<String> {
        self.config.read().broadcast_rpc_address.clone()
    }

    pub fn rpc_keepalive(&self) -> bool {
        self.config.read().rpc_keepalive
    }

    pub fn native_transport_port(&self) -> u16 {
        self.config.read().native_transport_port
    }

    pub fn storage_port(&self) -> u16 {
        self.config.read().storage_port
    }

    pub fn ssl_storage_port(&self) -> u16 {
        self.config.read().ssl_storage_port
    }

    // ── Native transport limits ──────────────────────────────────────────

    pub fn native_transport_max_concurrent_connections(&self) -> i32 {
        self.config
            .read()
            .native_transport_max_concurrent_connections
    }

    pub fn native_transport_max_frame_size(&self) -> u64 {
        self.config.read().native_transport_max_frame_size
    }

    pub fn native_transport_max_request_data_in_flight(&self) -> u64 {
        self.config
            .read()
            .native_transport_max_request_data_in_flight
    }

    pub fn start_native_transport(&self) -> bool {
        self.config.read().start_native_transport
    }

    pub fn native_transport_max_threads(&self) -> u32 {
        self.config.read().native_transport_max_threads
    }

    pub fn native_transport_max_auth_threads(&self) -> u32 {
        self.config.read().native_transport_max_auth_threads
    }

    pub fn native_transport_idle_timeout(&self) -> Duration {
        self.config.read().native_transport_idle_timeout
    }

    pub fn native_transport_allow_older_protocols(&self) -> bool {
        self.config.read().native_transport_allow_older_protocols
    }

    pub fn native_transport_flush_in_batches_legacy(&self) -> bool {
        self.config.read().native_transport_flush_in_batches_legacy
    }

    // ── Request timeouts ─────────────────────────────────────────────────

    pub fn request_timeout(&self) -> Duration {
        self.config.read().request_timeout
    }

    pub fn read_request_timeout(&self) -> Duration {
        self.config.read().read_request_timeout
    }

    pub fn range_request_timeout(&self) -> Duration {
        self.config.read().range_request_timeout
    }

    pub fn write_request_timeout(&self) -> Duration {
        self.config.read().write_request_timeout
    }

    pub fn counter_write_request_timeout(&self) -> Duration {
        self.config.read().counter_write_request_timeout
    }

    pub fn cas_contention_timeout(&self) -> Duration {
        self.config.read().cas_contention_timeout
    }

    pub fn truncate_request_timeout(&self) -> Duration {
        self.config.read().truncate_request_timeout
    }

    pub fn repair_request_timeout(&self) -> Duration {
        self.config.read().repair_request_timeout
    }

    pub fn slow_query_log_timeout(&self) -> Duration {
        self.config.read().slow_query_log_timeout
    }

    pub fn internode_timeout(&self) -> bool {
        self.config.read().internode_timeout
    }

    // ── Thread pools ─────────────────────────────────────────────────────

    pub fn concurrent_reads(&self) -> u32 {
        self.config.read().concurrent_reads
    }

    pub fn concurrent_writes(&self) -> u32 {
        self.config.read().concurrent_writes
    }

    pub fn concurrent_counter_writes(&self) -> u32 {
        self.config.read().concurrent_counter_writes
    }

    pub fn concurrent_materialized_view_builders(&self) -> u32 {
        self.config.read().concurrent_materialized_view_builders
    }

    // ── Storage directories ──────────────────────────────────────────────

    pub fn data_file_directories(&self) -> Option<Vec<String>> {
        self.config.read().data_file_directories.clone()
    }

    pub fn local_system_data_file_directory(&self) -> Option<String> {
        self.config.read().local_system_data_file_directory.clone()
    }

    pub fn commitlog_directory(&self) -> Option<String> {
        self.config.read().commitlog_directory.clone()
    }

    // ── Commitlog ────────────────────────────────────────────────────────

    pub fn commitlog_sync(&self) -> String {
        self.config.read().commitlog_sync.clone()
    }

    pub fn commitlog_sync_period(&self) -> Option<Duration> {
        self.config.read().commitlog_sync_period
    }

    pub fn commitlog_segment_size(&self) -> Option<DataSize> {
        self.config.read().commitlog_segment_size
    }

    pub fn commitlog_total_space(&self) -> Option<DataSize> {
        self.config.read().commitlog_total_space
    }

    pub fn commitlog_disk_access_mode(&self) -> Option<String> {
        self.config.read().commitlog_disk_access_mode.clone()
    }

    pub fn disk_access_mode(&self) -> Option<String> {
        self.config.read().disk_access_mode.clone()
    }

    pub fn commitlog_compression(&self) -> Option<Vec<crate::config::ClassWithListParameters>> {
        self.config.read().commitlog_compression.clone()
    }

    pub fn compaction_read_disk_access_mode(&self) -> String {
        self.config.read().compaction_read_disk_access_mode.clone()
    }

    pub fn flush_compression(&self) -> String {
        self.config.read().flush_compression.clone()
    }

    pub fn commitlog_sync_group_window(&self) -> Option<Duration> {
        self.config.read().commitlog_sync_group_window
    }

    pub fn periodic_commitlog_sync_lag_block(&self) -> Option<Duration> {
        self.config.read().periodic_commitlog_sync_lag_block
    }

    // ── Memtable / file cache ───────────────────────────────────────────

    pub fn memtable_heap_space(&self) -> Option<DataSize> {
        self.config.read().memtable_heap_space
    }

    pub fn memtable_offheap_space(&self) -> Option<DataSize> {
        self.config.read().memtable_offheap_space
    }

    pub fn memtable_cleanup_threshold(&self) -> Option<f64> {
        self.config.read().memtable_cleanup_threshold
    }

    pub fn memtable_allocation_type(&self) -> String {
        self.config.read().memtable_allocation_type.clone()
    }

    pub fn memtable_flush_writers(&self) -> u32 {
        self.config.read().memtable_flush_writers
    }

    pub fn file_cache_size(&self) -> Option<DataSize> {
        self.config.read().file_cache_size
    }

    pub fn networking_cache_size(&self) -> Option<DataSize> {
        self.config.read().networking_cache_size
    }

    pub fn file_cache_enabled(&self) -> bool {
        self.config.read().file_cache_enabled
    }

    pub fn buffer_pool_use_heap_if_exhausted(&self) -> bool {
        self.config.read().buffer_pool_use_heap_if_exhausted
    }

    pub fn disk_optimization_strategy(&self) -> String {
        self.config.read().disk_optimization_strategy.clone()
    }

    // ── Streaming / compaction throughput ───────────────────────────────

    pub fn streaming_connections_per_host(&self) -> u32 {
        self.config.read().streaming_connections_per_host
    }

    pub fn stream_throughput_outbound(&self) -> DataRateSpec {
        self.config.read().stream_throughput_outbound
    }

    pub fn inter_dc_stream_throughput_outbound(&self) -> DataRateSpec {
        self.config.read().inter_dc_stream_throughput_outbound
    }

    pub fn entire_sstable_stream_throughput_outbound(&self) -> DataRateSpec {
        self.config.read().entire_sstable_stream_throughput_outbound
    }

    pub fn entire_sstable_inter_dc_stream_throughput_outbound(&self) -> DataRateSpec {
        self.config
            .read()
            .entire_sstable_inter_dc_stream_throughput_outbound
    }

    pub fn stream_entire_sstables(&self) -> bool {
        self.config.read().stream_entire_sstables
    }

    pub fn compaction_throughput(&self) -> DataRateSpec {
        self.config.read().compaction_throughput
    }

    pub fn concurrent_compactors(&self) -> Option<u32> {
        self.config.read().concurrent_compactors
    }

    pub fn concurrent_validations(&self) -> Option<u32> {
        self.config.read().concurrent_validations
    }

    pub fn repair_session_space(&self) -> Option<DataSize> {
        self.config.read().repair_session_space
    }

    pub fn concurrent_merkle_tree_requests(&self) -> u32 {
        self.config.read().concurrent_merkle_tree_requests
    }

    pub fn trickle_fsync(&self) -> bool {
        self.config.read().trickle_fsync
    }

    pub fn trickle_fsync_interval(&self) -> DataSize {
        self.config.read().trickle_fsync_interval
    }

    // ── Internode messaging / streaming state ───────────────────────────

    pub fn internode_socket_send_buffer_size(&self) -> DataSize {
        self.config.read().internode_socket_send_buffer_size
    }

    pub fn internode_socket_receive_buffer_size(&self) -> DataSize {
        self.config.read().internode_socket_receive_buffer_size
    }

    pub fn internode_application_send_queue_capacity(&self) -> DataSize {
        self.config.read().internode_application_send_queue_capacity
    }

    pub fn internode_application_send_queue_reserve_endpoint_capacity(&self) -> DataSize {
        self.config
            .read()
            .internode_application_send_queue_reserve_endpoint_capacity
    }

    pub fn internode_application_send_queue_reserve_global_capacity(&self) -> DataSize {
        self.config
            .read()
            .internode_application_send_queue_reserve_global_capacity
    }

    pub fn internode_application_receive_queue_capacity(&self) -> DataSize {
        self.config
            .read()
            .internode_application_receive_queue_capacity
    }

    pub fn internode_application_receive_queue_reserve_endpoint_capacity(&self) -> DataSize {
        self.config
            .read()
            .internode_application_receive_queue_reserve_endpoint_capacity
    }

    pub fn internode_application_receive_queue_reserve_global_capacity(&self) -> DataSize {
        self.config
            .read()
            .internode_application_receive_queue_reserve_global_capacity
    }

    pub fn internode_tcp_connect_timeout(&self) -> Duration {
        self.config.read().internode_tcp_connect_timeout
    }

    pub fn internode_tcp_user_timeout(&self) -> Duration {
        self.config.read().internode_tcp_user_timeout
    }

    pub fn internode_streaming_tcp_user_timeout(&self) -> Duration {
        self.config.read().internode_streaming_tcp_user_timeout
    }

    pub fn streaming_keep_alive_period(&self) -> Duration {
        self.config.read().streaming_keep_alive_period
    }

    pub fn streaming_state_expires(&self) -> Duration {
        self.config.read().streaming_state_expires
    }

    pub fn streaming_state_size(&self) -> DataSize {
        self.config.read().streaming_state_size
    }

    pub fn streaming_stats_enabled(&self) -> bool {
        self.config.read().streaming_stats_enabled
    }

    // ── SSTable / snapshot storage ─────────────────────────────────────

    pub fn incremental_backups(&self) -> bool {
        self.config.read().incremental_backups
    }

    pub fn snapshot_before_compaction(&self) -> bool {
        self.config.read().snapshot_before_compaction
    }

    pub fn auto_snapshot(&self) -> bool {
        self.config.read().auto_snapshot
    }

    pub fn auto_snapshot_ttl(&self) -> Option<Duration> {
        self.config.read().auto_snapshot_ttl
    }

    pub fn snapshot_links_per_second(&self) -> u64 {
        self.config.read().snapshot_links_per_second
    }

    pub fn column_index_size(&self) -> Option<DataSize> {
        self.config.read().column_index_size
    }

    pub fn column_index_cache_size(&self) -> DataSize {
        self.config.read().column_index_cache_size
    }

    pub fn sstable_preemptive_open_interval(&self) -> Option<DataSize> {
        self.config.read().sstable_preemptive_open_interval
    }

    pub fn uuid_sstable_identifiers_enabled(&self) -> bool {
        self.config.read().uuid_sstable_identifiers_enabled
    }

    // ── Hints ────────────────────────────────────────────────────────────

    pub fn hinted_handoff_enabled(&self) -> bool {
        self.config.read().hinted_handoff_enabled
    }

    pub fn hinted_handoff_disabled_datacenters(&self) -> Option<Vec<String>> {
        self.config
            .read()
            .hinted_handoff_disabled_datacenters
            .clone()
    }

    pub fn max_hints_delivery_threads(&self) -> u32 {
        self.config.read().max_hints_delivery_threads
    }

    pub fn hints_flush_period(&self) -> Duration {
        self.config.read().hints_flush_period
    }

    pub fn max_hints_file_size(&self) -> DataSize {
        self.config.read().max_hints_file_size
    }

    pub fn auto_hints_cleanup_enabled(&self) -> bool {
        self.config.read().auto_hints_cleanup_enabled
    }

    pub fn max_hints_size_per_host(&self) -> DataSize {
        self.config.read().max_hints_size_per_host
    }

    pub fn transfer_hints_on_decommission(&self) -> bool {
        self.config.read().transfer_hints_on_decommission
    }

    pub fn hints_compression(&self) -> Option<Vec<crate::config::ClassWithListParameters>> {
        self.config.read().hints_compression.clone()
    }

    pub fn heap_dump_path(&self) -> Option<String> {
        self.config.read().heap_dump_path.clone()
    }

    pub fn dump_heap_on_uncaught_exception(&self) -> bool {
        self.config.read().dump_heap_on_uncaught_exception
    }

    pub fn hint_window_persistent_enabled(&self) -> bool {
        self.config.read().hint_window_persistent_enabled
    }

    pub fn batchlog_replay_throttle(&self) -> DataSize {
        self.config.read().batchlog_replay_throttle
    }

    pub fn batchlog_endpoint_strategy(&self) -> String {
        self.config.read().batchlog_endpoint_strategy.clone()
    }

    // ── Caches ───────────────────────────────────────────────────────────

    pub fn key_cache_size(&self) -> Option<DataSize> {
        self.config.read().key_cache_size
    }

    pub fn row_cache_size(&self) -> Option<DataSize> {
        self.config.read().row_cache_size
    }

    pub fn row_cache_save_period(&self) -> Option<Duration> {
        self.config.read().row_cache_save_period
    }

    pub fn key_cache_keys_to_save(&self) -> Option<u32> {
        self.config.read().key_cache_keys_to_save
    }

    pub fn row_cache_class_name(&self) -> Option<String> {
        self.config.read().row_cache_class_name.clone()
    }

    pub fn row_cache_keys_to_save(&self) -> Option<u32> {
        self.config.read().row_cache_keys_to_save
    }

    pub fn counter_cache_keys_to_save(&self) -> Option<u32> {
        self.config.read().counter_cache_keys_to_save
    }

    pub fn cache_load_timeout(&self) -> Duration {
        self.config.read().cache_load_timeout
    }

    pub fn counter_cache_size(&self) -> Option<DataSize> {
        self.config.read().counter_cache_size
    }

    pub fn counter_cache_save_period(&self) -> Option<Duration> {
        self.config.read().counter_cache_save_period
    }

    pub fn chunk_cache_size(&self) -> Option<DataSize> {
        self.config.read().chunk_cache_size
    }

    pub fn saved_caches_directory(&self) -> Option<String> {
        self.config.read().saved_caches_directory.clone()
    }

    // ── CDC / index summaries ───────────────────────────────────────────

    pub fn cdc_total_space(&self) -> DataSize {
        self.config.read().cdc_total_space
    }

    pub fn cdc_block_writes(&self) -> bool {
        self.config.read().cdc_block_writes
    }

    pub fn cdc_on_repair_enabled(&self) -> bool {
        self.config.read().cdc_on_repair_enabled
    }

    pub fn cdc_free_space_check_interval(&self) -> Duration {
        self.config.read().cdc_free_space_check_interval
    }

    pub fn index_summary_capacity(&self) -> Option<DataSize> {
        self.config.read().index_summary_capacity
    }

    pub fn index_summary_resize_interval(&self) -> Option<Duration> {
        self.config.read().index_summary_resize_interval
    }

    // ── Auth cache / authorization ─────────────────────────────────────

    pub fn traverse_auth_from_root(&self) -> bool {
        self.config.read().traverse_auth_from_root
    }

    pub fn permissions_validity_in_ms(&self) -> u64 {
        self.config.read().permissions_validity_in_ms
    }

    pub fn permissions_update_interval(&self) -> Option<Duration> {
        self.config.read().permissions_update_interval
    }

    pub fn permissions_cache_active_update(&self) -> bool {
        self.config.read().permissions_cache_active_update
    }

    pub fn roles_validity_in_ms(&self) -> u64 {
        self.config.read().roles_validity_in_ms
    }

    pub fn roles_update_interval(&self) -> Option<Duration> {
        self.config.read().roles_update_interval
    }

    pub fn roles_cache_active_update(&self) -> bool {
        self.config.read().roles_cache_active_update
    }

    pub fn credentials_validity_in_ms(&self) -> u64 {
        self.config.read().credentials_validity_in_ms
    }

    pub fn credentials_update_interval(&self) -> Option<Duration> {
        self.config.read().credentials_update_interval
    }

    pub fn credentials_cache_active_update(&self) -> bool {
        self.config.read().credentials_cache_active_update
    }

    // ── Failure policies ─────────────────────────────────────────────────

    pub fn disk_failure_policy(&self) -> String {
        self.config.read().disk_failure_policy.clone()
    }

    pub fn commit_failure_policy(&self) -> String {
        self.config.read().commit_failure_policy.clone()
    }

    // ── Admin ────────────────────────────────────────────────────────────

    pub fn admin_port(&self) -> u16 {
        self.config.read().admin_port
    }

    // ── Failure detection / dynamic snitch ──────────────────────────────

    pub fn phi_convict_threshold(&self) -> f64 {
        self.config.read().phi_convict_threshold
    }

    pub fn dynamic_snitch(&self) -> bool {
        self.config.read().dynamic_snitch
    }

    pub fn dynamic_snitch_update_interval(&self) -> Duration {
        self.config.read().dynamic_snitch_update_interval
    }

    pub fn dynamic_snitch_reset_interval(&self) -> Duration {
        self.config.read().dynamic_snitch_reset_interval
    }

    pub fn dynamic_snitch_badness_threshold(&self) -> f64 {
        self.config.read().dynamic_snitch_badness_threshold
    }

    pub fn initial_location_provider(&self) -> Option<String> {
        self.config.read().initial_location_provider.clone()
    }

    pub fn node_proximity(&self) -> Option<String> {
        self.config.read().node_proximity.clone()
    }

    pub fn addresses_config(&self) -> Option<String> {
        self.config.read().addresses_config.clone()
    }

    pub fn prefer_local_connections(&self) -> bool {
        self.config.read().prefer_local_connections
    }

    pub fn failure_detector(&self) -> String {
        self.config.read().failure_detector.clone()
    }

    // ── Partition denylist ──────────────────────────────────────────────

    pub fn partition_denylist_enabled(&self) -> bool {
        self.config.read().partition_denylist_enabled
    }

    pub fn denylist_writes_enabled(&self) -> bool {
        self.config.read().denylist_writes_enabled
    }

    pub fn denylist_reads_enabled(&self) -> bool {
        self.config.read().denylist_reads_enabled
    }

    pub fn denylist_range_reads_enabled(&self) -> bool {
        self.config.read().denylist_range_reads_enabled
    }

    pub fn denylist_refresh(&self) -> Duration {
        self.config.read().denylist_refresh
    }

    pub fn denylist_initial_load_retry(&self) -> Duration {
        self.config.read().denylist_initial_load_retry
    }

    pub fn denylist_max_keys_per_table(&self) -> u32 {
        self.config.read().denylist_max_keys_per_table
    }

    pub fn denylist_max_keys_total(&self) -> u32 {
        self.config.read().denylist_max_keys_total
    }

    pub fn denylist_consistency_level(&self) -> String {
        self.config.read().denylist_consistency_level.clone()
    }

    // ── TLS / Encryption ────────────────────────────────────────────────

    pub fn client_encryption_options(&self) -> Option<crate::config::EncryptionOptions> {
        self.config.read().client_encryption_options.clone()
    }

    pub fn server_encryption_options(&self) -> Option<crate::config::EncryptionOptions> {
        self.config.read().server_encryption_options.clone()
    }

    pub fn crypto_provider(&self) -> Option<Vec<crate::config::ClassWithListParameters>> {
        self.config.read().crypto_provider.clone()
    }

    pub fn transparent_data_encryption_options(
        &self,
    ) -> Option<crate::config::TransparentDataEncryptionConfig> {
        self.config
            .read()
            .transparent_data_encryption_options
            .clone()
    }

    pub fn internode_compression(&self) -> String {
        self.config.read().internode_compression.clone()
    }

    pub fn inter_dc_tcp_nodelay(&self) -> bool {
        self.config.read().inter_dc_tcp_nodelay
    }

    pub fn trace_type_query_ttl(&self) -> Duration {
        self.config.read().trace_type_query_ttl
    }

    pub fn trace_type_repair_ttl(&self) -> Duration {
        self.config.read().trace_type_repair_ttl
    }

    pub fn sstables_per_read_log_threshold(&self) -> u32 {
        self.config.read().sstables_per_read_log_threshold
    }

    pub fn corrupted_tombstone_strategy(&self) -> String {
        self.config.read().corrupted_tombstone_strategy.clone()
    }

    pub fn max_value_size(&self) -> DataSize {
        self.config.read().max_value_size
    }

    pub fn default_keyspace_rf(&self) -> u32 {
        self.config.read().default_keyspace_rf
    }

    pub fn ideal_consistency_level(&self) -> Option<String> {
        self.config.read().ideal_consistency_level.clone()
    }

    pub fn automatic_sstable_upgrade(&self) -> bool {
        self.config.read().automatic_sstable_upgrade
    }

    pub fn max_concurrent_automatic_sstable_upgrades(&self) -> u32 {
        self.config.read().max_concurrent_automatic_sstable_upgrades
    }

    pub fn tombstone_warn_threshold(&self) -> u32 {
        self.config.read().tombstone_warn_threshold
    }

    pub fn tombstone_failure_threshold(&self) -> u32 {
        self.config.read().tombstone_failure_threshold
    }

    pub fn batch_size_warn_threshold(&self) -> DataSize {
        self.config.read().batch_size_warn_threshold
    }

    pub fn batch_size_fail_threshold(&self) -> DataSize {
        self.config.read().batch_size_fail_threshold
    }

    pub fn read_thresholds_enabled(&self) -> bool {
        self.config.read().read_thresholds_enabled
    }

    pub fn coordinator_read_size_warn_threshold(&self) -> Option<DataSize> {
        self.config.read().coordinator_read_size_warn_threshold
    }

    pub fn coordinator_read_size_fail_threshold(&self) -> Option<DataSize> {
        self.config.read().coordinator_read_size_fail_threshold
    }

    pub fn local_read_size_warn_threshold(&self) -> Option<DataSize> {
        self.config.read().local_read_size_warn_threshold
    }

    pub fn local_read_size_fail_threshold(&self) -> Option<DataSize> {
        self.config.read().local_read_size_fail_threshold
    }

    pub fn row_index_read_size_warn_threshold(&self) -> Option<DataSize> {
        self.config.read().row_index_read_size_warn_threshold
    }

    pub fn row_index_read_size_fail_threshold(&self) -> Option<DataSize> {
        self.config.read().row_index_read_size_fail_threshold
    }

    pub fn diagnostic_events_enabled(&self) -> bool {
        self.config.read().diagnostic_events_enabled
    }

    pub fn auth_read_consistency_level(&self) -> Option<String> {
        self.config.read().auth_read_consistency_level.clone()
    }

    pub fn auth_write_consistency_level(&self) -> Option<String> {
        self.config.read().auth_write_consistency_level.clone()
    }

    pub fn auth_cache_warming_enabled(&self) -> bool {
        self.config.read().auth_cache_warming_enabled
    }

    pub fn dynamic_data_masking_enabled(&self) -> bool {
        self.config.read().dynamic_data_masking_enabled
    }

    pub fn materialized_views_enabled(&self) -> bool {
        self.config.read().materialized_views_enabled
    }

    pub fn materialized_views_on_repair_enabled(&self) -> bool {
        self.config.read().materialized_views_on_repair_enabled
    }

    pub fn use_statements_enabled(&self) -> bool {
        self.config.read().use_statements_enabled
    }

    pub fn client_error_reporting_exclusions(
        &self,
    ) -> Option<crate::config::ClientErrorReportingExclusions> {
        self.config.read().client_error_reporting_exclusions.clone()
    }

    pub fn jmx_server_options(&self) -> Option<crate::config::JmxServerOptionsConfig> {
        self.config.read().jmx_server_options.clone()
    }

    pub fn startup_checks(&self) -> Option<crate::config::StartupChecksConfig> {
        self.config.read().startup_checks.clone()
    }

    pub fn accord(&self) -> Option<crate::config::AccordRuntimeConfig> {
        self.config.read().accord.clone()
    }

    pub fn reject_repair_compaction_threshold(&self) -> Option<u32> {
        self.config.read().reject_repair_compaction_threshold
    }

    pub fn repair_disk_headroom_reject_ratio(&self) -> Option<f64> {
        self.config.read().repair_disk_headroom_reject_ratio
    }

    pub fn incremental_repair_disk_headroom_reject_ratio(&self) -> Option<f64> {
        self.config
            .read()
            .incremental_repair_disk_headroom_reject_ratio
    }

    pub fn auto_repair(&self) -> Option<crate::config::AutoRepairConfig> {
        self.config.read().auto_repair.clone()
    }

    pub fn storage_compatibility_mode(&self) -> String {
        self.config.read().storage_compatibility_mode.clone()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn snapshot_is_consistent() {
        let cfg = CassandraConfig::default();
        let g = GuardrailsConfig::default();
        let dd = DatabaseDescriptor::new(cfg, g);
        let snap = dd.snapshot();
        assert_eq!(snap.config.cluster_name, "Test Cluster");
        assert!(snap.guardrails.allow_filtering_enabled);
    }

    #[test]
    fn swap_config_updates() {
        let dd = DatabaseDescriptor::new(CassandraConfig::default(), GuardrailsConfig::default());
        assert_eq!(dd.cluster_name(), "Test Cluster");

        let mut new_cfg = CassandraConfig::default();
        new_cfg.cluster_name = "Swapped".to_string();
        dd.swap_config(new_cfg);
        assert_eq!(dd.cluster_name(), "Swapped");
    }

    #[test]
    fn swap_guardrails_updates() {
        let dd = DatabaseDescriptor::new(CassandraConfig::default(), GuardrailsConfig::default());
        assert!(dd.snapshot().guardrails.truncate_enabled);

        let mut new_g = GuardrailsConfig::default();
        new_g.truncate_enabled = false;
        dd.swap_guardrails(new_g);
        assert!(!dd.snapshot().guardrails.truncate_enabled);
    }

    #[test]
    fn typed_accessors() {
        let dd = DatabaseDescriptor::new(CassandraConfig::default(), GuardrailsConfig::default());
        assert_eq!(dd.native_transport_port(), 9042);
        assert_eq!(dd.storage_port(), 7000);
        assert_eq!(dd.concurrent_reads(), 32);
        assert_eq!(dd.admin_port(), 9090);
        assert!(dd.start_native_transport());
        assert_eq!(dd.native_transport_max_threads(), 128);
        assert_eq!(dd.native_transport_max_auth_threads(), 4);
        assert!(dd.native_transport_allow_older_protocols());
        assert!(!dd.native_transport_flush_in_batches_legacy());
        assert_eq!(dd.native_transport_idle_timeout().millis(), 0);
        assert!(dd.listen_address().is_none());
        assert!(dd.listen_interface().is_none());
        assert!(!dd.listen_interface_prefer_ipv6());
        assert!(dd.broadcast_address().is_none());
        assert!(!dd.listen_on_broadcast_address());
        assert!(dd.rpc_address().is_none());
        assert!(dd.rpc_interface().is_none());
        assert!(!dd.rpc_interface_prefer_ipv6());
        assert!(dd.broadcast_rpc_address().is_none());
        assert!(dd.rpc_keepalive());
        assert!(dd.hinted_handoff_enabled());
        assert!(dd.hinted_handoff_disabled_datacenters().is_none());
        assert_eq!(dd.hints_flush_period().millis(), 10_000);
        assert_eq!(dd.max_hints_file_size().mebibytes(), 128);
        assert_eq!(dd.max_hints_size_per_host().bytes(), 0);
        assert!(!dd.auto_hints_cleanup_enabled());
        assert!(dd.transfer_hints_on_decommission());
        assert!(dd.hints_compression().is_none());
        assert!(dd.heap_dump_path().is_none());
        assert!(!dd.dump_heap_on_uncaught_exception());
        assert!(dd.hint_window_persistent_enabled());
        assert_eq!(dd.batchlog_replay_throttle().kibibytes(), 1024);
        assert_eq!(dd.batchlog_endpoint_strategy(), "random_remote");
        assert!(dd.auto_bootstrap());
        assert_eq!(dd.endpoint_snitch(), "SimpleSnitch");
        assert_eq!(dd.read_request_timeout().millis(), 5_000);
        assert_eq!(dd.repair_request_timeout().millis(), 120_000);
        assert_eq!(dd.slow_query_log_timeout().millis(), 500);
        assert!(dd.internode_timeout());
        assert_eq!(dd.streaming_connections_per_host(), 1);
        assert_eq!(dd.compaction_throughput().mebibytes_per_second(), 64);
        assert_eq!(
            dd.entire_sstable_stream_throughput_outbound()
                .mebibytes_per_second(),
            24
        );
        assert_eq!(
            dd.entire_sstable_inter_dc_stream_throughput_outbound()
                .mebibytes_per_second(),
            24
        );
        assert!(dd.stream_entire_sstables());
        assert_eq!(dd.concurrent_materialized_view_builders(), 1);
        assert_eq!(dd.memtable_flush_writers(), 0);
        assert_eq!(dd.memtable_allocation_type(), "heap_buffers");
        assert_eq!(dd.disk_optimization_strategy(), "ssd");
        assert!(dd.networking_cache_size().is_none());
        assert!(!dd.file_cache_enabled());
        assert!(dd.local_system_data_file_directory().is_none());
        assert!(dd.commitlog_disk_access_mode().is_none());
        assert!(dd.disk_access_mode().is_none());
        assert!(dd.commitlog_compression().is_none());
        assert_eq!(dd.compaction_read_disk_access_mode(), "auto");
        assert_eq!(dd.flush_compression(), "fast");
        assert!(dd.commitlog_sync_group_window().is_none());
        assert!(dd.periodic_commitlog_sync_lag_block().is_none());
        assert!(dd.buffer_pool_use_heap_if_exhausted());
        assert!(!dd.trickle_fsync());
        assert_eq!(dd.trickle_fsync_interval().kibibytes(), 10_240);
        assert_eq!(dd.internode_socket_send_buffer_size().bytes(), 0);
        assert_eq!(dd.internode_socket_receive_buffer_size().bytes(), 0);
        assert_eq!(
            dd.internode_application_send_queue_capacity().mebibytes(),
            4
        );
        assert_eq!(
            dd.internode_application_send_queue_reserve_endpoint_capacity()
                .mebibytes(),
            128
        );
        assert_eq!(
            dd.internode_application_send_queue_reserve_global_capacity()
                .mebibytes(),
            512
        );
        assert_eq!(
            dd.internode_application_receive_queue_capacity()
                .mebibytes(),
            4
        );
        assert_eq!(
            dd.internode_application_receive_queue_reserve_endpoint_capacity()
                .mebibytes(),
            128
        );
        assert_eq!(
            dd.internode_application_receive_queue_reserve_global_capacity()
                .mebibytes(),
            512
        );
        assert_eq!(dd.internode_tcp_connect_timeout().millis(), 2_000);
        assert_eq!(dd.internode_tcp_user_timeout().millis(), 30_000);
        assert_eq!(dd.internode_streaming_tcp_user_timeout().millis(), 300_000);
        assert_eq!(dd.streaming_keep_alive_period().millis(), 300_000);
        assert_eq!(dd.streaming_state_expires().millis(), 259_200_000);
        assert_eq!(dd.streaming_state_size().mebibytes(), 40);
        assert!(dd.streaming_stats_enabled());
        assert!(!dd.incremental_backups());
        assert!(!dd.snapshot_before_compaction());
        assert!(dd.auto_snapshot());
        assert!(dd.auto_snapshot_ttl().is_none());
        assert_eq!(dd.snapshot_links_per_second(), 0);
        assert!(dd.row_cache_save_period().is_none());
        assert!(dd.key_cache_keys_to_save().is_none());
        assert!(dd.row_cache_class_name().is_none());
        assert!(dd.row_cache_keys_to_save().is_none());
        assert!(dd.counter_cache_keys_to_save().is_none());
        assert_eq!(dd.cache_load_timeout().millis(), 30_000);
        assert!(dd.column_index_size().is_none());
        assert_eq!(dd.column_index_cache_size().kibibytes(), 2);
        assert_eq!(
            dd.sstable_preemptive_open_interval().unwrap().mebibytes(),
            50
        );
        assert!(!dd.uuid_sstable_identifiers_enabled());
        assert_eq!(dd.cdc_total_space().mebibytes(), 4_096);
        assert!(dd.cdc_block_writes());
        assert!(dd.cdc_on_repair_enabled());
        assert_eq!(dd.cdc_free_space_check_interval().millis(), 250);
        assert!(!dd.traverse_auth_from_root());
        assert_eq!(dd.permissions_validity_in_ms(), 2_000);
        assert!(dd.permissions_update_interval().is_none());
        assert!(!dd.permissions_cache_active_update());
        assert_eq!(dd.roles_validity_in_ms(), 2_000);
        assert!(dd.roles_update_interval().is_none());
        assert!(!dd.roles_cache_active_update());
        assert_eq!(dd.credentials_validity_in_ms(), 2_000);
        assert!(dd.credentials_update_interval().is_none());
        assert!(!dd.credentials_cache_active_update());
        assert_eq!(dd.phi_convict_threshold(), 8.0);
        assert!(dd.dynamic_snitch());
        assert!(dd.initial_location_provider().is_none());
        assert!(dd.node_proximity().is_none());
        assert!(dd.addresses_config().is_none());
        assert!(!dd.prefer_local_connections());
        assert_eq!(dd.failure_detector(), "FailureDetector");
        assert!(!dd.partition_denylist_enabled());
        assert!(dd.denylist_writes_enabled());
        assert!(dd.denylist_reads_enabled());
        assert!(dd.denylist_range_reads_enabled());
        assert_eq!(dd.denylist_refresh().millis(), 600_000);
        assert_eq!(dd.denylist_initial_load_retry().millis(), 5_000);
        assert_eq!(dd.denylist_max_keys_per_table(), 1_000);
        assert_eq!(dd.denylist_max_keys_total(), 10_000);
        assert_eq!(dd.denylist_consistency_level(), "QUORUM");
        assert!(dd.client_encryption_options().is_none());
        assert!(dd.server_encryption_options().is_none());
        assert!(dd.crypto_provider().is_none());
        assert!(dd.transparent_data_encryption_options().is_none());
        assert_eq!(dd.internode_compression(), "dc");
        assert!(!dd.inter_dc_tcp_nodelay());
        assert_eq!(dd.trace_type_query_ttl().millis(), 86_400_000);
        assert_eq!(dd.trace_type_repair_ttl().millis(), 604_800_000);
        assert_eq!(dd.sstables_per_read_log_threshold(), 100);
        assert_eq!(dd.corrupted_tombstone_strategy(), "disabled");
        assert_eq!(dd.max_value_size().mebibytes(), 256);
        assert_eq!(dd.default_keyspace_rf(), 1);
        assert!(dd.ideal_consistency_level().is_none());
        assert!(!dd.automatic_sstable_upgrade());
        assert_eq!(dd.max_concurrent_automatic_sstable_upgrades(), 1);
        assert_eq!(dd.tombstone_warn_threshold(), 1_000);
        assert_eq!(dd.tombstone_failure_threshold(), 100_000);
        assert_eq!(dd.batch_size_warn_threshold().kibibytes(), 5);
        assert_eq!(dd.batch_size_fail_threshold().kibibytes(), 50);
        assert!(!dd.read_thresholds_enabled());
        assert!(dd.coordinator_read_size_warn_threshold().is_none());
        assert!(dd.coordinator_read_size_fail_threshold().is_none());
        assert!(dd.local_read_size_warn_threshold().is_none());
        assert!(dd.local_read_size_fail_threshold().is_none());
        assert!(dd.row_index_read_size_warn_threshold().is_none());
        assert!(dd.row_index_read_size_fail_threshold().is_none());
        assert!(!dd.diagnostic_events_enabled());
        assert!(dd.auth_read_consistency_level().is_none());
        assert!(dd.auth_write_consistency_level().is_none());
        assert!(!dd.auth_cache_warming_enabled());
        assert!(!dd.dynamic_data_masking_enabled());
        assert!(!dd.materialized_views_enabled());
        assert!(dd.materialized_views_on_repair_enabled());
        assert!(dd.use_statements_enabled());
        assert!(dd.client_error_reporting_exclusions().is_none());
        assert!(dd.jmx_server_options().is_none());
        assert!(dd.startup_checks().is_none());
        assert!(dd.accord().is_none());
        assert!(dd.reject_repair_compaction_threshold().is_none());
        assert!(dd.repair_disk_headroom_reject_ratio().is_none());
        assert!(dd.incremental_repair_disk_headroom_reject_ratio().is_none());
        assert!(dd.auto_repair().is_none());
        assert_eq!(dd.storage_compatibility_mode(), "NONE");
    }

    #[test]
    fn clone_shares_same_state() {
        let dd = DatabaseDescriptor::new(CassandraConfig::default(), GuardrailsConfig::default());
        let dd2 = dd.clone();

        let mut new_cfg = CassandraConfig::default();
        new_cfg.cluster_name = "Shared".to_string();
        dd.swap_config(new_cfg);

        // dd2 sees the same change because they share the Arc
        assert_eq!(dd2.cluster_name(), "Shared");
    }
}
