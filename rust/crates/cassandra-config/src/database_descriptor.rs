// Licensed under Apache License, Version 2.0.

//! Runtime configuration singleton.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.config.DatabaseDescriptor`
//!
//! Thread-safe holder for the active `CassandraConfig` and `GuardrailsConfig`,
//! providing typed accessor methods and consistent snapshot reads.

use crate::config::CassandraConfig;
use crate::guardrails::GuardrailsConfig;
use crate::units::{DataSize, Duration};
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

    // ── Networking ───────────────────────────────────────────────────────

    pub fn listen_address(&self) -> Option<String> {
        self.config.read().listen_address.clone()
    }

    pub fn rpc_address(&self) -> Option<String> {
        self.config.read().rpc_address.clone()
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
        self.config.read().native_transport_max_concurrent_connections
    }

    pub fn native_transport_max_frame_size(&self) -> u64 {
        self.config.read().native_transport_max_frame_size
    }

    pub fn native_transport_max_request_data_in_flight(&self) -> u64 {
        self.config.read().native_transport_max_request_data_in_flight
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

    // ── Storage directories ──────────────────────────────────────────────

    pub fn data_file_directories(&self) -> Option<Vec<String>> {
        self.config.read().data_file_directories.clone()
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

    // ── Hints ────────────────────────────────────────────────────────────

    pub fn hinted_handoff_enabled(&self) -> bool {
        self.config.read().hinted_handoff_enabled
    }

    pub fn max_hints_delivery_threads(&self) -> u32 {
        self.config.read().max_hints_delivery_threads
    }

    // ── Caches ───────────────────────────────────────────────────────────

    pub fn key_cache_size(&self) -> Option<DataSize> {
        self.config.read().key_cache_size
    }

    pub fn row_cache_size(&self) -> Option<DataSize> {
        self.config.read().row_cache_size
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
        assert!(dd.hinted_handoff_enabled());
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
