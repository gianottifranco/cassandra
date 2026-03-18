// Licensed under Apache License, Version 2.0.

//! Config hot-reload via file watching.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.config.DatabaseDescriptor.loadConfig()`
//!
//! Watches `cassandra.yaml` for changes, re-parses, validates, applies
//! env overrides, and atomically swaps the config in `DatabaseDescriptor`.
//! Notifies registered callbacks so subsystems can react.

use crate::database_descriptor::DatabaseDescriptor;
use crate::guardrails::GuardrailsConfig;
use crate::loader::LoadError;
use crate::properties;
use crate::validation;
use notify::{Event, EventKind, RecommendedWatcher, RecursiveMode, Watcher};
use parking_lot::Mutex;
use std::path::{Path, PathBuf};
use std::sync::Arc;

/// Callback invoked after a successful config reload.
pub type ReloadCallback = Box<dyn Fn() + Send + Sync>;

/// Parameters that are safe to hot-reload without restart.
///
/// Parameters NOT in this list require a full restart.
pub const HOT_RELOADABLE_PARAMS: &[&str] = &[
    "hinted_handoff_enabled",
    "max_hints_delivery_threads",
    "hinted_handoff_throttle",
    "native_transport_max_concurrent_connections",
    "native_transport_max_concurrent_connections_per_ip",
    "native_transport_max_request_data_in_flight",
    "native_transport_rate_limiting_enabled",
    "native_transport_max_requests_per_second",
    "permissions_validity_in_ms",
    "permissions_cache_max_entries",
    "roles_validity_in_ms",
    "roles_cache_max_entries",
    "credentials_validity_in_ms",
    "credentials_cache_max_entries",
    "concurrent_reads",
    "concurrent_writes",
    "concurrent_counter_writes",
    "concurrent_materialized_view_writes",
    // Guardrails are all hot-reloadable
    "guardrails",
];

/// Watches a YAML config file and hot-reloads on change.
pub struct ConfigWatcher {
    _watcher: RecommendedWatcher,
    callbacks: Arc<Mutex<Vec<ReloadCallback>>>,
}

impl ConfigWatcher {
    /// Start watching `config_path`. On file change, re-parse, validate,
    /// apply env overrides, and swap into `descriptor`.
    ///
    /// `guardrails_path` is optional; if provided, guardrails config is
    /// loaded from a separate file; otherwise it is expected to be embedded.
    pub fn new(
        config_path: PathBuf,
        guardrails_config: Option<PathBuf>,
        descriptor: DatabaseDescriptor,
    ) -> Result<Self, ConfigWatcherError> {
        let callbacks: Arc<Mutex<Vec<ReloadCallback>>> = Arc::new(Mutex::new(Vec::new()));
        let cb_clone = callbacks.clone();

        // Clone for the closure (the originals are used below for watch path)
        let config_path_for_closure = config_path.clone();

        let mut watcher =
            notify::recommended_watcher(move |res: Result<Event, notify::Error>| match res {
                Ok(event) => {
                    if matches!(
                        event.kind,
                        EventKind::Modify(_) | EventKind::Create(_)
                    ) {
                        match Self::reload(
                            &config_path_for_closure,
                            &guardrails_config,
                            &descriptor,
                        ) {
                            Ok(()) => {
                                tracing::info!("config hot-reloaded successfully");
                                for cb in cb_clone.lock().iter() {
                                    cb();
                                }
                            }
                            Err(e) => {
                                tracing::warn!(error = %e, "config reload failed, keeping previous");
                            }
                        }
                    }
                }
                Err(e) => tracing::error!(error = %e, "config watcher error"),
            })
            .map_err(|e| ConfigWatcherError::WatchError(e.to_string()))?;

        // Watch the parent directory (handles atomic file replacements)
        let parent = config_path.parent().unwrap_or(Path::new(".")).to_path_buf();
        watcher
            .watch(&parent, RecursiveMode::NonRecursive)
            .map_err(|e| ConfigWatcherError::WatchError(e.to_string()))?;

        Ok(Self {
            _watcher: watcher,
            callbacks,
        })
    }

    /// Register a callback invoked after each successful reload.
    pub fn on_reload(&self, callback: ReloadCallback) {
        self.callbacks.lock().push(callback);
    }

    fn reload(
        config_path: &Path,
        guardrails_path: &Option<PathBuf>,
        descriptor: &DatabaseDescriptor,
    ) -> Result<(), ConfigWatcherError> {
        let content = std::fs::read_to_string(config_path)
            .map_err(|e| ConfigWatcherError::ReloadFailed(LoadError::Io(e)))?;
        let mut config: crate::config::CassandraConfig = serde_yaml::from_str(&content)
            .map_err(|e| ConfigWatcherError::ReloadFailed(LoadError::Parse(e)))?;
        properties::apply_overrides(&mut config);
        let errors = validation::validate(&config);
        if !errors.is_empty() {
            return Err(ConfigWatcherError::ReloadFailed(LoadError::Validation(
                errors,
            )));
        }
        descriptor.swap_config(config);

        if let Some(gp) = guardrails_path {
            let g_content = std::fs::read_to_string(gp)
                .map_err(|e| ConfigWatcherError::ReloadFailed(LoadError::Io(e)))?;
            let guardrails: GuardrailsConfig = serde_yaml::from_str(&g_content)
                .map_err(|e| ConfigWatcherError::ReloadFailed(LoadError::Parse(e)))?;
            descriptor.swap_guardrails(guardrails);
        }

        Ok(())
    }
}

/// Errors from the config watcher.
#[derive(Debug, thiserror::Error)]
pub enum ConfigWatcherError {
    #[error("failed to set up watcher: {0}")]
    WatchError(String),
    #[error("reload failed: {0}")]
    ReloadFailed(#[from] LoadError),
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hot_reloadable_params_not_empty() {
        assert!(!HOT_RELOADABLE_PARAMS.is_empty());
        assert!(HOT_RELOADABLE_PARAMS.contains(&"concurrent_reads"));
        assert!(HOT_RELOADABLE_PARAMS.contains(&"guardrails"));
    }

    #[test]
    fn reload_from_valid_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cassandra.yaml");
        std::fs::write(
            &path,
            "cluster_name: Reloaded\nnum_tokens: 128\nnative_transport_port: 9042\n",
        )
        .unwrap();

        let descriptor = DatabaseDescriptor::new(Default::default(), GuardrailsConfig::default());
        ConfigWatcher::reload(&path, &None, &descriptor).unwrap();
        assert_eq!(descriptor.cluster_name(), "Reloaded");
    }

    #[test]
    fn reload_rejects_invalid() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("cassandra.yaml");
        std::fs::write(&path, "cluster_name: \"\"").unwrap();

        let descriptor = DatabaseDescriptor::new(Default::default(), GuardrailsConfig::default());
        let result = ConfigWatcher::reload(&path, &None, &descriptor);
        assert!(result.is_err());
        // Original config unchanged
        assert_eq!(descriptor.cluster_name(), "Test Cluster");
    }
}
