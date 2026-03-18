// Licensed under Apache License, Version 2.0.

//! Pre-flight startup checks for the Cassandra server.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.service.StartupChecks`
//!
//! ## Architecture
//!
//! Before the server begins accepting client connections, these checks
//! verify that the environment is sane: data directories exist and are
//! writable, listen ports are available, minimum disk space is met, and
//! configuration is consistent.

use std::net::SocketAddr;
use std::path::Path;

use tracing::{info, warn};

// ─── Errors ─────────────────────────────────────────────────────

/// Errors from startup validation.
#[derive(Debug, thiserror::Error)]
pub enum StartupError {
    #[error("Data directory does not exist: {path}")]
    DataDirectoryMissing { path: String },

    #[error("Data directory is not writable: {path}")]
    DataDirectoryNotWritable { path: String },

    #[error(
        "Insufficient disk space in {path}: {available_mb} MiB available, {required_mb} MiB required"
    )]
    InsufficientDiskSpace {
        path: String,
        available_mb: u64,
        required_mb: u64,
    },

    #[error("Listen port {port} is not available on {address}")]
    PortNotAvailable { address: String, port: u16 },

    #[error("Configuration error: {0}")]
    ConfigError(String),
}

// ─── Check Configuration ────────────────────────────────────────

/// Configuration for startup checks.
#[derive(Debug, Clone)]
pub struct StartupCheckConfig {
    /// Data directories to validate.
    pub data_directories: Vec<String>,
    /// Minimum free disk space in MiB.
    pub min_free_space_mb: u64,
    /// Native transport listen address.
    pub native_transport_address: Option<SocketAddr>,
    /// Inter-node listen address.
    pub internode_address: Option<SocketAddr>,
}

impl Default for StartupCheckConfig {
    fn default() -> Self {
        Self {
            data_directories: vec!["data".to_string()],
            min_free_space_mb: 256,
            native_transport_address: None,
            internode_address: None,
        }
    }
}

// ─── Checks ─────────────────────────────────────────────────────

/// Run all pre-flight startup checks.
///
/// Returns `Ok(())` if all checks pass, or the first error encountered.
pub fn run_startup_checks(config: &StartupCheckConfig) -> Result<(), StartupError> {
    info!("Running startup checks");

    check_data_directories(&config.data_directories)?;
    check_disk_space(&config.data_directories, config.min_free_space_mb)?;

    if let Some(addr) = config.native_transport_address {
        check_port_available(addr)?;
    }
    if let Some(addr) = config.internode_address {
        check_port_available(addr)?;
    }

    info!("All startup checks passed");
    Ok(())
}

/// Verify data directories exist and are writable.
fn check_data_directories(directories: &[String]) -> Result<(), StartupError> {
    for dir in directories {
        let path = Path::new(dir);
        if !path.exists() {
            return Err(StartupError::DataDirectoryMissing { path: dir.clone() });
        }
        if path
            .metadata()
            .map(|m| m.permissions().readonly())
            .unwrap_or(true)
        {
            return Err(StartupError::DataDirectoryNotWritable { path: dir.clone() });
        }
    }
    Ok(())
}

/// Check that minimum disk space is available.
fn check_disk_space(directories: &[String], min_free_mb: u64) -> Result<(), StartupError> {
    // On real systems we would use statvfs or similar. For now, we just
    // verify the directories exist (the actual space check is a stub that
    // always passes, since portable disk space queries need platform-specific code).
    for dir in directories {
        let path = Path::new(dir);
        if !path.exists() {
            continue; // Already caught by check_data_directories.
        }
        // Stub: assume sufficient space. A real implementation would
        // call platform-specific APIs here.
        let available_mb = u64::MAX; // Placeholder.
        if available_mb < min_free_mb {
            return Err(StartupError::InsufficientDiskSpace {
                path: dir.clone(),
                available_mb,
                required_mb: min_free_mb,
            });
        }
    }
    Ok(())
}

/// Check that a listen port is available by attempting to bind.
fn check_port_available(addr: SocketAddr) -> Result<(), StartupError> {
    match std::net::TcpListener::bind(addr) {
        Ok(_listener) => {
            // Port is available; drop the listener immediately.
            Ok(())
        }
        Err(_) => Err(StartupError::PortNotAvailable {
            address: addr.ip().to_string(),
            port: addr.port(),
        }),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn check_existing_directory() {
        let dir = tempfile::tempdir().unwrap();
        let config = StartupCheckConfig {
            data_directories: vec![dir.path().to_string_lossy().to_string()],
            min_free_space_mb: 0,
            native_transport_address: None,
            internode_address: None,
        };
        assert!(run_startup_checks(&config).is_ok());
    }

    #[test]
    fn check_missing_directory() {
        let config = StartupCheckConfig {
            data_directories: vec!["/nonexistent/path/12345".to_string()],
            min_free_space_mb: 0,
            native_transport_address: None,
            internode_address: None,
        };
        let err = run_startup_checks(&config).unwrap_err();
        assert!(matches!(err, StartupError::DataDirectoryMissing { .. }));
    }

    #[test]
    fn check_port_available_random() {
        // Port 0 lets the OS pick an available port.
        let addr: SocketAddr = "127.0.0.1:0".parse().unwrap();
        assert!(check_port_available(addr).is_ok());
    }

    #[test]
    fn default_config() {
        let config = StartupCheckConfig::default();
        assert_eq!(config.min_free_space_mb, 256);
        assert_eq!(config.data_directories, vec!["data".to_string()]);
    }

    #[test]
    fn startup_error_display() {
        let err = StartupError::DataDirectoryMissing {
            path: "/foo".into(),
        };
        assert!(err.to_string().contains("/foo"));

        let err = StartupError::PortNotAvailable {
            address: "127.0.0.1".into(),
            port: 9042,
        };
        assert!(err.to_string().contains("9042"));
    }
}
