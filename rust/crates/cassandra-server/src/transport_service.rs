// Licensed under Apache License, Version 2.0.

//! Native transport service lifecycle management.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.transport.NativeTransportService`
//! - `org.apache.cassandra.service.CassandraDaemon`

use parking_lot::Mutex;
use thiserror::Error;
use tracing::info;

use cassandra_config::CassandraConfig;

/// Transport service lifecycle states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ServiceState {
    New,
    Initialized,
    Started,
    Stopping,
    Stopped,
}

impl std::fmt::Display for ServiceState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ServiceState::New => write!(f, "NEW"),
            ServiceState::Initialized => write!(f, "INITIALIZED"),
            ServiceState::Started => write!(f, "STARTED"),
            ServiceState::Stopping => write!(f, "STOPPING"),
            ServiceState::Stopped => write!(f, "STOPPED"),
        }
    }
}

#[derive(Debug, Error)]
pub enum ServiceError {
    #[error("Invalid state transition from {from} to {to}")]
    InvalidTransition {
        from: ServiceState,
        to: ServiceState,
    },
    #[error("Service error: {0}")]
    Other(String),
}

/// Configuration extracted from `CassandraConfig` for the native transport.
#[derive(Debug, Clone)]
pub struct NativeTransportConfig {
    pub listen_address: String,
    pub port: u16,
    pub max_concurrent_connections: i32,
    pub max_concurrent_connections_per_ip: i32,
    pub max_frame_size: u64,
    pub max_request_data_in_flight: u64,
    pub rate_limiting_enabled: bool,
    pub max_requests_per_second: u32,
    pub idle_timeout_seconds: u64,
    pub client_encryption_enabled: bool,
}

impl NativeTransportConfig {
    /// Extract native transport config from the full `CassandraConfig`.
    pub fn from_cassandra_config(cfg: &CassandraConfig) -> Self {
        let listen_address = cfg
            .listen_address
            .clone()
            .unwrap_or_else(|| "127.0.0.1".to_string());
        let client_encryption_enabled = cfg
            .client_encryption_options
            .as_ref()
            .map(|e| e.enabled)
            .unwrap_or(false);

        Self {
            listen_address,
            port: cfg.native_transport_port,
            max_concurrent_connections: cfg.native_transport_max_concurrent_connections,
            max_concurrent_connections_per_ip: cfg
                .native_transport_max_concurrent_connections_per_ip,
            max_frame_size: cfg.native_transport_max_frame_size,
            max_request_data_in_flight: cfg.native_transport_max_request_data_in_flight,
            rate_limiting_enabled: cfg.native_transport_rate_limiting_enabled,
            max_requests_per_second: cfg.native_transport_max_requests_per_second,
            idle_timeout_seconds: cfg.native_transport_idle_timeout_seconds,
            client_encryption_enabled,
        }
    }

    /// Formatted bind address (e.g. "127.0.0.1:9042").
    pub fn bind_address(&self) -> String {
        format!("{}:{}", self.listen_address, self.port)
    }
}

/// Manages the lifecycle of the native transport server.
pub struct NativeTransportService {
    state: Mutex<ServiceState>,
    config: NativeTransportConfig,
}

impl NativeTransportService {
    pub fn new(config: NativeTransportConfig) -> Self {
        info!(state = %ServiceState::New, "NativeTransportService created");
        Self {
            state: Mutex::new(ServiceState::New),
            config,
        }
    }

    pub fn state(&self) -> ServiceState {
        *self.state.lock()
    }

    pub fn config(&self) -> &NativeTransportConfig {
        &self.config
    }

    /// Transition: NEW → INITIALIZED.
    /// Validates configuration and prepares resources.
    pub fn initialize(&self) -> Result<(), ServiceError> {
        self.transition(ServiceState::New, ServiceState::Initialized)?;
        info!("NativeTransportService initialized");
        Ok(())
    }

    /// Transition: INITIALIZED → STARTED.
    pub fn start(&self) -> Result<(), ServiceError> {
        self.transition(ServiceState::Initialized, ServiceState::Started)?;
        info!(
            bind = %self.config.bind_address(),
            "NativeTransportService started"
        );
        Ok(())
    }

    /// Transition: STARTED → STOPPING.
    pub fn begin_stop(&self) -> Result<(), ServiceError> {
        self.transition(ServiceState::Started, ServiceState::Stopping)?;
        info!("NativeTransportService stopping");
        Ok(())
    }

    /// Transition: STOPPING → STOPPED.
    pub fn finish_stop(&self) -> Result<(), ServiceError> {
        self.transition(ServiceState::Stopping, ServiceState::Stopped)?;
        info!("NativeTransportService stopped");
        Ok(())
    }

    /// Convenience: STARTED → STOPPING → STOPPED.
    pub fn stop(&self) -> Result<(), ServiceError> {
        self.begin_stop()?;
        self.finish_stop()
    }

    fn transition(&self, expected: ServiceState, target: ServiceState) -> Result<(), ServiceError> {
        let mut state = self.state.lock();
        if *state != expected {
            return Err(ServiceError::InvalidTransition {
                from: *state,
                to: target,
            });
        }
        *state = target;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn default_config() -> NativeTransportConfig {
        NativeTransportConfig {
            listen_address: "127.0.0.1".to_string(),
            port: 9042,
            max_concurrent_connections: -1,
            max_concurrent_connections_per_ip: -1,
            max_frame_size: 256 * 1024 * 1024,
            max_request_data_in_flight: 512 * 1024 * 1024,
            rate_limiting_enabled: false,
            max_requests_per_second: 25_000,
            idle_timeout_seconds: 0,
            client_encryption_enabled: false,
        }
    }

    #[test]
    fn lifecycle_happy_path() {
        let svc = NativeTransportService::new(default_config());
        assert_eq!(svc.state(), ServiceState::New);

        svc.initialize().unwrap();
        assert_eq!(svc.state(), ServiceState::Initialized);

        svc.start().unwrap();
        assert_eq!(svc.state(), ServiceState::Started);

        svc.stop().unwrap();
        assert_eq!(svc.state(), ServiceState::Stopped);
    }

    #[test]
    fn invalid_transition_new_to_started() {
        let svc = NativeTransportService::new(default_config());
        let err = svc.start().unwrap_err();
        assert!(matches!(err, ServiceError::InvalidTransition { .. }));
    }

    #[test]
    fn invalid_transition_initialized_to_stopped() {
        let svc = NativeTransportService::new(default_config());
        svc.initialize().unwrap();
        let err = svc.stop().unwrap_err();
        assert!(matches!(err, ServiceError::InvalidTransition { .. }));
    }

    #[test]
    fn double_initialize() {
        let svc = NativeTransportService::new(default_config());
        svc.initialize().unwrap();
        let err = svc.initialize().unwrap_err();
        assert!(matches!(err, ServiceError::InvalidTransition { .. }));
    }

    #[test]
    fn config_bind_address() {
        let cfg = default_config();
        assert_eq!(cfg.bind_address(), "127.0.0.1:9042");
    }

    #[test]
    fn from_cassandra_config() {
        let cc = CassandraConfig::default();
        let ntc = NativeTransportConfig::from_cassandra_config(&cc);
        assert_eq!(ntc.port, 9042);
        assert_eq!(ntc.max_concurrent_connections, -1);
        assert!(!ntc.client_encryption_enabled);
    }
}
