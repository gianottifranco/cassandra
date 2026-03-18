// Licensed under Apache License, Version 2.0.

//! Cassandra Rust Server – single-node binary.
//!
//! ## Startup sequence
//!
//! 1. Initialize tracing
//! 2. Create data directories
//! 3. Open storage engine
//! 4. Create schema catalog
//! 5. Replay commit log (crash recovery)
//! 6. Log ready status

mod auth;
mod backpressure;
mod client_state;
mod dispatcher;
mod error_mapping;
mod executor;
mod guardrail_checks;
mod query_processor;
mod resource_limits;
mod server;
mod shutdown;
mod startup_checks;
mod storage_service;
mod term_binding;
mod transport_metrics;
mod transport_service;
mod where_binding;

use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;
use tracing::info;

use cassandra_schema::SchemaCatalog;
use cassandra_storage::commitlog::CommitLogConfig;
use cassandra_storage::engine::{EngineConfig, StorageEngine};

use cassandra_security::auth::PasswordAuthenticator;

use crate::executor::QueryExecutor;
use crate::resource_limits::ResourceLimits;
use crate::server::{NativeServer, ServerConfig};
use crate::shutdown::ShutdownCoordinator;
use crate::transport_metrics::TransportMetrics;
use crate::transport_service::{NativeTransportConfig, NativeTransportService};

const VERSION: &str = env!("CARGO_PKG_VERSION");

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Tracing
    tracing_subscriber::fmt::init();

    info!("cassandra-server v{VERSION} starting");

    // 2. Data directories
    let data_dir = PathBuf::from("data");
    let commitlog_dir = data_dir.join("commitlog");

    // 3. Open storage engine
    let engine_config = EngineConfig {
        data_directories: vec![data_dir.clone()],
        commitlog: CommitLogConfig {
            max_segment_size: 32 * 1024 * 1024, // 32 MiB
            directory: commitlog_dir,
            ..CommitLogConfig::default()
        },
        memtable_flush_threshold: 128 * 1024 * 1024, // 128 MiB
        gc_grace_seconds: 864_000,                   // 10 days
        ..EngineConfig::default()
    };

    let engine =
        Arc::new(StorageEngine::open(engine_config).expect("Failed to open storage engine"));

    // 4. Schema catalog
    let catalog = Arc::new(RwLock::new(SchemaCatalog::new()));

    // 5. Commit log replay
    let replayed = engine.replay_commitlog().expect("Commit log replay failed");
    info!(replayed, "Commit log replay complete");

    // 6. Security & Auth Managers
    let role_manager = Arc::new(auth::SystemAuthRoleManager::new(Arc::clone(&engine)));
    let authorizer = Arc::new(auth::SystemAuthAuthorizer::new(
        Arc::clone(&engine),
        Arc::clone(&role_manager) as Arc<dyn cassandra_security::RoleManager>,
    ));

    // 7. Load CassandraConfig to configure TLS
    let config_path =
        std::env::var("CASSANDRA_CONFIG").unwrap_or_else(|_| "conf/cassandra.yaml".to_string());
    let cassandra_config = cassandra_config::load_config(std::path::Path::new(&config_path))
        .unwrap_or_else(|e| {
            tracing::warn!("Failed to parse config from {}: {}", config_path, e);
            cassandra_config::CassandraConfig::default()
        });

    let mut tls_acceptor = None;
    let mut client_encryption_enabled = false;
    if let Some(enc) = &cassandra_config.client_encryption_options {
        if enc.enabled {
            client_encryption_enabled = true;
            let tls_cfg = cassandra_security::tls::TlsConfig {
                enabled: true,
                certificate_path: enc.certificate.clone().unwrap_or_default().into(),
                key_path: enc.certificate_key.clone().unwrap_or_default().into(),
                ca_certificate_path: enc.ca_certificate.clone().map(Into::into),
                require_client_auth: enc.require_client_auth,
                ..Default::default()
            };
            match cassandra_security::tls::ReloadableTlsAcceptor::new(&tls_cfg) {
                Ok(acceptor) => {
                    tracing::info!("Native Protocol TLS configured successfully");
                    tls_acceptor = Some(acceptor);
                }
                Err(e) => {
                    tracing::error!("Failed to configure TLS for Native Protocol: {}", e);
                }
            }
        }
    }

    // 8. FQL and Audit Logger initialization
    let fql_enabled = cassandra_config
        .full_query_logging_options
        .as_ref()
        .map(|o| o.enabled)
        .unwrap_or(false);
    let fql_dir = cassandra_config
        .full_query_logging_options
        .as_ref()
        .and_then(|o| o.log_dir.clone())
        .unwrap_or_else(|| "logs/fql".to_string());
    let fql_max_size = cassandra_config
        .full_query_logging_options
        .as_ref()
        .and_then(|o| o.max_log_size_mb)
        .unwrap_or(500);

    let fql_logger = Arc::new(
        cassandra_security::fql::FqlLogger::new(PathBuf::from(fql_dir), fql_max_size, fql_enabled)
            .expect("Failed to initialize FQL logger"),
    );

    let audit_enabled = cassandra_config
        .audit_logging_options
        .as_ref()
        .map(|o| o.enabled)
        .unwrap_or(false);
    let audit_logger_type = cassandra_config
        .audit_logging_options
        .as_ref()
        .map(|o| o.logger.clone())
        .unwrap_or_else(|| "NoOpAuditLogger".to_string());
    let audit_dir = cassandra_config
        .audit_logging_options
        .as_ref()
        .and_then(|o| o.audit_logs_dir.clone())
        .unwrap_or_else(|| "logs/audit".to_string());

    let base_audit_logger: Box<dyn cassandra_security::audit::AuditLogger> =
        if audit_enabled && audit_logger_type == "FileAuditLogger" {
            // max_log_size or default
            let max_size = cassandra_config
                .audit_logging_options
                .as_ref()
                .and_then(|o| o.max_log_size)
                .unwrap_or(100 * 1024 * 1024);
            let fl =
                cassandra_security::audit::FileAuditLogger::new(PathBuf::from(audit_dir), max_size)
                    .expect("Failed to create FileAuditLogger");
            Box::new(fl)
        } else {
            Box::new(cassandra_security::audit::NoOpAuditLogger)
        };

    let (async_audit_logger, _audit_handle) =
        cassandra_security::audit::AsyncAuditLogger::new(base_audit_logger);
    let audit_logger =
        Arc::new(async_audit_logger) as Arc<dyn cassandra_security::audit::AuditLogger>;

    // 9. Build executor
    let executor = Arc::new(QueryExecutor::new(
        Arc::clone(&engine),
        Arc::clone(&catalog),
        Arc::clone(&role_manager) as Arc<dyn cassandra_security::RoleManager>,
        Arc::clone(&authorizer) as Arc<dyn cassandra_security::Authorizer>,
    ));

    // 10. Native Transport Service lifecycle
    let nt_config = NativeTransportConfig::from_cassandra_config(&cassandra_config);
    let transport_service = Arc::new(NativeTransportService::new(nt_config.clone()));
    transport_service
        .initialize()
        .map_err(|e| anyhow::anyhow!(e))?;

    // 11. Resource limits, metrics, shutdown coordinator
    let resource_limits = Arc::new(ResourceLimits::new(
        nt_config.max_concurrent_connections,
        nt_config.max_concurrent_connections_per_ip,
        nt_config.max_request_data_in_flight,
    ));
    let transport_metrics = Arc::new(TransportMetrics::new());
    let shutdown_coordinator =
        Arc::new(ShutdownCoordinator::new(std::time::Duration::from_secs(30)));

    // 12. Build NativeServer
    let native_auth = Arc::new(crate::server::NativeAuthWrapper::new(Arc::new(
        PasswordAuthenticator::new(Arc::clone(&role_manager)),
    )
        as Arc<dyn cassandra_security::auth::Authenticator>));

    let server_config = ServerConfig {
        listen_address: nt_config.bind_address(),
        client_encryption_enabled,
    };

    let server = Arc::new(NativeServer::new(
        server_config,
        Arc::clone(&executor),
        native_auth as Arc<dyn cassandra_native_protocol::auth::Authenticator>,
        tls_acceptor,
        Arc::clone(&fql_logger),
        Arc::clone(&audit_logger),
        Arc::clone(&resource_limits),
        Arc::clone(&transport_metrics),
        Arc::clone(&shutdown_coordinator),
    ));

    transport_service.start().map_err(|e| anyhow::anyhow!(e))?;
    info!("cassandra-server v{VERSION} ready");

    // 13. Install signal handler for graceful shutdown
    let shutdown_for_signal = Arc::clone(&shutdown_coordinator);
    tokio::spawn(async move {
        shutdown::signal_handler(shutdown_for_signal).await;
    });

    // Block on the server
    server.run().await?;

    // Drain and stop
    transport_service.begin_stop().ok();
    shutdown_coordinator.drain().await;
    transport_service.finish_stop().ok();
    info!("cassandra-server v{VERSION} shut down");

    Ok(())
}
