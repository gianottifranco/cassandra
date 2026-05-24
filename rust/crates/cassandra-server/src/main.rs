// Licensed under Apache License, Version 2.0.

#![allow(
    dead_code,
    clippy::collapsible_if,
    clippy::for_kv_map,
    clippy::large_enum_variant,
    clippy::question_mark,
    clippy::too_many_arguments,
    clippy::type_complexity,
    clippy::unnecessary_map_or,
    clippy::useless_format
)]

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
mod cdc_service;
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

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

use parking_lot::RwLock;
use tracing::{error, info};

use cassandra_cluster_metadata::{
    ClusterMetadata, Endpoint, NodeId, NodeInfo, SimpleSnitch, SimpleStrategy, TopologyCoordinator,
    TopologyOperation, TopologyState,
};
use cassandra_common::Token;
use cassandra_coordinator::verb_handlers::register_all_verb_handlers_with_storage;
use cassandra_coordinator::{
    BatchLogManager, HintStore, ReadCoordinator, StorageProxy, StorageProxyConfig, WriteCoordinator,
};
use cassandra_messaging::MessagingService;
use cassandra_schema::SchemaCatalog;
use cassandra_storage::commitlog::CommitLogConfig;
use cassandra_storage::commitlog::encrypted::CommitLogEncryptor;
use cassandra_storage::engine::{EngineConfig, StorageEngine};

use cassandra_security::auth::PasswordAuthenticator;

use crate::executor::QueryExecutor;
use crate::resource_limits::ResourceLimits;
use crate::server::{NativeServer, ServerConfig};
use crate::shutdown::ShutdownCoordinator;
use crate::storage_service::StorageService;
use crate::transport_metrics::TransportMetrics;
use crate::transport_service::{NativeTransportConfig, NativeTransportService};
use cdc_service::{CdcFollowerServiceConfig, spawn_cdc_follower_service};

const VERSION: &str = env!("CARGO_PKG_VERSION");

struct StorageEncryptorAdapter {
    inner: Box<dyn cassandra_security::StorageEncryptor>,
}

impl std::fmt::Debug for StorageEncryptorAdapter {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StorageEncryptorAdapter").finish()
    }
}

impl CommitLogEncryptor for StorageEncryptorAdapter {
    fn encrypt_segment(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        self.inner.encrypt_segment(data).map_err(|e| e.to_string())
    }

    fn decrypt_segment(&self, data: &[u8]) -> Result<Vec<u8>, String> {
        self.inner.decrypt_segment(data).map_err(|e| e.to_string())
    }

    fn is_enabled(&self) -> bool {
        self.inner.is_enabled()
    }
}

fn tde_key_directory(
    config: &cassandra_config::config::TransparentDataEncryptionConfig,
) -> Option<String> {
    for provider in &config.key_provider {
        for params in &provider.parameters {
            if let Some(dir) = params
                .get("keys_directory")
                .and_then(|value| value.as_str())
            {
                return Some(dir.to_string());
            }
            if let Some(path) = params.get("keystore").and_then(|value| value.as_str()) {
                let path = std::path::Path::new(path);
                if path.is_dir() {
                    return Some(path.display().to_string());
                }
                if let Some(parent) = path.parent() {
                    return Some(parent.display().to_string());
                }
            }
        }
    }
    None
}

fn build_commitlog_encryptor(
    config: &cassandra_config::CassandraConfig,
) -> Option<Arc<dyn CommitLogEncryptor>> {
    let tde = config.transparent_data_encryption_options.as_ref()?;
    if !tde.enabled {
        return None;
    }

    let key_dir = match tde_key_directory(tde) {
        Some(value) => value,
        None => {
            tracing::warn!(
                "TDE is enabled but no key directory was resolved from key_provider; commitlog encryption disabled"
            );
            return None;
        }
    };

    let options = cassandra_security::TransparentDataEncryptionOptions {
        enabled: true,
        chunk_length_kb: tde.chunk_length_kb,
        cipher: tde
            .cipher
            .clone()
            .unwrap_or_else(|| "AES/CBC/PKCS5Padding".to_string()),
        key_alias: tde.key_alias.clone(),
        ..Default::default()
    };
    let key_provider = Arc::new(cassandra_security::FileKeyProvider::new(key_dir));
    let context = Arc::new(cassandra_security::EncryptionContext::new(
        options,
        Some(key_provider),
    ));
    let encryptor = cassandra_security::create_encryptor(Some(context));

    if !encryptor.is_enabled() {
        tracing::warn!("TDE context resolved to disabled encryptor; commitlog encryption disabled");
        return None;
    }

    Some(Arc::new(StorageEncryptorAdapter { inner: encryptor }))
}

fn resolve_commitlog_sync_policy(
    config: &cassandra_config::CassandraConfig,
) -> cassandra_storage::commitlog::SyncPolicy {
    let sync = config.commitlog_sync.to_lowercase();
    if sync == "batch" {
        cassandra_storage::commitlog::SyncPolicy::Batch
    } else {
        let interval_ms = config
            .commitlog_sync_period
            .map(|duration| duration.millis())
            .unwrap_or(10_000);
        cassandra_storage::commitlog::SyncPolicy::Periodic { interval_ms }
    }
}

struct LocalTopologyController {
    storage_service: Arc<StorageService>,
    transport_service: Arc<NativeTransportService>,
    shutdown_coordinator: Arc<ShutdownCoordinator>,
    cluster: Arc<ClusterMetadata>,
    topology_coordinator: Arc<TopologyCoordinator>,
    local_endpoint: Endpoint,
}

impl cassandra_admin::TopologyController for LocalTopologyController {
    fn decommission_local(&self) -> Result<cassandra_admin::DecommissionReport, String> {
        let state_before = self.storage_service.state().to_string();
        self.storage_service
            .start_leaving()
            .map_err(|e| e.to_string())?;
        let leaving_tokens = self
            .storage_service
            .leaving_tokens()
            .into_iter()
            .map(|token| token.value())
            .collect();
        self.storage_service
            .finish_leaving()
            .map_err(|e| e.to_string())?;
        let state_after = self.storage_service.state().to_string();

        Ok(cassandra_admin::DecommissionReport {
            state_before,
            state_after,
            leaving_tokens,
        })
    }

    fn move_local(&self, new_token: i64) -> Result<cassandra_admin::MoveReport, String> {
        let state_before = self.storage_service.state().to_string();
        let owned_tokens_before = self
            .storage_service
            .owned_tokens()
            .into_iter()
            .map(|token| token.value())
            .collect();

        self.storage_service
            .plan_token_migration([Token::from_raw(new_token)])
            .map_err(|e| e.to_string())?;
        self.storage_service
            .apply_token_migration()
            .map_err(|e| e.to_string())?;

        let owned_tokens_after = self
            .storage_service
            .owned_tokens()
            .into_iter()
            .map(|token| token.value())
            .collect();
        let state_after = self.storage_service.state().to_string();

        Ok(cassandra_admin::MoveReport {
            state_before,
            state_after,
            owned_tokens_before,
            owned_tokens_after,
        })
    }

    fn remove_node_local(
        &self,
        host_id: &str,
    ) -> Result<cassandra_admin::RemoveNodeReport, String> {
        let snapshot_before = self.cluster.snapshot();
        let node_count_before = snapshot_before.node_count() as u64;
        let Some((endpoint, node)) = snapshot_before
            .nodes
            .iter()
            .find(|(_, info)| info.host_id.to_string() == host_id)
            .map(|(ep, info)| (*ep, info.clone()))
        else {
            return Err(format!("node with host_id '{}' not found", host_id));
        };
        self.cluster.remove_node(&endpoint);

        let snapshot_after = self.cluster.snapshot();
        let node_count_after = snapshot_after.node_count() as u64;

        Ok(cassandra_admin::RemoveNodeReport {
            host_id: node.host_id.to_string(),
            endpoint: endpoint.to_string(),
            node_count_before,
            node_count_after,
        })
    }

    fn rebuild_local(
        &self,
        source_dc: Option<&str>,
    ) -> Result<cassandra_admin::RebuildReport, String> {
        let state_before = self.topology_coordinator.state().to_string();
        let snapshot = self.cluster.snapshot();
        let Some(local_node) = snapshot.nodes.get(&self.local_endpoint).cloned() else {
            return Err(format!(
                "local endpoint '{}' not found in cluster metadata",
                self.local_endpoint
            ));
        };

        let plan = self
            .topology_coordinator
            .begin_rebuild(&local_node, source_dc)
            .map_err(|e| e.to_string())?;
        self.topology_coordinator
            .finish_rebuild()
            .map_err(|e| e.to_string())?;
        let state_after = self.topology_coordinator.state().to_string();

        Ok(cassandra_admin::RebuildReport {
            source_dc: source_dc.map(ToOwned::to_owned),
            stream_requests: plan.requests.len() as u64,
            state_before,
            state_after,
        })
    }

    fn join_local(&self) -> Result<cassandra_admin::JoinReport, String> {
        let state_before = self.storage_service.state().to_string();

        match self.storage_service.state() {
            crate::storage_service::NodeState::Starting => {
                let snapshot = self.cluster.snapshot();
                let local_tokens = snapshot
                    .nodes
                    .get(&self.local_endpoint)
                    .map(|node| node.tokens.clone())
                    .unwrap_or_default();
                if local_tokens.is_empty() {
                    self.storage_service
                        .initialize()
                        .map_err(|e| e.to_string())?;
                } else {
                    self.storage_service
                        .initialize_with_tokens(local_tokens)
                        .map_err(|e| e.to_string())?;
                }
            }
            crate::storage_service::NodeState::Joining
            | crate::storage_service::NodeState::Normal => {}
            other => return Err(format!("cannot join from state {other}")),
        }

        if self.storage_service.state() == crate::storage_service::NodeState::Joining {
            self.storage_service
                .set_normal()
                .map_err(|e| e.to_string())?;
        }

        let state_after = self.storage_service.state().to_string();
        let pending_bootstrap_tokens = self
            .storage_service
            .pending_bootstrap_tokens()
            .into_iter()
            .map(|token| token.value())
            .collect();
        let owned_tokens = self
            .storage_service
            .owned_tokens()
            .into_iter()
            .map(|token| token.value())
            .collect();

        Ok(cassandra_admin::JoinReport {
            state_before,
            state_after,
            pending_bootstrap_tokens,
            owned_tokens,
        })
    }

    fn bootstrap_resume_local(&self) -> Result<cassandra_admin::BootstrapReport, String> {
        let state_before = self.storage_service.state().to_string();
        let topology_state_before = self.topology_coordinator.state().to_string();

        if self.storage_service.state() == crate::storage_service::NodeState::Joining {
            self.storage_service
                .set_normal()
                .map_err(|e| e.to_string())?;
        }

        if matches!(
            self.topology_coordinator.state(),
            TopologyState::Done {
                operation: TopologyOperation::Bootstrap,
                success: false,
                ..
            }
        ) {
            self.topology_coordinator
                .reset()
                .map_err(|e| e.to_string())?;
        }

        let state_after = self.storage_service.state().to_string();
        let topology_state_after = self.topology_coordinator.state().to_string();

        Ok(cassandra_admin::BootstrapReport {
            action: "resume".to_string(),
            state_before,
            state_after,
            topology_state_before,
            topology_state_after,
        })
    }

    fn abort_bootstrap_local(&self) -> Result<cassandra_admin::BootstrapReport, String> {
        let state_before = self.storage_service.state().to_string();
        let topology_state_before = self.topology_coordinator.state().to_string();

        self.topology_coordinator.abort("aborted by operator");

        let state_after = self.storage_service.state().to_string();
        let topology_state_after = self.topology_coordinator.state().to_string();

        Ok(cassandra_admin::BootstrapReport {
            action: "abort".to_string(),
            state_before,
            state_after,
            topology_state_before,
            topology_state_after,
        })
    }

    fn assassinate_endpoint_local(
        &self,
        endpoint: &str,
    ) -> Result<cassandra_admin::AssassinateReport, String> {
        let endpoint_addr: SocketAddr = endpoint
            .parse()
            .map_err(|e| format!("invalid endpoint '{}': {e}", endpoint))?;
        let endpoint = Endpoint::new(endpoint_addr);
        let snapshot_before = self.cluster.snapshot();
        let node_count_before = snapshot_before.node_count() as u64;
        let removed = snapshot_before.nodes.contains_key(&endpoint);

        if removed {
            self.cluster.remove_node(&endpoint);
        }

        let snapshot_after = self.cluster.snapshot();
        let node_count_after = snapshot_after.node_count() as u64;

        Ok(cassandra_admin::AssassinateReport {
            endpoint: endpoint.to_string(),
            node_count_before,
            node_count_after,
            removed,
        })
    }

    fn topology_status_local(&self) -> Result<cassandra_admin::TopologyStatusReport, String> {
        let status = self.storage_service.state().to_string();
        let current_operation = match self.topology_coordinator.state() {
            TopologyState::Idle => None,
            TopologyState::Calculating { operation } => {
                Some(cassandra_admin::CurrentTopologyOperation {
                    operation_type: operation.to_string(),
                    status: "CALCULATING".to_string(),
                    progress: 0.0,
                    operation_id: None,
                })
            }
            TopologyState::Streaming {
                operation,
                progress,
                ..
            } => Some(cassandra_admin::CurrentTopologyOperation {
                operation_type: operation.to_string(),
                status: "STREAMING".to_string(),
                progress: (progress as f64) / 100.0,
                operation_id: None,
            }),
            TopologyState::Completing { operation } => {
                Some(cassandra_admin::CurrentTopologyOperation {
                    operation_type: operation.to_string(),
                    status: "COMPLETING".to_string(),
                    progress: 1.0,
                    operation_id: None,
                })
            }
            TopologyState::Done {
                operation, success, ..
            } => Some(cassandra_admin::CurrentTopologyOperation {
                operation_type: operation.to_string(),
                status: if success { "DONE" } else { "FAILED" }.to_string(),
                progress: 1.0,
                operation_id: None,
            }),
        };

        let snapshot = self.cluster.snapshot();
        let mut nodes = snapshot
            .nodes
            .values()
            .map(|node| cassandra_admin::TopologyNodeStatus {
                host_id: node.host_id.to_string(),
                address: node.endpoint.to_string(),
                state: node.state.to_string(),
                status: if node.state.is_live() {
                    "UP".to_string()
                } else {
                    "DOWN".to_string()
                },
            })
            .collect::<Vec<_>>();
        nodes.sort_by(|a, b| a.address.cmp(&b.address));

        Ok(cassandra_admin::TopologyStatusReport {
            status,
            current_operation,
            nodes,
        })
    }

    fn netstats_local(&self) -> Result<cassandra_admin::NetstatsReport, String> {
        let local = self.local_endpoint;
        let pending_ranges = self.topology_coordinator.pending_ranges();
        let mut receiving_by_peer: HashMap<String, u64> = HashMap::new();
        let mut sending_by_peer: HashMap<String, u64> = HashMap::new();
        for ranges in pending_ranges.ranges.values() {
            for pending in ranges {
                if pending.new_owner == local {
                    *receiving_by_peer
                        .entry(pending.current_owner.to_string())
                        .or_insert(0) += 1;
                }
                if pending.current_owner == local {
                    *sending_by_peer
                        .entry(pending.new_owner.to_string())
                        .or_insert(0) += 1;
                }
            }
        }

        let mut receiving = receiving_by_peer
            .into_iter()
            .map(|(peer, files)| cassandra_admin::StreamPeerStat {
                peer,
                files,
                bytes: 0,
            })
            .collect::<Vec<_>>();
        receiving.sort_by(|a, b| a.peer.cmp(&b.peer));

        let mut sending = sending_by_peer
            .into_iter()
            .map(|(peer, files)| cassandra_admin::StreamPeerStat {
                peer,
                files,
                bytes: 0,
            })
            .collect::<Vec<_>>();
        sending.sort_by(|a, b| a.peer.cmp(&b.peer));

        let commands = HashMap::from([(
            "STREAM".to_string(),
            cassandra_admin::CommandQueueStat {
                pending: pending_ranges.total_count() as u64,
                completed: 0,
            },
        )]);

        Ok(cassandra_admin::NetstatsReport {
            mode: self.topology_coordinator.state().to_string(),
            receiving,
            sending,
            commands,
        })
    }

    fn stop_daemon_local(&self) -> Result<cassandra_admin::StopDaemonReport, String> {
        let storage_state_before = self.storage_service.state().to_string();
        let transport_state_before = self.transport_service.state().to_string();

        if self.storage_service.state() == crate::storage_service::NodeState::Normal {
            self.storage_service
                .start_leaving()
                .map_err(|e| e.to_string())?;
        }

        if self.transport_service.state() == crate::transport_service::ServiceState::Started {
            self.transport_service
                .begin_stop()
                .map_err(|e| e.to_string())?;
        }

        self.shutdown_coordinator.signal_shutdown();

        Ok(cassandra_admin::StopDaemonReport {
            shutdown_signalled: self.shutdown_coordinator.is_shutting_down(),
            storage_state_before,
            storage_state_after: self.storage_service.state().to_string(),
            transport_state_before,
            transport_state_after: self.transport_service.state().to_string(),
            in_flight_requests: self.shutdown_coordinator.in_flight_count(),
        })
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // 1. Tracing
    tracing_subscriber::fmt::init();

    info!("cassandra-server v{VERSION} starting");

    // 2. Load CassandraConfig early (used for storage + TLS + logging setup)
    let config_path =
        std::env::var("CASSANDRA_CONFIG").unwrap_or_else(|_| "conf/cassandra.yaml".to_string());
    let cassandra_config = cassandra_config::load_config(std::path::Path::new(&config_path))
        .unwrap_or_else(|e| {
            tracing::warn!("Failed to parse config from {}: {}", config_path, e);
            cassandra_config::CassandraConfig::default()
        });
    let commitlog_encryptor = build_commitlog_encryptor(&cassandra_config);

    // 3. Storage paths and commitlog config derived from cassandra.yaml
    let data_directories: Vec<PathBuf> = cassandra_config
        .data_file_directories
        .clone()
        .unwrap_or_default()
        .into_iter()
        .map(PathBuf::from)
        .collect();
    let data_directories = if data_directories.is_empty() {
        vec![PathBuf::from("data")]
    } else {
        data_directories
    };

    let primary_data_dir = data_directories
        .first()
        .cloned()
        .unwrap_or_else(|| PathBuf::from("data"));
    let commitlog_dir = cassandra_config
        .commitlog_directory
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(|| primary_data_dir.join("commitlog"));
    let cdc_raw_dir = cassandra_config
        .cdc_raw_directory
        .as_ref()
        .map(PathBuf::from)
        .unwrap_or_else(|| primary_data_dir.join("cdc_raw"));

    let commitlog_sync_policy = resolve_commitlog_sync_policy(&cassandra_config);
    let commitlog_segment_size = cassandra_config
        .commitlog_segment_size
        .map(|size| size.bytes())
        .unwrap_or(32 * 1024 * 1024);
    let commitlog_compression_enabled = cassandra_config
        .commitlog_compression
        .as_ref()
        .map(|compressors| !compressors.is_empty())
        .unwrap_or(false);
    let cdc_enabled = cassandra_config.cdc_enabled.unwrap_or(false);
    let cdc_total_space = cassandra_config.cdc_total_space.bytes();
    let cdc_follower_encryptor = commitlog_encryptor.clone();
    let cdc_follower_raw_dir = cdc_raw_dir.clone();

    // 4. Open storage engine
    let engine_config = EngineConfig {
        data_directories: data_directories.clone(),
        commitlog: CommitLogConfig {
            max_segment_size: commitlog_segment_size,
            sync_policy: commitlog_sync_policy,
            directory: commitlog_dir,
            compression_enabled: commitlog_compression_enabled,
            cdc: cassandra_storage::commitlog::CdcConfig {
                enabled: cdc_enabled,
                raw_directory: cdc_raw_dir,
                size_limit: cdc_total_space,
            },
            encryptor: commitlog_encryptor,
            ..CommitLogConfig::default()
        },
        memtable_flush_threshold: 128 * 1024 * 1024, // 128 MiB
        gc_grace_seconds: 864_000,                   // 10 days
        ..EngineConfig::default()
    };

    let engine =
        Arc::new(StorageEngine::open(engine_config).expect("Failed to open storage engine"));

    // 5. Schema catalog
    let catalog = Arc::new(RwLock::new(SchemaCatalog::new()));

    // 6. Commit log replay
    let replayed = engine.replay_commitlog().expect("Commit log replay failed");
    info!(replayed, "Commit log replay complete");

    // 7. Security & Auth Managers
    let role_manager = Arc::new(auth::SystemAuthRoleManager::new(Arc::clone(&engine)));
    let authorizer = Arc::new(auth::SystemAuthAuthorizer::new(
        Arc::clone(&engine),
        Arc::clone(&role_manager) as Arc<dyn cassandra_security::RoleManager>,
    ));

    // 8. TLS configuration
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

    // 9. FQL and Audit Logger initialization
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

    // 10. Build read coordinator bridge and executor
    let internode_addr = format!(
        "{}:{}",
        cassandra_config
            .listen_address
            .as_deref()
            .unwrap_or("127.0.0.1"),
        cassandra_config.storage_port
    )
    .parse()
    .unwrap_or_else(|_| {
        std::net::SocketAddr::from(([127, 0, 0, 1], cassandra_config.storage_port))
    });
    let endpoint = Endpoint::new(internode_addr);
    let node = NodeInfo::new(
        NodeId::random(),
        endpoint,
        "dc1",
        "rack1",
        vec![Token::from_raw(0)],
    );
    let storage_service = Arc::new(StorageService::new());
    storage_service
        .initialize_with_tokens([Token::from_raw(0)])
        .map_err(|e| anyhow::anyhow!(e))?;
    storage_service
        .set_normal()
        .map_err(|e| anyhow::anyhow!(e))?;

    let cluster = Arc::new(ClusterMetadata::new(node));
    let hint_store = Arc::new(HintStore::new(10_000));
    let write_coordinator = Arc::new(WriteCoordinator::new(
        Arc::clone(&cluster),
        endpoint,
        Arc::clone(&hint_store),
    ));
    let read_coordinator = Arc::new(ReadCoordinator::new(Arc::clone(&cluster), endpoint));
    let messaging = Arc::new(MessagingService::new(internode_addr));
    let batch_log = Arc::new(BatchLogManager::new());
    let storage_proxy = Arc::new(StorageProxy::new(
        write_coordinator,
        read_coordinator,
        Arc::clone(&messaging),
        hint_store,
        batch_log,
        StorageProxyConfig {
            listen_address: internode_addr,
            ..StorageProxyConfig::default()
        },
    ));
    register_all_verb_handlers_with_storage(storage_proxy.messaging(), Arc::clone(&engine));
    let replication_strategy: Arc<dyn cassandra_cluster_metadata::ReplicationStrategy> =
        Arc::new(SimpleStrategy::new(1));
    let endpoint_snitch: Arc<dyn cassandra_cluster_metadata::Snitch> = Arc::new(SimpleSnitch);
    let select_read_sink = crate::executor::storage_proxy_select_read_sink(
        Arc::clone(&storage_proxy),
        Arc::clone(&replication_strategy),
        Arc::clone(&endpoint_snitch),
    );
    let batch_mutation_sink = crate::executor::storage_proxy_batch_mutation_sink(
        Arc::clone(&storage_proxy),
        Arc::clone(&replication_strategy),
        Arc::clone(&endpoint_snitch),
    );
    let mutation_sink = crate::executor::storage_proxy_mutation_sink(
        Arc::clone(&storage_proxy),
        Arc::clone(&replication_strategy),
        Arc::clone(&endpoint_snitch),
    );

    let executor = Arc::new(
        QueryExecutor::new(
            Arc::clone(&engine),
            Arc::clone(&catalog),
            Arc::clone(&role_manager) as Arc<dyn cassandra_security::RoleManager>,
            Arc::clone(&authorizer) as Arc<dyn cassandra_security::Authorizer>,
        )
        .with_select_read_sink(select_read_sink)
        .with_mutation_sink(mutation_sink)
        .with_batch_mutation_sink(batch_mutation_sink),
    );

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
        max_frame_size: nt_config.max_frame_size,
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

    // 13. Start admin HTTP API alongside native transport.
    let admin_bind_addr = format!(
        "{}:{}",
        cassandra_config
            .listen_address
            .as_deref()
            .unwrap_or("127.0.0.1"),
        cassandra_config.admin_port
    )
    .parse()
    .unwrap_or_else(|_| std::net::SocketAddr::from(([127, 0, 0, 1], cassandra_config.admin_port)));
    let topology_coordinator = Arc::new(TopologyCoordinator::new(Arc::clone(&cluster)));
    let admin_state = Arc::new(cassandra_admin::AdminState {
        metrics: Arc::new(cassandra_admin::MetricsRegistry::new()),
        operations: Arc::new(cassandra_admin::OperationTracker::new()),
        virtual_tables: Arc::new(cassandra_admin::VirtualTableRegistry::default()),
        repair_coordinator: None,
        storage_engine: Some(Arc::clone(&engine)),
        schema_catalog: Some(Arc::clone(&catalog)),
        topology_controller: Some(Arc::new(LocalTopologyController {
            storage_service: Arc::clone(&storage_service),
            transport_service: Arc::clone(&transport_service),
            shutdown_coordinator: Arc::clone(&shutdown_coordinator),
            cluster: Arc::clone(&cluster),
            topology_coordinator: Arc::clone(&topology_coordinator),
            local_endpoint: endpoint,
        })),
    });
    let admin_task = tokio::spawn({
        let admin_state = Arc::clone(&admin_state);
        async move {
            if let Err(e) = cassandra_admin::start_admin_server(admin_bind_addr, admin_state).await
            {
                error!(error = %e, "Admin HTTP server exited");
            }
        }
    });

    let cdc_follower_task = spawn_cdc_follower_service(
        CdcFollowerServiceConfig::from_runtime(cdc_enabled, cdc_follower_raw_dir),
        Arc::clone(&shutdown_coordinator),
        cdc_follower_encryptor,
    );

    transport_service.start().map_err(|e| anyhow::anyhow!(e))?;
    storage_service
        .start_native_transport()
        .map_err(|e| anyhow::anyhow!(e))?;
    info!("cassandra-server v{VERSION} ready");

    // 14. Install signal handler for graceful shutdown
    let shutdown_for_signal = Arc::clone(&shutdown_coordinator);
    tokio::spawn(async move {
        shutdown::signal_handler(shutdown_for_signal).await;
    });

    // Block on the server
    server.run().await?;

    // Drain and stop
    storage_service.start_leaving().ok();
    transport_service.begin_stop().ok();
    shutdown_coordinator.drain().await;
    storage_service.finish_leaving().ok();
    admin_task.abort();
    if let Some(task) = cdc_follower_task {
        let _ = tokio::time::timeout(std::time::Duration::from_secs(2), task).await;
    }
    transport_service.finish_stop().ok();
    info!("cassandra-server v{VERSION} shut down");

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_config::config::{ClassWithListParameters, TransparentDataEncryptionConfig};

    #[test]
    fn resolve_commitlog_sync_policy_batch() {
        let mut config = cassandra_config::CassandraConfig::default();
        config.commitlog_sync = "batch".to_string();
        assert_eq!(
            resolve_commitlog_sync_policy(&config),
            cassandra_storage::commitlog::SyncPolicy::Batch
        );
    }

    #[test]
    fn resolve_commitlog_sync_policy_periodic_with_configured_interval() {
        let mut config = cassandra_config::CassandraConfig::default();
        config.commitlog_sync = "periodic".to_string();
        config.commitlog_sync_period = Some(cassandra_config::Duration::from_millis(2500));
        assert_eq!(
            resolve_commitlog_sync_policy(&config),
            cassandra_storage::commitlog::SyncPolicy::Periodic { interval_ms: 2500 }
        );
    }

    #[test]
    fn resolve_commitlog_sync_policy_periodic_default_interval() {
        let mut config = cassandra_config::CassandraConfig::default();
        config.commitlog_sync = "periodic".to_string();
        config.commitlog_sync_period = None;
        assert_eq!(
            resolve_commitlog_sync_policy(&config),
            cassandra_storage::commitlog::SyncPolicy::Periodic {
                interval_ms: 10_000
            }
        );
    }

    #[test]
    fn tde_key_directory_prefers_keys_directory_parameter() {
        let tde = TransparentDataEncryptionConfig {
            enabled: true,
            chunk_length_kb: 64,
            cipher: Some("AES/CBC/PKCS5Padding".to_string()),
            key_alias: Some("testing:1".to_string()),
            iv_length: Some(16),
            key_provider: vec![ClassWithListParameters {
                class_name: "org.apache.cassandra.security.JKSKeyProvider".to_string(),
                parameters: vec![std::collections::HashMap::from([(
                    "keys_directory".to_string(),
                    serde_yaml::Value::String("/var/lib/cassandra/keys".to_string()),
                )])],
            }],
        };

        assert_eq!(
            tde_key_directory(&tde).as_deref(),
            Some("/var/lib/cassandra/keys")
        );
    }

    #[test]
    fn tde_key_directory_falls_back_to_keystore_parent() {
        let tde = TransparentDataEncryptionConfig {
            enabled: true,
            chunk_length_kb: 64,
            cipher: Some("AES/CBC/PKCS5Padding".to_string()),
            key_alias: Some("testing:1".to_string()),
            iv_length: Some(16),
            key_provider: vec![ClassWithListParameters {
                class_name: "org.apache.cassandra.security.JKSKeyProvider".to_string(),
                parameters: vec![std::collections::HashMap::from([(
                    "keystore".to_string(),
                    serde_yaml::Value::String("conf/.keystore".to_string()),
                )])],
            }],
        };

        assert_eq!(tde_key_directory(&tde).as_deref(), Some("conf"));
    }
}
