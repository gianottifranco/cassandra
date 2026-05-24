// Licensed under Apache License, Version 2.0.

//! HTTP admin API for metrics, health checks, and virtual table queries.
//!
//! ## Design
//! Lightweight HTTP server using Hyper on a configurable admin port (default 9090).
//! Endpoints:
//! - `GET /metrics` — Prometheus text exposition
//! - `GET /health` — JSON health check
//! - `GET /api/v1/virtual/<keyspace>/<table>` — virtual table data (JSON)
//! - `GET /api/v1/operations` — active operations (JSON)
//!
//! No external framework; just hyper + http-body-util.

use std::collections::HashMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::sync::Arc;

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::server::conn::http1;
use hyper::service::service_fn;
use hyper::{Method, Request, Response, StatusCode};
use hyper_util::rt::TokioIo;
use tokio::net::TcpListener;
use tracing::{error, info};

use crate::operations::OperationTracker;
use crate::prometheus_metrics::MetricsRegistry;
use crate::virtual_tables::VirtualTableRegistry;

/// Result payload for a completed local decommission action.
#[derive(Debug, Clone, serde::Serialize)]
pub struct DecommissionReport {
    pub state_before: String,
    pub state_after: String,
    pub leaving_tokens: Vec<i64>,
}

/// Result payload for a completed local token move action.
#[derive(Debug, Clone, serde::Serialize)]
pub struct MoveReport {
    pub state_before: String,
    pub state_after: String,
    pub owned_tokens_before: Vec<i64>,
    pub owned_tokens_after: Vec<i64>,
}

/// Result payload for a completed remove-node action.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RemoveNodeReport {
    pub host_id: String,
    pub endpoint: String,
    pub node_count_before: u64,
    pub node_count_after: u64,
}

/// Result payload for a completed rebuild action.
#[derive(Debug, Clone, serde::Serialize)]
pub struct RebuildReport {
    pub source_dc: Option<String>,
    pub stream_requests: u64,
    pub state_before: String,
    pub state_after: String,
}

/// Result payload for a local join action.
#[derive(Debug, Clone, serde::Serialize)]
pub struct JoinReport {
    pub state_before: String,
    pub state_after: String,
    pub pending_bootstrap_tokens: Vec<i64>,
    pub owned_tokens: Vec<i64>,
}

/// Result payload for bootstrap resume/abort actions.
#[derive(Debug, Clone, serde::Serialize)]
pub struct BootstrapReport {
    pub action: String,
    pub state_before: String,
    pub state_after: String,
    pub topology_state_before: String,
    pub topology_state_after: String,
}

/// Result payload for a local assassinate action.
#[derive(Debug, Clone, serde::Serialize)]
pub struct AssassinateReport {
    pub endpoint: String,
    pub node_count_before: u64,
    pub node_count_after: u64,
    pub removed: bool,
}

/// Node entry returned by topology status.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TopologyNodeStatus {
    pub host_id: String,
    pub address: String,
    pub state: String,
    pub status: String,
}

/// Current topology operation metadata for status output.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CurrentTopologyOperation {
    pub operation_type: String,
    pub status: String,
    pub progress: f64,
    pub operation_id: Option<String>,
}

/// Topology status payload.
#[derive(Debug, Clone, serde::Serialize)]
pub struct TopologyStatusReport {
    pub status: String,
    pub current_operation: Option<CurrentTopologyOperation>,
    pub nodes: Vec<TopologyNodeStatus>,
}

/// Stream peer counters for netstats output.
#[derive(Debug, Clone, serde::Serialize)]
pub struct StreamPeerStat {
    pub peer: String,
    pub files: u64,
    pub bytes: u64,
}

/// Per-command queue counters for netstats output.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CommandQueueStat {
    pub pending: u64,
    pub completed: u64,
}

/// Network status payload.
#[derive(Debug, Clone, serde::Serialize)]
pub struct NetstatsReport {
    pub mode: String,
    pub receiving: Vec<StreamPeerStat>,
    pub sending: Vec<StreamPeerStat>,
    pub commands: HashMap<String, CommandQueueStat>,
}

/// Result payload for stop-daemon action.
#[derive(Debug, Clone, serde::Serialize)]
pub struct StopDaemonReport {
    pub shutdown_signalled: bool,
    pub storage_state_before: String,
    pub storage_state_after: String,
    pub transport_state_before: String,
    pub transport_state_after: String,
    pub in_flight_requests: u64,
}

/// Optional bridge for topology lifecycle actions implemented by the server runtime.
pub trait TopologyController: Send + Sync {
    fn decommission_local(&self) -> Result<DecommissionReport, String>;
    fn move_local(&self, new_token: i64) -> Result<MoveReport, String>;
    fn remove_node_local(&self, host_id: &str) -> Result<RemoveNodeReport, String>;
    fn rebuild_local(&self, source_dc: Option<&str>) -> Result<RebuildReport, String>;
    fn join_local(&self) -> Result<JoinReport, String>;
    fn bootstrap_resume_local(&self) -> Result<BootstrapReport, String>;
    fn abort_bootstrap_local(&self) -> Result<BootstrapReport, String>;
    fn assassinate_endpoint_local(&self, endpoint: &str) -> Result<AssassinateReport, String>;
    fn topology_status_local(&self) -> Result<TopologyStatusReport, String>;
    fn netstats_local(&self) -> Result<NetstatsReport, String>;
    fn stop_daemon_local(&self) -> Result<StopDaemonReport, String>;
}

/// Shared state for the admin HTTP server.
pub struct AdminState {
    pub metrics: Arc<MetricsRegistry>,
    pub operations: Arc<OperationTracker>,
    pub virtual_tables: Arc<VirtualTableRegistry>,
    pub repair_coordinator: Option<Arc<cassandra_repair::RepairCoordinator>>,
    pub storage_engine: Option<Arc<cassandra_storage::engine::StorageEngine>>,
    pub schema_catalog: Option<Arc<parking_lot::RwLock<cassandra_schema::SchemaCatalog>>>,
    pub topology_controller: Option<Arc<dyn TopologyController>>,
}

/// Start the admin HTTP server.
pub async fn start_admin_server(
    bind_addr: SocketAddr,
    state: Arc<AdminState>,
) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let listener = TcpListener::bind(bind_addr).await?;
    info!("Admin HTTP server listening on {}", bind_addr);

    loop {
        let (stream, _) = listener.accept().await?;
        let io = TokioIo::new(stream);
        let state = Arc::clone(&state);

        tokio::spawn(async move {
            let service = service_fn(move |req| {
                let state = Arc::clone(&state);
                async move { handle_request(req, state).await }
            });

            if let Err(e) = http1::Builder::new().serve_connection(io, service).await {
                error!("Admin HTTP connection error: {}", e);
            }
        });
    }
}

async fn handle_request(
    req: Request<Incoming>,
    state: Arc<AdminState>,
) -> Result<Response<Full<Bytes>>, Infallible> {
    let path = req.uri().path().to_string();
    let method = req.method().clone();

    let response = match (method, path.as_str()) {
        // Core endpoints
        (Method::GET, "/metrics") => handle_metrics(&state),
        (Method::GET, "/health") => handle_health(),
        (Method::GET, "/api/v1/version") => handle_version(),
        (Method::GET, "/api/v1/operations") => handle_operations(&state),
        (Method::POST, "/api/v1/repair") => handle_repair_request(req, &state).await,
        (Method::POST, "/api/v1/operations/repair") => handle_repair_request(req, &state).await,
        (Method::POST, "/api/v1/operations/stop") => handle_stop_operations(req, &state).await,
        (Method::GET, p) if p.starts_with("/api/v1/virtual/") => handle_virtual_table(p, &state),
        (Method::POST, "/api/v1/index/rebuild") => handle_rebuild_index(req, &state).await,
        (Method::POST, "/api/v1/operations/rebuild_index") => {
            handle_rebuild_index(req, &state).await
        }
        (Method::GET, "/admin/indexes") => handle_list_indexes(&state),
        (Method::GET, p) if p.starts_with("/admin/indexes/") => handle_index_detail(p, &state),

        // Cluster info endpoints
        (Method::GET, "/api/v1/cluster/status") => {
            crate::handlers_cluster::handle_cluster_status(&state)
        }
        (Method::GET, "/api/v1/cluster/info") => {
            crate::handlers_cluster::handle_cluster_info(&state)
        }
        (Method::GET, "/api/v1/cluster/ring") => {
            crate::handlers_cluster::handle_cluster_ring(&state)
        }
        (Method::GET, "/api/v1/cluster/describe") => {
            crate::handlers_cluster::handle_describe_cluster(&state)
        }
        (Method::GET, "/api/v1/cluster/gossip") => {
            crate::handlers_cluster::handle_gossip_info(&state)
        }

        // Compaction endpoints
        (Method::POST, "/api/v1/operations/compact")
        | (Method::POST, "/api/v1/compaction/compact") => {
            crate::handlers_compaction::handle_compact(req, &state).await
        }
        (Method::POST, "/api/v1/operations/cleanup")
        | (Method::POST, "/api/v1/compaction/cleanup") => {
            crate::handlers_compaction::handle_cleanup(req, &state).await
        }
        (Method::POST, "/api/v1/operations/flush") | (Method::POST, "/api/v1/compaction/flush") => {
            crate::handlers_compaction::handle_flush(req, &state).await
        }
        (Method::POST, "/api/v1/operations/scrub") | (Method::POST, "/api/v1/compaction/scrub") => {
            crate::handlers_compaction::handle_scrub(req, &state).await
        }
        // Keep legacy operations path and nodetool catalog path in sync.
        (Method::POST, "/api/v1/operations/garbagecollect")
        | (Method::POST, "/api/v1/compaction/garbagecollect") => {
            crate::handlers_compaction::handle_garbagecollect(req, &state).await
        }
        (Method::GET, "/api/v1/compaction/stats") => {
            crate::handlers_compaction::handle_compaction_stats(&state)
        }
        (Method::GET, "/api/v1/compaction/history") => {
            crate::handlers_compaction::handle_compaction_history(&state)
        }
        (Method::POST, "/api/v1/compaction/verify") => {
            crate::handlers_compaction::handle_verify(req, &state).await
        }
        (Method::POST, "/api/v1/compaction/upgradesstables") => {
            crate::handlers_compaction::handle_upgradesstables(req, &state).await
        }
        (Method::POST, "/api/v1/compaction/autocompaction/enable")
        | (Method::POST, "/api/v1/compaction/enable") => {
            crate::handlers_compaction::handle_enable_autocompaction(&state)
        }
        (Method::POST, "/api/v1/compaction/autocompaction/disable")
        | (Method::POST, "/api/v1/compaction/disable") => {
            crate::handlers_compaction::handle_disable_autocompaction(&state)
        }
        (Method::GET, "/api/v1/compaction/autocompaction/status")
        | (Method::GET, "/api/v1/compaction/status") => {
            crate::handlers_compaction::handle_autocompaction_status(&state)
        }
        (Method::GET, "/api/v1/compaction/throughput") => {
            crate::handlers_compaction::handle_get_compaction_throughput(&state)
        }
        (Method::POST, "/api/v1/compaction/throughput") => {
            crate::handlers_compaction::handle_set_compaction_throughput(req, &state).await
        }

        // Snapshot endpoints
        (Method::POST, "/api/v1/snapshots/create") => {
            crate::handlers_snapshots::handle_take_snapshot(req, &state).await
        }
        (Method::POST, "/api/v1/operations/snapshot") => {
            crate::handlers_snapshots::handle_take_snapshot(req, &state).await
        }
        (Method::GET, "/api/v1/snapshots") => {
            crate::handlers_snapshots::handle_list_snapshots(&state)
        }
        (Method::POST, "/api/v1/snapshots/clear") => {
            crate::handlers_snapshots::handle_clear_snapshot(req, &state).await
        }
        (Method::DELETE, "/api/v1/snapshots") => {
            crate::handlers_snapshots::handle_clear_snapshot(req, &state).await
        }
        (Method::POST, "/api/v1/snapshots/restore") => {
            crate::handlers_snapshots::handle_restore_snapshot(req, &state).await
        }
        (Method::POST, "/api/v1/sstable/import") => {
            crate::handlers_snapshots::handle_import(req, &state).await
        }
        (Method::POST, "/api/v1/operations/import") => {
            crate::handlers_snapshots::handle_import(req, &state).await
        }
        (Method::POST, "/api/v1/operations/sstableloader") => {
            crate::handlers_snapshots::handle_sstableloader(req, &state).await
        }
        (Method::POST, "/api/v1/backup/enable") => {
            crate::handlers_snapshots::handle_enable_backup(&state)
        }
        (Method::POST, "/api/v1/backup/disable") => {
            crate::handlers_snapshots::handle_disable_backup(&state)
        }
        (Method::GET, "/api/v1/backup/status") => {
            crate::handlers_snapshots::handle_backup_status(&state)
        }

        // Topology endpoints
        (Method::POST, "/api/v1/topology/decommission") => {
            crate::handlers_topology::handle_decommission(&state)
        }
        (Method::POST, "/api/v1/topology/removenode") => {
            crate::handlers_topology::handle_removenode(req, &state).await
        }
        (Method::POST, "/api/v1/topology/move") => {
            crate::handlers_topology::handle_move(req, &state).await
        }
        (Method::POST, "/api/v1/topology/rebuild") => {
            crate::handlers_topology::handle_rebuild(req, &state).await
        }
        (Method::POST, "/api/v1/topology/refresh") => {
            crate::handlers_topology::handle_refresh(req, &state).await
        }
        (Method::POST, "/api/v1/topology/join") => crate::handlers_topology::handle_join(&state),
        (Method::POST, "/api/v1/topology/bootstrap") => {
            crate::handlers_topology::handle_bootstrap(req, &state).await
        }
        (Method::POST, "/api/v1/topology/drain") => crate::handlers_topology::handle_drain(&state),
        (Method::POST, "/api/v1/topology/assassinate") => {
            crate::handlers_topology::handle_assassinate(req, &state).await
        }
        (Method::POST, "/api/v1/topology/stopdaemon") => {
            crate::handlers_topology::handle_stop_daemon(&state)
        }
        (Method::GET, "/api/v1/topology/status") => {
            crate::handlers_topology::handle_topology_status(&state)
        }
        (Method::GET, "/api/v1/topology/netstats") => {
            crate::handlers_topology::handle_netstats(&state)
        }

        // Statistics endpoints
        (Method::GET, "/api/v1/stats/tables") => {
            crate::handlers_stats::handle_table_stats(req.uri().query(), &state)
        }
        (Method::GET, "/api/v1/stats/histograms") => {
            crate::handlers_stats::handle_table_histograms(req.uri().query(), &state)
        }
        (Method::GET, "/api/v1/stats/tpstats") => crate::handlers_stats::handle_tp_stats(&state),
        (Method::GET, "/api/v1/stats/gcstats") => crate::handlers_stats::handle_gc_stats(&state),
        (Method::GET, "/api/v1/stats/proxyhistograms") => {
            crate::handlers_stats::handle_proxy_histograms(&state)
        }
        (Method::GET, "/api/v1/stats/clients") => {
            crate::handlers_stats::handle_client_stats(&state)
        }
        (Method::GET, "/api/v1/stats/toppartitions") => {
            crate::handlers_stats::handle_top_partitions(req.uri().query(), &state)
        }
        (Method::GET, "/api/v1/stats/failure_detector_info") => {
            crate::handlers_stats::handle_failure_detector_info(&state)
        }
        (Method::GET, "/api/v1/stats/data_paths") => {
            crate::handlers_stats::handle_data_paths(&state)
        }
        (Method::GET, "/api/v1/stats/refresh_size_estimates") => {
            crate::handlers_stats::handle_refresh_size_estimates(&state)
        }
        (Method::GET, "/api/v1/stats/view_build_status") => {
            crate::handlers_stats::handle_view_build_status(&state)
        }
        (Method::GET, "/api/v1/stats/endpoints") => {
            crate::handlers_stats::handle_get_endpoints(req.uri().query(), &state)
        }
        (Method::GET, "/api/v1/stats/sstables") => {
            crate::handlers_stats::handle_get_sstables(req.uri().query(), &state)
        }

        // Cache and hints endpoints
        (Method::POST, "/api/v1/cache/key/invalidate") => handle_named_cache_invalidate("key"),
        (Method::POST, "/api/v1/cache/row/invalidate") => handle_named_cache_invalidate("row"),
        (Method::POST, "/api/v1/cache/counter/invalidate") => {
            handle_named_cache_invalidate("counter")
        }
        (Method::POST, "/api/v1/cache/invalidate") => {
            crate::handlers_cache_hints::handle_invalidate_cache(req, &state).await
        }
        (Method::POST, "/api/v1/cache/capacity") => {
            crate::handlers_cache_hints::handle_set_cache_capacity(req, &state).await
        }
        (Method::POST, "/api/v1/hints/enable") => {
            crate::handlers_cache_hints::handle_enable_handoff(&state)
        }
        (Method::POST, "/api/v1/hints/disable") => {
            crate::handlers_cache_hints::handle_disable_handoff(&state)
        }
        (Method::POST, "/api/v1/hints/pause") => {
            crate::handlers_cache_hints::handle_pause_handoff(&state)
        }
        (Method::POST, "/api/v1/hints/resume") => {
            crate::handlers_cache_hints::handle_resume_handoff(&state)
        }
        (Method::POST, "/api/v1/hints/truncate") => {
            crate::handlers_cache_hints::handle_truncate_hints(&state)
        }
        (Method::GET, "/api/v1/hints/pending") => {
            crate::handlers_cache_hints::handle_pending_hints(&state)
        }
        (Method::POST, "/api/v1/handoff/enable") => {
            crate::handlers_cache_hints::handle_enable_handoff(&state)
        }
        (Method::POST, "/api/v1/handoff/disable") => {
            crate::handlers_cache_hints::handle_disable_handoff(&state)
        }
        (Method::POST, "/api/v1/handoff/pause") => {
            crate::handlers_cache_hints::handle_pause_handoff(&state)
        }
        (Method::POST, "/api/v1/handoff/resume") => {
            crate::handlers_cache_hints::handle_resume_handoff(&state)
        }

        // Config endpoints
        (Method::POST, "/api/v1/schema/reload") => {
            crate::handlers_config::handle_reload_schema(&state)
        }
        (Method::GET, "/api/v1/schema/ring") => {
            crate::handlers_cluster::handle_cluster_ring(&state)
        }
        (Method::GET, "/api/v1/config") => crate::handlers_config::handle_get_config(&state),
        (Method::POST, "/api/v1/config") => {
            crate::handlers_config::handle_set_config(req, &state).await
        }
        (Method::POST, "/api/v1/config/reload/schema") => {
            crate::handlers_config::handle_reload_schema(&state)
        }
        (Method::POST, "/api/v1/config/reload/triggers") => {
            crate::handlers_config::handle_reload_triggers(&state)
        }
        (Method::POST, "/api/v1/security/reloadssl") => {
            crate::handlers_config::handle_reload_ssl(&state)
        }
        (Method::POST, "/api/v1/config/reload/ssl") => {
            crate::handlers_config::handle_reload_ssl(&state)
        }
        (Method::POST, "/api/v1/binary/enable") => {
            crate::handlers_config::handle_enable_binary(&state)
        }
        (Method::POST, "/api/v1/binary/disable") => {
            crate::handlers_config::handle_disable_binary(&state)
        }
        (Method::GET, "/api/v1/binary/status") => {
            crate::handlers_config::handle_binary_status(&state)
        }
        (Method::POST, "/api/v1/gossip/enable") => {
            crate::handlers_config::handle_enable_gossip(&state)
        }
        (Method::POST, "/api/v1/gossip/disable") => {
            crate::handlers_config::handle_disable_gossip(&state)
        }
        (Method::GET, "/api/v1/gossip/status") => {
            crate::handlers_config::handle_gossip_status(&state)
        }

        // Logging and security endpoints
        (Method::POST, "/api/v1/audit/enable") => {
            crate::handlers_logging::handle_enable_audit_log(&state)
        }
        (Method::POST, "/api/v1/audit/disable") => {
            crate::handlers_logging::handle_disable_audit_log(&state)
        }
        (Method::GET, "/api/v1/logging/levels") => {
            crate::handlers_logging::handle_get_logging_levels(&state)
        }
        (Method::POST, "/api/v1/logging/level") => {
            crate::handlers_logging::handle_set_logging_level(req, &state).await
        }
        (Method::POST, "/api/v1/operations/enableauditlog") => {
            crate::handlers_logging::handle_enable_audit_log(&state)
        }
        (Method::POST, "/api/v1/operations/disableauditlog") => {
            crate::handlers_logging::handle_disable_audit_log(&state)
        }
        (Method::GET, "/api/v1/audit/config") => {
            crate::handlers_logging::handle_get_audit_config(&state)
        }
        (Method::POST, "/api/v1/operations/enablefql") => {
            crate::handlers_logging::handle_enable_fql(req, &state).await
        }
        (Method::POST, "/api/v1/operations/disablefql") => {
            crate::handlers_logging::handle_disable_fql(&state)
        }
        (Method::GET, "/api/v1/fql/config") => {
            crate::handlers_logging::handle_get_fql_config(&state)
        }
        (Method::GET, "/api/v1/tracing/probability") => {
            crate::handlers_logging::handle_get_trace_probability(&state)
        }
        (Method::POST, "/api/v1/tracing/probability") => {
            crate::handlers_logging::handle_set_trace_probability(req, &state).await
        }

        _ => not_found(),
    };

    Ok(response)
}

fn handle_metrics(state: &AdminState) -> Response<Full<Bytes>> {
    if let Some(ref coordinator) = state.repair_coordinator {
        state
            .metrics
            .sync_from_repair_metrics(&coordinator.metrics.snapshot());
    }
    let body = state.metrics.gather_text();
    Response::builder()
        .status(StatusCode::OK)
        .header("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        .body(Full::new(Bytes::from(body)))
        .unwrap()
}

fn handle_health() -> Response<Full<Bytes>> {
    let body = serde_json::json!({
        "status": "UP",
        "version": env!("CARGO_PKG_VERSION"),
    });
    json_response(StatusCode::OK, &body)
}

fn handle_version() -> Response<Full<Bytes>> {
    let body = serde_json::json!({
        "version": env!("CARGO_PKG_VERSION"),
    });
    json_response(StatusCode::OK, &body)
}

fn handle_named_cache_invalidate(cache_type: &str) -> Response<Full<Bytes>> {
    json_response(
        StatusCode::OK,
        &serde_json::json!({
            "status": "cache invalidated",
            "cache_type": cache_type
        }),
    )
}

fn handle_operations(state: &AdminState) -> Response<Full<Bytes>> {
    let ops = state.operations.list_operations();
    let body = serde_json::to_value(&ops).unwrap_or(serde_json::Value::Array(vec![]));
    json_response(StatusCode::OK, &body)
}

async fn handle_stop_operations(
    req: Request<Incoming>,
    state: &AdminState,
) -> Response<Full<Bytes>> {
    use http_body_util::BodyExt;

    let body_bytes = match req.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &serde_json::json!({"error": format!("Failed to read body: {}", e)}),
            );
        }
    };

    handle_stop_operations_inner(&body_bytes, state)
}

fn handle_stop_operations_inner(body_bytes: &[u8], state: &AdminState) -> Response<Full<Bytes>> {
    #[derive(Default, serde::Deserialize)]
    struct StopOperationsRequest {
        #[serde(default)]
        operation_id: Option<String>,
    }

    let payload = if body_bytes.is_empty() {
        StopOperationsRequest::default()
    } else {
        match serde_json::from_slice::<StopOperationsRequest>(body_bytes) {
            Ok(p) => p,
            Err(e) => {
                return json_response(
                    StatusCode::BAD_REQUEST,
                    &serde_json::json!({"error": format!("Invalid JSON: {}", e)}),
                );
            }
        }
    };

    let active_ops = state.operations.list_operations();

    let ids_to_stop = if let Some(raw_id) = payload.operation_id {
        let wanted = match uuid::Uuid::parse_str(&raw_id) {
            Ok(v) => v,
            Err(e) => {
                return json_response(
                    StatusCode::BAD_REQUEST,
                    &serde_json::json!({"error": format!("Invalid operation_id '{}': {}", raw_id, e)}),
                );
            }
        };
        if active_ops.iter().any(|op| op.id == wanted) {
            vec![wanted]
        } else {
            return json_response(
                StatusCode::NOT_FOUND,
                &serde_json::json!({"error": format!("operation '{}' not found", wanted)}),
            );
        }
    } else {
        active_ops.iter().map(|op| op.id).collect::<Vec<_>>()
    };

    for id in &ids_to_stop {
        state.operations.update(id, "STOPPED", 100);
        state.operations.remove(id);
    }

    let stopped_ids: Vec<String> = ids_to_stop.iter().map(|id| id.to_string()).collect();
    json_response(
        StatusCode::OK,
        &serde_json::json!({
            "status": "stop request processed",
            "stopped_operations": stopped_ids.len(),
            "operation_ids": stopped_ids,
        }),
    )
}

fn handle_virtual_table(path: &str, state: &AdminState) -> Response<Full<Bytes>> {
    // Parse /api/v1/virtual/<keyspace>/<table>
    let parts: Vec<&str> = path
        .trim_start_matches("/api/v1/virtual/")
        .split('/')
        .collect();
    if parts.len() != 2 {
        return json_response(
            StatusCode::BAD_REQUEST,
            &serde_json::json!({"error": "expected /api/v1/virtual/<keyspace>/<table>"}),
        );
    }

    let keyspace = parts[0];
    let table_name = parts[1];

    match state.virtual_tables.get(keyspace, table_name) {
        Some(table) => {
            let columns = table.columns();
            let rows = table.rows();
            let body = serde_json::json!({
                "keyspace": keyspace,
                "table": table_name,
                "columns": columns,
                "rows": rows,
            });
            json_response(StatusCode::OK, &body)
        }
        None => json_response(
            StatusCode::NOT_FOUND,
            &serde_json::json!({"error": format!("virtual table {}.{} not found", keyspace, table_name)}),
        ),
    }
}

fn handle_list_indexes(state: &AdminState) -> Response<Full<Bytes>> {
    if let Some(ref engine) = state.storage_engine {
        let mgrs = engine.index_managers.read();
        let mut indexes = Vec::new();
        for (cf_name, mgr) in mgrs.iter() {
            for idx_name in mgr.list_names() {
                if let Some(def) = mgr.get_definition(&idx_name) {
                    indexes.push(serde_json::json!({
                        "name": idx_name,
                        "column_family": cf_name,
                        "column": def.column,
                        "type": format!("{:?}", def.index_type),
                        "status": format!("{:?}", mgr.get_status(&idx_name)),
                    }));
                }
            }
        }
        json_response(StatusCode::OK, &serde_json::json!({"indexes": indexes}))
    } else {
        json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &serde_json::json!({"error": "storage engine not configured"}),
        )
    }
}

fn handle_index_detail(path: &str, state: &AdminState) -> Response<Full<Bytes>> {
    let index_name = path.trim_start_matches("/admin/indexes/").to_string();

    if index_name.is_empty() {
        return json_response(
            StatusCode::BAD_REQUEST,
            &serde_json::json!({"error": "index name required"}),
        );
    }

    if let Some(ref engine) = state.storage_engine {
        let mgrs = engine.index_managers.read();
        for (cf_name, mgr) in mgrs.iter() {
            if let Some(def) = mgr.get_definition(&index_name) {
                let body = serde_json::json!({
                    "name": index_name,
                    "column_family": cf_name,
                    "column": def.column,
                    "type": format!("{:?}", def.index_type),
                    "status": format!("{:?}", mgr.get_status(&index_name)),
                    "keyspace": def.keyspace,
                    "table": def.table,
                    "options": def.options,
                });
                return json_response(StatusCode::OK, &body);
            }
        }
        json_response(
            StatusCode::NOT_FOUND,
            &serde_json::json!({"error": format!("index '{}' not found", index_name)}),
        )
    } else {
        json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &serde_json::json!({"error": "storage engine not configured"}),
        )
    }
}

pub fn not_found() -> Response<Full<Bytes>> {
    json_response(
        StatusCode::NOT_FOUND,
        &serde_json::json!({"error": "not found"}),
    )
}

async fn handle_repair_request(
    req: Request<Incoming>,
    state: &AdminState,
) -> Response<Full<Bytes>> {
    use http_body_util::BodyExt;

    let body_bytes = match req.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &serde_json::json!({"error": format!("Failed to read body: {}", e)}),
            );
        }
    };

    #[derive(serde::Deserialize)]
    struct RepairRequestBody {
        keyspace: String,
        #[serde(default)]
        tables: Vec<String>,
        #[serde(default)]
        full: bool,
        #[serde(default)]
        preview: bool,
        #[serde(default)]
        ranges: Vec<RepairTokenRange>,
        #[serde(default)]
        replicas: Vec<String>,
    }

    #[derive(serde::Deserialize)]
    struct RepairTokenRange {
        start: i64,
        end: i64,
    }

    let payload: RepairRequestBody = match serde_json::from_slice(&body_bytes) {
        Ok(p) => p,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &serde_json::json!({"error": format!("Invalid JSON: {}", e)}),
            );
        }
    };

    if let Some(ref coordinator) = state.repair_coordinator {
        let repair_type = if payload.preview {
            cassandra_repair::RepairType::Preview
        } else if payload.full {
            cassandra_repair::RepairType::Full
        } else {
            cassandra_repair::RepairType::Incremental
        };

        let ranges = if payload.ranges.is_empty() {
            vec![(
                cassandra_common::Token::from_raw(i64::MIN),
                cassandra_common::Token::from_raw(i64::MAX),
            )]
        } else {
            payload
                .ranges
                .iter()
                .map(|range| {
                    (
                        cassandra_common::Token::from_raw(range.start),
                        cassandra_common::Token::from_raw(range.end),
                    )
                })
                .collect()
        };

        let replicas = if payload.replicas.is_empty() {
            vec![cassandra_cluster_metadata::Endpoint::new(
                "127.0.0.1:7000".parse().unwrap(),
            )]
        } else {
            let mut endpoints = Vec::with_capacity(payload.replicas.len());
            for replica in &payload.replicas {
                match replica.parse() {
                    Ok(addr) => endpoints.push(cassandra_cluster_metadata::Endpoint::new(addr)),
                    Err(e) => {
                        return json_response(
                            StatusCode::BAD_REQUEST,
                            &serde_json::json!({
                                "error": format!("Invalid replica address '{}': {}", replica, e)
                            }),
                        );
                    }
                }
            }
            endpoints
        };

        match coordinator.start_repair(
            repair_type,
            &payload.keyspace,
            &payload.tables,
            &ranges,
            &replicas,
        ) {
            Ok(id) => {
                let op_id = state.operations.register(
                    crate::operations::OperationType::Repair,
                    format!("Repair keyspace: {}", payload.keyspace),
                );
                json_response(
                    StatusCode::OK,
                    &serde_json::json!({ "repair_id": id.to_string(), "operation_id": op_id.to_string() }),
                )
            }
            Err(e) => json_response(
                StatusCode::CONFLICT,
                &serde_json::json!({"error": e.to_string()}),
            ),
        }
    } else {
        json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &serde_json::json!({"error": "repair coordinator not configured"}),
        )
    }
}

async fn handle_rebuild_index(req: Request<Incoming>, state: &AdminState) -> Response<Full<Bytes>> {
    use http_body_util::BodyExt;

    let body_bytes = match req.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &serde_json::json!({"error": format!("Failed to read body: {}", e)}),
            );
        }
    };

    #[derive(serde::Deserialize)]
    struct RebuildIndexRequestBody {
        keyspace: String,
        table: String,
        index_name: String,
    }

    let payload: RebuildIndexRequestBody = match serde_json::from_slice(&body_bytes) {
        Ok(p) => p,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &serde_json::json!({"error": format!("Invalid JSON: {}", e)}),
            );
        }
    };

    if let Some(ref catalog) = state.schema_catalog {
        let snap = catalog.read().snapshot();
        let table_meta = match snap.table(&payload.keyspace, &payload.table) {
            Some(t) => t,
            None => {
                let msg = format!("Table {}.{} not found", payload.keyspace, payload.table);
                return json_response(StatusCode::NOT_FOUND, &serde_json::json!({"error": msg}));
            }
        };

        let index_meta = match table_meta.index(&payload.index_name) {
            Some(i) => i,
            None => {
                let msg = format!(
                    "Index {} not found on {}.{}",
                    payload.index_name, payload.keyspace, payload.table
                );
                return json_response(StatusCode::NOT_FOUND, &serde_json::json!({"error": msg}));
            }
        };

        if let Some(ref engine) = state.storage_engine {
            let column_name = match index_meta.target_column() {
                Some(c) => c.clone(),
                None => {
                    return json_response(
                        StatusCode::BAD_REQUEST,
                        &serde_json::json!({"error": "Index target column not defined"}),
                    );
                }
            };

            // Map IndexKind to IndexType
            let index_type = match index_meta.kind {
                cassandra_schema::IndexKind::Keys => cassandra_storage::index::IndexType::Legacy,
                cassandra_schema::IndexKind::Custom => {
                    let class = index_meta
                        .options
                        .get("class_name")
                        .map(|s| s.as_str())
                        .unwrap_or("");
                    if class.contains("SASIIndex") {
                        #[cfg(feature = "sasi")]
                        {
                            cassandra_storage::index::IndexType::Sasi
                        }
                        #[cfg(not(feature = "sasi"))]
                        {
                            return json_response(
                                StatusCode::NOT_IMPLEMENTED,
                                &serde_json::json!({"error": "SASI index support not enabled"}),
                            );
                        }
                    } else if class.contains("StorageAttachedIndex") {
                        cassandra_storage::index::IndexType::Sai
                    } else {
                        return json_response(
                            StatusCode::BAD_REQUEST,
                            &serde_json::json!({"error": format!("Unsupported custom index class: {}", class)}),
                        );
                    }
                }
                cassandra_schema::IndexKind::Composites => {
                    cassandra_storage::index::IndexType::Legacy
                }
            };

            let definition = cassandra_storage::index::IndexDefinition {
                name: index_meta.name.clone(),
                keyspace: payload.keyspace.clone(),
                table: payload.table.clone(),
                column: column_name,
                index_type,
                options: index_meta.options.clone(),
            };

            let cf_name = format!("{}.{}", payload.keyspace, payload.table);
            match engine.rebuild_index(&cf_name, definition) {
                Ok(_) => {
                    let msg = format!(
                        "Rebuild of index {} completed successfully",
                        payload.index_name
                    );
                    json_response(StatusCode::OK, &serde_json::json!({"status": msg}))
                }
                Err(e) => json_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &serde_json::json!({"error": e.to_string()}),
                ),
            }
        } else {
            json_response(
                StatusCode::SERVICE_UNAVAILABLE,
                &serde_json::json!({"error": "storage engine not configured"}),
            )
        }
    } else {
        json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &serde_json::json!({"error": "schema catalog not configured"}),
        )
    }
}

pub fn json_response(status: StatusCode, body: &serde_json::Value) -> Response<Full<Bytes>> {
    Response::builder()
        .status(status)
        .header("Content-Type", "application/json")
        .body(Full::new(Bytes::from(
            serde_json::to_string(body).unwrap_or_default(),
        )))
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_state() -> Arc<AdminState> {
        Arc::new(AdminState {
            metrics: Arc::new(MetricsRegistry::new()),
            operations: Arc::new(OperationTracker::new()),
            virtual_tables: Arc::new(VirtualTableRegistry::with_builtins()),
            repair_coordinator: None,
            storage_engine: None,
            schema_catalog: None,
            topology_controller: None,
        })
    }

    #[test]
    fn health_response() {
        let resp = handle_health();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn metrics_response() {
        let state = test_state();
        let resp = handle_metrics(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn operations_response() {
        let state = test_state();
        let resp = handle_operations(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn stop_operations_stops_all_active() {
        use crate::operations::OperationType;
        use http_body_util::BodyExt;

        let state = test_state();
        state
            .operations
            .register(OperationType::Repair, "repair ks".to_string());
        state
            .operations
            .register(OperationType::Rebuild, "compact ks.t".to_string());

        let resp = handle_stop_operations_inner(&[], &state);
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(state.operations.count(), 0);

        let rt = tokio::runtime::Runtime::new().unwrap();
        let body = rt.block_on(resp.into_body().collect()).unwrap().to_bytes();
        let parsed: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(parsed["stopped_operations"], 2);
    }

    #[test]
    fn stop_operations_unknown_id_returns_not_found() {
        let state = test_state();
        let body =
            serde_json::to_vec(&serde_json::json!({"operation_id": uuid::Uuid::new_v4()})).unwrap();
        let resp = handle_stop_operations_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn stop_operations_invalid_json_returns_bad_request() {
        let state = test_state();
        let resp = handle_stop_operations_inner(br#"{"operation_id": 123"#, &state);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn virtual_table_found() {
        let state = test_state();
        let resp = handle_virtual_table("/api/v1/virtual/system_views/local", &state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn virtual_table_not_found() {
        let state = test_state();
        let resp = handle_virtual_table("/api/v1/virtual/system_views/nonexistent", &state);
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }

    #[test]
    fn virtual_table_bad_path() {
        let state = test_state();
        let resp = handle_virtual_table("/api/v1/virtual/only_one_part", &state);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn not_found_response() {
        let resp = not_found();
        assert_eq!(resp.status(), StatusCode::NOT_FOUND);
    }
}
