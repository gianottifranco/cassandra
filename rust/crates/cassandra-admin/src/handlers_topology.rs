// Licensed under Apache License, Version 2.0.

//! Topology and cluster membership HTTP handlers.
//!
//! Provides endpoints for node lifecycle operations such as decommission,
//! bootstrap, drain, rebuild, and topology status queries.

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use serde_json::json;

use crate::http_admin::{AdminState, json_response};
use crate::operations::OperationType;

/// Initiate decommission of the local node.
pub fn handle_decommission(state: &AdminState) -> Response<Full<Bytes>> {
    let Some(controller) = state.topology_controller.as_ref() else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({"error": "topology controller not configured"}),
        );
    };

    let op_id = state.operations.register(
        OperationType::Decommission,
        "Decommission local node".to_string(),
    );

    let response = match controller.decommission_local() {
        Ok(report) => json_response(
            StatusCode::OK,
            &json!({
                "operation_id": op_id.to_string(),
                "status": "decommission completed",
                "state_before": report.state_before,
                "state_after": report.state_after,
                "leaving_tokens": report.leaving_tokens,
            }),
        ),
        Err(err) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &json!({"error": format!("decommission failed: {err}")}),
        ),
    };

    if response.status() == StatusCode::OK {
        state.operations.update(&op_id, "COMPLETED", 100);
    } else {
        state.operations.update(&op_id, "FAILED", 100);
    }
    state.operations.remove(&op_id);
    response
}

/// Remove a node from the cluster by host ID.
pub async fn handle_removenode(
    req: Request<Incoming>,
    state: &AdminState,
) -> Response<Full<Bytes>> {
    use http_body_util::BodyExt;

    let body_bytes = match req.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Failed to read body: {}", e)}),
            );
        }
    };

    handle_removenode_inner(&body_bytes, state)
}

fn handle_removenode_inner(body_bytes: &[u8], state: &AdminState) -> Response<Full<Bytes>> {
    #[derive(serde::Deserialize)]
    struct RemoveNodeRequest {
        host_id: String,
    }

    let payload: RemoveNodeRequest = match serde_json::from_slice(body_bytes) {
        Ok(p) => p,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Invalid JSON: {}", e)}),
            );
        }
    };

    let Some(controller) = state.topology_controller.as_ref() else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({"error": "topology controller not configured"}),
        );
    };

    let op_id = state.operations.register(
        OperationType::RemoveNode,
        format!("Remove node {}", payload.host_id),
    );

    let response = match controller.remove_node_local(&payload.host_id) {
        Ok(report) => json_response(
            StatusCode::OK,
            &json!({
                "operation_id": op_id.to_string(),
                "host_id": report.host_id,
                "endpoint": report.endpoint,
                "status": "removenode completed",
                "node_count_before": report.node_count_before,
                "node_count_after": report.node_count_after,
            }),
        ),
        Err(err) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &json!({"error": format!("removenode failed: {err}")}),
        ),
    };

    if response.status() == StatusCode::OK {
        state.operations.update(&op_id, "COMPLETED", 100);
    } else {
        state.operations.update(&op_id, "FAILED", 100);
    }
    state.operations.remove(&op_id);
    response
}

/// Move the local node to a new token.
pub async fn handle_move(req: Request<Incoming>, state: &AdminState) -> Response<Full<Bytes>> {
    use http_body_util::BodyExt;

    let body_bytes = match req.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Failed to read body: {}", e)}),
            );
        }
    };

    handle_move_inner(&body_bytes, state)
}

fn handle_move_inner(body_bytes: &[u8], state: &AdminState) -> Response<Full<Bytes>> {
    #[derive(serde::Deserialize)]
    struct MoveRequest {
        new_token: String,
    }

    let payload: MoveRequest = match serde_json::from_slice(body_bytes) {
        Ok(p) => p,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Invalid JSON: {}", e)}),
            );
        }
    };

    let new_token = match payload.new_token.parse::<i64>() {
        Ok(token) => token,
        Err(_) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Invalid token value '{}'", payload.new_token)}),
            );
        }
    };

    let Some(controller) = state.topology_controller.as_ref() else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({"error": "topology controller not configured"}),
        );
    };

    let op_id = state.operations.register(
        OperationType::Stream,
        format!("Move to token {}", payload.new_token),
    );

    let response = match controller.move_local(new_token) {
        Ok(report) => json_response(
            StatusCode::OK,
            &json!({
                "operation_id": op_id.to_string(),
                "new_token": payload.new_token,
                "status": "move completed",
                "state_before": report.state_before,
                "state_after": report.state_after,
                "owned_tokens_before": report.owned_tokens_before,
                "owned_tokens_after": report.owned_tokens_after,
            }),
        ),
        Err(err) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &json!({"error": format!("move failed: {err}")}),
        ),
    };

    if response.status() == StatusCode::OK {
        state.operations.update(&op_id, "COMPLETED", 100);
    } else {
        state.operations.update(&op_id, "FAILED", 100);
    }
    state.operations.remove(&op_id);
    response
}

/// Rebuild data from another datacenter.
pub async fn handle_rebuild(req: Request<Incoming>, state: &AdminState) -> Response<Full<Bytes>> {
    use http_body_util::BodyExt;

    let body_bytes = match req.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Failed to read body: {}", e)}),
            );
        }
    };

    handle_rebuild_inner(&body_bytes, state)
}

fn handle_rebuild_inner(body_bytes: &[u8], state: &AdminState) -> Response<Full<Bytes>> {
    #[derive(serde::Deserialize)]
    struct RebuildRequest {
        #[serde(default)]
        source_dc: Option<String>,
    }

    let payload: RebuildRequest = match serde_json::from_slice(body_bytes) {
        Ok(p) => p,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Invalid JSON: {}", e)}),
            );
        }
    };

    let description = match payload.source_dc.as_ref() {
        Some(ref dc) => format!("Rebuild from DC {}", dc),
        None => "Rebuild from all DCs".to_string(),
    };

    let Some(controller) = state.topology_controller.as_ref() else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({"error": "topology controller not configured"}),
        );
    };

    let op_id = state
        .operations
        .register(OperationType::Rebuild, description);

    let response = match controller.rebuild_local(payload.source_dc.as_deref()) {
        Ok(report) => json_response(
            StatusCode::OK,
            &json!({
                "operation_id": op_id.to_string(),
                "source_dc": report.source_dc,
                "status": "rebuild completed",
                "stream_requests": report.stream_requests,
                "state_before": report.state_before,
                "state_after": report.state_after,
            }),
        ),
        Err(err) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &json!({"error": format!("rebuild failed: {err}")}),
        ),
    };

    if response.status() == StatusCode::OK {
        state.operations.update(&op_id, "COMPLETED", 100);
    } else {
        state.operations.update(&op_id, "FAILED", 100);
    }
    state.operations.remove(&op_id);
    response
}

/// Refresh SSTables for a specific table (load new SSTables from disk).
pub async fn handle_refresh(req: Request<Incoming>, state: &AdminState) -> Response<Full<Bytes>> {
    use http_body_util::BodyExt;

    let body_bytes = match req.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Failed to read body: {}", e)}),
            );
        }
    };

    handle_refresh_inner(&body_bytes, state)
}

fn handle_refresh_inner(body_bytes: &[u8], state: &AdminState) -> Response<Full<Bytes>> {
    #[derive(serde::Deserialize)]
    struct RefreshRequest {
        keyspace: String,
        table: String,
    }

    let payload: RefreshRequest = match serde_json::from_slice(body_bytes) {
        Ok(p) => p,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Invalid JSON: {}", e)}),
            );
        }
    };

    let Some(engine) = state.storage_engine.as_ref() else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({"error": "storage engine not configured"}),
        );
    };

    if payload.keyspace.trim().is_empty() || payload.table.trim().is_empty() {
        return json_response(
            StatusCode::BAD_REQUEST,
            &json!({"error": "keyspace and table must be non-empty"}),
        );
    }

    let op_id = state.operations.register(
        OperationType::Stream,
        format!("Refresh {}.{}", payload.keyspace, payload.table),
    );
    let before = engine
        .table_sstable_stats(&payload.keyspace, &payload.table)
        .sstable_count;
    let refresh_result = engine.refresh_sstables_from_disk();

    let response = match refresh_result {
        Ok(()) => {
            let after = engine
                .table_sstable_stats(&payload.keyspace, &payload.table)
                .sstable_count;
            json_response(
                StatusCode::OK,
                &json!({
                    "operation_id": op_id.to_string(),
                    "status": format!("refresh completed for {}.{}", payload.keyspace, payload.table),
                    "sstables_before": before,
                    "sstables_after": after,
                    "sstables_loaded": after.saturating_sub(before),
                }),
            )
        }
        Err(err) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &json!({"error": format!("failed to refresh SSTables: {err}")}),
        ),
    };

    if response.status() == StatusCode::OK {
        state.operations.update(&op_id, "COMPLETED", 100);
    } else {
        state.operations.update(&op_id, "FAILED", 100);
    }
    state.operations.remove(&op_id);

    response
}

/// Initiate a join to the ring.
pub fn handle_join(state: &AdminState) -> Response<Full<Bytes>> {
    let Some(controller) = state.topology_controller.as_ref() else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({"error": "topology controller not configured"}),
        );
    };

    let op_id = state
        .operations
        .register(OperationType::Bootstrap, "Join local node".to_string());
    let response = match controller.join_local() {
        Ok(report) => json_response(
            StatusCode::OK,
            &json!({
                "operation_id": op_id.to_string(),
                "status": "join completed",
                "state_before": report.state_before,
                "state_after": report.state_after,
                "pending_bootstrap_tokens": report.pending_bootstrap_tokens,
                "owned_tokens": report.owned_tokens,
            }),
        ),
        Err(err) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &json!({"error": format!("join failed: {err}")}),
        ),
    };

    if response.status() == StatusCode::OK {
        state.operations.update(&op_id, "COMPLETED", 100);
    } else {
        state.operations.update(&op_id, "FAILED", 100);
    }
    state.operations.remove(&op_id);
    response
}

/// Resume or abort bootstrap of the local node.
pub async fn handle_bootstrap(req: Request<Incoming>, state: &AdminState) -> Response<Full<Bytes>> {
    use http_body_util::BodyExt;

    let body_bytes = match req.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Failed to read body: {}", e)}),
            );
        }
    };

    handle_bootstrap_inner(&body_bytes, state)
}

fn handle_bootstrap_inner(body_bytes: &[u8], state: &AdminState) -> Response<Full<Bytes>> {
    #[derive(Default, serde::Deserialize)]
    struct BootstrapRequest {
        #[serde(default)]
        action: Option<String>,
    }

    let payload = if body_bytes.is_empty() {
        BootstrapRequest::default()
    } else {
        match serde_json::from_slice::<BootstrapRequest>(body_bytes) {
            Ok(value) => value,
            Err(e) => {
                return json_response(
                    StatusCode::BAD_REQUEST,
                    &json!({"error": format!("Invalid JSON: {}", e)}),
                );
            }
        }
    };

    let action = payload.action.as_deref().unwrap_or("resume");
    if action != "resume" && action != "abort" {
        return json_response(
            StatusCode::BAD_REQUEST,
            &json!({"error": format!("unsupported bootstrap action '{action}'")}),
        );
    }

    let Some(controller) = state.topology_controller.as_ref() else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({"error": "topology controller not configured"}),
        );
    };

    let op_id = state.operations.register(
        OperationType::Bootstrap,
        format!("Bootstrap local node ({action})"),
    );

    let response = match action {
        "abort" => match controller.abort_bootstrap_local() {
            Ok(report) => json_response(
                StatusCode::OK,
                &json!({
                    "operation_id": op_id.to_string(),
                    "status": "bootstrap aborted",
                    "action": report.action,
                    "state_before": report.state_before,
                    "state_after": report.state_after,
                    "topology_state_before": report.topology_state_before,
                    "topology_state_after": report.topology_state_after,
                }),
            ),
            Err(err) => json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &json!({"error": format!("bootstrap abort failed: {err}")}),
            ),
        },
        _ => match controller.bootstrap_resume_local() {
            Ok(report) => json_response(
                StatusCode::OK,
                &json!({
                    "operation_id": op_id.to_string(),
                    "status": "bootstrap resumed",
                    "action": report.action,
                    "state_before": report.state_before,
                    "state_after": report.state_after,
                    "topology_state_before": report.topology_state_before,
                    "topology_state_after": report.topology_state_after,
                }),
            ),
            Err(err) => json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &json!({"error": format!("bootstrap resume failed: {err}")}),
            ),
        },
    };

    if response.status() == StatusCode::OK {
        state.operations.update(&op_id, "COMPLETED", 100);
    } else {
        state.operations.update(&op_id, "FAILED", 100);
    }
    state.operations.remove(&op_id);
    response
}

/// Drain the node: stop accepting writes and flush all memtables.
pub fn handle_drain(state: &AdminState) -> Response<Full<Bytes>> {
    let Some(engine) = state.storage_engine.as_ref() else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({"error": "storage engine not configured"}),
        );
    };

    let op_id = state
        .operations
        .register(OperationType::Stream, "Drain local node".to_string());
    let before_flushes = engine.stats().flushes_completed;

    let response = match engine.flush_all() {
        Ok(()) => match engine.sync_commitlog() {
            Ok(()) => {
                let after_flushes = engine.stats().flushes_completed;
                json_response(
                    StatusCode::OK,
                    &json!({
                        "operation_id": op_id.to_string(),
                        "status": "drain completed",
                        "flushes_before": before_flushes,
                        "flushes_after": after_flushes,
                        "flushes_executed": after_flushes.saturating_sub(before_flushes),
                    }),
                )
            }
            Err(err) => json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &json!({"error": format!("failed to sync commitlog during drain: {err}")}),
            ),
        },
        Err(err) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &json!({"error": format!("failed to flush memtables during drain: {err}")}),
        ),
    };

    if response.status() == StatusCode::OK {
        state.operations.update(&op_id, "COMPLETED", 100);
    } else {
        state.operations.update(&op_id, "FAILED", 100);
    }
    state.operations.remove(&op_id);

    response
}

/// Stop Cassandra daemon gracefully.
pub fn handle_stop_daemon(state: &AdminState) -> Response<Full<Bytes>> {
    let Some(controller) = state.topology_controller.as_ref() else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({"error": "topology controller not configured"}),
        );
    };

    let op_id = state
        .operations
        .register(OperationType::Stream, "Stop daemon".to_string());
    let response = match controller.stop_daemon_local() {
        Ok(report) => json_response(
            StatusCode::OK,
            &json!({
                "operation_id": op_id.to_string(),
                "status": "stopdaemon completed",
                "shutdown_signalled": report.shutdown_signalled,
                "storage_state_before": report.storage_state_before,
                "storage_state_after": report.storage_state_after,
                "transport_state_before": report.transport_state_before,
                "transport_state_after": report.transport_state_after,
                "in_flight_requests": report.in_flight_requests,
            }),
        ),
        Err(err) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &json!({"error": format!("stopdaemon failed: {err}")}),
        ),
    };

    if response.status() == StatusCode::OK {
        state.operations.update(&op_id, "COMPLETED", 100);
    } else {
        state.operations.update(&op_id, "FAILED", 100);
    }
    state.operations.remove(&op_id);
    response
}

/// Assassinate a node by endpoint address.
pub async fn handle_assassinate(
    req: Request<Incoming>,
    state: &AdminState,
) -> Response<Full<Bytes>> {
    use http_body_util::BodyExt;

    let body_bytes = match req.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Failed to read body: {}", e)}),
            );
        }
    };

    handle_assassinate_inner(&body_bytes, state)
}

fn handle_assassinate_inner(body_bytes: &[u8], state: &AdminState) -> Response<Full<Bytes>> {
    #[derive(serde::Deserialize)]
    struct AssassinateRequest {
        endpoint: String,
    }

    let payload: AssassinateRequest = match serde_json::from_slice(body_bytes) {
        Ok(p) => p,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Invalid JSON: {}", e)}),
            );
        }
    };

    let Some(controller) = state.topology_controller.as_ref() else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({"error": "topology controller not configured"}),
        );
    };

    let op_id = state.operations.register(
        OperationType::RemoveNode,
        format!("Assassinate endpoint {}", payload.endpoint),
    );
    let response = match controller.assassinate_endpoint_local(&payload.endpoint) {
        Ok(report) => json_response(
            StatusCode::OK,
            &json!({
                "operation_id": op_id.to_string(),
                "status": format!("assassinate completed for {}", report.endpoint),
                "endpoint": report.endpoint,
                "node_count_before": report.node_count_before,
                "node_count_after": report.node_count_after,
                "removed": report.removed,
            }),
        ),
        Err(err) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &json!({"error": format!("assassinate failed: {err}")}),
        ),
    };

    if response.status() == StatusCode::OK {
        state.operations.update(&op_id, "COMPLETED", 100);
    } else {
        state.operations.update(&op_id, "FAILED", 100);
    }
    state.operations.remove(&op_id);
    response
}

/// Return current topology operations status.
pub fn handle_topology_status(state: &AdminState) -> Response<Full<Bytes>> {
    let Some(controller) = state.topology_controller.as_ref() else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({"error": "topology controller not configured"}),
        );
    };

    match controller.topology_status_local() {
        Ok(report) => json_response(StatusCode::OK, &json!(report)),
        Err(err) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &json!({"error": format!("failed to fetch topology status: {err}")}),
        ),
    }
}

/// Return network statistics (streaming sessions, message counts).
pub fn handle_netstats(state: &AdminState) -> Response<Full<Bytes>> {
    let Some(controller) = state.topology_controller.as_ref() else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({"error": "topology controller not configured"}),
        );
    };

    match controller.netstats_local() {
        Ok(report) => json_response(StatusCode::OK, &json!(report)),
        Err(err) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &json!({"error": format!("failed to fetch netstats: {err}")}),
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use parking_lot::RwLock;
    use std::fs;
    use std::sync::Arc;
    use tempfile::TempDir;

    use crate::http_admin::{
        AssassinateReport, BootstrapReport, CommandQueueStat, CurrentTopologyOperation,
        DecommissionReport, JoinReport, MoveReport, NetstatsReport, RebuildReport,
        RemoveNodeReport, StopDaemonReport, StreamPeerStat, TopologyController, TopologyNodeStatus,
        TopologyStatusReport,
    };
    use crate::operations::OperationTracker;
    use crate::prometheus_metrics::MetricsRegistry;
    use crate::virtual_tables::VirtualTableRegistry;
    use cassandra_schema::SchemaCatalog;
    use cassandra_storage::commitlog::{CellMutation, CommitLogConfig, Mutation, MutationRow};
    use cassandra_storage::engine::{EngineConfig, StorageEngine};

    struct TestTopologyController;

    impl TopologyController for TestTopologyController {
        fn decommission_local(&self) -> Result<DecommissionReport, String> {
            Ok(DecommissionReport {
                state_before: "NORMAL".to_string(),
                state_after: "LEFT".to_string(),
                leaving_tokens: vec![0],
            })
        }

        fn move_local(&self, new_token: i64) -> Result<MoveReport, String> {
            Ok(MoveReport {
                state_before: "NORMAL".to_string(),
                state_after: "NORMAL".to_string(),
                owned_tokens_before: vec![0],
                owned_tokens_after: vec![new_token],
            })
        }

        fn remove_node_local(&self, host_id: &str) -> Result<RemoveNodeReport, String> {
            Ok(RemoveNodeReport {
                host_id: host_id.to_string(),
                endpoint: "127.0.0.1:7002".to_string(),
                node_count_before: 3,
                node_count_after: 2,
            })
        }

        fn rebuild_local(&self, source_dc: Option<&str>) -> Result<RebuildReport, String> {
            Ok(RebuildReport {
                source_dc: source_dc.map(ToOwned::to_owned),
                stream_requests: 1,
                state_before: "Idle".to_string(),
                state_after: "Done(Rebuild, success=true)".to_string(),
            })
        }

        fn join_local(&self) -> Result<JoinReport, String> {
            Ok(JoinReport {
                state_before: "JOINING".to_string(),
                state_after: "NORMAL".to_string(),
                pending_bootstrap_tokens: vec![0],
                owned_tokens: vec![0],
            })
        }

        fn bootstrap_resume_local(&self) -> Result<BootstrapReport, String> {
            Ok(BootstrapReport {
                action: "resume".to_string(),
                state_before: "JOINING".to_string(),
                state_after: "NORMAL".to_string(),
                topology_state_before: "DONE(BOOTSTRAP, FAILED)".to_string(),
                topology_state_after: "IDLE".to_string(),
            })
        }

        fn abort_bootstrap_local(&self) -> Result<BootstrapReport, String> {
            Ok(BootstrapReport {
                action: "abort".to_string(),
                state_before: "JOINING".to_string(),
                state_after: "JOINING".to_string(),
                topology_state_before: "STREAMING(BOOTSTRAP, 0%)".to_string(),
                topology_state_after: "DONE(BOOTSTRAP, FAILED)".to_string(),
            })
        }

        fn assassinate_endpoint_local(&self, endpoint: &str) -> Result<AssassinateReport, String> {
            Ok(AssassinateReport {
                endpoint: endpoint.to_string(),
                node_count_before: 3,
                node_count_after: 2,
                removed: true,
            })
        }

        fn topology_status_local(&self) -> Result<TopologyStatusReport, String> {
            Ok(TopologyStatusReport {
                status: "NORMAL".to_string(),
                current_operation: Some(CurrentTopologyOperation {
                    operation_type: "BOOTSTRAP".to_string(),
                    status: "STREAMING".to_string(),
                    progress: 0.5,
                    operation_id: None,
                }),
                nodes: vec![TopologyNodeStatus {
                    host_id: "test-host-id".to_string(),
                    address: "127.0.0.1:7000".to_string(),
                    state: "NORMAL".to_string(),
                    status: "UP".to_string(),
                }],
            })
        }

        fn netstats_local(&self) -> Result<NetstatsReport, String> {
            Ok(NetstatsReport {
                mode: "NORMAL".to_string(),
                receiving: vec![StreamPeerStat {
                    peer: "127.0.0.2:7000".to_string(),
                    files: 2,
                    bytes: 4096,
                }],
                sending: vec![StreamPeerStat {
                    peer: "127.0.0.3:7000".to_string(),
                    files: 1,
                    bytes: 2048,
                }],
                commands: std::collections::HashMap::from([(
                    "STREAM".to_string(),
                    CommandQueueStat {
                        pending: 0,
                        completed: 1,
                    },
                )]),
            })
        }

        fn stop_daemon_local(&self) -> Result<StopDaemonReport, String> {
            Ok(StopDaemonReport {
                shutdown_signalled: true,
                storage_state_before: "NORMAL".to_string(),
                storage_state_after: "LEAVING".to_string(),
                transport_state_before: "STARTED".to_string(),
                transport_state_after: "STOPPING".to_string(),
                in_flight_requests: 0,
            })
        }
    }

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

    fn test_state_with_engine() -> (Arc<AdminState>, TempDir) {
        let dir = TempDir::new().unwrap();
        let engine = Arc::new(
            StorageEngine::open(EngineConfig {
                data_directories: vec![dir.path().join("data")],
                commitlog: CommitLogConfig {
                    directory: dir.path().join("commitlog"),
                    ..CommitLogConfig::default()
                },
                ..EngineConfig::default()
            })
            .unwrap(),
        );
        let state = Arc::new(AdminState {
            metrics: Arc::new(MetricsRegistry::new()),
            operations: Arc::new(OperationTracker::new()),
            virtual_tables: Arc::new(VirtualTableRegistry::with_builtins()),
            repair_coordinator: None,
            storage_engine: Some(engine),
            schema_catalog: Some(Arc::new(RwLock::new(SchemaCatalog::new()))),
            topology_controller: None,
        });
        (state, dir)
    }

    fn insert_and_flush(engine: &StorageEngine, keyspace: &str, table: &str, partition_key: &[u8]) {
        let mutation = Mutation {
            keyspace: keyspace.to_string(),
            table: table.to_string(),
            partition_key: partition_key.to_vec(),
            rows: vec![MutationRow {
                clustering_key: Vec::new(),
                cells: vec![CellMutation {
                    column: "name".to_string(),
                    value: Some(b"value".to_vec()),
                    timestamp: 1,
                    ttl: 0,
                    local_deletion_time: None,
                    is_tombstone: false,
                }],
                is_tombstone: false,
                local_deletion_time: None,
            }],
            timestamp: 1,
            cdc_enabled: false,
            static_cells: Vec::new(),
            partition_tombstone: None,
            range_tombstones: Vec::new(),
        };
        engine.apply_mutation(&mutation).unwrap();
        engine.flush_cf(&format!("{keyspace}.{table}")).unwrap();
    }

    #[test]
    fn decommission_registers_operation() {
        let mut state = test_state();
        Arc::get_mut(&mut state).unwrap().topology_controller =
            Some(Arc::new(TestTopologyController));
        let resp = handle_decommission(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(state.operations.count(), 0);
    }

    #[test]
    fn decommission_without_controller_returns_unavailable() {
        let state = test_state();
        let resp = handle_decommission(&state);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn join_returns_ok() {
        let mut state = test_state();
        Arc::get_mut(&mut state).unwrap().topology_controller =
            Some(Arc::new(TestTopologyController));
        let resp = handle_join(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(state.operations.count(), 0);
    }

    #[test]
    fn join_without_controller_returns_unavailable() {
        let state = test_state();
        let resp = handle_join(&state);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn bootstrap_registers_operation() {
        let mut state = test_state();
        Arc::get_mut(&mut state).unwrap().topology_controller =
            Some(Arc::new(TestTopologyController));
        let resp = handle_bootstrap_inner(b"{}", &state);
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(state.operations.count(), 0);
    }

    #[test]
    fn bootstrap_abort_action() {
        let mut state = test_state();
        Arc::get_mut(&mut state).unwrap().topology_controller =
            Some(Arc::new(TestTopologyController));
        let body = serde_json::to_vec(&json!({"action": "abort"})).unwrap();
        let resp = handle_bootstrap_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::OK);

        let collected = {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(resp.into_body().collect()).unwrap().to_bytes()
        };
        let parsed: serde_json::Value = serde_json::from_slice(&collected).unwrap();
        assert_eq!(parsed["action"], "abort");
        assert_eq!(parsed["status"], "bootstrap aborted");
    }

    #[test]
    fn bootstrap_without_controller_returns_unavailable() {
        let state = test_state();
        let resp = handle_bootstrap_inner(b"{}", &state);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn bootstrap_rejects_invalid_action() {
        let mut state = test_state();
        Arc::get_mut(&mut state).unwrap().topology_controller =
            Some(Arc::new(TestTopologyController));
        let body = serde_json::to_vec(&json!({"action": "pause"})).unwrap();
        let resp = handle_bootstrap_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn drain_returns_ok() {
        let (state, _dir) = test_state_with_engine();
        let resp = handle_drain(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn drain_without_engine_returns_unavailable() {
        let state = test_state();
        let resp = handle_drain(&state);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn stop_daemon_returns_ok() {
        let mut state = test_state();
        Arc::get_mut(&mut state).unwrap().topology_controller =
            Some(Arc::new(TestTopologyController));
        let resp = handle_stop_daemon(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(state.operations.count(), 0);
    }

    #[test]
    fn stop_daemon_without_controller_returns_unavailable() {
        let state = test_state();
        let resp = handle_stop_daemon(&state);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn topology_status_filters_operations() {
        let mut state = test_state();
        Arc::get_mut(&mut state).unwrap().topology_controller =
            Some(Arc::new(TestTopologyController));

        let resp = handle_topology_status(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn netstats_returns_ok() {
        let mut state = test_state();
        Arc::get_mut(&mut state).unwrap().topology_controller =
            Some(Arc::new(TestTopologyController));
        let resp = handle_netstats(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn topology_status_without_controller_returns_unavailable() {
        let state = test_state();
        let resp = handle_topology_status(&state);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn netstats_without_controller_returns_unavailable() {
        let state = test_state();
        let resp = handle_netstats(&state);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn removenode_with_valid_body() {
        let mut state = test_state();
        Arc::get_mut(&mut state).unwrap().topology_controller =
            Some(Arc::new(TestTopologyController));
        let body = serde_json::to_vec(&json!({"host_id": "abc-123"})).unwrap();

        let resp = handle_removenode_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(state.operations.count(), 0);

        let collected = {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(resp.into_body().collect()).unwrap().to_bytes()
        };
        let parsed: serde_json::Value = serde_json::from_slice(&collected).unwrap();
        assert_eq!(parsed["status"], "removenode completed");
        assert_eq!(parsed["node_count_before"], 3);
        assert_eq!(parsed["node_count_after"], 2);
    }

    #[test]
    fn removenode_invalid_json() {
        let state = test_state();

        let resp = handle_removenode_inner(b"{bad", &state);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn removenode_without_controller_returns_unavailable() {
        let state = test_state();
        let body = serde_json::to_vec(&json!({"host_id": "abc-123"})).unwrap();
        let resp = handle_removenode_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn rebuild_with_source_dc() {
        let mut state = test_state();
        Arc::get_mut(&mut state).unwrap().topology_controller =
            Some(Arc::new(TestTopologyController));
        let body = serde_json::to_vec(&json!({"source_dc": "dc2"})).unwrap();

        let resp = handle_rebuild_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(state.operations.count(), 0);

        let collected = {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(resp.into_body().collect()).unwrap().to_bytes()
        };
        let parsed: serde_json::Value = serde_json::from_slice(&collected).unwrap();
        assert_eq!(parsed["source_dc"], "dc2");
        assert_eq!(parsed["status"], "rebuild completed");
        assert_eq!(parsed["stream_requests"], 1);
    }

    #[test]
    fn rebuild_without_controller_returns_unavailable() {
        let state = test_state();
        let body = serde_json::to_vec(&json!({"source_dc": "dc2"})).unwrap();
        let resp = handle_rebuild_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn assassinate_with_valid_body() {
        let mut state = test_state();
        Arc::get_mut(&mut state).unwrap().topology_controller =
            Some(Arc::new(TestTopologyController));
        let body = serde_json::to_vec(&json!({"endpoint": "10.0.0.5:7000"})).unwrap();

        let resp = handle_assassinate_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn assassinate_without_controller_returns_unavailable() {
        let state = test_state();
        let body = serde_json::to_vec(&json!({"endpoint": "10.0.0.5:7000"})).unwrap();
        let resp = handle_assassinate_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn move_with_valid_body() {
        let mut state = test_state();
        Arc::get_mut(&mut state).unwrap().topology_controller =
            Some(Arc::new(TestTopologyController));
        let body = serde_json::to_vec(&json!({"new_token": "12345"})).unwrap();

        let resp = handle_move_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::OK);

        let collected = {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(resp.into_body().collect()).unwrap().to_bytes()
        };
        let parsed: serde_json::Value = serde_json::from_slice(&collected).unwrap();
        assert_eq!(parsed["new_token"], "12345");
        assert_eq!(parsed["owned_tokens_after"], json!([12345]));
    }

    #[test]
    fn move_without_controller_returns_unavailable() {
        let state = test_state();
        let body = serde_json::to_vec(&json!({"new_token": "12345"})).unwrap();
        let resp = handle_move_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn refresh_with_valid_body() {
        let (state, _dir) = test_state_with_engine();
        let engine = state.storage_engine.as_ref().unwrap();
        insert_and_flush(engine, "ks1", "tbl1", b"pk1");

        let body = serde_json::to_vec(&json!({"keyspace": "ks1", "table": "tbl1"})).unwrap();

        let resp = handle_refresh_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::OK);
        let collected = {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(resp.into_body().collect()).unwrap().to_bytes()
        };
        let parsed: serde_json::Value = serde_json::from_slice(&collected).unwrap();
        assert!(parsed["operation_id"].is_string());
        assert_eq!(parsed["sstables_before"], 1);
        assert_eq!(parsed["sstables_after"], 1);
        assert_eq!(parsed["sstables_loaded"], 0);
    }

    #[test]
    fn refresh_loads_new_sstables_from_disk() {
        let source_dir = TempDir::new().unwrap();
        let source_engine = StorageEngine::open(EngineConfig {
            data_directories: vec![source_dir.path().join("data")],
            commitlog: CommitLogConfig {
                directory: source_dir.path().join("commitlog"),
                ..CommitLogConfig::default()
            },
            ..EngineConfig::default()
        })
        .unwrap();
        insert_and_flush(&source_engine, "ks1", "tbl1", b"pk-refresh");

        let (state, target_dir) = test_state_with_engine();
        let target_data_dir = target_dir.path().join("data");
        for entry in fs::read_dir(source_dir.path().join("data")).unwrap() {
            let entry = entry.unwrap();
            if !entry.file_type().unwrap().is_file() {
                continue;
            }
            let file_name = entry.file_name();
            fs::copy(entry.path(), target_data_dir.join(file_name)).unwrap();
        }

        let body = serde_json::to_vec(&json!({"keyspace": "ks1", "table": "tbl1"})).unwrap();
        let resp = handle_refresh_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::OK);
        let collected = {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(resp.into_body().collect()).unwrap().to_bytes()
        };
        let parsed: serde_json::Value = serde_json::from_slice(&collected).unwrap();
        assert_eq!(parsed["sstables_before"], 0);
        assert_eq!(parsed["sstables_after"], 1);
        assert_eq!(parsed["sstables_loaded"], 1);

        let target_engine = state.storage_engine.as_ref().unwrap();
        assert!(
            target_engine
                .read_partition("ks1", "tbl1", b"pk-refresh")
                .is_some()
        );
    }

    #[test]
    fn refresh_without_engine_returns_unavailable() {
        let state = test_state();
        let body = serde_json::to_vec(&json!({"keyspace": "ks1", "table": "tbl1"})).unwrap();
        let resp = handle_refresh_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn drain_flushes_pending_memtable_data() {
        let (state, _dir) = test_state_with_engine();
        let engine = state.storage_engine.as_ref().unwrap();
        let before = engine.stats().flushes_completed;
        assert_eq!(engine.stats().sstable_count, 0);

        let mutation = Mutation {
            keyspace: "ks1".to_string(),
            table: "tbl1".to_string(),
            partition_key: b"pk-drain".to_vec(),
            rows: vec![MutationRow {
                clustering_key: Vec::new(),
                cells: vec![CellMutation {
                    column: "name".to_string(),
                    value: Some(b"value".to_vec()),
                    timestamp: 1,
                    ttl: 0,
                    local_deletion_time: None,
                    is_tombstone: false,
                }],
                is_tombstone: false,
                local_deletion_time: None,
            }],
            timestamp: 1,
            cdc_enabled: false,
            static_cells: Vec::new(),
            partition_tombstone: None,
            range_tombstones: Vec::new(),
        };
        engine.apply_mutation(&mutation).unwrap();

        let resp = handle_drain(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let collected = {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(resp.into_body().collect()).unwrap().to_bytes()
        };
        let parsed: serde_json::Value = serde_json::from_slice(&collected).unwrap();
        assert!(parsed["operation_id"].is_string());
        assert_eq!(parsed["flushes_before"], before);
        assert!(parsed["flushes_executed"].as_u64().unwrap_or(0) >= 1);
        assert!(engine.stats().sstable_count >= 1);
        assert!(engine.read_partition("ks1", "tbl1", b"pk-drain").is_some());
    }
}
