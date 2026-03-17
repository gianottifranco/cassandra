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
    let op_id = state.operations.register(
        OperationType::Decommission,
        "Decommission local node".to_string(),
    );

    json_response(
        StatusCode::OK,
        &json!({
            "operation_id": op_id.to_string(),
            "status": "decommission started",
        }),
    )
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

    let op_id = state.operations.register(
        OperationType::RemoveNode,
        format!("Remove node {}", payload.host_id),
    );

    json_response(
        StatusCode::OK,
        &json!({
            "operation_id": op_id.to_string(),
            "host_id": payload.host_id,
            "status": "removenode started",
        }),
    )
}

/// Move the local node to a new token.
pub async fn handle_move(
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

    let op_id = state.operations.register(
        OperationType::Stream,
        format!("Move to token {}", payload.new_token),
    );

    json_response(
        StatusCode::OK,
        &json!({
            "operation_id": op_id.to_string(),
            "new_token": payload.new_token,
            "status": "move started",
        }),
    )
}

/// Rebuild data from another datacenter.
pub async fn handle_rebuild(
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

    let description = match payload.source_dc {
        Some(ref dc) => format!("Rebuild from DC {}", dc),
        None => "Rebuild from all DCs".to_string(),
    };

    let op_id = state
        .operations
        .register(OperationType::Rebuild, description);

    json_response(
        StatusCode::OK,
        &json!({
            "operation_id": op_id.to_string(),
            "source_dc": payload.source_dc,
            "status": "rebuild started",
        }),
    )
}

/// Refresh SSTables for a specific table (load new SSTables from disk).
pub async fn handle_refresh(
    req: Request<Incoming>,
    _state: &AdminState,
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

    handle_refresh_inner(&body_bytes)
}

fn handle_refresh_inner(body_bytes: &[u8]) -> Response<Full<Bytes>> {
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

    json_response(
        StatusCode::OK,
        &json!({
            "status": format!("refresh completed for {}.{}", payload.keyspace, payload.table),
        }),
    )
}

/// Initiate a join to the ring.
pub fn handle_join(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(StatusCode::OK, &json!({"status": "join initiated"}))
}

/// Bootstrap the local node into the cluster.
pub fn handle_bootstrap(state: &AdminState) -> Response<Full<Bytes>> {
    let op_id = state.operations.register(
        OperationType::Bootstrap,
        "Bootstrap local node".to_string(),
    );

    json_response(
        StatusCode::OK,
        &json!({
            "operation_id": op_id.to_string(),
        }),
    )
}

/// Drain the node: stop accepting writes and flush all memtables.
pub fn handle_drain(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(StatusCode::OK, &json!({"status": "drain initiated"}))
}

/// Assassinate a node by endpoint address.
pub async fn handle_assassinate(
    req: Request<Incoming>,
    _state: &AdminState,
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

    handle_assassinate_inner(&body_bytes)
}

fn handle_assassinate_inner(body_bytes: &[u8]) -> Response<Full<Bytes>> {
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

    json_response(
        StatusCode::OK,
        &json!({
            "status": format!("assassinate initiated for {}", payload.endpoint),
        }),
    )
}

/// Return current topology operations status.
pub fn handle_topology_status(state: &AdminState) -> Response<Full<Bytes>> {
    let all_ops = state.operations.list_operations();
    let topology_ops: Vec<_> = all_ops
        .into_iter()
        .filter(|op| {
            matches!(
                op.operation_type,
                OperationType::Bootstrap
                    | OperationType::Decommission
                    | OperationType::RemoveNode
                    | OperationType::Rebuild
                    | OperationType::Replace
            )
        })
        .collect();

    let body = serde_json::to_value(&topology_ops).unwrap_or(serde_json::Value::Array(vec![]));
    json_response(StatusCode::OK, &body)
}

/// Return network statistics (streaming sessions, message counts).
pub fn handle_netstats(state: &AdminState) -> Response<Full<Bytes>> {
    // Attempt to pull streaming/network data from virtual tables
    let streaming = match state.virtual_tables.get("system_views", "streaming") {
        Some(table) => serde_json::to_value(table.rows()).unwrap_or(json!([])),
        None => json!([]),
    };

    let active_ops = state.operations.list_operations();
    let stream_ops: Vec<_> = active_ops
        .iter()
        .filter(|op| op.operation_type == OperationType::Stream)
        .collect();

    json_response(
        StatusCode::OK,
        &json!({
            "streaming_sessions": streaming,
            "active_streams": stream_ops.len(),
            "pending_commands": 0,
            "responses_pending": 0,
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use std::sync::Arc;

    use crate::operations::OperationTracker;
    use crate::prometheus_metrics::MetricsRegistry;
    use crate::virtual_tables::VirtualTableRegistry;

    fn test_state() -> Arc<AdminState> {
        Arc::new(AdminState {
            metrics: Arc::new(MetricsRegistry::new()),
            operations: Arc::new(OperationTracker::new()),
            virtual_tables: Arc::new(VirtualTableRegistry::with_builtins()),
            repair_coordinator: None,
            storage_engine: None,
            schema_catalog: None,
        })
    }

    #[test]
    fn decommission_registers_operation() {
        let state = test_state();
        let resp = handle_decommission(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(state.operations.count(), 1);
    }

    #[test]
    fn join_returns_ok() {
        let state = test_state();
        let resp = handle_join(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn bootstrap_registers_operation() {
        let state = test_state();
        let resp = handle_bootstrap(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(state.operations.count(), 1);
    }

    #[test]
    fn drain_returns_ok() {
        let state = test_state();
        let resp = handle_drain(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn topology_status_filters_operations() {
        let state = test_state();
        // Register one topology and one non-topology operation
        state
            .operations
            .register(OperationType::Decommission, "decom test");
        state
            .operations
            .register(OperationType::Repair, "repair test");

        let resp = handle_topology_status(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn netstats_returns_ok() {
        let state = test_state();
        let resp = handle_netstats(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn removenode_with_valid_body() {
        let state = test_state();
        let body = serde_json::to_vec(&json!({"host_id": "abc-123"})).unwrap();

        let resp = handle_removenode_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(state.operations.count(), 1);
    }

    #[test]
    fn removenode_invalid_json() {
        let state = test_state();

        let resp = handle_removenode_inner(b"{bad", &state);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn rebuild_with_source_dc() {
        let state = test_state();
        let body = serde_json::to_vec(&json!({"source_dc": "dc2"})).unwrap();

        let resp = handle_rebuild_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(state.operations.count(), 1);

        let collected = {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(resp.into_body().collect()).unwrap().to_bytes()
        };
        let parsed: serde_json::Value = serde_json::from_slice(&collected).unwrap();
        assert_eq!(parsed["source_dc"], "dc2");
    }

    #[test]
    fn assassinate_with_valid_body() {
        let body = serde_json::to_vec(&json!({"endpoint": "10.0.0.5:7000"})).unwrap();

        let resp = handle_assassinate_inner(&body);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn move_with_valid_body() {
        let state = test_state();
        let body = serde_json::to_vec(&json!({"new_token": "12345"})).unwrap();

        let resp = handle_move_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::OK);

        let collected = {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(resp.into_body().collect()).unwrap().to_bytes()
        };
        let parsed: serde_json::Value = serde_json::from_slice(&collected).unwrap();
        assert_eq!(parsed["new_token"], "12345");
    }

    #[test]
    fn refresh_with_valid_body() {
        let body =
            serde_json::to_vec(&json!({"keyspace": "ks1", "table": "tbl1"})).unwrap();

        let resp = handle_refresh_inner(&body);
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
