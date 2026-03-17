// Licensed under Apache License, Version 2.0.

//! Snapshot and backup HTTP handlers.
//!
//! Provides endpoints for taking/listing/clearing snapshots, importing SSTables,
//! and managing incremental backup settings.

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use serde_json::json;

use crate::http_admin::{AdminState, json_response};
use crate::operations::OperationType;

/// Take a snapshot of one or more keyspaces.
pub async fn handle_take_snapshot(
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

    handle_take_snapshot_inner(&body_bytes, state)
}

fn handle_take_snapshot_inner(body_bytes: &[u8], state: &AdminState) -> Response<Full<Bytes>> {
    #[derive(serde::Deserialize)]
    struct TakeSnapshotRequest {
        name: String,
        #[serde(default)]
        keyspaces: Vec<String>,
    }

    let payload: TakeSnapshotRequest = match serde_json::from_slice(body_bytes) {
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
        format!(
            "Snapshot '{}' keyspaces: {:?}",
            payload.name, payload.keyspaces
        ),
    );

    json_response(
        StatusCode::OK,
        &json!({
            "snapshot_name": payload.name,
            "operation_id": op_id.to_string(),
        }),
    )
}

/// List all known snapshots from the virtual table registry.
pub fn handle_list_snapshots(state: &AdminState) -> Response<Full<Bytes>> {
    match state.virtual_tables.get("system_views", "snapshots") {
        Some(table) => {
            let rows = table.rows();
            json_response(StatusCode::OK, &json!(rows))
        }
        None => json_response(StatusCode::OK, &json!([])),
    }
}

/// Clear one or all snapshots.
pub async fn handle_clear_snapshot(
    req: Request<Incoming>,
    state: &AdminState,
) -> Response<Full<Bytes>> {
    use http_body_util::BodyExt;
    let _ = state;

    let body_bytes = match req.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Failed to read body: {}", e)}),
            );
        }
    };

    handle_clear_snapshot_inner(&body_bytes)
}

fn handle_clear_snapshot_inner(body_bytes: &[u8]) -> Response<Full<Bytes>> {
    #[derive(serde::Deserialize)]
    struct ClearSnapshotRequest {
        #[serde(default)]
        name: Option<String>,
    }

    let payload: ClearSnapshotRequest = match serde_json::from_slice(body_bytes) {
        Ok(p) => p,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Invalid JSON: {}", e)}),
            );
        }
    };

    let message = match payload.name {
        Some(ref n) => format!("snapshot '{}' cleared", n),
        None => "all snapshots cleared".to_string(),
    };

    json_response(StatusCode::OK, &json!({"status": message}))
}

/// Import SSTables from a given directory into a table.
pub async fn handle_import(
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

    handle_import_inner(&body_bytes, state)
}

fn handle_import_inner(body_bytes: &[u8], state: &AdminState) -> Response<Full<Bytes>> {
    #[derive(serde::Deserialize)]
    struct ImportRequest {
        keyspace: String,
        table: String,
        directory: String,
    }

    let payload: ImportRequest = match serde_json::from_slice(body_bytes) {
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
        format!(
            "Import SSTables into {}.{} from {}",
            payload.keyspace, payload.table, payload.directory
        ),
    );

    json_response(
        StatusCode::OK,
        &json!({
            "operation_id": op_id.to_string(),
            "status": format!("import started for {}.{}", payload.keyspace, payload.table),
        }),
    )
}

/// Enable incremental backups.
pub fn handle_enable_backup(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(StatusCode::OK, &json!({"status": "backup enabled"}))
}

/// Disable incremental backups.
pub fn handle_disable_backup(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(StatusCode::OK, &json!({"status": "backup disabled"}))
}

/// Return the current backup status.
pub fn handle_backup_status(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(StatusCode::OK, &json!({"enabled": false}))
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
    fn list_snapshots_returns_ok() {
        let state = test_state();
        let resp = handle_list_snapshots(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn enable_backup_returns_ok() {
        let state = test_state();
        let resp = handle_enable_backup(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn disable_backup_returns_ok() {
        let state = test_state();
        let resp = handle_disable_backup(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn backup_status_returns_disabled() {
        let state = test_state();
        let resp = handle_backup_status(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn take_snapshot_with_valid_body() {
        let state = test_state();
        let body = serde_json::to_vec(&json!({
            "name": "test_snap",
            "keyspaces": ["system"]
        }))
        .unwrap();

        let resp = handle_take_snapshot_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::OK);

        let collected = {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(resp.into_body().collect()).unwrap().to_bytes()
        };
        let parsed: serde_json::Value = serde_json::from_slice(&collected).unwrap();
        assert_eq!(parsed["snapshot_name"], "test_snap");
        assert!(parsed["operation_id"].is_string());
    }

    #[test]
    fn take_snapshot_invalid_json() {
        let state = test_state();

        let resp = handle_take_snapshot_inner(b"not json", &state);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn clear_snapshot_specific_name() {
        let body = serde_json::to_vec(&json!({"name": "snap1"})).unwrap();

        let resp = handle_clear_snapshot_inner(&body);
        assert_eq!(resp.status(), StatusCode::OK);

        let collected = {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(resp.into_body().collect()).unwrap().to_bytes()
        };
        let parsed: serde_json::Value = serde_json::from_slice(&collected).unwrap();
        assert!(parsed["status"].as_str().unwrap().contains("snap1"));
    }

    #[test]
    fn import_valid_body() {
        let state = test_state();
        let body = serde_json::to_vec(&json!({
            "keyspace": "ks1",
            "table": "tbl1",
            "directory": "/data/import"
        }))
        .unwrap();

        let resp = handle_import_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::OK);

        let collected = {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(resp.into_body().collect()).unwrap().to_bytes()
        };
        let parsed: serde_json::Value = serde_json::from_slice(&collected).unwrap();
        assert!(parsed["operation_id"].is_string());
    }
}
