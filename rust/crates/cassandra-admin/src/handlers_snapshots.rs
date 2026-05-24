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
use std::path::Path;

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
        #[serde(default)]
        table: Option<String>,
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

    let Some(engine) = state.storage_engine.as_ref() else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({"error": "storage engine not configured"}),
        );
    };

    if payload.keyspaces.is_empty() {
        return json_response(
            StatusCode::BAD_REQUEST,
            &json!({"error": "keyspaces must not be empty"}),
        );
    }

    let mut targets = Vec::<(String, String)>::new();
    if let Some(table) = payload.table.clone() {
        for keyspace in &payload.keyspaces {
            targets.push((keyspace.clone(), table.clone()));
        }
    } else {
        let Some(catalog) = state.schema_catalog.as_ref() else {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": "schema catalog is required when snapshot table is omitted"}),
            );
        };
        let schema = catalog.read().snapshot();
        for keyspace in &payload.keyspaces {
            let Some(ks_meta) = schema.keyspace(keyspace) else {
                return json_response(
                    StatusCode::BAD_REQUEST,
                    &json!({"error": format!("unknown keyspace '{keyspace}'")}),
                );
            };
            for table_name in ks_meta.tables.keys() {
                targets.push((keyspace.clone(), table_name.clone()));
            }
        }
    }

    if targets.is_empty() {
        return json_response(
            StatusCode::BAD_REQUEST,
            &json!({"error": "no tables resolved for snapshot"}),
        );
    }

    if let Err(err) = engine.flush_all() {
        return json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &json!({"error": format!("failed to flush before snapshot: {err}")}),
        );
    }

    let op_id = state.operations.register(
        OperationType::Stream,
        format!("Snapshot '{}' targets: {:?}", payload.name, targets),
    );

    let mut created = Vec::new();
    for (keyspace, table) in &targets {
        match engine.snapshot(&payload.name, keyspace, table, None) {
            Ok(manifest) => {
                created.push(json!({
                    "name": manifest.name,
                    "keyspace": manifest.keyspace,
                    "table": manifest.table,
                    "created_at": manifest.created_at,
                    "files_count": manifest.files.len(),
                    "true_size_bytes": engine.snapshot_size_bytes(&manifest.name),
                }));
            }
            Err(err) => {
                state.operations.update(&op_id, "FAILED", 100);
                state.operations.remove(&op_id);
                return json_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &json!({"error": format!("failed to create snapshot: {err}")}),
                );
            }
        }
    }

    state.operations.update(&op_id, "COMPLETED", 100);
    state.operations.remove(&op_id);

    json_response(
        StatusCode::OK,
        &json!({
            "snapshot_name": payload.name,
            "operation_id": op_id.to_string(),
            "targets": created,
        }),
    )
}

/// List all known snapshots from storage engine (fallback: virtual table registry).
pub fn handle_list_snapshots(state: &AdminState) -> Response<Full<Bytes>> {
    if let Some(engine) = state.storage_engine.as_ref() {
        return match engine.list_snapshots() {
            Ok(manifests) => {
                let rows = manifests
                    .into_iter()
                    .map(|manifest| {
                        json!({
                            "name": manifest.name,
                            "keyspace": manifest.keyspace,
                            "table": manifest.table,
                            "created_at": manifest.created_at,
                            "files_count": manifest.files.len(),
                            "true_size_bytes": engine.snapshot_size_bytes(&manifest.name),
                        })
                    })
                    .collect::<Vec<_>>();
                json_response(StatusCode::OK, &json!(rows))
            }
            Err(err) => json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &json!({"error": format!("failed to list snapshots: {err}")}),
            ),
        };
    }

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

    let query_name = req
        .uri()
        .query()
        .and_then(|query| query_param(query, "name"));

    let body_bytes = match req.into_body().collect().await {
        Ok(collected) => collected.to_bytes(),
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Failed to read body: {}", e)}),
            );
        }
    };

    let body_name = match parse_clear_snapshot_request(&body_bytes) {
        Ok(name) => name,
        Err(err) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Invalid JSON: {err}")}),
            );
        }
    };

    let snapshot_name = query_name.or(body_name);
    handle_clear_snapshot_inner(snapshot_name.as_deref(), state)
}

fn parse_clear_snapshot_request(body_bytes: &[u8]) -> Result<Option<String>, serde_json::Error> {
    #[derive(serde::Deserialize)]
    struct ClearSnapshotRequest {
        #[serde(default)]
        name: Option<String>,
    }
    if body_bytes.is_empty() {
        return Ok(None);
    }
    serde_json::from_slice::<ClearSnapshotRequest>(body_bytes).map(|payload| payload.name)
}

fn query_param(query: &str, key: &str) -> Option<String> {
    query.split('&').find_map(|pair| {
        let mut parts = pair.splitn(2, '=');
        let k = parts.next()?;
        if k != key {
            return None;
        }
        parts.next().map(|value| value.to_string())
    })
}

fn handle_clear_snapshot_inner(
    snapshot_name: Option<&str>,
    state: &AdminState,
) -> Response<Full<Bytes>> {
    let Some(engine) = state.storage_engine.as_ref() else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({"error": "storage engine not configured"}),
        );
    };

    let message = if let Some(name) = snapshot_name {
        if let Err(err) = engine.delete_snapshot(name) {
            return json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &json!({"error": format!("failed to delete snapshot '{name}': {err}")}),
            );
        }
        format!("snapshot '{name}' cleared")
    } else {
        match engine.clear_snapshots() {
            Ok(removed) => format!("all snapshots cleared ({removed} removed)"),
            Err(err) => {
                return json_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &json!({"error": format!("failed to clear snapshots: {err}")}),
                );
            }
        }
    };

    json_response(StatusCode::OK, &json!({"status": message}))
}

/// Import SSTables from a given directory into a table.
pub async fn handle_import(req: Request<Incoming>, state: &AdminState) -> Response<Full<Bytes>> {
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

/// Bulk-load SSTables discovered under a source directory.
pub async fn handle_sstableloader(
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

    handle_sstableloader_inner(&body_bytes, state)
}

/// Restore files from a named snapshot back into active SSTables.
pub async fn handle_restore_snapshot(
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

    handle_restore_snapshot_inner(&body_bytes, state)
}

fn handle_restore_snapshot_inner(body_bytes: &[u8], state: &AdminState) -> Response<Full<Bytes>> {
    #[derive(serde::Deserialize)]
    struct RestoreSnapshotRequest {
        name: String,
    }

    let payload: RestoreSnapshotRequest = match serde_json::from_slice(body_bytes) {
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

    let op_id = state.operations.register(
        OperationType::Stream,
        format!("Restore snapshot '{}'", payload.name),
    );
    let result = engine.restore_snapshot(&payload.name);
    match result {
        Ok(manifest) => {
            state.operations.update(&op_id, "COMPLETED", 100);
            state.operations.remove(&op_id);
            json_response(
                StatusCode::OK,
                &json!({
                    "snapshot_name": manifest.name,
                    "keyspace": manifest.keyspace,
                    "table": manifest.table,
                    "files_count": manifest.files.len(),
                    "status": "restored",
                }),
            )
        }
        Err(err) => {
            state.operations.update(&op_id, "FAILED", 100);
            state.operations.remove(&op_id);
            json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &json!({"error": format!("failed to restore snapshot '{}': {err}", payload.name)}),
            )
        }
    }
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

    let Some(engine) = state.storage_engine.as_ref() else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({"error": "storage engine not configured"}),
        );
    };

    let directory = Path::new(&payload.directory);
    if !directory.exists() {
        return json_response(
            StatusCode::BAD_REQUEST,
            &json!({"error": format!("directory '{}' does not exist", payload.directory)}),
        );
    }
    if !directory.is_dir() {
        return json_response(
            StatusCode::BAD_REQUEST,
            &json!({"error": format!("path '{}' is not a directory", payload.directory)}),
        );
    }

    let op_id = state.operations.register(
        OperationType::Stream,
        format!(
            "Import SSTables into {}.{} from {}",
            payload.keyspace, payload.table, payload.directory
        ),
    );

    match engine.import_sstables(&payload.keyspace, &payload.table, directory) {
        Ok(imported) => {
            state.operations.update(&op_id, "COMPLETED", 100);
            state.operations.remove(&op_id);
            json_response(
                StatusCode::OK,
                &json!({
                    "operation_id": op_id.to_string(),
                    "status": format!("import completed for {}.{}", payload.keyspace, payload.table),
                    "imported_sstables": imported.imported_sstables,
                    "copied_files": imported.copied_files,
                }),
            )
        }
        Err(err) => {
            state.operations.update(&op_id, "FAILED", 100);
            state.operations.remove(&op_id);
            json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &json!({"error": format!("failed to import SSTables: {err}")}),
            )
        }
    }
}

fn handle_sstableloader_inner(body_bytes: &[u8], state: &AdminState) -> Response<Full<Bytes>> {
    #[derive(serde::Deserialize)]
    struct SSTableLoaderRequest {
        directory: String,
        #[serde(default)]
        nodes: Vec<String>,
    }

    let payload: SSTableLoaderRequest = match serde_json::from_slice(body_bytes) {
        Ok(p) => p,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Invalid JSON: {}", e)}),
            );
        }
    };

    if let Err(err) = validate_sstableloader_nodes(&payload.nodes) {
        return json_response(StatusCode::BAD_REQUEST, &json!({"error": err}));
    }

    let Some(engine) = state.storage_engine.as_ref() else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({"error": "storage engine not configured"}),
        );
    };

    let directory = Path::new(&payload.directory);
    if !directory.exists() {
        return json_response(
            StatusCode::BAD_REQUEST,
            &json!({"error": format!("directory '{}' does not exist", payload.directory)}),
        );
    }
    if !directory.is_dir() {
        return json_response(
            StatusCode::BAD_REQUEST,
            &json!({"error": format!("path '{}' is not a directory", payload.directory)}),
        );
    }

    let op_id = state.operations.register(
        OperationType::Stream,
        format!("SSTableLoader import from {}", payload.directory),
    );

    match engine.import_sstable_directory(directory) {
        Ok(imported) => {
            state.operations.update(&op_id, "COMPLETED", 100);
            state.operations.remove(&op_id);
            let tables = imported
                .imported_tables
                .into_iter()
                .map(|table| {
                    json!({
                        "keyspace": table.keyspace,
                        "table": table.table,
                        "imported_sstables": table.imported_sstables,
                        "copied_files": table.copied_files,
                    })
                })
                .collect::<Vec<_>>();
            json_response(
                StatusCode::OK,
                &json!({
                    "operation_id": op_id.to_string(),
                    "status": "sstableloader completed",
                    "directory": payload.directory,
                    "nodes": payload.nodes,
                    "imported_tables": tables,
                    "imported_sstables": imported.imported_sstables,
                    "copied_files": imported.copied_files,
                }),
            )
        }
        Err(err) => {
            state.operations.update(&op_id, "FAILED", 100);
            state.operations.remove(&op_id);
            json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &json!({"error": format!("failed to load SSTables: {err}")}),
            )
        }
    }
}

fn validate_sstableloader_nodes(nodes: &[String]) -> Result<(), String> {
    if nodes.len() > 1 {
        return Err("sstableloader currently supports a single local node target".to_string());
    }

    if let Some(node) = nodes.first() {
        let normalized = node.trim().to_ascii_lowercase();
        let is_local = normalized == "localhost"
            || normalized == "127.0.0.1"
            || normalized == "::1"
            || normalized.starts_with("127.0.0.1:")
            || normalized.starts_with("[::1]:");
        if !is_local {
            return Err(format!(
                "remote node target '{}' is not supported by local sstableloader runtime",
                node
            ));
        }
    }

    Ok(())
}

/// Enable incremental backups.
pub fn handle_enable_backup(state: &AdminState) -> Response<Full<Bytes>> {
    let Some(engine) = state.storage_engine.as_ref() else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({"error": "storage engine not configured"}),
        );
    };
    engine.set_incremental_backup_enabled(true);
    json_response(
        StatusCode::OK,
        &json!({
            "status": "backup enabled",
            "enabled": true,
            "directory": engine.incremental_backup_directory().display().to_string(),
        }),
    )
}

/// Disable incremental backups.
pub fn handle_disable_backup(state: &AdminState) -> Response<Full<Bytes>> {
    let Some(engine) = state.storage_engine.as_ref() else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({"error": "storage engine not configured"}),
        );
    };
    engine.set_incremental_backup_enabled(false);
    json_response(
        StatusCode::OK,
        &json!({
            "status": "backup disabled",
            "enabled": false,
            "directory": engine.incremental_backup_directory().display().to_string(),
        }),
    )
}

/// Return the current backup status.
pub fn handle_backup_status(state: &AdminState) -> Response<Full<Bytes>> {
    let Some(engine) = state.storage_engine.as_ref() else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({"error": "storage engine not configured"}),
        );
    };
    json_response(
        StatusCode::OK,
        &json!({
            "enabled": engine.is_incremental_backup_enabled(),
            "directory": engine.incremental_backup_directory().display().to_string(),
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use parking_lot::RwLock;
    use std::sync::Arc;
    use tempfile::TempDir;

    use crate::operations::OperationTracker;
    use crate::prometheus_metrics::MetricsRegistry;
    use crate::virtual_tables::VirtualTableRegistry;
    use cassandra_schema::SchemaCatalog;
    use cassandra_storage::commitlog::{CellMutation, CommitLogConfig, Mutation, MutationRow};
    use cassandra_storage::engine::{EngineConfig, StorageEngine};

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

    fn body_json(resp: Response<Full<Bytes>>) -> serde_json::Value {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let collected = rt.block_on(resp.into_body().collect()).unwrap().to_bytes();
        serde_json::from_slice(&collected).unwrap()
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
    fn list_snapshots_returns_ok() {
        let state = test_state();
        let resp = handle_list_snapshots(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn enable_backup_without_engine_returns_unavailable() {
        let state = test_state();
        let resp = handle_enable_backup(&state);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn disable_backup_without_engine_returns_unavailable() {
        let state = test_state();
        let resp = handle_disable_backup(&state);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn backup_status_without_engine_returns_unavailable() {
        let state = test_state();
        let resp = handle_backup_status(&state);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn take_snapshot_with_valid_body() {
        let (state, _dir) = test_state_with_engine();
        let engine = state.storage_engine.as_ref().unwrap();
        insert_and_flush(engine, "ks1", "tbl1", b"pk1");
        let body = serde_json::to_vec(&json!({
            "name": "test_snap",
            "keyspaces": ["ks1"],
            "table": "tbl1"
        }))
        .unwrap();

        let resp = handle_take_snapshot_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::OK);
        let parsed = body_json(resp);
        assert_eq!(parsed["snapshot_name"], "test_snap");
        assert!(parsed["operation_id"].is_string());
        assert_eq!(parsed["targets"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn take_snapshot_invalid_json() {
        let state = test_state();

        let resp = handle_take_snapshot_inner(b"not json", &state);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn clear_snapshot_specific_name() {
        let (state, _dir) = test_state_with_engine();
        let engine = state.storage_engine.as_ref().unwrap();
        insert_and_flush(engine, "ks1", "tbl1", b"pk1");
        engine.snapshot("snap1", "ks1", "tbl1", None).unwrap();

        let resp = handle_clear_snapshot_inner(Some("snap1"), &state);
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(engine.list_snapshots().unwrap().is_empty());
        let parsed = body_json(resp);
        assert!(parsed["status"].as_str().unwrap().contains("snap1"));
    }

    #[test]
    fn list_snapshots_from_storage_engine() {
        let (state, _dir) = test_state_with_engine();
        let engine = state.storage_engine.as_ref().unwrap();
        insert_and_flush(engine, "ks1", "tbl1", b"pk1");
        engine.snapshot("snap_list", "ks1", "tbl1", None).unwrap();

        let resp = handle_list_snapshots(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let parsed = body_json(resp);
        let rows = parsed.as_array().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0]["name"], "snap_list");
        assert_eq!(rows[0]["keyspace"], "ks1");
        assert_eq!(rows[0]["table"], "tbl1");
    }

    #[test]
    fn backup_toggle_and_status_use_engine_runtime_setting() {
        let (state, _dir) = test_state_with_engine();

        let status_before = body_json(handle_backup_status(&state));
        assert_eq!(status_before["enabled"], false);

        let enable = body_json(handle_enable_backup(&state));
        assert_eq!(enable["enabled"], true);

        let status_after_enable = body_json(handle_backup_status(&state));
        assert_eq!(status_after_enable["enabled"], true);

        let disable = body_json(handle_disable_backup(&state));
        assert_eq!(disable["enabled"], false);

        let status_after_disable = body_json(handle_backup_status(&state));
        assert_eq!(status_after_disable["enabled"], false);
    }

    #[test]
    fn restore_snapshot_restores_data_after_truncate() {
        let (state, _dir) = test_state_with_engine();
        let engine = state.storage_engine.as_ref().unwrap();
        insert_and_flush(engine, "ks1", "tbl1", b"pk1");
        engine
            .snapshot("restore_snap", "ks1", "tbl1", None)
            .unwrap();
        engine.truncate_table("ks1", "tbl1").unwrap();
        assert!(engine.read_partition("ks1", "tbl1", b"pk1").is_none());

        let body = serde_json::to_vec(&json!({
            "name": "restore_snap"
        }))
        .unwrap();
        let resp = handle_restore_snapshot_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::OK);
        let parsed = body_json(resp);
        assert_eq!(parsed["snapshot_name"], "restore_snap");
        assert_eq!(parsed["status"], "restored");

        assert!(engine.read_partition("ks1", "tbl1", b"pk1").is_some());
    }

    #[test]
    fn restore_snapshot_invalid_json() {
        let state = test_state();
        let resp = handle_restore_snapshot_inner(b"not-json", &state);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn import_valid_body() {
        let (state, _target_dir) = test_state_with_engine();
        let target_engine = state.storage_engine.as_ref().unwrap();
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
        insert_and_flush(&source_engine, "ks1", "tbl1", b"pk-import");

        let body = serde_json::to_vec(&json!({
            "keyspace": "ks1",
            "table": "tbl1",
            "directory": source_dir.path().join("data")
        }))
        .unwrap();

        let resp = handle_import_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::OK);

        let parsed = body_json(resp);
        assert!(parsed["operation_id"].is_string());
        assert_eq!(parsed["imported_sstables"], 1);
        assert!(parsed["copied_files"].as_u64().unwrap_or(0) > 0);
        assert!(
            target_engine
                .read_partition("ks1", "tbl1", b"pk-import")
                .is_some()
        );
    }

    #[test]
    fn import_without_engine_returns_unavailable() {
        let state = test_state();
        let body = serde_json::to_vec(&json!({
            "keyspace": "ks1",
            "table": "tbl1",
            "directory": "/tmp/unused"
        }))
        .unwrap();

        let resp = handle_import_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn sstableloader_imports_directory_tables() {
        let (state, _target_dir) = test_state_with_engine();
        let target_engine = state.storage_engine.as_ref().unwrap();
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
        insert_and_flush(&source_engine, "ks1", "tbl1", b"pk-import-1");
        insert_and_flush(&source_engine, "ks1", "tbl2", b"pk-import-2");

        let body = serde_json::to_vec(&json!({
            "directory": source_dir.path().join("data"),
            "nodes": ["127.0.0.1"]
        }))
        .unwrap();
        let resp = handle_sstableloader_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::OK);
        let parsed = body_json(resp);
        assert!(parsed["operation_id"].is_string());
        assert_eq!(parsed["status"], "sstableloader completed");
        assert_eq!(parsed["imported_tables"].as_array().unwrap().len(), 2);
        assert_eq!(parsed["imported_sstables"], 2);
        assert!(
            target_engine
                .read_partition("ks1", "tbl1", b"pk-import-1")
                .is_some()
        );
        assert!(
            target_engine
                .read_partition("ks1", "tbl2", b"pk-import-2")
                .is_some()
        );
    }

    #[test]
    fn sstableloader_without_engine_returns_unavailable() {
        let state = test_state();
        let body = serde_json::to_vec(&json!({
            "directory": "/tmp/unused",
            "nodes": []
        }))
        .unwrap();
        let resp = handle_sstableloader_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn sstableloader_rejects_multiple_nodes() {
        let (state, _target_dir) = test_state_with_engine();
        let source_dir = TempDir::new().unwrap();
        let body = serde_json::to_vec(&json!({
            "directory": source_dir.path(),
            "nodes": ["127.0.0.1", "127.0.0.2"]
        }))
        .unwrap();
        let resp = handle_sstableloader_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let parsed = body_json(resp);
        assert!(
            parsed["error"]
                .as_str()
                .unwrap_or_default()
                .contains("single local node target")
        );
    }

    #[test]
    fn sstableloader_rejects_remote_node_target() {
        let (state, _target_dir) = test_state_with_engine();
        let source_dir = TempDir::new().unwrap();
        let body = serde_json::to_vec(&json!({
            "directory": source_dir.path(),
            "nodes": ["10.0.0.1:7000"]
        }))
        .unwrap();
        let resp = handle_sstableloader_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        let parsed = body_json(resp);
        assert!(
            parsed["error"]
                .as_str()
                .unwrap_or_default()
                .contains("remote node target")
        );
    }

    #[test]
    fn sstableloader_accepts_local_node_target() {
        let (state, _target_dir) = test_state_with_engine();
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
        insert_and_flush(&source_engine, "ks1", "tbl1", b"pk-local-node");
        let body = serde_json::to_vec(&json!({
            "directory": source_dir.path().join("data"),
            "nodes": ["127.0.0.1:7000"]
        }))
        .unwrap();
        let resp = handle_sstableloader_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::OK);
    }
}
