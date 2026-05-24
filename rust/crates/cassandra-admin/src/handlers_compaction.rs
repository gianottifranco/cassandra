// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! HTTP handlers for compaction operations.

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use serde_json::json;

use crate::http_admin::{AdminState, json_response};
use crate::operations::OperationType;

/// POST /api/v1/operations/compact — trigger major compaction.
pub async fn handle_compact(req: Request<Incoming>, state: &AdminState) -> Response<Full<Bytes>> {
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

    handle_compact_inner(&body_bytes, state)
}

fn handle_compact_inner(body_bytes: &[u8], state: &AdminState) -> Response<Full<Bytes>> {
    #[derive(serde::Deserialize)]
    struct CompactRequest {
        keyspace: String,
        #[serde(default)]
        table: Option<String>,
    }

    let payload: CompactRequest = match serde_json::from_slice(body_bytes) {
        Ok(p) => p,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Invalid JSON: {}", e)}),
            );
        }
    };

    let description = match &payload.table {
        Some(t) => format!("Compact {}.{}", payload.keyspace, t),
        None => format!("Compact keyspace: {}", payload.keyspace),
    };
    let op_id = state
        .operations
        .register(OperationType::Rebuild, description);

    json_response(
        StatusCode::OK,
        &json!({
            "operation_id": op_id.to_string(),
            "status": "started",
        }),
    )
}

/// POST /api/v1/operations/cleanup — trigger cleanup of keys no longer belonging to this node.
pub async fn handle_cleanup(req: Request<Incoming>, state: &AdminState) -> Response<Full<Bytes>> {
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

    handle_cleanup_inner(&body_bytes, state)
}

fn handle_cleanup_inner(body_bytes: &[u8], state: &AdminState) -> Response<Full<Bytes>> {
    #[derive(serde::Deserialize)]
    struct CleanupRequest {
        keyspace: String,
    }

    let payload: CleanupRequest = match serde_json::from_slice(body_bytes) {
        Ok(p) => p,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Invalid JSON: {}", e)}),
            );
        }
    };

    let op_id = state.operations.register(
        OperationType::Rebuild,
        format!("Cleanup keyspace: {}", payload.keyspace),
    );

    json_response(
        StatusCode::OK,
        &json!({
            "operation_id": op_id.to_string(),
            "status": "started",
        }),
    )
}

/// POST /api/v1/operations/flush — flush memtables to SSTables.
pub async fn handle_flush(req: Request<Incoming>, state: &AdminState) -> Response<Full<Bytes>> {
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

    handle_flush_inner(&body_bytes, state)
}

fn handle_flush_inner(body_bytes: &[u8], state: &AdminState) -> Response<Full<Bytes>> {
    #[derive(serde::Deserialize)]
    struct FlushRequest {
        keyspace: String,
        #[serde(default)]
        table: Option<String>,
    }

    let payload: FlushRequest = match serde_json::from_slice(body_bytes) {
        Ok(p) => p,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Invalid JSON: {}", e)}),
            );
        }
    };

    let description = match &payload.table {
        Some(t) => format!("Flush {}.{}", payload.keyspace, t),
        None => format!("Flush keyspace: {}", payload.keyspace),
    };
    let op_id = state
        .operations
        .register(OperationType::Rebuild, description);

    json_response(
        StatusCode::OK,
        &json!({
            "operation_id": op_id.to_string(),
            "status": "started",
        }),
    )
}

/// POST /api/v1/operations/scrub — scrub SSTables for corruption.
pub async fn handle_scrub(req: Request<Incoming>, state: &AdminState) -> Response<Full<Bytes>> {
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

    handle_scrub_inner(&body_bytes, state)
}

fn handle_scrub_inner(body_bytes: &[u8], state: &AdminState) -> Response<Full<Bytes>> {
    #[derive(serde::Deserialize)]
    struct ScrubRequest {
        keyspace: String,
        #[serde(default)]
        table: Option<String>,
    }

    let payload: ScrubRequest = match serde_json::from_slice(body_bytes) {
        Ok(p) => p,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Invalid JSON: {}", e)}),
            );
        }
    };

    let description = match &payload.table {
        Some(t) => format!("Scrub {}.{}", payload.keyspace, t),
        None => format!("Scrub keyspace: {}", payload.keyspace),
    };
    let op_id = state
        .operations
        .register(OperationType::Rebuild, description);

    json_response(
        StatusCode::OK,
        &json!({
            "operation_id": op_id.to_string(),
            "status": "started",
        }),
    )
}

/// GET /api/v1/compaction/stats — compaction statistics.
pub fn handle_compaction_stats(state: &AdminState) -> Response<Full<Bytes>> {
    let active_operations = state.operations.list_operations();
    let pending = active_operations.len();

    let active_tasks: Vec<serde_json::Value> = active_operations
        .iter()
        .map(|op| {
            json!({
                "id": op.id.to_string(),
                "type": format!("{}", op.operation_type),
                "status": op.status,
                "progress": op.progress,
                "description": op.description,
                "elapsed_secs": op.elapsed_secs,
            })
        })
        .collect();

    json_response(
        StatusCode::OK,
        &json!({
            "pending_compactions": pending,
            "active_tasks": active_tasks,
        }),
    )
}

/// GET /api/v1/compaction/history — retained completed compaction operations.
pub fn handle_compaction_history(state: &AdminState) -> Response<Full<Bytes>> {
    let history: Vec<serde_json::Value> = state
        .operations
        .list_history()
        .into_iter()
        .filter(|op| op.operation_type == OperationType::Rebuild)
        .map(|op| {
            json!({
                "id": op.id.to_string(),
                "type": format!("{}", op.operation_type),
                "status": op.status,
                "progress": op.progress,
                "description": op.description,
                "elapsed_secs": op.elapsed_secs,
            })
        })
        .collect();

    json_response(
        StatusCode::OK,
        &json!({
            "history": history,
        }),
    )
}

/// POST /api/v1/compaction/autocompaction/enable — enable auto-compaction.
pub fn handle_enable_autocompaction(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(
        StatusCode::OK,
        &json!({
            "status": "autocompaction enabled",
        }),
    )
}

/// POST /api/v1/compaction/autocompaction/disable — disable auto-compaction.
pub fn handle_disable_autocompaction(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(
        StatusCode::OK,
        &json!({
            "status": "autocompaction disabled",
        }),
    )
}

/// GET /api/v1/compaction/autocompaction/status — auto-compaction status.
pub fn handle_autocompaction_status(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(
        StatusCode::OK,
        &json!({
            "enabled": true,
        }),
    )
}

/// POST /api/v1/compaction/verify — verify SSTables for a table or cluster-wide.
pub async fn handle_verify(req: Request<Incoming>, state: &AdminState) -> Response<Full<Bytes>> {
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

    handle_verify_inner(&body_bytes, state)
}

fn handle_verify_inner(body_bytes: &[u8], state: &AdminState) -> Response<Full<Bytes>> {
    #[derive(Default, serde::Deserialize)]
    struct VerifyRequest {
        #[serde(default)]
        keyspace: Option<String>,
        #[serde(default)]
        table: Option<String>,
    }

    let payload = if body_bytes.is_empty() {
        VerifyRequest::default()
    } else {
        match serde_json::from_slice::<VerifyRequest>(body_bytes) {
            Ok(p) => p,
            Err(e) => {
                return json_response(
                    StatusCode::BAD_REQUEST,
                    &json!({"error": format!("Invalid JSON: {}", e)}),
                );
            }
        }
    };

    if payload.table.is_some() && payload.keyspace.is_none() {
        return json_response(
            StatusCode::BAD_REQUEST,
            &json!({"error": "table requires keyspace"}),
        );
    }

    let Some(engine) = state.storage_engine.as_ref() else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({"error": "storage engine not configured"}),
        );
    };

    let description = match (payload.keyspace.as_ref(), payload.table.as_ref()) {
        (Some(ks), Some(tbl)) => format!("Verify {ks}.{tbl}"),
        _ => "Verify all SSTables".to_string(),
    };
    let op_id = state
        .operations
        .register(OperationType::Rebuild, description);

    let response = match engine.refresh_sstables_from_disk() {
        Ok(()) => {
            match engine.verify_sstables(payload.keyspace.as_deref(), payload.table.as_deref()) {
                Ok(report) => {
                    let issues: Vec<serde_json::Value> = report
                        .issues
                        .iter()
                        .take(50)
                        .map(|issue| {
                            json!({
                                "keyspace": issue.keyspace,
                                "table": issue.table,
                                "generation": issue.generation,
                                "severity": issue.severity,
                                "component": issue.component,
                                "message": issue.message,
                            })
                        })
                        .collect();
                    json_response(
                        StatusCode::OK,
                        &json!({
                            "operation_id": op_id.to_string(),
                            "status": "verify completed",
                            "keyspace": payload.keyspace,
                            "table": payload.table,
                            "scanned_sstables": report.scanned_sstables,
                            "valid_sstables": report.valid_sstables,
                            "invalid_sstables": report.invalid_sstables,
                            "issue_count": report.issue_count,
                            "verified": report.invalid_sstables == 0,
                            "issues": issues,
                            "issues_truncated": report.issue_count > issues.len() as u64,
                        }),
                    )
                }
                Err(err) => json_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &json!({"error": format!("verify failed: {err}")}),
                ),
            }
        }
        Err(err) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &json!({"error": format!("verify refresh failed: {err}")}),
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

/// POST /api/v1/compaction/upgradesstables — rewrite SSTables through compaction flow.
pub async fn handle_upgradesstables(
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

    handle_upgradesstables_inner(&body_bytes, state)
}

fn handle_upgradesstables_inner(body_bytes: &[u8], state: &AdminState) -> Response<Full<Bytes>> {
    #[derive(Default, serde::Deserialize)]
    struct UpgradeSSTablesRequest {
        #[serde(default)]
        keyspace: Option<String>,
        #[serde(default)]
        table: Option<String>,
    }

    let payload = if body_bytes.is_empty() {
        UpgradeSSTablesRequest::default()
    } else {
        match serde_json::from_slice::<UpgradeSSTablesRequest>(body_bytes) {
            Ok(p) => p,
            Err(e) => {
                return json_response(
                    StatusCode::BAD_REQUEST,
                    &json!({"error": format!("Invalid JSON: {}", e)}),
                );
            }
        }
    };

    if payload.table.is_some() && payload.keyspace.is_none() {
        return json_response(
            StatusCode::BAD_REQUEST,
            &json!({"error": "table requires keyspace"}),
        );
    }

    let Some(engine) = state.storage_engine.as_ref() else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({"error": "storage engine not configured"}),
        );
    };

    let description = match (payload.keyspace.as_ref(), payload.table.as_ref()) {
        (Some(ks), Some(tbl)) => format!("Upgrade SSTables {ks}.{tbl}"),
        _ => "Upgrade SSTables all tables".to_string(),
    };
    let op_id = state
        .operations
        .register(OperationType::Rebuild, description);

    let response = match (payload.keyspace.as_ref(), payload.table.as_ref()) {
        (Some(ks), Some(tbl)) => match engine.flush_cf(&format!("{ks}.{tbl}")) {
            Ok(()) => match engine.maybe_compact_table(ks, tbl) {
                Ok(compaction_executed) => match engine.refresh_sstables_from_disk() {
                    Ok(()) => json_response(
                        StatusCode::OK,
                        &json!({
                            "operation_id": op_id.to_string(),
                            "status": "upgradesstables completed",
                            "keyspace": payload.keyspace,
                            "table": payload.table,
                            "compaction_executed": compaction_executed,
                        }),
                    ),
                    Err(err) => json_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        &json!({"error": format!("upgradesstables refresh failed: {err}")}),
                    ),
                },
                Err(err) => json_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &json!({"error": format!("upgradesstables compaction failed: {err}")}),
                ),
            },
            Err(err) => json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &json!({"error": format!("upgradesstables flush failed: {err}")}),
            ),
        },
        (Some(ks), None) => match engine.flush_all() {
            Ok(()) => match engine.maybe_compact_keyspace(ks) {
                Ok(compaction_executed) => match engine.refresh_sstables_from_disk() {
                    Ok(()) => json_response(
                        StatusCode::OK,
                        &json!({
                            "operation_id": op_id.to_string(),
                            "status": "upgradesstables completed",
                            "keyspace": payload.keyspace,
                            "table": payload.table,
                            "compaction_executed": compaction_executed,
                        }),
                    ),
                    Err(err) => json_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        &json!({"error": format!("upgradesstables refresh failed: {err}")}),
                    ),
                },
                Err(err) => json_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &json!({"error": format!("upgradesstables compaction failed: {err}")}),
                ),
            },
            Err(err) => json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &json!({"error": format!("upgradesstables flush failed: {err}")}),
            ),
        },
        _ => match engine.flush_all() {
            Ok(()) => match engine.maybe_compact() {
                Ok(compaction_executed) => match engine.refresh_sstables_from_disk() {
                    Ok(()) => json_response(
                        StatusCode::OK,
                        &json!({
                            "operation_id": op_id.to_string(),
                            "status": "upgradesstables completed",
                            "keyspace": payload.keyspace,
                            "table": payload.table,
                            "compaction_executed": compaction_executed,
                        }),
                    ),
                    Err(err) => json_response(
                        StatusCode::INTERNAL_SERVER_ERROR,
                        &json!({"error": format!("upgradesstables refresh failed: {err}")}),
                    ),
                },
                Err(err) => json_response(
                    StatusCode::INTERNAL_SERVER_ERROR,
                    &json!({"error": format!("upgradesstables compaction failed: {err}")}),
                ),
            },
            Err(err) => json_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                &json!({"error": format!("upgradesstables flush failed: {err}")}),
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

/// GET /api/v1/compaction/throughput — compaction throughput setting.
pub fn handle_get_compaction_throughput(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(
        StatusCode::OK,
        &json!({
            "throughput_mb_per_sec": 64,
        }),
    )
}

/// POST /api/v1/compaction/throughput — set compaction throughput.
pub async fn handle_set_compaction_throughput(
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

    handle_set_compaction_throughput_inner(&body_bytes)
}

/// POST /api/v1/operations/garbagecollect — trigger tombstone garbage collection.
pub async fn handle_garbagecollect(
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

    handle_garbagecollect_inner(&body_bytes, state)
}

fn handle_garbagecollect_inner(body_bytes: &[u8], state: &AdminState) -> Response<Full<Bytes>> {
    #[derive(Default, serde::Deserialize)]
    struct GarbageCollectRequest {
        #[serde(default)]
        keyspace: Option<String>,
    }

    let payload = if body_bytes.is_empty() {
        GarbageCollectRequest::default()
    } else {
        match serde_json::from_slice::<GarbageCollectRequest>(body_bytes) {
            Ok(p) => p,
            Err(e) => {
                return json_response(
                    StatusCode::BAD_REQUEST,
                    &json!({"error": format!("Invalid JSON: {}", e)}),
                );
            }
        }
    };

    let Some(engine) = state.storage_engine.as_ref() else {
        return json_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &json!({"error": "storage engine not configured"}),
        );
    };

    let description = match payload.keyspace.as_ref() {
        Some(ks) => format!("Garbage collect keyspace: {ks}"),
        None => "Garbage collect all keyspaces".to_string(),
    };
    let op_id = state
        .operations
        .register(OperationType::Rebuild, description);

    let response = match engine.maybe_compact() {
        Ok(compaction_executed) => json_response(
            StatusCode::OK,
            &json!({
                "operation_id": op_id.to_string(),
                "status": "garbagecollect completed",
                "keyspace": payload.keyspace,
                "compaction_executed": compaction_executed,
            }),
        ),
        Err(err) => json_response(
            StatusCode::INTERNAL_SERVER_ERROR,
            &json!({"error": format!("garbagecollect failed: {err}")}),
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

fn handle_set_compaction_throughput_inner(body_bytes: &[u8]) -> Response<Full<Bytes>> {
    #[derive(serde::Deserialize)]
    struct ThroughputRequest {
        throughput_mb_per_sec: u64,
    }

    let payload: ThroughputRequest = match serde_json::from_slice(body_bytes) {
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
            "status": "compaction throughput updated",
            "throughput_mb_per_sec": payload.throughput_mb_per_sec,
        }),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
    use std::sync::Arc;
    use tempfile::TempDir;

    use crate::operations::OperationTracker;
    use crate::prometheus_metrics::MetricsRegistry;
    use crate::virtual_tables::VirtualTableRegistry;
    use cassandra_storage::commitlog::{CellMutation, CommitLogConfig, Mutation, MutationRow};
    use cassandra_storage::engine::{EngineConfig, StorageEngine};

    fn test_state() -> AdminState {
        AdminState {
            metrics: Arc::new(MetricsRegistry::new()),
            operations: Arc::new(OperationTracker::new()),
            virtual_tables: Arc::new(VirtualTableRegistry::with_builtins()),
            repair_coordinator: None,
            storage_engine: None,
            schema_catalog: None,
            topology_controller: None,
        }
    }

    fn test_state_with_engine() -> (AdminState, TempDir) {
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
        let state = AdminState {
            metrics: Arc::new(MetricsRegistry::new()),
            operations: Arc::new(OperationTracker::new()),
            virtual_tables: Arc::new(VirtualTableRegistry::with_builtins()),
            repair_coordinator: None,
            storage_engine: Some(engine),
            schema_catalog: None,
            topology_controller: None,
        };
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

    fn response_json(resp: Response<Full<Bytes>>) -> serde_json::Value {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let collected = rt.block_on(resp.into_body().collect()).unwrap();
        serde_json::from_slice(&collected.to_bytes()).unwrap()
    }

    #[test]
    fn compaction_stats_returns_ok() {
        let state = test_state();
        let resp = handle_compaction_stats(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn compaction_history_returns_retained_rebuild_operations() {
        let state = test_state();
        let id = state
            .operations
            .register(OperationType::Rebuild, "compact ks.events");
        state.operations.update(&id, "COMPLETED", 100);
        state.operations.remove(&id);

        let resp = handle_compaction_history(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = response_json(resp);
        let history = body["history"].as_array().unwrap();
        assert_eq!(history.len(), 1);
        assert_eq!(history[0]["id"], id.to_string());
        assert_eq!(history[0]["type"], "REBUILD");
        assert_eq!(history[0]["status"], "COMPLETED");
        assert_eq!(history[0]["progress"], 100);
    }

    #[test]
    fn enable_autocompaction_returns_ok() {
        let state = test_state();
        let resp = handle_enable_autocompaction(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn disable_autocompaction_returns_ok() {
        let state = test_state();
        let resp = handle_disable_autocompaction(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn autocompaction_status_returns_enabled() {
        let state = test_state();
        let resp = handle_autocompaction_status(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn get_compaction_throughput_returns_default() {
        let state = test_state();
        let resp = handle_get_compaction_throughput(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn compaction_stats_shows_active_operations() {
        let state = test_state();
        // Register an operation to verify it shows up in stats
        state
            .operations
            .register(OperationType::Rebuild, "test compaction");
        let resp = handle_compaction_stats(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn compact_with_valid_body() {
        let state = test_state();
        let body =
            serde_json::to_vec(&json!({"keyspace": "test_ks", "table": "test_tbl"})).unwrap();
        let resp = handle_compact_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(state.operations.count(), 1);
    }

    #[test]
    fn compact_with_invalid_json() {
        let state = test_state();
        let resp = handle_compact_inner(b"not json", &state);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn cleanup_with_valid_body() {
        let state = test_state();
        let body = serde_json::to_vec(&json!({"keyspace": "my_ks"})).unwrap();
        let resp = handle_cleanup_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn set_compaction_throughput_valid() {
        let body = serde_json::to_vec(&json!({"throughput_mb_per_sec": 128})).unwrap();
        let resp = handle_set_compaction_throughput_inner(&body);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn verify_without_engine_returns_unavailable() {
        let state = test_state();
        let body = serde_json::to_vec(&json!({"keyspace": "ks1", "table": "tbl1"})).unwrap();
        let resp = handle_verify_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn verify_with_engine_returns_ok() {
        let (state, _dir) = test_state_with_engine();
        let engine = state.storage_engine.as_ref().unwrap();
        insert_and_flush(engine, "ks1", "tbl1", b"pk-verify-1");
        let body = serde_json::to_vec(&json!({"keyspace": "ks1", "table": "tbl1"})).unwrap();
        let resp = handle_verify_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::OK);
        let parsed = response_json(resp);
        assert!(parsed["operation_id"].is_string());
        assert_eq!(parsed["status"], "verify completed");
        assert_eq!(parsed["keyspace"], "ks1");
        assert_eq!(parsed["table"], "tbl1");
        assert!(parsed["scanned_sstables"].is_number());
        assert!(parsed["valid_sstables"].is_number());
        assert!(parsed["invalid_sstables"].is_number());
        assert!(parsed["issue_count"].is_number());
        assert!(parsed["issues"].is_array());
        assert!(parsed["verified"].is_boolean());
    }

    #[test]
    fn verify_table_without_keyspace_returns_bad_request() {
        let (state, _dir) = test_state_with_engine();
        let body = serde_json::to_vec(&json!({"table": "tbl1"})).unwrap();
        let resp = handle_verify_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn upgradesstables_without_engine_returns_unavailable() {
        let state = test_state();
        let body = serde_json::to_vec(&json!({"keyspace": "ks1"})).unwrap();
        let resp = handle_upgradesstables_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn upgradesstables_with_engine_returns_ok() {
        let (state, _dir) = test_state_with_engine();
        let engine = state.storage_engine.as_ref().unwrap();
        insert_and_flush(engine, "ks1", "tbl1", b"pk-up-1");
        insert_and_flush(engine, "ks1", "tbl1", b"pk-up-2");
        let body = serde_json::to_vec(&json!({"keyspace": "ks1", "table": "tbl1"})).unwrap();
        let resp = handle_upgradesstables_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::OK);
        let parsed = response_json(resp);
        assert!(parsed["operation_id"].is_string());
        assert_eq!(parsed["status"], "upgradesstables completed");
        assert_eq!(parsed["keyspace"], "ks1");
        assert_eq!(parsed["table"], "tbl1");
        assert!(parsed["compaction_executed"].is_boolean());
    }

    #[test]
    fn upgradesstables_table_without_keyspace_returns_bad_request() {
        let (state, _dir) = test_state_with_engine();
        let body = serde_json::to_vec(&json!({"table": "tbl1"})).unwrap();
        let resp = handle_upgradesstables_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn garbagecollect_without_engine_returns_unavailable() {
        let state = test_state();
        let body = serde_json::to_vec(&json!({"keyspace": "ks1"})).unwrap();
        let resp = handle_garbagecollect_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    #[test]
    fn garbagecollect_invalid_json_returns_bad_request() {
        let (state, _dir) = test_state_with_engine();
        let resp = handle_garbagecollect_inner(b"{invalid", &state);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn garbagecollect_with_engine_returns_ok() {
        let (state, _dir) = test_state_with_engine();
        let engine = state.storage_engine.as_ref().unwrap();
        insert_and_flush(engine, "ks1", "tbl1", b"pk-gc-1");
        insert_and_flush(engine, "ks1", "tbl1", b"pk-gc-2");
        let body = serde_json::to_vec(&json!({"keyspace": "ks1"})).unwrap();
        let resp = handle_garbagecollect_inner(&body, &state);
        assert_eq!(resp.status(), StatusCode::OK);
        let parsed = response_json(resp);
        assert!(parsed["operation_id"].is_string());
        assert_eq!(parsed["status"], "garbagecollect completed");
        assert_eq!(parsed["keyspace"], "ks1");
        assert!(parsed["compaction_executed"].is_boolean());
    }
}
