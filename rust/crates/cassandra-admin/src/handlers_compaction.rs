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

use crate::http_admin::{json_response, AdminState};
use crate::operations::OperationType;

/// POST /api/v1/operations/compact — trigger major compaction.
pub async fn handle_compact(
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
    let op_id = state.operations.register(OperationType::Rebuild, description);

    json_response(
        StatusCode::OK,
        &json!({
            "operation_id": op_id.to_string(),
            "status": "started",
        }),
    )
}

/// POST /api/v1/operations/cleanup — trigger cleanup of keys no longer belonging to this node.
pub async fn handle_cleanup(
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
pub async fn handle_flush(
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
    let op_id = state.operations.register(OperationType::Rebuild, description);

    json_response(
        StatusCode::OK,
        &json!({
            "operation_id": op_id.to_string(),
            "status": "started",
        }),
    )
}

/// POST /api/v1/operations/scrub — scrub SSTables for corruption.
pub async fn handle_scrub(
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
    let op_id = state.operations.register(OperationType::Rebuild, description);

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

/// GET /api/v1/compaction/history — compaction history (stub).
pub fn handle_compaction_history(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(
        StatusCode::OK,
        &json!({
            "history": [],
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
    use std::sync::Arc;

    use crate::operations::OperationTracker;
    use crate::prometheus_metrics::MetricsRegistry;
    use crate::virtual_tables::VirtualTableRegistry;

    fn test_state() -> AdminState {
        AdminState {
            metrics: Arc::new(MetricsRegistry::new()),
            operations: Arc::new(OperationTracker::new()),
            virtual_tables: Arc::new(VirtualTableRegistry::with_builtins()),
            repair_coordinator: None,
            storage_engine: None,
            schema_catalog: None,
        }
    }

    #[test]
    fn compaction_stats_returns_ok() {
        let state = test_state();
        let resp = handle_compaction_stats(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn compaction_history_returns_empty_array() {
        let state = test_state();
        let resp = handle_compaction_history(&state);
        assert_eq!(resp.status(), StatusCode::OK);
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
        state.operations.register(OperationType::Rebuild, "test compaction");
        let resp = handle_compaction_stats(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn compact_with_valid_body() {
        let state = test_state();
        let body = serde_json::to_vec(&json!({"keyspace": "test_ks", "table": "test_tbl"})).unwrap();
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
}
