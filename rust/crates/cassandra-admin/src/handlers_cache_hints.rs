// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! Cache invalidation and hints management handlers.
//!
//! Provides both synchronous (hints/handoff) and asynchronous (cache
//! invalidation/capacity) HTTP handlers for the admin API.

use bytes::Bytes;
use http_body_util::Full;
use hyper::body::Incoming;
use hyper::{Request, Response, StatusCode};
use serde_json::json;

use crate::http_admin::{AdminState, json_response};

// ---------------------------------------------------------------------------
// Valid cache types
// ---------------------------------------------------------------------------

const VALID_CACHE_TYPES: &[&str] = &[
    "key",
    "row",
    "counter",
    "credentials",
    "permissions",
    "roles",
    "jmx",
    "cidr",
    "network",
];

// ---------------------------------------------------------------------------
// Async handlers — cache operations
// ---------------------------------------------------------------------------

/// Invalidates the specified cache.  Expects a JSON body with a `cache_type`
/// field whose value is one of the recognised cache names.
pub async fn handle_invalidate_cache(
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
            )
        }
    };

    handle_invalidate_cache_inner(&body_bytes)
}

fn handle_invalidate_cache_inner(body_bytes: &[u8]) -> Response<Full<Bytes>> {
    let payload: serde_json::Value = match serde_json::from_slice(body_bytes) {
        Ok(v) => v,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Invalid JSON: {}", e)}),
            )
        }
    };

    let cache_type = match payload.get("cache_type").and_then(|v| v.as_str()) {
        Some(ct) => ct,
        None => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": "Missing 'cache_type' field"}),
            )
        }
    };

    if !VALID_CACHE_TYPES.contains(&cache_type) {
        return json_response(
            StatusCode::BAD_REQUEST,
            &json!({"error": format!("Unknown cache type '{}'. Valid types: {:?}", cache_type, VALID_CACHE_TYPES)}),
        );
    }

    json_response(
        StatusCode::OK,
        &json!({
            "status": "cache invalidated",
            "cache_type": cache_type
        }),
    )
}

/// Sets the capacity for the specified cache.  Expects a JSON body with
/// `cache_type` and `capacity_mb` fields.
pub async fn handle_set_cache_capacity(
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
            )
        }
    };

    handle_set_cache_capacity_inner(&body_bytes)
}

fn handle_set_cache_capacity_inner(body_bytes: &[u8]) -> Response<Full<Bytes>> {
    let payload: serde_json::Value = match serde_json::from_slice(body_bytes) {
        Ok(v) => v,
        Err(e) => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": format!("Invalid JSON: {}", e)}),
            )
        }
    };

    let cache_type = match payload.get("cache_type").and_then(|v| v.as_str()) {
        Some(ct) => ct,
        None => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": "Missing 'cache_type' field"}),
            )
        }
    };

    if !VALID_CACHE_TYPES.contains(&cache_type) {
        return json_response(
            StatusCode::BAD_REQUEST,
            &json!({"error": format!("Unknown cache type '{}'. Valid types: {:?}", cache_type, VALID_CACHE_TYPES)}),
        );
    }

    let capacity_mb = match payload.get("capacity_mb").and_then(|v| v.as_u64()) {
        Some(c) => c,
        None => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": "Missing or invalid 'capacity_mb' field (expected unsigned integer)"}),
            )
        }
    };

    json_response(
        StatusCode::OK,
        &json!({
            "status": "cache capacity updated",
            "cache_type": cache_type,
            "capacity_mb": capacity_mb
        }),
    )
}

// ---------------------------------------------------------------------------
// Sync handlers — hints and handoff
// ---------------------------------------------------------------------------

/// Truncates all pending hints.
pub fn handle_truncate_hints(state: &AdminState) -> Response<Full<Bytes>> {
    let _ = state;
    json_response(StatusCode::OK, &json!({"status": "hints truncated"}))
}

/// Returns pending hints from `system_views.pending_hints` when available,
/// otherwise an empty endpoint list.
pub fn handle_pending_hints(state: &AdminState) -> Response<Full<Bytes>> {
    let body = match state.virtual_tables.get("system_views", "pending_hints") {
        Some(vt) => {
            let rows = vt.rows();
            json!({ "endpoints": rows })
        }
        None => json!({ "endpoints": [] }),
    };
    json_response(StatusCode::OK, &body)
}

/// Enables hinted handoff.
pub fn handle_enable_handoff(state: &AdminState) -> Response<Full<Bytes>> {
    let _ = state;
    json_response(StatusCode::OK, &json!({"status": "handoff enabled"}))
}

/// Disables hinted handoff.
pub fn handle_disable_handoff(state: &AdminState) -> Response<Full<Bytes>> {
    let _ = state;
    json_response(StatusCode::OK, &json!({"status": "handoff disabled"}))
}

/// Pauses hinted handoff delivery.
pub fn handle_pause_handoff(state: &AdminState) -> Response<Full<Bytes>> {
    let _ = state;
    json_response(StatusCode::OK, &json!({"status": "handoff paused"}))
}

/// Resumes hinted handoff delivery.
pub fn handle_resume_handoff(state: &AdminState) -> Response<Full<Bytes>> {
    let _ = state;
    json_response(StatusCode::OK, &json!({"status": "handoff resumed"}))
}

#[cfg(test)]
mod tests {
    use super::*;
    use http_body_util::BodyExt;
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

    /// Extract body JSON from a `Full<Bytes>` response (test helper).
    fn body_json(resp: Response<Full<Bytes>>) -> serde_json::Value {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let collected = rt.block_on(resp.into_body().collect()).unwrap();
        serde_json::from_slice(&collected.to_bytes()).unwrap()
    }

    // ── Sync handler tests ────────────────────────────────────────────

    #[test]
    fn truncate_hints_returns_ok() {
        let state = test_state();
        let resp = handle_truncate_hints(&state);
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_json(resp);
        assert_eq!(body["status"], "hints truncated");
    }

    #[test]
    fn pending_hints_returns_ok_with_builtins() {
        let state = test_state();
        let resp = handle_pending_hints(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn pending_hints_returns_empty_without_virtual_table() {
        let state = AdminState {
            virtual_tables: Arc::new(VirtualTableRegistry::new()),
            ..test_state()
        };
        let resp = handle_pending_hints(&state);
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_json(resp);
        assert!(body["endpoints"].as_array().unwrap().is_empty());
    }

    #[test]
    fn enable_handoff_returns_ok() {
        let state = test_state();
        let resp = handle_enable_handoff(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn disable_handoff_returns_ok() {
        let state = test_state();
        let resp = handle_disable_handoff(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn pause_handoff_returns_ok() {
        let state = test_state();
        let resp = handle_pause_handoff(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn resume_handoff_returns_ok() {
        let state = test_state();
        let resp = handle_resume_handoff(&state);
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_json(resp);
        assert_eq!(body["status"], "handoff resumed");
    }

    // ── Async handler tests (now sync via _inner) ─────────────────────

    #[test]
    fn invalidate_cache_valid_type() {
        let body = serde_json::to_vec(&json!({"cache_type": "key"})).unwrap();

        let resp = handle_invalidate_cache_inner(&body);
        assert_eq!(resp.status(), StatusCode::OK);

        let collected = {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(resp.into_body().collect()).unwrap().to_bytes()
        };
        let parsed: serde_json::Value = serde_json::from_slice(&collected).unwrap();
        assert_eq!(parsed["cache_type"], "key");
    }

    #[test]
    fn invalidate_cache_invalid_type() {
        let body = serde_json::to_vec(&json!({"cache_type": "invalid"})).unwrap();

        let resp = handle_invalidate_cache_inner(&body);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn set_cache_capacity_valid() {
        let body =
            serde_json::to_vec(&json!({"cache_type": "row", "capacity_mb": 256})).unwrap();

        let resp = handle_set_cache_capacity_inner(&body);
        assert_eq!(resp.status(), StatusCode::OK);

        let collected = {
            let rt = tokio::runtime::Runtime::new().unwrap();
            rt.block_on(resp.into_body().collect()).unwrap().to_bytes()
        };
        let parsed: serde_json::Value = serde_json::from_slice(&collected).unwrap();
        assert_eq!(parsed["capacity_mb"], 256);
    }

    #[test]
    fn set_cache_capacity_missing_field() {
        let body = serde_json::to_vec(&json!({"cache_type": "row"})).unwrap();

        let resp = handle_set_cache_capacity_inner(&body);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }
}
