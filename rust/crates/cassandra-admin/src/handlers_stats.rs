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

//! Statistics and monitoring handlers.
//!
//! All handlers in this module are synchronous and read-only, returning
//! JSON snapshots of table stats, histograms, thread-pool info, GC state,
//! proxy histograms, client connections, and top-partition data.

use bytes::Bytes;
use http_body_util::Full;
use hyper::{Response, StatusCode};
use serde_json::json;

use crate::http_admin::{AdminState, json_response};

/// Returns table statistics from the `system_views.sstable_tasks` virtual table
/// when available, otherwise returns stub data.
pub fn handle_table_stats(state: &AdminState) -> Response<Full<Bytes>> {
    let body = match state.virtual_tables.get("system_views", "sstable_tasks") {
        Some(vt) => {
            let rows = vt.rows();
            json!({
                "tables": rows,
                "total_disk_space_used": rows.len() // placeholder aggregate
            })
        }
        None => json!({
            "tables": [],
            "total_disk_space_used": 0
        }),
    };
    json_response(StatusCode::OK, &body)
}

/// Returns latency histogram data with default percentile values.
pub fn handle_table_histograms(state: &AdminState) -> Response<Full<Bytes>> {
    let _ = state; // read-only; no virtual table source yet
    let body = json!({
        "latency_histograms": {
            "read": {
                "p50": 0.0,
                "p75": 0.0,
                "p95": 0.0,
                "p99": 0.0,
                "max": 0.0
            },
            "write": {
                "p50": 0.0,
                "p75": 0.0,
                "p95": 0.0,
                "p99": 0.0,
                "max": 0.0
            }
        }
    });
    json_response(StatusCode::OK, &body)
}

/// Returns thread-pool statistics from `system_views.thread_pools` when
/// available, otherwise returns a default empty list.
pub fn handle_tp_stats(state: &AdminState) -> Response<Full<Bytes>> {
    let body = match state.virtual_tables.get("system_views", "thread_pools") {
        Some(vt) => {
            let rows = vt.rows();
            json!({ "thread_pools": rows })
        }
        None => json!({
            "thread_pools": [
                {"name": "ReadStage", "active": 0, "pending": 0, "completed": 0, "blocked": 0},
                {"name": "MutationStage", "active": 0, "pending": 0, "completed": 0, "blocked": 0},
                {"name": "CompactionExecutor", "active": 0, "pending": 0, "completed": 0, "blocked": 0}
            ]
        }),
    };
    json_response(StatusCode::OK, &body)
}

/// Returns GC statistics.  Rust has no garbage collector so this always
/// reports GC as disabled.
pub fn handle_gc_stats(state: &AdminState) -> Response<Full<Bytes>> {
    let _ = state;
    let body = json!({
        "gc_enabled": false,
        "note": "Rust implementation - no garbage collector"
    });
    json_response(StatusCode::OK, &body)
}

/// Returns proxy histogram statistics with stub percentile values.
pub fn handle_proxy_histograms(state: &AdminState) -> Response<Full<Bytes>> {
    let _ = state;
    let body = json!({
        "read_latency": {
            "p50": 0.0,
            "p75": 0.0,
            "p95": 0.0,
            "p99": 0.0,
            "max": 0.0
        },
        "write_latency": {
            "p50": 0.0,
            "p75": 0.0,
            "p95": 0.0,
            "p99": 0.0,
            "max": 0.0
        },
        "range_latency": {
            "p50": 0.0,
            "p75": 0.0,
            "p95": 0.0,
            "p99": 0.0,
            "max": 0.0
        }
    });
    json_response(StatusCode::OK, &body)
}

/// Returns connected-client information from `system_views.clients` when
/// available, otherwise returns a zero-count stub.
pub fn handle_client_stats(state: &AdminState) -> Response<Full<Bytes>> {
    let body = match state.virtual_tables.get("system_views", "clients") {
        Some(vt) => {
            let rows = vt.rows();
            json!({
                "connected_clients": rows.len(),
                "clients": rows
            })
        }
        None => json!({
            "connected_clients": 0
        }),
    };
    json_response(StatusCode::OK, &body)
}

/// Returns top-partition statistics (stubs — no live tracking yet).
pub fn handle_top_partitions(state: &AdminState) -> Response<Full<Bytes>> {
    let _ = state;
    let body = json!({
        "top_read_partitions": [],
        "top_write_partitions": []
    });
    json_response(StatusCode::OK, &body)
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

    #[test]
    fn table_stats_returns_ok_with_virtual_table() {
        let state = test_state();
        let resp = handle_table_stats(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn table_stats_returns_stub_without_virtual_table() {
        let state = AdminState {
            virtual_tables: Arc::new(VirtualTableRegistry::new()),
            ..test_state()
        };
        let resp = handle_table_stats(&state);
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_json(resp);
        assert_eq!(body["total_disk_space_used"], 0);
        assert!(body["tables"].as_array().unwrap().is_empty());
    }

    #[test]
    fn table_histograms_returns_percentiles() {
        let state = test_state();
        let resp = handle_table_histograms(&state);
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_json(resp);
        assert!(body["latency_histograms"]["read"]["p50"].is_f64()
            || body["latency_histograms"]["read"]["p50"].is_i64());
    }

    #[test]
    fn tp_stats_returns_ok() {
        let state = test_state();
        let resp = handle_tp_stats(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn gc_stats_reports_no_gc() {
        let state = test_state();
        let resp = handle_gc_stats(&state);
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_json(resp);
        assert_eq!(body["gc_enabled"], false);
        assert!(body["note"].as_str().unwrap().contains("no garbage collector"));
    }

    #[test]
    fn proxy_histograms_returns_ok() {
        let state = test_state();
        let resp = handle_proxy_histograms(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn client_stats_returns_ok() {
        let state = test_state();
        let resp = handle_client_stats(&state);
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn top_partitions_returns_empty() {
        let state = test_state();
        let resp = handle_top_partitions(&state);
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_json(resp);
        assert!(body["top_read_partitions"].as_array().unwrap().is_empty());
        assert!(body["top_write_partitions"].as_array().unwrap().is_empty());
    }
}
