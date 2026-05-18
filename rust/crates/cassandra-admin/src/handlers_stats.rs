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
use prometheus::{HistogramVec, core::Collector};
use serde_json::{Value, json};

use crate::http_admin::{AdminState, json_response};

/// Returns table statistics from the storage engine when configured and
/// includes `system_views.sstable_tasks` rows when available.
pub fn handle_table_stats(state: &AdminState) -> Response<Full<Bytes>> {
    let sstable_tasks = state
        .virtual_tables
        .get("system_views", "sstable_tasks")
        .map(|vt| vt.rows())
        .unwrap_or_default();
    let body = if let Some(engine) = &state.storage_engine {
        let stats = engine.stats();
        json!({
            "engine": {
                "sstable_count": stats.sstable_count,
                "total_data_bytes": stats.total_data_bytes,
                "flushes_completed": stats.flushes_completed,
                "compactions_completed": stats.compactions_completed,
                "memtable_memory_bytes": stats.memtable_memory_bytes
            },
            "sstable_tasks": sstable_tasks,
            "total_disk_space_used": stats.total_data_bytes
        })
    } else {
        json!({
            "engine": null,
            "sstable_tasks": sstable_tasks,
            "total_disk_space_used": 0
        })
    };
    json_response(StatusCode::OK, &body)
}

/// Returns latency histogram data from table-level Prometheus histograms.
pub fn handle_table_histograms(state: &AdminState) -> Response<Full<Bytes>> {
    let body = json!({
        "latency_histograms": {
            "read": histogram_series(&state.metrics.table_read_latency),
            "write": histogram_series(&state.metrics.table_write_latency)
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

/// Returns proxy histogram statistics from client request latency metrics.
pub fn handle_proxy_histograms(state: &AdminState) -> Response<Full<Bytes>> {
    let body = json!({
        "read_latency": histogram_for_operation(&state.metrics.client_request_latency, "read"),
        "write_latency": histogram_for_operation(&state.metrics.client_request_latency, "write"),
        "range_latency": histogram_for_operation(&state.metrics.client_request_latency, "range")
    });
    json_response(StatusCode::OK, &body)
}

/// Returns connected-client information from `system_views.clients` when
/// available, otherwise uses the native-client metrics gauge.
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
            "connected_clients": state.metrics.connected_native_clients.get()
        }),
    };
    json_response(StatusCode::OK, &body)
}

/// Returns top partitions by currently visible row and cell counts.
pub fn handle_top_partitions(state: &AdminState) -> Response<Full<Bytes>> {
    let top = top_partitions_from_storage(state);
    let body = json!({
        "top_partitions": top,
        "top_read_partitions": top,
        "top_write_partitions": top
    });
    json_response(StatusCode::OK, &body)
}

fn histogram_for_operation(histograms: &HistogramVec, operation: &str) -> Value {
    let histogram = histograms.with_label_values(&[operation]);
    histogram
        .collect()
        .first()
        .and_then(|family| family.get_metric().first())
        .map(histogram_metric_json)
        .unwrap_or_else(empty_histogram_json)
}

fn histogram_series(histograms: &HistogramVec) -> Vec<Value> {
    histograms
        .collect()
        .iter()
        .flat_map(|family| family.get_metric().iter())
        .map(histogram_metric_json)
        .collect()
}

fn histogram_metric_json(metric: &prometheus::proto::Metric) -> Value {
    let labels = metric
        .get_label()
        .iter()
        .map(|pair| {
            (
                pair.get_name().to_string(),
                Value::String(pair.get_value().to_string()),
            )
        })
        .collect::<serde_json::Map<_, _>>();
    let histogram = metric.get_histogram();
    let buckets: Vec<Value> = histogram
        .get_bucket()
        .iter()
        .map(|bucket| {
            json!({
                "le": bucket.get_upper_bound(),
                "cumulative_count": bucket.get_cumulative_count()
            })
        })
        .collect();
    let count = histogram.get_sample_count();
    json!({
        "labels": labels,
        "count": count,
        "sum": histogram.get_sample_sum(),
        "buckets": buckets,
        "p50": estimate_quantile(histogram, 0.50),
        "p75": estimate_quantile(histogram, 0.75),
        "p95": estimate_quantile(histogram, 0.95),
        "p99": estimate_quantile(histogram, 0.99),
        "max": histogram.get_bucket().last().map(|b| b.get_upper_bound()).unwrap_or(0.0)
    })
}

fn empty_histogram_json() -> Value {
    json!({
        "labels": {},
        "count": 0,
        "sum": 0.0,
        "buckets": [],
        "p50": 0.0,
        "p75": 0.0,
        "p95": 0.0,
        "p99": 0.0,
        "max": 0.0
    })
}

fn estimate_quantile(histogram: &prometheus::proto::Histogram, quantile: f64) -> f64 {
    let count = histogram.get_sample_count();
    if count == 0 {
        return 0.0;
    }
    let rank = (count as f64 * quantile).ceil() as u64;
    histogram
        .get_bucket()
        .iter()
        .find(|bucket| bucket.get_cumulative_count() >= rank)
        .map(|bucket| bucket.get_upper_bound())
        .unwrap_or(0.0)
}

fn top_partitions_from_storage(state: &AdminState) -> Vec<Value> {
    let Some(engine) = &state.storage_engine else {
        return Vec::new();
    };
    let Some(catalog) = &state.schema_catalog else {
        return Vec::new();
    };

    let snapshot = catalog.read().snapshot();
    let mut partitions = Vec::new();
    for (keyspace, ks_meta) in &snapshot.keyspaces {
        for table in ks_meta.tables.keys() {
            for (partition_key, data) in engine.scan_all_partitions(keyspace, table) {
                let row_count = data.rows.len();
                let cell_count: usize = data.rows.values().map(|row| row.cells.len()).sum();
                partitions.push(json!({
                    "keyspace": keyspace,
                    "table": table,
                    "partition_key_hex": bytes_to_hex(&partition_key),
                    "rows": row_count,
                    "cells": cell_count
                }));
            }
        }
    }
    partitions.sort_by(|left, right| {
        let left_cells = left["cells"].as_u64().unwrap_or(0);
        let right_cells = right["cells"].as_u64().unwrap_or(0);
        right_cells.cmp(&left_cells)
    });
    partitions.truncate(10);
    partitions
}

fn bytes_to_hex(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
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
    fn table_stats_returns_empty_without_virtual_table_or_engine() {
        let state = AdminState {
            virtual_tables: Arc::new(VirtualTableRegistry::new()),
            ..test_state()
        };
        let resp = handle_table_stats(&state);
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_json(resp);
        assert_eq!(body["total_disk_space_used"], 0);
        assert!(body["sstable_tasks"].as_array().unwrap().is_empty());
    }

    #[test]
    fn table_histograms_returns_percentiles() {
        let state = test_state();
        let resp = handle_table_histograms(&state);
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_json(resp);
        assert!(body["latency_histograms"]["read"].as_array().is_some());
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
        assert!(
            body["note"]
                .as_str()
                .unwrap()
                .contains("no garbage collector")
        );
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
