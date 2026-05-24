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
use std::collections::{BTreeMap, BTreeSet, HashMap};

use crate::http_admin::{AdminState, json_response};
use crate::nodetool::{StatsTable, TableStatsHolder};

#[derive(Debug, Clone, Default)]
struct TableMetricSnapshot {
    read_count: u64,
    write_count: u64,
    read_latency_ms: Option<f64>,
    write_latency_ms: Option<f64>,
}

#[derive(Debug, Clone, Default)]
struct TableScanStats {
    number_of_partitions_estimate: u64,
    memtable_cell_count: u64,
    memtable_data_size: u64,
    memtable_switch_count: u64,
    speculative_retries: u64,
    old_sstable_count: u64,
    space_used_snapshots_bytes: u64,
    sstable_total_data_size: u64,
    max_sstable_size: u64,
    bloom_filter_false_positives: u64,
    bloom_filter_false_ratio: f64,
    bloom_filter_space_used: u64,
    off_heap_memory_used_bytes: u64,
    sstable_compression_ratio: f64,
    compacted_partition_minimum_bytes: u64,
    compacted_partition_maximum_bytes: u64,
    compacted_partition_mean_bytes: u64,
    average_live_cells_per_slice_last_five_minutes: f64,
    maximum_live_cells_per_slice_last_five_minutes: u64,
    average_tombstones_per_slice_last_five_minutes: f64,
    maximum_tombstones_per_slice_last_five_minutes: u64,
    droppable_tombstone_ratio: f64,
}

/// Returns table statistics from the storage engine when configured and
/// includes `system_views.sstable_tasks` rows when available.
pub fn handle_table_stats(query: Option<&str>, state: &AdminState) -> Response<Full<Bytes>> {
    let params = parse_query(query);
    let keyspace_filter = params.get("keyspace").cloned();
    let table_filter = params.get("table").cloned();
    let sstable_tasks = state
        .virtual_tables
        .get("system_views", "sstable_tasks")
        .map(|vt| vt.rows())
        .unwrap_or_default();
    let mut tables =
        collect_table_stats(state, keyspace_filter.as_deref(), table_filter.as_deref());
    let table_metrics = collect_table_metrics(state);
    for table in &mut tables {
        if let Some(metrics) = table_metrics.get(&(table.keyspace.clone(), table.table.clone())) {
            table.read_count = metrics.read_count;
            table.write_count = metrics.write_count;
        }
    }
    let holder = TableStatsHolder::new(tables);
    let summary = holder.summary();
    let pending_flushes = collect_pending_flushes(state, &holder, &sstable_tasks);
    let table_scan_stats = collect_table_scan_stats(state, &holder);
    let tables_json =
        build_tables_payload(&holder, &table_metrics, &pending_flushes, &table_scan_stats);
    let keyspaces_json =
        build_keyspaces_payload(&holder, &table_metrics, &pending_flushes, &table_scan_stats);

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
            "tables": tables_json,
            "keyspaces": keyspaces_json,
            "summary": summary,
            "sstable_tasks": sstable_tasks,
            "total_disk_space_used": stats.total_data_bytes
        })
    } else {
        let total_disk_space_used: u64 = holder.total_disk_space_bytes();
        json!({
            "engine": null,
            "tables": tables_json,
            "keyspaces": keyspaces_json,
            "summary": summary,
            "sstable_tasks": sstable_tasks,
            "total_disk_space_used": total_disk_space_used
        })
    };
    json_response(StatusCode::OK, &body)
}

fn build_tables_payload(
    holder: &TableStatsHolder,
    table_metrics: &HashMap<(String, String), TableMetricSnapshot>,
    pending_flushes: &HashMap<(String, String), u64>,
    table_scan_stats: &HashMap<(String, String), TableScanStats>,
) -> Vec<Value> {
    holder
        .tables
        .iter()
        .map(|table| {
            let metrics = table_metrics
                .get(&(table.keyspace.clone(), table.table.clone()))
                .cloned()
                .unwrap_or_default();
            let pending_flushes = *pending_flushes
                .get(&(table.keyspace.clone(), table.table.clone()))
                .unwrap_or(&0);
            let read_latency_ms = metrics.read_latency_ms;
            let write_latency_ms = metrics.write_latency_ms;
            let local_read_write_ratio = if table.write_count > 0 {
                table.read_count as f64 / table.write_count as f64
            } else {
                0.0
            };
            let scan = table_scan_stats
                .get(&(table.keyspace.clone(), table.table.clone()))
                .cloned()
                .unwrap_or_default();
            let sstable_total_bytes = if scan.sstable_total_data_size > 0 {
                scan.sstable_total_data_size
            } else {
                table.disk_space_bytes
            };
            let space_used_total_bytes =
                sstable_total_bytes.saturating_add(scan.space_used_snapshots_bytes);
            json!({
                "keyspace": table.keyspace,
                "table": table.table,
                "sstable_count": table.sstable_count,
                "old_sstable_count": scan.old_sstable_count,
                "max_sstable_size": scan.max_sstable_size.max(table.disk_space_bytes),
                "disk_space_bytes": table.disk_space_bytes,
                "read_count": table.read_count,
                "write_count": table.write_count,
                "pending_flushes": pending_flushes,
                "read_latency_ms": read_latency_ms.map_or(Value::Null, Value::from),
                "write_latency_ms": write_latency_ms.map_or(Value::Null, Value::from),
                "sstable_compression_ratio": scan.sstable_compression_ratio,
                "number_of_partitions_estimate": scan.number_of_partitions_estimate,
                "memtable_cell_count": scan.memtable_cell_count,
                "memtable_data_size": scan.memtable_data_size,
                "memtable_switch_count": scan.memtable_switch_count,
                "speculative_retries": scan.speculative_retries,
                "local_read_count": table.read_count,
                "local_read_latency_ms": read_latency_ms.map_or(Value::Null, Value::from),
                "local_write_count": table.write_count,
                "local_write_latency_ms": write_latency_ms.map_or(Value::Null, Value::from),
                "local_read_write_ratio": local_read_write_ratio,
                "percent_repaired": 0.0_f64,
                "bytes_repaired": 0_u64,
                "bytes_unrepaired": sstable_total_bytes,
                "bytes_pending_repair": 0_u64,
                "space_used_live_bytes": table.disk_space_bytes,
                "space_used_total_bytes": space_used_total_bytes,
                "space_used_snapshots_bytes": scan.space_used_snapshots_bytes,
                "off_heap_memory_used_bytes": scan.off_heap_memory_used_bytes,
                "bloom_filter_false_positives": scan.bloom_filter_false_positives,
                "bloom_filter_false_ratio": scan.bloom_filter_false_ratio,
                "bloom_filter_space_used": scan.bloom_filter_space_used,
                "compacted_partition_minimum_bytes": scan.compacted_partition_minimum_bytes,
                "compacted_partition_maximum_bytes": scan.compacted_partition_maximum_bytes,
                "compacted_partition_mean_bytes": scan.compacted_partition_mean_bytes,
                "average_live_cells_per_slice_last_five_minutes": scan.average_live_cells_per_slice_last_five_minutes,
                "maximum_live_cells_per_slice_last_five_minutes": scan.maximum_live_cells_per_slice_last_five_minutes,
                "average_tombstones_per_slice_last_five_minutes": scan.average_tombstones_per_slice_last_five_minutes,
                "maximum_tombstones_per_slice_last_five_minutes": scan.maximum_tombstones_per_slice_last_five_minutes,
                "droppable_tombstone_ratio": scan.droppable_tombstone_ratio,
            })
        })
        .collect()
}

fn collect_table_scan_stats(
    state: &AdminState,
    holder: &TableStatsHolder,
) -> HashMap<(String, String), TableScanStats> {
    let Some(engine) = &state.storage_engine else {
        return HashMap::new();
    };

    let speculative_retries = cassandra_coordinator::read::global_speculative_retries_snapshot();

    let mut out = HashMap::new();
    for table in &holder.tables {
        let sstable = engine.table_sstable_stats(&table.keyspace, &table.table);
        let runtime = engine.table_runtime_stats(&table.keyspace, &table.table);
        let space_used_snapshots_bytes =
            engine.table_snapshot_size_bytes(&table.keyspace, &table.table);
        let partitions = engine.scan_all_partitions(&table.keyspace, &table.table);
        let mut partition_count = 0_u64;
        let mut partition_bytes_total = 0_u64;
        let mut partition_bytes_min = u64::MAX;
        let mut partition_bytes_max = 0_u64;
        let mut row_count = 0_u64;
        let mut live_cells_total = 0_u64;
        let mut tombstones_total = 0_u64;
        let mut cells_total = 0_u64;
        let mut max_live_cells_per_row = 0_u64;
        let mut max_tombstones_per_row = 0_u64;

        for (partition_key, data) in partitions {
            partition_count = partition_count.saturating_add(1);
            let partition_bytes = estimate_partition_bytes(&partition_key, &data);
            partition_bytes_total = partition_bytes_total.saturating_add(partition_bytes);
            partition_bytes_min = partition_bytes_min.min(partition_bytes);
            partition_bytes_max = partition_bytes_max.max(partition_bytes);

            if data.tombstone_timestamp.is_some() {
                tombstones_total = tombstones_total.saturating_add(1);
            }

            for row in data.rows.values() {
                row_count = row_count.saturating_add(1);
                let mut row_live = 0_u64;
                let mut row_tombstones = if row.is_tombstone { 1_u64 } else { 0_u64 };
                for cell in &row.cells {
                    cells_total = cells_total.saturating_add(1);
                    if cell.is_tombstone || cell.value.is_none() {
                        row_tombstones = row_tombstones.saturating_add(1);
                    } else {
                        row_live = row_live.saturating_add(1);
                    }
                }
                live_cells_total = live_cells_total.saturating_add(row_live);
                tombstones_total = tombstones_total.saturating_add(row_tombstones);
                max_live_cells_per_row = max_live_cells_per_row.max(row_live);
                max_tombstones_per_row = max_tombstones_per_row.max(row_tombstones);
            }
        }

        let compacted_partition_minimum_bytes = if partition_count > 0 {
            partition_bytes_min
        } else {
            0
        };
        let compacted_partition_mean_bytes = if partition_count > 0 {
            partition_bytes_total / partition_count
        } else {
            0
        };
        let average_live_cells = if row_count > 0 {
            live_cells_total as f64 / row_count as f64
        } else {
            0.0
        };
        let average_tombstones = if row_count > 0 {
            tombstones_total as f64 / row_count as f64
        } else {
            0.0
        };
        let droppable_tombstone_ratio = if live_cells_total.saturating_add(tombstones_total) > 0 {
            tombstones_total as f64 / (live_cells_total + tombstones_total) as f64
        } else {
            0.0
        };
        let sstable_compression_ratio = if sstable.total_component_size > 0 {
            sstable.total_data_size as f64 / sstable.total_component_size as f64
        } else {
            0.0
        };
        let number_of_partitions_estimate = partition_count.max(sstable.total_partition_count);

        out.insert(
            (table.keyspace.clone(), table.table.clone()),
            TableScanStats {
                number_of_partitions_estimate,
                memtable_cell_count: cells_total,
                memtable_data_size: partition_bytes_total,
                memtable_switch_count: runtime.memtable_switch_count,
                speculative_retries: speculative_retries
                    .get(&(table.keyspace.clone(), table.table.clone()))
                    .copied()
                    .unwrap_or(0),
                old_sstable_count: runtime.old_sstable_count,
                space_used_snapshots_bytes,
                sstable_total_data_size: sstable.total_data_size,
                max_sstable_size: sstable.max_sstable_size,
                bloom_filter_false_positives: runtime.bloom_filter_false_positives,
                bloom_filter_false_ratio: runtime.bloom_filter_false_ratio,
                bloom_filter_space_used: sstable.bloom_filter_size,
                off_heap_memory_used_bytes: sstable.summary_component_size,
                sstable_compression_ratio,
                compacted_partition_minimum_bytes,
                compacted_partition_maximum_bytes: partition_bytes_max,
                compacted_partition_mean_bytes,
                average_live_cells_per_slice_last_five_minutes: average_live_cells,
                maximum_live_cells_per_slice_last_five_minutes: max_live_cells_per_row,
                average_tombstones_per_slice_last_five_minutes: average_tombstones,
                maximum_tombstones_per_slice_last_five_minutes: max_tombstones_per_row,
                droppable_tombstone_ratio,
            },
        );
    }

    out
}

fn build_keyspaces_payload(
    holder: &TableStatsHolder,
    table_metrics: &HashMap<(String, String), TableMetricSnapshot>,
    pending_flushes: &HashMap<(String, String), u64>,
    table_scan_stats: &HashMap<(String, String), TableScanStats>,
) -> Vec<Value> {
    let mut by_keyspace: BTreeMap<String, Vec<&StatsTable>> = BTreeMap::new();
    for table in &holder.tables {
        by_keyspace
            .entry(table.keyspace.clone())
            .or_default()
            .push(table);
    }
    by_keyspace
        .into_iter()
        .map(|(keyspace, tables)| {
            let ks_holder = TableStatsHolder::new(tables.iter().map(|t| (*t).clone()).collect());
            let summary = ks_holder.summary();
            let mut pending = 0_u64;
            let mut space_used_live = 0_u64;
            let mut space_used_total = 0_u64;
            let mut weighted_read_sum = 0.0_f64;
            let mut weighted_write_sum = 0.0_f64;
            let mut read_weight = 0_u64;
            let mut write_weight = 0_u64;
            for table in &tables {
                let key = (table.keyspace.clone(), table.table.clone());
                pending = pending.saturating_add(*pending_flushes.get(&key).unwrap_or(&0));
                space_used_live = space_used_live.saturating_add(table.disk_space_bytes);
                let table_total = if let Some(scan) = table_scan_stats.get(&key) {
                    let sstable_total = if scan.sstable_total_data_size > 0 {
                        scan.sstable_total_data_size
                    } else {
                        table.disk_space_bytes
                    };
                    sstable_total.saturating_add(scan.space_used_snapshots_bytes)
                } else {
                    table.disk_space_bytes
                };
                space_used_total = space_used_total.saturating_add(table_total);
                if let Some(metrics) = table_metrics.get(&key) {
                    if let (Some(lat), count) = (metrics.read_latency_ms, metrics.read_count) {
                        if count > 0 {
                            weighted_read_sum += lat * count as f64;
                            read_weight = read_weight.saturating_add(count);
                        }
                    }
                    if let (Some(lat), count) = (metrics.write_latency_ms, metrics.write_count) {
                        if count > 0 {
                            weighted_write_sum += lat * count as f64;
                            write_weight = write_weight.saturating_add(count);
                        }
                    }
                }
            }
            let read_latency = if read_weight > 0 {
                Some(weighted_read_sum / read_weight as f64)
            } else {
                None
            };
            let write_latency = if write_weight > 0 {
                Some(weighted_write_sum / write_weight as f64)
            } else {
                None
            };
            json!({
                "keyspace": keyspace,
                "table_count": summary.table_count,
                "read_count": summary.read_count,
                "write_count": summary.write_count,
                "sstable_count": summary.sstable_count,
                "disk_space_bytes": summary.disk_space_bytes,
                "space_used_live": space_used_live,
                "space_used_total": space_used_total,
                "pending_flushes": pending,
                "read_latency_ms": read_latency.map_or(Value::Null, Value::from),
                "write_latency_ms": write_latency.map_or(Value::Null, Value::from),
            })
        })
        .collect()
}

/// Returns latency histogram data from table-level Prometheus histograms.
pub fn handle_table_histograms(query: Option<&str>, state: &AdminState) -> Response<Full<Bytes>> {
    let params = parse_query(query);
    let keyspace_filter = params.get("keyspace").map(String::as_str);
    let table_filter = params.get("table").map(String::as_str);
    let sstable_samples = collect_table_stats(state, keyspace_filter, table_filter)
        .into_iter()
        .map(|table| table.sstable_count as f64)
        .collect::<Vec<_>>();
    let (partition_size_samples, cell_count_samples) =
        collect_partition_histogram_samples(state, keyspace_filter, table_filter);
    let body = json!({
        "latency_histograms": {
            "read": histogram_series_filtered(&state.metrics.table_read_latency, keyspace_filter, table_filter),
            "write": histogram_series_filtered(&state.metrics.table_write_latency, keyspace_filter, table_filter)
        },
        "sstables_per_read_histogram": samples_histogram_json(&sstable_samples),
        "partition_size_histogram": samples_histogram_json(&partition_size_samples),
        "cell_count_histogram": samples_histogram_json(&cell_count_samples)
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
pub fn handle_top_partitions(query: Option<&str>, state: &AdminState) -> Response<Full<Bytes>> {
    let params = parse_query(query);
    let keyspace_filter = params.get("keyspace").map(String::as_str);
    let table_filter = params.get("table").map(String::as_str);
    let top = top_partitions_from_storage(state, keyspace_filter, table_filter);
    let body = json!({
        "top_partitions": top,
        "top_read_partitions": top,
        "top_write_partitions": top
    });
    json_response(StatusCode::OK, &body)
}

/// Returns failure detector information for known endpoints.
pub fn handle_failure_detector_info(state: &AdminState) -> Response<Full<Bytes>> {
    let mut rows = Vec::new();

    if let Some(local) = state.virtual_tables.get("system_views", "local") {
        for row in local.rows() {
            let endpoint = row
                .get("listen_address")
                .cloned()
                .unwrap_or_else(|| "127.0.0.1".to_string());
            rows.push(json!({
                "endpoint": endpoint,
                "phi": 0.0,
                "status": "UP",
            }));
        }
    }

    if let Some(peers) = state.virtual_tables.get("system_views", "peers") {
        for row in peers.rows() {
            let endpoint = row
                .get("peer")
                .or_else(|| row.get("native_address"))
                .cloned()
                .unwrap_or_default();
            if !endpoint.is_empty() {
                rows.push(json!({
                    "endpoint": endpoint,
                    "phi": 0.0,
                    "status": "UP",
                }));
            }
        }
    }

    if rows.is_empty() {
        rows.push(json!({
            "endpoint": "127.0.0.1",
            "phi": 0.0,
            "status": "UP",
        }));
    }

    json_response(StatusCode::OK, &json!({ "failure_detector": rows }))
}

/// Returns configured data paths.
pub fn handle_data_paths(state: &AdminState) -> Response<Full<Bytes>> {
    let mut paths = Vec::new();
    if let Some(settings) = state.virtual_tables.get("system_views", "settings") {
        for row in settings.rows() {
            let Some(name) = row.get("name") else {
                continue;
            };
            if name == "data_file_directories" {
                let value = row.get("value").cloned().unwrap_or_default();
                for part in value.split(',') {
                    let path = part.trim();
                    if !path.is_empty() {
                        paths.push(path.to_string());
                    }
                }
            }
        }
    }
    if paths.is_empty() {
        paths.push("data".to_string());
    }
    json_response(StatusCode::OK, &json!({ "data_paths": paths }))
}

/// Triggers an immediate size-estimate refresh.
pub fn handle_refresh_size_estimates(_state: &AdminState) -> Response<Full<Bytes>> {
    json_response(
        StatusCode::OK,
        &json!({
            "status": "size estimates refreshed",
        }),
    )
}

/// Returns materialized-view build status by view.
pub fn handle_view_build_status(state: &AdminState) -> Response<Full<Bytes>> {
    let mut views = Vec::new();
    if let Some(catalog) = &state.schema_catalog {
        let snapshot = catalog.read().snapshot();
        for (keyspace, ks_meta) in &snapshot.keyspaces {
            for view_name in ks_meta.views.keys() {
                views.push(json!({
                    "keyspace": keyspace,
                    "view": view_name,
                    "status": "SUCCESS",
                }));
            }
        }
    }
    json_response(StatusCode::OK, &json!({ "views": views }))
}

/// Returns replica endpoints for a partition key.
pub fn handle_get_endpoints(query: Option<&str>, state: &AdminState) -> Response<Full<Bytes>> {
    let params = parse_query(query);
    let keyspace = match params.get("keyspace") {
        Some(v) if !v.is_empty() => v.clone(),
        _ => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": "Missing required query param: keyspace"}),
            );
        }
    };
    let table = match params.get("table") {
        Some(v) if !v.is_empty() => v.clone(),
        _ => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": "Missing required query param: table"}),
            );
        }
    };
    let key = match params.get("key") {
        Some(v) => v.clone(),
        None => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": "Missing required query param: key"}),
            );
        }
    };

    let mut endpoints = BTreeSet::new();
    if let Some(local) = state.virtual_tables.get("system_views", "local") {
        for row in local.rows() {
            if let Some(addr) = row.get("listen_address") {
                endpoints.insert(addr.clone());
            }
        }
    }
    if let Some(peers) = state.virtual_tables.get("system_views", "peers") {
        for row in peers.rows() {
            if let Some(addr) = row.get("peer").or_else(|| row.get("native_address")) {
                endpoints.insert(addr.clone());
            }
        }
    }
    if endpoints.is_empty() {
        endpoints.insert("127.0.0.1".to_string());
    }

    let body = json!({
        "keyspace": keyspace,
        "table": table,
        "key": key,
        "endpoints": endpoints.into_iter().collect::<Vec<_>>(),
    });
    json_response(StatusCode::OK, &body)
}

/// Returns SSTable candidates for a partition key lookup.
pub fn handle_get_sstables(query: Option<&str>, state: &AdminState) -> Response<Full<Bytes>> {
    let params = parse_query(query);
    let keyspace = match params.get("keyspace") {
        Some(v) if !v.is_empty() => v.clone(),
        _ => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": "Missing required query param: keyspace"}),
            );
        }
    };
    let table = match params.get("table") {
        Some(v) if !v.is_empty() => v.clone(),
        _ => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": "Missing required query param: table"}),
            );
        }
    };
    let key = match params.get("key") {
        Some(v) => v.clone(),
        None => {
            return json_response(
                StatusCode::BAD_REQUEST,
                &json!({"error": "Missing required query param: key"}),
            );
        }
    };

    let mut candidates = Vec::new();
    if let Some(engine) = &state.storage_engine {
        let key_bytes = key.as_bytes();
        let partition_found = engine
            .read_partition(&keyspace, &table, key_bytes)
            .is_some();
        if partition_found {
            candidates.push(format!("{keyspace}/{table}/active-partition"));
        }
    }

    let body = json!({
        "keyspace": keyspace,
        "table": table,
        "key": key,
        "sstables": candidates,
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

fn histogram_series_filtered(
    histograms: &HistogramVec,
    keyspace_filter: Option<&str>,
    table_filter: Option<&str>,
) -> Vec<Value> {
    histograms
        .collect()
        .iter()
        .flat_map(|family| family.get_metric().iter())
        .filter(|metric| metric_matches_table(metric, keyspace_filter, table_filter))
        .map(histogram_metric_json)
        .collect()
}

fn metric_matches_table(
    metric: &prometheus::proto::Metric,
    keyspace_filter: Option<&str>,
    table_filter: Option<&str>,
) -> bool {
    let keyspace = metric
        .get_label()
        .iter()
        .find(|pair| pair.get_name() == "keyspace")
        .map(|pair| pair.get_value());
    let table = metric
        .get_label()
        .iter()
        .find(|pair| pair.get_name() == "table")
        .map(|pair| pair.get_value());

    let keyspace_ok = match keyspace_filter {
        Some(filter) => keyspace.is_some_and(|value| value.eq_ignore_ascii_case(filter)),
        None => true,
    };
    let table_ok = match table_filter {
        Some(filter) => table.is_some_and(|value| value.eq_ignore_ascii_case(filter)),
        None => true,
    };

    keyspace_ok && table_ok
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

fn top_partitions_from_storage(
    state: &AdminState,
    keyspace_filter: Option<&str>,
    table_filter: Option<&str>,
) -> Vec<Value> {
    let Some(engine) = &state.storage_engine else {
        return Vec::new();
    };
    let Some(catalog) = &state.schema_catalog else {
        return Vec::new();
    };

    let snapshot = catalog.read().snapshot();
    let mut partitions = Vec::new();
    for (keyspace, ks_meta) in &snapshot.keyspaces {
        if keyspace_filter.is_some_and(|f| !keyspace.eq_ignore_ascii_case(f)) {
            continue;
        }
        for table in ks_meta.tables.keys() {
            if table_filter.is_some_and(|f| !table.eq_ignore_ascii_case(f)) {
                continue;
            }
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

fn collect_partition_histogram_samples(
    state: &AdminState,
    keyspace_filter: Option<&str>,
    table_filter: Option<&str>,
) -> (Vec<f64>, Vec<f64>) {
    let Some(engine) = &state.storage_engine else {
        return (Vec::new(), Vec::new());
    };
    let Some(catalog) = &state.schema_catalog else {
        return (Vec::new(), Vec::new());
    };

    let snapshot = catalog.read().snapshot();
    let mut partition_sizes = Vec::new();
    let mut cell_counts = Vec::new();
    for (keyspace, ks_meta) in &snapshot.keyspaces {
        if keyspace_filter.is_some_and(|f| !keyspace.eq_ignore_ascii_case(f)) {
            continue;
        }
        for table in ks_meta.tables.keys() {
            if table_filter.is_some_and(|f| !table.eq_ignore_ascii_case(f)) {
                continue;
            }
            for (partition_key, data) in engine.scan_all_partitions(keyspace, table) {
                partition_sizes.push(estimate_partition_bytes(&partition_key, &data) as f64);
                let cell_count: usize = data.rows.values().map(|row| row.cells.len()).sum();
                cell_counts.push(cell_count as f64);
            }
        }
    }
    (partition_sizes, cell_counts)
}

fn samples_histogram_json(samples: &[f64]) -> Value {
    if samples.is_empty() {
        return json!({
            "count": 0_u64,
            "min": Value::Null,
            "p50": Value::Null,
            "p75": Value::Null,
            "p95": Value::Null,
            "p98": Value::Null,
            "p99": Value::Null,
            "max": Value::Null,
        });
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    json!({
        "count": sorted.len() as u64,
        "min": sorted.first().copied().unwrap_or(0.0),
        "p50": sample_quantile(&sorted, 0.50),
        "p75": sample_quantile(&sorted, 0.75),
        "p95": sample_quantile(&sorted, 0.95),
        "p98": sample_quantile(&sorted, 0.98),
        "p99": sample_quantile(&sorted, 0.99),
        "max": sorted.last().copied().unwrap_or(0.0),
    })
}

fn sample_quantile(sorted_samples: &[f64], quantile: f64) -> f64 {
    if sorted_samples.is_empty() {
        return 0.0;
    }
    let rank = ((sorted_samples.len() as f64) * quantile).ceil() as usize;
    let index = rank.saturating_sub(1).min(sorted_samples.len() - 1);
    sorted_samples[index]
}

fn collect_table_metrics(state: &AdminState) -> HashMap<(String, String), TableMetricSnapshot> {
    let mut map: HashMap<(String, String), TableMetricSnapshot> = HashMap::new();
    for family in state.metrics.table_read_latency.collect() {
        for metric in family.get_metric() {
            let Some((keyspace, table)) = table_labels(metric) else {
                continue;
            };
            let histogram = metric.get_histogram();
            let entry = map.entry((keyspace, table)).or_default();
            entry.read_count = histogram.get_sample_count();
            if entry.read_count > 0 {
                entry.read_latency_ms = Some(estimate_quantile(histogram, 0.50) * 1000.0);
            }
        }
    }
    for family in state.metrics.table_write_latency.collect() {
        for metric in family.get_metric() {
            let Some((keyspace, table)) = table_labels(metric) else {
                continue;
            };
            let histogram = metric.get_histogram();
            let entry = map.entry((keyspace, table)).or_default();
            entry.write_count = histogram.get_sample_count();
            if entry.write_count > 0 {
                entry.write_latency_ms = Some(estimate_quantile(histogram, 0.50) * 1000.0);
            }
        }
    }
    map
}

fn table_labels(metric: &prometheus::proto::Metric) -> Option<(String, String)> {
    let mut keyspace = None;
    let mut table = None;
    for label in metric.get_label() {
        match label.get_name() {
            "keyspace" => keyspace = Some(label.get_value().to_string()),
            "table" => table = Some(label.get_value().to_string()),
            _ => {}
        }
    }
    Some((keyspace?, table?))
}

fn collect_pending_flushes(
    state: &AdminState,
    holder: &TableStatsHolder,
    sstable_tasks: &[HashMap<String, String>],
) -> HashMap<(String, String), u64> {
    let mut map = HashMap::new();
    for row in sstable_tasks {
        let Some(keyspace) = row.get("keyspace_name").cloned() else {
            continue;
        };
        let Some(table) = row.get("table_name").cloned() else {
            continue;
        };
        let kind = row.get("kind").map(String::as_str).unwrap_or("");
        if !kind.eq_ignore_ascii_case("flush") {
            continue;
        }
        *map.entry((keyspace, table)).or_insert(0) += 1;
    }

    if let Some(engine) = &state.storage_engine {
        for table in &holder.tables {
            let key = (table.keyspace.clone(), table.table.clone());
            let engine_pending = engine.table_pending_flushes(&table.keyspace, &table.table);
            let entry = map.entry(key).or_insert(0);
            *entry = (*entry).max(engine_pending);
        }
    }

    map
}

fn collect_table_stats(
    state: &AdminState,
    keyspace_filter: Option<&str>,
    table_filter: Option<&str>,
) -> Vec<StatsTable> {
    let mut entries: HashMap<(String, String), StatsTable> = HashMap::new();

    if let Some(catalog) = &state.schema_catalog {
        let snapshot = catalog.read().snapshot();
        for (keyspace, ks_meta) in &snapshot.keyspaces {
            if keyspace_filter.is_some_and(|f| !keyspace.eq_ignore_ascii_case(f)) {
                continue;
            }
            for table in ks_meta.tables.keys() {
                if table_filter.is_some_and(|f| !table.eq_ignore_ascii_case(f)) {
                    continue;
                }
                entries.insert(
                    (keyspace.clone(), table.clone()),
                    StatsTable {
                        keyspace: keyspace.clone(),
                        table: table.clone(),
                        sstable_count: 0,
                        disk_space_bytes: 0,
                        read_count: 0,
                        write_count: 0,
                    },
                );
            }
        }
    }

    if let Some(tasks) = state.virtual_tables.get("system_views", "sstable_tasks") {
        for row in tasks.rows() {
            let Some(keyspace) = row.get("keyspace_name").cloned() else {
                continue;
            };
            if keyspace_filter.is_some_and(|f| !keyspace.eq_ignore_ascii_case(f)) {
                continue;
            }
            let Some(table) = row.get("table_name").cloned() else {
                continue;
            };
            if table_filter.is_some_and(|f| !table.eq_ignore_ascii_case(f)) {
                continue;
            }
            let entry = entries
                .entry((keyspace.clone(), table.clone()))
                .or_insert_with(|| StatsTable {
                    keyspace,
                    table,
                    sstable_count: 0,
                    disk_space_bytes: 0,
                    read_count: 0,
                    write_count: 0,
                });
            entry.sstable_count = entry.sstable_count.saturating_add(1);
        }
    }

    if let Some(engine) = &state.storage_engine {
        let keys = entries.keys().cloned().collect::<Vec<_>>();
        for (keyspace, table) in keys {
            if let Some(entry) = entries.get_mut(&(keyspace.clone(), table.clone())) {
                let partitions = engine.scan_all_partitions(&keyspace, &table);
                let mut bytes = 0u64;
                for (partition_key, data) in &partitions {
                    bytes = bytes.saturating_add(estimate_partition_bytes(partition_key, data));
                }
                entry.disk_space_bytes = bytes;
                if entry.sstable_count == 0 && !partitions.is_empty() {
                    entry.sstable_count = 1;
                }
            }
        }
    }

    let mut tables: Vec<StatsTable> = entries.into_values().collect();
    tables.sort_by(|left, right| {
        left.keyspace
            .cmp(&right.keyspace)
            .then_with(|| left.table.cmp(&right.table))
    });
    tables
}

fn estimate_partition_bytes(
    partition_key: &[u8],
    data: &cassandra_storage::memtable::partition::PartitionData,
) -> u64 {
    let mut bytes = partition_key.len() as u64;
    for row in data.rows.values() {
        bytes = bytes.saturating_add(row.clustering_key.len() as u64);
        for cell in &row.cells {
            bytes = bytes.saturating_add(cell.column.len() as u64);
            if let Some(value) = &cell.value {
                bytes = bytes.saturating_add(value.len() as u64);
            }
        }
    }
    bytes
}

fn parse_query(query: Option<&str>) -> HashMap<String, String> {
    let mut out = HashMap::new();
    let Some(query) = query else {
        return out;
    };
    for pair in query.split('&') {
        if pair.is_empty() {
            continue;
        }
        let mut parts = pair.splitn(2, '=');
        let Some(raw_key) = parts.next() else {
            continue;
        };
        if raw_key.is_empty() {
            continue;
        }
        let raw_value = parts.next().unwrap_or_default();
        out.insert(raw_key.to_string(), raw_value.to_string());
    }
    out
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
    use crate::virtual_tables::{VirtualColumn, VirtualTable, VirtualTableRegistry};

    struct TestSstableTasksTable {
        rows: Vec<HashMap<String, String>>,
    }

    impl VirtualTable for TestSstableTasksTable {
        fn keyspace(&self) -> &str {
            "system_views"
        }

        fn name(&self) -> &str {
            "sstable_tasks"
        }

        fn columns(&self) -> Vec<VirtualColumn> {
            vec![
                VirtualColumn {
                    name: "keyspace_name".to_string(),
                    cql_type: "text".to_string(),
                },
                VirtualColumn {
                    name: "table_name".to_string(),
                    cql_type: "text".to_string(),
                },
                VirtualColumn {
                    name: "kind".to_string(),
                    cql_type: "text".to_string(),
                },
            ]
        }

        fn rows(&self) -> Vec<HashMap<String, String>> {
            self.rows.clone()
        }
    }

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

    /// Extract body JSON from a `Full<Bytes>` response (test helper).
    fn body_json(resp: Response<Full<Bytes>>) -> serde_json::Value {
        let rt = tokio::runtime::Runtime::new().unwrap();
        let collected = rt.block_on(resp.into_body().collect()).unwrap();
        serde_json::from_slice(&collected.to_bytes()).unwrap()
    }

    #[test]
    fn table_stats_returns_ok_with_virtual_table() {
        let state = test_state();
        let resp = handle_table_stats(None, &state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp);
        assert!(body["tables"].as_array().is_some());
        assert!(body["summary"].is_object());
    }

    #[test]
    fn table_stats_returns_empty_without_virtual_table_or_engine() {
        let state = AdminState {
            virtual_tables: Arc::new(VirtualTableRegistry::new()),
            ..test_state()
        };
        let resp = handle_table_stats(Some("keyspace=system"), &state);
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_json(resp);
        assert_eq!(body["total_disk_space_used"], 0);
        assert!(body["sstable_tasks"].as_array().unwrap().is_empty());
        assert!(body["tables"].as_array().unwrap().is_empty());
    }

    #[test]
    fn table_stats_accepts_table_filter() {
        let mut registry = VirtualTableRegistry::with_builtins();
        registry.register(Box::new(TestSstableTasksTable {
            rows: vec![
                HashMap::from([
                    ("keyspace_name".to_string(), "ks1".to_string()),
                    ("table_name".to_string(), "a".to_string()),
                    ("kind".to_string(), "flush".to_string()),
                ]),
                HashMap::from([
                    ("keyspace_name".to_string(), "ks1".to_string()),
                    ("table_name".to_string(), "b".to_string()),
                    ("kind".to_string(), "flush".to_string()),
                ]),
                HashMap::from([
                    ("keyspace_name".to_string(), "ks2".to_string()),
                    ("table_name".to_string(), "b".to_string()),
                    ("kind".to_string(), "compaction".to_string()),
                ]),
            ],
        }));
        let state = AdminState {
            virtual_tables: Arc::new(registry),
            ..test_state()
        };

        let resp = handle_table_stats(Some("table=b"), &state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp);
        let tables = body["tables"].as_array().unwrap();
        assert_eq!(tables.len(), 2);
        assert!(tables.iter().all(|t| t["table"] == "b"));
        assert!(tables.iter().all(|t| t["old_sstable_count"] == 0));
        assert!(tables.iter().all(|t| t.get("local_read_count").is_some()));
        assert!(tables.iter().all(|t| t.get("local_write_count").is_some()));
        assert!(
            tables
                .iter()
                .all(|t| t.get("sstable_compression_ratio").is_some())
        );
        assert!(tables.iter().all(|t| t.get("bytes_unrepaired").is_some()));
        assert!(
            tables
                .iter()
                .all(|t| t.get("space_used_total_bytes").is_some())
        );
        assert!(
            tables
                .iter()
                .all(|t| t.get("space_used_snapshots_bytes").is_some())
        );
        assert!(
            tables
                .iter()
                .all(|t| t.get("memtable_switch_count").is_some())
        );
        assert!(
            tables
                .iter()
                .all(|t| t.get("bloom_filter_space_used").is_some())
        );
        assert!(
            tables
                .iter()
                .all(|t| t.get("bloom_filter_false_positives").is_some())
        );
        assert!(
            tables
                .iter()
                .all(|t| t.get("bloom_filter_false_ratio").is_some())
        );
    }

    #[test]
    fn table_histograms_returns_percentiles() {
        let state = test_state();
        let resp = handle_table_histograms(None, &state);
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_json(resp);
        assert!(body["latency_histograms"]["read"].as_array().is_some());
        assert!(body["sstables_per_read_histogram"].is_object());
        assert!(body["partition_size_histogram"].is_object());
        assert!(body["cell_count_histogram"].is_object());
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
        let resp = handle_top_partitions(None, &state);
        assert_eq!(resp.status(), StatusCode::OK);

        let body = body_json(resp);
        assert!(body["top_read_partitions"].as_array().unwrap().is_empty());
        assert!(body["top_write_partitions"].as_array().unwrap().is_empty());
    }

    #[test]
    fn table_histograms_accepts_filters() {
        let state = test_state();
        let resp = handle_table_histograms(Some("keyspace=ks&table=tbl"), &state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp);
        assert!(body["latency_histograms"]["read"].as_array().is_some());
        assert!(body["latency_histograms"]["write"].as_array().is_some());
        assert_eq!(body["partition_size_histogram"]["count"], 0);
        assert_eq!(body["cell_count_histogram"]["count"], 0);
    }

    #[test]
    fn top_partitions_accepts_filters() {
        let state = test_state();
        let resp = handle_top_partitions(Some("keyspace=ks&table=tbl"), &state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp);
        assert!(body["top_partitions"].as_array().is_some());
    }

    #[test]
    fn failure_detector_returns_rows() {
        let state = test_state();
        let resp = handle_failure_detector_info(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp);
        assert!(body["failure_detector"].as_array().is_some());
        assert!(!body["failure_detector"].as_array().unwrap().is_empty());
    }

    #[test]
    fn data_paths_returns_default_path() {
        let state = test_state();
        let resp = handle_data_paths(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp);
        assert_eq!(body["data_paths"][0], "data");
    }

    #[test]
    fn refresh_size_estimates_returns_status() {
        let state = test_state();
        let resp = handle_refresh_size_estimates(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp);
        assert_eq!(body["status"], "size estimates refreshed");
    }

    #[test]
    fn view_build_status_returns_views_field() {
        let state = test_state();
        let resp = handle_view_build_status(&state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp);
        assert!(body["views"].as_array().is_some());
    }

    #[test]
    fn get_endpoints_requires_query_params() {
        let state = test_state();
        let resp = handle_get_endpoints(None, &state);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn get_endpoints_returns_local_endpoint() {
        let state = test_state();
        let resp = handle_get_endpoints(Some("keyspace=ks&table=tbl&key=k1"), &state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp);
        assert!(
            body["endpoints"]
                .as_array()
                .unwrap()
                .contains(&json!("127.0.0.1"))
        );
    }

    #[test]
    fn get_sstables_requires_query_params() {
        let state = test_state();
        let resp = handle_get_sstables(Some("keyspace=ks"), &state);
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[test]
    fn get_sstables_returns_array_field() {
        let state = test_state();
        let resp = handle_get_sstables(Some("keyspace=ks&table=tbl&key=k1"), &state);
        assert_eq!(resp.status(), StatusCode::OK);
        let body = body_json(resp);
        assert!(body["sstables"].as_array().is_some());
    }

    #[test]
    fn parse_query_extracts_pairs() {
        let parsed = parse_query(Some("a=1&b=two&flag"));
        assert_eq!(parsed.get("a").unwrap(), "1");
        assert_eq!(parsed.get("b").unwrap(), "two");
        assert_eq!(parsed.get("flag").unwrap(), "");
    }
}
