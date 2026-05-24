// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

//! Statistics commands — nodetool equivalents for table stats, thread pools,
//! GC stats, proxy histograms, top partitions, and more.

use crate::admin_client::AdminClient;
use crate::table_formatter::{TableColumn, TableFormatter};
use cassandra_admin::{StatsTable, TableStatsHolder};
use serde_json::Value;
use std::collections::{BTreeMap, HashMap};

/// Show table statistics, optionally filtered by keyspace/table.
pub fn table_stats(client: &AdminClient, keyspace: Option<&str>, table: Option<&str>) {
    let mut path = "/api/v1/stats/tables".to_string();
    let mut params = Vec::new();
    if let Some(ks) = keyspace {
        params.push(format!("keyspace={}", ks));
    }
    if let Some(tbl) = table {
        params.push(format!("table={}", tbl));
    }
    if !params.is_empty() {
        path.push('?');
        path.push_str(&params.join("&"));
    }
    match client.get(&path) {
        Ok(value) => match format_table_stats(&value, keyspace, table) {
            Some(formatted) => println!("{formatted}"),
            None => print_json(&value),
        },
        Err(e) => eprintln!("Error fetching table stats: {}", e),
    }
}

/// Show table histograms (read/write latency distributions).
pub fn table_histograms(client: &AdminClient, keyspace: Option<&str>, table: Option<&str>) {
    let mut path = "/api/v1/stats/histograms".to_string();
    let mut params = Vec::new();
    if let Some(ks) = keyspace {
        params.push(format!("keyspace={}", ks));
    }
    if let Some(tbl) = table {
        params.push(format!("table={}", tbl));
    }
    if !params.is_empty() {
        path.push('?');
        path.push_str(&params.join("&"));
    }
    match client.get(&path) {
        Ok(value) => match format_table_histograms(&value) {
            Some(formatted) => println!("{formatted}"),
            None => print_json(&value),
        },
        Err(e) => eprintln!("Error fetching table histograms: {}", e),
    }
}

/// Show thread pool statistics.
pub fn tp_stats(client: &AdminClient) {
    match client.get("/api/v1/stats/tpstats") {
        Ok(value) => match format_tpstats_table(&value) {
            Some(formatted) => println!("{formatted}"),
            None => print_json(&value),
        },
        Err(e) => eprintln!("Error fetching thread pool stats: {}", e),
    }
}

/// Show garbage collection statistics.
pub fn gc_stats(client: &AdminClient) {
    match client.get("/api/v1/stats/gcstats") {
        Ok(value) => match format_gc_stats(&value) {
            Some(formatted) => println!("{formatted}"),
            None => print_json(&value),
        },
        Err(e) => eprintln!("Error fetching GC stats: {}", e),
    }
}

/// Show coordinator read/write latency percentiles (proxy histograms).
pub fn proxy_histograms(client: &AdminClient) {
    match client.get("/api/v1/stats/proxyhistograms") {
        Ok(value) => match format_proxy_histograms_table(&value) {
            Some(formatted) => println!("{formatted}"),
            None => print_json(&value),
        },
        Err(e) => eprintln!("Error fetching proxy histograms: {}", e),
    }
}

/// Show connected client statistics.
pub fn client_stats(client: &AdminClient) {
    match client.get("/api/v1/stats/clients") {
        Ok(value) => match format_client_stats(&value) {
            Some(formatted) => println!("{formatted}"),
            None => print_json(&value),
        },
        Err(e) => eprintln!("Error fetching client stats: {}", e),
    }
}

/// Show top partitions by read/write activity over a sampling duration.
pub fn top_partitions(
    client: &AdminClient,
    keyspace: Option<&str>,
    table: Option<&str>,
    duration_ms: u64,
) {
    let mut path = format!("/api/v1/stats/toppartitions?duration_ms={}", duration_ms);
    if let Some(ks) = keyspace {
        path.push_str(&format!("&keyspace={}", ks));
    }
    if let Some(tbl) = table {
        path.push_str(&format!("&table={}", tbl));
    }
    match client.get(&path) {
        Ok(value) => match format_top_partitions(&value) {
            Some(formatted) => println!("{formatted}"),
            None => print_json(&value),
        },
        Err(e) => eprintln!("Error fetching top partitions: {}", e),
    }
}

/// Show failure detector information (phi values for each endpoint).
pub fn failure_detector_info(client: &AdminClient) {
    match client.get("/api/v1/stats/failure_detector_info") {
        Ok(value) => match format_failure_detector_info(&value) {
            Some(formatted) => println!("{formatted}"),
            None => print_json(&value),
        },
        Err(e) => {
            let msg = format!("{}", e);
            if msg.contains("404") {
                println!("Failure detector info is not available on this node.");
            } else {
                eprintln!("Error fetching failure detector info: {}", e);
            }
        }
    }
}

/// Show data file paths for this node.
pub fn data_paths(client: &AdminClient) {
    match client.get("/api/v1/stats/data_paths") {
        Ok(value) => match format_data_paths(&value) {
            Some(formatted) => println!("{formatted}"),
            None => print_json(&value),
        },
        Err(e) => {
            let msg = format!("{}", e);
            if msg.contains("404") {
                println!("Data paths endpoint is not available on this node.");
            } else {
                eprintln!("Error fetching data paths: {}", e);
            }
        }
    }
}

/// Force a refresh of size estimates for all tables.
pub fn refresh_size_estimates(client: &AdminClient) {
    match client.get("/api/v1/stats/refresh_size_estimates") {
        Ok(value) => match format_refresh_size_estimates(&value) {
            Some(formatted) => println!("{formatted}"),
            None => print_json(&value),
        },
        Err(e) => {
            let msg = format!("{}", e);
            if msg.contains("404") {
                println!("Refresh size estimates is not available on this node.");
            } else {
                eprintln!("Error refreshing size estimates: {}", e);
            }
        }
    }
}

/// Show materialized view build status.
pub fn view_build_status(client: &AdminClient) {
    match client.get("/api/v1/stats/view_build_status") {
        Ok(value) => match format_view_build_status(&value) {
            Some(formatted) => println!("{formatted}"),
            None => print_json(&value),
        },
        Err(e) => {
            let msg = format!("{}", e);
            if msg.contains("404") {
                println!("View build status is not available on this node.");
            } else {
                eprintln!("Error fetching view build status: {}", e);
            }
        }
    }
}

/// Get the endpoints responsible for a given partition key.
pub fn get_endpoints(client: &AdminClient, keyspace: &str, table: &str, key: &str) {
    let path = format!(
        "/api/v1/stats/endpoints?keyspace={}&table={}&key={}",
        keyspace, table, key
    );
    match client.get(&path) {
        Ok(value) => match format_endpoints(&value) {
            Some(formatted) => println!("{formatted}"),
            None => print_json(&value),
        },
        Err(e) => eprintln!("Error fetching endpoints: {}", e),
    }
}

/// Get the SSTables containing a given partition key.
pub fn get_sstables(client: &AdminClient, keyspace: &str, table: &str, key: &str) {
    let path = format!(
        "/api/v1/stats/sstables?keyspace={}&table={}&key={}",
        keyspace, table, key
    );
    match client.get(&path) {
        Ok(value) => match format_sstables(&value) {
            Some(formatted) => println!("{formatted}"),
            None => print_json(&value),
        },
        Err(e) => eprintln!("Error fetching SSTables: {}", e),
    }
}

fn print_json(value: &Value) {
    println!(
        "{}",
        serde_json::to_string_pretty(value).unwrap_or_else(|_| value.to_string())
    );
}

fn format_tpstats_table(value: &Value) -> Option<String> {
    let pools = value.get("thread_pools")?.as_array()?;
    let mut table = TableFormatter::new(vec![
        TableColumn::left("Pool Name").min_width(18),
        TableColumn::right("Active").min_width(6),
        TableColumn::right("Pending").min_width(7),
        TableColumn::right("Completed").min_width(9),
        TableColumn::right("Blocked").min_width(7),
        TableColumn::right("All time blocked").min_width(16),
    ]);

    for pool in pools {
        let name = field_as_text(pool, &["name", "pool_name"], "?");
        let active = field_as_text(pool, &["active", "active_tasks"], "0");
        let pending = field_as_text(pool, &["pending", "pending_tasks"], "0");
        let completed = field_as_text(pool, &["completed", "completed_tasks"], "0");
        let blocked = field_as_text(pool, &["blocked", "blocked_tasks"], "0");
        let all_time = field_as_text(pool, &["all_time_blocked", "all_time_blocked_tasks"], "0");
        table.add_row([name, active, pending, completed, blocked, all_time]);
    }

    Some(table.render())
}

fn format_table_stats(
    value: &Value,
    keyspace_filter: Option<&str>,
    table_filter: Option<&str>,
) -> Option<String> {
    let mut holder: TableStatsHolder = serde_json::from_value(value.clone()).ok()?;
    holder = holder.sorted_by_name();
    if let Some(keyspace) = keyspace_filter {
        holder = holder.filter_keyspace(keyspace);
    }
    if let Some(table) = table_filter {
        holder = TableStatsHolder::new(
            holder
                .tables
                .iter()
                .filter(|t| t.table.eq_ignore_ascii_case(table))
                .cloned()
                .collect(),
        );
    }
    let total_disk_space_used = value
        .get("total_disk_space_used")
        .and_then(Value::as_u64)
        .unwrap_or_else(|| holder.total_disk_space_bytes());
    let table_extras = parse_table_extras(value);
    let keyspace_extras = parse_keyspace_extras(value);
    Some(render_table_stats_by_keyspace(
        &holder,
        total_disk_space_used,
        &table_extras,
        &keyspace_extras,
    ))
}

fn format_table_histograms(value: &Value) -> Option<String> {
    let latency = value.get("latency_histograms")?;
    let read = latency
        .get("read")
        .and_then(Value::as_array)
        .and_then(|arr| arr.first())
        .unwrap_or(&Value::Null);
    let write = latency
        .get("write")
        .and_then(Value::as_array)
        .and_then(|arr| arr.first())
        .unwrap_or(&Value::Null);
    let sstables = value
        .get("sstables_per_read_histogram")
        .unwrap_or(&Value::Null);
    let partition_size = value
        .get("partition_size_histogram")
        .unwrap_or(&Value::Null);
    let cell_count = value.get("cell_count_histogram").unwrap_or(&Value::Null);

    let mut table = TableFormatter::new(vec![
        TableColumn::left("Percentile").min_width(10),
        TableColumn::right("SSTables").min_width(8),
        TableColumn::right("Write Latency").min_width(13),
        TableColumn::right("Read Latency").min_width(12),
        TableColumn::right("Partition Size").min_width(14),
        TableColumn::right("Cell Count").min_width(10),
    ]);
    for (label, key) in [
        ("50%", "p50"),
        ("75%", "p75"),
        ("95%", "p95"),
        ("98%", "p98"),
        ("99%", "p99"),
        ("Min", "min"),
        ("Max", "max"),
    ] {
        table.add_row([
            label.to_string(),
            format_histogram_number(histogram_value(sstables, key)),
            format_histogram_number(write.get(key).and_then(Value::as_f64)),
            format_histogram_number(read.get(key).and_then(Value::as_f64)),
            format_histogram_number(histogram_value(partition_size, key)),
            format_histogram_number(histogram_value(cell_count, key)),
        ]);
    }

    let mut lines = Vec::new();
    lines.push(table.render());
    lines.push(String::new());
    lines.push(format!(
        "SSTables per read count: {}",
        format_uint(sstables.get("count"))
    ));
    lines.push(format!("Read count: {}", format_uint(read.get("count"))));
    lines.push(format!("Write count: {}", format_uint(write.get("count"))));
    lines.push(format!(
        "Partition size count: {}",
        format_uint(partition_size.get("count"))
    ));
    lines.push(format!(
        "Cell count: {}",
        format_uint(cell_count.get("count"))
    ));
    Some(lines.join("\n"))
}

fn format_gc_stats(value: &Value) -> Option<String> {
    let enabled = value.get("gc_enabled").and_then(Value::as_bool)?;
    let note = value
        .get("note")
        .and_then(Value::as_str)
        .unwrap_or("No additional details.");
    let mut lines = Vec::new();
    lines.push(format!(
        "GC status: {}",
        if enabled { "enabled" } else { "disabled" }
    ));
    lines.push(format!("Note: {note}"));
    Some(lines.join("\n"))
}

fn format_top_partitions(value: &Value) -> Option<String> {
    let top = value
        .get("top_partitions")
        .and_then(Value::as_array)
        .or_else(|| value.get("top_read_partitions").and_then(Value::as_array))?;
    if top.is_empty() {
        return Some("No top partitions found.".to_string());
    }
    let mut table = TableFormatter::new(vec![
        TableColumn::left("Keyspace").min_width(8),
        TableColumn::left("Table").min_width(8),
        TableColumn::left("Partition Key").min_width(16),
        TableColumn::right("Rows").min_width(4),
        TableColumn::right("Cells").min_width(5),
    ]);
    for row in top {
        let keyspace = field_as_text(row, &["keyspace"], "?");
        let table_name = field_as_text(row, &["table"], "?");
        let partition_key = field_as_text(row, &["partition_key_hex"], "?");
        let rows = field_as_text(row, &["rows"], "0");
        let cells = field_as_text(row, &["cells"], "0");
        table.add_row([keyspace, table_name, partition_key, rows, cells]);
    }
    Some(table.render())
}

fn format_client_stats(value: &Value) -> Option<String> {
    let connected_clients = value.get("connected_clients").and_then(Value::as_u64);
    let clients = value.get("clients").and_then(Value::as_array);
    let mut lines = Vec::new();
    if let Some(count) = connected_clients {
        lines.push(format!("Connected clients: {count}"));
    }
    let Some(clients) = clients else {
        if lines.is_empty() {
            return None;
        }
        return Some(lines.join("\n"));
    };

    lines.push(String::new());
    let mut table = TableFormatter::new(vec![
        TableColumn::left("Address").min_width(15),
        TableColumn::right("Port").min_width(4),
        TableColumn::left("User").min_width(8),
        TableColumn::left("Stage").min_width(8),
        TableColumn::right("Proto").min_width(5),
        TableColumn::left("SSL").min_width(3),
    ]);
    for client in clients {
        let address = field_as_text(client, &["address"], "?");
        let port = field_as_text(client, &["port"], "?");
        let username = field_as_text(client, &["username"], "?");
        let stage = field_as_text(client, &["connection_stage"], "?");
        let proto = field_as_text(client, &["protocol_version"], "?");
        let ssl = field_as_text(client, &["ssl"], "?");
        table.add_row([address, port, username, stage, proto, ssl]);
    }
    lines.push(table.render());
    Some(lines.join("\n"))
}

fn format_failure_detector_info(value: &Value) -> Option<String> {
    let rows = value
        .get("failure_detector")
        .and_then(Value::as_array)
        .or_else(|| value.get("endpoints").and_then(Value::as_array))?;
    let mut table = TableFormatter::new(vec![
        TableColumn::left("Endpoint").min_width(15),
        TableColumn::right("Phi").min_width(7),
        TableColumn::left("Status").min_width(6),
    ]);
    for row in rows {
        let endpoint = field_as_text(row, &["endpoint", "address"], "?");
        let phi = if let Some(v) = row.get("phi").and_then(Value::as_f64) {
            format!("{v:.3}")
        } else {
            "NaN".to_string()
        };
        let status = field_as_text(row, &["status"], "?");
        table.add_row([endpoint, phi, status]);
    }
    Some(table.render())
}

fn format_data_paths(value: &Value) -> Option<String> {
    let paths = value.get("data_paths").and_then(Value::as_array)?;
    let mut lines = vec!["Data paths:".to_string()];
    for path in paths {
        if let Some(p) = path.as_str() {
            lines.push(format!("  {p}"));
        }
    }
    Some(lines.join("\n"))
}

fn format_refresh_size_estimates(value: &Value) -> Option<String> {
    let status = value.get("status").and_then(Value::as_str)?;
    Some(format!("Refresh size estimates: {status}"))
}

fn format_view_build_status(value: &Value) -> Option<String> {
    let views = value.get("views").and_then(Value::as_array)?;
    if views.is_empty() {
        return Some("No materialized views are currently tracked.".to_string());
    }
    let mut table = TableFormatter::new(vec![
        TableColumn::left("Keyspace").min_width(8),
        TableColumn::left("View").min_width(8),
        TableColumn::left("Status").min_width(7),
    ]);
    for view in views {
        let keyspace = field_as_text(view, &["keyspace"], "?");
        let name = field_as_text(view, &["view"], "?");
        let status = field_as_text(view, &["status"], "?");
        table.add_row([keyspace, name, status]);
    }
    Some(table.render())
}

fn format_endpoints(value: &Value) -> Option<String> {
    let endpoints = value.get("endpoints").and_then(Value::as_array)?;
    if endpoints.is_empty() {
        return Some("No endpoints found.".to_string());
    }
    let mut lines = Vec::new();
    if let (Some(ks), Some(tbl), Some(key)) = (
        value.get("keyspace").and_then(Value::as_str),
        value.get("table").and_then(Value::as_str),
        value.get("key").and_then(Value::as_str),
    ) {
        lines.push(format!("Endpoints for {ks}.{tbl} key={key}:"));
    } else {
        lines.push("Endpoints:".to_string());
    }
    for endpoint in endpoints {
        if let Some(ep) = endpoint.as_str() {
            lines.push(format!("  {ep}"));
        }
    }
    Some(lines.join("\n"))
}

fn format_sstables(value: &Value) -> Option<String> {
    let sstables = value.get("sstables").and_then(Value::as_array)?;
    if sstables.is_empty() {
        return Some("No SSTables found for key.".to_string());
    }
    let mut lines = Vec::new();
    if let (Some(ks), Some(tbl), Some(key)) = (
        value.get("keyspace").and_then(Value::as_str),
        value.get("table").and_then(Value::as_str),
        value.get("key").and_then(Value::as_str),
    ) {
        lines.push(format!("SSTables for {ks}.{tbl} key={key}:"));
    } else {
        lines.push("SSTables:".to_string());
    }
    for sstable in sstables {
        if let Some(path) = sstable.as_str() {
            lines.push(format!("  {path}"));
        }
    }
    Some(lines.join("\n"))
}

#[derive(Debug, Clone, Default)]
struct TableStatsExtras {
    old_sstable_count: Option<u64>,
    max_sstable_size: Option<u64>,
    pending_flushes: u64,
    sstable_compression_ratio: Option<f64>,
    number_of_partitions_estimate: Option<u64>,
    memtable_cell_count: Option<u64>,
    memtable_data_size: Option<u64>,
    memtable_switch_count: Option<u64>,
    speculative_retries: Option<u64>,
    local_read_count: Option<u64>,
    local_read_latency_ms: Option<f64>,
    local_write_count: Option<u64>,
    local_write_latency_ms: Option<f64>,
    local_read_write_ratio: Option<f64>,
    percent_repaired: Option<f64>,
    bytes_repaired: Option<u64>,
    bytes_unrepaired: Option<u64>,
    bytes_pending_repair: Option<u64>,
    read_latency_ms: Option<f64>,
    write_latency_ms: Option<f64>,
    space_used_live_bytes: Option<u64>,
    space_used_total_bytes: Option<u64>,
    space_used_snapshots_bytes: Option<u64>,
    off_heap_memory_used_bytes: Option<u64>,
    bloom_filter_false_positives: Option<u64>,
    bloom_filter_false_ratio: Option<f64>,
    bloom_filter_space_used: Option<u64>,
    compacted_partition_minimum_bytes: Option<u64>,
    compacted_partition_maximum_bytes: Option<u64>,
    compacted_partition_mean_bytes: Option<u64>,
    average_live_cells_per_slice_last_five_minutes: Option<f64>,
    maximum_live_cells_per_slice_last_five_minutes: Option<u64>,
    average_tombstones_per_slice_last_five_minutes: Option<f64>,
    maximum_tombstones_per_slice_last_five_minutes: Option<u64>,
    droppable_tombstone_ratio: Option<f64>,
}

#[derive(Debug, Clone, Default)]
struct KeyspaceStatsExtras {
    pending_flushes: u64,
    read_latency_ms: Option<f64>,
    write_latency_ms: Option<f64>,
    space_used_live: Option<u64>,
    space_used_total: Option<u64>,
}

fn render_table_stats_by_keyspace(
    holder: &TableStatsHolder,
    total_disk_space_used: u64,
    table_extras: &HashMap<(String, String), TableStatsExtras>,
    keyspace_extras: &HashMap<String, KeyspaceStatsExtras>,
) -> String {
    let mut lines = Vec::new();
    lines.push(format!("Total number of tables: {}", holder.tables.len()));
    lines.push("----------------".to_string());

    let mut by_keyspace: BTreeMap<String, Vec<&StatsTable>> = BTreeMap::new();
    for table in &holder.tables {
        by_keyspace
            .entry(table.keyspace.clone())
            .or_default()
            .push(table);
    }

    for (keyspace, tables) in by_keyspace {
        let keyspace_holder = TableStatsHolder::new(tables.iter().map(|t| (*t).clone()).collect());
        let summary = keyspace_holder.summary();
        let keyspace_extra = keyspace_extras.get(&keyspace).cloned().unwrap_or_default();

        lines.push(format!("Keyspace: {keyspace}"));
        lines.push(format!("\tRead Count: {}", summary.read_count));
        lines.push(format!(
            "\tRead Latency: {} ms",
            format_optional_ms(keyspace_extra.read_latency_ms)
        ));
        lines.push(format!("\tWrite Count: {}", summary.write_count));
        lines.push(format!(
            "\tWrite Latency: {} ms",
            format_optional_ms(keyspace_extra.write_latency_ms)
        ));
        lines.push(format!(
            "\tPending Flushes: {}",
            keyspace_extra.pending_flushes
        ));
        lines.push(format!(
            "\tSpace used (live): {}",
            keyspace_extra
                .space_used_live
                .unwrap_or(summary.disk_space_bytes)
        ));
        lines.push(format!(
            "\tSpace used (total): {}",
            keyspace_extra
                .space_used_total
                .unwrap_or(summary.disk_space_bytes)
        ));

        for table in tables {
            let extra = table_extras
                .get(&(table.keyspace.clone(), table.table.clone()))
                .cloned()
                .unwrap_or_default();
            lines.push(format!("\t\tTable: {}", table.table));
            lines.push(format!("\t\tSSTable count: {}", table.sstable_count));
            lines.push(format!(
                "\t\tOld SSTable count: {}",
                extra.old_sstable_count.unwrap_or(0)
            ));
            lines.push(format!(
                "\t\tMax SSTable size: {}",
                extra.max_sstable_size.unwrap_or(0)
            ));
            lines.push(format!(
                "\t\tSpace used (live): {}",
                extra
                    .space_used_live_bytes
                    .unwrap_or(table.disk_space_bytes)
            ));
            lines.push(format!(
                "\t\tSpace used (total): {}",
                extra
                    .space_used_total_bytes
                    .unwrap_or(table.disk_space_bytes)
            ));
            lines.push(format!(
                "\t\tSpace used by snapshots (total): {}",
                extra.space_used_snapshots_bytes.unwrap_or(0)
            ));
            lines.push(format!(
                "\t\tOff heap memory used (total): {}",
                extra.off_heap_memory_used_bytes.unwrap_or(0)
            ));
            lines.push(format!(
                "\t\tSSTable Compression Ratio: {}",
                format_ratio(extra.sstable_compression_ratio)
            ));
            lines.push(format!(
                "\t\tNumber of partitions (estimate): {}",
                extra.number_of_partitions_estimate.unwrap_or(0)
            ));
            lines.push(format!(
                "\t\tMemtable cell count: {}",
                extra.memtable_cell_count.unwrap_or(0)
            ));
            lines.push(format!(
                "\t\tMemtable data size: {}",
                extra.memtable_data_size.unwrap_or(0)
            ));
            lines.push(format!(
                "\t\tMemtable switch count: {}",
                extra.memtable_switch_count.unwrap_or(0)
            ));
            lines.push(format!(
                "\t\tSpeculative retries: {}",
                extra.speculative_retries.unwrap_or(0)
            ));
            lines.push(format!(
                "\t\tLocal read count: {}",
                extra.local_read_count.unwrap_or(table.read_count)
            ));
            lines.push(format!(
                "\t\tLocal read latency: {} ms",
                format_optional_ms(extra.local_read_latency_ms.or(extra.read_latency_ms))
            ));
            lines.push(format!(
                "\t\tLocal write count: {}",
                extra.local_write_count.unwrap_or(table.write_count)
            ));
            lines.push(format!(
                "\t\tLocal write latency: {} ms",
                format_optional_ms(extra.local_write_latency_ms.or(extra.write_latency_ms))
            ));
            lines.push(format!(
                "\t\tLocal read/write ratio: {}",
                format_ratio(extra.local_read_write_ratio)
            ));
            lines.push(format!("\t\tPending flushes: {}", extra.pending_flushes));
            lines.push(format!(
                "\t\tPercent repaired: {}",
                format_ratio(extra.percent_repaired)
            ));
            lines.push(format!(
                "\t\tBytes repaired: {}",
                extra.bytes_repaired.unwrap_or(0)
            ));
            lines.push(format!(
                "\t\tBytes unrepaired: {}",
                extra.bytes_unrepaired.unwrap_or(table.disk_space_bytes)
            ));
            lines.push(format!(
                "\t\tBytes pending repair: {}",
                extra.bytes_pending_repair.unwrap_or(0)
            ));
            lines.push(format!(
                "\t\tBloom filter false positives: {}",
                extra.bloom_filter_false_positives.unwrap_or(0)
            ));
            lines.push(format!(
                "\t\tBloom filter false ratio: {:.5}",
                extra.bloom_filter_false_ratio.unwrap_or(0.0)
            ));
            lines.push(format!(
                "\t\tBloom filter space used: {}",
                extra.bloom_filter_space_used.unwrap_or(0)
            ));
            lines.push(format!(
                "\t\tCompacted partition minimum bytes: {}",
                extra.compacted_partition_minimum_bytes.unwrap_or(0)
            ));
            lines.push(format!(
                "\t\tCompacted partition maximum bytes: {}",
                extra.compacted_partition_maximum_bytes.unwrap_or(0)
            ));
            lines.push(format!(
                "\t\tCompacted partition mean bytes: {}",
                extra.compacted_partition_mean_bytes.unwrap_or(0)
            ));
            lines.push(format!(
                "\t\tAverage live cells per slice (last five minutes): {}",
                format_ratio(extra.average_live_cells_per_slice_last_five_minutes)
            ));
            lines.push(format!(
                "\t\tMaximum live cells per slice (last five minutes): {}",
                extra
                    .maximum_live_cells_per_slice_last_five_minutes
                    .unwrap_or(0)
            ));
            lines.push(format!(
                "\t\tAverage tombstones per slice (last five minutes): {}",
                format_ratio(extra.average_tombstones_per_slice_last_five_minutes)
            ));
            lines.push(format!(
                "\t\tMaximum tombstones per slice (last five minutes): {}",
                extra
                    .maximum_tombstones_per_slice_last_five_minutes
                    .unwrap_or(0)
            ));
            lines.push(format!(
                "\t\tDroppable tombstone ratio: {}",
                format_ratio(extra.droppable_tombstone_ratio)
            ));
            lines.push(String::new());
        }
        lines.push("----------------".to_string());
    }

    lines.push(format!(
        "Total disk space used (bytes): {}",
        total_disk_space_used
    ));

    lines.join("\n")
}

fn parse_table_extras(value: &Value) -> HashMap<(String, String), TableStatsExtras> {
    let mut map = HashMap::new();
    let Some(tables) = value.get("tables").and_then(Value::as_array) else {
        return map;
    };
    for table in tables {
        let Some(keyspace) = table.get("keyspace").and_then(Value::as_str) else {
            continue;
        };
        let Some(name) = table.get("table").and_then(Value::as_str) else {
            continue;
        };
        let extras = TableStatsExtras {
            old_sstable_count: table.get("old_sstable_count").and_then(Value::as_u64),
            max_sstable_size: table.get("max_sstable_size").and_then(Value::as_u64),
            pending_flushes: table
                .get("pending_flushes")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            sstable_compression_ratio: table
                .get("sstable_compression_ratio")
                .and_then(Value::as_f64),
            number_of_partitions_estimate: table
                .get("number_of_partitions_estimate")
                .and_then(Value::as_u64),
            memtable_cell_count: table.get("memtable_cell_count").and_then(Value::as_u64),
            memtable_data_size: table.get("memtable_data_size").and_then(Value::as_u64),
            memtable_switch_count: table.get("memtable_switch_count").and_then(Value::as_u64),
            speculative_retries: table.get("speculative_retries").and_then(Value::as_u64),
            local_read_count: table.get("local_read_count").and_then(Value::as_u64),
            local_read_latency_ms: table.get("local_read_latency_ms").and_then(Value::as_f64),
            local_write_count: table.get("local_write_count").and_then(Value::as_u64),
            local_write_latency_ms: table.get("local_write_latency_ms").and_then(Value::as_f64),
            local_read_write_ratio: table.get("local_read_write_ratio").and_then(Value::as_f64),
            percent_repaired: table.get("percent_repaired").and_then(Value::as_f64),
            bytes_repaired: table.get("bytes_repaired").and_then(Value::as_u64),
            bytes_unrepaired: table.get("bytes_unrepaired").and_then(Value::as_u64),
            bytes_pending_repair: table.get("bytes_pending_repair").and_then(Value::as_u64),
            read_latency_ms: table.get("read_latency_ms").and_then(Value::as_f64),
            write_latency_ms: table.get("write_latency_ms").and_then(Value::as_f64),
            space_used_live_bytes: table.get("space_used_live_bytes").and_then(Value::as_u64),
            space_used_total_bytes: table.get("space_used_total_bytes").and_then(Value::as_u64),
            space_used_snapshots_bytes: table
                .get("space_used_snapshots_bytes")
                .and_then(Value::as_u64),
            off_heap_memory_used_bytes: table
                .get("off_heap_memory_used_bytes")
                .and_then(Value::as_u64),
            bloom_filter_false_positives: table
                .get("bloom_filter_false_positives")
                .and_then(Value::as_u64),
            bloom_filter_false_ratio: table
                .get("bloom_filter_false_ratio")
                .and_then(Value::as_f64),
            bloom_filter_space_used: table.get("bloom_filter_space_used").and_then(Value::as_u64),
            compacted_partition_minimum_bytes: table
                .get("compacted_partition_minimum_bytes")
                .and_then(Value::as_u64),
            compacted_partition_maximum_bytes: table
                .get("compacted_partition_maximum_bytes")
                .and_then(Value::as_u64),
            compacted_partition_mean_bytes: table
                .get("compacted_partition_mean_bytes")
                .and_then(Value::as_u64),
            average_live_cells_per_slice_last_five_minutes: table
                .get("average_live_cells_per_slice_last_five_minutes")
                .and_then(Value::as_f64),
            maximum_live_cells_per_slice_last_five_minutes: table
                .get("maximum_live_cells_per_slice_last_five_minutes")
                .and_then(Value::as_u64),
            average_tombstones_per_slice_last_five_minutes: table
                .get("average_tombstones_per_slice_last_five_minutes")
                .and_then(Value::as_f64),
            maximum_tombstones_per_slice_last_five_minutes: table
                .get("maximum_tombstones_per_slice_last_five_minutes")
                .and_then(Value::as_u64),
            droppable_tombstone_ratio: table
                .get("droppable_tombstone_ratio")
                .and_then(Value::as_f64),
        };
        map.insert((keyspace.to_string(), name.to_string()), extras);
    }
    map
}

fn parse_keyspace_extras(value: &Value) -> HashMap<String, KeyspaceStatsExtras> {
    let mut map = HashMap::new();
    let Some(keyspaces) = value.get("keyspaces").and_then(Value::as_array) else {
        return map;
    };
    for ks in keyspaces {
        let Some(name) = ks.get("keyspace").and_then(Value::as_str) else {
            continue;
        };
        let extras = KeyspaceStatsExtras {
            pending_flushes: ks
                .get("pending_flushes")
                .and_then(Value::as_u64)
                .unwrap_or(0),
            read_latency_ms: ks.get("read_latency_ms").and_then(Value::as_f64),
            write_latency_ms: ks.get("write_latency_ms").and_then(Value::as_f64),
            space_used_live: ks.get("space_used_live").and_then(Value::as_u64),
            space_used_total: ks.get("space_used_total").and_then(Value::as_u64),
        };
        map.insert(name.to_string(), extras);
    }
    map
}

fn format_optional_ms(value: Option<f64>) -> String {
    match value {
        Some(v) => format!("{v:.3}"),
        None => "NaN".to_string(),
    }
}

fn format_ratio(value: Option<f64>) -> String {
    match value {
        Some(v) => format!("{v:.5}"),
        None => "NaN".to_string(),
    }
}

fn format_histogram_number(value: Option<f64>) -> String {
    match value {
        Some(v) => format!("{v:.3}"),
        None => "NaN".to_string(),
    }
}

fn histogram_value(histogram: &Value, key: &str) -> Option<f64> {
    if key == "p98" {
        return histogram
            .get("p98")
            .and_then(Value::as_f64)
            .or_else(|| estimate_json_histogram_quantile(histogram, 0.98));
    }
    histogram.get(key).and_then(Value::as_f64)
}

fn estimate_json_histogram_quantile(histogram: &Value, quantile: f64) -> Option<f64> {
    let count = histogram.get("count").and_then(Value::as_u64)?;
    if count == 0 {
        return None;
    }
    let rank = (count as f64 * quantile).ceil() as u64;
    histogram
        .get("buckets")
        .and_then(Value::as_array)?
        .iter()
        .find_map(|bucket| {
            let cumulative = bucket.get("cumulative_count").and_then(Value::as_u64)?;
            if cumulative >= rank {
                bucket.get("le").and_then(Value::as_f64)
            } else {
                None
            }
        })
}

fn format_proxy_histograms_table(value: &Value) -> Option<String> {
    let mut table = TableFormatter::new(vec![
        TableColumn::left("Operation").min_width(11),
        TableColumn::right("p50").min_width(8),
        TableColumn::right("p75").min_width(8),
        TableColumn::right("p95").min_width(8),
        TableColumn::right("p99").min_width(8),
        TableColumn::right("Max").min_width(8),
        TableColumn::right("Count").min_width(8),
    ]);

    let mut rows = 0usize;
    for (label, key) in [
        ("Read", "read_latency"),
        ("Write", "write_latency"),
        ("Range", "range_latency"),
    ] {
        let Some(histogram) = value.get(key) else {
            continue;
        };
        rows += 1;
        table.add_row([
            label.to_string(),
            format_float(
                histogram
                    .get("p50")
                    .and_then(Value::as_f64)
                    .unwrap_or_default(),
            ),
            format_float(
                histogram
                    .get("p75")
                    .and_then(Value::as_f64)
                    .unwrap_or_default(),
            ),
            format_float(
                histogram
                    .get("p95")
                    .and_then(Value::as_f64)
                    .unwrap_or_default(),
            ),
            format_float(
                histogram
                    .get("p99")
                    .and_then(Value::as_f64)
                    .unwrap_or_default(),
            ),
            format_float(
                histogram
                    .get("max")
                    .and_then(Value::as_f64)
                    .unwrap_or_default(),
            ),
            format_uint(histogram.get("count")),
        ]);
    }

    if rows == 0 {
        None
    } else {
        Some(table.render())
    }
}

fn format_float(value: f64) -> String {
    format!("{value:.2}")
}

fn format_uint(value: Option<&Value>) -> String {
    value
        .and_then(|v| {
            v.as_u64()
                .or_else(|| v.as_i64().and_then(|n| u64::try_from(n).ok()))
        })
        .map(|n| n.to_string())
        .unwrap_or_else(|| "0".to_string())
}

fn field_as_text(value: &Value, keys: &[&str], default: &str) -> String {
    for key in keys {
        if let Some(field) = value.get(*key) {
            if let Some(s) = field.as_str() {
                return s.to_string();
            }
            if field.is_number() || field.is_boolean() {
                return field.to_string();
            }
        }
    }
    default.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    /// Verify that table_stats does not panic when the server is unreachable.
    #[test]
    fn test_table_stats_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        // Should print an error, not panic.
        table_stats(&client, None, None);
    }

    /// Verify that table_stats with a keyspace filter does not panic.
    #[test]
    fn test_table_stats_with_keyspace() {
        let client = AdminClient::new("127.0.0.1", 1);
        table_stats(&client, Some("system"), None);
    }

    /// Verify that table_stats with only a table filter does not panic.
    #[test]
    fn test_table_stats_with_table_only() {
        let client = AdminClient::new("127.0.0.1", 1);
        table_stats(&client, None, Some("peers"));
    }

    /// Verify that tp_stats does not panic when the server is unreachable.
    #[test]
    fn test_tp_stats_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        tp_stats(&client);
    }

    /// Verify that top_partitions builds the correct path with all parameters.
    #[test]
    fn test_top_partitions_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        top_partitions(&client, Some("ks"), Some("tbl"), 1000);
    }

    /// Verify that failure_detector_info handles errors gracefully.
    #[test]
    fn test_failure_detector_info_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        failure_detector_info(&client);
    }

    /// Verify that get_endpoints does not panic.
    #[test]
    fn test_get_endpoints_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        get_endpoints(&client, "ks", "tbl", "key1");
    }

    #[test]
    fn format_tpstats_supports_virtual_table_keys() {
        let payload = json!({
            "thread_pools": [
                {
                    "name": "ReadStage",
                    "active_tasks": "1",
                    "pending_tasks": "2",
                    "completed_tasks": "3",
                    "blocked_tasks": "4"
                }
            ]
        });

        let rendered = format_tpstats_table(&payload).expect("table");
        assert!(rendered.contains("Pool Name"));
        assert!(rendered.contains("ReadStage"));
        assert!(rendered.contains("1"));
        assert!(rendered.contains("2"));
        assert!(rendered.contains("3"));
        assert!(rendered.contains("4"));
    }

    #[test]
    fn format_proxy_histograms_renders_operation_rows() {
        let payload = json!({
            "read_latency": {"p50": 1.0, "p75": 2.0, "p95": 3.0, "p99": 4.0, "max": 5.0, "count": 11},
            "write_latency": {"p50": 6.0, "p75": 7.0, "p95": 8.0, "p99": 9.0, "max": 10.0, "count": 12},
            "range_latency": {"p50": 11.0, "p75": 12.0, "p95": 13.0, "p99": 14.0, "max": 15.0, "count": 13}
        });

        let rendered = format_proxy_histograms_table(&payload).expect("table");
        assert!(rendered.contains("Operation"));
        assert!(rendered.contains("Read"));
        assert!(rendered.contains("Write"));
        assert!(rendered.contains("Range"));
        assert!(rendered.contains("11"));
        assert!(rendered.contains("13"));
    }

    #[test]
    fn format_table_histograms_renders_percentiles() {
        let payload = json!({
            "latency_histograms": {
                "read": [{"count": 10, "p50": 1.0, "p75": 2.0, "p95": 3.0, "p99": 4.0, "max": 5.0}],
                "write": [{"count": 20, "p50": 6.0, "p75": 7.0, "p95": 8.0, "p99": 9.0, "max": 10.0}]
            },
            "sstables_per_read_histogram": {"count": 2, "min": 1.0, "p50": 1.5, "p75": 2.0, "p95": 3.0, "p98": 3.0, "p99": 3.0, "max": 3.0},
            "partition_size_histogram": {"count": 2, "min": 128.0, "p50": 256.0, "p75": 512.0, "p95": 1024.0, "p98": 1024.0, "p99": 1024.0, "max": 1024.0},
            "cell_count_histogram": {"count": 2, "min": 2.0, "p50": 4.0, "p75": 6.0, "p95": 8.0, "p98": 8.0, "p99": 8.0, "max": 8.0}
        });
        let rendered = format_table_histograms(&payload).expect("histogram table");
        assert!(rendered.contains("Percentile"));
        assert!(rendered.contains("SSTables"));
        assert!(rendered.contains("Partition Size"));
        assert!(rendered.contains("Cell Count"));
        assert!(rendered.contains("50%"));
        assert!(rendered.contains("98%"));
        assert!(rendered.contains("Min"));
        assert!(rendered.contains("Read count: 10"));
        assert!(rendered.contains("Write count: 20"));
        assert!(rendered.contains("SSTables per read count: 2"));
        assert!(rendered.contains("Partition size count: 2"));
        assert!(rendered.contains("Cell count: 2"));
    }

    #[test]
    fn format_client_stats_renders_clients_table() {
        let payload = json!({
            "connected_clients": 1,
            "clients": [
                {
                    "address": "127.0.0.1",
                    "port": "9042",
                    "username": "cassandra",
                    "connection_stage": "READY",
                    "protocol_version": "5",
                    "ssl": "false"
                }
            ]
        });
        let rendered = format_client_stats(&payload).expect("clients table");
        assert!(rendered.contains("Connected clients: 1"));
        assert!(rendered.contains("127.0.0.1"));
        assert!(rendered.contains("cassandra"));
        assert!(rendered.contains("READY"));
    }

    #[test]
    fn format_gc_stats_renders_status() {
        let payload = json!({
            "gc_enabled": false,
            "note": "Rust implementation - no garbage collector"
        });
        let rendered = format_gc_stats(&payload).expect("gc");
        assert!(rendered.contains("GC status: disabled"));
        assert!(rendered.contains("no garbage collector"));
    }

    #[test]
    fn format_top_partitions_renders_table() {
        let payload = json!({
            "top_partitions": [
                {"keyspace": "ks", "table": "tbl", "partition_key_hex": "616263", "rows": 2, "cells": 5}
            ]
        });
        let rendered = format_top_partitions(&payload).expect("top");
        assert!(rendered.contains("Keyspace"));
        assert!(rendered.contains("616263"));
        assert!(rendered.contains("tbl"));
    }

    #[test]
    fn format_failure_detector_info_renders_table() {
        let payload = json!({
            "failure_detector": [
                {"endpoint": "127.0.0.1", "phi": 0.0, "status": "UP"}
            ]
        });
        let rendered = format_failure_detector_info(&payload).expect("fd table");
        assert!(rendered.contains("Endpoint"));
        assert!(rendered.contains("127.0.0.1"));
        assert!(rendered.contains("UP"));
    }

    #[test]
    fn format_data_paths_renders_lines() {
        let payload = json!({"data_paths": ["data", "data2"]});
        let rendered = format_data_paths(&payload).expect("paths");
        assert!(rendered.contains("Data paths:"));
        assert!(rendered.contains("data2"));
    }

    #[test]
    fn format_refresh_size_estimates_renders_status() {
        let payload = json!({"status": "size estimates refreshed"});
        let rendered = format_refresh_size_estimates(&payload).expect("refresh");
        assert!(rendered.contains("size estimates refreshed"));
    }

    #[test]
    fn format_view_build_status_renders_table() {
        let payload = json!({
            "views": [
                {"keyspace": "ks", "view": "mv1", "status": "SUCCESS"}
            ]
        });
        let rendered = format_view_build_status(&payload).expect("views");
        assert!(rendered.contains("Keyspace"));
        assert!(rendered.contains("mv1"));
        assert!(rendered.contains("SUCCESS"));
    }

    #[test]
    fn format_endpoints_renders_list() {
        let payload = json!({
            "keyspace": "ks",
            "table": "tbl",
            "key": "k1",
            "endpoints": ["127.0.0.1", "127.0.0.2"]
        });
        let rendered = format_endpoints(&payload).expect("endpoints");
        assert!(rendered.contains("Endpoints for ks.tbl key=k1:"));
        assert!(rendered.contains("127.0.0.2"));
    }

    #[test]
    fn format_sstables_renders_empty_message() {
        let payload = json!({"sstables": []});
        let rendered = format_sstables(&payload).expect("sstables");
        assert_eq!(rendered, "No SSTables found for key.");
    }

    #[test]
    fn format_table_stats_renders_holder_table() {
        let payload = json!({
            "tables": [
                {
                    "keyspace": "ks",
                    "table": "tbl",
                    "sstable_count": 2,
                    "disk_space_bytes": 300,
                    "read_count": 4,
                    "write_count": 5,
                    "pending_flushes": 0,
                    "read_latency_ms": 0.123,
                    "write_latency_ms": 0.456,
                    "space_used_live_bytes": 200,
                    "space_used_total_bytes": 999,
                    "space_used_snapshots_bytes": 0,
                    "off_heap_memory_used_bytes": 0,
                    "bytes_unrepaired": 999,
                    "bloom_filter_false_positives": 0,
                    "bloom_filter_false_ratio": 0.0
                }
            ],
            "keyspaces": [
                {
                    "keyspace": "ks",
                    "table_count": 1,
                    "read_count": 4,
                    "write_count": 5,
                    "sstable_count": 2,
                    "disk_space_bytes": 300,
                    "pending_flushes": 0,
                    "read_latency_ms": 0.123,
                    "write_latency_ms": 0.456
                }
            ],
            "total_disk_space_used": 300
        });

        let rendered = format_table_stats(&payload, None, None).expect("table");
        assert!(rendered.contains("Total number of tables: 1"));
        assert!(rendered.contains("Keyspace: ks"));
        assert!(rendered.contains("Table: tbl"));
        assert!(rendered.contains("SSTable count: 2"));
        assert!(rendered.contains("Read Latency: 0.123 ms"));
        assert!(rendered.contains("Write Latency: 0.456 ms"));
        assert!(rendered.contains("Space used (live): 200"));
        assert!(rendered.contains("Space used (total): 999"));
        assert!(rendered.contains("Bytes unrepaired: 999"));
        assert!(rendered.contains("Local read count: 4"));
        assert!(rendered.contains("Compacted partition mean bytes: 0"));
        assert!(rendered.contains("Bloom filter false ratio: 0.00000"));
    }

    #[test]
    fn format_table_stats_honors_keyspace_filter() {
        let payload = json!({
            "tables": [
                {
                    "keyspace": "ks1",
                    "table": "a",
                    "sstable_count": 1,
                    "disk_space_bytes": 10,
                    "read_count": 0,
                    "write_count": 0
                },
                {
                    "keyspace": "ks2",
                    "table": "b",
                    "sstable_count": 1,
                    "disk_space_bytes": 10,
                    "read_count": 0,
                    "write_count": 0
                }
            ]
        });

        let rendered = format_table_stats(&payload, Some("ks1"), None).expect("table");
        assert!(rendered.contains("ks1"));
        assert!(!rendered.contains("ks2"));
    }

    #[test]
    fn format_table_stats_honors_table_filter() {
        let payload = json!({
            "tables": [
                {
                    "keyspace": "ks1",
                    "table": "a",
                    "sstable_count": 1,
                    "disk_space_bytes": 10,
                    "read_count": 0,
                    "write_count": 0
                },
                {
                    "keyspace": "ks1",
                    "table": "b",
                    "sstable_count": 1,
                    "disk_space_bytes": 10,
                    "read_count": 0,
                    "write_count": 0
                }
            ]
        });

        let rendered = format_table_stats(&payload, Some("ks1"), Some("b")).expect("table");
        assert!(rendered.contains("Table: b"));
        assert!(!rendered.contains("Table: a"));
    }
}
