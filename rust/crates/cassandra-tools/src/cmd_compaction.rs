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

//! Compaction and maintenance operation commands.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.tools.nodetool.Compact`
//! - `org.apache.cassandra.tools.nodetool.Cleanup`
//! - `org.apache.cassandra.tools.nodetool.Flush`
//! - `org.apache.cassandra.tools.nodetool.Scrub`
//! - `org.apache.cassandra.tools.nodetool.CompactionStats`
//! - `org.apache.cassandra.tools.nodetool.CompactionHistory`
//! - `org.apache.cassandra.tools.nodetool.Repair`

use crate::admin_client::AdminClient;
use serde_json::json;

/// Force compaction on a keyspace/table.
///
/// POST /api/v1/operations/compact with keyspace and table in JSON body.
pub fn compact(client: &AdminClient, keyspace: Option<&str>, table: Option<&str>) {
    let body = build_ks_table_body("compact", keyspace, table);
    match client.post_json("/api/v1/operations/compact", &body) {
        Ok(resp) => print_operation_id(&resp, "Compact"),
        Err(e) => eprintln!("Error starting compaction: {}", e),
    }
}

/// Run cleanup to remove data not belonging to this node.
///
/// POST /api/v1/operations/cleanup with optional keyspace.
pub fn cleanup(client: &AdminClient, keyspace: Option<&str>) {
    let body = build_ks_table_body("cleanup", keyspace, None);
    match client.post_json("/api/v1/operations/cleanup", &body) {
        Ok(resp) => print_operation_id(&resp, "Cleanup"),
        Err(e) => eprintln!("Error starting cleanup: {}", e),
    }
}

/// Flush memtables to SSTables.
///
/// POST /api/v1/operations/flush with keyspace and table in JSON body.
pub fn flush(client: &AdminClient, keyspace: Option<&str>, table: Option<&str>) {
    let body = build_ks_table_body("flush", keyspace, table);
    match client.post_json("/api/v1/operations/flush", &body) {
        Ok(resp) => print_operation_id(&resp, "Flush"),
        Err(e) => eprintln!("Error starting flush: {}", e),
    }
}

/// Scrub a keyspace/table via the admin API.
///
/// POST /api/v1/operations/scrub with keyspace and table in JSON body.
pub fn scrub(client: &AdminClient, keyspace: Option<&str>, table: Option<&str>) {
    let body = build_ks_table_body("scrub", keyspace, table);
    match client.post_json("/api/v1/operations/scrub", &body) {
        Ok(resp) => print_operation_id(&resp, "Scrub"),
        Err(e) => eprintln!("Error starting scrub: {}", e),
    }
}

/// Show compaction statistics.
///
/// GET /api/v1/compaction/stats — returns pending tasks and active compactions.
pub fn compaction_stats(client: &AdminClient) {
    match client.get("/api/v1/compaction/stats") {
        Ok(resp) => {
            if let Some(pending) = resp.get("pending_tasks").and_then(|v| v.as_u64()) {
                println!("pending tasks: {}", pending);
            }

            if let Some(compactions) = resp.get("compactions").and_then(|v| v.as_array()) {
                if compactions.is_empty() {
                    println!("Active compaction remaining time :        n/a");
                } else {
                    println!(
                        "{:<12} {:<20} {:<20} {:<12} {:<12} unit",
                        "compaction type", "keyspace", "table", "completed", "total"
                    );
                    for c in compactions {
                        let ctype = c.get("task_type").and_then(|v| v.as_str()).unwrap_or("?");
                        let ks = c.get("keyspace").and_then(|v| v.as_str()).unwrap_or("?");
                        let tbl = c.get("table").and_then(|v| v.as_str()).unwrap_or("?");
                        let completed = c
                            .get("completed")
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "?".to_string());
                        let total = c
                            .get("total")
                            .map(|v| v.to_string())
                            .unwrap_or_else(|| "?".to_string());
                        let unit = c.get("unit").and_then(|v| v.as_str()).unwrap_or("bytes");
                        println!(
                            "{:<12} {:<20} {:<20} {:<12} {:<12} {}",
                            ctype, ks, tbl, completed, total, unit
                        );
                    }
                }
            }
        }
        Err(e) => eprintln!("Error fetching compaction stats: {}", e),
    }
}

/// Show compaction history.
///
/// GET /api/v1/compaction/history — returns a list of past compactions.
pub fn compaction_history(client: &AdminClient) {
    match client.get("/api/v1/compaction/history") {
        Ok(resp) => {
            if let Some(history) = resp.get("history").and_then(|v| v.as_array()) {
                if history.is_empty() {
                    println!("No compaction history available.");
                    return;
                }
                println!(
                    "{:<38} {:<20} {:<20} {:<12} {:<12} {:<12} rows_merged",
                    "id", "keyspace", "table", "compacted_at", "bytes_in", "bytes_out"
                );
                for entry in history {
                    let id = entry.get("id").and_then(|v| v.as_str()).unwrap_or("?");
                    let ks = entry
                        .get("keyspace")
                        .and_then(|v| v.as_str())
                        .unwrap_or("?");
                    let tbl = entry.get("table").and_then(|v| v.as_str()).unwrap_or("?");
                    let at = entry
                        .get("compacted_at")
                        .and_then(|v| v.as_str())
                        .unwrap_or("?");
                    let bytes_in = entry
                        .get("bytes_in")
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "?".to_string());
                    let bytes_out = entry
                        .get("bytes_out")
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "?".to_string());
                    let rows = entry
                        .get("rows_merged")
                        .map(|v| v.to_string())
                        .unwrap_or_else(|| "?".to_string());
                    println!(
                        "{:<38} {:<20} {:<20} {:<12} {:<12} {:<12} {}",
                        id, ks, tbl, at, bytes_in, bytes_out, rows
                    );
                }
            } else {
                println!("{}", resp);
            }
        }
        Err(e) => eprintln!("Error fetching compaction history: {}", e),
    }
}

/// Enable auto-compaction.
///
/// POST /api/v1/compaction/autocompaction/enable
pub fn enable_autocompaction(client: &AdminClient) {
    match client.post_empty("/api/v1/compaction/autocompaction/enable") {
        Ok(_) => println!("Auto-compaction enabled."),
        Err(e) => eprintln!("Error enabling auto-compaction: {}", e),
    }
}

/// Disable auto-compaction.
///
/// POST /api/v1/compaction/autocompaction/disable
pub fn disable_autocompaction(client: &AdminClient) {
    match client.post_empty("/api/v1/compaction/autocompaction/disable") {
        Ok(_) => println!("Auto-compaction disabled."),
        Err(e) => eprintln!("Error disabling auto-compaction: {}", e),
    }
}

/// Show auto-compaction status.
///
/// GET /api/v1/compaction/autocompaction/status
pub fn status_autocompaction(client: &AdminClient) {
    match client.get("/api/v1/compaction/autocompaction/status") {
        Ok(resp) => {
            if let Some(enabled) = resp.get("enabled").and_then(|v| v.as_bool()) {
                println!(
                    "Auto-compaction is {}.",
                    if enabled { "enabled" } else { "disabled" }
                );
            } else {
                println!("{}", resp);
            }
        }
        Err(e) => eprintln!("Error fetching auto-compaction status: {}", e),
    }
}

/// Get current compaction throughput.
///
/// GET /api/v1/compaction/throughput
pub fn get_compaction_throughput(client: &AdminClient) {
    match client.get("/api/v1/compaction/throughput") {
        Ok(resp) => {
            if let Some(mb) = resp.get("throughput_mb").and_then(|v| v.as_u64()) {
                println!("Current compaction throughput: {} MB/s", mb);
            } else {
                println!("{}", resp);
            }
        }
        Err(e) => eprintln!("Error fetching compaction throughput: {}", e),
    }
}

/// Set compaction throughput.
///
/// POST /api/v1/compaction/throughput with throughput value.
pub fn set_compaction_throughput(client: &AdminClient, throughput_mb: u32) {
    let body = json!({"throughput_mb": throughput_mb});
    match client.post_json("/api/v1/compaction/throughput", &body) {
        Ok(_) => println!("Compaction throughput set to {} MB/s.", throughput_mb),
        Err(e) => eprintln!("Error setting compaction throughput: {}", e),
    }
}

/// Force compaction for a specific keyspace and table.
///
/// POST /api/v1/operations/compact with keyspace and table.
pub fn force_compact(client: &AdminClient, keyspace: &str, table: &str) {
    let body = json!({
        "operation": "compact",
        "keyspace": keyspace,
        "table": table,
        "force": true,
    });
    match client.post_json("/api/v1/operations/compact", &body) {
        Ok(resp) => print_operation_id(&resp, "Force compact"),
        Err(e) => eprintln!("Error starting force compact: {}", e),
    }
}

/// Garbage collect tombstones.
///
/// POST /api/v1/operations/garbagecollect with optional keyspace.
pub fn garbage_collect(client: &AdminClient, keyspace: Option<&str>) {
    let body = if let Some(ks) = keyspace {
        json!({"operation": "garbagecollect", "keyspace": ks})
    } else {
        json!({"operation": "garbagecollect"})
    };
    match client.post_json("/api/v1/operations/garbagecollect", &body) {
        Ok(resp) => print_operation_id(&resp, "Garbage collect"),
        Err(e) => eprintln!("Error starting garbage collection: {}", e),
    }
}

/// Run repair on a keyspace.
///
/// POST /api/v1/operations/repair with keyspace, tables, and repair options.
pub fn repair(
    client: &AdminClient,
    keyspace: Option<&str>,
    tables: &[String],
    full: bool,
    incremental: bool,
    preview: bool,
) {
    let mut body = json!({
        "operation": "repair",
        "full": full,
        "incremental": incremental,
        "preview": preview,
    });

    if let Some(ks) = keyspace {
        body["keyspace"] = json!(ks);
    }
    if !tables.is_empty() {
        body["tables"] = json!(tables);
    }

    match client.post_json("/api/v1/operations/repair", &body) {
        Ok(resp) => print_operation_id(&resp, "Repair"),
        Err(e) => eprintln!("Error starting repair: {}", e),
    }
}

/// Rebuild one or more native secondary indexes.
///
/// POST /api/v1/operations/rebuild_index for each comma-separated index name.
pub fn rebuild_index(client: &AdminClient, keyspace: &str, table: &str, index_names: &str) {
    for name in index_names.split(',') {
        let name = name.trim();
        if name.is_empty() {
            continue;
        }
        let body = json!({
            "operation": "rebuild_index",
            "keyspace": keyspace,
            "table": table,
            "index_name": name,
        });
        match client.post_json("/api/v1/operations/rebuild_index", &body) {
            Ok(resp) => print_operation_id(&resp, &format!("Rebuild index '{}'", name)),
            Err(e) => eprintln!("Error rebuilding index '{}': {}", name, e),
        }
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────

/// Build a JSON body with operation name, optional keyspace, and optional table.
fn build_ks_table_body(
    operation: &str,
    keyspace: Option<&str>,
    table: Option<&str>,
) -> serde_json::Value {
    let mut body = json!({"operation": operation});
    if let Some(ks) = keyspace {
        body["keyspace"] = json!(ks);
    }
    if let Some(tbl) = table {
        body["table"] = json!(tbl);
    }
    body
}

/// Print the operation_id from a response, or the full response if not present.
fn print_operation_id(resp: &serde_json::Value, label: &str) {
    if let Some(id) = resp.get("operation_id").and_then(|v| v.as_str()) {
        println!("{} started. Operation ID: {}", label, id);
    } else {
        println!("{} submitted: {}", label, resp);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_build_ks_table_body_all_fields() {
        let body = build_ks_table_body("compact", Some("ks1"), Some("tbl1"));
        assert_eq!(body["operation"], "compact");
        assert_eq!(body["keyspace"], "ks1");
        assert_eq!(body["table"], "tbl1");
    }

    #[test]
    fn test_build_ks_table_body_no_optional() {
        let body = build_ks_table_body("cleanup", None, None);
        assert_eq!(body["operation"], "cleanup");
        assert!(body.get("keyspace").is_none());
        assert!(body.get("table").is_none());
    }

    #[test]
    fn test_build_ks_table_body_only_keyspace() {
        let body = build_ks_table_body("flush", Some("system"), None);
        assert_eq!(body["operation"], "flush");
        assert_eq!(body["keyspace"], "system");
        assert!(body.get("table").is_none());
    }

    #[test]
    fn test_print_operation_id_with_id() {
        let resp = json!({"operation_id": "abc-123"});
        // Verify it runs without panic (output goes to stdout).
        print_operation_id(&resp, "Compact");
    }

    #[test]
    fn test_print_operation_id_without_id() {
        let resp = json!({"status": "ok"});
        print_operation_id(&resp, "Flush");
    }

    #[test]
    fn test_function_signatures() {
        // Verify all public functions have the expected signatures.
        fn _assert_ks_tbl(_f: fn(&AdminClient, Option<&str>, Option<&str>)) {}
        fn _assert_ks(_f: fn(&AdminClient, Option<&str>)) {}
        fn _assert_simple(_f: fn(&AdminClient)) {}

        _assert_ks_tbl(compact);
        _assert_ks_tbl(flush);
        _assert_ks_tbl(scrub);
        _assert_ks(cleanup);
        _assert_ks(garbage_collect);
        _assert_simple(compaction_stats);
        _assert_simple(compaction_history);
        _assert_simple(enable_autocompaction);
        _assert_simple(disable_autocompaction);
        _assert_simple(status_autocompaction);
        _assert_simple(get_compaction_throughput);
    }
}
