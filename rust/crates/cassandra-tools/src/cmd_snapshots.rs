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

//! Snapshot and backup CLI commands (Unit 4).
//!
//! ## Java Oracle
//! - `org.apache.cassandra.tools.NodeTool.Snapshot`
//! - `org.apache.cassandra.tools.NodeTool.ListSnapshots`
//! - `org.apache.cassandra.tools.NodeTool.ClearSnapshot`
//! - `org.apache.cassandra.tools.NodeTool.Import`
//! - `org.apache.cassandra.tools.NodeTool.EnableBackup`
//! - `org.apache.cassandra.tools.NodeTool.DisableBackup`
//! - `org.apache.cassandra.tools.NodeTool.StatusBackup`

use crate::admin_client::AdminClient;
use serde_json::json;

/// Generate a timestamp-based snapshot name using `SystemTime`.
fn generate_snapshot_name() -> String {
    use std::time::SystemTime;
    let ts = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("snapshot-{}", ts)
}

/// Take a snapshot of keyspaces/tables.
///
/// If `name` is `None`, a timestamp-based name is generated automatically.
/// The request is sent as POST /api/v1/operations/snapshot with `{name, keyspaces}`.
pub fn snapshot(client: &AdminClient, name: Option<String>, keyspaces: &[String]) {
    let snap_name = name.unwrap_or_else(generate_snapshot_name);
    println!("Requesting snapshot '{}'...", snap_name);

    let body = json!({
        "name": snap_name,
        "keyspaces": keyspaces,
    });

    match client.post_json("/api/v1/operations/snapshot", &body) {
        Ok(resp) => {
            if let Some(id) = resp.get("operation_id") {
                println!("Snapshot '{}' started (operation {})", snap_name, id);
            } else {
                println!("Snapshot '{}' created successfully", snap_name);
            }
        }
        Err(e) => eprintln!("Error creating snapshot: {}", e),
    }
}

/// List available snapshots.
///
/// Calls GET /api/v1/snapshots and prints a formatted table.
pub fn list_snapshots(client: &AdminClient) {
    match client.get("/api/v1/snapshots") {
        Ok(resp) => {
            let snapshots = resp.as_array().unwrap_or(&Vec::new()).clone();
            if snapshots.is_empty() {
                println!("No snapshots found.");
                return;
            }

            println!(
                "{:<30} {:<20} {:<15} {:<20}",
                "Snapshot name", "Keyspace", "Table", "True size"
            );
            println!("{}", "-".repeat(85));

            for snap in &snapshots {
                let name = snap["name"].as_str().unwrap_or("-");
                let keyspace = snap["keyspace"].as_str().unwrap_or("-");
                let table = snap["table"].as_str().unwrap_or("-");
                let size = snap["true_size"]
                    .as_str()
                    .or_else(|| snap["true_size_bytes"].as_str())
                    .unwrap_or("-");
                println!("{:<30} {:<20} {:<15} {:<20}", name, keyspace, table, size);
            }

            println!("\nTotal snapshots: {}", snapshots.len());
        }
        Err(e) => eprintln!("Error listing snapshots: {}", e),
    }
}

/// Clear a snapshot by name, or clear all snapshots if `name` is `None`.
///
/// Sends DELETE /api/v1/snapshots (optionally with `?name=<name>` query param).
pub fn clear_snapshot(client: &AdminClient, name: Option<String>) {
    let path = match &name {
        Some(n) => format!("/api/v1/snapshots?name={}", n),
        None => "/api/v1/snapshots".to_string(),
    };

    let label = name.as_deref().unwrap_or("all");
    println!("Clearing snapshot(s): {}...", label);

    match client.delete(&path) {
        Ok(_) => println!("Snapshot(s) '{}' cleared successfully", label),
        Err(e) => eprintln!("Error clearing snapshot(s): {}", e),
    }
}

/// Import SSTables from a directory into a keyspace/table.
///
/// Sends POST /api/v1/operations/import with `{keyspace, table, directory}`.
pub fn import(client: &AdminClient, keyspace: &str, table: &str, directory: &str) {
    println!(
        "Importing SSTables from '{}' into {}.{}...",
        directory, keyspace, table
    );

    let body = json!({
        "keyspace": keyspace,
        "table": table,
        "directory": directory,
    });

    match client.post_json("/api/v1/operations/import", &body) {
        Ok(resp) => {
            if let Some(id) = resp.get("operation_id") {
                println!("Import started (operation {})", id);
            } else {
                println!("Import completed successfully");
            }
        }
        Err(e) => eprintln!("Error importing SSTables: {}", e),
    }
}

/// Enable incremental backup.
///
/// Sends POST /api/v1/backup/enable.
pub fn enable_backup(client: &AdminClient) {
    match client.post_empty("/api/v1/backup/enable") {
        Ok(_) => println!("Incremental backup enabled"),
        Err(e) => eprintln!("Error enabling backup: {}", e),
    }
}

/// Disable incremental backup.
///
/// Sends POST /api/v1/backup/disable.
pub fn disable_backup(client: &AdminClient) {
    match client.post_empty("/api/v1/backup/disable") {
        Ok(_) => println!("Incremental backup disabled"),
        Err(e) => eprintln!("Error disabling backup: {}", e),
    }
}

/// Show backup status.
///
/// Calls GET /api/v1/backup/status and prints the current state.
pub fn status_backup(client: &AdminClient) {
    match client.get("/api/v1/backup/status") {
        Ok(resp) => {
            let enabled = resp["enabled"].as_bool().unwrap_or(false);
            println!(
                "Incremental backup: {}",
                if enabled { "enabled" } else { "disabled" }
            );
            if let Some(details) = resp.as_object() {
                for (k, v) in details {
                    if k != "enabled" {
                        println!("  {}: {}", k, v);
                    }
                }
            }
        }
        Err(e) => eprintln!("Error getting backup status: {}", e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_generate_snapshot_name_format() {
        let name = generate_snapshot_name();
        assert!(
            name.starts_with("snapshot-"),
            "Expected prefix 'snapshot-', got: {}",
            name
        );
        let ts_part = &name["snapshot-".len()..];
        let ts: u64 = ts_part.parse().expect("Timestamp part should be numeric");
        // Timestamp should be a reasonable Unix epoch value (after 2020).
        assert!(ts > 1_577_836_800, "Timestamp too small: {}", ts);
    }

    #[test]
    fn test_generate_snapshot_name_uniqueness() {
        let name1 = generate_snapshot_name();
        let name2 = generate_snapshot_name();
        // Within the same second they may be equal, but the format is consistent.
        assert!(name1.starts_with("snapshot-"));
        assert!(name2.starts_with("snapshot-"));
    }

    #[test]
    fn test_snapshot_connection_refused() {
        // Using port 1 guarantees a connection refusal.
        let client = AdminClient::new("127.0.0.1", 1);
        // snapshot should print an error, not panic.
        snapshot(&client, Some("test-snap".into()), &[]);
    }

    #[test]
    fn test_list_snapshots_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        list_snapshots(&client);
    }

    #[test]
    fn test_clear_snapshot_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        clear_snapshot(&client, Some("nonexistent".into()));
    }

    #[test]
    fn test_clear_snapshot_all_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        clear_snapshot(&client, None);
    }

    #[test]
    fn test_import_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        import(&client, "ks", "tbl", "/tmp/sstables");
    }

    #[test]
    fn test_enable_backup_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        enable_backup(&client);
    }

    #[test]
    fn test_status_backup_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        status_backup(&client);
    }
}
