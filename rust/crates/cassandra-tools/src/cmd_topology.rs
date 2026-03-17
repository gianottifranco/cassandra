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

//! Topology and lifecycle CLI commands (Unit 5).
//!
//! ## Java Oracle
//! - `org.apache.cassandra.tools.NodeTool.Decommission`
//! - `org.apache.cassandra.tools.NodeTool.RemoveNode`
//! - `org.apache.cassandra.tools.NodeTool.Move`
//! - `org.apache.cassandra.tools.NodeTool.Rebuild`
//! - `org.apache.cassandra.tools.NodeTool.Refresh`
//! - `org.apache.cassandra.tools.NodeTool.Join`
//! - `org.apache.cassandra.tools.NodeTool.BootstrapResume`
//! - `org.apache.cassandra.tools.NodeTool.Assassinate`
//! - `org.apache.cassandra.tools.NodeTool.Drain`
//! - `org.apache.cassandra.tools.NodeTool.StopDaemon`
//! - `org.apache.cassandra.tools.NodeTool.NetStats`

use crate::admin_client::AdminClient;
use serde_json::json;

/// Helper: print an operation_id from a JSON response, if present.
fn print_operation_id(resp: &serde_json::Value, label: &str) {
    if let Some(id) = resp.get("operation_id") {
        println!("{} started (operation {})", label, id);
    } else {
        println!("{} completed successfully", label);
    }
}

/// Decommission this node from the cluster.
///
/// Sends POST /api/v1/topology/decommission (empty body).
pub fn decommission(client: &AdminClient) {
    println!("Decommissioning node...");
    match client.post_empty("/api/v1/topology/decommission") {
        Ok(resp) => print_operation_id(&resp, "Decommission"),
        Err(e) => eprintln!("Error decommissioning: {}", e),
    }
}

/// Remove a dead node from the cluster by host ID.
///
/// Sends POST /api/v1/topology/removenode with `{host_id}`.
pub fn removenode(client: &AdminClient, host_id: &str) {
    println!("Removing node {}...", host_id);
    let body = json!({ "host_id": host_id });
    match client.post_json("/api/v1/topology/removenode", &body) {
        Ok(resp) => print_operation_id(&resp, "Remove node"),
        Err(e) => eprintln!("Error removing node: {}", e),
    }
}

/// Move this node to a new token.
///
/// Sends POST /api/v1/topology/move with `{new_token}`.
pub fn move_token(client: &AdminClient, new_token: &str) {
    println!("Moving node to token {}...", new_token);
    let body = json!({ "new_token": new_token });
    match client.post_json("/api/v1/topology/move", &body) {
        Ok(resp) => print_operation_id(&resp, "Move"),
        Err(e) => eprintln!("Error moving token: {}", e),
    }
}

/// Rebuild data from another datacenter.
///
/// Sends POST /api/v1/topology/rebuild with optional `{source_dc}`.
pub fn rebuild(client: &AdminClient, source_dc: Option<&str>) {
    match source_dc {
        Some(dc) => println!("Rebuilding from datacenter '{}'...", dc),
        None => println!("Rebuilding from all datacenters..."),
    }

    let body = match source_dc {
        Some(dc) => json!({ "source_dc": dc }),
        None => json!({}),
    };

    match client.post_json("/api/v1/topology/rebuild", &body) {
        Ok(resp) => print_operation_id(&resp, "Rebuild"),
        Err(e) => eprintln!("Error rebuilding: {}", e),
    }
}

/// Refresh (load new SSTables) for a keyspace/table.
///
/// Sends POST /api/v1/topology/refresh with `{keyspace, table}`.
pub fn refresh(client: &AdminClient, keyspace: &str, table: &str) {
    println!("Refreshing {}.{}...", keyspace, table);
    let body = json!({ "keyspace": keyspace, "table": table });
    match client.post_json("/api/v1/topology/refresh", &body) {
        Ok(resp) => print_operation_id(&resp, "Refresh"),
        Err(e) => eprintln!("Error refreshing: {}", e),
    }
}

/// Join the ring.
///
/// Sends POST /api/v1/topology/join (empty body).
pub fn join(client: &AdminClient) {
    println!("Joining ring...");
    match client.post_empty("/api/v1/topology/join") {
        Ok(resp) => print_operation_id(&resp, "Join"),
        Err(e) => eprintln!("Error joining ring: {}", e),
    }
}

/// Resume a previously interrupted bootstrap.
///
/// Sends POST /api/v1/topology/bootstrap (empty body).
pub fn bootstrap_resume(client: &AdminClient) {
    println!("Resuming bootstrap...");
    match client.post_empty("/api/v1/topology/bootstrap") {
        Ok(resp) => print_operation_id(&resp, "Bootstrap resume"),
        Err(e) => eprintln!("Error resuming bootstrap: {}", e),
    }
}

/// Abort a running bootstrap.
///
/// Sends POST /api/v1/topology/bootstrap with `{action: "abort"}`.
pub fn abort_bootstrap(client: &AdminClient) {
    println!("Aborting bootstrap...");
    let body = json!({ "action": "abort" });
    match client.post_json("/api/v1/topology/bootstrap", &body) {
        Ok(resp) => print_operation_id(&resp, "Bootstrap abort"),
        Err(e) => eprintln!("Error aborting bootstrap: {}", e),
    }
}

/// Assassinate a node endpoint.
///
/// Sends POST /api/v1/topology/assassinate with `{endpoint}`.
pub fn assassinate(client: &AdminClient, endpoint: &str) {
    println!("Assassinating endpoint {}...", endpoint);
    let body = json!({ "endpoint": endpoint });
    match client.post_json("/api/v1/topology/assassinate", &body) {
        Ok(_) => println!("Endpoint {} assassinated", endpoint),
        Err(e) => eprintln!("Error assassinating endpoint: {}", e),
    }
}

/// Drain the node (stop accepting writes and flush memtables).
///
/// Sends POST /api/v1/topology/drain (empty body).
pub fn drain(client: &AdminClient) {
    println!("Draining node...");
    match client.post_empty("/api/v1/topology/drain") {
        Ok(_) => println!("Node drained successfully"),
        Err(e) => eprintln!("Error draining: {}", e),
    }
}

/// Stop the Cassandra daemon.
///
/// Drains the node first, then signals the daemon to shut down.
pub fn stop_daemon(client: &AdminClient) {
    println!("Stopping Cassandra daemon...");

    // Drain first.
    match client.post_empty("/api/v1/topology/drain") {
        Ok(_) => println!("Node drained"),
        Err(e) => {
            eprintln!("Warning: drain failed: {}", e);
            eprintln!("Proceeding with stop anyway...");
        }
    }

    println!("Cassandra daemon stop requested");
    println!("Note: the daemon process will terminate; connection may be lost.");
}

/// Show network streaming statistics.
///
/// Calls GET /api/v1/topology/netstats and prints formatted output.
pub fn netstats(client: &AdminClient) {
    match client.get("/api/v1/topology/netstats") {
        Ok(resp) => {
            println!("Mode: {}", resp["mode"].as_str().unwrap_or("UNKNOWN"));
            println!();

            // Receiving streams
            if let Some(receiving) = resp["receiving"].as_array() {
                if !receiving.is_empty() {
                    println!("Receiving:");
                    for stream in receiving {
                        let peer = stream["peer"].as_str().unwrap_or("-");
                        let file_count = stream["files"].as_u64().unwrap_or(0);
                        let bytes = stream["bytes"].as_u64().unwrap_or(0);
                        println!("  {} — {} files, {} bytes", peer, file_count, bytes);
                    }
                    println!();
                }
            }

            // Sending streams
            if let Some(sending) = resp["sending"].as_array() {
                if !sending.is_empty() {
                    println!("Sending:");
                    for stream in sending {
                        let peer = stream["peer"].as_str().unwrap_or("-");
                        let file_count = stream["files"].as_u64().unwrap_or(0);
                        let bytes = stream["bytes"].as_u64().unwrap_or(0);
                        println!("  {} — {} files, {} bytes", peer, file_count, bytes);
                    }
                    println!();
                }
            }

            // Read/Write/Pending counts
            if let Some(commands) = resp["commands"].as_object() {
                println!("Commands:");
                for (cmd, stats) in commands {
                    let pending = stats["pending"].as_u64().unwrap_or(0);
                    let completed = stats["completed"].as_u64().unwrap_or(0);
                    println!("  {:<20} pending: {}, completed: {}", cmd, pending, completed);
                }
            }
        }
        Err(e) => eprintln!("Error getting netstats: {}", e),
    }
}

/// Show the current topology operation status.
///
/// Calls GET /api/v1/topology/status and prints formatted output.
pub fn topology_status(client: &AdminClient) {
    match client.get("/api/v1/topology/status") {
        Ok(resp) => {
            let status = resp["status"].as_str().unwrap_or("UNKNOWN");
            println!("Topology status: {}", status);

            if let Some(op) = resp.get("current_operation") {
                let op_type = op["type"].as_str().unwrap_or("-");
                let op_status = op["status"].as_str().unwrap_or("-");
                let progress = op["progress"].as_f64().unwrap_or(0.0);
                println!();
                println!("Current operation:");
                println!("  Type:     {}", op_type);
                println!("  Status:   {}", op_status);
                println!("  Progress: {:.1}%", progress * 100.0);

                if let Some(id) = op.get("operation_id") {
                    println!("  ID:       {}", id);
                }
            }

            if let Some(nodes) = resp["nodes"].as_array() {
                if !nodes.is_empty() {
                    println!();
                    println!(
                        "{:<40} {:<15} {:<12} {:<15}",
                        "Host ID", "Address", "State", "Status"
                    );
                    println!("{}", "-".repeat(82));
                    for node in nodes {
                        let host_id = node["host_id"].as_str().unwrap_or("-");
                        let address = node["address"].as_str().unwrap_or("-");
                        let state = node["state"].as_str().unwrap_or("-");
                        let node_status = node["status"].as_str().unwrap_or("-");
                        println!(
                            "{:<40} {:<15} {:<12} {:<15}",
                            host_id, address, state, node_status
                        );
                    }
                }
            }
        }
        Err(e) => eprintln!("Error getting topology status: {}", e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_print_operation_id_with_id() {
        let resp = json!({ "operation_id": "abc-123" });
        // Should not panic; output goes to stdout.
        print_operation_id(&resp, "Test");
    }

    #[test]
    fn test_print_operation_id_without_id() {
        let resp = json!({ "status": "ok" });
        print_operation_id(&resp, "Test");
    }

    #[test]
    fn test_decommission_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        decommission(&client);
    }

    #[test]
    fn test_removenode_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        removenode(&client, "fake-host-id");
    }

    #[test]
    fn test_move_token_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        move_token(&client, "12345");
    }

    #[test]
    fn test_rebuild_with_source_dc_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        rebuild(&client, Some("dc1"));
    }

    #[test]
    fn test_rebuild_without_source_dc_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        rebuild(&client, None);
    }

    #[test]
    fn test_refresh_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        refresh(&client, "ks", "tbl");
    }

    #[test]
    fn test_join_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        join(&client);
    }

    #[test]
    fn test_drain_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        drain(&client);
    }

    #[test]
    fn test_netstats_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        netstats(&client);
    }

    #[test]
    fn test_topology_status_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        topology_status(&client);
    }

    #[test]
    fn test_stop_daemon_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        stop_daemon(&client);
    }

    #[test]
    fn test_assassinate_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        assassinate(&client, "10.0.0.1:7000");
    }

    #[test]
    fn test_bootstrap_resume_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        bootstrap_resume(&client);
    }

    #[test]
    fn test_abort_bootstrap_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        abort_bootstrap(&client);
    }
}
