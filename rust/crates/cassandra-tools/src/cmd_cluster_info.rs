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

//! Cluster information commands (nodetool status, info, ring, describecluster, gossipinfo).
//!
//! ## Java Oracle
//! - `org.apache.cassandra.tools.nodetool.Status`
//! - `org.apache.cassandra.tools.nodetool.Info`
//! - `org.apache.cassandra.tools.nodetool.Ring`
//! - `org.apache.cassandra.tools.nodetool.DescribeCluster`
//! - `org.apache.cassandra.tools.nodetool.GossipInfo`

use crate::admin_client::AdminClient;

/// Show cluster status (nodetool status equivalent).
///
/// GET /api/v1/cluster/status — returns datacenter info with node details.
pub fn status(client: &AdminClient) {
    match client.get("/api/v1/cluster/status") {
        Ok(resp) => {
            if print_status_response(&resp) {
                return;
            } else {
                println!("{}", resp);
            }
        }
        Err(e) => {
            eprintln!("Error fetching cluster status: {}", e);
            print_offline_status();
        }
    }
}

fn print_status_response(resp: &serde_json::Value) -> bool {
    let Some(datacenters) = resp.get("datacenters").and_then(|v| v.as_array()) else {
        return false;
    };

    for dc in datacenters {
        let dc_name = dc.get("name").and_then(|v| v.as_str()).unwrap_or("unknown");
        print_status_header(dc_name);

        if let Some(nodes) = dc.get("nodes").and_then(|v| v.as_array()) {
            for node in nodes {
                let status_str = format_node_status(node);
                let address = node.get("address").and_then(|v| v.as_str()).unwrap_or("?");
                let load = node.get("load").and_then(|v| v.as_str()).unwrap_or("?");
                let tokens = node
                    .get("tokens")
                    .and_then(|v| v.as_u64())
                    .map(|t| t.to_string())
                    .unwrap_or_else(|| "?".to_string());
                let owns = node.get("owns").and_then(|v| v.as_str()).unwrap_or("?");
                let host_id = node.get("host_id").and_then(|v| v.as_str()).unwrap_or("?");
                let rack = node.get("rack").and_then(|v| v.as_str()).unwrap_or("?");

                println!(
                    "{:<4} {:<16} {:<12} {:<8} {:<8} {:<38} {:<12}",
                    status_str, address, load, tokens, owns, host_id, rack
                );
            }
        }
        println!();
    }

    true
}

fn print_offline_status() {
    print_status_header("datacenter1");
    println!(
        "{:<4} {:<16} {:<12} {:<8} {:<8} {:<38} {:<12}",
        "DN", "127.0.0.1", "?", "?", "?", "?", "rack1"
    );
    println!();
}

fn print_status_header(dc_name: &str) {
    println!("Datacenter: {}", dc_name);
    println!("==========");
    println!(
        "{:<4} {:<16} {:<12} {:<8} {:<8} {:<38} {:<12}",
        "Status", "Address", "Load", "Tokens", "Owns", "Host ID", "Rack"
    );
}

/// Show local node information (nodetool info equivalent).
///
/// GET /api/v1/cluster/info — returns node-level metadata.
pub fn info(client: &AdminClient) {
    match client.get("/api/v1/cluster/info") {
        Ok(resp) => print_info_response(&resp),
        Err(e) => {
            eprintln!("Error fetching node info: {}", e);
            print_offline_info();
        }
    }
}

fn print_info_response(resp: &serde_json::Value) {
    print_field(resp, "ID", "id");
    print_field(resp, "Gossip active", "gossip_active");
    print_field(resp, "Native Transport active", "native_transport_active");
    print_field(resp, "Load", "load");
    print_field(resp, "Generation No", "generation");
    print_field(resp, "Uptime (seconds)", "uptime_seconds");
    print_field(resp, "Heap Memory (MB)", "heap_memory_mb");
    print_field(resp, "Off Heap Memory (MB)", "off_heap_memory_mb");
    print_field(resp, "Data Center", "data_center");
    print_field(resp, "Rack", "rack");
    print_field(resp, "Exceptions", "exceptions");
    print_field(resp, "Key Cache", "key_cache");
    print_field(resp, "Row Cache", "row_cache");
    print_field(resp, "Counter Cache", "counter_cache");
    print_field(resp, "Percent Repaired", "percent_repaired");

    if let Some(tokens) = resp.get("tokens").and_then(|v| v.as_array()) {
        println!("Token            : ({})", tokens.len());
    }
}

fn print_offline_info() {
    println!("ID                     : unavailable");
    println!("Gossip active          : false");
    println!("Native Transport active: false");
    println!("Load                   : ?");
    println!("Generation No          : ?");
    println!("Uptime (seconds)       : 0");
    println!("Heap Memory (MB)       : ?");
    println!("Off Heap Memory (MB)   : ?");
    println!("Data Center            : datacenter1");
    println!("Rack                   : rack1");
    println!("Exceptions             : 0");
    println!("Key Cache              : unavailable");
    println!("Row Cache              : unavailable");
    println!("Counter Cache          : unavailable");
    println!("Percent Repaired       : ?");
}

/// Show the token ring (nodetool ring equivalent).
///
/// GET /api/v1/cluster/ring — returns token-to-node mapping.
pub fn ring(client: &AdminClient) {
    match client.get("/api/v1/cluster/ring") {
        Ok(resp) => {
            println!(
                "{:<16} {:<12} {:<8} {:<8} {:<12} {:<8} {}",
                "Address", "Rack", "Status", "State", "Load", "Owns", "Token"
            );
            println!(
                "{:-<16} {:-<12} {:-<8} {:-<8} {:-<12} {:-<8} {:-<20}",
                "", "", "", "", "", "", ""
            );

            if let Some(nodes) = resp.get("nodes").and_then(|v| v.as_array()) {
                for node in nodes {
                    let address = node.get("address").and_then(|v| v.as_str()).unwrap_or("?");
                    let rack = node.get("rack").and_then(|v| v.as_str()).unwrap_or("?");
                    let status = node.get("status").and_then(|v| v.as_str()).unwrap_or("?");
                    let state = node.get("state").and_then(|v| v.as_str()).unwrap_or("?");
                    let load = node.get("load").and_then(|v| v.as_str()).unwrap_or("?");
                    let owns = node.get("owns").and_then(|v| v.as_str()).unwrap_or("?");
                    let token = node.get("token").and_then(|v| v.as_str()).unwrap_or("?");

                    println!(
                        "{:<16} {:<12} {:<8} {:<8} {:<12} {:<8} {}",
                        address, rack, status, state, load, owns, token
                    );
                }
            } else {
                println!("{}", resp);
            }
        }
        Err(e) => eprintln!("Error fetching ring: {}", e),
    }
}

/// Describe the cluster (nodetool describecluster equivalent).
///
/// GET /api/v1/cluster/describe — returns cluster name, snitch, partitioner, schema versions.
pub fn describe_cluster(client: &AdminClient) {
    match client.get("/api/v1/cluster/describe") {
        Ok(resp) => {
            if let Some(name) = resp.get("cluster_name").and_then(|v| v.as_str()) {
                println!("Cluster Name: {}", name);
            }
            if let Some(snitch) = resp.get("snitch").and_then(|v| v.as_str()) {
                println!("Snitch: {}", snitch);
            }
            if let Some(partitioner) = resp.get("partitioner").and_then(|v| v.as_str()) {
                println!("Partitioner: {}", partitioner);
            }

            if let Some(versions) = resp.get("schema_versions").and_then(|v| v.as_object()) {
                println!("Schema versions:");
                for (version, nodes) in versions {
                    let node_list = if let Some(arr) = nodes.as_array() {
                        arr.iter()
                            .filter_map(|v| v.as_str())
                            .collect::<Vec<_>>()
                            .join(", ")
                    } else {
                        nodes.to_string()
                    };
                    println!("  {} : [{}]", version, node_list);
                }
            }
        }
        Err(e) => eprintln!("Error describing cluster: {}", e),
    }
}

/// Show gossip information (nodetool gossipinfo equivalent).
///
/// GET /api/v1/cluster/gossip — returns gossip state for each endpoint.
pub fn gossip_info(client: &AdminClient) {
    match client.get("/api/v1/cluster/gossip") {
        Ok(resp) => {
            if let Some(endpoints) = resp.get("endpoints").and_then(|v| v.as_object()) {
                for (endpoint, state) in endpoints {
                    println!("/{}", endpoint);
                    if let Some(obj) = state.as_object() {
                        for (key, value) in obj {
                            let display = if let Some(s) = value.as_str() {
                                s.to_string()
                            } else {
                                value.to_string()
                            };
                            println!("  {}:{}", key, display);
                        }
                    }
                    println!();
                }
            } else if let Some(obj) = resp.as_object() {
                // Flat structure: keys are endpoints directly
                for (endpoint, state) in obj {
                    println!("/{}", endpoint);
                    if let Some(inner) = state.as_object() {
                        for (key, value) in inner {
                            let display = if let Some(s) = value.as_str() {
                                s.to_string()
                            } else {
                                value.to_string()
                            };
                            println!("  {}:{}", key, display);
                        }
                    }
                    println!();
                }
            } else {
                println!("{}", resp);
            }
        }
        Err(e) => eprintln!("Error fetching gossip info: {}", e),
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────

/// Format a node's status as a two-letter code (UN, DN, UL, etc.).
fn format_node_status(node: &serde_json::Value) -> String {
    let up = node
        .get("status")
        .and_then(|v| v.as_str())
        .map(|s| s.eq_ignore_ascii_case("up") || s == "U")
        .unwrap_or(false);
    let normal = node
        .get("state")
        .and_then(|v| v.as_str())
        .map(|s| s.eq_ignore_ascii_case("normal") || s == "N")
        .unwrap_or(false);

    let first = if up { 'U' } else { 'D' };
    let second = if normal { 'N' } else { 'L' };
    format!("{}{}", first, second)
}

/// Print a labeled field from a JSON value, if present.
fn print_field(value: &serde_json::Value, label: &str, key: &str) {
    if let Some(v) = value.get(key) {
        let display = if let Some(s) = v.as_str() {
            s.to_string()
        } else {
            v.to_string()
        };
        println!("{:<30}: {}", label, display);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_format_node_status_up_normal() {
        let node = json!({"status": "up", "state": "normal"});
        assert_eq!(format_node_status(&node), "UN");
    }

    #[test]
    fn test_format_node_status_down_leaving() {
        let node = json!({"status": "down", "state": "leaving"});
        assert_eq!(format_node_status(&node), "DL");
    }

    #[test]
    fn test_format_node_status_short_codes() {
        let node = json!({"status": "U", "state": "N"});
        assert_eq!(format_node_status(&node), "UN");
    }

    #[test]
    fn test_format_node_status_missing_fields() {
        let node = json!({});
        assert_eq!(format_node_status(&node), "DL");
    }

    #[test]
    fn test_print_field_string_value() {
        // Verify print_field does not panic and handles string values.
        let val = json!({"id": "abc-123", "load": 42});
        // We verify the function runs without panic; output goes to stdout.
        print_field(&val, "ID", "id");
        print_field(&val, "Load", "load");
        print_field(&val, "Missing", "missing_key");
    }

    #[test]
    fn test_status_function_signature() {
        // Verify the public API compiles and accepts AdminClient reference.
        fn _assert_fn(_f: fn(&AdminClient)) {}
        _assert_fn(status);
        _assert_fn(info);
        _assert_fn(ring);
        _assert_fn(describe_cluster);
        _assert_fn(gossip_info);
    }
}
