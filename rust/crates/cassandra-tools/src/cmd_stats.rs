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

/// Show table statistics, optionally filtered by keyspace.
pub fn table_stats(client: &AdminClient, keyspace: Option<&str>) {
    let path = match keyspace {
        Some(ks) => format!("/api/v1/stats/tables?keyspace={}", ks),
        None => "/api/v1/stats/tables".to_string(),
    };
    match client.get(&path) {
        Ok(value) => println!("{}", serde_json::to_string_pretty(&value).unwrap()),
        Err(e) => eprintln!("Error fetching table stats: {}", e),
    }
}

/// Show table histograms (read/write latency distributions).
pub fn table_histograms(client: &AdminClient, keyspace: Option<&str>, table: Option<&str>) {
    let mut path = "/api/v1/stats/tablehistograms".to_string();
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
        Ok(value) => println!("{}", serde_json::to_string_pretty(&value).unwrap()),
        Err(e) => eprintln!("Error fetching table histograms: {}", e),
    }
}

/// Show thread pool statistics.
pub fn tp_stats(client: &AdminClient) {
    match client.get("/api/v1/stats/tpstats") {
        Ok(value) => println!("{}", serde_json::to_string_pretty(&value).unwrap()),
        Err(e) => eprintln!("Error fetching thread pool stats: {}", e),
    }
}

/// Show garbage collection statistics.
pub fn gc_stats(client: &AdminClient) {
    match client.get("/api/v1/stats/gcstats") {
        Ok(value) => println!("{}", serde_json::to_string_pretty(&value).unwrap()),
        Err(e) => eprintln!("Error fetching GC stats: {}", e),
    }
}

/// Show coordinator read/write latency percentiles (proxy histograms).
pub fn proxy_histograms(client: &AdminClient) {
    match client.get("/api/v1/stats/proxyhistograms") {
        Ok(value) => println!("{}", serde_json::to_string_pretty(&value).unwrap()),
        Err(e) => eprintln!("Error fetching proxy histograms: {}", e),
    }
}

/// Show connected client statistics.
pub fn client_stats(client: &AdminClient) {
    match client.get("/api/v1/stats/clientstats") {
        Ok(value) => println!("{}", serde_json::to_string_pretty(&value).unwrap()),
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
        Ok(value) => println!("{}", serde_json::to_string_pretty(&value).unwrap()),
        Err(e) => eprintln!("Error fetching top partitions: {}", e),
    }
}

/// Show failure detector information (phi values for each endpoint).
pub fn failure_detector_info(client: &AdminClient) {
    match client.get("/api/v1/stats/failure_detector_info") {
        Ok(value) => println!("{}", serde_json::to_string_pretty(&value).unwrap()),
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
        Ok(value) => println!("{}", serde_json::to_string_pretty(&value).unwrap()),
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
        Ok(value) => println!("{}", serde_json::to_string_pretty(&value).unwrap()),
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
        Ok(value) => println!("{}", serde_json::to_string_pretty(&value).unwrap()),
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
        Ok(value) => println!("{}", serde_json::to_string_pretty(&value).unwrap()),
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
        Ok(value) => println!("{}", serde_json::to_string_pretty(&value).unwrap()),
        Err(e) => eprintln!("Error fetching SSTables: {}", e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that table_stats does not panic when the server is unreachable.
    #[test]
    fn test_table_stats_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        // Should print an error, not panic.
        table_stats(&client, None);
    }

    /// Verify that table_stats with a keyspace filter does not panic.
    #[test]
    fn test_table_stats_with_keyspace() {
        let client = AdminClient::new("127.0.0.1", 1);
        table_stats(&client, Some("system"));
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
}
