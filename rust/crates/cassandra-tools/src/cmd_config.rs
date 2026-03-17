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

//! Configuration commands — get/set config values, timeouts, throughput,
//! concurrent compactors, reload operations, and binary/gossip protocol control.

use crate::admin_client::AdminClient;
use serde_json::json;

/// Get one or all configuration values.
pub fn get_config(client: &AdminClient, key: Option<&str>) {
    let path = match key {
        Some(k) => format!("/api/v1/config?key={}", k),
        None => "/api/v1/config".to_string(),
    };
    match client.get(&path) {
        Ok(value) => println!("{}", serde_json::to_string_pretty(&value).unwrap()),
        Err(e) => eprintln!("Error fetching config: {}", e),
    }
}

/// Set a configuration value.
pub fn set_config(client: &AdminClient, key: &str, value: &str) {
    let body = json!({ "key": key, "value": value });
    match client.post_json("/api/v1/config", &body) {
        Ok(_) => println!("Configuration '{}' set to '{}'.", key, value),
        Err(e) => eprintln!("Error setting config '{}': {}", key, e),
    }
}

/// Get a request timeout by type (read, write, range, counter, truncate, misc).
pub fn get_timeout(client: &AdminClient, timeout_type: &str) {
    let key = format!("{}_request_timeout_in_ms", timeout_type);
    let path = format!("/api/v1/config?key={}", key);
    match client.get(&path) {
        Ok(value) => {
            if let Some(val) = value.get(&key) {
                println!("Current {} timeout: {} ms", timeout_type, val);
            } else if let Some(val) = value.get("value") {
                println!("Current {} timeout: {} ms", timeout_type, val);
            } else {
                println!("{}", serde_json::to_string_pretty(&value).unwrap());
            }
        }
        Err(e) => eprintln!("Error fetching {} timeout: {}", timeout_type, e),
    }
}

/// Set a request timeout by type.
pub fn set_timeout(client: &AdminClient, timeout_type: &str, timeout_ms: u64) {
    let key = format!("{}_request_timeout_in_ms", timeout_type);
    let body = json!({ "key": key, "value": timeout_ms.to_string() });
    match client.post_json("/api/v1/config", &body) {
        Ok(_) => println!(
            "Timeout '{}' set to {} ms.",
            timeout_type, timeout_ms
        ),
        Err(e) => eprintln!("Error setting {} timeout: {}", timeout_type, e),
    }
}

/// Get streaming throughput.
pub fn get_streaming_throughput(client: &AdminClient) {
    let path = "/api/v1/config?key=stream_throughput_outbound_megabits_per_sec";
    match client.get(path) {
        Ok(value) => {
            if let Some(val) = value.get("value") {
                println!("Current streaming throughput: {} Mb/s", val);
            } else {
                println!("{}", serde_json::to_string_pretty(&value).unwrap());
            }
        }
        Err(e) => eprintln!("Error fetching streaming throughput: {}", e),
    }
}

/// Set streaming throughput.
pub fn set_streaming_throughput(client: &AdminClient, throughput_mb: u32) {
    let body = json!({
        "key": "stream_throughput_outbound_megabits_per_sec",
        "value": throughput_mb.to_string(),
    });
    match client.post_json("/api/v1/config", &body) {
        Ok(_) => println!("Streaming throughput set to {} Mb/s.", throughput_mb),
        Err(e) => eprintln!("Error setting streaming throughput: {}", e),
    }
}

/// Get inter-datacenter streaming throughput.
pub fn get_interdc_stream_throughput(client: &AdminClient) {
    let path = "/api/v1/config?key=inter_dc_stream_throughput_outbound_megabits_per_sec";
    match client.get(path) {
        Ok(value) => {
            if let Some(val) = value.get("value") {
                println!("Current inter-DC streaming throughput: {} Mb/s", val);
            } else {
                println!("{}", serde_json::to_string_pretty(&value).unwrap());
            }
        }
        Err(e) => eprintln!("Error fetching inter-DC streaming throughput: {}", e),
    }
}

/// Set inter-datacenter streaming throughput.
pub fn set_interdc_stream_throughput(client: &AdminClient, throughput_mb: u32) {
    let body = json!({
        "key": "inter_dc_stream_throughput_outbound_megabits_per_sec",
        "value": throughput_mb.to_string(),
    });
    match client.post_json("/api/v1/config", &body) {
        Ok(_) => println!(
            "Inter-DC streaming throughput set to {} Mb/s.",
            throughput_mb
        ),
        Err(e) => eprintln!("Error setting inter-DC streaming throughput: {}", e),
    }
}

/// Get number of concurrent compactors.
pub fn get_concurrent_compactors(client: &AdminClient) {
    let path = "/api/v1/config?key=concurrent_compactors";
    match client.get(path) {
        Ok(value) => {
            if let Some(val) = value.get("value") {
                println!("Current concurrent compactors: {}", val);
            } else {
                println!("{}", serde_json::to_string_pretty(&value).unwrap());
            }
        }
        Err(e) => eprintln!("Error fetching concurrent compactors: {}", e),
    }
}

/// Set number of concurrent compactors.
pub fn set_concurrent_compactors(client: &AdminClient, value: u32) {
    let body = json!({
        "key": "concurrent_compactors",
        "value": value.to_string(),
    });
    match client.post_json("/api/v1/config", &body) {
        Ok(_) => println!("Concurrent compactors set to {}.", value),
        Err(e) => eprintln!("Error setting concurrent compactors: {}", e),
    }
}

/// Reload the local schema from disk.
pub fn reload_local_schema(client: &AdminClient) {
    match client.post_empty("/api/v1/config/reload/schema") {
        Ok(_) => println!("Local schema reloaded."),
        Err(e) => eprintln!("Error reloading local schema: {}", e),
    }
}

/// Reload trigger classes.
pub fn reload_triggers(client: &AdminClient) {
    match client.post_empty("/api/v1/config/reload/triggers") {
        Ok(_) => println!("Triggers reloaded."),
        Err(e) => eprintln!("Error reloading triggers: {}", e),
    }
}

/// Reload SSL certificates.
pub fn reload_ssl(client: &AdminClient) {
    match client.post_empty("/api/v1/config/reload/ssl") {
        Ok(_) => println!("SSL certificates reloaded."),
        Err(e) => eprintln!("Error reloading SSL certificates: {}", e),
    }
}

/// Enable the native transport (CQL binary protocol).
pub fn enable_binary(client: &AdminClient) {
    match client.post_empty("/api/v1/binary/enable") {
        Ok(_) => println!("Native transport enabled."),
        Err(e) => eprintln!("Error enabling native transport: {}", e),
    }
}

/// Disable the native transport (CQL binary protocol).
pub fn disable_binary(client: &AdminClient) {
    match client.post_empty("/api/v1/binary/disable") {
        Ok(_) => println!("Native transport disabled."),
        Err(e) => eprintln!("Error disabling native transport: {}", e),
    }
}

/// Show native transport status.
pub fn status_binary(client: &AdminClient) {
    match client.get("/api/v1/binary/status") {
        Ok(value) => {
            if let Some(running) = value.get("running") {
                if running.as_bool().unwrap_or(false) {
                    println!("Native transport is running.");
                } else {
                    println!("Native transport is not running.");
                }
            } else {
                println!("{}", serde_json::to_string_pretty(&value).unwrap());
            }
        }
        Err(e) => eprintln!("Error fetching native transport status: {}", e),
    }
}

/// Enable gossip protocol.
pub fn enable_gossip(client: &AdminClient) {
    match client.post_empty("/api/v1/gossip/enable") {
        Ok(_) => println!("Gossip enabled."),
        Err(e) => eprintln!("Error enabling gossip: {}", e),
    }
}

/// Disable gossip protocol.
pub fn disable_gossip(client: &AdminClient) {
    match client.post_empty("/api/v1/gossip/disable") {
        Ok(_) => println!("Gossip disabled."),
        Err(e) => eprintln!("Error disabling gossip: {}", e),
    }
}

/// Show gossip protocol status.
pub fn status_gossip(client: &AdminClient) {
    match client.get("/api/v1/gossip/status") {
        Ok(value) => {
            if let Some(running) = value.get("running") {
                if running.as_bool().unwrap_or(false) {
                    println!("Gossip is running.");
                } else {
                    println!("Gossip is not running.");
                }
            } else {
                println!("{}", serde_json::to_string_pretty(&value).unwrap());
            }
        }
        Err(e) => eprintln!("Error fetching gossip status: {}", e),
    }
}

/// Get seed node addresses from the configuration.
pub fn get_seeds(client: &AdminClient) {
    let path = "/api/v1/config?key=seed_provider";
    match client.get(path) {
        Ok(value) => println!("{}", serde_json::to_string_pretty(&value).unwrap()),
        Err(e) => eprintln!("Error fetching seeds: {}", e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that get_config does not panic when the server is unreachable.
    #[test]
    fn test_get_config_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        get_config(&client, None);
    }

    /// Verify that set_config does not panic when the server is unreachable.
    #[test]
    fn test_set_config_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        set_config(&client, "key", "value");
    }

    /// Verify that get_timeout does not panic when the server is unreachable.
    #[test]
    fn test_get_timeout_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        get_timeout(&client, "read");
    }

    /// Verify that enable_binary does not panic when the server is unreachable.
    #[test]
    fn test_enable_binary_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        enable_binary(&client);
    }

    /// Verify that status_gossip does not panic when the server is unreachable.
    #[test]
    fn test_status_gossip_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        status_gossip(&client);
    }

    /// Verify that get_seeds does not panic when the server is unreachable.
    #[test]
    fn test_get_seeds_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        get_seeds(&client);
    }
}
