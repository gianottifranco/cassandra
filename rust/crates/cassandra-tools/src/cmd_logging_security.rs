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

//! Logging and security commands — logging levels, audit logging,
//! full query logging (FQL), and trace probability.

use crate::admin_client::AdminClient;
use serde_json::json;

/// Show current logging levels for all configured loggers.
pub fn get_logging_levels(client: &AdminClient) {
    match client.get("/api/v1/logging/levels") {
        Ok(value) => println!("{}", serde_json::to_string_pretty(&value).unwrap()),
        Err(e) => eprintln!("Error fetching logging levels: {}", e),
    }
}

/// Set the logging level for a specific logger (class or package).
pub fn set_logging_level(client: &AdminClient, logger: &str, level: &str) {
    let body = json!({ "logger": logger, "level": level });
    match client.post_json("/api/v1/logging/level", &body) {
        Ok(_) => println!("Logging level for '{}' set to '{}'.", logger, level),
        Err(e) => eprintln!("Error setting logging level for '{}': {}", logger, e),
    }
}

/// Enable audit logging.
pub fn enable_audit_log(client: &AdminClient) {
    match client.post_empty("/api/v1/operations/enableauditlog") {
        Ok(_) => println!("Audit logging enabled."),
        Err(e) => eprintln!("Error enabling audit logging: {}", e),
    }
}

/// Disable audit logging.
pub fn disable_audit_log(client: &AdminClient) {
    match client.post_empty("/api/v1/operations/disableauditlog") {
        Ok(_) => println!("Audit logging disabled."),
        Err(e) => eprintln!("Error disabling audit logging: {}", e),
    }
}

/// Get the current audit log configuration.
pub fn get_audit_log_config(client: &AdminClient) {
    match client.get("/api/v1/audit/config") {
        Ok(value) => println!("{}", serde_json::to_string_pretty(&value).unwrap()),
        Err(e) => eprintln!("Error fetching audit log config: {}", e),
    }
}

/// Enable full query logging (FQL) to the specified directory.
pub fn enable_fql(client: &AdminClient, log_dir: &str) {
    let body = json!({ "log_dir": log_dir });
    match client.post_json("/api/v1/operations/enablefql", &body) {
        Ok(_) => println!("Full query logging enabled (log_dir: {}).", log_dir),
        Err(e) => eprintln!("Error enabling full query logging: {}", e),
    }
}

/// Disable full query logging.
pub fn disable_fql(client: &AdminClient) {
    match client.post_empty("/api/v1/operations/disablefql") {
        Ok(_) => println!("Full query logging disabled."),
        Err(e) => eprintln!("Error disabling full query logging: {}", e),
    }
}

/// Get the current FQL configuration.
pub fn get_fql_config(client: &AdminClient) {
    match client.get("/api/v1/fql/config") {
        Ok(value) => println!("{}", serde_json::to_string_pretty(&value).unwrap()),
        Err(e) => eprintln!("Error fetching FQL config: {}", e),
    }
}

/// Reset FQL (equivalent to disabling it).
pub fn reset_fql(client: &AdminClient) {
    match client.post_empty("/api/v1/operations/disablefql") {
        Ok(_) => println!("Full query logging reset (disabled)."),
        Err(e) => eprintln!("Error resetting full query logging: {}", e),
    }
}

/// Get the current trace probability (0.0 to 1.0).
pub fn get_trace_probability(client: &AdminClient) {
    match client.get("/api/v1/tracing/probability") {
        Ok(value) => {
            if let Some(prob) = value.get("probability") {
                println!("Current trace probability: {}", prob);
            } else {
                println!("{}", serde_json::to_string_pretty(&value).unwrap());
            }
        }
        Err(e) => eprintln!("Error fetching trace probability: {}", e),
    }
}

/// Set the trace probability (0.0 to 1.0).
pub fn set_trace_probability(client: &AdminClient, probability: f64) {
    let body = json!({ "probability": probability });
    match client.post_json("/api/v1/tracing/probability", &body) {
        Ok(_) => println!("Trace probability set to {}.", probability),
        Err(e) => eprintln!("Error setting trace probability: {}", e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that get_logging_levels does not panic when the server is unreachable.
    #[test]
    fn test_get_logging_levels_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        get_logging_levels(&client);
    }

    /// Verify that set_logging_level does not panic when the server is unreachable.
    #[test]
    fn test_set_logging_level_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        set_logging_level(&client, "org.apache.cassandra", "DEBUG");
    }

    /// Verify that enable_audit_log does not panic when the server is unreachable.
    #[test]
    fn test_enable_audit_log_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        enable_audit_log(&client);
    }

    /// Verify that enable_fql does not panic when the server is unreachable.
    #[test]
    fn test_enable_fql_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        enable_fql(&client, "/tmp/fql");
    }

    /// Verify that get_trace_probability does not panic when the server is unreachable.
    #[test]
    fn test_get_trace_probability_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        get_trace_probability(&client);
    }

    /// Verify that set_trace_probability does not panic when the server is unreachable.
    #[test]
    fn test_set_trace_probability_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        set_trace_probability(&client, 0.5);
    }
}
