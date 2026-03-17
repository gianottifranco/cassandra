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

//! Cache and hints commands — invalidate caches, manage cache capacity,
//! and control hinted handoff.

use crate::admin_client::AdminClient;
use serde_json::json;

/// Invalidate a cache by type (key, row, counter, credentials, permissions, roles).
pub fn invalidate_cache(client: &AdminClient, cache_type: &str) {
    let body = json!({ "cache_type": cache_type });
    match client.post_json("/api/v1/cache/invalidate", &body) {
        Ok(_) => println!("Successfully invalidated {} cache.", cache_type),
        Err(e) => eprintln!("Error invalidating {} cache: {}", cache_type, e),
    }
}

/// Set the capacity (in MB) of a cache.
pub fn set_cache_capacity(client: &AdminClient, cache_type: &str, capacity_mb: u32) {
    let body = json!({
        "cache_type": cache_type,
        "capacity_mb": capacity_mb,
    });
    match client.post_json("/api/v1/cache/capacity", &body) {
        Ok(_) => println!(
            "Successfully set {} cache capacity to {} MB.",
            cache_type, capacity_mb
        ),
        Err(e) => eprintln!("Error setting {} cache capacity: {}", cache_type, e),
    }
}

/// Enable hinted handoff.
pub fn enable_handoff(client: &AdminClient) {
    match client.post_empty("/api/v1/handoff/enable") {
        Ok(_) => println!("Hinted handoff enabled."),
        Err(e) => eprintln!("Error enabling hinted handoff: {}", e),
    }
}

/// Disable hinted handoff.
pub fn disable_handoff(client: &AdminClient) {
    match client.post_empty("/api/v1/handoff/disable") {
        Ok(_) => println!("Hinted handoff disabled."),
        Err(e) => eprintln!("Error disabling hinted handoff: {}", e),
    }
}

/// Pause hinted handoff delivery.
pub fn pause_handoff(client: &AdminClient) {
    match client.post_empty("/api/v1/handoff/pause") {
        Ok(_) => println!("Hinted handoff paused."),
        Err(e) => eprintln!("Error pausing hinted handoff: {}", e),
    }
}

/// Resume hinted handoff delivery.
pub fn resume_handoff(client: &AdminClient) {
    match client.post_empty("/api/v1/handoff/resume") {
        Ok(_) => println!("Hinted handoff resumed."),
        Err(e) => eprintln!("Error resuming hinted handoff: {}", e),
    }
}

/// Truncate (delete) all pending hints.
pub fn truncate_hints(client: &AdminClient) {
    match client.post_empty("/api/v1/hints/truncate") {
        Ok(_) => println!("All hints truncated."),
        Err(e) => eprintln!("Error truncating hints: {}", e),
    }
}

/// List pending hints per endpoint.
pub fn list_pending_hints(client: &AdminClient) {
    match client.get("/api/v1/hints/pending") {
        Ok(value) => println!("{}", serde_json::to_string_pretty(&value).unwrap()),
        Err(e) => eprintln!("Error listing pending hints: {}", e),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Verify that invalidate_cache does not panic when the server is unreachable.
    #[test]
    fn test_invalidate_cache_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        invalidate_cache(&client, "key");
    }

    /// Verify that set_cache_capacity does not panic when the server is unreachable.
    #[test]
    fn test_set_cache_capacity_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        set_cache_capacity(&client, "row", 512);
    }

    /// Verify that enable_handoff does not panic when the server is unreachable.
    #[test]
    fn test_enable_handoff_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        enable_handoff(&client);
    }

    /// Verify that truncate_hints does not panic when the server is unreachable.
    #[test]
    fn test_truncate_hints_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        truncate_hints(&client);
    }

    /// Verify that list_pending_hints does not panic when the server is unreachable.
    #[test]
    fn test_list_pending_hints_connection_refused() {
        let client = AdminClient::new("127.0.0.1", 1);
        list_pending_hints(&client);
    }
}
