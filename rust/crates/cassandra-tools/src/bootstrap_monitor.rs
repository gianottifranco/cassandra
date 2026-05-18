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

//! Monitor bootstrap progress by polling the admin API.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.tools.nodetool.BootstrapMonitor`

use crate::admin_client::AdminClient;

/// Format a progress bar string.
///
/// Returns a string like `[####----] 50%` for the given progress percentage.
pub fn format_progress_bar(progress: u64, bar_width: usize) -> String {
    let clamped = progress.min(100);
    let filled = (clamped as usize * bar_width) / 100;
    let empty = bar_width - filled;
    format!("[{}{}] {}%", "#".repeat(filled), "-".repeat(empty), clamped,)
}

/// Monitor bootstrap progress by polling the admin API operations endpoint.
///
/// Polls `GET /api/v1/operations` every 2 seconds, filtering for bootstrap
/// operations, and prints a live progress bar until the operation completes.
pub fn run(client: &AdminClient) {
    println!("Monitoring bootstrap progress...");
    println!("Polling {}/api/v1/operations", client.base_url());
    println!();

    // Poll operations endpoint for bootstrap operations
    loop {
        match client.get("/api/v1/operations") {
            Ok(ops) => {
                if let Some(arr) = ops.as_array() {
                    let bootstrap_ops: Vec<_> = arr
                        .iter()
                        .filter(|op| {
                            op.get("operation_type")
                                .and_then(|t| t.as_str())
                                .map(|t| t == "Bootstrap")
                                .unwrap_or(false)
                        })
                        .collect();

                    if bootstrap_ops.is_empty() {
                        println!("No bootstrap operation in progress.");
                        return;
                    }

                    for op in &bootstrap_ops {
                        let progress = op.get("progress").and_then(|p| p.as_u64()).unwrap_or(0);
                        let status = op
                            .get("status")
                            .and_then(|s| s.as_str())
                            .unwrap_or("UNKNOWN");
                        let desc = op.get("description").and_then(|d| d.as_str()).unwrap_or("");
                        let elapsed = op
                            .get("elapsed_secs")
                            .and_then(|e| e.as_f64())
                            .unwrap_or(0.0);

                        // Print progress bar
                        let bar = format_progress_bar(progress, 40);
                        print!("\r{} - {} ({}) {:.0}s", bar, status, desc, elapsed);

                        if status == "COMPLETED" || progress >= 100 {
                            println!();
                            println!("Bootstrap completed successfully!");
                            return;
                        }
                    }
                } else {
                    println!("No operations data available.");
                    return;
                }
            }
            Err(e) => {
                println!("Failed to connect to admin API: {}", e);
                println!("Is the node running? Check {}", client.base_url());
                return;
            }
        }

        std::thread::sleep(std::time::Duration::from_secs(2));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_run_function_signature() {
        // Verify the public API compiles and accepts AdminClient reference.
        fn _assert_fn(_f: fn(&AdminClient)) {}
        _assert_fn(run);
    }

    // ── Progress bar formatting ──────────────────────────────────────

    #[test]
    fn test_format_progress_bar_zero() {
        let bar = format_progress_bar(0, 40);
        assert_eq!(bar, "[----------------------------------------] 0%");
    }

    #[test]
    fn test_format_progress_bar_fifty() {
        let bar = format_progress_bar(50, 40);
        assert_eq!(bar, "[####################--------------------] 50%");
    }

    #[test]
    fn test_format_progress_bar_hundred() {
        let bar = format_progress_bar(100, 40);
        assert_eq!(bar, "[########################################] 100%");
    }

    #[test]
    fn test_format_progress_bar_clamps_above_100() {
        let bar = format_progress_bar(150, 40);
        assert_eq!(bar, "[########################################] 100%");
    }

    #[test]
    fn test_format_progress_bar_small_width() {
        let bar = format_progress_bar(50, 10);
        assert_eq!(bar, "[#####-----] 50%");
    }

    #[test]
    fn test_format_progress_bar_zero_width() {
        let bar = format_progress_bar(50, 0);
        assert_eq!(bar, "[] 50%");
    }

    // ── Operations JSON parsing ──────────────────────────────────────

    #[test]
    fn test_parse_bootstrap_operation() {
        let ops = json!([
            {
                "operation_type": "Bootstrap",
                "progress": 75,
                "status": "IN_PROGRESS",
                "description": "Streaming data",
                "elapsed_secs": 120.5
            },
            {
                "operation_type": "Compaction",
                "progress": 50,
                "status": "IN_PROGRESS",
                "description": "Compacting",
                "elapsed_secs": 30.0
            }
        ]);

        let arr = ops.as_array().unwrap();
        let bootstrap_ops: Vec<_> = arr
            .iter()
            .filter(|op| {
                op.get("operation_type")
                    .and_then(|t| t.as_str())
                    .map(|t| t == "Bootstrap")
                    .unwrap_or(false)
            })
            .collect();

        assert_eq!(bootstrap_ops.len(), 1);
        let op = bootstrap_ops[0];
        assert_eq!(op.get("progress").unwrap().as_u64().unwrap(), 75);
        assert_eq!(op.get("status").unwrap().as_str().unwrap(), "IN_PROGRESS");
        assert_eq!(
            op.get("description").unwrap().as_str().unwrap(),
            "Streaming data"
        );
        assert!((op.get("elapsed_secs").unwrap().as_f64().unwrap() - 120.5).abs() < f64::EPSILON);
    }

    #[test]
    fn test_parse_empty_operations() {
        let ops = json!([]);
        let arr = ops.as_array().unwrap();
        let bootstrap_ops: Vec<_> = arr
            .iter()
            .filter(|op| {
                op.get("operation_type")
                    .and_then(|t| t.as_str())
                    .map(|t| t == "Bootstrap")
                    .unwrap_or(false)
            })
            .collect();

        assert!(bootstrap_ops.is_empty());
    }

    #[test]
    fn test_parse_completed_bootstrap() {
        let ops = json!([
            {
                "operation_type": "Bootstrap",
                "progress": 100,
                "status": "COMPLETED",
                "description": "Done",
                "elapsed_secs": 300.0
            }
        ]);

        let arr = ops.as_array().unwrap();
        let op = &arr[0];
        let status = op
            .get("status")
            .and_then(|s| s.as_str())
            .unwrap_or("UNKNOWN");
        let progress = op.get("progress").and_then(|p| p.as_u64()).unwrap_or(0);

        assert_eq!(status, "COMPLETED");
        assert!(progress >= 100);
    }

    #[test]
    fn test_parse_operation_missing_fields() {
        let ops = json!([
            {
                "operation_type": "Bootstrap"
            }
        ]);

        let arr = ops.as_array().unwrap();
        let op = &arr[0];
        let progress = op.get("progress").and_then(|p| p.as_u64()).unwrap_or(0);
        let status = op
            .get("status")
            .and_then(|s| s.as_str())
            .unwrap_or("UNKNOWN");
        let desc = op.get("description").and_then(|d| d.as_str()).unwrap_or("");
        let elapsed = op
            .get("elapsed_secs")
            .and_then(|e| e.as_f64())
            .unwrap_or(0.0);

        assert_eq!(progress, 0);
        assert_eq!(status, "UNKNOWN");
        assert_eq!(desc, "");
        assert!((elapsed - 0.0).abs() < f64::EPSILON);
    }
}
