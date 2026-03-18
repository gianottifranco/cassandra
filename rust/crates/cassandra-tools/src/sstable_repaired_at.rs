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

//! SSTable repaired-at tool: sets or clears the repaired-at timestamp in
//! SSTable metadata.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.tools.SSTableRepairedAtSetter`
//!
//! Reads the Statistics.db companion file (JSON), sets the `repaired_at`
//! field to the given timestamp (or 0 to mark as unrepaired), and writes
//! it back. Used by operators to manually mark SSTables as repaired or
//! unrepaired after incremental repair issues.

use std::path::Path;

use cassandra_storage::sstable::format::{Component, SSTableDescriptor, SSTableFormat};

fn parse_descriptor(file: &str) -> Option<SSTableDescriptor> {
    let path = Path::new(file);
    let file_name = path.file_name()?.to_string_lossy();
    let parts: Vec<&str> = file_name.split('-').collect();
    if parts.len() < 5 {
        return None;
    }

    let ks = parts[0];
    let tbl = parts[1];
    let fmt_str = parts[2];
    let gen_str = parts[3];

    let generation = gen_str.parse::<u64>().ok()?;
    let dir = path.parent().unwrap_or_else(|| Path::new("."));

    let mut desc = SSTableDescriptor::new(dir, ks, tbl, generation);
    if fmt_str == "bti" {
        desc.format = SSTableFormat::Bti;
    }
    Some(desc)
}

/// Set or clear the repaired-at timestamp in SSTable metadata.
///
/// A `repaired_at` value of 0 marks the SSTable as unrepaired. Any positive
/// value is treated as a Unix timestamp (milliseconds) indicating when the
/// SSTable was last repaired.
pub fn run(file: &str, repaired_at: i64) {
    let path = Path::new(file);
    if !path.exists() {
        println!("Error: file not found: {}", file);
        return;
    }

    let desc = match parse_descriptor(file) {
        Some(d) => d,
        None => {
            println!("Error: invalid SSTable filename format: {}", file);
            return;
        }
    };

    let stats_path = desc.component_path(Component::Statistics);
    if !stats_path.exists() {
        println!(
            "Error: Statistics.db not found at: {}",
            stats_path.display()
        );
        return;
    }

    // Read the Statistics.db JSON
    let contents = match std::fs::read_to_string(&stats_path) {
        Ok(c) => c,
        Err(e) => {
            println!("Error reading Statistics.db: {}", e);
            return;
        }
    };

    let mut json: serde_json::Value = match serde_json::from_str(&contents) {
        Ok(v) => v,
        Err(e) => {
            println!("Error parsing Statistics.db JSON: {}", e);
            return;
        }
    };

    // Read current value for reporting
    let old_value = json
        .get("repaired_at")
        .and_then(|v| v.as_i64())
        .unwrap_or(0);

    // Set repaired_at
    if let Some(obj) = json.as_object_mut() {
        obj.insert(
            "repaired_at".to_string(),
            serde_json::Value::Number(serde_json::Number::from(repaired_at)),
        );
    }

    // Write back
    let updated = match serde_json::to_string_pretty(&json) {
        Ok(s) => s,
        Err(e) => {
            println!("Error serializing Statistics.db: {}", e);
            return;
        }
    };

    if let Err(e) = std::fs::write(&stats_path, updated) {
        println!("Error writing Statistics.db: {}", e);
        return;
    }

    if repaired_at == 0 {
        println!(
            "Cleared repaired-at timestamp (was {}) for {}",
            old_value, file
        );
    } else {
        println!(
            "Set repaired-at to {} (was {}) for {}",
            repaired_at, old_value, file
        );
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_file_not_found() {
        // Should print error and return without panic
        run("/nonexistent/path/ks-tbl-big-1-Data.db", 12345);
    }

    #[test]
    fn test_parse_descriptor_valid() {
        let desc = parse_descriptor("/tmp/ks-tbl-big-10-Data.db");
        assert!(desc.is_some());
        let d = desc.unwrap();
        assert_eq!(d.generation, 10);
    }

    #[test]
    fn test_parse_descriptor_invalid() {
        let desc = parse_descriptor("/tmp/bad.db");
        assert!(desc.is_none());
    }

    #[test]
    fn test_set_repaired_at_with_temp_file() {
        let tmp_dir = tempfile::tempdir().expect("create temp dir");
        let data_name = "ks-tbl-big-1-Data.db";
        let stats_name = "ks-tbl-big-1-Statistics.db";

        // Create dummy data file
        let data_path = tmp_dir.path().join(data_name);
        fs::write(&data_path, b"dummy").expect("write data");

        // Create Statistics.db with repaired_at = 0
        let stats_path = tmp_dir.path().join(stats_name);
        let stats_json = serde_json::json!({
            "partition_count": 50,
            "row_count": 100,
            "cell_count": 300,
            "min_timestamp": 1000,
            "max_timestamp": 2000,
            "data_size": 2048,
            "index_size": 256,
            "repaired_at": 0
        });
        fs::write(
            &stats_path,
            serde_json::to_string_pretty(&stats_json).unwrap(),
        )
        .expect("write stats");

        // Set repaired_at to a timestamp
        run(data_path.to_str().unwrap(), 1700000000000);

        // Verify
        let updated: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&stats_path).unwrap()).unwrap();
        assert_eq!(updated["repaired_at"], 1700000000000i64);

        // Clear it
        run(data_path.to_str().unwrap(), 0);

        let cleared: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&stats_path).unwrap()).unwrap();
        assert_eq!(cleared["repaired_at"], 0);
    }
}
