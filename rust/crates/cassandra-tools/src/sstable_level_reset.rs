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

//! SSTable level reset tool: resets the compaction level to 0 in SSTable
//! metadata.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.tools.SSTableLevelResetter`
//!
//! Reads the Statistics.db companion file (JSON), sets the `compaction_level`
//! field to 0, and writes it back. This is useful after migrating between
//! compaction strategies or recovering from mis-leveled SSTables.

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

/// Reset the compaction level to 0 in SSTable metadata.
///
/// Reads the Statistics.db companion file, sets `compaction_level` to 0, and
/// writes it back. The SSTable data file path is used to locate the companion
/// Statistics.db file.
pub fn run(file: &str) {
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

    // Read current level for reporting
    let old_level = json
        .get("compaction_level")
        .and_then(|v| v.as_i64())
        .unwrap_or(-1);

    // Set compaction_level to 0
    if let Some(obj) = json.as_object_mut() {
        obj.insert(
            "compaction_level".to_string(),
            serde_json::Value::Number(serde_json::Number::from(0)),
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

    if old_level > 0 {
        println!(
            "Reset compaction level from {} to 0 for {}",
            old_level, file
        );
    } else {
        println!("Reset compaction level to 0 for {}", file);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;

    #[test]
    fn test_file_not_found() {
        // Should print error and return without panic
        run("/nonexistent/path/ks-tbl-big-1-Data.db");
    }

    #[test]
    fn test_parse_descriptor_valid() {
        let desc = parse_descriptor("/tmp/mykeyspace-mytable-big-5-Data.db");
        assert!(desc.is_some());
        let d = desc.unwrap();
        assert_eq!(d.keyspace, "mykeyspace");
        assert_eq!(d.table, "mytable");
        assert_eq!(d.generation, 5);
    }

    #[test]
    fn test_parse_descriptor_invalid() {
        let desc = parse_descriptor("/tmp/badname.db");
        assert!(desc.is_none());
    }

    #[test]
    fn test_level_reset_with_temp_file() {
        let tmp_dir = tempfile::tempdir().expect("create temp dir");
        let data_name = "ks-tbl-big-1-Data.db";
        let stats_name = "ks-tbl-big-1-Statistics.db";

        // Create a dummy data file
        let data_path = tmp_dir.path().join(data_name);
        fs::write(&data_path, b"dummy").expect("write data");

        // Create Statistics.db with compaction_level = 5
        let stats_path = tmp_dir.path().join(stats_name);
        let stats_json = serde_json::json!({
            "partition_count": 100,
            "row_count": 200,
            "cell_count": 500,
            "min_timestamp": 1000,
            "max_timestamp": 2000,
            "data_size": 4096,
            "index_size": 512,
            "compaction_level": 5
        });
        fs::write(
            &stats_path,
            serde_json::to_string_pretty(&stats_json).unwrap(),
        )
        .expect("write stats");

        // Run the reset
        run(data_path.to_str().unwrap());

        // Verify compaction_level is now 0
        let updated: serde_json::Value =
            serde_json::from_str(&fs::read_to_string(&stats_path).unwrap()).unwrap();
        assert_eq!(updated["compaction_level"], 0);
    }
}
