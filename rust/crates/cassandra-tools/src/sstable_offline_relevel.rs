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

//! SSTable offline relevel tool: reassigns SSTable compaction levels for
//! Leveled Compaction Strategy (LCS).
//!
//! ## Java Oracle
//! - `org.apache.cassandra.tools.SSTableOfflineRelevel`
//!
//! Lists all SSTable data files in a directory, sorts them by size, and assigns
//! levels based on LCS size thresholds (L0 < 160 MB, L1 < 1.6 GB, etc., using
//! a 10x growth factor per level). Updates the Statistics.db companion file for
//! each SSTable.

use std::path::Path;

use cassandra_storage::sstable::format::{Component, SSTableDescriptor, SSTableFormat};

/// LCS base level max size: 160 MB for L0.
/// Each subsequent level is 10x larger.
const L0_MAX_BYTES: u64 = 160 * 1024 * 1024;
const LEVEL_FACTOR: u64 = 10;
const MAX_LEVEL: u32 = 9;

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

/// Determine the LCS level for a given file size.
fn compute_level(size_bytes: u64) -> u32 {
    let mut threshold = L0_MAX_BYTES;
    for level in 0..MAX_LEVEL {
        if size_bytes <= threshold {
            return level;
        }
        threshold = threshold.saturating_mul(LEVEL_FACTOR);
    }
    MAX_LEVEL
}

/// Reassign SSTable levels for Leveled Compaction Strategy.
///
/// Scans the given directory for SSTable data files (`*-Data.db`), sorts them
/// by size, assigns levels based on LCS size thresholds, and updates the
/// `compaction_level` field in each SSTable's Statistics.db.
pub fn run(dir: &str) {
    let dir_path = Path::new(dir);
    if !dir_path.is_dir() {
        println!("Error: not a directory: {}", dir);
        return;
    }

    // Find all Data.db files in the directory
    let entries = match std::fs::read_dir(dir_path) {
        Ok(e) => e,
        Err(e) => {
            println!("Error reading directory: {}", e);
            return;
        }
    };

    let mut sstables: Vec<(String, u64)> = Vec::new();

    for entry in entries {
        let entry = match entry {
            Ok(e) => e,
            Err(e) => {
                println!("Warning: error reading directory entry: {}", e);
                continue;
            }
        };

        let file_name = entry.file_name().to_string_lossy().to_string();
        if !file_name.ends_with("-Data.db") {
            continue;
        }

        let full_path = entry.path();
        let size = match std::fs::metadata(&full_path) {
            Ok(m) => m.len(),
            Err(e) => {
                println!(
                    "Warning: cannot read metadata for {}: {}",
                    full_path.display(),
                    e
                );
                continue;
            }
        };

        sstables.push((full_path.to_string_lossy().to_string(), size));
    }

    if sstables.is_empty() {
        println!("No SSTable data files found in {}", dir);
        return;
    }

    // Sort by size ascending (smaller SSTables get lower levels)
    sstables.sort_by_key(|&(_, size)| size);

    println!("Found {} SSTable(s) in {}", sstables.len(), dir);
    println!();
    println!("{:<50} {:>12} {:>6}", "SSTable", "Size (bytes)", "Level");
    println!("{:-<70}", "");

    let mut level_counts: Vec<u32> = vec![0; (MAX_LEVEL + 1) as usize];
    let mut updated = 0u32;
    let mut errors = 0u32;

    for (file_path, size) in &sstables {
        let level = compute_level(*size);
        level_counts[level as usize] += 1;

        let file_name = Path::new(file_path)
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| file_path.clone());

        println!("{:<50} {:>12} {:>6}", file_name, size, level);

        // Update Statistics.db
        let desc = match parse_descriptor(file_path) {
            Some(d) => d,
            None => {
                println!("  Warning: could not parse descriptor for {}", file_path);
                errors += 1;
                continue;
            }
        };

        let stats_path = desc.component_path(Component::Statistics);
        if !stats_path.exists() {
            println!("  Warning: Statistics.db not found for {}", file_name);
            errors += 1;
            continue;
        }

        let contents = match std::fs::read_to_string(&stats_path) {
            Ok(c) => c,
            Err(e) => {
                println!("  Error reading Statistics.db: {}", e);
                errors += 1;
                continue;
            }
        };

        let mut json: serde_json::Value = match serde_json::from_str(&contents) {
            Ok(v) => v,
            Err(e) => {
                println!("  Error parsing Statistics.db: {}", e);
                errors += 1;
                continue;
            }
        };

        if let Some(obj) = json.as_object_mut() {
            obj.insert(
                "compaction_level".to_string(),
                serde_json::Value::Number(serde_json::Number::from(level)),
            );
        }

        let serialized = match serde_json::to_string_pretty(&json) {
            Ok(s) => s,
            Err(e) => {
                println!("  Error serializing Statistics.db: {}", e);
                errors += 1;
                continue;
            }
        };

        if let Err(e) = std::fs::write(&stats_path, serialized) {
            println!("  Error writing Statistics.db: {}", e);
            errors += 1;
            continue;
        }

        updated += 1;
    }

    println!();
    println!("Level distribution:");
    for (level, count) in level_counts.iter().enumerate() {
        if *count > 0 {
            println!("  L{}: {} SSTable(s)", level, count);
        }
    }

    println!();
    println!("Releveled {} SSTable(s) ({} error(s)).", updated, errors);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compute_level_small() {
        // < 160 MB => L0
        assert_eq!(compute_level(100 * 1024 * 1024), 0);
    }

    #[test]
    fn test_compute_level_medium() {
        // 160 MB < size <= 1.6 GB => L1
        assert_eq!(compute_level(500 * 1024 * 1024), 1);
    }

    #[test]
    fn test_compute_level_large() {
        // 1.6 GB < size <= 16 GB => L2
        assert_eq!(compute_level(5 * 1024 * 1024 * 1024), 2);
    }

    #[test]
    fn test_compute_level_boundary() {
        // Exactly 160 MB => L0
        assert_eq!(compute_level(160 * 1024 * 1024), 0);
    }

    #[test]
    fn test_directory_not_found() {
        // Should print error and return without panic
        run("/nonexistent/directory");
    }

    #[test]
    fn test_empty_directory() {
        let tmp_dir = tempfile::tempdir().expect("create temp dir");
        run(tmp_dir.path().to_str().unwrap());
        // Should print "No SSTable data files found" and not panic
    }
}
