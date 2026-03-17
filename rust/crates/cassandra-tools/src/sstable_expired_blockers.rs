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

//! SSTable expired blockers tool: identifies SSTables that contain tombstones
//! blocking garbage collection.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.tools.SSTableExpiredBlockers`
//!
//! Scans an SSTable for tombstones and compares their timestamps against the
//! configured GC grace period. Reports which partitions contain expired
//! tombstones that are eligible for garbage collection but are being retained
//! because compaction has not yet removed them.

use std::path::Path;
use std::time::{SystemTime, UNIX_EPOCH};

use cassandra_storage::sstable::{
    format::{SSTableDescriptor, SSTableFormat},
    reader::SSTableReader,
};

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

/// Find tombstones blocking garbage collection in the given SSTable.
///
/// Iterates all partitions and rows, counting tombstones and checking whether
/// they have exceeded the GC grace period. Prints a summary of blocking
/// tombstones per partition.
pub fn run(file: &str, gc_grace_seconds: u64) {
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

    println!("Scanning for expired tombstone blockers: {}", desc.file_prefix());
    println!("GC grace period: {} seconds", gc_grace_seconds);
    println!();

    let reader = match SSTableReader::open(desc) {
        Ok(r) => r,
        Err(e) => {
            println!("Error opening SSTable: {}", e);
            return;
        }
    };

    let partitions = match reader.iter_partitions() {
        Ok(p) => p,
        Err(e) => {
            println!("Error reading partitions: {}", e);
            return;
        }
    };

    let now_seconds = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let gc_cutoff = now_seconds.saturating_sub(gc_grace_seconds);

    let mut total_tombstones = 0u64;
    let mut expired_tombstones = 0u64;
    let mut blocking_partitions: Vec<(String, u64, u64)> = Vec::new();

    for (pk, data) in &partitions {
        let mut partition_total = 0u64;
        let mut partition_expired = 0u64;

        for (_ck, row) in &data.rows {
            // Check row-level tombstones
            if row.is_tombstone {
                partition_total += 1;
                if let Some(ldt) = row.local_deletion_time {
                    if (ldt as u64) < gc_cutoff {
                        partition_expired += 1;
                    }
                }
            }

            // Check cell-level tombstones
            for cell in &row.cells {
                if cell.is_tombstone {
                    partition_total += 1;
                    // Use timestamp as microseconds, convert to seconds
                    let ts_seconds = (cell.timestamp / 1_000_000) as u64;
                    if ts_seconds < gc_cutoff {
                        partition_expired += 1;
                    }
                }
            }
        }

        total_tombstones += partition_total;
        expired_tombstones += partition_expired;

        if partition_expired > 0 {
            let pk_str = String::from_utf8_lossy(pk).to_string();
            blocking_partitions.push((pk_str, partition_total, partition_expired));
        }
    }

    // Print results
    if blocking_partitions.is_empty() {
        println!("No expired tombstones found blocking GC.");
    } else {
        println!(
            "Found {} partition(s) with expired tombstones blocking GC:",
            blocking_partitions.len()
        );
        println!();
        println!(
            "{:<40} {:>12} {:>12}",
            "Partition Key", "Total Tombs", "Expired"
        );
        println!("{:-<66}", "");

        for (pk, total, expired) in &blocking_partitions {
            println!("{:<40} {:>12} {:>12}", pk, total, expired);
        }
    }

    println!();
    println!("Summary:");
    println!("  Total partitions scanned : {}", partitions.len());
    println!("  Total tombstones         : {}", total_tombstones);
    println!("  Expired tombstones       : {}", expired_tombstones);
    println!(
        "  Blocking partitions      : {}",
        blocking_partitions.len()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_not_found() {
        // Should print error and return without panic
        run("/nonexistent/path/ks-tbl-big-1-Data.db", 864000);
    }

    #[test]
    fn test_parse_descriptor_valid() {
        let desc = parse_descriptor("/data/keyspace1-table1-big-3-Data.db");
        assert!(desc.is_some());
        let d = desc.unwrap();
        assert_eq!(d.keyspace, "keyspace1");
        assert_eq!(d.table, "table1");
        assert_eq!(d.generation, 3);
    }

    #[test]
    fn test_parse_descriptor_invalid() {
        let desc = parse_descriptor("/tmp/not-a-valid-sstable");
        // This has 5 parts separated by '-' so it may parse; test the edge case
        // of a truly short name
        let desc2 = parse_descriptor("/tmp/short.db");
        assert!(desc2.is_none());
    }

    #[test]
    fn test_parse_descriptor_bti_format() {
        let desc = parse_descriptor("/var/data/ks-tbl-bti-100-Data.db");
        assert!(desc.is_some());
        let d = desc.unwrap();
        assert_eq!(d.format, SSTableFormat::Bti);
        assert_eq!(d.generation, 100);
    }
}
