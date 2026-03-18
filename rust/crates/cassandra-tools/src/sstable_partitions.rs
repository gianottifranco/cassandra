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

//! SSTable partition analysis tool: analyzes partition size distribution within
//! an SSTable.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.tools.SSTablePartitions`
//!
//! Opens the SSTable, iterates all partitions, and collects size estimates
//! (using cell count as a proxy for size). Reports the top N largest partitions
//! and summary statistics including min, max, average, and median sizes.

use std::path::Path;

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

/// Analyze partition size distribution in an SSTable.
///
/// Opens the SSTable and iterates all partitions, using cell count as a proxy
/// for partition size. Prints the top N largest partitions and summary
/// statistics (min, max, average, median).
pub fn run(file: &str, top: usize) {
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

    println!("Analyzing partitions: {}", desc.file_prefix());
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

    if partitions.is_empty() {
        println!("SSTable contains no partitions.");
        return;
    }

    // Collect partition key + estimated size (cell count as proxy)
    let mut partition_sizes: Vec<(String, usize, usize)> = Vec::new();

    for (pk, data) in &partitions {
        let pk_str = String::from_utf8_lossy(pk).to_string();
        let row_count = data.rows.len();
        let cell_count: usize = data.rows.iter().map(|(_, row)| row.cells.len()).sum();
        partition_sizes.push((pk_str, row_count, cell_count));
    }

    // Sort by cell count descending
    partition_sizes.sort_by(|a, b| b.2.cmp(&a.2));

    // Print top N partitions
    let display_count = top.min(partition_sizes.len());
    println!(
        "Top {} partition(s) by estimated size (cell count):",
        display_count
    );
    println!();
    println!("{:<40} {:>8} {:>10}", "Partition Key", "Rows", "Cells");
    println!("{:-<60}", "");

    for (pk, rows, cells) in partition_sizes.iter().take(display_count) {
        let display_pk = if pk.len() > 38 {
            format!("{}...", &pk[..35])
        } else {
            pk.clone()
        };
        println!("{:<40} {:>8} {:>10}", display_pk, rows, cells);
    }

    // Compute summary statistics
    let sizes: Vec<usize> = partition_sizes.iter().map(|(_, _, c)| *c).collect();
    let total: usize = sizes.iter().sum();
    let count = sizes.len();
    let min = *sizes.last().unwrap_or(&0); // sorted descending, last is min
    let max = *sizes.first().unwrap_or(&0);
    let avg = if count > 0 { total / count } else { 0 };

    let median = if count == 0 {
        0
    } else if count % 2 == 0 {
        (sizes[count / 2 - 1] + sizes[count / 2]) / 2
    } else {
        sizes[count / 2]
    };

    let total_rows: usize = partition_sizes.iter().map(|(_, r, _)| *r).sum();

    println!();
    println!("Summary:");
    println!("  Total partitions : {}", count);
    println!("  Total rows       : {}", total_rows);
    println!("  Total cells      : {}", total);
    println!();
    println!("  Cell count per partition:");
    println!("    Min    : {}", min);
    println!("    Max    : {}", max);
    println!("    Avg    : {}", avg);
    println!("    Median : {}", median);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_not_found() {
        // Should print error and return without panic
        run("/nonexistent/path/ks-tbl-big-1-Data.db", 10);
    }

    #[test]
    fn test_parse_descriptor_valid() {
        let desc = parse_descriptor("/data/mykeyspace-mytable-big-99-Data.db");
        assert!(desc.is_some());
        let d = desc.unwrap();
        assert_eq!(d.keyspace, "mykeyspace");
        assert_eq!(d.table, "mytable");
        assert_eq!(d.generation, 99);
    }

    #[test]
    fn test_parse_descriptor_invalid() {
        let desc = parse_descriptor("/tmp/nope.db");
        assert!(desc.is_none());
    }

    #[test]
    fn test_parse_descriptor_bti_format() {
        let desc = parse_descriptor("/var/lib/ks-tbl-bti-50-Data.db");
        assert!(desc.is_some());
        let d = desc.unwrap();
        assert_eq!(d.format, SSTableFormat::Bti);
        assert_eq!(d.generation, 50);
    }
}
