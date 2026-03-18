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

//! SSTable split tool: splits large SSTables into smaller files by target size.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.tools.SSTableSplitter`
//!
//! Opens the SSTable, reads all partitions, and distributes them across multiple
//! output SSTables based on estimated size proportions derived from file size and
//! partition count.

use std::path::Path;

use cassandra_storage::sstable::{
    format::{SSTableDescriptor, SSTableFormat},
    reader::SSTableReader,
    writer::SSTableWriter,
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

/// Split a large SSTable into smaller files by target size.
///
/// Reads all partitions from the source SSTable and distributes them across
/// multiple output SSTables. Output files use incrementing generation numbers
/// starting from the source generation + 1.
pub fn run(file: &str, size_mb: u64) {
    let path = Path::new(file);
    if !path.exists() {
        println!("Error: file not found: {}", file);
        return;
    }

    let file_size = match std::fs::metadata(file) {
        Ok(m) => m.len(),
        Err(e) => {
            println!("Error reading file metadata: {}", e);
            return;
        }
    };

    let target_bytes = size_mb * 1024 * 1024;
    if file_size <= target_bytes {
        println!(
            "SSTable {} ({} bytes) is smaller than target size ({} MB). No split needed.",
            file, file_size, size_mb
        );
        return;
    }

    let desc = match parse_descriptor(file) {
        Some(d) => d,
        None => {
            println!("Error: invalid SSTable filename format: {}", file);
            return;
        }
    };

    println!("Splitting SSTable: {}", desc.file_prefix());
    println!("Source size: {} bytes", file_size);
    println!("Target split size: {} MB", size_mb);

    // Open the source SSTable
    let reader = match SSTableReader::open(desc.clone()) {
        Ok(r) => r,
        Err(e) => {
            println!("Error opening SSTable: {}", e);
            return;
        }
    };

    // Read all partitions
    let partitions = match reader.iter_partitions() {
        Ok(p) => p,
        Err(e) => {
            println!("Error reading partitions: {}", e);
            return;
        }
    };

    let total_partitions = partitions.len();
    if total_partitions == 0 {
        println!("SSTable contains no partitions. Nothing to split.");
        return;
    }

    // Calculate how many output files we need and partitions per file
    let num_files = ((file_size as f64) / (target_bytes as f64)).ceil() as usize;
    let partitions_per_file = (total_partitions + num_files - 1) / num_files;

    println!(
        "Splitting {} partitions into ~{} file(s) ({} partitions each)",
        total_partitions, num_files, partitions_per_file
    );

    let mut files_written = 0u64;
    let mut total_written = 0usize;

    for (chunk_idx, chunk) in partitions.chunks(partitions_per_file).enumerate() {
        let out_gen = desc.generation + 1 + chunk_idx as u64;
        let mut out_desc =
            SSTableDescriptor::new(&desc.directory, &desc.keyspace, &desc.table, out_gen);
        out_desc.format = desc.format;

        let writer = SSTableWriter::new(out_desc.clone());
        let chunk_vec: Vec<_> = chunk.to_vec();
        match writer.write(&chunk_vec) {
            Ok(stats) => {
                println!(
                    "  Written split {}: {} (gen {}, {} partitions, {} bytes)",
                    chunk_idx + 1,
                    out_desc.file_prefix(),
                    out_gen,
                    stats.partition_count,
                    stats.data_size,
                );
                files_written += 1;
                total_written += chunk.len();
            }
            Err(e) => {
                println!(
                    "  Error writing split {} (gen {}): {}",
                    chunk_idx + 1,
                    out_gen,
                    e
                );
            }
        }
    }

    println!();
    println!(
        "Split complete: {} partitions split into {} file(s).",
        total_written, files_written
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_file_not_found() {
        // Should print error and return without panic
        run("/nonexistent/path/ks-tbl-big-1-Data.db", 50);
    }

    #[test]
    fn test_parse_descriptor_valid() {
        let desc = parse_descriptor("/tmp/mykeyspace-mytable-big-42-Data.db");
        assert!(desc.is_some());
        let d = desc.unwrap();
        assert_eq!(d.keyspace, "mykeyspace");
        assert_eq!(d.table, "mytable");
        assert_eq!(d.generation, 42);
    }

    #[test]
    fn test_parse_descriptor_invalid() {
        let desc = parse_descriptor("/tmp/invalid-file.db");
        assert!(desc.is_none());
    }

    #[test]
    fn test_parse_descriptor_bti_format() {
        let desc = parse_descriptor("/tmp/ks-tbl-bti-7-Data.db");
        assert!(desc.is_some());
        let d = desc.unwrap();
        assert_eq!(d.format, SSTableFormat::Bti);
        assert_eq!(d.generation, 7);
    }
}
