// Licensed under Apache License, Version 2.0.

//! SSTable upgrade tool: reads an SSTable and rewrites it in the current format
//! with an incremented generation number.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.tools.SSTableUpgrader`
//! - `org.apache.cassandra.io.sstable.SSTableRewriter`
//!
//! Opens the source SSTable, reads all partitions, and writes them to a new
//! SSTable using the latest writer implementation. This ensures the on-disk
//! format is up to date even if the original was written by an older version.

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

/// Run SSTable upgrade on the given file path.
///
/// Reads all partitions from the source SSTable and writes them to a new
/// SSTable with an incremented generation, using the current writer format.
pub fn run(file: &str) {
    let path = Path::new(file);
    if !path.exists() {
        eprintln!("Error: file not found: {}", file);
        return;
    }

    let desc = match parse_descriptor(file) {
        Some(d) => d,
        None => {
            eprintln!("Error: invalid SSTable filename format: {}", file);
            return;
        }
    };

    println!("Upgrading SSTable: {}", desc.file_prefix());
    println!("Source directory: {}", desc.directory.display());
    println!("Source generation: {}", desc.generation);

    // Open the source SSTable
    let reader = match SSTableReader::open(desc.clone()) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error opening SSTable: {}", e);
            return;
        }
    };

    // Print source stats if available
    if let Some(stats) = reader.stats() {
        println!();
        println!("Source statistics:");
        println!("  Partitions : {}", stats.partition_count);
        println!("  Rows       : {}", stats.row_count);
        println!("  Cells      : {}", stats.cell_count);
        println!("  Data size  : {} bytes", stats.data_size);
    }

    // Read all partitions
    let partitions = match reader.iter_partitions() {
        Ok(p) => p,
        Err(e) => {
            eprintln!("Error reading partitions: {}", e);
            return;
        }
    };

    // Create output descriptor with incremented generation
    let mut out_desc = SSTableDescriptor::new(
        &desc.directory,
        &desc.keyspace,
        &desc.table,
        desc.generation + 1,
    );
    out_desc.format = desc.format;

    println!();
    println!("Writing upgraded SSTable: {}", out_desc.file_prefix());
    println!("Target generation: {}", out_desc.generation);

    // Write using current format
    let writer = SSTableWriter::new(out_desc);
    match writer.write(&partitions) {
        Ok(stats) => {
            println!();
            println!("Upgrade completed successfully:");
            println!("  Partitions written : {}", stats.partition_count);
            println!("  Rows written       : {}", stats.row_count);
            println!("  Cells written      : {}", stats.cell_count);
            println!("  Min timestamp      : {}", stats.min_timestamp);
            println!("  Max timestamp      : {}", stats.max_timestamp);
            println!("  Data size          : {} bytes", stats.data_size);
            println!("  Index size         : {} bytes", stats.index_size);
        }
        Err(e) => {
            eprintln!("Error writing upgraded SSTable: {}", e);
        }
    }
}
