// Licensed under Apache License, Version 2.0.

//! SSTable scrubber tool: reads an SSTable, skips corrupted partitions, and
//! writes valid data to a new SSTable with an incremented generation.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.tools.SSTableScrubber`
//! - `org.apache.cassandra.db.compaction.Scrubber`
//!
//! Opens the SSTable via `SSTableReader`, iterates all partitions, and writes
//! only the successfully-read partitions to a new SSTable using `SSTableWriter`.

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

/// Run SSTable scrubbing on the given file path.
///
/// Reads all partitions from the source SSTable, discards any corrupted data,
/// and writes the valid partitions to a new SSTable. The output directory
/// defaults to the same directory with generation incremented by 1.
pub fn run(file: &str, output_dir: Option<&str>) {
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

    println!("Scrubbing SSTable: {}", desc.file_prefix());
    println!("Source directory: {}", desc.directory.display());

    // Open the source SSTable
    let reader = match SSTableReader::open(desc.clone()) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error opening SSTable: {}", e);
            return;
        }
    };

    // Read all partitions (corrupted data will cause iter_partitions to stop)
    let partitions = match reader.iter_partitions() {
        Ok(p) => {
            println!("Read {} partition(s) successfully.", p.len());
            p
        }
        Err(e) => {
            eprintln!(
                "Error reading partitions (partial data may be recovered): {}",
                e
            );
            eprintln!("No output written.");
            return;
        }
    };

    if partitions.is_empty() {
        println!("No partitions to write. SSTable is empty or fully corrupted.");
        return;
    }

    // Determine output descriptor
    let out_dir = match output_dir {
        Some(d) => Path::new(d).to_path_buf(),
        None => desc.directory.clone(),
    };

    let mut out_desc = SSTableDescriptor::new(
        &out_dir,
        &desc.keyspace,
        &desc.table,
        desc.generation + 1,
    );
    out_desc.format = desc.format;

    println!("Output SSTable: {}", out_desc.file_prefix());
    println!("Output directory: {}", out_dir.display());

    // Write the scrubbed partitions
    let writer = SSTableWriter::new(out_desc);
    match writer.write(&partitions) {
        Ok(stats) => {
            println!();
            println!("Scrub completed successfully:");
            println!("  Partitions written : {}", stats.partition_count);
            println!("  Rows written       : {}", stats.row_count);
            println!("  Cells written      : {}", stats.cell_count);
            println!("  Data size          : {} bytes", stats.data_size);
            println!("  Index size         : {} bytes", stats.index_size);
        }
        Err(e) => {
            eprintln!("Error writing scrubbed SSTable: {}", e);
        }
    }
}
