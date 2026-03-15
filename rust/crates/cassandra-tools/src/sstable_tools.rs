// Licensed under Apache License, Version 2.0.

//! SSTable CLI tools: dump and metadata inspection.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.tools.SSTableExport` (sstabledump)
//! - `org.apache.cassandra.tools.SSTableMetadataViewer` (sstablemetadata)

use std::path::Path;

/// Dump the contents of an SSTable to stdout in JSON format.
///
/// TODO: Actually read the SSTable using cassandra-storage.
/// Current implementation is a stub that reports the file status.
pub fn dump_sstable(file: &str) {
    let path = Path::new(file);
    if !path.exists() {
        eprintln!("Error: file not found: {}", file);
        return;
    }

    let size = std::fs::metadata(path)
        .map(|m| m.len())
        .unwrap_or(0);

    println!("SSTable: {}", file);
    println!("Size: {} bytes", size);
    println!();
    println!("[");
    println!("  // SSTable dump not yet implemented.");
    println!("  // TODO: Parse SSTable format via cassandra-storage crate.");
    println!("  // File exists and is {} bytes.", size);
    println!("]");
}

/// Show SSTable metadata statistics.
///
/// TODO: Actually parse SSTable-Statistics.db and TOC.
pub fn show_metadata(file: &str) {
    let path = Path::new(file);
    if !path.exists() {
        eprintln!("Error: file not found: {}", file);
        return;
    }

    let size = std::fs::metadata(path)
        .map(|m| m.len())
        .unwrap_or(0);

    println!("SSTable: {}", file);
    println!("Size: {} bytes", size);
    println!();
    println!("Metadata:");
    println!("  Estimated partitions : (not yet implemented)");
    println!("  Estimated cells      : (not yet implemented)");
    println!("  SSTable Level        : 0");
    println!("  Compression          : none (not yet implemented)");
    println!("  Min/Max timestamp    : (not yet implemented)");
    println!("  Min/Max local del    : (not yet implemented)");
    println!("  Bloom filter FP      : (not yet implemented)");
    println!();
    println!("  // TODO: Parse SSTable metadata via cassandra-storage crate.");
}
