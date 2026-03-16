// Licensed under Apache License, Version 2.0.

//! SSTable CLI tools: dump and metadata inspection.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.tools.SSTableExport` (sstabledump)
//! - `org.apache.cassandra.tools.SSTableMetadataViewer` (sstablemetadata)

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

/// Dump the contents of an SSTable to stdout in JSON format.
pub fn dump_sstable(file: &str) {
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

    let reader = match SSTableReader::open(desc) {
        Ok(r) => r,
        Err(e) => {
            eprintln!("Error opening SSTable for dump: {}", e);
            return;
        }
    };

    println!("[");
    match reader.iter_partitions() {
        Ok(partitions) => {
            let mut first_p = true;
            for (pk, data) in partitions {
                if !first_p {
                    println!(",");
                }
                first_p = false;
                println!("  {{");
                println!(
                    "    \"partition_key\": \"{}\",",
                    String::from_utf8_lossy(&pk)
                );
                println!("    \"rows\": [");
                let mut first_r = true;
                for (ck, row) in &data.rows {
                    if !first_r {
                        println!(",");
                    }
                    first_r = false;
                    println!("      {{");
                    println!(
                        "        \"clustering_key\": \"{}\",",
                        String::from_utf8_lossy(ck)
                    );
                    println!("        \"is_tombstone\": {},", row.is_tombstone);
                    if let Some(ldt) = row.local_deletion_time {
                        println!("        \"local_deletion_time\": {},", ldt);
                    }
                    println!("        \"cells\": [");
                    let mut first_c = true;
                    for cell in &row.cells {
                        if !first_c {
                            println!(",");
                        }
                        first_c = false;
                        println!("          {{");
                        println!("            \"column\": \"{}\",", cell.column);
                        let val_str = cell
                            .value
                            .as_deref()
                            .map(|v| String::from_utf8_lossy(v).into_owned())
                            .unwrap_or_else(|| "null".to_string());
                        println!("            \"value\": \"{}\",", val_str);
                        println!("            \"timestamp\": {},", cell.timestamp);
                        println!("            \"ttl\": {},", cell.ttl);
                        println!("            \"is_tombstone\": {}", cell.is_tombstone);
                        print!("          }}");
                    }
                    println!("\n        ]");
                    print!("      }}");
                }
                println!("\n    ]");
                print!("  }}");
            }
        }
        Err(e) => {
            eprintln!("Error iterating partitions: {}", e);
            return;
        }
    }
    println!("\n]");
}

/// Show SSTable metadata statistics.
pub fn show_metadata(file: &str) {
    let path = Path::new(file);
    if !path.exists() {
        eprintln!("Error: file not found: {}", file);
        return;
    }

    let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);

    let desc = match parse_descriptor(file) {
        Some(d) => d,
        None => {
            eprintln!("Error: invalid SSTable filename format: {}", file);
            return;
        }
    };

    println!("SSTable: {}", desc.file_prefix());
    println!("Format: {:?}", desc.format);
    println!("Generation: {}", desc.generation);
    println!("Size: {} bytes", size);
    println!();
    println!("Metadata:");

    match SSTableReader::open(desc) {
        Ok(reader) => {
            if let Some(stats) = reader.stats() {
                println!("  Estimated partitions : {}", stats.partition_count);
                println!("  Estimated cells      : {}", stats.cell_count);
                println!("  Estimated rows       : {}", stats.row_count);
                println!("  Min timestamp        : {}", stats.min_timestamp);
                println!("  Max timestamp        : {}", stats.max_timestamp);
                println!("  Data size            : {}", stats.data_size);
                println!("  Index size           : {}", stats.index_size);
            } else {
                println!("  (No Statistics.db available)");
            }
        }
        Err(e) => {
            eprintln!("  Error reading SSTable metadata: {}", e);
        }
    }
}
