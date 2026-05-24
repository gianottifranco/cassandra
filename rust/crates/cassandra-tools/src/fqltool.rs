// Licensed under Apache License, Version 2.0.

//! `fqltool` — Full Query Log tool.
//!
//! Reads binary FQL log files produced by `cassandra-server` and dumps
//! records as human-readable JSON lines.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.tools.fqltool.FQLQuery`
//! - `org.apache.cassandra.tools.fqltool.Dump`
//!
//! ## Usage
//! ```text
//! fqltool dump <FQL_LOG_FILE>
//! fqltool dump --json <FQL_LOG_FILE>
//! ```

use cassandra_security::fql::FqlReader;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

/// FQL Tool — inspect Full Query Log binary files.
#[derive(Parser)]
#[command(name = "fqltool", version, about = "Full Query Log inspection tool")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Dump all records from an FQL binary log file.
    Dump {
        /// Path to the FQL binary log file.
        path: PathBuf,

        /// Output as JSON lines (default: human-readable table).
        #[arg(long)]
        json: bool,
    },

    /// Print summary statistics for an FQL log file.
    Stats {
        /// Path to the FQL binary log file.
        path: PathBuf,
    },
}

fn run(cli: Cli) -> anyhow::Result<()> {
    match cli.command {
        Commands::Dump { path, json } => {
            let records = FqlReader::read_all(&path)?;

            if json {
                for record in &records {
                    let j = serde_json::to_string(record)?;
                    println!("{}", j);
                }
            } else {
                println!("{:<26} {:<6} QUERY", "TIMESTAMP_MICROS", "CL");
                println!("{}", "-".repeat(80));
                for record in &records {
                    println!(
                        "{:<26} {:<6} {}",
                        record.timestamp_micros, record.consistency_level, record.query,
                    );
                }
            }

            println!("\n--- {} record(s) ---", records.len());
        }

        Commands::Stats { path } => {
            let records = FqlReader::read_all(&path)?;
            let total = records.len();
            let min_ts = records
                .iter()
                .map(|r| r.timestamp_micros)
                .min()
                .unwrap_or(0);
            let max_ts = records
                .iter()
                .map(|r| r.timestamp_micros)
                .max()
                .unwrap_or(0);

            let unique_queries: std::collections::HashSet<&str> =
                records.iter().map(|r| r.query.as_str()).collect();

            let total_bind_values: usize = records.iter().map(|r| r.bind_values.len()).sum();

            println!("FQL Stats for: {}", path.display());
            println!("  Total records:       {}", total);
            println!("  Unique queries:      {}", unique_queries.len());
            println!("  Total bind values:   {}", total_bind_values);
            if total > 0 {
                println!(
                    "  Time range:          {} → {} ({} µs span)",
                    min_ts,
                    max_ts,
                    max_ts - min_ts
                );
            }
        }
    }

    Ok(())
}

pub fn run_with_args(args: &[String]) -> anyhow::Result<()> {
    let cli =
        Cli::try_parse_from(std::iter::once("fqltool".to_string()).chain(args.iter().cloned()))?;
    run(cli)
}

#[allow(dead_code)]
fn main() -> anyhow::Result<()> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    run_with_args(&args)
}
