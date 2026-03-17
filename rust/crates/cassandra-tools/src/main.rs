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

//! # cassandra-tools
//!
//! CLI tools providing nodetool-equivalent functionality, SSTable utilities,
//! and operational administration commands.
//!
//! ## Usage
//! ```text
//! cassandra-tools status              Show ring and node states
//! cassandra-tools info                Show node information
//! cassandra-tools version             Show server version
//! cassandra-tools ring                Show token ring
//! cassandra-tools describecluster     Show cluster description
//! cassandra-tools snapshot            Take a snapshot
//! cassandra-tools listsnapshots       List available snapshots
//! cassandra-tools clearsnapshot       Clear a snapshot
//! cassandra-tools compact             Force compaction
//! cassandra-tools repair              Run repair
//! cassandra-tools cleanup             Run cleanup
//! cassandra-tools enableauditlog      Enable audit logging
//! cassandra-tools disableauditlog     Disable audit logging
//! cassandra-tools getlogginglevels    Show logging levels
//! cassandra-tools setlogginglevel     Set a logging level
//! cassandra-tools sstabledump         Dump SSTable contents
//! cassandra-tools sstablemetadata     Show SSTable metadata
//! cassandra-tools sstableverify       Verify SSTable integrity
//! cassandra-tools sstablescrub        Scrub an SSTable (recover valid data)
//! cassandra-tools sstableupgrade      Upgrade SSTable to current format
//! cassandra-tools rebuild_index       A full rebuild of native secondary indexes for a given table
//! ```

mod sstable_scrub;
mod sstable_tools;
mod sstable_upgrade;
mod sstable_verify;

use cassandra_common::version::version_string;
use clap::{Parser, Subcommand};

/// Cassandra administration and operations CLI — Rust implementation.
///
/// Provides nodetool-equivalent functionality plus SSTable utilities.
/// Connects to the Cassandra admin HTTP API for cluster operations.
#[derive(Parser)]
#[command(name = "cassandra-tools")]
#[command(version, about, long_about = None)]
struct Cli {
    /// Admin API host (default: 127.0.0.1).
    #[arg(long, default_value = "127.0.0.1")]
    host: String,

    /// Admin API port (default: 9090).
    #[arg(long, default_value_t = 9090)]
    port: u16,

    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Show ring status and node states.
    Status,
    /// Show local node information.
    Info,
    /// Show server version.
    Version,
    /// Show token ring.
    Ring,
    /// Describe the cluster.
    Describecluster,
    /// Take a snapshot of keyspaces/tables.
    Snapshot {
        /// Snapshot name.
        #[arg(long)]
        name: Option<String>,
        /// Keyspace(s) to snapshot.
        keyspaces: Vec<String>,
    },
    /// List available snapshots.
    Listsnapshots,
    /// Clear a snapshot.
    Clearsnapshot {
        /// Snapshot name to clear.
        #[arg(long)]
        name: Option<String>,
    },
    /// Force compaction on keyspace/table.
    Compact {
        /// Keyspace.
        keyspace: Option<String>,
        /// Table.
        table: Option<String>,
    },
    /// Run repair on a keyspace.
    Repair {
        /// Keyspace.
        keyspace: Option<String>,
        /// Tables to repair.
        tables: Vec<String>,
        /// Full repair.
        #[arg(long, default_value_t = false)]
        full: bool,
        /// Incremental repair.
        #[arg(long, default_value_t = false)]
        incremental: bool,
        /// Preview repair.
        #[arg(long, default_value_t = false)]
        preview: bool,
    },
    /// Run cleanup (remove data not belonging to this node).
    Cleanup {
        /// Keyspace.
        keyspace: Option<String>,
    },
    /// Enable audit logging.
    Enableauditlog,
    /// Disable audit logging.
    Disableauditlog,
    /// Enable full query logging.
    Enablefql {
        /// FQL log directory.
        #[arg(long, default_value = "logs/fql")]
        log_dir: String,
    },
    /// Disable full query logging.
    Disablefql,
    /// Show current logging levels.
    Getlogginglevels,
    /// Set a logging level.
    Setlogginglevel {
        /// Logger name (class or package).
        logger: String,
        /// Level: TRACE, DEBUG, INFO, WARN, ERROR.
        level: String,
    },
    /// Dump the contents of an SSTable.
    Sstabledump {
        /// Path to the SSTable data file.
        file: String,
    },
    /// Show SSTable metadata.
    Sstablemetadata {
        /// Path to the SSTable.
        file: String,
    },
    /// Verify SSTable integrity (magic bytes, CRC, index order, bloom filter).
    Sstableverify {
        /// Path to the SSTable data file.
        file: String,
    },
    /// Scrub an SSTable: read valid partitions and write to a new SSTable.
    Sstablescrub {
        /// Path to the SSTable data file.
        file: String,
        /// Output directory (defaults to same directory, generation+1).
        #[arg(long)]
        output_dir: Option<String>,
    },
    /// Upgrade an SSTable by rewriting it in the current format.
    Sstableupgrade {
        /// Path to the SSTable data file.
        file: String,
    },
    /// Bulk load SSTables to a cluster.
    Sstableloader {
        /// Path to the SSTables directory.
        dir: String,
    },
    /// View audit logs.
    Auditlogviewer {
        /// Directory or files to view.
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Full query log tool (fqltool).
    Fqltool {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Cassandra stress testing tool.
    CassandraStress {
        #[arg(trailing_var_arg = true, allow_hyphen_values = true)]
        args: Vec<String>,
    },
    /// Rebuild a native secondary index.
    #[command(name = "rebuild_index")]
    RebuildIndex {
        /// Keyspace.
        keyspace: String,
        /// Table.
        table: String,
        /// Index names (comma separated).
        index_names: String,
    },

    // ── Topology operations ─────────────────────────────────────────
    /// Decommission this node from the cluster.
    Decommission,
    /// Remove a dead node from the cluster.
    Removenode {
        /// Host ID of the node to remove.
        host_id: String,
    },
    /// Move this node to a new token.
    Move {
        /// New token value.
        new_token: String,
    },
    /// Rebuild data from another datacenter.
    Rebuild {
        /// Source datacenter (optional; rebuild from all if omitted).
        #[arg(long)]
        source_dc: Option<String>,
    },
    /// Refresh (load new SSTables for a table).
    Refresh {
        /// Keyspace.
        keyspace: String,
        /// Table.
        table: String,
    },
    /// Show network streaming statistics.
    Netstats,
    /// Show current topology operation status.
    Topologystatus,
}

fn main() {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    let base_url = format!("http://{}:{}", cli.host, cli.port);

    match cli.command {
        Commands::Version => {
            println!("cassandra-tools {}", version_string());
            println!("Rust implementation — Apache Cassandra rewrite");
        }
        Commands::Status => {
            println!("Datacenter: datacenter1");
            println!("=======================");
            println!("Status=Up/Down");
            println!("|/ State=Normal/Leaving/Joining/Moving");
            println!(
                "--  Address    Load       Tokens  Owns  Host ID                               Rack"
            );
            // TODO: Fetch from admin API at {base_url}/api/v1/virtual/system_views/local
            println!(
                "UN  127.0.0.1  ?          ?       ?     ?                                     rack1"
            );
            println!();
            println!("(connect to admin API at {} for live data)", base_url);
        }
        Commands::Info => {
            println!("{}", version_string());
            // TODO: Fetch from /api/v1/virtual/system_views/local
            println!("ID                     : (connect to {})", base_url);
            println!("Gossip active          : true");
            println!("Native Transport active: true");
            println!("Load                   : ?");
            println!("Generation No          : 1");
            println!("Uptime (seconds)       : ?");
            println!("Heap Memory (MB)       : N/A (Rust)");
            println!("Data Center            : datacenter1");
            println!("Rack                   : rack1");
            println!("Exceptions             : 0");
            println!(
                "Key Cache              : entries 0, size 0 bytes, capacity 0 bytes, hits 0, requests 0"
            );
        }
        Commands::Ring => {
            println!("Address     Rack        Status  State   Load    Owns    Token");
            // TODO: Fetch from admin API
            println!("127.0.0.1   rack1       Up      Normal  ?       ?       (no token data)");
            println!();
            println!("(connect to admin API at {} for live data)", base_url);
        }
        Commands::Describecluster => {
            println!("Cluster Information:");
            println!("\tName: Test Cluster");
            println!("\tSnitch: org.apache.cassandra.locator.SimpleSnitch");
            println!("\tDynamicEndPointSnitch: enabled");
            println!("\tPartitioner: org.apache.cassandra.dht.Murmur3Partitioner");
            println!("\tSchema versions:");
            println!("\t\t(connect to {} for live data)", base_url);
        }
        Commands::Snapshot { name, keyspaces } => {
            let snap_name = name.unwrap_or_else(|| format!("snapshot-{}", chrono_like_timestamp()));
            println!(
                "Requested snapshot '{}' for keyspaces: {:?}",
                snap_name, keyspaces
            );
            // TODO: POST to /api/v1/operations/snapshot
            println!("Snapshot directory: data/snapshots/{}", snap_name);
            println!("(stub — implement via admin API)");
        }
        Commands::Listsnapshots => {
            println!("Snapshot name    Keyspace   Column family   True size   Size on disk");
            // TODO: GET from /api/v1/operations/snapshots
            println!(
                "(no snapshots found — connect to {} for live data)",
                base_url
            );
        }
        Commands::Clearsnapshot { name } => {
            match name {
                Some(n) => println!("Clearing snapshot '{}'...", n),
                None => println!("Clearing all snapshots..."),
            }
            println!("(stub — implement via admin API)");
        }
        Commands::Compact { keyspace, table } => {
            match (keyspace, table) {
                (Some(ks), Some(tbl)) => println!("Compacting {}.{}...", ks, tbl),
                (Some(ks), None) => println!("Compacting keyspace {}...", ks),
                _ => println!("Compacting all keyspaces..."),
            }
            // TODO: POST to /api/v1/operations/compact
            println!("(stub — implement via admin API)");
        }
        Commands::Repair {
            keyspace,
            tables,
            full,
            incremental,
            preview,
        } => {
            let ks = keyspace.unwrap_or_else(|| "system_distributed".to_string());
            let is_full = full || (!incremental && !preview);

            println!(
                "Starting {}repair on keyspace {}...",
                if preview {
                    "preview "
                } else if is_full {
                    "full "
                } else {
                    "incremental "
                },
                ks
            );

            let payload = serde_json::json!({
                "keyspace": ks,
                "tables": tables,
                "full": is_full,
                "preview": preview,
            });

            let client = reqwest::blocking::Client::new();
            let url = format!("{}/api/v1/operations/repair", base_url);

            match client.post(&url).json(&payload).send() {
                Ok(resp) => {
                    if resp.status().is_success() {
                        if let Ok(json) = resp.json::<serde_json::Value>() {
                            println!(
                                "Repair started successfully. Session ID: {}",
                                json["repair_id"]
                            );
                        } else {
                            println!("Repair started successfully.");
                        }
                    } else {
                        println!("Failed to start repair. Status: {}", resp.status());
                        if let Ok(err_text) = resp.text() {
                            println!("Error: {}", err_text);
                        }
                    }
                }
                Err(e) => {
                    println!("Failed to connect to admin API: {}", e);
                }
            }
        }
        Commands::Cleanup { keyspace } => {
            match keyspace {
                Some(ks) => println!("Cleaning up keyspace {}...", ks),
                None => println!("Cleaning up all keyspaces..."),
            }
            println!("(stub — implement via admin API)");
        }
        Commands::Enableauditlog => {
            println!("Enabling audit logging...");
            let client = reqwest::blocking::Client::new();
            let url = format!("{}/api/v1/operations/enableauditlog", base_url);
            match client.post(&url).send() {
                Ok(resp) if resp.status().is_success() => println!("Audit logging enabled."),
                Ok(resp) => println!("Failed: {}", resp.status()),
                Err(e) => println!("Failed to connect to admin API: {}", e),
            }
        }
        Commands::Disableauditlog => {
            println!("Disabling audit logging...");
            let client = reqwest::blocking::Client::new();
            let url = format!("{}/api/v1/operations/disableauditlog", base_url);
            match client.post(&url).send() {
                Ok(resp) if resp.status().is_success() => println!("Audit logging disabled."),
                Ok(resp) => println!("Failed: {}", resp.status()),
                Err(e) => println!("Failed to connect to admin API: {}", e),
            }
        }
        Commands::Enablefql { log_dir } => {
            println!("Enabling full query logging to {}...", log_dir);
            let client = reqwest::blocking::Client::new();
            let url = format!("{}/api/v1/operations/enablefql", base_url);
            let payload = serde_json::json!({ "log_dir": log_dir });
            match client.post(&url).json(&payload).send() {
                Ok(resp) if resp.status().is_success() => println!("FQL enabled."),
                Ok(resp) => println!("Failed: {}", resp.status()),
                Err(e) => println!("Failed to connect to admin API: {}", e),
            }
        }
        Commands::Disablefql => {
            println!("Disabling full query logging...");
            let client = reqwest::blocking::Client::new();
            let url = format!("{}/api/v1/operations/disablefql", base_url);
            match client.post(&url).send() {
                Ok(resp) if resp.status().is_success() => println!("FQL disabled."),
                Ok(resp) => println!("Failed: {}", resp.status()),
                Err(e) => println!("Failed to connect to admin API: {}", e),
            }
        }
        Commands::Getlogginglevels => {
            println!("Logger Name           Log Level");
            println!("ROOT                  INFO");
            println!("(stub — connect to {} for live data)", base_url);
        }
        Commands::Setlogginglevel { logger, level } => {
            println!("Setting {} to {}...", logger, level);
            println!("(stub — implement via admin API)");
        }
        Commands::Sstabledump { file } => {
            sstable_tools::dump_sstable(&file);
        }
        Commands::Sstablemetadata { file } => {
            sstable_tools::show_metadata(&file);
        }
        Commands::Sstableverify { file } => {
            sstable_verify::run(&file);
        }
        Commands::Sstablescrub { file, output_dir } => {
            sstable_scrub::run(&file, output_dir.as_deref());
        }
        Commands::Sstableupgrade { file } => {
            sstable_upgrade::run(&file);
        }
        Commands::Sstableloader { dir } => {
            println!("Loading SSTables from {}...", dir);
            // TODO: POST to /api/v1/operations/sstableloader
            println!("(stub — implement via admin API)");
        }
        Commands::Auditlogviewer { args } => {
            println!("Auditlogviewer: {:?}", args);
            // TODO: read audit logs
            println!("(stub — implemented in cassandra-tools)");
        }
        Commands::Fqltool { args } => {
            println!("fqltool: {:?}", args);
            // TODO: read or manipulate FQL
            println!("(stub — implemented in cassandra-tools)");
        }
        Commands::CassandraStress { args } => {
            println!("cassandra-stress: {:?}", args);
            // TODO: load generation and stress testing
            println!("(stub — implemented in cassandra-tools)");
        }
        Commands::RebuildIndex {
            keyspace,
            table,
            index_names,
        } => {
            println!(
                "Rebuilding indexes: {} on {}.{}...",
                index_names, keyspace, table
            );
            for idx_name in index_names.split(',') {
                let idx_name = idx_name.trim();
                let payload = serde_json::json!({
                    "keyspace": keyspace,
                    "table": table,
                    "index_name": idx_name,
                });
                let client = reqwest::blocking::Client::new();
                let url = format!("{}/api/v1/operations/rebuild_index", base_url);
                match client.post(&url).json(&payload).send() {
                    Ok(resp) => {
                        if resp.status().is_success() {
                            println!("Index {} rebuilt successfully.", idx_name);
                        } else {
                            println!("Failed to rebuild {}. Status: {}", idx_name, resp.status());
                            if let Ok(err) = resp.text() {
                                println!("Error: {}", err);
                            }
                        }
                    }
                    Err(e) => println!("Failed to connect to admin API: {}", e),
                }
            }
        }

        // ── Topology operation commands ──────────────────────────────
        Commands::Decommission => {
            println!("Decommissioning node...");
            println!("POST {}/api/v1/topology/decommission", base_url);
            println!("Mode: DECOMMISSIONED");
            // TODO: Actually POST to admin API and poll for completion
            println!("(stub — connect to admin API for live operation)");
        }
        Commands::Removenode { host_id } => {
            println!("Removing node with Host ID: {}", host_id);
            println!("POST {}/api/v1/topology/removenode", base_url);
            // TODO: Validate host_id format, POST to admin API
            println!("(stub — connect to admin API for live operation)");
        }
        Commands::Move { new_token } => {
            println!("Moving node to new token: {}", new_token);
            println!("POST {}/api/v1/topology/move", base_url);
            // TODO: Parse token, POST to admin API
            println!("(stub — connect to admin API for live operation)");
        }
        Commands::Rebuild { source_dc } => {
            match &source_dc {
                Some(dc) => println!("Rebuilding from datacenter: {}", dc),
                None => println!("Rebuilding from all datacenters..."),
            }
            println!("POST {}/api/v1/topology/rebuild", base_url);
            // TODO: POST to admin API with optional source_dc
            println!("(stub — connect to admin API for live operation)");
        }
        Commands::Refresh { keyspace, table } => {
            println!("Refreshing {}.{}...", keyspace, table);
            println!("POST {}/api/v1/topology/refresh", base_url);
            // TODO: POST to admin API
            println!("(stub — connect to admin API for live operation)");
        }
        Commands::Netstats => {
            println!("Mode: NORMAL");
            println!("Not sending any streams.");
            println!("Read Repair Statistics:");
            println!("Attempted: 0");
            println!("Mismatch (Blocking): 0");
            println!("Mismatch (Background): 0");
            println!("Pool Name    Active  Pending  Completed  Dropped");
            println!("Large messages  0       0        0          0");
            println!("Small messages  0       0        0          0");
            println!("Gossip messages 0       0        0          0");
            println!();
            println!(
                "(connect to admin API at {}/api/v1/streaming/sessions for live data)",
                base_url
            );
        }
        Commands::Topologystatus => {
            println!("Current topology operation status:");
            println!("Operation: IDLE");
            println!("Epoch: 0");
            println!("Pending ranges: 0");
            println!();
            println!(
                "(connect to admin API at {}/api/v1/topology/status for live data)",
                base_url
            );
        }
    }
}

fn chrono_like_timestamp() -> String {
    use std::time::SystemTime;
    let ts = SystemTime::now()
        .duration_since(SystemTime::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    format!("{}", ts)
}
