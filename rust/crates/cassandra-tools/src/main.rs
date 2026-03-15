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
//! ```

mod sstable_tools;

use clap::{Parser, Subcommand};
use cassandra_common::version::version_string;

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
            println!("--  Address    Load       Tokens  Owns  Host ID                               Rack");
            // TODO: Fetch from admin API at {base_url}/api/v1/virtual/system_views/local
            println!("UN  127.0.0.1  ?          ?       ?     ?                                     rack1");
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
            println!("Key Cache              : entries 0, size 0 bytes, capacity 0 bytes, hits 0, requests 0");
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
            let snap_name = name.unwrap_or_else(|| {
                format!("snapshot-{}", chrono_like_timestamp())
            });
            println!("Requested snapshot '{}' for keyspaces: {:?}", snap_name, keyspaces);
            // TODO: POST to /api/v1/operations/snapshot
            println!("Snapshot directory: data/snapshots/{}", snap_name);
            println!("(stub — implement via admin API)");
        }
        Commands::Listsnapshots => {
            println!("Snapshot name    Keyspace   Column family   True size   Size on disk");
            // TODO: GET from /api/v1/operations/snapshots
            println!("(no snapshots found — connect to {} for live data)", base_url);
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
        Commands::Repair { keyspace } => {
            match keyspace {
                Some(ks) => println!("Repairing keyspace {}...", ks),
                None => println!("Repairing all keyspaces..."),
            }
            println!("(stub — implement via admin API)");
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
            // TODO: POST to /api/v1/operations/enableauditlog
            println!("(stub — implement via admin API)");
        }
        Commands::Disableauditlog => {
            println!("Disabling audit logging...");
            println!("(stub — implement via admin API)");
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
