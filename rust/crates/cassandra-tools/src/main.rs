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
//! cassandra-tools rebuild_index       Rebuild native secondary indexes
//! cassandra-tools sstablesplit        Split large SSTables
//! cassandra-tools sstablelevelreset   Reset compaction level
//! cassandra-tools sstablerepairedset  Set repaired-at timestamp
//! cassandra-tools sstableexpiredblockers  Find SSTables blocking tombstone GC
//! cassandra-tools sstableofflinerelevel   Reassign SSTable levels for LCS
//! cassandra-tools sstablepartitions   Analyze partition size distribution
//! cassandra-tools bootstrapmonitor    Monitor bootstrap progress
//! cassandra-tools generatetokens      Generate tokens for cluster
//! cassandra-tools hashpassword        Hash a password with bcrypt
//! ```

// Existing SSTable modules
mod sstable_scrub;
mod sstable_tools;
mod sstable_upgrade;
mod sstable_verify;

// Admin client
mod admin_client;

// CLI command modules (Units 2-9)
mod cmd_cache_hints;
mod cmd_cluster_info;
mod cmd_compaction;
mod cmd_config;
mod cmd_logging_security;
mod cmd_snapshots;
mod cmd_stats;
mod cmd_topology;

// New offline SSTable tools (Unit 10)
mod sstable_expired_blockers;
mod sstable_level_reset;
mod sstable_offline_relevel;
mod sstable_partitions;
mod sstable_repaired_at;
mod sstable_split;

// Utility tools (Unit 11)
mod bootstrap_monitor;
mod generate_tokens;
mod hash_password;

use admin_client::AdminClient;
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
    // ── Cluster info (Unit 2) ─────────────────────────────────────
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
    /// Show gossip information for all nodes.
    Gossipinfo,

    // ── Compaction (Unit 3) ───────────────────────────────────────
    /// Force compaction on keyspace/table.
    Compact {
        /// Keyspace.
        keyspace: Option<String>,
        /// Table.
        table: Option<String>,
    },
    /// Run cleanup (remove data not belonging to this node).
    Cleanup {
        /// Keyspace.
        keyspace: Option<String>,
    },
    /// Flush memtables to SSTables.
    Flush {
        /// Keyspace.
        keyspace: Option<String>,
        /// Table.
        table: Option<String>,
    },
    /// Scrub keyspace/table via admin API.
    Scrub {
        /// Keyspace.
        keyspace: Option<String>,
        /// Table.
        table: Option<String>,
    },
    /// Show compaction statistics.
    Compactionstats,
    /// Show compaction history.
    Compactionhistory,
    /// Enable auto-compaction.
    Enableautocompaction,
    /// Disable auto-compaction.
    Disableautocompaction,
    /// Show auto-compaction status.
    Statusautocompaction,
    /// Get compaction throughput.
    Getcompactionthroughput,
    /// Set compaction throughput.
    Setcompactionthroughput {
        /// Throughput in MB/s.
        throughput_mb: u32,
    },
    /// Force compaction for specific partitions.
    Forcecompact {
        /// Keyspace.
        keyspace: String,
        /// Table.
        table: String,
    },
    /// Garbage collect tombstones.
    Garbagecollect {
        /// Keyspace.
        keyspace: Option<String>,
    },

    // ── Snapshots (Unit 4) ────────────────────────────────────────
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
    /// Import SSTables.
    Import {
        /// Keyspace.
        keyspace: String,
        /// Table.
        table: String,
        /// Directory with SSTables.
        directory: String,
    },
    /// Enable incremental backup.
    Enablebackup,
    /// Disable incremental backup.
    Disablebackup,
    /// Show backup status.
    Statusbackup,

    // ── Topology (Unit 5) ─────────────────────────────────────────
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
    /// Join the ring.
    Join,
    /// Resume bootstrap.
    Bootstrapresume,
    /// Abort bootstrap.
    Abortbootstrap,
    /// Assassinate a node endpoint.
    Assassinate {
        /// Endpoint address.
        endpoint: String,
    },
    /// Drain the node (stop accepting writes, flush).
    Drain,
    /// Stop the Cassandra daemon.
    Stopdaemon,
    /// Show network streaming statistics.
    Netstats,
    /// Show current topology operation status.
    Topologystatus,

    // ── Statistics (Unit 6) ───────────────────────────────────────
    /// Show table statistics.
    Tablestats {
        /// Keyspace (optional).
        keyspace: Option<String>,
    },
    /// Show table histograms.
    Tablehistograms {
        /// Keyspace.
        keyspace: Option<String>,
        /// Table.
        table: Option<String>,
    },
    /// Show thread pool statistics.
    Tpstats,
    /// Show GC statistics.
    Gcstats,
    /// Show coordinator read/write latency percentiles.
    Proxyhistograms,
    /// Show connected client statistics.
    Clientstats,
    /// Show top partitions.
    Toppartitions {
        /// Keyspace.
        keyspace: Option<String>,
        /// Table.
        table: Option<String>,
        /// Duration in milliseconds.
        #[arg(long, default_value_t = 5000)]
        duration_ms: u64,
    },
    /// Show failure detector information.
    Failuredetectorinfo,
    /// Show data file paths.
    Datapaths,
    /// Refresh size estimates.
    Refreshsizeestimates,
    /// Show materialized view build status.
    Viewbuildstatus,
    /// Get endpoints for a partition key.
    Getendpoints {
        /// Keyspace.
        keyspace: String,
        /// Table.
        table: String,
        /// Partition key.
        key: String,
    },
    /// Get SSTables for a partition key.
    Getsstables {
        /// Keyspace.
        keyspace: String,
        /// Table.
        table: String,
        /// Partition key.
        key: String,
    },

    // ── Cache & Hints (Unit 7) ────────────────────────────────────
    /// Invalidate key cache.
    Invalidatekeycache,
    /// Invalidate row cache.
    Invalidaterowcache,
    /// Invalidate counter cache.
    Invalidatecountercache,
    /// Invalidate credentials cache.
    Invalidatecredentialscache,
    /// Invalidate permissions cache.
    Invalidatepermissionscache,
    /// Invalidate roles cache.
    Invalidaterolescache,
    /// Set cache capacity.
    Setcachecapacity {
        /// Cache type (key, row, counter).
        cache_type: String,
        /// Capacity in MB.
        capacity_mb: u32,
    },
    /// Enable hinted handoff.
    Enablehandoff,
    /// Disable hinted handoff.
    Disablehandoff,
    /// Pause hinted handoff.
    Pausehandoff,
    /// Resume hinted handoff.
    Resumehandoff,
    /// Truncate all hints.
    Truncatehints,
    /// List pending hints.
    Listpendinghints,

    // ── Config (Unit 8) ───────────────────────────────────────────
    /// Get a configuration value.
    Getconfig {
        /// Configuration key.
        key: Option<String>,
    },
    /// Set a configuration value.
    Setconfig {
        /// Configuration key.
        key: String,
        /// Configuration value.
        value: String,
    },
    /// Get request timeout.
    Gettimeout {
        /// Timeout type (read, write, range, counter, truncate, misc).
        timeout_type: String,
    },
    /// Set request timeout.
    Settimeout {
        /// Timeout type.
        timeout_type: String,
        /// Timeout in milliseconds.
        timeout_ms: u64,
    },
    /// Get streaming throughput.
    Getstreamingthroughput,
    /// Set streaming throughput.
    Setstreamingthroughput {
        /// Throughput in MB/s.
        throughput_mb: u32,
    },
    /// Get inter-datacenter streaming throughput.
    Getinterdcstreamthroughput,
    /// Set inter-datacenter streaming throughput.
    Setinterdcstreamthroughput {
        /// Throughput in MB/s.
        throughput_mb: u32,
    },
    /// Get concurrent compactors.
    Getconcurrentcompactors,
    /// Set concurrent compactors.
    Setconcurrentcompactors {
        /// Number of concurrent compactors.
        value: u32,
    },
    /// Reload local node schema.
    Reloadlocalschema,
    /// Reload triggers.
    Reloadtriggers,
    /// Reload SSL certificates.
    Reloadssl,
    /// Enable native transport (CQL).
    Enablebinary,
    /// Disable native transport (CQL).
    Disablebinary,
    /// Show native transport status.
    Statusbinary,
    /// Enable gossip.
    Enablegossip,
    /// Disable gossip.
    Disablegossip,
    /// Show gossip status.
    Statusgossip,
    /// Get seed nodes.
    Getseeds,

    // ── Logging & Security (Unit 9) ───────────────────────────────
    /// Show current logging levels.
    Getlogginglevels,
    /// Set a logging level.
    Setlogginglevel {
        /// Logger name (class or package).
        logger: String,
        /// Level: TRACE, DEBUG, INFO, WARN, ERROR.
        level: String,
    },
    /// Enable audit logging.
    Enableauditlog,
    /// Disable audit logging.
    Disableauditlog,
    /// Get audit log configuration.
    Getauditlogconfig,
    /// Enable full query logging.
    Enablefql {
        /// FQL log directory.
        #[arg(long, default_value = "logs/fql")]
        log_dir: String,
    },
    /// Disable full query logging.
    Disablefql,
    /// Get FQL configuration.
    Getfqlconfig,
    /// Reset FQL.
    Resetfql,
    /// Get trace probability.
    Gettraceprobability,
    /// Set trace probability.
    Settraceprobability {
        /// Probability (0.0 to 1.0).
        probability: f64,
    },

    // ── Repair ────────────────────────────────────────────────────
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

    // ── SSTable tools ─────────────────────────────────────────────
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
    /// Split large SSTables by target size.
    Sstablesplit {
        /// Path to the SSTable data file.
        file: String,
        /// Target size in MB (default: 50).
        #[arg(long, default_value_t = 50)]
        size_mb: u64,
    },
    /// Reset compaction level to 0 in SSTable metadata.
    Sstablelevelreset {
        /// Path to the SSTable data file.
        file: String,
    },
    /// Set or clear the repaired-at timestamp.
    Sstablerepairedset {
        /// Path to the SSTable data file.
        file: String,
        /// Repaired-at timestamp (0 to clear).
        #[arg(long, default_value_t = 0)]
        repaired_at: i64,
    },
    /// Identify SSTables blocking tombstone garbage collection.
    Sstableexpiredblockers {
        /// Path to the SSTable directory or data file.
        file: String,
        /// GC grace seconds.
        #[arg(long, default_value_t = 864000)]
        gc_grace_seconds: u64,
    },
    /// Reassign SSTable levels for Leveled Compaction Strategy offline.
    Sstableofflinerelevel {
        /// Path to the SSTable directory.
        dir: String,
    },
    /// Analyze partition size distribution in an SSTable.
    Sstablepartitions {
        /// Path to the SSTable data file.
        file: String,
        /// Show top N partitions.
        #[arg(long, default_value_t = 10)]
        top: usize,
    },

    // ── Utility tools (Unit 11) ───────────────────────────────────
    /// Bulk load SSTables to a cluster.
    Sstableloader {
        /// Path to the SSTables directory.
        dir: String,
        /// Target node addresses.
        #[arg(long)]
        nodes: Option<String>,
    },
    /// Monitor bootstrap progress.
    Bootstrapmonitor,
    /// Generate tokens for a cluster.
    Generatetokens {
        /// Number of nodes.
        #[arg(long)]
        nodes: u32,
        /// Number of vnodes per node (default: 256).
        #[arg(long, default_value_t = 256)]
        tokens: u32,
        /// Number of racks (default: 1).
        #[arg(long, default_value_t = 1)]
        racks: u32,
    },
    /// Hash a password using bcrypt.
    Hashpassword {
        /// Password to hash (reads from stdin if omitted).
        #[arg(long)]
        password: Option<String>,
        /// bcrypt rounds (default: 10).
        #[arg(long, default_value_t = 10)]
        rounds: u32,
    },

    // ── Existing pass-through tools ───────────────────────────────
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
}

fn main() {
    tracing_subscriber::fmt::init();

    let cli = Cli::parse();
    let client = AdminClient::new(&cli.host, cli.port);

    match cli.command {
        // ── Cluster info ──────────────────────────────────────────
        Commands::Version => {
            println!("cassandra-tools {}", version_string());
            println!("Rust implementation — Apache Cassandra rewrite");
        }
        Commands::Status => cmd_cluster_info::status(&client),
        Commands::Info => cmd_cluster_info::info(&client),
        Commands::Ring => cmd_cluster_info::ring(&client),
        Commands::Describecluster => cmd_cluster_info::describe_cluster(&client),
        Commands::Gossipinfo => cmd_cluster_info::gossip_info(&client),

        // ── Compaction ────────────────────────────────────────────
        Commands::Compact { keyspace, table } => {
            cmd_compaction::compact(&client, keyspace.as_deref(), table.as_deref())
        }
        Commands::Cleanup { keyspace } => cmd_compaction::cleanup(&client, keyspace.as_deref()),
        Commands::Flush { keyspace, table } => {
            cmd_compaction::flush(&client, keyspace.as_deref(), table.as_deref())
        }
        Commands::Scrub { keyspace, table } => {
            cmd_compaction::scrub(&client, keyspace.as_deref(), table.as_deref())
        }
        Commands::Compactionstats => cmd_compaction::compaction_stats(&client),
        Commands::Compactionhistory => cmd_compaction::compaction_history(&client),
        Commands::Enableautocompaction => cmd_compaction::enable_autocompaction(&client),
        Commands::Disableautocompaction => cmd_compaction::disable_autocompaction(&client),
        Commands::Statusautocompaction => cmd_compaction::status_autocompaction(&client),
        Commands::Getcompactionthroughput => cmd_compaction::get_compaction_throughput(&client),
        Commands::Setcompactionthroughput { throughput_mb } => {
            cmd_compaction::set_compaction_throughput(&client, throughput_mb)
        }
        Commands::Forcecompact { keyspace, table } => {
            cmd_compaction::force_compact(&client, &keyspace, &table)
        }
        Commands::Garbagecollect { keyspace } => {
            cmd_compaction::garbage_collect(&client, keyspace.as_deref())
        }

        // ── Snapshots ─────────────────────────────────────────────
        Commands::Snapshot { name, keyspaces } => {
            cmd_snapshots::snapshot(&client, name, &keyspaces)
        }
        Commands::Listsnapshots => cmd_snapshots::list_snapshots(&client),
        Commands::Clearsnapshot { name } => cmd_snapshots::clear_snapshot(&client, name),
        Commands::Import {
            keyspace,
            table,
            directory,
        } => cmd_snapshots::import(&client, &keyspace, &table, &directory),
        Commands::Enablebackup => cmd_snapshots::enable_backup(&client),
        Commands::Disablebackup => cmd_snapshots::disable_backup(&client),
        Commands::Statusbackup => cmd_snapshots::status_backup(&client),

        // ── Topology ──────────────────────────────────────────────
        Commands::Decommission => cmd_topology::decommission(&client),
        Commands::Removenode { host_id } => cmd_topology::removenode(&client, &host_id),
        Commands::Move { new_token } => cmd_topology::move_token(&client, &new_token),
        Commands::Rebuild { source_dc } => cmd_topology::rebuild(&client, source_dc.as_deref()),
        Commands::Refresh { keyspace, table } => cmd_topology::refresh(&client, &keyspace, &table),
        Commands::Join => cmd_topology::join(&client),
        Commands::Bootstrapresume => cmd_topology::bootstrap_resume(&client),
        Commands::Abortbootstrap => cmd_topology::abort_bootstrap(&client),
        Commands::Assassinate { endpoint } => cmd_topology::assassinate(&client, &endpoint),
        Commands::Drain => cmd_topology::drain(&client),
        Commands::Stopdaemon => cmd_topology::stop_daemon(&client),
        Commands::Netstats => cmd_topology::netstats(&client),
        Commands::Topologystatus => cmd_topology::topology_status(&client),

        // ── Statistics ────────────────────────────────────────────
        Commands::Tablestats { keyspace } => cmd_stats::table_stats(&client, keyspace.as_deref()),
        Commands::Tablehistograms { keyspace, table } => {
            cmd_stats::table_histograms(&client, keyspace.as_deref(), table.as_deref())
        }
        Commands::Tpstats => cmd_stats::tp_stats(&client),
        Commands::Gcstats => cmd_stats::gc_stats(&client),
        Commands::Proxyhistograms => cmd_stats::proxy_histograms(&client),
        Commands::Clientstats => cmd_stats::client_stats(&client),
        Commands::Toppartitions {
            keyspace,
            table,
            duration_ms,
        } => cmd_stats::top_partitions(&client, keyspace.as_deref(), table.as_deref(), duration_ms),
        Commands::Failuredetectorinfo => cmd_stats::failure_detector_info(&client),
        Commands::Datapaths => cmd_stats::data_paths(&client),
        Commands::Refreshsizeestimates => cmd_stats::refresh_size_estimates(&client),
        Commands::Viewbuildstatus => cmd_stats::view_build_status(&client),
        Commands::Getendpoints {
            keyspace,
            table,
            key,
        } => cmd_stats::get_endpoints(&client, &keyspace, &table, &key),
        Commands::Getsstables {
            keyspace,
            table,
            key,
        } => cmd_stats::get_sstables(&client, &keyspace, &table, &key),

        // ── Cache & Hints ─────────────────────────────────────────
        Commands::Invalidatekeycache => cmd_cache_hints::invalidate_cache(&client, "key"),
        Commands::Invalidaterowcache => cmd_cache_hints::invalidate_cache(&client, "row"),
        Commands::Invalidatecountercache => cmd_cache_hints::invalidate_cache(&client, "counter"),
        Commands::Invalidatecredentialscache => {
            cmd_cache_hints::invalidate_cache(&client, "credentials")
        }
        Commands::Invalidatepermissionscache => {
            cmd_cache_hints::invalidate_cache(&client, "permissions")
        }
        Commands::Invalidaterolescache => cmd_cache_hints::invalidate_cache(&client, "roles"),
        Commands::Setcachecapacity {
            cache_type,
            capacity_mb,
        } => cmd_cache_hints::set_cache_capacity(&client, &cache_type, capacity_mb),
        Commands::Enablehandoff => cmd_cache_hints::enable_handoff(&client),
        Commands::Disablehandoff => cmd_cache_hints::disable_handoff(&client),
        Commands::Pausehandoff => cmd_cache_hints::pause_handoff(&client),
        Commands::Resumehandoff => cmd_cache_hints::resume_handoff(&client),
        Commands::Truncatehints => cmd_cache_hints::truncate_hints(&client),
        Commands::Listpendinghints => cmd_cache_hints::list_pending_hints(&client),

        // ── Config ────────────────────────────────────────────────
        Commands::Getconfig { key } => cmd_config::get_config(&client, key.as_deref()),
        Commands::Setconfig { key, value } => cmd_config::set_config(&client, &key, &value),
        Commands::Gettimeout { timeout_type } => cmd_config::get_timeout(&client, &timeout_type),
        Commands::Settimeout {
            timeout_type,
            timeout_ms,
        } => cmd_config::set_timeout(&client, &timeout_type, timeout_ms),
        Commands::Getstreamingthroughput => cmd_config::get_streaming_throughput(&client),
        Commands::Setstreamingthroughput { throughput_mb } => {
            cmd_config::set_streaming_throughput(&client, throughput_mb)
        }
        Commands::Getinterdcstreamthroughput => cmd_config::get_interdc_stream_throughput(&client),
        Commands::Setinterdcstreamthroughput { throughput_mb } => {
            cmd_config::set_interdc_stream_throughput(&client, throughput_mb)
        }
        Commands::Getconcurrentcompactors => cmd_config::get_concurrent_compactors(&client),
        Commands::Setconcurrentcompactors { value } => {
            cmd_config::set_concurrent_compactors(&client, value)
        }
        Commands::Reloadlocalschema => cmd_config::reload_local_schema(&client),
        Commands::Reloadtriggers => cmd_config::reload_triggers(&client),
        Commands::Reloadssl => cmd_config::reload_ssl(&client),
        Commands::Enablebinary => cmd_config::enable_binary(&client),
        Commands::Disablebinary => cmd_config::disable_binary(&client),
        Commands::Statusbinary => cmd_config::status_binary(&client),
        Commands::Enablegossip => cmd_config::enable_gossip(&client),
        Commands::Disablegossip => cmd_config::disable_gossip(&client),
        Commands::Statusgossip => cmd_config::status_gossip(&client),
        Commands::Getseeds => cmd_config::get_seeds(&client),

        // ── Logging & Security ────────────────────────────────────
        Commands::Getlogginglevels => cmd_logging_security::get_logging_levels(&client),
        Commands::Setlogginglevel { logger, level } => {
            cmd_logging_security::set_logging_level(&client, &logger, &level)
        }
        Commands::Enableauditlog => cmd_logging_security::enable_audit_log(&client),
        Commands::Disableauditlog => cmd_logging_security::disable_audit_log(&client),
        Commands::Getauditlogconfig => cmd_logging_security::get_audit_log_config(&client),
        Commands::Enablefql { log_dir } => cmd_logging_security::enable_fql(&client, &log_dir),
        Commands::Disablefql => cmd_logging_security::disable_fql(&client),
        Commands::Getfqlconfig => cmd_logging_security::get_fql_config(&client),
        Commands::Resetfql => cmd_logging_security::reset_fql(&client),
        Commands::Gettraceprobability => cmd_logging_security::get_trace_probability(&client),
        Commands::Settraceprobability { probability } => {
            cmd_logging_security::set_trace_probability(&client, probability)
        }

        // ── Repair ────────────────────────────────────────────────
        Commands::Repair {
            keyspace,
            tables,
            full,
            incremental,
            preview,
        } => {
            cmd_compaction::repair(
                &client,
                keyspace.as_deref(),
                &tables,
                full,
                incremental,
                preview,
            );
        }
        Commands::RebuildIndex {
            keyspace,
            table,
            index_names,
        } => {
            cmd_compaction::rebuild_index(&client, &keyspace, &table, &index_names);
        }

        // ── SSTable tools ─────────────────────────────────────────
        Commands::Sstabledump { file } => sstable_tools::dump_sstable(&file),
        Commands::Sstablemetadata { file } => sstable_tools::show_metadata(&file),
        Commands::Sstableverify { file } => sstable_verify::run(&file),
        Commands::Sstablescrub { file, output_dir } => {
            sstable_scrub::run(&file, output_dir.as_deref())
        }
        Commands::Sstableupgrade { file } => sstable_upgrade::run(&file),
        Commands::Sstablesplit { file, size_mb } => sstable_split::run(&file, size_mb),
        Commands::Sstablelevelreset { file } => sstable_level_reset::run(&file),
        Commands::Sstablerepairedset { file, repaired_at } => {
            sstable_repaired_at::run(&file, repaired_at)
        }
        Commands::Sstableexpiredblockers {
            file,
            gc_grace_seconds,
        } => sstable_expired_blockers::run(&file, gc_grace_seconds),
        Commands::Sstableofflinerelevel { dir } => sstable_offline_relevel::run(&dir),
        Commands::Sstablepartitions { file, top } => sstable_partitions::run(&file, top),

        // ── Utility tools ─────────────────────────────────────────
        Commands::Sstableloader { dir, nodes } => {
            println!("Loading SSTables from {}...", dir);
            let body = serde_json::json!({
                "directory": dir,
                "nodes": nodes,
            });
            match client.post_json("/api/v1/operations/sstableloader", &body) {
                Ok(resp) => {
                    if let Some(id) = resp.get("operation_id") {
                        println!("SSTable load started (operation {})", id);
                    } else {
                        println!("SSTable load request accepted: {}", resp);
                    }
                }
                Err(e) => eprintln!("Error loading SSTables: {}", e),
            }
        }
        Commands::Bootstrapmonitor => bootstrap_monitor::run(&client),
        Commands::Generatetokens {
            nodes,
            tokens,
            racks,
        } => generate_tokens::run(nodes, tokens, racks),
        Commands::Hashpassword { password, rounds } => hash_password::run(password, rounds),

        // ── Pass-through tools ────────────────────────────────────
        Commands::Auditlogviewer { args } => {
            println!("Auditlogviewer: {:?}", args);
            println!("Use the audit log viewer command implementation with the listed arguments.");
        }
        Commands::Fqltool { args } => {
            println!("fqltool: {:?}", args);
            println!("Use the bundled fqltool binary with the listed arguments.");
        }
        Commands::CassandraStress { args } => {
            println!("cassandra-stress: {:?}", args);
            println!("Use the Rust stress command implementation with the listed arguments.");
        }
    }
}
