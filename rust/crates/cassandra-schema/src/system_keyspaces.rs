// Licensed under Apache License, Version 2.0.

//! System keyspace and table definitions.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.SystemKeyspace`
//! - `org.apache.cassandra.schema.SchemaKeyspace`
//! - `org.apache.cassandra.schema.SchemaKeyspaceTables`
//! - `org.apache.cassandra.schema.SystemDistributedKeyspace`
//! - `org.apache.cassandra.auth.AuthKeyspace`
//! - `org.apache.cassandra.tracing.TraceKeyspace`
//!
//! All system table definitions modeled as Rust structs with column metadata
//! and CQL CREATE TABLE statements matching the Java baseline.

use crate::column::{ClusteringOrder, ColumnKind, ColumnMetadata};
use crate::keyspace::{KeyspaceMetadata, KeyspaceParams, ReplicationParams};
use crate::schema_constants;
use crate::table::{TableMetadata, TableMetadataBuilder};
use cassandra_types::CqlType;

// ─── System Table Column Spec ──────────────────────────────────────────────

/// A lightweight column specification used to define system table schemas.
#[derive(Debug, Clone)]
pub struct SystemColumnSpec {
    pub name: &'static str,
    pub cql_type: CqlType,
    pub kind: ColumnKind,
    /// Position within partition key or clustering key (0-indexed).
    pub position: u32,
}

impl SystemColumnSpec {
    pub const fn partition_key(name: &'static str, cql_type: CqlType, pos: u32) -> Self {
        Self {
            name,
            cql_type,
            kind: ColumnKind::PartitionKey,
            position: pos,
        }
    }
    pub const fn clustering(name: &'static str, cql_type: CqlType, pos: u32) -> Self {
        Self {
            name,
            cql_type,
            kind: ColumnKind::Clustering,
            position: pos,
        }
    }
    pub const fn regular(name: &'static str, cql_type: CqlType) -> Self {
        Self {
            name,
            cql_type,
            kind: ColumnKind::Regular,
            position: 0,
        }
    }
}

// ─── System Table Definition ───────────────────────────────────────────────

/// A system table definition with all metadata needed to bootstrap the table.
#[derive(Debug, Clone)]
pub struct SystemTableDef {
    pub keyspace: &'static str,
    pub name: &'static str,
    pub comment: &'static str,
    pub columns: Vec<SystemColumnSpec>,
    pub gc_grace_seconds: i32,
    pub default_ttl: i32,
}

impl SystemTableDef {
    /// Convert to a `TableMetadata` instance.
    pub fn to_table_metadata(&self) -> TableMetadata {
        let mut builder = TableMetadataBuilder::new(self.keyspace, self.name);
        for col in &self.columns {
            builder = builder.add_column(ColumnMetadata {
                name: col.name.to_string(),
                kind: col.kind,
                position: col.position,
                column_type: col.cql_type.clone(),
                clustering_order: ClusteringOrder::Asc,
                masked_with: None,
                constraints: Vec::new(),
            });
        }
        builder.build()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// system KEYSPACE
// ═══════════════════════════════════════════════════════════════════════════

/// `system.local` — information about the local node.
pub fn local_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SYSTEM_KEYSPACE,
        name: "local",
        comment: "information about the local node",
        columns: vec![
            SystemColumnSpec::partition_key("key", CqlType::Varchar, 0),
            SystemColumnSpec::regular("bootstrapped", CqlType::Varchar),
            SystemColumnSpec::regular("broadcast_address", CqlType::Inet),
            SystemColumnSpec::regular("broadcast_port", CqlType::Int),
            SystemColumnSpec::regular("cluster_name", CqlType::Varchar),
            SystemColumnSpec::regular("cql_version", CqlType::Varchar),
            SystemColumnSpec::regular("data_center", CqlType::Varchar),
            SystemColumnSpec::regular("gossip_generation", CqlType::Int),
            SystemColumnSpec::regular("host_id", CqlType::Uuid),
            SystemColumnSpec::regular("listen_address", CqlType::Inet),
            SystemColumnSpec::regular("listen_port", CqlType::Int),
            SystemColumnSpec::regular("native_protocol_version", CqlType::Varchar),
            SystemColumnSpec::regular("partitioner", CqlType::Varchar),
            SystemColumnSpec::regular("rack", CqlType::Varchar),
            SystemColumnSpec::regular("release_version", CqlType::Varchar),
            SystemColumnSpec::regular("rpc_address", CqlType::Inet),
            SystemColumnSpec::regular("rpc_port", CqlType::Int),
            SystemColumnSpec::regular("schema_version", CqlType::Uuid),
            SystemColumnSpec::regular("tokens", CqlType::Set(Box::new(CqlType::Varchar), false)),
            SystemColumnSpec::regular(
                "truncated_at",
                CqlType::Map(Box::new(CqlType::Uuid), Box::new(CqlType::Blob), false),
            ),
        ],
        gc_grace_seconds: 0,
        default_ttl: 0,
    }
}

/// `system.peers_v2` — information about known peers in the cluster.
pub fn peers_v2_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SYSTEM_KEYSPACE,
        name: "peers_v2",
        comment: "information about known peers in the cluster",
        columns: vec![
            SystemColumnSpec::partition_key("peer", CqlType::Inet, 0),
            SystemColumnSpec::clustering("peer_port", CqlType::Int, 0),
            SystemColumnSpec::regular("data_center", CqlType::Varchar),
            SystemColumnSpec::regular("host_id", CqlType::Uuid),
            SystemColumnSpec::regular("preferred_ip", CqlType::Inet),
            SystemColumnSpec::regular("preferred_port", CqlType::Int),
            SystemColumnSpec::regular("rack", CqlType::Varchar),
            SystemColumnSpec::regular("release_version", CqlType::Varchar),
            SystemColumnSpec::regular("native_address", CqlType::Inet),
            SystemColumnSpec::regular("native_port", CqlType::Int),
            SystemColumnSpec::regular("schema_version", CqlType::Uuid),
            SystemColumnSpec::regular("tokens", CqlType::Set(Box::new(CqlType::Varchar), false)),
        ],
        gc_grace_seconds: 0,
        default_ttl: 0,
    }
}

/// `system.peers` — legacy peers table (deprecated since 4.0).
pub fn legacy_peers_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SYSTEM_KEYSPACE,
        name: "peers",
        comment: "information about known peers in the cluster (legacy)",
        columns: vec![
            SystemColumnSpec::partition_key("peer", CqlType::Inet, 0),
            SystemColumnSpec::regular("data_center", CqlType::Varchar),
            SystemColumnSpec::regular("host_id", CqlType::Uuid),
            SystemColumnSpec::regular("preferred_ip", CqlType::Inet),
            SystemColumnSpec::regular("rack", CqlType::Varchar),
            SystemColumnSpec::regular("release_version", CqlType::Varchar),
            SystemColumnSpec::regular("rpc_address", CqlType::Inet),
            SystemColumnSpec::regular("schema_version", CqlType::Uuid),
            SystemColumnSpec::regular("tokens", CqlType::Set(Box::new(CqlType::Varchar), false)),
        ],
        gc_grace_seconds: 0,
        default_ttl: 0,
    }
}

/// `system.peer_events_v2` — events related to peers.
pub fn peer_events_v2_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SYSTEM_KEYSPACE,
        name: "peer_events_v2",
        comment: "events related to peers",
        columns: vec![
            SystemColumnSpec::partition_key("peer", CqlType::Inet, 0),
            SystemColumnSpec::clustering("peer_port", CqlType::Int, 0),
            SystemColumnSpec::regular(
                "hints_dropped",
                CqlType::Map(Box::new(CqlType::Uuid), Box::new(CqlType::Int), false),
            ),
        ],
        gc_grace_seconds: 0,
        default_ttl: 0,
    }
}

/// `system.batches` — batches awaiting replay.
pub fn batches_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SYSTEM_KEYSPACE,
        name: "batches",
        comment: "batches awaiting replay",
        columns: vec![
            SystemColumnSpec::partition_key("id", CqlType::Uuid, 0),
            SystemColumnSpec::regular("mutations", CqlType::List(Box::new(CqlType::Blob), false)),
            SystemColumnSpec::regular("version", CqlType::Int),
        ],
        gc_grace_seconds: 0,
        default_ttl: 0,
    }
}

/// `system.paxos` — in-progress paxos proposals.
pub fn paxos_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SYSTEM_KEYSPACE,
        name: "paxos",
        comment: "in-progress paxos proposals",
        columns: vec![
            SystemColumnSpec::partition_key("row_key", CqlType::Blob, 0),
            SystemColumnSpec::clustering("cf_id", CqlType::Uuid, 0),
            SystemColumnSpec::regular("in_progress_ballot", CqlType::Uuid),
            SystemColumnSpec::regular("in_progress_read_ballot", CqlType::Uuid),
            SystemColumnSpec::regular("most_recent_commit", CqlType::Blob),
            SystemColumnSpec::regular("most_recent_commit_at", CqlType::Uuid),
            SystemColumnSpec::regular("most_recent_commit_version", CqlType::Int),
            SystemColumnSpec::regular("proposal", CqlType::Blob),
            SystemColumnSpec::regular("proposal_ballot", CqlType::Uuid),
            SystemColumnSpec::regular("proposal_version", CqlType::Int),
        ],
        gc_grace_seconds: 0,
        default_ttl: 0,
    }
}

/// `system.paxos_repair_history` — last successful paxos repairs by range.
pub fn paxos_repair_history_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SYSTEM_KEYSPACE,
        name: "paxos_repair_history",
        comment: "last successful paxos repairs by range",
        columns: vec![
            SystemColumnSpec::partition_key("keyspace_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("table_name", CqlType::Varchar, 0),
            SystemColumnSpec::regular("points", CqlType::List(Box::new(CqlType::Blob), false)),
        ],
        gc_grace_seconds: 0,
        default_ttl: 0,
    }
}

/// `system.consensus_migration_state` — keys migrated to another consensus protocol.
pub fn consensus_migration_state_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SYSTEM_KEYSPACE,
        name: "consensus_migration_state",
        comment: "keys that have been migrated to another consensus protocol",
        columns: vec![
            SystemColumnSpec::partition_key("row_key", CqlType::Blob, 0),
            SystemColumnSpec::clustering("cf_id", CqlType::Uuid, 0),
            SystemColumnSpec::clustering("consensus_migrated_at_epoch", CqlType::Bigint, 1),
            SystemColumnSpec::regular("consensus_max_hlc", CqlType::Bigint),
            SystemColumnSpec::regular("consensus_target", CqlType::Tinyint),
        ],
        gc_grace_seconds: 0,
        default_ttl: 604800, // 7 days
    }
}

/// `system.IndexInfo` — built column indexes.
pub fn built_indexes_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SYSTEM_KEYSPACE,
        name: "IndexInfo",
        comment: "built column indexes",
        columns: vec![
            SystemColumnSpec::partition_key("table_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("index_name", CqlType::Varchar, 0),
            SystemColumnSpec::regular("value", CqlType::Blob),
        ],
        gc_grace_seconds: 0,
        default_ttl: 0,
    }
}

/// `system.compaction_history` — week-long compaction history.
pub fn compaction_history_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SYSTEM_KEYSPACE,
        name: "compaction_history",
        comment: "week-long compaction history",
        columns: vec![
            SystemColumnSpec::partition_key("id", CqlType::Uuid, 0),
            SystemColumnSpec::regular("bytes_in", CqlType::Bigint),
            SystemColumnSpec::regular("bytes_out", CqlType::Bigint),
            SystemColumnSpec::regular("columnfamily_name", CqlType::Varchar),
            SystemColumnSpec::regular("compacted_at", CqlType::Timestamp),
            SystemColumnSpec::regular("keyspace_name", CqlType::Varchar),
            SystemColumnSpec::regular(
                "rows_merged",
                CqlType::Map(Box::new(CqlType::Int), Box::new(CqlType::Bigint), false),
            ),
        ],
        gc_grace_seconds: 0,
        default_ttl: 604800, // 7 days
    }
}

/// `system.sstable_activity_v2` — historic sstable read rates.
pub fn sstable_activity_v2_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SYSTEM_KEYSPACE,
        name: "sstable_activity_v2",
        comment: "historic sstable read rates",
        columns: vec![
            SystemColumnSpec::partition_key("keyspace_name", CqlType::Varchar, 0),
            SystemColumnSpec::partition_key("table_name", CqlType::Varchar, 1),
            SystemColumnSpec::partition_key("id", CqlType::Varchar, 2),
            SystemColumnSpec::regular("rate_120m", CqlType::Double),
            SystemColumnSpec::regular("rate_15m", CqlType::Double),
        ],
        gc_grace_seconds: 0,
        default_ttl: 0,
    }
}

/// `system.table_estimates` — per-table range size estimates.
pub fn table_estimates_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SYSTEM_KEYSPACE,
        name: "table_estimates",
        comment: "per-table range size estimates",
        columns: vec![
            SystemColumnSpec::partition_key("keyspace_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("table_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("range_type", CqlType::Varchar, 1),
            SystemColumnSpec::clustering("range_start", CqlType::Varchar, 2),
            SystemColumnSpec::clustering("range_end", CqlType::Varchar, 3),
            SystemColumnSpec::regular("mean_partition_size", CqlType::Bigint),
            SystemColumnSpec::regular("partitions_count", CqlType::Bigint),
        ],
        gc_grace_seconds: 0,
        default_ttl: 0,
    }
}

/// `system.size_estimates` — legacy size estimates (deprecated since 4.0).
pub fn legacy_size_estimates_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SYSTEM_KEYSPACE,
        name: "size_estimates",
        comment: "per-table primary range size estimates (legacy, deprecated)",
        columns: vec![
            SystemColumnSpec::partition_key("keyspace_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("table_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("range_start", CqlType::Varchar, 1),
            SystemColumnSpec::clustering("range_end", CqlType::Varchar, 2),
            SystemColumnSpec::regular("mean_partition_size", CqlType::Bigint),
            SystemColumnSpec::regular("partitions_count", CqlType::Bigint),
        ],
        gc_grace_seconds: 0,
        default_ttl: 0,
    }
}

/// `system.available_ranges_v2` — available ranges during bootstrap/replace.
pub fn available_ranges_v2_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SYSTEM_KEYSPACE,
        name: "available_ranges_v2",
        comment: "available keyspace/ranges during bootstrap/replace",
        columns: vec![
            SystemColumnSpec::partition_key("keyspace_name", CqlType::Varchar, 0),
            SystemColumnSpec::regular("full_ranges", CqlType::Set(Box::new(CqlType::Blob), false)),
            SystemColumnSpec::regular(
                "transient_ranges",
                CqlType::Set(Box::new(CqlType::Blob), false),
            ),
        ],
        gc_grace_seconds: 0,
        default_ttl: 0,
    }
}

/// `system.transferred_ranges_v2` — record of transferred ranges for streaming.
pub fn transferred_ranges_v2_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SYSTEM_KEYSPACE,
        name: "transferred_ranges_v2",
        comment: "record of transferred ranges for streaming operation",
        columns: vec![
            SystemColumnSpec::partition_key("operation", CqlType::Varchar, 0),
            SystemColumnSpec::partition_key("keyspace_name", CqlType::Varchar, 1),
            SystemColumnSpec::clustering("peer", CqlType::Inet, 0),
            SystemColumnSpec::clustering("peer_port", CqlType::Int, 1),
            SystemColumnSpec::regular("ranges", CqlType::Set(Box::new(CqlType::Blob), false)),
        ],
        gc_grace_seconds: 0,
        default_ttl: 0,
    }
}

/// `system.view_builds_in_progress` — views builds current progress.
pub fn view_builds_in_progress_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SYSTEM_KEYSPACE,
        name: "view_builds_in_progress",
        comment: "views builds current progress",
        columns: vec![
            SystemColumnSpec::partition_key("keyspace_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("view_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("start_token", CqlType::Varchar, 1),
            SystemColumnSpec::clustering("end_token", CqlType::Varchar, 2),
            SystemColumnSpec::regular("last_token", CqlType::Varchar),
            SystemColumnSpec::regular("keys_built", CqlType::Bigint),
        ],
        gc_grace_seconds: 0,
        default_ttl: 0,
    }
}

/// `system.built_views` — built views.
pub fn built_views_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SYSTEM_KEYSPACE,
        name: "built_views",
        comment: "built views",
        columns: vec![
            SystemColumnSpec::partition_key("keyspace_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("view_name", CqlType::Varchar, 0),
            SystemColumnSpec::regular("status_replicated", CqlType::Boolean),
        ],
        gc_grace_seconds: 0,
        default_ttl: 0,
    }
}

/// `system.prepared_statements` — prepared statements.
pub fn prepared_statements_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SYSTEM_KEYSPACE,
        name: "prepared_statements",
        comment: "prepared statements",
        columns: vec![
            SystemColumnSpec::partition_key("prepared_id", CqlType::Blob, 0),
            SystemColumnSpec::regular("logged_keyspace", CqlType::Varchar),
            SystemColumnSpec::regular("query_string", CqlType::Varchar),
        ],
        gc_grace_seconds: 0,
        default_ttl: 0,
    }
}

/// `system.repairs` — active and completed repairs.
pub fn repairs_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SYSTEM_KEYSPACE,
        name: "repairs",
        comment: "repairs",
        columns: vec![
            SystemColumnSpec::partition_key("parent_id", CqlType::Uuid, 0),
            SystemColumnSpec::regular("started_at", CqlType::Timestamp),
            SystemColumnSpec::regular("last_update", CqlType::Timestamp),
            SystemColumnSpec::regular("repaired_at", CqlType::Timestamp),
            SystemColumnSpec::regular("state", CqlType::Int),
            SystemColumnSpec::regular("coordinator", CqlType::Inet),
            SystemColumnSpec::regular("coordinator_port", CqlType::Int),
            SystemColumnSpec::regular("participants", CqlType::Set(Box::new(CqlType::Inet), false)),
            SystemColumnSpec::regular(
                "participants_wp",
                CqlType::Set(Box::new(CqlType::Varchar), false),
            ),
            SystemColumnSpec::regular("ranges", CqlType::Set(Box::new(CqlType::Blob), false)),
            SystemColumnSpec::regular("cfids", CqlType::Set(Box::new(CqlType::Uuid), false)),
        ],
        gc_grace_seconds: 0,
        default_ttl: 0,
    }
}

/// `system.top_partitions` — stores the top partitions.
pub fn top_partitions_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SYSTEM_KEYSPACE,
        name: "top_partitions",
        comment: "stores the top partitions",
        columns: vec![
            SystemColumnSpec::partition_key("keyspace_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("table_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("top_type", CqlType::Varchar, 1),
            SystemColumnSpec::regular("top", CqlType::List(Box::new(CqlType::Blob), false)),
            SystemColumnSpec::regular("last_update", CqlType::Timestamp),
        ],
        gc_grace_seconds: 0,
        default_ttl: 0,
    }
}

/// `system.local_metadata_log` — local metadata log.
pub fn local_metadata_log_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SYSTEM_KEYSPACE,
        name: "local_metadata_log",
        comment: "local metadata log",
        columns: vec![
            SystemColumnSpec::partition_key("epoch", CqlType::Bigint, 0),
            SystemColumnSpec::regular("entry_id", CqlType::Bigint),
            SystemColumnSpec::regular("transformation", CqlType::Blob),
            SystemColumnSpec::regular("kind", CqlType::Int),
        ],
        gc_grace_seconds: 0,
        default_ttl: 0,
    }
}

/// `system.metadata_snapshots` — ClusterMetadata snapshots.
pub fn metadata_snapshots_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SYSTEM_KEYSPACE,
        name: "metadata_snapshots",
        comment: "ClusterMetadata snapshots",
        columns: vec![
            SystemColumnSpec::partition_key("epoch", CqlType::Bigint, 0),
            SystemColumnSpec::regular("snapshot", CqlType::Blob),
        ],
        gc_grace_seconds: 0,
        default_ttl: 0,
    }
}

/// Returns all table definitions for the `system` keyspace.
pub fn system_keyspace_tables() -> Vec<SystemTableDef> {
    vec![
        local_table(),
        peers_v2_table(),
        legacy_peers_table(),
        peer_events_v2_table(),
        batches_table(),
        paxos_table(),
        paxos_repair_history_table(),
        consensus_migration_state_table(),
        built_indexes_table(),
        compaction_history_table(),
        sstable_activity_v2_table(),
        table_estimates_table(),
        legacy_size_estimates_table(),
        available_ranges_v2_table(),
        transferred_ranges_v2_table(),
        view_builds_in_progress_table(),
        built_views_table(),
        prepared_statements_table(),
        repairs_table(),
        top_partitions_table(),
        local_metadata_log_table(),
        metadata_snapshots_table(),
    ]
}

// ═══════════════════════════════════════════════════════════════════════════
// system_schema KEYSPACE
// ═══════════════════════════════════════════════════════════════════════════

/// `system_schema.keyspaces`
pub fn schema_keyspaces_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SCHEMA_KEYSPACE,
        name: "keyspaces",
        comment: "keyspace definitions",
        columns: vec![
            SystemColumnSpec::partition_key("keyspace_name", CqlType::Varchar, 0),
            SystemColumnSpec::regular("durable_writes", CqlType::Boolean),
            SystemColumnSpec::regular(
                "replication",
                CqlType::Map(
                    Box::new(CqlType::Varchar),
                    Box::new(CqlType::Varchar),
                    false,
                ),
            ),
        ],
        gc_grace_seconds: 604800,
        default_ttl: 0,
    }
}

/// `system_schema.tables`
pub fn schema_tables_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SCHEMA_KEYSPACE,
        name: "tables",
        comment: "table definitions",
        columns: vec![
            SystemColumnSpec::partition_key("keyspace_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("table_name", CqlType::Varchar, 0),
            SystemColumnSpec::regular("bloom_filter_fp_chance", CqlType::Double),
            SystemColumnSpec::regular(
                "caching",
                CqlType::Map(
                    Box::new(CqlType::Varchar),
                    Box::new(CqlType::Varchar),
                    false,
                ),
            ),
            SystemColumnSpec::regular("comment", CqlType::Varchar),
            SystemColumnSpec::regular(
                "compaction",
                CqlType::Map(
                    Box::new(CqlType::Varchar),
                    Box::new(CqlType::Varchar),
                    false,
                ),
            ),
            SystemColumnSpec::regular(
                "compression",
                CqlType::Map(
                    Box::new(CqlType::Varchar),
                    Box::new(CqlType::Varchar),
                    false,
                ),
            ),
            SystemColumnSpec::regular("crc_check_chance", CqlType::Double),
            SystemColumnSpec::regular("dclocal_read_repair_chance", CqlType::Double),
            SystemColumnSpec::regular("default_time_to_live", CqlType::Int),
            SystemColumnSpec::regular(
                "extensions",
                CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Blob), false),
            ),
            SystemColumnSpec::regular("flags", CqlType::Set(Box::new(CqlType::Varchar), false)),
            SystemColumnSpec::regular("gc_grace_seconds", CqlType::Int),
            SystemColumnSpec::regular("id", CqlType::Uuid),
            SystemColumnSpec::regular("max_index_interval", CqlType::Int),
            SystemColumnSpec::regular("memtable_flush_period_in_ms", CqlType::Int),
            SystemColumnSpec::regular("min_index_interval", CqlType::Int),
            SystemColumnSpec::regular("read_repair_chance", CqlType::Double),
            SystemColumnSpec::regular("speculative_retry", CqlType::Varchar),
        ],
        gc_grace_seconds: 604800,
        default_ttl: 0,
    }
}

/// `system_schema.columns`
pub fn schema_columns_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SCHEMA_KEYSPACE,
        name: "columns",
        comment: "column definitions",
        columns: vec![
            SystemColumnSpec::partition_key("keyspace_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("table_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("column_name", CqlType::Varchar, 1),
            SystemColumnSpec::regular("clustering_order", CqlType::Varchar),
            SystemColumnSpec::regular("column_name_bytes", CqlType::Blob),
            SystemColumnSpec::regular("kind", CqlType::Varchar),
            SystemColumnSpec::regular("position", CqlType::Int),
            SystemColumnSpec::regular("type", CqlType::Varchar),
        ],
        gc_grace_seconds: 604800,
        default_ttl: 0,
    }
}

/// `system_schema.column_masks`
pub fn schema_column_masks_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SCHEMA_KEYSPACE,
        name: "column_masks",
        comment: "column mask definitions",
        columns: vec![
            SystemColumnSpec::partition_key("keyspace_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("table_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("column_name", CqlType::Varchar, 1),
            SystemColumnSpec::regular("function_name", CqlType::Varchar),
            SystemColumnSpec::regular("function_keyspace_name", CqlType::Varchar),
            SystemColumnSpec::regular(
                "function_argument_types",
                CqlType::List(Box::new(CqlType::Varchar), false),
            ),
        ],
        gc_grace_seconds: 604800,
        default_ttl: 0,
    }
}

/// `system_schema.dropped_columns`
pub fn schema_dropped_columns_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SCHEMA_KEYSPACE,
        name: "dropped_columns",
        comment: "dropped column metadata",
        columns: vec![
            SystemColumnSpec::partition_key("keyspace_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("table_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("column_name", CqlType::Varchar, 1),
            SystemColumnSpec::regular("dropped_time", CqlType::Timestamp),
            SystemColumnSpec::regular("kind", CqlType::Varchar),
            SystemColumnSpec::regular("type", CqlType::Varchar),
        ],
        gc_grace_seconds: 604800,
        default_ttl: 0,
    }
}

/// `system_schema.triggers`
pub fn schema_triggers_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SCHEMA_KEYSPACE,
        name: "triggers",
        comment: "trigger definitions",
        columns: vec![
            SystemColumnSpec::partition_key("keyspace_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("table_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("trigger_name", CqlType::Varchar, 1),
            SystemColumnSpec::regular(
                "options",
                CqlType::Map(
                    Box::new(CqlType::Varchar),
                    Box::new(CqlType::Varchar),
                    false,
                ),
            ),
        ],
        gc_grace_seconds: 604800,
        default_ttl: 0,
    }
}

/// `system_schema.views`
pub fn schema_views_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SCHEMA_KEYSPACE,
        name: "views",
        comment: "view definitions",
        columns: vec![
            SystemColumnSpec::partition_key("keyspace_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("view_name", CqlType::Varchar, 0),
            SystemColumnSpec::regular("base_table_id", CqlType::Uuid),
            SystemColumnSpec::regular("base_table_name", CqlType::Varchar),
            SystemColumnSpec::regular("bloom_filter_fp_chance", CqlType::Double),
            SystemColumnSpec::regular(
                "caching",
                CqlType::Map(
                    Box::new(CqlType::Varchar),
                    Box::new(CqlType::Varchar),
                    false,
                ),
            ),
            SystemColumnSpec::regular("comment", CqlType::Varchar),
            SystemColumnSpec::regular(
                "compaction",
                CqlType::Map(
                    Box::new(CqlType::Varchar),
                    Box::new(CqlType::Varchar),
                    false,
                ),
            ),
            SystemColumnSpec::regular(
                "compression",
                CqlType::Map(
                    Box::new(CqlType::Varchar),
                    Box::new(CqlType::Varchar),
                    false,
                ),
            ),
            SystemColumnSpec::regular("crc_check_chance", CqlType::Double),
            SystemColumnSpec::regular("dclocal_read_repair_chance", CqlType::Double),
            SystemColumnSpec::regular("default_time_to_live", CqlType::Int),
            SystemColumnSpec::regular(
                "extensions",
                CqlType::Map(Box::new(CqlType::Varchar), Box::new(CqlType::Blob), false),
            ),
            SystemColumnSpec::regular("gc_grace_seconds", CqlType::Int),
            SystemColumnSpec::regular("id", CqlType::Uuid),
            SystemColumnSpec::regular("include_all_columns", CqlType::Boolean),
            SystemColumnSpec::regular("max_index_interval", CqlType::Int),
            SystemColumnSpec::regular("memtable_flush_period_in_ms", CqlType::Int),
            SystemColumnSpec::regular("min_index_interval", CqlType::Int),
            SystemColumnSpec::regular("read_repair_chance", CqlType::Double),
            SystemColumnSpec::regular("speculative_retry", CqlType::Varchar),
            SystemColumnSpec::regular("where_clause", CqlType::Varchar),
        ],
        gc_grace_seconds: 604800,
        default_ttl: 0,
    }
}

/// `system_schema.types`
pub fn schema_types_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SCHEMA_KEYSPACE,
        name: "types",
        comment: "user defined type definitions",
        columns: vec![
            SystemColumnSpec::partition_key("keyspace_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("type_name", CqlType::Varchar, 0),
            SystemColumnSpec::regular(
                "field_names",
                CqlType::List(Box::new(CqlType::Varchar), false),
            ),
            SystemColumnSpec::regular(
                "field_types",
                CqlType::List(Box::new(CqlType::Varchar), false),
            ),
        ],
        gc_grace_seconds: 604800,
        default_ttl: 0,
    }
}

/// `system_schema.functions`
pub fn schema_functions_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SCHEMA_KEYSPACE,
        name: "functions",
        comment: "user defined function definitions",
        columns: vec![
            SystemColumnSpec::partition_key("keyspace_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("function_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering(
                "argument_types",
                CqlType::List(Box::new(CqlType::Varchar), false),
                1,
            ),
            SystemColumnSpec::regular(
                "argument_names",
                CqlType::List(Box::new(CqlType::Varchar), false),
            ),
            SystemColumnSpec::regular("body", CqlType::Varchar),
            SystemColumnSpec::regular("called_on_null_input", CqlType::Boolean),
            SystemColumnSpec::regular("language", CqlType::Varchar),
            SystemColumnSpec::regular("return_type", CqlType::Varchar),
        ],
        gc_grace_seconds: 604800,
        default_ttl: 0,
    }
}

/// `system_schema.aggregates`
pub fn schema_aggregates_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SCHEMA_KEYSPACE,
        name: "aggregates",
        comment: "user defined aggregate definitions",
        columns: vec![
            SystemColumnSpec::partition_key("keyspace_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("aggregate_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering(
                "argument_types",
                CqlType::List(Box::new(CqlType::Varchar), false),
                1,
            ),
            SystemColumnSpec::regular("final_func", CqlType::Varchar),
            SystemColumnSpec::regular("initcond", CqlType::Varchar),
            SystemColumnSpec::regular("return_type", CqlType::Varchar),
            SystemColumnSpec::regular("state_func", CqlType::Varchar),
            SystemColumnSpec::regular("state_type", CqlType::Varchar),
        ],
        gc_grace_seconds: 604800,
        default_ttl: 0,
    }
}

/// `system_schema.indexes`
pub fn schema_indexes_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::SCHEMA_KEYSPACE,
        name: "indexes",
        comment: "secondary index definitions",
        columns: vec![
            SystemColumnSpec::partition_key("keyspace_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("table_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("index_name", CqlType::Varchar, 1),
            SystemColumnSpec::regular("kind", CqlType::Varchar),
            SystemColumnSpec::regular(
                "options",
                CqlType::Map(
                    Box::new(CqlType::Varchar),
                    Box::new(CqlType::Varchar),
                    false,
                ),
            ),
        ],
        gc_grace_seconds: 604800,
        default_ttl: 0,
    }
}

/// Returns all table definitions for the `system_schema` keyspace.
pub fn system_schema_tables() -> Vec<SystemTableDef> {
    vec![
        schema_keyspaces_table(),
        schema_tables_table(),
        schema_columns_table(),
        schema_column_masks_table(),
        schema_dropped_columns_table(),
        schema_triggers_table(),
        schema_views_table(),
        schema_types_table(),
        schema_functions_table(),
        schema_aggregates_table(),
        schema_indexes_table(),
    ]
}

// ═══════════════════════════════════════════════════════════════════════════
// system_auth KEYSPACE
// ═══════════════════════════════════════════════════════════════════════════

/// `system_auth.roles`
pub fn auth_roles_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::AUTH_KEYSPACE,
        name: "roles",
        comment: "role definitions",
        columns: vec![
            SystemColumnSpec::partition_key("role", CqlType::Varchar, 0),
            SystemColumnSpec::regular("can_login", CqlType::Boolean),
            SystemColumnSpec::regular("is_superuser", CqlType::Boolean),
            SystemColumnSpec::regular("member_of", CqlType::Set(Box::new(CqlType::Varchar), false)),
            SystemColumnSpec::regular("salted_hash", CqlType::Varchar),
        ],
        gc_grace_seconds: 7776000, // 90 days
        default_ttl: 0,
    }
}

/// `system_auth.role_members`
pub fn auth_role_members_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::AUTH_KEYSPACE,
        name: "role_members",
        comment: "role memberships",
        columns: vec![
            SystemColumnSpec::partition_key("role", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("member", CqlType::Varchar, 0),
        ],
        gc_grace_seconds: 7776000,
        default_ttl: 0,
    }
}

/// `system_auth.role_permissions`
pub fn auth_role_permissions_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::AUTH_KEYSPACE,
        name: "role_permissions",
        comment: "role permissions",
        columns: vec![
            SystemColumnSpec::partition_key("role", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("resource", CqlType::Varchar, 0),
            SystemColumnSpec::regular(
                "permissions",
                CqlType::Set(Box::new(CqlType::Varchar), false),
            ),
        ],
        gc_grace_seconds: 7776000,
        default_ttl: 0,
    }
}

/// `system_auth.network_permissions`
pub fn auth_network_permissions_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::AUTH_KEYSPACE,
        name: "network_permissions",
        comment: "network permission definitions",
        columns: vec![
            SystemColumnSpec::partition_key("role", CqlType::Varchar, 0),
            SystemColumnSpec::regular("dcs", CqlType::Set(Box::new(CqlType::Varchar), false)),
        ],
        gc_grace_seconds: 7776000,
        default_ttl: 0,
    }
}

/// `system_auth.cidr_permissions`
pub fn auth_cidr_permissions_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::AUTH_KEYSPACE,
        name: "cidr_permissions",
        comment: "CIDR-based network permission definitions",
        columns: vec![
            SystemColumnSpec::partition_key("role", CqlType::Varchar, 0),
            SystemColumnSpec::regular("cidrs", CqlType::Set(Box::new(CqlType::Varchar), false)),
        ],
        gc_grace_seconds: 7776000,
        default_ttl: 0,
    }
}

/// `system_auth.cidr_groups`
pub fn auth_cidr_groups_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::AUTH_KEYSPACE,
        name: "cidr_groups",
        comment: "named CIDR group definitions",
        columns: vec![
            SystemColumnSpec::partition_key("cidr_group", CqlType::Varchar, 0),
            SystemColumnSpec::regular("cidrs", CqlType::Set(Box::new(CqlType::Varchar), false)),
        ],
        gc_grace_seconds: 7776000,
        default_ttl: 0,
    }
}

/// `system_auth.identity_to_roles`
pub fn auth_identity_to_roles_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::AUTH_KEYSPACE,
        name: "identity_to_roles",
        comment: "maps certificate identities to roles",
        columns: vec![
            SystemColumnSpec::partition_key("identity", CqlType::Varchar, 0),
            SystemColumnSpec::regular("role", CqlType::Varchar),
        ],
        gc_grace_seconds: 7776000,
        default_ttl: 0,
    }
}

/// `system_auth.resource_role_index`
pub fn auth_resource_role_index_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::AUTH_KEYSPACE,
        name: "resource_role_index",
        comment: "reverse index from resource to roles with permissions",
        columns: vec![
            SystemColumnSpec::partition_key("resource", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("role", CqlType::Varchar, 0),
        ],
        gc_grace_seconds: 7776000,
        default_ttl: 0,
    }
}

/// Returns all table definitions for the `system_auth` keyspace.
pub fn system_auth_tables() -> Vec<SystemTableDef> {
    vec![
        auth_roles_table(),
        auth_role_members_table(),
        auth_role_permissions_table(),
        auth_network_permissions_table(),
        auth_cidr_permissions_table(),
        auth_cidr_groups_table(),
        auth_identity_to_roles_table(),
        auth_resource_role_index_table(),
    ]
}

// ═══════════════════════════════════════════════════════════════════════════
// system_traces KEYSPACE
// ═══════════════════════════════════════════════════════════════════════════

/// `system_traces.sessions`
pub fn trace_sessions_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::TRACE_KEYSPACE,
        name: "sessions",
        comment: "tracing sessions",
        columns: vec![
            SystemColumnSpec::partition_key("session_id", CqlType::Uuid, 0),
            SystemColumnSpec::regular("client", CqlType::Inet),
            SystemColumnSpec::regular("command", CqlType::Varchar),
            SystemColumnSpec::regular("coordinator", CqlType::Inet),
            SystemColumnSpec::regular("coordinator_port", CqlType::Int),
            SystemColumnSpec::regular("duration", CqlType::Int),
            SystemColumnSpec::regular(
                "parameters",
                CqlType::Map(
                    Box::new(CqlType::Varchar),
                    Box::new(CqlType::Varchar),
                    false,
                ),
            ),
            SystemColumnSpec::regular("request", CqlType::Varchar),
            SystemColumnSpec::regular("started_at", CqlType::Timestamp),
        ],
        gc_grace_seconds: 0,
        default_ttl: 86400, // 24h
    }
}

/// `system_traces.events`
pub fn trace_events_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::TRACE_KEYSPACE,
        name: "events",
        comment: "tracing events",
        columns: vec![
            SystemColumnSpec::partition_key("session_id", CqlType::Uuid, 0),
            SystemColumnSpec::clustering("event_id", CqlType::Uuid, 0),
            SystemColumnSpec::regular("activity", CqlType::Varchar),
            SystemColumnSpec::regular("source", CqlType::Inet),
            SystemColumnSpec::regular("source_port", CqlType::Int),
            SystemColumnSpec::regular("source_elapsed", CqlType::Int),
            SystemColumnSpec::regular("thread", CqlType::Varchar),
        ],
        gc_grace_seconds: 0,
        default_ttl: 86400,
    }
}

/// Returns all table definitions for the `system_traces` keyspace.
pub fn system_traces_tables() -> Vec<SystemTableDef> {
    vec![trace_sessions_table(), trace_events_table()]
}

// ═══════════════════════════════════════════════════════════════════════════
// system_distributed KEYSPACE
// ═══════════════════════════════════════════════════════════════════════════

/// `system_distributed.repair_history`
pub fn distributed_repair_history_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::DISTRIBUTED_KEYSPACE,
        name: "repair_history",
        comment: "Repair history",
        columns: vec![
            SystemColumnSpec::partition_key("keyspace_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("columnfamily_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("id", CqlType::Uuid, 1),
            SystemColumnSpec::regular("coordinator", CqlType::Inet),
            SystemColumnSpec::regular("coordinator_port", CqlType::Int),
            SystemColumnSpec::regular("exception_message", CqlType::Varchar),
            SystemColumnSpec::regular("exception_stacktrace", CqlType::Varchar),
            SystemColumnSpec::regular("finished_at", CqlType::Timestamp),
            SystemColumnSpec::regular("participants", CqlType::Set(Box::new(CqlType::Inet), false)),
            SystemColumnSpec::regular(
                "participants_wp",
                CqlType::Set(Box::new(CqlType::Varchar), false),
            ),
            SystemColumnSpec::regular("range_begin", CqlType::Varchar),
            SystemColumnSpec::regular("range_end", CqlType::Varchar),
            SystemColumnSpec::regular("started_at", CqlType::Timestamp),
            SystemColumnSpec::regular("status", CqlType::Varchar),
        ],
        gc_grace_seconds: 0,
        default_ttl: 0,
    }
}

/// `system_distributed.parent_repair_history`
pub fn distributed_parent_repair_history_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::DISTRIBUTED_KEYSPACE,
        name: "parent_repair_history",
        comment: "Repair history",
        columns: vec![
            SystemColumnSpec::partition_key("parent_id", CqlType::Uuid, 0),
            SystemColumnSpec::regular(
                "columnfamily_names",
                CqlType::Set(Box::new(CqlType::Varchar), false),
            ),
            SystemColumnSpec::regular("exception_message", CqlType::Varchar),
            SystemColumnSpec::regular("exception_stacktrace", CqlType::Varchar),
            SystemColumnSpec::regular("finished_at", CqlType::Timestamp),
            SystemColumnSpec::regular("keyspace_name", CqlType::Varchar),
            SystemColumnSpec::regular(
                "requested_ranges",
                CqlType::Set(Box::new(CqlType::Varchar), false),
            ),
            SystemColumnSpec::regular("started_at", CqlType::Timestamp),
            SystemColumnSpec::regular(
                "successful_ranges",
                CqlType::Set(Box::new(CqlType::Varchar), false),
            ),
            SystemColumnSpec::regular(
                "options",
                CqlType::Map(
                    Box::new(CqlType::Varchar),
                    Box::new(CqlType::Varchar),
                    false,
                ),
            ),
        ],
        gc_grace_seconds: 0,
        default_ttl: 0,
    }
}

/// `system_distributed.view_build_status`
pub fn distributed_view_build_status_table() -> SystemTableDef {
    SystemTableDef {
        keyspace: schema_constants::DISTRIBUTED_KEYSPACE,
        name: "view_build_status",
        comment: "view build status",
        columns: vec![
            SystemColumnSpec::partition_key("keyspace_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("view_name", CqlType::Varchar, 0),
            SystemColumnSpec::clustering("host_id", CqlType::Uuid, 1),
            SystemColumnSpec::regular("status", CqlType::Varchar),
        ],
        gc_grace_seconds: 0,
        default_ttl: 0,
    }
}

/// Returns all table definitions for the `system_distributed` keyspace.
pub fn system_distributed_tables() -> Vec<SystemTableDef> {
    vec![
        distributed_repair_history_table(),
        distributed_parent_repair_history_table(),
        distributed_view_build_status_table(),
    ]
}

// ═══════════════════════════════════════════════════════════════════════════
// Keyspace metadata builders
// ═══════════════════════════════════════════════════════════════════════════

/// Build a full `KeyspaceMetadata` for the `system` keyspace.
pub fn build_system_keyspace() -> KeyspaceMetadata {
    let mut ks = KeyspaceMetadata::system(schema_constants::SYSTEM_KEYSPACE);
    for def in system_keyspace_tables() {
        ks = ks.with_table(def.to_table_metadata());
    }
    ks
}

/// Build a full `KeyspaceMetadata` for the `system_schema` keyspace.
pub fn build_system_schema_keyspace() -> KeyspaceMetadata {
    let mut ks = KeyspaceMetadata::system(schema_constants::SCHEMA_KEYSPACE);
    for def in system_schema_tables() {
        ks = ks.with_table(def.to_table_metadata());
    }
    ks
}

/// Build a full `KeyspaceMetadata` for the `system_auth` keyspace.
pub fn build_system_auth_keyspace() -> KeyspaceMetadata {
    let mut ks = KeyspaceMetadata::new(
        schema_constants::AUTH_KEYSPACE,
        KeyspaceParams {
            replication: ReplicationParams::simple(1),
            durable_writes: true,
        },
    );
    for def in system_auth_tables() {
        ks = ks.with_table(def.to_table_metadata());
    }
    ks
}

/// Build a full `KeyspaceMetadata` for the `system_traces` keyspace.
pub fn build_system_traces_keyspace() -> KeyspaceMetadata {
    let mut ks = KeyspaceMetadata::new(
        schema_constants::TRACE_KEYSPACE,
        KeyspaceParams {
            replication: ReplicationParams::simple(2),
            durable_writes: true,
        },
    );
    for def in system_traces_tables() {
        ks = ks.with_table(def.to_table_metadata());
    }
    ks
}

/// Build a full `KeyspaceMetadata` for the `system_distributed` keyspace.
pub fn build_system_distributed_keyspace() -> KeyspaceMetadata {
    let mut ks = KeyspaceMetadata::new(
        schema_constants::DISTRIBUTED_KEYSPACE,
        KeyspaceParams {
            replication: ReplicationParams::simple(3),
            durable_writes: true,
        },
    );
    for def in system_distributed_tables() {
        ks = ks.with_table(def.to_table_metadata());
    }
    ks
}

/// Build all system keyspaces and return them.
pub fn all_system_keyspaces() -> Vec<KeyspaceMetadata> {
    vec![
        build_system_keyspace(),
        build_system_schema_keyspace(),
        build_system_auth_keyspace(),
        build_system_traces_keyspace(),
        build_system_distributed_keyspace(),
    ]
}

// ═══════════════════════════════════════════════════════════════════════════
// Bootstrap state (matches Java SystemKeyspace.BootstrapState)
// ═══════════════════════════════════════════════════════════════════════════

/// Bootstrap state of a Cassandra node.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum BootstrapState {
    NeedsBootstrap,
    Completed,
    InProgress,
    Decommissioned,
}

impl std::fmt::Display for BootstrapState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::NeedsBootstrap => write!(f, "NEEDS_BOOTSTRAP"),
            Self::Completed => write!(f, "COMPLETED"),
            Self::InProgress => write!(f, "IN_PROGRESS"),
            Self::Decommissioned => write!(f, "DECOMMISSIONED"),
        }
    }
}

impl std::str::FromStr for BootstrapState {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "NEEDS_BOOTSTRAP" => Ok(Self::NeedsBootstrap),
            "COMPLETED" => Ok(Self::Completed),
            "IN_PROGRESS" => Ok(Self::InProgress),
            "DECOMMISSIONED" => Ok(Self::Decommissioned),
            _ => Err(format!("unknown bootstrap state: {}", s)),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_keyspace_has_all_tables() {
        let tables = system_keyspace_tables();
        // Java baseline has 22+ tables (excluding deprecated duplicates added separately)
        assert!(
            tables.len() >= 22,
            "expected >=22 system tables, got {}",
            tables.len()
        );

        let names: Vec<&str> = tables.iter().map(|t| t.name).collect();
        // Spot-check critical tables
        assert!(names.contains(&"local"), "missing system.local");
        assert!(names.contains(&"peers_v2"), "missing system.peers_v2");
        assert!(names.contains(&"peers"), "missing system.peers (legacy)");
        assert!(names.contains(&"batches"), "missing system.batches");
        assert!(names.contains(&"paxos"), "missing system.paxos");
        assert!(
            names.contains(&"compaction_history"),
            "missing system.compaction_history"
        );
        assert!(names.contains(&"repairs"), "missing system.repairs");
        assert!(
            names.contains(&"prepared_statements"),
            "missing system.prepared_statements"
        );
        assert!(
            names.contains(&"table_estimates"),
            "missing system.table_estimates"
        );
        assert!(
            names.contains(&"size_estimates"),
            "missing system.size_estimates (legacy)"
        );
    }

    #[test]
    fn system_schema_has_all_tables() {
        let tables = system_schema_tables();
        assert_eq!(
            tables.len(),
            11,
            "expected 11 system_schema tables, got {}",
            tables.len()
        );

        let names: Vec<&str> = tables.iter().map(|t| t.name).collect();
        assert!(names.contains(&"keyspaces"));
        assert!(names.contains(&"tables"));
        assert!(names.contains(&"columns"));
        assert!(names.contains(&"indexes"));
        assert!(names.contains(&"types"));
        assert!(names.contains(&"functions"));
        assert!(names.contains(&"aggregates"));
        assert!(names.contains(&"views"));
        assert!(names.contains(&"triggers"));
        assert!(names.contains(&"dropped_columns"));
        assert!(names.contains(&"column_masks"));
    }

    #[test]
    fn system_auth_has_all_tables() {
        let tables = system_auth_tables();
        assert!(tables.len() >= 8);
        let names: Vec<&str> = tables.iter().map(|t| t.name).collect();
        assert!(names.contains(&"roles"));
        assert!(names.contains(&"role_members"));
        assert!(names.contains(&"role_permissions"));
        assert!(names.contains(&"network_permissions"));
    }

    #[test]
    fn system_traces_has_all_tables() {
        let tables = system_traces_tables();
        assert_eq!(tables.len(), 2);
    }

    #[test]
    fn system_distributed_has_all_tables() {
        let tables = system_distributed_tables();
        assert_eq!(tables.len(), 3);
    }

    #[test]
    fn all_keyspaces_returned() {
        let kss = all_system_keyspaces();
        assert_eq!(kss.len(), 5);
        let names: Vec<&str> = kss.iter().map(|ks| ks.name.as_str()).collect();
        assert!(names.contains(&"system"));
        assert!(names.contains(&"system_schema"));
        assert!(names.contains(&"system_auth"));
        assert!(names.contains(&"system_traces"));
        assert!(names.contains(&"system_distributed"));
    }

    #[test]
    fn to_table_metadata_works() {
        let def = local_table();
        let meta = def.to_table_metadata();
        assert_eq!(meta.name, "local");
        assert!(meta.columns.len() >= 20);
    }

    #[test]
    fn bootstrap_state_roundtrip() {
        for state in [
            BootstrapState::NeedsBootstrap,
            BootstrapState::Completed,
            BootstrapState::InProgress,
            BootstrapState::Decommissioned,
        ] {
            let s = state.to_string();
            let parsed: BootstrapState = s.parse().unwrap();
            assert_eq!(parsed, state);
        }
    }

    #[test]
    fn system_keyspace_metadata_is_local_strategy() {
        let ks = build_system_keyspace();
        assert!(
            ks.params
                .replication
                .strategy_class
                .contains("LocalStrategy")
        );
    }

    #[test]
    fn auth_keyspace_is_replicated() {
        let ks = build_system_auth_keyspace();
        assert!(
            ks.params
                .replication
                .strategy_class
                .contains("SimpleStrategy")
        );
    }

    #[test]
    fn table_names_unique_within_keyspace() {
        for ks in all_system_keyspaces() {
            let table_names: Vec<&str> = ks.tables.keys().map(|s| s.as_str()).collect();
            let unique: std::collections::HashSet<&str> = table_names.iter().copied().collect();
            assert_eq!(
                table_names.len(),
                unique.len(),
                "duplicate table names in keyspace {}",
                ks.name
            );
        }
    }

    #[test]
    fn all_tables_have_partition_key() {
        for ks in all_system_keyspaces() {
            for (tname, table) in &ks.tables {
                let has_pk = table
                    .columns
                    .iter()
                    .any(|c| c.kind == ColumnKind::PartitionKey);
                assert!(has_pk, "table {}.{} has no partition key", ks.name, tname);
            }
        }
    }
}
