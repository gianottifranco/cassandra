// Licensed under Apache License, Version 2.0.

//! Virtual tables framework and built-in system views.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.virtual.*`
//! - `system_views.local`, `system_views.peers`, etc.
//!
//! Virtual tables are read-only views into server internal state,
//! exposed to CQL queries. This module implements the full set of
//! virtual tables matching the Java baseline.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

// ─── Virtual Column ────────────────────────────────────────────────────────

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VirtualColumn {
    pub name: String,
    pub cql_type: String,
}

// ─── Virtual Table Trait ───────────────────────────────────────────────────

/// A virtual table exposes server-internal state as CQL-queryable rows.
pub trait VirtualTable: Send + Sync {
    /// Keyspace name (e.g., "system_views").
    fn keyspace(&self) -> &str { "system_views" }

    /// Table name.
    fn name(&self) -> &str;

    /// Column definitions.
    fn columns(&self) -> Vec<VirtualColumn>;

    /// Rows of data. Each row is a map of column name → string value.
    fn rows(&self) -> Vec<HashMap<String, String>>;
}

// ─── Virtual Table Registry ────────────────────────────────────────────────

/// Registry of all virtual tables.
pub struct VirtualTableRegistry {
    tables: HashMap<String, Box<dyn VirtualTable>>,
}

impl VirtualTableRegistry {
    pub fn new() -> Self {
        Self {
            tables: HashMap::new(),
        }
    }

    /// Register a virtual table.
    pub fn register(&mut self, table: Box<dyn VirtualTable>) {
        let key = format!("{}.{}", table.keyspace(), table.name());
        self.tables.insert(key, table);
    }

    /// Get a virtual table by keyspace.name.
    pub fn get(&self, keyspace: &str, name: &str) -> Option<&dyn VirtualTable> {
        let key = format!("{}.{}", keyspace, name);
        self.tables.get(&key).map(|b| b.as_ref())
    }

    /// List all registered virtual table names.
    pub fn list_tables(&self) -> Vec<String> {
        self.tables.keys().cloned().collect()
    }

    /// Create a registry with all built-in virtual tables.
    pub fn with_builtins() -> Self {
        let mut reg = Self::new();
        // ─── Core ──────────────────────────────────────────────────────
        reg.register(Box::new(LocalInfoTable::default()));
        reg.register(Box::new(SettingsTable::default()));
        reg.register(Box::new(ThreadPoolsTable::default()));
        reg.register(Box::new(SstableTasksTable::default()));
        reg.register(Box::new(ClientsTable::default()));
        // ─── Cluster State ─────────────────────────────────────────────
        reg.register(Box::new(GossipInfoTable::default()));
        reg.register(Box::new(CachesTable::default()));
        reg.register(Box::new(SnapshotsTable::default()));
        // ─── Networking ────────────────────────────────────────────────
        reg.register(Box::new(InternodeInboundTable::default()));
        reg.register(Box::new(InternodeOutboundTable::default()));
        reg.register(Box::new(StreamingTable::default()));
        // ─── Diagnostics ───────────────────────────────────────────────
        reg.register(Box::new(LogMessagesTable::default()));
        reg.register(Box::new(QueriesTable::default()));
        reg.register(Box::new(SlowQueriesTable::default()));
        reg.register(Box::new(SystemPropertiesTable::default()));
        // ─── Hints & Repair ────────────────────────────────────────────
        reg.register(Box::new(PendingHintsTable::default()));
        reg.register(Box::new(LocalRepairTable::default()));
        // ─── Metrics ───────────────────────────────────────────────────
        reg.register(Box::new(BatchMetricsTable::default()));
        reg.register(Box::new(CqlMetricsTable::default()));
        // ─── Security Caches ───────────────────────────────────────────
        reg.register(Box::new(CredentialsCacheKeysTable::default()));
        reg.register(Box::new(PermissionsCacheKeysTable::default()));
        reg.register(Box::new(RolesCacheKeysTable::default()));
        reg
    }
}

impl Default for VirtualTableRegistry {
    fn default() -> Self {
        Self::with_builtins()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// Built-in Virtual Tables
// ═══════════════════════════════════════════════════════════════════════════

// ─── system_views.local ────────────────────────────────────────────────────

/// `system_views.local` — local node information.
pub struct LocalInfoTable {
    pub host_id: String,
    pub cluster_name: String,
    pub data_center: String,
    pub rack: String,
    pub listen_address: String,
    pub native_transport_port: u16,
    pub release_version: String,
}

impl Default for LocalInfoTable {
    fn default() -> Self {
        Self {
            host_id: uuid::Uuid::new_v4().to_string(),
            cluster_name: "Test Cluster".into(),
            data_center: "datacenter1".into(),
            rack: "rack1".into(),
            listen_address: "127.0.0.1".into(),
            native_transport_port: 9042,
            release_version: env!("CARGO_PKG_VERSION").into(),
        }
    }
}

impl VirtualTable for LocalInfoTable {
    fn name(&self) -> &str { "local" }

    fn columns(&self) -> Vec<VirtualColumn> {
        vec![
            VirtualColumn { name: "host_id".into(), cql_type: "uuid".into() },
            VirtualColumn { name: "cluster_name".into(), cql_type: "text".into() },
            VirtualColumn { name: "data_center".into(), cql_type: "text".into() },
            VirtualColumn { name: "rack".into(), cql_type: "text".into() },
            VirtualColumn { name: "listen_address".into(), cql_type: "inet".into() },
            VirtualColumn { name: "native_transport_port".into(), cql_type: "int".into() },
            VirtualColumn { name: "release_version".into(), cql_type: "text".into() },
        ]
    }

    fn rows(&self) -> Vec<HashMap<String, String>> {
        let mut row = HashMap::new();
        row.insert("host_id".into(), self.host_id.clone());
        row.insert("cluster_name".into(), self.cluster_name.clone());
        row.insert("data_center".into(), self.data_center.clone());
        row.insert("rack".into(), self.rack.clone());
        row.insert("listen_address".into(), self.listen_address.clone());
        row.insert("native_transport_port".into(), self.native_transport_port.to_string());
        row.insert("release_version".into(), self.release_version.clone());
        vec![row]
    }
}

// ─── system_views.settings ─────────────────────────────────────────────────

/// `system_views.settings` — current server configuration values.
pub struct SettingsTable {
    pub settings: Vec<(String, String)>,
}

impl Default for SettingsTable {
    fn default() -> Self {
        Self {
            settings: vec![
                ("cluster_name".into(), "Test Cluster".into()),
                ("partitioner".into(), "org.apache.cassandra.dht.Murmur3Partitioner".into()),
                ("native_transport_port".into(), "9042".into()),
                ("storage_port".into(), "7000".into()),
                ("commitlog_sync".into(), "periodic".into()),
                ("concurrent_reads".into(), "32".into()),
                ("concurrent_writes".into(), "32".into()),
            ],
        }
    }
}

impl VirtualTable for SettingsTable {
    fn name(&self) -> &str { "settings" }

    fn columns(&self) -> Vec<VirtualColumn> {
        vec![
            VirtualColumn { name: "name".into(), cql_type: "text".into() },
            VirtualColumn { name: "value".into(), cql_type: "text".into() },
        ]
    }

    fn rows(&self) -> Vec<HashMap<String, String>> {
        self.settings
            .iter()
            .map(|(k, v)| {
                let mut row = HashMap::new();
                row.insert("name".into(), k.clone());
                row.insert("value".into(), v.clone());
                row
            })
            .collect()
    }
}

// ─── system_views.thread_pools ─────────────────────────────────────────────

/// `system_views.thread_pools` — thread pool statistics.
pub struct ThreadPoolsTable {
    pub pools: Vec<ThreadPoolInfo>,
}

#[derive(Debug, Clone)]
pub struct ThreadPoolInfo {
    pub name: String,
    pub active_tasks: u64,
    pub pending_tasks: u64,
    pub completed_tasks: u64,
    pub blocked_tasks: u64,
    pub max_pool_size: u64,
}

impl Default for ThreadPoolsTable {
    fn default() -> Self {
        Self {
            pools: vec![
                ThreadPoolInfo {
                    name: "ReadStage".into(),
                    active_tasks: 0,
                    pending_tasks: 0,
                    completed_tasks: 0,
                    blocked_tasks: 0,
                    max_pool_size: 32,
                },
                ThreadPoolInfo {
                    name: "MutationStage".into(),
                    active_tasks: 0,
                    pending_tasks: 0,
                    completed_tasks: 0,
                    blocked_tasks: 0,
                    max_pool_size: 32,
                },
                ThreadPoolInfo {
                    name: "CompactionExecutor".into(),
                    active_tasks: 0,
                    pending_tasks: 0,
                    completed_tasks: 0,
                    blocked_tasks: 0,
                    max_pool_size: 4,
                },
            ],
        }
    }
}

impl VirtualTable for ThreadPoolsTable {
    fn name(&self) -> &str { "thread_pools" }

    fn columns(&self) -> Vec<VirtualColumn> {
        vec![
            VirtualColumn { name: "name".into(), cql_type: "text".into() },
            VirtualColumn { name: "active_tasks".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "pending_tasks".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "completed_tasks".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "blocked_tasks".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "max_pool_size".into(), cql_type: "bigint".into() },
        ]
    }

    fn rows(&self) -> Vec<HashMap<String, String>> {
        self.pools
            .iter()
            .map(|p| {
                let mut row = HashMap::new();
                row.insert("name".into(), p.name.clone());
                row.insert("active_tasks".into(), p.active_tasks.to_string());
                row.insert("pending_tasks".into(), p.pending_tasks.to_string());
                row.insert("completed_tasks".into(), p.completed_tasks.to_string());
                row.insert("blocked_tasks".into(), p.blocked_tasks.to_string());
                row.insert("max_pool_size".into(), p.max_pool_size.to_string());
                row
            })
            .collect()
    }
}

// ─── system_views.sstable_tasks ────────────────────────────────────────────

/// `system_views.sstable_tasks` — active SSTable compaction/streaming tasks.
pub struct SstableTasksTable {
    pub tasks: Vec<SstableTask>,
}

#[derive(Debug, Clone)]
pub struct SstableTask {
    pub keyspace: String,
    pub table: String,
    pub task_id: String,
    pub kind: String,
    pub progress: f64,
}

impl Default for SstableTasksTable {
    fn default() -> Self {
        Self { tasks: Vec::new() }
    }
}

impl VirtualTable for SstableTasksTable {
    fn name(&self) -> &str { "sstable_tasks" }

    fn columns(&self) -> Vec<VirtualColumn> {
        vec![
            VirtualColumn { name: "keyspace_name".into(), cql_type: "text".into() },
            VirtualColumn { name: "table_name".into(), cql_type: "text".into() },
            VirtualColumn { name: "task_id".into(), cql_type: "uuid".into() },
            VirtualColumn { name: "kind".into(), cql_type: "text".into() },
            VirtualColumn { name: "progress".into(), cql_type: "double".into() },
        ]
    }

    fn rows(&self) -> Vec<HashMap<String, String>> {
        self.tasks
            .iter()
            .map(|t| {
                let mut row = HashMap::new();
                row.insert("keyspace_name".into(), t.keyspace.clone());
                row.insert("table_name".into(), t.table.clone());
                row.insert("task_id".into(), t.task_id.clone());
                row.insert("kind".into(), t.kind.clone());
                row.insert("progress".into(), format!("{:.1}", t.progress));
                row
            })
            .collect()
    }
}

// ─── system_views.clients ──────────────────────────────────────────────────

/// `system_views.clients` — connected native protocol clients.
pub struct ClientsTable {
    pub clients: Vec<ClientInfo>,
}

#[derive(Debug, Clone)]
pub struct ClientInfo {
    pub address: String,
    pub port: u16,
    pub username: String,
    pub connection_stage: String,
    pub protocol_version: u8,
    pub ssl: bool,
}

impl Default for ClientsTable {
    fn default() -> Self {
        Self { clients: Vec::new() }
    }
}

impl VirtualTable for ClientsTable {
    fn name(&self) -> &str { "clients" }

    fn columns(&self) -> Vec<VirtualColumn> {
        vec![
            VirtualColumn { name: "address".into(), cql_type: "inet".into() },
            VirtualColumn { name: "port".into(), cql_type: "int".into() },
            VirtualColumn { name: "username".into(), cql_type: "text".into() },
            VirtualColumn { name: "connection_stage".into(), cql_type: "text".into() },
            VirtualColumn { name: "protocol_version".into(), cql_type: "int".into() },
            VirtualColumn { name: "ssl".into(), cql_type: "boolean".into() },
        ]
    }

    fn rows(&self) -> Vec<HashMap<String, String>> {
        self.clients
            .iter()
            .map(|c| {
                let mut row = HashMap::new();
                row.insert("address".into(), c.address.clone());
                row.insert("port".into(), c.port.to_string());
                row.insert("username".into(), c.username.clone());
                row.insert("connection_stage".into(), c.connection_stage.clone());
                row.insert("protocol_version".into(), c.protocol_version.to_string());
                row.insert("ssl".into(), c.ssl.to_string());
                row
            })
            .collect()
    }
}

// ═══════════════════════════════════════════════════════════════════════════
// NEW Virtual Tables (matching Java system_views)
// ═══════════════════════════════════════════════════════════════════════════

// ─── system_views.gossip_info ──────────────────────────────────────────────

/// `system_views.gossip_info` — gossip state for all known endpoints.
#[derive(Default)]
pub struct GossipInfoTable {
    pub endpoints: Vec<GossipEndpointInfo>,
}

#[derive(Debug, Clone)]
pub struct GossipEndpointInfo {
    pub address: String,
    pub port: u16,
    pub hostname: String,
    pub generation: i64,
    pub heartbeat: i64,
    pub status: String,
    pub load: String,
    pub data_center: String,
    pub rack: String,
    pub release_version: String,
    pub host_id: String,
}

impl VirtualTable for GossipInfoTable {
    fn name(&self) -> &str { "gossip_info" }
    fn columns(&self) -> Vec<VirtualColumn> {
        vec![
            VirtualColumn { name: "address".into(), cql_type: "inet".into() },
            VirtualColumn { name: "port".into(), cql_type: "int".into() },
            VirtualColumn { name: "hostname".into(), cql_type: "text".into() },
            VirtualColumn { name: "generation".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "heartbeat".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "status".into(), cql_type: "text".into() },
            VirtualColumn { name: "load".into(), cql_type: "text".into() },
            VirtualColumn { name: "data_center".into(), cql_type: "text".into() },
            VirtualColumn { name: "rack".into(), cql_type: "text".into() },
            VirtualColumn { name: "release_version".into(), cql_type: "text".into() },
            VirtualColumn { name: "host_id".into(), cql_type: "text".into() },
        ]
    }
    fn rows(&self) -> Vec<HashMap<String, String>> {
        self.endpoints.iter().map(|e| {
            let mut row = HashMap::new();
            row.insert("address".into(), e.address.clone());
            row.insert("port".into(), e.port.to_string());
            row.insert("hostname".into(), e.hostname.clone());
            row.insert("generation".into(), e.generation.to_string());
            row.insert("heartbeat".into(), e.heartbeat.to_string());
            row.insert("status".into(), e.status.clone());
            row.insert("load".into(), e.load.clone());
            row.insert("data_center".into(), e.data_center.clone());
            row.insert("rack".into(), e.rack.clone());
            row.insert("release_version".into(), e.release_version.clone());
            row.insert("host_id".into(), e.host_id.clone());
            row
        }).collect()
    }
}

// ─── system_views.caches ───────────────────────────────────────────────────

/// `system_views.caches` — cache statistics.
#[derive(Default)]
pub struct CachesTable {
    pub caches: Vec<CacheInfo>,
}

#[derive(Debug, Clone)]
pub struct CacheInfo {
    pub name: String,
    pub capacity_bytes: u64,
    pub size_bytes: u64,
    pub entries: u64,
    pub hit_count: u64,
    pub hit_ratio: f64,
    pub miss_count: u64,
    pub request_count: u64,
}

impl VirtualTable for CachesTable {
    fn name(&self) -> &str { "caches" }
    fn columns(&self) -> Vec<VirtualColumn> {
        vec![
            VirtualColumn { name: "name".into(), cql_type: "text".into() },
            VirtualColumn { name: "capacity_bytes".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "size_bytes".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "entries".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "hit_count".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "hit_ratio".into(), cql_type: "double".into() },
            VirtualColumn { name: "miss_count".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "request_count".into(), cql_type: "bigint".into() },
        ]
    }
    fn rows(&self) -> Vec<HashMap<String, String>> {
        self.caches.iter().map(|c| {
            let mut row = HashMap::new();
            row.insert("name".into(), c.name.clone());
            row.insert("capacity_bytes".into(), c.capacity_bytes.to_string());
            row.insert("size_bytes".into(), c.size_bytes.to_string());
            row.insert("entries".into(), c.entries.to_string());
            row.insert("hit_count".into(), c.hit_count.to_string());
            row.insert("hit_ratio".into(), format!("{:.4}", c.hit_ratio));
            row.insert("miss_count".into(), c.miss_count.to_string());
            row.insert("request_count".into(), c.request_count.to_string());
            row
        }).collect()
    }
}

// ─── system_views.internode_inbound ────────────────────────────────────────

/// `system_views.internode_inbound` — inbound internode messaging stats.
#[derive(Default)]
pub struct InternodeInboundTable {
    pub connections: Vec<InternodeInboundInfo>,
}

#[derive(Debug, Clone)]
pub struct InternodeInboundInfo {
    pub address: String,
    pub port: u16,
    pub dc: String,
    pub rack: String,
    pub using_bytes: u64,
    pub using_reserve_bytes: u64,
    pub corrupt_frames_recovered: u64,
    pub corrupt_frames_unrecovered: u64,
}

impl VirtualTable for InternodeInboundTable {
    fn name(&self) -> &str { "internode_inbound" }
    fn columns(&self) -> Vec<VirtualColumn> {
        vec![
            VirtualColumn { name: "address".into(), cql_type: "inet".into() },
            VirtualColumn { name: "port".into(), cql_type: "int".into() },
            VirtualColumn { name: "dc".into(), cql_type: "text".into() },
            VirtualColumn { name: "rack".into(), cql_type: "text".into() },
            VirtualColumn { name: "using_bytes".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "using_reserve_bytes".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "corrupt_frames_recovered".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "corrupt_frames_unrecovered".into(), cql_type: "bigint".into() },
        ]
    }
    fn rows(&self) -> Vec<HashMap<String, String>> {
        self.connections.iter().map(|c| {
            let mut row = HashMap::new();
            row.insert("address".into(), c.address.clone());
            row.insert("port".into(), c.port.to_string());
            row.insert("dc".into(), c.dc.clone());
            row.insert("rack".into(), c.rack.clone());
            row.insert("using_bytes".into(), c.using_bytes.to_string());
            row.insert("using_reserve_bytes".into(), c.using_reserve_bytes.to_string());
            row.insert("corrupt_frames_recovered".into(), c.corrupt_frames_recovered.to_string());
            row.insert("corrupt_frames_unrecovered".into(), c.corrupt_frames_unrecovered.to_string());
            row
        }).collect()
    }
}

// ─── system_views.internode_outbound ───────────────────────────────────────

/// `system_views.internode_outbound` — outbound internode messaging stats.
#[derive(Default)]
pub struct InternodeOutboundTable {
    pub connections: Vec<InternodeOutboundInfo>,
}

#[derive(Debug, Clone)]
pub struct InternodeOutboundInfo {
    pub address: String,
    pub port: u16,
    pub dc: String,
    pub rack: String,
    pub msg_type: String,
    pub pending_count: u64,
    pub pending_bytes: u64,
    pub sent_count: u64,
    pub sent_bytes: u64,
    pub expired_count: u64,
    pub expired_bytes: u64,
    pub error_count: u64,
    pub error_bytes: u64,
}

impl VirtualTable for InternodeOutboundTable {
    fn name(&self) -> &str { "internode_outbound" }
    fn columns(&self) -> Vec<VirtualColumn> {
        vec![
            VirtualColumn { name: "address".into(), cql_type: "inet".into() },
            VirtualColumn { name: "port".into(), cql_type: "int".into() },
            VirtualColumn { name: "dc".into(), cql_type: "text".into() },
            VirtualColumn { name: "rack".into(), cql_type: "text".into() },
            VirtualColumn { name: "msg_type".into(), cql_type: "text".into() },
            VirtualColumn { name: "pending_count".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "pending_bytes".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "sent_count".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "sent_bytes".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "expired_count".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "expired_bytes".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "error_count".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "error_bytes".into(), cql_type: "bigint".into() },
        ]
    }
    fn rows(&self) -> Vec<HashMap<String, String>> {
        self.connections.iter().map(|c| {
            let mut row = HashMap::new();
            row.insert("address".into(), c.address.clone());
            row.insert("port".into(), c.port.to_string());
            row.insert("dc".into(), c.dc.clone());
            row.insert("rack".into(), c.rack.clone());
            row.insert("msg_type".into(), c.msg_type.clone());
            row.insert("pending_count".into(), c.pending_count.to_string());
            row.insert("pending_bytes".into(), c.pending_bytes.to_string());
            row.insert("sent_count".into(), c.sent_count.to_string());
            row.insert("sent_bytes".into(), c.sent_bytes.to_string());
            row.insert("expired_count".into(), c.expired_count.to_string());
            row.insert("expired_bytes".into(), c.expired_bytes.to_string());
            row.insert("error_count".into(), c.error_count.to_string());
            row.insert("error_bytes".into(), c.error_bytes.to_string());
            row
        }).collect()
    }
}

// ─── system_views.streaming ────────────────────────────────────────────────

/// `system_views.streaming` — active streaming sessions.
#[derive(Default)]
pub struct StreamingTable {
    pub sessions: Vec<StreamingInfo>,
}

#[derive(Debug, Clone)]
pub struct StreamingInfo {
    pub peer: String,
    pub peer_port: u16,
    pub session_id: String,
    pub direction: String,
    pub files_sent: u64,
    pub files_to_send: u64,
    pub bytes_sent: u64,
    pub bytes_to_send: u64,
    pub files_received: u64,
    pub files_to_receive: u64,
    pub bytes_received: u64,
    pub bytes_to_receive: u64,
}

impl VirtualTable for StreamingTable {
    fn name(&self) -> &str { "streaming" }
    fn columns(&self) -> Vec<VirtualColumn> {
        vec![
            VirtualColumn { name: "peer".into(), cql_type: "inet".into() },
            VirtualColumn { name: "peer_port".into(), cql_type: "int".into() },
            VirtualColumn { name: "session_id".into(), cql_type: "uuid".into() },
            VirtualColumn { name: "direction".into(), cql_type: "text".into() },
            VirtualColumn { name: "files_sent".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "files_to_send".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "bytes_sent".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "bytes_to_send".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "files_received".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "files_to_receive".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "bytes_received".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "bytes_to_receive".into(), cql_type: "bigint".into() },
        ]
    }
    fn rows(&self) -> Vec<HashMap<String, String>> {
        self.sessions.iter().map(|s| {
            let mut row = HashMap::new();
            row.insert("peer".into(), s.peer.clone());
            row.insert("peer_port".into(), s.peer_port.to_string());
            row.insert("session_id".into(), s.session_id.clone());
            row.insert("direction".into(), s.direction.clone());
            row.insert("files_sent".into(), s.files_sent.to_string());
            row.insert("files_to_send".into(), s.files_to_send.to_string());
            row.insert("bytes_sent".into(), s.bytes_sent.to_string());
            row.insert("bytes_to_send".into(), s.bytes_to_send.to_string());
            row.insert("files_received".into(), s.files_received.to_string());
            row.insert("files_to_receive".into(), s.files_to_receive.to_string());
            row.insert("bytes_received".into(), s.bytes_received.to_string());
            row.insert("bytes_to_receive".into(), s.bytes_to_receive.to_string());
            row
        }).collect()
    }
}

// ─── system_views.snapshots ────────────────────────────────────────────────

/// `system_views.snapshots` — available snapshots.
#[derive(Default)]
pub struct SnapshotsTable {
    pub snapshots: Vec<SnapshotInfo>,
}

#[derive(Debug, Clone)]
pub struct SnapshotInfo {
    pub keyspace_name: String,
    pub table_name: String,
    pub snapshot_name: String,
    pub true_size: u64,
    pub size_on_disk: u64,
    pub created_at: String,
    pub expires_at: String,
    pub ephemeral: bool,
}

impl VirtualTable for SnapshotsTable {
    fn name(&self) -> &str { "snapshots" }
    fn columns(&self) -> Vec<VirtualColumn> {
        vec![
            VirtualColumn { name: "keyspace_name".into(), cql_type: "text".into() },
            VirtualColumn { name: "table_name".into(), cql_type: "text".into() },
            VirtualColumn { name: "snapshot_name".into(), cql_type: "text".into() },
            VirtualColumn { name: "true_size".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "size_on_disk".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "created_at".into(), cql_type: "text".into() },
            VirtualColumn { name: "expires_at".into(), cql_type: "text".into() },
            VirtualColumn { name: "ephemeral".into(), cql_type: "boolean".into() },
        ]
    }
    fn rows(&self) -> Vec<HashMap<String, String>> {
        self.snapshots.iter().map(|s| {
            let mut row = HashMap::new();
            row.insert("keyspace_name".into(), s.keyspace_name.clone());
            row.insert("table_name".into(), s.table_name.clone());
            row.insert("snapshot_name".into(), s.snapshot_name.clone());
            row.insert("true_size".into(), s.true_size.to_string());
            row.insert("size_on_disk".into(), s.size_on_disk.to_string());
            row.insert("created_at".into(), s.created_at.clone());
            row.insert("expires_at".into(), s.expires_at.clone());
            row.insert("ephemeral".into(), s.ephemeral.to_string());
            row
        }).collect()
    }
}

// ─── system_views.log_messages ─────────────────────────────────────────────

/// `system_views.system_logs` — recent log messages.
#[derive(Default)]
pub struct LogMessagesTable {
    pub messages: Vec<LogMessage>,
}

#[derive(Debug, Clone)]
pub struct LogMessage {
    pub timestamp: String,
    pub logger: String,
    pub level: String,
    pub message: String,
}

impl VirtualTable for LogMessagesTable {
    fn name(&self) -> &str { "system_logs" }
    fn columns(&self) -> Vec<VirtualColumn> {
        vec![
            VirtualColumn { name: "timestamp".into(), cql_type: "timestamp".into() },
            VirtualColumn { name: "logger".into(), cql_type: "text".into() },
            VirtualColumn { name: "level".into(), cql_type: "text".into() },
            VirtualColumn { name: "message".into(), cql_type: "text".into() },
        ]
    }
    fn rows(&self) -> Vec<HashMap<String, String>> {
        self.messages.iter().map(|m| {
            let mut row = HashMap::new();
            row.insert("timestamp".into(), m.timestamp.clone());
            row.insert("logger".into(), m.logger.clone());
            row.insert("level".into(), m.level.clone());
            row.insert("message".into(), m.message.clone());
            row
        }).collect()
    }
}

// ─── system_views.queries ──────────────────────────────────────────────────

/// `system_views.queries` — currently executing queries.
#[derive(Default)]
pub struct QueriesTable {
    pub queries: Vec<QueryInfo>,
}

#[derive(Debug, Clone)]
pub struct QueryInfo {
    pub thread_id: String,
    pub duration_millis: u64,
    pub query: String,
    pub client_address: String,
}

impl VirtualTable for QueriesTable {
    fn name(&self) -> &str { "queries" }
    fn columns(&self) -> Vec<VirtualColumn> {
        vec![
            VirtualColumn { name: "thread_id".into(), cql_type: "text".into() },
            VirtualColumn { name: "duration_millis".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "query".into(), cql_type: "text".into() },
            VirtualColumn { name: "client_address".into(), cql_type: "inet".into() },
        ]
    }
    fn rows(&self) -> Vec<HashMap<String, String>> {
        self.queries.iter().map(|q| {
            let mut row = HashMap::new();
            row.insert("thread_id".into(), q.thread_id.clone());
            row.insert("duration_millis".into(), q.duration_millis.to_string());
            row.insert("query".into(), q.query.clone());
            row.insert("client_address".into(), q.client_address.clone());
            row
        }).collect()
    }
}

// ─── system_views.slow_queries ─────────────────────────────────────────────

/// `system_views.slow_queries` — recently observed slow queries.
#[derive(Default)]
pub struct SlowQueriesTable {
    pub queries: Vec<SlowQueryInfo>,
}

#[derive(Debug, Clone)]
pub struct SlowQueryInfo {
    pub keyspace_name: String,
    pub table_name: String,
    pub query: String,
    pub duration_millis: u64,
    pub client_address: String,
    pub timestamp: String,
}

impl VirtualTable for SlowQueriesTable {
    fn name(&self) -> &str { "slow_queries" }
    fn columns(&self) -> Vec<VirtualColumn> {
        vec![
            VirtualColumn { name: "keyspace_name".into(), cql_type: "text".into() },
            VirtualColumn { name: "table_name".into(), cql_type: "text".into() },
            VirtualColumn { name: "query".into(), cql_type: "text".into() },
            VirtualColumn { name: "duration_millis".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "client_address".into(), cql_type: "inet".into() },
            VirtualColumn { name: "timestamp".into(), cql_type: "timestamp".into() },
        ]
    }
    fn rows(&self) -> Vec<HashMap<String, String>> {
        self.queries.iter().map(|q| {
            let mut row = HashMap::new();
            row.insert("keyspace_name".into(), q.keyspace_name.clone());
            row.insert("table_name".into(), q.table_name.clone());
            row.insert("query".into(), q.query.clone());
            row.insert("duration_millis".into(), q.duration_millis.to_string());
            row.insert("client_address".into(), q.client_address.clone());
            row.insert("timestamp".into(), q.timestamp.clone());
            row
        }).collect()
    }
}

// ─── system_views.system_properties ────────────────────────────────────────

/// `system_views.system_properties` — JVM/runtime system properties.
#[derive(Default)]
pub struct SystemPropertiesTable {
    pub properties: Vec<(String, String)>,
}

impl VirtualTable for SystemPropertiesTable {
    fn name(&self) -> &str { "system_properties" }
    fn columns(&self) -> Vec<VirtualColumn> {
        vec![
            VirtualColumn { name: "name".into(), cql_type: "text".into() },
            VirtualColumn { name: "value".into(), cql_type: "text".into() },
        ]
    }
    fn rows(&self) -> Vec<HashMap<String, String>> {
        self.properties.iter().map(|(k, v)| {
            let mut row = HashMap::new();
            row.insert("name".into(), k.clone());
            row.insert("value".into(), v.clone());
            row
        }).collect()
    }
}

// ─── system_views.pending_hints ────────────────────────────────────────────

/// `system_views.pending_hints` — pending hint counts per host.
#[derive(Default)]
pub struct PendingHintsTable {
    pub hints: Vec<PendingHintInfo>,
}

#[derive(Debug, Clone)]
pub struct PendingHintInfo {
    pub peer: String,
    pub peer_port: u16,
    pub host_id: String,
    pub total_files: u64,
    pub oldest_timestamp: String,
    pub newest_timestamp: String,
}

impl VirtualTable for PendingHintsTable {
    fn name(&self) -> &str { "pending_hints" }
    fn columns(&self) -> Vec<VirtualColumn> {
        vec![
            VirtualColumn { name: "peer".into(), cql_type: "inet".into() },
            VirtualColumn { name: "peer_port".into(), cql_type: "int".into() },
            VirtualColumn { name: "host_id".into(), cql_type: "uuid".into() },
            VirtualColumn { name: "total_files".into(), cql_type: "bigint".into() },
            VirtualColumn { name: "oldest_timestamp".into(), cql_type: "timestamp".into() },
            VirtualColumn { name: "newest_timestamp".into(), cql_type: "timestamp".into() },
        ]
    }
    fn rows(&self) -> Vec<HashMap<String, String>> {
        self.hints.iter().map(|h| {
            let mut row = HashMap::new();
            row.insert("peer".into(), h.peer.clone());
            row.insert("peer_port".into(), h.peer_port.to_string());
            row.insert("host_id".into(), h.host_id.clone());
            row.insert("total_files".into(), h.total_files.to_string());
            row.insert("oldest_timestamp".into(), h.oldest_timestamp.clone());
            row.insert("newest_timestamp".into(), h.newest_timestamp.clone());
            row
        }).collect()
    }
}

// ─── system_views.batch_metrics ────────────────────────────────────────────

/// `system_views.batch_metrics` — batch statement metrics.
#[derive(Default)]
pub struct BatchMetricsTable {
    pub partitions_per_logged_batch: String,
    pub partitions_per_unlogged_batch: String,
    pub partitions_per_counter_batch: String,
}

impl VirtualTable for BatchMetricsTable {
    fn name(&self) -> &str { "batch_metrics" }
    fn columns(&self) -> Vec<VirtualColumn> {
        vec![
            VirtualColumn { name: "name".into(), cql_type: "text".into() },
            VirtualColumn { name: "value".into(), cql_type: "text".into() },
        ]
    }
    fn rows(&self) -> Vec<HashMap<String, String>> {
        vec![
            [("name".into(), "partitions_per_logged_batch".into()), ("value".into(), self.partitions_per_logged_batch.clone())].into_iter().collect(),
            [("name".into(), "partitions_per_unlogged_batch".into()), ("value".into(), self.partitions_per_unlogged_batch.clone())].into_iter().collect(),
            [("name".into(), "partitions_per_counter_batch".into()), ("value".into(), self.partitions_per_counter_batch.clone())].into_iter().collect(),
        ]
    }
}

// ─── system_views.cql_metrics ──────────────────────────────────────────────

/// `system_views.cql_metrics` — CQL query metrics.
#[derive(Default)]
pub struct CqlMetricsTable {
    pub prepared_statements_count: u64,
    pub prepared_statements_evicted: u64,
    pub prepared_statements_executed: u64,
    pub prepared_statements_ratio: f64,
    pub regular_statements_executed: u64,
}

impl VirtualTable for CqlMetricsTable {
    fn name(&self) -> &str { "cql_metrics" }
    fn columns(&self) -> Vec<VirtualColumn> {
        vec![
            VirtualColumn { name: "name".into(), cql_type: "text".into() },
            VirtualColumn { name: "value".into(), cql_type: "double".into() },
        ]
    }
    fn rows(&self) -> Vec<HashMap<String, String>> {
        vec![
            [("name".into(), "prepared_statements_count".into()), ("value".into(), self.prepared_statements_count.to_string())].into_iter().collect(),
            [("name".into(), "prepared_statements_evicted".into()), ("value".into(), self.prepared_statements_evicted.to_string())].into_iter().collect(),
            [("name".into(), "prepared_statements_executed".into()), ("value".into(), self.prepared_statements_executed.to_string())].into_iter().collect(),
            [("name".into(), "prepared_statements_ratio".into()), ("value".into(), format!("{:.4}", self.prepared_statements_ratio))].into_iter().collect(),
            [("name".into(), "regular_statements_executed".into()), ("value".into(), self.regular_statements_executed.to_string())].into_iter().collect(),
        ]
    }
}

// ─── system_views.local_repair ─────────────────────────────────────────────

/// `system_views.repairs` — local repair status.
#[derive(Default)]
pub struct LocalRepairTable {
    pub repairs: Vec<LocalRepairInfo>,
}

#[derive(Debug, Clone)]
pub struct LocalRepairInfo {
    pub id: String,
    pub keyspace_name: String,
    pub table_names: String,
    pub state: String,
    pub progress: f64,
    pub started_at: String,
    pub last_updated: String,
}

impl VirtualTable for LocalRepairTable {
    fn name(&self) -> &str { "repairs" }
    fn columns(&self) -> Vec<VirtualColumn> {
        vec![
            VirtualColumn { name: "id".into(), cql_type: "uuid".into() },
            VirtualColumn { name: "keyspace_name".into(), cql_type: "text".into() },
            VirtualColumn { name: "table_names".into(), cql_type: "text".into() },
            VirtualColumn { name: "state".into(), cql_type: "text".into() },
            VirtualColumn { name: "progress".into(), cql_type: "double".into() },
            VirtualColumn { name: "started_at".into(), cql_type: "timestamp".into() },
            VirtualColumn { name: "last_updated".into(), cql_type: "timestamp".into() },
        ]
    }
    fn rows(&self) -> Vec<HashMap<String, String>> {
        self.repairs.iter().map(|r| {
            let mut row = HashMap::new();
            row.insert("id".into(), r.id.clone());
            row.insert("keyspace_name".into(), r.keyspace_name.clone());
            row.insert("table_names".into(), r.table_names.clone());
            row.insert("state".into(), r.state.clone());
            row.insert("progress".into(), format!("{:.1}", r.progress));
            row.insert("started_at".into(), r.started_at.clone());
            row.insert("last_updated".into(), r.last_updated.clone());
            row
        }).collect()
    }
}

// ─── Security cache keys virtual tables ────────────────────────────────────

macro_rules! cache_keys_table {
    ($struct_name:ident, $table_name:expr) => {
        #[derive(Default)]
        pub struct $struct_name {
            pub keys: Vec<String>,
        }

        impl VirtualTable for $struct_name {
            fn name(&self) -> &str { $table_name }
            fn columns(&self) -> Vec<VirtualColumn> {
                vec![
                    VirtualColumn { name: "cache_key".into(), cql_type: "text".into() },
                ]
            }
            fn rows(&self) -> Vec<HashMap<String, String>> {
                self.keys.iter().map(|k| {
                    let mut row = HashMap::new();
                    row.insert("cache_key".into(), k.clone());
                    row
                }).collect()
            }
        }
    };
}

cache_keys_table!(CredentialsCacheKeysTable, "credentials_cache_keys");
cache_keys_table!(PermissionsCacheKeysTable, "permissions_cache_keys");
cache_keys_table!(RolesCacheKeysTable, "roles_cache_keys");

// ═══════════════════════════════════════════════════════════════════════════
// Tests
// ═══════════════════════════════════════════════════════════════════════════

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_with_builtins() {
        let reg = VirtualTableRegistry::with_builtins();
        let tables = reg.list_tables();
        // Now we have 22 built-in virtual tables
        assert!(tables.len() >= 22, "expected >=22 virtual tables, got {}", tables.len());
    }

    #[test]
    fn local_info_table() {
        let table = LocalInfoTable::default();
        assert_eq!(table.name(), "local");
        let cols = table.columns();
        assert!(cols.iter().any(|c| c.name == "host_id"));
        let rows = table.rows();
        assert_eq!(rows.len(), 1);
        assert!(rows[0].contains_key("cluster_name"));
    }

    #[test]
    fn settings_table() {
        let table = SettingsTable::default();
        assert_eq!(table.name(), "settings");
        let rows = table.rows();
        assert!(!rows.is_empty());
        assert!(rows.iter().any(|r| r.get("name").map(|v| v == "cluster_name").unwrap_or(false)));
    }

    #[test]
    fn thread_pools_table() {
        let table = ThreadPoolsTable::default();
        assert_eq!(table.name(), "thread_pools");
        let rows = table.rows();
        assert_eq!(rows.len(), 3);
    }

    #[test]
    fn sstable_tasks_empty() {
        let table = SstableTasksTable::default();
        assert_eq!(table.name(), "sstable_tasks");
        assert!(table.rows().is_empty());
    }

    #[test]
    fn clients_table_empty() {
        let table = ClientsTable::default();
        assert_eq!(table.name(), "clients");
        assert!(table.rows().is_empty());
    }

    #[test]
    fn registry_lookup() {
        let reg = VirtualTableRegistry::with_builtins();
        let local = reg.get("system_views", "local");
        assert!(local.is_some());
        assert_eq!(local.unwrap().name(), "local");

        let missing = reg.get("system_views", "nonexistent");
        assert!(missing.is_none());
    }

    #[test]
    fn gossip_info_table() {
        let table = GossipInfoTable::default();
        assert_eq!(table.name(), "gossip_info");
        assert_eq!(table.columns().len(), 11);
        assert!(table.rows().is_empty());
    }

    #[test]
    fn caches_table() {
        let table = CachesTable::default();
        assert_eq!(table.name(), "caches");
        assert_eq!(table.columns().len(), 8);
    }

    #[test]
    fn internode_tables() {
        let inbound = InternodeInboundTable::default();
        assert_eq!(inbound.name(), "internode_inbound");
        let outbound = InternodeOutboundTable::default();
        assert_eq!(outbound.name(), "internode_outbound");
    }

    #[test]
    fn streaming_table() {
        let table = StreamingTable::default();
        assert_eq!(table.name(), "streaming");
        assert_eq!(table.columns().len(), 12);
    }

    #[test]
    fn snapshots_table() {
        let table = SnapshotsTable::default();
        assert_eq!(table.name(), "snapshots");
        assert_eq!(table.columns().len(), 8);
    }

    #[test]
    fn log_messages_table() {
        let table = LogMessagesTable::default();
        assert_eq!(table.name(), "system_logs");
    }

    #[test]
    fn queries_table() {
        let table = QueriesTable::default();
        assert_eq!(table.name(), "queries");
    }

    #[test]
    fn slow_queries_table() {
        let table = SlowQueriesTable::default();
        assert_eq!(table.name(), "slow_queries");
    }

    #[test]
    fn batch_metrics_table() {
        let table = BatchMetricsTable::default();
        assert_eq!(table.name(), "batch_metrics");
        assert_eq!(table.rows().len(), 3);
    }

    #[test]
    fn cql_metrics_table() {
        let table = CqlMetricsTable::default();
        assert_eq!(table.name(), "cql_metrics");
        assert_eq!(table.rows().len(), 5);
    }

    #[test]
    fn security_cache_keys_tables() {
        let table = CredentialsCacheKeysTable::default();
        assert_eq!(table.name(), "credentials_cache_keys");
        let table = PermissionsCacheKeysTable::default();
        assert_eq!(table.name(), "permissions_cache_keys");
        let table = RolesCacheKeysTable::default();
        assert_eq!(table.name(), "roles_cache_keys");
    }

    #[test]
    fn custom_virtual_table() {
        struct CustomTable;
        impl VirtualTable for CustomTable {
            fn name(&self) -> &str { "custom" }
            fn columns(&self) -> Vec<VirtualColumn> {
                vec![VirtualColumn { name: "value".into(), cql_type: "text".into() }]
            }
            fn rows(&self) -> Vec<HashMap<String, String>> {
                let mut row = HashMap::new();
                row.insert("value".into(), "hello".into());
                vec![row]
            }
        }

        let mut reg = VirtualTableRegistry::new();
        reg.register(Box::new(CustomTable));
        let table = reg.get("system_views", "custom").unwrap();
        assert_eq!(table.rows()[0]["value"], "hello");
    }

    #[test]
    fn all_virtual_tables_have_unique_names() {
        let reg = VirtualTableRegistry::with_builtins();
        let tables = reg.list_tables();
        let unique: std::collections::HashSet<String> = tables.iter().cloned().collect();
        assert_eq!(tables.len(), unique.len(), "duplicate virtual table names found");
    }

    #[test]
    fn all_virtual_tables_have_columns() {
        let reg = VirtualTableRegistry::with_builtins();
        for name in reg.list_tables() {
            let parts: Vec<&str> = name.splitn(2, '.').collect();
            let table = reg.get(parts[0], parts[1]).unwrap();
            assert!(!table.columns().is_empty(), "table {} has no columns", name);
        }
    }
}

