// Licensed under Apache License, Version 2.0.

//! Virtual tables framework and built-in system views.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.virtual.*`
//! - `system_views.local`, `system_views.peers`, etc.
//!
//! Virtual tables are read-only views into server internal state,
//! exposed to CQL queries.

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
        reg.register(Box::new(LocalInfoTable::default()));
        reg.register(Box::new(SettingsTable::default()));
        reg.register(Box::new(ThreadPoolsTable::default()));
        reg.register(Box::new(SstableTasksTable::default()));
        reg.register(Box::new(ClientsTable::default()));
        reg
    }
}

impl Default for VirtualTableRegistry {
    fn default() -> Self {
        Self::with_builtins()
    }
}

// ─── Built-in Virtual Tables ───────────────────────────────────────────────

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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_with_builtins() {
        let reg = VirtualTableRegistry::with_builtins();
        let tables = reg.list_tables();
        assert!(tables.len() >= 5);
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
        // Should contain cluster_name setting
        assert!(rows.iter().any(|r| r.get("name").map(|v| v == "cluster_name").unwrap_or(false)));
    }

    #[test]
    fn thread_pools_table() {
        let table = ThreadPoolsTable::default();
        assert_eq!(table.name(), "thread_pools");
        let rows = table.rows();
        assert_eq!(rows.len(), 3); // ReadStage, MutationStage, CompactionExecutor
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
}
