// Licensed under Apache License, Version 2.0.

//! Nodetool support primitives.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.tools.NodeTool`
//! - `org.apache.cassandra.tools.nodetool.formatter.TableBuilder`
//! - `org.apache.cassandra.tools.nodetool.stats.TableStatsHolder`

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFormat {
    Table,
    Json,
    Yaml,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableFormatter {
    headers: Vec<String>,
    rows: Vec<Vec<String>>,
}

impl TableFormatter {
    pub fn new(headers: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self {
            headers: headers.into_iter().map(Into::into).collect(),
            rows: Vec::new(),
        }
    }

    pub fn add_row(&mut self, row: impl IntoIterator<Item = impl Into<String>>) {
        self.rows.push(row.into_iter().map(Into::into).collect());
    }

    pub fn render(&self) -> String {
        let widths = self.column_widths();
        let mut lines = Vec::new();
        lines.push(render_row(&self.headers, &widths));
        lines.push(
            widths
                .iter()
                .map(|width| "-".repeat(*width))
                .collect::<Vec<_>>()
                .join("  "),
        );
        for row in &self.rows {
            lines.push(render_row(row, &widths));
        }
        lines.join("\n")
    }

    fn column_widths(&self) -> Vec<usize> {
        let mut widths = self
            .headers
            .iter()
            .map(|header| header.len())
            .collect::<Vec<_>>();
        for row in &self.rows {
            if row.len() > widths.len() {
                widths.resize(row.len(), 0);
            }
            for (idx, value) in row.iter().enumerate() {
                widths[idx] = widths[idx].max(value.len());
            }
        }
        widths
    }
}

fn render_row(row: &[String], widths: &[usize]) -> String {
    widths
        .iter()
        .enumerate()
        .map(|(idx, width)| {
            let value = row.get(idx).map(String::as_str).unwrap_or("");
            format!("{value:<width$}")
        })
        .collect::<Vec<_>>()
        .join("  ")
}

pub fn render_json<T: Serialize>(value: &T) -> Result<String, serde_json::Error> {
    serde_json::to_string_pretty(value)
}

pub fn render_yaml<T: Serialize>(value: &T) -> Result<String, serde_yaml::Error> {
    serde_yaml::to_string(value)
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct NodetoolCommand {
    pub name: &'static str,
    pub category: &'static str,
    pub admin_path: Option<&'static str>,
}

pub fn command_catalog() -> Vec<NodetoolCommand> {
    vec![
        cmd("status", "cluster", Some("/api/v1/cluster/status")),
        cmd("info", "cluster", Some("/api/v1/cluster/info")),
        cmd("version", "cluster", Some("/api/v1/version")),
        cmd("ring", "cluster", Some("/api/v1/cluster/ring")),
        cmd(
            "describecluster",
            "cluster",
            Some("/api/v1/cluster/describe"),
        ),
        cmd("gossipinfo", "cluster", Some("/api/v1/cluster/gossip")),
        cmd("netstats", "topology", Some("/api/v1/topology/netstats")),
        cmd("move", "topology", Some("/api/v1/topology/move")),
        cmd("join", "topology", Some("/api/v1/topology/join")),
        cmd(
            "decommission",
            "topology",
            Some("/api/v1/topology/decommission"),
        ),
        cmd(
            "removenode",
            "topology",
            Some("/api/v1/topology/removenode"),
        ),
        cmd("rebuild", "topology", Some("/api/v1/topology/rebuild")),
        cmd("bootstrap", "topology", Some("/api/v1/topology/bootstrap")),
        cmd(
            "stopdaemon",
            "topology",
            Some("/api/v1/topology/stopdaemon"),
        ),
        cmd("snapshot", "snapshots", Some("/api/v1/snapshots/create")),
        cmd("listsnapshots", "snapshots", Some("/api/v1/snapshots")),
        cmd(
            "clearsnapshot",
            "snapshots",
            Some("/api/v1/snapshots/clear"),
        ),
        cmd("flush", "compaction", Some("/api/v1/compaction/flush")),
        cmd("compact", "compaction", Some("/api/v1/compaction/compact")),
        cmd("cleanup", "compaction", Some("/api/v1/compaction/cleanup")),
        cmd("scrub", "compaction", Some("/api/v1/compaction/scrub")),
        cmd("verify", "compaction", Some("/api/v1/compaction/verify")),
        cmd(
            "upgradesstables",
            "compaction",
            Some("/api/v1/compaction/upgradesstables"),
        ),
        cmd(
            "garbagecollect",
            "compaction",
            Some("/api/v1/compaction/garbagecollect"),
        ),
        cmd(
            "compactionstats",
            "compaction",
            Some("/api/v1/compaction/stats"),
        ),
        cmd(
            "compactionhistory",
            "compaction",
            Some("/api/v1/compaction/history"),
        ),
        cmd(
            "enableautocompaction",
            "compaction",
            Some("/api/v1/compaction/enable"),
        ),
        cmd(
            "disableautocompaction",
            "compaction",
            Some("/api/v1/compaction/disable"),
        ),
        cmd(
            "statusautocompaction",
            "compaction",
            Some("/api/v1/compaction/status"),
        ),
        cmd(
            "getcompactionthroughput",
            "compaction",
            Some("/api/v1/compaction/throughput"),
        ),
        cmd(
            "setcompactionthroughput",
            "compaction",
            Some("/api/v1/compaction/throughput"),
        ),
        cmd("tablestats", "stats", Some("/api/v1/stats/tables")),
        cmd("cfstats", "stats", Some("/api/v1/stats/tables")),
        cmd("tablehistograms", "stats", Some("/api/v1/stats/histograms")),
        cmd("cfhistograms", "stats", Some("/api/v1/stats/histograms")),
        cmd("tpstats", "stats", Some("/api/v1/stats/tpstats")),
        cmd("gcstats", "stats", Some("/api/v1/stats/gcstats")),
        cmd(
            "proxyhistograms",
            "stats",
            Some("/api/v1/stats/proxyhistograms"),
        ),
        cmd("clientstats", "stats", Some("/api/v1/stats/clients")),
        cmd(
            "toppartitions",
            "stats",
            Some("/api/v1/stats/toppartitions"),
        ),
        cmd("getendpoints", "stats", Some("/api/v1/stats/endpoints")),
        cmd("getsstables", "stats", Some("/api/v1/stats/sstables")),
        cmd(
            "invalidatekeycache",
            "cache",
            Some("/api/v1/cache/key/invalidate"),
        ),
        cmd(
            "invalidaterowcache",
            "cache",
            Some("/api/v1/cache/row/invalidate"),
        ),
        cmd(
            "invalidatecountercache",
            "cache",
            Some("/api/v1/cache/counter/invalidate"),
        ),
        cmd("enablehandoff", "hints", Some("/api/v1/hints/enable")),
        cmd("disablehandoff", "hints", Some("/api/v1/hints/disable")),
        cmd("truncatehints", "hints", Some("/api/v1/hints/truncate")),
        cmd("pausehandoff", "hints", Some("/api/v1/hints/pause")),
        cmd("resumehandoff", "hints", Some("/api/v1/hints/resume")),
        cmd("enableauditlog", "logging", Some("/api/v1/audit/enable")),
        cmd("disableauditlog", "logging", Some("/api/v1/audit/disable")),
        cmd(
            "getlogginglevels",
            "logging",
            Some("/api/v1/logging/levels"),
        ),
        cmd("setlogginglevel", "logging", Some("/api/v1/logging/levels")),
        cmd("reloadssl", "security", Some("/api/v1/security/reloadssl")),
        cmd("reloadlocalschema", "schema", Some("/api/v1/schema/reload")),
        cmd("describering", "schema", Some("/api/v1/schema/ring")),
        cmd("repair", "repair", Some("/api/v1/repair")),
        cmd("rebuild_index", "index", Some("/api/v1/index/rebuild")),
        cmd("import", "sstable", Some("/api/v1/sstable/import")),
        cmd("stop", "operations", Some("/api/v1/operations/stop")),
        cmd(
            "settraceprobability",
            "tracing",
            Some("/api/v1/tracing/probability"),
        ),
    ]
}

pub fn command_by_name(name: &str) -> Option<NodetoolCommand> {
    command_catalog()
        .into_iter()
        .find(|command| command.name.eq_ignore_ascii_case(name))
}

fn cmd(
    name: &'static str,
    category: &'static str,
    admin_path: Option<&'static str>,
) -> NodetoolCommand {
    NodetoolCommand {
        name,
        category,
        admin_path,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct StatsTable {
    pub keyspace: String,
    pub table: String,
    pub sstable_count: u64,
    pub disk_space_bytes: u64,
    pub read_count: u64,
    pub write_count: u64,
}

#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableStatsSummary {
    pub table_count: usize,
    pub sstable_count: u64,
    pub disk_space_bytes: u64,
    pub read_count: u64,
    pub write_count: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TableStatsHolder {
    pub tables: Vec<StatsTable>,
}

impl TableStatsHolder {
    pub fn new(tables: Vec<StatsTable>) -> Self {
        Self { tables }
    }

    pub fn total_disk_space_bytes(&self) -> u64 {
        self.tables.iter().map(|table| table.disk_space_bytes).sum()
    }

    pub fn total_sstable_count(&self) -> u64 {
        self.tables.iter().map(|table| table.sstable_count).sum()
    }

    pub fn total_read_count(&self) -> u64 {
        self.tables.iter().map(|table| table.read_count).sum()
    }

    pub fn total_write_count(&self) -> u64 {
        self.tables.iter().map(|table| table.write_count).sum()
    }

    pub fn summary(&self) -> TableStatsSummary {
        TableStatsSummary {
            table_count: self.tables.len(),
            sstable_count: self.total_sstable_count(),
            disk_space_bytes: self.total_disk_space_bytes(),
            read_count: self.total_read_count(),
            write_count: self.total_write_count(),
        }
    }

    pub fn filter_keyspace(&self, keyspace: &str) -> Self {
        Self {
            tables: self
                .tables
                .iter()
                .filter(|table| table.keyspace.eq_ignore_ascii_case(keyspace))
                .cloned()
                .collect(),
        }
    }

    pub fn filter_table(&self, keyspace: &str, table: &str) -> Self {
        Self {
            tables: self
                .tables
                .iter()
                .filter(|stats| {
                    stats.keyspace.eq_ignore_ascii_case(keyspace)
                        && stats.table.eq_ignore_ascii_case(table)
                })
                .cloned()
                .collect(),
        }
    }

    pub fn sorted_by_name(&self) -> Self {
        let mut tables = self.tables.clone();
        tables.sort_by(|left, right| {
            left.keyspace
                .cmp(&right.keyspace)
                .then_with(|| left.table.cmp(&right.table))
        });
        Self { tables }
    }

    pub fn render(&self, format: OutputFormat) -> String {
        match format {
            OutputFormat::Json => render_json(self).unwrap_or_else(|_| "{}".to_string()),
            OutputFormat::Yaml => render_yaml(self).unwrap_or_default(),
            OutputFormat::Table => {
                let mut table = TableFormatter::new([
                    "Keyspace",
                    "Table",
                    "SSTables",
                    "Disk bytes",
                    "Reads",
                    "Writes",
                ]);
                for stats in &self.tables {
                    table.add_row([
                        stats.keyspace.clone(),
                        stats.table.clone(),
                        stats.sstable_count.to_string(),
                        stats.disk_space_bytes.to_string(),
                        stats.read_count.to_string(),
                        stats.write_count.to_string(),
                    ]);
                }
                let summary = self.summary();
                table.add_row([
                    "TOTAL".to_string(),
                    summary.table_count.to_string(),
                    summary.sstable_count.to_string(),
                    summary.disk_space_bytes.to_string(),
                    summary.read_count.to_string(),
                    summary.write_count.to_string(),
                ]);
                table.render()
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn table_formatter_aligns_columns() {
        let mut formatter = TableFormatter::new(["Name", "Value"]);
        formatter.add_row(["ReadStage", "10"]);
        formatter.add_row(["MutationStage", "200"]);
        let rendered = formatter.render();
        assert!(rendered.contains("Name           Value"));
        assert!(rendered.contains("MutationStage  200"));
    }

    #[test]
    fn command_catalog_contains_admin_mapped_commands() {
        let commands = command_catalog();
        assert!(commands.len() >= 50);
        assert_eq!(
            command_by_name("TABLeSTATS").unwrap().admin_path,
            Some("/api/v1/stats/tables")
        );
        assert_eq!(
            command_by_name("STOPDAEMON").unwrap().admin_path,
            Some("/api/v1/topology/stopdaemon")
        );
    }

    #[test]
    fn table_stats_holder_aggregates_and_renders() {
        let holder = TableStatsHolder::new(vec![
            StatsTable {
                keyspace: "ks".into(),
                table: "a".into(),
                sstable_count: 2,
                disk_space_bytes: 100,
                read_count: 4,
                write_count: 5,
            },
            StatsTable {
                keyspace: "ks".into(),
                table: "b".into(),
                sstable_count: 3,
                disk_space_bytes: 200,
                read_count: 6,
                write_count: 7,
            },
        ]);
        assert_eq!(holder.total_sstable_count(), 5);
        assert_eq!(holder.total_disk_space_bytes(), 300);
        assert_eq!(holder.total_read_count(), 10);
        assert_eq!(holder.total_write_count(), 12);
        assert_eq!(holder.summary().table_count, 2);
        assert_eq!(holder.filter_keyspace("ks").tables.len(), 2);
        assert_eq!(holder.filter_table("ks", "a").tables.len(), 1);
        assert_eq!(holder.sorted_by_name().tables[0].table, "a");
        assert!(holder.render(OutputFormat::Table).contains("Disk bytes"));
        assert!(holder.render(OutputFormat::Table).contains("TOTAL"));
        assert!(holder.render(OutputFormat::Json).contains("\"tables\""));
        assert!(holder.render(OutputFormat::Yaml).contains("tables:"));
    }
}
