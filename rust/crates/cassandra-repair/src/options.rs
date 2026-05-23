// Licensed under Apache License, Version 2.0.

//! Repair options and parallelism modes.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.repair.RepairParallelism`
//! - `org.apache.cassandra.repair.RepairOption`

use std::fmt;

use serde::{Deserialize, Serialize};

use cassandra_cluster_metadata::Endpoint;
use cassandra_common::Token;

use crate::coordinator::RepairType;

/// How repair sessions are parallelized across replicas.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairParallelism {
    /// One range repaired at a time across the cluster.
    #[default]
    Sequential,
    /// All ranges repaired concurrently.
    Parallel,
    /// Parallel within each datacenter, sequential across datacenters.
    DatacenterAware,
}

impl fmt::Display for RepairParallelism {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Sequential => write!(f, "sequential"),
            Self::Parallel => write!(f, "parallel"),
            Self::DatacenterAware => write!(f, "dc_parallel"),
        }
    }
}

/// A token range with the endpoints responsible for it.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepairRange {
    /// Token range (start inclusive, end exclusive).
    pub range: (Token, Token),
    /// Endpoints holding replicas of this range.
    pub endpoints: Vec<Endpoint>,
}

/// Full repair configuration mirroring Java's `RepairOption`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RepairOption {
    /// Keyspace to repair.
    pub keyspace: String,
    /// Tables to repair (empty = all tables).
    pub tables: Vec<String>,
    /// Specific token ranges to repair (empty = all ranges).
    pub ranges: Vec<(Token, Token)>,
    /// Parallelism mode.
    pub parallelism: RepairParallelism,
    /// Type of repair.
    pub repair_type: RepairType,
    /// Whether this is an incremental repair.
    pub incremental: bool,
    /// Only repair primary ranges owned by this node.
    pub primary_range_only: bool,
    /// Limit repair to specific datacenters.
    pub data_centers: Vec<String>,
    /// Limit repair to specific hosts.
    pub hosts: Vec<Endpoint>,
    /// Preview only — compute diffs but don't stream.
    pub preview_only: bool,
    /// Force repair even if other repairs are running.
    pub force: bool,
    /// Pull repair: stream data to this node only.
    pub pull_repair: bool,
}

impl RepairOption {
    /// Create a builder for repair options.
    pub fn builder(keyspace: impl Into<String>) -> RepairOptionBuilder {
        RepairOptionBuilder {
            keyspace: keyspace.into(),
            tables: Vec::new(),
            ranges: Vec::new(),
            parallelism: RepairParallelism::default(),
            repair_type: RepairType::Full,
            incremental: false,
            primary_range_only: false,
            data_centers: Vec::new(),
            hosts: Vec::new(),
            preview_only: false,
            force: false,
            pull_repair: false,
        }
    }
}

impl fmt::Display for RepairOption {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "repair {} (type={}, parallelism={}, tables={}, ranges={}, incremental={}, \
             primary_range_only={}, preview={})",
            self.keyspace,
            self.repair_type,
            self.parallelism,
            if self.tables.is_empty() {
                "<all>".to_string()
            } else {
                self.tables.join(",")
            },
            if self.ranges.is_empty() {
                "<all>".to_string()
            } else {
                format!("{}", self.ranges.len())
            },
            self.incremental,
            self.primary_range_only,
            self.preview_only,
        )
    }
}

/// Builder for [`RepairOption`].
pub struct RepairOptionBuilder {
    keyspace: String,
    tables: Vec<String>,
    ranges: Vec<(Token, Token)>,
    parallelism: RepairParallelism,
    repair_type: RepairType,
    incremental: bool,
    primary_range_only: bool,
    data_centers: Vec<String>,
    hosts: Vec<Endpoint>,
    preview_only: bool,
    force: bool,
    pull_repair: bool,
}

impl RepairOptionBuilder {
    pub fn tables(mut self, tables: Vec<String>) -> Self {
        self.tables = tables;
        self
    }

    pub fn ranges(mut self, ranges: Vec<(Token, Token)>) -> Self {
        self.ranges = ranges;
        self
    }

    pub fn parallelism(mut self, p: RepairParallelism) -> Self {
        self.parallelism = p;
        self
    }

    pub fn repair_type(mut self, t: RepairType) -> Self {
        self.repair_type = t;
        self
    }

    pub fn incremental(mut self, v: bool) -> Self {
        self.incremental = v;
        self
    }

    pub fn primary_range_only(mut self, v: bool) -> Self {
        self.primary_range_only = v;
        self
    }

    pub fn data_centers(mut self, dcs: Vec<String>) -> Self {
        self.data_centers = dcs;
        self
    }

    pub fn hosts(mut self, hosts: Vec<Endpoint>) -> Self {
        self.hosts = hosts;
        self
    }

    pub fn preview_only(mut self, v: bool) -> Self {
        self.preview_only = v;
        self
    }

    pub fn force(mut self, v: bool) -> Self {
        self.force = v;
        self
    }

    pub fn pull_repair(mut self, v: bool) -> Self {
        self.pull_repair = v;
        self
    }

    pub fn build(self) -> RepairOption {
        RepairOption {
            keyspace: self.keyspace,
            tables: self.tables,
            ranges: self.ranges,
            parallelism: self.parallelism,
            repair_type: self.repair_type,
            incremental: self.incremental,
            primary_range_only: self.primary_range_only,
            data_centers: self.data_centers,
            hosts: self.hosts,
            preview_only: self.preview_only,
            force: self.force,
            pull_repair: self.pull_repair,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_defaults() {
        let opt = RepairOption::builder("ks1").build();
        assert_eq!(opt.keyspace, "ks1");
        assert!(opt.tables.is_empty());
        assert!(opt.ranges.is_empty());
        assert_eq!(opt.parallelism, RepairParallelism::Sequential);
        assert_eq!(opt.repair_type, RepairType::Full);
        assert!(!opt.incremental);
        assert!(!opt.primary_range_only);
        assert!(!opt.preview_only);
        assert!(!opt.force);
        assert!(!opt.pull_repair);
    }

    #[test]
    fn builder_with_options() {
        let opt = RepairOption::builder("ks2")
            .tables(vec!["t1".into(), "t2".into()])
            .parallelism(RepairParallelism::Parallel)
            .repair_type(RepairType::Incremental)
            .incremental(true)
            .primary_range_only(true)
            .force(true)
            .build();

        assert_eq!(opt.keyspace, "ks2");
        assert_eq!(opt.tables.len(), 2);
        assert_eq!(opt.parallelism, RepairParallelism::Parallel);
        assert_eq!(opt.repair_type, RepairType::Incremental);
        assert!(opt.incremental);
        assert!(opt.primary_range_only);
        assert!(opt.force);
    }

    #[test]
    fn serde_round_trip() {
        let opt = RepairOption::builder("ks")
            .parallelism(RepairParallelism::DatacenterAware)
            .repair_type(RepairType::Preview)
            .preview_only(true)
            .build();

        let json = serde_json::to_string(&opt).unwrap();
        let deser: RepairOption = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.keyspace, "ks");
        assert_eq!(deser.parallelism, RepairParallelism::DatacenterAware);
        assert_eq!(deser.repair_type, RepairType::Preview);
        assert!(deser.preview_only);
    }

    #[test]
    fn parallelism_display() {
        assert_eq!(RepairParallelism::Sequential.to_string(), "sequential");
        assert_eq!(RepairParallelism::Parallel.to_string(), "parallel");
        assert_eq!(
            RepairParallelism::DatacenterAware.to_string(),
            "dc_parallel"
        );
    }

    #[test]
    fn repair_option_display() {
        let opt = RepairOption::builder("my_ks")
            .tables(vec!["t1".into()])
            .build();
        let display = opt.to_string();
        assert!(display.contains("my_ks"));
        assert!(display.contains("t1"));
    }

    #[test]
    fn repair_range_serde() {
        use std::net::{IpAddr, Ipv4Addr, SocketAddr};
        let rr = RepairRange {
            range: (Token::from_raw(0), Token::from_raw(100)),
            endpoints: vec![Endpoint::new(SocketAddr::new(
                IpAddr::V4(Ipv4Addr::LOCALHOST),
                9042,
            ))],
        };
        let json = serde_json::to_string(&rr).unwrap();
        let deser: RepairRange = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.range.0.value(), 0);
        assert_eq!(deser.endpoints.len(), 1);
    }
}
