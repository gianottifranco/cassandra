// Licensed under Apache License, Version 2.0.

//! Partition denylist: prevents reads/writes on specific partitions.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.schema.PartitionDenylist`
//!
//! The partition denylist is an operator-facing control surface that allows
//! blocking hot or problematic partitions. It is stored in a system table
//! and loaded into memory at startup and on config reload.

use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// A partition denylist entry: keyspace.table → set of denied partition keys.
///
/// In Java, partition keys are stored as ByteBuffer. Here we use `Vec<u8>`
/// for the binary partition key representation.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct PartitionDenylist {
    /// Enabled flag (feature-gated).
    pub enabled: bool,

    /// Map of "keyspace.table" → set of denied partition key bytes.
    entries: HashMap<String, HashSet<Vec<u8>>>,

    /// Metrics: number of denied reads.
    #[serde(skip)]
    denied_reads: u64,

    /// Metrics: number of denied writes.
    #[serde(skip)]
    denied_writes: u64,
}

impl PartitionDenylist {
    /// Create a new empty denylist.
    pub fn new(enabled: bool) -> Self {
        Self {
            enabled,
            entries: HashMap::new(),
            denied_reads: 0,
            denied_writes: 0,
        }
    }

    /// Check if a partition key is denied for a given keyspace.table.
    pub fn is_denied(&self, keyspace: &str, table: &str, partition_key: &[u8]) -> bool {
        if !self.enabled {
            return false;
        }
        let key = format!("{}.{}", keyspace, table);
        self.entries
            .get(&key)
            .is_some_and(|s| s.contains(partition_key))
    }

    /// Add a partition key to the denylist.
    pub fn deny(&mut self, keyspace: &str, table: &str, partition_key: Vec<u8>) {
        let key = format!("{}.{}", keyspace, table);
        self.entries.entry(key).or_default().insert(partition_key);
    }

    /// Remove a partition key from the denylist.
    pub fn allow(&mut self, keyspace: &str, table: &str, partition_key: &[u8]) -> bool {
        let key = format!("{}.{}", keyspace, table);
        if let Some(set) = self.entries.get_mut(&key) {
            let removed = set.remove(partition_key);
            if set.is_empty() {
                self.entries.remove(&key);
            }
            removed
        } else {
            false
        }
    }

    /// Total number of denied partitions.
    pub fn total_denied(&self) -> usize {
        self.entries.values().map(|s| s.len()).sum()
    }

    /// Record a denied read.
    pub fn record_denied_read(&mut self) {
        self.denied_reads += 1;
    }

    /// Record a denied write.
    pub fn record_denied_write(&mut self) {
        self.denied_writes += 1;
    }

    /// Get denied reads count.
    pub fn denied_reads(&self) -> u64 {
        self.denied_reads
    }

    /// Get denied writes count.
    pub fn denied_writes(&self) -> u64 {
        self.denied_writes
    }

    /// Cannot denylist system keyspaces.
    pub fn can_denylist_keyspace(keyspace: &str) -> bool {
        !keyspace.starts_with("system")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn disabled_allows_everything() {
        let dl = PartitionDenylist::new(false);
        assert!(!dl.is_denied("ks", "t", &[1, 2, 3]));
    }

    #[test]
    fn deny_and_check() {
        let mut dl = PartitionDenylist::new(true);
        dl.deny("ks", "t", vec![1, 2, 3]);
        assert!(dl.is_denied("ks", "t", &[1, 2, 3]));
        assert!(!dl.is_denied("ks", "t", &[4, 5, 6]));
        assert!(!dl.is_denied("ks", "other", &[1, 2, 3]));
    }

    #[test]
    fn allow_removes() {
        let mut dl = PartitionDenylist::new(true);
        dl.deny("ks", "t", vec![1, 2, 3]);
        assert!(dl.allow("ks", "t", &[1, 2, 3]));
        assert!(!dl.is_denied("ks", "t", &[1, 2, 3]));
    }

    #[test]
    fn total_denied_count() {
        let mut dl = PartitionDenylist::new(true);
        dl.deny("ks", "t1", vec![1]);
        dl.deny("ks", "t1", vec![2]);
        dl.deny("ks", "t2", vec![3]);
        assert_eq!(dl.total_denied(), 3);
    }

    #[test]
    fn cannot_denylist_system() {
        assert!(!PartitionDenylist::can_denylist_keyspace("system"));
        assert!(!PartitionDenylist::can_denylist_keyspace("system_auth"));
        assert!(PartitionDenylist::can_denylist_keyspace("my_keyspace"));
    }

    #[test]
    fn metrics_tracking() {
        let mut dl = PartitionDenylist::new(true);
        dl.record_denied_read();
        dl.record_denied_read();
        dl.record_denied_write();
        assert_eq!(dl.denied_reads(), 2);
        assert_eq!(dl.denied_writes(), 1);
    }
}
