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

//! Read command types modeling the Java `ReadCommand` hierarchy.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.db.ReadCommand`
//! - `org.apache.cassandra.db.SinglePartitionReadCommand`
//! - `org.apache.cassandra.db.PartitionRangeReadCommand`
//! - `org.apache.cassandra.db.filter.ClusteringIndexFilter`
//! - `org.apache.cassandra.db.filter.ColumnFilter`
//! - `org.apache.cassandra.db.filter.DataLimits`

use std::fmt;

use cassandra_common::Token;

/// Top-level read command dispatched from the coordinator.
#[derive(Debug, Clone)]
pub enum ReadCommand {
    /// Read a single partition by exact partition key.
    SinglePartition(SinglePartitionReadCommand),
    /// Range scan over a token range.
    PartitionRange(PartitionRangeReadCommand),
}

impl ReadCommand {
    pub fn keyspace(&self) -> &str {
        match self {
            Self::SinglePartition(cmd) => &cmd.keyspace,
            Self::PartitionRange(cmd) => &cmd.keyspace,
        }
    }

    pub fn table(&self) -> &str {
        match self {
            Self::SinglePartition(cmd) => &cmd.table,
            Self::PartitionRange(cmd) => &cmd.table,
        }
    }

    pub fn limits(&self) -> &ReadLimits {
        match self {
            Self::SinglePartition(cmd) => &cmd.limits,
            Self::PartitionRange(cmd) => &cmd.limits,
        }
    }

    pub fn is_reversed(&self) -> bool {
        match self {
            Self::SinglePartition(cmd) => cmd.is_reversed,
            Self::PartitionRange(cmd) => cmd.is_reversed,
        }
    }

    /// Whether this is a digest-only request (hash, not full data).
    pub fn is_digest_query(&self) -> bool {
        match self {
            Self::SinglePartition(cmd) => cmd.is_digest_query,
            Self::PartitionRange(cmd) => cmd.is_digest_query,
        }
    }

    pub fn set_digest_query(&mut self, digest: bool) {
        match self {
            Self::SinglePartition(cmd) => cmd.is_digest_query = digest,
            Self::PartitionRange(cmd) => cmd.is_digest_query = digest,
        }
    }
}

/// Read a single partition.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.db.SinglePartitionReadCommand`
#[derive(Debug, Clone)]
pub struct SinglePartitionReadCommand {
    pub keyspace: String,
    pub table: String,
    /// Serialized partition key bytes.
    pub partition_key: Vec<u8>,
    /// Optional clustering range filter.
    pub clustering_slice: Option<ClusteringSlice>,
    /// Column filter: which columns to return.
    pub column_filter: ColumnFilter,
    /// Per-partition and per-query limits.
    pub limits: ReadLimits,
    /// Whether to read in reverse clustering order.
    pub is_reversed: bool,
    /// Whether this is a digest-only query.
    pub is_digest_query: bool,
    /// Timestamp at which to evaluate TTLs (microseconds).
    pub now_in_seconds: i32,
}

impl SinglePartitionReadCommand {
    /// Create a full-partition read with default limits.
    pub fn full_partition(
        keyspace: impl Into<String>,
        table: impl Into<String>,
        partition_key: Vec<u8>,
    ) -> Self {
        let now_in_seconds = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i32;
        Self {
            keyspace: keyspace.into(),
            table: table.into(),
            partition_key,
            clustering_slice: None,
            column_filter: ColumnFilter::All,
            limits: ReadLimits::default(),
            is_reversed: false,
            is_digest_query: false,
            now_in_seconds,
        }
    }

    /// Compute the token for this partition key.
    pub fn token(&self) -> Token {
        Token::from_partition_key(&self.partition_key)
    }

    pub fn with_limits(mut self, limits: ReadLimits) -> Self {
        self.limits = limits;
        self
    }

    pub fn with_clustering_slice(mut self, slice: ClusteringSlice) -> Self {
        self.clustering_slice = Some(slice);
        self
    }

    pub fn reversed(mut self) -> Self {
        self.is_reversed = true;
        self
    }
}

/// Range read across a token range.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.db.PartitionRangeReadCommand`
#[derive(Debug, Clone)]
pub struct PartitionRangeReadCommand {
    pub keyspace: String,
    pub table: String,
    /// Token range to scan.
    pub data_range: DataRange,
    /// Column filter.
    pub column_filter: ColumnFilter,
    /// Limits.
    pub limits: ReadLimits,
    /// Whether to read in reverse clustering order within each partition.
    pub is_reversed: bool,
    /// Whether this is a digest-only query.
    pub is_digest_query: bool,
    /// Now in seconds for TTL evaluation.
    pub now_in_seconds: i32,
}

impl PartitionRangeReadCommand {
    /// Full-table scan with default limits.
    pub fn full_scan(
        keyspace: impl Into<String>,
        table: impl Into<String>,
    ) -> Self {
        let now_in_seconds = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs() as i32;
        Self {
            keyspace: keyspace.into(),
            table: table.into(),
            data_range: DataRange::full_ring(),
            column_filter: ColumnFilter::All,
            limits: ReadLimits::default(),
            is_reversed: false,
            is_digest_query: false,
            now_in_seconds,
        }
    }

    pub fn with_range(mut self, range: DataRange) -> Self {
        self.data_range = range;
        self
    }

    pub fn with_limits(mut self, limits: ReadLimits) -> Self {
        self.limits = limits;
        self
    }
}

// ─── Data Range ──────────────────────────────────────────────────

/// A token range for range reads.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.dht.AbstractBounds` subclasses.
#[derive(Debug, Clone)]
pub struct DataRange {
    /// Start token (exclusive by default, like `(start_token, end_token]`).
    pub start_token: Token,
    /// End token (inclusive).
    pub end_token: Token,
    /// Whether start is inclusive.
    pub start_inclusive: bool,
    /// Whether end is inclusive.
    pub end_inclusive: bool,
    /// Optional clustering filter applied to all partitions in the range.
    pub clustering_slice: Option<ClusteringSlice>,
}

impl DataRange {
    /// Full ring scan.
    pub fn full_ring() -> Self {
        Self {
            start_token: Token::from_raw(i64::MIN),
            end_token: Token::from_raw(i64::MAX),
            start_inclusive: true,
            end_inclusive: true,
            clustering_slice: None,
        }
    }

    /// A specific token range.
    pub fn for_token_range(start: Token, end: Token) -> Self {
        Self {
            start_token: start,
            end_token: end,
            start_inclusive: false,
            end_inclusive: true,
            clustering_slice: None,
        }
    }

    /// Whether a token falls within this range.
    pub fn contains(&self, token: Token) -> bool {
        let start_ok = if self.start_inclusive {
            token >= self.start_token
        } else {
            token > self.start_token
        };
        let end_ok = if self.end_inclusive {
            token <= self.end_token
        } else {
            token < self.end_token
        };
        start_ok && end_ok
    }
}

// ─── Clustering Slice ────────────────────────────────────────────

/// A clustering key slice filter.
///
/// Selects rows within a partition whose clustering key falls in `[start, end]`.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.db.filter.ClusteringIndexSliceFilter`
#[derive(Debug, Clone)]
pub struct ClusteringSlice {
    /// Inclusive start bound (empty = beginning).
    pub start: Vec<u8>,
    /// Inclusive end bound (empty = end).
    pub end: Vec<u8>,
    /// Whether start is inclusive.
    pub start_inclusive: bool,
    /// Whether end is inclusive.
    pub end_inclusive: bool,
}

impl ClusteringSlice {
    /// Unbounded slice: all rows.
    pub fn all() -> Self {
        Self {
            start: Vec::new(),
            end: Vec::new(),
            start_inclusive: true,
            end_inclusive: true,
        }
    }

    /// Check if a clustering key is within this slice.
    pub fn includes(&self, clustering_key: &[u8]) -> bool {
        let start_ok = if self.start.is_empty() {
            true
        } else if self.start_inclusive {
            clustering_key >= self.start.as_slice()
        } else {
            clustering_key > self.start.as_slice()
        };

        let end_ok = if self.end.is_empty() {
            true
        } else if self.end_inclusive {
            clustering_key <= self.end.as_slice()
        } else {
            clustering_key < self.end.as_slice()
        };

        start_ok && end_ok
    }
}

// ─── Column Filter ──────────────────────────────────────────────

/// Which columns to include in the result.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.db.filter.ColumnFilter`
#[derive(Debug, Clone)]
pub enum ColumnFilter {
    /// All columns.
    All,
    /// Only these named columns.
    Selection(Vec<String>),
}

impl ColumnFilter {
    /// Whether a column name is included in this filter.
    pub fn includes(&self, column: &str) -> bool {
        match self {
            Self::All => true,
            Self::Selection(cols) => cols.iter().any(|c| c == column),
        }
    }
}

// ─── Read Limits ────────────────────────────────────────────────

/// Per-partition and per-query row/cell limits.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.db.filter.DataLimits`
#[derive(Debug, Clone)]
pub struct ReadLimits {
    /// Maximum rows to return per partition. 0 = unlimited.
    pub per_partition_limit: u32,
    /// Maximum rows across the entire query. 0 = unlimited.
    pub count_limit: u32,
    /// Page size (rows per page). 0 = no paging.
    pub page_size: u32,
    /// Whether this is a DISTINCT query (return only partition keys).
    pub is_distinct: bool,
}

impl ReadLimits {
    /// Unlimited reads.
    pub const UNLIMITED: Self = Self {
        per_partition_limit: 0,
        count_limit: 0,
        page_size: 0,
        is_distinct: false,
    };

    /// Effective per-partition limit.
    pub fn effective_per_partition_limit(&self) -> usize {
        if self.per_partition_limit == 0 {
            usize::MAX
        } else {
            self.per_partition_limit as usize
        }
    }

    /// Effective total count limit.
    pub fn effective_count_limit(&self) -> usize {
        if self.count_limit == 0 {
            usize::MAX
        } else {
            self.count_limit as usize
        }
    }

    /// Effective page size.
    pub fn effective_page_size(&self) -> usize {
        if self.page_size == 0 {
            usize::MAX
        } else {
            self.page_size as usize
        }
    }
}

impl Default for ReadLimits {
    fn default() -> Self {
        Self::UNLIMITED
    }
}

impl fmt::Display for ReadCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::SinglePartition(cmd) => {
                write!(f, "SinglePartition({}.{}, pk={} bytes)", cmd.keyspace, cmd.table, cmd.partition_key.len())
            }
            Self::PartitionRange(cmd) => {
                write!(f, "PartitionRange({}.{})", cmd.keyspace, cmd.table)
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn single_partition_command_construction() {
        let cmd = SinglePartitionReadCommand::full_partition("ks", "t1", b"pk1".to_vec());
        assert_eq!(cmd.keyspace, "ks");
        assert_eq!(cmd.table, "t1");
        assert_eq!(cmd.partition_key, b"pk1");
        assert!(!cmd.is_reversed);
        assert!(!cmd.is_digest_query);
        assert!(cmd.clustering_slice.is_none());
    }

    #[test]
    fn single_partition_with_limits() {
        let cmd = SinglePartitionReadCommand::full_partition("ks", "t1", b"pk1".to_vec())
            .with_limits(ReadLimits {
                per_partition_limit: 100,
                count_limit: 1000,
                page_size: 50,
                is_distinct: false,
            });
        assert_eq!(cmd.limits.effective_per_partition_limit(), 100);
        assert_eq!(cmd.limits.effective_count_limit(), 1000);
        assert_eq!(cmd.limits.effective_page_size(), 50);
    }

    #[test]
    fn range_command_construction() {
        let cmd = PartitionRangeReadCommand::full_scan("ks", "t1");
        assert_eq!(cmd.keyspace, "ks");
        assert!(!cmd.is_reversed);
    }

    #[test]
    fn data_range_full_ring_contains_all() {
        let range = DataRange::full_ring();
        assert!(range.contains(Token::from_raw(0)));
        assert!(range.contains(Token::from_raw(i64::MIN)));
        assert!(range.contains(Token::from_raw(i64::MAX)));
    }

    #[test]
    fn data_range_token_range_boundaries() {
        let range = DataRange::for_token_range(Token::from_raw(10), Token::from_raw(100));
        assert!(!range.contains(Token::from_raw(10))); // start exclusive
        assert!(range.contains(Token::from_raw(11)));
        assert!(range.contains(Token::from_raw(100))); // end inclusive
        assert!(!range.contains(Token::from_raw(101)));
    }

    #[test]
    fn clustering_slice_all() {
        let slice = ClusteringSlice::all();
        assert!(slice.includes(b"anything"));
        assert!(slice.includes(b""));
    }

    #[test]
    fn clustering_slice_filtering() {
        let slice = ClusteringSlice {
            start: b"b".to_vec(),
            end: b"d".to_vec(),
            start_inclusive: true,
            end_inclusive: true,
        };
        assert!(!slice.includes(b"a"));
        assert!(slice.includes(b"b"));
        assert!(slice.includes(b"c"));
        assert!(slice.includes(b"d"));
        assert!(!slice.includes(b"e"));
    }

    #[test]
    fn column_filter_all() {
        let filter = ColumnFilter::All;
        assert!(filter.includes("any_column"));
    }

    #[test]
    fn column_filter_selection() {
        let filter = ColumnFilter::Selection(vec!["a".into(), "b".into()]);
        assert!(filter.includes("a"));
        assert!(filter.includes("b"));
        assert!(!filter.includes("c"));
    }

    #[test]
    fn read_limits_defaults_are_unlimited() {
        let limits = ReadLimits::default();
        assert_eq!(limits.effective_per_partition_limit(), usize::MAX);
        assert_eq!(limits.effective_count_limit(), usize::MAX);
        assert_eq!(limits.effective_page_size(), usize::MAX);
    }

    #[test]
    fn read_command_display() {
        let cmd = ReadCommand::SinglePartition(
            SinglePartitionReadCommand::full_partition("ks", "users", b"pk1".to_vec())
        );
        let s = format!("{cmd}");
        assert!(s.contains("SinglePartition"));
        assert!(s.contains("ks.users"));
    }

    #[test]
    fn reversed_command() {
        let cmd = SinglePartitionReadCommand::full_partition("ks", "t1", b"pk1".to_vec())
            .reversed();
        assert!(cmd.is_reversed);
    }
}
