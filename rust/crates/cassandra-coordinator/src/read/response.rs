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

//! Read response types with tombstone tracking and digest computation.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.db.ReadResponse`
//! - `org.apache.cassandra.db.partitions.UnfilteredPartitionIterator`
//! - `org.apache.cassandra.service.reads.TombstoneCounter`

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use tracing::warn;

use cassandra_storage::memtable::partition::{Cell, PartitionData, Row};

// ─── Digest ─────────────────────────────────────────────────────

/// A digest (hash) of a read response for comparison across replicas.
///
/// ## Java Oracle
///
/// Digest is computed via `ReadResponse.digest()` using MD5 in Java.
/// We use a fast non-crypto hash for the same purpose.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Digest(pub [u8; 8]);

impl Digest {
    /// Compute digest from a `PartitionData`.
    pub fn from_partition(data: &PartitionData) -> Self {
        let mut hasher = DefaultHasher::new();
        // Hash tombstone
        data.tombstone_timestamp.hash(&mut hasher);
        data.tombstone_local_deletion_time.hash(&mut hasher);
        // Hash rows in order (BTreeMap iteration is sorted)
        for (ck, row) in &data.rows {
            ck.hash(&mut hasher);
            row.is_tombstone.hash(&mut hasher);
            row.local_deletion_time.hash(&mut hasher);
            for cell in &row.cells {
                cell.column.hash(&mut hasher);
                cell.value.hash(&mut hasher);
                cell.timestamp.hash(&mut hasher);
                cell.ttl.hash(&mut hasher);
                cell.is_tombstone.hash(&mut hasher);
            }
        }
        Self(hasher.finish().to_be_bytes())
    }

    /// Compute digest from raw bytes.
    pub fn from_bytes(data: &[u8]) -> Self {
        let mut hasher = DefaultHasher::new();
        data.hash(&mut hasher);
        Self(hasher.finish().to_be_bytes())
    }

    /// Empty digest (for missing data).
    pub fn empty() -> Self {
        Self([0u8; 8])
    }
}

// ─── Read Response ──────────────────────────────────────────────

/// Response from a single replica for a read request.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.db.ReadResponse` — can be data or digest.
#[derive(Debug, Clone)]
pub enum ReadResponse {
    /// Full data response.
    Data(DataResponse),
    /// Digest-only response.
    DigestOnly(Digest),
}

impl ReadResponse {
    /// Get the digest of this response.
    pub fn digest(&self) -> Digest {
        match self {
            Self::Data(data) => data.digest(),
            Self::DigestOnly(d) => d.clone(),
        }
    }

    /// Try to get the data from this response.
    pub fn into_data(self) -> Option<DataResponse> {
        match self {
            Self::Data(data) => Some(data),
            Self::DigestOnly(_) => None,
        }
    }

    pub fn is_data(&self) -> bool {
        matches!(self, Self::Data(_))
    }
}

/// Full data response from a replica.
#[derive(Debug, Clone)]
pub struct DataResponse {
    /// Partitions returned.
    pub partitions: Vec<PartitionResult>,
    /// Number of tombstones encountered during the read.
    pub tombstones_read: u32,
    /// Whether more data is available (for paging).
    pub is_short_read: bool,
}

impl DataResponse {
    /// Create an empty data response.
    pub fn empty() -> Self {
        Self {
            partitions: Vec::new(),
            tombstones_read: 0,
            is_short_read: false,
        }
    }

    /// Create a data response from a single partition.
    pub fn from_partition(partition_key: Vec<u8>, data: PartitionData, now_seconds: i32) -> Self {
        let mut tracker = TombstoneTracker::new(TombstoneThresholds::default());
        let partition =
            PartitionResult::from_partition_data(partition_key, data, now_seconds, &mut tracker);
        Self {
            partitions: vec![partition],
            tombstones_read: tracker.count,
            is_short_read: false,
        }
    }

    /// Compute digest of this data response.
    pub fn digest(&self) -> Digest {
        let mut hasher = DefaultHasher::new();
        for p in &self.partitions {
            p.partition_key.hash(&mut hasher);
            if let Some(ref data) = p.data {
                data.tombstone_timestamp.hash(&mut hasher);
                for (ck, row) in &data.rows {
                    ck.hash(&mut hasher);
                    for cell in &row.cells {
                        cell.column.hash(&mut hasher);
                        cell.value.hash(&mut hasher);
                        cell.timestamp.hash(&mut hasher);
                    }
                }
            }
        }
        Digest(hasher.finish().to_be_bytes())
    }

    /// Total rows across all partitions.
    pub fn row_count(&self) -> usize {
        self.partitions.iter().map(|p| p.row_count()).sum()
    }
}

/// A single partition in a read response.
#[derive(Debug, Clone)]
pub struct PartitionResult {
    /// Partition key.
    pub partition_key: Vec<u8>,
    /// The partition data (filtered, live rows only).
    pub data: Option<PartitionData>,
    /// Number of live rows in this partition.
    pub live_row_count: usize,
    /// Whether this partition was truncated by per-partition limit.
    pub was_truncated: bool,
}

impl PartitionResult {
    /// Build from a `PartitionData`, filtering tombstones and expired TTLs.
    pub fn from_partition_data(
        partition_key: Vec<u8>,
        data: PartitionData,
        now_seconds: i32,
        tracker: &mut TombstoneTracker,
    ) -> Self {
        let mut live_data = PartitionData::new();
        let mut live_count = 0usize;

        // Copy partition tombstone
        if let (Some(ts), Some(ldt)) =
            (data.tombstone_timestamp, data.tombstone_local_deletion_time)
        {
            live_data.set_tombstone(ts, ldt);
            tracker.track_partition_tombstone();
        }

        for (ck, row) in &data.rows {
            if row.is_tombstone {
                tracker.track_row_tombstone();
                continue;
            }
            // Check partition tombstone
            if let Some(pt_ts) = data.tombstone_timestamp {
                if !row.cells.iter().any(|c| c.timestamp > pt_ts) {
                    tracker.track_row_tombstone();
                    continue;
                }
            }

            let mut live_cells: Vec<Cell> = Vec::new();
            for cell in &row.cells {
                if cell.is_tombstone {
                    tracker.track_cell_tombstone();
                    continue;
                }
                if !cell.is_live_at(now_seconds) {
                    tracker.track_cell_tombstone();
                    continue;
                }
                live_cells.push(cell.clone());
            }

            if !live_cells.is_empty() {
                live_data.apply_row(Row {
                    clustering_key: ck.clone(),
                    cells: live_cells,
                    is_tombstone: false,
                    local_deletion_time: None,
                });
                live_count += 1;
            }
        }

        Self {
            partition_key,
            data: if live_count > 0 || live_data.tombstone_timestamp.is_some() {
                Some(live_data)
            } else {
                None
            },
            live_row_count: live_count,
            was_truncated: false,
        }
    }

    pub fn row_count(&self) -> usize {
        self.live_row_count
    }
}

// ─── Tombstone Tracking ─────────────────────────────────────────

/// Thresholds for tombstone warnings and failures.
///
/// ## Java Oracle
///
/// `cassandra.yaml`: `tombstone_warn_threshold` (1000 default),
/// `tombstone_failure_threshold` (100000 default).
#[derive(Debug, Clone)]
pub struct TombstoneThresholds {
    /// Warn when this many tombstones are scanned.
    pub warn_threshold: u32,
    /// Fail when this many tombstones are scanned.
    pub fail_threshold: u32,
}

impl Default for TombstoneThresholds {
    fn default() -> Self {
        Self {
            warn_threshold: 1000,
            fail_threshold: 100_000,
        }
    }
}

/// Tracks tombstones encountered during a read and enforces thresholds.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.db.filter.TombstoneCounter`
#[derive(Debug)]
pub struct TombstoneTracker {
    /// Total tombstones encountered.
    pub count: u32,
    /// Thresholds.
    pub thresholds: TombstoneThresholds,
    /// Whether the warning threshold was reached.
    pub warning_issued: bool,
    /// Warnings to include in the response.
    pub warnings: Vec<String>,
}

impl TombstoneTracker {
    pub fn new(thresholds: TombstoneThresholds) -> Self {
        Self {
            count: 0,
            thresholds,
            warning_issued: false,
            warnings: Vec::new(),
        }
    }

    /// Track a partition-level tombstone.
    pub fn track_partition_tombstone(&mut self) {
        self.count += 1;
        self.check_thresholds();
    }

    /// Track a row-level tombstone.
    pub fn track_row_tombstone(&mut self) {
        self.count += 1;
        self.check_thresholds();
    }

    /// Track a cell-level tombstone.
    pub fn track_cell_tombstone(&mut self) {
        self.count += 1;
        self.check_thresholds();
    }

    /// Check if we've exceeded the failure threshold.
    pub fn is_failure(&self) -> bool {
        self.count >= self.thresholds.fail_threshold
    }

    /// Check thresholds and emit warnings.
    fn check_thresholds(&mut self) {
        if !self.warning_issued && self.count >= self.thresholds.warn_threshold {
            self.warning_issued = true;
            let msg = format!(
                "Read {} tombstones, which exceeds the warning threshold of {}",
                self.count, self.thresholds.warn_threshold
            );
            warn!("{}", msg);
            self.warnings.push(msg);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn digest_from_partition_deterministic() {
        let mut pd = PartitionData::new();
        pd.apply_row(Row {
            clustering_key: b"ck1".to_vec(),
            cells: vec![Cell {
                column: "col".into(),
                value: Some(b"val".to_vec()),
                timestamp: 100,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });
        let d1 = Digest::from_partition(&pd);
        let d2 = Digest::from_partition(&pd);
        assert_eq!(d1, d2);
    }

    #[test]
    fn digest_changes_with_different_data() {
        let mut pd1 = PartitionData::new();
        pd1.apply_row(Row {
            clustering_key: b"ck1".to_vec(),
            cells: vec![Cell {
                column: "col".into(),
                value: Some(b"val1".to_vec()),
                timestamp: 100,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });

        let mut pd2 = PartitionData::new();
        pd2.apply_row(Row {
            clustering_key: b"ck1".to_vec(),
            cells: vec![Cell {
                column: "col".into(),
                value: Some(b"val2".to_vec()),
                timestamp: 200,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });

        assert_ne!(Digest::from_partition(&pd1), Digest::from_partition(&pd2));
    }

    #[test]
    fn tombstone_warn_threshold() {
        let mut tracker = TombstoneTracker::new(TombstoneThresholds {
            warn_threshold: 3,
            fail_threshold: 100,
        });
        tracker.track_row_tombstone();
        tracker.track_row_tombstone();
        assert!(!tracker.warning_issued);
        tracker.track_row_tombstone();
        assert!(tracker.warning_issued);
        assert_eq!(tracker.warnings.len(), 1);
    }

    #[test]
    fn tombstone_fail_threshold() {
        let mut tracker = TombstoneTracker::new(TombstoneThresholds {
            warn_threshold: 1,
            fail_threshold: 5,
        });
        for _ in 0..5 {
            tracker.track_cell_tombstone();
        }
        assert!(tracker.is_failure());
    }

    #[test]
    fn tombstone_tracker_counts_all_types() {
        let mut tracker = TombstoneTracker::new(TombstoneThresholds::default());
        tracker.track_partition_tombstone();
        tracker.track_row_tombstone();
        tracker.track_cell_tombstone();
        assert_eq!(tracker.count, 3);
    }

    #[test]
    fn data_response_empty() {
        let resp = DataResponse::empty();
        assert_eq!(resp.row_count(), 0);
        assert!(resp.partitions.is_empty());
    }

    #[test]
    fn read_response_digest_roundtrip() {
        let mut pd = PartitionData::new();
        pd.apply_row(Row {
            clustering_key: b"ck".to_vec(),
            cells: vec![Cell {
                column: "c".into(),
                value: Some(b"v".to_vec()),
                timestamp: 1,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });

        let data_resp = DataResponse::from_partition(b"pk".to_vec(), pd, 100);
        let digest_from_data = ReadResponse::Data(data_resp.clone()).digest();
        let digest_only = ReadResponse::DigestOnly(digest_from_data.clone());
        assert_eq!(digest_only.digest(), digest_from_data);
    }

    #[test]
    fn partition_result_filters_tombstones() {
        let mut pd = PartitionData::new();
        // Live row
        pd.apply_row(Row {
            clustering_key: b"ck1".to_vec(),
            cells: vec![Cell {
                column: "c".into(),
                value: Some(b"live".to_vec()),
                timestamp: 100,
                ttl: 0,
                local_deletion_time: None,
                is_tombstone: false,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });
        // Tombstone row
        pd.apply_row(Row {
            clustering_key: b"ck2".to_vec(),
            cells: Vec::new(),
            is_tombstone: true,
            local_deletion_time: Some(50),
        });

        let mut tracker = TombstoneTracker::new(TombstoneThresholds::default());
        let result = PartitionResult::from_partition_data(b"pk".to_vec(), pd, 100, &mut tracker);
        assert_eq!(result.live_row_count, 1);
        assert_eq!(tracker.count, 1); // one row tombstone
    }
}
