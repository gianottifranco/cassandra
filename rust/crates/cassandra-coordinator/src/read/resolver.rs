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

//! Digest and data resolvers for read coordination.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.service.reads.DigestResolver`
//! - `org.apache.cassandra.service.reads.DataResolver`
//! - `org.apache.cassandra.service.reads.ResponseResolver`

use cassandra_storage::memtable::partition::PartitionData;

use super::response::{DataResponse, Digest, PartitionResult, TombstoneThresholds, TombstoneTracker};

// ─── Digest Resolver ────────────────────────────────────────────

/// Resolves digest responses from multiple replicas.
///
/// When all digests match, the data response is returned directly.
/// When a mismatch is detected, a full data repair cycle is needed.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.service.reads.DigestResolver`
#[derive(Debug)]
pub struct DigestResolver {
    /// The data response (from the data replica).
    data_response: Option<DataResponse>,
    /// Digest of the data response.
    data_digest: Option<Digest>,
    /// Digests from digest replicas.
    digest_responses: Vec<Digest>,
    /// Required number of responses.
    required: usize,
}

impl DigestResolver {
    pub fn new(required: usize) -> Self {
        Self {
            data_response: None,
            data_digest: None,
            digest_responses: Vec::new(),
            required,
        }
    }

    /// Add the data response (full data from the chosen data replica).
    pub fn add_data_response(&mut self, response: DataResponse) {
        self.data_digest = Some(response.digest());
        self.data_response = Some(response);
    }

    /// Add a digest response from a digest replica.
    pub fn add_digest_response(&mut self, digest: Digest) {
        self.digest_responses.push(digest);
    }

    /// Check whether enough responses have been received.
    pub fn has_enough_responses(&self) -> bool {
        let total = (if self.data_response.is_some() { 1 } else { 0 })
            + self.digest_responses.len();
        total >= self.required
    }

    /// Resolve: return Ok(DataResponse) if digests match, Err(mismatch count) if not.
    pub fn resolve(self) -> Result<DataResponse, DigestMismatch> {
        let data_response = self.data_response.ok_or(DigestMismatch {
            mismatched_count: 0,
            data_digest: Digest::empty(),
            mismatched_digests: Vec::new(),
        })?;

        let data_digest = self.data_digest.unwrap_or_else(Digest::empty);

        let mut mismatched = Vec::new();
        for (i, digest) in self.digest_responses.iter().enumerate() {
            if *digest != data_digest {
                mismatched.push((i, digest.clone()));
            }
        }

        if mismatched.is_empty() {
            Ok(data_response)
        } else {
            Err(DigestMismatch {
                mismatched_count: mismatched.len(),
                data_digest,
                mismatched_digests: mismatched,
            })
        }
    }
}

/// Digest mismatch detected across replicas.
#[derive(Debug)]
pub struct DigestMismatch {
    /// How many digest replicas disagreed.
    pub mismatched_count: usize,
    /// The data replica's digest.
    pub data_digest: Digest,
    /// Indices and digests of mismatched replicas.
    pub mismatched_digests: Vec<(usize, Digest)>,
}

// ─── Data Resolver ──────────────────────────────────────────────

/// Resolves full data responses from multiple replicas via merge reconciliation.
///
/// Used after a digest mismatch: all replicas are re-queried for full data,
/// then merged using timestamp-based LWW resolution.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.service.reads.DataResolver`
#[derive(Debug)]
pub struct DataResolver {
    /// Full data responses from all replicas.
    responses: Vec<DataResponse>,
    /// Tombstone tracking thresholds.
    tombstone_thresholds: TombstoneThresholds,
}

impl DataResolver {
    pub fn new(tombstone_thresholds: TombstoneThresholds) -> Self {
        Self {
            responses: Vec::new(),
            tombstone_thresholds,
        }
    }

    pub fn with_default_thresholds() -> Self {
        Self::new(TombstoneThresholds::default())
    }

    /// Add a full data response from a replica.
    pub fn add_response(&mut self, response: DataResponse) {
        self.responses.push(response);
    }

    /// Merge all responses into a single reconciled result.
    ///
    /// Returns the merged data and a set of repair mutations that should
    /// be sent to stale replicas.
    pub fn resolve(self, now_seconds: i32) -> ResolvedData {
        if self.responses.is_empty() {
            return ResolvedData {
                data: DataResponse::empty(),
                repair_mutations: Vec::new(),
                tombstone_tracker: TombstoneTracker::new(self.tombstone_thresholds),
            };
        }

        if self.responses.len() == 1 {
            let data = self.responses.into_iter().next().unwrap();
            return ResolvedData {
                tombstone_tracker: TombstoneTracker::new(self.tombstone_thresholds),
                data,
                repair_mutations: Vec::new(),
            };
        }

        // Collect all partition keys across all responses
        let mut all_keys: Vec<Vec<u8>> = Vec::new();
        for resp in &self.responses {
            for p in &resp.partitions {
                if !all_keys.contains(&p.partition_key) {
                    all_keys.push(p.partition_key.clone());
                }
            }
        }

        let mut tracker = TombstoneTracker::new(self.tombstone_thresholds);
        let mut merged_partitions = Vec::new();
        let mut repairs = Vec::new();

        for pk in &all_keys {
            // Gather all PartitionData for this partition key from all responses
            let mut partition_versions: Vec<Option<&PartitionData>> = Vec::new();
            for resp in &self.responses {
                let pd = resp.partitions.iter()
                    .find(|p| p.partition_key == *pk)
                    .and_then(|p| p.data.as_ref());
                partition_versions.push(pd);
            }

            // Merge all versions
            let merged = merge_partition_data(&partition_versions);

            // Compute repair mutations: for each replica that doesn't have
            // the merged version, generate a repair.
            for (replica_idx, version) in partition_versions.iter().enumerate() {
                let version_digest = version.map(Digest::from_partition)
                    .unwrap_or_else(Digest::empty);
                let merged_digest = Digest::from_partition(&merged);
                if version_digest != merged_digest {
                    repairs.push(RepairMutation {
                        replica_index: replica_idx,
                        partition_key: pk.clone(),
                        merged_data: merged.clone(),
                    });
                }
            }

            let result = PartitionResult::from_partition_data(
                pk.clone(), merged, now_seconds, &mut tracker,
            );
            if result.live_row_count > 0 || result.data.is_some() {
                merged_partitions.push(result);
            }
        }

        let tombstones_read = tracker.count;
        ResolvedData {
            data: DataResponse {
                partitions: merged_partitions,
                tombstones_read,
                is_short_read: false,
            },
            repair_mutations: repairs,
            tombstone_tracker: tracker,
        }
    }
}

/// Result of data resolution.
#[derive(Debug)]
pub struct ResolvedData {
    /// The merged data response.
    pub data: DataResponse,
    /// Mutations to send to stale replicas for read repair.
    pub repair_mutations: Vec<RepairMutation>,
    /// Tombstone tracker.
    pub tombstone_tracker: TombstoneTracker,
}

/// A mutation to send to a stale replica during read repair.
#[derive(Debug, Clone)]
pub struct RepairMutation {
    /// Index of the replica in the contacted list.
    pub replica_index: usize,
    /// Partition key.
    pub partition_key: Vec<u8>,
    /// The merged data to write.
    pub merged_data: PartitionData,
}

// ─── Merge Logic ────────────────────────────────────────────────

/// Merge multiple versions of the same partition using timestamp-based LWW.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.db.rows.Rows.merge()`
fn merge_partition_data(versions: &[Option<&PartitionData>]) -> PartitionData {
    let mut merged = PartitionData::new();

    for version in versions {
        if let Some(pd) = version {
            // Merge partition tombstone
            if let (Some(ts), Some(ldt)) = (pd.tombstone_timestamp, pd.tombstone_local_deletion_time) {
                merged.set_tombstone(ts, ldt);
            }

            // Merge rows
            for (_ck, row) in &pd.rows {
                merged.apply_row(row.clone());
            }
        }
    }

    merged
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_storage::memtable::partition::{Cell, Row};

    fn make_cell(col: &str, val: &[u8], ts: i64) -> Cell {
        Cell {
            column: col.into(),
            value: Some(val.to_vec()),
            timestamp: ts,
            ttl: 0,
            local_deletion_time: None,
            is_tombstone: false,
        }
    }

    fn make_row(ck: &[u8], cells: Vec<Cell>) -> Row {
        Row {
            clustering_key: ck.to_vec(),
            cells,
            is_tombstone: false,
            local_deletion_time: None,
        }
    }

    fn make_partition(rows: Vec<Row>) -> PartitionData {
        let mut pd = PartitionData::new();
        for row in rows {
            pd.apply_row(row);
        }
        pd
    }

    fn make_data_response(pk: &[u8], pd: PartitionData) -> DataResponse {
        DataResponse {
            partitions: vec![PartitionResult {
                partition_key: pk.to_vec(),
                data: Some(pd.clone()),
                live_row_count: pd.rows.len(),
                was_truncated: false,
            }],
            tombstones_read: 0,
            is_short_read: false,
        }
    }

    #[test]
    fn digest_match_returns_data() {
        let pd = make_partition(vec![make_row(b"ck", vec![make_cell("c", b"v", 100)])]);
        let data_resp = make_data_response(b"pk", pd.clone());
        let data_digest = data_resp.digest();

        let mut resolver = DigestResolver::new(2);
        resolver.add_data_response(data_resp);
        resolver.add_digest_response(data_digest);

        let result = resolver.resolve();
        assert!(result.is_ok());
        assert_eq!(result.unwrap().row_count(), 1);
    }

    #[test]
    fn digest_mismatch_triggers_error() {
        let pd = make_partition(vec![make_row(b"ck", vec![make_cell("c", b"v", 100)])]);
        let data_resp = make_data_response(b"pk", pd);

        let mut resolver = DigestResolver::new(2);
        resolver.add_data_response(data_resp);
        resolver.add_digest_response(Digest::from_bytes(b"totally_different"));

        let result = resolver.resolve();
        assert!(result.is_err());
        let mismatch = result.unwrap_err();
        assert_eq!(mismatch.mismatched_count, 1);
    }

    #[test]
    fn data_resolver_merge_by_timestamp() {
        // Replica 1 has older data
        let pd1 = make_partition(vec![make_row(b"ck", vec![make_cell("c", b"old", 100)])]);
        // Replica 2 has newer data
        let pd2 = make_partition(vec![make_row(b"ck", vec![make_cell("c", b"new", 200)])]);

        let mut resolver = DataResolver::with_default_thresholds();
        resolver.add_response(make_data_response(b"pk", pd1));
        resolver.add_response(make_data_response(b"pk", pd2));

        let resolved = resolver.resolve(300);
        assert_eq!(resolved.data.row_count(), 1);

        let row = resolved.data.partitions[0].data.as_ref().unwrap()
            .rows.values().next().unwrap();
        let cell = row.cells.iter().find(|c| c.column == "c").unwrap();
        assert_eq!(cell.value.as_deref(), Some(b"new".as_slice()));
        assert_eq!(cell.timestamp, 200);
    }

    #[test]
    fn data_resolver_generates_repair_mutations() {
        let pd1 = make_partition(vec![make_row(b"ck", vec![make_cell("c", b"old", 100)])]);
        let pd2 = make_partition(vec![make_row(b"ck", vec![make_cell("c", b"new", 200)])]);

        let mut resolver = DataResolver::with_default_thresholds();
        resolver.add_response(make_data_response(b"pk", pd1));
        resolver.add_response(make_data_response(b"pk", pd2));

        let resolved = resolver.resolve(300);
        // Replica 0 has stale data, so it needs repair
        assert!(!resolved.repair_mutations.is_empty());
        let repair = resolved.repair_mutations.iter().find(|r| r.replica_index == 0);
        assert!(repair.is_some());
    }

    #[test]
    fn data_resolver_single_response_no_repair() {
        let pd = make_partition(vec![make_row(b"ck", vec![make_cell("c", b"v", 100)])]);
        let mut resolver = DataResolver::with_default_thresholds();
        resolver.add_response(make_data_response(b"pk", pd));

        let resolved = resolver.resolve(200);
        assert!(resolved.repair_mutations.is_empty());
        assert_eq!(resolved.data.row_count(), 1);
    }

    #[test]
    fn data_resolver_tombstone_wins_on_tie() {
        let pd1 = make_partition(vec![make_row(b"ck", vec![make_cell("c", b"val", 100)])]);

        let mut pd2 = PartitionData::new();
        pd2.apply_row(Row {
            clustering_key: b"ck".to_vec(),
            cells: vec![Cell {
                column: "c".into(),
                value: None,
                timestamp: 100,
                ttl: 0,
                local_deletion_time: Some(100),
                is_tombstone: true,
            }],
            is_tombstone: false,
            local_deletion_time: None,
        });

        let mut resolver = DataResolver::with_default_thresholds();
        resolver.add_response(make_data_response(b"pk", pd1));
        resolver.add_response(make_data_response(b"pk", pd2));

        let resolved = resolver.resolve(200);
        // The tombstone should win on equal timestamp — no live cells.
        // Either the partition has no live rows, or it's omitted entirely.
        let total_live: usize = resolved.data.partitions.iter()
            .map(|p| p.live_row_count)
            .sum();
        assert_eq!(total_live, 0);
    }

    #[test]
    fn data_resolver_merges_different_rows() {
        let pd1 = make_partition(vec![make_row(b"ck1", vec![make_cell("c", b"v1", 100)])]);
        let pd2 = make_partition(vec![make_row(b"ck2", vec![make_cell("c", b"v2", 100)])]);

        let mut resolver = DataResolver::with_default_thresholds();
        resolver.add_response(make_data_response(b"pk", pd1));
        resolver.add_response(make_data_response(b"pk", pd2));

        let resolved = resolver.resolve(200);
        assert_eq!(resolved.data.row_count(), 2);
    }

    #[test]
    fn digest_resolver_not_enough_responses() {
        let mut resolver = DigestResolver::new(3);
        let pd = make_partition(vec![make_row(b"ck", vec![make_cell("c", b"v", 100)])]);
        resolver.add_data_response(make_data_response(b"pk", pd));

        assert!(!resolver.has_enough_responses());
    }
}
