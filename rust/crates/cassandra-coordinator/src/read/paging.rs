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

//! Paging state for resumable queries.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.transport.PagingState`
//! - `org.apache.cassandra.db.filter.DataLimits`

use std::fmt;

use cassandra_storage::memtable::partition::PartitionData;

use super::response::PartitionResult;

/// Paging state that can be serialized into native protocol responses
/// and deserialized from subsequent requests to resume iteration.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.transport.PagingState` — serialized as:
/// `[partition_key][row_mark(clustering)][remaining][remaining_in_partition]`
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PagingState {
    /// The last partition key returned.
    pub partition_key: Vec<u8>,
    /// The last clustering key returned within that partition.
    /// Empty if we finished the partition.
    pub row_mark: Vec<u8>,
    /// Remaining rows in the overall query (global LIMIT tracking).
    pub remaining: u32,
    /// Remaining rows in the current partition (per-partition LIMIT tracking).
    pub remaining_in_partition: u32,
}

impl PagingState {
    /// Create a new paging state.
    pub fn new(
        partition_key: Vec<u8>,
        row_mark: Vec<u8>,
        remaining: u32,
        remaining_in_partition: u32,
    ) -> Self {
        Self {
            partition_key,
            row_mark,
            remaining,
            remaining_in_partition,
        }
    }

    /// Serialize to bytes for the native protocol paging_state field.
    ///
    /// Format: `[pk_len:4][pk:N][rm_len:4][rm:M][remaining:4][remaining_in_partition:4]`
    pub fn serialize(&self) -> Vec<u8> {
        let mut buf =
            Vec::with_capacity(4 + self.partition_key.len() + 4 + self.row_mark.len() + 8);
        buf.extend_from_slice(&(self.partition_key.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.partition_key);
        buf.extend_from_slice(&(self.row_mark.len() as u32).to_be_bytes());
        buf.extend_from_slice(&self.row_mark);
        buf.extend_from_slice(&self.remaining.to_be_bytes());
        buf.extend_from_slice(&self.remaining_in_partition.to_be_bytes());
        buf
    }

    /// Deserialize from bytes.
    pub fn deserialize(data: &[u8]) -> Option<Self> {
        if data.len() < 16 {
            return None;
        }
        let mut pos = 0;

        let pk_len = u32::from_be_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        if pos + pk_len > data.len() {
            return None;
        }
        let partition_key = data[pos..pos + pk_len].to_vec();
        pos += pk_len;

        if pos + 4 > data.len() {
            return None;
        }
        let rm_len = u32::from_be_bytes(data[pos..pos + 4].try_into().ok()?) as usize;
        pos += 4;
        if pos + rm_len > data.len() {
            return None;
        }
        let row_mark = data[pos..pos + rm_len].to_vec();
        pos += rm_len;

        if pos + 8 > data.len() {
            return None;
        }
        let remaining = u32::from_be_bytes(data[pos..pos + 4].try_into().ok()?);
        pos += 4;
        let remaining_in_partition = u32::from_be_bytes(data[pos..pos + 4].try_into().ok()?);

        Some(Self {
            partition_key,
            row_mark,
            remaining,
            remaining_in_partition,
        })
    }

    /// Whether this state indicates there are more rows to fetch.
    pub fn has_more(&self) -> bool {
        self.remaining > 0
    }
}

impl fmt::Display for PagingState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "PagingState(pk={} bytes, rm={} bytes, remaining={}, remaining_in_partition={})",
            self.partition_key.len(),
            self.row_mark.len(),
            self.remaining,
            self.remaining_in_partition,
        )
    }
}

/// Page size control for coordinated reads.
#[derive(Debug, Clone, Copy)]
pub struct PageSizeControl {
    /// Requested page size (rows).
    pub page_size: u32,
    /// Current offset within the global query.
    pub rows_fetched_so_far: u32,
}

impl PageSizeControl {
    pub fn new(page_size: u32) -> Self {
        Self {
            page_size,
            rows_fetched_so_far: 0,
        }
    }

    /// How many more rows we need for this page.
    pub fn rows_remaining(&self) -> u32 {
        self.page_size.saturating_sub(self.rows_fetched_so_far)
    }

    /// Record that some rows were fetched.
    pub fn record_fetched(&mut self, count: u32) {
        self.rows_fetched_so_far += count;
    }

    /// Whether the page is complete.
    pub fn is_page_complete(&self) -> bool {
        self.rows_fetched_so_far >= self.page_size
    }
}

/// One page returned by a [`QueryPager`].
#[derive(Debug, Clone)]
pub struct QueryPage {
    /// Page partitions. Partition row maps are trimmed to this page.
    pub partitions: Vec<PartitionResult>,
    /// Continuation state for the next page.
    pub paging_state: Option<PagingState>,
    /// Number of live rows returned in this page.
    pub rows_returned: usize,
}

/// Executes row-based paging over read partitions.
///
/// This mirrors the coordinator-facing responsibilities of Java's
/// `QueryPager`: enforce page size/count limit, return a continuation state,
/// and resume after the last returned partition/row marker.
#[derive(Debug, Clone)]
pub struct QueryPager {
    partitions: Vec<PartitionResult>,
    page_size: usize,
    remaining: usize,
    start_after: Option<(Vec<u8>, Vec<u8>)>,
}

impl QueryPager {
    /// Create a pager for a single command result.
    ///
    /// `page_size == 0` disables page-size limiting. `count_limit == 0`
    /// means "all rows".
    pub fn new(partitions: Vec<PartitionResult>, page_size: u32, count_limit: u32) -> Self {
        let available = rows_after(&partitions, None);
        let remaining = if count_limit == 0 {
            available
        } else {
            available.min(count_limit as usize)
        };
        Self {
            partitions,
            page_size: effective_limit(page_size),
            remaining,
            start_after: None,
        }
    }

    /// Resume a pager from a native-protocol paging state.
    pub fn from_paging_state(
        partitions: Vec<PartitionResult>,
        page_size: u32,
        state: PagingState,
    ) -> Self {
        Self {
            partitions,
            page_size: effective_limit(page_size),
            remaining: state.remaining as usize,
            start_after: Some((state.partition_key, state.row_mark)),
        }
    }

    /// Return the next page, or an empty page if no rows remain.
    pub fn next_page(&mut self) -> QueryPage {
        if self.remaining == 0 {
            return QueryPage {
                partitions: Vec::new(),
                paging_state: None,
                rows_returned: 0,
            };
        }

        let mut budget = self.page_size.min(self.remaining);
        if budget == 0 {
            budget = self.remaining;
        }

        let mut output = Vec::new();
        let mut rows_returned = 0usize;
        let mut last_marker: Option<(Vec<u8>, Vec<u8>, bool, u32)> = None;
        let start_after = self
            .start_after
            .as_ref()
            .map(|(pk, row)| (pk.as_slice(), row.as_slice()));

        for partition in &self.partitions {
            if budget == 0 {
                break;
            }
            let Some(data) = &partition.data else {
                continue;
            };
            let Some(start_idx) = resume_row_index(partition, start_after) else {
                continue;
            };
            if start_idx >= data.rows.len() {
                continue;
            }

            let mut page_data = PartitionData::new();
            if let (Some(ts), Some(ldt)) =
                (data.tombstone_timestamp, data.tombstone_local_deletion_time)
            {
                page_data.set_tombstone(ts, ldt);
            }

            let rows = data.rows.iter().collect::<Vec<_>>();
            let mut rows_in_partition = 0usize;
            for (idx, (_ck, row)) in rows.iter().enumerate().skip(start_idx) {
                if budget == 0 {
                    break;
                }
                page_data.apply_row((*row).clone());
                budget -= 1;
                rows_returned += 1;
                rows_in_partition += 1;

                let ended_partition = idx + 1 == rows.len();
                let remaining_in_partition = rows.len().saturating_sub(idx + 1) as u32;
                last_marker = Some((
                    partition.partition_key.clone(),
                    if ended_partition {
                        Vec::new()
                    } else {
                        row.clustering_key.clone()
                    },
                    ended_partition,
                    remaining_in_partition,
                ));
            }

            if rows_in_partition > 0 {
                output.push(PartitionResult {
                    partition_key: partition.partition_key.clone(),
                    data: Some(page_data),
                    live_row_count: rows_in_partition,
                    was_truncated: rows_in_partition < data.rows.len(),
                });
            }
        }

        self.remaining = self.remaining.saturating_sub(rows_returned);
        let paging_state = last_marker.and_then(
            |(partition_key, row_mark, ended_partition, remaining_in_partition)| {
                if self.remaining == 0 {
                    None
                } else {
                    self.start_after = Some((partition_key.clone(), row_mark.clone()));
                    Some(PagingState::new(
                        partition_key,
                        row_mark,
                        self.remaining.min(u32::MAX as usize) as u32,
                        if ended_partition {
                            0
                        } else {
                            remaining_in_partition
                        },
                    ))
                }
            },
        );

        QueryPage {
            partitions: output,
            paging_state,
            rows_returned,
        }
    }
}

/// Pager for range/multi-partition reads.
pub type MultiPartitionPager = QueryPager;

fn effective_limit(limit: u32) -> usize {
    if limit == 0 {
        usize::MAX
    } else {
        limit as usize
    }
}

fn rows_after(partitions: &[PartitionResult], start_after: Option<(&[u8], &[u8])>) -> usize {
    partitions
        .iter()
        .filter_map(|partition| {
            let data = partition.data.as_ref()?;
            let start_idx = resume_row_index(partition, start_after)?;
            Some(data.rows.len().saturating_sub(start_idx))
        })
        .sum()
}

fn resume_row_index(
    partition: &PartitionResult,
    start_after: Option<(&[u8], &[u8])>,
) -> Option<usize> {
    let data = partition.data.as_ref()?;
    let Some((last_pk, last_row)) = start_after else {
        return Some(0);
    };

    match partition.partition_key.as_slice().cmp(last_pk) {
        std::cmp::Ordering::Less => None,
        std::cmp::Ordering::Greater => Some(0),
        std::cmp::Ordering::Equal if last_row.is_empty() => Some(data.rows.len()),
        std::cmp::Ordering::Equal => {
            let mut idx = 0usize;
            for ck in data.rows.keys() {
                idx += 1;
                if ck.as_slice() == last_row {
                    return Some(idx);
                }
            }
            Some(0)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use cassandra_storage::memtable::partition::{Cell, Row};

    #[test]
    fn paging_state_roundtrip() {
        let state = PagingState::new(b"pk123".to_vec(), b"ck456".to_vec(), 42, 10);
        let bytes = state.serialize();
        let recovered = PagingState::deserialize(&bytes).unwrap();
        assert_eq!(state, recovered);
    }

    #[test]
    fn paging_state_empty_row_mark() {
        let state = PagingState::new(b"pk".to_vec(), Vec::new(), 100, 0);
        let bytes = state.serialize();
        let recovered = PagingState::deserialize(&bytes).unwrap();
        assert_eq!(recovered.row_mark, Vec::<u8>::new());
        assert_eq!(recovered.remaining, 100);
    }

    #[test]
    fn paging_state_has_more() {
        assert!(PagingState::new(b"pk".to_vec(), vec![], 10, 0).has_more());
        assert!(!PagingState::new(b"pk".to_vec(), vec![], 0, 0).has_more());
    }

    #[test]
    fn paging_state_deserialize_too_short() {
        assert!(PagingState::deserialize(b"short").is_none());
    }

    #[test]
    fn page_size_control() {
        let mut ctrl = PageSizeControl::new(10);
        assert_eq!(ctrl.rows_remaining(), 10);
        assert!(!ctrl.is_page_complete());

        ctrl.record_fetched(5);
        assert_eq!(ctrl.rows_remaining(), 5);
        assert!(!ctrl.is_page_complete());

        ctrl.record_fetched(5);
        assert_eq!(ctrl.rows_remaining(), 0);
        assert!(ctrl.is_page_complete());
    }

    #[test]
    fn page_size_control_overflow() {
        let mut ctrl = PageSizeControl::new(5);
        ctrl.record_fetched(10);
        assert_eq!(ctrl.rows_remaining(), 0);
        assert!(ctrl.is_page_complete());
    }

    #[test]
    fn paging_state_display() {
        let state = PagingState::new(b"pk".to_vec(), b"ck".to_vec(), 42, 10);
        let s = format!("{state}");
        assert!(s.contains("42"));
        assert!(s.contains("10"));
    }

    fn partition(pk: &[u8], row_keys: &[&[u8]]) -> PartitionResult {
        let mut data = PartitionData::new();
        for row_key in row_keys {
            data.apply_row(Row {
                clustering_key: row_key.to_vec(),
                cells: vec![Cell {
                    column: "v".to_string(),
                    value: Some(row_key.to_vec()),
                    timestamp: 1,
                    ttl: 0,
                    local_deletion_time: None,
                    is_tombstone: false,
                }],
                is_tombstone: false,
                local_deletion_time: None,
            });
        }
        PartitionResult {
            partition_key: pk.to_vec(),
            data: Some(data),
            live_row_count: row_keys.len(),
            was_truncated: false,
        }
    }

    #[test]
    fn query_pager_pages_across_partitions_and_resumes() {
        let partitions = vec![
            partition(b"pk1", &[b"ck1", b"ck2"]),
            partition(b"pk2", &[b"ck1", b"ck2"]),
        ];
        let mut pager = QueryPager::new(partitions.clone(), 3, 0);

        let first = pager.next_page();
        assert_eq!(first.rows_returned, 3);
        assert_eq!(first.partitions.len(), 2);
        let state = first.paging_state.unwrap();
        assert_eq!(state.partition_key, b"pk2");
        assert_eq!(state.row_mark, b"ck1");
        assert_eq!(state.remaining, 1);
        assert_eq!(state.remaining_in_partition, 1);

        let mut resumed = QueryPager::from_paging_state(partitions, 3, state);
        let second = resumed.next_page();
        assert_eq!(second.rows_returned, 1);
        assert!(second.paging_state.is_none());
        assert_eq!(second.partitions[0].partition_key, b"pk2");
    }

    #[test]
    fn query_pager_marks_finished_partition_with_empty_row_mark() {
        let partitions = vec![
            partition(b"pk1", &[b"ck1", b"ck2"]),
            partition(b"pk2", &[b"ck1"]),
        ];
        let mut pager = QueryPager::new(partitions.clone(), 2, 0);
        let first = pager.next_page();
        let state = first.paging_state.unwrap();
        assert_eq!(state.partition_key, b"pk1");
        assert!(state.row_mark.is_empty());

        let mut resumed = MultiPartitionPager::from_paging_state(partitions, 2, state);
        let second = resumed.next_page();
        assert_eq!(second.rows_returned, 1);
        assert_eq!(second.partitions[0].partition_key, b"pk2");
    }
}
