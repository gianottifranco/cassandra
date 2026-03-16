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

#[cfg(test)]
mod tests {
    use super::*;

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
}
