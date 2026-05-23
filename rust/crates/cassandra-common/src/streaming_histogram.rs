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

//! Streaming histogram builder for tombstone drop-time metadata.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.utils.streamhist.StreamingTombstoneHistogramBuilder`
//!
//! Cassandra stores tombstone drop-time histograms as sorted `(point, count)`
//! pairs. This builder keeps exact counts while the number of distinct points
//! is within capacity and then repeatedly merges the closest adjacent buckets.

use std::collections::BTreeMap;

/// A compact sorted histogram of tombstone drop times.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StreamingHistogram {
    entries: Vec<HistogramEntry>,
}

/// A single histogram bucket.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct HistogramEntry {
    pub point: i64,
    pub count: u64,
}

impl StreamingHistogram {
    pub fn new(entries: Vec<HistogramEntry>) -> Self {
        Self { entries }
    }

    pub fn entries(&self) -> &[HistogramEntry] {
        &self.entries
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn total_count(&self) -> u64 {
        self.entries.iter().map(|entry| entry.count).sum()
    }
}

/// Builds compact histograms from streaming tombstone observations.
#[derive(Debug, Clone)]
pub struct StreamingTombstoneHistogramBuilder {
    capacity: usize,
    counts: BTreeMap<i64, u64>,
}

impl StreamingTombstoneHistogramBuilder {
    /// Create a new builder capped at `capacity` buckets.
    pub fn new(capacity: usize) -> Self {
        assert!(capacity > 0, "histogram capacity must be positive");
        Self {
            capacity,
            counts: BTreeMap::new(),
        }
    }

    /// Add one observation at `point`.
    pub fn add(&mut self, point: i64) {
        self.add_count(point, 1);
    }

    /// Add `count` observations at `point`.
    pub fn add_count(&mut self, point: i64, count: u64) {
        if count == 0 {
            return;
        }
        *self.counts.entry(point).or_insert(0) += count;
    }

    /// Build the final compact histogram.
    pub fn build(&self) -> StreamingHistogram {
        let mut entries: Vec<HistogramEntry> = self
            .counts
            .iter()
            .map(|(&point, &count)| HistogramEntry { point, count })
            .collect();

        while entries.len() > self.capacity {
            merge_closest_pair(&mut entries);
        }

        StreamingHistogram::new(entries)
    }
}

fn merge_closest_pair(entries: &mut Vec<HistogramEntry>) {
    debug_assert!(entries.len() >= 2);

    let mut best_index = 0;
    let mut best_distance = i64::MAX;
    for idx in 0..entries.len() - 1 {
        let distance = entries[idx + 1].point.saturating_sub(entries[idx].point);
        if distance < best_distance {
            best_distance = distance;
            best_index = idx;
        }
    }

    let left = entries[best_index];
    let right = entries[best_index + 1];
    let count = left.count + right.count;
    let weighted_point = ((left.point as i128 * left.count as i128
        + right.point as i128 * right.count as i128)
        / count as i128) as i64;

    entries[best_index] = HistogramEntry {
        point: weighted_point,
        count,
    };
    entries.remove(best_index + 1);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_exact_points_within_capacity() {
        let mut builder = StreamingTombstoneHistogramBuilder::new(4);
        builder.add(10);
        builder.add(20);
        builder.add_count(20, 2);
        builder.add(30);

        let histogram = builder.build();
        assert_eq!(
            histogram.entries(),
            &[
                HistogramEntry {
                    point: 10,
                    count: 1
                },
                HistogramEntry {
                    point: 20,
                    count: 3
                },
                HistogramEntry {
                    point: 30,
                    count: 1
                },
            ]
        );
        assert_eq!(histogram.total_count(), 5);
    }

    #[test]
    fn merges_closest_adjacent_points_when_over_capacity() {
        let mut builder = StreamingTombstoneHistogramBuilder::new(2);
        builder.add(10);
        builder.add(11);
        builder.add(100);

        let histogram = builder.build();
        assert_eq!(
            histogram.entries(),
            &[
                HistogramEntry {
                    point: 10,
                    count: 2
                },
                HistogramEntry {
                    point: 100,
                    count: 1
                },
            ]
        );
    }

    #[test]
    fn weighted_merge_uses_observation_counts() {
        let mut builder = StreamingTombstoneHistogramBuilder::new(1);
        builder.add_count(10, 3);
        builder.add_count(20, 1);

        let histogram = builder.build();
        assert_eq!(
            histogram.entries(),
            &[HistogramEntry {
                point: 12,
                count: 4
            }]
        );
    }

    #[test]
    fn ignores_zero_count_updates() {
        let mut builder = StreamingTombstoneHistogramBuilder::new(2);
        builder.add_count(10, 0);
        assert!(builder.build().is_empty());
    }
}
