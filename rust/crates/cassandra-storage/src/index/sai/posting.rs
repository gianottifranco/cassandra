// Licensed under Apache License, Version 2.0.

//! Posting list: sorted collection of row locations for a given index term.
//!
//! ## Java Oracle
//!
//! `org.apache.cassandra.index.sai.disk.v1.postings.PostingsReader`
//!
//! A posting list stores the set of base-table rows that contain a given
//! indexed value.  Entries are sorted by (partition_key, clustering_key)
//! for efficient merging and intersection.

use serde::{Deserialize, Serialize};

/// A location in the base table identified by partition + clustering key.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub struct RowLocation {
    pub partition_key: Vec<u8>,
    pub clustering_key: Vec<u8>,
}

/// A sorted posting list of row locations.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PostingList {
    /// Sorted by (partition_key, clustering_key).
    locations: Vec<RowLocation>,
}

impl PostingList {
    pub fn new() -> Self {
        Self {
            locations: Vec::new(),
        }
    }

    /// Add a row location, maintaining sort order.
    pub fn add(&mut self, partition_key: Vec<u8>, clustering_key: Vec<u8>) {
        let loc = RowLocation {
            partition_key,
            clustering_key,
        };
        match self.locations.binary_search(&loc) {
            Ok(_) => {} // Already present
            Err(pos) => self.locations.insert(pos, loc),
        }
    }

    /// Remove a specific row location.
    pub fn remove(&mut self, partition_key: &[u8], clustering_key: &[u8]) {
        self.locations.retain(|loc| {
            loc.partition_key != partition_key || loc.clustering_key != clustering_key
        });
    }

    /// Get all locations.
    pub fn locations(&self) -> &[RowLocation] {
        &self.locations
    }

    /// Number of entries.
    pub fn len(&self) -> usize {
        self.locations.len()
    }

    /// Check if empty.
    pub fn is_empty(&self) -> bool {
        self.locations.is_empty()
    }

    /// Merge another posting list into this one (union).
    pub fn merge(&mut self, other: &PostingList) {
        for loc in &other.locations {
            self.add(loc.partition_key.clone(), loc.clustering_key.clone());
        }
    }

    /// Intersect with another posting list.
    pub fn intersect(&self, other: &PostingList) -> PostingList {
        let mut result = PostingList::new();
        let (mut i, mut j) = (0, 0);

        while i < self.locations.len() && j < other.locations.len() {
            match self.locations[i].cmp(&other.locations[j]) {
                std::cmp::Ordering::Equal => {
                    result.locations.push(self.locations[i].clone());
                    i += 1;
                    j += 1;
                }
                std::cmp::Ordering::Less => i += 1,
                std::cmp::Ordering::Greater => j += 1,
            }
        }

        result
    }

    /// Serialize to bytes (for on-disk persistence).
    pub fn serialize(&self) -> Vec<u8> {
        serde_json::to_vec(&self.locations).expect("PostingList serialization should never fail")
    }

    /// Deserialize from bytes.
    pub fn deserialize(data: &[u8]) -> Result<Self, String> {
        let locations: Vec<RowLocation> =
            serde_json::from_slice(data).map_err(|e| e.to_string())?;
        Ok(Self { locations })
    }
}

impl Default for PostingList {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn add_maintains_sort_order() {
        let mut pl = PostingList::new();
        pl.add(b"c".to_vec(), b"".to_vec());
        pl.add(b"a".to_vec(), b"".to_vec());
        pl.add(b"b".to_vec(), b"".to_vec());

        let pks: Vec<&[u8]> = pl
            .locations()
            .iter()
            .map(|l| l.partition_key.as_slice())
            .collect();
        assert_eq!(pks, vec![b"a".as_slice(), b"b", b"c"]);
    }

    #[test]
    fn add_deduplicates() {
        let mut pl = PostingList::new();
        pl.add(b"a".to_vec(), b"".to_vec());
        pl.add(b"a".to_vec(), b"".to_vec());
        assert_eq!(pl.len(), 1);
    }

    #[test]
    fn remove_entry() {
        let mut pl = PostingList::new();
        pl.add(b"a".to_vec(), b"1".to_vec());
        pl.add(b"b".to_vec(), b"2".to_vec());
        pl.remove(b"a", b"1");
        assert_eq!(pl.len(), 1);
        assert_eq!(pl.locations()[0].partition_key, b"b");
    }

    #[test]
    fn merge() {
        let mut pl1 = PostingList::new();
        pl1.add(b"a".to_vec(), b"".to_vec());
        pl1.add(b"c".to_vec(), b"".to_vec());

        let mut pl2 = PostingList::new();
        pl2.add(b"b".to_vec(), b"".to_vec());
        pl2.add(b"c".to_vec(), b"".to_vec()); // duplicate

        pl1.merge(&pl2);
        assert_eq!(pl1.len(), 3); // a, b, c
    }

    #[test]
    fn intersect() {
        let mut pl1 = PostingList::new();
        pl1.add(b"a".to_vec(), b"".to_vec());
        pl1.add(b"b".to_vec(), b"".to_vec());
        pl1.add(b"c".to_vec(), b"".to_vec());

        let mut pl2 = PostingList::new();
        pl2.add(b"b".to_vec(), b"".to_vec());
        pl2.add(b"c".to_vec(), b"".to_vec());
        pl2.add(b"d".to_vec(), b"".to_vec());

        let result = pl1.intersect(&pl2);
        assert_eq!(result.len(), 2); // b, c
    }

    #[test]
    fn serialize_round_trip() {
        let mut pl = PostingList::new();
        pl.add(b"pk1".to_vec(), b"ck1".to_vec());
        pl.add(b"pk2".to_vec(), b"ck2".to_vec());

        let bytes = pl.serialize();
        let decoded = PostingList::deserialize(&bytes).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded.locations()[0].partition_key, b"pk1");
    }

    #[test]
    fn empty_posting_list() {
        let pl = PostingList::new();
        assert!(pl.is_empty());
        assert_eq!(pl.len(), 0);
    }
}
