// Licensed under Apache License, Version 2.0.

//! Anti-compaction: splits SSTables into repaired/unrepaired after repair.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.db.compaction.CompactionManager.antiCompact`
//! - `org.apache.cassandra.repair.messages.AnticompactionRequest`
//!
//! ## Design
//!
//! After an incremental repair, data that was in the repaired ranges must
//! be marked as repaired (so it won't be re-repaired). This module provides
//! range classification logic — actual I/O is delegated to the compaction
//! layer.

use serde::{Deserialize, Serialize};
use uuid::Uuid;

use cassandra_common::Token;

/// Whether data is repaired, unrepaired, or pending repair.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum RepairedState {
    /// Data has not been repaired.
    Unrepaired,
    /// Data has been repaired in a committed session.
    Repaired(Uuid),
    /// Data is part of an in-progress repair session.
    PendingRepair(Uuid),
}

impl std::fmt::Display for RepairedState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Unrepaired => write!(f, "unrepaired"),
            Self::Repaired(id) => write!(f, "repaired({id})"),
            Self::PendingRepair(id) => write!(f, "pending({id})"),
        }
    }
}

/// Request to anti-compact after repair.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AntiCompactionRequest {
    /// Parent repair session identifier.
    pub parent_id: Uuid,
    /// Ranges that were repaired.
    pub ranges: Vec<(Token, Token)>,
    /// Keyspace.
    pub keyspace: String,
    /// Table.
    pub table: String,
}

/// Result of an anti-compaction operation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AntiCompactionResult {
    /// Number of partitions classified as repaired.
    pub repaired_count: u64,
    /// Number of partitions classified as unrepaired.
    pub unrepaired_count: u64,
    /// Total bytes processed.
    pub bytes_processed: u64,
}

/// Check whether a token range is fully covered by repaired ranges.
pub fn is_range_fully_repaired(range: (Token, Token), repaired_ranges: &[(Token, Token)]) -> bool {
    let (start, end) = (range.0.value(), range.1.value());
    if start >= end {
        return false; // Empty or wrapping range not supported here
    }

    // Collect repaired sub-ranges that overlap [start, end)
    let mut covering: Vec<(i64, i64)> = repaired_ranges
        .iter()
        .filter_map(|(rs, re)| {
            let (rs, re) = (rs.value(), re.value());
            if rs >= end || re <= start {
                None
            } else {
                Some((rs.max(start), re.min(end)))
            }
        })
        .collect();

    if covering.is_empty() {
        return false;
    }

    // Sort by start and merge
    covering.sort_by_key(|(s, _)| *s);
    let mut covered_end = start;
    for (s, e) in &covering {
        if *s > covered_end {
            return false; // Gap
        }
        covered_end = covered_end.max(*e);
    }

    covered_end >= end
}

/// Classify partition tokens into repaired and unrepaired sets.
///
/// Returns `(repaired_tokens, unrepaired_tokens)`.
pub fn classify_partitions(
    partition_tokens: &[Token],
    repaired_ranges: &[(Token, Token)],
) -> (Vec<Token>, Vec<Token>) {
    let mut repaired = Vec::new();
    let mut unrepaired = Vec::new();

    for &tok in partition_tokens {
        if token_in_any_range(tok, repaired_ranges) {
            repaired.push(tok);
        } else {
            unrepaired.push(tok);
        }
    }

    (repaired, unrepaired)
}

/// Check if a token falls within any of the given ranges.
fn token_in_any_range(tok: Token, ranges: &[(Token, Token)]) -> bool {
    let v = tok.value();
    ranges.iter().any(|(start, end)| {
        let (s, e) = (start.value(), end.value());
        if s <= e {
            v >= s && v < e
        } else {
            v >= s || v < e
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tok(v: i64) -> Token {
        Token::from_raw(v)
    }

    #[test]
    fn range_fully_repaired_exact() {
        let range = (tok(0), tok(100));
        let repaired = vec![(tok(0), tok(100))];
        assert!(is_range_fully_repaired(range, &repaired));
    }

    #[test]
    fn range_fully_repaired_multiple_segments() {
        let range = (tok(0), tok(100));
        let repaired = vec![(tok(0), tok(50)), (tok(50), tok(100))];
        assert!(is_range_fully_repaired(range, &repaired));
    }

    #[test]
    fn range_partially_repaired() {
        let range = (tok(0), tok(100));
        let repaired = vec![(tok(0), tok(50))];
        assert!(!is_range_fully_repaired(range, &repaired));
    }

    #[test]
    fn range_no_overlap() {
        let range = (tok(0), tok(100));
        let repaired = vec![(tok(200), tok(300))];
        assert!(!is_range_fully_repaired(range, &repaired));
    }

    #[test]
    fn range_empty_repaired() {
        let range = (tok(0), tok(100));
        assert!(!is_range_fully_repaired(range, &[]));
    }

    #[test]
    fn range_with_gap() {
        let range = (tok(0), tok(100));
        let repaired = vec![(tok(0), tok(40)), (tok(60), tok(100))];
        assert!(!is_range_fully_repaired(range, &repaired));
    }

    #[test]
    fn range_superset() {
        let range = (tok(10), tok(90));
        let repaired = vec![(tok(0), tok(100))];
        assert!(is_range_fully_repaired(range, &repaired));
    }

    #[test]
    fn classify_partitions_basic() {
        let tokens = vec![tok(10), tok(50), tok(150), tok(200)];
        let repaired = vec![(tok(0), tok(100))];
        let (rep, unrep) = classify_partitions(&tokens, &repaired);
        assert_eq!(rep.len(), 2); // 10, 50
        assert_eq!(unrep.len(), 2); // 150, 200
    }

    #[test]
    fn classify_partitions_empty() {
        let (rep, unrep) = classify_partitions(&[], &[(tok(0), tok(100))]);
        assert!(rep.is_empty());
        assert!(unrep.is_empty());
    }

    #[test]
    fn classify_all_repaired() {
        let tokens = vec![tok(10), tok(50)];
        let repaired = vec![(tok(0), tok(100))];
        let (rep, unrep) = classify_partitions(&tokens, &repaired);
        assert_eq!(rep.len(), 2);
        assert!(unrep.is_empty());
    }

    #[test]
    fn classify_none_repaired() {
        let tokens = vec![tok(150), tok(200)];
        let repaired = vec![(tok(0), tok(100))];
        let (rep, unrep) = classify_partitions(&tokens, &repaired);
        assert!(rep.is_empty());
        assert_eq!(unrep.len(), 2);
    }

    #[test]
    fn repaired_state_display() {
        let id = Uuid::new_v4();
        assert_eq!(RepairedState::Unrepaired.to_string(), "unrepaired");
        assert!(RepairedState::Repaired(id).to_string().contains("repaired"));
        assert!(
            RepairedState::PendingRepair(id)
                .to_string()
                .contains("pending")
        );
    }

    #[test]
    fn anti_compaction_request_serde() {
        let req = AntiCompactionRequest {
            parent_id: Uuid::new_v4(),
            ranges: vec![(tok(0), tok(100))],
            keyspace: "ks".into(),
            table: "t1".into(),
        };
        let json = serde_json::to_string(&req).unwrap();
        let deser: AntiCompactionRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.keyspace, "ks");
    }

    #[test]
    fn anti_compaction_result_serde() {
        let res = AntiCompactionResult {
            repaired_count: 100,
            unrepaired_count: 50,
            bytes_processed: 1024,
        };
        let json = serde_json::to_string(&res).unwrap();
        let deser: AntiCompactionResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deser.repaired_count, 100);
    }
}
