// Licensed under Apache License, Version 2.0.

//! Anti-compaction: separate repaired and unrepaired partitions into different SSTables.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.db.compaction.CompactionManager#performAnticompaction`

use cassandra_common::Token;
use crate::memtable::partition::PartitionData;
use crate::compaction::anticompact_partitions;

/// Anti-compacts a set of partitions: separates partitions into "repaired" and "unrepaired"
/// depending on whether they fall within the given token range.
pub fn anticompact_range(
    partitions: Vec<(Vec<u8>, PartitionData)>,
    range: (Token, Token),
) -> (
    Vec<(Vec<u8>, PartitionData)>, // Inside range (repaired)
    Vec<(Vec<u8>, PartitionData)>, // Outside range (unrepaired)
) {
    anticompact_partitions(partitions, |pk| {
        let pk_token = Token::from_partition_key(pk);
        if range.0.value() <= range.1.value() {
            pk_token.value() >= range.0.value() && pk_token.value() < range.1.value()
        } else {
            pk_token.value() >= range.0.value() || pk_token.value() < range.1.value()
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn anticompact_splits_correctly() {
        let p_in = PartitionData::new();
        let _p_out = PartitionData::new();

        let mut partitions = Vec::new();

        // Let's create partitions and find their actual tokens to mock ranges.
        let pk_1 = b"pk_1".to_vec();
        let t_1 = Token::from_partition_key(&pk_1);

        partitions.push((pk_1, p_in));

        // Let's split using a range that includes t_1
        let range = (Token::from_raw(t_1.value() - 100), Token::from_raw(t_1.value() + 100));

        let (inside, outside) = anticompact_range(partitions, range);

        assert_eq!(inside.len(), 1);
        assert_eq!(outside.len(), 0);
    }
}
