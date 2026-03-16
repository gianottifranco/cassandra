// Licensed under Apache License, Version 2.0.

//! DecoratedKey: partition key + precomputed token.
//!
//! ## Java Oracle
//! `org.apache.cassandra.db.DecoratedKey`

use cassandra_common::token::Token;

/// A partition key decorated with its precomputed token.
///
/// Ordering is by token first, then by raw key bytes as a tiebreaker
/// (important for ByteOrderedPartitioner, but Murmur3 rarely ties).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct DecoratedKey {
    /// The raw partition key bytes.
    pub key: Vec<u8>,
    /// Precomputed token (Murmur3 hash of key).
    pub token: Token,
}

impl DecoratedKey {
    /// Create a new decorated key, computing the token from the key.
    pub fn new(key: Vec<u8>) -> Self {
        let token = Token::from_partition_key(&key);
        Self { key, token }
    }

    /// Create with a precomputed token.
    pub const fn with_token(key: Vec<u8>, token: Token) -> Self {
        Self { key, token }
    }
}

impl Ord for DecoratedKey {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.token
            .cmp(&other.token)
            .then_with(|| self.key.cmp(&other.key))
    }
}

impl PartialOrd for DecoratedKey {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn token_computed_from_key() {
        let dk = DecoratedKey::new(b"test_key".to_vec());
        assert_eq!(dk.token, Token::from_partition_key(b"test_key"));
    }

    #[test]
    fn ordering_by_token() {
        let dk1 = DecoratedKey::with_token(b"a".to_vec(), Token::from_raw(100));
        let dk2 = DecoratedKey::with_token(b"b".to_vec(), Token::from_raw(200));
        assert!(dk1 < dk2);
    }

    #[test]
    fn ordering_tiebreak_by_key() {
        let dk1 = DecoratedKey::with_token(b"a".to_vec(), Token::from_raw(100));
        let dk2 = DecoratedKey::with_token(b"b".to_vec(), Token::from_raw(100));
        assert!(dk1 < dk2);
    }

    #[test]
    fn deterministic() {
        let dk1 = DecoratedKey::new(b"hello".to_vec());
        let dk2 = DecoratedKey::new(b"hello".to_vec());
        assert_eq!(dk1, dk2);
    }
}
