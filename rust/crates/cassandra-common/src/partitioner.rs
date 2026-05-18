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

//! Partitioners: map partition keys to tokens for ring placement.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.dht.IPartitioner`
//! - `org.apache.cassandra.dht.Murmur3Partitioner`
//! - `org.apache.cassandra.dht.RandomPartitioner`
//! - `org.apache.cassandra.dht.ByteOrderedPartitioner`

use std::collections::HashMap;

use crate::murmur3;
use crate::token::Token;

/// Partitioner trait: maps raw partition keys to tokens on the ring.
///
/// Every Cassandra cluster uses exactly one partitioner for the lifetime of
/// its data. The default (and recommended) is `Murmur3Partitioner`.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.dht.IPartitioner`
pub trait Partitioner: Send + Sync {
    /// Compute the token for a given partition key.
    fn get_token(&self, key: &[u8]) -> Token;

    /// Canonical class name (for compatibility with Java configs / system tables).
    fn name(&self) -> &'static str;

    /// Minimum token for this partitioner's token space.
    fn min_token(&self) -> Token;

    /// Maximum token for this partitioner's token space.
    fn max_token(&self) -> Token;

    /// Midpoint between two tokens (used for range splitting).
    fn midpoint(&self, left: Token, right: Token) -> Token {
        Token::midpoint(left, right)
    }

    /// Split a token range into `pieces` equal sub-ranges.
    ///
    /// Returns `pieces + 1` boundary tokens (including start and end).
    fn split(&self, start: Token, end: Token, pieces: usize) -> Vec<Token> {
        if pieces == 0 {
            return vec![start, end];
        }
        let mut result = Vec::with_capacity(pieces + 1);
        result.push(start);
        let s = start.value() as i128;
        let e = if end.value() <= start.value() {
            // Wrap-around: treat end as beyond max
            end.value() as i128 + (1i128 << 64)
        } else {
            end.value() as i128
        };
        for i in 1..pieces {
            let mid = s + (e - s) * i as i128 / pieces as i128;
            result.push(Token::from_raw(mid as i64));
        }
        result.push(end);
        result
    }

    /// Whether this partitioner preserves byte-order of keys.
    fn preserves_order(&self) -> bool {
        false
    }

    /// Generate a random token in this partitioner's token space.
    fn random_token(&self) -> Token {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        Token::from_raw(rng.r#gen())
    }

    /// Describe ownership: fraction of the ring owned by each token.
    ///
    /// Given a sorted list of tokens, returns the fraction of the total
    /// token space each token is responsible for (the range from the
    /// previous token to this token).
    fn describe_ownership(&self, sorted_tokens: &[Token]) -> HashMap<Token, f64> {
        let mut result = HashMap::new();
        if sorted_tokens.is_empty() {
            return result;
        }
        if sorted_tokens.len() == 1 {
            result.insert(sorted_tokens[0], 1.0);
            return result;
        }
        let total_range = self.max_token().value() as f64 - self.min_token().value() as f64 + 1.0;
        for i in 0..sorted_tokens.len() {
            let prev = if i == 0 {
                sorted_tokens[sorted_tokens.len() - 1]
            } else {
                sorted_tokens[i - 1]
            };
            let current = sorted_tokens[i];
            let range = if current.value() > prev.value() {
                (current.value() - prev.value()) as f64
            } else {
                // Wrap-around
                (self.max_token().value() as f64 - prev.value() as f64)
                    + (current.value() as f64 - self.min_token().value() as f64)
                    + 1.0
            };
            result.insert(current, range / total_range);
        }
        result
    }
}

/// Factory for creating tokens from string or byte representations.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.dht.Token.TokenFactory`
pub trait TokenFactory: Send + Sync {
    /// Parse a token from its string representation.
    fn from_string(&self, s: &str) -> Option<Token>;

    /// Create a token from its byte representation.
    fn from_bytes(&self, bytes: &[u8]) -> Option<Token>;

    /// Serialize a token to its string representation.
    fn to_string(&self, token: Token) -> String;

    /// Serialize a token to bytes.
    fn to_bytes(&self, token: Token) -> Vec<u8>;
}

/// Token factory for i64-based tokens (Murmur3, Random).
#[derive(Debug, Clone, Copy, Default)]
pub struct LongTokenFactory;

impl TokenFactory for LongTokenFactory {
    fn from_string(&self, s: &str) -> Option<Token> {
        s.parse::<i64>().ok().map(Token::from_raw)
    }

    fn from_bytes(&self, bytes: &[u8]) -> Option<Token> {
        if bytes.len() >= 8 {
            let mut buf = [0u8; 8];
            buf.copy_from_slice(&bytes[..8]);
            Some(Token::deserialize(&buf))
        } else {
            None
        }
    }

    fn to_string(&self, token: Token) -> String {
        token.value().to_string()
    }

    fn to_bytes(&self, token: Token) -> Vec<u8> {
        let mut buf = [0u8; 8];
        token.serialize(&mut buf);
        buf.to_vec()
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Murmur3Partitioner
// ─────────────────────────────────────────────────────────────────────────────

/// Murmur3Partitioner: the default and recommended partitioner.
///
/// Token space: `[i64::MIN, i64::MAX]`.
/// Uses the first 64 bits of Murmur3-128 (seed=0) as the token value.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.dht.Murmur3Partitioner`
#[derive(Debug, Clone, Copy, Default)]
pub struct Murmur3Partitioner;

impl Partitioner for Murmur3Partitioner {
    #[inline]
    fn get_token(&self, key: &[u8]) -> Token {
        Token::from_raw(murmur3::murmur3_token(key))
    }

    fn name(&self) -> &'static str {
        "org.apache.cassandra.dht.Murmur3Partitioner"
    }

    fn min_token(&self) -> Token {
        Token::MINIMUM
    }

    fn max_token(&self) -> Token {
        Token::MAXIMUM
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// RandomPartitioner
// ─────────────────────────────────────────────────────────────────────────────

/// RandomPartitioner: uses MD5 hashing. Token space is `[0, 2^127)`.
///
/// Tokens are mapped to i64 by taking the upper 64 bits of the MD5.
/// This partitioner exists for backward compatibility; new clusters
/// should use `Murmur3Partitioner`.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.dht.RandomPartitioner`
#[derive(Debug, Clone, Copy, Default)]
pub struct RandomPartitioner;

impl Partitioner for RandomPartitioner {
    fn get_token(&self, key: &[u8]) -> Token {
        // MD5 of the partition key, interpret upper 8 bytes as i64 (big-endian).
        // Java uses BigInteger over 16 bytes; we approximate with i64 for ring
        // compatibility while preserving uniform distribution.
        let digest = md5_hash(key);
        let value = i64::from_be_bytes([
            digest[0], digest[1], digest[2], digest[3], digest[4], digest[5], digest[6], digest[7],
        ]);
        // Ensure non-negative to match Java's BigInteger(1, md5)
        Token::from_raw(value & i64::MAX)
    }

    fn name(&self) -> &'static str {
        "org.apache.cassandra.dht.RandomPartitioner"
    }

    fn min_token(&self) -> Token {
        Token::from_raw(0)
    }

    fn max_token(&self) -> Token {
        Token::MAXIMUM
    }
}

/// Minimal MD5 — we use the `md-5` crate already in workspace deps.
fn md5_hash(data: &[u8]) -> [u8; 16] {
    use md5::{Digest, Md5};
    let mut hasher = Md5::new();
    hasher.update(data);
    let result = hasher.finalize();
    let mut out = [0u8; 16];
    out.copy_from_slice(&result);
    out
}

// ─────────────────────────────────────────────────────────────────────────────
// ByteOrderedPartitioner
// ─────────────────────────────────────────────────────────────────────────────

/// ByteOrderedPartitioner: tokens preserve the byte-order of the key.
///
/// Primarily used for ordered scans. **Not recommended** for production
/// because it can lead to hot-spots.
///
/// Token is the first 8 bytes of the key, zero-padded and interpreted
/// as a big-endian i64.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.dht.ByteOrderedPartitioner`
#[derive(Debug, Clone, Copy, Default)]
pub struct ByteOrderedPartitioner;

impl Partitioner for ByteOrderedPartitioner {
    fn get_token(&self, key: &[u8]) -> Token {
        let mut buf = [0u8; 8];
        let len = key.len().min(8);
        buf[..len].copy_from_slice(&key[..len]);
        Token::from_raw(i64::from_be_bytes(buf))
    }

    fn name(&self) -> &'static str {
        "org.apache.cassandra.dht.ByteOrderedPartitioner"
    }

    fn min_token(&self) -> Token {
        Token::MINIMUM
    }

    fn max_token(&self) -> Token {
        Token::MAXIMUM
    }

    fn preserves_order(&self) -> bool {
        true
    }

    fn random_token(&self) -> Token {
        use rand::Rng;
        let mut rng = rand::thread_rng();
        // For BOP, generate a random positive token (keys are typically positive)
        Token::from_raw(rng.r#gen::<i64>().abs())
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// LocalPartitioner
// ─────────────────────────────────────────────────────────────────────────────

/// LocalPartitioner: returns a fixed token for all keys.
///
/// Used by system keyspaces and local-only data that doesn't need
/// distributed partitioning. Every key maps to the same token.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.dht.LocalPartitioner`
#[derive(Debug, Clone, Copy)]
pub struct LocalPartitioner {
    token: Token,
}

impl LocalPartitioner {
    /// Create a LocalPartitioner that always returns the given token.
    pub fn new(token: Token) -> Self {
        Self { token }
    }
}

impl Default for LocalPartitioner {
    fn default() -> Self {
        Self {
            token: Token::from_raw(0),
        }
    }
}

impl Partitioner for LocalPartitioner {
    fn get_token(&self, _key: &[u8]) -> Token {
        self.token
    }

    fn name(&self) -> &'static str {
        "org.apache.cassandra.dht.LocalPartitioner"
    }

    fn min_token(&self) -> Token {
        self.token
    }

    fn max_token(&self) -> Token {
        self.token
    }

    fn preserves_order(&self) -> bool {
        true
    }

    fn random_token(&self) -> Token {
        self.token
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Factory
// ─────────────────────────────────────────────────────────────────────────────

/// Create a partitioner by its Java class name (or short alias).
///
/// Matches the `partitioner` key in `cassandra.yaml`.
pub fn create_partitioner(name: &str) -> Box<dyn Partitioner> {
    if name.contains("Murmur3Partitioner") || name == "murmur3" {
        Box::new(Murmur3Partitioner)
    } else if name.contains("RandomPartitioner") || name == "random" {
        Box::new(RandomPartitioner)
    } else if name.contains("ByteOrderedPartitioner") || name == "byteordered" {
        Box::new(ByteOrderedPartitioner)
    } else if name.contains("LocalPartitioner") || name == "local" {
        Box::new(LocalPartitioner::default())
    } else {
        // Default to Murmur3 (matches Java behavior)
        Box::new(Murmur3Partitioner)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn murmur3_matches_token() {
        let p = Murmur3Partitioner;
        let key = b"hello world";
        let t = p.get_token(key);
        assert_eq!(t, Token::from_partition_key(key));
    }

    #[test]
    fn murmur3_deterministic() {
        let p = Murmur3Partitioner;
        assert_eq!(p.get_token(b"test"), p.get_token(b"test"));
    }

    #[test]
    fn murmur3_name() {
        assert_eq!(
            Murmur3Partitioner.name(),
            "org.apache.cassandra.dht.Murmur3Partitioner"
        );
    }

    #[test]
    fn random_deterministic() {
        let p = RandomPartitioner;
        let t1 = p.get_token(b"key");
        let t2 = p.get_token(b"key");
        assert_eq!(t1, t2);
    }

    #[test]
    fn random_non_negative() {
        let p = RandomPartitioner;
        for i in 0..100u32 {
            let t = p.get_token(&i.to_be_bytes());
            assert!(t.value() >= 0, "RandomPartitioner token must be >= 0");
        }
    }

    #[test]
    fn random_different_keys() {
        let p = RandomPartitioner;
        assert_ne!(p.get_token(b"a"), p.get_token(b"b"));
    }

    #[test]
    fn bop_preserves_order() {
        let p = ByteOrderedPartitioner;
        let t1 = p.get_token(b"\x00\x01");
        let t2 = p.get_token(b"\x00\x02");
        let t3 = p.get_token(b"\x01\x00");
        assert!(t1 < t2);
        assert!(t2 < t3);
    }

    #[test]
    fn bop_short_key_zero_padded() {
        let p = ByteOrderedPartitioner;
        let t = p.get_token(b"\x01");
        // Should be 0x01_00_00_00_00_00_00_00 as i64
        assert_eq!(t.value(), 0x0100_0000_0000_0000_i64);
    }

    #[test]
    fn factory_murmur3() {
        let p = create_partitioner("org.apache.cassandra.dht.Murmur3Partitioner");
        assert_eq!(p.name(), "org.apache.cassandra.dht.Murmur3Partitioner");
    }

    #[test]
    fn factory_random() {
        let p = create_partitioner("random");
        assert_eq!(p.name(), "org.apache.cassandra.dht.RandomPartitioner");
    }

    #[test]
    fn factory_bop() {
        let p = create_partitioner("byteordered");
        assert_eq!(p.name(), "org.apache.cassandra.dht.ByteOrderedPartitioner");
    }

    #[test]
    fn factory_default_murmur3() {
        let p = create_partitioner("unknown");
        assert_eq!(p.name(), "org.apache.cassandra.dht.Murmur3Partitioner");
    }

    #[test]
    fn min_max_tokens() {
        let m = Murmur3Partitioner;
        assert!(m.min_token() < m.max_token());
        assert_eq!(m.min_token(), Token::MINIMUM);
        assert_eq!(m.max_token(), Token::MAXIMUM);

        let r = RandomPartitioner;
        assert!(r.min_token() < r.max_token());
        assert_eq!(r.min_token().value(), 0);
    }

    #[test]
    fn preserves_order() {
        assert!(!Murmur3Partitioner.preserves_order());
        assert!(!RandomPartitioner.preserves_order());
        assert!(ByteOrderedPartitioner.preserves_order());
        assert!(LocalPartitioner::default().preserves_order());
    }

    #[test]
    fn local_partitioner_fixed_token() {
        let p = LocalPartitioner::new(Token::from_raw(42));
        assert_eq!(p.get_token(b"any key"), Token::from_raw(42));
        assert_eq!(p.get_token(b"other key"), Token::from_raw(42));
        assert_eq!(p.min_token(), Token::from_raw(42));
        assert_eq!(p.max_token(), Token::from_raw(42));
    }

    #[test]
    fn factory_local() {
        let p = create_partitioner("local");
        assert_eq!(p.name(), "org.apache.cassandra.dht.LocalPartitioner");
    }

    #[test]
    fn split_range() {
        let p = Murmur3Partitioner;
        let splits = p.split(Token::from_raw(0), Token::from_raw(100), 4);
        assert_eq!(splits.len(), 5);
        assert_eq!(splits[0], Token::from_raw(0));
        assert_eq!(splits[1], Token::from_raw(25));
        assert_eq!(splits[2], Token::from_raw(50));
        assert_eq!(splits[3], Token::from_raw(75));
        assert_eq!(splits[4], Token::from_raw(100));
    }

    #[test]
    fn describe_ownership_even() {
        let p = Murmur3Partitioner;
        // Evenly spaced tokens: each should own ~1/3 of the ring
        // Token space: i64::MIN to i64::MAX, total range ~2^64
        // Split into thirds:
        let third = (i64::MAX as i128 - i64::MIN as i128) / 3;
        let tokens = vec![
            Token::from_raw((i64::MIN as i128 + third) as i64),
            Token::from_raw((i64::MIN as i128 + 2 * third) as i64),
            Token::from_raw(i64::MAX),
        ];
        let ownership = p.describe_ownership(&tokens);
        assert_eq!(ownership.len(), 3);
        // Each should own roughly 1/3 of the ring
        for (_, frac) in &ownership {
            assert!(*frac > 0.2 && *frac < 0.45, "fraction={}", frac);
        }
    }

    #[test]
    fn describe_ownership_single() {
        let p = Murmur3Partitioner;
        let tokens = vec![Token::from_raw(0)];
        let ownership = p.describe_ownership(&tokens);
        assert_eq!(ownership[&Token::from_raw(0)], 1.0);
    }

    #[test]
    fn token_factory_round_trip() {
        let factory = LongTokenFactory;
        let token = Token::from_raw(12345);
        let s = factory.to_string(token);
        assert_eq!(factory.from_string(&s), Some(token));
        let bytes = factory.to_bytes(token);
        assert_eq!(factory.from_bytes(&bytes), Some(token));
    }

    #[test]
    fn random_token_in_range() {
        let p = Murmur3Partitioner;
        // Just verify it doesn't panic and returns a valid token
        let t = p.random_token();
        assert!(t.value() >= i64::MIN && t.value() <= i64::MAX);
    }
}
