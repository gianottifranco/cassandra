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

use crate::murmur3;
use crate::token::Token;

/// Partitioner trait: maps raw partition keys to tokens on the ring.
///
/// Every Cassandra cluster uses exactly one partitioner for the lifetime of
/// its data. The default (and recommended) is `Murmur3Partitioner`.
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
            digest[0], digest[1], digest[2], digest[3],
            digest[4], digest[5], digest[6], digest[7],
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
    use md5::{Md5, Digest};
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
}
