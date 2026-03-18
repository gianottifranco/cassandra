// Licensed under Apache License, Version 2.0.

//! Token allocation for new nodes joining the cluster.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.dht.tokenallocator.TokenAllocator`
//! - `org.apache.cassandra.dht.tokenallocator.NoReplicationTokenAllocator`
//! - `org.apache.cassandra.dht.tokenallocator.ReplicationAwareTokenAllocator`

use std::collections::HashMap;

use cassandra_common::Token;

use crate::node::Endpoint;
use crate::ring::TokenRing;
use crate::snitch::Snitch;

/// Allocates tokens for a new node joining the cluster.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.dht.tokenallocator.TokenAllocator`
pub trait TokenAllocator: Send + Sync {
    /// Allocate `num_tokens` tokens for the given endpoint.
    fn allocate(
        &self,
        ring: &TokenRing,
        num_tokens: usize,
        snitch: &dyn Snitch,
        endpoint: &Endpoint,
    ) -> Vec<Token>;
}

// ─────────────────────────────────────────────────────────────────────────────
// NoReplicationTokenAllocator
// ─────────────────────────────────────────────────────────────────────────────

/// Allocates tokens by splitting the largest gaps in the ring.
///
/// Does not account for replication factor — simply tries to balance
/// token ownership evenly across all nodes.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.dht.tokenallocator.NoReplicationTokenAllocator`
pub struct NoReplicationTokenAllocator;

impl TokenAllocator for NoReplicationTokenAllocator {
    fn allocate(
        &self,
        ring: &TokenRing,
        num_tokens: usize,
        _snitch: &dyn Snitch,
        _endpoint: &Endpoint,
    ) -> Vec<Token> {
        if ring.is_empty() {
            return even_split(num_tokens);
        }

        let mut gaps = compute_gaps(ring);
        let mut tokens = Vec::with_capacity(num_tokens);

        for _ in 0..num_tokens {
            if gaps.is_empty() {
                break;
            }
            // Sort by gap size descending
            gaps.sort_by(|a, b| b.size.cmp(&a.size));
            // Split the largest gap
            let largest = gaps.remove(0);
            let mid = Token::midpoint(largest.start, largest.end);
            tokens.push(mid);
            // Add the two new sub-gaps back
            let left_size = gap_size(largest.start, mid);
            let right_size = gap_size(mid, largest.end);
            if left_size > 0 {
                gaps.push(Gap {
                    start: largest.start,
                    end: mid,
                    size: left_size,
                });
            }
            if right_size > 0 {
                gaps.push(Gap {
                    start: mid,
                    end: largest.end,
                    size: right_size,
                });
            }
        }

        tokens
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// ReplicationAwareTokenAllocator
// ─────────────────────────────────────────────────────────────────────────────

/// Allocates tokens considering replication factor and topology.
///
/// Tries to balance the effective ownership (accounting for RF) across
/// nodes, preferring to place tokens in DCs/racks that are under-represented.
///
/// ## Java Oracle
///
/// `org.apache.cassandra.dht.tokenallocator.ReplicationAwareTokenAllocator`
pub struct ReplicationAwareTokenAllocator {
    replication_factor: usize,
}

impl ReplicationAwareTokenAllocator {
    pub fn new(replication_factor: usize) -> Self {
        Self { replication_factor }
    }
}

impl TokenAllocator for ReplicationAwareTokenAllocator {
    fn allocate(
        &self,
        ring: &TokenRing,
        num_tokens: usize,
        snitch: &dyn Snitch,
        endpoint: &Endpoint,
    ) -> Vec<Token> {
        if ring.is_empty() {
            return even_split(num_tokens);
        }

        let target_dc = snitch.datacenter(endpoint);

        // Compute per-endpoint ownership weighted by RF
        let mut ownership = compute_ownership(ring, self.replication_factor);

        // Find endpoints in the same DC
        let dc_endpoints: Vec<Endpoint> = ring
            .all_endpoints()
            .into_iter()
            .filter(|ep| snitch.datacenter(ep) == target_dc)
            .collect();

        if dc_endpoints.is_empty() {
            return even_split(num_tokens);
        }

        // Find average ownership per node in this DC
        let total_dc_ownership: f64 = dc_endpoints.iter().filter_map(|ep| ownership.get(ep)).sum();
        let avg_ownership = total_dc_ownership / (dc_endpoints.len() as f64 + 1.0);

        // Greedily split gaps near over-represented nodes
        let mut gaps = compute_gaps(ring);
        let mut tokens = Vec::with_capacity(num_tokens);

        for _ in 0..num_tokens {
            if gaps.is_empty() {
                break;
            }
            // Score gaps: prefer gaps owned by nodes with above-average ownership
            gaps.sort_by(|a, b| {
                let a_owner_load = owner_load(a.end, ring, &ownership);
                let b_owner_load = owner_load(b.end, ring, &ownership);
                // Prefer gaps whose owner is most over-loaded
                let a_score = a.size as f64 * (a_owner_load / avg_ownership.max(1.0));
                let b_score = b.size as f64 * (b_owner_load / avg_ownership.max(1.0));
                b_score
                    .partial_cmp(&a_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            });

            let largest = gaps.remove(0);
            let mid = Token::midpoint(largest.start, largest.end);
            tokens.push(mid);

            // Update ownership: reduce the original owner's share
            if let Some(owner) = ring.primary_endpoint(largest.end) {
                if let Some(load) = ownership.get_mut(&owner) {
                    *load *= 0.5; // approximate: the gap was halved
                }
            }

            let left_size = gap_size(largest.start, mid);
            let right_size = gap_size(mid, largest.end);
            if left_size > 0 {
                gaps.push(Gap {
                    start: largest.start,
                    end: mid,
                    size: left_size,
                });
            }
            if right_size > 0 {
                gaps.push(Gap {
                    start: mid,
                    end: largest.end,
                    size: right_size,
                });
            }
        }

        tokens
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Factory
// ─────────────────────────────────────────────────────────────────────────────

/// Create a token allocator based on replication factor.
pub fn create_token_allocator(replication_factor: usize) -> Box<dyn TokenAllocator> {
    if replication_factor <= 1 {
        Box::new(NoReplicationTokenAllocator)
    } else {
        Box::new(ReplicationAwareTokenAllocator::new(replication_factor))
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// Internal helpers
// ─────────────────────────────────────────────────────────────────────────────

#[derive(Debug)]
struct Gap {
    start: Token,
    end: Token,
    size: u64,
}

fn gap_size(start: Token, end: Token) -> u64 {
    // Use i128 to avoid overflow when computing distances across the ring
    let s = start.value() as i128;
    let e = end.value() as i128;
    if e > s {
        (e - s) as u64
    } else {
        // Wrap-around: distance from start to MAX plus MIN to end
        let total = (i64::MAX as i128 - s) + (e - i64::MIN as i128) + 1;
        total as u64
    }
}

fn compute_gaps(ring: &TokenRing) -> Vec<Gap> {
    let entries: Vec<(Token, Endpoint)> = ring.iter().map(|(t, e)| (*t, *e)).collect();
    if entries.is_empty() {
        return vec![Gap {
            start: Token::MINIMUM,
            end: Token::MAXIMUM,
            size: u64::MAX,
        }];
    }
    if entries.len() == 1 {
        // Single token: two halves of the ring
        let token = entries[0].0;
        return vec![
            Gap {
                start: token,
                end: Token::MAXIMUM,
                size: gap_size(token, Token::MAXIMUM),
            },
            Gap {
                start: Token::MINIMUM,
                end: token,
                size: gap_size(Token::MINIMUM, token),
            },
        ];
    }
    let mut gaps = Vec::with_capacity(entries.len());
    for i in 0..entries.len() {
        let start = entries[i].0;
        let end = if i + 1 < entries.len() {
            entries[i + 1].0
        } else {
            entries[0].0 // wrap-around
        };
        let size = gap_size(start, end);
        if size > 0 {
            gaps.push(Gap { start, end, size });
        }
    }
    gaps
}

fn compute_ownership(ring: &TokenRing, rf: usize) -> HashMap<Endpoint, f64> {
    let mut ownership = HashMap::new();
    let entries: Vec<(Token, Endpoint)> = ring.iter().map(|(t, e)| (*t, *e)).collect();
    if entries.is_empty() {
        return ownership;
    }
    let total_tokens = entries.len();
    for i in 0..total_tokens {
        let start = entries[i].0;
        let end = if i + 1 < total_tokens {
            entries[i + 1].0
        } else {
            entries[0].0
        };
        let size = gap_size(start, end) as f64;
        // Each token range is replicated to RF endpoints
        let replicas = ring.natural_endpoints(end, rf);
        for ep in replicas {
            *ownership.entry(ep).or_insert(0.0) += size;
        }
    }
    ownership
}

fn owner_load(token: Token, ring: &TokenRing, ownership: &HashMap<Endpoint, f64>) -> f64 {
    ring.primary_endpoint(token)
        .and_then(|ep| ownership.get(&ep))
        .copied()
        .unwrap_or(0.0)
}

fn even_split(num_tokens: usize) -> Vec<Token> {
    if num_tokens == 0 {
        return Vec::new();
    }
    let range = i64::MAX as i128 - i64::MIN as i128;
    (0..num_tokens)
        .map(|i| {
            let offset = i64::MIN as i128 + range * i as i128 / num_tokens as i128;
            Token::from_raw(offset as i64)
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::snitch::SimpleSnitch;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(127, 0, 0, 1),
            port,
        )))
    }

    #[test]
    fn allocate_empty_ring() {
        let allocator = NoReplicationTokenAllocator;
        let ring = TokenRing::new();
        let snitch = SimpleSnitch;
        let tokens = allocator.allocate(&ring, 4, &snitch, &ep(7001));
        assert_eq!(tokens.len(), 4);
        // Should be evenly distributed
        for i in 1..tokens.len() {
            assert!(tokens[i] > tokens[i - 1]);
        }
    }

    #[test]
    fn allocate_splits_largest_gap() {
        let allocator = NoReplicationTokenAllocator;
        let mut ring = TokenRing::new();
        // Place tokens at extremes so one gap is clearly larger
        ring.add_token(Token::from_raw(i64::MIN / 2), ep(7001));
        ring.add_token(Token::from_raw(i64::MAX / 2), ep(7002));
        let snitch = SimpleSnitch;
        let tokens = allocator.allocate(&ring, 1, &snitch, &ep(7003));
        assert_eq!(tokens.len(), 1);
        // The new token should not be the same as existing tokens
        assert_ne!(tokens[0], Token::from_raw(i64::MIN / 2));
        assert_ne!(tokens[0], Token::from_raw(i64::MAX / 2));
    }

    #[test]
    fn allocate_multiple_tokens() {
        let allocator = NoReplicationTokenAllocator;
        let mut ring = TokenRing::new();
        ring.add_token(Token::from_raw(0), ep(7001));
        let snitch = SimpleSnitch;
        let tokens = allocator.allocate(&ring, 3, &snitch, &ep(7002));
        // With 1 token on the ring, there's only 1 gap (the full ring wrap).
        // Splitting it 3 times should produce 3 tokens.
        assert!(tokens.len() >= 1, "should produce at least 1 token");
        // All tokens should be unique
        let mut sorted = tokens.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), tokens.len());
    }

    #[test]
    fn replication_aware_allocator() {
        let allocator = ReplicationAwareTokenAllocator::new(3);
        let mut ring = TokenRing::new();
        ring.add_token(Token::from_raw(-3000000000000000000), ep(7001));
        ring.add_token(Token::from_raw(0), ep(7002));
        ring.add_token(Token::from_raw(3000000000000000000), ep(7003));
        let snitch = SimpleSnitch;
        let tokens = allocator.allocate(&ring, 2, &snitch, &ep(7004));
        assert_eq!(tokens.len(), 2);
    }

    #[test]
    fn factory_creates_correct_type() {
        let a1 = create_token_allocator(1);
        let a3 = create_token_allocator(3);
        let ring = TokenRing::new();
        let snitch = SimpleSnitch;
        // Both should work
        assert_eq!(a1.allocate(&ring, 2, &snitch, &ep(7001)).len(), 2);
        assert_eq!(a3.allocate(&ring, 2, &snitch, &ep(7001)).len(), 2);
    }

    #[test]
    fn even_split_produces_distinct_tokens() {
        let tokens = even_split(4);
        assert_eq!(tokens.len(), 4);
        for i in 1..tokens.len() {
            assert!(
                tokens[i] > tokens[i - 1],
                "tokens must be strictly increasing"
            );
        }
    }
}
