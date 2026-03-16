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

//! Token ring: maps tokens to endpoints for partition routing.
//!
//! The ring is a sorted list of `(Token, Endpoint)` pairs. To find the
//! owning node for a given token, binary-search for the first entry whose
//! token is >= the query token, wrapping around if needed.
//!
//! ## Java Oracle
//!
//! - `org.apache.cassandra.locator.TokenMetadata`
//! - `org.apache.cassandra.dht.Range`

use std::collections::{BTreeMap, HashMap, HashSet};

use cassandra_common::Token;

use crate::node::Endpoint;

/// The token ring: maps tokens to the endpoints that own them.
///
/// Invariant: `entries` is always sorted by token.
/// Each node may own multiple tokens (vnodes).
///
/// ## Pending Ranges
///
/// During topology changes (bootstrap, decommission, move), some token
/// ranges may have *pending* endpoints that will own them after the
/// operation completes. The coordinator must send writes to both
/// current and pending owners to maintain consistency.
///
/// ## Java Oracle
///
/// - `org.apache.cassandra.locator.TokenMetadata`
/// - `org.apache.cassandra.locator.TokenMetadata.pendingEndpointsFor()`
#[derive(Debug, Clone)]
pub struct TokenRing {
    /// Sorted map of token → owning endpoint.
    entries: BTreeMap<Token, Endpoint>,
    /// Reverse map: endpoint → set of owned tokens.
    endpoint_tokens: HashMap<Endpoint, HashSet<Token>>,
    /// Pending range assignments: token → endpoints that will own
    /// this token after the in-progress topology operation completes.
    pending_ranges: BTreeMap<Token, Vec<Endpoint>>,
}

impl TokenRing {
    /// Create an empty ring.
    pub fn new() -> Self {
        Self {
            entries: BTreeMap::new(),
            endpoint_tokens: HashMap::new(),
            pending_ranges: BTreeMap::new(),
        }
    }

    /// Number of distinct tokens on the ring.
    pub fn token_count(&self) -> usize {
        self.entries.len()
    }

    /// Number of distinct endpoints on the ring.
    pub fn endpoint_count(&self) -> usize {
        self.endpoint_tokens.len()
    }

    /// Returns `true` if the ring has no tokens.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Add a token→endpoint mapping. If the token already exists, it is
    /// reassigned to the new endpoint.
    pub fn add_token(&mut self, token: Token, endpoint: Endpoint) {
        // Remove old ownership if token was previously assigned
        if let Some(old_ep) = self.entries.insert(token, endpoint) {
            if old_ep != endpoint {
                if let Some(tokens) = self.endpoint_tokens.get_mut(&old_ep) {
                    tokens.remove(&token);
                    if tokens.is_empty() {
                        self.endpoint_tokens.remove(&old_ep);
                    }
                }
            }
        }
        self.endpoint_tokens
            .entry(endpoint)
            .or_default()
            .insert(token);
    }

    /// Add all tokens for an endpoint at once.
    pub fn add_node(&mut self, endpoint: Endpoint, tokens: &[Token]) {
        for &token in tokens {
            self.add_token(token, endpoint);
        }
    }

    /// Remove all tokens owned by the given endpoint.
    pub fn remove_node(&mut self, endpoint: &Endpoint) {
        if let Some(tokens) = self.endpoint_tokens.remove(endpoint) {
            for token in tokens {
                self.entries.remove(&token);
            }
        }
    }

    /// Find the primary endpoint for a given token.
    ///
    /// Returns the endpoint that owns the first token >= the query token,
    /// wrapping around the ring if needed. This matches Java's
    /// `TokenMetadata.firstTokenUpFrom()`.
    pub fn primary_endpoint(&self, token: Token) -> Option<Endpoint> {
        if self.entries.is_empty() {
            return None;
        }

        // Find first entry with token >= query
        if let Some((_t, ep)) = self.entries.range(token..).next() {
            return Some(*ep);
        }

        // Wrap around: return the first entry on the ring
        self.entries.values().next().copied()
    }

    /// Get natural endpoints for a token — walk the ring clockwise and
    /// collect up to `count` distinct endpoints.
    ///
    /// This is the core primitive used by replication strategies.
    pub fn natural_endpoints(&self, token: Token, count: usize) -> Vec<Endpoint> {
        if self.entries.is_empty() || count == 0 {
            return Vec::new();
        }

        let mut result = Vec::with_capacity(count);
        let mut seen = HashSet::new();

        // Walk clockwise from the token
        for (_t, ep) in self.entries.range(token..) {
            if seen.insert(*ep) {
                result.push(*ep);
                if result.len() >= count {
                    return result;
                }
            }
        }

        // Wrap around from the beginning
        for (_t, ep) in self.entries.iter() {
            if seen.insert(*ep) {
                result.push(*ep);
                if result.len() >= count {
                    return result;
                }
            }
        }

        result
    }

    /// Get all endpoints on the ring.
    pub fn all_endpoints(&self) -> Vec<Endpoint> {
        self.endpoint_tokens.keys().copied().collect()
    }

    /// Get all tokens for a given endpoint.
    pub fn tokens_for(&self, endpoint: &Endpoint) -> Vec<Token> {
        self.endpoint_tokens
            .get(endpoint)
            .map(|s| {
                let mut tokens: Vec<Token> = s.iter().copied().collect();
                tokens.sort();
                tokens
            })
            .unwrap_or_default()
    }

    /// Iterate over all (token, endpoint) pairs in ring order.
    pub fn iter(&self) -> impl Iterator<Item = (&Token, &Endpoint)> {
        self.entries.iter()
    }

    /// Find the token immediately before the given token on the ring.
    ///
    /// Returns the predecessor token by walking counter-clockwise.
    /// If the query token is the first entry, wraps to the last token.
    pub fn previous_token(&self, token: Token) -> Option<Token> {
        if self.entries.is_empty() {
            return None;
        }
        // Find the entry just before `token`
        if let Some((&prev_token, _)) = self.entries.range(..token).next_back() {
            Some(prev_token)
        } else {
            // Wrap: return the last token on the ring
            self.entries.keys().next_back().copied()
        }
    }

    // ── Pending Ranges ──────────────────────────────────────────────────

    /// Add a pending range: during a topology operation, `endpoint` will
    /// take ownership of `token` after the operation completes.
    ///
    /// ## Java Oracle
    ///
    /// `TokenMetadata.addPendingRange()`
    pub fn add_pending_range(&mut self, token: Token, endpoint: Endpoint) {
        let entry = self.pending_ranges.entry(token).or_default();
        if !entry.contains(&endpoint) {
            entry.push(endpoint);
        }
    }

    /// Remove all pending ranges for a specific endpoint (operation completed or aborted).
    pub fn remove_pending_ranges_for(&mut self, endpoint: &Endpoint) {
        self.pending_ranges.retain(|_, eps| {
            eps.retain(|e| e != endpoint);
            !eps.is_empty()
        });
    }

    /// Clear all pending ranges (e.g. after topology operation completes).
    pub fn clear_pending_ranges(&mut self) {
        self.pending_ranges.clear();
    }

    /// Get pending endpoints for a specific token.
    ///
    /// Returns endpoints that will own this token after the in-progress
    /// topology operation completes.
    pub fn pending_endpoints_for(&self, token: Token) -> Vec<Endpoint> {
        // Walk pending ranges the same way we walk natural endpoints:
        // find first pending token >= query token, else wrap.
        if self.pending_ranges.is_empty() {
            return Vec::new();
        }

        if let Some((_, eps)) = self.pending_ranges.range(token..).next() {
            return eps.clone();
        }
        // Wrap around
        self.pending_ranges.values().next().cloned().unwrap_or_default()
    }

    /// Whether there are any pending ranges.
    pub fn has_pending_ranges(&self) -> bool {
        !self.pending_ranges.is_empty()
    }

    /// Get all pending ranges as a snapshot.
    pub fn all_pending_ranges(&self) -> &BTreeMap<Token, Vec<Endpoint>> {
        &self.pending_ranges
    }

    /// Write replicas: natural endpoints PLUS pending endpoints for a token.
    ///
    /// During topology changes, writes must go to both the current owners
    /// and the pending owners to ensure data availability after the change.
    ///
    /// ## Java Oracle
    ///
    /// `StorageProxy.getLiveSortedEndpoints()` combined with
    /// `TokenMetadata.pendingEndpointsFor()`
    pub fn write_replicas(&self, token: Token, natural_count: usize) -> Vec<Endpoint> {
        let mut replicas = self.natural_endpoints(token, natural_count);
        let pending = self.pending_endpoints_for(token);
        for ep in pending {
            if !replicas.contains(&ep) {
                replicas.push(ep);
            }
        }
        replicas
    }
}

impl Default for TokenRing {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, SocketAddr, SocketAddrV4};

    fn ep(port: u16) -> Endpoint {
        Endpoint::new(SocketAddr::V4(SocketAddrV4::new(
            Ipv4Addr::new(127, 0, 0, 1),
            port,
        )))
    }

    #[test]
    fn empty_ring() {
        let ring = TokenRing::new();
        assert!(ring.is_empty());
        assert_eq!(ring.primary_endpoint(Token::from_raw(0)), None);
        assert!(ring.natural_endpoints(Token::from_raw(0), 3).is_empty());
    }

    #[test]
    fn single_node() {
        let mut ring = TokenRing::new();
        ring.add_token(Token::from_raw(0), ep(7001));

        assert_eq!(ring.token_count(), 1);
        assert_eq!(ring.endpoint_count(), 1);
        assert_eq!(
            ring.primary_endpoint(Token::from_raw(0)),
            Some(ep(7001))
        );
        // Any token should resolve to the single node
        assert_eq!(
            ring.primary_endpoint(Token::from_raw(100)),
            Some(ep(7001))
        );
    }

    #[test]
    fn three_node_ring() {
        let mut ring = TokenRing::new();
        ring.add_token(Token::from_raw(-100), ep(7001));
        ring.add_token(Token::from_raw(0), ep(7002));
        ring.add_token(Token::from_raw(100), ep(7003));

        // Token -50 → first endpoint >= -50 → ep(7002) at token 0
        assert_eq!(
            ring.primary_endpoint(Token::from_raw(-50)),
            Some(ep(7002))
        );

        // Token 50 → first endpoint >= 50 → ep(7003) at token 100
        assert_eq!(
            ring.primary_endpoint(Token::from_raw(50)),
            Some(ep(7003))
        );

        // Token 200 → wraps around → ep(7001) at token -100
        assert_eq!(
            ring.primary_endpoint(Token::from_raw(200)),
            Some(ep(7001))
        );
    }

    #[test]
    fn natural_endpoints_rf3() {
        let mut ring = TokenRing::new();
        ring.add_token(Token::from_raw(-100), ep(7001));
        ring.add_token(Token::from_raw(0), ep(7002));
        ring.add_token(Token::from_raw(100), ep(7003));

        let replicas = ring.natural_endpoints(Token::from_raw(-50), 3);
        assert_eq!(replicas.len(), 3);
        // Should walk clockwise: 7002 (token 0), 7003 (token 100), 7001 (wrap)
        assert_eq!(replicas[0], ep(7002));
        assert_eq!(replicas[1], ep(7003));
        assert_eq!(replicas[2], ep(7001));
    }

    #[test]
    fn natural_endpoints_rf_exceeds_nodes() {
        let mut ring = TokenRing::new();
        ring.add_token(Token::from_raw(0), ep(7001));
        ring.add_token(Token::from_raw(100), ep(7002));

        // Asking for RF=5 but only 2 nodes
        let replicas = ring.natural_endpoints(Token::from_raw(-10), 5);
        assert_eq!(replicas.len(), 2);
    }

    #[test]
    fn add_node_multiple_tokens() {
        let mut ring = TokenRing::new();
        ring.add_node(ep(7001), &[Token::from_raw(-50), Token::from_raw(50)]);
        ring.add_node(ep(7002), &[Token::from_raw(-25), Token::from_raw(75)]);

        assert_eq!(ring.token_count(), 4);
        assert_eq!(ring.endpoint_count(), 2);

        let tokens = ring.tokens_for(&ep(7001));
        assert_eq!(tokens.len(), 2);
    }

    #[test]
    fn remove_node() {
        let mut ring = TokenRing::new();
        ring.add_node(ep(7001), &[Token::from_raw(0)]);
        ring.add_node(ep(7002), &[Token::from_raw(100)]);

        ring.remove_node(&ep(7001));
        assert_eq!(ring.token_count(), 1);
        assert_eq!(ring.endpoint_count(), 1);
        assert_eq!(
            ring.primary_endpoint(Token::from_raw(0)),
            Some(ep(7002))
        );
    }

    #[test]
    fn vnodes_same_endpoint() {
        // A node with multiple vnodes should only appear once in natural_endpoints
        let mut ring = TokenRing::new();
        ring.add_node(
            ep(7001),
            &[Token::from_raw(-100), Token::from_raw(0), Token::from_raw(100)],
        );
        ring.add_node(ep(7002), &[Token::from_raw(50)]);

        let replicas = ring.natural_endpoints(Token::from_raw(-50), 3);
        // Should deduplicate: ep(7001) appears multiple times on ring but once in result
        assert_eq!(replicas.len(), 2); // only 2 distinct endpoints
    }

    // ── Pending Ranges Tests ────────────────────────────────────────────

    #[test]
    fn pending_ranges_empty_by_default() {
        let ring = TokenRing::new();
        assert!(!ring.has_pending_ranges());
        assert!(ring.pending_endpoints_for(Token::from_raw(0)).is_empty());
    }

    #[test]
    fn add_and_query_pending_ranges() {
        let mut ring = TokenRing::new();
        ring.add_token(Token::from_raw(-100), ep(7001));
        ring.add_token(Token::from_raw(0), ep(7002));
        ring.add_token(Token::from_raw(100), ep(7003));

        // Simulate bootstrap: ep(7004) is pending for token 50
        ring.add_pending_range(Token::from_raw(50), ep(7004));
        assert!(ring.has_pending_ranges());

        // Pending endpoints for token 50
        let pending = ring.pending_endpoints_for(Token::from_raw(50));
        assert!(pending.contains(&ep(7004)));
    }

    #[test]
    fn write_replicas_includes_pending() {
        let mut ring = TokenRing::new();
        ring.add_token(Token::from_raw(-100), ep(7001));
        ring.add_token(Token::from_raw(0), ep(7002));
        ring.add_token(Token::from_raw(100), ep(7003));

        // No pending: write replicas = natural only
        let wr = ring.write_replicas(Token::from_raw(-50), 2);
        assert_eq!(wr.len(), 2);

        // Add pending
        ring.add_pending_range(Token::from_raw(0), ep(7004));
        let wr = ring.write_replicas(Token::from_raw(-50), 2);
        assert!(wr.len() >= 2);
        assert!(wr.contains(&ep(7004)));
    }

    #[test]
    fn remove_pending_ranges_for_endpoint() {
        let mut ring = TokenRing::new();
        ring.add_pending_range(Token::from_raw(0), ep(7004));
        ring.add_pending_range(Token::from_raw(100), ep(7004));
        ring.add_pending_range(Token::from_raw(100), ep(7005));
        assert!(ring.has_pending_ranges());

        ring.remove_pending_ranges_for(&ep(7004));
        // Only ep(7005) pending at token 100 should remain
        let pending = ring.all_pending_ranges();
        assert_eq!(pending.len(), 1);
        assert!(pending.get(&Token::from_raw(100)).unwrap().contains(&ep(7005)));
    }

    #[test]
    fn clear_pending_ranges() {
        let mut ring = TokenRing::new();
        ring.add_pending_range(Token::from_raw(0), ep(7004));
        ring.add_pending_range(Token::from_raw(100), ep(7005));
        assert!(ring.has_pending_ranges());

        ring.clear_pending_ranges();
        assert!(!ring.has_pending_ranges());
    }

    #[test]
    fn pending_range_no_duplicate() {
        let mut ring = TokenRing::new();
        ring.add_pending_range(Token::from_raw(0), ep(7004));
        ring.add_pending_range(Token::from_raw(0), ep(7004)); // duplicate
        let pending = ring.all_pending_ranges();
        assert_eq!(pending.get(&Token::from_raw(0)).unwrap().len(), 1);
    }
}
