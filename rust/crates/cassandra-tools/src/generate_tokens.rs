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

//! Generate tokens for Murmur3Partitioner with rack-aware distribution.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.tools.GenerateTokens`
//! - `org.apache.cassandra.dht.Murmur3Partitioner`

/// Compute the token list for a single node.
///
/// Returns a sorted vector of `i64` tokens for the given `node` index
/// (0-based) when distributing `total_tokens` evenly across the Murmur3
/// signed 64-bit range.
pub fn compute_node_tokens(node: u32, tokens_per_node: u32, total_tokens: u32) -> Vec<i64> {
    // Murmur3 range: i64::MIN to i64::MAX — 2^64 total values (unsigned view)
    let range = u128::from(u64::MAX) + 1;

    let mut node_tokens: Vec<i64> = Vec::with_capacity(tokens_per_node as usize);
    for t in 0..tokens_per_node {
        let index = (node * tokens_per_node + t) as u128;
        let token_val = ((index * range) / total_tokens as u128) as i64;
        // Shift to signed range
        let token = token_val.wrapping_add(i64::MIN);
        node_tokens.push(token);
    }

    node_tokens.sort();
    node_tokens
}

/// Generate and print tokens for a cluster with rack-aware distribution.
///
/// Divides the Murmur3Partitioner token range evenly across
/// `nodes * tokens` total vnodes, assigning each node to a rack
/// in round-robin fashion.
pub fn run(nodes: u32, tokens: u32, racks: u32) {
    println!(
        "Generating tokens for {} nodes with {} vnodes per node ({} racks)",
        nodes, tokens, racks
    );
    println!();

    if nodes == 0 || tokens == 0 {
        println!("Error: nodes and tokens must be > 0");
        return;
    }

    let total_tokens = nodes * tokens;

    for node in 0..nodes {
        let rack = node % racks.max(1);
        println!("Node {} (rack {}):", node + 1, rack + 1);

        let node_tokens = compute_node_tokens(node, tokens, total_tokens);
        let tokens_str: Vec<String> = node_tokens.iter().map(|t| t.to_string()).collect();
        println!("  initial_token: {}", tokens_str.join(","));
        println!();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashSet;

    #[test]
    fn test_single_node_single_token() {
        let tokens = compute_node_tokens(0, 1, 1);
        assert_eq!(tokens.len(), 1);
        // With 1 total token, index=0 => token_val=0, shifted => i64::MIN
        assert_eq!(tokens[0], i64::MIN);
    }

    #[test]
    fn test_three_nodes_256_tokens_unique() {
        let nodes = 3u32;
        let tokens_per_node = 256u32;
        let total = nodes * tokens_per_node;

        let mut all_tokens = HashSet::new();
        for node in 0..nodes {
            let node_tokens = compute_node_tokens(node, tokens_per_node, total);
            assert_eq!(node_tokens.len(), 256);
            for t in &node_tokens {
                all_tokens.insert(*t);
            }
        }

        // All 768 tokens should be unique
        assert_eq!(all_tokens.len(), (nodes * tokens_per_node) as usize);
    }

    #[test]
    fn test_tokens_are_sorted() {
        let tokens = compute_node_tokens(1, 16, 48);
        for window in tokens.windows(2) {
            assert!(window[0] <= window[1], "tokens must be sorted");
        }
    }

    #[test]
    fn test_racks_greater_than_nodes() {
        // Should not panic — racks > nodes is valid (some racks are empty).
        run(2, 4, 5);
    }

    #[test]
    fn test_zero_nodes_prints_error() {
        // Should not panic, just prints an error.
        run(0, 256, 1);
    }

    #[test]
    fn test_zero_tokens_prints_error() {
        // Should not panic, just prints an error.
        run(3, 0, 1);
    }

    #[test]
    fn test_two_nodes_tokens_cover_range() {
        let nodes = 2u32;
        let tpn = 1u32;
        let total = nodes * tpn;

        let t0 = compute_node_tokens(0, tpn, total);
        let t1 = compute_node_tokens(1, tpn, total);

        // Two tokens should split the range roughly in half
        assert_eq!(t0.len(), 1);
        assert_eq!(t1.len(), 1);
        assert_ne!(t0[0], t1[0]);
    }

    #[test]
    fn test_run_function_does_not_panic() {
        // Smoke test: valid parameters should produce output without panicking.
        run(3, 256, 3);
    }
}
