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

//! Counter context: distributed counter CRDT implementation.
//!
//! ## Java Oracle
//!
//! `org.apache.cassandra.db.context.CounterContext`
//!
//! ## Design
//!
//! A counter value is stored as a **shard vector**: an ordered list of
//! `(node_id, clock, count)` tuples.  Each node only ever increments its
//! own shard.  The "current value" is the sum of all shard counts.
//!
//! **Merge** (CRDT): for each node_id, pick the shard with the highest
//! clock.  This is a state-based CRDT (GCounter generalization), guaranteeing
//! convergence under any merge order.
//!
//! ## Binary Format
//!
//! Compatible with Java's `CounterContext`:
//! - Header: 2 bytes (header length, currently unused flags)
//! - Each shard: 32 bytes
//!   - 16 bytes: node_id (UUID, big-endian)
//!   -  8 bytes: clock (i64, big-endian)
//!   -  8 bytes: count (i64, big-endian)
//!
//! Shards are sorted by node_id for deterministic binary representation.

use std::collections::BTreeMap;
use std::fmt;

use byteorder::{BigEndian, ReadBytesExt, WriteBytesExt};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

/// The size of one shard entry in the binary format.
const SHARD_SIZE: usize = 32; // 16 (uuid) + 8 (clock) + 8 (count)

/// Header size in the binary format.
const HEADER_SIZE: usize = 2;

/// A single counter shard — one node's contribution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct CounterShard {
    /// The node that owns this shard.
    pub node_id: Uuid,
    /// Logical clock — incremented on each update by this node.
    pub clock: i64,
    /// Accumulated count for this node.
    pub count: i64,
}

/// A counter context — the full distributed counter state.
///
/// Internally stored as a BTreeMap keyed by node_id for O(log n) lookup
/// and deterministic iteration order.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct CounterContext {
    shards: BTreeMap<Uuid, CounterShard>,
}

impl CounterContext {
    /// Create an empty counter context.
    pub fn new() -> Self {
        Self {
            shards: BTreeMap::new(),
        }
    }

    /// Create a context from an existing set of shards.
    pub fn from_shards(shards: Vec<CounterShard>) -> Self {
        let mut ctx = Self::new();
        for shard in shards {
            ctx.shards.insert(shard.node_id, shard);
        }
        ctx
    }

    /// Get the total counter value (sum of all shard counts).
    pub fn total(&self) -> i64 {
        self.shards.values().map(|s| s.count).sum()
    }

    /// Apply a local increment/decrement.
    ///
    /// `node_id` is the current node. `delta` is the increment (positive)
    /// or decrement (negative) value.
    pub fn apply_local(&mut self, node_id: Uuid, delta: i64) {
        let shard = self.shards.entry(node_id).or_insert(CounterShard {
            node_id,
            clock: 0,
            count: 0,
        });
        shard.clock += 1;
        shard.count += delta;
    }

    /// Merge another context into this one (CRDT merge).
    ///
    /// For each node_id present in either context, we keep the shard with
    /// the higher clock value.  If clocks are equal, we take the shard with
    /// the higher count to ensure commutativity (the choice must be
    /// deterministic regardless of merge order).
    pub fn merge(&mut self, other: &CounterContext) {
        for (node_id, other_shard) in &other.shards {
            match self.shards.get(node_id) {
                Some(local_shard) => {
                    if other_shard.clock > local_shard.clock {
                        self.shards.insert(*node_id, *other_shard);
                    } else if other_shard.clock == local_shard.clock
                        && other_shard.count > local_shard.count
                    {
                        // Equal clocks, take higher count for commutativity
                        self.shards.insert(*node_id, *other_shard);
                    }
                    // If local clock is higher, keep local — no-op.
                }
                None => {
                    self.shards.insert(*node_id, *other_shard);
                }
            }
        }
    }

    /// Get all shards.
    pub fn shards(&self) -> Vec<&CounterShard> {
        self.shards.values().collect()
    }

    /// Number of contributing nodes.
    pub fn shard_count(&self) -> usize {
        self.shards.len()
    }

    /// Check if the context has shards from nodes other than `local_node`.
    ///
    /// After anti-entropy / repair, a node may hold shards originally
    /// contributed by other nodes.  These can be cleaned up once the
    /// authoritative shard is known to be durable on the owning node.
    pub fn has_remote_shards(&self, local_node: Uuid) -> bool {
        self.shards.keys().any(|id| *id != local_node)
    }

    /// Check if the context has a shard for the given node.
    pub fn has_local_shard(&self, node_id: Uuid) -> bool {
        self.shards.contains_key(&node_id)
    }

    /// Clean up remote shards by folding all non-local shard counts into
    /// the local shard.  This is safe only after repair has confirmed all
    /// remote shards are durable on their respective nodes.
    ///
    /// ## Java Oracle
    ///
    /// `org.apache.cassandra.db.context.CounterContext.clearAllRemote()`
    ///
    /// ## Invariant
    ///
    /// `total()` before cleanup == `total()` after cleanup.
    /// The cleanup only reduces the number of shards for storage efficiency.
    pub fn cleanup(&mut self, local_node: Uuid) {
        let remote_sum: i64 = self.shards.iter()
            .filter(|(id, _)| **id != local_node)
            .map(|(_, s)| s.count)
            .sum();

        let remote_max_clock: i64 = self.shards.iter()
            .filter(|(id, _)| **id != local_node)
            .map(|(_, s)| s.clock)
            .max()
            .unwrap_or(0);

        // Remove all remote shards
        self.shards.retain(|id, _| *id == local_node);

        // Fold their count into local shard
        if remote_sum != 0 || !self.shards.contains_key(&local_node) {
            let local_shard = self.shards.entry(local_node).or_insert(CounterShard {
                node_id: local_node,
                clock: 0,
                count: 0,
            });
            local_shard.count += remote_sum;
            // Advance clock to indicate this shard has absorbed remote data
            local_shard.clock = local_shard.clock.max(remote_max_clock) + 1;
        }
    }

    /// Serialize to the binary counter context format (Java-compatible).
    pub fn serialize(&self) -> Vec<u8> {
        let total_size = HEADER_SIZE + self.shards.len() * SHARD_SIZE;
        let mut buf = Vec::with_capacity(total_size);

        // Header: 2 bytes (version/flags, currently 0)
        buf.write_u16::<BigEndian>(0).unwrap();

        // Shards sorted by node_id (BTreeMap guarantees this)
        for shard in self.shards.values() {
            // UUID: 16 bytes (big-endian MSB/LSB)
            let (msb, lsb) = shard.node_id.as_u64_pair();
            buf.write_u64::<BigEndian>(msb).unwrap();
            buf.write_u64::<BigEndian>(lsb).unwrap();
            // Clock: 8 bytes
            buf.write_i64::<BigEndian>(shard.clock).unwrap();
            // Count: 8 bytes
            buf.write_i64::<BigEndian>(shard.count).unwrap();
        }

        buf
    }

    /// Deserialize from the binary counter context format.
    pub fn deserialize(data: &[u8]) -> Result<Self, CounterError> {
        if data.len() < HEADER_SIZE {
            return Err(CounterError::InvalidFormat("data too short for header".into()));
        }

        let mut cursor = std::io::Cursor::new(data);
        let _header = cursor.read_u16::<BigEndian>()
            .map_err(|e| CounterError::InvalidFormat(e.to_string()))?;

        let remaining = data.len() - HEADER_SIZE;
        if remaining % SHARD_SIZE != 0 {
            return Err(CounterError::InvalidFormat(
                format!("remaining bytes {} not divisible by shard size {}", remaining, SHARD_SIZE),
            ));
        }

        let shard_count = remaining / SHARD_SIZE;
        let mut shards = BTreeMap::new();

        for _ in 0..shard_count {
            let msb = cursor.read_u64::<BigEndian>()
                .map_err(|e| CounterError::InvalidFormat(e.to_string()))?;
            let lsb = cursor.read_u64::<BigEndian>()
                .map_err(|e| CounterError::InvalidFormat(e.to_string()))?;
            let node_id = Uuid::from_u64_pair(msb, lsb);
            let clock = cursor.read_i64::<BigEndian>()
                .map_err(|e| CounterError::InvalidFormat(e.to_string()))?;
            let count = cursor.read_i64::<BigEndian>()
                .map_err(|e| CounterError::InvalidFormat(e.to_string()))?;

            shards.insert(node_id, CounterShard { node_id, clock, count });
        }

        Ok(Self { shards })
    }
}

impl Default for CounterContext {
    fn default() -> Self {
        Self::new()
    }
}

impl fmt::Display for CounterContext {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "Counter(total={}, shards={})", self.total(), self.shard_count())
    }
}

/// Counter operation errors.
#[derive(Debug, thiserror::Error)]
pub enum CounterError {
    #[error("Invalid counter context format: {0}")]
    InvalidFormat(String),

    #[error("Counter mutation error: {0}")]
    MutationError(String),
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node_a() -> Uuid {
        Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap()
    }

    fn node_b() -> Uuid {
        Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap()
    }

    fn node_c() -> Uuid {
        Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap()
    }

    #[test]
    fn empty_counter_is_zero() {
        let ctx = CounterContext::new();
        assert_eq!(ctx.total(), 0);
        assert_eq!(ctx.shard_count(), 0);
    }

    #[test]
    fn local_increment() {
        let mut ctx = CounterContext::new();
        ctx.apply_local(node_a(), 5);
        assert_eq!(ctx.total(), 5);
        assert_eq!(ctx.shard_count(), 1);

        ctx.apply_local(node_a(), 3);
        assert_eq!(ctx.total(), 8);
        assert_eq!(ctx.shard_count(), 1);
    }

    #[test]
    fn local_decrement() {
        let mut ctx = CounterContext::new();
        ctx.apply_local(node_a(), 10);
        ctx.apply_local(node_a(), -3);
        assert_eq!(ctx.total(), 7);
    }

    #[test]
    fn multi_node_total() {
        let mut ctx = CounterContext::new();
        ctx.apply_local(node_a(), 10);
        ctx.apply_local(node_b(), 20);
        ctx.apply_local(node_c(), 30);
        assert_eq!(ctx.total(), 60);
        assert_eq!(ctx.shard_count(), 3);
    }

    #[test]
    fn merge_disjoint() {
        let mut ctx1 = CounterContext::new();
        ctx1.apply_local(node_a(), 10);

        let mut ctx2 = CounterContext::new();
        ctx2.apply_local(node_b(), 20);

        ctx1.merge(&ctx2);
        assert_eq!(ctx1.total(), 30);
        assert_eq!(ctx1.shard_count(), 2);
    }

    #[test]
    fn merge_overlapping_higher_clock_wins() {
        let mut ctx1 = CounterContext::new();
        ctx1.apply_local(node_a(), 10);
        ctx1.apply_local(node_a(), 5); // clock=2, count=15

        let mut ctx2 = CounterContext::new();
        ctx2.apply_local(node_a(), 10); // clock=1, count=10

        ctx1.merge(&ctx2); // ctx1 has clock=2, keeps its shard
        assert_eq!(ctx1.total(), 15);

        // Reverse merge — ctx2 should adopt ctx1's shard
        ctx2.merge(&ctx1);
        assert_eq!(ctx2.total(), 15);
    }

    #[test]
    fn merge_is_commutative() {
        let mut ctx1 = CounterContext::new();
        ctx1.apply_local(node_a(), 10);

        let mut ctx2 = CounterContext::new();
        ctx2.apply_local(node_b(), 20);

        let mut m1 = ctx1.clone();
        m1.merge(&ctx2);

        let mut m2 = ctx2.clone();
        m2.merge(&ctx1);

        assert_eq!(m1.total(), m2.total());
    }

    #[test]
    fn merge_is_idempotent() {
        let mut ctx1 = CounterContext::new();
        ctx1.apply_local(node_a(), 10);

        let mut ctx2 = ctx1.clone();
        ctx2.merge(&ctx1);
        assert_eq!(ctx2.total(), 10); // Not 20 — idempotent
    }

    #[test]
    fn merge_is_associative() {
        let mut a = CounterContext::new();
        a.apply_local(node_a(), 10);
        let mut b = CounterContext::new();
        b.apply_local(node_b(), 20);
        let mut c = CounterContext::new();
        c.apply_local(node_c(), 30);

        // (a merge b) merge c
        let mut ab = a.clone();
        ab.merge(&b);
        let mut abc1 = ab;
        abc1.merge(&c);

        // a merge (b merge c)
        let mut bc = b.clone();
        bc.merge(&c);
        let mut abc2 = a.clone();
        abc2.merge(&bc);

        assert_eq!(abc1.total(), abc2.total());
        assert_eq!(abc1.total(), 60);
    }

    #[test]
    fn serialize_deserialize_round_trip() {
        let mut ctx = CounterContext::new();
        ctx.apply_local(node_a(), 42);
        ctx.apply_local(node_b(), -7);

        let bytes = ctx.serialize();
        let decoded = CounterContext::deserialize(&bytes).unwrap();

        assert_eq!(decoded.total(), ctx.total());
        assert_eq!(decoded.shard_count(), ctx.shard_count());
        assert_eq!(decoded, ctx);
    }

    #[test]
    fn serialize_empty() {
        let ctx = CounterContext::new();
        let bytes = ctx.serialize();
        assert_eq!(bytes.len(), HEADER_SIZE);

        let decoded = CounterContext::deserialize(&bytes).unwrap();
        assert_eq!(decoded.total(), 0);
    }

    #[test]
    fn deserialize_invalid_length() {
        let data = vec![0u8, 0, 1, 2, 3]; // header + 3 bytes (not a full shard)
        let result = CounterContext::deserialize(&data);
        assert!(result.is_err());
    }

    #[test]
    fn from_shards() {
        let shards = vec![
            CounterShard { node_id: node_a(), clock: 1, count: 10 },
            CounterShard { node_id: node_b(), clock: 2, count: 20 },
        ];
        let ctx = CounterContext::from_shards(shards);
        assert_eq!(ctx.total(), 30);
    }

    #[test]
    fn display() {
        let mut ctx = CounterContext::new();
        ctx.apply_local(node_a(), 42);
        let s = format!("{}", ctx);
        assert!(s.contains("42"));
        assert!(s.contains("1"));
    }

    // ─── Cleanup and remote shard tests ───────────────────────────────────

    #[test]
    fn has_remote_shards() {
        let mut ctx = CounterContext::new();
        ctx.apply_local(node_a(), 10);
        assert!(!ctx.has_remote_shards(node_a()));
        assert!(ctx.has_remote_shards(node_b())); // a is remote from b's perspective

        ctx.apply_local(node_b(), 20);
        assert!(ctx.has_remote_shards(node_a())); // b is remote from a's perspective
    }

    #[test]
    fn cleanup_preserves_total() {
        let mut ctx = CounterContext::new();
        ctx.apply_local(node_a(), 10);
        ctx.apply_local(node_b(), 20);
        ctx.apply_local(node_c(), 30);

        let total_before = ctx.total();
        ctx.cleanup(node_a());

        assert_eq!(ctx.total(), total_before);
        assert_eq!(ctx.shard_count(), 1);
        assert!(ctx.has_local_shard(node_a()));
        assert!(!ctx.has_remote_shards(node_a()));
    }

    #[test]
    fn cleanup_with_negative_remote_shards() {
        let mut ctx = CounterContext::new();
        ctx.apply_local(node_a(), 100);
        ctx.apply_local(node_b(), -30);

        let total_before = ctx.total(); // 70
        ctx.cleanup(node_a());

        assert_eq!(ctx.total(), total_before);
        assert_eq!(ctx.shard_count(), 1);
    }

    #[test]
    fn cleanup_noop_when_only_local() {
        let mut ctx = CounterContext::new();
        ctx.apply_local(node_a(), 42);

        ctx.cleanup(node_a());
        assert_eq!(ctx.total(), 42);
        assert_eq!(ctx.shard_count(), 1);
    }

    #[test]
    fn cleanup_empty_context() {
        let mut ctx = CounterContext::new();
        ctx.cleanup(node_a()); // Should not panic
        assert_eq!(ctx.total(), 0);
    }

    #[test]
    fn overflow_boundary() {
        let mut ctx = CounterContext::new();
        ctx.apply_local(node_a(), i64::MAX);
        assert_eq!(ctx.total(), i64::MAX);

        // Applying 1 more should wrap (Rust defaults to panic in debug,
        // but Cassandra counters use wrapping semantics in practice).
        // We test that the counter holds large values correctly.
        let bytes = ctx.serialize();
        let decoded = CounterContext::deserialize(&bytes).unwrap();
        assert_eq!(decoded.total(), i64::MAX);
    }

    #[test]
    fn negative_counter_total() {
        let mut ctx = CounterContext::new();
        ctx.apply_local(node_a(), -100);
        assert_eq!(ctx.total(), -100);
    }
}

#[cfg(test)]
mod proptests {
    use super::*;
    use proptest::prelude::*;

    fn arb_uuid() -> impl Strategy<Value = Uuid> {
        prop_oneof![
            Just(Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap()),
            Just(Uuid::parse_str("00000000-0000-0000-0000-000000000002").unwrap()),
            Just(Uuid::parse_str("00000000-0000-0000-0000-000000000003").unwrap()),
        ]
    }

    fn arb_counter() -> impl Strategy<Value = CounterContext> {
        prop::collection::vec((arb_uuid(), -1000i64..1000i64), 0..5)
            .prop_map(|ops| {
                let mut ctx = CounterContext::new();
                for (node, delta) in ops {
                    ctx.apply_local(node, delta);
                }
                ctx
            })
    }

    proptest! {
        #[test]
        fn merge_is_commutative_prop(a in arb_counter(), b in arb_counter()) {
            let mut ab = a.clone();
            ab.merge(&b);
            let mut ba = b.clone();
            ba.merge(&a);
            prop_assert_eq!(ab.total(), ba.total());
        }

        #[test]
        fn merge_is_idempotent_prop(a in arb_counter()) {
            let mut aa = a.clone();
            aa.merge(&a);
            prop_assert_eq!(aa.total(), a.total());
        }

        #[test]
        fn merge_is_associative_prop(a in arb_counter(), b in arb_counter(), c in arb_counter()) {
            let mut ab = a.clone();
            ab.merge(&b);
            let mut abc1 = ab;
            abc1.merge(&c);

            let mut bc = b.clone();
            bc.merge(&c);
            let mut abc2 = a.clone();
            abc2.merge(&bc);

            prop_assert_eq!(abc1.total(), abc2.total());
        }

        #[test]
        fn serialize_round_trip_prop(ctx in arb_counter()) {
            let bytes = ctx.serialize();
            let decoded = CounterContext::deserialize(&bytes).unwrap();
            prop_assert_eq!(decoded.total(), ctx.total());
            prop_assert_eq!(decoded.shard_count(), ctx.shard_count());
        }

        #[test]
        fn cleanup_preserves_total_prop(ctx in arb_counter()) {
            let total = ctx.total();
            let mut cleaned = ctx.clone();
            let local_node = Uuid::parse_str("00000000-0000-0000-0000-000000000001").unwrap();
            cleaned.cleanup(local_node);
            prop_assert_eq!(cleaned.total(), total);
        }
    }
}

