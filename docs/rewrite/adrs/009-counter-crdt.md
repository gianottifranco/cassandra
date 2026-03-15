# ADR-009: Counter CRDT Design

**Status**: Accepted
**Date**: 2026-03-15
**Context**: Cassandra Rust Rewrite Phase 3

## Decision

Implement distributed counters using a shard-vector CRDT, matching Java's
`org.apache.cassandra.db.context.CounterContext`.

### Data Model

- `CounterContext` = sorted vector of `CounterShard { node_id: Uuid, clock: i64, count: i64 }`.
- Each node only increments its own shard.
- Total counter value = sum of all shard counts.

### Merge Semantics (CRDT)

- For each `node_id`, keep the shard with the **highest clock**.
- This is a state-based CRDT (G-Counter generalization) guaranteeing:
  - **Commutativity**: `merge(A, B) = merge(B, A)`
  - **Associativity**: `merge(merge(A, B), C) = merge(A, merge(B, C))`
  - **Idempotency**: `merge(A, A) = A`

### Binary Format

Compatible with Java's `CounterContext`:
- 2-byte header (version/flags)
- Each shard: 32 bytes (16 UUID + 8 clock + 8 count), big-endian
- Shards sorted by `node_id` for deterministic representation

### Write Path

1. Coordinator receives `UPDATE ... SET counter_col = counter_col + N`
2. Read current counter context from replicas at CL
3. Create new shard `(local_node_id, clock+1, existing_count + N)`
4. Write merged context to replicas

## Consequences

- Counter merge is conflict-free by design.
- Counter values are eventually consistent.
- Counter deletes (tombstones) not yet implemented — TODO.
- Global shard cleanup (removing shards from decommissioned nodes) — TODO.

## Alternatives Considered

- **Simple i64 with LWW**: Loses increments under concurrent writes.
- **Operation-based CRDT**: Requires reliable delivery; state-based is simpler with Cassandra's gossip model.
