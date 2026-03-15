# ADR-001: Compatibility Criteria

- **Status**: Accepted
- **Date**: 2026-03-15
- **Context**: Cassandra Rust rewrite baseline freeze

## Context

We are rewriting Apache Cassandra from Java to Rust. The fundamental question
is: what does "compatible" mean? Without a rigorous definition, we risk either
over-engineering (trying to match every JVM behavior) or under-delivering
(breaking existing deployments).

## Decision

We adopt a **layered compatibility model** with four tiers:

### Tier 1 – Wire Protocol (MUST)
- CQL native protocol v4 and v5 frame-level byte compatibility.
- Identical responses for identical queries given identical data.
- Gossip protocol wire format compatibility for mixed-cluster operation.
- Inter-node messaging format compatibility.

### Tier 2 – Data Format (MUST)
- Read-compatible with existing SSTable formats (mc, nb, big).
- Write format may evolve but must be readable by Java nodes in mixed clusters.
- CommitLog segment format compatibility (at least read).
- Hints file format compatibility (at least read).

### Tier 3 – Behavioral Semantics (MUST)
- Consistency level semantics: identical quorum calculations, identical
  read/write paths for all consistency levels.
- Identical CQL type semantics: ordering, equality, serialization.
- Identical schema agreement behavior.
- Identical failure modes: same error codes, same timeout semantics.

### Tier 4 – Operational (SHOULD)
- `cassandra.yaml` config file compatibility (parse and honor same keys).
- nodetool CLI compatibility (subset; undocumented options may diverge).
- Metric names and semantics should match (format may change: JMX → Prometheus).
- Log messages may differ in format but must convey equivalent information.

### Explicitly NOT required
- JMX API binary compatibility (replaced with gRPC/HTTP admin API).
- Java plugin/trigger binary compatibility.
- Thrift protocol.
- Identical GC/memory behavior (obviously).

## Verification Strategy

Each tier has a corresponding test gate:
1. **Protocol fuzz tests**: send randomized valid/invalid frames to both
   implementations and compare responses byte-by-byte.
2. **SSTable golden tests**: read the same SSTable files with both
   implementations, compare deserialized output.
3. **Differential CQL tests**: execute CQL workloads against both, compare
   result sets.
4. **Operational smoke tests**: run standard nodetool commands, compare
   output structure.

## Consequences

- Extra work to maintain two implementations until parity is proven.
- Need a differential test harness early (Phase 2-3).
- Can defer operational compatibility (Tier 4) to later phases.
