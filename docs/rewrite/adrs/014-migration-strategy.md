# ADR-014: Migration Strategy – Dual-Cluster with Shadow Traffic

**Status**: Accepted
**Date**: 2026-03-15
**Deciders**: Principal Engineer

## Context

We need to define how operators migrate from a Java Cassandra cluster to the Rust implementation. Two primary strategies were considered:

1. **Mixed-cluster**: Rust nodes join an existing Java ring via gossip, sharing token ownership.
2. **Dual-cluster with shadow traffic**: Separate Rust cluster receives replayed traffic; data divergence is detected offline.

## Decision

We adopt **dual-cluster with shadow traffic** as the migration strategy.

## Rationale

### Why not mixed-cluster

Mixed-cluster requires:
- Gossip wire-format binary compatibility (not achieved)
- Internode messaging wire compatibility (not achieved)
- SSTable format cross-compatibility (not achieved)
- Schema coordination via gossip (not implemented)

These are individually large efforts and, if buggy, risk data loss in a production ring. Mixed-cluster is deferred to a post-GA milestone if ever needed.

### Why dual-cluster works

- **Isolation**: Rust cluster is fully independent; a bug cannot corrupt the Java cluster.
- **Rollback**: Instant — stop sending traffic to Rust, Java continues unchanged.
- **Validation depth**: FQL replay + response comparison catches semantic divergence that wire-format testing alone would miss.
- **Operational familiarity**: operators already run multiple clusters (e.g., staging/production).

## Migration Flow

```
┌──────────────┐    FQL logs    ┌──────────────┐
│  Java Cluster │──────────────▶│  Shadow Tool  │
│  (production) │               │  (replay +    │
└──────────────┘               │   compare)    │
                                └──────┬───────┘
                                       │ replay
                                       ▼
                                ┌──────────────┐
                                │  Rust Cluster │
                                │  (shadow)     │
                                └──────────────┘
```

### Steps

1. **Pre-migration**: Deploy Rust cluster, apply same schema.
2. **Shadow phase**: Enable FQL on Java; replay logs to Rust; compare responses.
3. **Bake period**: Run shadow for 1-2 weeks; monitor divergence rate.
4. **Cutover**: Switch application traffic to Rust; keep Java warm for rollback.
5. **Teardown**: After bake period, decommission Java cluster.

## Consequences

- Operators need 2× cluster hardware during migration (temporary).
- Data migration for existing data requires bulk import or SSTable conversion.
- FQL must capture enough information for meaningful comparison.
- Rollback is trivial: revert DNS/LB to Java cluster.

## Related

- [ADR-006: Java Oracle Policy](006-java-oracle-policy.md)
- [Compatibility Matrix](../compatibility_matrix.md)
- [Migration Guide](../migration-guide.md)
