# ADR-029: Partitioning and Replica Placement Design

## Status

Accepted

## Context

The Java codebase has a deep hierarchy for token bounds (`AbstractBounds`, `Range`, `Bounds`, etc.), a class-based `IPartitioner` interface with factory methods, cloud-provider snitches that fetch metadata via HTTP, and a token allocator that balances ownership considering replication factor and topology.

The Rust rewrite needs to model these concepts idiomatically while preserving compatibility with the Java wire protocol and on-disk formats.

## Decision

### Partitioner Trait

Extended the existing `Partitioner` trait with additional methods (`split`, `preserves_order`, `random_token`, `describe_ownership`) rather than creating separate traits. Added `TokenFactory` as a separate trait for serialization concerns. Added `LocalPartitioner` for system keyspaces.

**Rationale:** Keeping one trait avoids the need for trait objects to implement multiple traits. `TokenFactory` is separate because serialization is orthogonal to partitioning.

### AbstractBounds as Enum

Modeled Java's class hierarchy (`Range<T>`, `Bounds<T>`, `IncludingExcludingBounds<T>`, `ExcludingBounds<T>`) as a single generic enum `AbstractBounds<T>` with four variants.

**Rationale:** Rust enums naturally model closed class hierarchies. The enum approach provides exhaustive matching, avoids heap allocation, and makes the inclusivity/exclusivity explicit in the variant name.

### ReplicationFactor as Struct

Replaced the bare `usize` replication factor with a `ReplicationFactor` struct that tracks both total and full replica counts, with `parse("3/1")` support.

**Rationale:** Transient replication requires distinguishing full from transient replicas. A typed struct prevents mixing up total vs full counts.

### Cloud Snitches: Parsing vs HTTP

Separated cloud metadata into two layers:
1. **Parsing functions** (`parse_ec2_az`, `parse_gce_zone`, etc.) — pure, testable
2. **Snitch constructors** (`from_metadata`) — accept parsed results

The actual HTTP fetching is left to the runtime layer, avoiding a hard dependency on `reqwest` in the library crate.

**Rationale:** Metadata parsing is the complex, error-prone part that benefits from unit testing. HTTP fetching is straightforward and environment-dependent. This separation keeps the library crate lightweight and testable without network access.

### Token Allocator Strategy

Two allocators:
- `NoReplicationTokenAllocator` — splits largest gaps (RF=1)
- `ReplicationAwareTokenAllocator` — scores gaps by owner load weighted by RF

Both use greedy midpoint splitting rather than the full optimization from Java's implementation.

**Rationale:** The greedy approach produces good-enough balance for most clusters and is simpler to maintain. The full Java implementation uses a complex iterative optimization that can be added later if needed.

### TransientReplicationStrategy Ordering

Full replicas are sorted before transient replicas in the replica list returned by `calculate_natural_replicas()`.

**Rationale:** Read operations only contact full replicas. Having full replicas first means callers can simply take the first N replicas for reads without filtering.

## Consequences

- Existing code using `usize` for RF will need to migrate to `ReplicationFactor` where transient replication is used
- Cloud snitches require an external HTTP layer to be fully functional in production
- The token allocator may produce slightly different placements than Java's implementation, but the invariant (balanced ownership) is preserved
