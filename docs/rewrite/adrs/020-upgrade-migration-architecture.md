# ADR-020: Upgrade/Migration Architecture

**Status**: Accepted
**Date**: 2026-03-16
**Deciders**: Principal Engineer

## Context

With the `cassandra-migration` crate implemented, we need to formalize the architecture decisions around upgrade paths, data migration, CDC/FQL continuity, and rollback guarantees.

## Decisions

### 1. Mixed-Cluster Remains Infeasible

Mixed-cluster (Rust nodes joining Java ring) requires binary-compatible gossip, internode messaging, and SSTable formats. These remain incompatible and the effort to achieve compatibility would be prohibitive with high risk of production data corruption.

**Alternative**: Dual-cluster with shadow traffic (ADR-014) remains the migration strategy.

### 2. SSTable Import as Primary Data Migration Path

Rather than streaming data via internode protocol, we import Java SSTables via file-level conversion:
- Read Java SSTable components (Data.db, Index.db, Filter.db, etc.)
- Convert to Rust-native format
- Validate checksums during conversion
- Import into Rust data directory

**Rationale**: File-level import is safer, auditable, and doesn't depend on network protocol compatibility. It can be paused, resumed, and validated offline.

### 3. CDC Bridge via Checkpoint + Resume

CDC continuity is maintained by:
1. Recording a checkpoint of the last Java CDC segment ID at migration time
2. Copying any unconsumed CDC segments to the Rust cluster
3. Starting Rust CDC from checkpoint + 1
4. Validating no gap exists in the sequence

### 4. FQL Conversion to JSON

Java FQL uses Chronicle-Queue binary format. Rather than implementing full Chronicle-Queue parsing in Rust, we:
- Provide a simplified binary scanner for common cases
- Recommend using Java `fqltool dump` for production conversion
- Use JSON as the interchange format for FQL replay

### 5. Rollback at Any Phase

Rollback is guaranteed at every migration phase:
- **Pre-cutover**: No action needed; Java is still primary
- **During cutover**: Switch DNS/LB back to Java
- **Post-cutover**: Restore Java from pre-migration snapshot

The Rust cluster never modifies the Java cluster's data.

## Consequences

- Operators need 2× cluster hardware during migration (temporary)
- SSTable import time is proportional to data volume
- CDC consumers must be aware of the migration boundary
- Java cluster must be maintained until Rust cluster is fully validated

## Related

- [ADR-014: Migration Strategy](014-migration-strategy.md)
- [Migration Reference](../migration_upgrade_rollback.md)
