# ADR-006: Java Oracle Maintenance Policy

- **Status**: Accepted
- **Date**: 2026-03-15
- **Context**: Cassandra Rust rewrite baseline freeze

## Context

The Java implementation of Cassandra is the source of truth (oracle) for
correctness during the rewrite. We need clear rules about:
- When and how to reference the Java code.
- When it's safe to remove or modify Java code.
- How to handle Java code that is buggy or deprecated.
- How to manage the dual codebase in terms of build, CI, and developer
  experience.

## Decision

### Principle: Java Is Read-Only Until Parity Is Proven

The Java source under `src/java/` and tests under `test/` must remain
**untouched and compilable** until the Rust equivalent passes the
corresponding differential test gates.

### Oracle Usage Levels

| Level | When | Action |
|-------|------|--------|
| **Reference** | During design | Read Java source to understand behavior. Document findings. |
| **Golden Input** | During testing | Run Java implementation to generate expected outputs. |
| **Differential** | During verification | Run both implementations side by side, compare outputs. |
| **Shadow** | Pre-production | Route production traffic to both, compare results, serve Java response. |

### Retirement Criteria

A Java subsystem may be marked `oracle-retired` only when ALL of:
1. The Rust implementation passes 100% of the corresponding Java unit tests
   (adapted to Rust).
2. Differential tests show byte-identical outputs for protocol-level
   operations.
3. Performance benchmarks show the Rust implementation meets or exceeds
   Java's throughput and p99 latency.
4. The change has been documented in an ADR or phase report.
5. A rollback plan exists (feature flag to re-enable Java path).

**Even after retirement, the Java source is preserved in the repository**
under `src/java/`. It is only removed when the next major version is cut
and the legacy compatibility window closes.

### Dual-Build Coexistence

- The existing Ant/Maven Java build (`build.xml`) remains functional.
- Rust build (`Cargo.toml` + Makefile) is independent.
- CI jobs run both builds. A Rust regression does not block Java builds
  and vice versa.
- The `.gitignore` is updated to handle both `target/` (Rust) and
  `build/` (Java) directories.

### Handling Java Bugs

If a Java behavior appears buggy:
1. Check if it's a known issue in the Cassandra JIRA.
2. If it is documented as a bug, the Rust implementation should implement
   the **correct** behavior and document the divergence.
3. If the behavior is ambiguous, match the Java behavior and file a
   follow-up issue to investigate.

### Documentation Obligations

Every Rust module that replaces Java functionality must include a doc comment
referencing the corresponding Java class(es):

```rust
//! Rust implementation of the CQL native protocol frame codec.
//!
//! Java oracle: `org.apache.cassandra.transport.Frame`
//! Java oracle: `org.apache.cassandra.transport.FrameEncoder`
//! Java oracle: `org.apache.cassandra.transport.FrameDecoder`
```

## Consequences

- The repository will contain both Java and Rust code for an extended period.
- Disk usage and CI time increase.
- Developers need familiarity with both ecosystems.
- Clear retirement gates prevent premature removal.
