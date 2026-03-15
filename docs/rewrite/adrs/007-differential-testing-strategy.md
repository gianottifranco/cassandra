# ADR-007: Differential Testing Strategy

- **Status**: Accepted
- **Date**: 2026-03-15
- **Context**: Phase 2 of the Cassandra Rust rewrite

## Context

The Cassandra Rust rewrite requires a rigorous verification strategy that
treats the Java implementation as an executable specification. We need to:
- Compare observable behavior between Java and Rust implementations.
- Automate this comparison at multiple levels of fidelity.
- Track progress as the Rust implementation grows.
- Catch regressions before they accumulate.

## Decision

### Three-Tier Testing Strategy

| Tier | Name | When | Infrastructure |
|------|------|------|----------------|
| **1. Golden** | Offline golden tests | Always (CI, local) | Pre-committed fixtures only |
| **2. Live** | Docker dual-cluster | CI trunk push, local opt-in | Docker Compose |
| **3. Fuzz** | Property-based + model | CI + local | proptest, Harry hooks |

### Tier 1: Golden Tests (Offline)

Pre-generated fixtures from the Java oracle are committed to VCS at
`rust/diff-tests/golden/`. The `cassandra-diff-tests` Rust crate loads
these fixtures and verifies that:
- Rust serialization of CQL types matches Java's byte-for-byte.
- Protocol frame parsing roundtrips correctly.
- Error codes match the protocol specification.

**Advantages**: Fast, deterministic, no external dependencies.
**Limitations**: Only tests what was pre-captured; can go stale.

**Update policy**: Golden fixtures are regenerated when:
1. The baseline Java commit changes.
2. New CQL features or types are added to the scope.
3. A test failure indicates fixture staleness.

### Tier 2: Live Differential Tests (Docker)

Docker Compose orchestrates a Java oracle node and a Rust SUT (system
under test). Python pytest tests send the same operations to both clusters
and compare results.

**Why Docker Compose over CCM?**
- **Reproducibility**: Docker images are hermetic; CCM depends on local
  Java/Python/OS state.
- **CI portability**: GitHub Actions has native Docker support; CCM requires
  complex setup.
- **Isolation**: Docker networks prevent port conflicts with local services.
- **Multi-platform**: Docker works on Linux, macOS, and CI runners.

CCM remains available as a fallback for developers who prefer it, but is
not the primary test infrastructure.

### Tier 3: Fuzz / Model Testing

- **proptest**: Property-based strategies for protocol frames and CQL values.
  Verifies encoding/decoding roundtrip properties.
- **Harry**: The existing `ci/harry_simulation.sh` runs against Java.
  A hook is provided to run the same workload against the Rust node once
  it supports enough CQL (estimated Phase 5+).
- **FQL replay**: A documented interface for replaying Full Query Logs
  captured from Java against the Rust node (deferred — FQL is out of
  initial scope).

### XFAIL-to-PASS Progression Model

During early phases, Rust-side tests are marked `@pytest.mark.xfail`.
As protocol and CQL handling is implemented:
1. Tests naturally start passing →  XFAIL becomes XPASS.
2. XPASS tests should be promoted to regular tests (remove xfail marker).
3. The XFAIL count across phases is the primary progress metric.

This model provides:
- **No false failures**: XFAIL tests don't fail CI.
- **Visible progress**: XPASS count increases as features land.
- **Regression detection**: A previously passing test going back to XFAIL
  signals a regression.

## Consequences

- Golden tests add ~50 files of fixtures to the repository.
- Docker-based tests are slow (60-90s startup) → run only on trunk push.
- Developers need Docker installed for full diff-test runs.
- The XFAIL model requires manual promotion of passing tests.
- A "fixture freshness" CI check should be added when fixtures can be
  regenerated automatically (requires Java build in CI).
