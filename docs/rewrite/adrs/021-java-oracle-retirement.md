# ADR-021: Java Oracle Retention Policy

**Status**: Accepted
**Date**: 2026-03-16
**Deciders**: Principal Engineer

## Context

The Cassandra Rust rewrite has been developed alongside the original Java implementation. With the rewrite approaching release candidate status, a formal decision is needed on whether to retain, archive, or remove the Java codebase from the repository.

The Java codebase serves multiple purposes:
1. **Behavioral oracle** — The definitive reference for correct behavior.
2. **Differential testing** — Golden fixture generation and live diff-tests.
3. **Fallback** — A known-working implementation for production rollback.
4. **Migration source** — SSTable format reference for import/export.

## Decision

**Retain the Java codebase as an oracle and validation branch** until the Rust implementation passes all functional gates at parity.

### Retention Strategy

1. **Java remains in `main` branch** as read-only reference code.
2. **No new Java development** — Bug fixes only in Rust.
3. **Java is buildable** — `ant jar` continues to work for oracle use.
4. **Differential tests depend on Java** — Golden fixture generation, Docker oracle.
5. **Java is clearly marked** — `src/java/` directory with README explaining its role.

### Criteria for Java Removal

The Java codebase MAY be removed from `main` (archived to `java-oracle` branch) only when ALL of the following are met:

| # | Criterion | Evidence Required |
|---|-----------|-------------------|
| R1 | All F-gates in GA checklist at ✅ | `ga-checklist.md` fully green |
| R2 | All P-gates (performance) measured and within budget | Benchmark reports |
| R3 | Soak test passing for ≥1hr continuous | Soak test logs |
| R4 | Chaos tests passing (all 10 scenarios) | CI green |
| R5 | 3-node cluster validated | Integration test evidence |
| R6 | Migration path tested (Java→Rust→Java roundtrip) | Migration test suite |
| R7 | Community PMC vote to archive Java | Apache governance process |
| R8 | ≥3 months production soak by ≥2 operators | Operator attestation |

### Fallback Policy

If a critical production issue is discovered in the Rust implementation:
1. Rollback to Java is always possible via `scripts/rollback-drill.sh`.
2. SSTable format remains compatible for both read and write.
3. Java binary can be built from the `java-oracle` branch at any time.

## Consequences

- Repository size remains larger while Java is retained.
- CI must continue building Java (minimal cost — only for golden fixture generation).
- Clear separation prevents accidental Java modifications.
- Provides confidence to operators considering adoption.

## Alternatives Considered

1. **Remove Java immediately** — Rejected. Parity not yet demonstrated for all gates.
2. **Move Java to separate repository** — Rejected. Breaks diff-test infrastructure.
3. **Keep Java as git submodule** — Unnecessarily complex for the current phase.

## Related

- [GA Readiness Checklist](../ga-checklist.md)
- [Final Gap Matrix](../final_gap_matrix.md)
- [ADR-006: Java Oracle Policy](006-java-oracle-policy.md)
- [Migration Guide](../migration-guide.md)
