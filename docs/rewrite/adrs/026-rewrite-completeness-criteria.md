# ADR-026: Rewrite Completeness Criteria

## Status

Accepted — 2026-03-17

## Context

With 63+ features in the gap matrix and 187 Java packages, we need a clear
definition of what "complete" means for the rewrite. Without explicit criteria,
features may be declared done prematurely or linger in partial state.

## Decision

### Feature-level completeness

A feature is **rewrite-complete** when ALL of the following hold:

1. **Status = `done`** in `final_gap_matrix.yaml`.
2. **Evidence exists**: At least one of `unit`, `integration`, `golden`, or `e2e`
   test suites exercises the feature.
3. **No `#[ignore]` gap guards** remain for that feature in `gap_guards.rs`.
4. **Coverage audit passes**: `python3 scripts/coverage_audit.py --check-matrix`
   shows 0 unclassified packages for this feature's Java packages.
5. **Performance budget met**: If the feature has a P0 criticality, it must pass
   the corresponding benchmark gate (ADR-015).

### Subsystem-level completeness

A subsystem (CQL, Storage, Distributed, etc.) is complete when:

1. All features in the subsystem are `done`, `baseline-excluded`, or `trunk-only`
   with appropriate feature gating.
2. Integration tests verify cross-feature interactions within the subsystem.
3. The subsystem passes differential testing against the Java oracle.

### Project-level completeness (GA readiness)

The rewrite is GA-ready when:

1. **All P0 features** are `done`.
2. **All P1 features** are `done` or `partial` with documented workarounds.
3. **Coverage audit**: 0 unclassified packages at sub-package level.
4. **Gap guard count**: All non-deferred `#[ignore]` tests are resolved.
5. **Performance**: All ADR-015 budget gates pass.
6. **Security**: ADR-012 security audit passes.
7. **Migration**: Dual-cluster migration validated per ADR-014.

### Tracking enforcement

- `cargo xtask coverage-audit` runs in CI and fails on regressions.
- Gap guard count is tracked per-PR; net new `#[ignore]` tests require
  justification in the PR description.
- The `final_gap_matrix.md` summary is regenerated on every matrix change.

## Consequences

1. Clear, measurable criteria prevent premature "done" declarations.
2. CI enforcement catches regressions automatically.
3. The gap matrix becomes the single source of truth for project progress.
4. Deferred/experimental features have a clear path to completion tracked in
   the matrix rather than being silently ignored.
