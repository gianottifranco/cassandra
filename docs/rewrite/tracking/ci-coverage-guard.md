# CI Coverage Guard

**Added**: Prompt 11 (2026-03-17)

## Purpose

The coverage guard CI job ensures that:
1. Every Java package is classified in the gap matrix (strict sub-package validation)
2. The gap matrix YAML is valid and parseable
3. Gap guard test counts are tracked for regression detection

## CI Jobs

### `coverage-guard` (GitHub Actions)

Runs on every PR and trunk push. Fails if:
- Any Java sub-package is unclassified in `final_gap_matrix.yaml`
- The YAML file is malformed
- The Python audit script exits non-zero

### `gap-audit` (existing)

Scans for TODOs, stubs, and gap guard tests. Reports counts but does not fail
on gap guards (they are expected during development).

## Local Validation

```bash
# From repo root:
python3 scripts/coverage_audit.py --check-matrix --strict --repo-root .

# From rust/ directory:
make coverage-guard

# Full audit via xtask:
cargo xtask coverage-audit
```

## Regression Policy

- Net new `#[ignore]` gap guard tests require justification in PR description
- Removing a classified package from the matrix without replacing it is blocked
- Any decrease in classified package count triggers CI failure
