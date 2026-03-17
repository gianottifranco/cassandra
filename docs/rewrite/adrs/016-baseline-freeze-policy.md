# ADR-016: Dual-Baseline Freeze Policy

## Status

Accepted — 2026-03-15 | Definitive freeze — 2026-03-17 (Prompt 11)

## Context

Prompts 01–10 established a single baseline against Apache Cassandra **trunk**
(commit `076c6f11364645bbb43360f013bee6f50a099185`, 2026-03-15). However, trunk
contains features that are experimental, under active iteration, or not present
in any stable release. To distinguish between stable parity targets and
trunk-only work, we adopt a **dual-baseline** approach.

## Decision

### Primary baseline (parity target)

| Field           | Value                                      |
|-----------------|--------------------------------------------|
| Branch          | `trunk`                                    |
| Commit          | `076c6f11364645bbb43360f013bee6f50a099185`  |
| Date frozen     | 2026-03-15                                 |
| Cassandra ver.  | post-5.0, pre-6.0                          |

This is the *oracle commit*. All behavior observable at this commit—whether
stable or experimental—must be accounted for in the gap matrix.

### Stable reference baseline

| Field           | Value                                      |
|-----------------|--------------------------------------------|
| Branch          | `cassandra-5.0`                            |
| Tag (pinned)    | `cassandra-5.0.3`                          |
| Purpose         | Distinguish stable vs trunk-only features  |

### Git tag

The freeze is tagged as `cassandra-rewrite-baseline-v1` in the Java oracle repo.
Automated verification: `bash scripts/verify_baseline.sh`

Features present **only in trunk** (not in `cassandra-5.0`) are classified
`trunk-only` in the gap matrix and gated behind `#[cfg(feature = "trunk_only")]`.

### Re-freeze policy

- No re-freeze without a new ADR.
- If trunk moves significantly before GA, we evaluate whether to advance the
  frozen commit or absorb the delta as a maintenance batch.
- The gap matrix must be updated within 48h of any re-freeze.

## Consequences

1. Every feature is classified against two references: trunk (what exists) and
   stable (what is production-grade in Java).
2. Developers can build with `--features trunk_only` to include experimental
   subsystems, or omit them for a stable-only build.
3. CI runs both profiles: stable-only (must pass) and trunk-only (allowed to
   have known gaps, tracked in the matrix).
4. The coverage-audit xtask validates that no Java class falls outside the
   classification.
