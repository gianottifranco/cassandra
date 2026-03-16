# ADR-022: Non-Degradation Policy

## Status

Accepted — 2026-03-16

## Context

The Cassandra Rust rewrite proceeds through 27 prompts (00–26) executed sequentially.
Each prompt adds functionality but must not regress existing capabilities. Without a
formal non-degradation policy, later prompts could silently break earlier work,
increasing debugging cost and eroding confidence in the codebase.

## Decision

### What Cannot Regress Between Prompts

| Metric | Measurement | Enforcement |
|--------|-------------|-------------|
| Test count | `cargo test --workspace 2>&1 \| grep "test result"` | Count must be ≥ previous prompt's count |
| Test pass rate | Same command, 0 failures | No new failures allowed |
| Build success | `cargo check --workspace` exit 0 | CI gate on every PR and push |
| Clippy clean | `cargo clippy --workspace -- -D warnings` | CI gate |
| Gap guard count | `python3 rust/scripts/gap_audit.py gap-guards --json` | Count must be ≤ previous prompt's count |
| TODO count in critical subsystems | `python3 rust/scripts/gap_audit.py todos --json` | Must not increase in critical crates |

### Measurement Method

1. **Before each prompt begins**: Record baseline metrics by running:
   ```bash
   cargo test --workspace 2>&1 | tail -1           # test count + pass rate
   python3 rust/scripts/gap_audit.py todos --json   # TODO baseline
   python3 rust/scripts/gap_audit.py gap-guards --json  # gap guard baseline
   ```

2. **After each prompt completes**: Run same commands and compare.

3. **CI enforcement**: The `gap-audit` CI job runs on every push and PR, ensuring
   markers don't proliferate in critical subsystems.

### Exception Process

If a prompt must temporarily regress a metric (e.g., adding a gap guard for a
newly-discovered gap):

1. Document the regression in the prompt's completion report
2. Add a tracking entry to `gap_backlog.md` with the owning prompt for resolution
3. The gap guard must reference the tracking entry (e.g., `// GAP-GUARD: BB-5, prompt-10`)

### Critical Subsystems

The following crates have stricter enforcement (zero tolerance for new untracked markers):
- `cassandra-storage`
- `cassandra-cql`
- `cassandra-coordinator`
- `cassandra-native-protocol`
- `cassandra-server`

## Consequences

1. Every prompt completion is verifiable against objective metrics
2. Regressions are caught immediately rather than accumulating
3. The gap audit script becomes the single source of truth for marker tracking
4. CI prevents unintentional degradation from reaching trunk
5. Exception process allows legitimate temporary regressions with traceability
