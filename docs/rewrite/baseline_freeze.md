# Baseline Freeze Document

## Status

**Definitive** — Frozen 2026-03-15, Tag `cassandra-rewrite-baseline-v1`

## Primary Baseline (Oracle Commit)

| Field               | Value                                              |
|---------------------|----------------------------------------------------|
| Branch              | `trunk`                                            |
| Commit              | `076c6f11364645bbb43360f013bee6f50a099185`          |
| Date frozen         | 2026-03-15                                         |
| Cassandra ver.      | post-5.0, pre-6.0                                  |
| Git tag             | `cassandra-rewrite-baseline-v1`                    |
| Rust HEAD at freeze | `fffb78a5283efacb1e912d17b96f7d972916c0e1`         |

### SHA Verification

```bash
# Verify the baseline commit SHA256
echo "076c6f11364645bbb43360f013bee6f50a099185" | sha256sum
# Expected: deterministic hash of the commit ID string

# In the Java oracle checkout:
cd /path/to/cassandra-java
git rev-parse cassandra-rewrite-baseline-v1
# Expected: 076c6f11364645bbb43360f013bee6f50a099185

git log -1 --format="%H %ai %s" cassandra-rewrite-baseline-v1
```

## Stable Reference Baseline

| Field               | Value                                              |
|---------------------|----------------------------------------------------|
| Branch              | `cassandra-5.0`                                    |
| Tag                 | `cassandra-5.0.3` (pinned at freeze)               |
| Purpose             | Distinguish stable vs trunk-only features           |

## Cross-References

- **ADR-016**: Dual-Baseline Freeze Policy — defines primary/stable distinction
- **ADR-017**: Experimental Features Policy — trunk-only gating via `#[cfg(feature = "trunk_only")]`
- **ADR-025**: JMX Replacement Strategy — HTTP admin API replaces JMX
- **ADR-026**: Rewrite Completeness Criteria — defines what "done" means
- **Gap Matrix**: `docs/rewrite/final_gap_matrix.yaml` — `baseline` section records same commits
- **Verification**: `scripts/verify_baseline.sh` — automated consistency check

## Branch Policy

1. All prompt work (01–26) targets `trunk` branch in this repo
2. No Java baseline advance without a new ADR (per ADR-016 re-freeze policy)
3. If Java trunk moves significantly before GA, a delta evaluation ADR is required within 48h
4. The gap matrix must be updated within 48h of any re-freeze

## Feature Gating

| Build Profile | Description |
|---------------|-------------|
| Default       | Stable-only features (matching `cassandra-5.0`) |
| `--features trunk_only` | Includes TCM, Accord, Journal, experimental subsystems |

CI runs both profiles: stable-only (must pass) and trunk-only (allowed known gaps).

## Validation Steps

Run the automated verification script:

```bash
bash scripts/verify_baseline.sh
```

Or verify manually:

```bash
# 1. Check Java oracle commit (if cloned)
cd /path/to/cassandra-java
git log -1 --format="%H"
# Expected: 076c6f11364645bbb43360f013bee6f50a099185

# 2. Check Rust workspace baseline reference
grep "commit:" docs/rewrite/final_gap_matrix.yaml
# Expected: "076c6f11364645bbb43360f013bee6f50a099185"

# 3. Verify gap matrix baseline section
python3 -c "
import yaml
with open('docs/rewrite/final_gap_matrix.yaml') as f:
    data = yaml.safe_load(f)
b = data['baseline']['primary']
assert b['commit'] == '076c6f11364645bbb43360f013bee6f50a099185'
assert b['date'] == '2026-03-15'
print('Baseline verified:', b['branch'], '@', b['commit'][:12])
"

# 4. Run full verification script
bash scripts/verify_baseline.sh
```

## Re-Freeze Criteria

A re-freeze is warranted if:
1. Java trunk gains a major subsystem rewrite that invalidates current gap analysis
2. A critical security fix in trunk changes behavior the Rust rewrite depends on
3. More than 6 months pass since freeze with significant trunk divergence

Re-freeze requires:
- New ADR documenting rationale
- Updated `final_gap_matrix.yaml` baseline section
- Updated `baseline_freeze.md` (this file)
- Updated tag: `cassandra-rewrite-baseline-v2`
- Full re-run of coverage audit: `python3 scripts/coverage_audit.py --check-matrix --repo-root .`
- Gap matrix update within 48h
