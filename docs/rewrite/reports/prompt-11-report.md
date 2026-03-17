# Prompt 11 Report: Coverage Audit, Baseline Freeze & Executable Gap Matrix

**Date**: 2026-03-17
**Status**: Complete

## Summary

Prompt 11 produced a definitive baseline freeze and expanded the gap matrix from
63 features to 118, covering all Java sub-packages. CI guard-rails ensure no
regression in coverage classification.

## Changes Summary

### WU-01: Baseline Re-freeze Documentation
- Updated `baseline_freeze.md` with definitive status, git tag, SHA verification
- Created `scripts/verify_baseline.sh` for automated baseline consistency checks
- Updated `charter.md` section 2 with pinned stable tag (`cassandra-5.0.3`)

### WU-02: ADR Updates
- Updated ADR-016 with definitive freeze status
- Updated ADR-017 with prompt-11 update notice
- Created ADR-025: JMX Replacement Strategy
- Created ADR-026: Rewrite Completeness Criteria
- Updated charter sections 3-4 with JMX and completeness references

### WU-03: Coverage Audit Script Enhancement
- Added `--strict` mode for sub-package-level validation
- Added `--json` output for CI/xtask consumption
- Added orphan detection (matrix entries referencing non-existent packages)
- Added cross-reference annotations in inventory (classified yes/no)

### WU-04: Xtask Coverage-Audit Enhancement
- Enhanced `cargo xtask coverage-audit` to use strict mode
- Added JSON output parsing with structured summary
- Added `run_cmd_capture` helper for JSON output

### WU-05: Gap Matrix Expansion (CQL & Storage)
- Added 18 new feature entries for CQL and Storage sub-packages
- CQL: masking functions, function types, schema statements, selection functions, advanced terms
- Storage: unified compaction, compaction writers, SSTable format/indexing/metadata,
  compressed I/O, I/O utilities, trie memtable, commitlog compression, column index,
  type marshalling extended

### WU-06: Gap Matrix Expansion (Distributed, Security & Tooling)
- Added 37 new feature entries across all remaining sections
- Distributed: dynamic snitch, read repair, repair messages/asymmetric/consistent,
  streaming management/messages, hints persistence, service consensus sub-packages
- Indexes: SAI disk format, SAI query planning, SAI analyzers
- Security: auth persistent stores
- Tooling: nodetool formatters, nodetool stats
- Common: bloom filter, memory management, OffHeap BitSet, concurrent utilities,
  streaming histograms, ByteComparable, CDC, type serializers, Harry (excluded)
- TCM: expanded to cover all 9 sub-packages

### WU-07: Gap Guard-Rails (CQL & Storage)
- Added 14 new `#[ignore]` guard tests for CQL and Storage gaps
- CQL: masking, function types, schema statements, selection functions, advanced terms
- Storage: trie memtable, commitlog compression, SSTable read compat, unified compaction,
  compaction writers, index summary, SSTable metadata, column index, bloom filter

### WU-08: Gap Guard-Rails (Distributed, Security & Tooling)
- Added 16 new `#[ignore]` guard tests
- Distributed: dynamic snitch, read repair, repair messages, consistent repair,
  streaming messages, hints persistence
- Security: auth persistent stores
- Tooling: nodetool formatters, nodetool stats, SAI disk format, SAI analyzers
- Common: memory utilities, concurrent utilities, ByteComparable, CDC

### WU-09: Benchmark Placeholders
- Created `p0_gap_benchmarks.rs` with Criterion stubs for:
  - CQL parsing (SELECT, INSERT, BATCH)
  - SSTable read/write (1K rows, point lookup)
  - Compaction (2-way merge)
  - Coordinator latency (local read/write)
  - Protocol encoding (100-row result, query decode)
- Added `[[bench]]` entry to Cargo.toml

### WU-10: Gap Backlog Issues
- Created 13 structured markdown files in `docs/rewrite/tracking/gap-*.md`
- Each file covers one subsystem with description, acceptance criteria,
  dependencies, and test strategy for every `missing` feature
- Total: 45 missing features documented

### WU-11: CI Integration
- Added `coverage-guard` Makefile target
- Added `coverage-guard` CI workflow job in `rust-ci.yml`
- Validates strict matrix coverage, YAML validity, gap guard counts
- Created `docs/rewrite/tracking/ci-coverage-guard.md` documentation

### WU-12: This Report

## Metrics

| Metric | Before (Prompt 10) | After (Prompt 11) |
|--------|--------------------|--------------------|
| Features tracked | 63 | 118 |
| Gap guard tests | 26 | 56 |
| Ignored gap guards | 24 | 54 |
| Active gap guards | 2 | 2 |
| Backlog issue files | 0 | 13 |
| ADRs | 24 | 26 |
| CI jobs | 6 | 7 |

### Status Breakdown (118 features)

| Status | Count |
|--------|-------|
| `partial` | 44 |
| `missing` | 45 |
| `stub` | 18 |
| `experimental` | 5 |
| `baseline-excluded` | 4 |
| `trunk-only` | 2 |

## Risks

1. **Sub-package granularity**: Some Java packages may gain new sub-packages
   between freeze and GA. The coverage audit will detect these.
2. **Missing features count**: 45 features remain `missing`. These are tracked
   in the backlog and assigned closure phases.
3. **Benchmark stubs**: P0 benchmarks are stubs only. Real measurements require
   implementation of the underlying features.

## Acceptance Criteria

- [x] `python3 scripts/coverage_audit.py --check-matrix --repo-root .` passes (0 unclassified top-level)
- [x] `cargo xtask coverage-audit` passes (from rust/ dir)
- [x] `cargo test -p cassandra-diff-tests` compiles (ignored tests count as pass)
- [x] YAML is valid: `python3 -c "import yaml; yaml.safe_load(open('docs/rewrite/final_gap_matrix.yaml'))"`
- [x] Baseline verification: `bash scripts/verify_baseline.sh`

## Next Steps

- Close P0 `missing` features (prompt-12+)
- Replace benchmark stubs with real implementations
- Run strict mode in CI to catch sub-package regressions
