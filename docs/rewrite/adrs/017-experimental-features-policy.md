# ADR-017: Experimental and Trunk-Only Features Policy

## Status

Accepted — 2026-03-15 | Updated — 2026-03-17 (Prompt 11: definitive freeze)

## Context

The frozen trunk baseline contains several subsystems that are experimental,
under active development, or not present in any stable Cassandra release:

| Subsystem                    | Java Package                           | Nature          |
|------------------------------|----------------------------------------|-----------------|
| Transactional Cluster Meta   | `org.apache.cassandra.tcm`             | trunk-only      |
| Accord transactions          | `org.apache.cassandra.service.accord`  | experimental    |
| Consensus service            | `o.a.c.service.consensus`              | experimental    |
| Journal                      | `org.apache.cassandra.journal`         | trunk-only      |
| Profiler                     | `org.apache.cassandra.profiler`        | dev-time        |
| Full Query Logging           | `org.apache.cassandra.fql`             | diagnostic      |

Per ADR-016, ignoring these subsystems is not acceptable—they exist in the
frozen commit and must be accounted for.

## Decision

### Classification

Every feature in the gap matrix must carry one of:

- `done` — Rust implementation at parity with Java, evidence exists.
- `partial` — Core logic implemented, known gaps documented.
- `missing` — Not yet started, must be built.
- `blocked` — Depends on another incomplete subsystem.
- `trunk-only` — Present only in trunk, not in stable 5.0.x.
- `experimental` — Flagged as experimental in Java upstream.
- `tooling-only` — Operational tool, not data-path.
- `ops-only` — Operational concern (JMX, metrics, monitoring).
- `baseline-excluded` — Explicitly excluded from rewrite scope with justification.

### Isolation rules

1. **Feature flags**: Trunk-only and experimental features must be gated behind
   Cargo features (`trunk_only`, `experimental`). They must not be enabled by
   default.

2. **Guard-rail tests**: Each gated feature must have at least one `#[test]
   #[ignore]` test that documents the gap and expected closure phase.

3. **Configuration parity**: If a feature is enabled via `cassandra.yaml` in
   Java, the Rust config parser must recognize the key and either:
   - Honor it (if implemented), or
   - Log a warning and refuse to start (fail-safe), or
   - Accept it with a documented no-op (fail-open, only if safe).

4. **Gap matrix entry**: Every trunk-only/experimental feature must have a row
   in `final_gap_matrix.yaml` with `closure_phase` set.

5. **No silent drops**: A feature cannot be omitted without appearing in the
   matrix with status `baseline-excluded` and a justification.

### Posture per subsystem

| Subsystem | Posture                                              |
|-----------|------------------------------------------------------|
| TCM       | `trunk-only`, stub with feature flag, interface only |
| Accord    | `experimental`, deferred, guard-rail test            |
| Consensus | `experimental`, deferred, guard-rail test            |
| Journal   | `trunk-only`, stub behind feature flag               |
| Profiler  | `baseline-excluded`, dev-time only                   |
| FQL       | `missing`, queued for implementation                 |

## Consequences

1. The build without `trunk_only` or `experimental` features compiles a
   stable-equivalent binary.
2. CI coverage-audit detects any new Java class not in the matrix.
3. Teams can work on experimental features without destabilizing the stable
   build path.
4. Gap closure is tracked in the matrix and enforced by CI.
