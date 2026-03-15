# ADR-011: Paxos Recovery and Contention Handling

**Status**: Accepted
**Date**: 2026-03-15

## Context

The Paxos/LWT coordinator orchestrates Compare-and-Set operations across
replicas.  Two production-critical behaviors need explicit design:

1. **Recovery**: When a coordinator discovers an in-progress (accepted but
   not committed) proposal during Prepare phase, it must complete that
   round before starting its own — this is the Paxos liveness requirement.

2. **Contention**: Under high write concurrency to the same partition key,
   multiple coordinators may preempt each other indefinitely.  A backoff
   strategy is needed to ensure forward progress.

## Decision

### Recovery

When `PaxosPromise` responses contain an `in_progress` proposal (accepted
ballot > committed ballot on any replica):

- The coordinator **adopts** the in-progress mutation.
- It proposes and commits that mutation first.
- The caller's CAS then retries with a fresh ballot and re-evaluates
  conditions.

This matches `StorageProxy.cas()` in Java Cassandra.

### Contention Backoff

Introduced `PaxosConfig` with:
- `max_contention_retries` (default: 4)
- `base_backoff_micros` (default: 100µs)
- `use_jitter` (default: true)

Backoff is computed as `base * 2^(attempt-1)` with optional random jitter
(0.5x–1.5x multiplier).  This prevents thundering-herd scenarios.

### State Machine Extensions

Added to `PaxosState`:
- `needs_recovery()` — returns true if accepted > committed
- `in_progress_proposal()` — returns the in-flight proposal
- `promised_ballot()` — accessor for current promise

## Consequences

- **Safety preserved**: Adopting in-progress proposals guarantees Paxos
  safety — no committed value is ever overwritten.
- **Liveness improved**: Contention backoff ensures forward progress under
  load.
- **Configurability**: Operators can tune retry/backoff parameters per
  use case.
- **Test coverage**: 12 new tests cover recovery, contention cascades,
  and backoff arithmetic.
