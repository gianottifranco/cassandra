# ADR-008: Paxos/LWT Implementation Design

**Status**: Accepted
**Date**: 2026-03-15
**Context**: Cassandra Rust Rewrite Phase 3

## Decision

Implement classic single-decree Paxos for Lightweight Transactions (IF EXISTS / IF conditions),
matching Cassandra Java's `org.apache.cassandra.service.paxos.PaxosState`.

### Ballot Design

- `Ballot = (timestamp_micros: i64, node_id: Uuid)` with lexicographic total ordering.
- Matches Java's timeuuid-based ballot ordering.
- `Ballot::none()` (all zeros) serves as the bottom element.

### State Model

- Per-partition `PaxosState` stored in `DashMap<Vec<u8>, PaxosState>` (in-memory).
- Three fields: `promised` (Ballot), `accepted` (Option<Proposal>), `committed` (Option<Proposal>).
- Invariant: `committed.ballot <= accepted.ballot <= promised`.

### Recovery

- On Prepare, if a replica returns an in-progress accepted proposal, the new proposer
  **must adopt it** (Paxos safety). This prevents losing a decided value.
- A committed proposal clears the accepted state for its round.

### Serial Consistency

- `SERIAL` → requires quorum of **all** Paxos participants.
- `LOCAL_SERIAL` → requires quorum of local-DC participants only.
- Both map to standard quorum via `ConsistencyLevel::block_for()`.

### Contention Handling

- Up to 4 retries with strictly increasing ballots.
- Exponential backoff not yet implemented (TODO).

## Consequences

- LWT is functional for single-partition CAS operations.
- Multi-partition CAS (batch CAS) is not supported (matches Java).
- Persistent Paxos state (system.paxos table) is not yet implemented — state is in-memory only.
  This means Paxos state is lost on node restart. TODO: persist to system table.

## Alternatives Considered

- **Raft**: Simpler protocol but doesn't match Cassandra's observable behavior.
- **Accord**: Future replacement but still fluid in Java trunk; deferred.
