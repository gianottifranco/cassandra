# ADR-024: Paxos to Accord Migration Strategy

## Status

Accepted

## Context

Tables may transition from Paxos LWT to Accord transactions. During migration, both protocols must coexist safely. Java Cassandra uses `TransactionalMode` (Off/Paxos/Accord/Mixed) and per-key migration tracking.

## Decision

### Per-Key Migration State

Each partition key tracks its own migration state via `KeyMigrationState`:
- **Paxos** — Key is managed by Paxos (default)
- **Migrating** — Key is in transition; both protocols may be active
- **Accord** — Key has been fully migrated to Accord

`TableMigrationState` aggregates per-key states per table, with progress tracking (0.0 to 1.0).

### ConsensusRouter Mixed Mode

When `TransactionalMode::Mixed`:
1. Router queries `TableMigrationState` for the partition key
2. Keys in `Paxos` or `Migrating` state route to Paxos (safe default)
3. Keys in `Accord` state route to Accord
4. Metrics track routing decisions for observability

### Safety Invariant

During migration, Paxos is the safe fallback. A key is only routed to Accord after it has been explicitly marked as `Accord` in the migration state. This prevents split-brain between the two protocols.

### Metrics

`ConsensusMetrics` tracks:
- `paxos_count` — Operations routed to Paxos
- `accord_count` — Operations routed to Accord
- `rejected_count` — Operations rejected (mode Off)
- `migration_count` — Operations in Mixed mode

## Consequences

- Migration is gradual and per-key, not per-table all-at-once
- Paxos remains the safe default during transition
- Operators can monitor migration progress via metrics
- Rollback is possible by resetting key states back to Paxos
