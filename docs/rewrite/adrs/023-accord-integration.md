# ADR-023: Accord Integration Architecture

## Status

Accepted

## Context

Cassandra 5.0 introduces the Accord distributed consensus protocol as an alternative to Paxos for lightweight transactions. The Rust rewrite must support both protocols and the migration path between them.

Java Cassandra's Accord subsystem spans 72 classes (~1.7MB). Rather than translating all at once, we build a layered architecture that can evolve incrementally.

## Decision

### Layered Architecture

The Accord integration follows a four-layer design:

1. **Core Types** (`types.rs`, `error.rs`) — TxnId, Timestamp, Keys, Txn, CommandStatus, AccordError
2. **State Machine** (`command_store.rs`) — In-memory DashMap tracking transaction status through PreAccepted -> Accepted -> Committed -> Applied
3. **Durability** (`journal.rs`) — Write-ahead journal for crash recovery, with replay capability
4. **Execution** (`executor.rs`, `task.rs`, `service.rs`) — AccordExecutor runs committed transactions; AccordService drives the 4-phase protocol

### Protocol Phases

The AccordService implements the simplified 4-phase protocol:
1. **PreAccept** — Register transaction with initial timestamp
2. **Accept** — Conflict resolution (simplified in initial implementation)
3. **Commit** — Durable decision
4. **Apply** — Execute mutation via AccordExecutor

### Feature Gating

Accord is disabled by default (`AccordConfig.enabled = false`). When disabled, `execute_transaction()` returns an error immediately. This allows deployment without Accord impact.

### Topology Mapping

`AccordTopology` maintains bidirectional mapping between Cassandra node UUIDs and Accord integer node IDs, with epoch tracking for topology changes.

## Consequences

- Accord can be enabled per-node via configuration
- The CommandStore uses DashMap for lock-free concurrent access
- The Journal provides crash recovery via entry replay
- Migration state tracking enables gradual Paxos -> Accord transition
- The simplified protocol lacks full conflict resolution (future work)
