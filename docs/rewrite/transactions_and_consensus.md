# Transactions and Consensus API

## Lightweight Transactions (Paxos/LWT)
Apache Cassandra supports linearizable isolation via its implementation of single-decree Paxos. This is exposed via the CQL `IF` conditions (e.g., `INSERT ... IF NOT EXISTS`).

In the Rust implementation, LWT is managed by the `cassandra_coordinator::paxos` module.
The phases are:
1. **Prepare/Promise**: A proposer sends a `PaxosPrepare` message with a unique monotonically increasing ballot. Replicas return `PaxosPromise` which locks out older ballots and returns any in-progress accepted proposals.
2. **Read**: The coordinator executes a Quorum read at `SERIAL` consistency to fetch the current state of the row.
3. **Execute Condition**: The IF conditions are evaluated. If false, the CAS returns the current row.
4. **Propose/Accept**: The new mutation is sent to replicas via `PaxosPropose`. Replicas return `PaxosAccept` saving the proposal.
5. **Commit**: The mutation is sent as a `PaxosCommit` and applied durably to the storage layer.

State is persisted locally in the `system.paxos` table to ensure node restarts do not lose accepted proposals.

### Paxos Persistence & Recovery

`PaxosStorage` provides persistence to `system.paxos` with methods: `save_promise`, `save_proposal`, `save_commit`, `load_state`, and `load_all_uncommitted`. On startup, `PaxosReplica::recover()` scans for accepted-but-not-committed proposals and loads them into memory.

### Paxos Repair

`PaxosRepair` scans `system.paxos` for uncommitted proposals and drives them to completion via the coordinator. This runs during anti-entropy repair and on node startup.

### Contention Strategy

`ContentionStrategy` trait with `ExponentialBackoff` and `ConstantBackoff` implementations provides pluggable backoff behavior for CAS retries under contention.

### Paxos Cleanup

`PaxosCleanup` periodically removes committed Paxos state older than `gc_grace_seconds`, preventing unbounded growth of the `system.paxos` table.

## Shared Counters
Shared counters (distributed G-Counters / state-based CRDTs) are coordinated using a read-before-write process.
When a `CounterMutation` is sent to a coordinator:
1. The coordinator selects a replica to act as the leader.
2. The leader reads the current `CounterContext` CRDT from local storage.
3. The leader merges the delta into the `CounterContext`.
4. The leader forwards the merged context to remaining replicas.

Periodically (or via repair), replicas run a consolidation process to fold non-local shards into their local shard to prevent infinite shard vector growth.

## Accord Distributed Transactions

The `cassandra-accord` crate implements the Accord consensus protocol for multi-partition transactions:

- **Core Types**: `TxnId`, `Timestamp`, `Keys`, `Txn`, `CommandStatus`
- **CommandStore**: In-memory DashMap tracking transaction status (PreAccepted -> Accepted -> Committed -> Applied)
- **AccordJournal**: Write-ahead journal for crash recovery
- **AccordExecutor**: Executes committed transactions
- **AccordTopology**: Maps Cassandra node UUIDs to Accord integer IDs

### 4-Phase Protocol
1. **PreAccept** — Register transaction with initial timestamp
2. **Accept** — Conflict resolution and execution timestamp agreement
3. **Commit** — Durable decision
4. **Apply** — Execute mutation

## Consensus Migration (Paxos -> Accord)

The `ConsensusRouter` dispatches CAS and transaction operations between Paxos and Accord based on `TransactionalMode` (Off, Paxos, Accord, Mixed).

In Mixed mode, the router performs per-key migration-aware routing:
- Keys in `Paxos` or `Migrating` state route to Paxos (safe default)
- Keys in `Accord` state route to Accord
- `TableMigrationState` tracks per-table migration progress

`ConsensusMetrics` provides observability into routing decisions.

## CQL Transaction Statement

`BEGIN TRANSACTION ... COMMIT TRANSACTION` syntax supports:
- `LET` bindings for read-phase SELECT queries
- DML statements (INSERT, UPDATE, DELETE) for the write phase
- Optional `RETURNING` clause for result projection

`ConditionStatement` supports Accord-style multi-partition IF conditions with variable references from LET bindings.

## StorageProxy CAS Entry Points

`StorageProxy` provides:
- `cas()` — Routes via TransactionalMode
- `cas_paxos()` — Direct Paxos CAS
- `cas_accord()` — Direct Accord transaction execution
