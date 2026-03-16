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

## Shared Counters
Shared counters (distributed G-Counters / state-based CRDTs) are coordinated using a read-before-write process.
When a `CounterMutation` is sent to a coordinator:
1. The coordinator selects a replica to act as the leader.
2. The leader reads the current `CounterContext` CRDT from local storage.
3. The leader merges the delta into the `CounterContext`.
4. The leader forwards the merged context to remaining replicas.

Periodically (or via repair), replicas run a consolidation process to fold non-local shards into their local shard to prevent infinite shard vector growth.

## Accord & Consensus Migration
As per Cassandra 5.0 and beyond, Accord provides true multi-partition strictly serializable transactions.
The Rust implementation provides the `cassandra-accord` crate which mirrors `org.apache.cassandra.service.accord`.
A routing layer (the `consensus::Router`) dispatches CAS and transaction operations between classical Paxos and Accord.
This dispatch is determined by the `TransactionalMode` (Off, Paxos, Accord, Mixed) defined in `TableMetadata`.
