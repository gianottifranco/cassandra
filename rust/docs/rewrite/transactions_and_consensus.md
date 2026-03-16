# Transactions and Strong Consistency Architecture

## Overview
Apache Cassandra traditionally provides eventual clustering and tunable consistency. However, certain workloads require strong consistency, serializability, and isolated transactions. Cassandra has historically supported this via Lightweight Transactions (LWT) using a customized Paxos protocol.
With recent advancements (and trunk development in Cassandra 5.0+), Apache Accord is being integrated to provide distributed, multi-key, multi-partition transactions.

This document serves as the Architecture Decision Record (ADR) and implementation guide for how the Rust rewrite structures these transactional protocols.

## 1. Lightweight Transactions (Paxos)
LWTs in Cassandra execute using a four-phase Paxos protocol optimized for CAS (Compare and Set) operations:
1. **Prepare/Promise**: A coordinator generates a unique, monotonic `Ballot` and sends `Prepare` messages to the replica set.
2. **Read**: The coordinator performs a quorum read at `SERIAL` consistency to determine the current state and evaluate the CAS condition.
3. **Propose/Accept**: If the CAS condition matches, the coordinator proposes a new state.
4. **Commit/Acknowledge**: Once accepted by a quorum, the mutation is committed to the base tables, and the Paxos state is marked as committed.

### Rust Implementation Details
- **`cassandra-coordinator::paxos`**: Contains the protocol state (`PaxosState`, `Ballot`), message tracking (`Promises`, `Accepts`), and the core `PaxosCoordinator::execute_cas` logic.
- **`system.paxos` Table**: Stores the in-progress and committed Paxos state. We implemented `PaxosStorage` to persist and load this state natively onto the disk.
- **Contention Handling**: The `execute_cas` function employs exponential backoff with jitter and configurable retries when it encounters older/preempted ballots or in-progress transactions, matching the Java baseline behavior (`max_contention_retries`).

## 2. Counters and Read-Before-Write
Counters (`COUNTER` column type) represent replicated CRDTs (Conflict-Free Replicated Data Types). They cannot be safely updated idempotently, so they follow a Read-Before-Write coordination path:
1. **Read-Before-Write**: A coordinator targets a single replica (usually the closest or the leader for that partition) to perform a local read of the `CounterContext`.
2. **Merge**: The delta is applied to the retrieved context.
3. **Replicate**: The updated, fully merged context is sent to all standard replicas according to the Replication Strategy.

### Rust Implementation Details
- **`cassandra-coordinator::counter`**: Centralizes the Counter coordination logic, handling leadership election for the write and context merging.
- **`cassandra-storage::counter`**: Contains the `CounterContext` CRDT data structure, tracking epoch, node ID, and logical clock/count to correctly merge concurrent offline updates.

## 3. Accord (Distributed Transactions)
Apache Accord introduces a new epoch-based, leaderless dependency-graph consensus protocol ensuring strict serializable isolation across multiple partitions.

### Rust Implementation Details
- **`cassandra-accord`**: We established a foundational crate representing the Accord Service. Currently, this acts as a stub to safely wire up the routing layers without pulling in a highly volatile and immense protocol port.
- **`TransactionalMode`**: In parity with Cassandra trunk (CEP-15), we extended `TableMetadata` to track the `transactional_mode` parameter, mapping to `Off`, `Paxos`, `Accord`, or `Mixed`.
- **`ConsensusRouter`**: Located in `cassandra-coordinator::consensus::router`, this layer intercepts standard `IF` queries (CAS updates). Based on the table's `TransactionalMode`, it dispatches the operation to either the legacy Paxos coordinator or the newly wired Accord service.

## 4. Migration and Coexistence
The `TransactionalMode::Mixed` is designed to facilitate live migration of a table from Paxos-driven LWTs to Accord transactions without downtime. In the Rust implementation, pending a full Accord integration, Mixed mode falls back to Paxos for absolute safety.

## 5. Testing and Validation
- **Linearizability**: The Paxos coordinator has unit and integration tests enforcing linearizability properties (highest ballot wins, concurrent CAS failures).
- **CRDT Integrity**: The counter logic enforces proper CRDT merging across independent offline modifications.
- **Topology Awareness**: All writes and reads interact directly with `cassandra-cluster-metadata` ring implementations and failure detectors.
