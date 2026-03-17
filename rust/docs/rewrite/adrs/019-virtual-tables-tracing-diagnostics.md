# ADR-019: Virtual Tables, Tracing, Diagnostics, and System Keyspaces

## Status
Accepted

## Context

Prompt 19 closes operational surface gaps across five subsystems: virtual tables, system keyspaces, tracing, audit filtering, and notifications/diagnostics. The Java codebase exposes these as tightly coupled components; the Rust rewrite maintains clean module boundaries.

## Decisions

### 1. Virtual Table Provider/Trait Pattern

Virtual tables implement the `VirtualTable` trait, returning `Vec<HashMap<String, String>>` rows. Populated tables wrap real data sources (e.g., `MetricsRegistry`, `OperationTracker`) behind `Arc` pointers. Factory functions create pre-configured table instances.

**Rationale:** Trait-based design allows new virtual tables without modifying the registry. String-typed rows simplify CQL serialization at the cost of runtime type safety — acceptable for read-only diagnostic data.

### 2. Tracing In-Memory Ring Buffer

`TracingManager` stores active sessions in a `DashMap` and completed sessions in a bounded `VecDeque` (capacity 1000). No disk persistence — tracing data is ephemeral.

**Rationale:** In-memory storage avoids write amplification from persisting every trace event. The ring buffer bounds memory usage. Production deployments that need durable traces can implement the `ExpirableSessionStore` trait with a persistent backend.

### 3. Tracing TTL Cleanup Separation

`TracingCleanupTask` and `ExpirableSessionStore` are fully independent from `TracingManager`. Each defines its own data structures.

**Rationale:** Separation allows TTL cleanup logic to be tested and evolved independently. The cleanup task can be wired to different session stores without coupling to the manager's internal state.

### 4. Audit Filtering at Caller Site

`AuditFilter` wraps `AuditLoggingOptions` and provides `should_log(ctx)` for include/exclude enforcement on keyspaces and categories. The filter is evaluated at the call site before constructing the full audit event.

**Rationale:** Early filtering avoids unnecessary `AuditEvent` construction on the hot path. Category-based filtering (Auth, Query, DML, DDL, DCL) maps cleanly to `AuditEventType` variants.

### 5. DiagnosticEventService Separation from StorageEventBus

`DiagnosticEventService` is a separate pub/sub system in `cassandra-admin`, distinct from `StorageEventBus` in `cassandra-storage`. It handles broader runtime events (schema changes, bootstrap, GC pauses, slow queries) with a history ring buffer.

**Rationale:** Storage events are low-level (SSTable added/removed). Diagnostic events span the full server lifecycle. Keeping them separate prevents `cassandra-storage` from depending on admin-level concerns. Both use the same panic-isolated dispatch pattern.

### 6. Extended Storage Events

`ExtendedEventBus` adds new event types (memtable flush start, schema changed, bootstrap, snapshots) as a separate enum and bus, preserving backward compatibility with existing `StorageEventBus` consumers.

**Rationale:** Adding variants to the existing `StorageEvent` enum would be a breaking change for all pattern matches. A separate enum and bus allows gradual adoption.

### 7. System Keyspace Explicit Bootstrap

`SystemKeyspaceManager::bootstrap()` explicitly adds all 5 system keyspaces to a `SchemaCatalog`. This is a one-time operation during node startup.

**Rationale:** Explicit bootstrap makes the initialization sequence visible and testable, unlike Java's implicit static initialization. The manager tracks bootstrap state to prevent double-initialization.

### 8. System.local / Peers Data Structs

`LocalNodeInfo` and `PeersEntry` are plain data structs with `to_row_map()` for virtual table integration. They do not read from storage — callers populate them from node state.

**Rationale:** Decoupling data population from storage reads allows these types to be used in tests and during bootstrap before storage is fully initialized.

### 9. HTTP Diagnostics via Traits

HTTP handlers accept trait objects (`DiagnosticHistoryProvider`, `TracingInfoProvider`, `AuditConfigProvider`) rather than concrete types.

**Rationale:** Trait-based handlers are testable without spinning up real services. The admin server can wire different implementations based on node configuration.

## Consequences

- Virtual tables now serve real data from metrics and operations
- Tracing has a complete lifecycle: begin → record → finish → expire
- Audit filtering reduces overhead on un-audited operations
- Diagnostic events provide a unified view of server-wide events
- System keyspaces are explicitly bootstrapped with all table schemas
- All new modules are independently testable with no cross-dependencies
