# ADR-002: QueryProcessor as Central Orchestrator

## Status
Accepted

## Context

The Cassandra Rust rewrite needs a central query processing layer between the native protocol server and the executor. In Java Cassandra, `org.apache.cassandra.cql3.QueryProcessor` serves this role, orchestrating parsing, planning, authorization, prepared statement caching, and execution.

Before this ADR, the server's `handle_query()` method inlined the parse→plan→execute pipeline, only handled `Message::Query`, returned `Void` for SELECT results, and mapped all errors to 0x2200 (INVALID).

## Decision

### 1. QueryProcessor as Orchestrator Pattern

Introduce `QueryProcessor` as a dedicated struct that owns `Arc<QueryExecutor>`, `Arc<PreparedCache>`, and `Arc<RwLock<SchemaCatalog>>`. It provides typed methods for each query flow:

- `process_query()` — simple QUERY messages
- `process_prepare()` — PREPARE messages
- `process_execute()` — EXECUTE messages (prepared statements)
- `process_batch()` — BATCH messages
- `execute_internal()` — system queries without auth

This matches Java's `QueryProcessor` responsibility split.

### 2. QueryOptions at CQL Level (not Protocol Level)

`QueryOptions` lives in `cassandra-cql` rather than `cassandra-native-protocol`. It wraps protocol-level `QueryParams` with CQL-semantic additions and provides:

- `From<QueryParams>` conversion for the protocol boundary
- `for_internal_calls()` factory for system queries
- Builder pattern for test construction

**Rationale:** Protocol types (`QueryParams`) should remain pure wire format. CQL-level types add semantic meaning (e.g., default consistency for internal calls).

### 3. ResultSet Conversion Strategy

Two conversion paths:

- **Executor → Protocol:** `QueryResult::Rows` (executor) → `RowsResult` (protocol) via explicit column spec conversion using `ColumnType::from_cql_type()`. Done in `server.rs:encode_query_result()`.
- **Rich path:** `ResultSet` → `RowsResult` via `From` impl, with automatic global table spec detection and metadata flag computation. Used for future paging and tracing support.

### 4. Prepared Statement ID Compatibility (MD5)

Statement IDs are computed as MD5 of the query text, matching Java's `QueryProcessor.computeId()`. This ensures prepared statement IDs are wire-compatible between Java and Rust nodes during rolling upgrades.

### 5. Error Mapping

`ExecutorError` variants map to specific `CassandraError` variants (and thus protocol error codes):

| ExecutorError | CassandraError | Code |
|---|---|---|
| InvalidQuery | InvalidQuery | 0x2200 |
| KeyspaceNotFound | InvalidQuery | 0x2200 |
| TableNotFound | InvalidQuery | 0x2200 |
| StorageError | ServerError | 0x0000 |
| SchemaError | ConfigError | 0x2300 |

`PlanError` variants follow a similar mapping. The protocol server uses `error_codes::error_to_message()` for wire encoding.

## Consequences

- All query types (QUERY, PREPARE, EXECUTE, BATCH) now have proper handlers
- SELECT queries return actual row data instead of Void
- Error codes are protocol-accurate instead of hardcoded 0x2200
- Prepared statement flow is functional (parse, cache, lookup, execute)
- UntypedResultSet enables typed access for internal system queries
- MonotonIc timestamps (CAS loop) match Java's ClientState behavior
