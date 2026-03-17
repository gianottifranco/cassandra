# Frontend Parity: CQL Client-Server Surface

## Protocol Features

| Feature | Status | Notes |
|---------|--------|-------|
| v4 frame codec | Complete | Full encode/decode |
| v5 frame codec | Complete | Beta flag validation |
| v5 `now_in_seconds` | Complete | Parsed from query params |
| Custom payload (request) | Complete | Bytes-map decoding |
| Custom payload (response) | Complete | Encoding in `wrap_response` |
| Tracing flag | Complete | `TraceSession` UUID attached |
| Warning flag | Complete | String list in response |
| Compression (LZ4/Snappy) | Complete | Feature-gated |
| All 18 opcodes | Complete | STARTUP through AUTH_SUCCESS |
| Version negotiation | Complete | v5 error frame uses highest supported |
| SUPPORTED response | Complete | Includes PROTOCOL_VERSIONS |
| Auth state machine | Complete | PLAIN + challenge/response |
| Event dispatch | Complete | TOPOLOGY/STATUS/SCHEMA_CHANGE |

## CQL Statement Coverage

| Statement Type | Parse | Plan | Execute |
|---------------|-------|------|---------|
| SELECT | Yes | Yes | Yes |
| SELECT JSON | Yes | Yes | Yes |
| INSERT | Yes | Yes | Yes |
| INSERT JSON | Yes | Yes | Yes |
| UPDATE | Yes | Yes | Yes |
| DELETE | Yes | Yes | Yes |
| BATCH | Yes | Yes | Yes |
| USE | Yes | Yes | Yes |
| TRUNCATE | Yes | Yes | Yes |
| CREATE/ALTER/DROP KEYSPACE | Yes | Yes | Yes |
| CREATE/ALTER/DROP TABLE | Yes | Yes | Yes |
| CREATE/DROP INDEX | Yes | Yes | Yes |
| CREATE/ALTER/DROP MV | Yes | Yes | Yes |
| CREATE/DROP TYPE | Yes | Yes | Yes |
| ALTER TYPE | Yes | Yes | Yes |
| CREATE/DROP FUNCTION | Yes | Yes | Metadata only |
| CREATE/DROP AGGREGATE | Yes | Yes | Metadata only |
| CREATE/DROP TRIGGER | Yes | Yes | Metadata + check |
| CREATE/ALTER/DROP ROLE | Yes | Yes | Yes |
| GRANT/REVOKE | Yes | Yes | Yes |
| LIST ROLES/PERMISSIONS | Yes | Yes | Yes |
| DESCRIBE | Yes | Yes | Yes |
| BEGIN TRANSACTION | Yes | Stub | Stub |

## CQL Function Coverage

| Category | Functions | Status |
|----------|-----------|--------|
| Time/UUID | `now()`, `uuid()`, `currentTimestamp()`, etc. | Complete |
| Token/Cast/Blob | `token()`, `cast()`, `blobAsX()`, `XAsBlob()` | Complete |
| Math | `abs()`, `ceil()`, `floor()`, `round()` | Complete |
| String | `length()` | Complete |
| JSON | `toJson()`, `fromJson()` | Complete |
| Vector Similarity | `similarity_cosine()`, etc. | Complete |
| Masking | `mask_default()`, `mask_null()`, `mask_inner()`, etc. | Complete |
| Aggregates | `count()`, `sum()`, `avg()`, `min()`, `max()` | Complete |
| WRITETIME/TTL | `writetime()`, `ttl()`, `maxwritetime()` | Complete |

## Prepared Statement Features

| Feature | Status | Notes |
|---------|--------|-------|
| PREPARE/EXECUTE cycle | Complete | MD5-based statement IDs |
| Bind metadata resolution | Complete | Column types from schema |
| Result metadata inference | Complete | Via query planning |
| `result_metadata_id` | Complete | MD5 of result metadata |
| METADATA_CHANGED flag | Complete | Schema version comparison |
| Schema-based invalidation | Complete | Real schema version tracking |
| Concurrent cache access | Complete | DashMap-based |
| Keyspace-scoped prepare | Complete | Per-keyspace invalidation |

## UDF/UDA/Trigger Status

| Feature | Status | Notes |
|---------|--------|-------|
| UDF Registry | Complete | Metadata + executor trait |
| UDF WASM Runtime | Stub | `udf-wasm` feature flag, wasmtime planned |
| UDF Native (Rust) | Complete | `UdfExecutor` trait |
| UDA Registry | Complete | SFUNC/FINALFUNC lifecycle |
| UDA Execution | Complete | `AggregateState` accumulator |
| Trigger Registry | Complete | Per-table registration |
| Trigger Execution | Partial | Check + event construction, no loaded impls |

## Driver Compatibility Notes

- **Statement IDs**: MD5 hash of query text, matching Java behavior.
- **Protocol versions**: v4 (stable) and v5 (beta, requires USE_BETA flag).
- **Compression**: LZ4 and Snappy, negotiated during STARTUP.
- **Custom payloads**: Decoded from request frames, encoded in responses.
- **Tracing**: UUID-based sessions, attached to response when TRACING flag set.
- **METADATA_CHANGED**: Drivers using `result_metadata_id` will get updated metadata.
- **Unprepared handling**: Returns UNPREPARED error with statement ID for re-prepare.

## Known Gaps

1. **UDF Java bytecode**: Cannot execute Java UDFs; requires WASM recompilation.
2. **Trigger loading**: Trigger implementations not dynamically loadable yet.
3. **Paging**: Query paging state serialization not fully wired.
4. **Lightweight Transactions**: IF conditions evaluated but Paxos consensus stubbed.
5. **CDC integration**: Change Data Capture not wired to commit log.
