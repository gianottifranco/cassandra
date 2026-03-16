# Plan: Prompt 01 — Native Transport Server Enhancement

## Context

The gap analysis identifies the native transport server as a P0 gap: while a basic TCP server exists (`cassandra-server/src/server.rs`), it lacks connection limits, backpressure, a proper dispatcher, graceful shutdown, idle timeouts, metrics, and integration tests. The existing server handles connections inline with no resource controls — a single misbehaving client could exhaust resources.

This plan closes these gaps by enhancing the existing server with production-grade features matching the Java `Server.java` / `Dispatcher.java` / `NativeTransportService.java` architecture.

## Research Findings

**What already exists (no need to rewrite):**
- TCP listener with accept loop (`server.rs:58-101`)
- Frame codec with tokio Decoder/Encoder (`native-protocol/frame.rs`)
- Connection state machine: New→Authenticating→Ready (`native-protocol/connection.rs`)
- Auth framework: AllowAllAuthenticator + PasswordAuthenticator (`native-protocol/auth.rs`, `security/auth.rs`)
- Event dispatcher with broadcast channel (`native-protocol/event_dispatcher.rs`)
- TLS via rustls with ReloadableTlsAcceptor (`security/tls.rs`)
- Config loading from YAML (`config/config.rs`)
- Daemon bootstrap: tracing, storage, schema, auth, FQL, audit, executor (`server/main.rs`)

**What's missing (gaps to close):**
1. No connection limits (global or per-IP)
2. No backpressure or memory limits per client
3. No dispatcher — queries processed inline on connection task
4. No graceful shutdown — `run()` loops forever
5. No signal handling (SIGTERM/SIGINT)
6. No idle timeout
7. No connection metrics
8. No ClientState/QueryState types for Prompt 02
9. Missing config keys: `native_transport_max_threads`, `native_transport_max_concurrent_connections`, `native_transport_max_concurrent_connections_per_ip`, `native_transport_idle_timeout`, `native_transport_timeout`
10. No integration tests

## Work Units

### Unit 1: Config — native transport settings
**Size:** S | **Files:** `cassandra-config/src/config.rs`

Add to `CassandraConfig`:
- `native_transport_max_threads: Option<u32>` (default 128)
- `native_transport_max_concurrent_connections: Option<u32>` (default None = unlimited)
- `native_transport_max_concurrent_connections_per_ip: Option<u32>` (default None = unlimited)
- `native_transport_idle_timeout: Option<Duration>` (default None)
- `native_transport_timeout: Option<Duration>` (default 10s)
- `native_transport_max_request_queue_size: Option<u32>` (default 4096)

Add default functions and deserialization tests. Follow existing patterns in `defaults` module.

---

### Unit 2: ClientState / QueryState placeholder types
**Size:** S | **Files:** `cassandra-native-protocol/src/client_state.rs` (NEW), `cassandra-native-protocol/src/lib.rs` (add 1 line)

Create types that Prompt 02 can extend:
```rust
pub struct ClientState {
    pub remote_address: SocketAddr,
    pub authenticated_user: Option<String>,
    pub keyspace: Option<String>,
    pub driver_name: Option<String>,
    pub driver_version: Option<String>,
}

pub struct QueryState {
    pub client_state: Arc<ClientState>,
    pub consistency: Consistency,
    pub timestamp: Option<i64>,
    pub paging_state: Option<Vec<u8>>,
    pub page_size: Option<i32>,
}
```

Add `pub mod client_state;` to `lib.rs`. Include unit tests.

---

### Unit 3: ConnectionTracker module
**Size:** M | **Files:** `cassandra-native-protocol/src/connection_tracker.rs` (NEW), `cassandra-native-protocol/src/lib.rs` (add 1 line)

Standalone data structure (no server.rs changes):
- `ConnectionTracker::new(max_global: Option<u32>, max_per_ip: Option<u32>)`
- `fn try_acquire(&self, addr: IpAddr) -> Result<ConnectionPermit, ConnectionLimitError>` — RAII permit
- `fn active_count(&self) -> usize`
- `fn active_count_for_ip(&self, addr: IpAddr) -> usize`
- Uses `parking_lot::Mutex<HashMap<IpAddr, usize>>` + `AtomicUsize` for global count
- `ConnectionPermit` implements `Drop` to auto-release

Add `pub mod connection_tracker;` to `lib.rs`. Include unit tests for limits and RAII drop.

---

### Unit 4: Integration tests (against existing server)
**Size:** M | **Files:** `cassandra-server/tests/native_protocol_integration.rs` (NEW)

Tests that work against the CURRENT server (before enhancements):
1. TCP connect → send OPTIONS → receive SUPPORTED
2. TCP connect → send STARTUP → receive READY (AllowAll)
3. TCP connect → send STARTUP + AUTH_RESPONSE → receive AUTH_SUCCESS
4. TCP connect → send invalid version → receive protocol error
5. Send message in wrong state → receive error
6. Clean disconnect (client closes, server doesn't crash)

Each test boots a NativeServer on port 0 (random) with a real StorageEngine in a tempdir. Uses raw TCP + FrameCodec for protocol-level validation.

---

### Unit 5: Server enhancement — shutdown, limits, dispatcher, idle timeout, metrics
**Size:** L | **Files:** `cassandra-server/src/server.rs` (refactor), `cassandra-server/src/main.rs` (refactor), `cassandra-server/src/dispatcher.rs` (NEW), `cassandra-server/Cargo.toml` (add deps)

**This is the main integration unit.** Changes:

**server.rs:**
- `ServerConfig` gains: `max_connections`, `max_connections_per_ip`, `idle_timeout_ms`, `request_timeout_ms`, `max_request_queue_size`
- `NativeServer::run()` accepts a `CancellationToken` for shutdown; uses `tokio::select!` between accept and shutdown
- Accept loop checks connection limits (inline ConnectionTracker or import from Unit 3 if merged)
- `handle_connection()` wraps read loop in `tokio::time::timeout` for idle detection
- New `Dispatcher` struct with configurable concurrency: uses `tokio::sync::Semaphore` for request queue depth
- Metrics: `Arc<ServerMetrics>` with atomics for connections_accepted, connections_active, connections_rejected, requests_dispatched, requests_completed
- JoinSet to track spawned connection tasks for graceful drain

**main.rs:**
- Signal handling: `tokio::signal::ctrl_c()` on all platforms, SIGTERM on Unix
- Reads new config keys from `CassandraConfig` (uses defaults if fields missing — no hard dependency on Unit 1)
- Propagates `CancellationToken` to server
- Logs graceful shutdown progress

**dispatcher.rs:**
- `Dispatcher::new(max_concurrent: usize)` with Semaphore-based admission control
- `async fn dispatch(request, executor, ctx) -> Response` acquires permit, processes, releases
- Separate auth vs request paths (following Java's CASSANDRA-17812 pattern)
- Request timeout enforcement

---

### Unit 6: ADR documentation
**Size:** S | **Files:** `docs/rewrite/adrs/022-native-transport-server.md` (NEW)

Documents:
- Architecture decision: Tokio-based async server (vs. Netty in Java)
- Connection lifecycle state machine
- Backpressure strategy (semaphore-based vs. Java's permit model)
- Dispatcher threading model
- Gap closure status and residual gaps
- Compatibility notes with Java baseline

---

## E2E Test Recipe

Workers should verify their changes with:

1. `cd rust && cargo build -p cassandra-server` — must compile
2. `cd rust && cargo test -p cassandra-server` — unit tests pass
3. `cd rust && cargo test -p cassandra-native-protocol` — protocol tests pass
4. `cd rust && cargo test -p cassandra-config` — config tests pass (for Unit 1)
5. For Unit 4 (integration tests): `cd rust && cargo test -p cassandra-server --test native_protocol_integration` — all integration tests pass

Skip browser/UI e2e — this is a TCP server with no UI. The integration tests in Unit 4 serve as the e2e verification (raw TCP connect + protocol exchange).

## Conventions

- Rust edition 2024, workspace deps via `{ workspace = true }`
- `parking_lot` for mutexes, `dashmap` available but not required
- `tracing` for logging (not `log`)
- `thiserror` for error types, `anyhow` for main/binary error handling
- Tests in same file (`#[cfg(test)] mod tests`) for unit tests, `tests/` dir for integration
- Doc comments reference Java Oracle classes
- License header: Apache 2.0 (match existing files)

## Worker Instructions Template

```
After you finish implementing the change:
1. **Simplify** — Invoke the `Skill` tool with `skill: "simplify"` to review and clean up your changes.
2. **Run unit tests** — Run the project's test suite (check for package.json scripts, Makefile targets, or common commands like `npm test`, `bun test`, `pytest`, `go test`). If tests fail, fix them.
3. **Test end-to-end** — Follow the e2e test recipe from the coordinator's prompt (below). If the recipe says to skip e2e for this unit, skip it.
4. **Commit and push** — Commit all changes with a clear message, push the branch, and create a PR with `gh pr create`. Use a descriptive title. If `gh` is not available or the push fails, note it in your final message.
5. **Report** — End with a single line: `PR: <url>` so the coordinator can track it. If no PR was created, end with `PR: none — <reason>`.
```
