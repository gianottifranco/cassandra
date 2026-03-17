# ADR-027: UDF/UDA WASM Sandbox Strategy

## Status

Accepted

## Context

Apache Cassandra (Java) supports User-Defined Functions (UDFs) and User-Defined
Aggregates (UDAs) written in Java (and formerly JavaScript via Nashorn). UDFs
execute user-supplied bytecode inside the database process itself, creating
significant security and resource isolation concerns. The Java implementation
uses a combination of:

- A custom `SecurityManager` to restrict filesystem, network, and reflection access
- Thread-based CPU timeouts to kill runaway functions
- Heap allocation tracking (approximate) to limit memory

This approach has known limitations: the SecurityManager is deprecated in modern
Java, thread-based timeouts are non-deterministic, and JVM-level isolation is
coarse-grained.

For the Rust rewrite, we need a UDF execution strategy that provides:

1. **Language-agnostic execution** - not tied to a single source language
2. **Strong sandboxing** - memory, CPU, and syscall isolation
3. **Deterministic resource limits** - not dependent on OS thread scheduling
4. **Low integration overhead** - embeddable in the server process

## Decision

### Use WebAssembly (wasmtime) for UDF Sandboxing

We adopt WASM as the UDF execution backend, using the `wasmtime` runtime:

| Concern | Approach |
|---------|----------|
| **Sandbox** | WASM modules have no ambient capabilities; all host access must be explicitly granted |
| **Memory limits** | wasmtime `Store` config: `memory_limit(max_bytes)` caps linear memory |
| **CPU limits** | Fuel metering: `consume_fuel(true)` + `fuel_remaining()` provides deterministic instruction budgets |
| **Language support** | Any language compiling to WASM (Rust, C, Go, AssemblyScript, etc.) |
| **Java UDF compat** | Java UDFs would need recompilation to WASM (e.g., via TeaVM or GraalWasm); this is a known migration cost |

### Feature-Gated Compilation

WASM support is behind the `udf-wasm` Cargo feature flag:

- **Default (disabled):** `WasmUdfExecutor::new()` returns an error. Zero runtime
  overhead, no wasmtime dependency compiled.
- **Enabled (`--features udf-wasm`):** Full WASM compilation, instantiation, and
  execution via wasmtime. The `wasmtime` crate is only linked when opted in.

This keeps the default binary lean while allowing operators to enable UDF support
when needed.

### Native Rust UDFs

As a complementary path, we support native Rust UDFs via the `UdfExecutor` trait.
Server operators can register Rust closures at compile time for
performance-critical functions. These bypass the WASM sandbox but require
recompilation.

### UDA Wiring

UDAs compose existing UDFs (SFUNC and optionally FINALFUNC) into an aggregation
pipeline. The `UdaRegistry` stores metadata; at execution time, SFUNC and
FINALFUNC are resolved from the `UdfRegistry` and wired into an `AggregateState`
instance that processes rows.

### Trigger Execution Model

Triggers are checked before mutation application. The `TriggerRegistry` is
consulted for each INSERT/UPDATE/DELETE; if triggers exist for the target table,
a `MutationEvent` is constructed and passed to the trigger chain. Trigger
implementations follow the `Trigger` trait (analogous to Java's `ITrigger`).

## Consequences

### Positive

- **Strong isolation:** WASM provides memory-safe, capability-based sandboxing
  without relying on OS-level mechanisms.
- **Deterministic limits:** Fuel metering gives precise instruction budgets,
  unlike thread-based timeouts.
- **Polyglot:** Any WASM-targeting language can be used for UDFs.
- **Zero cost when disabled:** Feature flag means no runtime or compile-time
  overhead for deployments that don't use UDFs.
- **Upgradeable:** As wasmtime matures (component model, WASI preview 2), we
  can expand capabilities without changing the user-facing API.

### Negative

- **Java UDF migration:** Existing Java UDFs cannot run as-is; they must be
  recompiled to WASM or rewritten. This is a known compatibility gap.
- **wasmtime dependency:** When enabled, wasmtime is a significant dependency
  (~10MB+ binary size increase, non-trivial compile time).
- **Performance ceiling:** WASM execution is slower than native Rust for
  compute-heavy UDFs. The native Rust UDF path exists for this case.
- **Debugging complexity:** WASM stack traces are less readable than native
  stack traces; source maps help but add tooling requirements.

## Alternatives Considered

### 1. Lua (mlua/rlua)

Lightweight and embeddable, but single-threaded by design and lacks the
standardized sandbox model of WASM. Memory limits require custom allocators.

### 2. JavaScript (V8 via rusty_v8)

Good sandbox model, but V8 is an enormous dependency (~50MB), has complex
build requirements, and ties us to a single language.

### 3. No UDF Support

Simplest option, but breaks compatibility with existing Cassandra workloads
that rely on UDFs. The feature-gated approach gives us the best of both
worlds: zero cost for those who don't need it, full support for those who do.
