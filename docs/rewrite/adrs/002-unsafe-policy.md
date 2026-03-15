# ADR-002: Unsafe Code Policy

- **Status**: Accepted
- **Date**: 2026-03-15
- **Context**: Cassandra Rust rewrite baseline freeze

## Context

A high-performance database will inevitably need some `unsafe` blocks for
memory-mapped I/O, zero-copy deserialization, custom allocators, and
potentially SIMD-accelerated operations. Without a clear policy, `unsafe`
proliferates and undermines Rust's safety guarantees.

## Decision

### Rules

1. **Minimize**: prefer safe abstractions. `unsafe` is a last resort, not a
   convenience shortcut.
2. **Isolate**: all `unsafe` must live behind a safe public API. No `unsafe`
   in application-level logic.
3. **Justify**: every `unsafe` block must have a `// SAFETY:` comment
   explaining:
   - What invariant the compiler cannot verify.
   - Why this invariant holds.
   - What would happen if the invariant were violated.
4. **Test**: every module containing `unsafe` must have:
   - Unit tests that exercise the safe API boundary.
   - Miri tests where feasible (no FFI, no I/O).
   - Property-based tests for serialization/deserialization.
5. **Audit**: `unsafe` blocks are tracked via `cargo geiger` in CI.
   Increases require review justification.
6. **Prefer crates**: use battle-tested crates (`bytes`, `memmap2`,
   `crossbeam`) over hand-rolled `unsafe`.

### Acceptable Uses

| Use Case               | Allowed | Notes                             |
|------------------------|---------|-----------------------------------|
| mmap I/O               | Yes     | Via `memmap2` crate               |
| Zero-copy parsing      | Yes     | Must validate before deref        |
| Custom allocator        | Yes     | Arena allocators for SSTable reads |
| FFI (JNI bridge)       | Yes     | Isolated behind safe wrapper      |
| `transmute`            | Avoid   | Use `bytemuck` or `zerocopy` crate|
| `static mut`           | No      | Use `OnceLock` / `LazyLock`       |
| Inline assembly        | No      | Use intrinsics or SIMD crates     |

## Consequences

- Slightly slower initial development for hot paths.
- Much higher confidence in memory safety.
- CI pipeline enforces `unsafe` accounting.
