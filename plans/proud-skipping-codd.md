# Prompt 04 — Marshal/Type System, Serializers, Term Binding, Type Compatibility

## Context

The Cassandra Rust rewrite has a partial type system in `cassandra-types` using a flat `CqlType` enum with basic serialization (`codec.rs`) and comparison (`comparator.rs`). The gap analysis identifies the following critical missing pieces:

- **CQL Type System** (P0, partial): "Missing frozen deep-nesting edge cases, composite types, DynamicCompositeType"
- **CQL Terms & Literals** (P0, partial): "Missing function call terms, type casts, collection constructors"
- **ByteComparable**: Listed as TODO in cassandra-common — no implementation exists
- **TypeParser**: No Java class-name format parser (needed for sstable metadata and system_schema)
- **ValueAccessor abstraction**: Not present — typed operations work directly on `&[u8]`
- **Duration serializer**: Uses fixed-width encoding instead of Java's vint format
- **Decimal/Varint**: Stored as raw `Vec<u8>` without arithmetic or proper comparison normalization

This phase creates a `marshal/` module within `cassandra-types` that provides the equivalent of Java's `org.apache.cassandra.db.marshal.*` and `org.apache.cassandra.serializers.*`, plus term binding in `cassandra-cql`.

## Architecture Decision

**Extend the enum model, don't replace it.** The existing `CqlType` enum stays as the type descriptor. New capabilities are added via:
1. A `marshal/` submodule with traits (`AbstractType`, `ValueAccessor`, `ByteComparable`)
2. Trait implementations for `CqlType` that dispatch to per-type logic
3. New standalone types where needed (`CompositeType`, `Varint`, `Decimal`)

This preserves the zero-cost dispatch and exhaustive matching benefits documented in `lib.rs`.

## Work Units

### Unit 1: Marshal Module Scaffold + ValueAccessor Trait
**Files:** Create `rust/crates/cassandra-types/src/marshal/mod.rs`, `marshal/value_accessor.rs`. Modify `lib.rs` (add `pub mod marshal;`).
**Description:** Create the `marshal/` module root and the `ValueAccessor<V>` trait providing generic byte-value abstraction over `&[u8]`, `Vec<u8>`, `bytes::Bytes`. Methods: `as_slice()`, `len()`, `is_empty()`, `slice()`, `get_byte()`, `get_i32()`, `get_i64()`, `compare()`. Implement for `&[u8]` and `Vec<u8>`. Add `MarshalError` error type.

### Unit 2: Vint Encoding Utilities
**Files:** Create `marshal/vint.rs`. Modify `marshal/mod.rs` (add `pub mod vint;`).
**Description:** Variable-length integer encoding/decoding matching Java's `VIntCoding`. Functions: `write_vint(i64)`, `read_vint(&[u8])`, `write_unsigned_vint(u64)`, `read_unsigned_vint(&[u8])`, `compute_vint_size(i64)`. Uses zigzag encoding. Critical for Duration serializer and sstable on-disk format.

### Unit 3: Duration Vint Serializer
**Files:** Create `marshal/duration.rs`. Modify `marshal/mod.rs`.
**Description:** Correct Duration serialization using vint encoding to match Java's `DurationSerializer`. Current codec.rs uses fixed 4+4+8 bytes; Java uses zigzag vints for months/days/nanoseconds. Includes validation (same-sign rule), comparison, and round-trip tests. Self-contained — includes inline vint helpers if Unit 2 not yet merged.

### Unit 4: Arbitrary-Precision Varint
**Files:** Create `marshal/varint.rs`. Modify `marshal/mod.rs`.
**Description:** `Varint` type wrapping two's-complement `Vec<u8>` representation. Provides `from_i64()`, `from_bytes()`, `to_i64()`, `to_string()`, arithmetic (add, negate), proper `Ord` implementation, validation (rejects non-minimal encodings per Java's IntegerType). Tests for edge cases: 0, -1, i64::MIN, large values.

### Unit 5: Arbitrary-Precision Decimal
**Files:** Create `marshal/decimal.rs`. Modify `marshal/mod.rs`.
**Description:** `Decimal` type wrapping `{scale: i32, unscaled: Vec<u8>}`. Provides `from_string()`, `to_string()`, normalized comparison (1.0 == 1.00), serialization matching Java's `DecimalSerializer` (4-byte scale + varint unscaled). Can operate on raw bytes independently of Unit 4.

### Unit 6: TypeParser (Java Class-Name Format)
**Files:** Create `marshal/type_parser.rs`. Modify `marshal/mod.rs`.
**Description:** Parses Java-style type strings from system tables and sstable metadata into `CqlType`. Handles: `"org.apache.cassandra.db.marshal.UTF8Type"`, `"ListType(UTF8Type)"`, `"CompositeType(UTF8Type,Int32Type)"`, `"ReversedType(TimestampType)"`, `"FrozenType(MapType(UTF8Type,Int32Type))"`, UDT syntax. Recursive descent parser with comprehensive error messages.

### Unit 7: CompositeType and DynamicCompositeType
**Files:** Create `marshal/composite.rs`. Modify `marshal/mod.rs`.
**Description:** Entirely missing types. `CompositeType`: multi-column key encoding with `[2-byte-len][value][EOC-byte]` per component. EOC handling: 0x00=equal, 0xFF(-1)=less, 0x01=greater for range queries. `DynamicCompositeType`: inline type identifiers per component. Both with serialize, deserialize, compare, builder API. Uses existing `compare_bytes()` from `comparator.rs`.

### Unit 8: ByteComparable Framework
**Files:** Create `marshal/byte_comparable.rs`. Modify `marshal/mod.rs`.
**Description:** `ByteComparable` trait + `ByteSource` abstraction from Java's `o.a.c.utils.bytecomparable`. Encodes typed values into byte sequences where unsigned comparison yields correct type-aware ordering. Implementations for all `CqlType` variants: signed ints XOR sign bit, variable-length with escape terminators, float NaN handling, collection/tuple recursive composition. Versions: `Legacy` and `OSS50`.

### Unit 9: AbstractType Trait (Unified Type Operations)
**Files:** Create `marshal/abstract_type.rs`. Modify `marshal/mod.rs`.
**Description:** Trait that unifies per-type operations: `compare()`, `validate()`, `serialize()`, `deserialize()`, `is_compatible_with()`, `is_value_compatible_with()`, `cql_name()`, `is_reversed()`, `freeze()`/`unfreeze()`. Implement for `CqlType` by dispatching to existing `comparator.rs` and `codec.rs` functions. Adds type compatibility matrix (e.g., Int value-compatible with Bigint, Varchar with Ascii).

### Unit 10: Type Coercion and Assignment Rules
**Files:** Create `marshal/coercion.rs`. Modify `marshal/mod.rs`.
**Description:** CQL type assignment rules matching Java's `AssignmentTestable`. `is_assignable_from(target, source) -> AssignmentResult {ExactMatch, Coercible, NotAssignable}`. Rules: int literals → any numeric, text → ascii, frozen ↔ unfrozen collections, tuple widening. Needed by query processor for INSERT/UPDATE validation.

### Unit 11: Term Binding (AST Term → CqlValue)
**Files:** Create `rust/crates/cassandra-cql/src/term_binding.rs`. Modify `cassandra-cql/src/lib.rs`.
**Description:** Resolves parsed AST `Term` values into typed `CqlValue` given target `CqlType`. Covers: integer literals → numeric types (with range check), string literals → text/timestamp/date/uuid/inet (with format parsing), hex blobs, collection/map/tuple literals, TypeHint application, BindMarker placeholder tracking. Returns `BindError` for type mismatches. Includes minimal inline coercion rules.

### Unit 12: Round-Trip and Diff Tests
**Files:** Create `rust/crates/cassandra-diff-tests/tests/marshal_round_trip.rs`, `rust/diff-tests/golden/types/marshal_test_vectors.json`.
**Description:** Comprehensive test suite: (1) Round-trip encode/decode for every CqlType variant including edge cases (min/max, empty, NaN, epoch boundaries). (2) Golden test vectors from Java oracle for byte-exact serialization verification. (3) Comparison ordering tests (verify sort order matches Java). (4) Collection nesting tests (frozen<list<frozen<set<int>>>>). Tests use existing `cassandra-diff-tests` infrastructure.

### Unit 13: ADR Documentation
**Files:** Create `docs/rewrite/adr-018-marshal-type-system.md`.
**Description:** Architecture Decision Record documenting: the enum+trait design (vs. trait-object hierarchy), ValueAccessor design, ByteComparable version compatibility, CompositeType encoding format, type compatibility matrix, gap analysis citations, and continuation plan for BTI/SAI integration.

## E2E Verification Recipe

This is a library/framework change with no UI or API endpoints. Verification:
1. `cd rust && cargo build --workspace` — must compile clean
2. `cd rust && cargo test --workspace` — all tests pass (unit + integration)
3. `cd rust && cargo test -p cassandra-types` — type-specific tests
4. `cd rust && cargo test -p cassandra-cql` — CQL term binding tests
5. `cd rust && cargo test -p cassandra-diff-tests` — diff/golden tests

No e2e browser/CLI verification needed — unit + integration tests are sufficient for a type system library.

## Worker Instructions Template

Each worker receives:
- The overall goal (marshal/type system implementation)
- Its specific unit task (title, file list, description)
- Codebase conventions: Apache 2.0 license header, `//!` doc comments with `## Java Oracle` section referencing the corresponding Java class, exhaustive match arms, `thiserror` for errors, `#[cfg(test)] mod tests` inline
- Existing code to reference: `native.rs` (CqlType enum), `codec.rs` (CqlValue + serialize/deserialize), `comparator.rs` (compare_bytes), `collections.rs` (ListType/SetType/MapType/TupleType)
- Key dependency: `cassandra-common` for shared error types; `byteorder` for big-endian; `bytes` for byte buffers

## Merge Order

**Wave 1 (no deps, parallel):** Units 1, 2, 4, 6, 7, 8, 9, 10, 12, 13
**Wave 2 (soft deps):** Units 3 (needs vint from 2), 5 (uses varint from 4), 11 (uses coercion from 10)

All Wave 1 units create new files only. The single shared touchpoint is `marshal/mod.rs` — each adds one `pub mod` line, trivially resolvable.

## Files Summary

| File | Action | Unit |
|------|--------|------|
| `cassandra-types/src/lib.rs` | Add `pub mod marshal;` | 1 |
| `cassandra-types/src/marshal/mod.rs` | Create (module root) | 1 (extended by all) |
| `cassandra-types/src/marshal/value_accessor.rs` | Create | 1 |
| `cassandra-types/src/marshal/vint.rs` | Create | 2 |
| `cassandra-types/src/marshal/duration.rs` | Create | 3 |
| `cassandra-types/src/marshal/varint.rs` | Create | 4 |
| `cassandra-types/src/marshal/decimal.rs` | Create | 5 |
| `cassandra-types/src/marshal/type_parser.rs` | Create | 6 |
| `cassandra-types/src/marshal/composite.rs` | Create | 7 |
| `cassandra-types/src/marshal/byte_comparable.rs` | Create | 8 |
| `cassandra-types/src/marshal/abstract_type.rs` | Create | 9 |
| `cassandra-types/src/marshal/coercion.rs` | Create | 10 |
| `cassandra-cql/src/lib.rs` | Add `pub mod term_binding;` | 11 |
| `cassandra-cql/src/term_binding.rs` | Create | 11 |
| `cassandra-diff-tests/tests/marshal_round_trip.rs` | Create | 12 |
| `diff-tests/golden/types/marshal_test_vectors.json` | Create | 12 |
| `docs/rewrite/adr-018-marshal-type-system.md` | Create | 13 |
