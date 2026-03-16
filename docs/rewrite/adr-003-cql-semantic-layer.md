# ADR-003: CQL Semantic Layer

## Status
Accepted

## Context
The CQL pipeline (lexer → parser → planner → executor) lacked semantic validation between parsing and execution. The parser produced a full AST but the planner passed it through with zero validation of WHERE clauses, SELECT columns, IF conditions, or built-in functions. This mirrored Java's `cql3/restrictions`, `cql3/selection`, `cql3/conditions`, and `cql3/functions` packages (91+ Java files total).

## Decision

### Restrictions Framework
We use a **classify-then-validate** pattern: `StatementRestrictions::build()` is the single entry point that classifies each WHERE relation by column kind (partition key, clustering, regular, static), then delegates to specialized validators:
- `PartitionKeyRestrictions`: validates all PK columns have EQ/IN or token-based restriction
- `ClusteringColumnRestrictions`: validates contiguous prefix rule, slice only on last column
- `type_compat`: validates operator-type compatibility (CONTAINS requires collection, etc.)
- Non-key restrictions require secondary index or ALLOW FILTERING

This matches Java's `StatementRestrictions` class hierarchy but avoids the deep inheritance tree.

### Function Registry
Built-in functions use a `CqlFunction` trait with `execute(&[Option<&[u8]>]) → Option<Vec<u8>>` operating on raw CQL bytes. The `FunctionRegistry` uses `DashMap` for concurrent access with overload resolution by name + argument types. Aggregate functions extend with `init_state`/`accumulate`/`finalize`.

Functions are organized by category:
- `time_uuid`: now(), currentTimestamp(), toTimestamp(), uuid(), minTimeuuid(), etc.
- `token_cast_blob`: token() with Murmur3, cast() for type conversions, typeAsBlob/blobAsType
- `math_json`: abs/ceil/floor/round, length(), toJson/fromJson
- `aggregates`: count/sum/avg/min/max with accumulator pattern

### Selection Pipeline
`SelectorEvaluator` takes AST `Selector` + function registry + row data and evaluates column lookups, function calls, and aliases. Post-processing stages (DISTINCT, ORDER BY, LIMIT) are composable functions that transform `Vec<Vec<Option<Vec<u8>>>>`.

### Condition Evaluation
`ConditionEvaluator` handles LWT IF clauses: IF EXISTS, IF NOT EXISTS, and column-level conditions. Returns `ConditionResult { applied, current_values }` for the `[applied]` column in result sets.

### Planner Integration
The planner now validates SELECT column names against table metadata and runs `StatementRestrictions::build()` for WHERE clauses. Validation is optional (skipped when table metadata is unavailable) to maintain backward compatibility.

## Consequences
- Invalid WHERE clauses now produce descriptive error messages matching Java behavior
- Column name typos in SELECT are caught at plan time rather than execution time
- Built-in functions are available for selector evaluation
- The restriction framework enables future query optimization (partition pruning, index selection)
- New statement types (ALTER TYPE, ALTER MATERIALIZED VIEW, DESCRIBE) parse correctly but return "not yet implemented" from the planner
