# ADR-013: Observability & Administration Architecture

**Status:** Accepted  
**Date:** 2026-03-15  
**Context:** Phase 4 — Operations & Security

## Decision

### Metrics: Prometheus-native

Java Cassandra uses Dropwizard Metrics exposed via JMX. Instead of bridging:

1. **Prometheus crate** — register metrics directly.
2. **Text exposition** — `/metrics` endpoint serves Prometheus scrape format.
3. **Metric naming** — `cassandra_` prefix, snake_case, units in name:
   - `cassandra_client_request_latency_seconds` (histogram)
   - `cassandra_read_count_total` (counter)
   - `cassandra_live_sstable_count` (gauge)

This eliminates the JMX dependency and enables modern monitoring stacks directly.

### Virtual Tables

- `VirtualTable` trait: `name()`, `columns()`, `rows()`.
- Built-in tables in `system_views` keyspace:
  - `local` — node identity (host_id, DC, rack, version)
  - `settings` — server configuration values
  - `thread_pools` — thread pool statistics
  - `sstable_tasks` — active compaction/streaming tasks
  - `clients` — connected native protocol clients
- Extensible via `VirtualTableRegistry`.

### Admin HTTP API

- Lightweight Hyper HTTP server on port 9090 (configurable).
- Routes:
  - `GET /metrics` — Prometheus text
  - `GET /health` — JSON health status
  - `GET /api/v1/operations` — active operations
  - `GET /api/v1/virtual/{ks}/{table}` — virtual table data
- Replaces JMX for administrative access.

### Admin CLI (`cassandra-tools`)

- Clap-based CLI matching nodetool interface:
  - `status`, `info`, `version`, `ring`, `describecluster`
  - `snapshot`, `listsnapshots`, `clearsnapshot`
  - `compact`, `repair`, `cleanup`
  - `enableauditlog`, `disableauditlog`
  - `getlogginglevels`, `setlogginglevel`
  - `sstabledump`, `sstablemetadata`
- Connects to admin HTTP API for remote operations.

## Consequences

- JMX is not supported (intentional — replaced by HTTP API + Prometheus).
- Existing monitoring that depends on JMX MBeans must migrate to Prometheus scraping.
- The HTTP admin API has no authentication yet (should be added before production).

## Gaps

| Gap | Impact | Plan |
|-----|--------|------|
| Admin API authentication | Security risk | Add token/TLS client cert based auth |
| Real-time virtual table updates | Stale data | Wire virtual tables to actual subsystem state |
| SSTable tools implementation | Offline analysis | Parse SSTable format via cassandra-storage |
| JMX compatibility shim | Legacy tooling | Low priority — teams should migrate to Prometheus |
