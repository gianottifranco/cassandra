# ADR-025: JMX Replacement Strategy

## Status

Accepted — 2026-03-17

## Context

Java Cassandra exposes all operational controls via JMX (Java Management
Extensions). JMX MBeans power nodetool, OpsCenter, and third-party monitoring
tools. The Rust rewrite has no JVM and therefore no JMX runtime.

We need a replacement that preserves operational parity without introducing a
JVM dependency.

## Decision

### HTTP Admin API replaces JMX

All JMX MBean operations are exposed as HTTP endpoints in the `cassandra-admin`
crate. The API surface is organized by domain:

| JMX Domain                | HTTP Path Prefix          | Crate               |
|---------------------------|---------------------------|----------------------|
| `o.a.c.db:type=Storage`  | `/api/v1/storage`         | cassandra-admin      |
| `o.a.c.db:type=Compaction`| `/api/v1/compaction`     | cassandra-admin      |
| `o.a.c.net:type=Gossip`  | `/api/v1/cluster`         | cassandra-admin      |
| `o.a.c.metrics:*`        | `/metrics` (Prometheus)   | cassandra-admin      |
| `o.a.c.auth:*`           | `/api/v1/auth`            | cassandra-admin      |

### CLI tool (`cassandra-tools`)

The `cassandra-tools` crate provides a `nodetool`-compatible CLI that calls the
HTTP Admin API instead of JMX. Command names and output format match the Java
nodetool where possible.

### Metrics

- **Prometheus-native**: Metrics are exposed at `/metrics` in Prometheus
  exposition format. No JMX-to-Prometheus bridge needed.
- **Metric naming**: Follows the pattern `cassandra_<subsystem>_<metric>` to
  align with the community Prometheus exporter naming convention.
- **JMX metric mapping**: A reference table mapping every Java JMX metric name
  to its Prometheus equivalent is maintained in `docs/rewrite/tracking/`.

### Migration path for operators

1. Replace `nodetool` invocations with `cassandra-tools` (same subcommands).
2. Point Prometheus scrape config at `<host>:9180/metrics`.
3. JMX-only tools (OpsCenter, etc.) require the community to build HTTP adapters.

## Consequences

1. No JVM dependency for operations.
2. Third-party tools relying on JMX MBeans need adapters or replacement.
3. Monitoring dashboards (Grafana) that use JMX metrics need updated queries
   targeting Prometheus metric names.
4. The HTTP Admin API becomes a public interface — backward compatibility
   policy applies from GA onward.
