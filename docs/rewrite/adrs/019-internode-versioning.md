# ADR-019: Internode Messaging Versioning

## Status

Accepted

## Context

Cassandra nodes may run different software versions during rolling upgrades.
The internode messaging protocol must handle version differences gracefully,
supporting both forward and backward compatibility.

## Decision

### Wire Format

Messages use a length-prefixed binary format:

```
┌──────────┬──────────┬──────────┬──────────┬──────────┬──────────┐
│ length   │ verb_id  │ msg_id   │ flags    │ ts_epoch │ body ... │
│ (u32)    │ (i32)    │ (u64)    │ (u32)    │ (i64)    │          │
└──────────┴──────────┴──────────┴──────────┴──────────┴──────────┘
```

### Flags

- `0x01` — RESPONSE
- `0x02` — FAILURE
- `0x04` — COMPRESSED (reserved)
- `0x08` — TRACING (carries session ID in payload)
- `0x10` — FORWARDING (message forwarded from another coordinator)

### Backpressure

Per-endpoint in-flight tracking via `DashMap<SocketAddr, usize>`. Sends are
rejected with `MessagingError::Backpressure` when the per-endpoint count
exceeds `max_inflight` (default: 1024).

### Version Negotiation (Planned)

On connection establishment, nodes will exchange a handshake message containing
their messaging protocol version. If incompatible, the connection is rejected.
Currently, all nodes are assumed to run the same version.

## Consequences

- New flags (TRACING, FORWARDING) are backward-compatible: old nodes ignore
  unknown flag bits.
- Backpressure prevents slow or unresponsive nodes from causing memory
  exhaustion on the sender.
- Unknown verb IDs cause the message to be dropped with a warning log.
- Future: connection pooling will replace connect-per-send for efficiency.

## References

- `org.apache.cassandra.net.MessagingService`
- `cassandra-messaging::frame`
- `cassandra-messaging::service`
