# ADR-018: Gossip ↔ TCM Coexistence

## Status

Accepted

## Context

Apache Cassandra's control plane is evolving from pure gossip-based membership
to Transactional Cluster Metadata (TCM). During the transition period, both
mechanisms must coexist. The Rust rewrite needs to support this coexistence
cleanly.

## Decision

We introduce three control plane modes via `ControlPlaneMode`:

1. **GossipOnly** — Legacy mode. All metadata (topology, schema, status) is
   propagated via the gossip protocol. This is the default.

2. **TcmWithGossipBridge** — Hybrid mode. TCM is authoritative for topology and
   schema changes (committed via the metadata log). Gossip carries health/status
   information and the current TCM epoch so peers can detect they're behind and
   fetch missing log entries.

3. **TcmOnly** — Future target. Gossip is completely disabled for metadata;
   only TCM is used.

The `ControlPlaneBridge` dispatches operations to the appropriate subsystem
based on the configured mode.

## Consequences

- Operators can upgrade incrementally by switching modes via config.
- The gossip layer remains fully functional for health monitoring in all modes.
- TCM epoch is disseminated via a new `ApplicationState::NetVersion` in gossip.
- Schema changes go through TCM commit in hybrid/TCM-only modes, ensuring
  linearizable ordering.
- Feature-flagged via Cargo features to allow builds without TCM support.

## References

- `org.apache.cassandra.tcm.*`
- `cassandra-cluster-metadata::tcm::bridge`
