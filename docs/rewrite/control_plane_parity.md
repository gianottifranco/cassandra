# Control Plane — Parity with Java Baseline

> Tracks the Rust implementation of Cassandra's control plane against the
> Java baseline. Updated after Phase 17 implementation.

## Status: ✅ Core Complete / 🔧 Hardening Done

## Completed Features

| Feature | Java Class | Rust Module | Tests |
|---------|-----------|-------------|-------|
| Murmur3Partitioner | `Murmur3Partitioner` | `cassandra_common::partitioner` | ✅ |
| RandomPartitioner | `RandomPartitioner` | `cassandra_common::partitioner` | ✅ |
| ByteOrderedPartitioner | `ByteOrderedPartitioner` | `cassandra_common::partitioner` | ✅ |
| Token / TokenRange | `Token`, `Range<Token>` | `cassandra_common::token` | ✅ |
| TokenRing (pending ranges) | `TokenMetadata` | `cassandra_cluster_metadata::ring` | ✅ 8 tests |
| Write replicas (natural+pending) | `StorageProxy` | `ring::write_replicas()` | ✅ |
| SimpleSnitch | `SimpleSnitch` | `cassandra_cluster_metadata::snitch` | ✅ |
| PropertyFileSnitch | `PropertyFileSnitch` | `snitch::PropertyFileSnitch` | ✅ |
| GossipingPropertyFileSnitch | `GossipingPropertyFileSnitch` | `snitch::GossipingPropertyFileSnitch` | ✅ |
| DynamicEndpointSnitch | `DynamicEndpointSnitch` | `snitch::DynamicEndpointSnitch` | ✅ |
| RackInferringSnitch | `RackInferringSnitch` | `snitch::RackInferringSnitch` | ✅ |
| Ec2Snitch | `Ec2Snitch` | `snitch::Ec2Snitch` | ✅ |
| **Ec2MultiRegionSnitch** | `Ec2MultiRegionSnitch` | `snitch::Ec2MultiRegionSnitch` | ✅ NEW |
| **GoogleCloudSnitch** | `GoogleCloudSnitch` | `snitch::GoogleCloudSnitch` | ✅ NEW (stub) |
| SimpleStrategy | `SimpleStrategy` | `replication::SimpleStrategy` | ✅ |
| NetworkTopologyStrategy | `NetworkTopologyStrategy` | `replication::NetworkTopologyStrategy` | ✅ |
| Phi-Accrual Failure Detector | `FailureDetector` | `gossip::failure_detector` | ✅ |
| Gossip SYN/ACK/ACK2 | `Gossiper` | `gossip::mod` | ✅ |
| **Dead endpoint probing** | `Gossiper.maybeGossipToUnreachable` | `gossip::maybe_probe_dead_endpoints` | ✅ NEW |
| **Shadow round** | `Gossiper.doShadowRound` | `gossip::do_shadow_round` | ✅ NEW |
| **Generation-based state reset** | `Gossiper.handleMajorStateChange` | `gossip::handle_generation_change` | ✅ NEW |
| Schema agreement | `Gossiper.getSchemaVersion` | `gossip::schema_agreement` | ✅ |
| Multi-DC gossip convergence | — | `gossip::tests` | ✅ 6-node test |
| Internode message framing | `MessageOut/MessageIn` | `cassandra_messaging::frame` | ✅ |
| **Version negotiation** | `OutboundConnectionInitiator` | `frame::VersionNegotiation` | ✅ NEW |
| **Connection pooling** | `OutboundConnections` | `service::ConnectionPool` | ✅ NEW |
| **Per-verb timeouts** | `DatabaseDescriptor.getRpcTimeout` | `service::VerbTimeoutConfig` | ✅ NEW |
| **Message flags (CROSS_DC, etc.)** | `Message.header().flag()` | `frame::flags::CROSS_DC` | ✅ NEW |
| Backpressure | `OutboundConnection.enqueue` | `service::MessagingService` | ✅ |
| Request/response correlation | `RequestCallbacks` | `service::MessagingService` | ✅ |
| TCM Epoch / Transformation | `Epoch`, `Transformation` | `tcm::Epoch`, `tcm::Transformation` | ✅ |
| MetadataLog | `LogState` | `tcm::MetadataLog` | ✅ |
| **TcmSnapshot** | `ClusterMetadataSnapshot` | `tcm::TcmSnapshot` | ✅ NEW |
| **Sealed periods** | `LogStorage.SealedPeriod` | `tcm::SealedPeriod` | ✅ NEW |
| **Log compaction** | `LogStorage.compact` | `tcm::TcmMetadata::compact` | ✅ NEW |
| **Snapshot restore** | — | `tcm::TcmMetadata::restore_from_snapshot` | ✅ NEW |
| LockedRanges | `LockedRanges` | `tcm::LockedRanges` | ✅ |
| InProgressSequence | `InProgressSequences` | `tcm::InProgressSequence` | ✅ |
| **Epoch dissemination via gossip** | `Gossiper.addLocalApplicationState` | `tcm::bridge::disseminate_epoch` | ✅ NEW |
| **Stale peer detection** | — | `tcm::bridge::detect_stale_peers` | ✅ NEW |
| **Peer catch-up entries** | `TcmFetch` | `tcm::bridge::entries_for_catch_up` | ✅ NEW |
| ControlPlaneBridge | `ClusterMetadataService` | `tcm::bridge::ControlPlaneBridge` | ✅ |
| Topology ops state machine | `StorageService` | `topology::TopologyCoordinator` | ✅ |

## Open Items (Deferred)

| Item | Priority | Notes |
|------|----------|-------|
| Golden tests: Java Murmur3 oracle | Medium | Need Java fixture generator |
| GoogleCloudSnitch: GCE metadata fetch | Low | Stub ready, needs metadata API |
| Multi-DC chaos tests | Medium | Requires network partition injection |
| ADR 020: partitioner-ring integration | Low | Architecture doc |
| Integration/dtest harness | Medium | Multi-node end-to-end tests |
| TLS for internode messaging | Medium | Covered in security crate |
