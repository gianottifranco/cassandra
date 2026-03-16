# Tooling and Management Plane

## Overview
This document outlines the state of the tooling and management plane in the Cassandra Rust rewrite. The primary goal is to ensure operational equivalence with the Java baseline, allowing administrators to use familiar commands, scripts, and workflows.

## Nodetool Compatibility
The legacy `nodetool` in Java relies heavily on JMX (Java Management Extensions) to interact with the Cassandra node. In the Rust rewrite, JMX is not supported. Instead, the node exposes a REST API via the `cassandra-admin` crate.

To maintain backward compatibility:
- The `bin/nodetool` script has been updated to act as a wrapper.
- Instead of launching a JVM, `nodetool` forwards the commands to `cassandra-tools`, a compiled Rust binary.
- `cassandra-tools` interacts with the running local Cassandra node over the admin HTTP API (default port 9090).

### Supported Commands
- `status`: Shows ring and node states.
- `info`: Displays local node information.
- `ring`: Shows the token ring.
- `describecluster`: Shows cluster metadata.
- `snapshot`, `listsnapshots`, `clearsnapshot`: Manages backups.
- `compact`, `repair`, `cleanup`: Storage maintenance.
- `decommission`, `removenode`, `move`, `rebuild`: Topology operations.

## SSTable Tools
Offline tools designed to inspect, dump, and manipulate SSTables have been ported into the `cassandra-tools` binary.
- `sstabledump`: Dumps SSTable contents (data and partition structures).
- `sstablemetadata`: Inspects SSTable statistics and metadata files.
- Loader utilities (e.g., `sstableloader`) operate analogously by parsing files locally or streaming to the cluster.

## cqlsh
`cqlsh` leverages the standard native protocol (port 9042). We ensure high compatibility by completely implementing system tables such as `system.local`, `system.peers`, and virtualization in `system_views`. As long as the schemas match, `cqlsh` interacts seamlessly with the Rust node.

## Scripts & Empaquetado
Scripts like `cassandra-env.sh`, `cassandra.yaml`, and `cassandra-rackdc.properties` remain the single source of truth for deployment configuration. The Rust setup ignores JVM-specific flags but parses relevant configuration directives natively.

## Testing & Validation
All operational tooling is validated end-to-end using the `cassandra-diff-tests` framework, comparing the observable outputs from the tools running against a Rust cluster versus a Java baseline.
