# ADR-012: Security Architecture

**Status:** Accepted  
**Date:** 2026-03-15  
**Context:** Phase 4 — Operations & Security

## Decision

### TLS: rustls over OpenSSL

We use `rustls` (pure Rust TLS) instead of linking OpenSSL:

1. **No C dependency** — eliminates cross-compilation headaches and supply-chain risk.
2. **Memory safety** — no `unsafe` in the TLS stack itself.
3. **Performance** — rustls benchmarks within 5-10% of OpenSSL for most workloads.
4. **JKS keystore gap** — Java Cassandra uses JKS/PKCS12 keystores. Our Rust implementation
   reads PEM files directly. We document this gap and provide migration guidance.

The `TlsConfig` supports PEM-based cert/key/CA paths. Hot-reload is supported via
`ReloadableTlsAcceptor` with periodic file polling (configurable interval, default 600s).
This approach trades push-notification precision for simplicity and portability.

### Authentication: Trait-based pluggable design

- `Authenticator` trait with `authenticate(Credentials) → AuthenticatedUser`.
- `AllowAllAuthenticator` — default, zero-overhead when auth is disabled.
- `PasswordAuthenticator` — bcrypt-hashed passwords.
- bcrypt cost factor: configurable, default 10 (production), 4 (tests for speed).

### Authorization: Role-based with inheritance

- `Authorizer` trait with resource-based permission model.
- `CassandraAuthorizer` checks permissions with transitive role inheritance.
- Permission hierarchy: ROOT → KEYSPACE → TABLE (matches Java semantics).
- In-memory permission store for single-node. Persistence to `system_auth` deferred.

### CIDR Authorizer

- Restricts login per-role to specific IPv4/IPv6 CIDR ranges.
- `deny_by_default` mode for locked-down environments.

### Dynamic Data Masking (DDM)

- `MaskingFunction` trait with built-in masks: default, replace, partial, hash, null.
- Column masking policies applied at read time for non-privileged users.

### Audit Logging

- `AuditLogger` trait with configurable sinks.
- `FileAuditLogger` — JSON lines with file rotation.
- `AsyncAuditLogger` — channel-based decoupling from hot path via `tokio::mpsc`.
- Not on the critical path: audit events are fire-and-forget through the channel.

### Full Query Logging (FQL)

- Binary format: length-prefixed records with version byte.
- Rolling file writer, feature-gated behind `fql` flag.
- `FqlReader` for offline analysis.

## Consequences

- JKS keystores cannot be used directly (PEM conversion required).
- In-memory role/permission storage means data loss on restart (persistence is a gap).
- bcrypt is intentionally slow; cost factor must be tuned for hot-path auth checks vs. security.
- Audit channel can drop events under extreme back-pressure (bounded by mpsc channel type).

## Gaps

| Gap | Impact | Plan |
|-----|--------|------|
| JKS keystore support | Migration friction | Document PEM conversion in runbook |
| Persistent role storage | Data loss on restart | Persist to system_auth tables |
| LDAP/Kerberos auth | Enterprise deployments | Add as pluggable Authenticator impl |
| Syslog audit sink | Compliance | Implement SyslogAuditLogger |
| SHA-256 for hash mask | Security | Add sha2 crate dependency |
