# Security Gaps Tracking

## Authentication

| Component | Status | Owning Prompt | Closure Criteria |
|-----------|--------|---------------|------------------|
| PasswordAuthenticator | Stub (accepts any) | 14 | Real password verification against system_auth |
| AllowAllAuthenticator | Done | — | — |
| LDAP authenticator | Missing | deferred | LDAP integration |
| Kerberos authenticator | Missing | deferred | Kerberos/GSSAPI support |
| CIDR-based auth | Missing | 14 | Network-based access control |
| mTLS client auth | Missing | 14 | Client certificate authentication |

## Authorization

| Component | Status | Owning Prompt | Closure Criteria |
|-----------|--------|---------------|------------------|
| Role hierarchy | Partial | 08 | Full role inheritance |
| Permission store | Missing | 14 | Persisted to system_auth.role_permissions |
| CQL permission statements | Missing | 03 | GRANT/REVOKE parsed and executed |

## TLS/SSL

| Component | Status | Owning Prompt | Closure Criteria |
|-----------|--------|---------------|------------------|
| rustls integration | Partial | 08 | TLS on client and internode |
| Certificate file watcher | Partial | 08 | Auto-reload on cert change |
| Client listener TLS | Missing | 01, 08 | TLS on native protocol listener |
| Internode TLS | Missing | 08 | TLS on internode connections |

## Audit & Logging

| Component | Status | Owning Prompt | Closure Criteria |
|-----------|--------|---------------|------------------|
| AuditLogger (serde) | Partial | 08 | Full query audit logging |
| Syslog sink | Missing | 19 | Syslog output for audit events |
| BinAuditLogger | Missing | 19 | Binary audit log format |
| Full Query Logging (FQL) | Missing | 19 | File-based FQL equivalent |

## Encryption at Rest

| Component | Status | Owning Prompt | Closure Criteria |
|-----------|--------|---------------|------------------|
| SSTable encryption | Missing | 15 | Encrypted SSTable files |
| CommitLog encryption | Missing | 15 | Encrypted commit log segments |
| Key management | Missing | 15 | Pluggable key provider |
