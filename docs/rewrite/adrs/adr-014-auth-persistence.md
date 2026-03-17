# ADR-014: Auth Persistence & Complete Auth Subsystem

## Status
Accepted

## Context
The Cassandra Rust rewrite had auth traits and in-memory implementations but lacked:
- Persistence of auth data (roles, permissions lost on restart)
- Auth caches (every operation hit storage)
- mTLS authentication and certificate-based identity mapping
- Network/DC-based authorization
- Internode authentication
- CIDR groups management
- The native protocol authenticator was a stub accepting any credentials

## Decision

### Architecture
- **Trait-based pluggability**: All auth components implement traits (`Authenticator`, `Authorizer`, `RoleManager`, `NetworkAuthorizer`, `InternodeAuthenticator`, `CidrGroupsManager`)
- **Dual implementations**: In-memory (DashMap-backed) for testing + storage-backed (`SystemAuth*`) for production
- **AuthManager**: Central orchestrator owning all auth components and caches, providing `full_access_check` that chains authentication → CIDR → network → permission checks
- **NativeAuthWrapper** bridges protocol-level SASL parsing to security-layer bcrypt verification

### Persistence
- `SystemAuthRoleManager` persists to `system_auth.roles` and `system_auth.role_members` via commitlog mutations
- `SystemAuthAuthorizer` persists to `system_auth.role_permissions` and `system_auth.resource_role_index`
- Role membership (`member_of`) stored as set elements in column names (`member_of:role_name`)
- Permission sets stored similarly (`permissions:PERMISSION_NAME`)
- `Resource.to_cql_string()`/`from_cql_string()` for storage serialization (e.g., `data/ks/table`)

### Caching
- Generic `AuthCache<K,V>` with TTL expiration, max entries, and background refresh
- Three specialized caches: `PermissionsCache`, `RolesCache`, `CredentialsCache`
- Cache validity configurable via cassandra.yaml (`permissions_validity_in_ms`, etc.)

### New Tables (system_auth)
- `cidr_groups` — named CIDR group definitions
- `identity_to_roles` — maps certificate identities to roles
- `resource_role_index` — reverse index from resource to roles with permissions

### mTLS
- `CertificateValidator` trait with `SubjectCnValidator` (CN extraction) and `SpiffeCertificateValidator` (SPIFFE URI from SAN)
- `IdentityRoleMapper` trait with `InMemoryIdentityRoleMapper` for identity-to-role mapping
- `MutualTlsAuthenticator` chains validator → mapper → role lookup
- `MutualTlsInternodeAuthenticator` validates peer certificates against trusted CAs

## Consequences
- Auth data survives restarts via storage-backed implementations
- Caches reduce storage reads for hot paths (authentication, authorization)
- mTLS enables certificate-based service identity without passwords
- Network authorizer enables DC-level access control
- All components are independently testable via trait-based design
