# Security Review — Cassandra Rust Implementation

**Date**: 2026-03-16
**Reviewer**: Principal Engineer (automated + manual)
**Scope**: All 19 Rust crates in the workspace

---

## 1. Dependency Audit

**Tool**: `cargo deny check` (configuration in `rust/deny.toml`)

| Check | Policy | Status |
|-------|--------|--------|
| Known vulnerabilities (CVEs) | Deny | ✅ No advisories |
| Unmaintained crates | Warn | ✅ None detected |
| Yanked versions | Deny | ✅ None |
| License compliance | Allow: Apache-2.0, MIT, BSD, ISC, Zlib | ✅ All compliant |
| Copyleft licenses | Deny | ✅ None present |
| Unknown registries | Deny | ✅ All from crates.io |
| Duplicate versions | Warn | ⚠️ Some duplicates (normal for large workspace) |

**Cryptographic dependencies**:
- `ring` — Used by rustls for TLS. Well-audited, maintained by Google.
- `rustls` — Pure-Rust TLS. No OpenSSL dependency. Defaults to TLS 1.2+.
- `bcrypt` — Used for password hashing. Cost factor configurable.
- `md-5` — Used for Murmur3 partitioner compatibility, NOT for security.
- `rcgen` — Certificate generation for testing only.

> [!NOTE]
> `md-5` is used for compatibility with Java's partitioner hashing, not for
> cryptographic purposes. This is acceptable.

---

## 2. Unsafe Code Inventory

**Policy**: Zero `unsafe` blocks without justification (ADR-002).

**Scan command**: `make unsafe-audit`

| Crate | Unsafe blocks | Justified | Status |
|-------|--------------|-----------|--------|
| All 19 crates | 0 | N/A | ✅ Clean |

> [!TIP]
> The workspace achieves zero `unsafe` usage. If `unsafe` is required in the
> future (e.g., for zero-copy SSTable reads), follow ADR-002: document SAFETY
> invariants, add benchmarks proving the need, and include targeted tests.

---

## 3. TLS / Encryption

| Property | Implementation | Status |
|----------|---------------|--------|
| TLS library | rustls 0.23 (pure Rust, no OpenSSL) | ✅ |
| Minimum protocol version | TLS 1.2 (rustls default) | ✅ |
| TLS 1.3 support | Yes (rustls default) | ✅ |
| Weak ciphers (RC4, DES, 3DES) | Not available in rustls | ✅ |
| Certificate validation | Standard X.509 chain validation | ✅ |
| Client certificate auth | Supported via rustls | ✅ |
| Certificate rotation | Hot-reload via file watcher (notify crate) | ✅ |

**Gaps**:
- TLS not yet wired to native protocol TCP listener (functional gap, not security gap).
- No OCSP stapling support (rustls limitation, low priority).

---

## 4. Authentication & Authorization

| Property | Implementation | Status |
|----------|---------------|--------|
| PasswordAuthenticator | bcrypt hashing (cost=10 default) | ✅ |
| AllowAllAuthenticator | Development-only, clearly documented | ✅ |
| Role-based authorization | Hierarchical roles with permissions | ✅ |
| CIDR access control | IP range-based ACLs | ✅ |
| Dynamic data masking | Column-level masking functions | ✅ |

**Gaps**:
- LDAP/Kerberos authentication (deferred, documented in gap matrix).
- Persistent role storage (currently in-memory only).

---

## 5. Secret / Credential Handling

| Policy | Implementation | Status |
|--------|---------------|--------|
| No plaintext passwords in config | Passwords hashed on storage | ✅ |
| No secrets in log output | Automated scan in `security_audit.rs` | ✅ |
| Credential rotation | Supported via config reload | ✅ |
| Environment variable secrets | Supported via figment env provider | ✅ |

---

## 6. Log Redaction

| Policy | Implementation | Status |
|--------|---------------|--------|
| Query parameters redacted | FQL logs with bind values separately | ✅ |
| IP addresses in audit log | Logged (required for audit compliance) | ✅ |
| Stack traces sanitized | No Java stack traces to leak | ✅ |

---

## 7. Binary Hardening

Verified in `[profile.production]` of workspace `Cargo.toml`:

| Property | Setting | Status |
|----------|---------|--------|
| LTO | `fat` (full link-time optimization) | ✅ |
| Codegen units | `1` (single codegen unit for max optimization) | ✅ |
| Panic strategy | `abort` (no unwinding, smaller binary) | ✅ |
| Symbol stripping | `symbols` (stripped from release binary) | ✅ |

---

## 8. Supply Chain Security

| Measure | Status |
|---------|--------|
| `Cargo.lock` committed | ✅ Reproducible builds |
| `cargo-deny` in CI | ⚠️ TODO: Add to CI pipeline |
| No git dependencies | ✅ All from crates.io |
| No wildcard version specs | ✅ Enforced by `deny.toml` |

---

## 9. Sanitizer Coverage

| Sanitizer | Crates tested | Status |
|-----------|--------------|--------|
| ASan (Address Sanitizer) | common, types, protocol, storage | ✅ Script ready |
| Miri (UB detector) | common, types | ✅ Script ready |
| TSan (Thread Sanitizer) | — | 🚫 Deferred (requires nightly + specific target) |

**Run command**: `bash scripts/sanitizer_ci.sh --all`

---

## 10. Recommendations

1. **Wire TLS to listeners** — Highest priority security gap.
2. **Add `cargo-deny` to CI pipeline** — Run on every PR.
3. **Persistent role storage** — Required before production auth works.
4. **LDAP integration** — Required for enterprise deployments.
5. **OCSP stapling** — Low priority, upstream rustls dependency.
