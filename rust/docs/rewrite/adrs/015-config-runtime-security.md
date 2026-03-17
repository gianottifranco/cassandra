# ADR 015: Config Runtime, Security, and Guardrails

**Status:** Accepted
**Date:** 2026-03-17

## Context

The Cassandra Java→Rust rewrite needed runtime config management, env var overrides, hot-reload, guardrails enforcement, encryption-at-rest, and a crypto provider abstraction. These are critical infrastructure pieces referenced by all other subsystems.

## Decisions

### 1. Config Precedence Model

**Decision:** Environment variable > YAML file > compiled default.

Environment variables use the `CASSANDRA_` prefix with uppercase snake_case names (e.g., `CASSANDRA_NATIVE_TRANSPORT_PORT`). This matches Docker/K8s conventions and avoids needing to mount config files for simple overrides.

Overrides are applied by `properties::apply_overrides()` during config loading, after YAML parsing but before validation.

### 2. DatabaseDescriptor as Arc-Wrapped Singleton

**Decision:** `DatabaseDescriptor` wraps `Arc<RwLock<CassandraConfig>>` and `Arc<RwLock<GuardrailsConfig>>`.

Rationale:
- `parking_lot::RwLock` for minimal contention on the read-heavy path
- `Arc` allows sharing across subsystems without static mutable state
- `snapshot()` provides consistent reads of both configs
- `swap_config()` / `swap_guardrails()` enable atomic hot-reload
- Typed accessor methods prevent callers from holding the lock

### 3. Hot-Reload Safety

**Decision:** Only a subset of parameters are safe to hot-reload. The list is documented in `HOT_RELOADABLE_PARAMS`.

Params requiring restart (NOT hot-reloadable):
- `cluster_name`, `partitioner` — identity
- `listen_address`, `storage_port`, `native_transport_port` — require socket rebind
- `data_file_directories`, `commitlog_directory` — require storage engine restart
- `commitlog_sync` — requires commitlog restart

Hot-reloadable params:
- Thread pool sizes, rate limits, cache TTLs
- All guardrails configuration
- Hint delivery settings

On file change, the watcher: re-parses YAML → validates → applies env overrides → atomically swaps → notifies callbacks. If validation fails, the previous config is retained.

### 4. Guardrail Enforcement Architecture

**Decision:** Three-layer design:
1. **GuardrailsConfig** — pure data, loaded from YAML
2. **Guardrail trait + implementations** — EnableFlagGuardrail, ValuesGuardrail, PasswordPolicyGuardrail
3. **CQL integration** — `guardrail_checks` module in cassandra-server wires checks into query paths

Enforcement points:
- CREATE TABLE → tables_per_keyspace, columns_per_table
- CREATE INDEX → secondary_indexes_per_table
- SELECT with ALLOW FILTERING → allow_filtering_enabled
- TRUNCATE → truncate_enabled
- Page size → page_size threshold
- DROP KEYSPACE → drop_keyspace_enabled

Each check returns Allowed/Warned/Rejected. Warnings are logged but don't block. Rejections return an error to the client.

### 5. Crypto Provider Choice

**Decision:** AES-CBC with PKCS7 padding as the default cipher, matching Java's `AES/CBC/PKCS5Padding`.

Rationale:
- Java's PKCS5Padding is functionally PKCS7 for AES block sizes
- AES-CBC is the established cipher in Cassandra's TDE
- Uses the `aes` and `cbc` crates from the RustCrypto project (audited, no-std capable)
- `CryptoProvider` trait allows adding AES-GCM or other ciphers later

Key management: `KeyProvider` trait with `FileKeyProvider` implementation supporting raw binary and hex-encoded key files.

### 6. Encryption-at-Rest Integration

**Decision:** `StorageEncryptor` trait with hook points, defaulting to `NoOpStorageEncryptor`.

The trait defines `encrypt_segment` / `decrypt_segment` / `is_enabled`. Storage subsystems (commitlog, SSTable writer) call these hooks at data block boundaries. The encrypted commitlog module is a stub — actual integration will happen when SSTable writers are wired up.

`EncryptionContext` handles chunked encryption with per-chunk headers containing cipher info, IV, and key alias. This supports key rotation (different chunks can use different keys).

### 7. DataRateSpec Unit Type

**Decision:** New `DataRateSpec` type in `units.rs` parsing strings like "10MiB/s".

Follows the same pattern as `DataSize` and `Duration`: newtype wrapper around `u64` (bytes/sec), with `FromStr` and `Display` supporting MiB/s, KiB/s, B/s suffixes.

## Consequences

- All subsystems access config through `DatabaseDescriptor`, enabling consistent hot-reload
- Environment variables provide 12-factor app compliance for containerized deployments
- Guardrails are enforced at the CQL layer, preventing dangerous operations before they reach storage
- Encryption-at-rest is opt-in with sensible defaults (disabled, AES-128-CBC if enabled)
- The `StorageEncryptor` trait allows gradual integration without changing existing storage code
