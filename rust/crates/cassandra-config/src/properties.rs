// Licensed under Apache License, Version 2.0.

//! Environment variable overrides for configuration.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.config.CassandraRelevantProperties`
//!
//! Defines env → config field mappings with precedence: env > YAML > default.

use crate::config::CassandraConfig;

/// A single env-var → config-field mapping.
struct PropertyMapping {
    env_var: &'static str,
    apply: fn(&mut CassandraConfig, &str),
}

/// All recognised env-var overrides.
///
/// Convention: `CASSANDRA_` prefix, uppercase, underscores.
const MAPPINGS: &[PropertyMapping] = &[
    // ── Cluster identity ──
    PropertyMapping {
        env_var: "CASSANDRA_CLUSTER_NAME",
        apply: |c, v| c.cluster_name = v.to_string(),
    },
    PropertyMapping {
        env_var: "CASSANDRA_NUM_TOKENS",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.num_tokens = n;
            }
        },
    },
    // ── Networking ──
    PropertyMapping {
        env_var: "CASSANDRA_LISTEN_ADDRESS",
        apply: |c, v| c.listen_address = Some(v.to_string()),
    },
    PropertyMapping {
        env_var: "CASSANDRA_RPC_ADDRESS",
        apply: |c, v| c.rpc_address = Some(v.to_string()),
    },
    PropertyMapping {
        env_var: "CASSANDRA_NATIVE_TRANSPORT_PORT",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.native_transport_port = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_STORAGE_PORT",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.storage_port = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_SSL_STORAGE_PORT",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.ssl_storage_port = n;
            }
        },
    },
    // ── Directories ──
    PropertyMapping {
        env_var: "CASSANDRA_DATA_FILE_DIRECTORIES",
        apply: |c, v| {
            c.data_file_directories = Some(v.split(',').map(|s| s.trim().to_string()).collect());
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_COMMITLOG_DIRECTORY",
        apply: |c, v| c.commitlog_directory = Some(v.to_string()),
    },
    PropertyMapping {
        env_var: "CASSANDRA_HINTS_DIRECTORY",
        apply: |c, v| c.hints_directory = Some(v.to_string()),
    },
    PropertyMapping {
        env_var: "CASSANDRA_SAVED_CACHES_DIRECTORY",
        apply: |c, v| c.saved_caches_directory = Some(v.to_string()),
    },
    PropertyMapping {
        env_var: "CASSANDRA_CDC_RAW_DIRECTORY",
        apply: |c, v| c.cdc_raw_directory = Some(v.to_string()),
    },
    // ── Thread pools ──
    PropertyMapping {
        env_var: "CASSANDRA_CONCURRENT_READS",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.concurrent_reads = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_CONCURRENT_WRITES",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.concurrent_writes = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_CONCURRENT_COUNTER_WRITES",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.concurrent_counter_writes = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_CONCURRENT_MV_WRITES",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.concurrent_materialized_view_writes = n;
            }
        },
    },
    // ── Hints ──
    PropertyMapping {
        env_var: "CASSANDRA_HINTED_HANDOFF_ENABLED",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.hinted_handoff_enabled = b;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_MAX_HINTS_DELIVERY_THREADS",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.max_hints_delivery_threads = n;
            }
        },
    },
    // ── Commitlog ──
    PropertyMapping {
        env_var: "CASSANDRA_COMMITLOG_SYNC",
        apply: |c, v| c.commitlog_sync = v.to_string(),
    },
    // ── Native transport limits ──
    PropertyMapping {
        env_var: "CASSANDRA_NATIVE_TRANSPORT_MAX_CONCURRENT_CONNECTIONS",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.native_transport_max_concurrent_connections = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_NATIVE_TRANSPORT_MAX_FRAME_SIZE",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.native_transport_max_frame_size = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_NATIVE_TRANSPORT_MAX_REQUESTS_PER_SECOND",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.native_transport_max_requests_per_second = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_NATIVE_TRANSPORT_RATE_LIMITING",
        apply: |c, v| {
            if let Ok(b) = v.parse() {
                c.native_transport_rate_limiting_enabled = b;
            }
        },
    },
    // ── Failure policies ──
    PropertyMapping {
        env_var: "CASSANDRA_DISK_FAILURE_POLICY",
        apply: |c, v| c.disk_failure_policy = v.to_string(),
    },
    PropertyMapping {
        env_var: "CASSANDRA_COMMIT_FAILURE_POLICY",
        apply: |c, v| c.commit_failure_policy = v.to_string(),
    },
    // ── Auth cache ──
    PropertyMapping {
        env_var: "CASSANDRA_PERMISSIONS_VALIDITY_MS",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.permissions_validity_in_ms = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_ROLES_VALIDITY_MS",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.roles_validity_in_ms = n;
            }
        },
    },
    PropertyMapping {
        env_var: "CASSANDRA_CREDENTIALS_VALIDITY_MS",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.credentials_validity_in_ms = n;
            }
        },
    },
    // ── Admin ──
    PropertyMapping {
        env_var: "CASSANDRA_ADMIN_PORT",
        apply: |c, v| {
            if let Ok(n) = v.parse() {
                c.admin_port = n;
            }
        },
    },
];

/// Apply environment variable overrides to a config.
///
/// For each recognised `CASSANDRA_*` env var that is set, its value
/// overrides the corresponding config field. Unknown env vars are ignored.
pub fn apply_overrides(config: &mut CassandraConfig) {
    for mapping in MAPPINGS {
        if let Ok(val) = std::env::var(mapping.env_var) {
            tracing::debug!(env = mapping.env_var, value = %val, "env override applied");
            (mapping.apply)(config, &val);
        }
    }
}

/// List all recognised property env-var names (for diagnostics).
pub fn known_properties() -> Vec<&'static str> {
    MAPPINGS.iter().map(|m| m.env_var).collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::CassandraConfig;

    // SAFETY: These tests manipulate env vars and must run serially.
    // In production code, env vars are only read (never set) so this is safe.

    #[test]
    fn apply_cluster_name_override() {
        unsafe { std::env::set_var("CASSANDRA_CLUSTER_NAME", "EnvCluster") };
        let mut cfg = CassandraConfig::default();
        apply_overrides(&mut cfg);
        assert_eq!(cfg.cluster_name, "EnvCluster");
        unsafe { std::env::remove_var("CASSANDRA_CLUSTER_NAME") };
    }

    #[test]
    fn apply_port_override() {
        unsafe { std::env::set_var("CASSANDRA_NATIVE_TRANSPORT_PORT", "19042") };
        let mut cfg = CassandraConfig::default();
        apply_overrides(&mut cfg);
        assert_eq!(cfg.native_transport_port, 19042);
        unsafe { std::env::remove_var("CASSANDRA_NATIVE_TRANSPORT_PORT") };
    }

    #[test]
    fn invalid_port_ignored() {
        unsafe { std::env::set_var("CASSANDRA_NATIVE_TRANSPORT_PORT", "not_a_number") };
        let mut cfg = CassandraConfig::default();
        apply_overrides(&mut cfg);
        assert_eq!(cfg.native_transport_port, 9042); // unchanged
        unsafe { std::env::remove_var("CASSANDRA_NATIVE_TRANSPORT_PORT") };
    }

    #[test]
    fn data_dirs_comma_separated() {
        unsafe { std::env::set_var("CASSANDRA_DATA_FILE_DIRECTORIES", "/data1, /data2, /data3") };
        let mut cfg = CassandraConfig::default();
        apply_overrides(&mut cfg);
        let dirs = cfg.data_file_directories.unwrap();
        assert_eq!(dirs, vec!["/data1", "/data2", "/data3"]);
        unsafe { std::env::remove_var("CASSANDRA_DATA_FILE_DIRECTORIES") };
    }

    #[test]
    fn known_properties_not_empty() {
        let props = known_properties();
        assert!(props.len() >= 20);
        assert!(props.contains(&"CASSANDRA_CLUSTER_NAME"));
    }

    #[test]
    fn unset_env_vars_leave_defaults() {
        for name in known_properties() {
            unsafe { std::env::remove_var(name) };
        }
        let mut cfg = CassandraConfig::default();
        let original = CassandraConfig::default();
        apply_overrides(&mut cfg);
        assert_eq!(cfg.cluster_name, original.cluster_name);
        assert_eq!(cfg.native_transport_port, original.native_transport_port);
    }
}
