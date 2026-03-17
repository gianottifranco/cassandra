// Licensed under Apache License, Version 2.0.

//! CIDR-based network authorizer.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.auth.CIDRPermissionsManager`
//! - `org.apache.cassandra.auth.CIDRAuthorizor`
//!
//! Restricts which IP ranges can connect as specific roles.

use std::net::IpAddr;

use ipnet::IpNet;
use serde::{Deserialize, Serialize};

use dashmap::DashMap;

use crate::SecurityError;

// ─── CIDR Group ────────────────────────────────────────────────────────────

/// A named group of CIDR ranges.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CidrGroup {
    pub name: String,
    pub ranges: Vec<String>, // Stored as strings for serialization; parsed on check.
}

// ─── CIDR Groups Manager ──────────────────────────────────────────────────

/// Manages named CIDR groups (CRUD operations).
///
/// ## Java Oracle
/// - `org.apache.cassandra.auth.CIDRGroupsMappingManager`
pub trait CidrGroupsManager: Send + Sync {
    fn create_group(&self, group: CidrGroup) -> Result<(), SecurityError>;
    fn get_group(&self, name: &str) -> Option<CidrGroup>;
    fn update_group(&self, name: &str, ranges: Vec<String>) -> Result<(), SecurityError>;
    fn delete_group(&self, name: &str) -> Result<(), SecurityError>;
    fn list_groups(&self) -> Vec<CidrGroup>;
}

/// In-memory CIDR groups manager.
pub struct InMemoryCidrGroupsManager {
    groups: DashMap<String, CidrGroup>,
}

impl InMemoryCidrGroupsManager {
    pub fn new() -> Self {
        Self {
            groups: DashMap::new(),
        }
    }
}

impl Default for InMemoryCidrGroupsManager {
    fn default() -> Self {
        Self::new()
    }
}

impl CidrGroupsManager for InMemoryCidrGroupsManager {
    fn create_group(&self, group: CidrGroup) -> Result<(), SecurityError> {
        if self.groups.contains_key(&group.name) {
            return Err(SecurityError::AuthError(format!(
                "CIDR group '{}' already exists",
                group.name
            )));
        }
        // Validate ranges
        for r in &group.ranges {
            r.parse::<IpNet>()
                .map_err(|e| SecurityError::AuthError(format!("invalid CIDR '{}': {}", r, e)))?;
        }
        self.groups.insert(group.name.clone(), group);
        Ok(())
    }

    fn get_group(&self, name: &str) -> Option<CidrGroup> {
        self.groups.get(name).map(|g| g.clone())
    }

    fn update_group(&self, name: &str, ranges: Vec<String>) -> Result<(), SecurityError> {
        for r in &ranges {
            r.parse::<IpNet>()
                .map_err(|e| SecurityError::AuthError(format!("invalid CIDR '{}': {}", r, e)))?;
        }
        let mut entry = self
            .groups
            .get_mut(name)
            .ok_or_else(|| SecurityError::AuthError(format!("CIDR group '{}' not found", name)))?;
        entry.ranges = ranges;
        Ok(())
    }

    fn delete_group(&self, name: &str) -> Result<(), SecurityError> {
        self.groups
            .remove(name)
            .ok_or_else(|| SecurityError::AuthError(format!("CIDR group '{}' not found", name)))?;
        Ok(())
    }

    fn list_groups(&self) -> Vec<CidrGroup> {
        self.groups.iter().map(|e| e.value().clone()).collect()
    }
}

// ─── CIDR Authorizer ───────────────────────────────────────────────────────

/// Controls which IP ranges can authenticate as which roles.
pub struct CidrAuthorizer {
    /// Map of role name → allowed CIDR ranges.
    role_cidrs: parking_lot::RwLock<std::collections::HashMap<String, Vec<IpNet>>>,
    /// Global default: if true, connections from unlisted IPs are denied.
    deny_by_default: bool,
}

impl CidrAuthorizer {
    pub fn new(deny_by_default: bool) -> Self {
        Self {
            role_cidrs: parking_lot::RwLock::new(std::collections::HashMap::new()),
            deny_by_default,
        }
    }

    /// Set allowed CIDR ranges for a role.
    pub fn set_role_cidrs(&self, role: &str, cidrs: Vec<String>) -> Result<(), SecurityError> {
        let parsed: Result<Vec<IpNet>, _> = cidrs.iter().map(|s| s.parse::<IpNet>()).collect();
        let parsed =
            parsed.map_err(|e| SecurityError::AuthError(format!("invalid CIDR: {}", e)))?;
        self.role_cidrs.write().insert(role.to_string(), parsed);
        Ok(())
    }

    /// Remove CIDR restrictions for a role.
    pub fn clear_role_cidrs(&self, role: &str) {
        self.role_cidrs.write().remove(role);
    }

    /// Check whether `addr` is allowed to connect as `role`.
    pub fn check_access(&self, addr: IpAddr, role: &str) -> Result<(), SecurityError> {
        let cidrs = self.role_cidrs.read();

        match cidrs.get(role) {
            Some(allowed) => {
                for net in allowed {
                    if net.contains(&addr) {
                        return Ok(());
                    }
                }
                Err(SecurityError::AuthError(format!(
                    "connection from {} not permitted for role '{}'",
                    addr, role
                )))
            }
            None => {
                if self.deny_by_default {
                    Err(SecurityError::AuthError(format!(
                        "no CIDR rules for role '{}' and deny_by_default is true",
                        role
                    )))
                } else {
                    Ok(())
                }
            }
        }
    }

    /// List all configured CIDR restrictions.
    pub fn list_restrictions(&self) -> Vec<(String, Vec<String>)> {
        self.role_cidrs
            .read()
            .iter()
            .map(|(role, nets)| (role.clone(), nets.iter().map(|n| n.to_string()).collect()))
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{Ipv4Addr, Ipv6Addr};

    #[test]
    fn allow_matching_ipv4() {
        let authz = CidrAuthorizer::new(false);
        authz
            .set_role_cidrs("admin", vec!["192.168.1.0/24".into()])
            .unwrap();

        assert!(
            authz
                .check_access(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 100)), "admin")
                .is_ok()
        );
    }

    #[test]
    fn deny_non_matching_ipv4() {
        let authz = CidrAuthorizer::new(false);
        authz
            .set_role_cidrs("admin", vec!["10.0.0.0/8".into()])
            .unwrap();

        assert!(
            authz
                .check_access(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), "admin")
                .is_err()
        );
    }

    #[test]
    fn allow_ipv6() {
        let authz = CidrAuthorizer::new(false);
        authz
            .set_role_cidrs("user", vec!["::1/128".into()])
            .unwrap();

        assert!(
            authz
                .check_access(IpAddr::V6(Ipv6Addr::LOCALHOST), "user")
                .is_ok()
        );
    }

    #[test]
    fn deny_ipv6_wrong_range() {
        let authz = CidrAuthorizer::new(false);
        authz
            .set_role_cidrs("user", vec!["fe80::/10".into()])
            .unwrap();

        assert!(
            authz
                .check_access(IpAddr::V6(Ipv6Addr::LOCALHOST), "user")
                .is_err()
        );
    }

    #[test]
    fn no_restriction_allows_by_default() {
        let authz = CidrAuthorizer::new(false);
        assert!(
            authz
                .check_access(IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)), "anyone")
                .is_ok()
        );
    }

    #[test]
    fn deny_by_default_when_no_rules() {
        let authz = CidrAuthorizer::new(true);
        assert!(
            authz
                .check_access(IpAddr::V4(Ipv4Addr::new(1, 2, 3, 4)), "anyone")
                .is_err()
        );
    }

    #[test]
    fn multiple_cidr_ranges() {
        let authz = CidrAuthorizer::new(false);
        authz
            .set_role_cidrs("multi", vec!["10.0.0.0/8".into(), "172.16.0.0/12".into()])
            .unwrap();

        assert!(
            authz
                .check_access(IpAddr::V4(Ipv4Addr::new(10, 1, 2, 3)), "multi")
                .is_ok()
        );
        assert!(
            authz
                .check_access(IpAddr::V4(Ipv4Addr::new(172, 20, 0, 1)), "multi")
                .is_ok()
        );
        assert!(
            authz
                .check_access(IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1)), "multi")
                .is_err()
        );
    }

    #[test]
    fn clear_restrictions() {
        let authz = CidrAuthorizer::new(false);
        authz
            .set_role_cidrs("temp", vec!["127.0.0.0/8".into()])
            .unwrap();
        authz.clear_role_cidrs("temp");
        assert!(
            authz
                .check_access(IpAddr::V4(Ipv4Addr::new(1, 1, 1, 1)), "temp")
                .is_ok()
        );
    }

    #[test]
    fn invalid_cidr_rejected() {
        let authz = CidrAuthorizer::new(false);
        assert!(
            authz
                .set_role_cidrs("x", vec!["not-a-cidr".into()])
                .is_err()
        );
    }

    #[test]
    fn list_restrictions() {
        let authz = CidrAuthorizer::new(false);
        authz
            .set_role_cidrs("a", vec!["10.0.0.0/8".into()])
            .unwrap();
        let list = authz.list_restrictions();
        assert_eq!(list.len(), 1);
    }

    // ─── CIDR Groups Manager Tests ───────────────────────────────────────

    #[test]
    fn create_and_get_group() {
        let mgr = InMemoryCidrGroupsManager::new();
        mgr.create_group(CidrGroup {
            name: "office".into(),
            ranges: vec!["10.0.0.0/8".into()],
        })
        .unwrap();
        let group = mgr.get_group("office").unwrap();
        assert_eq!(group.ranges, vec!["10.0.0.0/8".to_string()]);
    }

    #[test]
    fn duplicate_group_fails() {
        let mgr = InMemoryCidrGroupsManager::new();
        mgr.create_group(CidrGroup {
            name: "g".into(),
            ranges: vec!["10.0.0.0/8".into()],
        })
        .unwrap();
        assert!(
            mgr.create_group(CidrGroup {
                name: "g".into(),
                ranges: vec![],
            })
            .is_err()
        );
    }

    #[test]
    fn update_group() {
        let mgr = InMemoryCidrGroupsManager::new();
        mgr.create_group(CidrGroup {
            name: "g".into(),
            ranges: vec!["10.0.0.0/8".into()],
        })
        .unwrap();
        mgr.update_group("g", vec!["192.168.0.0/16".into()])
            .unwrap();
        let group = mgr.get_group("g").unwrap();
        assert_eq!(group.ranges, vec!["192.168.0.0/16".to_string()]);
    }

    #[test]
    fn delete_group() {
        let mgr = InMemoryCidrGroupsManager::new();
        mgr.create_group(CidrGroup {
            name: "g".into(),
            ranges: vec!["10.0.0.0/8".into()],
        })
        .unwrap();
        mgr.delete_group("g").unwrap();
        assert!(mgr.get_group("g").is_none());
    }

    #[test]
    fn invalid_cidr_in_group_rejected() {
        let mgr = InMemoryCidrGroupsManager::new();
        assert!(
            mgr.create_group(CidrGroup {
                name: "bad".into(),
                ranges: vec!["not-valid".into()],
            })
            .is_err()
        );
    }

    #[test]
    fn list_groups() {
        let mgr = InMemoryCidrGroupsManager::new();
        mgr.create_group(CidrGroup {
            name: "a".into(),
            ranges: vec!["10.0.0.0/8".into()],
        })
        .unwrap();
        mgr.create_group(CidrGroup {
            name: "b".into(),
            ranges: vec!["172.16.0.0/12".into()],
        })
        .unwrap();
        assert_eq!(mgr.list_groups().len(), 2);
    }
}
