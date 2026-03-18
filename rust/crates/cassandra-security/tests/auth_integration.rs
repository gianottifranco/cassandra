// Licensed under Apache License, Version 2.0.

//! Integration tests for the auth subsystem.
//! Tests the full flow: create role → authenticate → grant → authorize → revoke → denied.

use std::sync::Arc;

use cassandra_security::auth::{Authenticator, Credentials, PasswordAuthenticator, hash_password};
use cassandra_security::authz::{Authorizer, CassandraAuthorizer, Permission, Resource};
use cassandra_security::cache::AuthCacheConfig;
use cassandra_security::roles::{InMemoryRoleManager, Role, RoleManager};
use cassandra_security::{
    AllowAllNetworkAuthorizer, AuthManager, CidrAuthorizer, CredentialsCache, PermissionsCache,
    RolesCache,
};

#[test]
fn full_auth_flow_create_authenticate_grant_authorize() {
    let role_manager = Arc::new(InMemoryRoleManager::new());

    // Create a role with password
    let hashed = hash_password("secret123").unwrap();
    role_manager.create_role(Role {
        name: "app_user".into(),
        is_superuser: false,
        can_login: true,
        hashed_password: Some(hashed),
        member_of: vec![],
        network_permissions: None,
    });

    // Authenticate
    let authenticator = PasswordAuthenticator::new(role_manager.clone());
    let creds = Credentials {
        username: "app_user".into(),
        password: "secret123".into(),
        source_address: None,
    };
    let user = authenticator.authenticate(&creds).unwrap();
    assert_eq!(user.role_name, "app_user");
    assert!(!user.is_superuser);

    // Grant SELECT on a keyspace
    let authorizer = CassandraAuthorizer::new(role_manager.clone());
    let resource = Resource::Keyspace("my_keyspace".into());
    authorizer
        .grant("cassandra", "app_user", &resource, Permission::Select)
        .unwrap();

    // Authorize — should succeed
    assert!(
        authorizer
            .authorize("app_user", &resource, Permission::Select)
            .is_ok()
    );

    // Authorize a different permission — should fail
    assert!(
        authorizer
            .authorize("app_user", &resource, Permission::Modify)
            .is_err()
    );

    // Revoke SELECT
    authorizer
        .revoke("cassandra", "app_user", &resource, Permission::Select)
        .unwrap();

    // Authorize again — should fail now
    assert!(
        authorizer
            .authorize("app_user", &resource, Permission::Select)
            .is_err()
    );
}

#[test]
fn role_inheritance_grants_transitive_permissions() {
    let role_manager = Arc::new(InMemoryRoleManager::new());

    role_manager.create_role(Role {
        name: "reader_role".into(),
        is_superuser: false,
        can_login: false,
        hashed_password: None,
        member_of: vec![],
        network_permissions: None,
    });

    role_manager.create_role(Role {
        name: "user1".into(),
        is_superuser: false,
        can_login: true,
        hashed_password: Some(hash_password("pass").unwrap()),
        member_of: vec![],
        network_permissions: None,
    });

    // Grant user1 membership in reader_role
    role_manager.grant_role("reader_role", "user1").unwrap();

    let authorizer = CassandraAuthorizer::new(role_manager.clone());
    let resource = Resource::Keyspace("data_ks".into());

    // Grant SELECT to reader_role
    authorizer
        .grant("cassandra", "reader_role", &resource, Permission::Select)
        .unwrap();

    // user1 should inherit SELECT through reader_role
    assert!(
        authorizer
            .authorize("user1", &resource, Permission::Select)
            .is_ok()
    );

    // Revoke membership
    role_manager.revoke_role("reader_role", "user1").unwrap();

    // user1 should no longer have SELECT
    assert!(
        authorizer
            .authorize("user1", &resource, Permission::Select)
            .is_err()
    );
}

#[test]
fn cache_invalidation() {
    let perms_cache = PermissionsCache::new(AuthCacheConfig::default());
    let roles_cache = RolesCache::new(AuthCacheConfig::default());
    let creds_cache = CredentialsCache::new(AuthCacheConfig::default());

    // Populate caches
    perms_cache.put(
        "user1",
        &Resource::Root,
        vec![Permission::Select, Permission::Modify],
    );
    roles_cache.put("user1", vec!["user1".into(), "admin".into()]);
    creds_cache.put("user1", "$2b$12$hash".into());

    // Verify cached
    assert!(perms_cache.get("user1", &Resource::Root).is_some());
    assert!(roles_cache.get("user1").is_some());
    assert!(creds_cache.get("user1").is_some());

    // Invalidate
    perms_cache.invalidate_all();
    roles_cache.invalidate_all();
    creds_cache.invalidate_all();

    assert!(perms_cache.get("user1", &Resource::Root).is_none());
    assert!(roles_cache.get("user1").is_none());
    assert!(creds_cache.get("user1").is_none());
}

#[test]
fn mtls_identity_extraction() {
    use cassandra_security::mtls::CertificateValidator;
    use cassandra_security::mtls::SubjectCnValidator;

    let mut params =
        rcgen::CertificateParams::new(vec!["service.example.com".to_string()]).unwrap();
    params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "service.example.com");
    let key_pair = rcgen::KeyPair::generate().unwrap();
    let cert = params.self_signed(&key_pair).unwrap();
    let cert_der = cert.der().to_vec();

    let validator = SubjectCnValidator;
    let cn = validator.validate(&cert_der).unwrap();
    assert_eq!(cn, "service.example.com");
}

#[test]
fn cidr_group_management() {
    use cassandra_security::{CidrGroup, CidrGroupsManager, InMemoryCidrGroupsManager};

    let mgr = InMemoryCidrGroupsManager::new();

    // Create a group
    mgr.create_group(CidrGroup {
        name: "office".into(),
        ranges: vec!["10.0.0.0/8".into(), "192.168.0.0/16".into()],
    })
    .unwrap();

    // Verify
    let group = mgr.get_group("office").unwrap();
    assert_eq!(group.ranges.len(), 2);

    // Update
    mgr.update_group("office", vec!["10.0.0.0/8".into()])
        .unwrap();
    let group = mgr.get_group("office").unwrap();
    assert_eq!(group.ranges.len(), 1);

    // Delete
    mgr.delete_group("office").unwrap();
    assert!(mgr.get_group("office").is_none());
}

#[test]
fn auth_manager_full_pipeline() {
    let role_manager = Arc::new(InMemoryRoleManager::new());
    let authorizer = Arc::new(cassandra_security::AllowAllAuthorizer);

    let auth_manager = AuthManager::new(
        Arc::new(cassandra_security::AllowAllAuthenticator),
        authorizer,
        role_manager,
        Arc::new(AllowAllNetworkAuthorizer),
        None,
        AuthCacheConfig::default(),
    );

    let creds = Credentials {
        username: "any".into(),
        password: "any".into(),
        source_address: None,
    };

    let user = auth_manager
        .full_access_check(&creds, &Resource::Root, Permission::Select, "dc1")
        .unwrap();
    assert!(user.is_anonymous);
}

#[test]
fn cidr_authorizer_blocks_wrong_ip() {
    use std::net::{IpAddr, Ipv4Addr};

    let cidr = Arc::new(CidrAuthorizer::new(false));
    cidr.set_role_cidrs("restricted", vec!["10.0.0.0/8".into()])
        .unwrap();

    let role_manager = Arc::new(InMemoryRoleManager::new());
    let auth_manager = AuthManager::new(
        Arc::new(cassandra_security::AllowAllAuthenticator),
        Arc::new(cassandra_security::AllowAllAuthorizer),
        role_manager,
        Arc::new(AllowAllNetworkAuthorizer),
        Some(cidr),
        AuthCacheConfig::default(),
    );

    // CIDR check should fail for wrong IP
    assert!(
        auth_manager
            .check_cidr_access(IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)), "restricted")
            .is_err()
    );

    // CIDR check should pass for correct IP
    assert!(
        auth_manager
            .check_cidr_access(IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)), "restricted")
            .is_ok()
    );
}
