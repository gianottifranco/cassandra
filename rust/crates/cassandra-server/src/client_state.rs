// Licensed under Apache License, Version 2.0.

//! Per-connection client state and per-query state.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.transport.ClientStat`
//! - `org.apache.cassandra.service.QueryState`

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

/// Per-connection state tracked across the lifetime of a client connection.
#[derive(Debug)]
pub struct ClientState {
    /// Remote socket address of the connected client.
    remote_address: SocketAddr,
    /// Authenticated user name, `None` until AUTH_SUCCESS.
    authenticated_user: Option<String>,
    /// Active keyspace set via `USE <ks>`.
    keyspace: Option<String>,
    /// Negotiated protocol version (from STARTUP).
    protocol_version: u8,
    /// Driver name reported in STARTUP options.
    driver_name: Option<String>,
    /// Driver version reported in STARTUP options.
    driver_version: Option<String>,
    /// Number of requests processed on this connection.
    request_count: AtomicU64,
    /// Timestamp when the connection was established.
    connected_at: Instant,
}

impl ClientState {
    /// Create a new `ClientState` for an accepted connection.
    pub fn new(remote_address: SocketAddr) -> Self {
        Self {
            remote_address,
            authenticated_user: None,
            keyspace: None,
            protocol_version: 0,
            driver_name: None,
            driver_version: None,
            request_count: AtomicU64::new(0),
            connected_at: Instant::now(),
        }
    }

    pub fn remote_address(&self) -> SocketAddr {
        self.remote_address
    }

    pub fn authenticated_user(&self) -> Option<&str> {
        self.authenticated_user.as_deref()
    }

    pub fn set_authenticated_user(&mut self, user: String) {
        self.authenticated_user = Some(user);
    }

    pub fn keyspace(&self) -> Option<&str> {
        self.keyspace.as_deref()
    }

    pub fn set_keyspace(&mut self, ks: String) {
        self.keyspace = Some(ks);
    }

    pub fn protocol_version(&self) -> u8 {
        self.protocol_version
    }

    pub fn set_protocol_version(&mut self, version: u8) {
        self.protocol_version = version;
    }

    pub fn driver_name(&self) -> Option<&str> {
        self.driver_name.as_deref()
    }

    pub fn set_driver_name(&mut self, name: String) {
        self.driver_name = Some(name);
    }

    pub fn driver_version(&self) -> Option<&str> {
        self.driver_version.as_deref()
    }

    pub fn set_driver_version(&mut self, version: String) {
        self.driver_version = Some(version);
    }

    /// Increment request counter and return the new value.
    pub fn increment_request_count(&self) -> u64 {
        self.request_count.fetch_add(1, Ordering::Relaxed) + 1
    }

    pub fn request_count(&self) -> u64 {
        self.request_count.load(Ordering::Relaxed)
    }

    /// Duration since the connection was established.
    pub fn elapsed(&self) -> std::time::Duration {
        self.connected_at.elapsed()
    }
}

/// Per-query state: a reference to the connection's `ClientState` plus
/// per-request parameters (consistency level, timestamp, etc.).
pub struct QueryState<'a> {
    pub client_state: &'a ClientState,
    pub consistency_level: u16,
    pub timestamp_micros: Option<i64>,
}

impl<'a> QueryState<'a> {
    pub fn new(client_state: &'a ClientState) -> Self {
        Self {
            client_state,
            consistency_level: 1, // ONE
            timestamp_micros: None,
        }
    }

    pub fn with_consistency(mut self, cl: u16) -> Self {
        self.consistency_level = cl;
        self
    }

    pub fn with_timestamp(mut self, ts: i64) -> Self {
        self.timestamp_micros = Some(ts);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn localhost() -> SocketAddr {
        "127.0.0.1:9042".parse().unwrap()
    }

    #[test]
    fn new_client_state() {
        let cs = ClientState::new(localhost());
        assert_eq!(cs.remote_address().port(), 9042);
        assert!(cs.authenticated_user().is_none());
        assert!(cs.keyspace().is_none());
        assert_eq!(cs.protocol_version(), 0);
        assert_eq!(cs.request_count(), 0);
    }

    #[test]
    fn login_and_set_keyspace() {
        let mut cs = ClientState::new(localhost());
        cs.set_authenticated_user("cassandra".to_string());
        cs.set_keyspace("system".to_string());
        cs.set_protocol_version(4);
        cs.set_driver_name("rust-driver".to_string());
        cs.set_driver_version("0.1.0".to_string());

        assert_eq!(cs.authenticated_user(), Some("cassandra"));
        assert_eq!(cs.keyspace(), Some("system"));
        assert_eq!(cs.protocol_version(), 4);
        assert_eq!(cs.driver_name(), Some("rust-driver"));
        assert_eq!(cs.driver_version(), Some("0.1.0"));
    }

    #[test]
    fn request_counter() {
        let cs = ClientState::new(localhost());
        assert_eq!(cs.increment_request_count(), 1);
        assert_eq!(cs.increment_request_count(), 2);
        assert_eq!(cs.request_count(), 2);
    }

    #[test]
    fn elapsed_is_nonnegative() {
        let cs = ClientState::new(localhost());
        assert!(cs.elapsed().as_nanos() >= 0);
    }

    #[test]
    fn query_state_basics() {
        let cs = ClientState::new(localhost());
        let qs = QueryState::new(&cs).with_consistency(10).with_timestamp(12345);
        assert_eq!(qs.consistency_level, 10);
        assert_eq!(qs.timestamp_micros, Some(12345));
        assert_eq!(qs.client_state.remote_address(), localhost());
    }
}
