// Licensed under Apache License, Version 2.0.

//! Per-connection client state and per-query state.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.transport.ClientStat`
//! - `org.apache.cassandra.service.QueryState`

use std::net::SocketAddr;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Per-connection state tracked across the lifetime of a client connection.
#[derive(Debug)]
pub struct ClientState {
    /// Remote socket address of the connected client. `None` for internal calls.
    remote_address: Option<SocketAddr>,
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
    /// Last assigned timestamp for monotonicity (microseconds since epoch).
    last_timestamp_micros: AtomicI64,
    /// Whether this is an internal (superuser) client.
    is_internal: bool,
}

impl ClientState {
    /// Create a new `ClientState` for an accepted external connection.
    pub fn new(remote_address: SocketAddr) -> Self {
        Self::for_external_calls(remote_address)
    }

    /// Create state for an external client connection.
    pub fn for_external_calls(remote_address: SocketAddr) -> Self {
        Self {
            remote_address: Some(remote_address),
            authenticated_user: None,
            keyspace: None,
            protocol_version: 0,
            driver_name: None,
            driver_version: None,
            request_count: AtomicU64::new(0),
            connected_at: Instant::now(),
            last_timestamp_micros: AtomicI64::new(0),
            is_internal: false,
        }
    }

    /// Create state for internal system queries (superuser, no address).
    pub fn for_internal_calls() -> Self {
        Self {
            remote_address: None,
            authenticated_user: Some("system".to_string()),
            keyspace: None,
            protocol_version: 4,
            driver_name: None,
            driver_version: None,
            request_count: AtomicU64::new(0),
            connected_at: Instant::now(),
            last_timestamp_micros: AtomicI64::new(0),
            is_internal: true,
        }
    }

    pub fn remote_address(&self) -> SocketAddr {
        self.remote_address
            .unwrap_or_else(|| "127.0.0.1:0".parse().unwrap())
    }

    pub fn remote_address_opt(&self) -> Option<SocketAddr> {
        self.remote_address
    }

    pub fn is_internal(&self) -> bool {
        self.is_internal
    }

    pub fn authenticated_user(&self) -> Option<&str> {
        self.authenticated_user.as_deref()
    }

    pub fn set_authenticated_user(&mut self, user: String) {
        self.authenticated_user = Some(user);
    }

    /// Alias for `set_authenticated_user`.
    pub fn login(&mut self, user: String) {
        self.set_authenticated_user(user);
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

    /// Get a monotonically increasing timestamp in microseconds.
    ///
    /// Matches Java `ClientState.getTimestamp()`: CAS loop ensuring
    /// the returned timestamp is always strictly greater than the last.
    pub fn get_timestamp(&self) -> i64 {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as i64;

        loop {
            let last = self.last_timestamp_micros.load(Ordering::Acquire);
            let candidate = if now > last { now } else { last + 1 };
            match self.last_timestamp_micros.compare_exchange_weak(
                last,
                candidate,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => return candidate,
                Err(_) => continue,
            }
        }
    }
}

/// Per-query state: owns the connection's `ClientState` reference plus
/// per-request parameters (consistency level, timestamp, etc.).
pub struct QueryState<'a> {
    pub client_state: &'a ClientState,
    pub consistency_level: u16,
    pub timestamp_micros: Option<i64>,
    pub now_in_seconds: Option<i32>,
    pub trace_enabled: bool,
    pub trace_id: Option<uuid::Uuid>,
}

impl<'a> QueryState<'a> {
    pub fn new(client_state: &'a ClientState) -> Self {
        Self {
            client_state,
            consistency_level: 1, // ONE
            timestamp_micros: None,
            now_in_seconds: None,
            trace_enabled: false,
            trace_id: None,
        }
    }

    /// Create a QueryState for internal system queries.
    pub fn for_internal_calls(client_state: &'a ClientState) -> Self {
        Self {
            client_state,
            consistency_level: 1, // ONE
            timestamp_micros: None,
            now_in_seconds: None,
            trace_enabled: false,
            trace_id: None,
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

    /// Get the timestamp, lazily fetching from ClientState if not set.
    pub fn get_timestamp(&self) -> i64 {
        self.timestamp_micros
            .unwrap_or_else(|| self.client_state.get_timestamp())
    }

    /// Get now_in_seconds, lazily computing if not set.
    pub fn get_now_in_seconds(&self) -> i32 {
        self.now_in_seconds.unwrap_or_else(|| {
            (SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs()) as i32
        })
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
        assert!(!cs.is_internal());
    }

    #[test]
    fn for_internal_calls() {
        let cs = ClientState::for_internal_calls();
        assert!(cs.is_internal());
        assert_eq!(cs.authenticated_user(), Some("system"));
        assert!(cs.remote_address_opt().is_none());
        assert_eq!(cs.protocol_version(), 4);
    }

    #[test]
    fn for_external_calls() {
        let cs = ClientState::for_external_calls(localhost());
        assert!(!cs.is_internal());
        assert_eq!(cs.remote_address_opt(), Some(localhost()));
    }

    #[test]
    fn login_and_set_keyspace() {
        let mut cs = ClientState::new(localhost());
        cs.login("cassandra".to_string());
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
    fn monotonic_timestamp() {
        let cs = ClientState::new(localhost());
        let t1 = cs.get_timestamp();
        let t2 = cs.get_timestamp();
        let t3 = cs.get_timestamp();
        assert!(t2 > t1);
        assert!(t3 > t2);
    }

    #[test]
    fn query_state_basics() {
        let cs = ClientState::new(localhost());
        let qs = QueryState::new(&cs).with_consistency(10).with_timestamp(12345);
        assert_eq!(qs.consistency_level, 10);
        assert_eq!(qs.timestamp_micros, Some(12345));
        assert_eq!(qs.client_state.remote_address(), localhost());
        assert!(!qs.trace_enabled);
        assert!(qs.trace_id.is_none());
    }

    #[test]
    fn query_state_get_timestamp_uses_explicit() {
        let cs = ClientState::new(localhost());
        let qs = QueryState::new(&cs).with_timestamp(42);
        assert_eq!(qs.get_timestamp(), 42);
    }

    #[test]
    fn query_state_get_timestamp_lazy() {
        let cs = ClientState::new(localhost());
        let qs = QueryState::new(&cs);
        let ts = qs.get_timestamp();
        assert!(ts > 0);
    }

    #[test]
    fn query_state_for_internal_calls() {
        let cs = ClientState::for_internal_calls();
        let qs = QueryState::for_internal_calls(&cs);
        assert_eq!(qs.consistency_level, 1);
        assert!(qs.client_state.is_internal());
    }

    #[test]
    fn query_state_get_now_in_seconds() {
        let cs = ClientState::new(localhost());
        let qs = QueryState::new(&cs);
        let now = qs.get_now_in_seconds();
        assert!(now > 0);
    }
}
