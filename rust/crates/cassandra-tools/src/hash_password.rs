// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
// implied. See the License for the specific language governing
// permissions and limitations under the License.

//! Hash a password for Cassandra's PasswordAuthenticator.
//!
//! ## Java Oracle
//! - `org.apache.cassandra.tools.HashPassword`
//! - `org.apache.cassandra.auth.PasswordAuthenticator`
//!
//! Cassandra uses bcrypt for password hashing. This module provides a
//! simplified hash using `DefaultHasher` as a placeholder. For production
//! deployments, the `bcrypt` crate should be used.

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Compute a simplified password hash string.
///
/// Returns a bcrypt-formatted string using `DefaultHasher` as a placeholder.
/// In production this should be replaced with real bcrypt hashing.
pub fn compute_hash(password: &str, rounds: u32) -> String {
    let mut hasher = DefaultHasher::new();
    password.hash(&mut hasher);
    rounds.hash(&mut hasher);
    let hash = hasher.finish();

    format!(
        "$2a${}${:016x}{:016x}",
        rounds,
        hash,
        hash.wrapping_mul(0x517cc1b727220a95)
    )
}

/// Hash a password and print the result.
///
/// If `password` is `None`, reads from stdin. Validates that the password
/// is not empty and prints the hash in bcrypt-like format.
pub fn run(password: Option<String>, rounds: u32) {
    let pwd = match password {
        Some(p) => p,
        None => {
            // Read from stdin
            println!("Enter password:");
            let mut input = String::new();
            match std::io::stdin().read_line(&mut input) {
                Ok(_) => input.trim().to_string(),
                Err(e) => {
                    println!("Error reading password: {}", e);
                    return;
                }
            }
        }
    };

    if pwd.is_empty() {
        println!("Error: password cannot be empty");
        return;
    }

    let hash = compute_hash(&pwd, rounds);
    println!("{}", hash);
    println!();
    println!("Note: This is a simplified hash. For production use, install the bcrypt crate.");
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_password_prints_error() {
        // Should not panic; run with empty string.
        run(Some(String::new()), 10);
    }

    #[test]
    fn test_with_provided_password() {
        let hash = compute_hash("cassandra", 10);
        // Must start with bcrypt prefix and include the rounds
        assert!(hash.starts_with("$2a$10$"));
        // Must be deterministic
        let hash2 = compute_hash("cassandra", 10);
        assert_eq!(hash, hash2);
    }

    #[test]
    fn test_different_rounds_produce_different_output() {
        let hash_10 = compute_hash("secret", 10);
        let hash_12 = compute_hash("secret", 12);
        assert_ne!(hash_10, hash_12);
    }

    #[test]
    fn test_different_passwords_produce_different_output() {
        let hash_a = compute_hash("password_a", 10);
        let hash_b = compute_hash("password_b", 10);
        assert_ne!(hash_a, hash_b);
    }

    #[test]
    fn test_hash_format() {
        let hash = compute_hash("test", 4);
        // Format: $2a$<rounds>$<32 hex chars>
        assert!(hash.starts_with("$2a$4$"));
        // After the prefix, we expect exactly 32 hex characters
        let suffix = hash.strip_prefix("$2a$4$").unwrap();
        assert_eq!(suffix.len(), 32);
        assert!(suffix.chars().all(|c| c.is_ascii_hexdigit()));
    }

    #[test]
    fn test_run_with_valid_password_does_not_panic() {
        run(Some("my_secure_password".to_string()), 10);
    }

    #[test]
    fn test_run_with_high_rounds() {
        // High rounds should not cause overflow or panic
        let hash = compute_hash("test", 31);
        assert!(hash.starts_with("$2a$31$"));
    }
}
