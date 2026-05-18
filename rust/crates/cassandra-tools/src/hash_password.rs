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
//! Cassandra uses bcrypt for password hashing.

/// Compute a bcrypt password hash string.
pub fn compute_hash(password: &str, rounds: u32) -> Result<String, bcrypt::BcryptError> {
    bcrypt::hash(password, rounds)
}

/// Hash a password and print the result.
///
/// If `password` is `None`, reads from stdin. Validates that the password
/// is not empty and prints the hash in bcrypt format.
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

    match compute_hash(&pwd, rounds) {
        Ok(hash) => println!("{}", hash),
        Err(e) => println!("Error hashing password: {}", e),
    }
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
        let hash = compute_hash("cassandra", 4).unwrap();
        assert!(hash.starts_with("$2"));
        assert!(bcrypt::verify("cassandra", &hash).unwrap());
    }

    #[test]
    fn test_different_rounds_produce_different_output() {
        let hash_4 = compute_hash("secret", 4).unwrap();
        let hash_5 = compute_hash("secret", 5).unwrap();
        assert_ne!(hash_4, hash_5);
        assert!(bcrypt::verify("secret", &hash_4).unwrap());
        assert!(bcrypt::verify("secret", &hash_5).unwrap());
    }

    #[test]
    fn test_different_passwords_produce_different_output() {
        let hash_a = compute_hash("password_a", 4).unwrap();
        let hash_b = compute_hash("password_b", 4).unwrap();
        assert_ne!(hash_a, hash_b);
        assert!(bcrypt::verify("password_a", &hash_a).unwrap());
        assert!(bcrypt::verify("password_b", &hash_b).unwrap());
    }

    #[test]
    fn test_hash_format() {
        let hash = compute_hash("test", 4).unwrap();
        assert!(hash.starts_with("$2"));
        assert!(hash.contains("$04$"));
        assert!(bcrypt::verify("test", &hash).unwrap());
    }

    #[test]
    fn test_run_with_valid_password_does_not_panic() {
        run(Some("my_secure_password".to_string()), 4);
    }

    #[test]
    fn test_invalid_rounds_return_error() {
        assert!(compute_hash("test", 3).is_err());
    }
}
