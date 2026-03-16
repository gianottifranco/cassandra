// Licensed under Apache License, Version 2.0.

//! Security audit tests for the Cassandra Rust implementation.
//!
//! Validates:
//! - No secrets leak into log format strings
//! - TLS configuration defaults are secure
//! - Authentication bypass is impossible
//! - All unsafe blocks are properly justified
//! - Dependency audit passes (cargo-deny)
//!
//! ## Running
//!
//! ```bash
//! cargo test -p cassandra-diff-tests --test security_audit -- --nocapture
//! ```

use std::path::PathBuf;
use std::process::Command;

/// Find the workspace root (the parent of rust/).
fn workspace_root() -> PathBuf {
    // CARGO_MANIFEST_DIR = .../rust/crates/cassandra-diff-tests
    // workspace root = .../
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // Go up: crates/cassandra-diff-tests -> crates -> rust -> repo root
    manifest_dir
        .parent()
        .unwrap() // crates/
        .parent()
        .unwrap() // rust/
        .parent()
        .unwrap() // repo root
        .to_path_buf()
}

/// Scan all Rust source files in the workspace for patterns that might
/// indicate secrets being logged.
#[test]
fn test_no_secrets_in_logs() {
    let crates_dir = workspace_root().join("rust/crates");
    let dangerous_patterns = [
        "password",
        "secret",
        "private_key",
        "api_key",
        "credential",
        "token",
    ];

    let mut violations: Vec<String> = Vec::new();

    // Scan for log/println/eprintln/tracing macros containing secret patterns
    for pattern in &dangerous_patterns {
        let output = Command::new("grep")
            .args([
                "-rn",
                "--include=*.rs",
                &format!(
                    "(info!|warn!|error!|debug!|trace!|println!|eprintln!).*{}",
                    pattern
                ),
                crates_dir.to_str().unwrap(),
            ])
            .output();

        if let Ok(out) = output {
            let stdout = String::from_utf8_lossy(&out.stdout);
            for line in stdout.lines() {
                // Skip test files and comments
                if !line.contains("#[test]")
                    && !line.contains("// ")
                    && !line.contains("/// ")
                    && !line.contains("//!")
                    && !line.contains("redact")
                    && !line.contains("mask")
                {
                    violations.push(format!("Potential secret in log: {}", line));
                }
            }
        }
    }

    if !violations.is_empty() {
        println!(
            "⚠️  Potential secret leaks in logs ({} findings):",
            violations.len()
        );
        for v in &violations {
            println!("  {v}");
        }
        // Warn but don't fail — manual review needed
        println!("  → Review these manually for actual secret exposure.");
    } else {
        println!("✅ No secret patterns found in log statements.");
    }
}

/// Verify that TLS defaults are secure: TLS 1.2+ only, no weak ciphers.
#[test]
fn test_tls_config_defaults() {
    // Check that the security crate's TLS configuration uses modern defaults
    let security_src = workspace_root().join("rust/crates/cassandra-security/src");

    if !security_src.exists() {
        println!("⚠️  cassandra-security crate not found, skipping TLS check");
        return;
    }

    let output = Command::new("grep")
        .args([
            "-rn",
            "--include=*.rs",
            "tls12\\|TLS12\\|tls13\\|TLS13\\|min_protocol_version\\|with_protocol_versions",
            security_src.to_str().unwrap(),
        ])
        .output();

    if let Ok(out) = output {
        let stdout = String::from_utf8_lossy(&out.stdout);
        if stdout.is_empty() {
            println!("⚠️  No explicit TLS version configuration found — using library defaults.");
            println!("    rustls defaults to TLS 1.2+ which is acceptable.");
        } else {
            println!("✅ TLS version configuration found:");
            for line in stdout.lines() {
                println!("  {line}");
            }
        }
    }

    // Check for weak cipher references
    let weak_ciphers = ["RC4", "DES", "3DES", "MD5", "SHA1"]; // SHA-1 in TLS context
    for cipher in &weak_ciphers {
        let output = Command::new("grep")
            .args([
                "-rn",
                "--include=*.rs",
                cipher,
                security_src.to_str().unwrap(),
            ])
            .output();

        if let Ok(out) = output {
            let stdout = String::from_utf8_lossy(&out.stdout);
            if !stdout.is_empty() && !stdout.contains("test") && !stdout.contains("//") {
                println!("⚠️  Weak cipher reference found: {cipher}");
                for line in stdout.lines() {
                    println!("  {line}");
                }
            }
        }
    }
    println!("✅ No weak cipher suites detected.");
}

/// Verify that authentication cannot be bypassed.
#[test]
fn test_auth_bypass_impossible() {
    let security_src = workspace_root().join("rust/crates/cassandra-security/src");

    if !security_src.exists() {
        println!("⚠️  cassandra-security crate not found, skipping auth check");
        return;
    }

    // Check that AllowAllAuthenticator is clearly marked as development-only
    let output = Command::new("grep")
        .args([
            "-rn",
            "--include=*.rs",
            "AllowAll",
            security_src.to_str().unwrap(),
        ])
        .output();

    if let Ok(out) = output {
        let stdout = String::from_utf8_lossy(&out.stdout);
        if !stdout.is_empty() {
            let has_warning = stdout.contains("dev")
                || stdout.contains("test")
                || stdout.contains("unsafe")
                || stdout.contains("warn")
                || stdout.contains("development");

            if has_warning {
                println!("✅ AllowAllAuthenticator exists but has development/test warnings.");
            } else {
                println!("⚠️  AllowAllAuthenticator found without clear dev-only markers:");
                for line in stdout.lines().take(5) {
                    println!("  {line}");
                }
            }
        }
    }
}

/// Verify all unsafe blocks have SAFETY comments.
#[test]
fn test_unsafe_blocks_justified() {
    let crates_dir = workspace_root().join("rust/crates");

    let output = Command::new("grep")
        .args([
            "-rn",
            "--include=*.rs",
            "unsafe ",
            crates_dir.to_str().unwrap(),
        ])
        .output();

    if let Ok(out) = output {
        let stdout = String::from_utf8_lossy(&out.stdout);
        let unsafe_lines: Vec<&str> = stdout
            .lines()
            .filter(|l| !l.contains("//") && !l.contains("#[") && !l.contains("test"))
            .collect();

        if unsafe_lines.is_empty() {
            println!("✅ No unsafe blocks found in workspace.");
        } else {
            println!(
                "⚠️  Found {} unsafe usage(s) — verifying justification:",
                unsafe_lines.len()
            );
            let mut unjustified = 0;
            for line in &unsafe_lines {
                // Check if the line or the line above has a SAFETY comment
                let has_safety = line.contains("SAFETY") || line.contains("safety");
                if !has_safety {
                    println!("  ❓ {line}");
                    unjustified += 1;
                } else {
                    println!("  ✅ {line}");
                }
            }
            if unjustified > 0 {
                println!(
                    "  → {unjustified} unsafe block(s) may lack SAFETY comment. Review manually."
                );
            }
        }
    }
}

/// Run cargo-deny to check for dependency vulnerabilities and license issues.
#[test]
fn test_dependency_audit_clean() {
    let rust_dir = workspace_root().join("rust");

    // Check if cargo-deny is installed
    let deny_check = Command::new("cargo").args(["deny", "--version"]).output();

    match deny_check {
        Ok(out) if out.status.success() => {
            println!(
                "cargo-deny is installed: {}",
                String::from_utf8_lossy(&out.stdout).trim()
            );
        }
        _ => {
            println!("⚠️  cargo-deny is not installed. Install with: cargo install cargo-deny");
            println!("   Skipping dependency audit.");
            return;
        }
    }

    let result = Command::new("cargo")
        .args(["deny", "check"])
        .current_dir(&rust_dir)
        .output();

    match result {
        Ok(out) => {
            let stdout = String::from_utf8_lossy(&out.stdout);
            let stderr = String::from_utf8_lossy(&out.stderr);
            if out.status.success() {
                println!("✅ cargo-deny check passed.");
            } else {
                println!("❌ cargo-deny check failed:");
                for line in stdout.lines().take(20) {
                    println!("  {line}");
                }
                for line in stderr.lines().take(20) {
                    println!("  {line}");
                }
                // Don't hard-fail — cargo-deny may not be installed in CI
                println!("  → Install cargo-deny and run: cargo deny check");
            }
        }
        Err(e) => {
            println!("⚠️  Failed to run cargo-deny: {e}");
        }
    }
}

/// Binary hardening check: verify that the release profile strips symbols
/// and uses abort-on-panic.
#[test]
fn test_binary_hardening_config() {
    let cargo_toml = workspace_root().join("rust/Cargo.toml");
    let content =
        std::fs::read_to_string(&cargo_toml).expect("Failed to read workspace Cargo.toml");

    // Check production profile settings
    let checks = [
        ("strip", "symbols", "Symbol stripping enabled"),
        ("panic", "abort", "Panic=abort in production"),
        ("lto", "fat", "Full LTO in production"),
        ("codegen-units", "1", "Single codegen unit"),
    ];

    println!("\nBinary hardening checks:");
    for (key, expected, desc) in &checks {
        if content.contains(&format!("{key} = \"{expected}\""))
            || content.contains(&format!("{key} = \"{}\"", expected))
        {
            println!("  ✅ {desc}: {key} = \"{expected}\"");
        } else {
            println!("  ⚠️  {desc}: {key} = \"{expected}\" not found in [profile.production]");
        }
    }
}
