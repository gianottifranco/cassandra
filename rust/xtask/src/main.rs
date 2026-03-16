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

//! # xtask
//!
//! Build and test automation for the Cassandra Rust rewrite.
//!
//! Usage:
//!   cargo xtask diff-test       Run the full differential test suite
//!   cargo xtask golden-test     Run offline golden tests (no Docker)
//!   cargo xtask generate-golden Generate golden fixtures from Java oracle
//!   cargo xtask help            Show help

use std::process::{Command, ExitCode, Stdio};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();

    match args.first().map(|s| s.as_str()) {
        Some("diff-test") => cmd_diff_test(),
        Some("golden-test") => cmd_golden_test(),
        Some("generate-golden") => cmd_generate_golden(),
        Some("coverage-audit") => cmd_coverage_audit(),
        Some("help") | Some("--help") | Some("-h") | None => {
            print_help();
            ExitCode::SUCCESS
        }
        Some(unknown) => {
            eprintln!("Unknown command: {}", unknown);
            print_help();
            ExitCode::FAILURE
        }
    }
}

fn print_help() {
    println!(
        r#"
Cassandra Rust Rewrite — xtask Runner

USAGE:
    cargo xtask <COMMAND>

COMMANDS:
    diff-test         Run the full differential test suite:
                      1. Start Java oracle + Rust stub via Docker Compose
                      2. Generate golden fixtures from Java oracle
                      3. Run Rust golden tests (offline)
                      4. Run pytest diff-tests (live dual-cluster)
                      5. Stop Docker services

    golden-test       Run only offline golden tests (no Docker required).
                      Tests compare Rust output against pre-committed fixtures.

    generate-golden   Generate golden fixtures from a running Java oracle.
                      Requires Docker or a local Cassandra instance.

    coverage-audit    Audit Java→Rust coverage:
                      1. Regenerate package inventory from Java source tree
                      2. Validate gap matrix completeness
                      3. Report unclassified features
                      Fails CI if any Java package is unclassified.

    help              Show this help message.

EXAMPLES:
    cargo xtask diff-test          # Full end-to-end
    cargo xtask golden-test        # Just offline tests
    cargo xtask coverage-audit     # Check coverage completeness
    make diff-test                 # Same as cargo xtask diff-test
"#
    );
}

fn cmd_coverage_audit() -> ExitCode {
    println!("═══════════════════════════════════════════════════════════");
    println!("  Coverage Audit — Java→Rust Gap Analysis");
    println!("═══════════════════════════════════════════════════════════\n");

    let repo_root = repo_root();

    // Step 1: Regenerate package inventory
    println!("── Step 1: Regenerating Package Inventory ─────────────────\n");
    let inventory_ok = run_cmd(
        "python3",
        &[
            "scripts/coverage_audit.py",
            "--generate-inventory",
            "--repo-root",
            ".",
        ],
        Some(&repo_root),
    );

    if !inventory_ok {
        eprintln!("❌ Failed to generate package inventory");
        return ExitCode::FAILURE;
    }

    // Step 2: Validate gap matrix
    println!("\n── Step 2: Validating Gap Matrix ────────────────────────\n");
    let matrix_ok = run_cmd(
        "python3",
        &[
            "scripts/coverage_audit.py",
            "--check-matrix",
            "--repo-root",
            ".",
        ],
        Some(&repo_root),
    );

    // Step 3: Check gap matrix YAML exists and is non-empty
    println!("\n── Step 3: Checking Matrix YAML ─────────────────────────\n");
    let matrix_path = repo_root.join("docs/rewrite/final_gap_matrix.yaml");
    let yaml_ok = if matrix_path.exists() {
        let content = std::fs::read_to_string(&matrix_path).unwrap_or_default();
        if content.contains("status:") {
            println!("  ✅ Gap matrix YAML exists and contains entries");
            true
        } else {
            eprintln!("  ❌ Gap matrix YAML exists but has no status entries");
            false
        }
    } else {
        eprintln!("  ❌ Gap matrix YAML not found at {:?}", matrix_path);
        false
    };

    // Summary
    println!("\n═══════════════════════════════════════════════════════════");
    println!("  Coverage Audit Results");
    println!("═══════════════════════════════════════════════════════════");
    println!(
        "  Inventory generation: {}",
        status_icon(inventory_ok)
    );
    println!("  Matrix validation:   {}", status_icon(matrix_ok));
    println!("  Matrix YAML:         {}", status_icon(yaml_ok));
    println!("═══════════════════════════════════════════════════════════\n");

    if inventory_ok && matrix_ok && yaml_ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

/// Repo root is two levels up from xtask: rust/xtask -> rust -> cassandra
fn repo_root() -> std::path::PathBuf {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    manifest_dir
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .to_path_buf()
}

fn cmd_diff_test() -> ExitCode {
    println!("═══════════════════════════════════════════════════════════");
    println!("  Cassandra Diff Test Suite");
    println!("═══════════════════════════════════════════════════════════\n");

    // Step 1: Run offline golden tests (always available)
    println!("── Step 1: Offline Golden Tests ──────────────────────────\n");
    let golden_result = run_golden_tests();

    // Step 2: Check Docker availability
    println!("\n── Step 2: Docker Availability Check ────────────────────\n");
    let docker_available = check_docker();

    if !docker_available {
        println!("⚠️  Docker not available. Skipping live diff-tests.");
        println!("   Run 'docker compose up -d' manually, then 'cargo xtask diff-test'.\n");

        return if golden_result {
            println!("✅ Golden tests passed (Docker tests skipped)");
            ExitCode::SUCCESS
        } else {
            println!("❌ Golden tests failed");
            ExitCode::FAILURE
        };
    }

    // Step 3: Start Docker services
    println!("\n── Step 3: Starting Docker Services ────────────────────\n");
    let compose_dir = workspace_root().join("diff-tests");
    let compose_up = run_cmd(
        "docker",
        &[
            "compose",
            "-f",
            "docker-compose.yml",
            "up",
            "-d",
            "java-oracle",
            "rust-sut",
        ],
        Some(&compose_dir),
    );

    if !compose_up {
        eprintln!("❌ Failed to start Docker services");
        return ExitCode::FAILURE;
    }

    // Step 4: Wait for Java oracle to be healthy
    println!("\n── Step 4: Waiting for Java Oracle ─────────────────────\n");
    println!("   This may take 60-90 seconds on first run...");
    let oracle_ready = wait_for_service("localhost", 19042, 120);

    if !oracle_ready {
        eprintln!("❌ Java oracle did not become healthy within timeout");
        // Clean up
        run_cmd(
            "docker",
            &["compose", "-f", "docker-compose.yml", "down"],
            Some(&compose_dir),
        );
        return ExitCode::FAILURE;
    }
    println!("   ✅ Java oracle is ready");

    // Step 5: Generate golden fixtures
    println!("\n── Step 5: Generating Golden Fixtures ──────────────────\n");
    let gen_dir = compose_dir.join("golden-gen");
    let gen_result = run_cmd(
        "python3",
        &[
            "generate_golden.py",
            "--host",
            "localhost",
            "--port",
            "19042",
            "--output",
            "../golden",
        ],
        Some(&gen_dir),
    );

    if !gen_result {
        eprintln!("⚠️  Golden fixture generation had issues (continuing anyway)");
    }

    // Step 6: Run Rust golden tests again (with fresh fixtures)
    println!("\n── Step 6: Rust Golden Tests (with fresh fixtures) ─────\n");
    let golden_result_2 = run_golden_tests();

    // Step 7: Run pytest diff-tests
    println!("\n── Step 7: Pytest Diff-Tests ────────────────────────────\n");
    let pytest_dir = compose_dir.join("pytest");
    let pytest_result = if pytest_dir.exists() {
        run_cmd(
            "python3",
            &["-m", "pytest", "-v", "--tb=short", "-x"],
            Some(&pytest_dir),
        )
    } else {
        println!("   ⚠️  Pytest directory not found, skipping");
        true
    };

    // Step 8: Teardown
    println!("\n── Step 8: Stopping Docker Services ───────────────────\n");
    run_cmd(
        "docker",
        &["compose", "-f", "docker-compose.yml", "down"],
        Some(&compose_dir),
    );

    // Summary
    println!("\n═══════════════════════════════════════════════════════════");
    println!("  Results Summary");
    println!("═══════════════════════════════════════════════════════════");
    println!(
        "  Golden tests:  {}",
        status_icon(golden_result && golden_result_2)
    );
    println!("  Docker tests:  {}", status_icon(pytest_result));
    println!("═══════════════════════════════════════════════════════════\n");

    if golden_result && golden_result_2 && pytest_result {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn cmd_golden_test() -> ExitCode {
    println!("Running offline golden tests...\n");
    if run_golden_tests() {
        println!("\n✅ All golden tests passed");
        ExitCode::SUCCESS
    } else {
        println!("\n❌ Some golden tests failed");
        ExitCode::FAILURE
    }
}

fn cmd_generate_golden() -> ExitCode {
    println!("Generating golden fixtures...\n");

    let compose_dir = workspace_root().join("diff-tests");
    let gen_dir = compose_dir.join("golden-gen");

    // Try localhost first, then Docker
    let host = std::env::var("CASSANDRA_HOST").unwrap_or_else(|_| "localhost".into());
    let port = std::env::var("CASSANDRA_PORT").unwrap_or_else(|_| "9042".into());

    let result = run_cmd(
        "python3",
        &[
            "generate_golden.py",
            "--host",
            &host,
            "--port",
            &port,
            "--output",
            "../golden",
        ],
        Some(&gen_dir),
    );

    if result {
        println!("\n✅ Golden fixtures generated");
        ExitCode::SUCCESS
    } else {
        eprintln!("\n❌ Golden fixture generation failed");
        ExitCode::FAILURE
    }
}

// ── Helpers ──────────────────────────────────────────────────────────────

fn workspace_root() -> std::path::PathBuf {
    let manifest_dir = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    // xtask is at rust/xtask/, workspace root is rust/
    manifest_dir.parent().unwrap().to_path_buf()
}

fn run_golden_tests() -> bool {
    run_cmd(
        "cargo",
        &["test", "-p", "cassandra-diff-tests", "--", "--nocapture"],
        Some(&workspace_root()),
    )
}

fn run_cmd(program: &str, args: &[&str], cwd: Option<&std::path::Path>) -> bool {
    let mut cmd = Command::new(program);
    cmd.args(args)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());

    if let Some(dir) = cwd {
        cmd.current_dir(dir);
    }

    println!("  $ {} {}", program, args.join(" "));

    match cmd.status() {
        Ok(status) => status.success(),
        Err(e) => {
            eprintln!("  Failed to run '{}': {}", program, e);
            false
        }
    }
}

fn check_docker() -> bool {
    Command::new("docker")
        .args(["info"])
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|s| s.success())
        .unwrap_or(false)
}

fn wait_for_service(host: &str, port: u16, timeout_secs: u32) -> bool {
    use std::net::TcpStream;
    use std::time::Duration;

    let addr = format!("{}:{}", host, port);
    for i in 0..timeout_secs {
        if TcpStream::connect_timeout(&addr.parse().unwrap(), Duration::from_secs(1)).is_ok() {
            return true;
        }
        if i % 10 == 0 {
            println!("   Waiting... ({}/{}s)", i, timeout_secs);
        }
        std::thread::sleep(Duration::from_secs(1));
    }
    false
}

fn status_icon(passed: bool) -> &'static str {
    if passed { "✅ PASS" } else { "❌ FAIL" }
}
