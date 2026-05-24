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
//!   cargo xtask diff-test-rust-only Run Rust-only diff suite (no Java oracle)
//!   cargo xtask golden-test     Run offline golden tests (no Docker)
//!   cargo xtask generate-golden Generate golden fixtures from Java oracle
//!   cargo xtask help            Show help

use std::path::PathBuf;
use std::process::{Command, ExitCode, Stdio};

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();

    match args.first().map(|s| s.as_str()) {
        Some("diff-test") => cmd_diff_test(),
        Some("diff-test-rust-only") => cmd_diff_test_rust_only(),
        Some("golden-test") => cmd_golden_test(),
        Some("generate-golden") => cmd_generate_golden(),
        Some("coverage-audit") => cmd_coverage_audit(),
        Some("java-code-audit") => cmd_java_code_audit(false),
        Some("java-code-audit-strict") => cmd_java_code_audit(true),
        Some("final-validate") => cmd_final_validate(false),
        Some("final-validate-strict") => cmd_final_validate(true),
        Some("capture-evidence") => cmd_capture_evidence(false),
        Some("capture-evidence-strict") => cmd_capture_evidence(true),
        Some("phase26-validate") => cmd_phase26_validate(false),
        Some("phase26-validate-strict") => cmd_phase26_validate(true),
        Some("phase26-validate-rust-only") => cmd_phase26_validate_rust_only(false),
        Some("phase26-validate-rust-only-strict") => cmd_phase26_validate_rust_only(true),
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
                      Set CASSANDRA_NO_JAVA_ORACLE=1 to run the Rust-only
                      differential suite without Java oracle dependencies.

    diff-test-rust-only
                      Run Rust-only differential suite:
                      1. Offline golden fixture verification
                      2. Differential fuzz tests
                      3. Differential error/tombstone/protocol comparators
                      No Docker or Java oracle required.

    golden-test       Run only offline golden tests (no Docker required).
                      Tests compare Rust output against pre-committed fixtures.

    generate-golden   Generate golden fixtures from a running Java oracle.
                      Requires Docker or a local Cassandra instance.

    coverage-audit    Audit Java→Rust coverage:
                      1. Regenerate package inventory from Java source tree
                      2. Validate gap matrix completeness
                      3. Report unclassified features
                      Fails CI if any Java package is unclassified.

    java-code-audit   Audit remaining Java files in the repository.
                      Advisory by default; reports counts by path group.

    java-code-audit-strict
                      Same as java-code-audit, but fails if any Java
                      file remains.

    final-validate    Run full Phase 25 validation:
                      1. Chaos tests (10 scenarios)
                      2. Soak tests (endurance scenarios)
                      3. Performance budget checks
                      4. Property-based fuzz tests
                      5. Security audit tests
                      6. Golden baseline checks
                      Set CASSANDRA_STRICT_PERF_BUDGET=1 to fail on
                      perf budget overruns (default is advisory warnings).
                      Reports pass/fail summary for each category.

    final-validate-strict
                      Same as final-validate, but forces strict
                      performance budget gating.

    capture-evidence  Capture Phase 26 test evidence logs and
                      summary JSON in rust/evidence/phase-26/.

    capture-evidence-strict
                      Same as capture-evidence, but forces strict
                      performance budget gating.

    phase26-validate  Run full Phase 26 validation:
                      1. Workspace quality checks (fmt/clippy/build/test)
                      2. Chaos, soak, perf, fuzz, security, backup tests
                      3. Evidence capture
                      4. Single-node cluster validation

    phase26-validate-strict
                      Same as phase26-validate, but forces strict
                      performance budget gating.

    phase26-validate-rust-only
                      Run Java-free validation:
                      1. Phase 26 validation suite
                      2. Rust-only differential suite
                      3. Coverage audit
                      No Java oracle or Docker required.

    phase26-validate-rust-only-strict
                      Same as phase26-validate-rust-only, but forces
                      strict performance budget gating.

    help              Show this help message.

EXAMPLES:
    cargo xtask diff-test          # Full end-to-end
    cargo xtask diff-test-rust-only # Rust-only differential checks
    cargo xtask golden-test        # Just offline tests
    cargo xtask coverage-audit     # Check coverage completeness
    cargo xtask java-code-audit    # Report remaining Java files
    cargo xtask final-validate     # Full Phase 25 validation
    cargo xtask final-validate-strict # Full validation + strict perf gates
    cargo xtask capture-evidence   # Capture Phase 26 evidence logs
    cargo xtask phase26-validate   # Full Phase 26 validation
    cargo xtask phase26-validate-rust-only # Java-free full validation
    make diff-test                 # Same as cargo xtask diff-test
    make diff-test-rust-only       # Same as cargo xtask diff-test-rust-only
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

    // Step 2: Validate gap matrix (strict mode — sub-package level)
    println!("\n── Step 2: Validating Gap Matrix (strict) ──────────────\n");
    let matrix_ok = run_cmd(
        "python3",
        &[
            "scripts/coverage_audit.py",
            "--check-matrix",
            "--strict",
            "--repo-root",
            ".",
        ],
        Some(&repo_root),
    );

    // Step 3: Get JSON summary for structured output
    println!("\n── Step 3: Structured Summary ───────────────────────────\n");
    let json_output = run_cmd_capture(
        "python3",
        &[
            "scripts/coverage_audit.py",
            "--check-matrix",
            "--strict",
            "--json",
            "--repo-root",
            ".",
        ],
        Some(&repo_root),
    );

    let json_ok = if let Some(ref output) = json_output {
        match parse_audit_json(output) {
            Some(summary) => {
                println!("  Total features:        {}", summary.total_features);
                println!("  Total packages:        {}", summary.total_packages);
                println!("  Classified packages:   {}", summary.classified_packages);
                println!("  Unclassified packages: {}", summary.unclassified_count);
                println!("  Orphan entries:        {}", summary.orphan_count);
                println!();
                if !summary.status_counts.is_empty() {
                    println!("  Status breakdown:");
                    for (status, count) in &summary.status_counts {
                        println!("    {}: {}", status, count);
                    }
                }
                summary.passed
            }
            None => {
                eprintln!("  ⚠️  Could not parse JSON output");
                false
            }
        }
    } else {
        eprintln!("  ⚠️  JSON audit command failed");
        false
    };

    // Step 4: Check YAML validity
    println!("\n── Step 4: Checking Matrix YAML ─────────────────────────\n");
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
    println!("  Inventory generation: {}", status_icon(inventory_ok));
    println!("  Matrix validation:   {}", status_icon(matrix_ok));
    println!("  JSON summary:        {}", status_icon(json_ok));
    println!("  Matrix YAML:         {}", status_icon(yaml_ok));
    println!("═══════════════════════════════════════════════════════════\n");

    if inventory_ok && matrix_ok && yaml_ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

fn cmd_java_code_audit(strict: bool) -> ExitCode {
    println!("═══════════════════════════════════════════════════════════");
    println!("  Java Code Audit");
    println!("═══════════════════════════════════════════════════════════\n");

    let repo = repo_root();
    let mut args = vec!["scripts/java_code_audit.py", "--repo-root", "."];
    if strict {
        args.push("--strict");
    }

    let ok = run_cmd("python3", &args, Some(&repo));
    if ok {
        ExitCode::SUCCESS
    } else {
        ExitCode::FAILURE
    }
}

struct AuditSummary {
    passed: bool,
    total_features: u64,
    total_packages: u64,
    classified_packages: u64,
    unclassified_count: u64,
    orphan_count: u64,
    status_counts: Vec<(String, u64)>,
}

fn parse_audit_json(json_str: &str) -> Option<AuditSummary> {
    // Minimal JSON parsing without serde dependency
    let passed = json_str.contains("\"passed\": true");
    let total_features = extract_json_number(json_str, "total_features");
    let total_packages = extract_json_number(json_str, "total_packages");
    let classified_packages = extract_json_number(json_str, "classified_packages");

    let orphan_count = json_str.matches("\"feature_id\"").count() as u64;

    // Extract status counts
    let mut status_counts = Vec::new();
    for status in &[
        "done",
        "partial",
        "stub",
        "missing",
        "trunk-only",
        "experimental",
        "baseline-excluded",
        "tooling-only",
        "ops-only",
        "blocked",
    ] {
        let key = format!("\"{}\": ", status);
        if let Some(pos) = json_str.find(&key) {
            let rest = &json_str[pos + key.len()..];
            if let Some(end) = rest.find([',', '}', '\n']) {
                if let Ok(n) = rest[..end].trim().parse::<u64>() {
                    if n > 0 {
                        status_counts.push((status.to_string(), n));
                    }
                }
            }
        }
    }

    Some(AuditSummary {
        passed,
        total_features,
        total_packages,
        classified_packages,
        unclassified_count: extract_json_array_len(json_str, "unclassified"),
        orphan_count,
        status_counts,
    })
}

fn extract_json_number(json_str: &str, key: &str) -> u64 {
    let pattern = format!("\"{}\": ", key);
    if let Some(pos) = json_str.find(&pattern) {
        let rest = &json_str[pos + pattern.len()..];
        if let Some(end) = rest.find([',', '}', '\n', ' ']) {
            return rest[..end].trim().parse().unwrap_or(0);
        }
    }
    0
}

fn extract_json_array_len(json_str: &str, key: &str) -> u64 {
    let pattern = format!("\"{}\": [", key);
    if let Some(pos) = json_str.find(&pattern) {
        let rest = &json_str[pos + pattern.len()..];
        if let Some(end) = rest.find(']') {
            let content = rest[..end].trim();
            if content.is_empty() {
                return 0;
            }
            return content.split(',').count() as u64;
        }
    }
    0
}

fn run_cmd_capture(program: &str, args: &[&str], cwd: Option<&std::path::Path>) -> Option<String> {
    let mut cmd = Command::new(program);
    cmd.args(args).stderr(Stdio::inherit());

    if let Some(dir) = cwd {
        cmd.current_dir(dir);
    }

    match cmd.output() {
        Ok(output) if output.status.success() => String::from_utf8(output.stdout).ok(),
        _ => None,
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
    if env_flag_enabled("CASSANDRA_NO_JAVA_ORACLE") {
        println!("CASSANDRA_NO_JAVA_ORACLE=1 detected; running Rust-only diff suite.");
        return cmd_diff_test_rust_only();
    }

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

fn cmd_diff_test_rust_only() -> ExitCode {
    println!("═══════════════════════════════════════════════════════════");
    println!("  Cassandra Diff Test Suite (Rust-Only)");
    println!("═══════════════════════════════════════════════════════════\n");

    let ws = workspace_root();

    println!("── Step 1: Offline Golden Fixture Checks ─────────────────\n");
    let golden_ok = run_golden_tests();

    println!("\n── Step 2: Differential Fuzz Suite ───────────────────────\n");
    let fuzz_ok = run_cmd(
        "cargo",
        &[
            "test",
            "-p",
            "cassandra-diff-tests",
            "--test",
            "fuzz_tests",
            "--",
            "--nocapture",
        ],
        Some(&ws),
    );

    println!("\n── Step 3: Differential Comparator Suites ────────────────\n");
    let error_ok = run_cmd(
        "cargo",
        &[
            "test",
            "-p",
            "cassandra-diff-tests",
            "--lib",
            "comparators::error::",
            "--",
            "--nocapture",
        ],
        Some(&ws),
    );
    let tombstone_ok = run_cmd(
        "cargo",
        &[
            "test",
            "-p",
            "cassandra-diff-tests",
            "--lib",
            "comparators::tombstone::",
            "--",
            "--nocapture",
        ],
        Some(&ws),
    );
    let protocol_ok = run_cmd(
        "cargo",
        &[
            "test",
            "-p",
            "cassandra-diff-tests",
            "--lib",
            "comparators::protocol::",
            "--",
            "--nocapture",
        ],
        Some(&ws),
    );

    println!("\n═══════════════════════════════════════════════════════════");
    println!("  Rust-Only Differential Results");
    println!("═══════════════════════════════════════════════════════════");
    println!("  Golden fixtures:   {}", status_icon(golden_ok));
    println!("  Fuzz tests:        {}", status_icon(fuzz_ok));
    println!("  Error diff tests:  {}", status_icon(error_ok));
    println!("  Tombstone diffs:   {}", status_icon(tombstone_ok));
    println!("  Protocol diffs:    {}", status_icon(protocol_ok));
    println!("═══════════════════════════════════════════════════════════\n");

    if golden_ok && fuzz_ok && error_ok && tombstone_ok && protocol_ok {
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
    run_cmd_with_env(program, args, &[], cwd)
}

fn run_cmd_with_env(
    program: &str,
    args: &[&str],
    envs: &[(&str, &str)],
    cwd: Option<&std::path::Path>,
) -> bool {
    let resolved_program = resolve_program(program);
    let mut cmd = Command::new(&resolved_program);
    cmd.args(args)
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit());
    for (key, value) in envs {
        cmd.env(key, value);
    }

    if let Some(dir) = cwd {
        cmd.current_dir(dir);
    }

    let env_prefix = if envs.is_empty() {
        String::new()
    } else {
        let mut rendered = String::new();
        for (i, (key, value)) in envs.iter().enumerate() {
            if i > 0 {
                rendered.push(' ');
            }
            rendered.push_str(key);
            rendered.push('=');
            rendered.push_str(value);
        }
        format!("{rendered} ")
    };
    println!("  $ {env_prefix}{resolved_program} {}", args.join(" "));

    match cmd.status() {
        Ok(status) => status.success(),
        Err(e) => {
            eprintln!("  Failed to run '{}': {}", resolved_program, e);
            false
        }
    }
}

fn resolve_program(program: &str) -> String {
    if program == "cargo" {
        if let Some(home) = std::env::var_os("HOME") {
            let candidate = PathBuf::from(home).join(".cargo/bin/cargo");
            if candidate.is_file() {
                return candidate.to_string_lossy().into_owned();
            }
        }
    }
    program.to_string()
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

fn env_flag_enabled(name: &str) -> bool {
    std::env::var(name)
        .map(|value| {
            matches!(
                value.trim().to_ascii_lowercase().as_str(),
                "1" | "true" | "yes" | "on"
            )
        })
        .unwrap_or(false)
}

fn cmd_final_validate(force_strict_perf: bool) -> ExitCode {
    println!("═══════════════════════════════════════════════════════════");
    println!("  Phase 25 — Final Validation Suite");
    println!("═══════════════════════════════════════════════════════════\n");

    let ws = workspace_root();
    let strict_perf = force_strict_perf || env_flag_enabled("CASSANDRA_STRICT_PERF_BUDGET");
    println!(
        "Perf budget mode: {}\n",
        if strict_perf {
            "STRICT (budget overruns fail test)"
        } else {
            "ADVISORY (budget overruns warn only)"
        }
    );

    // Step 1: Chaos tests
    println!("── Step 1: Chaos Tests (10 scenarios) ───────────────────\n");
    let chaos_ok = run_cmd(
        "cargo",
        &[
            "test",
            "-p",
            "cassandra-diff-tests",
            "--test",
            "chaos_tests",
            "--",
            "--nocapture",
        ],
        Some(&ws),
    );

    // Step 2: Soak tests
    println!("\n── Step 2: Soak Tests ─────────────────────────────────────\n");
    let soak_ok = run_cmd(
        "cargo",
        &[
            "test",
            "-p",
            "cassandra-diff-tests",
            "--test",
            "soak_tests",
            "--",
            "--nocapture",
        ],
        Some(&ws),
    );

    // Step 3: Performance budget tests
    println!("\n── Step 3: Performance Budgets ───────────────────────────\n");
    let perf_ok = if strict_perf {
        run_cmd_with_env(
            "cargo",
            &[
                "test",
                "-p",
                "cassandra-diff-tests",
                "--test",
                "perf_budget_tests",
                "--",
                "--nocapture",
            ],
            &[("CASSANDRA_STRICT_PERF_BUDGET", "1")],
            Some(&ws),
        )
    } else {
        run_cmd(
            "cargo",
            &[
                "test",
                "-p",
                "cassandra-diff-tests",
                "--test",
                "perf_budget_tests",
                "--",
                "--nocapture",
            ],
            Some(&ws),
        )
    };

    // Step 4: Property-based fuzz tests
    println!("\n── Step 4: Property-Based Fuzz Tests ──────────────────────\n");
    let fuzz_ok = run_cmd(
        "cargo",
        &[
            "test",
            "-p",
            "cassandra-diff-tests",
            "--test",
            "fuzz_tests",
            "--",
            "--nocapture",
        ],
        Some(&ws),
    );

    // Step 5: Security audit tests
    println!("\n── Step 5: Security Audit ──────────────────────────────────\n");
    let security_ok = run_cmd(
        "cargo",
        &[
            "test",
            "-p",
            "cassandra-diff-tests",
            "--test",
            "security_audit",
            "--",
            "--nocapture",
        ],
        Some(&ws),
    );

    // Step 6: Golden tests (baseline diff)
    println!("\n── Step 6: Golden Tests ────────────────────────────────────\n");
    let golden_ok = run_golden_tests();

    // Summary
    println!("\n═══════════════════════════════════════════════════════════");
    println!("  Phase 25 — Final Validation Results");
    println!("═══════════════════════════════════════════════════════════");
    println!("  Chaos tests:     {}", status_icon(chaos_ok));
    println!("  Soak tests:      {}", status_icon(soak_ok));
    println!("  Perf budgets:    {}", status_icon(perf_ok));
    println!("  Fuzz tests:      {}", status_icon(fuzz_ok));
    println!("  Security audit:  {}", status_icon(security_ok));
    println!("  Golden tests:    {}", status_icon(golden_ok));
    println!("═══════════════════════════════════════════════════════════\n");

    if chaos_ok && soak_ok && perf_ok && fuzz_ok && security_ok && golden_ok {
        println!("✅ Phase 25 final validation PASSED.");
        ExitCode::SUCCESS
    } else {
        println!("❌ Phase 25 final validation has FAILURES. Review above.");
        ExitCode::FAILURE
    }
}

fn cmd_capture_evidence(force_strict_perf: bool) -> ExitCode {
    println!("═══════════════════════════════════════════════════════════");
    println!("  Phase 26 — Evidence Capture");
    println!("═══════════════════════════════════════════════════════════\n");

    let ws = workspace_root();
    let strict_perf = force_strict_perf || env_flag_enabled("CASSANDRA_STRICT_PERF_BUDGET");
    println!(
        "Perf budget mode: {}\n",
        if strict_perf {
            "STRICT (budget overruns fail test)"
        } else {
            "ADVISORY (budget overruns warn only)"
        }
    );

    let ok = if strict_perf {
        run_cmd(
            "bash",
            &["scripts/capture-evidence.sh", "--strict-perf"],
            Some(&ws),
        )
    } else {
        run_cmd("bash", &["scripts/capture-evidence.sh"], Some(&ws))
    };

    if ok {
        println!("✅ Evidence capture complete.");
        ExitCode::SUCCESS
    } else {
        println!("❌ Evidence capture failed. Review logs above.");
        ExitCode::FAILURE
    }
}

fn cmd_phase26_validate(force_strict_perf: bool) -> ExitCode {
    println!("═══════════════════════════════════════════════════════════");
    println!("  Phase 26 — Final Validation Suite");
    println!("═══════════════════════════════════════════════════════════\n");

    let ws = workspace_root();
    let strict_perf = force_strict_perf || env_flag_enabled("CASSANDRA_STRICT_PERF_BUDGET");
    println!(
        "Perf budget mode: {}\n",
        if strict_perf {
            "STRICT (budget overruns fail test)"
        } else {
            "ADVISORY (budget overruns warn only)"
        }
    );

    println!("── Step 1: Workspace Checks (fmt/clippy/build/test) ─────\n");
    let fmt_ok = run_cmd("cargo", &["fmt", "--all", "--", "--check"], Some(&ws));
    let clippy_ok = run_cmd(
        "cargo",
        &["clippy", "--workspace", "--", "-D", "warnings"],
        Some(&ws),
    );
    let build_ok = run_cmd("cargo", &["build", "--workspace"], Some(&ws));
    let test_ok = run_cmd("cargo", &["test", "--workspace"], Some(&ws));

    println!("\n── Step 2: Chaos Tests ───────────────────────────────────\n");
    let chaos_ok = run_cmd(
        "cargo",
        &[
            "test",
            "-p",
            "cassandra-diff-tests",
            "--test",
            "chaos_tests",
            "--",
            "--nocapture",
        ],
        Some(&ws),
    );

    println!("\n── Step 3: Soak Tests ────────────────────────────────────\n");
    let soak_ok = run_cmd(
        "cargo",
        &[
            "test",
            "-p",
            "cassandra-diff-tests",
            "--test",
            "soak_tests",
            "--",
            "--nocapture",
        ],
        Some(&ws),
    );

    println!("\n── Step 4: Performance Budgets ───────────────────────────\n");
    let perf_ok = if strict_perf {
        run_cmd_with_env(
            "cargo",
            &[
                "test",
                "-p",
                "cassandra-diff-tests",
                "--test",
                "perf_budget_tests",
                "--",
                "--nocapture",
            ],
            &[("CASSANDRA_STRICT_PERF_BUDGET", "1")],
            Some(&ws),
        )
    } else {
        run_cmd(
            "cargo",
            &[
                "test",
                "-p",
                "cassandra-diff-tests",
                "--test",
                "perf_budget_tests",
                "--",
                "--nocapture",
            ],
            Some(&ws),
        )
    };

    println!("\n── Step 5: Property-Based Fuzz Tests ─────────────────────\n");
    let fuzz_ok = run_cmd(
        "cargo",
        &[
            "test",
            "-p",
            "cassandra-diff-tests",
            "--test",
            "fuzz_tests",
            "--",
            "--nocapture",
        ],
        Some(&ws),
    );

    println!("\n── Step 6: Security Audit Tests ──────────────────────────\n");
    let security_ok = run_cmd(
        "cargo",
        &[
            "test",
            "-p",
            "cassandra-diff-tests",
            "--test",
            "security_audit",
            "--",
            "--nocapture",
        ],
        Some(&ws),
    );

    println!("\n── Step 7: Backup/Restore Tests ──────────────────────────\n");
    let backup_ok = run_cmd(
        "cargo",
        &[
            "test",
            "-p",
            "cassandra-diff-tests",
            "--test",
            "backup_restore_tests",
            "--",
            "--nocapture",
        ],
        Some(&ws),
    );

    println!("\n── Step 8: Evidence Capture ──────────────────────────────\n");
    let evidence_ok = cmd_capture_evidence(strict_perf) == ExitCode::SUCCESS;

    println!("\n── Step 9: Cluster Validation (single-node) ─────────────\n");
    let cluster_ok = run_cmd(
        "bash",
        &["scripts/cluster-validate.sh", "--single-node-only"],
        Some(&ws),
    );

    println!("\n═══════════════════════════════════════════════════════════");
    println!("  Phase 26 — Final Validation Results");
    println!("═══════════════════════════════════════════════════════════");
    println!(
        "  Workspace checks: {}",
        status_icon(fmt_ok && clippy_ok && build_ok && test_ok)
    );
    println!("  Chaos tests:      {}", status_icon(chaos_ok));
    println!("  Soak tests:       {}", status_icon(soak_ok));
    println!("  Perf budgets:     {}", status_icon(perf_ok));
    println!("  Fuzz tests:       {}", status_icon(fuzz_ok));
    println!("  Security audit:   {}", status_icon(security_ok));
    println!("  Backup/restore:   {}", status_icon(backup_ok));
    println!("  Evidence capture: {}", status_icon(evidence_ok));
    println!("  Cluster validate: {}", status_icon(cluster_ok));
    println!("═══════════════════════════════════════════════════════════\n");

    if fmt_ok
        && clippy_ok
        && build_ok
        && test_ok
        && chaos_ok
        && soak_ok
        && perf_ok
        && fuzz_ok
        && security_ok
        && backup_ok
        && evidence_ok
        && cluster_ok
    {
        println!("✅ Phase 26 validation PASSED.");
        ExitCode::SUCCESS
    } else {
        println!("❌ Phase 26 validation has FAILURES. Review above.");
        ExitCode::FAILURE
    }
}

fn cmd_phase26_validate_rust_only(force_strict_perf: bool) -> ExitCode {
    println!("═══════════════════════════════════════════════════════════");
    println!("  Phase 26 — Java-Free Validation Suite");
    println!("═══════════════════════════════════════════════════════════\n");

    let strict_perf = force_strict_perf || env_flag_enabled("CASSANDRA_STRICT_PERF_BUDGET");
    println!(
        "Perf budget mode: {}\n",
        if strict_perf {
            "STRICT (budget overruns fail test)"
        } else {
            "ADVISORY (budget overruns warn only)"
        }
    );

    println!("── Step 1: Phase 26 Validation ───────────────────────────\n");
    let phase26_ok = cmd_phase26_validate(strict_perf) == ExitCode::SUCCESS;

    println!("\n── Step 2: Rust-Only Differential Suite ──────────────────\n");
    let rust_diff_ok = cmd_diff_test_rust_only() == ExitCode::SUCCESS;

    println!("\n── Step 3: Coverage Audit ────────────────────────────────\n");
    let coverage_ok = cmd_coverage_audit() == ExitCode::SUCCESS;

    println!("\n── Step 4: Java Code Audit (advisory) ────────────────────\n");
    let java_audit_ok = cmd_java_code_audit(false) == ExitCode::SUCCESS;

    println!("\n═══════════════════════════════════════════════════════════");
    println!("  Java-Free Validation Results");
    println!("═══════════════════════════════════════════════════════════");
    println!("  Phase 26 suite:     {}", status_icon(phase26_ok));
    println!("  Rust-only diff:     {}", status_icon(rust_diff_ok));
    println!("  Coverage audit:     {}", status_icon(coverage_ok));
    println!("  Java code audit:    {}", status_icon(java_audit_ok));
    println!("═══════════════════════════════════════════════════════════\n");

    if phase26_ok && rust_diff_ok && coverage_ok && java_audit_ok {
        println!("✅ Java-free validation PASSED.");
        ExitCode::SUCCESS
    } else {
        println!("❌ Java-free validation has FAILURES. Review above.");
        ExitCode::FAILURE
    }
}
