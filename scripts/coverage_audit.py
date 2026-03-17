#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""
Coverage Audit Script for Cassandra Java->Rust Rewrite

Scans the Java source tree and cross-references against the gap matrix
to detect unclassified packages, classes, nodetool commands, virtual tables,
and config files.

Usage:
    python3 scripts/coverage_audit.py [--repo-root /path/to/cassandra]
    python3 scripts/coverage_audit.py --generate-inventory
    python3 scripts/coverage_audit.py --check-matrix [--strict]
    python3 scripts/coverage_audit.py --check-matrix --json

Exit codes:
    0 - All items classified
    1 - Unclassified items found
    2 - Matrix file missing or malformed
"""

import argparse
import json
import os
import sys
from collections import defaultdict
from pathlib import Path
from typing import Optional

try:
    import yaml
    HAS_YAML = True
except ImportError:
    HAS_YAML = False


def find_repo_root(start: Optional[Path] = None) -> Path:
    """Walk up from start to find the repo root (contains src/java)."""
    p = start or Path(__file__).resolve().parent.parent
    while p != p.parent:
        if (p / "src" / "java").exists():
            return p
        p = p.parent
    raise FileNotFoundError("Cannot find repo root with src/java")


def scan_java_packages(repo: Path) -> dict:
    """Scan all Java packages and count files per package."""
    java_root = repo / "src" / "java" / "org" / "apache" / "cassandra"
    packages = defaultdict(lambda: {"files": [], "count": 0})

    for java_file in java_root.rglob("*.java"):
        rel = java_file.relative_to(java_root)
        pkg_parts = list(rel.parent.parts)
        pkg_name = "org.apache.cassandra"
        if pkg_parts and pkg_parts != ["."]:
            pkg_name += "." + ".".join(pkg_parts)

        class_name = java_file.stem
        packages[pkg_name]["files"].append(class_name)
        packages[pkg_name]["count"] += 1

    return dict(packages)


def scan_nodetool_commands(repo: Path) -> list:
    """Extract nodetool command class names."""
    nodetool_dir = (
        repo / "src" / "java" / "org" / "apache" / "cassandra" / "tools" / "nodetool"
    )
    commands = []
    if nodetool_dir.exists():
        for f in sorted(nodetool_dir.glob("*.java")):
            name = f.stem
            if name.startswith("Abstract") or name in (
                "NodetoolCommand", "CommandUtils", "PrintPortMixin",
                "JmxConnect", "HostStat", "HostStatWithPort",
                "SetHostStat", "SetHostStatWithPort", "Help", "History",
            ):
                continue
            commands.append(name)
    return commands


def scan_virtual_tables(repo: Path) -> list:
    """Extract virtual table class names."""
    vt_dir = (
        repo / "src" / "java" / "org" / "apache" / "cassandra" / "db" / "virtual"
    )
    tables = []
    if vt_dir.exists():
        for f in sorted(vt_dir.glob("*.java")):
            name = f.stem
            if name.startswith("Abstract") or name in (
                "VirtualKeyspace", "VirtualKeyspaceRegistry", "VirtualMutation",
                "VirtualSchemaKeyspace", "VirtualTable", "SimpleDataSet",
                "Column", "RowWalker", "CollectionVirtualTableAdapter",
                "RemoteToLocalVirtualKeyspace", "RemoteToLocalVirtualTable",
                "SystemViewsKeyspace",
            ):
                continue
            if "Table" in name or "Row" in name or "Walker" in name:
                tables.append(name)
    return tables


def scan_config_files(repo: Path) -> list:
    """List configuration files."""
    conf_dir = repo / "conf"
    files = []
    if conf_dir.exists():
        for f in sorted(conf_dir.iterdir()):
            if f.is_file():
                files.append(f.name)
    return files


def scan_sstable_tools(repo: Path) -> list:
    """Extract standalone SSTable tool class names."""
    tools_dir = repo / "src" / "java" / "org" / "apache" / "cassandra" / "tools"
    tools = []
    if tools_dir.exists():
        for f in sorted(tools_dir.glob("*.java")):
            name = f.stem
            if name.startswith("Standalone") or name.startswith("SSTable"):
                tools.append(name)
    return tools


def scan_test_directories(repo: Path) -> dict:
    """Scan test directory structure."""
    test_root = repo / "test"
    tests = {"unit": 0, "distributed": 0, "long": 0, "other": 0}
    if test_root.exists():
        for d in test_root.iterdir():
            if d.is_dir():
                count = sum(1 for _ in d.rglob("*.java"))
                key = d.name if d.name in tests else "other"
                tests[key] = count
    return tests


def load_gap_matrix(repo: Path) -> Optional[list]:
    """Load the gap matrix YAML."""
    matrix_path = repo / "docs" / "rewrite" / "final_gap_matrix.yaml"
    if not matrix_path.exists():
        return None

    if HAS_YAML:
        with open(matrix_path) as f:
            data = yaml.safe_load(f)
        return data.get("features", data) if isinstance(data, dict) else data
    else:
        return []


def get_classified_packages(matrix: list) -> set:
    """Extract the set of Java packages covered by the matrix."""
    classified = set()
    if not matrix:
        return classified
    for entry in matrix:
        if isinstance(entry, dict):
            pkgs = entry.get("java_packages", [])
            if isinstance(pkgs, str):
                pkgs = [pkgs]
            for p in pkgs:
                classified.add(p)
    return classified


def is_package_classified(pkg: str, classified: set) -> bool:
    """Check if a package is classified (exact or parent/child match)."""
    if pkg in classified:
        return True
    for c in classified:
        if pkg.startswith(c + ".") or c.startswith(pkg + "."):
            return True
    return False


def check_orphan_entries(matrix: list, packages: dict) -> list:
    """Find matrix entries referencing packages that don't exist in Java."""
    orphans = []
    all_pkg_names = set(packages.keys())
    if not matrix:
        return orphans
    for entry in matrix:
        if not isinstance(entry, dict):
            continue
        for pkg in entry.get("java_packages", []):
            if not isinstance(pkg, str):
                continue
            found = False
            for existing in all_pkg_names:
                if existing == pkg or existing.startswith(pkg + ".") or pkg.startswith(existing + "."):
                    found = True
                    break
            if not found and pkg:
                orphans.append((entry.get("id", "unknown"), pkg))
    return orphans


def generate_inventory(repo: Path, matrix: Optional[list] = None, output_path: Optional[Path] = None):
    """Generate the package_inventory.md file."""
    packages = scan_java_packages(repo)
    nodetool_cmds = scan_nodetool_commands(repo)
    virtual_tables = scan_virtual_tables(repo)
    config_files = scan_config_files(repo)
    sstable_tools = scan_sstable_tools(repo)
    test_stats = scan_test_directories(repo)

    classified = get_classified_packages(matrix) if matrix else set()

    top_level = defaultdict(list)
    for pkg in sorted(packages.keys()):
        parts = pkg.replace("org.apache.cassandra", "").lstrip(".")
        top = parts.split(".")[0] if parts else "(root)"
        top_level[top].append(pkg)

    out = output_path or (repo / "docs" / "rewrite" / "package_inventory.md")
    out.parent.mkdir(parents=True, exist_ok=True)

    total_files = sum(p["count"] for p in packages.values())
    total_packages = len(packages)
    classified_count = sum(
        1 for pkg in packages if is_package_classified(pkg, classified)
    )

    with open(out, "w") as f:
        f.write("# Java Package Inventory\n\n")
        f.write(f"**Generated**: auto by `scripts/coverage_audit.py`\n")
        f.write(f"**Total packages**: {total_packages}\n")
        f.write(f"**Total Java files**: {total_files}\n")
        if classified:
            f.write(f"**Classified**: {classified_count}/{total_packages}\n")
        f.write("\n")

        f.write("## Packages by Subsystem\n\n")
        f.write("| Top-Level | Packages | Files | Classified |\n")
        f.write("|-----------|----------|-------|------------|\n")
        for top in sorted(top_level.keys()):
            pkgs = top_level[top]
            file_count = sum(packages[p]["count"] for p in pkgs)
            cls_count = sum(
                1 for p in pkgs if is_package_classified(p, classified)
            ) if classified else "-"
            f.write(f"| `{top}` | {len(pkgs)} | {file_count} | {cls_count} |\n")

        f.write(f"\n## All Packages ({total_packages})\n\n")
        f.write("| Package | Files | Classified |\n")
        f.write("|---------|-------|------------|\n")
        for pkg in sorted(packages.keys()):
            cls = "yes" if is_package_classified(pkg, classified) else "**NO**"
            if not classified:
                cls = "-"
            f.write(f"| `{pkg}` | {packages[pkg]['count']} | {cls} |\n")

        f.write(f"\n## Nodetool Commands ({len(nodetool_cmds)})\n\n")
        for i, cmd in enumerate(nodetool_cmds):
            f.write(f"{i+1}. `{cmd}`\n")

        f.write(f"\n## Virtual Tables ({len(virtual_tables)})\n\n")
        for vt in virtual_tables:
            f.write(f"- `{vt}`\n")

        f.write(f"\n## SSTable Tools ({len(sstable_tools)})\n\n")
        for tool in sstable_tools:
            f.write(f"- `{tool}`\n")

        f.write(f"\n## Configuration Files ({len(config_files)})\n\n")
        for cf in config_files:
            f.write(f"- `{cf}`\n")

        f.write(f"\n## Test Statistics\n\n")
        f.write("| Category | Java Test Files |\n")
        f.write("|----------|-----------------|\n")
        for cat, count in sorted(test_stats.items()):
            f.write(f"| {cat} | {count} |\n")

    print(f"Inventory written to {out}")
    return {
        "packages": packages,
        "nodetool_commands": nodetool_cmds,
        "virtual_tables": virtual_tables,
        "config_files": config_files,
        "sstable_tools": sstable_tools,
        "test_stats": test_stats,
    }


def check_matrix(repo: Path, strict: bool = False, json_output: bool = False) -> bool:
    """Validate the gap matrix covers all Java packages."""
    packages = scan_java_packages(repo)
    matrix = load_gap_matrix(repo)

    if matrix is None:
        msg = "Gap matrix not found at docs/rewrite/final_gap_matrix.yaml"
        if json_output:
            json.dump({"error": msg, "passed": False}, sys.stdout, indent=2)
            print()
        else:
            print(f"FAIL: {msg}", file=sys.stderr)
        return False

    classified = get_classified_packages(matrix)
    all_ok = True
    results = {
        "passed": True,
        "mode": "strict" if strict else "top-level",
        "total_packages": len(packages),
        "classified_packages": 0,
        "unclassified": [],
        "orphan_entries": [],
        "missing_status": [],
        "status_counts": {},
    }

    if strict:
        # Sub-package-level: every single Java package must be classified
        unclassified = []
        for pkg in sorted(packages.keys()):
            if not is_package_classified(pkg, classified):
                unclassified.append(pkg)
                all_ok = False

        results["classified_packages"] = len(packages) - len(unclassified)
        results["unclassified"] = unclassified

        if unclassified and not json_output:
            print(
                f"FAIL: {len(unclassified)} unclassified sub-packages:",
                file=sys.stderr,
            )
            for pkg in unclassified:
                print(f"   - {pkg}", file=sys.stderr)
        elif not unclassified and not json_output:
            print(f"PASS: All {len(packages)} sub-packages are classified")
    else:
        # Top-level only
        top_level_pkgs = set()
        for pkg in packages.keys():
            parts = pkg.replace("org.apache.cassandra.", "").split(".")
            top = "org.apache.cassandra." + parts[0] if parts[0] else "org.apache.cassandra"
            top_level_pkgs.add(top)

        unclassified = []
        for pkg in sorted(top_level_pkgs):
            if not is_package_classified(pkg, classified):
                unclassified.append(pkg)
                all_ok = False

        results["classified_packages"] = len(top_level_pkgs) - len(unclassified)
        results["unclassified"] = unclassified

        if unclassified and not json_output:
            print(
                f"FAIL: {len(unclassified)} unclassified top-level packages:",
                file=sys.stderr,
            )
            for pkg in unclassified:
                print(f"   - {pkg}", file=sys.stderr)
        elif not unclassified and not json_output:
            print(f"PASS: All {len(top_level_pkgs)} top-level packages are classified")

    # Check for orphan matrix entries
    orphans = check_orphan_entries(matrix, packages)
    results["orphan_entries"] = [
        {"feature_id": fid, "package": pkg} for fid, pkg in orphans
    ]
    if orphans and not json_output:
        print(
            f"WARNING: {len(orphans)} matrix entries reference non-existent packages:",
            file=sys.stderr,
        )
        for fid, pkg in orphans:
            print(f"   - {fid}: {pkg}", file=sys.stderr)

    # Check matrix entries have required fields
    missing_status = []
    if matrix:
        for entry in matrix:
            if isinstance(entry, dict):
                if not entry.get("status"):
                    missing_status.append(entry.get("id", "unknown"))
        results["missing_status"] = missing_status
        if missing_status:
            if not json_output:
                print(
                    f"FAIL: {len(missing_status)} matrix entries missing status:",
                    file=sys.stderr,
                )
                for ms in missing_status:
                    print(f"   - {ms}", file=sys.stderr)
            all_ok = False

    # Status summary
    if matrix:
        status_counts = defaultdict(int)
        for entry in matrix:
            if isinstance(entry, dict):
                status_counts[entry.get("status", "unknown")] += 1
        results["status_counts"] = dict(status_counts)
        results["total_features"] = sum(status_counts.values())
        if not json_output:
            print("\nGap Matrix Summary:")
            for status, count in sorted(status_counts.items()):
                print(f"   {status}: {count}")
            print(f"   TOTAL: {sum(status_counts.values())}")

    results["passed"] = all_ok
    if json_output:
        json.dump(results, sys.stdout, indent=2)
        print()

    return all_ok


def main():
    parser = argparse.ArgumentParser(description="Cassandra coverage audit")
    parser.add_argument("--repo-root", type=Path, default=None,
                        help="Path to repository root")
    parser.add_argument("--generate-inventory", action="store_true",
                        help="Generate package_inventory.md")
    parser.add_argument("--check-matrix", action="store_true",
                        help="Validate gap matrix completeness")
    parser.add_argument("--strict", action="store_true",
                        help="Sub-package-level validation (default: top-level only)")
    parser.add_argument("--json", action="store_true",
                        help="Output as JSON (for CI/xtask consumption)")
    args = parser.parse_args()

    repo = args.repo_root or find_repo_root()

    if args.generate_inventory or not args.check_matrix:
        matrix = load_gap_matrix(repo)
        inventory = generate_inventory(repo, matrix)
        if args.json and not args.check_matrix:
            out = {
                "nodetool_commands": inventory["nodetool_commands"],
                "virtual_tables": inventory["virtual_tables"],
                "config_files": inventory["config_files"],
                "sstable_tools": inventory["sstable_tools"],
                "package_count": len(inventory["packages"]),
                "file_count": sum(p["count"] for p in inventory["packages"].values()),
            }
            json.dump(out, sys.stdout, indent=2)
            print()

    if args.check_matrix:
        ok = check_matrix(repo, strict=args.strict, json_output=args.json)
        sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
