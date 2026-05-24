#!/usr/bin/env python3
"""
Audit Java source presence in the repository.

This script is used during Java→Rust transition:
- Advisory mode (default): report Java file counts, always exit 0.
- Strict mode (--strict): fail if any .java file remains.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path


def classify(java_file: Path, repo_root: Path) -> str:
    rel = java_file.relative_to(repo_root)
    parts = rel.parts
    if len(parts) >= 2:
        return f"{parts[0]}/{parts[1]}"
    return parts[0]


def collect_java_files(repo_root: Path) -> list[Path]:
    return sorted(p for p in repo_root.rglob("*.java") if p.is_file())


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Audit Java file presence in the Cassandra repository."
    )
    parser.add_argument(
        "--repo-root", default=".", help="Repository root (default: current directory)"
    )
    parser.add_argument(
        "--strict", action="store_true", help="Exit non-zero if any Java file remains"
    )
    parser.add_argument(
        "--json", action="store_true", help="Emit machine-readable JSON summary"
    )
    parser.add_argument(
        "--sample-limit",
        type=int,
        default=20,
        help="Number of sample files to print in text mode (default: 20)",
    )
    args = parser.parse_args()

    repo_root = Path(args.repo_root).resolve()
    java_files = collect_java_files(repo_root)

    by_group: dict[str, int] = {}
    for jf in java_files:
        key = classify(jf, repo_root)
        by_group[key] = by_group.get(key, 0) + 1

    grouped = sorted(by_group.items(), key=lambda kv: (-kv[1], kv[0]))
    summary = {
        "repo_root": str(repo_root),
        "java_file_count": len(java_files),
        "groups": [{"path_group": k, "count": v} for (k, v) in grouped],
        "sample_files": [
            str(p.relative_to(repo_root)) for p in java_files[: max(args.sample_limit, 0)]
        ],
        "strict_passed": len(java_files) == 0,
    }

    if args.json:
        print(json.dumps(summary, indent=2))
    else:
        print("═══════════════════════════════════════════════════════")
        print("  Java Code Audit")
        print("═══════════════════════════════════════════════════════")
        print(f"Repository: {repo_root}")
        print(f"Java files: {len(java_files)}")
        print("")
        print("Top groups:")
        for path_group, count in grouped[:15]:
            print(f"  {path_group:<30} {count:>6}")
        if java_files:
            print("")
            print(f"Sample files (first {min(len(java_files), args.sample_limit)}):")
            for sample in summary["sample_files"]:
                print(f"  - {sample}")
        print("")
        if len(java_files) == 0:
            print("PASS: No Java files remain.")
        elif args.strict:
            print(f"FAIL: {len(java_files)} Java files remain (strict mode).")
        else:
            print(
                f"WARN: {len(java_files)} Java files remain. "
                "Run with --strict to enforce zero-Java."
            )

    if args.strict and len(java_files) > 0:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

