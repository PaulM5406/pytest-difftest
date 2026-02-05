"""CLI commands for pytest-diff."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path
from typing import Any


def merge_databases(output: str, inputs: list[str]) -> int:
    """Merge multiple pytest-diff databases into one.

    Args:
        output: Path to the output database file.
        inputs: List of paths to input database files.

    Returns:
        Exit code (0 for success, 1 for failure).
    """
    from pytest_diff._core import PytestDiffDatabase

    if not inputs:
        print("Error: At least one input database required", file=sys.stderr)
        return 1

    # Verify all input files exist first
    for input_path in inputs:
        if not Path(input_path).exists():
            print(f"Error: Input file not found: {input_path}", file=sys.stderr)
            return 1

    output_path = Path(output)
    db = PytestDiffDatabase(str(output_path))

    # Check for commit consistency before merging
    _check_merge_commit_consistency(db, inputs)

    total_baselines = 0
    total_tests = 0
    for input_path in inputs:
        result = db.merge_baseline_from(input_path)
        print(
            f"Merged {result.baseline_count} baselines"
            f" and {result.test_execution_count} test executions from {input_path}"
        )
        total_baselines += result.baseline_count
        total_tests += result.test_execution_count

    db.close()
    print(f"Total: {total_baselines} baselines and {total_tests} test executions in {output}")
    return 0


def _check_merge_commit_consistency(db: Any, inputs: list[str]) -> None:
    """Check that all input databases have the same baseline_commit."""
    commits: dict[str, list[str]] = {}  # commit -> list of filenames

    for input_path in inputs:
        try:
            commit = db.get_external_metadata(input_path, "baseline_commit")
            if commit:
                commits.setdefault(commit, []).append(Path(input_path).name)
        except Exception:
            pass  # Silently skip if we can't read metadata

    if len(commits) > 1:
        details = ", ".join(f"{sha[:8]}({len(files)} files)" for sha, files in commits.items())
        print(
            f"Warning: Merging baselines from different commits: {details}. "
            "This may cause inconsistent test selection.",
            file=sys.stderr,
        )


def main() -> int:
    """Main entry point for pytest-diff CLI."""
    parser = argparse.ArgumentParser(
        prog="pytest-diff",
        description="pytest-diff command line tools",
    )
    subparsers = parser.add_subparsers(dest="command", help="Available commands")

    # merge command
    merge_parser = subparsers.add_parser(
        "merge",
        help="Merge multiple pytest-diff databases into one",
        description="Merge multiple pytest-diff databases into one. "
        "Usage: pytest-diff merge output.db input1.db input2.db input3.db",
    )
    merge_parser.add_argument(
        "output",
        help="Path to the output database file",
    )
    merge_parser.add_argument(
        "inputs",
        nargs="+",
        help="Paths to input database files to merge",
    )

    args = parser.parse_args()

    if args.command is None:
        parser.print_help()
        return 0

    if args.command == "merge":
        return merge_databases(args.output, args.inputs)

    return 0


if __name__ == "__main__":
    sys.exit(main())
