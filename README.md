# pytest-diff

> **⚠️** This project is still a work in progress. APIs and behavior may change without notice.

**Fast test selection for pytest** - Only run tests affected by your changes, powered by Rust 🦀

[![CI](https://github.com/PaulM5406/pytest-diff/workflows/CI/badge.svg)](https://github.com/paulmilesi/pytest-diff/actions)
[![PyPI](https://img.shields.io/pypi/v/pytest-diff.svg)](https://pypi.org/project/pytest-diff/)
[![Python Versions](https://img.shields.io/pypi/pyversions/pytest-diff.svg)](https://pypi.org/project/pytest-diff/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## What is pytest-diff?

pytest-diff is a pytest plugin that intelligently selects and runs only the tests affected by your code changes.

### Key Features

- 🎯 **Smart test selection** - Only runs tests that touch changed code
- 🔍 **Block-level granularity** - Tracks changes at function/class level, not just files
- 🔧 **pytest-xdist compatible** - Works with parallel test execution
- 💾 **SQLite storage** - Reliable, portable test dependency database
- 🚀 **Upload/Download baseline to S3** - Easily share baseline of tests across a team
- 📦 **Portable baselines** - Stores relative paths so baselines work across machines (CI → local)

## Installation

```bash
pip install pytest-diff
```

Or with uv (recommended):

```bash
uv add --dev pytest-diff
```

## Quick Start

```bash
# 1. Save a baseline (runs all tests and records current state)
pytest --diff-baseline

# 2. After making changes, run only affected tests
pytest --diff

# 3. Update baseline incrementally (only re-runs affected tests)
pytest --diff-baseline

# 4. Force a full baseline rebuild when needed
pytest --diff-baseline --diff-force

# Example output:
# pytest-diff: Detected 3 modified files
#   Running 12 affected tests
#   Skipping 438 unaffected tests
# ==================== 12 passed in 0.8s =====================
```

## How It Works

pytest-diff uses a three-phase approach:

### 1. **Baseline** (`--diff-baseline`)
- First run: executes all tests with coverage, maps which tests touch which code blocks
- Subsequent runs: **incremental** — only re-runs tests affected by changes since last baseline
- Stores dependency graph in `.pytest_cache/pytest-diff/pytest_diff.db` SQLite database
- Use `--diff-force` to force a full rebuild (re-runs all tests AND recomputes all fingerprints)

### 2. **Change Detection** (`--diff`)
- Parses modified files with Rust (blazingly fast!)
- Calculates checksums for each code block
- Compares against stored fingerprints to find changed blocks

### 3. **Test Selection**
- Queries database for tests that depend on changed blocks
- Runs only affected tests

```
Code Change → AST Parsing (Rust) → Block Checksums → Database Query → Run Tests
     ↓                                                                      ↓
  detector.py                                                    test_detector.py
  line 15 changed                                                (runs because it
                                                                 used detector.py)
```

## Test Selection Scenarios

| Scenario | `--diff` behavior | `--diff-baseline` behavior |
|----------|-------------------|---------------------------|
| **No changes** | Skips all tests | Skips all tests (incremental) |
| **Modified source file** | Runs tests that depend on changed blocks | Runs affected tests, updates baseline |
| **New test file** | Detects as changed, runs all tests in the new file | Runs all tests in the new file, adds to baseline |
| **New source file** | Detects as changed, runs tests that depend on it | Runs tests that depend on it, adds to baseline |
| **Sub-scope** (e.g. baseline=`tests/`, diff=`tests/unit/`) | No warning, baseline already covers the scope | No warning, proceeds normally |
| **Broader scope** (e.g. baseline=`tests/unit/`, diff=`tests/`) | Warns, proceeds with test selection (may miss tests) | Warns, runs all tests to rebuild baseline |
| **Both `--diff` and `--diff-baseline`** | `--diff-baseline` takes precedence, `--diff` is ignored | - |
| **Failing tests** | Always re-selected (not recorded until they pass) | Re-run on incremental baseline until they pass |
| **First baseline (empty DB)** | Runs all tests to build the database | Runs all tests |
| **`--diff-force`** | N/A | Forces full rebuild, runs all tests |

## Configuration

### Command Line Options

| Option | Description |
|--------|-------------|
| `--diff` | Enable pytest-diff (select tests based on changes) |
| `--diff-baseline` | Compute baseline. First run executes all tests; subsequent runs are incremental (only affected tests) |
| `--diff-force` | Force full rebuild: runs all tests AND recomputes all fingerprints (use with `--diff-baseline`) |
| `--diff-v` | Enable verbose logging (shows timing and debug info) |
| `--diff-batch-size N` | Number of test executions to batch before DB write (default: 20) |
| `--diff-cache-size N` | Maximum fingerprints to cache in memory (default: 100000) |
| `--diff-remote URL` | Remote storage URL for a single baseline DB file (e.g. `s3://bucket/baseline.db`) |
| `--diff-upload` | Upload baseline DB to remote storage after `--diff-baseline` completes |

```bash
# Run only tests affected by your changes
pytest --diff

# Save baseline (first run: all tests; subsequent runs: only affected tests)
pytest --diff-baseline

# Force a full baseline rebuild
pytest --diff-baseline --diff-force

# Save baseline and upload to S3
pytest --diff-baseline --diff-upload --diff-remote "s3://my-bucket/baselines/baseline.db"

# Run affected tests, fetching baseline from remote
pytest --diff --diff-remote "s3://my-bucket/baselines/baseline.db"
```

### pyproject.toml

All options can be configured in `pyproject.toml` so you don't need to pass them on every invocation:

```toml
[tool.pytest.ini_options]
diff_batch_size = "50"
diff_cache_size = "200000"
diff_remote_url = "s3://my-ci-bucket/baselines/baseline.db"
diff_remote_key = "baseline.db"
```

CLI options take precedence over `pyproject.toml` values when both are provided.

### Remote Baseline Storage

pytest-diff supports storing the baseline database in remote storage, enabling a CI/CD workflow where CI computes the baseline and developers automatically fetch it.

**Supported backends:**

| Scheme | Backend | Requirements |
|--------|---------|-------------|
| `s3://bucket/path/file.db` | Amazon S3 | `pip install pytest-diff[s3]` |
| `file:///path/to/file.db` | Local filesystem | None |

**Simple CI/CD workflow (single job):**

1. **CI (on merge to main):** `pytest --diff-baseline --diff-upload --diff-remote "s3://bucket/baseline.db"`
2. **Developer local:** `pytest --diff --diff-remote "s3://bucket/baseline.db"` (auto-fetches latest baseline)

S3 uses ETag-based caching to avoid re-downloading unchanged baselines. Any S3 error (authentication, network, permissions, etc.) will **immediately abort the test run** to avoid silently running without a baseline.

**Recommended CI workflow (parallel jobs):**

When running tests in parallel across multiple CI jobs, each job uploads its baseline to a unique key. A final step merges them and uploads the result:

```bash
# Step 1: Each CI job uploads its baseline to a unique key
pytest --diff-baseline --diff-upload --diff-remote "s3://bucket/run-123/job-unit.db"
pytest --diff-baseline --diff-upload --diff-remote "s3://bucket/run-123/job-integration.db"

# Step 2: Final CI step merges all baselines and uploads the result
pytest-diff merge s3://bucket/baseline.db s3://bucket/run-123/

# Step 3: Developers fetch the single merged baseline
pytest --diff --diff-remote "s3://bucket/baseline.db"
```

### CLI Commands

pytest-diff provides a CLI for offline database operations:

```bash
# Merge local database files
pytest-diff merge output.db input1.db input2.db

# Merge all .db files from a local directory
pytest-diff merge output.db ./results/

# Merge from a remote prefix (downloads all .db files from it)
pytest-diff merge output.db s3://bucket/run-123/

# Merge and upload result to S3
pytest-diff merge s3://bucket/baseline.db input1.db input2.db

# Full remote: download from prefix, merge, and upload
pytest-diff merge s3://bucket/baseline.db s3://bucket/run-123/

# Mix local files, directories, and remote inputs
pytest-diff merge output.db input1.db ./results/ s3://bucket/run-123/
```

The `output` argument can be a local path or a remote URL (s3://..., file://...). When it's a remote URL, a temporary file is used locally and uploaded at the end. Each input can be a local file, a local directory (collects all `.db` files), or a remote URL (prefix ending with `/` downloads all `.db` files).

## Development Setup

pytest-diff uses modern Python tooling:

### Prerequisites

- [mise](https://mise.jdx.dev/) - Version manager for Python and Rust
- [uv](https://github.com/astral-sh/uv) - Fast Python package manager

### Setup

```bash
# Clone the repository
git clone https://github.com/paulmilesi/pytest-diff.git
cd pytest-diff

# Install mise (if not already installed)
curl https://mise.run | sh

# Install Python and Rust via mise
mise install

# Create virtual environment and install dependencies
uv sync --all-extras --dev

# Build the Rust extension
maturin develop

# Run tests
pytest

# Run Rust tests
cargo test

# Run benchmarks
cargo bench
```

## Architecture

```
pytest (Python)
    ↓
pytest-diff plugin (Python)
    ├── plugin.py        — pytest hooks & orchestration
    ├── _config.py       — configuration helpers
    ├── _git.py          — git commit SHA & staleness checks
    ├── _storage_ops.py  — remote storage operations
    ├── _xdist.py        — pytest-xdist coordination
    └── storage/         — S3 & local backends
    ↓ (PyO3 bindings)
pytest-diff-core (Rust)
    ├── AST Parser (RustPython parser)
    ├── Fingerprint Engine (CRC32)
    └── Database Layer (SQLite + LRU Cache)
```

### Development Workflow

```bash
# Make changes to Rust code
# Rebuild with:
maturin develop

# Run tests
pytest
cargo test

# Format code
cargo fmt
ruff format python/

# Lint
cargo clippy
ruff check python/

# Typing
ty check python/
```

## License

MIT License - see [LICENSE](LICENSE) file for details.

## Credits

- Inspired by [pytest-testmon](https://github.com/tarpas/pytest-testmon)
- Built with [Ruff's Python parser](https://github.com/astral-sh/ruff)
- Powered by [PyO3](https://github.com/PyO3/pyo3) and [Maturin](https://github.com/PyO3/maturin)

---

**Made with ❤️**
