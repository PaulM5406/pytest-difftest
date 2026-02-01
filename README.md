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

# Example output:
# pytest-diff: Detected 3 modified files
#   Running 12 affected tests
#   Skipping 438 unaffected tests
# ==================== 12 passed in 0.8s =====================
```

## How It Works

pytest-diff uses a three-phase approach:

### 1. **Dependency Tracking** (First Run)
- Runs all tests with coverage enabled
- Maps which tests execute which code blocks
- Stores dependency graph in `.pytest_cache/pytest-diff/pytest_diff.db` SQLite database

### 2. **Change Detection** (Subsequent Runs)
- Parses modified files with Rust (blazingly fast!)
- Calculates checksums for each code block
- Compares against stored fingerprints to find changed blocks

### 3. **Test Selection**
- Queries database for tests that depend on changed blocks
- Runs only affected tests
- Updates database with new fingerprints

```
Code Change → AST Parsing (Rust) → Block Checksums → Database Query → Run Tests
     ↓                                                                      ↓
  detector.py                                                    test_detector.py
  line 15 changed                                                (runs because it
                                                                 used detector.py)
```

## Configuration

### Command Line Options

| Option | Description |
|--------|-------------|
| `--diff` | Enable pytest-diff (select tests based on changes) |
| `--diff-baseline` | Run all tests and save current state as baseline for change detection |
| `--diff-v` | Enable verbose logging (shows timing and debug info) |
| `--diff-batch-size N` | Number of test executions to batch before DB write (default: 20) |
| `--diff-cache-size N` | Maximum fingerprints to cache in memory (default: 100000) |
| `--diff-remote URL` | Remote storage URL for baseline DB (e.g. `s3://bucket/prefix/`, `file:///path/`) |
| `--diff-upload` | Upload baseline DB to remote storage after `--diff-baseline` completes |

```bash
# Run only tests affected by your changes
pytest --diff

# Save baseline (run all tests, record current state)
pytest --diff-baseline

# Save baseline and upload to S3
pytest --diff-baseline --diff-upload --diff-remote "s3://my-bucket/pytest-diff/"

# Run affected tests, fetching baseline from remote
pytest --diff --diff-remote "s3://my-bucket/pytest-diff/"
```

### pyproject.toml

All options can be configured in `pyproject.toml` so you don't need to pass them on every invocation:

```toml
[tool.pytest.ini_options]
diff_batch_size = "50"
diff_cache_size = "200000"
diff_remote_url = "s3://my-ci-bucket/pytest-diff/"
diff_remote_key = "baseline.db"
```

CLI options take precedence over `pyproject.toml` values when both are provided.

### Remote Baseline Storage

pytest-diff supports storing the baseline database in remote storage, enabling a CI/CD workflow where CI computes the baseline and developers automatically fetch it.

**Supported backends:**

| Scheme | Backend | Requirements |
|--------|---------|-------------|
| `s3://bucket/prefix/` | Amazon S3 | `pip install pytest-diff[s3]` |
| `file:///path/to/dir/` | Local filesystem | None |

**Typical CI/CD workflow:**

1. **CI (on merge to main):** `pytest --diff-baseline --diff-upload --diff-remote "s3://bucket/prefix/"`
2. **Developer local:** `pytest --diff --diff-remote "s3://bucket/prefix/"` (auto-fetches latest baseline)

S3 uses ETag-based caching to avoid re-downloading unchanged baselines. All remote errors are non-fatal -- if the download fails, tests run normally using the local baseline.

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
    ↓ (PyO3 bindings)
pytest-diff-core (Rust)
    ├── AST Parser (RustPython parser)
    ├── Fingerprint Engine (CRC32)
    └── Database Layer (SQLite + Cache)
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

## Support

- 🐛 [Issue Tracker](https://github.com/paulmilesi/pytest-diff/issues)
- 💬 [Discussions](https://github.com/paulmilesi/pytest-diff/discussions)

---

**Made with ❤️ and 🦀 by Paul Milesi**
