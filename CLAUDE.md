# Project Rules

## Hints

- Factorize common code
- Split big class and method into smaller parts
- Add at least one relevant test when fixing a bug

## Toolchain

- **mise** manages Python, Rust and uv versions (see `.mise.toml`). Run `mise install` to bootstrap.
- **uv** is the Python package manager. Use `uv sync --all-extras --dev` to install dependencies.
- **maturin** builds the Rust extension into the Python package. Run `maturin develop` after any Rust change.
- **ruff** is the Python linter and formatter (`ruff check python/`, `ruff format python/`).
- **ty** is the Python type checker (`ty check python/`).
- **clippy** is the Rust linter. Run `cargo clippy --lib -- -D warnings` and fix any warnings.
- **rustfmt** is the Rust formatter. Run `cargo fmt` after any Rust change.

## Verification

Always :

- keep README up to date
- run these checks before considering work done:

```bash
# Rebuild Rust extension
maturin develop

# Python tests (37 tests)
pytest

# Rust tests
cargo test --lib

# Rust lint + format
cargo clippy --lib -- -D warnings
cargo fmt --check

# Python lint + format + type check
ruff check python/
ruff format --check python/
ty check python/
```