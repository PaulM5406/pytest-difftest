// pytest-diff-core: Rust core for blazingly fast test selection
//
// This module provides the core functionality for pytest-diff:
// - Python AST parsing with Ruff
// - Code block fingerprinting with CRC32
// - SQLite database operations with caching
// - Coverage collection integration

use pyo3::prelude::*;

mod cache;
mod database;
mod fingerprint;
mod fingerprint_cache;
mod parser;
mod types;

pub use database::{ImportResult, PytestDiffDatabase};
pub use fingerprint::{
    calculate_fingerprint, detect_changes, process_coverage_data, save_baseline,
};
pub use fingerprint_cache::FingerprintCache;
pub use parser::parse_module;
pub use types::{Block, ChangedFiles, Fingerprint, TestExecution};

/// Python module initialization
#[pymodule]
fn _core(m: &Bound<'_, PyModule>) -> PyResult<()> {
    // Register types
    m.add_class::<Block>()?;
    m.add_class::<Fingerprint>()?;
    m.add_class::<ChangedFiles>()?;
    m.add_class::<TestExecution>()?;
    m.add_class::<PytestDiffDatabase>()?;
    m.add_class::<ImportResult>()?;
    m.add_class::<FingerprintCache>()?;

    // Register functions
    m.add_function(wrap_pyfunction!(parse_module, m)?)?;
    m.add_function(wrap_pyfunction!(calculate_fingerprint, m)?)?;
    m.add_function(wrap_pyfunction!(detect_changes, m)?)?;
    m.add_function(wrap_pyfunction!(save_baseline, m)?)?;
    m.add_function(wrap_pyfunction!(process_coverage_data, m)?)?;

    // Module metadata
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    m.add("__author__", "Paul Milesi")?;

    Ok(())
}
