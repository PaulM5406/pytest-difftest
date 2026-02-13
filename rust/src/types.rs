// Core types for pytest-diff
//
// These types are exposed to Python via PyO3 and represent
// the fundamental data structures used throughout the system.

use pyo3::prelude::*;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a code block (function, class, module, etc.)
///
/// Blocks are the granularity at which we track code changes.
/// Each block has a checksum calculated from its source code.
#[pyclass]
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Block {
    /// Starting line number (1-indexed, inclusive)
    #[pyo3(get)]
    pub start_line: usize,

    /// Ending line number (1-indexed, inclusive)
    #[pyo3(get)]
    pub end_line: usize,

    /// CRC32 checksum of the block's source code (as signed i32)
    #[pyo3(get)]
    pub checksum: i32,

    /// Name of the block (function/class name, or "module" for top-level)
    #[pyo3(get)]
    pub name: String,

    /// Block type: "module", "class", "function", "async_function"
    #[pyo3(get)]
    pub block_type: String,
}

#[pymethods]
impl Block {
    #[new]
    fn new(
        start_line: usize,
        end_line: usize,
        checksum: i32,
        name: String,
        block_type: String,
    ) -> Self {
        Self {
            start_line,
            end_line,
            checksum,
            name,
            block_type,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Block(name='{}', type='{}', lines={}-{}, checksum={})",
            self.name, self.block_type, self.start_line, self.end_line, self.checksum
        )
    }

    fn __str__(&self) -> String {
        self.__repr__()
    }
}

/// File fingerprint containing block checksums and metadata
///
/// This represents the "signature" of a file at a point in time,
/// allowing us to detect when the file has changed.
#[pyclass]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Fingerprint {
    /// Path to the file (relative to project root when stored in DB)
    #[pyo3(get)]
    pub filename: String,

    /// List of block checksums in the file
    #[pyo3(get)]
    pub checksums: Vec<i32>,

    /// Blake3 hash of entire file content
    #[pyo3(get)]
    pub file_hash: String,

    /// Modification time (Unix timestamp)
    #[pyo3(get)]
    pub mtime: f64,

    /// List of blocks (optional, for detailed inspection)
    #[pyo3(get)]
    pub blocks: Option<Vec<Block>>,
}

#[pymethods]
impl Fingerprint {
    #[new]
    #[pyo3(signature = (filename, checksums, file_hash, mtime, blocks=None))]
    fn new(
        filename: String,
        checksums: Vec<i32>,
        file_hash: String,
        mtime: f64,
        blocks: Option<Vec<Block>>,
    ) -> Self {
        Self {
            filename,
            checksums,
            file_hash,
            mtime,
            blocks,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "Fingerprint(file='{}', blocks={}, hash={}..)",
            self.filename,
            self.checksums.len(),
            &self.file_hash[..8]
        )
    }
}

/// Result of change detection
///
/// Contains lists of modified files and the specific blocks that changed.
#[pyclass]
#[derive(Clone, Debug)]
pub struct ChangedFiles {
    /// List of file paths that were modified
    #[pyo3(get)]
    pub modified: Vec<String>,

    /// Map of filename -> list of changed block checksums
    #[pyo3(get)]
    pub changed_blocks: HashMap<String, Vec<i32>>,
}

#[pymethods]
impl ChangedFiles {
    #[new]
    fn new(modified: Vec<String>, changed_blocks: HashMap<String, Vec<i32>>) -> Self {
        Self {
            modified,
            changed_blocks,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "ChangedFiles(modified={} files, changed_blocks={} files)",
            self.modified.len(),
            self.changed_blocks.len()
        )
    }

    /// Check if any files were modified
    fn has_changes(&self) -> bool {
        !self.modified.is_empty()
    }

    /// Get total number of changed blocks across all files
    fn total_changed_blocks(&self) -> usize {
        self.changed_blocks.values().map(|v| v.len()).sum()
    }
}

/// Test execution record
///
/// Stores information about a single test run, including which
/// code blocks it executed and its result.
#[pyclass]
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TestExecution {
    /// Test node ID (pytest's unique test identifier)
    #[pyo3(get)]
    pub test_name: String,

    /// Test execution duration in seconds
    #[pyo3(get)]
    pub duration: f64,

    /// Whether the test failed
    #[pyo3(get)]
    pub failed: bool,

    /// Whether the test was forced to run (not skipped by selection)
    #[pyo3(get)]
    pub forced: bool,

    /// List of fingerprints for files this test executed
    #[pyo3(get)]
    pub fingerprints: Vec<Fingerprint>,
}

#[pymethods]
impl TestExecution {
    #[new]
    fn new(
        test_name: String,
        duration: f64,
        failed: bool,
        forced: bool,
        fingerprints: Vec<Fingerprint>,
    ) -> Self {
        Self {
            test_name,
            duration,
            failed,
            forced,
            fingerprints,
        }
    }

    fn __repr__(&self) -> String {
        format!(
            "TestExecution(test='{}', duration={:.3}s, failed={}, files={})",
            self.test_name,
            self.duration,
            self.failed,
            self.fingerprints.len()
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_block_creation() {
        let block = Block::new(
            1,
            10,
            0x12345678,
            "test_func".to_string(),
            "function".to_string(),
        );

        assert_eq!(block.start_line, 1);
        assert_eq!(block.end_line, 10);
        assert_eq!(block.checksum, 0x12345678);
        assert_eq!(block.name, "test_func");
    }

    #[test]
    fn test_changed_files_has_changes() {
        let changed = ChangedFiles::new(
            vec!["file1.py".to_string()],
            [(String::from("file1.py"), vec![123, 456])]
                .into_iter()
                .collect(),
        );

        assert!(changed.has_changes());
        assert_eq!(changed.total_changed_blocks(), 2);
    }

    #[test]
    fn test_changed_files_no_changes() {
        let changed = ChangedFiles::new(vec![], HashMap::new());

        assert!(!changed.has_changes());
        assert_eq!(changed.total_changed_blocks(), 0);
    }
}
