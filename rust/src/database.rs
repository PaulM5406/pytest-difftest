// SQLite database layer with caching
//
// This module provides:
// - SQLite database operations for test executions and fingerprints
// - In-memory caching for hot paths
// - Prepared statement management
// - Concurrent access support (WAL mode + busy timeout)
// - Automatic cleanup of old test executions

use anyhow::{Context, Result};
use parking_lot::RwLock;
use pyo3::prelude::*;
use rusqlite::{params, Connection, OptionalExtension};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;

use crate::cache::Cache;
use crate::types::Fingerprint;

/// Default busy timeout in milliseconds for concurrent access
const BUSY_TIMEOUT_MS: i32 = 30_000; // 30 seconds

/// Result of an import or merge operation
#[pyclass]
#[derive(Clone, Debug)]
pub struct ImportResult {
    #[pyo3(get)]
    pub baseline_count: usize,
    #[pyo3(get)]
    pub test_execution_count: usize,
}

/// Main database interface for pytest-diff
///
/// Manages the pytest-diff SQLite database with optimizations:
/// - WAL mode for concurrent access
/// - Prepared statement caching
/// - Memory-mapped I/O
/// - In-memory cache for frequently accessed data
#[pyclass(unsendable)]
pub struct PytestDiffDatabase {
    conn: Arc<RwLock<Connection>>,
    cache: Arc<Cache>,
    current_environment_id: Arc<RwLock<Option<i64>>>,
}

impl PytestDiffDatabase {
    /// Create a new database connection (public Rust API)
    pub fn open(path: &str) -> Result<Self> {
        Self::new_internal(path)
    }

    /// Create a new database connection with optimizations
    fn new_internal(path: &str) -> Result<Self> {
        let path_obj = Path::new(path);

        // Create parent directory if it doesn't exist
        if let Some(parent) = path_obj.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory: {:?}", parent))?;
        }

        let conn =
            Connection::open(path).with_context(|| format!("Failed to open database: {}", path))?;

        // Set busy timeout FIRST for concurrent access (pytest-xdist compatibility)
        // This makes SQLite retry for up to BUSY_TIMEOUT_MS when database is locked
        conn.busy_timeout(std::time::Duration::from_millis(BUSY_TIMEOUT_MS as u64))
            .context("Failed to set busy timeout")?;

        // Apply performance optimizations
        conn.execute_batch(
            "
            PRAGMA journal_mode = WAL;
            PRAGMA synchronous = NORMAL;
            PRAGMA cache_size = -64000;
            PRAGMA temp_store = MEMORY;
            PRAGMA mmap_size = 268435456;
            PRAGMA foreign_keys = ON;
            ",
        )
        .context("Failed to set SQLite pragmas")?;

        // Create schema
        Self::create_schema(&conn)?;

        #[allow(clippy::arc_with_non_send_sync)]
        Ok(Self {
            conn: Arc::new(RwLock::new(conn)),
            cache: Arc::new(Cache::new()),
            current_environment_id: Arc::new(RwLock::new(None)),
        })
    }

    /// Create database schema if it doesn't exist
    fn create_schema(conn: &Connection) -> Result<()> {
        conn.execute_batch(include_str!("schema.sql"))
            .context("Failed to create database schema")?;
        Ok(())
    }

    /// Close database and checkpoint WAL (public Rust API)
    pub fn close_and_checkpoint(&self) -> Result<()> {
        let conn = self.conn.write();
        // Checkpoint WAL to merge changes into main database file
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .context("Failed to checkpoint WAL")?;
        Ok(())
    }

    /// Get or create environment ID for current Python environment
    fn get_or_create_environment(&self, env_name: &str, python_version: &str) -> Result<i64> {
        // Check cache first
        {
            let cached_id = self.current_environment_id.read();
            if let Some(id) = *cached_id {
                return Ok(id);
            }
        }

        let conn = self.conn.write();

        // Try to find existing environment
        let existing_id: Option<i64> = conn
            .query_row(
                "SELECT id FROM environment WHERE environment_name = ?1 AND python_version = ?2",
                params![env_name, python_version],
                |row| row.get(0),
            )
            .optional()
            .context("Failed to query environment")?;

        let env_id = if let Some(id) = existing_id {
            id
        } else {
            // Create new environment
            conn.execute(
                "INSERT INTO environment (environment_name, system_packages, python_version)
                 VALUES (?1, ?2, ?3)",
                params![env_name, "", python_version],
            )
            .context("Failed to insert environment")?;

            conn.last_insert_rowid()
        };

        // Cache the environment ID
        {
            let mut cached_id = self.current_environment_id.write();
            *cached_id = Some(env_id);
        }

        Ok(env_id)
    }

    /// Store or retrieve fingerprint ID (used in tests)
    #[cfg(test)]
    fn get_or_create_fingerprint(&self, fp: &Fingerprint) -> Result<i64> {
        let conn = self.conn.write();

        // Serialize checksums to blob
        let checksums_blob = serialize_checksums(&fp.checksums);

        // Try to find existing fingerprint
        let existing_id: Option<i64> = conn
            .query_row(
                "SELECT id FROM file_fp
                 WHERE filename = ?1 AND fsha = ?2 AND method_checksums = ?3",
                params![&fp.filename, &fp.file_hash, checksums_blob],
                |row| row.get(0),
            )
            .optional()
            .context("Failed to query fingerprint")?;

        if let Some(id) = existing_id {
            Ok(id)
        } else {
            // Insert new fingerprint
            conn.execute(
                "INSERT INTO file_fp (filename, method_checksums, mtime, fsha)
                 VALUES (?1, ?2, ?3, ?4)",
                params![&fp.filename, checksums_blob, fp.mtime, &fp.file_hash],
            )
            .context("Failed to insert fingerprint")?;

            Ok(conn.last_insert_rowid())
        }
    }

    /// Get stored fingerprint for a file (public Rust API)
    pub fn get_fingerprint_rust(&self, filename: &str) -> Result<Option<Fingerprint>> {
        self.get_fingerprint_internal(filename)
    }

    /// Get baseline fingerprint for a file (public Rust API)
    pub fn get_baseline_fingerprint_rust(&self, filename: &str) -> Result<Option<Fingerprint>> {
        self.get_baseline_fingerprint_internal(filename)
    }

    /// Get stored fingerprint from database, bypassing cache
    /// This should be used for change detection to ensure we get the latest stored value
    pub fn get_fingerprint_no_cache(&self, filename: &str) -> Result<Option<Fingerprint>> {
        let conn = self.conn.read();

        conn.query_row(
            "SELECT filename, method_checksums, mtime, fsha
                 FROM file_fp
                 WHERE filename = ?1
                 ORDER BY id DESC
                 LIMIT 1",
            params![filename],
            |row| {
                let checksums_blob: Vec<u8> = row.get(1)?;
                let checksums = deserialize_checksums(&checksums_blob);

                Ok(Fingerprint {
                    filename: row.get(0)?,
                    checksums,
                    mtime: row.get(2)?,
                    file_hash: row.get(3)?,
                    blocks: None,
                })
            },
        )
        .optional()
        .context("Failed to query fingerprint")
    }

    /// Get stored fingerprint for a file (if exists)
    fn get_fingerprint_internal(&self, filename: &str) -> Result<Option<Fingerprint>> {
        // Check cache first
        let path = Path::new(filename).to_path_buf();
        if let Some(cached) = self.cache.get_fingerprint(&path) {
            return Ok(Some(cached));
        }

        let conn = self.conn.read();

        let result = conn
            .query_row(
                "SELECT filename, method_checksums, mtime, fsha
                 FROM file_fp
                 WHERE filename = ?1
                 ORDER BY id DESC
                 LIMIT 1",
                params![filename],
                |row| {
                    let checksums_blob: Vec<u8> = row.get(1)?;
                    let checksums = deserialize_checksums(&checksums_blob);

                    Ok(Fingerprint {
                        filename: row.get(0)?,
                        checksums,
                        mtime: row.get(2)?,
                        file_hash: row.get(3)?,
                        blocks: None,
                    })
                },
            )
            .optional()
            .context("Failed to query fingerprint")?;

        // Cache if found
        if let Some(ref fp) = result {
            self.cache.insert_fingerprint(path, fp.clone());
        }

        Ok(result)
    }
}

#[pymethods]
impl PytestDiffDatabase {
    #[new]
    fn new(path: &str) -> PyResult<Self> {
        Self::new_internal(path).map_err(|e| {
            pyo3::exceptions::PyIOError::new_err(format!("Failed to open database: {}", e))
        })
    }

    /// Save a test execution record with its fingerprints
    ///
    /// # Arguments
    /// * `test_name` - pytest node ID (e.g., "test_module.py::test_function")
    /// * `fingerprints` - List of file fingerprints the test touched
    /// * `duration` - Test execution time in seconds
    /// * `failed` - Whether the test failed
    /// * `python_version` - Python version string (e.g., "3.12.0")
    #[pyo3(signature = (test_name, fingerprints, duration, failed, python_version = "3.12"))]
    fn save_test_execution(
        &mut self,
        test_name: &str,
        fingerprints: Vec<Fingerprint>,
        duration: f64,
        failed: bool,
        python_version: &str,
    ) -> PyResult<()> {
        self.save_test_execution_internal(test_name, fingerprints, duration, failed, python_version)
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to save test execution: {}",
                    e
                ))
            })
    }

    /// Get list of tests affected by changed blocks
    ///
    /// # Arguments
    /// * `changed_blocks` - Map of filename -> list of changed checksums
    ///
    /// # Returns
    /// * List of test names that should be run
    fn get_affected_tests(
        &self,
        changed_blocks: HashMap<String, Vec<i32>>,
    ) -> PyResult<Vec<String>> {
        self.get_affected_tests_internal(changed_blocks)
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to get affected tests: {}",
                    e
                ))
            })
    }

    /// Get stored fingerprint for a file
    fn get_fingerprint(&self, filename: &str) -> PyResult<Option<Fingerprint>> {
        self.get_fingerprint_internal(filename).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to get fingerprint: {}", e))
        })
    }

    /// Clear all cached data
    fn clear_cache(&self) -> PyResult<()> {
        self.cache.clear();
        Ok(())
    }

    /// Get database statistics
    fn get_stats(&self) -> PyResult<HashMap<String, i64>> {
        self.get_stats_internal().map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to get stats: {}", e))
        })
    }

    /// Save baseline fingerprint for a file
    ///
    /// This stores the "known good" state that change detection compares against.
    /// Replaces any existing baseline for the file.
    fn save_baseline_fingerprint(&mut self, fingerprint: Fingerprint) -> PyResult<()> {
        self.save_baseline_fingerprint_internal(fingerprint)
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to save baseline fingerprint: {}",
                    e
                ))
            })
    }

    /// Get baseline fingerprint for a file
    fn get_baseline_fingerprint(&self, filename: &str) -> PyResult<Option<Fingerprint>> {
        self.get_baseline_fingerprint_internal(filename)
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to get baseline fingerprint: {}",
                    e
                ))
            })
    }

    /// Clear all baseline fingerprints
    fn clear_baseline(&mut self) -> PyResult<()> {
        let conn = self.conn.write();
        conn.execute("DELETE FROM baseline_fp", []).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to clear baseline: {}", e))
        })?;
        Ok(())
    }

    /// Import baseline and test execution data from another database file using ATTACH DATABASE.
    ///
    /// Bulk-copies `baseline_fp`, `environment`, `file_fp`, `test_execution`, and
    /// `test_execution_file_fp` rows from `source_db_path` into the local database,
    /// replacing any existing data. Returns an `ImportResult` with counts.
    fn import_baseline_from(&mut self, source_db_path: &str) -> PyResult<ImportResult> {
        self.import_baseline_from_internal(source_db_path)
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to import baseline: {}",
                    e
                ))
            })
    }

    /// Merge baseline and test execution data from another database file using ATTACH DATABASE.
    ///
    /// Unlike `import_baseline_from`, this does NOT clear existing baselines first.
    /// Uses INSERT OR REPLACE to accumulate baselines from multiple sources,
    /// allowing incremental merging of databases from parallel CI jobs.
    /// Returns an `ImportResult` with counts.
    fn merge_baseline_from(&mut self, source_db_path: &str) -> PyResult<ImportResult> {
        self.merge_baseline_from_internal(source_db_path)
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to merge baseline: {}",
                    e
                ))
            })
    }

    /// Read a metadata value from an external database file without importing it.
    ///
    /// Useful for checking metadata (e.g., baseline_commit) before merging.
    fn get_external_metadata(&self, source_db_path: &str, key: &str) -> PyResult<Option<String>> {
        self.get_external_metadata_internal(source_db_path, key)
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to get external metadata: {}",
                    e
                ))
            })
    }

    /// Store a metadata key-value pair (INSERT OR REPLACE)
    fn set_metadata(&self, key: &str, value: &str) -> PyResult<()> {
        self.set_metadata_internal(key, value).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to set metadata: {}", e))
        })
    }

    /// Retrieve a metadata value by key, or None if not found
    fn get_metadata(&self, key: &str) -> PyResult<Option<String>> {
        self.get_metadata_internal(key).map_err(|e| {
            pyo3::exceptions::PyRuntimeError::new_err(format!("Failed to get metadata: {}", e))
        })
    }

    /// Close the database and checkpoint WAL to remove -wal and -shm files
    fn close(&self) -> PyResult<()> {
        let conn = self.conn.write();
        // Checkpoint WAL to merge it into main database file
        // TRUNCATE mode will truncate the WAL file to zero bytes
        conn.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);")
            .map_err(|e| {
                pyo3::exceptions::PyRuntimeError::new_err(format!(
                    "Failed to checkpoint WAL: {}",
                    e
                ))
            })?;
        Ok(())
    }
}

// Internal implementation methods
impl PytestDiffDatabase {
    fn save_test_execution_internal(
        &mut self,
        test_name: &str,
        fingerprints: Vec<Fingerprint>,
        duration: f64,
        failed: bool,
        python_version: &str,
    ) -> Result<()> {
        // Get or create environment
        let env_id = self.get_or_create_environment("default", python_version)?;

        let mut conn = self.conn.write();

        // Use BEGIN IMMEDIATE for fail-fast on write conflicts (pytest-xdist compatibility)
        let tx = conn.transaction_with_behavior(rusqlite::TransactionBehavior::Immediate)?;

        // Delete previous executions for this test in this environment
        // This keeps the database from growing unbounded
        tx.execute(
            "DELETE FROM test_execution
             WHERE environment_id = ?1 AND test_name = ?2",
            params![env_id, test_name],
        )
        .context("Failed to delete old test execution")?;

        // Insert test execution
        tx.execute(
            "INSERT INTO test_execution (environment_id, test_name, duration, failed, forced)
             VALUES (?1, ?2, ?3, ?4, ?5)",
            params![env_id, test_name, duration, if failed { 1 } else { 0 }, 0],
        )
        .context("Failed to insert test execution")?;

        let test_execution_id = tx.last_insert_rowid();

        // Insert fingerprints and link to test
        for fp in fingerprints {
            let fp_id = self.get_or_create_fingerprint_in_tx(&tx, &fp)?;

            tx.execute(
                "INSERT INTO test_execution_file_fp (test_execution_id, fingerprint_id)
                 VALUES (?1, ?2)",
                params![test_execution_id, fp_id],
            )
            .context("Failed to link test to fingerprint")?;
        }

        tx.commit().context("Failed to commit transaction")?;

        Ok(())
    }

    fn get_or_create_fingerprint_in_tx(
        &self,
        tx: &rusqlite::Transaction,
        fp: &Fingerprint,
    ) -> Result<i64> {
        let checksums_blob = serialize_checksums(&fp.checksums);

        let existing_id: Option<i64> = tx
            .query_row(
                "SELECT id FROM file_fp
                 WHERE filename = ?1 AND fsha = ?2 AND method_checksums = ?3",
                params![&fp.filename, &fp.file_hash, checksums_blob],
                |row| row.get(0),
            )
            .optional()?;

        if let Some(id) = existing_id {
            // Exact match found - reuse it
            Ok(id)
        } else {
            // No exact match - insert new fingerprint
            // We always insert new fingerprints to maintain history
            // Change detection relies on comparing current state vs stored state
            tx.execute(
                "INSERT INTO file_fp (filename, method_checksums, mtime, fsha)
                 VALUES (?1, ?2, ?3, ?4)",
                params![&fp.filename, checksums_blob, fp.mtime, &fp.file_hash],
            )?;
            Ok(tx.last_insert_rowid())
        }
    }

    fn get_affected_tests_internal(
        &self,
        changed_blocks: HashMap<String, Vec<i32>>,
    ) -> Result<Vec<String>> {
        if changed_blocks.is_empty() {
            return Ok(vec![]);
        }

        let conn = self.conn.read();

        // Build a single query for all changed files (more efficient than N queries)
        let filenames: Vec<&str> = changed_blocks.keys().map(|s| s.as_str()).collect();

        // Create placeholders for IN clause: (?1, ?2, ?3, ...)
        let placeholders: String = (1..=filenames.len())
            .map(|i| format!("?{}", i))
            .collect::<Vec<_>>()
            .join(", ");

        let query = format!(
            "SELECT DISTINCT te.test_name, fp.filename, fp.method_checksums
             FROM test_execution te
             JOIN test_execution_file_fp teff ON te.id = teff.test_execution_id
             JOIN file_fp fp ON teff.fingerprint_id = fp.id
             WHERE fp.filename IN ({})",
            placeholders
        );

        let mut stmt = conn.prepare(&query)?;

        // Convert filenames to rusqlite params
        let params: Vec<&dyn rusqlite::ToSql> = filenames
            .iter()
            .map(|s| s as &dyn rusqlite::ToSql)
            .collect();

        // Pre-compute changed checksums as HashSets for O(1) lookup
        let changed_checksum_sets: HashMap<&str, HashSet<i32>> = changed_blocks
            .iter()
            .map(|(filename, checksums)| (filename.as_str(), checksums.iter().copied().collect()))
            .collect();

        // Cache deserialized blobs to avoid re-deserializing the same blob
        let mut blob_cache: HashMap<Vec<u8>, Vec<i32>> = HashMap::new();

        let mut affected_tests: HashSet<String> = HashSet::new();

        let rows = stmt.query_map(params.as_slice(), |row| {
            let test_name: String = row.get(0)?;
            let filename: String = row.get(1)?;
            let blob: Vec<u8> = row.get(2)?;
            Ok((test_name, filename, blob))
        })?;

        for row_result in rows {
            let (test_name, filename, blob) = row_result?;

            // Get or compute deserialized checksums (cache for efficiency)
            let file_checksums = blob_cache
                .entry(blob.clone())
                .or_insert_with(|| deserialize_checksums(&blob));

            // Check if any changed checksum for this file matches
            if let Some(changed_set) = changed_checksum_sets.get(filename.as_str()) {
                if file_checksums.iter().any(|c| changed_set.contains(c)) {
                    affected_tests.insert(test_name);
                }
            }
        }

        // Convert HashSet to sorted Vec for consistent ordering
        let mut result: Vec<String> = affected_tests.into_iter().collect();
        result.sort();

        Ok(result)
    }

    fn get_stats_internal(&self) -> Result<HashMap<String, i64>> {
        let conn = self.conn.read();
        let mut stats = HashMap::new();

        // Count tests
        let test_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM test_execution", [], |row| row.get(0))?;
        stats.insert("test_count".to_string(), test_count);

        // Count files
        let file_count: i64 =
            conn.query_row("SELECT COUNT(DISTINCT filename) FROM file_fp", [], |row| {
                row.get(0)
            })?;
        stats.insert("file_count".to_string(), file_count);

        // Count fingerprints
        let fp_count: i64 = conn.query_row("SELECT COUNT(*) FROM file_fp", [], |row| row.get(0))?;
        stats.insert("fingerprint_count".to_string(), fp_count);

        // Count baselines
        let baseline_count: i64 =
            conn.query_row("SELECT COUNT(*) FROM baseline_fp", [], |row| row.get(0))?;
        stats.insert("baseline_count".to_string(), baseline_count);

        Ok(stats)
    }

    pub fn save_baseline_fingerprint_internal(&mut self, fp: Fingerprint) -> Result<()> {
        let conn = self.conn.write();
        let checksums_blob = serialize_checksums(&fp.checksums);

        // Use INSERT OR REPLACE to update existing baseline
        conn.execute(
            "INSERT OR REPLACE INTO baseline_fp (filename, method_checksums, mtime, fsha)
             VALUES (?1, ?2, ?3, ?4)",
            params![&fp.filename, checksums_blob, fp.mtime, &fp.file_hash],
        )
        .context("Failed to save baseline fingerprint")?;

        Ok(())
    }

    /// Batch save multiple baseline fingerprints in a single transaction
    pub fn save_baseline_fingerprints_batch(
        &mut self,
        fingerprints: Vec<Fingerprint>,
    ) -> Result<usize> {
        let mut conn = self.conn.write();

        // Start transaction
        let tx = conn.transaction()?;

        let mut count = 0;
        for fp in fingerprints {
            let checksums_blob = serialize_checksums(&fp.checksums);

            tx.execute(
                "INSERT OR REPLACE INTO baseline_fp (filename, method_checksums, mtime, fsha)
                 VALUES (?1, ?2, ?3, ?4)",
                params![&fp.filename, checksums_blob, fp.mtime, &fp.file_hash],
            )
            .context("Failed to save baseline fingerprint in batch")?;

            count += 1;
        }

        // Commit transaction
        tx.commit()?;

        Ok(count)
    }

    /// Check if a table exists in the attached source database.
    /// Used for backward compatibility with older databases that may not have
    /// test execution tables.
    fn source_table_exists(conn: &Connection, table_name: &str) -> Result<bool> {
        let count: i64 = conn
            .query_row(
                "SELECT COUNT(*) FROM source_db.sqlite_master WHERE type='table' AND name=?1",
                params![table_name],
                |row| row.get(0),
            )
            .context("Failed to check source table existence")?;
        Ok(count > 0)
    }

    fn import_baseline_from_internal(&mut self, source_db_path: &str) -> Result<ImportResult> {
        // Verify source file exists
        if !Path::new(source_db_path).exists() {
            anyhow::bail!("Source database does not exist: {}", source_db_path);
        }

        let conn = self.conn.write();

        // Attach the source database
        conn.execute("ATTACH DATABASE ?1 AS source_db", params![source_db_path])
            .with_context(|| format!("Failed to attach source database: {}", source_db_path))?;

        // Clear existing data and bulk-copy from source
        let result = (|| -> Result<ImportResult> {
            conn.execute("DELETE FROM baseline_fp", [])
                .context("Failed to clear existing baselines")?;

            let baseline_count = conn
                .execute(
                    "INSERT INTO baseline_fp (filename, method_checksums, mtime, fsha, created_at)
                 SELECT filename, method_checksums, mtime, fsha, created_at
                 FROM source_db.baseline_fp",
                    [],
                )
                .context("Failed to copy baselines from source")?;

            // Also copy metadata rows from source (e.g. baseline_commit SHA)
            conn.execute(
                "INSERT OR REPLACE INTO metadata (dataid, data)
                 SELECT dataid, data FROM source_db.metadata",
                [],
            )
            .context("Failed to copy metadata from source")?;

            // Copy test execution data if source has those tables (backward compat)
            let test_execution_count = if Self::source_table_exists(&conn, "test_execution")? {
                // Delete existing test execution data (in FK order)
                conn.execute("DELETE FROM test_execution_file_fp", [])
                    .context("Failed to clear test_execution_file_fp")?;
                conn.execute("DELETE FROM test_execution", [])
                    .context("Failed to clear test_execution")?;
                conn.execute("DELETE FROM file_fp", [])
                    .context("Failed to clear file_fp")?;
                conn.execute("DELETE FROM environment", [])
                    .context("Failed to clear environment")?;

                // Copy source IDs directly (no collision since we cleared everything)
                conn.execute(
                        "INSERT INTO environment (id, environment_name, system_packages, python_version)
                         SELECT id, environment_name, system_packages, python_version
                         FROM source_db.environment",
                        [],
                    )
                    .context("Failed to copy environment from source")?;

                conn.execute(
                    "INSERT INTO file_fp (id, filename, method_checksums, mtime, fsha)
                         SELECT id, filename, method_checksums, mtime, fsha
                         FROM source_db.file_fp",
                    [],
                )
                .context("Failed to copy file_fp from source")?;

                let te_count = conn
                        .execute(
                            "INSERT INTO test_execution (id, environment_id, test_name, duration, failed, forced)
                             SELECT id, environment_id, test_name, duration, failed, forced
                             FROM source_db.test_execution",
                            [],
                        )
                        .context("Failed to copy test_execution from source")?;

                conn.execute(
                    "INSERT INTO test_execution_file_fp (test_execution_id, fingerprint_id)
                         SELECT test_execution_id, fingerprint_id
                         FROM source_db.test_execution_file_fp",
                    [],
                )
                .context("Failed to copy test_execution_file_fp from source")?;

                te_count
            } else {
                0
            };

            Ok(ImportResult {
                baseline_count,
                test_execution_count,
            })
        })();

        // Always detach, even if the copy failed
        conn.execute("DETACH DATABASE source_db", [])
            .context("Failed to detach source database")?;

        // Invalidate cached environment ID since we replaced all environments
        drop(conn);
        {
            let mut cached_id = self.current_environment_id.write();
            *cached_id = None;
        }

        result
    }

    fn merge_baseline_from_internal(&mut self, source_db_path: &str) -> Result<ImportResult> {
        // Verify source file exists
        if !Path::new(source_db_path).exists() {
            anyhow::bail!("Source database does not exist: {}", source_db_path);
        }

        let conn = self.conn.write();

        // Attach the source database
        conn.execute("ATTACH DATABASE ?1 AS source_db", params![source_db_path])
            .with_context(|| format!("Failed to attach source database: {}", source_db_path))?;

        // Merge baselines using INSERT OR REPLACE (does NOT clear existing baselines)
        let result = (|| -> Result<ImportResult> {
            let baseline_count = conn
                .execute(
                    "INSERT OR REPLACE INTO baseline_fp (filename, method_checksums, mtime, fsha, created_at)
                     SELECT filename, method_checksums, mtime, fsha, created_at
                     FROM source_db.baseline_fp",
                    [],
                )
                .context("Failed to merge baselines from source")?;

            // Also merge metadata rows from source (e.g. baseline_commit SHA)
            conn.execute(
                "INSERT OR REPLACE INTO metadata (dataid, data)
                 SELECT dataid, data FROM source_db.metadata",
                [],
            )
            .context("Failed to merge metadata from source")?;

            // Merge test execution data if source has those tables (backward compat)
            let test_execution_count = if Self::source_table_exists(&conn, "test_execution")? {
                // 1. Merge environments (natural key: name+packages+version)
                conn.execute(
                        "INSERT OR IGNORE INTO environment (environment_name, system_packages, python_version)
                         SELECT environment_name, system_packages, python_version
                         FROM source_db.environment",
                        [],
                    )
                    .context("Failed to merge environment from source")?;

                // 2. Merge file fingerprints (natural key: filename+fsha+checksums)
                conn.execute(
                    "INSERT OR IGNORE INTO file_fp (filename, method_checksums, mtime, fsha)
                         SELECT filename, method_checksums, mtime, fsha
                         FROM source_db.file_fp",
                    [],
                )
                .context("Failed to merge file_fp from source")?;

                // 3. Delete stale test executions for tests that exist in the source
                //    CASCADE will clean up test_execution_file_fp
                conn.execute(
                    "DELETE FROM test_execution
                         WHERE test_name IN (SELECT test_name FROM source_db.test_execution)",
                    [],
                )
                .context("Failed to delete stale test executions")?;

                // 4. Insert test executions with remapped environment_id
                let te_count = conn
                        .execute(
                            "INSERT INTO test_execution (environment_id, test_name, duration, failed, forced)
                             SELECT e.id, ste.test_name, ste.duration, ste.failed, ste.forced
                             FROM source_db.test_execution ste
                             JOIN source_db.environment se ON ste.environment_id = se.id
                             JOIN environment e ON e.environment_name = se.environment_name
                                AND e.system_packages = se.system_packages
                                AND e.python_version = se.python_version",
                            [],
                        )
                        .context("Failed to merge test_execution from source")?;

                // 5. Insert junction rows with remapped IDs via natural key joins
                conn.execute(
                        "INSERT OR IGNORE INTO test_execution_file_fp (test_execution_id, fingerprint_id)
                         SELECT te.id, fp.id
                         FROM source_db.test_execution_file_fp steff
                         JOIN source_db.test_execution ste ON steff.test_execution_id = ste.id
                         JOIN source_db.environment se ON ste.environment_id = se.id
                         JOIN source_db.file_fp sfp ON steff.fingerprint_id = sfp.id
                         JOIN environment e ON e.environment_name = se.environment_name
                            AND e.system_packages = se.system_packages
                            AND e.python_version = se.python_version
                         JOIN test_execution te ON te.test_name = ste.test_name
                            AND te.environment_id = e.id
                         JOIN file_fp fp ON fp.filename = sfp.filename
                            AND fp.fsha = sfp.fsha
                            AND fp.method_checksums = sfp.method_checksums",
                        [],
                    )
                    .context("Failed to merge test_execution_file_fp from source")?;

                te_count
            } else {
                0
            };

            Ok(ImportResult {
                baseline_count,
                test_execution_count,
            })
        })();

        // Always detach, even if the merge failed
        conn.execute("DETACH DATABASE source_db", [])
            .context("Failed to detach source database")?;

        // Invalidate cached environment ID since we may have added environments
        drop(conn);
        {
            let mut cached_id = self.current_environment_id.write();
            *cached_id = None;
        }

        result
    }

    fn get_external_metadata_internal(
        &self,
        source_db_path: &str,
        key: &str,
    ) -> Result<Option<String>> {
        // Verify source file exists
        if !Path::new(source_db_path).exists() {
            anyhow::bail!("Source database does not exist: {}", source_db_path);
        }

        // ATTACH requires a write lock
        let conn = self.conn.write();

        // Attach the source database
        conn.execute("ATTACH DATABASE ?1 AS source_db", params![source_db_path])
            .with_context(|| format!("Failed to attach source database: {}", source_db_path))?;

        let result = conn
            .query_row(
                "SELECT data FROM source_db.metadata WHERE dataid = ?1",
                params![key],
                |row| row.get(0),
            )
            .optional()
            .context("Failed to query external metadata");

        // Always detach
        conn.execute("DETACH DATABASE source_db", [])
            .context("Failed to detach source database")?;

        result
    }

    fn set_metadata_internal(&self, key: &str, value: &str) -> Result<()> {
        let conn = self.conn.write();
        conn.execute(
            "INSERT OR REPLACE INTO metadata (dataid, data) VALUES (?1, ?2)",
            params![key, value],
        )
        .context("Failed to set metadata")?;
        Ok(())
    }

    fn get_metadata_internal(&self, key: &str) -> Result<Option<String>> {
        let conn = self.conn.read();
        conn.query_row(
            "SELECT data FROM metadata WHERE dataid = ?1",
            params![key],
            |row| row.get(0),
        )
        .optional()
        .context("Failed to get metadata")
    }

    fn get_baseline_fingerprint_internal(&self, filename: &str) -> Result<Option<Fingerprint>> {
        let conn = self.conn.read();

        conn.query_row(
            "SELECT filename, method_checksums, mtime, fsha
             FROM baseline_fp
             WHERE filename = ?1",
            params![filename],
            |row| {
                let checksums_blob: Vec<u8> = row.get(1)?;
                let checksums = deserialize_checksums(&checksums_blob);

                Ok(Fingerprint {
                    filename: row.get(0)?,
                    checksums,
                    mtime: row.get(2)?,
                    file_hash: row.get(3)?,
                    blocks: None,
                })
            },
        )
        .optional()
        .context("Failed to query baseline fingerprint")
    }

    /// Get all baseline fingerprints in a single query
    ///
    /// Returns a HashMap of filename -> Fingerprint for efficient lookup
    pub fn get_all_baseline_fingerprints(&self) -> Result<HashMap<String, Fingerprint>> {
        let conn = self.conn.read();

        let mut stmt =
            conn.prepare("SELECT filename, method_checksums, mtime, fsha FROM baseline_fp")?;

        let fingerprints = stmt
            .query_map([], |row| {
                let filename: String = row.get(0)?;
                let checksums_blob: Vec<u8> = row.get(1)?;
                let checksums = deserialize_checksums(&checksums_blob);

                Ok((
                    filename.clone(),
                    Fingerprint {
                        filename,
                        checksums,
                        mtime: row.get(2)?,
                        file_hash: row.get(3)?,
                        blocks: None,
                    },
                ))
            })?
            .filter_map(|r| r.ok())
            .collect();

        Ok(fingerprints)
    }
}

/// Serialize checksums (Vec<i32>) to blob
fn serialize_checksums(checksums: &[i32]) -> Vec<u8> {
    checksums.iter().flat_map(|c| c.to_le_bytes()).collect()
}

/// Deserialize checksums from blob to Vec<i32>
fn deserialize_checksums(blob: &[u8]) -> Vec<i32> {
    blob.chunks_exact(4)
        .map(|chunk| i32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_database_creation() {
        let temp_db = NamedTempFile::new().unwrap();
        let db = PytestDiffDatabase::new_internal(temp_db.path().to_str().unwrap());
        assert!(db.is_ok());
    }

    #[test]
    fn test_save_and_retrieve_fingerprint() {
        let temp_db = NamedTempFile::new().unwrap();
        let db = PytestDiffDatabase::new_internal(temp_db.path().to_str().unwrap()).unwrap();

        let fp = Fingerprint {
            filename: "test.py".to_string(),
            checksums: vec![123, 456, 789],
            file_hash: "abc123".to_string(),
            mtime: 1.0,
            blocks: None,
        };

        let fp_id = db.get_or_create_fingerprint(&fp).unwrap();
        assert!(fp_id > 0);

        // Retrieving again should return same ID
        let fp_id2 = db.get_or_create_fingerprint(&fp).unwrap();
        assert_eq!(fp_id, fp_id2);
    }

    #[test]
    fn test_save_test_execution() {
        let temp_db = NamedTempFile::new().unwrap();
        let mut db = PytestDiffDatabase::new_internal(temp_db.path().to_str().unwrap()).unwrap();

        let fp = Fingerprint {
            filename: "test.py".to_string(),
            checksums: vec![123],
            file_hash: "abc".to_string(),
            mtime: 1.0,
            blocks: None,
        };

        db.save_test_execution_internal("test_example", vec![fp], 0.5, false, "3.12")
            .unwrap();

        let stats = db.get_stats_internal().unwrap();
        assert_eq!(stats["test_count"], 1);
        assert_eq!(stats["file_count"], 1);
    }

    #[test]
    fn test_checksum_serialization() {
        let checksums = vec![123, -456, 789, -1];
        let blob = serialize_checksums(&checksums);
        let deserialized = deserialize_checksums(&blob);

        assert_eq!(checksums, deserialized);
    }

    #[test]
    fn test_import_baseline_from() {
        // Create source database with baseline fingerprints
        let source_db_file = NamedTempFile::new().unwrap();
        let mut source_db =
            PytestDiffDatabase::new_internal(source_db_file.path().to_str().unwrap()).unwrap();

        let fp1 = Fingerprint {
            filename: "src/foo.py".to_string(),
            checksums: vec![10, 20, 30],
            file_hash: "hash_foo".to_string(),
            mtime: 1.0,
            blocks: None,
        };
        let fp2 = Fingerprint {
            filename: "src/bar.py".to_string(),
            checksums: vec![40, 50],
            file_hash: "hash_bar".to_string(),
            mtime: 2.0,
            blocks: None,
        };

        source_db.save_baseline_fingerprint_internal(fp1).unwrap();
        source_db.save_baseline_fingerprint_internal(fp2).unwrap();
        source_db.close_and_checkpoint().unwrap();

        // Create target database (empty)
        let target_db_file = NamedTempFile::new().unwrap();
        let mut target_db =
            PytestDiffDatabase::new_internal(target_db_file.path().to_str().unwrap()).unwrap();

        // Verify target has no baselines
        let stats = target_db.get_stats_internal().unwrap();
        assert_eq!(stats["baseline_count"], 0);

        // Import from source
        let result = target_db
            .import_baseline_from_internal(source_db_file.path().to_str().unwrap())
            .unwrap();
        assert_eq!(result.baseline_count, 2);
        assert_eq!(result.test_execution_count, 0);

        // Verify baselines were imported
        let stats = target_db.get_stats_internal().unwrap();
        assert_eq!(stats["baseline_count"], 2);

        let imported_fp = target_db
            .get_baseline_fingerprint_internal("src/foo.py")
            .unwrap()
            .unwrap();
        assert_eq!(imported_fp.checksums, vec![10, 20, 30]);
        assert_eq!(imported_fp.file_hash, "hash_foo");
    }

    #[test]
    fn test_import_baseline_from_nonexistent() {
        let temp_db = NamedTempFile::new().unwrap();
        let mut db = PytestDiffDatabase::new_internal(temp_db.path().to_str().unwrap()).unwrap();

        let result = db.import_baseline_from_internal("/nonexistent/path.db");
        assert!(result.is_err());
    }

    #[test]
    fn test_metadata_set_and_get() {
        let temp_db = NamedTempFile::new().unwrap();
        let db = PytestDiffDatabase::new_internal(temp_db.path().to_str().unwrap()).unwrap();

        // Initially missing
        assert_eq!(db.get_metadata_internal("baseline_commit").unwrap(), None);

        // Set and retrieve
        db.set_metadata_internal("baseline_commit", "abc123def")
            .unwrap();
        assert_eq!(
            db.get_metadata_internal("baseline_commit").unwrap(),
            Some("abc123def".to_string())
        );

        // Overwrite
        db.set_metadata_internal("baseline_commit", "new_sha")
            .unwrap();
        assert_eq!(
            db.get_metadata_internal("baseline_commit").unwrap(),
            Some("new_sha".to_string())
        );
    }

    #[test]
    fn test_import_baseline_copies_metadata() {
        // Create source database with baseline + metadata
        let source_db_file = NamedTempFile::new().unwrap();
        let mut source_db =
            PytestDiffDatabase::new_internal(source_db_file.path().to_str().unwrap()).unwrap();

        let fp = Fingerprint {
            filename: "src/foo.py".to_string(),
            checksums: vec![10, 20],
            file_hash: "hash_foo".to_string(),
            mtime: 1.0,
            blocks: None,
        };
        source_db.save_baseline_fingerprint_internal(fp).unwrap();
        source_db
            .set_metadata_internal("baseline_commit", "source_sha_123")
            .unwrap();
        source_db.close_and_checkpoint().unwrap();

        // Create target database
        let target_db_file = NamedTempFile::new().unwrap();
        let mut target_db =
            PytestDiffDatabase::new_internal(target_db_file.path().to_str().unwrap()).unwrap();

        // Verify no metadata initially
        assert_eq!(
            target_db.get_metadata_internal("baseline_commit").unwrap(),
            None
        );

        // Import from source
        target_db
            .import_baseline_from_internal(source_db_file.path().to_str().unwrap())
            .unwrap();

        // Verify metadata was copied
        assert_eq!(
            target_db.get_metadata_internal("baseline_commit").unwrap(),
            Some("source_sha_123".to_string())
        );
    }

    #[test]
    fn test_merge_baseline_from() {
        // Create first source database with some baselines
        let source1_file = NamedTempFile::new().unwrap();
        let mut source1_db =
            PytestDiffDatabase::new_internal(source1_file.path().to_str().unwrap()).unwrap();

        let fp1 = Fingerprint {
            filename: "src/foo.py".to_string(),
            checksums: vec![10, 20, 30],
            file_hash: "hash_foo".to_string(),
            mtime: 1.0,
            blocks: None,
        };
        source1_db.save_baseline_fingerprint_internal(fp1).unwrap();
        source1_db.close_and_checkpoint().unwrap();

        // Create second source database with different baselines
        let source2_file = NamedTempFile::new().unwrap();
        let mut source2_db =
            PytestDiffDatabase::new_internal(source2_file.path().to_str().unwrap()).unwrap();

        let fp2 = Fingerprint {
            filename: "src/bar.py".to_string(),
            checksums: vec![40, 50],
            file_hash: "hash_bar".to_string(),
            mtime: 2.0,
            blocks: None,
        };
        source2_db.save_baseline_fingerprint_internal(fp2).unwrap();
        source2_db.close_and_checkpoint().unwrap();

        // Create target database and merge both sources
        let target_db_file = NamedTempFile::new().unwrap();
        let mut target_db =
            PytestDiffDatabase::new_internal(target_db_file.path().to_str().unwrap()).unwrap();

        // Verify target has no baselines
        let stats = target_db.get_stats_internal().unwrap();
        assert_eq!(stats["baseline_count"], 0);

        // Merge first source
        let result1 = target_db
            .merge_baseline_from_internal(source1_file.path().to_str().unwrap())
            .unwrap();
        assert_eq!(result1.baseline_count, 1);
        assert_eq!(result1.test_execution_count, 0);

        // Verify first merge
        let stats = target_db.get_stats_internal().unwrap();
        assert_eq!(stats["baseline_count"], 1);

        // Merge second source (should accumulate, not replace)
        let result2 = target_db
            .merge_baseline_from_internal(source2_file.path().to_str().unwrap())
            .unwrap();
        assert_eq!(result2.baseline_count, 1);
        assert_eq!(result2.test_execution_count, 0);

        // Verify both baselines exist
        let stats = target_db.get_stats_internal().unwrap();
        assert_eq!(stats["baseline_count"], 2);

        // Verify both fingerprints are accessible
        let imported_fp1 = target_db
            .get_baseline_fingerprint_internal("src/foo.py")
            .unwrap()
            .unwrap();
        assert_eq!(imported_fp1.checksums, vec![10, 20, 30]);

        let imported_fp2 = target_db
            .get_baseline_fingerprint_internal("src/bar.py")
            .unwrap()
            .unwrap();
        assert_eq!(imported_fp2.checksums, vec![40, 50]);
    }

    #[test]
    fn test_merge_baseline_from_replaces_same_file() {
        // Test that merging a database with the same file replaces it
        let source1_file = NamedTempFile::new().unwrap();
        let mut source1_db =
            PytestDiffDatabase::new_internal(source1_file.path().to_str().unwrap()).unwrap();

        let fp1 = Fingerprint {
            filename: "src/foo.py".to_string(),
            checksums: vec![10, 20],
            file_hash: "hash_old".to_string(),
            mtime: 1.0,
            blocks: None,
        };
        source1_db.save_baseline_fingerprint_internal(fp1).unwrap();
        source1_db.close_and_checkpoint().unwrap();

        // Create second source with same filename but different content
        let source2_file = NamedTempFile::new().unwrap();
        let mut source2_db =
            PytestDiffDatabase::new_internal(source2_file.path().to_str().unwrap()).unwrap();

        let fp2 = Fingerprint {
            filename: "src/foo.py".to_string(),
            checksums: vec![30, 40, 50],
            file_hash: "hash_new".to_string(),
            mtime: 2.0,
            blocks: None,
        };
        source2_db.save_baseline_fingerprint_internal(fp2).unwrap();
        source2_db.close_and_checkpoint().unwrap();

        // Merge both into target
        let target_db_file = NamedTempFile::new().unwrap();
        let mut target_db =
            PytestDiffDatabase::new_internal(target_db_file.path().to_str().unwrap()).unwrap();

        target_db
            .merge_baseline_from_internal(source1_file.path().to_str().unwrap())
            .unwrap();
        target_db
            .merge_baseline_from_internal(source2_file.path().to_str().unwrap())
            .unwrap();

        // Should still have 1 baseline (replaced)
        let stats = target_db.get_stats_internal().unwrap();
        assert_eq!(stats["baseline_count"], 1);

        // The newer version should win
        let imported_fp = target_db
            .get_baseline_fingerprint_internal("src/foo.py")
            .unwrap()
            .unwrap();
        assert_eq!(imported_fp.checksums, vec![30, 40, 50]);
        assert_eq!(imported_fp.file_hash, "hash_new");
    }

    #[test]
    fn test_get_affected_tests() {
        let temp_db = NamedTempFile::new().unwrap();
        let mut db = PytestDiffDatabase::new_internal(temp_db.path().to_str().unwrap()).unwrap();

        let fp = Fingerprint {
            filename: "module.py".to_string(),
            checksums: vec![100, 200],
            file_hash: "hash1".to_string(),
            mtime: 1.0,
            blocks: None,
        };

        db.save_test_execution_internal("test_one", vec![fp.clone()], 0.1, false, "3.12")
            .unwrap();
        db.save_test_execution_internal("test_two", vec![fp], 0.2, false, "3.12")
            .unwrap();

        let mut changed = HashMap::new();
        changed.insert("module.py".to_string(), vec![100]);

        let affected = db.get_affected_tests_internal(changed).unwrap();
        assert_eq!(affected.len(), 2);
        assert!(affected.contains(&"test_one".to_string()));
        assert!(affected.contains(&"test_two".to_string()));
    }

    #[test]
    fn test_import_baseline_copies_test_executions() {
        // Create source database with test execution data
        let source_db_file = NamedTempFile::new().unwrap();
        let mut source_db =
            PytestDiffDatabase::new_internal(source_db_file.path().to_str().unwrap()).unwrap();

        let fp = Fingerprint {
            filename: "module.py".to_string(),
            checksums: vec![100, 200],
            file_hash: "hash1".to_string(),
            mtime: 1.0,
            blocks: None,
        };

        source_db
            .save_test_execution_internal("test_one", vec![fp.clone()], 0.1, false, "3.12")
            .unwrap();
        source_db
            .save_test_execution_internal("test_two", vec![fp], 0.2, false, "3.12")
            .unwrap();
        source_db
            .save_baseline_fingerprint_internal(Fingerprint {
                filename: "module.py".to_string(),
                checksums: vec![100, 200],
                file_hash: "hash1".to_string(),
                mtime: 1.0,
                blocks: None,
            })
            .unwrap();
        source_db.close_and_checkpoint().unwrap();

        // Import into target
        let target_db_file = NamedTempFile::new().unwrap();
        let mut target_db =
            PytestDiffDatabase::new_internal(target_db_file.path().to_str().unwrap()).unwrap();

        let result = target_db
            .import_baseline_from_internal(source_db_file.path().to_str().unwrap())
            .unwrap();
        assert_eq!(result.baseline_count, 1);
        assert_eq!(result.test_execution_count, 2);

        // Verify get_affected_tests works on the imported data
        let mut changed = HashMap::new();
        changed.insert("module.py".to_string(), vec![100]);

        let affected = target_db.get_affected_tests_internal(changed).unwrap();
        assert_eq!(affected.len(), 2);
        assert!(affected.contains(&"test_one".to_string()));
        assert!(affected.contains(&"test_two".to_string()));
    }

    #[test]
    fn test_merge_copies_test_executions_with_remap() {
        // Create first source with test execution data
        let source1_file = NamedTempFile::new().unwrap();
        let mut source1_db =
            PytestDiffDatabase::new_internal(source1_file.path().to_str().unwrap()).unwrap();

        let fp1 = Fingerprint {
            filename: "module_a.py".to_string(),
            checksums: vec![100],
            file_hash: "hash_a".to_string(),
            mtime: 1.0,
            blocks: None,
        };
        source1_db
            .save_test_execution_internal("test_alpha", vec![fp1], 0.1, false, "3.12")
            .unwrap();
        source1_db.close_and_checkpoint().unwrap();

        // Create second source with different test execution data
        let source2_file = NamedTempFile::new().unwrap();
        let mut source2_db =
            PytestDiffDatabase::new_internal(source2_file.path().to_str().unwrap()).unwrap();

        let fp2 = Fingerprint {
            filename: "module_b.py".to_string(),
            checksums: vec![200],
            file_hash: "hash_b".to_string(),
            mtime: 2.0,
            blocks: None,
        };
        source2_db
            .save_test_execution_internal("test_beta", vec![fp2], 0.2, false, "3.12")
            .unwrap();
        source2_db.close_and_checkpoint().unwrap();

        // Merge both into target
        let target_db_file = NamedTempFile::new().unwrap();
        let mut target_db =
            PytestDiffDatabase::new_internal(target_db_file.path().to_str().unwrap()).unwrap();

        let result1 = target_db
            .merge_baseline_from_internal(source1_file.path().to_str().unwrap())
            .unwrap();
        assert_eq!(result1.test_execution_count, 1);

        let result2 = target_db
            .merge_baseline_from_internal(source2_file.path().to_str().unwrap())
            .unwrap();
        assert_eq!(result2.test_execution_count, 1);

        // Verify both tests are found via get_affected_tests
        let mut changed_a = HashMap::new();
        changed_a.insert("module_a.py".to_string(), vec![100]);
        let affected_a = target_db.get_affected_tests_internal(changed_a).unwrap();
        assert_eq!(affected_a, vec!["test_alpha"]);

        let mut changed_b = HashMap::new();
        changed_b.insert("module_b.py".to_string(), vec![200]);
        let affected_b = target_db.get_affected_tests_internal(changed_b).unwrap();
        assert_eq!(affected_b, vec!["test_beta"]);
    }

    #[test]
    fn test_import_from_old_db_without_test_data() {
        // Create a source database that only has baseline_fp (simulates old DB format)
        // Since all DBs created by new_internal have the full schema, we simulate
        // an "old" DB by just not inserting any test data
        let source_db_file = NamedTempFile::new().unwrap();
        let mut source_db =
            PytestDiffDatabase::new_internal(source_db_file.path().to_str().unwrap()).unwrap();

        source_db
            .save_baseline_fingerprint_internal(Fingerprint {
                filename: "module.py".to_string(),
                checksums: vec![42],
                file_hash: "hash42".to_string(),
                mtime: 1.0,
                blocks: None,
            })
            .unwrap();
        source_db.close_and_checkpoint().unwrap();

        // Import into target
        let target_db_file = NamedTempFile::new().unwrap();
        let mut target_db =
            PytestDiffDatabase::new_internal(target_db_file.path().to_str().unwrap()).unwrap();

        let result = target_db
            .import_baseline_from_internal(source_db_file.path().to_str().unwrap())
            .unwrap();
        assert_eq!(result.baseline_count, 1);
        assert_eq!(result.test_execution_count, 0);

        // Verify baseline was imported
        let stats = target_db.get_stats_internal().unwrap();
        assert_eq!(stats["baseline_count"], 1);
    }

    #[test]
    fn test_merge_from_old_db_without_test_data() {
        // Same as above but for merge path
        let source_db_file = NamedTempFile::new().unwrap();
        let mut source_db =
            PytestDiffDatabase::new_internal(source_db_file.path().to_str().unwrap()).unwrap();

        source_db
            .save_baseline_fingerprint_internal(Fingerprint {
                filename: "module.py".to_string(),
                checksums: vec![42],
                file_hash: "hash42".to_string(),
                mtime: 1.0,
                blocks: None,
            })
            .unwrap();
        source_db.close_and_checkpoint().unwrap();

        // Merge into target
        let target_db_file = NamedTempFile::new().unwrap();
        let mut target_db =
            PytestDiffDatabase::new_internal(target_db_file.path().to_str().unwrap()).unwrap();

        let result = target_db
            .merge_baseline_from_internal(source_db_file.path().to_str().unwrap())
            .unwrap();
        assert_eq!(result.baseline_count, 1);
        assert_eq!(result.test_execution_count, 0);

        // Verify baseline was merged
        let stats = target_db.get_stats_internal().unwrap();
        assert_eq!(stats["baseline_count"], 1);
    }
}
