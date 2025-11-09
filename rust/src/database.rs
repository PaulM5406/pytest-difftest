// SQLite database layer with caching
//
// This module provides:
// - SQLite database operations for test executions and fingerprints
// - In-memory caching for hot paths
// - Prepared statement management
// - Concurrent access support (WAL mode)

use anyhow::{Context, Result};
use parking_lot::RwLock;
use pyo3::prelude::*;
use rusqlite::{params, Connection, OptionalExtension, Row};
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use crate::cache::Cache;
use crate::types::Fingerprint;

/// Main database interface for pytest-diff
///
/// Manages the .testmondata SQLite database with optimizations:
/// - WAL mode for concurrent access
/// - Prepared statement caching
/// - Memory-mapped I/O
/// - In-memory cache for frequently accessed data
#[pyclass]
pub struct TestmonDatabase {
    conn: Arc<RwLock<Connection>>,
    cache: Arc<Cache>,
    db_path: String,
    current_environment_id: Arc<RwLock<Option<i64>>>,
}

impl TestmonDatabase {
    /// Create a new database connection with optimizations
    fn new_internal(path: &str) -> Result<Self> {
        let path_obj = Path::new(path);

        // Create parent directory if it doesn't exist
        if let Some(parent) = path_obj.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("Failed to create directory: {:?}", parent))?;
        }

        let conn = Connection::open(path)
            .with_context(|| format!("Failed to open database: {}", path))?;

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

        Ok(Self {
            conn: Arc::new(RwLock::new(conn)),
            cache: Arc::new(Cache::new()),
            db_path: path.to_string(),
            current_environment_id: Arc::new(RwLock::new(None)),
        })
    }

    /// Create database schema if it doesn't exist
    fn create_schema(conn: &Connection) -> Result<()> {
        conn.execute_batch(include_str!("schema.sql"))
            .context("Failed to create database schema")?;
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

    /// Store or retrieve fingerprint ID
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
impl TestmonDatabase {
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
    fn save_test_execution(
        &mut self,
        test_name: &str,
        fingerprints: Vec<Fingerprint>,
        duration: f64,
        failed: bool,
    ) -> PyResult<()> {
        self.save_test_execution_internal(test_name, fingerprints, duration, failed)
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
            pyo3::exceptions::PyRuntimeError::new_err(format!(
                "Failed to get fingerprint: {}",
                e
            ))
        })
    }

    /// Synchronize filesystem state with database
    fn sync_filesystem(&mut self, _root: &str) -> PyResult<()> {
        // TODO: Implement filesystem sync
        Ok(())
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
}

// Internal implementation methods
impl TestmonDatabase {
    fn save_test_execution_internal(
        &mut self,
        test_name: &str,
        fingerprints: Vec<Fingerprint>,
        duration: f64,
        failed: bool,
    ) -> Result<()> {
        // Get or create environment
        let env_id = self.get_or_create_environment("default", "3.12")?;

        let mut conn = self.conn.write();
        let tx = conn.transaction()?;

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
            Ok(id)
        } else {
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
        let mut all_tests = Vec::new();

        for (filename, _checksums) in changed_blocks {
            // Get all fingerprints for this file
            let mut stmt = conn.prepare(
                "SELECT DISTINCT te.test_name
                 FROM test_execution te
                 JOIN test_execution_file_fp teff ON te.id = teff.test_execution_id
                 JOIN file_fp fp ON teff.fingerprint_id = fp.id
                 WHERE fp.filename = ?1",
            )?;

            let tests: Vec<String> = stmt
                .query_map(params![filename], |row| row.get(0))?
                .filter_map(|r| r.ok())
                .collect();

            // TODO: Filter by specific checksums (requires deserializing blobs)
            all_tests.extend(tests);
        }

        // Deduplicate
        all_tests.sort();
        all_tests.dedup();

        Ok(all_tests)
    }

    fn get_stats_internal(&self) -> Result<HashMap<String, i64>> {
        let conn = self.conn.read();
        let mut stats = HashMap::new();

        // Count tests
        let test_count: i64 = conn.query_row("SELECT COUNT(*) FROM test_execution", [], |row| {
            row.get(0)
        })?;
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

        Ok(stats)
    }
}

/// Serialize checksums (Vec<i32>) to blob
fn serialize_checksums(checksums: &[i32]) -> Vec<u8> {
    checksums
        .iter()
        .flat_map(|c| c.to_le_bytes())
        .collect()
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
        let db = TestmonDatabase::new_internal(temp_db.path().to_str().unwrap());
        assert!(db.is_ok());
    }

    #[test]
    fn test_save_and_retrieve_fingerprint() {
        let temp_db = NamedTempFile::new().unwrap();
        let db = TestmonDatabase::new_internal(temp_db.path().to_str().unwrap()).unwrap();

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
        let mut db = TestmonDatabase::new_internal(temp_db.path().to_str().unwrap()).unwrap();

        let fp = Fingerprint {
            filename: "test.py".to_string(),
            checksums: vec![123],
            file_hash: "abc".to_string(),
            mtime: 1.0,
            blocks: None,
        };

        db.save_test_execution_internal("test_example", vec![fp], 0.5, false)
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
    fn test_get_affected_tests() {
        let temp_db = NamedTempFile::new().unwrap();
        let mut db = TestmonDatabase::new_internal(temp_db.path().to_str().unwrap()).unwrap();

        let fp = Fingerprint {
            filename: "module.py".to_string(),
            checksums: vec![100, 200],
            file_hash: "hash1".to_string(),
            mtime: 1.0,
            blocks: None,
        };

        db.save_test_execution_internal("test_one", vec![fp.clone()], 0.1, false)
            .unwrap();
        db.save_test_execution_internal("test_two", vec![fp], 0.2, false)
            .unwrap();

        let mut changed = HashMap::new();
        changed.insert("module.py".to_string(), vec![100]);

        let affected = db.get_affected_tests_internal(changed).unwrap();
        assert_eq!(affected.len(), 2);
        assert!(affected.contains(&"test_one".to_string()));
        assert!(affected.contains(&"test_two".to_string()));
    }
}
