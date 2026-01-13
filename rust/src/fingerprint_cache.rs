// In-memory fingerprint cache for avoiding re-parsing during test runs
//
// This module provides a thread-safe cache that stores parsed fingerprints
// in memory, avoiding the need to re-parse the same files for every test.

use anyhow::Result;
use parking_lot::RwLock;
use pyo3::prelude::*;
use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::UNIX_EPOCH;

use crate::fingerprint::calculate_fingerprint_internal;
use crate::types::Fingerprint;

/// Default maximum cache size (number of fingerprints)
/// Set to 100,000 to support large codebases while limiting memory usage
/// At ~5KB per fingerprint, this caps cache at ~500MB worst case
const DEFAULT_MAX_SIZE: usize = 100_000;

/// In-memory cache for fingerprints
///
/// This cache stores parsed fingerprints to avoid re-parsing the same files
/// repeatedly during a test run. It's especially effective when multiple tests
/// touch the same source files.
///
/// The cache has a configurable maximum size to prevent unbounded memory growth
/// on large codebases. When the limit is reached, 10% of entries are evicted.
#[pyclass(unsendable)]
pub struct FingerprintCache {
    // Cache: filepath -> (mtime, fingerprint)
    cache: Arc<RwLock<HashMap<String, (f64, Fingerprint)>>>,
    hits: Arc<RwLock<usize>>,
    misses: Arc<RwLock<usize>>,
    max_size: usize,
}

#[pymethods]
impl FingerprintCache {
    /// Create a new cache with default maximum size
    #[new]
    #[pyo3(signature = (max_size=None))]
    pub fn new(max_size: Option<usize>) -> Self {
        Self {
            cache: Arc::new(RwLock::new(HashMap::new())),
            hits: Arc::new(RwLock::new(0)),
            misses: Arc::new(RwLock::new(0)),
            max_size: max_size.unwrap_or(DEFAULT_MAX_SIZE),
        }
    }

    /// Get a fingerprint from cache or calculate it
    ///
    /// Checks mtime to determine if cached version is still valid.
    pub fn get_or_calculate(&self, path: &str) -> PyResult<Fingerprint> {
        let fingerprint = self
            .get_or_calculate_internal(path)
            .map_err(|e| pyo3::exceptions::PyRuntimeError::new_err(format!("{}", e)))?;
        Ok(fingerprint)
    }

    /// Clear the cache
    pub fn clear(&self) {
        self.cache.write().clear();
        *self.hits.write() = 0;
        *self.misses.write() = 0;
    }

    /// Get cache statistics
    pub fn stats(&self) -> (usize, usize, f64) {
        let hits = *self.hits.read();
        let misses = *self.misses.read();
        let total = hits + misses;
        let hit_rate = if total > 0 {
            hits as f64 / total as f64
        } else {
            0.0
        };
        (hits, misses, hit_rate)
    }

    /// Get number of cached entries
    pub fn size(&self) -> usize {
        self.cache.read().len()
    }

    /// Get maximum cache size
    pub fn max_size(&self) -> usize {
        self.max_size
    }
}

impl FingerprintCache {
    pub(crate) fn get_or_calculate_internal(&self, path: &str) -> Result<Fingerprint> {
        let path_obj = Path::new(path);

        // Get current mtime
        let metadata = std::fs::metadata(path_obj)?;
        let current_mtime = metadata
            .modified()?
            .duration_since(UNIX_EPOCH)?
            .as_secs_f64();

        // Check cache
        {
            let cache = self.cache.read();
            if let Some((cached_mtime, cached_fp)) = cache.get(path) {
                // Check if mtime matches (file hasn't changed)
                if (current_mtime - cached_mtime).abs() < 0.001 {
                    // Cache hit!
                    *self.hits.write() += 1;
                    return Ok(cached_fp.clone());
                }
            }
        }

        // Cache miss - calculate fingerprint
        *self.misses.write() += 1;
        let fingerprint = calculate_fingerprint_internal(path)?;

        // Update cache with size limit enforcement
        {
            let mut cache = self.cache.write();

            // Evict entries if cache is full
            if cache.len() >= self.max_size {
                self.evict_entries(&mut cache);
            }

            cache.insert(path.to_string(), (current_mtime, fingerprint.clone()));
        }

        Ok(fingerprint)
    }

    /// Evict 10% of cache entries when limit is reached
    ///
    /// Uses a simple strategy: remove arbitrary entries (HashMap iteration order).
    /// This is fast and provides reasonable eviction behavior.
    fn evict_entries(&self, cache: &mut HashMap<String, (f64, Fingerprint)>) {
        let to_remove = self.max_size / 10;
        let keys_to_remove: Vec<String> = cache
            .keys()
            .take(to_remove.max(1))
            .cloned()
            .collect();

        for key in keys_to_remove {
            cache.remove(&key);
        }
    }
}
