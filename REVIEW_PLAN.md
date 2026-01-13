# pytest-diff Code Review - Implementation Plan

## Review Date: 2024-12-19

---

## MUST FIX (Critical for Production)

### 1. Database Cleanup for Old Test Executions
**Location:** `rust/src/database.rs`
**Problem:** Every test run creates new `test_execution` rows that are never deleted. Database grows unbounded.
**Solution:**
- Add cleanup logic to keep only the most recent execution per test per environment
- Run cleanup during `save_test_execution` or as a separate maintenance operation
- Option: Add `--diff-cleanup` flag or auto-cleanup on baseline save

### 2. Concurrent Access Issues for pytest-xdist
**Location:** `rust/src/database.rs`
**Problem:** No protection against concurrent writes from multiple pytest-xdist workers.
**Solution:**
- Add SQLite busy timeout for graceful retry on lock contention
- Use `BEGIN IMMEDIATE` transactions to fail fast on write conflicts
- Document xdist limitations or implement worker-specific databases with merge

### 3. Optimize `get_affected_tests_internal` Query Performance
**Location:** `rust/src/database.rs:464-509`
**Problem:** O(N × M) complexity - queries all test executions per file, deserializes blobs repeatedly.
**Solution:**
- Use a single query with all changed files (UNION or IN clause)
- Deserialize checksums once per unique blob
- Consider inverted index table for future optimization

### 4. Add Size Limit to FingerprintCache
**Location:** `rust/src/fingerprint_cache.rs`
**Problem:** Cache grows unbounded, potential memory exhaustion on large codebases.
**Solution:**
- Add `max_size` parameter similar to `cache.rs`
- Implement LRU-style eviction when limit reached

---

## SHOULD FIX (Important but not blocking)

### 5. Remove Dead Code
- `rust/src/tracer.rs` - `CoverageCollector` is never used
- `rust/src/database.rs:317-320` - `sync_filesystem` does nothing

### 6. Fix Signature Detection for Complex Decorators
**Location:** `rust/src/parser.rs:114-148`
**Problem:** May incorrectly stop at `:` in decorator arguments.

### 7. Create LinearLocator Once Per File
**Location:** `rust/src/parser.rs:285-293`
**Problem:** Creates new locator for each offset lookup, O(n²) scanning.

### 8. Add Configurable Database Path for CI
**Location:** `python/pytest_diff/plugin.py`
**Problem:** Database in `.pytest_cache` may be wiped between CI runs.

---

## NICE TO HAVE (Future Optimization)

### 9. Pre-compute Inverted Index
Add `block_checksum → test_id` table for O(1) affected test lookup.

### 10. Use References Instead of Cloning
`filter_executed_blocks_rust` clones every Block unnecessarily.

### 11. Standardize Error Handling
Inconsistent use of `anyhow`, `PyO3` errors, and `unwrap_or_else`.

### 12. Named Constants for Magic Numbers
- `0.001` mtime epsilon
- `50` progress interval
- `20` default batch size
- `10_000` cache size

---

## Implementation Progress

- [x] 1. Database cleanup - Implemented in `save_test_execution_internal()` - deletes previous executions for same test before inserting new one
- [x] 2. Concurrent access (busy timeout + immediate transactions) - Added 30s busy timeout and BEGIN IMMEDIATE transactions
- [x] 3. Query optimization - Rewrote `get_affected_tests_internal()` to use single query with IN clause, HashSet for dedup, blob cache
- [x] 4. FingerprintCache size limit - Added configurable `max_size` with LRU-style eviction (default 100,000)

### Additional Improvements Made

- Added `--diff-cache-size` CLI option for configuring cache size
- Added `diff_cache_size` and `diff_batch_size` ini options for pyproject.toml configuration
- Updated default cache size from 10,000 to 100,000 to support large codebases
