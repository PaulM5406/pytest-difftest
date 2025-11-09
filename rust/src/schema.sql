-- pytest-testmon compatible database schema
-- This schema matches pytest-testmon's structure for compatibility

-- Metadata table (key-value store for database info)
CREATE TABLE IF NOT EXISTS metadata (
    dataid TEXT PRIMARY KEY,
    data TEXT
);

-- Python environment tracking
CREATE TABLE IF NOT EXISTS environment (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    environment_name TEXT,
    system_packages TEXT,
    python_version TEXT,
    UNIQUE(environment_name, system_packages, python_version)
);

-- Test execution records
CREATE TABLE IF NOT EXISTS test_execution (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    environment_id INTEGER NOT NULL,
    test_name TEXT NOT NULL,
    duration FLOAT,
    failed INTEGER,  -- 0 = passed, 1 = failed
    forced INTEGER,  -- 0 = selected, 1 = forced
    FOREIGN KEY(environment_id) REFERENCES environment(id) ON DELETE CASCADE
);

-- Indexes for fast test lookups
CREATE INDEX IF NOT EXISTS ix_test_execution_environment_id
    ON test_execution(environment_id);
CREATE INDEX IF NOT EXISTS ix_test_execution_test_name
    ON test_execution(test_name);

-- File fingerprints (block checksums)
CREATE TABLE IF NOT EXISTS file_fp (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    filename TEXT NOT NULL,
    method_checksums BLOB NOT NULL,  -- Array of i32 checksums
    mtime FLOAT NOT NULL,
    fsha TEXT NOT NULL,  -- File SHA hash
    UNIQUE(filename, fsha, method_checksums)
);

CREATE INDEX IF NOT EXISTS ix_file_fp_filename
    ON file_fp(filename);

-- Junction table: test_execution <-> file_fp (many-to-many)
CREATE TABLE IF NOT EXISTS test_execution_file_fp (
    test_execution_id INTEGER NOT NULL,
    fingerprint_id INTEGER NOT NULL,
    PRIMARY KEY (test_execution_id, fingerprint_id),
    FOREIGN KEY(test_execution_id) REFERENCES test_execution(id) ON DELETE CASCADE,
    FOREIGN KEY(fingerprint_id) REFERENCES file_fp(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS ix_test_execution_file_fp_fingerprint
    ON test_execution_file_fp(fingerprint_id);

-- Coarse-grained file tracking (entire suite)
CREATE TABLE IF NOT EXISTS suite_execution_file_fsha (
    suite_execution_id INTEGER NOT NULL,
    filename TEXT NOT NULL,
    fsha TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS ix_suite_execution_file_fsha_suite
    ON suite_execution_file_fsha(suite_execution_id);
