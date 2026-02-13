"""
Direct tests for the _core Rust module (fingerprint, cache, database).

These tests use tmp_path (standard pytest) instead of pytester.
"""

from pytest_diff import _core


def test_parse_module_returns_blocks():
    """parse_module extracts function/class blocks with correct names."""
    source = "def foo():\n    pass\n\nclass Bar:\n    def method(self):\n        pass\n"
    blocks = _core.parse_module(source)
    names = [b.name for b in blocks]
    assert "<module>" in names
    assert "foo" in names
    assert "Bar" in names
    assert "method" in names


def test_calculate_fingerprint(tmp_path):
    """Returns Fingerprint with filename, checksums, file_hash, mtime."""
    f = tmp_path / "example.py"
    f.write_text("def hello():\n    return 'world'\n")

    fp = _core.calculate_fingerprint(str(f))
    assert fp.filename == str(f)
    assert isinstance(fp.file_hash, str)
    assert len(fp.file_hash) == 64  # blake3 hex
    assert isinstance(fp.mtime, float)
    assert len(fp.checksums) > 0
    assert fp.blocks is not None and len(fp.blocks) > 0


def test_fingerprint_cache_hit_miss(tmp_path):
    """Cache reports 0 hits/1 miss on first call, 1 hit on second."""
    f = tmp_path / "cached.py"
    f.write_text("x = 1\n")

    cache = _core.FingerprintCache(100)

    # First call: miss
    cache.get_or_calculate(str(f))
    hits1, misses1, _ = cache.stats()
    assert hits1 == 0
    assert misses1 == 1

    # Second call: hit
    cache.get_or_calculate(str(f))
    hits2, misses2, _ = cache.stats()
    assert hits2 == 1
    assert misses2 == 1


def test_fingerprint_cache_eviction(tmp_path):
    """Cache with max_size=2 evicts when 3 entries added."""
    cache = _core.FingerprintCache(2)

    for i in range(3):
        f = tmp_path / f"mod{i}.py"
        f.write_text(f"x = {i}\n")
        cache.get_or_calculate(str(f))

    assert cache.size() == 2
    assert cache.max_size() == 2


def test_database_stats_empty(tmp_path):
    """New DB has test_count=0, file_count=0, baseline_count=0."""
    db_path = tmp_path / "test.db"
    db = _core.PytestDiffDatabase(str(db_path))
    stats = db.get_stats()
    assert stats["test_count"] == 0
    assert stats["file_count"] == 0
    assert stats["baseline_count"] == 0


def test_detect_changes_no_baseline(tmp_path):
    """Files with no baseline are detected as new/changed."""
    db_path = tmp_path / "test.db"
    # Create the DB so detect_changes doesn't fail on missing file
    _core.PytestDiffDatabase(str(db_path))

    f = tmp_path / "module.py"
    f.write_text("def foo(): pass\n")

    changes = _core.detect_changes(str(db_path), str(tmp_path), [str(tmp_path)])
    # New files (no baseline) should be detected as changed
    assert changes.has_changes()
    assert len(changes.modified) == 1


def test_import_baseline_returns_import_result(tmp_path):
    """import_baseline_from returns ImportResult with both counts."""
    source_path = tmp_path / "source.db"
    source_db = _core.PytestDiffDatabase(str(source_path))

    # Create a Python file and save test execution + baseline
    f = tmp_path / "module.py"
    f.write_text("def hello():\n    return 'world'\n")
    fp = _core.calculate_fingerprint(str(f))
    source_db.save_test_execution("test_hello", [fp], 0.1, False)
    source_db.save_baseline_fingerprint(fp)
    source_db.close()

    # Import into target
    target_path = tmp_path / "target.db"
    target_db = _core.PytestDiffDatabase(str(target_path))
    result = target_db.import_baseline_from(str(source_path))

    assert isinstance(result, _core.ImportResult)
    assert result.baseline_count == 1
    assert result.test_execution_count == 1


def test_merge_baseline_returns_import_result(tmp_path):
    """merge_baseline_from returns ImportResult with both counts."""
    source_path = tmp_path / "source.db"
    source_db = _core.PytestDiffDatabase(str(source_path))

    f = tmp_path / "module.py"
    f.write_text("def hello():\n    return 'world'\n")
    fp = _core.calculate_fingerprint(str(f))
    source_db.save_test_execution("test_hello", [fp], 0.1, False)
    source_db.save_baseline_fingerprint(fp)
    source_db.close()

    # Merge into target
    target_path = tmp_path / "target.db"
    target_db = _core.PytestDiffDatabase(str(target_path))
    result = target_db.merge_baseline_from(str(source_path))

    assert isinstance(result, _core.ImportResult)
    assert result.baseline_count == 1
    assert result.test_execution_count == 1


def test_detect_changes_returns_relative_paths(tmp_path):
    """detect_changes returns paths relative to project_root, not absolute."""
    db_path = tmp_path / "test.db"
    _core.PytestDiffDatabase(str(db_path))

    # Create a subdirectory with a Python file
    subdir = tmp_path / "src"
    subdir.mkdir()
    f = subdir / "module.py"
    f.write_text("def foo(): pass\n")

    changes = _core.detect_changes(str(db_path), str(tmp_path), [str(tmp_path)])
    assert changes.has_changes()
    # All paths should be relative (not starting with /)
    for path in changes.modified:
        assert not path.startswith("/"), f"Expected relative path, got: {path}"
    # Should contain the relative path
    assert "src/module.py" in changes.modified


def test_save_baseline_stores_relative_paths(tmp_path):
    """save_baseline stores relative paths in the database."""
    db_path = tmp_path / "test.db"

    # Create a subdirectory with a Python file
    subdir = tmp_path / "src"
    subdir.mkdir()
    f = subdir / "module.py"
    f.write_text("def foo(): pass\n")

    _core.save_baseline(str(db_path), str(tmp_path), False, [str(tmp_path)])

    # Check that the stored baseline uses relative path
    db = _core.PytestDiffDatabase(str(db_path))
    fp = db.get_baseline_fingerprint("src/module.py")
    assert fp is not None, "Baseline should be stored with relative path"
    assert fp.filename == "src/module.py"


def test_import_copies_test_execution_coverage(tmp_path):
    """Imported test execution data enables get_affected_tests."""
    source_path = tmp_path / "source.db"
    source_db = _core.PytestDiffDatabase(str(source_path))

    f = tmp_path / "module.py"
    f.write_text("def hello():\n    return 'world'\n")
    fp = _core.calculate_fingerprint(str(f))
    source_db.save_test_execution("test_hello", [fp], 0.1, False)
    source_db.save_baseline_fingerprint(fp)
    source_db.close()

    # Import into target
    target_path = tmp_path / "target.db"
    target_db = _core.PytestDiffDatabase(str(target_path))
    target_db.import_baseline_from(str(source_path))

    # get_affected_tests should find the imported test
    affected = target_db.get_affected_tests({fp.filename: list(fp.checksums)})
    assert "test_hello" in affected
