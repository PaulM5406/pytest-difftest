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
