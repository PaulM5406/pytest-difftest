"""
Test baseline fingerprint functionality and revert scenarios
"""

import tempfile
from pathlib import Path


def test_baseline_revert_scenario():
    """Test that reverting changes is properly detected with baseline"""
    import time

    from pytest_diff import _core

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create a simple Python module
        module_file = tmpdir / "calculator.py"
        original_code = "def add(a, b):\n    return a + b\n"
        buggy_code = "def add(a, b):\n    return a + b + 1  # BUG!\n"

        # Step 1: Write original code and save baseline
        module_file.write_text(original_code)
        db_path = tmpdir / ".testmondata"
        scope_paths = [str(tmpdir)]
        count = _core.save_baseline(str(db_path), str(tmpdir), False, scope_paths)
        assert count == 1, "Should save baseline for 1 file"

        # Step 2: Verify no changes initially
        changes = _core.detect_changes(str(db_path), str(tmpdir), scope_paths)
        assert not changes.has_changes(), "No changes at baseline"

        # Step 3: Introduce a bug
        time.sleep(0.01)  # Ensure mtime changes
        module_file.write_text(buggy_code)

        # Step 4: Detect changes
        changes = _core.detect_changes(str(db_path), str(tmpdir), scope_paths)
        assert changes.has_changes(), "Should detect changes after modification"
        assert len(changes.modified) == 1
        assert len(changes.changed_blocks) == 1

        # Step 5: Revert the change back to original
        time.sleep(0.01)  # Ensure mtime changes
        module_file.write_text(original_code)

        # Step 6: Verify NO changes detected (back to baseline!)
        changes = _core.detect_changes(str(db_path), str(tmpdir), scope_paths)
        assert not changes.has_changes(), (
            "Should detect NO changes after reverting to baseline - this is the key fix!"
        )
        assert len(changes.modified) == 0
        assert len(changes.changed_blocks) == 0


def test_save_baseline_function():
    """Test the save_baseline function directly"""
    from pytest_diff import _core

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create some Python files
        (tmpdir / "module1.py").write_text("def foo(): pass")
        (tmpdir / "module2.py").write_text("def bar(): pass")
        (tmpdir / "subdir").mkdir()
        (tmpdir / "subdir" / "module3.py").write_text("def baz(): pass")

        db_path = tmpdir / ".testmondata"
        scope_paths = [str(tmpdir)]

        # Save baseline
        count = _core.save_baseline(str(db_path), str(tmpdir), False, scope_paths)
        assert count == 3, f"Should save baseline for 3 files, got {count}"

        # Open database and verify baselines were saved
        db = _core.TestmonDatabase(str(db_path))
        stats = db.get_stats()
        assert stats["baseline_count"] == 3, "Should have 3 baselines in database"


def test_baseline_detects_no_change_on_revert():
    """Test that detect_changes returns no changes after reverting to baseline"""
    from pytest_diff import _core

    with tempfile.TemporaryDirectory() as tmpdir:
        tmpdir = Path(tmpdir)

        # Create original file
        module = tmpdir / "calc.py"
        original = "def add(a, b):\n    return a + b\n"
        module.write_text(original)

        db_path = tmpdir / ".testmondata"
        scope_paths = [str(tmpdir)]

        # Save baseline with original code
        _core.save_baseline(str(db_path), str(tmpdir), False, scope_paths)

        # Verify no changes detected initially
        changes = _core.detect_changes(str(db_path), str(tmpdir), scope_paths)
        assert not changes.has_changes(), "No changes should be detected initially"

        # Modify the file
        import time

        time.sleep(0.01)  # Ensure mtime changes
        module.write_text("def add(a, b):\n    return a + b + 1\n")

        # Should detect changes
        changes = _core.detect_changes(str(db_path), str(tmpdir), scope_paths)
        assert changes.has_changes(), "Changes should be detected after modification"
        assert len(changes.modified) == 1

        # Revert to original
        time.sleep(0.01)  # Ensure mtime changes
        module.write_text(original)

        # Should detect NO changes (back to baseline!)
        changes = _core.detect_changes(str(db_path), str(tmpdir), scope_paths)
        assert not changes.has_changes(), "No changes should be detected after revert to baseline"
        assert len(changes.modified) == 0
        assert len(changes.changed_blocks) == 0
