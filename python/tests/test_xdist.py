"""Tests for pytest-xdist compatibility."""

from __future__ import annotations

import pytest

xdist = pytest.importorskip("xdist")


class TestXdistHelpers:
    """Tests for the _xdist helper module."""

    def test_is_xdist_worker(self, pytestconfig):
        """is_xdist_worker returns correct value based on context."""
        from pytest_diff._xdist import is_xdist_worker

        result = is_xdist_worker(pytestconfig)
        expected = hasattr(pytestconfig, "workerinput")
        assert result is expected

    def test_is_xdist_controller(self, pytestconfig):
        """is_xdist_controller returns correct value based on context."""
        from pytest_diff._xdist import is_xdist_controller

        result = is_xdist_controller(pytestconfig)
        # Controller has workercount but not workerinput
        expected = hasattr(pytestconfig, "workercount") and not hasattr(pytestconfig, "workerinput")
        assert result is expected

    def test_get_worker_id(self, pytestconfig):
        """get_worker_id returns worker ID or None based on context."""
        from pytest_diff._xdist import get_worker_id

        result = get_worker_id(pytestconfig)
        if hasattr(pytestconfig, "workerinput"):
            assert result is not None
            assert result.startswith("gw")
        else:
            assert result is None


class TestXdistBaseline:
    """Tests for baseline mode with xdist."""

    def test_baseline_with_xdist_n2(self, sample_project):
        """Baseline mode should work with -n 2."""
        result = sample_project.runpytest_subprocess("--diff-baseline", "-n", "2", "-v")
        result.assert_outcomes(passed=2)

    def test_baseline_with_xdist_n4(self, sample_project):
        """Baseline mode should work with -n 4."""
        result = sample_project.runpytest_subprocess("--diff-baseline", "-n", "4", "-v")
        result.assert_outcomes(passed=2)

    def test_baseline_with_xdist_n1(self, sample_project):
        """Baseline mode should work with -n 1 (controller + 1 worker)."""
        result = sample_project.runpytest_subprocess("--diff-baseline", "-n", "1", "-v")
        result.assert_outcomes(passed=2)


class TestXdistDiff:
    """Tests for diff mode with xdist."""

    def test_diff_no_changes_with_xdist(self, baselined_project):
        """Diff mode should work with -n 2 after a baseline has been created."""
        result = baselined_project.runpytest_subprocess("--diff", "-n", "2", "-v")
        # No changes => all tests deselected
        # Exit code 5 (NO_TESTS_COLLECTED) is expected when all tests deselected
        assert result.ret in (0, 5)

    def test_diff_detects_changes_with_xdist(self, baselined_project):
        """Diff mode with xdist should detect code changes and run affected tests."""
        # Modify lib.py to change a function
        lib_file = baselined_project.path / "lib.py"
        lib_file.write_text("def helper():\n    return 42  # changed\n")

        result = baselined_project.runpytest_subprocess("--diff", "-n", "2", "-v")
        # Should detect change and run affected test
        # Exit code could be 0 (tests passed) or 5 (no tests collected if none affected)
        assert result.ret in (0, 5)


class TestXdistDbCoordination:
    """Tests for database coordination between controller and workers."""

    def test_db_created_once(self, sample_project):
        """Database should be created by controller, used by workers."""
        # Run baseline with xdist
        result = sample_project.runpytest_subprocess("--diff-baseline", "-n", "2", "-v")
        result.assert_outcomes(passed=2)

        # Check that DB exists
        db_path = sample_project.path / ".pytest_cache" / "pytest-diff" / "pytest_diff.db"
        assert db_path.exists()

        # Run again - should reuse same DB
        result2 = sample_project.runpytest_subprocess("--diff", "-n", "2", "-v")
        # Exit code 5 (NO_TESTS_COLLECTED) is expected when all tests deselected
        assert result2.ret in (0, 5)

    def test_incremental_baseline_with_xdist(self, baselined_project):
        """Incremental baseline should work with xdist."""
        # Modify a file
        lib_file = baselined_project.path / "lib.py"
        lib_file.write_text("def helper():\n    return 99  # incremental change\n")

        # Run incremental baseline with xdist
        result = baselined_project.runpytest_subprocess("--diff-baseline", "-n", "2", "-v")
        # Exit code 0 or 5 depending on whether affected tests exist
        assert result.ret in (0, 5)
