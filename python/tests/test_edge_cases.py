"""
Tests for edge cases and robustness.
"""


def test_diff_with_empty_database(pytester):
    """--diff with no prior baseline doesn't crash."""
    pytester.makepyfile("def test_noop(): pass")
    result = pytester.runpytest_subprocess("--diff", "-v")
    # Should not crash — may run all tests or skip all
    assert result.ret in (0, 5)


def test_syntax_error_in_source(baselined_project):
    """Source file with syntax error doesn't crash plugin."""
    import time

    time.sleep(0.01)
    calc = baselined_project.path / "mylib" / "calculator.py"
    calc.write_text("def broken(\n")  # syntax error

    result = baselined_project.runpytest_subprocess("--diff", "-v")
    # Plugin should handle gracefully — test collection may fail but plugin shouldn't panic
    assert result.ret in (0, 1, 2, 4, 5)


def test_empty_python_file(baselined_project):
    """Empty .py files don't crash fingerprinting."""
    import time

    time.sleep(0.01)
    empty = baselined_project.path / "mylib" / "empty.py"
    empty.write_text("")

    result = baselined_project.runpytest_subprocess("--diff", "-v")
    assert result.ret in (0, 5)


def test_database_corruption_recovery(pytester):
    """Corrupted DB file is detected and plugin recovers."""
    pytester.makepyfile("def test_noop(): pass")

    # Create corrupted database
    db_dir = pytester.path / ".pytest_cache" / "pytest-diff"
    db_dir.mkdir(parents=True, exist_ok=True)
    db_file = db_dir / "pytest_diff.db"
    db_file.write_bytes(b"THIS IS NOT A VALID SQLITE DATABASE")

    result = pytester.runpytest_subprocess("--diff-baseline", "-v")
    # Plugin should recover by deleting and recreating
    result.stdout.fnmatch_lines(["*pytest-diff*"])
    # Should still complete (may pass or fail depending on recovery)
    assert result.ret in (0, 1, 5)


def test_large_test_suite_batching(pytester):
    """50 tests with --diff-batch-size=10 all pass and flush correctly."""
    # Generate 50 test functions
    test_funcs = "\n".join(f"def test_{i}():\n    assert {i} == {i}\n" for i in range(50))
    pytester.makepyfile(test_generated=test_funcs)

    result = pytester.runpytest_subprocess(
        "--diff-baseline", "--diff-batch-size=10", "--diff-v", "-v"
    )
    result.assert_outcomes(passed=50)
    # With batch_size=10, we expect multiple flushes
    result.stdout.fnmatch_lines(["*Flushed 10 test executions*"])
