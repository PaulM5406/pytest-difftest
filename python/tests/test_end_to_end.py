"""
Full workflow tests: baseline -> modify -> diff.
"""

import time


def test_full_workflow_baseline_modify_diff(multi_module_project):
    """baseline(4 pass) -> modify math_ops -> diff(only math tests run)."""
    # Step 1: baseline all tests
    result = multi_module_project.runpytest_subprocess("--diff-baseline", "-v")
    result.assert_outcomes(passed=4)
    result.stdout.fnmatch_lines(["*Baseline saved*"])

    # Step 2: modify math_ops only
    time.sleep(0.01)
    math_ops = multi_module_project.path / "mylib" / "math_ops.py"
    math_ops.write_text(
        "def add(a, b):\n"
        "    return a + b + 0  # modified\n"
        "\n"
        "def subtract(a, b):\n"
        "    return a - b\n"
    )

    # Step 3: diff should only run math tests
    result = multi_module_project.runpytest_subprocess("--diff", "-v")
    result.stdout.fnmatch_lines(["*modified*"])
    result.stdout.fnmatch_lines(["*deselected*"])


def test_revert_after_change_skips_all(baselined_project):
    """baseline -> modify -> detect changes -> revert -> no changes."""
    calc = baselined_project.path / "mylib" / "calculator.py"
    original = calc.read_text()

    # Modify
    time.sleep(0.01)
    calc.write_text(
        "def add(a, b):\n    return a + b + 999\n\ndef multiply(a, b):\n    return a * b\n"
    )

    result = baselined_project.runpytest_subprocess("--diff", "-v")
    result.stdout.fnmatch_lines(["*modified*"])

    # Revert
    time.sleep(0.01)
    calc.write_text(original)

    result = baselined_project.runpytest_subprocess("--diff", "-v")
    result.stdout.fnmatch_lines(["*No changes detected*"])
    result.assert_outcomes()


def test_adding_new_file_doesnt_crash(baselined_project):
    """Adding a new .py file after baseline doesn't cause errors."""
    time.sleep(0.01)
    new_file = baselined_project.path / "mylib" / "new_module.py"
    new_file.write_text("def new_func():\n    return 42\n")

    result = baselined_project.runpytest_subprocess("--diff", "-v")
    # Should not crash — new file may or may not trigger change detection
    assert result.ret in (0, 5)  # 0=passed, 5=no tests collected


def test_deleting_source_file_doesnt_crash(baselined_project):
    """Removing a tracked file is handled gracefully."""
    time.sleep(0.01)
    calc = baselined_project.path / "mylib" / "calculator.py"
    calc.unlink()

    result = baselined_project.runpytest_subprocess("--diff", "-v")
    # Should not crash — deleted files are handled gracefully
    assert result.ret in (0, 1, 2, 5)  # May error on import but plugin shouldn't crash


def test_failed_test_still_recorded(sample_project):
    """Failed tests still get fingerprints saved to DB."""
    # Create a failing test
    fail_test = sample_project.path / "tests" / "test_calc.py"
    fail_test.write_text(
        "import sys\n"
        "sys.path.insert(0, str(__import__('pathlib').Path(__file__).parent.parent))\n"
        "from mylib.calculator import add\n"
        "\n"
        "def test_add_fails():\n"
        "    assert add(1, 2) == 999  # will fail\n"
        "\n"
        "def test_add_passes():\n"
        "    assert add(1, 2) == 3\n"
    )

    result = sample_project.runpytest_subprocess("--diff-baseline", "-v")
    result.assert_outcomes(passed=1, failed=1)
    # Baseline should still be saved
    result.stdout.fnmatch_lines(["*Baseline saved*"])
