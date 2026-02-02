"""
Tests for --diff: change detection and test selection.
"""

import time


def test_no_changes_skips_all(baselined_project):
    """After baseline with no changes, --diff skips all tests."""
    result = baselined_project.runpytest_subprocess("--diff", "-v")
    result.stdout.fnmatch_lines(["*No changes detected*"])
    # No tests should have run
    result.assert_outcomes()


def test_modified_source_runs_affected_tests(baselined_project):
    """Changing a source file causes dependent tests to run."""
    # Modify the calculator module
    time.sleep(0.01)
    calc = baselined_project.path / "mylib" / "calculator.py"
    calc.write_text(
        "def add(a, b):\n"
        "    return a + b + 0  # modified\n"
        "\n"
        "def multiply(a, b):\n"
        "    return a * b\n"
    )

    result = baselined_project.runpytest_subprocess("--diff", "-v")
    result.stdout.fnmatch_lines(["*modified*"])


def test_unmodified_module_tests_deselected(multi_module_project):
    """Only tests touching modified module run (multi-module project)."""
    # First, baseline
    result = multi_module_project.runpytest_subprocess("--diff-baseline", "-v")
    result.assert_outcomes(passed=4)

    # Modify only math_ops
    time.sleep(0.01)
    math_ops = multi_module_project.path / "mylib" / "math_ops.py"
    math_ops.write_text(
        "def add(a, b):\n"
        "    return a + b + 0  # modified\n"
        "\n"
        "def subtract(a, b):\n"
        "    return a - b\n"
    )

    result = multi_module_project.runpytest_subprocess("--diff", "-v")
    result.stdout.fnmatch_lines(["*modified*"])
    # string tests should be deselected
    result.stdout.fnmatch_lines(["*deselected*"])


def test_diff_keeps_selecting_affected_tests_until_rebaseline(baselined_project):
    """Running --diff repeatedly after a change keeps selecting affected tests.

    Regression test: previously, the first --diff run would save new fingerprints
    reflecting the modified file state, causing subsequent --diff runs to see
    no affected tests (the old baseline checksums were no longer in file_fp).
    """
    # Modify the calculator module
    time.sleep(0.01)
    calc = baselined_project.path / "mylib" / "calculator.py"
    calc.write_text(
        "def add(a, b):\n"
        "    return a + b + 0  # modified\n"
        "\n"
        "def multiply(a, b):\n"
        "    return a * b\n"
    )

    # First --diff run: should detect the change and run affected tests
    result = baselined_project.runpytest_subprocess("--diff", "-v")
    result.stdout.fnmatch_lines(["*modified*"])
    result.stdout.fnmatch_lines(["*affected*"])

    # Second --diff run: should STILL detect and run affected tests
    result = baselined_project.runpytest_subprocess("--diff", "-v")
    result.stdout.fnmatch_lines(["*modified*"])
    result.stdout.fnmatch_lines(["*affected*"])

    # After re-baselining (incremental: only affected tests run), --diff should skip all
    result = baselined_project.runpytest_subprocess("--diff-baseline", "-v")
    result.stdout.fnmatch_lines(["*Incremental baseline*"])

    result = baselined_project.runpytest_subprocess("--diff", "-v")
    result.stdout.fnmatch_lines(["*No changes detected*"])
    result.assert_outcomes()


def test_new_test_file_is_selected(baselined_project):
    """A new test file added after baseline should be selected by --diff."""
    # Add a new test file
    new_test = baselined_project.path / "tests" / "test_new.py"
    new_test.write_text("def test_brand_new():\n    assert 1 + 1 == 2\n")

    result = baselined_project.runpytest_subprocess("--diff", "-v")
    result.stdout.fnmatch_lines(["*modified*"])
    # The new test should be selected and pass
    result.assert_outcomes(passed=1)


def test_new_source_file_runs_dependent_tests(baselined_project):
    """A new source file added after baseline should trigger tests that import it."""
    # Add a new source module
    new_module = baselined_project.path / "mylib" / "helpers.py"
    new_module.write_text("def greet(name):\n    return f'Hello {name}'\n")

    # Add a test that imports the new module
    new_test = baselined_project.path / "tests" / "test_helpers.py"
    new_test.write_text(
        "import sys\n"
        "sys.path.insert(0, str(__import__('pathlib').Path(__file__).parent.parent))\n"
        "from mylib.helpers import greet\n"
        "\n"
        "def test_greet():\n"
        "    assert greet('world') == 'Hello world'\n"
    )

    result = baselined_project.runpytest_subprocess("--diff", "-v")
    result.stdout.fnmatch_lines(["*modified*"])
    # Both new files detected; the new test should run
    result.assert_outcomes(passed=1)


def test_multiple_diff_runs_stable(baselined_project):
    """Running --diff 3x without changes always skips all."""
    for _ in range(3):
        result = baselined_project.runpytest_subprocess("--diff", "-v")
        result.stdout.fnmatch_lines(["*No changes detected*"])
        result.assert_outcomes()
