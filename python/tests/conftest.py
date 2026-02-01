"""
Shared fixtures for pytest-diff test suite.

Uses pytester for integration tests that run pytest in subprocess isolation.
"""

import pytest

# Enable pytester fixture
pytest_plugins = ["pytester"]


@pytest.fixture
def sample_project(pytester):
    """Create a simple project with a calculator module and tests.

    Structure:
        mylib/calculator.py  - add(), multiply()
        tests/test_calc.py   - test_add(), test_multiply()
    """
    pytester.makepyfile(
        **{
            "mylib/__init__.py": "",
            "mylib/calculator.py": (
                "def add(a, b):\n    return a + b\n\ndef multiply(a, b):\n    return a * b\n"
            ),
            "tests/__init__.py": "",
            "tests/test_calc.py": (
                "import sys\n"
                "sys.path.insert(0, str(__import__('pathlib').Path(__file__).parent.parent))\n"
                "from mylib.calculator import add, multiply\n"
                "\n"
                "def test_add():\n"
                "    assert add(1, 2) == 3\n"
                "\n"
                "def test_multiply():\n"
                "    assert multiply(2, 3) == 6\n"
            ),
        }
    )
    return pytester


@pytest.fixture
def multi_module_project(pytester):
    """Create a project with two independent modules and separate test files.

    Structure:
        mylib/math_ops.py    - add(), subtract()
        mylib/string_ops.py  - upper(), concat()
        tests/test_math.py   - test_add(), test_subtract()
        tests/test_string.py - test_upper(), test_concat()
    """
    pytester.makepyfile(
        **{
            "mylib/__init__.py": "",
            "mylib/math_ops.py": (
                "def add(a, b):\n    return a + b\n\ndef subtract(a, b):\n    return a - b\n"
            ),
            "mylib/string_ops.py": (
                "def upper(s):\n    return s.upper()\n\ndef concat(a, b):\n    return a + b\n"
            ),
            "tests/__init__.py": "",
            "tests/test_math.py": (
                "import sys\n"
                "sys.path.insert(0, str(__import__('pathlib').Path(__file__).parent.parent))\n"
                "from mylib.math_ops import add, subtract\n"
                "\n"
                "def test_add():\n"
                "    assert add(1, 2) == 3\n"
                "\n"
                "def test_subtract():\n"
                "    assert subtract(5, 3) == 2\n"
            ),
            "tests/test_string.py": (
                "import sys\n"
                "sys.path.insert(0, str(__import__('pathlib').Path(__file__).parent.parent))\n"
                "from mylib.string_ops import upper, concat\n"
                "\n"
                "def test_upper():\n"
                "    assert upper('hello') == 'HELLO'\n"
                "\n"
                "def test_concat():\n"
                "    assert concat('foo', 'bar') == 'foobar'\n"
            ),
        }
    )
    return pytester


@pytest.fixture
def baselined_project(sample_project):
    """Run --diff-baseline on a sample project and return it ready for --diff runs."""
    result = sample_project.runpytest_subprocess("--diff-baseline", "-v")
    result.assert_outcomes(passed=2)
    return sample_project
