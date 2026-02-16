"""
Main pytest plugin for pytest-diff

This module integrates with pytest to provide intelligent test selection
based on code changes.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pytest_diff._config import (
    check_scope_mismatch,
    get_config_value,
    get_rootdir,
    get_scope_paths,
    get_workerinput,
    relative_scope_paths,
)
from pytest_diff._git import get_git_commit_sha
from pytest_diff._storage_ops import download_and_import_baseline, upload_baseline
from pytest_diff._xdist import is_xdist_controller, is_xdist_worker

import _pytest.outcomes

if TYPE_CHECKING:
    import pytest
    from _pytest.terminal import TerminalReporter

logger = logging.getLogger("pytest_diff")

# Coverage module will be imported when needed (not at module level)
# to avoid caching None if not installed during initial import

# Rust core will be imported when built with maturin
try:
    from pytest_diff import _core
except ImportError:
    _core = None  # type: ignore[assignment]  # Allow import before building


class PytestDiffPlugin:
    """Main plugin class for pytest-diff"""

    def __init__(self, config: pytest.Config) -> None:
        self.config: pytest.Config = config
        self.verbose: bool = config.getoption("--diff-v", False)

        # Configure logging before any log calls
        if not logger.handlers:
            handler = logging.StreamHandler(sys.stdout)
            handler.setFormatter(logging.Formatter("%(message)s"))
            logger.addHandler(handler)
            logger.propagate = False
        logger.setLevel(logging.DEBUG if self.verbose else logging.INFO)

        self.baseline: bool = config.getoption("--diff-baseline", False)
        self.force: bool = config.getoption("--diff-force", False)
        diff_flag: bool = config.getoption("--diff", False)
        self.enabled: bool = diff_flag or self.baseline
        if self.baseline and diff_flag:
            logger.warning(
                "Both --diff and --diff-baseline provided;"
                " --diff-baseline takes precedence (--diff will be ignored)"
            )
        self.upload: bool = config.getoption("--diff-upload", False)

        # xdist role detection (must be done early, before enabling checks)
        self.is_worker = is_xdist_worker(config)
        self.is_controller = is_xdist_controller(config)

        if not self.enabled:
            return

        if _core is None:
            raise ImportError(
                "pytest-diff Rust core not found. Please install with: pip install pytest-diff"
            )

        # Remote storage configuration
        self.remote_url: str | None = (
            config.getoption("--diff-remote", None) or config.getini("diff_remote_url") or None
        )
        self.remote_key: str = config.getini("diff_remote_key") or "baseline.db"

        # --diff-remote only accepts single file URLs, not prefixes
        if self.remote_url and self.remote_url.endswith("/"):
            raise ValueError(
                f"--diff-remote requires a single file URL, not a prefix: {self.remote_url}\n"
                "  Use 'pytest-diff merge --from-remote' to merge multiple baselines from a prefix."
            )

        # If remote URL points to a specific .db file, extract it as the remote key
        # e.g. s3://bucket/path/baseline.db -> url=s3://bucket/path/, key=baseline.db
        if (
            self.remote_url
            and self.remote_url.rsplit("/", 1)[-1].endswith(".db")
            and self.remote_key == "baseline.db"  # Only override default key
        ):
            url_parts = self.remote_url.rsplit("/", 1)
            self.remote_url = url_parts[0] + "/"
            self.remote_key = url_parts[1]
        self.storage: Any = None

        # Initialize components - store database in pytest cache folder
        cache_dir = get_rootdir(config) / ".pytest_cache" / "pytest-diff"
        cache_dir.mkdir(parents=True, exist_ok=True)
        self.db_path: Path = cache_dir / "pytest_diff.db"
        self.db: _core.PytestDiffDatabase | None = None
        self.cov: Any = None
        self.fp_cache: _core.FingerprintCache | None = (
            None  # Fingerprint cache for avoiding re-parsing
        )
        self.deselected_items: list[Any] = []
        self.current_test: str | None = None
        self.test_start_time: float | None = None
        self.test_files_executed: list[str] = []

        # Get Python version for environment tracking
        import sys as _sys

        self.python_version: str = (
            f"{_sys.version_info.major}.{_sys.version_info.minor}.{_sys.version_info.micro}"
        )

        # Batch writing for test executions
        self.test_execution_batch: list[tuple[str, list[Any], float, bool]] = []
        self.batch_size: int = get_config_value(config, "batch-size", "batch_size", 20)

        # Cache size for fingerprints (configurable for large codebases)
        self.cache_max_size: int = get_config_value(config, "cache-size", "cache_size", 100_000)

        # Get pytest invocation scope
        self.scope_paths: list[str] = get_scope_paths(config)
        if self.verbose or config.option.verbose >= 2:
            logger.debug("Scope paths: %s", self.scope_paths)

    @staticmethod
    def _format_size(size_bytes: int) -> str:
        """Format a byte count as a human-readable string."""
        size: float = float(size_bytes)
        for unit in ("B", "KB", "MB", "GB"):
            if size < 1024:
                return f"{size:.1f} {unit}" if unit != "B" else f"{int(size)} B"
            size /= 1024
        return f"{size:.1f} TB"

    def _flush_test_batch(self) -> None:
        """Flush batched test executions to database"""
        if not self.test_execution_batch or self.db is None:
            return

        import time

        batch_len = len(self.test_execution_batch)
        logger.info("pytest-diff: Saving %s test executions to DB...", batch_len)
        flush_start = time.time()
        for nodeid, fingerprints, duration, failed in self.test_execution_batch:
            self.db.save_test_execution(nodeid, fingerprints, duration, failed, self.python_version)
        elapsed = time.time() - flush_start
        logger.info("pytest-diff: Saved %s test executions to DB in %.1fs", batch_len, elapsed)
        logger.debug("Flushed %s test executions to DB in %.3fs", batch_len, elapsed)
        self.test_execution_batch = []

    def pytest_configure(self, config: pytest.Config) -> None:
        """Initialize database and coverage collector"""
        if config.option.verbose >= 2:
            logger.debug("PytestDiffPlugin.pytest_configure called, enabled=%s", self.enabled)

        if not self.enabled:
            return

        if self.is_worker:
            self._configure_as_worker(config)
        else:
            self._configure_as_controller_or_standalone(config)

    def _configure_as_worker(self, config: pytest.Config) -> None:
        """Configure as an xdist worker process.

        Workers receive the DB path from the controller via workerinput.
        They open the existing DB (schema already created) and skip remote download.
        """
        import time

        start = time.time()
        logger.debug("Configuring as xdist worker")

        # Get DB path from workerinput (set by controller in pytest_configure_node)
        db_path_str = get_workerinput(config).get("pytest_diff_db_path")
        if db_path_str:
            self.db_path = Path(db_path_str)
            logger.debug("Worker using DB path from controller: %s", self.db_path)

        # Open existing DB (schema already created by controller)
        try:
            db_start = time.time()
            self.db = _core.PytestDiffDatabase(str(self.db_path))
            logger.debug("Worker opened database in %.3fs", time.time() - db_start)
        except Exception as e:
            logger.warning("⚠ pytest-diff worker: Could not open database: %s", e)
            self.enabled = False
            return

        # Initialize fingerprint cache
        cache_start = time.time()
        self.fp_cache = _core.FingerprintCache(self.cache_max_size)
        logger.debug(
            "Worker fingerprint cache initialized (max_size=%s) in %.3fs",
            self.cache_max_size,
            time.time() - cache_start,
        )

        # Initialize coverage only in baseline mode (--diff mode never processes it)
        if self.baseline:
            self._init_coverage(config)

        # Skip remote download - controller already did it
        logger.debug("Worker pytest_configure completed in %.3fs", time.time() - start)

    def _configure_as_controller_or_standalone(self, config: pytest.Config) -> None:
        """Configure as the xdist controller or standalone (non-xdist) process.

        Controllers/standalone create the DB, download remote baseline, etc.
        """
        import time

        start = time.time()
        role = "controller" if self.is_controller else "standalone"
        logger.debug("Starting pytest_configure as %s", role)

        # Initialize Rust components
        try:
            db_start = time.time()
            self.db = _core.PytestDiffDatabase(str(self.db_path))
            logger.debug("Database opened in %.3fs", time.time() - db_start)
            if not (self.remote_url and not self.baseline):
                logger.info("✓ pytest-diff: Using database at %s", self.db_path)
        except Exception as e:
            logger.warning("⚠ pytest-diff: Could not open database: %s", e)
            # Try to delete corrupted database and create fresh
            try:
                if self.db_path.exists():
                    self.db_path.unlink()
                    logger.info(
                        "  Deleted corrupted database, creating new one at %s", self.db_path
                    )
                else:
                    logger.info("  Creating new database at %s", self.db_path)
                db_start = time.time()
                self.db = _core.PytestDiffDatabase(str(self.db_path))
                logger.debug("Database created in %.3fs", time.time() - db_start)
            except Exception as e2:
                logger.warning("⚠ pytest-diff: Failed to create database: %s", e2)
                self.enabled = False
                return

        # Initialize fingerprint cache with configurable size
        cache_start = time.time()
        self.fp_cache = _core.FingerprintCache(self.cache_max_size)
        logger.debug(
            "Fingerprint cache initialized (max_size=%s) in %.3fs",
            self.cache_max_size,
            time.time() - cache_start,
        )

        # Initialize coverage only in baseline mode (--diff mode never processes it)
        if self.baseline:
            self._init_coverage(config)

        # Remote baseline: download and import if --diff mode + remote configured
        if self.remote_url and not self.baseline:
            try:
                self.storage = download_and_import_baseline(
                    self.storage,
                    self.remote_url,
                    self.remote_key,
                    self.db,
                    self.db_path,
                    str(get_rootdir(self.config)),
                    logger,
                )
            except Exception as e:
                import pytest

                pytest.exit(
                    f"pytest-diff: Failed to download remote baseline — aborting.\n  {e}",
                    returncode=1,
                )

        logger.debug("pytest_configure completed in %.3fs", time.time() - start)

    def _init_coverage(self, config: pytest.Config) -> None:
        """Initialize coverage collector if available."""
        coverage_module: Any = None
        try:
            import coverage as coverage_module
        except ImportError:
            pass

        if config.option.verbose >= 2:
            logger.debug("Coverage module available: %s", coverage_module is not None)

        if coverage_module:
            import time

            cov_start = time.time()
            self.cov = coverage_module.Coverage(
                data_file=None,  # Don't save coverage data
                branch=False,
                config_file=False,
                source=[str(get_rootdir(config))],
            )
            logger.debug("Coverage initialized in %.3fs", time.time() - cov_start)
            if config.option.verbose >= 2:
                logger.debug("Coverage initialized successfully")

    def pytest_collection_modifyitems(self, config: pytest.Config, items: list[Any]) -> None:
        """Select tests based on code changes"""
        if not self.enabled:
            return

        if self.baseline and not self.force:
            # Scope mismatch in baseline mode: run all tests to rebuild properly
            if check_scope_mismatch(self.db, config, self.scope_paths, is_baseline=True):
                return

            # Incremental baseline: if DB already has test data, only run affected tests
            assert self.db is not None
            stats = self.db.get_stats()
            if stats.get("test_count", 0) > 0:
                try:
                    changed = _core.detect_changes(
                        str(self.db_path), str(get_rootdir(config)), self.scope_paths
                    )

                    # Find unrecorded tests (e.g. previously failed)
                    recorded_tests = set(self.db.get_recorded_tests())
                    unrecorded_tests = {
                        item.nodeid for item in items if item.nodeid not in recorded_tests
                    }

                    if changed.has_changes():
                        logger.info(
                            "\n✓ pytest-diff: Incremental baseline — %s modified files",
                            len(changed.modified),
                        )
                        affected_tests = set(self.db.get_affected_tests(changed.changed_blocks))
                        affected_tests |= unrecorded_tests
                        if affected_tests:
                            selected = [item for item in items if item.nodeid in affected_tests]
                            self.deselected_items = [item for item in items if item not in selected]
                            items[:] = selected
                            logger.info("  Running %s affected tests", len(selected))
                            logger.info(
                                "  Skipping %s unaffected tests", len(self.deselected_items)
                            )
                            if self.deselected_items:
                                config.hook.pytest_deselected(items=self.deselected_items)
                        else:
                            # Changes detected but no tests affected — skip all
                            logger.info("\n✓ pytest-diff: Incremental baseline — no tests affected")
                            self.deselected_items = items[:]
                            items[:] = []
                            config.hook.pytest_deselected(items=self.deselected_items)
                    elif unrecorded_tests:
                        logger.info(
                            "\n✓ pytest-diff: Incremental baseline — %s unrecorded tests",
                            len(unrecorded_tests),
                        )
                        selected = [item for item in items if item.nodeid in unrecorded_tests]
                        self.deselected_items = [item for item in items if item not in selected]
                        items[:] = selected
                        if self.deselected_items:
                            config.hook.pytest_deselected(items=self.deselected_items)
                    else:
                        # No changes — skip all tests
                        logger.info("\n✓ pytest-diff: No changes detected — skipping all tests")
                        self.deselected_items = items[:]
                        items[:] = []
                        config.hook.pytest_deselected(items=self.deselected_items)
                except Exception as e:
                    # On error, fall through to run all tests
                    logger.warning("\n⚠ pytest-diff: Error during incremental detection: %s", e)
                    logger.info("  Running all tests")
                return
            # else: empty DB, fall through to run all tests

        if self.baseline:
            # First baseline or --diff-force: run all tests
            return

        # Warn if diff scope differs from baseline scope
        check_scope_mismatch(self.db, config, self.scope_paths, is_baseline=False)

        try:
            # Detect changes
            changed = _core.detect_changes(
                str(self.db_path), str(get_rootdir(config)), self.scope_paths
            )

            assert self.db is not None

            # Find tests with no recorded execution (e.g. previously failed)
            recorded_tests = set(self.db.get_recorded_tests())
            unrecorded_tests = {item.nodeid for item in items if item.nodeid not in recorded_tests}
            if unrecorded_tests:
                logger.info("  %s unrecorded tests will be re-run", len(unrecorded_tests))

            if changed.has_changes():
                logger.info("\n✓ pytest-diff: Detected %s modified files", len(changed.modified))
                logger.info("  Changed blocks in %s files", len(changed.changed_blocks))

                # Get affected tests from database
                affected_tests = set(self.db.get_affected_tests(changed.changed_blocks))

                # Also select tests living in modified files (new test files)
                # changed.modified contains relative paths; resolve them against rootdir
                rootdir = get_rootdir(config)
                modified_abs = {str((rootdir / f).resolve()) for f in changed.modified}
                for item in items:
                    if str(Path(item.fspath).resolve()) in modified_abs:
                        affected_tests.add(item.nodeid)

                # Include unrecorded tests
                affected_tests |= unrecorded_tests

                if affected_tests:
                    # Select only affected tests
                    selected = [item for item in items if item.nodeid in affected_tests]
                    self.deselected_items = [item for item in items if item not in selected]
                    items[:] = selected

                    logger.info("  Running %s affected tests", len(selected))
                    logger.info("  Skipping %s unaffected tests", len(self.deselected_items))

                    if self.deselected_items:
                        config.hook.pytest_deselected(items=self.deselected_items)
                else:
                    # No tests affected - check if database has test data
                    stats = self.db.get_stats()
                    if stats.get("test_count", 0) == 0:
                        # Database is empty - run all tests to build it
                        logger.info("  No tests affected by changes (database is empty)")
                        logger.info("  Running all %s tests to build database", len(items))
                    else:
                        # Database has data but no tests affected - skip all
                        logger.info("  No tests affected by changes")
                        logger.info("  Skipping all %s tests", len(items))
                        self.deselected_items = items[:]
                        items[:] = []
                        config.hook.pytest_deselected(items=self.deselected_items)
            elif unrecorded_tests:
                logger.info("\n✓ pytest-diff: No changes detected")
                # Run unrecorded tests (previously failed)
                selected = [item for item in items if item.nodeid in unrecorded_tests]
                self.deselected_items = [item for item in items if item not in selected]
                items[:] = selected

                logger.info("  Running %s unrecorded tests", len(selected))
                logger.info("  Skipping %s recorded tests", len(self.deselected_items))

                if self.deselected_items:
                    config.hook.pytest_deselected(items=self.deselected_items)
            else:
                logger.info("\n✓ pytest-diff: No changes detected")
                logger.info("  Skipping all %s tests", len(items))
                self.deselected_items = items
                items[:] = []
                config.hook.pytest_deselected(items=self.deselected_items)
        except Exception as e:
            logger.warning("\n⚠ pytest-diff: Error during change detection: %s", e)
            logger.info("  Running all tests")
            import traceback

            traceback.print_exc()

    def _test_file_fingerprint(self, item: Any) -> list[Any]:
        """Calculate fingerprint for the test file itself (no coverage)."""
        test_file = Path(item.fspath).resolve()
        if test_file.exists() and test_file.suffix == ".py":
            try:
                return [_core.calculate_fingerprint(str(test_file), str(get_rootdir(self.config)))]
            except Exception:
                pass
        return []

    def pytest_runtest_protocol(self, item: Any, nextitem: Any) -> None:
        """Start coverage collection for a test"""
        if not self.enabled:
            return

        import time

        self.current_test = item.nodeid
        self.test_start_time = time.time()
        self.test_files_executed = []

        # Start coverage collection
        if self.cov:
            if self.config.option.verbose >= 2:
                logger.debug("Starting coverage for %s", item.nodeid)
            self.cov.start()
        elif self.config.option.verbose >= 2:
            logger.debug("Coverage not available for %s", item.nodeid)

    def pytest_runtest_makereport(self, item: Any, call: Any) -> None:
        """Capture test result and save to database"""
        if not self.enabled:
            return

        # In --diff mode, don't save test executions — preserve baseline fingerprints
        # so that changed tests keep being selected until a new baseline is set
        if not self.baseline:
            return

        # Handle skips during setup (e.g., @pytest.mark.skip, skipIf)
        # Coverage was started in pytest_runtest_protocol but test body never ran
        if (
            call.when == "setup"
            and call.excinfo is not None
            and call.excinfo.errisinstance(_pytest.outcomes.Skipped)
        ):
            if self.cov:
                self.cov.stop()
                self.cov.erase()
            fingerprints = self._test_file_fingerprint(item)
            if fingerprints:
                self.test_execution_batch.append((item.nodeid, fingerprints, 0.0, False))
                if len(self.test_execution_batch) >= self.batch_size:
                    self._flush_test_batch()
            return

        # Only save after test execution (not setup/teardown)
        if call.when != "call":
            return

        import time

        report_start = time.time()
        # Calculate duration safely - if test_start_time is None, duration is 0
        duration = time.time() - self.test_start_time if self.test_start_time else 0.0
        failed = call.excinfo is not None

        try:
            # Stop coverage and get executed files
            fingerprints: list[Any] = []

            if self.cov:
                cov_stop_start = time.time()
                self.cov.stop()
                data = self.cov.get_data()
                logger.debug("Coverage stop took %.3fs", time.time() - cov_stop_start)

                # Debug: log how many files coverage found
                measured = list(data.measured_files())
                logger.debug("Coverage measured %s files", len(measured))
                if self.config.option.verbose >= 2:
                    for f in measured[:5]:
                        logger.debug("  - %s", f)

                # Get test file path for filtering
                test_file = Path(item.fspath).resolve()
                test_file_str = str(test_file)

                # Extract coverage data as dict: filename -> list of executed lines
                extract_start = time.time()
                coverage_map: dict[str, list[int]] = {}
                for filename in measured:
                    filepath = Path(filename)
                    if filepath.suffix == ".py" and str(filepath).startswith(
                        str(get_rootdir(self.config))
                    ):
                        abs_path = str(filepath.resolve())
                        lines = data.lines(filename)
                        if lines is None:
                            continue
                        executed_lines = list(lines)
                        coverage_map[abs_path] = executed_lines
                logger.debug(
                    "Extracted coverage for %s files in %.3fs",
                    len(coverage_map),
                    time.time() - extract_start,
                )

                try:
                    process_start = time.time()
                    fingerprints = _core.process_coverage_data(
                        coverage_map,
                        str(get_rootdir(self.config)),
                        test_file_str,
                        self.config.option.verbose >= 2 or self.verbose,
                        self.scope_paths,
                        self.fp_cache,
                    )
                    logger.debug(
                        "Rust processing took %.3fs, got %s fingerprints",
                        time.time() - process_start,
                        len(fingerprints),
                    )
                except Exception as e:
                    if self.config.option.verbose:
                        logger.warning("⚠ pytest-diff: Error processing coverage: %s", e)
                        import traceback

                        traceback.print_exc()

                erase_start = time.time()
                self.cov.erase()
                logger.debug("Coverage erase took %.3fs", time.time() - erase_start)
            else:
                # If no coverage, still track the test file itself
                fingerprints = self._test_file_fingerprint(item)

            # Skip genuinely failed tests so they remain "unknown" and get re-selected
            # on the next --diff run until they pass.
            # But record skipped and xfail tests so they get deselected properly.
            if failed:
                import pytest as _pytest_mod

                is_skip = call.excinfo.errisinstance(_pytest.outcomes.Skipped)
                is_xfail = item.get_closest_marker(
                    "xfail"
                ) is not None or call.excinfo.errisinstance(_pytest_mod.xfail.Exception)
                if not is_skip and not is_xfail:
                    logger.debug(
                        "Skipping failed test %s (will be re-selected next run)", item.nodeid
                    )
                    return

            # Add to batch instead of saving immediately
            if fingerprints:
                self.test_execution_batch.append((item.nodeid, fingerprints, duration, False))
                logger.debug("Added to batch (size: %s)", len(self.test_execution_batch))

                # Flush batch if it reaches batch_size
                if len(self.test_execution_batch) >= self.batch_size:
                    self._flush_test_batch()

            logger.debug("Total report handling took %.3fs", time.time() - report_start)
        except Exception as e:
            # Don't fail the test run if we can't save to database
            if self.config.option.verbose:
                logger.warning("⚠ pytest-diff: Could not save test execution: %s", e)

    def pytest_terminal_summary(self, terminalreporter: TerminalReporter) -> None:
        """Show summary of deselected tests"""
        if not self.enabled:
            return

        # Flush any remaining batched test executions
        self._flush_test_batch()

        # Workers: close DB and return (don't save baseline or show summary)
        if self.is_worker:
            if self.db:
                try:
                    self.db.close()
                except Exception:
                    pass
            return

        # Show cache statistics
        if self.fp_cache and self.verbose:
            hits, misses, hit_rate = self.fp_cache.stats()
            cache_size = self.fp_cache.size()
            logger.debug(
                "Fingerprint cache stats: %s hits, %s misses, %.1f%% hit rate, %s cached files",
                hits,
                misses,
                hit_rate * 100,
                cache_size,
            )

        # If baseline mode, save baseline fingerprints (controller/standalone only)
        if self.baseline:
            try:
                import time

                logger.debug("Starting baseline save")
                upload_msg = (
                    f" (will upload to {self.remote_url})"
                    if self.upload and self.remote_url
                    else ""
                )
                logger.info("pytest-diff: Saving baseline fingerprints...%s", upload_msg)
                start = time.time()
                count = _core.save_baseline(
                    str(self.db_path),
                    str(get_rootdir(self.config)),
                    self.verbose,
                    self.scope_paths,
                    self.force,
                )
                elapsed = time.time() - start
                logger.debug("Baseline save completed in %.3fs", elapsed)
                db_size = self._format_size(self.db_path.stat().st_size)
                terminalreporter.write_sep(
                    "=",
                    f"pytest-diff: Baseline saved for {count} files in {elapsed:.1f}s ({db_size})",
                    green=True,
                )

                # Store git commit SHA in metadata for staleness detection
                sha = get_git_commit_sha(str(get_rootdir(self.config)))
                if sha and self.db:
                    self.db.set_metadata("baseline_commit", sha)
                    logger.debug("Stored baseline commit SHA: %s", sha[:10])

                # Store scope paths (relative to rootdir) so diff runs can detect mismatches
                if self.db:
                    import json

                    rootdir = str(get_rootdir(self.config))
                    relative_scopes = relative_scope_paths(self.scope_paths, rootdir)
                    self.db.set_metadata("baseline_scope", json.dumps(relative_scopes))
            except Exception as e:
                terminalreporter.write_sep(
                    "=",
                    f"pytest-diff: Failed to save baseline: {e}",
                    red=True,
                )
                return

            # Upload baseline if requested
            if self.upload and self.remote_url:
                # Close DB first to checkpoint WAL into single file
                if self.db:
                    try:
                        self.db.close()
                    except Exception:
                        pass
                try:
                    self.storage = upload_baseline(
                        self.storage,
                        self.remote_url,
                        self.remote_key,
                        self.db_path,
                        logger,
                    )
                except Exception as e:
                    terminalreporter.write_sep(
                        "=",
                        f"pytest-diff: Failed to upload baseline to remote storage: {e}",
                        red=True,
                    )
            return

        if self.deselected_items:
            terminalreporter.write_sep(
                "=",
                f"pytest-diff: {len(self.deselected_items)} tests deselected",
                green=True,
            )

        # Close database to checkpoint WAL and remove -wal/-shm files
        if self.db:
            try:
                self.db.close()
            except Exception:
                logger.info(" done")  # Ignore close errors


def pytest_addoption(parser: pytest.Parser) -> None:
    """Add command-line options for pytest-diff

    Options can also be set in pyproject.toml under [tool.pytest.ini_options]:

        [tool.pytest.ini_options]
        diff_batch_size = 50
        diff_cache_size = 200000
    """
    group = parser.getgroup("diff", "pytest-diff test selection")

    group.addoption(
        "--diff",
        action="store_true",
        help="Enable pytest-diff (select tests based on changes)",
    )

    group.addoption(
        "--diff-baseline",
        action="store_true",
        help="Compute baseline. Runs all tests on first use; incremental on subsequent runs. Use --diff-force to run all.",
    )

    group.addoption(
        "--diff-force",
        action="store_true",
        help="Force running all tests (use with --diff-baseline to rebuild from scratch)",
    )

    group.addoption(
        "--diff-v",
        action="store_true",
        help="Enable verbose logging for pytest-diff (shows timing and debug info)",
    )

    group.addoption(
        "--diff-batch-size",
        type=int,
        default=20,
        help="Number of test executions to batch before DB write (default: 20, larger = faster but more memory)",
    )

    group.addoption(
        "--diff-cache-size",
        type=int,
        default=100_000,
        help="Maximum fingerprints to cache in memory (default: 100000, increase for very large codebases)",
    )

    group.addoption(
        "--diff-remote",
        type=str,
        default=None,
        help="Remote storage URL for a single baseline DB file (e.g. s3://bucket/baseline.db)",
    )

    group.addoption(
        "--diff-upload",
        action="store_true",
        help="Upload baseline DB to remote storage after --diff-baseline completes",
    )

    # Register ini options for pyproject.toml configuration
    parser.addini(
        "diff_batch_size",
        type="string",
        default="20",
        help="Number of test executions to batch before DB write",
    )
    parser.addini(
        "diff_cache_size",
        type="string",
        default="100000",
        help="Maximum fingerprints to cache in memory",
    )
    parser.addini(
        "diff_remote_url",
        type="string",
        default="",
        help="Remote storage URL for a single baseline DB file (e.g. s3://bucket/baseline.db)",
    )
    parser.addini(
        "diff_remote_key",
        type="string",
        default="baseline.db",
        help="Remote key/filename for the baseline DB (default: baseline.db)",
    )


def pytest_configure(config: pytest.Config) -> None:
    """Register the plugin"""
    if (
        config.getoption("--diff")
        or config.getoption("--diff-baseline")
        or config.getoption("--diff-force")
    ):
        plugin = PytestDiffPlugin(config)
        config.pluginmanager.register(plugin, "pytest_diff")


try:
    import pytest

    @pytest.hookimpl(optionalhook=True)
    def pytest_configure_node(node: Any) -> None:
        """xdist hook: controller sends data to workers.

        This is called by the xdist controller for each worker node before
        tests start running. We use it to pass the DB path to workers.

        The optionalhook=True tells pluggy to skip validation when xdist
        is not installed.
        """
        plugin = node.config.pluginmanager.get_plugin("pytest_diff")
        if plugin is None or not plugin.enabled:
            return

        # Pass DB path to worker via workerinput dict
        node.workerinput["pytest_diff_db_path"] = str(plugin.db_path)
        node.workerinput["pytest_diff_initialized"] = True
except ImportError:
    pass
