"""
Main pytest plugin for pytest-diff

This module integrates with pytest to provide intelligent test selection
based on code changes.
"""

from __future__ import annotations

from pathlib import Path


# Coverage module will be imported when needed (not at module level)
# to avoid caching None if not installed during initial import

# Rust core will be imported when built with maturin
try:
    from pytest_diff import _core
except ImportError:
    _core = None  # type: ignore[assignment]  # Allow import before building


class TestmonPlugin:
    """Main plugin class for pytest-diff"""

    def __init__(self, config):
        self.config = config
        self.baseline = config.getoption("--diff-baseline", False)
        self.force = config.getoption("--diff-force", False)
        self.enabled = config.getoption("--diff", False) or self.baseline
        self.verbose = config.getoption("--diff-v", False)
        self.upload = config.getoption("--diff-upload", False)

        if not self.enabled:
            return

        if _core is None:
            raise ImportError(
                "pytest-diff Rust core not found. Please install with: pip install pytest-diff"
            )

        # Remote storage configuration
        self.remote_url = (
            config.getoption("--diff-remote", None) or config.getini("diff_remote_url") or None
        )
        self.remote_key = config.getini("diff_remote_key") or "baseline.db"
        self.storage = None

        # Initialize components - store database in pytest cache folder
        cache_dir = Path(config.rootdir) / ".pytest_cache" / "pytest-diff"
        cache_dir.mkdir(parents=True, exist_ok=True)
        self.db_path = cache_dir / "pytest_diff.db"
        self.db: _core.TestmonDatabase | None = None
        self.cov = None
        self.fp_cache: _core.FingerprintCache | None = (
            None  # Fingerprint cache for avoiding re-parsing
        )
        self.deselected_items = []
        self.current_test = None
        self.test_start_time = None
        self.test_files_executed = []

        # Get Python version for environment tracking
        import sys

        self.python_version = (
            f"{sys.version_info.major}.{sys.version_info.minor}.{sys.version_info.micro}"
        )

        # Batch writing for test executions
        self.test_execution_batch = []
        # Adaptive batch size based on total test count
        # For large test suites, larger batches = better performance
        # Check command line first, then ini option
        self.batch_size = self._get_config_value(config, "batch-size", "batch_size", 20)

        # Cache size for fingerprints (configurable for large codebases)
        self.cache_max_size = self._get_config_value(config, "cache-size", "cache_size", 100_000)

        # Get pytest invocation scope (e.g., if user runs 'pytest tests/unit/')
        # We'll use this to only track files within the specified scope
        self.scope_paths = self._get_scope_paths(config)
        if self.verbose or config.option.verbose >= 2:
            print(f"[DEBUG] Scope paths: {self.scope_paths}")

    def _log(self, message):
        """Log verbose message with timestamp"""
        if self.verbose:
            import time

            timestamp = time.strftime("%H:%M:%S")
            print(f"[{timestamp}] pytest-diff: {message}")

    @staticmethod
    def _get_config_value(config, cli_name: str, ini_name: str, default: int) -> int:
        """Get config value from CLI option or ini file, with fallback to default.

        CLI options take precedence over ini options. Supports configuration via:
        - Command line: --diff-{cli_name}
        - pyproject.toml: [tool.pytest.ini_options] diff_{ini_name} = value
        """
        # Check if CLI option was explicitly provided
        cli_value = config.getoption(f"--diff-{cli_name}", None)
        if cli_value is not None:
            return cli_value

        # Check ini option (from pyproject.toml or pytest.ini)
        ini_value = config.getini(f"diff_{ini_name}")
        if ini_value:
            try:
                return int(ini_value)
            except (ValueError, TypeError):
                pass

        return default

    def _get_scope_paths(self, config):
        """Get the absolute paths that define the pytest invocation scope.

        If user runs 'pytest tests/unit/', we should only track files under tests/unit/.
        If no args provided, track the entire rootdir.
        """
        if not config.args:
            # No specific paths given, use rootdir
            return [str(Path(config.rootdir).resolve())]

        scope_paths = []
        for arg in config.args:
            # Strip pytest node ID (e.g., "tests/test_foo.py::TestClass::test_method" -> "tests/test_foo.py")
            file_path = arg.split("::")[0]

            # Resolve to absolute path
            path = Path(file_path)
            if not path.is_absolute():
                path = Path(config.rootdir) / path

            # Handle both files and directories
            try:
                resolved = path.resolve(strict=False)  # Don't fail if path doesn't exist
                if resolved.is_dir():
                    scope_paths.append(str(resolved))
                elif resolved.is_file() or file_path.endswith(".py"):
                    # For files, use their parent directory as scope
                    scope_paths.append(str(resolved.parent))
            except (OSError, RuntimeError):
                # If path doesn't exist or can't be resolved, skip it
                pass

        return scope_paths if scope_paths else [str(Path(config.rootdir).resolve())]

    @staticmethod
    def _get_git_commit_sha(rootdir: str) -> str | None:
        """Get the current HEAD commit SHA from git.

        Returns None if git is unavailable, not a repo, or any error occurs.
        """
        import subprocess

        try:
            result = subprocess.run(
                ["git", "rev-parse", "HEAD"],
                cwd=rootdir,
                capture_output=True,
                text=True,
                timeout=5,
            )
            if result.returncode == 0:
                return result.stdout.strip()
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
            pass
        return None

    @staticmethod
    def _check_baseline_staleness(baseline_commit: str, rootdir: str) -> str | None:
        """Check if the baseline commit is stale relative to current HEAD.

        Returns None if the baseline is current, or a warning message string.
        """
        import subprocess

        current_sha = TestmonPlugin._get_git_commit_sha(rootdir)
        if current_sha is None:
            return None

        if baseline_commit == current_sha:
            return None

        short_baseline = baseline_commit[:10]
        short_head = current_sha[:10]

        # Check if baseline_commit is an ancestor of HEAD
        try:
            result = subprocess.run(
                ["git", "merge-base", "--is-ancestor", baseline_commit, "HEAD"],
                cwd=rootdir,
                capture_output=True,
                timeout=5,
            )
            if result.returncode == 0:
                return (
                    f"Baseline was built from commit {short_baseline}, "
                    f"current HEAD is {short_head}. "
                    f"Baseline is older but included in your history. "
                    f"Test selection may not be optimal for newly merged code."
                )
        except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
            pass

        return (
            f"Baseline is STALE: built from commit {short_baseline} "
            f"which is NOT in your current history (HEAD={short_head}). "
            f"Test selection may be unreliable. "
            f"Consider re-running: pytest --diff-baseline"
        )

    def _flush_test_batch(self):
        """Flush batched test executions to database"""
        if not self.test_execution_batch or self.db is None:
            return

        import time

        flush_start = time.time()
        for nodeid, fingerprints, duration, failed in self.test_execution_batch:
            self.db.save_test_execution(nodeid, fingerprints, duration, failed, self.python_version)
        self._log(
            f"Flushed {len(self.test_execution_batch)} test executions to DB in {time.time() - flush_start:.3f}s"
        )
        self.test_execution_batch = []

    def _init_storage(self):
        """Lazily initialize the remote storage backend."""
        if self.storage is not None or not self.remote_url:
            return
        try:
            from pytest_diff.storage import get_storage

            self.storage = get_storage(self.remote_url)
            if self.storage is None:
                print(f"⚠ pytest-diff: Unsupported remote URL scheme: {self.remote_url}")
        except Exception as e:
            print(f"⚠ pytest-diff: Failed to initialize remote storage: {e}")

    def _download_and_import_baseline(self):
        """Download remote baseline DB and import via ATTACH."""
        import tempfile
        import time

        self._init_storage()
        if self.storage is None:
            return

        dl_start = time.time()
        tmp_path = Path(tempfile.gettempdir()) / "pytest-diff-remote-baseline.db"

        try:
            downloaded = self.storage.download(self.remote_key, tmp_path)
            if downloaded:
                self._log(f"Downloaded remote baseline in {time.time() - dl_start:.3f}s")
            else:
                self._log("Remote baseline unchanged (cache hit)")
        except FileNotFoundError:
            self._log("No remote baseline found — skipping import")
            return
        except Exception as e:
            print(f"⚠ pytest-diff: Failed to download remote baseline: {e}")
            return

        # Import baselines from the downloaded DB into local DB
        if self.db is None:
            return
        try:
            import_start = time.time()
            count = self.db.import_baseline_from(str(tmp_path))
            self._log(
                f"Imported {count} baseline fingerprints in {time.time() - import_start:.3f}s"
            )
            print(f"✓ pytest-diff: Imported {count} baseline fingerprints from remote")

            # Check baseline staleness via stored commit SHA
            baseline_commit = self.db.get_metadata("baseline_commit")
            if baseline_commit:
                warning = self._check_baseline_staleness(baseline_commit, str(self.config.rootdir))
                if warning:
                    print(f"⚠ pytest-diff: {warning}")
            else:
                self._log("No baseline_commit metadata found — skipping staleness check")
        except Exception as e:
            print(f"⚠ pytest-diff: Failed to import remote baseline: {e}")

    def _upload_baseline(self):
        """Upload local baseline DB to remote storage."""
        import time

        self._init_storage()
        if self.storage is None:
            return

        try:
            upload_start = time.time()
            self.storage.upload(self.db_path, self.remote_key)
            self._log(f"Uploaded baseline in {time.time() - upload_start:.3f}s")
            print(f"✓ pytest-diff: Uploaded baseline to {self.remote_url}{self.remote_key}")
        except Exception as e:
            print(f"⚠ pytest-diff: Failed to upload baseline: {e}")

    def pytest_configure(self, config):
        """Initialize database and coverage collector"""
        if config.option.verbose >= 2:
            print(f"\n[DEBUG] TestmonPlugin.pytest_configure called, enabled={self.enabled}")

        if not self.enabled:
            return

        import time

        start = time.time()
        self._log("Starting pytest_configure")

        # Initialize Rust components
        try:
            db_start = time.time()
            self.db = _core.TestmonDatabase(str(self.db_path))
            self._log(f"Database opened in {time.time() - db_start:.3f}s")
            print(f"✓ pytest-diff: Using database at {self.db_path}")
        except Exception as e:
            print(f"⚠ pytest-diff: Could not open database: {e}")
            # Try to delete corrupted database and create fresh
            try:
                if self.db_path.exists():
                    self.db_path.unlink()
                    print(f"  Deleted corrupted database, creating new one at {self.db_path}")
                else:
                    print(f"  Creating new database at {self.db_path}")
                db_start = time.time()
                self.db = _core.TestmonDatabase(str(self.db_path))
                self._log(f"Database created in {time.time() - db_start:.3f}s")
            except Exception as e2:
                print(f"⚠ pytest-diff: Failed to create database: {e2}")
                self.enabled = False
                return

        # Initialize fingerprint cache with configurable size
        cache_start = time.time()
        self.fp_cache = _core.FingerprintCache(self.cache_max_size)
        self._log(
            f"Fingerprint cache initialized (max_size={self.cache_max_size}) in {time.time() - cache_start:.3f}s"
        )

        # Initialize coverage if available
        coverage_module = None
        try:
            import coverage as coverage_module
        except ImportError:
            pass

        if config.option.verbose >= 2:
            print(f"[DEBUG] Coverage module available: {coverage_module is not None}")

        if coverage_module:
            cov_start = time.time()
            self.cov = coverage_module.Coverage(
                data_file=None,  # Don't save coverage data
                branch=False,
                config_file=False,
                source=[str(config.rootdir)],
            )
            self._log(f"Coverage initialized in {time.time() - cov_start:.3f}s")
            if config.option.verbose >= 2:
                print("[DEBUG] Coverage initialized successfully")

        # Remote baseline: download and import if --diff mode + remote configured
        if self.remote_url and not self.baseline:
            self._download_and_import_baseline()

        self._log(f"pytest_configure completed in {time.time() - start:.3f}s")

    def pytest_collection_modifyitems(self, config, items):
        """Select tests based on code changes"""
        if not self.enabled:
            return

        if self.baseline and not self.force:
            # Incremental baseline: if DB already has test data, only run affected tests
            assert self.db is not None
            stats = self.db.get_stats()
            if stats.get("test_count", 0) > 0:
                try:
                    changed = _core.detect_changes(
                        str(self.db_path), str(config.rootdir), self.scope_paths
                    )
                    if changed.has_changes():
                        print(
                            f"\n✓ pytest-diff: Incremental baseline — {len(changed.modified)} modified files"
                        )
                        affected_tests = self.db.get_affected_tests(changed.changed_blocks)
                        if affected_tests:
                            selected = [item for item in items if item.nodeid in affected_tests]
                            self.deselected_items = [item for item in items if item not in selected]
                            items[:] = selected
                            print(f"  Running {len(selected)} affected tests")
                            print(f"  Skipping {len(self.deselected_items)} unaffected tests")
                            if self.deselected_items:
                                config.hook.pytest_deselected(items=self.deselected_items)
                        else:
                            # Changes detected but no tests affected — skip all
                            print("\n✓ pytest-diff: Incremental baseline — no tests affected")
                            self.deselected_items = items[:]
                            items[:] = []
                            config.hook.pytest_deselected(items=self.deselected_items)
                    else:
                        # No changes — skip all tests
                        print("\n✓ pytest-diff: No changes detected — skipping all tests")
                        self.deselected_items = items[:]
                        items[:] = []
                        config.hook.pytest_deselected(items=self.deselected_items)
                except Exception as e:
                    # On error, fall through to run all tests
                    print(f"\n⚠ pytest-diff: Error during incremental detection: {e}")
                    print("  Running all tests")
                return
            # else: empty DB, fall through to run all tests

        if self.baseline:
            # First baseline or --diff-force: run all tests
            return

        try:
            # Detect changes
            changed = _core.detect_changes(str(self.db_path), str(config.rootdir), self.scope_paths)

            if changed.has_changes():
                print(f"\n✓ pytest-diff: Detected {len(changed.modified)} modified files")
                print(f"  Changed blocks in {len(changed.changed_blocks)} files")

                # Get affected tests from database
                assert self.db is not None
                affected_tests = self.db.get_affected_tests(changed.changed_blocks)

                if affected_tests:
                    # Select only affected tests
                    selected = [item for item in items if item.nodeid in affected_tests]
                    self.deselected_items = [item for item in items if item not in selected]
                    items[:] = selected

                    print(f"  Running {len(selected)} affected tests")
                    print(f"  Skipping {len(self.deselected_items)} unaffected tests")

                    if self.deselected_items:
                        config.hook.pytest_deselected(items=self.deselected_items)
                else:
                    # No tests affected - check if database has test data
                    stats = self.db.get_stats()
                    if stats.get("test_count", 0) == 0:
                        # Database is empty - run all tests to build it
                        print("  No tests affected by changes (database is empty)")
                        print(f"  Running all {len(items)} tests to build database")
                    else:
                        # Database has data but no tests affected - skip all
                        print("  No tests affected by changes")
                        print(f"  Skipping all {len(items)} tests")
                        self.deselected_items = items[:]
                        items[:] = []
                        config.hook.pytest_deselected(items=self.deselected_items)
            else:
                print("\n✓ pytest-diff: No changes detected")
                print(f"  Skipping all {len(items)} tests")
                self.deselected_items = items
                items[:] = []
                config.hook.pytest_deselected(items=self.deselected_items)
        except Exception as e:
            print(f"\n⚠ pytest-diff: Error during change detection: {e}")
            print("  Running all tests")
            import traceback

            traceback.print_exc()

    def pytest_runtest_protocol(self, item, nextitem):
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
                print(f"\n[DEBUG] Starting coverage for {item.nodeid}")
            self.cov.start()
        elif self.config.option.verbose >= 2:
            print(f"\n[DEBUG] Coverage not available for {item.nodeid}")

    def pytest_runtest_makereport(self, item, call):
        """Capture test result and save to database"""
        if not self.enabled:
            return

        # In --diff mode, don't save test executions — preserve baseline fingerprints
        # so that changed tests keep being selected until a new baseline is set
        if not self.baseline:
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
            fingerprints = []

            if self.cov:
                cov_stop_start = time.time()
                self.cov.stop()
                data = self.cov.get_data()
                self._log(f"Coverage stop took {time.time() - cov_stop_start:.3f}s")

                # Debug: log how many files coverage found
                measured = list(data.measured_files())
                self._log(f"Coverage measured {len(measured)} files")
                if self.config.option.verbose >= 2:
                    print(f"\n[DEBUG] Coverage measured {len(measured)} files")
                    for f in measured[:5]:
                        print(f"  - {f}")

                # Get test file path for filtering
                test_file = Path(item.fspath).resolve()
                test_file_str = str(test_file)

                # Extract coverage data as dict: filename -> list of executed lines
                # This is the only Python-specific part; Rust does the rest!
                extract_start = time.time()
                coverage_map = {}
                for filename in measured:
                    filepath = Path(filename)
                    # Basic filtering: only .py files in project
                    if filepath.suffix == ".py" and str(filepath).startswith(
                        str(self.config.rootdir)
                    ):
                        abs_path = str(filepath.resolve())
                        lines = data.lines(filename)
                        if lines is None:
                            continue
                        executed_lines = list(lines)
                        coverage_map[abs_path] = executed_lines
                self._log(
                    f"Extracted coverage for {len(coverage_map)} files in {time.time() - extract_start:.3f}s"
                )

                # Let Rust do the heavy lifting in parallel!
                # This handles:
                # - File filtering (test files, etc.)
                # - Fingerprint calculation
                # - Block filtering by executed lines
                # - All done concurrently with rayon
                try:
                    process_start = time.time()
                    fingerprints = _core.process_coverage_data(
                        coverage_map,
                        str(self.config.rootdir),
                        test_file_str,
                        self.config.option.verbose >= 2 or self.verbose,
                        self.scope_paths,
                        self.fp_cache,  # Use cache to avoid re-parsing
                    )
                    self._log(
                        f"Rust processing took {time.time() - process_start:.3f}s, got {len(fingerprints)} fingerprints"
                    )
                except Exception as e:
                    if self.config.option.verbose:
                        print(f"\n⚠ pytest-diff: Error processing coverage: {e}")
                        import traceback

                        traceback.print_exc()

                erase_start = time.time()
                self.cov.erase()  # Clear coverage data for next test
                self._log(f"Coverage erase took {time.time() - erase_start:.3f}s")
            else:
                # If no coverage, still track the test file itself
                test_file = Path(item.fspath).resolve()
                test_file_str = str(test_file)
                if test_file.exists() and test_file.suffix == ".py":
                    try:
                        fingerprints.append(_core.calculate_fingerprint(test_file_str))
                    except Exception:
                        pass

            # Add to batch instead of saving immediately
            if fingerprints:
                self.test_execution_batch.append((item.nodeid, fingerprints, duration, failed))
                self._log(f"Added to batch (size: {len(self.test_execution_batch)})")

                # Flush batch if it reaches batch_size
                if len(self.test_execution_batch) >= self.batch_size:
                    self._flush_test_batch()

            self._log(f"Total report handling took {time.time() - report_start:.3f}s")
        except Exception as e:
            # Don't fail the test run if we can't save to database
            if self.config.option.verbose:
                print(f"\n⚠ pytest-diff: Could not save test execution: {e}")

    def pytest_terminal_summary(self, terminalreporter):
        """Show summary of deselected tests"""
        if not self.enabled:
            return

        # Flush any remaining batched test executions
        self._flush_test_batch()

        # Show cache statistics
        if self.fp_cache and self.verbose:
            hits, misses, hit_rate = self.fp_cache.stats()
            cache_size = self.fp_cache.size()
            self._log(
                f"Fingerprint cache stats: {hits} hits, {misses} misses, {hit_rate * 100:.1f}% hit rate, {cache_size} cached files"
            )

        # If baseline mode, save baseline fingerprints
        if self.baseline:
            try:
                import time

                self._log("Starting baseline save")
                start = time.time()
                count = _core.save_baseline(
                    str(self.db_path), str(self.config.rootdir), self.verbose, self.scope_paths
                )
                elapsed = time.time() - start
                self._log(f"Baseline save completed in {elapsed:.3f}s")
                terminalreporter.write_sep(
                    "=",
                    f"pytest-diff: Baseline saved for {count} files in {elapsed:.3f}s",
                    green=True,
                )

                # Store git commit SHA in metadata for staleness detection
                sha = self._get_git_commit_sha(str(self.config.rootdir))
                if sha and self.db:
                    self.db.set_metadata("baseline_commit", sha)
                    self._log(f"Stored baseline commit SHA: {sha[:10]}")
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
                self._upload_baseline()
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
                pass  # Ignore close errors


def pytest_addoption(parser):
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
        help="Remote storage URL for baseline DB (e.g. s3://bucket/prefix/, file:///path/)",
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
        help="Remote storage URL for baseline DB (e.g. s3://bucket/prefix/, file:///path/)",
    )
    parser.addini(
        "diff_remote_key",
        type="string",
        default="baseline.db",
        help="Remote key/filename for the baseline DB (default: baseline.db)",
    )


def pytest_configure(config):
    """Register the plugin"""
    if (
        config.getoption("--diff")
        or config.getoption("--diff-baseline")
        or config.getoption("--diff-force")
    ):
        plugin = TestmonPlugin(config)
        config.pluginmanager.register(plugin, "pytest_diff")
