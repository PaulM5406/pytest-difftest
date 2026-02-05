"""Remote storage operations for pytest-diff.

Extracted from plugin.py to keep the main module focused on pytest hooks.
"""

from __future__ import annotations

import logging
import tempfile
import time
from pathlib import Path
from typing import Any

logger = logging.getLogger("pytest_diff")


def init_storage(
    storage: Any,
    remote_url: str | None,
) -> Any:
    """Lazily initialize the remote storage backend.

    Returns the storage object (possibly newly created), or None.
    """
    if storage is not None or not remote_url:
        return storage
    try:
        from pytest_diff.storage import get_storage

        storage = get_storage(remote_url)
        if storage is None:
            logger.warning("⚠ pytest-diff: Unsupported remote URL scheme: %s", remote_url)
    except Exception as e:
        logger.warning("⚠ pytest-diff: Failed to initialize remote storage: %s", e)
    return storage


def _is_prefix_key(remote_key: str) -> bool:
    """Check if remote_key indicates a prefix (directory) rather than a single file."""
    return not remote_key or remote_key.endswith("/")


def download_and_import_baseline(
    storage: Any,
    remote_url: str | None,
    remote_key: str,
    db: Any,
    db_path: Path,
    rootdir: str,
    log: Any,
) -> Any:
    """Download remote baseline DB and import via ATTACH.

    If remote_key is empty or ends with '/', treats it as a prefix and downloads
    all .db files, merging them into the local database.

    *log* is a ``logging.Logger`` instance.

    Returns the (possibly newly created) storage object.
    """
    storage = init_storage(storage, remote_url)
    if storage is None:
        return storage

    # Handle prefix-based download (multiple databases)
    if _is_prefix_key(remote_key):
        return _download_and_merge_baselines(storage, remote_key, db, db_path, rootdir, log)

    # Single file download (existing behavior)
    return _download_single_baseline(storage, remote_key, db, db_path, rootdir, log)


def _download_single_baseline(
    storage: Any,
    remote_key: str,
    db: Any,
    db_path: Path,
    rootdir: str,
    log: Any,
) -> Any:
    """Download a single baseline file and import it."""
    dl_start = time.time()
    # Use NamedTemporaryFile for unique filename (avoids race conditions with xdist)
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as tmp:
        tmp_path = Path(tmp.name)
    # Remove the empty file so storage backends don't think it's a valid cached copy
    tmp_path.unlink(missing_ok=True)

    try:
        try:
            downloaded = storage.download(remote_key, tmp_path)
            if downloaded:
                log.debug("Downloaded remote baseline in %.3fs", time.time() - dl_start)
            else:
                log.debug("Remote baseline unchanged (cache hit)")
        except FileNotFoundError:
            log.debug("No remote baseline found — skipping import")
            return storage

        if db is None:
            return storage
        try:
            import_start = time.time()
            result = db.import_baseline_from(str(tmp_path))
            log.debug(
                "Imported %s baseline fingerprints and %s test executions in %.3fs",
                result.baseline_count,
                result.test_execution_count,
                time.time() - import_start,
            )
            logger.info(
                "✓ pytest-diff: Imported %s baseline fingerprints"
                " and %s test executions from remote into %s",
                result.baseline_count,
                result.test_execution_count,
                db_path,
            )

            _check_baseline_staleness(db, rootdir, log)
        except Exception as e:
            logger.warning("⚠ pytest-diff: Failed to import remote baseline: %s", e)
    finally:
        # Clean up temp file
        tmp_path.unlink(missing_ok=True)

    return storage


def _download_and_merge_baselines(
    storage: Any,
    prefix: str,
    db: Any,
    db_path: Path,
    rootdir: str,
    log: Any,
) -> Any:
    """Download all baseline files from a prefix and merge them."""
    dl_start = time.time()

    # Create temp directory for downloaded files
    temp_dir = Path(tempfile.mkdtemp(prefix="pytest_diff_"))

    try:
        downloaded_files = storage.download_all(temp_dir, prefix.rstrip("/"))

        if not downloaded_files:
            log.debug("No baseline files found at remote prefix — skipping import")
            return storage

        log.debug(
            "Downloaded %d baseline files in %.3fs", len(downloaded_files), time.time() - dl_start
        )

        if db is None:
            return storage

        # Check for commit mismatches before merging
        _check_commit_consistency(db, downloaded_files, log)

        # Merge all downloaded databases
        total_baseline_count = 0
        total_test_count = 0
        merge_start = time.time()
        for db_file in downloaded_files:
            try:
                result = db.merge_baseline_from(str(db_file))
                log.debug(
                    "Merged %d baselines and %d test executions from %s",
                    result.baseline_count,
                    result.test_execution_count,
                    db_file.name,
                )
                total_baseline_count += result.baseline_count
                total_test_count += result.test_execution_count
            except Exception as e:
                logger.warning("⚠ pytest-diff: Failed to merge baseline from %s: %s", db_file, e)

        log.debug(
            "Merged %d total baselines and %d test executions in %.3fs",
            total_baseline_count,
            total_test_count,
            time.time() - merge_start,
        )
        logger.info(
            "✓ pytest-diff: Merged %d baseline fingerprints"
            " and %d test executions from %d files into %s",
            total_baseline_count,
            total_test_count,
            len(downloaded_files),
            db_path,
        )

        _check_baseline_staleness(db, rootdir, log)

    finally:
        # Clean up temp directory
        import shutil

        shutil.rmtree(temp_dir, ignore_errors=True)

    return storage


def _check_commit_consistency(db: Any, db_files: list[Path], log: Any) -> None:
    """Check that all databases being merged have the same baseline_commit.

    Warns if databases have different commits, which could indicate
    they were generated from different CI runs.
    """
    commits: dict[str, list[str]] = {}  # commit -> list of filenames

    for db_file in db_files:
        try:
            commit = db.get_external_metadata(str(db_file), "baseline_commit")
            if commit:
                commits.setdefault(commit, []).append(db_file.name)
        except Exception as e:
            log.debug("Could not read baseline_commit from %s: %s", db_file.name, e)

    if len(commits) > 1:
        # Multiple different commits found
        details = ", ".join(f"{sha[:8]}({len(files)} files)" for sha, files in commits.items())
        logger.warning(
            "⚠ pytest-diff: Merging baselines from different commits: %s. "
            "This may cause inconsistent test selection.",
            details,
        )


def _check_baseline_staleness(db: Any, rootdir: str, log: Any) -> None:
    """Check if the baseline is stale compared to git history."""
    baseline_commit = db.get_metadata("baseline_commit")
    if baseline_commit:
        from pytest_diff._git import check_baseline_staleness

        warning = check_baseline_staleness(baseline_commit, rootdir)
        if warning:
            logger.warning("⚠ pytest-diff: %s", warning)
    else:
        log.debug("No baseline_commit metadata found — skipping staleness check")


def upload_baseline(
    storage: Any,
    remote_url: str | None,
    remote_key: str,
    db_path: Path,
    log: Any,
) -> Any:
    """Upload local baseline DB to remote storage.

    *log* is a ``logging.Logger`` instance.

    Returns the (possibly newly created) storage object.
    """
    storage = init_storage(storage, remote_url)
    if storage is None:
        return storage

    upload_start = time.time()
    storage.upload(db_path, remote_key)
    log.debug("Uploaded baseline in %.3fs", time.time() - upload_start)
    assert remote_url is not None
    url = remote_url.rstrip("/") + "/" + remote_key.lstrip("/")
    logger.info("✓ pytest-diff: Uploaded baseline to %s", url)

    return storage
