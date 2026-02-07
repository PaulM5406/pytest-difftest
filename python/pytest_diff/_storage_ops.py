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


def parse_remote_url(url: str) -> tuple[str, str]:
    """Split a remote URL into (base_url, key).

    - ``s3://bucket/prefix/``         → ``("s3://bucket/prefix/", "")``
    - ``s3://bucket/path/file.db``    → ``("s3://bucket/path/", "file.db")``
    - ``file:///tmp/dir/``            → ``("file:///tmp/dir/", "")``
    - ``file:///tmp/dir/baseline.db`` → ``("file:///tmp/dir/", "baseline.db")``
    """
    if url.endswith("/"):
        return (url, "")
    parts = url.rsplit("/", 1)
    if len(parts) == 2:
        return (parts[0] + "/", parts[1])
    return (url, "")


def download_remote_databases(remote_url: str, dest_dir: Path) -> list[Path]:
    """Download all .db files from a remote prefix to *dest_dir*.

    *remote_url* must be a prefix URL (ending with ``/``).
    Returns the list of downloaded file paths.
    """
    from pytest_diff.storage import get_storage

    base_url, _ = parse_remote_url(remote_url)
    storage = get_storage(base_url)
    if storage is None:
        raise ValueError(f"Unsupported remote URL scheme: {remote_url}")

    # The prefix is everything after the scheme+bucket in the base_url.
    # For storage backends, we pass an empty prefix since the base_url
    # already includes the full prefix path.
    return storage.download_all(dest_dir)


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


def download_and_import_baseline(
    storage: Any,
    remote_url: str | None,
    remote_key: str,
    db: Any,
    db_path: Path,
    rootdir: str,
    log: Any,
) -> Any:
    """Download a single remote baseline DB and import via ATTACH.

    *log* is a ``logging.Logger`` instance.

    Returns the (possibly newly created) storage object.
    """
    storage = init_storage(storage, remote_url)
    if storage is None:
        return storage

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


def upload_to_remote(remote_url: str, local_path: Path) -> None:
    """Upload a local file to a remote URL.

    *remote_url* must point to a specific file (not a prefix).
    """
    from pytest_diff.storage import get_storage

    base_url, key = parse_remote_url(remote_url)
    if not key:
        raise ValueError(f"Remote URL must point to a specific file, not a prefix: {remote_url}")

    storage = get_storage(base_url)
    if storage is None:
        raise ValueError(f"Unsupported remote URL scheme: {remote_url}")

    storage.upload(local_path, key)
