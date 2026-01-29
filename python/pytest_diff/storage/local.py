"""Local filesystem storage backend (``file://`` URLs)."""

from __future__ import annotations

import shutil
from pathlib import Path

from pytest_diff.storage.base import BaselineStorage


class LocalStorage(BaselineStorage):
    """Store/retrieve baseline DB on the local filesystem.

    URL format: ``file:///absolute/path/to/directory/``
    """

    def __init__(self, url: str) -> None:
        # Strip scheme; handle file:///path and file://localhost/path
        path_str = url.removeprefix("file://")
        if path_str.startswith("localhost"):
            path_str = path_str.removeprefix("localhost")
        self.root = Path(path_str)

    def upload(self, local_path: Path, remote_key: str) -> None:
        dest = self.root / remote_key
        dest.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(local_path, dest)

    def download(self, remote_key: str, local_path: Path) -> bool:
        src = self.root / remote_key
        if not src.exists():
            raise FileNotFoundError(f"Remote baseline not found: {src}")

        # Staleness check: skip download if local file exists and is at least
        # as new as the remote copy.
        if local_path.exists():
            local_mtime = local_path.stat().st_mtime
            remote_mtime = src.stat().st_mtime
            if local_mtime >= remote_mtime:
                return False

        local_path.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(src, local_path)
        return True
