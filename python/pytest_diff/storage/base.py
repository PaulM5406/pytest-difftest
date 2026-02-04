"""Abstract base class for remote baseline storage."""

from __future__ import annotations

from abc import ABC, abstractmethod
from pathlib import Path


class BaselineStorage(ABC):
    """Interface for uploading/downloading baseline DB files."""

    @abstractmethod
    def upload(self, local_path: Path, remote_key: str) -> None:
        """Upload a local file to remote storage.

        Raises on failure.
        """

    @abstractmethod
    def download(self, remote_key: str, local_path: Path) -> bool:
        """Download a remote file to a local path.

        Returns ``True`` if a new file was downloaded, ``False`` if the
        cached copy is already up-to-date (ETag / mtime match).

        Raises on failure (other than cache-hit).
        """

    def list_baselines(self, prefix: str = "") -> list[str]:
        """List all .db files under a prefix.

        Override in subclasses that support prefix listing.
        Default implementation returns empty list.
        """
        return []

    def download_all(self, local_dir: Path, prefix: str = "") -> list[Path]:
        """Download all .db files from the configured prefix to local_dir.

        Override in subclasses that support bulk downloads.
        Default implementation returns empty list.
        """
        return []
