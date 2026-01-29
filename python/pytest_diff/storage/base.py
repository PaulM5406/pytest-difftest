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
