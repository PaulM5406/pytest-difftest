"""Remote baseline storage backends for pytest-diff."""

from __future__ import annotations

from pytest_diff.storage.base import BaselineStorage


def get_storage(url: str) -> BaselineStorage | None:
    """Create a storage backend from a URL scheme.

    Supported schemes:
    - ``file:///path/to/dir/`` — local filesystem (for testing / simple CI)
    - ``s3://bucket/prefix/``  — Amazon S3 (requires ``boto3``)

    Returns ``None`` if the scheme is not recognised.
    """
    if url.startswith("file://"):
        from pytest_diff.storage.local import LocalStorage

        return LocalStorage(url)

    if url.startswith("s3://"):
        from pytest_diff.storage.s3 import S3Storage

        return S3Storage(url)

    return None


__all__ = ["BaselineStorage", "get_storage"]
