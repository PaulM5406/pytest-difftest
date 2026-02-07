"""Tests for local and S3 storage backends."""

from __future__ import annotations

from pathlib import Path

import pytest


def _create_source_db(db_path: Path, py_file: Path) -> None:
    """Create a source database with a baseline fingerprint from a Python file."""
    from pytest_diff._core import PytestDiffDatabase, calculate_fingerprint

    db = PytestDiffDatabase(str(db_path))
    db.save_baseline_fingerprint(calculate_fingerprint(str(py_file)))
    db.close()


class TestLocalStorage:
    """Tests for the local filesystem storage backend."""

    def test_upload_download_roundtrip(self, tmp_path: Path) -> None:
        from pytest_diff.storage.local import LocalStorage

        remote_dir = tmp_path / "remote"
        remote_dir.mkdir()
        storage = LocalStorage(f"file://{remote_dir}")

        # Create a local file and upload it
        local_file = tmp_path / "local.db"
        local_file.write_bytes(b"hello baseline")
        storage.upload(local_file, "baseline.db")

        # Download to a new location
        dest = tmp_path / "downloaded.db"
        downloaded = storage.download("baseline.db", dest)
        assert downloaded is True
        assert dest.read_bytes() == b"hello baseline"

    def test_download_not_found(self, tmp_path: Path) -> None:
        from pytest_diff.storage.local import LocalStorage

        remote_dir = tmp_path / "remote"
        remote_dir.mkdir()
        storage = LocalStorage(f"file://{remote_dir}")

        with pytest.raises(FileNotFoundError):
            storage.download("missing.db", tmp_path / "out.db")

    def test_download_cache_hit(self, tmp_path: Path) -> None:
        from pytest_diff.storage.local import LocalStorage

        remote_dir = tmp_path / "remote"
        remote_dir.mkdir()
        storage = LocalStorage(f"file://{remote_dir}")

        # Upload a file
        local_file = tmp_path / "local.db"
        local_file.write_bytes(b"data")
        storage.upload(local_file, "baseline.db")

        # First download: returns True
        dest = tmp_path / "downloaded.db"
        assert storage.download("baseline.db", dest) is True

        # Second download with same file already present: returns False (cache hit)
        assert storage.download("baseline.db", dest) is False


class TestS3Storage:
    """Tests for the S3 storage backend using moto."""

    @pytest.fixture(autouse=True)
    def _require_moto(self) -> None:
        pytest.importorskip("moto")

    @pytest.fixture()
    def s3_storage(self, tmp_path: Path):
        import boto3
        from moto import mock_aws  # type: ignore[import-not-found]

        from pytest_diff.storage.s3 import S3Storage

        with mock_aws():
            client = boto3.client("s3", region_name="us-east-1")
            client.create_bucket(Bucket="test-bucket")

            storage = S3Storage("s3://test-bucket/prefix/")
            # Override the lazily-created client with the mock one
            storage._client = client
            yield storage

    def test_upload_download_roundtrip(self, s3_storage, tmp_path: Path) -> None:
        local_file = tmp_path / "local.db"
        local_file.write_bytes(b"s3 baseline data")
        s3_storage.upload(local_file, "baseline.db")

        dest = tmp_path / "downloaded.db"
        downloaded = s3_storage.download("baseline.db", dest)
        assert downloaded is True
        assert dest.read_bytes() == b"s3 baseline data"

    def test_download_not_found(self, s3_storage, tmp_path: Path) -> None:
        with pytest.raises(FileNotFoundError):
            s3_storage.download("missing.db", tmp_path / "out.db")


class TestS3AuthErrors:
    """Tests for S3 authentication error detection (uses mocks, no moto needed)."""

    @pytest.fixture(autouse=True)
    def _require_botocore(self) -> None:
        pytest.importorskip("botocore")

    def test_download_access_denied_raises_auth_error(self, tmp_path: Path) -> None:
        from unittest.mock import MagicMock

        from botocore.exceptions import ClientError

        from pytest_diff.storage.base import StorageAuthenticationError
        from pytest_diff.storage.s3 import S3Storage

        storage = S3Storage("s3://test-bucket/prefix/")
        mock_client = MagicMock()
        error_response = {
            "Error": {"Code": "AccessDenied", "Message": "Access Denied"},
            "ResponseMetadata": {"HTTPStatusCode": 403},
        }
        mock_client.get_object.side_effect = ClientError(error_response, "GetObject")
        mock_client.exceptions.NoSuchKey = type("NoSuchKey", (ClientError,), {})
        storage._client = mock_client

        with pytest.raises(StorageAuthenticationError, match="authentication failed"):
            storage.download("baseline.db", tmp_path / "out.db")

    def test_list_baselines_access_denied_raises_auth_error(self) -> None:
        from unittest.mock import MagicMock

        from botocore.exceptions import ClientError

        from pytest_diff.storage.base import StorageAuthenticationError
        from pytest_diff.storage.s3 import S3Storage

        storage = S3Storage("s3://test-bucket/prefix/")
        mock_client = MagicMock()
        error_response = {
            "Error": {"Code": "AccessDenied", "Message": "Access Denied"},
            "ResponseMetadata": {"HTTPStatusCode": 403},
        }
        paginator = MagicMock()
        paginator.paginate.side_effect = ClientError(error_response, "ListObjectsV2")
        mock_client.get_paginator.return_value = paginator
        storage._client = mock_client

        with pytest.raises(StorageAuthenticationError, match="authentication failed"):
            storage.list_baselines()


class TestLocalStorageListAndDownloadAll:
    """Tests for list_baselines and download_all on local storage."""

    def test_list_baselines_empty(self, tmp_path: Path) -> None:
        from pytest_diff.storage.local import LocalStorage

        remote_dir = tmp_path / "remote"
        remote_dir.mkdir()
        storage = LocalStorage(f"file://{remote_dir}")

        assert storage.list_baselines() == []

    def test_list_baselines_finds_db_files(self, tmp_path: Path) -> None:
        from pytest_diff.storage.local import LocalStorage

        remote_dir = tmp_path / "remote"
        remote_dir.mkdir()
        (remote_dir / "baselines").mkdir()
        (remote_dir / "baselines" / "job1.db").write_bytes(b"db1")
        (remote_dir / "baselines" / "job2.db").write_bytes(b"db2")
        (remote_dir / "baselines" / "not_a_db.txt").write_bytes(b"txt")

        storage = LocalStorage(f"file://{remote_dir}")
        keys = storage.list_baselines("baselines")

        assert len(keys) == 2
        assert "baselines/job1.db" in keys
        assert "baselines/job2.db" in keys

    def test_download_all(self, tmp_path: Path) -> None:
        from pytest_diff.storage.local import LocalStorage

        remote_dir = tmp_path / "remote"
        remote_dir.mkdir()
        (remote_dir / "baselines").mkdir()
        (remote_dir / "baselines" / "job1.db").write_bytes(b"db1 content")
        (remote_dir / "baselines" / "job2.db").write_bytes(b"db2 content")

        storage = LocalStorage(f"file://{remote_dir}")
        local_dir = tmp_path / "local"
        local_dir.mkdir()

        downloaded = storage.download_all(local_dir, "baselines")

        assert len(downloaded) == 2
        assert (local_dir / "job1.db").exists()
        assert (local_dir / "job2.db").exists()
        assert (local_dir / "job1.db").read_bytes() == b"db1 content"


class TestCliMerge:
    """Tests for the CLI merge command."""

    def test_merge_databases(self, tmp_path: Path) -> None:
        from pytest_diff._core import PytestDiffDatabase, calculate_fingerprint
        from pytest_diff.cli import merge_databases

        # Create Python files for fingerprinting
        foo_file = tmp_path / "foo.py"
        foo_file.write_text("def foo():\n    return 'foo'\n")
        bar_file = tmp_path / "bar.py"
        bar_file.write_text("def bar():\n    return 'bar'\n")

        # Create first source database
        source1_path = tmp_path / "source1.db"
        source1_db = PytestDiffDatabase(str(source1_path))
        fp1 = calculate_fingerprint(str(foo_file))
        source1_db.save_baseline_fingerprint(fp1)
        source1_db.close()

        # Create second source database
        source2_path = tmp_path / "source2.db"
        source2_db = PytestDiffDatabase(str(source2_path))
        fp2 = calculate_fingerprint(str(bar_file))
        source2_db.save_baseline_fingerprint(fp2)
        source2_db.close()

        # Merge into output
        output_path = tmp_path / "output.db"
        result = merge_databases(str(output_path), [str(source1_path), str(source2_path)])

        assert result == 0

        # Verify merged database
        output_db = PytestDiffDatabase(str(output_path))
        stats = output_db.get_stats()
        assert stats["baseline_count"] == 2

    def test_merge_no_inputs(self) -> None:
        from pytest_diff.cli import merge_databases

        result = merge_databases("output.db", [])
        assert result == 1

    def test_merge_missing_input(self, tmp_path: Path) -> None:
        from pytest_diff.cli import merge_databases

        result = merge_databases(str(tmp_path / "output.db"), ["/nonexistent/path.db"])
        assert result == 1


class TestCommitConsistencyWarning:
    """Tests for warnings when merging databases with different commits."""

    def test_cli_warns_on_different_commits(self, tmp_path: Path, capsys) -> None:
        from pytest_diff._core import PytestDiffDatabase, calculate_fingerprint
        from pytest_diff.cli import merge_databases

        # Create Python files
        foo_file = tmp_path / "foo.py"
        foo_file.write_text("def foo():\n    return 'foo'\n")
        bar_file = tmp_path / "bar.py"
        bar_file.write_text("def bar():\n    return 'bar'\n")

        # Create first database with commit A
        source1_path = tmp_path / "source1.db"
        source1_db = PytestDiffDatabase(str(source1_path))
        source1_db.save_baseline_fingerprint(calculate_fingerprint(str(foo_file)))
        source1_db.set_metadata("baseline_commit", "aaaa1111222233334444555566667777")
        source1_db.close()

        # Create second database with commit B (different)
        source2_path = tmp_path / "source2.db"
        source2_db = PytestDiffDatabase(str(source2_path))
        source2_db.save_baseline_fingerprint(calculate_fingerprint(str(bar_file)))
        source2_db.set_metadata("baseline_commit", "bbbb1111222233334444555566667777")
        source2_db.close()

        # Merge should succeed but warn
        output_path = tmp_path / "output.db"
        result = merge_databases(str(output_path), [str(source1_path), str(source2_path)])

        assert result == 0
        captured = capsys.readouterr()
        assert "different commits" in captured.err
        assert "aaaa1111" in captured.err
        assert "bbbb1111" in captured.err

    def test_cli_no_warning_on_same_commits(self, tmp_path: Path, capsys) -> None:
        from pytest_diff._core import PytestDiffDatabase, calculate_fingerprint
        from pytest_diff.cli import merge_databases

        # Create Python files
        foo_file = tmp_path / "foo.py"
        foo_file.write_text("def foo():\n    return 'foo'\n")
        bar_file = tmp_path / "bar.py"
        bar_file.write_text("def bar():\n    return 'bar'\n")

        # Create both databases with the same commit
        same_commit = "cccc1111222233334444555566667777"

        source1_path = tmp_path / "source1.db"
        source1_db = PytestDiffDatabase(str(source1_path))
        source1_db.save_baseline_fingerprint(calculate_fingerprint(str(foo_file)))
        source1_db.set_metadata("baseline_commit", same_commit)
        source1_db.close()

        source2_path = tmp_path / "source2.db"
        source2_db = PytestDiffDatabase(str(source2_path))
        source2_db.save_baseline_fingerprint(calculate_fingerprint(str(bar_file)))
        source2_db.set_metadata("baseline_commit", same_commit)
        source2_db.close()

        # Merge should succeed without warning
        output_path = tmp_path / "output.db"
        result = merge_databases(str(output_path), [str(source1_path), str(source2_path)])

        assert result == 0
        captured = capsys.readouterr()
        assert "different commits" not in captured.err

    def test_get_external_metadata(self, tmp_path: Path) -> None:
        from pytest_diff._core import PytestDiffDatabase

        # Create a database with metadata
        source_path = tmp_path / "source.db"
        source_db = PytestDiffDatabase(str(source_path))
        source_db.set_metadata("baseline_commit", "test_commit_sha")
        source_db.set_metadata("other_key", "other_value")
        source_db.close()

        # Read metadata from external database
        reader_db = PytestDiffDatabase(str(tmp_path / "reader.db"))
        commit = reader_db.get_external_metadata(str(source_path), "baseline_commit")
        other = reader_db.get_external_metadata(str(source_path), "other_key")
        missing = reader_db.get_external_metadata(str(source_path), "nonexistent_key")

        assert commit == "test_commit_sha"
        assert other == "other_value"
        assert missing is None


class TestParseRemoteUrl:
    """Tests for parse_remote_url helper."""

    def test_prefix_url(self) -> None:
        from pytest_diff._storage_ops import parse_remote_url

        assert parse_remote_url("s3://bucket/prefix/") == ("s3://bucket/prefix/", "")

    def test_file_url(self) -> None:
        from pytest_diff._storage_ops import parse_remote_url

        assert parse_remote_url("s3://bucket/path/baseline.db") == (
            "s3://bucket/path/",
            "baseline.db",
        )

    def test_file_url_local(self) -> None:
        from pytest_diff._storage_ops import parse_remote_url

        assert parse_remote_url("file:///tmp/dir/baseline.db") == (
            "file:///tmp/dir/",
            "baseline.db",
        )

    def test_prefix_url_local(self) -> None:
        from pytest_diff._storage_ops import parse_remote_url

        assert parse_remote_url("file:///tmp/dir/") == ("file:///tmp/dir/", "")


class TestCliMergeRemote:
    """Tests for CLI merge with remote support using file:// URLs."""

    def test_merge_from_remote_prefix(self, tmp_path: Path) -> None:
        from pytest_diff._core import PytestDiffDatabase
        from pytest_diff.cli import merge_databases

        # Set up remote directory with .db files
        remote_dir = tmp_path / "remote"
        remote_dir.mkdir()

        foo_file = tmp_path / "foo.py"
        foo_file.write_text("def foo():\n    return 'foo'\n")
        bar_file = tmp_path / "bar.py"
        bar_file.write_text("def bar():\n    return 'bar'\n")

        _create_source_db(remote_dir / "job1.db", foo_file)
        _create_source_db(remote_dir / "job2.db", bar_file)

        output_path = tmp_path / "merged.db"
        result = merge_databases(
            str(output_path),
            [f"file://{remote_dir}/"],
        )

        assert result == 0
        db = PytestDiffDatabase(str(output_path))
        stats = db.get_stats()
        assert stats["baseline_count"] == 2

    def test_merge_to_remote(self, tmp_path: Path) -> None:
        from pytest_diff.cli import merge_databases

        # Create local source databases
        foo_file = tmp_path / "foo.py"
        foo_file.write_text("def foo():\n    return 'foo'\n")
        bar_file = tmp_path / "bar.py"
        bar_file.write_text("def bar():\n    return 'bar'\n")

        source1 = tmp_path / "source1.db"
        source2 = tmp_path / "source2.db"
        _create_source_db(source1, foo_file)
        _create_source_db(source2, bar_file)

        # Set up remote destination
        remote_dir = tmp_path / "remote"
        remote_dir.mkdir()

        result = merge_databases(
            f"file://{remote_dir}/baseline.db",
            [str(source1), str(source2)],
        )

        assert result == 0
        assert (remote_dir / "baseline.db").exists()

    def test_merge_full_remote_round_trip(self, tmp_path: Path) -> None:
        """Remote-to-remote merge: download from prefix, upload to remote URL."""
        from pytest_diff._core import PytestDiffDatabase
        from pytest_diff.cli import merge_databases

        # Set up remote source with .db files
        remote_src = tmp_path / "remote_src"
        remote_src.mkdir()

        foo_file = tmp_path / "foo.py"
        foo_file.write_text("def foo():\n    return 'foo'\n")
        bar_file = tmp_path / "bar.py"
        bar_file.write_text("def bar():\n    return 'bar'\n")

        _create_source_db(remote_src / "job1.db", foo_file)
        _create_source_db(remote_src / "job2.db", bar_file)

        # Set up remote destination
        remote_dst = tmp_path / "remote_dst"
        remote_dst.mkdir()

        # Remote output + remote input prefix
        result = merge_databases(
            f"file://{remote_dst}/baseline.db",
            [f"file://{remote_src}/"],
        )

        assert result == 0
        assert (remote_dst / "baseline.db").exists()

        # Verify the uploaded file is a valid database
        db = PytestDiffDatabase(str(remote_dst / "baseline.db"))
        stats = db.get_stats()
        assert stats["baseline_count"] == 2

    def test_merge_local_output_remote_input(self, tmp_path: Path) -> None:
        """Merge remote inputs into a local output path."""
        from pytest_diff._core import PytestDiffDatabase
        from pytest_diff.cli import merge_databases

        remote_src = tmp_path / "remote_src"
        remote_src.mkdir()

        foo_file = tmp_path / "foo.py"
        foo_file.write_text("def foo():\n    return 'foo'\n")
        bar_file = tmp_path / "bar.py"
        bar_file.write_text("def bar():\n    return 'bar'\n")

        _create_source_db(remote_src / "job1.db", foo_file)
        _create_source_db(remote_src / "job2.db", bar_file)

        output_path = tmp_path / "merged.db"
        result = merge_databases(
            str(output_path),
            [f"file://{remote_src}/"],
        )

        assert result == 0
        assert output_path.exists()

        db = PytestDiffDatabase(str(output_path))
        stats = db.get_stats()
        assert stats["baseline_count"] == 2

    def test_merge_mixed_local_and_remote(self, tmp_path: Path) -> None:
        from pytest_diff._core import PytestDiffDatabase
        from pytest_diff.cli import merge_databases

        # Set up remote source
        remote_dir = tmp_path / "remote"
        remote_dir.mkdir()

        foo_file = tmp_path / "foo.py"
        foo_file.write_text("def foo():\n    return 'foo'\n")
        bar_file = tmp_path / "bar.py"
        bar_file.write_text("def bar():\n    return 'bar'\n")
        baz_file = tmp_path / "baz.py"
        baz_file.write_text("def baz():\n    return 'baz'\n")

        _create_source_db(remote_dir / "remote_job.db", foo_file)

        local_source = tmp_path / "local.db"
        _create_source_db(local_source, bar_file)

        local_source2 = tmp_path / "local2.db"
        _create_source_db(local_source2, baz_file)

        output_path = tmp_path / "merged.db"
        result = merge_databases(
            str(output_path),
            [str(local_source), str(local_source2), f"file://{remote_dir}/"],
        )

        assert result == 0
        db = PytestDiffDatabase(str(output_path))
        stats = db.get_stats()
        assert stats["baseline_count"] == 3

    def test_merge_from_remote_empty_prefix(self, tmp_path: Path, capsys) -> None:
        from pytest_diff.cli import merge_databases

        # Remote dir exists but has no .db files
        remote_dir = tmp_path / "remote"
        remote_dir.mkdir()

        output_path = tmp_path / "merged.db"
        result = merge_databases(
            str(output_path),
            [f"file://{remote_dir}/"],
        )

        assert result == 1
        captured = capsys.readouterr()
        assert "No .db files found" in captured.err

    def test_merge_from_local_directory(self, tmp_path: Path) -> None:
        from pytest_diff._core import PytestDiffDatabase
        from pytest_diff.cli import merge_databases

        # Set up a local directory with .db files
        input_dir = tmp_path / "inputs"
        input_dir.mkdir()

        foo_file = tmp_path / "foo.py"
        foo_file.write_text("def foo():\n    return 'foo'\n")
        bar_file = tmp_path / "bar.py"
        bar_file.write_text("def bar():\n    return 'bar'\n")

        _create_source_db(input_dir / "job1.db", foo_file)
        _create_source_db(input_dir / "job2.db", bar_file)

        output_path = tmp_path / "merged.db"
        result = merge_databases(str(output_path), [str(input_dir)])

        assert result == 0
        db = PytestDiffDatabase(str(output_path))
        stats = db.get_stats()
        assert stats["baseline_count"] == 2

    def test_merge_no_inputs_error(self, capsys) -> None:
        from pytest_diff.cli import merge_databases

        result = merge_databases("output.db", [])

        assert result == 1
        captured = capsys.readouterr()
        assert "input database required" in captured.err
