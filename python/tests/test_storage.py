"""Tests for local and S3 storage backends."""

from __future__ import annotations

from pathlib import Path

import pytest


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
