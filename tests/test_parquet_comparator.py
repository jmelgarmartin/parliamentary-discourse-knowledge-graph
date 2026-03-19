import json
import pathlib

import pandas as pd
import pytest
from congress_analysis.parquet_comparator import ParquetComparator


@pytest.fixture()  # type: ignore[misc]
def comparator(tmp_path: pathlib.Path) -> ParquetComparator:
    return ParquetComparator(data_root=str(tmp_path / "data"), reports_root=str(tmp_path / "reports"))


def test_compare_identical_parquets(tmp_path: pathlib.Path, comparator: ParquetComparator) -> None:
    """Test that identical parquet files are correctly identified."""
    data_dir = tmp_path / "data"
    backup_path = tmp_path / "backups" / "2026-03-19_100000"
    parquet_backup_dir = backup_path / "parquet" / "silver"
    parquet_backup_dir.mkdir(parents=True)

    current_dir = data_dir / "silver"
    current_dir.mkdir(parents=True)

    df = pd.DataFrame({"id": [1, 2], "name": ["Alice", "Bob"]})
    df.to_parquet(parquet_backup_dir / "test.parquet")
    df.to_parquet(current_dir / "test.parquet")

    report_path_str = comparator.compare_backup_to_current(str(backup_path))
    assert report_path_str != ""

    with open(pathlib.Path(report_path_str) / "summary.json", "r") as f:
        summary = json.load(f)

    assert summary["datasets"][0]["is_identical"] is True
    assert summary["datasets"][0]["common_row_hashes"] == 2


def test_detect_missing_current_parquet(tmp_path: pathlib.Path, comparator: ParquetComparator) -> None:
    """Test that missing current parquet files are detected."""
    backup_path = tmp_path / "backups" / "2026-03-19_100000"
    parquet_backup_dir = backup_path / "parquet" / "bronze"
    parquet_backup_dir.mkdir(parents=True)

    pd.DataFrame({"a": [1]}).to_parquet(parquet_backup_dir / "only_in_backup.parquet")

    report_path_str = comparator.compare_backup_to_current(str(backup_path))

    with open(pathlib.Path(report_path_str) / "summary.json", "r") as f:
        summary = json.load(f)

    assert summary["datasets"][0]["current_exists"] is False
    assert summary["datasets"][0]["is_identical"] is False


def test_detect_different_columns(tmp_path: pathlib.Path, comparator: ParquetComparator) -> None:
    """Test that schema differences are detected."""
    data_dir = tmp_path / "data" / "silver"
    data_dir.mkdir(parents=True)
    backup_dir = tmp_path / "backups" / "ts" / "parquet" / "silver"
    backup_dir.mkdir(parents=True)

    pd.DataFrame({"id": [1], "name": ["X"]}).to_parquet(backup_dir / "cols.parquet")
    pd.DataFrame({"id": [1], "age": [30]}).to_parquet(data_dir / "cols.parquet")

    report_path_str = comparator.compare_backup_to_current(str(tmp_path / "backups" / "ts"))

    with open(pathlib.Path(report_path_str) / "summary.json", "r") as f:
        summary = json.load(f)

    assert summary["datasets"][0]["columns_match"] is False
    assert summary["datasets"][0]["is_identical"] is False


def test_detect_row_content_differences(tmp_path: pathlib.Path, comparator: ParquetComparator) -> None:
    """Test that content differences are detected even with matching schemas and counts."""
    data_dir = tmp_path / "data" / "silver"
    data_dir.mkdir(parents=True)
    backup_dir = tmp_path / "backups" / "ts" / "parquet" / "silver"
    backup_dir.mkdir(parents=True)

    pd.DataFrame({"id": [1, 2], "val": ["A", "B"]}).to_parquet(backup_dir / "diff.parquet")
    pd.DataFrame({"id": [1, 2], "val": ["A", "C"]}).to_parquet(data_dir / "diff.parquet")

    report_path_str = comparator.compare_backup_to_current(str(tmp_path / "backups" / "ts"))

    with open(pathlib.Path(report_path_str) / "summary.json", "r") as f:
        summary = json.load(f)

    ds = summary["datasets"][0]
    assert ds["is_identical"] is False
    assert ds["common_row_hashes"] == 1
    assert ds["only_in_backup"] == 1
    assert ds["only_in_current"] == 1


def test_detect_different_row_counts(tmp_path: pathlib.Path, comparator: ParquetComparator) -> None:
    """Test that different row counts are detected."""
    data_dir = tmp_path / "data" / "silver"
    data_dir.mkdir(parents=True)
    backup_dir = tmp_path / "backups" / "ts" / "parquet" / "silver"
    backup_dir.mkdir(parents=True)

    pd.DataFrame({"id": [1, 2]}).to_parquet(backup_dir / "count.parquet")
    pd.DataFrame({"id": [1, 2, 3]}).to_parquet(data_dir / "count.parquet")

    report_path_str = comparator.compare_backup_to_current(str(tmp_path / "backups" / "ts"))

    with open(pathlib.Path(report_path_str) / "summary.json", "r") as f:
        summary = json.load(f)

    ds = summary["datasets"][0]
    assert ds["backup_row_count"] == 2
    assert ds["current_row_count"] == 3
    assert ds["is_identical"] is False


def test_summary_json_is_created(tmp_path: pathlib.Path, comparator: ParquetComparator) -> None:
    """Test that the summary.json file is correctly generated with metadata."""
    backup_path = tmp_path / "backups" / "2026-03-19_120000"
    (backup_path / "parquet").mkdir(parents=True)

    report_path_str = comparator.compare_backup_to_current(str(backup_path))

    report_path = pathlib.Path(report_path_str)
    assert (report_path / "summary.json").exists()

    with open(report_path / "summary.json", "r") as f:
        summary = json.load(f)

    assert summary["backup_timestamp"] == "2026-03-19_120000"
    assert "generated_at" in summary
    assert isinstance(summary["datasets"], list)
