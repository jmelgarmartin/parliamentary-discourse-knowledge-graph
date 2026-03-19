import json
import pathlib

from congress_analysis.backup_manager import BackupManager


def test_create_backup_creates_directory(tmp_path: pathlib.Path) -> None:
    """Test that a backup directory is created with a timestamped name."""
    data_dir = tmp_path / "data"
    data_dir.mkdir()
    backup_dir = tmp_path / "backups"

    manager = BackupManager(data_root=str(data_dir), backup_root=str(backup_dir))
    backup_path_str = manager.create_backup()

    assert backup_path_str != ""
    backup_path = pathlib.Path(backup_path_str)
    assert backup_path.exists()
    assert backup_path.is_dir()
    assert (backup_path / "manifest.json").exists()


def test_backup_contains_parquet_files(tmp_path: pathlib.Path) -> None:
    """Test that parquet files are correctly copied into the backup."""
    data_dir = tmp_path / "data"
    bronze_dir = data_dir / "bronze" / "test_legislature"
    bronze_dir.mkdir(parents=True)

    parquet_content = b"fake parquet content"
    p_file = bronze_dir / "test.parquet"
    p_file.write_bytes(parquet_content)

    backup_dir = tmp_path / "backups"
    manager = BackupManager(data_root=str(data_dir), backup_root=str(backup_dir))
    backup_path_str = manager.create_backup()

    backup_path = pathlib.Path(backup_path_str)
    copied_p_file = backup_path / "parquet" / "bronze" / "test_legislature" / "test.parquet"
    assert copied_p_file.exists()
    assert copied_p_file.read_bytes() == parquet_content


def test_backup_contains_mapping_if_exists(tmp_path: pathlib.Path) -> None:
    """Test that manual mapping files are correctly copied into the backup."""
    data_dir = tmp_path / "data"
    ref_dir = data_dir / "reference" / "legislature_15"
    ref_dir.mkdir(parents=True)

    mapping_content = "id,name\n1,Test"
    m_file = ref_dir / "government_manual_mapping.csv"
    m_file.write_text(mapping_content)

    backup_dir = tmp_path / "backups"
    manager = BackupManager(data_root=str(data_dir), backup_root=str(backup_dir))
    backup_path_str = manager.create_backup()

    backup_path = pathlib.Path(backup_path_str)
    copied_m_file = backup_path / "mappings" / "reference" / "legislature_15" / "government_manual_mapping.csv"
    assert copied_m_file.exists()
    assert copied_m_file.read_text() == mapping_content


def test_manifest_is_created(tmp_path: pathlib.Path) -> None:
    """Test that the manifest.json file contains valid metadata."""
    data_dir = tmp_path / "data"
    bronze_dir = data_dir / "bronze"
    bronze_dir.mkdir(parents=True)
    (bronze_dir / "t.parquet").write_bytes(b"data")

    backup_dir = tmp_path / "backups"
    manager = BackupManager(data_root=str(data_dir), backup_root=str(backup_dir))
    backup_path_str = manager.create_backup()

    manifest_path = pathlib.Path(backup_path_str) / "manifest.json"
    with open(manifest_path, "r") as f:
        manifest = json.load(f)

    assert "timestamp" in manifest
    assert "files" in manifest
    assert len(manifest["files"]) == 1
    assert manifest["files"][0]["path"] == "parquet/bronze/t.parquet"


def test_no_failure_if_no_files_exist(tmp_path: pathlib.Path) -> None:
    """Test that the backup doesn't fail if the data directory is empty or missing."""
    data_dir = tmp_path / "non_existent_data"
    backup_dir = tmp_path / "backups"

    manager = BackupManager(data_root=str(data_dir), backup_root=str(backup_dir))
    backup_path_str = manager.create_backup()

    assert backup_path_str != ""
    backup_path = pathlib.Path(backup_path_str)
    assert backup_path.exists()

    manifest_path = backup_path / "manifest.json"
    with open(manifest_path, "r") as f:
        manifest = json.load(f)
    assert manifest["files"] == []
    assert manifest["mapping_files"] == []
