"""
Manages creation of execution backups for parquet outputs and manual mappings.
"""

import datetime
import json
import logging
import pathlib
import shutil
from typing import Any, List

logger = logging.getLogger(__name__)


class BackupManager:
    """
    Manages creation of execution backups for parquet outputs and manual mappings.
    """

    def __init__(self, data_root: str = "data", backup_root: str = "backups"):
        self.data_root = pathlib.Path(data_root)
        self.backup_root = pathlib.Path(backup_root)

    def create_backup(self) -> str:
        """
        Creates a full backup and returns the backup directory path.
        """
        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
        backup_path = self.backup_root / timestamp

        try:
            backup_path.mkdir(parents=True, exist_ok=False)
            logger.info(f"Creating backup in {backup_path}")

            parquet_files = self._copy_parquet_files(backup_path)
            mapping_files = self._copy_mapping_file(backup_path)

            self._generate_manifest(backup_path, parquet_info=parquet_files, mapping_info=mapping_files)

            logger.info(f"Backup completed: {len(parquet_files)} parquet files and {len(mapping_files)} mapping files.")
            return str(backup_path)
        except Exception as e:
            logger.error(f"Failed to create backup: {e}")
            # In SAFE MODE, we don't want to break the pipeline if backup fails
            return ""

    def _copy_parquet_files(self, backup_path: pathlib.Path) -> List[dict[str, Any]]:
        """
        Copies parquet files into the backup parquet directory.
        """
        parquet_backup_dir = backup_path / "parquet"
        parquet_backup_dir.mkdir(parents=True, exist_ok=True)

        backed_up = []

        # Search for all .parquet files in bronze and silver layers
        for layer in ["bronze", "silver"]:
            layer_dir = self.data_root / layer
            if not layer_dir.exists():
                continue

            for p_file in layer_dir.rglob("*.parquet"):
                # Preserve relative structure within parquet backup dir?
                # The requirement says parquet/*.parquet, let's keep it simple as requested.
                # If there are name collisions, we might need a more complex structure,
                # but for now let's use the relative path as part of the filename or mirror the structure.
                # Mirroring structure is safer.

                rel_path = p_file.relative_to(self.data_root)
                target_path = parquet_backup_dir / rel_path
                target_path.parent.mkdir(parents=True, exist_ok=True)

                try:
                    shutil.copy2(p_file, target_path)
                    backed_up.append({"path": f"parquet/{rel_path.as_posix()}", "size_bytes": p_file.stat().st_size})
                except Exception as e:
                    logger.warning(f"Could not backup {p_file}: {e}")

        return backed_up

    def _copy_mapping_file(self, backup_path: pathlib.Path) -> List[dict[str, Any]]:
        """
        Copies government_manual_mapping.csv into the backup mappings directory if it exists.
        """
        mappings_backup_dir = backup_path / "mappings"
        mappings_backup_dir.mkdir(parents=True, exist_ok=True)

        backed_up = []
        reference_dir = self.data_root / "reference"

        if reference_dir.exists():
            for m_file in reference_dir.rglob("government_manual_mapping.csv"):
                rel_path = m_file.relative_to(self.data_root)
                target_path = mappings_backup_dir / rel_path
                target_path.parent.mkdir(parents=True, exist_ok=True)

                try:
                    shutil.copy2(m_file, target_path)
                    backed_up.append({"path": f"mappings/{rel_path.as_posix()}", "size_bytes": m_file.stat().st_size})
                except Exception as e:
                    logger.warning(f"Could not backup {m_file}: {e}")

        return backed_up

    def _generate_manifest(
        self, backup_path: pathlib.Path, parquet_info: List[dict[str, Any]], mapping_info: List[dict[str, Any]]
    ) -> None:
        """
        Generates a manifest.json file describing backed up files.
        """
        manifest_data = {"timestamp": backup_path.name, "files": parquet_info, "mapping_files": mapping_info}

        manifest_path = backup_path / "manifest.json"
        with open(manifest_path, "w", encoding="utf-8") as f:
            json.dump(manifest_data, f, indent=2)
