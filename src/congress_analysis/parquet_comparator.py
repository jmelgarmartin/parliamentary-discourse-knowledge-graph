"""
Compares backed up parquet datasets against current parquet datasets and generates comparison reports.
"""

import datetime
import hashlib
import json
import logging
import pathlib
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

logger = logging.getLogger(__name__)


class ParquetComparator:
    """
    Compares backed up parquet datasets against current parquet datasets and generates comparison reports.
    """

    def __init__(self, data_root: str = "data", reports_root: str = "comparison_reports"):
        self.data_root = pathlib.Path(data_root)
        self.reports_root = pathlib.Path(reports_root)

    def compare_backup_to_current(self, backup_path_str: str) -> str:
        """
        Compares backup parquet files against current parquet files and returns the report directory path.
        """
        backup_path = pathlib.Path(backup_path_str)
        if not backup_path.exists():
            logger.error(f"Backup path does not exist: {backup_path}")
            return ""

        timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M%S")
        report_dir = self.reports_root / timestamp
        report_dir.mkdir(parents=True, exist_ok=True)

        backup_parquets = self._discover_backup_parquets(backup_path)
        dataset_results = []

        for b_file in backup_parquets:
            current_file = self._resolve_current_parquet(b_file, backup_path)
            try:
                result = self._compare_dataset(b_file, current_file)
                dataset_results.append(result)
            except Exception as e:
                logger.error(f"Error comparing dataset {b_file.name}: {e}")
                dataset_results.append({"dataset": b_file.name, "error": str(e), "is_identical": False})

        self._generate_summary(report_dir, dataset_results, backup_path.name)

        logger.info(f"Comparison report generated at {report_dir}")
        return str(report_dir)

    def _discover_backup_parquets(self, backup_path: pathlib.Path) -> List[pathlib.Path]:
        """
        Returns parquet files found inside the backup parquet directory.
        """
        parquet_dir = backup_path / "parquet"
        if not parquet_dir.exists():
            return []
        return list(parquet_dir.rglob("*.parquet"))

    def _resolve_current_parquet(self, backup_file: pathlib.Path, backup_root: pathlib.Path) -> Optional[pathlib.Path]:
        """
        Resolves the matching current parquet file for a given backup parquet path.
        Assumes the structure inside backup_root/parquet/ matches the structure inside data_root/.
        """
        try:
            rel_path = backup_file.relative_to(backup_root / "parquet")
            current_file = self.data_root / rel_path
            return current_file if current_file.exists() else None
        except ValueError:
            return None

    def _compare_dataset(self, backup_file: pathlib.Path, current_file: Optional[pathlib.Path]) -> Dict[str, Any]:
        """
        Compares schema, row counts and normalized row hashes for a single dataset.
        """
        result: Dict[str, Any] = {
            "dataset": backup_file.name,
            "backup_path": backup_file.as_posix(),
            "current_exists": current_file is not None,
            "is_identical": False,
        }

        if current_file is None:
            return result

        # Load data
        try:
            backup_cols, backup_hashes = self._load_parquet_as_normalized_rows(backup_file)
            current_cols, current_hashes = self._load_parquet_as_normalized_rows(current_file)
        except Exception as e:
            result["error"] = f"Load error: {e}"
            return result

        # Basic metrics
        result["columns_match"] = sorted(backup_cols) == sorted(current_cols)
        result["backup_row_count"] = len(backup_hashes)
        result["current_row_count"] = len(current_hashes)

        # Content comparison using sets of hashes
        backup_set = set(backup_hashes)
        current_set = set(current_hashes)

        common = backup_set.intersection(current_set)
        only_backup = backup_set - current_set
        only_current = current_set - backup_set

        result["common_row_hashes"] = len(common)
        result["only_in_backup"] = len(only_backup)
        result["only_in_current"] = len(only_current)

        # Identity check: schemas must match AND all rows must be identical
        result["is_identical"] = result["columns_match"] and len(only_backup) == 0 and len(only_current) == 0

        return result

    def _load_parquet_as_normalized_rows(self, parquet_file: pathlib.Path) -> Tuple[List[str], List[str]]:
        """
        Loads a parquet file and returns normalized column names and row hashes.
        Uses pandas for stable normalization.
        """
        df = pd.read_parquet(parquet_file)

        # Normalize columns: sorted list
        cols = sorted(df.columns.tolist())
        df = df[cols]

        # Row normalization:
        # 1. Fill NaNs with a unique string to ensure consistent hashing
        # 2. Convert all to strings for stability
        # 3. Create a combined string per row and hash it
        df_str = df.fillna("__NULL__").astype(str)

        # Vectorized hashing for performance
        row_strings = df_str.apply(lambda x: "|".join(x), axis=1)
        hashes = row_strings.apply(lambda x: hashlib.md5(x.encode("utf-8")).hexdigest()).tolist()

        return cols, hashes

    def _generate_summary(
        self, report_dir: pathlib.Path, dataset_results: List[Dict[str, Any]], backup_timestamp: str
    ) -> None:
        """
        Generates summary.json for the comparison.
        """
        summary = {
            "backup_timestamp": backup_timestamp,
            "generated_at": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "datasets": dataset_results,
        }

        with open(report_dir / "summary.json", "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=2)
