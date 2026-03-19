import os
import sys
import unittest
from typing import Any, List, Tuple
from unittest.mock import MagicMock, mock_open, patch

import pandas as pd

# Add src to path to import main
sys.path.append(os.path.join(os.getcwd(), "src"))
import main  # noqa: E402


class TestMainStreaming(unittest.TestCase):
    def _get_mock_df(self, name_col: str = "name") -> pd.DataFrame:
        return pd.DataFrame(
            {
                "deputy_id": ["1"],
                name_col: ["D1"],
                "substitutes": [""],
                "substituted_by": [""],
                "start_date": ["2024-01-01"],
                "end_date": ["2024-12-31"],
            }
        )

    @patch("main.SessionsScraper")
    @patch("main.InterventionsExtractor")
    @patch("main.pd.read_parquet")
    @patch("main.pd.DataFrame.to_parquet")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("main.setup_logging")
    @patch("main.BackupManager")
    def test_main_with_streaming_flag(
        self,
        mock_backup: MagicMock,
        mock_logging: MagicMock,
        mock_args: MagicMock,
        mock_to_parquet: MagicMock,
        mock_read_parquet: MagicMock,
        mock_extractor: MagicMock,
        mock_scraper: MagicMock,
    ) -> None:
        """Test that the experimental streaming flag correctly wires the callback and matches parity."""
        # Setup mocks
        mock_args.return_value = MagicMock(
            term="15",
            driver_path=None,
            state_path="state/bronze.duckdb",
            log_level="INFO",
            headless=True,
            experimental_streaming=True,
        )

        # Mock scraper to return dummy data and trigger callback via side_effect
        mock_scraper_inst = mock_scraper.return_value

        def scraper_run_side_effect(*args: Any, **kwargs: Any) -> Tuple[Any, List[str]]:
            callback = kwargs.get("content_callback")
            if callback:
                callback("doc1", "<html>content</html>")
            return (MagicMock(), ["test.html"])

        mock_scraper_inst.run.side_effect = scraper_run_side_effect

        # Mock extractor to return a matching document_id for parity check
        mock_extractor_inst = mock_extractor.return_value
        mock_extractor_inst.extract_from_content.return_value = [{"document_id": "doc1", "intervention": "test"}]
        mock_extractor_inst.run.return_value = pd.DataFrame([{"document_id": "doc1", "intervention": "test"}])

        # Mock read_parquet to return robust DataFrames for all phases
        def mock_read_side_effect(path: Any, **kwargs: Any) -> pd.DataFrame:
            path_str = str(path)
            if "substitutions.parquet" in path_str:
                return self._get_mock_df()
            if "deputies.parquet" in path_str:
                return self._get_mock_df()
            if "groups.parquet" in path_str:
                return pd.DataFrame({"name": ["G1"]})
            if "sessions.parquet" in path_str:
                return pd.DataFrame({"document_id": ["S1"]})
            return pd.DataFrame()

        mock_read_parquet.side_effect = mock_read_side_effect

        with patch("main.GroupsScraper"), patch("main.DeputiesScraper"), patch(
            "main.SubstitutionsEnricher",
            return_value=MagicMock(enrich=MagicMock(return_value=(pd.DataFrame(), pd.DataFrame()))),
        ), patch("main.run_interventions_enrichment"), patch("pathlib.Path.mkdir"), patch(
            "pathlib.Path.exists", return_value=True
        ), patch("builtins.open", mock_open()):
            main.main()
            mock_extractor_inst.extract_from_content.assert_called_once_with(
                "<html>content</html>", "doc1", "doc1.html"
            )

    @patch("main.SessionsScraper")
    @patch("main.InterventionsExtractor")
    @patch("main.pd.read_parquet")
    @patch("main.pd.DataFrame.to_parquet")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("main.setup_logging")
    @patch("main.BackupManager")
    def test_main_with_document_mismatch(
        self,
        mock_backup: MagicMock,
        mock_logging: MagicMock,
        mock_args: MagicMock,
        mock_to_parquet: MagicMock,
        mock_read_parquet: MagicMock,
        mock_extractor: MagicMock,
        mock_scraper: MagicMock,
    ) -> None:
        """Test global match but document-level mismatch (cross-document count errors)."""
        mock_args.return_value = MagicMock(
            term="15",
            driver_path=None,
            state_path="state/bronze.duckdb",
            log_level="INFO",
            headless=True,
            experimental_streaming=True,
        )

        # Scraper triggers callback for TWO documents
        mock_scraper_inst = mock_scraper.return_value

        def scraper_run_side_effect(*args: Any, **kwargs: Any) -> Tuple[Any, List[str]]:
            callback = kwargs.get("content_callback")
            if callback:
                callback("doc1", "<html>c1</html>")
                callback("doc2", "<html>c2</html>")
            return (MagicMock(), ["test1.html", "test2.html"])

        mock_scraper_inst.run.side_effect = scraper_run_side_effect

        # Streaming side: doc1 has 2 rows, doc2 has 0 rows (Total 2)
        mock_extractor_inst = mock_extractor.return_value
        mock_extractor_inst.extract_from_content.side_effect = [
            [{"document_id": "doc1", "int": "1"}, {"document_id": "doc1", "int": "2"}],
            [],
        ]

        # Batch side: doc1 has 1 row, doc2 has 1 row (Total 2)
        # Global match (2 vs 2), but Doc-Level mismatch!
        mock_extractor_inst.run.return_value = pd.DataFrame(
            [{"document_id": "doc1", "int": "A"}, {"document_id": "doc2", "int": "B"}]
        )

        mock_read_parquet.return_value = self._get_mock_df()

        with patch("main.GroupsScraper"), patch("main.DeputiesScraper"), patch(
            "main.SubstitutionsEnricher",
            return_value=MagicMock(enrich=MagicMock(return_value=(pd.DataFrame(), pd.DataFrame()))),
        ), patch("main.run_interventions_enrichment"), patch("pathlib.Path.mkdir"), patch(
            "pathlib.Path.exists", return_value=True
        ), patch("builtins.open", mock_open()):
            main.main()

    @patch("main.SessionsScraper")
    @patch("main.InterventionsExtractor")
    @patch("main.pd.read_parquet")
    @patch("main.pd.DataFrame.to_parquet")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("main.setup_logging")
    @patch("main.BackupManager")
    def test_main_with_mismatch_diagnosis(
        self,
        mock_backup: MagicMock,
        mock_logging: MagicMock,
        mock_args: MagicMock,
        mock_to_parquet: MagicMock,
        mock_read_parquet: MagicMock,
        mock_extractor: MagicMock,
        mock_scraper: MagicMock,
    ) -> None:
        """Test that mismatch diagnosis correctly identifies missing vs extra rows."""
        mock_args.return_value = MagicMock(
            term="15",
            driver_path=None,
            state_path="state/bronze.duckdb",
            log_level="INFO",
            headless=True,
            experimental_streaming=True,
        )

        mock_scraper_inst = mock_scraper.return_value

        def scraper_run_side_effect(*args: Any, **kwargs: Any) -> Tuple[Any, List[str]]:
            callback = kwargs.get("content_callback")
            if callback:
                callback("docA", "<html>A</html>")
                callback("docB", "<html>B</html>")
            return (MagicMock(), ["testA.html", "testB.html"])

        mock_scraper_inst.run.side_effect = scraper_run_side_effect

        # docA: Streaming missing 1 row (Batch=2, Stream=1)
        # docB: Streaming extra 1 row (Batch=1, Stream=2)
        mock_extractor_inst = mock_extractor.return_value
        mock_extractor_inst.extract_from_content.side_effect = [
            [{"document_id": "docA", "int": "1"}],  # Stream A (1)
            [{"document_id": "docB", "int": "1"}, {"document_id": "docB", "int": "2"}],  # Stream B (2)
        ]

        # Batch results
        mock_extractor_inst.run.return_value = pd.DataFrame(
            [
                {"document_id": "docA", "int": "A1"},
                {"document_id": "docA", "int": "A2"},
                {"document_id": "docB", "int": "B1"},
            ]
        )

        mock_read_parquet.return_value = self._get_mock_df()

        with patch("main.GroupsScraper"), patch("main.DeputiesScraper"), patch(
            "main.SubstitutionsEnricher",
            return_value=MagicMock(enrich=MagicMock(return_value=(pd.DataFrame(), pd.DataFrame()))),
        ), patch("main.run_interventions_enrichment"), patch("pathlib.Path.mkdir"), patch(
            "pathlib.Path.exists", return_value=True
        ), patch("builtins.open", mock_open()):
            main.main()

    @patch("main.SessionsScraper")
    @patch("main.pd.read_parquet")
    @patch("main.argparse.ArgumentParser.parse_args")
    def test_main_with_no_new_files(
        self, mock_args: MagicMock, mock_read_parquet: MagicMock, mock_scraper: MagicMock
    ) -> None:
        """Test that parity validation is SKIPPED when no new files are detected."""
        mock_args.return_value = MagicMock(
            term="15",
            driver_path=None,
            state_path="state/bronze.duckdb",
            log_level="INFO",
            headless=True,
            experimental_streaming=True,
        )
        mock_scraper_inst = mock_scraper.return_value
        mock_scraper_inst.run.return_value = (MagicMock(), [])

        # Consistent DataFrames for audit
        mock_read_parquet.return_value = self._get_mock_df()

        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch(
            "main.SubstitutionsEnricher",
            return_value=MagicMock(enrich=MagicMock(return_value=(pd.DataFrame(), pd.DataFrame()))),
        ), patch("pathlib.Path.exists", return_value=True), patch("builtins.open", mock_open()):
            main.main()

    @patch("main.SessionsScraper")
    @patch("main.pd.read_parquet")
    @patch("main.argparse.ArgumentParser.parse_args")
    def test_main_without_streaming_flag(
        self, mock_args: MagicMock, mock_read_parquet: MagicMock, mock_scraper: MagicMock
    ) -> None:
        """Test that the callback is NOT wired if the flag is missing."""
        mock_args.return_value = MagicMock(
            term="15",
            driver_path=None,
            state_path="state/bronze.duckdb",
            log_level="INFO",
            headless=True,
            experimental_streaming=False,
        )
        mock_read_parquet.return_value = self._get_mock_df()
        mock_scraper_inst = mock_scraper.return_value
        mock_scraper_inst.run.side_effect = None
        mock_scraper_inst.run.return_value = (MagicMock(), [])

        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch(
            "main.SubstitutionsEnricher",
            return_value=MagicMock(enrich=MagicMock(return_value=(pd.DataFrame(), pd.DataFrame()))),
        ), patch("pathlib.Path.exists", return_value=True):
            main.main()
            args, kwargs = mock_scraper_inst.run.call_args
            self.assertIsNone(kwargs.get("content_callback"))


if __name__ == "__main__":
    unittest.main()
