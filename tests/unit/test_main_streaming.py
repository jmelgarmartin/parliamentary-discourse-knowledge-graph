import os
import sys
import unittest
from unittest.mock import MagicMock, patch

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

    def _get_enricher_mock(self) -> MagicMock:
        mock = MagicMock()
        mock.enrich.return_value = (pd.DataFrame(), pd.DataFrame())
        return mock

    # --- Phase 11: Confidence Metrics Tests ---

    @patch("main.SessionsScraper")
    @patch("main.InterventionsExtractor")
    @patch("main.pd.read_parquet")
    @patch("main.pd.DataFrame.to_parquet")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("json.dump")
    def test_main_reports_full_match_confidence(
        self,
        mock_json_dump: MagicMock,
        mock_args: MagicMock,
        mock_to_parquet: MagicMock,
        mock_read_parquet: MagicMock,
        mock_extractor: MagicMock,
        mock_scraper: MagicMock,
    ) -> None:
        """Verify 1.0 confidence and FULL_MATCH on perfect parity."""
        mock_args.return_value = MagicMock(
            term="15",
            experimental_streaming=True,
            use_streaming_candidate=False,
            streaming_confidence_threshold=None,
            log_level="INFO",
            headless=True,
            state_path="s",
            driver_path=None,
        )
        mock_scraper_inst = mock_scraper.return_value
        data = [{"document_id": "d1", "intervention_id": "id1", "intervention_order": 0}]
        mock_extractor_inst = mock_extractor.return_value
        mock_extractor_inst.extract_from_content.return_value = data
        mock_extractor_inst.run.return_value = pd.DataFrame(data)
        mock_scraper_inst.run.side_effect = lambda *a, **kw: (kw.get("content_callback")("d1", "h"), ["f1"])
        mock_read_parquet.return_value = self._get_mock_df()

        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "main.run_interventions_enrichment"
        ), patch("pathlib.Path.mkdir"), patch("pathlib.Path.exists", return_value=True), patch("builtins.open"):
            main.main()

            report = mock_json_dump.call_args[0][0]
            self.assertEqual(report["confidence_level"], "FULL_MATCH")
            self.assertEqual(report["confidence_score"], 1.0)

    @patch("main.SessionsScraper")
    @patch("main.InterventionsExtractor")
    @patch("main.pd.read_parquet")
    @patch("main.pd.DataFrame.to_parquet")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("json.dump")
    def test_main_reports_partial_confidence_on_mismatch(
        self,
        mock_json_dump: MagicMock,
        mock_args: MagicMock,
        mock_to_parquet: MagicMock,
        mock_read_parquet: MagicMock,
        mock_extractor: MagicMock,
        mock_scraper: MagicMock,
    ) -> None:
        """Verify reduced confidence score on row-identity mismatch."""
        mock_args.return_value = MagicMock(
            term="15",
            experimental_streaming=True,
            use_streaming_candidate=False,
            streaming_confidence_threshold=None,
            log_level="INFO",
            headless=True,
            state_path="s",
            driver_path=None,
        )
        mock_scraper_inst = mock_scraper.return_value
        # Streaming has 2 rows, Batch has 1 (MISMATCH)
        mock_extractor_inst = mock_extractor.return_value
        mock_extractor_inst.extract_from_content.return_value = [
            {"document_id": "d1", "intervention_id": "id1", "intervention_order": 0},
            {"document_id": "d1", "intervention_id": "id2_stream", "intervention_order": 1},
        ]
        mock_extractor_inst.run.return_value = pd.DataFrame(
            [{"document_id": "d1", "intervention_id": "id1", "intervention_order": 0}]
        )
        mock_read_parquet.return_value = self._get_mock_df()
        mock_scraper_inst.run.side_effect = lambda *a, **kw: (kw.get("content_callback")("d1", "h"), ["f1"])

        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "main.run_interventions_enrichment"
        ), patch("pathlib.Path.mkdir"), patch("pathlib.Path.exists", return_value=True), patch("builtins.open"):
            main.main()

            report = mock_json_dump.call_args[0][0]
            self.assertIn(report["confidence_level"], ["HIGH_CONFIDENCE", "PARTIAL_MATCH", "LOW_CONFIDENCE"])
            self.assertLess(report["confidence_score"], 1.0)

    @patch("main.SessionsScraper")
    @patch("main.pd.read_parquet")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("json.dump")
    def test_main_reports_skipped_confidence(
        self, mock_json_dump: MagicMock, mock_args: MagicMock, mock_read_parquet: MagicMock, mock_scraper: MagicMock
    ) -> None:
        """Verify 0.0 confidence and SKIPPED status when no files processed."""
        mock_args.return_value = MagicMock(
            term="15",
            experimental_streaming=True,
            use_streaming_candidate=False,
            streaming_confidence_threshold=None,
            log_level="INFO",
            headless=True,
            state_path="s",
            driver_path=None,
        )
        mock_scraper_inst = mock_scraper.return_value
        mock_scraper_inst.run.return_value = (MagicMock(), [])  # No new files
        mock_read_parquet.return_value = self._get_mock_df()

        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "main.run_interventions_enrichment"
        ), patch("pathlib.Path.mkdir"), patch("pathlib.Path.exists", return_value=True), patch("builtins.open"):
            main.main()

            report = mock_json_dump.call_args[0][0]
            self.assertEqual(report["confidence_level"], "SKIPPED")
            self.assertEqual(report["confidence_score"], 0.0)

    # --- Phase 12: Confidence-Gated Promotion Tests ---

    @patch("main.SessionsScraper")
    @patch("main.InterventionsExtractor")
    @patch("main.pd.read_parquet")
    @patch("main.pd.DataFrame.to_parquet")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("json.dump")
    def test_main_keeps_strict_behavior_without_threshold(
        self,
        mock_json_dump: MagicMock,
        mock_args: MagicMock,
        mock_to_parquet: MagicMock,
        mock_read_parquet: MagicMock,
        mock_extractor: MagicMock,
        mock_scraper: MagicMock,
    ) -> None:
        """Verify that without threshold, strict MATCH is required (Phase 10 behavior kept)."""
        mock_args.return_value = MagicMock(
            term="15",
            experimental_streaming=True,
            use_streaming_candidate=True,
            streaming_confidence_threshold=None,
            log_level="INFO",
            headless=True,
            state_path="s",
            driver_path=None,
        )
        mock_scraper_inst = mock_scraper.return_value
        mock_scraper_inst.run.side_effect = lambda *a, **kw: (kw.get("content_callback")("d1", "h"), ["f1"])

        mock_extractor_inst = mock_extractor.return_value
        mock_extractor_inst.extract_from_content.return_value = [
            {"document_id": "d1", "intervention_id": "id1", "intervention_order": 0},
            {"document_id": "d1", "intervention_id": "id2_stream", "intervention_order": 1},
        ]
        mock_extractor_inst.run.return_value = pd.DataFrame(
            [{"document_id": "d1", "intervention_id": "id1", "intervention_order": 0}]
        )
        mock_read_parquet.return_value = self._get_mock_df()

        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "main.run_interventions_enrichment"
        ) as mock_run_enrich, patch("pathlib.Path.mkdir"), patch("pathlib.Path.exists", return_value=True), patch(
            "builtins.open"
        ):
            main.main()

            # Should fall back to batch because strict match failed
            expected_batch = os.path.normpath("data/silver/interventions/legislature=15/interventions_raw.parquet")
            mock_run_enrich.assert_called_once_with("15", expected_batch, None)

            report = mock_json_dump.call_args[0][0]
            self.assertEqual(report["selected_source"], "OFFICIAL_BATCH")

    @patch("main.SessionsScraper")
    @patch("main.InterventionsExtractor")
    @patch("main.pd.read_parquet")
    @patch("main.pd.DataFrame.to_parquet")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("json.dump")
    def test_main_uses_candidate_when_confidence_exceeds_threshold(
        self,
        mock_json_dump: MagicMock,
        mock_args: MagicMock,
        mock_to_parquet: MagicMock,
        mock_read_parquet: MagicMock,
        mock_extractor: MagicMock,
        mock_scraper: MagicMock,
    ) -> None:
        """Verify candidate promotion when confidence score >= threshold."""
        threshold = 0.95
        mock_args.return_value = MagicMock(
            term="15",
            experimental_streaming=True,
            use_streaming_candidate=True,
            streaming_confidence_threshold=threshold,
            log_level="INFO",
            headless=True,
            state_path="s",
            driver_path=None,
        )
        mock_scraper_inst = mock_scraper.return_value
        mock_scraper_inst.run.side_effect = lambda *a, **kw: (kw.get("content_callback")("d1", "h"), ["f1"])

        data = [{"document_id": "d1", "intervention_id": "id1", "intervention_order": 0}]
        mock_extractor_inst = mock_extractor.return_value
        mock_extractor_inst.extract_from_content.return_value = data
        mock_extractor_inst.run.return_value = pd.DataFrame(data)
        mock_read_parquet.return_value = self._get_mock_df()

        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "main.run_interventions_enrichment"
        ) as mock_run_enrich, patch("pathlib.Path.mkdir"), patch("pathlib.Path.exists", return_value=True), patch(
            "builtins.open"
        ):
            main.main()

            expected_candidate = os.path.normpath(
                "data/validation/legislature=15/interventions_streaming_candidate.parquet"
            )
            mock_run_enrich.assert_called_once_with("15", expected_candidate, None)

            report = mock_json_dump.call_args[0][0]
            self.assertEqual(report["policy_used"], "confidence_threshold")
            self.assertIn("interventions_streaming_candidate", report["selected_source"])

    @patch("main.SessionsScraper")
    @patch("main.InterventionsExtractor")
    @patch("main.pd.read_parquet")
    @patch("main.pd.DataFrame.to_parquet")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("json.dump")
    def test_main_falls_back_when_confidence_below_threshold(
        self,
        mock_json_dump: MagicMock,
        mock_args: MagicMock,
        mock_to_parquet: MagicMock,
        mock_read_parquet: MagicMock,
        mock_extractor: MagicMock,
        mock_scraper: MagicMock,
    ) -> None:
        """Verify fallback when score < threshold."""
        mock_args.return_value = MagicMock(
            term="15",
            experimental_streaming=True,
            use_streaming_candidate=True,
            streaming_confidence_threshold=0.99,
            log_level="INFO",
            headless=True,
            state_path="s",
            driver_path=None,
        )
        mock_scraper_inst = mock_scraper.return_value
        mock_extractor_inst = mock_extractor.return_value
        mock_extractor_inst.extract_from_content.return_value = [
            {"document_id": "d1", "intervention_id": "id_stream", "intervention_order": 0}
        ]
        mock_extractor_inst.run.return_value = pd.DataFrame(
            [{"document_id": "d1", "intervention_id": "id_batch", "intervention_order": 0}]
        )
        mock_read_parquet.return_value = self._get_mock_df()
        mock_scraper_inst.run.side_effect = lambda *a, **kw: (kw.get("content_callback")("d1", "h"), ["f1"])

        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "main.run_interventions_enrichment"
        ) as mock_run_enrich, patch("pathlib.Path.mkdir"), patch("pathlib.Path.exists", return_value=True), patch(
            "builtins.open"
        ):
            main.main()

            expected_batch = os.path.normpath("data/silver/interventions/legislature=15/interventions_raw.parquet")
            mock_run_enrich.assert_called_once_with("15", expected_batch, None)

    @patch("main.sys.exit")
    @patch("main.argparse.ArgumentParser.parse_args")
    def test_main_fails_on_invalid_threshold(self, mock_args: MagicMock, mock_exit: MagicMock) -> None:
        """Verify early exit for threshold outside [0, 1]."""
        mock_args.return_value = MagicMock(
            term="15",
            streaming_confidence_threshold=1.5,
            log_level="INFO",
            headless=True,
            state_path="s",
            driver_path=None,
        )
        mock_exit.side_effect = SystemExit(1)
        with patch("main.setup_logging"), patch("main.BackupManager"):
            with self.assertRaises(SystemExit):
                main.main()
        mock_exit.assert_called_with(1)

    @patch("main.SessionsScraper")
    @patch("main.InterventionsExtractor")
    @patch("main.pd.DataFrame.to_parquet")
    @patch("main.pd.read_parquet")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("main.run_interventions_enrichment")
    def test_main_uses_batch_when_no_streaming_flag(
        self,
        mock_run_enrich: MagicMock,
        mock_args: MagicMock,
        mock_read_parquet: MagicMock,
        mock_to_parquet: MagicMock,
        mock_extractor: MagicMock,
        mock_scraper: MagicMock,
    ) -> None:
        """Verify always batch when --use-streaming-candidate is False."""
        mock_args.return_value = MagicMock(
            term="15",
            experimental_streaming=True,
            use_streaming_candidate=False,
            streaming_confidence_threshold=None,
            log_level="INFO",
            headless=True,
            state_path="s",
            driver_path=None,
        )
        mock_read_parquet.return_value = self._get_mock_df()
        data = [{"document_id": "d1", "intervention_id": "id1", "intervention_order": 0}]
        mock_extractor_inst = mock_extractor.return_value
        mock_extractor_inst.extract_from_content.return_value = data
        mock_extractor_inst.run.return_value = pd.DataFrame(data)
        mock_scraper.return_value.run.side_effect = lambda *a, **kw: (
            kw.get("content_callback")("d1", "h") if kw.get("content_callback") else None,
            ["f1"],
        )

        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "pathlib.Path.exists", return_value=True
        ), patch("builtins.open"), patch("json.dump"):
            main.main()
            expected_batch = os.path.normpath("data/silver/interventions/legislature=15/interventions_raw.parquet")
            mock_run_enrich.assert_called_once_with("15", expected_batch, None)


if __name__ == "__main__":
    unittest.main()
