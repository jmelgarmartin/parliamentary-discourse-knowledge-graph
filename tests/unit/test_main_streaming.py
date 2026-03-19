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
        mock_args.return_value = MagicMock(
            term="15",
            driver_path=None,
            state_path="state/bronze.duckdb",
            log_level="INFO",
            headless=True,
            experimental_streaming=True,
            use_streaming_candidate=False,
        )

        mock_scraper_inst = mock_scraper.return_value
        mock_scraper_inst.run.side_effect = lambda *a, **kw: (
            kw.get("content_callback")("doc1", "<html>c</html>"),
            ["t.html"],
        )

        mock_extractor_inst = mock_extractor.return_value
        mock_extractor_inst.extract_from_content.return_value = [
            {"document_id": "doc1", "intervention": "t", "intervention_id": "id1", "intervention_order": 0}
        ]
        mock_extractor_inst.run.return_value = pd.DataFrame(
            [{"document_id": "doc1", "intervention": "t", "intervention_id": "id1", "intervention_order": 0}]
        )

        mock_read_parquet.side_effect = lambda path, **kw: self._get_mock_df()

        with patch("main.GroupsScraper"), patch("main.DeputiesScraper"), patch(
            "main.SubstitutionsEnricher", return_value=self._get_enricher_mock()
        ), patch("main.run_interventions_enrichment"), patch("pathlib.Path.mkdir"), patch(
            "pathlib.Path.exists", return_value=True
        ), patch("builtins.open"), patch("json.dump"):
            main.main()

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
        """Verify metrics for perfect match (1.0 confidence, FULL_MATCH)."""
        mock_args.return_value = MagicMock(
            term="15",
            experimental_streaming=True,
            use_streaming_candidate=False,
            log_level="INFO",
            headless=True,
            state_path="s",
            driver_path=None,
        )
        mock_scraper_inst = mock_scraper.return_value
        mock_scraper_inst.run.side_effect = lambda *a, **kw: (kw.get("content_callback")("d1", "h"), ["f1"])

        data = [{"document_id": "d1", "intervention_id": "id1", "intervention_order": 0}]
        mock_extractor_inst = mock_extractor.return_value
        mock_extractor_inst.run.return_value = pd.DataFrame(data)
        mock_extractor_inst.extract_from_content.return_value = data
        mock_read_parquet.return_value = self._get_mock_df()

        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "main.run_interventions_enrichment"
        ), patch("pathlib.Path.mkdir"), patch("pathlib.Path.exists", return_value=True), patch("builtins.open"):
            main.main()

            self.assertTrue(mock_json_dump.called)
            report = mock_json_dump.call_args[0][0]
            self.assertEqual(report["confidence_level"], "FULL_MATCH")
            self.assertEqual(report["confidence_score"], 1.0)
            self.assertEqual(report["global_match_ratio"], 1.0)
            self.assertEqual(report["document_match_ratio"], 1.0)
            self.assertEqual(report["row_identity_match_ratio"], 1.0)

    @patch("main.SessionsScraper")
    @patch("main.InterventionsExtractor")
    @patch("main.pd.read_parquet")
    @patch("main.pd.DataFrame.to_parquet")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("json.dump")
    def test_main_reports_partial_confidence_on_document_or_row_mismatch(
        self,
        mock_json_dump: MagicMock,
        mock_args: MagicMock,
        mock_to_parquet: MagicMock,
        mock_read_parquet: MagicMock,
        mock_extractor: MagicMock,
        mock_scraper: MagicMock,
    ) -> None:
        """Verify reduced confidence score when rows differ (identity mismatch)."""
        mock_args.return_value = MagicMock(
            term="15",
            experimental_streaming=True,
            use_streaming_candidate=False,
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
            [
                {"document_id": "d1", "intervention_id": "id1", "intervention_order": 0},
                {"document_id": "d1", "intervention_id": "id2_batch", "intervention_order": 1},
            ]
        )
        mock_read_parquet.return_value = self._get_mock_df()

        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "main.run_interventions_enrichment"
        ), patch("pathlib.Path.mkdir"), patch("pathlib.Path.exists", return_value=True), patch("builtins.open"):
            main.main()

            report = mock_json_dump.call_args[0][0]
            self.assertLess(report["confidence_score"], 1.0)
            self.assertEqual(report["confidence_level"], "LOW_CONFIDENCE")
            self.assertEqual(report["row_identity_match_ratio"], round(1 / 3, 4))

    @patch("main.SessionsScraper")
    @patch("main.pd.read_parquet")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("json.dump")
    def test_main_reports_skipped_confidence(
        self, mock_json_dump: MagicMock, mock_args: MagicMock, mock_read_parquet: MagicMock, mock_scraper: MagicMock
    ) -> None:
        """Verify skipped status and zero metrics when no files are processed."""
        mock_args.return_value = MagicMock(
            term="15",
            experimental_streaming=True,
            use_streaming_candidate=False,
            log_level="INFO",
            headless=True,
            state_path="s",
            driver_path=None,
        )
        mock_scraper_inst = mock_scraper.return_value
        mock_scraper_inst.run.return_value = (MagicMock(), [])

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

    @patch("main.SessionsScraper")
    @patch("main.InterventionsExtractor")
    @patch("main.pd.read_parquet")
    @patch("main.pd.DataFrame.to_parquet")
    @patch("main.argparse.ArgumentParser.parse_args")
    def test_main_persists_streaming_candidate_output(
        self,
        mock_args: MagicMock,
        mock_to_parquet: MagicMock,
        mock_read_parquet: MagicMock,
        mock_extractor: MagicMock,
        mock_scraper: MagicMock,
    ) -> None:
        """Verify that interventions_streaming_candidate.parquet is persisted when records exist."""
        mock_args.return_value = MagicMock(
            term="15",
            experimental_streaming=True,
            use_streaming_candidate=False,
            log_level="INFO",
            headless=True,
            state_path="s",
            driver_path=None,
        )

        mock_scraper_inst = mock_scraper.return_value
        mock_scraper_inst.run.side_effect = lambda *a, **kw: (kw.get("content_callback")("d1", "h"), ["f1"])

        data = [{"document_id": "d1", "intervention_order": 0, "intervention_id": "id1"}]
        mock_extractor_inst = mock_extractor.return_value
        mock_extractor_inst.extract_from_content.return_value = data
        mock_extractor_inst.run.return_value = pd.DataFrame(data)

        mock_read_parquet.return_value = self._get_mock_df()

        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "main.run_interventions_enrichment"
        ), patch("pathlib.Path.mkdir"), patch("pathlib.Path.exists", return_value=True), patch("builtins.open"), patch(
            "json.dump"
        ):
            main.main()

            candidate_calls = [
                call
                for call in mock_to_parquet.call_args_list
                if "interventions_streaming_candidate.parquet" in str(call[0][0])
            ]
            self.assertTrue(len(candidate_calls) > 0, "Candidate parquet was not persisted.")

    @patch("main.SessionsScraper")
    @patch("main.InterventionsExtractor")
    @patch("main.pd.read_parquet")
    @patch("main.pd.DataFrame.to_parquet")
    @patch("main.argparse.ArgumentParser.parse_args")
    def test_main_does_not_persist_candidate_when_no_streaming_records(
        self,
        mock_args: MagicMock,
        mock_to_parquet: MagicMock,
        mock_read_parquet: MagicMock,
        mock_extractor: MagicMock,
        mock_scraper: MagicMock,
    ) -> None:
        """Verify that candidate parquet is NOT persisted if no records are collected."""
        mock_args.return_value = MagicMock(
            term="15",
            experimental_streaming=True,
            use_streaming_candidate=False,
            log_level="INFO",
            headless=True,
            state_path="s",
            driver_path=None,
        )

        mock_scraper_inst = mock_scraper.return_value
        mock_scraper_inst.run.return_value = (MagicMock(), ["f1"])

        mock_extractor_inst = mock_extractor.return_value
        mock_extractor_inst.run.return_value = pd.DataFrame(
            [{"document_id": "d1", "intervention_order": 0, "intervention_id": "id1"}]
        )

        mock_read_parquet.return_value = self._get_mock_df()

        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "main.run_interventions_enrichment"
        ), patch("pathlib.Path.mkdir"), patch("pathlib.Path.exists", return_value=True), patch("builtins.open"), patch(
            "json.dump"
        ):
            main.main()

            candidate_calls = [
                call
                for call in mock_to_parquet.call_args_list
                if "interventions_streaming_candidate.parquet" in str(call[0][0])
            ]
            self.assertEqual(len(candidate_calls), 0, "Candidate parquet was persisted unexpectedly.")

    @patch("main.SessionsScraper")
    @patch("main.InterventionsExtractor")
    @patch("main.pd.read_parquet")
    @patch("main.pd.DataFrame.to_parquet")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("main.run_interventions_enrichment")
    def test_main_uses_batch_by_default(
        self,
        mock_run_enrich: MagicMock,
        mock_args: MagicMock,
        mock_to_parquet: MagicMock,
        mock_read_parquet: MagicMock,
        mock_extractor: MagicMock,
        mock_scraper: MagicMock,
    ) -> None:
        """Verify that without --use-streaming-candidate, the pipeline uses the batch path."""
        mock_args.return_value = MagicMock(
            term="15",
            experimental_streaming=True,
            use_streaming_candidate=False,
            log_level="INFO",
            headless=True,
            state_path="s",
            driver_path=None,
        )

        mock_scraper_inst = mock_scraper.return_value
        mock_scraper_inst.run.side_effect = lambda *a, **kw: (kw.get("content_callback")("d1", "h"), ["f1"])

        data = [{"document_id": "d1", "intervention_order": 0, "intervention_id": "id1"}]
        mock_extractor_inst = mock_extractor.return_value
        mock_extractor_inst.run.return_value = pd.DataFrame(data)
        mock_extractor_inst.extract_from_content.return_value = data

        mock_read_parquet.return_value = self._get_mock_df()

        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "pathlib.Path.mkdir"
        ), patch("pathlib.Path.exists", return_value=True), patch("builtins.open"), patch("json.dump"):
            main.main()

            expected_batch_path = "data/silver/interventions/legislature=15/interventions_raw.parquet"
            mock_run_enrich.assert_called_once_with("15", expected_batch_path, None)

    @patch("main.SessionsScraper")
    @patch("main.InterventionsExtractor")
    @patch("main.pd.read_parquet")
    @patch("main.pd.DataFrame.to_parquet")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("main.run_interventions_enrichment")
    def test_main_uses_streaming_candidate_when_validation_matches(
        self,
        mock_run_enrich: MagicMock,
        mock_args: MagicMock,
        mock_to_parquet: MagicMock,
        mock_read_parquet: MagicMock,
        mock_extractor: MagicMock,
        mock_scraper: MagicMock,
    ) -> None:
        """Verify that with matching validation, the candidate source is selected."""
        mock_args.return_value = MagicMock(
            term="15",
            experimental_streaming=True,
            use_streaming_candidate=True,
            log_level="INFO",
            headless=True,
            state_path="s",
            driver_path=None,
        )

        mock_scraper_inst = mock_scraper.return_value
        mock_scraper_inst.run.side_effect = lambda *a, **kw: (kw.get("content_callback")("d1", "h"), ["f1"])

        data = [{"document_id": "d1", "intervention_order": 0, "intervention_id": "id1"}]
        mock_extractor_inst = mock_extractor.return_value
        mock_extractor_inst.run.return_value = pd.DataFrame(data)
        mock_extractor_inst.extract_from_content.return_value = data

        mock_read_parquet.return_value = self._get_mock_df()

        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "pathlib.Path.mkdir"
        ), patch("pathlib.Path.exists", return_value=True), patch("builtins.open"), patch("json.dump"):
            main.main()

            expected_candidate_path = "data/validation/legislature=15/interventions_streaming_candidate.parquet"
            mock_run_enrich.assert_called_once_with("15", expected_candidate_path, None)

    @patch("main.SessionsScraper")
    @patch("main.InterventionsExtractor")
    @patch("main.pd.read_parquet")
    @patch("main.pd.DataFrame.to_parquet")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("main.run_interventions_enrichment")
    def test_main_falls_back_to_batch_when_validation_fails(
        self,
        mock_run_enrich: MagicMock,
        mock_args: MagicMock,
        mock_to_parquet: MagicMock,
        mock_read_parquet: MagicMock,
        mock_extractor: MagicMock,
        mock_scraper: MagicMock,
    ) -> None:
        """Verify that fallback to batch occurs if validation fails, even with the switch flag."""
        mock_args.return_value = MagicMock(
            term="15",
            experimental_streaming=True,
            use_streaming_candidate=True,
            log_level="INFO",
            headless=True,
            state_path="s",
            driver_path=None,
        )

        mock_scraper_inst = mock_scraper.return_value
        mock_scraper_inst.run.side_effect = lambda *a, **kw: (kw.get("content_callback")("d1", "h"), ["f1"])

        mock_extractor_inst = mock_extractor.return_value
        # Streaming has id_stream
        mock_extractor_inst.extract_from_content.return_value = [
            {"document_id": "d1", "intervention_order": 0, "intervention_id": "id_stream"}
        ]
        # Batch has id_batch (MISMATCH!)
        mock_extractor_inst.run.return_value = pd.DataFrame(
            [{"document_id": "d1", "intervention_order": 0, "intervention_id": "id_batch"}]
        )

        mock_read_parquet.return_value = self._get_mock_df()

        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "pathlib.Path.mkdir"
        ), patch("pathlib.Path.exists", return_value=True), patch("builtins.open"), patch("json.dump"):
            main.main()

            expected_batch_path = "data/silver/interventions/legislature=15/interventions_raw.parquet"
            mock_run_enrich.assert_called_once_with("15", expected_batch_path, None)

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
            experimental_streaming=True,
            use_streaming_candidate=False,
            log_level="INFO",
            headless=True,
            state_path="s",
            driver_path=None,
        )

        mock_scraper_inst = mock_scraper.return_value
        mock_scraper_inst.run.side_effect = lambda *a, **kw: (kw.get("content_callback")("docA", "html"), ["fA"])

        mock_extractor_inst = mock_extractor.return_value
        mock_extractor_inst.extract_from_content.side_effect = [
            [{"document_id": "docA", "intervention_order": 0, "intervention_id": "idA1"}],
        ]

        mock_extractor_inst.run.return_value = pd.DataFrame(
            [
                {"document_id": "docA", "intervention_order": 0, "intervention_id": "idA1"},
                {"document_id": "docA", "intervention_order": 1, "intervention_id": "idA2"},
            ]
        )

        mock_read_parquet.return_value = self._get_mock_df()

        with patch("main.GroupsScraper"), patch("main.DeputiesScraper"), patch(
            "main.SubstitutionsEnricher", return_value=self._get_enricher_mock()
        ), patch("main.run_interventions_enrichment"), patch("pathlib.Path.mkdir"), patch(
            "pathlib.Path.exists", return_value=True
        ), patch("builtins.open"), patch("json.dump"):
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
            experimental_streaming=False,
            use_streaming_candidate=False,
            log_level="INFO",
            headless=True,
            state_path="s",
            driver_path=None,
        )
        mock_read_parquet.return_value = self._get_mock_df()
        mock_scraper_inst = mock_scraper.return_value
        mock_scraper_inst.run.return_value = (MagicMock(), [])

        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "pathlib.Path.exists", return_value=True
        ):
            main.main()
            args, kwargs = mock_scraper_inst.run.call_args
            self.assertIsNone(kwargs.get("content_callback"))


if __name__ == "__main__":
    unittest.main()
