import argparse
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

    def _get_enricher_mock(self) -> MagicMock:
        mock = MagicMock()
        mock.enrich.return_value = (pd.DataFrame(), pd.DataFrame())
        return mock

    def _setup_common_mocks(self, mock_read_parquet: MagicMock, mock_scraper: MagicMock) -> None:
        mock_read_parquet.return_value = self._get_mock_df()
        mock_scraper.return_value.run.return_value = (pd.DataFrame(), ["f1"])

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
        mock_args.return_value = argparse.Namespace(
            term="15",
            disable_streaming=False,
            experimental_streaming=True,
            use_streaming_candidate=False,
            promote_streaming=False,
            streaming_confidence_threshold=None,
            batch_strategy="always",
            batch_sample_every=5,
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

        def scraper_side_effect(*a: Any, **kw: Any) -> Tuple[pd.DataFrame, List[str]]:
            callback = kw.get("content_callback")
            if callback:
                callback("d1", "h")
            return (pd.DataFrame(), ["f1"])

        mock_scraper_inst.run.side_effect = scraper_side_effect
        mock_read_parquet.return_value = self._get_mock_df()

        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "main.run_interventions_enrichment"
        ), patch("pathlib.Path.mkdir"), patch("pathlib.Path.exists", return_value=True), patch("builtins.open"):
            main.main()

            report = mock_json_dump.call_args_list[0][0][0]
            self.assertEqual(report["confidence_level"], "FULL_MATCH")
            self.assertEqual(report["confidence_score"], 1.0)

    # --- Phase 22A Hardening Tests ---

    @patch("main.SessionsScraper")
    @patch("main.InterventionsExtractor")
    @patch("main.pd.read_parquet")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("json.dump")
    def test_batch_strategy_sampled_uses_integer_interval(
        self,
        mock_json_dump: MagicMock,
        mock_args: MagicMock,
        mock_read_parquet: MagicMock,
        mock_extractor: MagicMock,
        mock_scraper: MagicMock,
    ) -> None:
        """Verify that strategy='sampled' uses rule: (total_runs + 1) % batch_sample_every == 0."""
        self._setup_common_mocks(mock_read_parquet, mock_scraper)

        args = argparse.Namespace(
            term="15",
            driver_path=None,
            state_path="s",
            log_level="INFO",
            headless=True,
            disable_streaming=False,
            experimental_streaming=True,
            use_streaming_candidate=False,
            promote_streaming=False,
            streaming_confidence_threshold=None,
            batch_strategy="sampled",
            batch_sample_every=2,
        )
        mock_args.return_value = args

        # Case 1: SKIP (total_runs=0)
        mock_summary = '{"overall_counts": {"total_runs": 0}, "stability_metrics": {"stable_streaming": true}}'
        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "pathlib.Path.exists", return_value=True
        ), patch("builtins.open", mock_open(read_data=mock_summary)), patch("main.run_interventions_enrichment"), patch(
            "pathlib.Path.mkdir"
        ):
            main.main()
            mock_extractor.return_value.run.assert_not_called()
            summary = mock_json_dump.call_args_list[1][0][0]
            self.assertEqual(summary["batch_executed"], False)
            self.assertEqual(summary["parity_status"], "SKIPPED")

        # Case 2: RUN (total_runs=1)
        mock_extractor.return_value.run.reset_mock()
        mock_json_dump.reset_mock()
        mock_summary = '{"overall_counts": {"total_runs": 1}, "stability_metrics": {"stable_streaming": true}}'
        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "pathlib.Path.exists", return_value=True
        ), patch("builtins.open", mock_open(read_data=mock_summary)), patch("main.run_interventions_enrichment"), patch(
            "pathlib.Path.mkdir"
        ):
            main.main()
            mock_extractor.return_value.run.assert_called()
            summary = mock_json_dump.call_args_list[1][0][0]
            self.assertEqual(summary["batch_executed"], True)

    @patch("main.SessionsScraper")
    @patch("main.InterventionsExtractor")
    @patch("main.pd.read_parquet")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("json.dump")
    def test_batch_fallback_on_missing_summary(
        self,
        mock_json_dump: MagicMock,
        mock_args: MagicMock,
        mock_read_parquet: MagicMock,
        mock_extractor: MagicMock,
        mock_scraper: MagicMock,
    ) -> None:
        """Verify run_batch=True and specific reason when summary file is missing."""
        self._setup_common_mocks(mock_read_parquet, mock_scraper)
        mock_args.return_value = argparse.Namespace(
            term="15",
            driver_path=None,
            state_path="s",
            log_level="INFO",
            headless=True,
            disable_streaming=False,
            experimental_streaming=True,
            use_streaming_candidate=False,
            promote_streaming=False,
            streaming_confidence_threshold=None,
            batch_strategy="sampled",
            batch_sample_every=5,
        )

        def exists_side_effect(self_obj: Any) -> bool:
            if "promotion_monitoring_summary.json" in str(self_obj):
                return False
            return True

        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "pathlib.Path.exists", side_effect=exists_side_effect, autospec=True
        ), patch("main.run_interventions_enrichment"), patch("pathlib.Path.mkdir"):
            main.main()

            mock_extractor.return_value.run.assert_called()
            summary = mock_json_dump.call_args_list[1][0][0]
            self.assertEqual(summary["batch_executed"], True)
            self.assertEqual(summary["batch_skip_reason"], "missing_or_invalid_summary_fallback")

    @patch("main.SessionsScraper")
    @patch("main.InterventionsExtractor")
    @patch("main.pd.read_parquet")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("json.dump")
    def test_batch_fallback_on_malformed_summary(
        self,
        mock_json_dump: MagicMock,
        mock_args: MagicMock,
        mock_read_parquet: MagicMock,
        mock_extractor: MagicMock,
        mock_scraper: MagicMock,
    ) -> None:
        """Verify run_batch=True when summary JSON is malformed."""
        self._setup_common_mocks(mock_read_parquet, mock_scraper)
        mock_args.return_value = argparse.Namespace(
            term="15",
            driver_path=None,
            state_path="s",
            log_level="INFO",
            headless=True,
            disable_streaming=False,
            experimental_streaming=True,
            use_streaming_candidate=False,
            promote_streaming=False,
            streaming_confidence_threshold=None,
            batch_strategy="sampled",
            batch_sample_every=5,
        )

        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "pathlib.Path.exists", return_value=True
        ), patch("builtins.open", mock_open(read_data="INVALID JSON")), patch(
            "main.run_interventions_enrichment"
        ), patch("pathlib.Path.mkdir"):
            main.main()

            mock_extractor.return_value.run.assert_called()
            summary = mock_json_dump.call_args_list[1][0][0]
            self.assertEqual(summary["batch_skip_reason"], "missing_or_invalid_summary_fallback")

    @patch("main.SessionsScraper")
    @patch("main.InterventionsExtractor")
    @patch("main.pd.read_parquet")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("json.dump")
    def test_batch_fallback_on_incomplete_summary(
        self,
        mock_json_dump: MagicMock,
        mock_args: MagicMock,
        mock_read_parquet: MagicMock,
        mock_extractor: MagicMock,
        mock_scraper: MagicMock,
    ) -> None:
        """Verify run_batch=True when summary is missing required keys."""
        self._setup_common_mocks(mock_read_parquet, mock_scraper)
        mock_args.return_value = argparse.Namespace(
            term="15",
            driver_path=None,
            state_path="s",
            log_level="INFO",
            headless=True,
            disable_streaming=False,
            experimental_streaming=True,
            use_streaming_candidate=False,
            promote_streaming=False,
            streaming_confidence_threshold=None,
            batch_strategy="sampled",
            batch_sample_every=5,
        )

        # Missing "overall_counts"
        mock_summary = '{"stability_metrics": {"stable_streaming": true}}'

        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "pathlib.Path.exists", return_value=True
        ), patch("builtins.open", mock_open(read_data=mock_summary)), patch("main.run_interventions_enrichment"), patch(
            "pathlib.Path.mkdir"
        ):
            main.main()

            mock_extractor.return_value.run.assert_called()
            summary = mock_json_dump.call_args_list[1][0][0]
            self.assertEqual(summary["batch_skip_reason"], "missing_or_invalid_summary_fallback")

    @patch("main.BackupManager")
    @patch("main.GroupsScraper")
    @patch("main.DeputiesScraper")
    @patch("main.SubstitutionsEnricher")
    @patch("main.run_interventions_enrichment")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("main.pd.read_parquet")
    @patch("main.pd.DataFrame.to_parquet")
    @patch("main.InterventionsExtractor")
    @patch("main.SessionsScraper")
    @patch("json.dump")
    @patch("builtins.open", new_callable=mock_open)
    def test_parity_status_is_skipped_when_batch_skipped(
        self,
        mock_file: MagicMock,
        mock_json_dump: MagicMock,
        mock_sessions_scraper: MagicMock,
        mock_extractor: MagicMock,
        mock_to_parquet: MagicMock,
        mock_read_parquet: MagicMock,
        mock_parse_args: MagicMock,
        mock_run_enrich: MagicMock,
        mock_subst_enricher: MagicMock,
        mock_deputies_scraper: MagicMock,
        mock_groups_scraper: MagicMock,
        mock_backup_manager: MagicMock,
    ) -> None:
        """Verify that parity_status is 'SKIPPED' if run_batch is False."""
        self._setup_common_mocks(mock_read_parquet, mock_sessions_scraper)
        mock_parse_args.return_value = argparse.Namespace(
            term="15",
            driver_path=None,
            state_path="s",
            log_level="INFO",
            headless=True,
            disable_streaming=False,
            experimental_streaming=True,
            use_streaming_candidate=False,
            promote_streaming=False,
            streaming_confidence_threshold=None,
            batch_strategy="sampled",
            batch_sample_every=100,  # Force skip
        )
        mock_subst_enricher.return_value = self._get_enricher_mock()

        with patch("pathlib.Path.exists", return_value=True), patch("pathlib.Path.mkdir"), patch("main.setup_logging"):
            # Mock valid summary with total_runs=0
            mock_file().read.return_value = (
                '{"overall_counts": {"total_runs": 0}, "stability_metrics": {"stable_streaming": true}}'
            )
            main.main()

        # The second dump is validation_run_summary.json
        summary = mock_json_dump.call_args_list[1][0][0]
        self.assertEqual(summary["batch_executed"], False)
        self.assertEqual(summary["parity_status"], "SKIPPED")


if __name__ == "__main__":
    unittest.main()
