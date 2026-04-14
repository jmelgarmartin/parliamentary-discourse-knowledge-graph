import argparse
import json
import os
import sys
import unittest
from typing import Any, List, Tuple
from unittest.mock import MagicMock, mock_open, patch

import pandas as pd

ORIGINAL_DF = pd.DataFrame

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
            batch_freshness_window=5,
            log_level="INFO",
            headless=True,
            state_path="s",
            driver_path=None,
            allow_inferred_promotion=False,
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
            batch_freshness_window=5,
            allow_inferred_promotion=False,
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
            self.assertEqual(summary["validation_mode"], "skipped_batch")

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
            self.assertEqual(summary["validation_mode"], "full_batch_validation")

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
            batch_freshness_window=5,
            allow_inferred_promotion=False,
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
            batch_freshness_window=5,
            allow_inferred_promotion=False,
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
            batch_freshness_window=5,
            allow_inferred_promotion=False,
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
            batch_freshness_window=5,
            allow_inferred_promotion=False,
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
        self.assertEqual(summary["validation_mode"], "skipped_batch")

    @patch("main.pd.read_parquet")
    @patch("main.GroupsScraper")
    @patch("main.DeputiesScraper")
    @patch("main.BackupManager")
    @patch("main.SubstitutionsEnricher")
    @patch("main.run_interventions_enrichment")
    @patch("main.SessionsScraper")
    @patch("main.InterventionsExtractor")
    @patch("json.dump")
    def test_streaming_only_validation_classification(
        self,
        mock_json_dump: MagicMock,
        mock_extractor: MagicMock,
        mock_scraper: MagicMock,
        mock_run_enrich: MagicMock,
        mock_subst_enricher: MagicMock,
        mock_backup: MagicMock,
        mock_deputies: MagicMock,
        mock_groups: MagicMock,
        mock_read_parquet: MagicMock,
    ) -> None:
        """Verify that streaming_only_validation is classified but not promoted."""
        # 1. Mock a stable summary
        stable_summary = {
            "overall_counts": {"total_runs": 10},
            "stability_metrics": {"stable_streaming": True},
            "accuracy_metrics": {"strict_match_success_rate": 1.0, "fallback_rate": 0.0, "avg_confidence_score": 1.0},
        }

        # 2. Mock s_scraper.run to call the callback
        def scraper_run_side_effect(content_callback: Any = None) -> Tuple[None, List[str]]:
            if content_callback:
                content_callback("doc1", "<html></html>")
            return (None, ["file1.html"])

        mock_read_parquet.return_value = self._get_mock_df()
        mock_scraper.return_value.run.side_effect = scraper_run_side_effect
        mock_extractor.return_value.extract_from_content.return_value = [{"id": "r1"}]
        mock_extractor.return_value.run.return_value = ORIGINAL_DF()  # Batch skipped
        mock_subst_enricher.return_value = self._get_enricher_mock()

        with patch("pathlib.Path.exists", side_effect=lambda *args, **kwargs: True), patch(
            "builtins.open", mock_open(read_data=json.dumps(stable_summary))
        ), patch("main.setup_logging"), patch("pathlib.Path.mkdir"):
            with patch("main.pd.DataFrame") as mock_df_class:
                # I want the one at line 468 in main.py: streaming_df = pd.DataFrame(streaming_records)
                mock_streaming_df = MagicMock()
                mock_streaming_df.empty = False
                mock_streaming_df.__len__.return_value = 10
                mock_streaming_df.columns = ["document_id", "intervention_order", "intervention_id"]
                mock_streaming_df["document_id"].unique.return_value = ["doc1"]

                # Mock the DataFrame class to return our mock when called with list (streaming_records)
                def df_side_effect(data: Any = None, **kwargs: Any) -> Any:
                    if isinstance(data, list) and len(data) > 0:
                        return mock_streaming_df
                    return ORIGINAL_DF(data, **kwargs)

                mock_df_class.side_effect = df_side_effect

                with patch("sys.argv", ["main.py", "--batch-strategy", "sampled"]):
                    # Sampling logic: (10 + 1) % 5 = 1 != 0 -> batch skipped.
                    import main

                    main.main()

                    # Check summary (the 2nd dump)
                    summary = mock_json_dump.call_args_list[1][0][0]
                    self.assertEqual(summary["validation_mode"], "streaming_only_validation")
                    self.assertEqual(summary["parity_status"], "INFERRED_VALID")
                    self.assertEqual(summary["promotion_result"], "FALLBACK")  # MUST NOT PROMOTE
                    self.assertEqual(summary["streaming_validation_basis"]["avg_confidence_score"], 1.0)

    @patch("main.pd.read_parquet")
    @patch("main.GroupsScraper")
    @patch("main.DeputiesScraper")
    @patch("main.BackupManager")
    @patch("main.SubstitutionsEnricher")
    @patch("main.run_interventions_enrichment")
    @patch("main.SessionsScraper")
    @patch("main.InterventionsExtractor")
    @patch("json.dump")
    def test_inferred_promotion_allowed_when_conditions_met(
        self,
        mock_json_dump: MagicMock,
        mock_extractor: MagicMock,
        mock_scraper: MagicMock,
        mock_run_enrich: MagicMock,
        mock_subst_enricher: MagicMock,
        mock_backup: MagicMock,
        mock_deputies: MagicMock,
        mock_groups: MagicMock,
        mock_read_parquet: MagicMock,
    ) -> None:
        """Verify that PROMOTED_INFERRED happens with flag and stable metrics."""
        stable_summary = {
            "overall_counts": {"total_runs": 10},
            "stability_metrics": {"stable_streaming": True},
            "accuracy_metrics": {"strict_match_success_rate": 1.0, "fallback_rate": 0.0, "avg_confidence_score": 1.0},
        }
        mock_read_parquet.return_value = self._get_mock_df()

        def scraper_run_side_effect(content_callback: Any = None) -> Tuple[None, List[str]]:
            if content_callback:
                content_callback("doc1", "<html></html>")
            return (None, ["file1.html"])

        mock_scraper.return_value.run.side_effect = scraper_run_side_effect
        mock_extractor.return_value.extract_from_content.return_value = [{"id": "r1"}]
        mock_extractor.return_value.run.return_value = ORIGINAL_DF()
        mock_subst_enricher.return_value = self._get_enricher_mock()

        with patch("pathlib.Path.exists", return_value=True), patch(
            "builtins.open", mock_open(read_data=json.dumps(stable_summary))
        ), patch("main.setup_logging"), patch("pathlib.Path.mkdir"):
            # Mock candidate exists
            with patch("sys.argv", ["main.py", "--batch-strategy", "sampled", "--allow-inferred-promotion"]):
                import main

                main.main()

                summary = mock_json_dump.call_args_list[1][0][0]
                self.assertEqual(summary["promotion_result"], "PROMOTED_INFERRED")
                self.assertEqual(summary["promotion_basis"], "inferred_validation")
                self.assertTrue(summary["inferred_promotion_allowed"])

    @patch("main.pd.read_parquet")
    @patch("main.GroupsScraper")
    @patch("main.DeputiesScraper")
    @patch("main.BackupManager")
    @patch("main.SubstitutionsEnricher")
    @patch("main.run_interventions_enrichment")
    @patch("main.SessionsScraper")
    @patch("main.InterventionsExtractor")
    @patch("json.dump")
    def test_inferred_promotion_denied_on_low_metrics(
        self,
        mock_json_dump: MagicMock,
        mock_extractor: MagicMock,
        mock_scraper: MagicMock,
        mock_run_enrich: MagicMock,
        mock_subst_enricher: MagicMock,
        mock_backup: MagicMock,
        mock_deputies: MagicMock,
        mock_groups: MagicMock,
        mock_read_parquet: MagicMock,
    ) -> None:
        """Verify that inferred promotion is denied if accuracy metrics are below threshold."""
        weak_summary = {
            "overall_counts": {"total_runs": 10},
            "stability_metrics": {"stable_streaming": True},
            "accuracy_metrics": {
                "strict_match_success_rate": 1.0,
                "fallback_rate": 0.0,
                "avg_confidence_score": 0.94,  # BELOW 0.95
            },
        }
        mock_read_parquet.return_value = self._get_mock_df()

        def scraper_run_side_effect(content_callback: Any = None) -> Tuple[None, List[str]]:
            if content_callback:
                content_callback("doc1", "<html></html>")
            return (None, ["file1.html"])

        mock_scraper.return_value.run.side_effect = scraper_run_side_effect
        mock_extractor.return_value.extract_from_content.return_value = [{"id": "r1"}]
        mock_extractor.return_value.run.return_value = ORIGINAL_DF()
        mock_subst_enricher.return_value = self._get_enricher_mock()

        with patch("pathlib.Path.exists", return_value=True), patch(
            "builtins.open", mock_open(read_data=json.dumps(weak_summary))
        ), patch("main.setup_logging"), patch("pathlib.Path.mkdir"):
            with patch("sys.argv", ["main.py", "--batch-strategy", "sampled", "--allow-inferred-promotion"]):
                import main

                main.main()

                summary = mock_json_dump.call_args_list[1][0][0]
                self.assertEqual(summary["promotion_result"], "FALLBACK")
                self.assertEqual(summary["fallback_reason"], "batch_skipped_no_inferred_eligibility")
                self.assertFalse(summary["inferred_promotion_allowed"])

    def test_allow_inferred_promotion_incompatible_with_disable_streaming(self) -> None:
        """Verify CLI safety: --allow-inferred-promotion and --disable-streaming are incompatible."""
        with patch("sys.argv", ["main.py", "--disable-streaming", "--allow-inferred-promotion"]), patch(
            "main.setup_logging"
        ):
            with self.assertRaises(SystemExit) as cm:
                import main

                main.main()
            self.assertEqual(cm.exception.code, 1)

    # --- Phase 24 Adaptive Batch Tests ---

    @patch("main.SessionsScraper")
    @patch("main.InterventionsExtractor")
    @patch("main.pd.read_parquet")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("json.dump")
    def test_adaptive_strategy_runs_batch_when_unstable(
        self,
        mock_json_dump: MagicMock,
        mock_args: MagicMock,
        mock_read_parquet: MagicMock,
        mock_extractor: MagicMock,
        mock_scraper: MagicMock,
    ) -> None:
        """Rule A: adaptive strategy runs batch when stable_streaming == False."""
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
            batch_strategy="adaptive",
            batch_freshness_window=5,
            batch_sample_every=5,
            allow_inferred_promotion=False,
        )
        # Summary says UNSTABLE
        mock_summary = json.dumps(
            {
                "overall_counts": {"total_runs": 10},
                "stability_metrics": {
                    "stable_streaming": False,
                    "runs_since_last_full_batch": 1,
                    "rolling_metrics_last_10": {"fallback_rate": 0.0},
                },
            }
        )
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
            self.assertEqual(summary["adaptive_batch_reason"], "unstable_streaming")
            self.assertTrue(summary["batch_required_by_rule"])

    @patch("main.InterventionsExtractor")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("json.dump")
    def test_adaptive_strategy_runs_batch_when_stale(
        self,
        mock_json_dump: MagicMock,
        mock_args: MagicMock,
        mock_extractor: MagicMock,
    ) -> None:
        """Rule C: adaptive strategy runs batch when evidence is stale (runs_since_last_batch >= window)."""
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
            batch_strategy="adaptive",
            batch_freshness_window=5,
            batch_sample_every=5,
            allow_inferred_promotion=False,
        )
        # Summary says STABLE but STALE (runs_since = 5, window = 5)
        mock_summary = json.dumps(
            {
                "overall_counts": {"total_runs": 10},
                "stability_metrics": {
                    "stable_streaming": True,
                    "runs_since_last_full_batch": 5,
                    "rolling_metrics_last_10": {"fallback_rate": 0.0},
                },
            }
        )
        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "pathlib.Path.exists", return_value=True
        ), patch("builtins.open", mock_open(read_data=mock_summary)), patch("main.run_interventions_enrichment"), patch(
            "pathlib.Path.mkdir"
        ), patch("main.SessionsScraper") as mock_s_scraper_class, patch("main.pd.read_parquet") as mock_rp:
            self._setup_common_mocks(mock_rp, mock_s_scraper_class)
            main.main()
            mock_extractor.return_value.run.assert_called()
            summary = mock_json_dump.call_args_list[1][0][0]
            self.assertEqual(summary["batch_executed"], True)
            self.assertTrue("stale_evidence" in summary["adaptive_batch_reason"])
            self.assertTrue(summary["batch_required_by_rule"])

    @patch("main.InterventionsExtractor")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("json.dump")
    def test_adaptive_strategy_skips_batch_when_fresh_and_stable(
        self,
        mock_json_dump: MagicMock,
        mock_args: MagicMock,
        mock_extractor: MagicMock,
    ) -> None:
        """Rule D: adaptive strategy skips batch when stable and evidence is fresh."""
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
            batch_strategy="adaptive",
            batch_freshness_window=5,
            batch_sample_every=5,
            allow_inferred_promotion=False,
        )
        # Summary says STABLE and FRESH
        mock_summary = json.dumps(
            {
                "overall_counts": {"total_runs": 10},
                "stability_metrics": {
                    "stable_streaming": True,
                    "runs_since_last_full_batch": 1,
                    "rolling_metrics_last_10": {"fallback_rate": 0.0},
                },
            }
        )
        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "pathlib.Path.exists", return_value=True
        ), patch("builtins.open", mock_open(read_data=mock_summary)), patch("main.run_interventions_enrichment"), patch(
            "pathlib.Path.mkdir"
        ), patch("main.SessionsScraper") as mock_s_scraper_class, patch("main.pd.read_parquet") as mock_rp:
            self._setup_common_mocks(mock_rp, mock_s_scraper_class)
            main.main()
            mock_extractor.return_value.run.assert_not_called()
            summary = mock_json_dump.call_args_list[1][0][0]
            self.assertEqual(summary["batch_executed"], False)
            self.assertEqual(summary["adaptive_batch_reason"], "safe_adaptive_skip")
            self.assertFalse(summary["batch_required_by_rule"])

    @patch("main.InterventionsExtractor")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("json.dump")
    def test_adaptive_strategy_runs_batch_after_recent_fallback(
        self,
        mock_json_dump: MagicMock,
        mock_args: MagicMock,
        mock_extractor: MagicMock,
    ) -> None:
        """Rule B: adaptive strategy runs batch if rolling fallback rate > 0."""
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
            batch_strategy="adaptive",
            batch_freshness_window=5,
            batch_sample_every=5,
            allow_inferred_promotion=False,
        )
        # Summary says STABLE and FRESH but RECENT FALLBACK
        mock_summary = json.dumps(
            {
                "overall_counts": {"total_runs": 10},
                "stability_metrics": {
                    "stable_streaming": True,
                    "runs_since_last_full_batch": 1,
                    "rolling_metrics_last_10": {"fallback_rate": 0.1},
                },
            }
        )
        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "pathlib.Path.exists", return_value=True
        ), patch("builtins.open", mock_open(read_data=mock_summary)), patch("main.run_interventions_enrichment"), patch(
            "pathlib.Path.mkdir"
        ), patch("main.SessionsScraper") as mock_s_scraper_class, patch("main.pd.read_parquet") as mock_rp:
            self._setup_common_mocks(mock_rp, mock_s_scraper_class)
            main.main()
            mock_extractor.return_value.run.assert_called()
            summary = mock_json_dump.call_args_list[1][0][0]
            self.assertEqual(summary["batch_executed"], True)
            self.assertTrue("recent_fallback_detected" in summary["adaptive_batch_reason"])
            self.assertTrue(summary["batch_required_by_rule"])

    # --- Phase 26 Periodic Audit Tests ---

    @patch("main.InterventionsExtractor")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("json.dump")
    def test_periodic_audit_runs_batch_when_due(
        self,
        mock_json_dump: MagicMock,
        mock_args: MagicMock,
        mock_extractor: MagicMock,
    ) -> None:
        """Periodic audit runs batch when audit window is reached."""
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
            batch_strategy="periodic_audit",
            batch_audit_every=10,
            batch_freshness_window=5,
            batch_sample_every=5,
            allow_inferred_promotion=False,
        )
        # Summary says STABLE but AUDIT DUE (runs_since = 10, window = 10)
        mock_summary = json.dumps(
            {
                "overall_counts": {"total_runs": 10},
                "stability_metrics": {
                    "stable_streaming": True,
                    "runs_since_last_full_batch": 10,
                    "rolling_metrics_last_10": {"fallback_rate": 0.0},
                },
            }
        )
        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "pathlib.Path.exists", return_value=True
        ), patch("builtins.open", mock_open(read_data=mock_summary)), patch("main.run_interventions_enrichment"), patch(
            "pathlib.Path.mkdir"
        ), patch("main.SessionsScraper") as mock_s_scraper_class, patch("main.pd.read_parquet") as mock_rp:
            self._setup_common_mocks(mock_rp, mock_s_scraper_class)
            main.main()
            mock_extractor.return_value.run.assert_called()
            summary = mock_json_dump.call_args_list[1][0][0]
            self.assertEqual(summary["batch_executed"], True)
            self.assertTrue(summary["batch_audit_due"])
            self.assertEqual(summary["batch_strategy"], "periodic_audit")
            self.assertTrue("audit_due" in summary["batch_audit_reason"])

    @patch("main.InterventionsExtractor")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("json.dump")
    def test_periodic_audit_skips_batch_when_not_due(
        self,
        mock_json_dump: MagicMock,
        mock_args: MagicMock,
        mock_extractor: MagicMock,
    ) -> None:
        """Periodic audit skips batch when audit window is not yet reached and system is stable."""
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
            batch_strategy="periodic_audit",
            batch_audit_every=10,
            batch_freshness_window=5,
            batch_sample_every=5,
            allow_inferred_promotion=False,
        )
        # Summary says STABLE and NOT DUE (runs_since = 5, window = 10)
        mock_summary = json.dumps(
            {
                "overall_counts": {"total_runs": 10},
                "stability_metrics": {
                    "stable_streaming": True,
                    "runs_since_last_full_batch": 5,
                    "rolling_metrics_last_10": {"fallback_rate": 0.0},
                },
            }
        )
        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "pathlib.Path.exists", return_value=True
        ), patch("builtins.open", mock_open(read_data=mock_summary)), patch("main.run_interventions_enrichment"), patch(
            "pathlib.Path.mkdir"
        ), patch("main.SessionsScraper") as mock_s_scraper_class, patch("main.pd.read_parquet") as mock_rp:
            self._setup_common_mocks(mock_rp, mock_s_scraper_class)
            main.main()
            mock_extractor.return_value.run.assert_not_called()
            summary = mock_json_dump.call_args_list[1][0][0]
            self.assertEqual(summary["batch_executed"], False)
            self.assertFalse(summary["batch_audit_due"])
            self.assertEqual(summary["batch_skip_reason"], "periodic_audit_skip")

    @patch("main.InterventionsExtractor")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("json.dump")
    def test_periodic_audit_runs_batch_on_instability(
        self,
        mock_json_dump: MagicMock,
        mock_args: MagicMock,
        mock_extractor: MagicMock,
    ) -> None:
        """Periodic audit triggers batch if stable_streaming is False, regardless of freshness."""
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
            batch_strategy="periodic_audit",
            batch_audit_every=10,
            allow_inferred_promotion=False,
        )
        # Summary says UNSTABLE but FRESH (runs_since = 1)
        mock_summary = json.dumps(
            {
                "overall_counts": {"total_runs": 10},
                "stability_metrics": {
                    "stable_streaming": False,
                    "runs_since_last_full_batch": 1,
                    "rolling_metrics_last_10": {"fallback_rate": 0.0},
                },
            }
        )
        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "pathlib.Path.exists", return_value=True
        ), patch("builtins.open", mock_open(read_data=mock_summary)), patch("main.run_interventions_enrichment"), patch(
            "pathlib.Path.mkdir"
        ), patch("main.SessionsScraper") as mock_s_scraper_class, patch("main.pd.read_parquet") as mock_rp:
            self._setup_common_mocks(mock_rp, mock_s_scraper_class)
            main.main()
            mock_extractor.return_value.run.assert_called()
            summary = mock_json_dump.call_args_list[1][0][0]
            self.assertEqual(summary["batch_executed"], True)
            self.assertEqual(summary["batch_audit_reason"], "unstable_streaming")
            self.assertTrue(summary["batch_audit_due"])

    @patch("main.InterventionsExtractor")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("json.dump")
    def test_periodic_audit_fallback_on_invalid_context(
        self,
        mock_json_dump: MagicMock,
        mock_args: MagicMock,
        mock_extractor: MagicMock,
    ) -> None:
        """Periodic audit triggers batch if monitoring context is missing or invalid."""
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
            batch_strategy="periodic_audit",
            batch_audit_every=10,
            allow_inferred_promotion=False,
        )

        # Summary is MISSING
        def exists_side_effect(self_obj: Any) -> bool:
            if "promotion_monitoring_summary.json" in str(self_obj):
                return False
            return True

        with patch("main.setup_logging"), patch("main.BackupManager"), patch("main.GroupsScraper"), patch(
            "main.DeputiesScraper"
        ), patch("main.SubstitutionsEnricher", return_value=self._get_enricher_mock()), patch(
            "pathlib.Path.exists", side_effect=exists_side_effect, autospec=True
        ), patch("main.run_interventions_enrichment"), patch("pathlib.Path.mkdir"), patch(
            "main.SessionsScraper"
        ) as mock_s_scraper_class, patch("main.pd.read_parquet") as mock_rp:
            self._setup_common_mocks(mock_rp, mock_s_scraper_class)
            main.main()
            mock_extractor.return_value.run.assert_called()
            summary = mock_json_dump.call_args_list[1][0][0]
            self.assertEqual(summary["batch_executed"], True)
            self.assertTrue(summary["batch_audit_due"])
            self.assertEqual(summary["batch_audit_reason"], "missing_or_invalid_monitoring_context")
            self.assertTrue(summary["periodic_audit_mode_active"])


if __name__ == "__main__":
    unittest.main()
