import argparse
import logging
import os
import sys
import unittest
from unittest.mock import mock_open, patch

import pandas as pd

# Add src to path
sys.path.append(os.path.join(os.path.dirname(__file__), "../../src"))

import main as main_module


class TestRecoveryLogic(unittest.TestCase):
    def setUp(self):
        # Ensure logging is at least at INFO level
        logging.getLogger().setLevel(logging.INFO)

    @patch("main.setup_logging")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("main.json.loads")
    @patch("main.pd.read_parquet")
    @patch("main.run_interventions_enrichment")
    @patch("main.InterventionsExtractor")
    @patch("main.SubstitutionsEnricher")
    @patch("main.SessionsScraper")
    @patch("main.DeputiesScraper")
    @patch("main.GroupsScraper")
    @patch("main.BackupManager")
    @patch("main.pathlib.Path.exists")
    def test_recovery_skips_when_stable(
        self,
        mock_exists,
        mock_backup_mgr,
        mock_g_scraper,
        mock_d_scraper,
        mock_s_scraper,
        mock_sub_enrich,
        mock_int_ext,
        mock_int_enrich,
        mock_read_parquet,
        mock_json_loads,
        mock_parse_args,
        mock_setup_logging,
    ):
        mock_parse_args.return_value = argparse.Namespace(
            term="15",
            batch_strategy="recovery_only",
            disable_streaming=False,
            experimental_streaming=True,
            use_streaming_candidate=False,
            promote_streaming=False,
            streaming_confidence_threshold=None,
            allow_inferred_promotion=False,
            batch_freshness_window=5,
            batch_sample_every=5,
            batch_audit_every=10,
            driver_path=None,
            state_path="state/bronze.duckdb",
            log_level="INFO",
            headless=True,
        )

        mock_exists.return_value = True
        mock_json_loads.return_value = {
            "stability_metrics": {"stable_streaming": True, "rolling_metrics_last_10": {"fallback_rate": 0.0}},
            "overall_counts": {"total_runs": 10},
            "accuracy_metrics": {"fallback_rate": 0.0},
        }

        mock_read_parquet.return_value = pd.DataFrame()
        mock_s_scraper.return_value.run.return_value = (pd.DataFrame(), [])
        mock_d_scraper.return_value.run.return_value = pd.DataFrame()
        mock_g_scraper.return_value.run.return_value = pd.DataFrame()
        mock_sub_enrich.return_value.enrich.return_value = (pd.DataFrame(), pd.DataFrame())
        mock_int_ext.return_value.run.return_value = pd.DataFrame()

        m_open = mock_open(read_data='{"dummy": true}')
        with patch("main.open", m_open):
            with self.assertLogs("main", level="INFO") as cm:
                try:
                    main_module.main()
                except SystemExit:
                    pass

                # Verify skip log
                found = any(
                    "Recovery-only baseline met" in output or "Skipping batch extraction" in output
                    for output in cm.output
                )
                self.assertTrue(found, f"Should have logged stable skip. Logs: {cm.output}")

    @patch("main.setup_logging")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("main.json.loads")
    @patch("main.pd.read_parquet")
    @patch("main.run_interventions_enrichment")
    @patch("main.InterventionsExtractor")
    @patch("main.SubstitutionsEnricher")
    @patch("main.SessionsScraper")
    @patch("main.DeputiesScraper")
    @patch("main.GroupsScraper")
    @patch("main.BackupManager")
    @patch("main.pathlib.Path.exists")
    def test_recovery_triggers_on_instability(
        self,
        mock_exists,
        mock_backup_mgr,
        mock_g_scraper,
        mock_d_scraper,
        mock_s_scraper,
        mock_sub_enrich,
        mock_int_ext,
        mock_int_enrich,
        mock_read_parquet,
        mock_json_loads,
        mock_parse_args,
        mock_setup_logging,
    ):
        mock_parse_args.return_value = argparse.Namespace(
            term="15",
            batch_strategy="recovery_only",
            disable_streaming=False,
            experimental_streaming=True,
            use_streaming_candidate=False,
            promote_streaming=False,
            streaming_confidence_threshold=None,
            allow_inferred_promotion=False,
            batch_freshness_window=5,
            batch_sample_every=5,
            batch_audit_every=10,
            driver_path=None,
            state_path="state/bronze.duckdb",
            log_level="INFO",
            headless=True,
        )

        mock_exists.return_value = True
        mock_json_loads.return_value = {
            "stability_metrics": {"stable_streaming": False, "rolling_metrics_last_10": {"fallback_rate": 0.0}},
            "overall_counts": {"total_runs": 10},
            "accuracy_metrics": {"fallback_rate": 0.0},
        }

        mock_read_parquet.return_value = pd.DataFrame()
        mock_s_scraper.return_value.run.return_value = (pd.DataFrame(), [])
        mock_d_scraper.return_value.run.return_value = pd.DataFrame()
        mock_g_scraper.return_value.run.return_value = pd.DataFrame()
        mock_sub_enrich.return_value.enrich.return_value = (pd.DataFrame(), pd.DataFrame())
        mock_int_ext.return_value.run.return_value = pd.DataFrame()

        m_open = mock_open(read_data='{"dummy": true}')
        with patch("main.open", m_open):
            with self.assertLogs("main", level="WARNING") as cm:
                try:
                    main_module.main()
                except SystemExit:
                    pass

                # Verify recovery triggered log
                found = any("Recovery triggered: detected_instability" in output for output in cm.output)
                self.assertTrue(found, f"Should have logged recovery trigger due to instability. Logs: {cm.output}")

    @patch("main.setup_logging")
    @patch("main.argparse.ArgumentParser.parse_args")
    @patch("main.json.loads")
    @patch("main.pd.read_parquet")
    @patch("main.run_interventions_enrichment")
    @patch("main.InterventionsExtractor")
    @patch("main.SubstitutionsEnricher")
    @patch("main.SessionsScraper")
    @patch("main.DeputiesScraper")
    @patch("main.GroupsScraper")
    @patch("main.BackupManager")
    @patch("main.pathlib.Path.exists")
    def test_recovery_triggers_on_missing_context(
        self,
        mock_exists,
        mock_backup_mgr,
        mock_g_scraper,
        mock_d_scraper,
        mock_s_scraper,
        mock_sub_enrich,
        mock_int_ext,
        mock_int_enrich,
        mock_read_parquet,
        mock_json_loads,
        mock_parse_args,
        mock_setup_logging,
    ):
        mock_parse_args.return_value = argparse.Namespace(
            term="15",
            batch_strategy="recovery_only",
            disable_streaming=False,
            experimental_streaming=True,
            use_streaming_candidate=False,
            promote_streaming=False,
            streaming_confidence_threshold=None,
            allow_inferred_promotion=False,
            batch_freshness_window=5,
            batch_sample_every=5,
            batch_audit_every=10,
            driver_path=None,
            state_path="state/bronze.duckdb",
            log_level="INFO",
            headless=True,
        )

        mock_exists.side_effect = (
            lambda *args, **kwargs: "promotion_monitoring_summary.json" not in str(args[0]) if args else True
        )

        mock_read_parquet.return_value = pd.DataFrame()
        mock_s_scraper.return_value.run.return_value = (pd.DataFrame(), [])
        mock_d_scraper.return_value.run.return_value = pd.DataFrame()
        mock_g_scraper.return_value.run.return_value = pd.DataFrame()
        mock_sub_enrich.return_value.enrich.return_value = (pd.DataFrame(), pd.DataFrame())
        mock_int_ext.return_value.run.return_value = pd.DataFrame()

        # We don't need to mock open here because exists fails for the summary
        with self.assertLogs("main", level="WARNING") as cm:
            try:
                main_module.main()
            except SystemExit:
                pass

            # Verify recovery triggered log
            found = any("Recovery triggered: missing_or_invalid_monitoring_context" in output for output in cm.output)
            self.assertTrue(found, f"Should have logged recovery trigger due to missing context. Logs: {cm.output}")


if __name__ == "__main__":
    unittest.main()
