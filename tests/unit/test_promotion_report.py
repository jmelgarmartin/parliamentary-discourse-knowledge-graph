import json
import os
import pathlib

# Import the code to test.
# Since it's in a scripts directory, we might need to handle imports carefully
import sys
import unittest
from unittest.mock import MagicMock, patch

sys.path.append(os.path.join(os.getcwd(), "scripts"))
from promotion_report import run_report  # noqa: E402


class TestPromotionReport(unittest.TestCase):
    def setUp(self) -> None:
        self.history_path = pathlib.Path("data/validation/validation_run_history.jsonl")
        self.summary_path = pathlib.Path("data/validation/promotion_monitoring_summary.json")

    @patch("pathlib.Path.exists")
    @patch("builtins.open")
    @patch("os.makedirs")
    @patch("json.dump")
    def test_ready_for_retirement_true_when_criteria_met(
        self, mock_json_dump: MagicMock, mock_makedirs: MagicMock, mock_open_file: MagicMock, mock_exists: MagicMock
    ) -> None:
        """Verify ready_for_retirement=True when all rules are satisfied."""
        # 1. Provide a history with:
        # - Stable streaming (last 10 of evaluable are 100% match)
        # - No fallbacks in last 20
        # - Fresh enough (last full batch is recent)
        # - At least one adaptive skip

        history_entries = []
        # Add 10 evaluable matches
        for i in range(10):
            history_entries.append(
                {
                    "validation_mode": "full_batch_validation",
                    "confidence_level": "FULL_MATCH",
                    "confidence_score": 1.0,
                    "promotion_result": "PROMOTED",
                    "timestamp": "2024-01-01T00:00:00",
                }
            )

        # Add 5 adaptive skips
        for i in range(5):
            history_entries.append(
                {
                    "validation_mode": "skipped_batch",
                    "adaptive_batch_reason": "safe_adaptive_skip",
                    "promotion_result": "PROMOTED_INFERRED",
                    "timestamp": "2024-01-01T00:01:00",
                }
            )

        mock_exists.return_value = True
        mock_open_file.return_value.__enter__.return_value = [json.dumps(e) for e in history_entries]

        run_report()

        # Capture the report passed to json.dump
        report = mock_json_dump.call_args[0][0]
        readiness = report["batch_retirement_readiness"]

        self.assertTrue(readiness["ready_for_batch_retirement"])
        self.assertEqual(readiness["recommended_next_mode"], "move_batch_to_periodic_audit")
        self.assertEqual(readiness["metrics"]["recent_fallbacks"], 0)
        self.assertGreater(readiness["metrics"]["recent_adaptive_skip_count"], 0)

    @patch("pathlib.Path.exists")
    @patch("builtins.open")
    @patch("os.makedirs")
    @patch("json.dump")
    def test_ready_for_retirement_false_when_fallback_recent(
        self, mock_json_dump: MagicMock, mock_makedirs: MagicMock, mock_open_file: MagicMock, mock_exists: MagicMock
    ) -> None:
        """Verify ready_for_retirement=False if a fallback occurred recently."""
        history_entries = []
        for i in range(15):
            history_entries.append(
                {
                    "validation_mode": "full_batch_validation",
                    "confidence_level": "FULL_MATCH",
                    "confidence_score": 1.0,
                    "promotion_result": "PROMOTED",
                }
            )

        # Add one fallback in the last 20 window
        history_entries.append(
            {"validation_mode": "full_batch_validation", "confidence_level": "MISMATCH", "promotion_result": "FALLBACK"}
        )

        mock_exists.return_value = True
        mock_open_file.return_value.__enter__.return_value = [json.dumps(e) for e in history_entries]

        run_report()

        report = mock_json_dump.call_args[0][0]
        readiness = report["batch_retirement_readiness"]

        self.assertFalse(readiness["ready_for_batch_retirement"])
        self.assertEqual(readiness["recommended_next_mode"], "keep_adaptive_batch")
        self.assertIn("Detected 1 fallbacks", readiness["retirement_reason"])

    @patch("pathlib.Path.exists")
    @patch("builtins.open")
    @patch("os.makedirs")
    @patch("json.dump")
    def test_ready_for_retirement_false_when_evidence_stale(
        self, mock_json_dump: MagicMock, mock_makedirs: MagicMock, mock_open_file: MagicMock, mock_exists: MagicMock
    ) -> None:
        """Verify ready_for_retirement=False if last full batch is too old (> 30)."""
        history_entries = []
        # One full batch at the beginning
        history_entries.append(
            {
                "validation_mode": "full_batch_validation",
                "confidence_level": "FULL_MATCH",
                "confidence_score": 1.0,
                "promotion_result": "PROMOTED",
            }
        )
        # Followed by 31 skips
        for i in range(31):
            history_entries.append(
                {
                    "validation_mode": "skipped_batch",
                    "adaptive_batch_reason": "safe_adaptive_skip",
                    "promotion_result": "PROMOTED_INFERRED",
                }
            )

        mock_exists.return_value = True
        mock_open_file.return_value.__enter__.return_value = [json.dumps(e) for e in history_entries]

        run_report()

        report = mock_json_dump.call_args[0][0]
        readiness = report["batch_retirement_readiness"]

        self.assertFalse(readiness["ready_for_batch_retirement"])
        self.assertIn("Full batch evidence is too stale", readiness["retirement_reason"])


if __name__ == "__main__":
    unittest.main()
