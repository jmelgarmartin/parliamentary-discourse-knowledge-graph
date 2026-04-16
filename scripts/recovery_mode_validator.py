import json
import os
import pathlib
import shutil
import subprocess
import time
from datetime import datetime
from typing import Any, Dict, List


def run_pipeline(batch_strategy: str = "recovery_only") -> Dict[str, Any]:
    """Executes a single pipeline run and returns the summary metadata."""
    cmd = [
        "e:\\CONGRESO\\congreso-analisis-historico\\.venv\\Scripts\\python.exe",
        "src/main.py",
        "--term",
        "15",
        "--batch-strategy",
        batch_strategy,
    ]

    print(f"Executing: {' '.join(cmd)}")
    env = os.environ.copy()
    env["PYTHONUTF8"] = "1"
    result = subprocess.run(cmd, capture_output=True, text=True, encoding="utf-8", errors="replace", env=env)

    if result.returncode != 0:
        print(f"Pipeline run failed with exit code {result.returncode}")
        print(f"Stderr: {result.stderr}")
        return {}

    # The actual path in src/main.py is data/validation/legislature={args.term}/validation_run_summary.json
    summary_path = pathlib.Path("data/validation/legislature=15/validation_run_summary.json")
    if summary_path.exists():
        with open(summary_path, "r", encoding="utf-8") as f:
            return json.load(f)
    else:
        print(f"Warning: Summary file not found at {summary_path}")
    return {}


def aggregate_results(runs: List[Dict[str, Any]], context_restored: bool) -> Dict[str, Any]:
    """Aggregates per-run results into a final summary."""
    total_runs = len(runs)
    stable_runs = sum(1 for r in runs if r.get("scenario_type") == "stable")
    degradation_runs = sum(1 for r in runs if r.get("scenario_type") == "controlled_degradation")

    recovery_triggered_count = sum(1 for r in runs if r.get("recovery_triggered") is True)
    batch_execution_count = sum(1 for r in runs if r.get("batch_executed") is True)
    mismatch_count = sum(1 for r in runs if r.get("parity_status") == "MISMATCH")
    fallback_count = sum(1 for r in runs if r.get("promotion_result") == "FALLBACK")

    # Stability rate: runs without fallback or mismatch
    stability_count = sum(
        1 for r in runs if r.get("parity_status") != "MISMATCH" and r.get("promotion_result") != "FALLBACK"
    )
    stability_rate = round(stability_count / total_runs, 4) if total_runs > 0 else 0.0

    return {
        "campaign_metadata": {
            "timestamp": datetime.now().isoformat(),
            "target_strategy": "recovery_only",
            "success_criteria": {"stable_trigger_rate": 0, "stable_batch_rate": 0, "degradation_trigger_success": True},
        },
        "aggregate_metrics": {
            "total_runs": total_runs,
            "stable_runs": stable_runs,
            "degradation_runs": degradation_runs,
            "recovery_triggered_count": recovery_triggered_count,
            "recovery_trigger_rate": round(recovery_triggered_count / total_runs, 4) if total_runs > 0 else 0.0,
            "batch_execution_rate": round(batch_execution_count / total_runs, 4) if total_runs > 0 else 0.0,
            "mismatch_count": mismatch_count,
            "fallback_count": fallback_count,
            "stability_rate": stability_rate,
            "monitoring_context_restored": context_restored,
        },
        "run_history": runs,
    }


def initialize_stable_monitoring(summary_path: pathlib.Path):
    """Creates a 'clean' stable monitoring summary."""
    if summary_path.exists():
        with open(summary_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        data["stability_metrics"]["stable_streaming"] = True
        data["stability_metrics"]["reason"] = "STABLE_INITIALIZATION_FOR_VALIDATION"
        data.setdefault("stability_metrics", {}).setdefault("rolling_metrics_last_10", {})["fallback_rate"] = 0.0

        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        print("Initialized monitoring summary to STABLE state.")


def main():
    summary_path = pathlib.Path("data/validation/promotion_monitoring_summary.json")
    backup_path = pathlib.Path("data/validation/promotion_monitoring_summary.json.bak")
    report_path = pathlib.Path("data/validation/recovery_mode_validation_summary.json")

    runs_data = []
    context_restored = False

    # 0. Backup original state
    if summary_path.exists():
        print(f"Backing up original monitoring summary to {backup_path}")
        shutil.copy2(summary_path, backup_path)

    try:
        # 1. Stable Campaign (10 runs)
        print("\n>>> Starting STABLE campaign (10 runs)")
        initialize_stable_monitoring(summary_path)

        for i in range(10):
            print(f"Stable Run {i+1}/10")
            summary = run_pipeline()
            if summary:
                summary["scenario_type"] = "stable"
                summary["fallback_occurred"] = summary.get("promotion_result") == "FALLBACK"
                runs_data.append(summary)
                time.sleep(2)  # Increased delay to avoid backup collisions

        # 2. Controlled Degradation (2 runs)
        print("\n>>> Starting CONTROLLED DEGRADATION campaign (2 runs)")
        # Create a degraded summary
        with open(summary_path, "r", encoding="utf-8") as f:
            data = json.load(f)

        data["stability_metrics"]["stable_streaming"] = False
        data["stability_metrics"]["reason"] = "FORCED_DEGRADATION_FOR_VALIDATION"

        with open(summary_path, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=4)
        print("Initialized monitoring summary to UNSTABLE state.")

        for i in range(2):
            print(f"Degradation Run {i+1}/2")
            summary = run_pipeline()
            if summary:
                summary["scenario_type"] = "controlled_degradation"
                summary["fallback_occurred"] = summary.get("promotion_result") == "FALLBACK"
                runs_data.append(summary)
            time.sleep(2)

    finally:
        if backup_path.exists():
            print(f"Restoring original monitoring summary from {backup_path}")
            shutil.copy2(backup_path, summary_path)
            os.remove(backup_path)
            context_restored = True
        else:
            print("No backup found to restore.")

    # 3. Aggregate and Save
    print("\n>>> Finalizing validation report")
    report = aggregate_results(runs_data, context_restored)

    os.makedirs(report_path.parent, exist_ok=True)
    with open(report_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=4)

    print(f"Validation summary generated at: {report_path}")
    print(
        f"Aggregate metrics: Triggers={report['aggregate_metrics']['recovery_triggered_count']} | Batch Exec={report['aggregate_metrics']['batch_execution_rate']:.2%}"
    )


if __name__ == "__main__":
    main()
