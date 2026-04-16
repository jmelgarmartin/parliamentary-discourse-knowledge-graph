import datetime
import json
import subprocess


def run_command(cmd_list):
    print(f"Executing: {' '.join(cmd_list)}")
    result = subprocess.run(cmd_list, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
    return result


def main():
    campaign_id = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    history_file = "data/validation/validation_run_history.jsonl"
    diagnosis_file = "data/validation/streaming_empty_records_diagnosis.json"

    print(f"Starting post-hardening revalidation campaign: {campaign_id}")

    # 1. Clear state for a known problematic document to force re-download
    # Using DSCD-15-PL-176 as it was the reproduction case
    print("Clearing state for DSCD-15-PL-176...")
    subprocess.run(
        [
            "e:/CONGRESO/congreso-analisis-historico/.venv/Scripts/python.exe",
            "-c",
            "import duckdb; conn = duckdb.connect('state/bronze.duckdb'); conn.execute(\"DELETE FROM bronze_documents WHERE document_id = 'DSCD-15-PL-176'\"); conn.close()",
        ]
    )

    scenarios = [
        ["src/main.py", "--batch-strategy", "sampled", "--term", "15"],
        ["src/main.py", "--batch-strategy", "adaptive", "--term", "15"],
        ["src/main.py", "--batch-strategy", "periodic_audit", "--term", "15"],
    ]

    for cmd in scenarios:
        full_cmd = ["e:/CONGRESO/congreso-analisis-historico/.venv/Scripts/python.exe"] + cmd
        run_command(full_cmd)

    # 2. Analyze results
    with open(history_file, "r") as f:
        history_lines = [json.loads(line) for line in f]

    # Analyze only POST-FIX runs (Phase 30 start was around 2026-04-16T11:20:00)
    cutoff = "2026-04-16T11:20:00"
    post_fix_runs = [r for r in history_lines if r.get("timestamp", "") > cutoff]
    pre_fix_runs = [r for r in history_lines if r.get("timestamp", "") <= cutoff]

    # Metrics calculation
    def calc_metrics(runs):
        return {
            "total": len(runs),
            "mismatches": sum(1 for r in runs if r.get("parity_status") == "MISMATCH"),
            "empty_streaming": sum(
                1 for r in runs if r.get("parity_status") == "MISMATCH" and r.get("docs_compared") == 0
            ),
            "corrective_audits": sum(
                1
                for r in runs
                if r.get("promotion_result") == "FALLBACK" and r.get("validation_mode") == "full_batch_validation"
            ),
            "full_match_rate": (sum(1 for r in runs if r.get("parity_status") == "MATCH") / len(runs)) if runs else 0,
        }

    pre_metrics = calc_metrics(pre_fix_runs[-20:])  # Last 20 pre-fix runs
    post_metrics = calc_metrics(post_fix_runs)

    summary = {
        "campaign_id": campaign_id,
        "total_campaign_runs": len(post_fix_runs),
        "post_fix_metrics": post_metrics,
        "pre_fix_comparison_baseline": pre_metrics,
        "empty_streaming_records_detected": post_metrics["empty_streaming"] > 0,
        "readiness_summary": "Improved"
        if post_metrics["empty_streaming"] == 0 and pre_metrics["empty_streaming"] > 0
        else "Unchanged",
    }

    out_path = "data/validation/post_hardening_revalidation_summary.json"
    with open(out_path, "w") as f:
        json.dump(summary, f, indent=4)

    print(f"Campaign summary saved to {out_path}")

    # 3. Refresh promotion report
    print("Refreshing promotion report...")
    run_command(["e:/CONGRESO/congreso-analisis-historico/.venv/Scripts/python.exe", "scripts/promotion_report.py"])


if __name__ == "__main__":
    main()
