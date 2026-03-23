import json
import os
import pathlib
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Union


def run_report() -> None:
    history_path = pathlib.Path("data/validation/validation_run_history.jsonl")
    output_path = pathlib.Path("data/validation/promotion_monitoring_summary.json")

    entries = []
    if not history_path.exists():
        print(f"Warning: {history_path} not found. Generating empty summary.")
    else:
        with open(history_path, "r", encoding="utf-8") as f:
            for i, line in enumerate(f, 1):
                line = line.strip()
                if not line:
                    continue
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError as e:
                    print(f"Warning: Malformed JSON at line {i}: {e}")

    # 1. Distinguish runs
    total_runs = len(entries)
    evaluable_entries = [e for e in entries if e.get("parity_status") != "SKIPPED"]
    skipped_entries = [e for e in entries if e.get("parity_status") == "SKIPPED"]

    # 2. run_mode distribution (Global)
    run_modes = [e.get("run_mode", "unknown") for e in entries]
    run_mode_dist = dict(Counter(run_modes))

    # 3. Promotion metrics (Global)
    promotion_attempted_count = sum(1 for e in entries if e.get("promotion_attempted") is True)
    promotion_results = [e.get("promotion_result", "NOT_ATTEMPTED") for e in entries]
    promotion_dist = dict(Counter(promotion_results))

    def calculate_metrics(subset: List[Dict[str, Any]]) -> Dict[str, Union[int, float]]:
        if not subset:
            return {"count": 0, "strict_match_success_rate": 0.0, "avg_confidence_score": 0.0}

        successes = sum(
            1
            for e in subset
            if e.get("parity_status") == "MATCH"
            and e.get("doc_level_parity") == "MATCH"
            and e.get("row_level_parity") == "MATCH"
        )
        confidence_scores = [float(e.get("confidence_score", 0.0)) for e in subset]

        return {
            "count": len(subset),
            "strict_match_success_rate": round(successes / len(subset), 4),
            "avg_confidence_score": round(sum(confidence_scores) / len(subset), 4),
        }

    # 4. Accuracy & Rolling Metrics (evaluable ONLY)
    global_accuracy = calculate_metrics(evaluable_entries)
    rolling_10 = calculate_metrics(evaluable_entries[-10:])
    rolling_20 = calculate_metrics(evaluable_entries[-20:])

    # 5. Readiness Heuristic
    # Rule: 100% strict match success rate in the last 10 evaluable runs, with at least 5 evaluable runs total.
    ready = False
    rule = "100% strict match success rate in last 10 evaluable runs (min 5 runs)"
    reason = ""

    if len(evaluable_entries) < 5:
        ready = False
        reason = f"Insufficient evaluable data (current: {len(evaluable_entries)} runs, need 5)"
    elif rolling_10["strict_match_success_rate"] >= 1.0:
        ready = True
        reason = "Perfect parity achieved across all recent evaluable runs."
    else:
        ready = False
        reason = f"Recent success rate ({rolling_10['strict_match_success_rate']:.2%}) is below 100%."

    report = {
        "overall_counts": {
            "total_runs": total_runs,
            "evaluable_runs": len(evaluable_entries),
            "skipped_runs": len(skipped_entries),
        },
        "run_mode_distribution": run_mode_dist,
        "promotion_metrics": {"attempted": promotion_attempted_count, "result_distribution": promotion_dist},
        "accuracy_metrics": global_accuracy,
        "rolling_metrics_last_10": rolling_10,
        "rolling_metrics_last_20": rolling_20,
        "recommendation": {"ready_for_default_promotion": ready, "rule_applied": rule, "reason": reason},
        "generated_at": datetime.now().isoformat(),
    }

    os.makedirs(output_path.parent, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=4)

    print(f"Report generated successfully: {output_path}")
    if ready:
        print(">>> RECOMMENDATION: READY for default promotion.")
    else:
        print(">>> RECOMMENDATION: NOT READY yet.")


if __name__ == "__main__":
    run_report()
