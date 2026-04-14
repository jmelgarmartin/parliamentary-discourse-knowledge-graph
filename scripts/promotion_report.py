import json
import os
import pathlib
from collections import Counter
from datetime import datetime
from typing import Any, Dict, List, Union


def calculate_metrics(subset: List[Dict[str, Any]]) -> Dict[str, Union[int, float]]:
    """Calculates accuracy and fallback metrics for a subset of validation entries."""
    if not subset:
        return {
            "count": 0,
            "strict_match_success_rate": 0.0,
            "fallback_runs": 0,
            "fallback_rate": 0.0,
            "avg_confidence_score": 0.0,
        }

    successes = sum(1 for e in subset if e.get("confidence_level") == "FULL_MATCH")
    fallback_runs = sum(1 for e in subset if e.get("promotion_result") == "FALLBACK")
    confidence_scores = [float(e.get("confidence_score", 0.0)) for e in subset]

    count = len(subset)
    return {
        "count": count,
        "strict_match_success_rate": round(successes / count, 4),
        "fallback_runs": fallback_runs,
        "fallback_rate": round(fallback_runs / count, 4),
        "avg_confidence_score": round(sum(confidence_scores) / count, 4),
    }


def run_report() -> None:
    """Generates the promotion monitoring summary report from validation history."""
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
    full_batch_evaluable_runs = [e for e in entries if e.get("validation_mode") == "full_batch_validation"]
    streaming_only_evaluable_runs = [e for e in entries if e.get("validation_mode") == "streaming_only_validation"]

    # evaluable_entries (Legacy/Decision metrics) must remain strictly full_batch_validation
    evaluable_entries = full_batch_evaluable_runs
    skipped_entries = [
        e for e in entries if e.get("validation_mode") not in ["full_batch_validation", "streaming_only_validation"]
    ]

    # Freshness metric (Phase 24)
    # distance from current end to the last "full_batch_validation"
    runs_since_last_full_batch = 0
    if total_runs > 0:
        last_full_batch_idx = -1
        for i in range(total_runs - 1, -1, -1):
            if entries[i].get("validation_mode") == "full_batch_validation":
                last_full_batch_idx = i
                break

        if last_full_batch_idx == -1:
            runs_since_last_full_batch = total_runs
        else:
            runs_since_last_full_batch = (total_runs - 1) - last_full_batch_idx

    # 2. run_mode distribution (Global)
    run_modes = [e.get("run_mode", "unknown") for e in entries]
    run_mode_dist = dict(Counter(run_modes))

    # 3. Promotion metrics (Global)
    promotion_attempted_count = sum(1 for e in entries if e.get("promotion_attempted") is True)
    promotion_results = [e.get("promotion_result", "NOT_ATTEMPTED") for e in entries]
    promotion_dist = dict(Counter(promotion_results))

    # 4. Accuracy & Rolling Metrics (evaluable ONLY)
    global_accuracy = calculate_metrics(evaluable_entries)
    rolling_10_subset = evaluable_entries[-10:]
    rolling_20_subset = evaluable_entries[-20:]

    rolling_10 = calculate_metrics(rolling_10_subset)
    rolling_20 = calculate_metrics(rolling_20_subset)

    # 5. Stability Window & Gating
    # Rule:
    # - At least 5 evaluable runs in the last 10 entries
    # - 100% strict match success rate (FULL_MATCH)
    # - 0% fallback rate
    # - 1.0 average confidence score

    stable = False
    rule = "Min 5 evaluable runs, 100% SUCCESS, 0% FALLBACK, 1.0 AVG_CONF (Last 10)"
    reason = ""

    if len(rolling_10_subset) < 5:
        stable = False
        reason = f"Insufficient evaluable data in window (current: {len(rolling_10_subset)} runs, need 5)"
    elif (
        rolling_10["strict_match_success_rate"] >= 1.0
        and rolling_10["fallback_rate"] == 0.0
        and rolling_10["avg_confidence_score"] >= 1.0
    ):
        stable = True
        reason = "Stability window requirements met (100% success, 0% fallback, 1.0 confidence)."
    else:
        stable = False
        reason = (
            f"Stability window requirements NOT met: "
            f"success={rolling_10['strict_match_success_rate']:.2%}, "
            f"fallback={rolling_10['fallback_rate']:.2%}, "
            f"avg_conf={rolling_10['avg_confidence_score']:.2f}"
        )

    report = {
        "overall_counts": {
            "total_runs": total_runs,
            "full_batch_evaluable_runs": len(full_batch_evaluable_runs),
            "streaming_only_evaluable_runs": len(streaming_only_evaluable_runs),
            "skipped_runs": len(skipped_entries),
            "runs_since_last_full_batch": runs_since_last_full_batch,
        },
        "run_mode_distribution": run_mode_dist,
        "promotion_metrics": {"attempted": promotion_attempted_count, "result_distribution": promotion_dist},
        "accuracy_metrics": global_accuracy,
        "stability_metrics": {
            "rolling_metrics_last_10": {**rolling_10, "stability_window_evaluable_runs": len(rolling_10_subset)},
            "rolling_metrics_last_20": {**rolling_20, "stability_window_evaluable_runs": len(rolling_20_subset)},
            "stable_streaming": stable,
            "stability_rule_applied": rule,
            "stability_reason": reason,
            "runs_since_last_full_batch": runs_since_last_full_batch,
        },
        "recommendation": {
            "ready_for_default_promotion": stable,  # Synced with stability for now
            "rule_applied": rule,
            "reason": reason,
        },
        "generated_at": datetime.now().isoformat(),
    }

    os.makedirs(output_path.parent, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=4)

    print(f"Report generated successfully: {output_path}")
    print(
        f">>> Streaming stability window | evaluable_runs(batch)={len(rolling_10_subset)} | "
        f"success={rolling_10['strict_match_success_rate']:.2%} | stable={str(stable).lower()}"
    )
    inferred_promotions = promotion_dist.get("PROMOTED_INFERRED", 0)
    print(
        f">>> Inferred coverage | streaming_only_evaluable_runs={len(streaming_only_evaluable_runs)} | "
        f"inferred_promotions={inferred_promotions}"
    )
    print(f">>> Adaptive batch | runs_since_last_full_batch={runs_since_last_full_batch}")

    if stable:
        print(">>> STATUS: STABLE streaming pipeline.")
    else:
        print(">>> STATUS: UNSTABLE streaming pipeline.")


if __name__ == "__main__":
    run_report()
