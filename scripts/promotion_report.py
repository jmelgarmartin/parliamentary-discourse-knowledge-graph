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

    # 6. Active Recovery Monitoring (formerly Recovery-Only Readiness)
    recent_entry_window = 20
    recent_entries = entries[-recent_entry_window:]

    # Audit Classifications (only for batch runs)
    audit_entries = [entry for entry in recent_entries if entry.get("batch_executed") is True]

    corrective_audits = []
    confirmatory_audits = []
    redundant_audits = []

    for entry in audit_entries:
        # Corrective if it detected a mismatch or triggered a fallback
        if entry.get("parity_status") == "MISMATCH" or entry.get("promotion_result") == "FALLBACK":
            corrective_audits.append(entry)
            audit_type = "corrective"
        # Confirmatory if it matched and allowed promotion
        elif entry.get("parity_status") == "MATCH" and entry.get("promotion_result") == "PROMOTED":
            confirmatory_audits.append(entry)
            audit_type = "confirmatory"
        else:
            redundant_audits.append(entry)
            audit_type = "redundant"

        # Logging classification as requested in Phase 27
        if "timestamp" in entry:
            print(f"Audit classification | timestamp={entry['timestamp']} | type={audit_type}")

    recent_audit_count = len(audit_entries)
    corrective_count = len(corrective_audits)
    confirmatory_count = len(confirmatory_audits)
    redundant_count = len(redundant_audits)

    corrective_rate = round(corrective_count / recent_audit_count, 4) if recent_audit_count > 0 else 0.0
    confirmatory_rate = round(confirmatory_count / recent_audit_count, 4) if recent_audit_count > 0 else 0.0
    redundant_rate = round(redundant_count / recent_audit_count, 4) if recent_audit_count > 0 else 0.0

    full_match_audits = sum(1 for entry in audit_entries if entry.get("confidence_level") == "FULL_MATCH")
    recent_audit_full_match_rate = round(full_match_audits / recent_audit_count, 4) if recent_audit_count > 0 else 0.0

    # runs_since_last_mismatch
    # Mismatch is parity_status == "MISMATCH"
    runs_since_last_mismatch = 0
    mismatch_found = False
    if total_runs > 0:
        for i in range(total_runs - 1, -1, -1):
            if entries[i].get("parity_status") == "MISMATCH":
                runs_since_last_mismatch = (total_runs - 1) - i
                mismatch_found = True
                break
        if not mismatch_found:
            # Documented convention: if never matched, use total historical runs
            runs_since_last_mismatch = total_runs

    recovery_rule = (
        "stable_streaming == True AND recent_mismatch_count == 0 AND "
        "recent_audit_full_match_rate == 1.0 AND runs_since_last_mismatch >= 50"
    )

    recovery_reasons = []
    if not stable:
        recovery_reasons.append("Streaming pipeline is not yet stable")

    # recent_audit_mismatch_count
    recent_audit_mismatch_count = sum(1 for entry in audit_entries if entry.get("parity_status") == "MISMATCH")
    if recent_audit_mismatch_count > 0:
        recovery_reasons.append(f"Detected {recent_audit_mismatch_count} mismatches in the audit window")

    if recent_audit_full_match_rate < 1.0:
        recovery_reasons.append(f"Audit full match rate ({recent_audit_full_match_rate:.2%}) is below 100%")

    if runs_since_last_mismatch < 50:
        mismatch_context = (
            f"(last mismatch {runs_since_last_mismatch} runs ago)" if mismatch_found else "(never matched)"
        )
        recovery_reasons.append(f"Insufficient history since last mismatch {mismatch_context}, need 50 runs")

    ready_for_recovery_only = len(recovery_reasons) == 0
    recovery_readiness = {
        "ready": ready_for_recovery_only,
        "recommended_batch_mode": "recovery_only",
        "rule_applied": recovery_rule,
        "reason": "Criteria met" if ready_for_recovery_only else "; ".join(recovery_reasons),
        "metrics": {
            "recent_periodic_audit_count": recent_audit_count,
            "recent_periodic_audit_match_rate": round(
                (recent_audit_count - recent_audit_mismatch_count) / recent_audit_count, 4
            )
            if recent_audit_count > 0
            else 1.0,
            "recent_periodic_audit_mismatch_count": recent_audit_mismatch_count,
            "recent_periodic_audit_full_match_rate": recent_audit_full_match_rate,
            "runs_since_last_mismatch": runs_since_last_mismatch,
            "mismatch_history_context": "last_mismatch_found" if mismatch_found else "no_mismatch_in_history",
            "audit_effectiveness": {
                "corrective_audit_count": corrective_count,
                "corrective_audit_rate": corrective_rate,
                "confirmatory_audit_count": confirmatory_count,
                "confirmatory_audit_rate": confirmatory_rate,
                "redundant_audit_count": redundant_count,
                "redundant_audit_rate": redundant_rate,
            },
        },
    }

    # 7. Legacy Audit Readiness (DEPRECATED)
    recent_fallbacks = sum(1 for e in recent_entries if e.get("promotion_result") == "FALLBACK")
    recent_full_batch_count = sum(1 for e in recent_entries if e.get("validation_mode") == "full_batch_validation")
    recent_streaming_only_count = sum(
        1 for e in recent_entries if e.get("validation_mode") == "streaming_only_validation"
    )
    recent_inferred_promotion_count = sum(1 for e in recent_entries if e.get("promotion_result") == "PROMOTED_INFERRED")
    recent_adaptive_skip_count = sum(
        1 for e in recent_entries if e.get("adaptive_batch_reason") == "safe_adaptive_skip"
    )
    # Phase 26 metrics
    recent_periodic_audit_count_v26 = sum(
        1 for e in recent_entries if e.get("batch_strategy") == "periodic_audit" and e.get("batch_executed") is True
    )
    recent_periodic_audit_skip = sum(
        1 for e in recent_entries if e.get("batch_strategy") == "periodic_audit" and e.get("batch_executed") is False
    )

    # Phase 32 metrics: recovery_only
    recent_recovery_only_runs = [e for e in recent_entries if e.get("batch_strategy") == "recovery_only"]
    recent_recovery_triggered = sum(1 for e in recent_recovery_only_runs if e.get("recovery_triggered") is True)
    recent_recovery_skipped = sum(1 for e in recent_recovery_only_runs if e.get("recovery_triggered") is False)

    reactivations = sum(1 for e in recent_entries if e.get("batch_required_by_rule") is True)
    batch_reactivation_rate = round(reactivations / len(recent_entries), 4) if recent_entries else 0.0

    retirement_rule = (
        "stable_streaming == True AND recent_fallbacks == 0 AND "
        "runs_since_last_full_batch < 30 AND recent_adaptive_skip_count > 0"
    )

    ready_reasons = []
    if not stable:
        ready_reasons.append("Streaming pipeline is not yet stable")
    if recent_fallbacks > 0:
        ready_reasons.append(f"Detected {recent_fallbacks} fallbacks in the last {recent_entry_window} runs")
    if runs_since_last_full_batch >= 30:
        ready_reasons.append(
            f"Full batch evidence is too stale (runs since last: {runs_since_last_full_batch}, threshold: 30)"
        )
    if recent_adaptive_skip_count == 0:
        ready_reasons.append("Adaptive batch skip has not been exercised successfully in recent runs")

    ready_for_retirement = len(ready_reasons) == 0
    recommended_mode = "move_batch_to_periodic_audit" if ready_for_retirement else "keep_adaptive_batch"

    retirement_readiness = {
        "ready_for_batch_retirement": ready_for_retirement,
        "recommended_next_mode": recommended_mode,
        "retirement_rule_applied": retirement_rule,
        "retirement_reason": "Criteria met" if ready_for_retirement else "; ".join(ready_reasons),
        "metrics": {
            "recent_window_size": len(recent_entries),
            "recent_fallbacks": recent_fallbacks,
            "recent_full_batch_count": recent_full_batch_count,
            "recent_streaming_only_count": recent_streaming_only_count,
            "recent_inferred_promotion_count": recent_inferred_promotion_count,
            "recent_adaptive_skip_count": recent_adaptive_skip_count,
            "recent_periodic_audit_count": recent_periodic_audit_count_v26,
            "recent_periodic_audit_skip": recent_periodic_audit_skip,
            "recent_recovery_only_count": len(recent_recovery_only_runs),
            "recent_recovery_triggered": recent_recovery_triggered,
            "recent_recovery_skipped": recent_recovery_skipped,
            "batch_reactivation_rate": batch_reactivation_rate,
            "runs_since_last_full_batch": runs_since_last_full_batch,
            "audit_freshness_status": runs_since_last_full_batch < 10,
        },
    }

    report = {
        "overall_counts": {
            "total_runs": total_runs,
            "full_batch_evaluable_runs": len(full_batch_evaluable_runs),
            "streaming_only_evaluable_runs": len(streaming_only_evaluable_runs),
            "skipped_runs": len(skipped_entries),
            "runs_since_last_full_batch": runs_since_last_full_batch,
            "runs_since_last_mismatch": runs_since_last_mismatch,
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
        "legacy_audit_readiness": retirement_readiness,
        "active_recovery_monitoring": recovery_readiness,
        "recommendation": {
            "recommended_batch_mode": "recovery_only",
            "ready_for_default_promotion": stable,
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
    print(
        f">>> Batch retirement readiness | ready={str(ready_for_retirement).lower()} | "
        f"mode={recommended_mode} | reason={retirement_readiness['retirement_reason']}"
    )

    if stable:
        print(">>> STATUS: STABLE streaming pipeline.")
    else:
        print(">>> STATUS: UNSTABLE streaming pipeline.")


if __name__ == "__main__":
    run_report()
