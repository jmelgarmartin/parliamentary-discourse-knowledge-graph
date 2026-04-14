"""
Main execution pipeline for the Parliamentary Discourse Knowledge Graph project.
"""

import argparse
import logging
import os
import pathlib
import sys
import time
from datetime import datetime
from typing import Any, Dict, List

import pandas as pd
from congress_analysis.backup_manager import BackupManager

# Allow execution of main.py within the src/ directory path context.
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from congress_analysis.ingestion.scrappers.deputies_scraper import DeputiesScraper  # noqa: E402
from congress_analysis.ingestion.scrappers.groups_scraper import GroupsScraper  # noqa: E402
from congress_analysis.ingestion.scrappers.sessions_scraper import SessionsScraper  # noqa: E402
from congress_analysis.ingestion.transformers.substitutions_enricher import SubstitutionsEnricher  # noqa: E402
from congress_analysis.silver.enrich_legislature import run_enrichment as run_interventions_enrichment  # noqa: E402
from congress_analysis.silver.interventions_extractor import InterventionsExtractor  # noqa: E402


def setup_logging(process_name: str, log_level: str = "INFO") -> None:
    """Configures the root logger."""
    os.makedirs("logs", exist_ok=True)
    handlers = logging.root.handlers[:]
    for handler in handlers:
        logging.root.removeHandler(handler)

    # Format: YYYY-MM-DD_HHMM followed by the process name (e.g., 2026-03-03_1652_pipeline_execution.log)
    timestamp = datetime.now().strftime("%Y-%m-%d_%H%M")
    log_filename = f"{timestamp}_{process_name}.log"

    numeric_level = getattr(logging, log_level.upper(), logging.INFO)

    logging.basicConfig(
        level=numeric_level,
        format="[%(asctime)s] %(levelname)s [%(name)s] %(message)s",
        handlers=[
            logging.FileHandler(f"logs/{log_filename}", mode="w", encoding="utf-8"),
            logging.StreamHandler(),
        ],
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Execute the complete Bronze-to-Silver ingestion pipeline.")
    parser.add_argument("--term", default="15", help="Legislative term number to process (default: 15)")
    parser.add_argument("--driver-path", default=None, help="Path to the ChromeDriver executable")
    parser.add_argument("--state-path", default="state/bronze.duckdb", help="Path to the DuckDB state file")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"],
        help="Set the logging level (default: INFO)",
    )
    parser.add_argument(
        "--allow-inferred-promotion",
        action="store_true",
        help="Allow promotion based on streaming-only validation",
    )
    parser.add_argument(
        "--no-headless", action="store_false", dest="headless", help="Run browser in non-headless mode (visible GUI)"
    )
    parser.add_argument(
        "--disable-streaming",
        action="store_true",
        help="Forces pure batch mode by disabling all experimental streaming logic and validation.",
    )
    parser.add_argument(
        "--experimental-streaming",
        action="store_true",
        help=(
            "Optional explicit flag to enable in-memory streaming extraction "
            "in shadow mode (active by default unless --disable-streaming is used)."
        ),
    )
    parser.add_argument(
        "--use-streaming-candidate",
        action="store_true",
        help=(
            "Enable evaluation of the streaming candidate path. "
            "Downstream source selection still defaults to batch unless --promote-streaming is used."
        ),
    )
    parser.add_argument(
        "--promote-streaming",
        action="store_true",
        help=(
            "Explicitly promotes streaming candidate to downstream processing if validation passes "
            "(strict_match required). Falls back to batch otherwise."
        ),
    )
    parser.add_argument(
        "--streaming-confidence-threshold",
        type=float,
        default=None,
        help="Confidence threshold [0.0, 1.0] to promote streaming candidate (requires --use-streaming-candidate)",
    )
    parser.add_argument(
        "--batch-strategy",
        choices=["always", "sampled", "disabled_only_if_stable", "adaptive", "periodic_audit"],
        default="always",
        help="Strategy for batch execution (always, sampled, disabled_only_if_stable, adaptive, or periodic_audit).",
    )
    parser.add_argument(
        "--batch-freshness-window",
        type=int,
        default=5,
        help="Run batch every Nth run if strategy is 'adaptive' and evidence is stale (default: 5).",
    )
    parser.add_argument(
        "--batch-sample-every",
        type=int,
        default=5,
        help="Run batch every Nth execution when strategy is 'sampled' (default: 5).",
    )
    parser.add_argument(
        "--batch-audit-every",
        type=int,
        default=10,
        help="Run batch every Nth execution when strategy is 'periodic_audit' (default: 10).",
    )

    args = parser.parse_args()

    setup_logging("pipeline_execution", log_level=args.log_level)
    logger = logging.getLogger(__name__)

    # --- CLI Validation for Default Shadow Mode (Phase 20A) ---
    streaming_flags = [
        args.experimental_streaming,
        args.use_streaming_candidate,
        args.promote_streaming,
        args.streaming_confidence_threshold is not None,
    ]

    if args.streaming_confidence_threshold is not None:
        if not (0.0 <= args.streaming_confidence_threshold <= 1.0):
            logger.error(
                f"Invalid streaming confidence threshold: {args.streaming_confidence_threshold}. Must be in [0.0, 1.0]."
            )
            sys.exit(1)

    if args.disable_streaming:
        if any(streaming_flags):
            logger.error("--disable-streaming is authoritative and cannot be combined with any streaming-related flags")
            sys.exit(1)

    if args.promote_streaming:
        if not args.use_streaming_candidate:
            logger.error("--promote-streaming requires --use-streaming-candidate")
            sys.exit(1)
        if args.streaming_confidence_threshold is not None:
            logger.error("--promote-streaming is incompatible with --streaming-confidence-threshold")
            sys.exit(1)

    if args.allow_inferred_promotion:
        if args.disable_streaming:
            logger.error("--allow-inferred-promotion is incompatible with --disable-streaming")
            sys.exit(1)

    # --- Phase 20A: Determine Default Shadow Mode ---
    default_streaming_active = not args.disable_streaming
    experimental_streaming = args.experimental_streaming or default_streaming_active

    if default_streaming_active:
        logger.info("Streaming default guarded mode active | automatic promotion enabled | strict_match required")

    # --- Phase 22A: Batch Sampling Strategy (Hardened) ---
    batch_strategy = args.batch_strategy
    run_batch = True
    batch_skip_reason = None
    stable_streaming = False
    total_runs_history = 0
    accuracy_metrics = {
        "strict_match_success_rate": 0.0,
        "fallback_rate": 1.0,
        "avg_confidence_score": 0.0,
    }
    summary_valid = False
    inferred_promotion_allowed = False
    runs_since_last_full_batch = 0
    rolling_fallback_rate = 1.0
    batch_validation_fresh = True
    batch_required_by_rule = False
    adaptive_batch_reason = None

    # Phase 26: Periodic Audit Metadata
    batch_audit_due = False
    batch_audit_reason = ""
    periodic_audit_mode_active = args.batch_strategy == "periodic_audit"

    # Output variables initialization (Safety/Observability)
    selected_source = None
    promotion_result = "SKIPPED"
    fallback_reason = None
    confidence_level = "SKIPPED"
    confidence_score = 0.0
    parity_status = "SKIPPED"
    doc_level_parity = "SKIPPED"
    row_level_parity = "SKIPPED"
    docs_compared = 0
    mismatched_docs: List[Dict[str, Any]] = []
    promotion_basis = "none"
    confidence_metrics = {
        "global_match_ratio": 0.0,
        "document_match_ratio": 0.0,
        "row_identity_match_ratio": 0.0,
        "confidence_score": 0.0,
        "confidence_level": "SKIPPED",
    }
    selection_policy = "none"
    skip_reason = None
    promotion_attempted = False
    validation_mode = "skipped_batch"
    execution_mode = "default_shadow" if default_streaming_active else "batch_only"
    if args.disable_streaming:
        execution_mode = "batch_only"

    # Try to load stability context with robust fallback
    summary_path = pathlib.Path("data/validation/promotion_monitoring_summary.json")
    if summary_path.exists():
        try:
            import json

            with open(summary_path, "r", encoding="utf-8") as f:
                content = f.read()
                if content and isinstance(content, str) and content.strip().startswith("{"):
                    summary_data = json.loads(content)
                    if "stability_metrics" in summary_data and "overall_counts" in summary_data:
                        stable_streaming = summary_data["stability_metrics"].get("stable_streaming")
                        total_runs_history = summary_data["overall_counts"].get("total_runs")
                        if "accuracy_metrics" in summary_data:
                            accuracy_metrics.update(summary_data["accuracy_metrics"])

                        if stable_streaming is not None and total_runs_history is not None:
                            summary_valid = True
                            runs_since_last_full_batch = summary_data["stability_metrics"].get(
                                "runs_since_last_full_batch", 0
                            )
                            rolling_fallback_rate = (
                                summary_data["stability_metrics"]
                                .get("rolling_metrics_last_10", {})
                                .get("fallback_rate", 1.0)
                            )
        except Exception as e:
            logger.warning(f"Malformed or invalid stability summary: {e}")

    # Decision Logic
    if not summary_valid and batch_strategy != "always":
        run_batch = True
        batch_skip_reason = "missing_or_invalid_summary_fallback"
        batch_audit_due = True
        batch_audit_reason = "missing_or_invalid_monitoring_context"
    elif batch_strategy == "always":
        run_batch = True
        batch_skip_reason = "always_run"
    elif batch_strategy == "disabled_only_if_stable":
        if stable_streaming:
            run_batch = False
            batch_skip_reason = "stable_streaming"
        else:
            run_batch = True
            batch_skip_reason = "not_stable"
    elif batch_strategy == "sampled":
        # Deterministic sampling based on history count (Hardened)
        if (total_runs_history + 1) % args.batch_sample_every == 0:
            run_batch = True
            batch_skip_reason = "sample_selected"
        else:
            run_batch = False
            batch_skip_reason = "sample_not_selected"
    elif batch_strategy == "adaptive":
        # Rule C: Freshness check
        batch_validation_fresh = runs_since_last_full_batch < args.batch_freshness_window

        if not summary_valid:
            run_batch = True
            batch_required_by_rule = True
            adaptive_batch_reason = "missing_or_invalid_monitoring_context"
        elif not stable_streaming:
            # Rule A: Stability
            run_batch = True
            batch_required_by_rule = True
            adaptive_batch_reason = "unstable_streaming"
        elif rolling_fallback_rate > 0:
            # Rule B: Recent fallback (Phase 24 Rule B)
            run_batch = True
            batch_required_by_rule = True
            adaptive_batch_reason = f"recent_fallback_detected (rate={rolling_fallback_rate})"
        elif not batch_validation_fresh:
            # Rule C: Freshness
            run_batch = True
            batch_required_by_rule = True
            adaptive_batch_reason = f"stale_evidence (runs_since_last_batch={runs_since_last_full_batch})"
        else:
            # Rule D: Ready
            run_batch = False
            batch_required_by_rule = False
            adaptive_batch_reason = "safe_adaptive_skip"

        batch_skip_reason = adaptive_batch_reason
    elif batch_strategy == "periodic_audit":
        # Rule A: Unstable
        if not stable_streaming:
            batch_audit_due = True
            batch_audit_reason = "unstable_streaming"
        # Rule B: Recent fallback (Phase 26 Trigger B)
        elif rolling_fallback_rate > 0:
            batch_audit_due = True
            batch_audit_reason = f"recent_fallback_detected (rate={rolling_fallback_rate})"
        # Rule C: Audit due (freshness)
        elif runs_since_last_full_batch >= args.batch_audit_every:
            batch_audit_due = True
            batch_audit_reason = f"audit_due (runs_since_last: {runs_since_last_full_batch})"

        if batch_audit_due:
            run_batch = True
            batch_skip_reason = f"audit_trigger: {batch_audit_reason}"
        else:
            run_batch = False
            batch_skip_reason = "periodic_audit_skip"
            batch_required_by_rule = False

    def is_streaming_validation_eligible(stable: bool, metrics: Dict[str, Any], streaming_success: bool) -> bool:
        """Determines if a run can be classified as streaming_only_validation."""
        return (
            stable is True
            and metrics.get("strict_match_success_rate", 0.0) >= 1.0
            and metrics.get("fallback_rate", 1.0) == 0.0
            and metrics.get("avg_confidence_score", 0.0) >= 0.95
            and streaming_success is True
        )

    if run_batch:
        validation_mode = "full_batch_validation"
    else:
        # Initial classification based on historical performance
        # Will be refined after streaming execution in Phase 7
        validation_mode = "skipped_batch"

    if batch_strategy == "adaptive":
        logger.info(
            f"Batch adaptive decision | executed={str(run_batch).lower()} | "
            f"stable={str(stable_streaming).lower()} | fresh={str(batch_validation_fresh).lower()} | "
            f"reason={adaptive_batch_reason}"
        )
    elif batch_strategy == "periodic_audit":
        logger.info(
            f"Batch periodic audit decision | executed={str(run_batch).lower()} | due={str(batch_audit_due).lower()} | "
            f"reason={batch_audit_reason if batch_audit_due else 'audit_not_due'}"
        )
    else:
        logger.info(
            f"Batch decision | strategy={batch_strategy} | executed={str(run_batch).lower()} | "
            f"reason={batch_skip_reason or 'always_run'}"
        )

    logger.info(f"Validation mode | mode={validation_mode} | evaluable={str(run_batch).lower()}")

    # --- PHASE 0: Pre-execution Backup ---
    logger.info(">>> phase 0: pre-execution backup")
    backup_mgr = BackupManager()
    backup_path = backup_mgr.create_backup()
    if backup_path:
        logger.info(f"Backup created successfully at: {backup_path}")
    else:
        logger.warning("Backup skipped or failed. Continuing with pipeline execution.")

    logger.info("=========================================")
    logger.info(f"STARTING PIPELINE FOR LEGISLATURE {args.term}")
    logger.info("=========================================")

    # 1. Scrape Groups
    logger.info(">>> phase 1: groups_scraper")
    t0_groups = time.time()
    g_scraper = GroupsScraper(
        driver_path=args.driver_path, term=args.term, state_path=args.state_path, headless=args.headless
    )
    g_scraper.run()
    t1_groups = time.time()

    groups_path = pathlib.Path(f"data/bronze/groups/legislature={args.term}/groups.parquet")
    num_groups = len(pd.read_parquet(groups_path)) if groups_path.exists() else 0

    # 2. Scrape Deputies
    logger.info(">>> phase 2: deputies_scraper")
    t0_deputies = time.time()
    d_scraper = DeputiesScraper(
        driver_path=args.driver_path, term=args.term, state_path=args.state_path, headless=args.headless
    )
    deputies_df = d_scraper.run()
    t1_deputies = time.time()
    num_deputies = len(deputies_df) if deputies_df is not None else 0

    # 3. Scrape Sessions (run full)
    logger.info(">>> phase 3: sessions_scraper")
    t0_sessions = time.time()
    s_scraper = SessionsScraper(
        driver_path=args.driver_path, term=args.term, state_path=args.state_path, headless=args.headless
    )

    streaming_records = []
    content_callback = None

    if experimental_streaming:
        logger.info("Experimental streaming enabled. Wiring callback for in-memory extraction.")
        extractor_stream = InterventionsExtractor(args.term)

        def extraction_callback(doc_id: str, html_content: str) -> None:
            """Callback to trigger extraction immediately after HTML is obtained."""
            try:
                # Deduce doc_name as expected by the extractor
                doc_name = f"{doc_id}.html"
                records = extractor_stream.extract_from_content(html_content, doc_id, doc_name)
                streaming_records.extend(records)
                logger.debug(f"Stream-extracted {len(records)} interventions from {doc_id}")
            except Exception as e:
                logger.error(f"Error in experimental streaming callback for {doc_id}: {e}")

        content_callback = extraction_callback

    sessions_df, new_files = s_scraper.run(content_callback=content_callback)
    t1_sessions = time.time()

    sessions_path = pathlib.Path(f"data/bronze/sessions/legislature={args.term}/sessions.parquet")
    num_sessions = len(pd.read_parquet(sessions_path)) if sessions_path.exists() else 0

    # 4. Enrich Deputies (Bronze to Silver)
    logger.info(">>> phase 4: substitutions_enricher (Silver Layer)")
    t0_enrich = time.time()

    bronze_dir = pathlib.Path("data/bronze")
    deputies_path = bronze_dir / "deputies" / f"legislature={args.term}" / "deputies.parquet"
    substitutions_path = bronze_dir / "substitutions" / f"legislature={args.term}" / "substitutions.parquet"

    if not deputies_path.exists():
        logger.error(f"Deputies file not found at {deputies_path}. Cannot proceed with enrichment.")
        return

    # Load Bronze data
    deputies_df = pd.read_parquet(deputies_path)
    has_substitutes = 0
    has_substituted_by = 0

    if substitutions_path.exists():
        substitutions_df = pd.read_parquet(substitutions_path)
        logger.info(f"Loaded {len(substitutions_df)} raw substitution events from Bronze layer.")

        # --- SECTION A: Audit Metrics ---
        # 1. Row metrics
        total_rows_subst = len(substitutions_df)
        if total_rows_subst > 0:
            has_substitutes = (substitutions_df["substitutes"].str.strip() != "").sum()
            has_substituted_by = (substitutions_df["substituted_by"].str.strip() != "").sum()
            both_empty = (
                (substitutions_df["substitutes"].str.strip() == "")
                & (substitutions_df["substituted_by"].str.strip() == "")
            ).sum()

            logger.info(f"Audit Metric - Total substitutions: {total_rows_subst}")
            logger.info(
                f"Audit Metric - Rows with 'substitutes': {has_substitutes} "
                f"({(has_substitutes/total_rows_subst)*100:.1f}%)"
            )
            logger.info(
                f"Audit Metric - Rows with 'substituted_by': {has_substituted_by} "
                f"({(has_substituted_by/total_rows_subst)*100:.1f}%)"
            )
            logger.info(f"Audit Metric - Rows with both empty: {both_empty} ({(both_empty/total_rows_subst)*100:.1f}%)")

            if both_empty > total_rows_subst * 0.5:
                logger.warning("More than 50% of substitution rows are empty. Check scraper selectors.")
        else:
            logger.info("Audit Metric - Total substitutions: 0")
    else:
        logger.warning(f"Substitutions file not found at {substitutions_path}. Proceeding with empty substitutions.")
        substitutions_df = pd.DataFrame(columns=["name", "substitutes", "substituted_by", "start_date", "end_date"])

    enricher = SubstitutionsEnricher()
    silver_deputies_df, silver_relationships_df = enricher.enrich(deputies_df, substitutions_df)

    # --- SECTION D: Validation & Match Check ---
    expected_min_rels = has_substitutes + has_substituted_by
    actual_rels = len(silver_relationships_df)

    logger.info(f"Validation - Expected approx relationships: {expected_min_rels}, Actual: {actual_rels}")

    if expected_min_rels > 0 and actual_rels < expected_min_rels * 0.5:
        logger.error(
            f"CRITICAL: Relationship count ({actual_rels}) is significantly lower "
            f"than populated raw rows ({expected_min_rels}). Check name matching/normalization."
        )

    silver_dir = pathlib.Path(f"data/silver/deputies/legislature={args.term}")
    silver_dir.mkdir(parents=True, exist_ok=True)

    if not silver_deputies_df.empty:
        silver_deputies_df.to_parquet(silver_dir / "deputies_enriched.parquet", index=False)
        logger.info(f"Saved {len(silver_deputies_df)} enriched deputies to Silver layer.")

    if not silver_relationships_df.empty:
        silver_relationships_df.to_parquet(silver_dir / "relationships.parquet", index=False)
        logger.info(f"Saved {actual_rels} substitution relationships to Silver layer.")
    else:
        logger.info("No substitution relationships were generated.")

    t1_enrich = time.time()

    # 5. Extraction (Silver Layer)
    logger.info(">>> phase 5: interventions_extractor (Silver Layer)")
    t0_ext = time.time()
    num_extracted = 0
    df_ext = pd.DataFrame()

    if not run_batch:
        logger.info(f"Skipping batch extraction per strategy: {batch_strategy} (reason: {batch_skip_reason})")
    elif new_files:
        logger.info(f"New files detected ({len(new_files)}). Starting incremental extraction...")
        extractor = InterventionsExtractor(args.term)
        df_ext = extractor.run(file_list=new_files)
        num_extracted = len(df_ext)
    else:
        logger.info("No new session files detected. Skipping interventions extraction.")
    t1_ext = time.time()

    # --- Default downstream source selection logic (Phase 20B/22A) ---
    batch_interventions_path = (
        pathlib.Path("data/silver/interventions") / f"legislature={args.term}" / "interventions_raw.parquet"
    )

    # Output variables initialization (Safety/Observability)
    selected_source = None
    selection_policy = "none"
    promotion_result = "SKIPPED"
    fallback_reason = None
    confidence_level = "SKIPPED"
    confidence_score = 0.0
    parity_status = "SKIPPED"
    doc_level_parity = "SKIPPED"
    row_level_parity = "SKIPPED"
    docs_compared = 0
    matched_rows_count = 0
    total_rows_union_count = 0
    mismatched_docs = []

    confidence_metrics = {
        "global_match_ratio": 0.0,
        "document_match_ratio": 0.0,
        "row_identity_match_ratio": 0.0,
        "confidence_score": 0.0,
        "confidence_level": "SKIPPED",
    }
    skip_reason = None
    promotion_attempted = False
    execution_mode = "default_shadow" if default_streaming_active else "shadow"

    if args.disable_streaming:
        execution_mode = "batch_only"

    # --- PHASE 7: Experimental Streaming Validation ---
    if experimental_streaming or default_streaming_active:
        import json

        val_dir = pathlib.Path(f"data/validation/legislature={args.term}")
        val_dir.mkdir(parents=True, exist_ok=True)
        streaming_candidate_path = val_dir / "interventions_streaming_candidate.parquet"
        if not run_batch:
            # Classification Logic (Phase 23A)
            eligible = is_streaming_validation_eligible(stable_streaming, accuracy_metrics, bool(streaming_records))
            if eligible:
                validation_mode = "streaming_only_validation"
                parity_status = "INFERRED_VALID"

                # Check for inferred promotion (Phase 23B)
                if args.allow_inferred_promotion:
                    if summary_valid:
                        # Re-verify eligibility using strict criteria
                        if is_streaming_validation_eligible(stable_streaming, accuracy_metrics, True):
                            inferred_promotion_allowed = True
                            logger.info("Inferred promotion | status=ALLOWED | stable=true | source=streaming")
                        else:
                            fallback_reason = "inferred_promotion_conditions_unmet"
                    else:
                        fallback_reason = "missing_or_invalid_stability_context"
                else:
                    fallback_reason = "inferred_promotion_disabled"

            else:
                validation_mode = "skipped_batch"
                parity_status = "SKIPPED"
                fallback_reason = "batch_skipped_no_inferred_eligibility"

            doc_level_parity = "SKIPPED"
            row_level_parity = "SKIPPED"
            confidence_level = "SKIPPED"
            skip_reason = "batch_skipped"
            if not eligible:
                logger.info("Skipping parity validation because batch execution was skipped by strategy.")
        elif not new_files:
            parity_status = "SKIPPED"
            doc_level_parity = "SKIPPED"
            row_level_parity = "SKIPPED"
            confidence_level = "SKIPPED"
            skip_reason = "no_new_files"
            logger.info("Parity validation SKIPPED: no new session files were processed in this run.")
        elif streaming_records:
            logger.info(">>> phase 7: experimental streaming validation")
            streaming_df = pd.DataFrame(streaming_records)
            sort_cols = ["document_id", "intervention_order"]
            if all(c in streaming_df.columns for c in sort_cols):
                streaming_df.sort_values(sort_cols, inplace=True)

            streaming_df.to_parquet(streaming_candidate_path, index=False)
            logger.info(f"Saved {len(streaming_df)} streaming-extracted records to {streaming_candidate_path}")
            streaming_count = len(streaming_df)

            processed_doc_ids = set(streaming_df["document_id"].unique())
            batch_subset_df = df_ext[df_ext["document_id"].isin(processed_doc_ids)]
            batch_subset_count = len(batch_subset_df)
            parity_status = "MATCH" if streaming_count == batch_subset_count else "MISMATCH"

            st_counts = streaming_df.groupby("document_id").size().to_dict()
            bt_counts = batch_subset_df.groupby("document_id").size().to_dict()
            all_docs = set(st_counts.keys()) | set(bt_counts.keys())
            docs_compared = len(all_docs)

            row_level_parity = "MATCH"
            for doc_id in all_docs:
                s_df_doc = streaming_df[streaming_df["document_id"] == doc_id]
                b_df_doc = batch_subset_df[batch_subset_df["document_id"] == doc_id]
                s_ids = set(s_df_doc["intervention_id"])
                b_ids = set(b_df_doc["intervention_id"])

                matched_rows_count += len(s_ids & b_ids)
                total_rows_union_count += len(s_ids | b_ids)

                if s_ids != b_ids:
                    mismatched_docs.append(
                        {
                            "document_id": doc_id,
                            "streaming_count": len(s_ids),
                            "batch_count": len(b_ids),
                            "diagnosis": {
                                "missing_row_keys_sample": list(b_ids - s_ids)[:5],
                                "extra_row_keys_sample": list(s_ids - b_ids)[:5],
                            },
                        }
                    )
                    row_level_parity = "MISMATCH"
            doc_level_parity = "MATCH" if not mismatched_docs else "MISMATCH"

            # Confidence metrics
            global_match_ratio = 1.0 if parity_status == "MATCH" else 0.0
            document_match_ratio = (docs_compared - len(mismatched_docs)) / docs_compared if docs_compared > 0 else 1.0
            row_identity_match_ratio = (
                matched_rows_count / total_rows_union_count if total_rows_union_count > 0 else 1.0
            )
            confidence_score = 0.2 * global_match_ratio + 0.3 * document_match_ratio + 0.5 * row_identity_match_ratio

            if parity_status == "MATCH" and doc_level_parity == "MATCH" and row_level_parity == "MATCH":
                confidence_level = "FULL_MATCH"
            elif confidence_score >= 0.99:
                confidence_level = "HIGH_CONFIDENCE"
            elif confidence_score >= 0.90:
                confidence_level = "PARTIAL_MATCH"
            else:
                confidence_level = "LOW_CONFIDENCE"

            confidence_metrics = {
                "global_match_ratio": round(global_match_ratio, 4),
                "document_match_ratio": round(document_match_ratio, 4),
                "row_identity_match_ratio": round(row_identity_match_ratio, 4),
                "confidence_score": round(confidence_score, 4),
                "confidence_level": confidence_level,
            }
        else:
            logger.warning("No streaming records collected despite new files being processed.")
            parity_status = "MISMATCH"
            doc_level_parity = "MISMATCH"
            row_level_parity = "MISMATCH"
            skip_reason = "empty_streaming_records"

        # Selection Logic
        promotion_attempted = True
        if args.promote_streaming or args.use_streaming_candidate or default_streaming_active:
            if args.streaming_confidence_threshold is None:
                selection_policy = "strict_match"
                if confidence_level == "FULL_MATCH" and streaming_candidate_path.exists():
                    selected_source = str(streaming_candidate_path)
                    promotion_result = "PROMOTED"
                    promotion_basis = "batch_validated"
                    logger.info("Streaming guarded mode | strict_match=PASS → streaming selected")
                elif validation_mode == "streaming_only_validation" and inferred_promotion_allowed:
                    selected_source = str(streaming_candidate_path)
                    promotion_result = "PROMOTED_INFERRED"
                    promotion_basis = "inferred_validation"
                    logger.info("Inferred promotion | status=ALLOWED | source=streaming")
                else:
                    promotion_result = "FALLBACK"
                    promotion_basis = "none"
                    if parity_status == "SKIPPED":
                        if fallback_reason is None:
                            fallback_reason = "validation_skipped"
                    elif validation_mode == "streaming_only_validation" and not inferred_promotion_allowed:
                        # reason already set in Phase 7 logic
                        pass
                    elif not streaming_candidate_path.exists():
                        fallback_reason = "candidate_missing"
                    else:
                        fallback_reason = "strict_match_failed"

                    if validation_mode == "streaming_only_validation":
                        logger.info(f"Inferred promotion | status=DENIED | reason={fallback_reason}")
                    else:
                        logger.info(
                            f"Streaming guarded mode | strict_match=FAIL → fallback to batch | reason={fallback_reason}"
                        )
            else:
                selection_policy = "confidence_threshold"
                threshold = args.streaming_confidence_threshold
                if confidence_level == "SKIPPED":
                    fallback_reason = "validation_skipped"
                elif not streaming_candidate_path.exists():
                    fallback_reason = "candidate_missing"
                elif confidence_score >= threshold:
                    selected_source = str(streaming_candidate_path)
                    promotion_result = "PROMOTED"
                else:
                    fallback_reason = "confidence_below_threshold"
                    promotion_result = "FALLBACK"

                if promotion_result == "FALLBACK":
                    logger.info(
                        f"Falling back to official batch source | policy=confidence_threshold | "
                        f"reason={fallback_reason}"
                    )
                else:
                    logger.info(
                        f"Streaming candidate promoted | policy=confidence_threshold | "
                        f"confidence={confidence_score:.4f}"
                    )

        # Final Report & Summary
        report = {
            "legislature": args.term,
            "timestamp": datetime.now().isoformat(),
            "policy_used": selection_policy,
            "selected_source": selected_source or "OFFICIAL_BATCH",
            "parity_status": parity_status,
            "document_level_parity": doc_level_parity,
            "row_level_parity": row_level_parity,
            "docs_compared": docs_compared,
            "mismatched_documents": mismatched_docs,
        }
        report.update(confidence_metrics)
        if skip_reason:
            report["skip_reason"] = skip_reason

        report_file = val_dir / "parity_report.json"
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=4)

        if args.streaming_confidence_threshold is not None:
            run_mode = "threshold_evaluation"
        elif args.promote_streaming:
            run_mode = "explicit_promotion"
        elif args.use_streaming_candidate:
            run_mode = "candidate_evaluation"
        else:
            run_mode = "guarded_default"

        summary = {
            "term": args.term,
            "run_mode": run_mode,
            "promotion_attempted": promotion_attempted,
            "promotion_result": promotion_result,
            "execution_mode": execution_mode,
            "default_streaming_active": default_streaming_active,
            "selection_policy": selection_policy,
            "candidate_selected": bool(selected_source),
            "selected_source": "streaming_candidate_source" if selected_source else "official_batch_source",
            "parity_status": parity_status,
            "document_level_parity": doc_level_parity,
            "row_level_parity": row_level_parity,
            "confidence_score": confidence_score,
            "confidence_level": confidence_level,
            "docs_compared": docs_compared,
            "mismatched_document_count": len(mismatched_docs),
            "fallback_reason": fallback_reason,
            "batch_strategy": batch_strategy,
            "batch_executed": run_batch,
            "batch_skip_reason": batch_skip_reason,
            "batch_validation_fresh": batch_validation_fresh if batch_strategy == "adaptive" else None,
            "batch_required_by_rule": batch_required_by_rule
            if batch_strategy in ["adaptive", "periodic_audit"]
            else None,
            "adaptive_batch_reason": adaptive_batch_reason,
            "batch_audit_due": batch_audit_due,
            "batch_audit_reason": batch_audit_reason,
            "periodic_audit_mode_active": periodic_audit_mode_active,
            "validation_mode": validation_mode,
            "promotion_basis": promotion_basis,
            "inferred_promotion_allowed": inferred_promotion_allowed,
            "streaming_validation_basis": accuracy_metrics if not run_batch else None,
            "timestamp": datetime.now().isoformat(),
        }

        summary_file = val_dir / "validation_run_summary.json"
        with open(summary_file, "w", encoding="utf-8") as f:
            json.dump(summary, f, indent=4)

        try:
            history_file = pathlib.Path("data/validation/validation_run_history.jsonl")
            history_file.parent.mkdir(parents=True, exist_ok=True)
            with open(history_file, "a", encoding="utf-8") as f:
                f.write(json.dumps(summary) + "\n")
        except Exception as e:
            logger.warning(f"Failed to append validation run history: {e}")
    else:
        logger.info("Using official batch source | policy=batch_only")

    # 6. Interventions Enrichment (Silver Layer)
    logger.info(">>> phase 6: interventions_enrichment (Silver Layer)")
    t0_int_enrich = time.time()
    if new_files:
        final_source = selected_source or str(batch_interventions_path)
        logger.info(f"New data detected. Starting interventions enrichment using source: {final_source}")
        run_interventions_enrichment(args.term, final_source, args.driver_path)
    else:
        logger.info("No new data to enrich. Skipping.")
    t1_int_enrich = time.time()

    logger.info("=========================================")
    logger.info("EXECUTION METRICS SUMMARY")
    logger.info(f"Phase 1: Groups Scraper   -> Time: {t1_groups - t0_groups:.2f}s | Rows extracted: {num_groups}")
    logger.info(f"Phase 2: Deputies Scraper -> Time: {t1_deputies - t0_deputies:.2f}s | Rows extracted: {num_deputies}")
    logger.info(f"Phase 3: Sessions Scraper -> Time: {t1_sessions - t0_sessions:.2f}s | Rows extracted: {num_sessions}")
    logger.info(
        f"Phase 4: Data Enrichment  -> Time: {t1_enrich - t0_enrich:.2f}s | Relationships generated: {actual_rels}"
    )
    logger.info(f"Phase 5: Interv. Extract -> Time: {t1_ext - t0_ext:.2f}s | Total rows: {num_extracted}")
    logger.info(f"Phase 6: Interv. Enrich  -> Time: {t1_int_enrich - t0_int_enrich:.2f}s")
    logger.info("=========================================")
    logger.info("PIPELINE COMPLETED SUCCESSFULLY")
    logger.info("=========================================")


if __name__ == "__main__":
    main()
