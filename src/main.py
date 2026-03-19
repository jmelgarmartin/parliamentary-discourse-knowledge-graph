"""
Main execution pipeline for the Parliamentary Discourse Knowledge Graph project.
"""

import argparse
import logging
import os
import pathlib
import sys
import time
from typing import Optional

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

    import datetime

    # Format: YYYY-MM-DD_HHMM followed by the process name (e.g., 2026-03-03_1652_pipeline_execution.log)
    timestamp = datetime.datetime.now().strftime("%Y-%m-%d_%H%M")
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
        "--no-headless", action="store_false", dest="headless", help="Run browser in non-headless mode (visible GUI)"
    )
    parser.add_argument(
        "--experimental-streaming",
        action="store_true",
        help=(
            "Enable experimental in-memory streaming extraction during session scraping"
            " (generates validation artifacts)"
        ),
    )
    parser.add_argument(
        "--use-streaming-candidate",
        action="store_true",
        help=(
            "Use streaming candidate as downstream source for enrichment "
            "(requires --experimental-streaming; subject to parity matching)"
        ),
    )
    parser.add_argument(
        "--streaming-confidence-threshold",
        type=float,
        default=None,
        help="Confidence threshold [0.0, 1.0] to promote streaming candidate (requires --use-streaming-candidate)",
    )
    args = parser.parse_args()

    setup_logging("pipeline_execution", log_level=args.log_level)
    logger = logging.getLogger(__name__)

    if args.streaming_confidence_threshold is not None:
        if not (0.0 <= args.streaming_confidence_threshold <= 1.0):
            logger.error(
                f"Invalid streaming confidence threshold: {args.streaming_confidence_threshold}. Must be in [0.0, 1.0]."
            )
            sys.exit(1)

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

    if args.experimental_streaming:
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

    if new_files:
        logger.info(f"New files detected ({len(new_files)}). Starting incremental extraction...")
        extractor = InterventionsExtractor(args.term)
        df_ext = extractor.run(file_list=new_files)
        num_extracted = len(df_ext)
    else:
        logger.info("No new session files detected. Skipping interventions extraction.")
    t1_ext = time.time()

    # --- Default downstream source selection (Batch) ---
    batch_interventions_path = (
        pathlib.Path("data/silver/interventions") / f"legislature={args.term}" / "interventions_raw.parquet"
    )
    selected_source: Optional[str] = None
    selection_policy: str = "batch_only"

    # Parity statuses for reporting
    parity_status = "NOT_RUN"
    doc_level_parity = "NOT_RUN"
    row_level_parity = "NOT_RUN"
    confidence_metrics = {}

    # --- PHASE 7: Experimental Streaming Validation ---
    if args.experimental_streaming:
        logger.info(">>> phase 7: experimental streaming validation")
        import json

        val_dir = pathlib.Path(f"data/validation/legislature={args.term}")
        val_dir.mkdir(parents=True, exist_ok=True)
        streaming_candidate_path = val_dir / "interventions_streaming_candidate.parquet"

        streaming_count = 0
        batch_subset_count = 0
        mismatched_docs = []
        docs_compared = 0
        matched_rows_count = 0
        total_rows_union_count = 0
        skip_reason = None

        if not new_files:
            parity_status = "SKIPPED"
            doc_level_parity = "SKIPPED"
            row_level_parity = "SKIPPED"
            skip_reason = "no_new_files"
            logger.info("Parity validation SKIPPED: no new session files were processed in this run.")
        else:
            if streaming_records:
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
            else:
                logger.warning("No streaming records collected despite new files being processed.")
                parity_status = "MISMATCH"
                doc_level_parity = "MISMATCH"
                row_level_parity = "MISMATCH"
                skip_reason = "empty_streaming_records"

        # Quantitative Metrics & Confidence Mapping
        if parity_status == "SKIPPED":
            confidence_score = 0.0
            confidence_level = "SKIPPED"
            global_match_ratio = 0.0
            document_match_ratio = 0.0
            row_identity_match_ratio = 0.0
        else:
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

        # --- Decision Hierarchy for Downstream Source ---
        if args.use_streaming_candidate:
            if args.streaming_confidence_threshold is None:
                selection_policy = "strict_match"
                if parity_status == "MATCH" and doc_level_parity == "MATCH" and row_level_parity == "MATCH":
                    selected_source = str(streaming_candidate_path)
                    logger.info("Streaming candidate source selected | policy=strict_match | status=MATCH")
                else:
                    logger.info(
                        "Falling back to official batch source | policy=strict_match | reason=strict_match_failed"
                    )
            else:
                selection_policy = "confidence_threshold"
                threshold = args.streaming_confidence_threshold
                if confidence_level == "SKIPPED":
                    logger.info(
                        "Falling back to official batch source | policy=confidence_threshold"
                        " | reason=validation_skipped"
                    )
                elif not streaming_candidate_path.exists():
                    logger.info(
                        "Falling back to official batch source | policy=confidence_threshold"
                        " | reason=candidate_missing"
                    )
                elif confidence_score >= threshold:
                    selected_source = str(streaming_candidate_path)
                    logger.info(
                        f"Streaming candidate promoted | policy=confidence_threshold"
                        f" | confidence={confidence_score:.4f} >= threshold={threshold:.4f}"
                    )
                else:
                    logger.info(
                        f"Falling back to official batch source | policy=confidence_threshold"
                        f" | confidence={confidence_score:.4f} < threshold={threshold:.4f}"
                        f" | reason=confidence_below_threshold"
                    )
        else:
            selection_policy = "batch_only"
            logger.info("Using official batch source | policy=batch_only")

        # Create parity report (updated with final decision and metadata)
        report = {
            "legislature": args.term,
            "timestamp": pd.Timestamp.now().isoformat(),
            "policy_used": selection_policy,
            "selected_source": selected_source or "OFFICIAL_BATCH",
            "parity_status": parity_status,
            "document_level_parity": doc_level_parity,
            "row_level_parity": row_level_parity,
            "streaming_count": streaming_count,
            "batch_subset_count": batch_subset_count,
            "docs_compared": docs_compared,
            "matched_documents": docs_compared - len(mismatched_docs),
            "total_rows_considered": total_rows_union_count,
            "matched_rows": matched_rows_count,
            "mismatched_documents": mismatched_docs,
        }
        report.update(confidence_metrics)
        if skip_reason:
            report["skip_reason"] = skip_reason
        if args.streaming_confidence_threshold is not None:
            report["streaming_confidence_threshold"] = args.streaming_confidence_threshold

        report_file = val_dir / "parity_report.json"
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=4)

        if parity_status == "SKIPPED":
            logger.info(f"Parity report: SKIPPED ({skip_reason}). Report saved to {report_file}")
            logger.info(f"Validation summary: SKIPPED | reason={skip_reason}")
        else:
            logger.info(
                f"Validation summary: {confidence_level}"
                f" | confidence={confidence_score:.4f}"
                f" | docs={docs_compared - len(mismatched_docs)}/{docs_compared}"
                f" | rows={matched_rows_count}/{total_rows_union_count}"
            )
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
