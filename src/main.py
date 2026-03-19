"""
Main execution pipeline for the Parliamentary Discourse Knowledge Graph project.
"""

import argparse
import logging
import os
import pathlib
import sys
import time

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
        help="Enable experimental in-memory extraction during scraping for validation purposes.",
    )
    args = parser.parse_args()

    setup_logging("pipeline_execution", log_level=args.log_level)
    logger = logging.getLogger(__name__)

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

    if substitutions_path.exists():
        substitutions_df = pd.read_parquet(substitutions_path)
        logger.info(f"Loaded {len(substitutions_df)} raw substitution events from Bronze layer.")

        # --- SECTION A: Audit Metrics ---
        # 1. Row metrics
        total_rows = len(substitutions_df)
        if total_rows > 0:
            has_substitutes = (substitutions_df["substitutes"].str.strip() != "").sum()
            has_substituted_by = (substitutions_df["substituted_by"].str.strip() != "").sum()
            both_empty = (
                (substitutions_df["substitutes"].str.strip() == "")
                & (substitutions_df["substituted_by"].str.strip() == "")
            ).sum()

            logger.info(f"Audit Metric - Total substitutions: {total_rows}")
            logger.info(
                f"Audit Metric - Rows with 'substitutes': {has_substitutes} "
                f"({(has_substitutes/total_rows)*100:.1f}%)"
            )
            logger.info(
                f"Audit Metric - Rows with 'substituted_by': {has_substituted_by} "
                f"({(has_substituted_by/total_rows)*100:.1f}%)"
            )
            logger.info(f"Audit Metric - Rows with both empty: {both_empty} ({(both_empty/total_rows)*100:.1f}%)")

            if both_empty > total_rows * 0.5:
                logger.warning("More than 50% of substitution rows are empty. Check scraper selectors.")
        else:
            logger.info("Audit Metric - Total substitutions: 0")
    else:
        logger.warning(f"Substitutions file not found at {substitutions_path}. Proceeding with empty substitutions.")
        substitutions_df = pd.DataFrame(columns=["name", "substitutes", "substituted_by", "start_date", "end_date"])

    enricher = SubstitutionsEnricher()
    silver_deputies_df, silver_relationships_df = enricher.enrich(deputies_df, substitutions_df)

    # --- SECTION D: Validation & Match Check ---
    expected_min_rels = (has_substitutes + has_substituted_by) if "has_substitutes" in locals() else 0
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

    # 6. Interventions Enrichment (Silver Layer)
    logger.info(">>> phase 6: interventions_enrichment (Silver Layer)")
    t0_int_enrich = time.time()
    if new_files:
        logger.info("New data detected. Starting interventions enrichment...")
        run_interventions_enrichment(args.term, None, None)
    else:
        logger.info("No new data to enrich. Skipping.")
    t1_int_enrich = time.time()

    # --- PHASE 7: Experimental Streaming Validation ---
    if args.experimental_streaming:
        logger.info(">>> phase 7: experimental streaming validation")
        import json

        val_dir = pathlib.Path(f"data/validation/legislature={args.term}")
        val_dir.mkdir(parents=True, exist_ok=True)

        streaming_count = 0
        batch_subset_count = 0
        parity_status = "MATCH"
        skip_reason = None

        if not new_files:
            parity_status = "SKIPPED"
            skip_reason = "no_new_files"
            logger.info("Parity validation SKIPPED: no new session files were processed in this run.")
        else:
            # Persist streaming results (validation only)
            if streaming_records:
                streaming_df = pd.DataFrame(streaming_records)
                val_file = val_dir / "interventions_streaming.parquet"
                streaming_df.to_parquet(val_file, index=False)
                logger.info(f"Saved {len(streaming_df)} streaming-extracted records to {val_file}")
                streaming_count = len(streaming_df)

                # Filter batch results to only include the same documents for fair comparison
                processed_doc_ids = set(streaming_df["document_id"].unique())
                batch_subset_df = df_ext[df_ext["document_id"].isin(processed_doc_ids)]
                batch_subset_count = len(batch_subset_df)
                parity_status = "MATCH" if streaming_count == batch_subset_count else "MISMATCH"
            else:
                logger.warning("No streaming records collected despite new files being processed.")
                parity_status = "MISMATCH"
                skip_reason = "empty_streaming_records"

        # Structured Parity Report
        report = {
            "timestamp": pd.Timestamp.now().isoformat(),
            "term": args.term,
            "streaming_count": streaming_count,
            "batch_subset_count": batch_subset_count,
            "total_batch_count": len(df_ext) if df_ext is not None else 0,
            "parity_status": parity_status,
            "diff_count": streaming_count - batch_subset_count,
        }
        if skip_reason:
            report["skip_reason"] = skip_reason

        report_file = val_dir / "parity_report.json"
        with open(report_file, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=4)

        if parity_status == "SKIPPED":
            logger.info(f"Parity check: SKIPPED ({skip_reason}). Report saved to {report_file}")
        else:
            logger.info(
                f"Parity check: Streaming={streaming_count} vs BatchSubset={batch_subset_count}. "
                f"Status: {parity_status}. Report saved to {report_file}"
            )

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
