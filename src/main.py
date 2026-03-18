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

# Allow execution of main.py within the src/ directory path context.
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from congreso_analisis.ingestion.scrappers.deputies_scraper import DeputiesScraper  # noqa: E402
from congreso_analisis.ingestion.scrappers.groups_scraper import GroupsScraper  # noqa: E402
from congreso_analisis.ingestion.scrappers.sessions_scraper import SessionsScraper  # noqa: E402
from congreso_analisis.ingestion.transformers.enriquecedor_suplencias import EnriquecedorSuplencias  # noqa: E402
from congreso_analisis.silver.enrich_legislature import run_enrichment as run_interventions_enrichment  # noqa: E402
from congreso_analisis.silver.interventions_extractor import InterventionsExtractor  # noqa: E402


def setup_logging(process_name: str, log_level: str = "INFO") -> None:
    """Configures the root logger."""
    os.makedirs("logs", exist_ok=True)
    handlers = logging.root.handlers[:]
    for handler in handlers:
        logging.root.removeHandler(handler)

    import datetime

    # Format: fecha y hora minuto (e.g., 2026-03-03_1652) despues _ y el nombre del proceso
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
    args = parser.parse_args()

    setup_logging("pipeline_execution", log_level=args.log_level)
    logger = logging.getLogger(__name__)

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
    sessions_df, new_files = s_scraper.run()
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
        has_substitutes = (substitutions_df["substitutes"].str.strip() != "").sum()
        has_substituted_by = (substitutions_df["substituted_by"].str.strip() != "").sum()
        both_empty = (
            (substitutions_df["substitutes"].str.strip() == "") & (substitutions_df["substituted_by"].str.strip() == "")
        ).sum()

        logger.info(f"Audit Metric - Total substitutions: {total_rows}")
        logger.info(
            f"Audit Metric - Rows with 'substitutes': {has_substitutes} " f"({(has_substitutes/total_rows)*100:.1f}%)"
        )
        logger.info(
            f"Audit Metric - Rows with 'substituted_by': {has_substituted_by} "
            f"({(has_substituted_by/total_rows)*100:.1f}%)"
        )
        logger.info(f"Audit Metric - Rows with both empty: {both_empty} ({(both_empty/total_rows)*100:.1f}%)")

        if both_empty > total_rows * 0.5:
            logger.warning("More than 50% of substitution rows are empty. Check scraper selectors.")
    else:
        logger.warning(f"Substitutions file not found at {substitutions_path}. Proceeding with empty substitutions.")
        substitutions_df = pd.DataFrame(columns=["name", "substitutes", "substituted_by", "start_date", "end_date"])

    enricher = EnriquecedorSuplencias()
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
