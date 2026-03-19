import argparse
import logging
import pathlib
from typing import Optional, Set

import pandas as pd
from congress_analysis.processing.speaker_resolution import SpeakerResolver

# Setup logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)


def run_enrichment(
    legislature: str, raw_parquet_path: Optional[str] = None, output_parquet_path: Optional[str] = None
) -> None:
    logger.info(f"Starting enrichment for legislature {legislature}...")

    # Set default paths if None
    if raw_parquet_path is None:
        raw_parquet_path = f"data/silver/interventions/legislature={legislature}/interventions_raw.parquet"
    if output_parquet_path is None:
        output_path = f"data/silver/interventions/legislature={legislature}/interventions_enriched.parquet"
        output_parquet_path = output_path

    path_obj = pathlib.Path(raw_parquet_path)
    if not path_obj.exists():
        logger.error(f"Input file not found: {raw_parquet_path}")
        return

    df = pd.read_parquet(raw_parquet_path)
    logger.info(f"Loaded {len(df)} interventions from {raw_parquet_path}")

    # 2. Load Rosters
    roster_mps: Set[str] = set()
    deputies_path = pathlib.Path(f"data/silver/deputies/legislature={legislature}/deputies_enriched.parquet")
    if deputies_path.exists():
        df_deps = pd.read_parquet(deputies_path)
        name_col = "full_name" if "full_name" in df_deps.columns else df_deps.columns[0]
        roster_mps = set(df_deps[name_col].dropna().unique().astype(str))
        logger.info(f"Loaded {len(roster_mps)} MPs from {deputies_path}")
    else:
        logger.warning(f"Deputies roster not found at {deputies_path}. MP detection will rely on regex.")

    # 3. Process with new SpeakerResolver
    manual_dict_path = f"data/reference/legislature_{legislature}/government_manual_mapping.csv"
    resolver = SpeakerResolver(df_deps, manual_dict_path=manual_dict_path)

    # 4. Process resolutions
    logger.info("Resolving speaker identities...")
    df_enriched = resolver.resolver(df)

    # Persist manual government dictionary if there were changes
    resolver.save_manual_dictionary()

    # 5. Generate reports and save
    final_output_path = pathlib.Path(output_parquet_path)
    final_output_path.parent.mkdir(parents=True, exist_ok=True)
    df_enriched.to_parquet(final_output_path, index=False)

    # 4b. Save Review Report in the same directory
    report_path = final_output_path.parent / "speaker_review.txt"
    resolver.generate_review_report(df_enriched, str(report_path))

    # 5. Summary Stats
    stats = df_enriched["speaker_status"].value_counts().to_dict()
    logger.info("Enrichment Stats (Speaker Resolution):")
    for status, count in stats.items():
        logger.info(f"  - {status}: {count}")
    logger.info(f"Saved enriched data to {output_parquet_path}")
    logger.info(f"Saved review report to {report_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enrich and filter interventions")
    parser.add_argument("--legislature", type=str, required=True)
    parser.add_argument("--input", type=str, default=None)
    parser.add_argument("--output", type=str, default=None)

    cli_args = parser.parse_args()

    legislature_id = cli_args.legislature
    input_path = cli_args.input or f"data/silver/interventions/legislature={legislature_id}/interventions_raw.parquet"
    output_path_cli = (
        cli_args.output or f"data/silver/interventions/legislature={legislature_id}/interventions_enriched.parquet"
    )

    run_enrichment(legislature_id, input_path, output_path_cli)
