import argparse
import json
import logging
import pathlib
from typing import Optional, Set

import pandas as pd
from congreso_analisis.processing.enrichment import enrich_and_filter_interventions

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
        out_p = f"data/silver/interventions/legislature={legislature}/interventions_enriched.parquet"
        output_parquet_path = out_p

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

    roster_gov = {}
    gov_path = pathlib.Path(f"data/static/roster_gov_l{legislature}.json")
    if gov_path.exists():
        with open(gov_path, "r", encoding="utf-8") as f:
            roster_gov = json.load(f)
        logger.info(f"Loaded {len(roster_gov)} Government members from {gov_path}")
    else:
        gov_path_alt = pathlib.Path(f"data/static/roster_gov_{legislature}.json")
        if gov_path_alt.exists():
            with open(gov_path_alt, "r", encoding="utf-8") as f:
                roster_gov = json.load(f)
            logger.info(f"Loaded {len(roster_gov)} Government members from {gov_path_alt}")

    # 3. Process
    df_enriched = enrich_and_filter_interventions(df, roster_mps=roster_mps, roster_gov=roster_gov)

    # 4. Save
    final_output_path = pathlib.Path(output_parquet_path)
    final_output_path.parent.mkdir(parents=True, exist_ok=True)
    df_enriched.to_parquet(final_output_path, index=False)

    # 5. Summary Stats
    stats = df_enriched["speaker_role"].value_counts().to_dict()
    kept_count = df_enriched["keep_for_graph"].sum()
    admin_count = df_enriched["is_admin_block"].sum()

    logger.info("Enrichment Stats:")
    for role, count in stats.items():
        logger.info(f"  - {role}: {count}")
    logger.info(f"  - Admin blocks: {admin_count}")
    logger.info(f"  - Kept for graph: {kept_count} / {len(df_enriched)}")
    logger.info(f"Saved enriched data to {output_parquet_path}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Enrich and filter interventions")
    parser.add_argument("--legislature", type=str, required=True)
    parser.add_argument("--input", type=str, default=None)
    parser.add_argument("--output", type=str, default=None)

    cli_args = parser.parse_args()

    leg = cli_args.legislature
    in_p = cli_args.input or f"data/silver/interventions/legislature={leg}/interventions_raw.parquet"
    out_p_cli = cli_args.output or f"data/silver/interventions/legislature={leg}/interventions_enriched.parquet"

    run_enrichment(leg, in_p, out_p_cli)
