"""
Substitutions enricher for the Bronze layer.

This module is part of the post-processing phase of the Bronze layer.
It takes a dataframe of base deputies and a dataframe of substitution events,
and merges them deterministically to identify the full lifecycle of a seat.
"""

import logging
from typing import Dict, Tuple, cast

import pandas as pd

logger = logging.getLogger(__name__)


class SubstitutionsEnricher:
    """
    Enricher for parliamentary deputies with substitution information.
    Operates independently of web scraping to maintain separation of concerns.

    Expected `deputies_df` columns:
        - name (or full_name)
        - deputy_id (stable identifier)
        - legislature

    Expected `substitutions_df` columns:
        - name (name of the active deputy in the substitution event)
        - substitutes (name of the deputy being substituted, can be null)
        - substituted_by (name of the deputy who substitutes, can be null)
        - start_date (start date of substitution, can be null)
        - end_date (end date of substitution, can be null)
    """

    def _normalize_name(self, name: str) -> str:
        """
        Normalizes a deputy name to facilitate deterministic matching.
        Strips whitespace, collapses multiple spaces, upper-cases string, removes common titles,
        and removes accents (Unicode normalization).
        """
        if pd.isna(name) or not isinstance(name, str):
            return ""

        import unicodedata

        # Remove extra spaces and uppercase
        name = " ".join(name.split()).upper()

        # Remove common formal titles
        titles_to_remove = [
            "EL SEÑOR",
            "LA SEÑORA",
            "DON",
            "DOÑA",
            "D.",
            "Dª.",
            "DÑA.",
            "SR.",
            "SRA.",
        ]
        for title in titles_to_remove:
            if name.startswith(title + " "):
                name = name[len(title) + 1 :].strip()

        # Remove accents and special characters
        name = "".join(c for c in unicodedata.normalize("NFKD", name) if not unicodedata.combining(c))

        return name

    def _normalize_date(self, date_str: str) -> str:
        """
        Attempts to parse a date string and return it in YYYY-MM-DD format.
        If it cannot be parsed, returns the original string stripped.
        """
        if pd.isna(date_str) or not isinstance(date_str, str):
            return ""

        date_str = date_str.strip()
        if not date_str:
            return ""

        try:
            # Try to infer datetime - usually source dates are in DD/MM/YYYY format
            parsed_date = pd.to_datetime(date_str, dayfirst=True)
            return cast(str, parsed_date.strftime("%Y-%m-%d"))
        except Exception:
            # Fallback
            return date_str

    def enrich(self, deputies_df: pd.DataFrame, substitutions_df: pd.DataFrame) -> Tuple[pd.DataFrame, pd.DataFrame]:
        """
        Enriches deputies with their substitution data and returns the relationships.

        :param deputies_df: Bronze DataFrame containing the basic deputies extracted.
        :param substitutions_df: DataFrame containing the raw substitution parsed data.
        :return: A tuple containing:
                 - The enriched deputies DataFrame.
                 - A normalized relationships DataFrame with schema:
                   [deputy_id, substituted_deputy_id, start_date, end_date, legislature]
        :raises ValueError: If required columns are missing in input DataFrames.
        """
        logger.info("Starting enrichment of deputies with substitutions data.")

        # 1. Validation of required columns
        deputies_required = {"deputy_id"}
        if "name" in deputies_df.columns:
            name_col = "name"
        elif "full_name" in deputies_df.columns:
            name_col = "full_name"
        else:
            raise ValueError(f"Missing name column in deputies_df. Found: {deputies_df.columns.tolist()}")

        if not deputies_required.issubset(deputies_df.columns):
            missing = deputies_required - set(deputies_df.columns)
            raise ValueError(f"Missing required columns in deputies_df: {missing}")

        substitutions_required = {"name", "substitutes", "substituted_by", "start_date", "end_date"}
        if not substitutions_df.empty and not substitutions_required.issubset(substitutions_df.columns):
            missing = substitutions_required - set(substitutions_df.columns)
            raise ValueError(f"Missing required columns in substitutions_df: {missing}")

        if deputies_df.empty or substitutions_df.empty:
            logger.warning("Empty input dataframes provided. Skipping enrichment.")
            relationships_df = pd.DataFrame(
                columns=["deputy_id", "substituted_deputy_id", "start_date", "end_date", "legislature"]
            )
            return deputies_df.copy(), relationships_df

        # Determine the legislature from the deputies dataframe
        legislature = deputies_df["legislature"].iloc[0] if "legislature" in deputies_df.columns else ""

        # 2. Build ID Mapper from normalized deputy names to IDs
        # To handle mapping properly, we pre-normalize the source dataframe
        deputies_df_work = deputies_df.copy()
        deputies_df_work["_normalized_name"] = deputies_df_work[name_col].apply(self._normalize_name)

        # In case a name resolves to multiple IDs unexpectedly, take the first to avoid collisions.
        # Ideally, names are unique per legislature in the Bronze layer.
        name_to_id: Dict[str, str] = (
            deputies_df_work.drop_duplicates(subset=["_normalized_name"])
            .set_index("_normalized_name")["deputy_id"]
            .to_dict()
        )

        # 3. Enrich Deputies DataFrame
        substitutions_renamed = substitutions_df.rename(
            columns={
                "start_date": "substitution_start_date",
                "end_date": "substitution_end_date",
                "name": "substitution_name",
            }
        ).copy()

        # Normalize substitution dates
        substitutions_renamed["substitution_start_date"] = substitutions_renamed["substitution_start_date"].apply(
            self._normalize_date
        )
        substitutions_renamed["substitution_end_date"] = substitutions_renamed["substitution_end_date"].apply(
            self._normalize_date
        )

        # Normalize substitution names for merging
        substitutions_renamed["_normalized_name"] = substitutions_renamed["substitution_name"].apply(
            self._normalize_name
        )

        enriched_deputies_df = pd.merge(
            deputies_df_work,
            substitutions_renamed,
            on="_normalized_name",
            how="left",
            suffixes=("_original", "_substitution"),
        )

        # Clean up temporary columns
        if "substitution_name" in enriched_deputies_df.columns:
            enriched_deputies_df = enriched_deputies_df.drop(columns=["substitution_name"])
        enriched_deputies_df = enriched_deputies_df.drop(columns=["_normalized_name"])

        # 4. Create Relationships DataFrame based on IDs
        relationships_data = []

        # Iterate over valid substitution records
        for _, row in substitutions_renamed.dropna(subset=["substitutes", "substituted_by"], how="all").iterrows():
            # Source name
            source_norm_name = row.get("_normalized_name", "")
            source_deputy_id = name_to_id.get(source_norm_name)

            if not source_deputy_id:
                logger.warning(
                    f"Could not resolve ID for deputy: '{row.get('substitution_name')}' "
                    f"(normalized: '{source_norm_name}')"
                )
                continue

            start_date = row.get("substitution_start_date", "")
            end_date = row.get("substitution_end_date", "")

            # Generate relation if they substitute someone (Incoming)
            substitutes_str = row.get("substitutes", "")
            if pd.notna(substitutes_str) and str(substitutes_str).strip() != "":
                target_norm_name = self._normalize_name(str(substitutes_str))
                target_deputy_id = name_to_id.get(target_norm_name)

                if target_deputy_id:
                    relationships_data.append(
                        {
                            "deputy_id": source_deputy_id,
                            "substituted_deputy_id": target_deputy_id,
                            "start_date": start_date,
                            "end_date": end_date,
                            "legislature": legislature,
                        }
                    )
                else:
                    logger.warning(
                        f"Match failure: '{substitutes_str}' (target) not found in deputies list. "
                        f"Source row deputy: '{row.get('substitution_name')}'"
                    )

            # Generate relation if they are substituted by someone (Outgoing)
            substituted_by_str = row.get("substituted_by", "")
            if pd.notna(substituted_by_str) and str(substituted_by_str).strip() != "":
                target_norm_name = self._normalize_name(str(substituted_by_str))
                target_deputy_id = name_to_id.get(target_norm_name)

                if target_deputy_id:
                    relationships_data.append(
                        {
                            "deputy_id": target_deputy_id,
                            "substituted_deputy_id": source_deputy_id,
                            "start_date": start_date,
                            "end_date": end_date,
                            "legislature": legislature,
                        }
                    )
                else:
                    logger.warning(
                        f"Match failure: '{substituted_by_str}' (replacement) not found in deputies list. "
                        f"Source row deputy: '{row.get('substitution_name')}'"
                    )

        # 5. Build, Sort and Deduplicate
        relationships_df = pd.DataFrame(
            relationships_data, columns=["deputy_id", "substituted_deputy_id", "start_date", "end_date", "legislature"]
        )

        relationships_df = relationships_df.drop_duplicates()

        # Sort relationships deterministically
        relationships_df = relationships_df.sort_values(
            by=["deputy_id", "substituted_deputy_id", "start_date"]
        ).reset_index(drop=True)

        logger.info(f"Enrichment complete. Created {len(relationships_df)} relationship records.")
        return enriched_deputies_df, relationships_df
