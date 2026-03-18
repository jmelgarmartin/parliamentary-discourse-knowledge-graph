"""
Graph Builder for Neo4j loading operations.
"""
import logging
from typing import Optional

import pandas as pd

logger = logging.getLogger(__name__)


class GraphBuilder:
    """
    Class responsible for transformation and loading of Silver data into Neo4j (Gold layer).
    """

    def __init__(self, legislature: Optional[str] = None) -> None:
        """
        Initializes the GraphBuilder.

        :param legislature: Optional legislative term to filter data.
        """
        self.legislature = legislature

    def validate_dataframe(self, df: pd.DataFrame) -> bool:
        """
        Validates if the dataframe has the required columns for graph injection.
        The 'legislatura' column is checked for presence if required.

        :param df: Input DataFrame from Silver layer.
        :return: True if valid, False otherwise.
        """
        required_cols = ["legislature"]  # Based on user request to review this

        missing = [col for col in required_cols if col not in df.columns]
        if missing:
            logger.error(f"Missing mandatory columns for GraphBuilder: {missing}")
            return False

        if self.legislature:
            # Check if all rows belong to the requested legislature
            unique_terms = df["legislature"].unique()
            if len(unique_terms) > 1 or str(unique_terms[0]) != self.legislature:
                logger.warning(
                    f"DataFrame contains multiple or mismatched legislatures: {unique_terms}. "
                    f"Expected: {self.legislature}"
                )

        return True

    def build_graph(self, deputies_df: pd.DataFrame, interventions_df: pd.DataFrame) -> None:
        """
        Main entry point for building the graph.

        :param deputies_df: Enriched deputies data.
        :param interventions_df: Extracted interventions data.
        """
        if not self.validate_dataframe(deputies_df):
            logger.error("Deputies DataFrame validation failed. Aborting graph build.")
            return

        logger.info("Starting graph construction in Neo4j...")
        # TODO: Implement Neo4j loading logic using a driver
        pass
