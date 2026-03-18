"""
Interfaces for the cross-module storage (Storage).
"""
from abc import ABC, abstractmethod
from datetime import date

import pandas as pd


class BaseStorageProvider(ABC):
    """Abstract provider for disk/storage access in Bronze and Silver layers."""

    @abstractmethod
    def save_bronze_html(self, session_id: str, content: str, version: str) -> str:
        """Saves versioned raw HTML file."""
        pass

    @abstractmethod
    def save_silver_partition(self, entity_name: str, df: pd.DataFrame, legislature: str, fecha: date) -> None:
        """
        Saves DataFrame in Parquet format, overwriting the partition
        idempotently.
        """
        pass
