"""
Interfaces para el módulo de almacenamiento cruzado (Storage).
"""
from abc import ABC, abstractmethod
from datetime import date

import pandas as pd


class BaseStorageProvider(ABC):
    """Proveedor abstracto para acceso a disco/storage en capas Bronze y Silver."""

    @abstractmethod
    def save_bronze_html(self, session_id: str, content: str, version: str) -> str:
        """Guarda archivo HTML crudo versionado."""
        pass

    @abstractmethod
    def save_silver_partition(
        self, entity_name: str, df: pd.DataFrame, legislatura: str, fecha: date
    ) -> None:
        """
        Guarda DataFrame en formato Parquet sobrescribiendo la partición de
        forma idempotente.
        """
        pass
