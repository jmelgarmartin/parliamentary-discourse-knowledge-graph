"""
Configuración central de la aplicación basada en Pydantic y Variables de Entorno.
"""
from pathlib import Path

from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):  # type: ignore[misc]
    """Configuración principal usando python-dotenv y Pydantic para validación."""

    BASE_DIR: Path = Path(__file__).resolve().parent.parent.parent.parent

    # Rutas relativas del Data Lake
    BRONZE_DIR: Path = BASE_DIR / "data" / "bronze"
    SILVER_DIR: Path = BASE_DIR / "data" / "silver"
    GOLD_DIR: Path = BASE_DIR / "data" / "gold"

    # Base de Datos de Estado (DuckDB)
    DUCKDB_PATH: str = str(BASE_DIR / "duckdb_state.db")

    # Base de Datos de Grafos (Neo4j)
    NEO4J_URI: str = Field("bolt://localhost:7687", env="NEO4J_URI")
    NEO4J_USER: str = Field("neo4j", env="NEO4J_USER")
    NEO4J_PASSWORD: str = Field("password", env="NEO4J_PASSWORD")

    # LLM Auth
    OPENAI_API_KEY: str = Field("", env="OPENAI_API_KEY")

    # Logging y Monitoreo
    LOG_LEVEL: str = Field("INFO", env="LOG_LEVEL")
    LOG_FILE: str = str(BASE_DIR / "app.log")

    model_config = SettingsConfigDict(env_file=".env", env_file_encoding="utf-8")


settings = Settings()
