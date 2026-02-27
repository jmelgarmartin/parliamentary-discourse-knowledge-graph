"""
Tests unitarios de las utilidades.
"""
from congreso_analisis.utils.hashing import text_to_hash
from congreso_analisis.utils.logging_utils import setup_logger
from congreso_analisis.utils.time_utils import get_current_partition_date


def test_hashing() -> None:
    """Prueba hashing stub."""
    assert text_to_hash("test") == ""


def test_time_utils() -> None:
    """Prueba time_utils stub."""
    assert get_current_partition_date() == ""


def test_logging_utils() -> None:
    """Prueba logging_utils stub."""
    assert setup_logger("test") is None
