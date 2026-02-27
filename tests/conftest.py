"""
Fixtures globales para pytest.
"""
from typing import Any
from unittest.mock import MagicMock

import pytest


@pytest.fixture  # type: ignore[misc]
def mock_settings() -> Any:
    """Mock basic settings."""
    return MagicMock()
