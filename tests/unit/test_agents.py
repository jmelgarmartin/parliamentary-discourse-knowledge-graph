"""
Tests unitarios de los Agentes de Orquestación.
"""
from congreso_analisis.orchestration.coordinator import Coordinator
from congreso_analisis.orchestration.ingestion_agent import IngestionAgent


def test_ingestion_agent_run() -> None:
    """Prueba que el IngestionAgent se ejecuta correctamente (stub)."""
    agent = IngestionAgent()
    assert agent.validate_input({}) is True
    agent.run({})
    assert agent.report() == {}


def test_coordinator_run() -> None:
    """Prueba que el Coordinator se ejecuta correctamente (stub)."""
    coord = Coordinator()
    assert coord.validate_input({}) is True
    coord.run({})
    assert coord.report() == {}
