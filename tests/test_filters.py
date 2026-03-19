from congress_analysis.processing.filters import is_admin_block, should_keep_for_graph
from congress_analysis.processing.roles import SpeakerRole


def test_is_admin_block() -> None:
    # Typical admin block with many names
    text = "García Pérez, Juan\nLópez Sánchez, María\nRodríguez Martín, José"
    assert is_admin_block(text, min_namelike_lines=2) is True


def test_is_not_admin_block() -> None:
    text = "Señora Presidenta, pido la palabra para una cuestión de orden."
    assert is_admin_block(text) is False


def test_should_keep_for_graph() -> None:
    # MP normal
    assert should_keep_for_graph(SpeakerRole.MP, False) is True
    # Admin block
    assert should_keep_for_graph(SpeakerRole.MP, True) is False
    # Chair (normally exclude, but override)
    assert should_keep_for_graph(SpeakerRole.CHAIR, False) is False
    assert should_keep_for_graph(SpeakerRole.CHAIR, False, include_chair_speech=True) is True
