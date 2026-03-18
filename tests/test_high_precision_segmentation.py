import pathlib
from unittest.mock import MagicMock

import pytest
from congreso_analisis.silver.interventions_extractor import InterventionsExtractor


class MockSoup:
    def __init__(self, text_blocks: list[str]) -> None:
        self.text_blocks = text_blocks

    def get_text(self, separator: str = "\n") -> str:
        return "\n".join(self.text_blocks)

    def __call__(self, *args: object, **kwargs: object) -> list[object]:
        return []


@pytest.fixture  # type: ignore[misc]
def extractor() -> InterventionsExtractor:
    return InterventionsExtractor(term="15")


def test_conservative_segmentation(extractor: InterventionsExtractor, monkeypatch: pytest.MonkeyPatch) -> None:
    # Definimos los bloques de texto reales proporcionados por el usuario
    text_blocks = [
        "La señora PRESIDENTA: Señorías, silencio.",  # 1. Clean header -> New intervention
        "El señor TELLADO FILGUEIRA: Gracias, presidenta.",  # 2. Clean header -> New intervention
        "El señor Sánchez ha dicho que la verdad es la realidad.",  # 3. Narrativa -> Acumular
        "La señora Martínez, de SUMAR, habla de los servicios públicos.",  # 4. Narrativa -> Acumular
        "Presidente, señorías, volvemos de nuevo a este asunto.",  # 5. Narrativa (falsa cabecera) -> Acumular
        "(Aplausos.-El señor TELLADO FILGUEIRA)",  # 6. Rescate embebido -> Pendiente
        "(Aplausos)",  # 7. Pure stage direction -> Ignore for activation
        "",  # 8. Empty line -> Ignore
        "Muchas gracias, señorías.",  # 9. Texto real -> Activa Tellado
        "(Pausa.-La señora MINISTRA DE SANIDAD)",  # 10. Rescate embebido -> Pendiente
        "Tiene la palabra la señora ministra.",  # 11. Narrative text (activates pending 'ministra')
    ]

    # Mock BeautifulSoup para devolver estos bloques
    mock_soup = MagicMock()
    mock_soup.get_text.return_value = "\n".join(text_blocks)
    mock_soup.return_value = []

    # Patch BeautifulSoup in the module
    monkeypatch.setattr(
        "congreso_analisis.silver.interventions_extractor.BeautifulSoup",
        lambda *args, **kwargs: mock_soup,
    )
    # Patch open to avoid reading real files
    monkeypatch.setattr("builtins.open", MagicMock())

    records = extractor._process_file(pathlib.Path("test.html"))

    # Analysis of expected results:
    # 1. PRESIDENTA (Señorías, silencio. El señor Sánchez ha dicho... La señora Martínez... Presidente, señorías...)
    # 2. TELLADO FILGUEIRA (Gracias, presidenta. (Aplausos.-El señor TELLADO...))
    # 3. TELLADO FILGUEIRA (from annotation 6) activates at step 9.
    # 4. MINISTRA (from annotation 10) activates at step 11.

    # Speaker 1: La señora PRESIDENTA
    assert records[0]["speaker_label"] == "La señora PRESIDENTA"
    assert "Señorías, silencio" in records[0]["text_raw"]

    # Speaker 2: El señor TELLADO FILGUEIRA
    assert records[1]["speaker_label"] == "El señor TELLADO FILGUEIRA"
    assert "Gracias, presidenta." in records[1]["text_raw"]
    # Verificamos que las narrativas se acumularon en Tellado (2)
    assert "El señor Sánchez ha dicho" in records[1]["text_raw"]
    assert "La señora Martínez" in records[1]["text_raw"]
    assert "Presidente, señorías" in records[1]["text_raw"]
    # The annotation with rescue accumulates in current speaker (Tellado 2)
    assert "(Aplausos.-El señor TELLADO FILGUEIRA)" in records[1]["text_raw"]

    # Speaker 3: El señor TELLADO FILGUEIRA (Rescued)
    assert records[2]["speaker_label"] == "El señor TELLADO FILGUEIRA"
    assert "Muchas gracias, señorías." in records[2]["text_raw"]
    # The annotation with rescue accumulates in current speaker (Tellado 3)
    assert "(Pausa.-La señora MINISTRA DE SANIDAD)" in records[2]["text_raw"]

    # Speaker 4: La señora MINISTRA DE SANIDAD (Rescued)
    assert records[3]["speaker_label"] == "La señora MINISTRA DE SANIDAD"
    assert "Tiene la palabra la señora ministra." in records[3]["text_raw"]

    assert len(records) == 4
