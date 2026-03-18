import pathlib
from unittest.mock import MagicMock

import pytest
from congreso_analisis.silver.interventions_extractor import InterventionsExtractor


class MockSoup:
    def __init__(self, text_blocks):
        self.text_blocks = text_blocks

    def get_text(self, separator="\n"):
        return "\n".join(self.text_blocks)

    def __call__(self, *args, **kwargs):
        return []


@pytest.fixture
def extractor():
    return InterventionsExtractor(term="15")


def test_conservative_segmentation(extractor, monkeypatch):
    # Definimos los bloques de texto reales proporcionados por el usuario
    text_blocks = [
        "La señora PRESIDENTA: Señorías, silencio.",  # 1. Cabecera limpia -> Nueva intervención
        "El señor TELLADO FILGUEIRA: Gracias, presidenta.",  # 2. Cabecera limpia -> Nueva intervención
        "El señor Sánchez ha dicho que la verdad es la realidad.",  # 3. Narrativa -> Acumular
        "La señora Martínez, de SUMAR, habla de los servicios públicos.",  # 4. Narrativa -> Acumular
        "Presidente, señorías, volvemos de nuevo a este asunto.",  # 5. Narrativa (falsa cabecera) -> Acumular
        "(Aplausos.-El señor TELLADO FILGUEIRA)",  # 6. Rescate embebido -> Pendiente
        "(Aplausos)",  # 7. Acotación pura -> Ignorar para activación
        "",  # 8. Línea vacía -> Ignorar
        "Muchas gracias, señorías.",  # 9. Texto real -> Activa Tellado
        "(Pausa.-La señora MINISTRA DE SANIDAD)",  # 10. Rescate embebido -> Pendiente
        "Tiene la palabra la señora ministra.",  # 11. Texto real (falso positivo de activación? No, activa ministra)
    ]

    # Mock BeautifulSoup para devolver estos bloques
    mock_soup = MagicMock()
    mock_soup.get_text.return_value = "\n".join(text_blocks)
    mock_soup.return_value = []

    # Parcheamos BeautifulSoup en el módulo
    monkeypatch.setattr(
        "congreso_analisis.silver.interventions_extractor.BeautifulSoup",
        lambda *args, **kwargs: mock_soup,
    )
    # Parcheamos open para no leer ficheros reales
    monkeypatch.setattr("builtins.open", MagicMock())

    records = extractor._process_file(pathlib.Path("test.html"))

    # Análisis de resultados esperados:
    # 1. PRESIDENTA (Señorías, silencio. El señor Sánchez ha dicho... La señora Martínez... Presidente, señorías...)
    # 2. TELLADO FILGUEIRA (Gracias, presidenta. (Aplausos.-El señor TELLADO...))
    # -> Espera, Tellado se abre en el punto 2.
    # 3. TELLADO FILGUEIRA (de acotación 6) se activa en el punto 9.
    # 4. MINISTRA (de acotación 10) se activa en el punto 11.

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
    # La acotación con rescate se acumula en el orador actual (Tellado 2)
    assert "(Aplausos.-El señor TELLADO FILGUEIRA)" in records[1]["text_raw"]

    # Speaker 3: El señor TELLADO FILGUEIRA (Rescatado)
    assert records[2]["speaker_label"] == "El señor TELLADO FILGUEIRA"
    assert "Muchas gracias, señorías." in records[2]["text_raw"]
    # La acotación con rescate se acumula en el orador actual (Tellado 3)
    assert "(Pausa.-La señora MINISTRA DE SANIDAD)" in records[2]["text_raw"]

    # Speaker 4: La señora MINISTRA DE SANIDAD (Rescatada)
    assert records[3]["speaker_label"] == "La señora MINISTRA DE SANIDAD"
    assert "Tiene la palabra la señora ministra." in records[3]["text_raw"]

    assert len(records) == 4
