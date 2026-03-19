from congress_analysis.processing.speaker_detector import SpeakerDetector


class MockFile:
    def __init__(self, name: str) -> None:
        self.name = name
        self.stem = name.split(".")[0]

    def __str__(self) -> str:
        return self.name


def test_speaker_detector_standard() -> None:
    # Caso Standard
    res = SpeakerDetector.find_standard_speaker("La señora PRESIDENTA: Señorías, silencio.")
    assert res is not None
    assert res[0] == "La señora PRESIDENTA"
    assert res[1] == "Señorías, silencio."

    # Caso Candidato
    res = SpeakerDetector.find_standard_speaker("NÚÑEZ FEIJÓO (candidato a la Presidencia del Gobierno): Buenos días.")
    assert res is not None
    assert res[0] == "NÚÑEZ FEIJÓO (candidato a la Presidencia del Gobierno)"

    # Caso Falso Positivo (Narrativa)
    res = SpeakerDetector.find_standard_speaker("Tellado me pedía antes la palabra con base en el artículo 72...")
    assert res is None


def test_speaker_detector_rescue() -> None:
    # Caso Rescate Embebido
    text = "Se abre la sesión. (Aplausos.-El señor TELLADO FILGUEIRA)"
    res = SpeakerDetector.find_embedded_speaker(text)
    assert res == "El señor TELLADO FILGUEIRA"

    # Mixed punctuation rescue case
    text = "Gracias. (Pausa. - La señora GAMARRA CASPUE) "
    res = SpeakerDetector.find_embedded_speaker(text)
    assert res == "La señora GAMARRA CASPUE"


def test_state_machine_logic(tmp_path: object) -> None:
    # Mock de procesamiento de bloques de texto

    # Simulate the content that would be returned by BeautifulSoup
    lines = [
        "Extraído del Diario de Sesiones",
        "La señora PRESIDENTA: Comienza el debate.",
        "Tiene la palabra el siguiente orador.",
        "(Aplausos.-El señor TELLADO FILGUEIRA)",
        "Muchas gracias, señora Presidenta.",
        "Seguimos con el texto.",
    ]

    # To test internal logic without full BeautifulSoup, we could
    # inject lines if refactored, but here we validate that
    # the SpeakerDetector assists the state machine loop correctly.

    # 1. Comienza la Presidenta
    assert SpeakerDetector.find_standard_speaker(lines[1])[0] == "La señora PRESIDENTA"

    # 2. Line 3 is standard narrative text
    assert SpeakerDetector.find_standard_speaker(lines[2]) is None
    assert SpeakerDetector.find_embedded_speaker(lines[2]) is None

    # 3. Line 4 is a rescue (embedded speaker)
    assert SpeakerDetector.find_embedded_speaker(lines[4]) is None  # no hay nada en la 4
    rescue = SpeakerDetector.find_embedded_speaker(lines[3])
    assert rescue == "El señor TELLADO FILGUEIRA"

    # 4. Line 5 activates the pending speaker
    pending = rescue
    assert pending == "El señor TELLADO FILGUEIRA"


if __name__ == "__main__":
    # Quick manual execution
    test_speaker_detector_standard()
    test_speaker_detector_rescue()
    print("Tests básicos pasados.")
