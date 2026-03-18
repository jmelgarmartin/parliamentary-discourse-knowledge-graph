from congreso_analisis.processing.speaker_detector import SpeakerDetector


class MockFile:
    def __init__(self, name):
        self.name = name
        self.stem = name.split(".")[0]

    def __str__(self):
        return self.name


def test_speaker_detector_standard():
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


def test_speaker_detector_rescue():
    # Caso Rescate Embebido
    text = "Se abre la sesión. (Aplausos.-El señor TELLADO FILGUEIRA)"
    res = SpeakerDetector.find_embedded_speaker(text)
    assert res == "El señor TELLADO FILGUEIRA"

    # Caso Rescate con puntuación varia
    text = "Gracias. (Pausa. - La señora GAMARRA CASPUE) "
    res = SpeakerDetector.find_embedded_speaker(text)
    assert res == "La señora GAMARRA CASPUE"


def test_state_machine_logic(tmp_path):
    # Mock de procesamiento de bloques de texto

    # Simulamos el contenido que devolvería el soup
    lines = [
        "Extraído del Diario de Sesiones",
        "La señora PRESIDENTA: Comienza el debate.",
        "Tiene la palabra el siguiente orador.",
        "(Aplausos.-El señor TELLADO FILGUEIRA)",
        "Muchas gracias, señora Presidenta.",
        "Seguimos con el texto.",
    ]

    # Para probar la lógica interna sin BeautifulSoup completo,
    # podemos inyectar las líneas si refactorizamos un poco,
    # pero aquí validaremos que el SpeakerDetector ayuda al loop.

    # 1. Comienza la Presidenta
    assert SpeakerDetector.find_standard_speaker(lines[1])[0] == "La señora PRESIDENTA"

    # 2. Línea 3 es texto normal
    assert SpeakerDetector.find_standard_speaker(lines[2]) is None
    assert SpeakerDetector.find_embedded_speaker(lines[2]) is None

    # 3. Línea 4 es rescate
    assert SpeakerDetector.find_embedded_speaker(lines[4]) is None  # no hay nada en la 4
    rescue = SpeakerDetector.find_embedded_speaker(lines[3])
    assert rescue == "El señor TELLADO FILGUEIRA"

    # 4. Línea 5 activa al pendiente
    pending = rescue
    assert pending == "El señor TELLADO FILGUEIRA"


if __name__ == "__main__":
    # Ejecución manual rápida
    test_speaker_detector_standard()
    test_speaker_detector_rescue()
    print("Tests básicos pasados.")
