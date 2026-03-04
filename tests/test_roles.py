from congreso_analisis.processing.roles import SpeakerRole, detect_role_by_regex, normalize_person_name


def test_normalize_person_name_chair() -> None:
    assert normalize_person_name("PRESIDENTA DE LA MESA DE EDAD (Narbona Ruiz)") == "Narbona Ruiz"
    assert normalize_person_name("La señora PRESIDENTA: ") == "PRESIDENTA"  # Fallback if no paren


def test_normalize_person_name() -> None:
    assert normalize_person_name("SÁNCHEZ PÉREZ-CASTEJÓN, Pedro") == "SÁNCHEZ PÉREZ-CASTEJÓN, Pedro"
    assert normalize_person_name("El señor SÁNCHEZ PÉREZ-CASTEJÓN: ") == "SÁNCHEZ PÉREZ-CASTEJÓN"


def test_detect_role_chair() -> None:
    role = detect_role_by_regex(
        "La señora PRESIDENTA DE LA MESA DE EDAD (Narbona Ruiz):", "PRESIDENTA DE LA MESA DE EDAD (Narbona Ruiz)"
    )
    assert role == SpeakerRole.CHAIR


def test_detect_role_regex() -> None:
    role = detect_role_by_regex("El señor presidente del Gobierno (Sánchez Pérez-Castejón):", "Sánchez Pérez-Castejón")
    assert role == SpeakerRole.GOV_MEMBER


def test_detect_role_long_line() -> None:
    # Test line length limit if applicable
    long_label = "La señora " + "A" * 150 + ":"
    assert detect_role_by_regex(long_label) == SpeakerRole.MP
