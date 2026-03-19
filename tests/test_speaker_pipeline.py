import pandas as pd
import pytest
from congress_analysis.processing.speaker_resolution import SpeakerResolver, SpeakerStatus
from congress_analysis.processing.speaker_validation import SpeakerValidator

# Fake deputy data for testing
DEPUTIES_DATA = [
    {"name": "Rodríguez Gómez de Celis, Alfonso"},
    {"name": "Gamarra Ruiz-Clavijo, Concepción"},
    {"name": "Tellado Filgueira, Miguel"},
    {"name": "Mico Mico, Águeda"},
    {"name": "Rufián Romero, Gabriel"},
    {"name": "Pisarello Prados, Gerardo"},
    {"name": "Jorda i Roura, Teresa"},
    {"name": "de Olano Vela, Jaime"},
    {"name": "Sagastizabal Unzetabarrenetxea, Idoia"},
    {"name": "Alvarez de Toledo Peralta-Ramos, Cayetana"},
    {"name": "Gavin i Valls, Isidre"},
    {"name": "Vázquez Blanco, Ana Belen"},
]


@pytest.fixture  # type: ignore[misc]
def resolver(tmp_path: object) -> SpeakerResolver:
    df_deps = pd.DataFrame(DEPUTIES_DATA)
    manual_path = tmp_path / "government_manual_mapping.csv"  # type: ignore[operator]
    return SpeakerResolver(df_deps, str(manual_path))


def test_validation() -> None:
    # Cases that MUST be valid (Headers now require :)
    assert SpeakerValidator.is_likely_speaker("El señor RODRÍGUEZ GÓMEZ DE CELIS:")
    assert SpeakerValidator.is_likely_speaker("La señora PRESIDENTA:")
    assert SpeakerValidator.is_likely_speaker("REPRESENTANTE DEL PARLAMENTO DE CATALUÑA (Munell i Garcia):")

    # Cases that MUST be invalid (Narrative)
    assert not SpeakerValidator.is_likely_speaker("Tellado me pedía antes la palabra con base en el artículo 72")
    assert not SpeakerValidator.is_likely_speaker("Abascal no está -¿para qué se va a quedar a escucharnos?")
    assert not SpeakerValidator.is_likely_speaker("Feijóo dijo que España tenía que aprender")
    assert not SpeakerValidator.is_likely_speaker("Pisarello hablaba de tres bloques")


def test_classification_and_matching(resolver: SpeakerResolver) -> None:
    # 1. Institutional Role
    df_test = pd.DataFrame([{"speaker_label": "La señora PRESIDENTA", "document_id": "test"}])
    res = resolver.resolver(df_test)
    assert res.iloc[0]["speaker_status"] == SpeakerStatus.INSTITUTIONAL.value

    # 2. Regional Representative
    df_test = pd.DataFrame(
        [{"speaker_label": "REPRESENTANTE DEL PARLAMENTO DE CATALUÑA (Munell i Garcia)", "document_id": "test"}]
    )
    res = resolver.resolver(df_test)
    assert res.iloc[0]["speaker_status"] == SpeakerStatus.REGIONAL.value
    assert res.iloc[0]["name_hint"] == "MUNELL I GARCIA"

    # 3. Other Institutional
    df_test = pd.DataFrame([{"speaker_label": "DEFENSOR DEL PUEBLO (Gabilondo Pujol)", "document_id": "test"}])
    res = resolver.resolver(df_test)
    assert res.iloc[0]["speaker_status"] == SpeakerStatus.OTHER_INST.value

    # 4. Exact Match
    df_test = pd.DataFrame([{"speaker_label": "El señor RODRÍGUEZ GÓMEZ DE CELIS", "document_id": "test"}])
    res = resolver.resolver(df_test)
    assert res.iloc[0]["speaker_status"] == SpeakerStatus.MATCHED_DEPUTY.value
    assert "Rodríguez Gómez de Celis" in res.iloc[0]["matched_name"]

    # 5. Fuzzy Match (OCR Error: GAMARA -> GAMARRA)
    df_test = pd.DataFrame([{"speaker_label": "La señora GAMARA RUIZ-CLAVIJO", "document_id": "test"}])
    res = resolver.resolver(df_test)
    assert res.iloc[0]["speaker_status"] == SpeakerStatus.MATCHED_DEPUTY.value
    assert res.iloc[0]["match_method"] == "fuzzy_match"
    assert "Gamarra Ruiz-Clavijo" in res.iloc[0]["matched_name"]

    # 6. Normalization (OCR Error: PERALTARAMOS -> PERALTA-RAMOS)
    df_test = pd.DataFrame([{"speaker_label": "ÁLVAREZ DE TOLEDO PERALTARAMOS", "document_id": "test"}])
    res = resolver.resolver(df_test)
    assert res.iloc[0]["speaker_status"] == SpeakerStatus.MATCHED_DEPUTY.value
    assert "Alvarez de Toledo" in res.iloc[0]["matched_name"]

    # 7. Prefix normalization (DE OLANO VELA)
    df_test = pd.DataFrame([{"speaker_label": "DE OLANO VELA", "document_id": "test"}])
    res = resolver.resolver(df_test)
    assert res.iloc[0]["speaker_status"] == SpeakerStatus.MATCHED_DEPUTY.value
    assert "de Olano Vela" in res.iloc[0]["matched_name"]


if __name__ == "__main__":
    # Script mode
    df_deps = pd.DataFrame(DEPUTIES_DATA)
    r = SpeakerResolver(df_deps)
    print("Testing Validation...")
    test_validation()
    print("Testing Resolution...")
    test_classification_and_matching(r)
    print("All tests passed!")
