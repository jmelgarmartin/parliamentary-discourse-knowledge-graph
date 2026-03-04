import re
from enum import Enum


class SpeakerRole(Enum):
    MP = "MP"  # Diputado/a
    GOV_MEMBER = "GOV_MEMBER"  # Miembro del Gobierno
    CHAIR = "CHAIR"  # Presidencia / Mesa
    UNKNOWN = "UNKNOWN"


def normalize_person_name(label: str) -> str:
    """
    Extrae el nombre de pila/apellidos de etiquetas complejas.
    Ej: "La señora PRESIDENTA (Narbona Ruiz)" -> "Narbona Ruiz"
    Ej: "El señor SÁNCHEZ PÉREZ-CASTEJÓN" -> "SÁNCHEZ PÉREZ-CASTEJÓN"
    """
    clean = label.strip()

    # 1. Caso con paréntesis: "ROL (Nombre Real)"
    match_paren = re.search(r"\(([^)]+)\)", clean)
    if match_paren:
        return match_paren.group(1).strip()

    # 2. Caso sin paréntesis, limpiar prefijos comunes
    # Eliminar "El señor " / "La señora " / "Señor " / "Señora " (case insensitive)
    clean = re.sub(r"^(El|La|Los|Las)?\s*(señor(a|es|as)?)\s+", "", clean, flags=re.IGNORECASE)

    # Eliminar puntuación final (:)
    clean = clean.replace(":", "").strip()

    return clean


def detect_role_by_regex(raw_text: str, label_norm: str = "") -> SpeakerRole:
    """
    Intenta detectar el rol basado en el texto del speaker.
    """
    raw_upper = raw_text.upper()
    label_upper = (label_norm or raw_text).upper()

    # 1. Detect Government (Presidente del Gobierno, Ministros...)
    gov_keywords = [
        "PRESIDENTE DEL GOBIERNO",
        "MINISTRO",
        "MINISTRA",
        "VICEPRESIDENT",
    ]
    if any(k in raw_upper for k in gov_keywords):
        return SpeakerRole.GOV_MEMBER

    # 2. Detect by common prefixes/keywords
    if "PRESIDENT" in label_upper or "MESA" in label_upper:
        # If it contains President but not "del Gobierno" (handled above)
        return SpeakerRole.CHAIR

    # Default candidates
    if "EL SEÑOR" in raw_upper or "LA SEÑORA" in raw_upper:
        return SpeakerRole.MP

    return SpeakerRole.UNKNOWN
