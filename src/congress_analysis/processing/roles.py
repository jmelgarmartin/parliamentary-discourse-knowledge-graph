import re
from enum import Enum


class SpeakerRole(Enum):
    MP = "MP"  # Member of Parliament (Diputado/a)
    GOV_MEMBER = "GOV_MEMBER"  # Government Member (Miembro del Gobierno)
    CHAIR = "CHAIR"  # Speaker / Presidency / Bureau (Presidencia / Mesa)
    UNKNOWN = "UNKNOWN"


def normalize_person_name(label: str) -> str:
    """
    Extracts the first name/surnames from complex labels.
    E.g.: "The Madam PRESIDENT (Narbona Ruiz)" -> "Narbona Ruiz"
    E.g.: "Mr. SÁNCHEZ PÉREZ-CASTEJÓN" -> "SÁNCHEZ PÉREZ-CASTEJÓN"
    """
    clean = label.strip()

    # 1. Case with parentheses: "ROLE (Real Name)"
    match_paren = re.search(r"\(([^)]+)\)", clean)
    if match_paren:
        return match_paren.group(1).strip()

    # 2. Case without parentheses, clean common prefixes
    # Remove "El señor " / "La señora " / "Señor " / "Señora " (case insensitive)
    # Using Spanish treatment prefixes as they appear in the source text
    clean = re.sub(r"^(El|La|Los|Las)?\s*(señor(a|es|as)?)\s+", "", clean, flags=re.IGNORECASE)

    # Remove final punctuation (:)
    clean = clean.replace(":", "").strip()

    return clean


def detect_role_by_regex(raw_text: str, label_norm: str = "") -> SpeakerRole:
    """
    Attempts to detect the role based on the speaker text.
    """
    raw_upper = raw_text.upper()
    label_upper = (label_norm or raw_text).upper()

    # 1. Detect Government (President of the Government, Ministers...)
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
