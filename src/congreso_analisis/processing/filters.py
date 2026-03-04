import re

from .roles import SpeakerRole


def is_admin_block(text: str, min_namelike_lines: int = 30) -> bool:
    """
    Detects if a text block is administrative/formal rather than standard speech.
    """
    # 1. Strong pattern detection
    admin_patterns = [
        r"RELACIÓN ALFABÉTICA",
        r"REAL DECRETO",
        r"Por los señores secretarios",
        r"se procede a dar lectura",
        r"RECURSOS CONTENCIOSO-ELECTORALES",
        r"Página \d+",
    ]

    for pattern in admin_patterns:
        if re.search(pattern, text):
            return True

    # 2. Ratio of lines looking like names
    # Patterns: "APELLIDOS, Nombre" or "APELLIDOS" in uppercase
    lines = [line.strip() for line in text.split("\n") if line.strip()]
    if not lines:
        return False

    name_like_count = 0
    # Pattern: UPPERCASE NAMES, Optional comma + Name
    name_pat = re.compile(r"^[A-ZÁÉÍÓÚÜÑ\-\s]+(?:,\s+.+)?$")

    for line in lines:
        if name_pat.match(line):
            name_like_count += 1

    if name_like_count >= min_namelike_lines:
        return True

    return False


def should_keep_for_graph(
    role: SpeakerRole, is_admin: bool, include_chair_speech: bool = False, keep_unknown: bool = False
) -> bool:
    """
    Determines if an intervention should be included in the final graph.
    """
    if is_admin:
        return False

    if role in [SpeakerRole.MP, SpeakerRole.GOV_MEMBER]:
        return True

    if role == SpeakerRole.CHAIR:
        return include_chair_speech

    if role == SpeakerRole.UNKNOWN:
        return keep_unknown

    return False
