import re
from typing import Optional, Tuple

from congreso_analisis.processing.speaker_validation import SpeakerValidator


class SpeakerDetector:
    """
    Specialized class for detecting speaker patterns (Standard and Embedded).
    Follows the state machine specification.
    """

    # Pattern families (Must end with :)
    # 1. Strict Structure: (Prefix) + UPPERCASE_BLOCK + [parentheses] + :
    # Note: We don't use IGNORECASE to enforce uppercase names in standard headers.
    RE_STANDARD = re.compile(r"^((?:El señor|La señora)\s+([A-ZÁÉÍÓÚÑ\s\-\.,]{3,})(?:\s*\([^)]*\))?)\s*:(.*)")

    # 2. Direct Roles (Optionally preserved in case they appear without prefix,
    # but the validator decides if they actually open a turn)
    RE_ROLE = re.compile(r"^(PRESIDENT[EA]|VICEPRESIDENT[EA]|SECRETARI[OA])\s*:(.*)")

    # 3. Government and Candidates (Unified in strict structure or special cases)
    RE_GOV = re.compile(
        r"^((?:MINISTRO|MINISTRA|VICEPRESIDENTA|PRESIDENTE)\s+(?:DE|DEL)\s+[A-ZÁÉÍÓÚÑ\s]{5,100})\s*:(.*)"
    )
    RE_CANDIDATE = re.compile(
        r"^([A-ZÁÉÍÓÚÑ\s]{3,60}\s*\((?:candidato|vicepresidenta|ministr|comisaria|ponente)[^)]*\))\s*:(.*)"
    )

    PATTERNS = [RE_STANDARD, RE_ROLE, RE_GOV, RE_CANDIDATE]

    # Pattern for embedded speaker at the end of an annotation: (Applause.-Mr. ...)
    RE_EMBEDDED_MARKER = re.compile(
        r"(\.|\-|\s)+((?:El|La|Los|Las)\s+señor[aes]*|PRESIDENT[EA])\s+([A-ZÁÉÍÓÚÑ\s]+)$", re.IGNORECASE
    )

    @classmethod
    def find_standard_speaker(cls, text: str) -> Optional[Tuple[str, str]]:
        """
        Checks if the line is a standard speaker header.
        """
        # Header must contain ":"
        if ":" not in text:
            return None

        for pattern in cls.PATTERNS:
            match = pattern.match(text)
            if match:
                header = match.group(1).strip()
                initial_text = match.group(len(match.groups())).strip()

                # Validate header quality
                if SpeakerValidator.is_likely_speaker(header + ":"):
                    return header, initial_text

        return None

    @classmethod
    def find_embedded_speaker(cls, text: str) -> Optional[str]:
        """
        Checks if the line contains a rescuable speaker at the end (embedded).
        E.g.: "... (Applause.-Mr. TELLADO FILGUEIRA)"
        """
        # Look for the last bracketed block
        matches = list(re.finditer(r"\(([^)]+)\)", text))
        if not matches:
            # Case of unclosed brackets at the end of paragraph
            last_bracket = text.rfind("(")
            if last_bracket != -1 and last_bracket > len(text) - 100:
                inner_content = text[last_bracket + 1 :]
            else:
                return None
        else:
            inner_content = matches[-1].group(1)

        marker_match = cls.RE_EMBEDDED_MARKER.search(inner_content)
        if marker_match:
            treatment = marker_match.group(2)
            # The name might contain a header with ":" (e.g., "Mr. X: Yes")
            # or be narrative (e.g., "Mr. X says...")
            potential_name = marker_match.group(3).strip()

            # Cut if there is a ":"
            if ":" in potential_name:
                potential_name = potential_name.split(":", 1)[0].strip()

            # Validate that it looks like a name (short, no narrative verbs)
            candidate = f"{treatment} {potential_name}".strip()
            if SpeakerValidator.is_likely_speaker(candidate + ":"):
                return candidate

        return None

    @staticmethod
    def is_pure_acotacion(text: str) -> bool:
        """
        Determines if a line is just a technical annotation (noise).
        E.g.: (Applause), (Pause), (Rumors).
        """
        text_clean = text.strip()
        if text_clean.startswith("(") and text_clean.endswith(")"):
            # If no embedded speaker inside, it's pure annotation
            return True
        return False
