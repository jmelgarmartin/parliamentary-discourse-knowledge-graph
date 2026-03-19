import re

from congress_analysis.processing.speaker_normalization import SpeakerNormalizer


class SpeakerValidator:
    """
    Validates if a text fragment extracted from the transcript appears to be a legitimate speaker
    and not narrative text or part of the speech.
    """

    # Extended narrative blacklist (words appearing in non-speaker contexts)
    # These stay in Spanish as they match the source text.
    NARRATIVE_BLACKLIST = [
        "ha dicho",
        "habla de",
        "ocupa la presidencia",
        "hace gestos",
        "niega con la cabeza",
        "realiza gestos",
        "golpean los escaños",
        "vuelve a mostrar",
        "vuelve a pedir",
        "gesticula",
        "aplaude",
        "se levanta",
        "sale del hemiciclo",
        "hace signos",
        "muestra",
        "continúa hablando",
        "alza el móvil",
        "se frota",
        "gestos negativos",
        "signos negativos",
    ]

    # Individual narrative verbs often used in descriptions
    NARRATIVE_VERBS = {
        "pedía",
        "pedia",
        "pide",
        "dijo",
        "dice",
        "hablaba",
        "comentó",
        "comento",
        "señala",
        "recordó",
        "recordo",
        "afirma",
        "explica",
        "reitera",
        "anunciaba",
        "anuncia",
        "pronuncia",
        "defendía",
        "defendia",
        "intervino",
    }

    @staticmethod
    def is_likely_speaker(text: str) -> bool:
        """
        Strict heuristic (Golden Rule):
        (Prefix) + NOMINAL_BLOCK + [optional parentheses] + :
        """
        if not text:
            return False

        text_strip = text.strip()
        has_colon = text_strip.endswith(":")
        text_lower = text_strip.lower()

        # 1. Basic safety filters and Blacklist
        if len(text_strip) > 250 or "?" in text_strip:
            return False

        if any(narrative in text_lower for narrative in SpeakerValidator.NARRATIVE_BLACKLIST):
            return False

        words = set(re.findall(r"\w+", text_lower))
        if words.intersection(SpeakerValidator.NARRATIVE_VERBS):
            return False

        # 2. Robust Prefix Verification
        text_norm = SpeakerNormalizer.normalize_text(text_strip)
        normal_prefixes = ["EL SENOR", "LA SENORA", "LOS SENORES", "LAS SENORAS"]

        prefix_detected = False
        for p in normal_prefixes:
            if text_norm.startswith(p + " "):
                prefix_detected = True
                break

        if prefix_detected:
            # Standard speaker headers MUST end with ":"
            if not has_colon:
                return False

            # Locate the nominal block after the prefix
            # Using regex to find the actual boundary (handling variations and OCR noise)
            m = re.match(r"^(?:El|La|Los|Las)\s+se[\s~ñ]*or[aes]*\s+", text_strip, re.IGNORECASE)
            if m:
                nominal_start = m.end()
            else:
                # Fallback: assume first two words are the prefix
                parts = text_strip.split(None, 2)
                if len(parts) >= 3:
                    nominal_start = text_strip.find(parts[2])
                else:
                    return False

            # Extract content between prefix and the ":"
            content = text_strip[nominal_start:-1].strip()

            # Clean parentheses to validate the nominal block (name or role)
            nominal_block = re.sub(r"\(.*?\)", "", content).strip()
            if not nominal_block:
                return False

            # Structural Rule:
            # 1. Allow specific lowercase tokens (connectors and titles)
            # 2. Reast must be UPPERCASE or Title Case (for mixed names)
            # 3. At least one "strong" token (UPPERCASE or Title Case of len >= 2)
            allowed_lc = {
                "de",
                "del",
                "la",
                "las",
                "los",
                "y",
                "i",
                "diputado",
                "diputada",
                "ministro",
                "ministra",
                "vicepresidenta",
                "vicepresidente",
            }

            tokens = nominal_block.split()
            has_strong_token = False

            for t in tokens:
                # Clean punctuation attached to token (e.g., "RODRÍGUEZ,")
                t_clean = t.strip(",.;")
                if not t_clean:
                    continue

                # Check 1: Allowed lowercase tokens
                if t_clean.lower() in allowed_lc:
                    continue

                # Check 2: Must be Uppercase or Title Case
                if not (t_clean.isupper() or t_clean.istitle()):
                    return False

                # Check 3: Strong token detection
                if len(t_clean) >= 2:
                    has_strong_token = True

            return has_strong_token

        # 3. Institutional Roles (No prefix or special cases)
        if has_colon:
            pure_roles = [
                "PRESIDENTE",
                "PRESIDENCIA",
                "VICEPRESIDENTE",
                "SECRETARIO",
                "MINISTRO",
                "MINISTRA",
                "REPRESENTANTE",
                "DEFENSOR",
            ]
            header_no_colon = text_strip[:-1].strip()
            header_upper = header_no_colon.upper()

            if any(role in header_upper for role in pure_roles):
                # Ignore parentheses content for casing balance (candidates have long lowercase descriptions)
                header_check = re.sub(r"\(.*?\)", "", header_no_colon).strip()
                count_upper = sum(1 for c in header_check if c.isupper())
                count_lower = sum(1 for c in header_check if c.islower())
                if count_upper > count_lower or count_lower == 0:
                    return True

            # 4. All-Uppercase Headers (Name format without prefix)
            if header_upper == header_no_colon and len(header_no_colon) >= 5:
                # Must contain at least one space to look like a full name (Surname Surname)
                if " " in header_no_colon:
                    return True

        return False


def is_likely_speaker(text: str) -> bool:
    """Compatibility wrapper for the current pipeline."""
    clean = text.rstrip(":").strip()
    return SpeakerValidator.is_likely_speaker(clean + ":")
