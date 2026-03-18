import re
import unicodedata


class SpeakerNormalizer:
    """
    Phase 2: Canonical Speaker Normalization.
    Handles OCR error robustness and treatment/prefix cleaning.
    """

    @staticmethod
    def normalize_text(text: str) -> str:
        """
        Removes accents, special characters and normalizes whitespace.
        Adds tolerance for common OCR errors for the letter 'Ñ' (e.g., 'SEOR', 'SENOR').
        """
        if not text:
            return ""

        # Normalize to NFD to separate accents from letters
        text = "".join(c for c in unicodedata.normalize("NFD", text) if unicodedata.category(c) != "Mn")

        # Convert to uppercase for uniform comparisons
        text = text.upper()

        # OCR tolerance for "Ñ": If we detect SEOR, SENOR or SE~OR, unify to SENOR
        # Also supports SEORA, SEORES, SEORAS.
        text = re.sub(r"\bSEOR(A|ES|AS)?\b", r"SENOR\1", text)
        text = re.sub(r"\bSE~OR(A|ES|AS)?\b", r"SENOR\1", text)

        # Clean basic non-alphanumeric characters (preserving spaces and hyphens)
        text = re.sub(r"[^A-Z0-9\s\-]", " ", text)

        return " ".join(text.split())

    @staticmethod
    def clean_treatment(text: str) -> str:
        """
        Removes basic treatments and prefixes to obtain a clean name or role.
        Operates on already normalized text (uppercased and without accents).
        """
        text = SpeakerNormalizer.normalize_text(text)

        # Prefixes to remove: Sr/Sra (Mr/Ms) and derivatives.
        # Note: SENOR/A is already normalized by normalize_text call.
        prefixes = [
            r"^(EL|LA|LOS|LAS)\s+SENOR(A|ES|AS)?(\s+DON(A)?)?\b",
            r"^DIPUTADO\b",
            r"^DIPUTADA\b",
        ]

        for p in prefixes:
            text = re.sub(p, "", text).strip()

        return text

    @staticmethod
    def robust_person_normalization(text: str) -> str:
        """
        Robust normalization for person keys:
        - Removes accents
        - Uppercase
        - Normalizes hyphens and spaces
        - REMOVES forbidden roles, treatments, ordinals and prepositions
        Example: "LA SENORA VICEPRESIDENTA PRIMERA (Montero Cuadrado)" -> "MONTERO CUADRADO"
        """
        if not text:
            return ""

        # 1. Basic normalization (accents, uppercase, OCR tolerance)
        text = SpeakerNormalizer.normalize_text(text)

        # 2. Specific cleaning for person names: Remove roles and treatments
        # This list should be exhaustive for common parliamentary labels
        forbidden = [
            r"\bSENOR(A|ES|AS)?\b",
            r"\bDON(A)?\b",
            r"\bMINISTRO\b",
            r"\bMINISTRA\b",
            r"\bVICEPRESIDENTE\b",
            r"\bVICEPRESIDENTA\b",
            r"\bPRESIDENTE\s+DEL\s+GOBIERNO\b",
            r"\bPRESIDENTE\b",
            r"\bPRESIDENTA\b",
            r"\bDIPUTADO\b",
            r"\bDIPUTADA\b",
            r"\bPRIMER(O|A)?\b",
            r"\bSEGUND(O|A)?\b",
            r"\bTERCER(O|A)?\b",
            r"\bCUART(O|A)?\b",
            r"\bEL\b",
            r"\bLA\b",
            r"\bLOS\b",
            r"\bLAS\b",
            r"\bDE\b",
            r"\bDEL\b",
            r"\bY\b",
        ]

        for p in forbidden:
            text = re.sub(p, " ", text).strip()  # Use space to avoid joining words

        # 3. Final cleaning: Remove any remaining non-alphanumeric except spaces
        text = text.replace("-", " ")
        text = re.sub(r"[^A-Z0-9\s]", "", text)

        # 4. Collapse multiple spaces
        return " ".join(text.split()).strip()

    @staticmethod
    def is_probably_person_name(text: str) -> bool:
        """
        Heuristic to determine if a string is a person's name or a role.
        Names usually don't contain role keywords and follow certain patterns.
        """
        if not text:
            return False

        cleaned = SpeakerNormalizer.robust_person_normalization(text)
        if not cleaned:
            return False

        # If the cleaned text still contains role keywords (which robust_person_normalization should remove)
        # but for extra safety we check length and content.
        # Roles are often long and descriptive. Names are usually 2-4 tokens.
        tokens = cleaned.split()
        if len(tokens) < 1 or len(tokens) > 6:
            return False

        return True
