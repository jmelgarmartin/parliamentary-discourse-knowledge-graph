import logging
import os
import re
from difflib import SequenceMatcher
from enum import Enum
from typing import Any, Dict, Optional, Tuple

import pandas as pd
from congress_analysis.processing.speaker_normalization import SpeakerNormalizer
from congress_analysis.processing.speaker_validation import SpeakerValidator

logger = logging.getLogger(__name__)


class SpeakerStatus(Enum):
    MATCHED_DEPUTY = "matched_deputy"
    UNRESOLVED = "unresolved_speaker"
    INVALID = "invalid_speaker_label"
    INSTITUTIONAL = "institutional_role"
    REGIONAL = "regional_representative"
    GOV = "matched_government_member"
    OTHER_INST = "other_institutional_speaker"
    RESOLVED_MANUAL = "resolved_manual"  # Nuevo status para mapeos manuales confirmados


class GovernmentManualManager:
    """
    Manages the persistent dictionary of non-MP Government members and candidates.
    Groups multiple label variants under a unique 'canonical_person_key'.
    """

    def __init__(self, file_path: str):
        self.file_path = file_path
        self.df = self._load_or_create()
        self._modified = False

    def _load_or_create(self) -> pd.DataFrame:
        columns = [
            "canonical_person_key",
            "matched_person_name",
            "status",
            "preferred_name_hint",
            "preferred_cargo",
            "aliases",
            "notes",
        ]
        if os.path.exists(self.file_path):
            try:
                df = pd.read_csv(self.file_path, encoding="utf-8")
                # Handle migration if old columns exist
                if "speaker_clean" in df.columns and "canonical_person_key" not in df.columns:
                    logger.info("Migrating old manual dictionary schema...")
                    df["canonical_person_key"] = df["speaker_clean"]
                    df["preferred_name_hint"] = df["name_hint_parentheses"].fillna(df["name_hint_outside_parentheses"])
                    df["preferred_cargo"] = df["cargo_normalized"]
                    df["aliases"] = df["speaker_label_original"]

                # Ensure all new columns exist
                for col in columns:
                    if col not in df.columns:
                        df[col] = ""

                return df[columns]  # Keep only canonical columns
            except Exception as e:
                logger.error(f"Error loading manual dictionary: {e}")
                return pd.DataFrame(columns=columns)
        else:
            os.makedirs(os.path.dirname(self.file_path), exist_ok=True)
            df = pd.DataFrame(columns=columns)
            df.to_csv(self.file_path, index=False, encoding="utf-8")
            return df

    def find_entry(self, name_hint: Optional[str] = None, cargo_hint: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Finds an entry by name or cargo hint, with subset and fuzzy matching support.
        """
        if self.df.empty:
            return None

        if name_hint:
            norm_name = SpeakerNormalizer.robust_person_normalization(name_hint)
            if not norm_name:
                return None

            # 1. Direct match on canonical_person_key
            match = self.df[self.df["canonical_person_key"] == norm_name]
            if not match.empty:
                return dict(match.iloc[0].to_dict())

            # 2. Search in aliases
            # We use a simple loop over rows as the DF is small (< 1000 rows)
            for _, row in self.df.iterrows():
                aliases = [a.strip().upper() for a in str(row.get("aliases", "")).split(";")]
                if norm_name in aliases:
                    return dict(row.to_dict())

            # 3. Subset/Swap/Fuzzy matching (Smart matching)
            target_tokens = set(norm_name.split())
            for _, row in self.df.iterrows():
                row_key = str(row["canonical_person_key"])
                row_tokens = set(row_key.split())

                if (target_tokens == row_tokens and len(target_tokens) > 1) or (
                    (target_tokens.issubset(row_tokens) or row_tokens.issubset(target_tokens))
                    and len(target_tokens.intersection(row_tokens)) >= 2
                ):
                    return dict(row.to_dict())

                # High-confidence Fuzzy (for OCR errors)
                if SequenceMatcher(None, norm_name, row_key).ratio() > 0.85:
                    return dict(row.to_dict())

        if cargo_hint:
            norm_cargo = SpeakerNormalizer.normalize_text(cargo_hint)
            if norm_cargo:
                match = self.df[self.df["preferred_cargo"] == norm_cargo]
                if not match.empty:
                    return dict(match.iloc[0].to_dict())

        return None

    def add_or_update_entry(self, person_key: str, label_variant: str, name_hint: str, cargo: str) -> None:
        """
        Add a new entry or update an existing one by person_key.
        Enforces strict name vs cargo separation.
        """
        if not person_key:
            return

        # 1. Validate if the key is a real person name
        is_provisional = not SpeakerNormalizer.is_probably_person_name(person_key)

        # 2. Check for existing entry (exact or set-based)
        existing = self.find_entry(person_key)

        if not existing:
            # Add new row
            status = "pending_manual_review"
            if is_provisional:
                status = "provisional_cargo"

            # Ensure name_hint is name and cargo is cargo
            # If we don't have a name_hint (e.g., it was keyed by cargo), swap or leave empty
            final_name_hint = name_hint if SpeakerNormalizer.is_probably_person_name(name_hint) else ""
            final_cargo = cargo

            # Special fix for cases where name might be in cargo:
            if not final_name_hint and SpeakerNormalizer.is_probably_person_name(cargo):
                final_name_hint = cargo
                final_cargo = ""

            new_entry = {
                "canonical_person_key": person_key,
                "matched_person_name": "",
                "status": status,
                "preferred_name_hint": final_name_hint,
                "preferred_cargo": final_cargo,
                "aliases": label_variant,
                "notes": "Added automatically" + (" (Provisional)" if is_provisional else ""),
            }
            self.df = pd.concat([self.df, pd.DataFrame([new_entry])], ignore_index=True)
            self._modified = True
            logger.info(f"New entry added to manual dictionary: {person_key} ({status})")
        else:
            # Update existing entry with new alias
            pk = existing["canonical_person_key"]
            idx = self.df.index[self.df["canonical_person_key"] == pk].tolist()[0]

            current_aliases = str(self.df.at[idx, "aliases"]).split(";")
            current_aliases = [a.strip() for a in current_aliases if a.strip()]

            if label_variant not in current_aliases:
                current_aliases.append(label_variant)
                self.df.at[idx, "aliases"] = "; ".join(current_aliases)
                self._modified = True
                logger.debug(f"New alias added to {pk}: {label_variant}")

    def consolidate_entries(self) -> None:
        """
        Phase 6: Consolidate equivalent entries.
        - Merge name swaps (PEREZ SANCHEZ vs SANCHEZ PEREZ).
        - Merge subset names (GRANDE MARLASKA vs GRANDE MARLASKA GOMEZ).
        - Merge provisional cargo rows into person rows if name hint matches.
        """
        if self.df.empty:
            return

        logger.info("Starting manual dictionary consolidation...")
        initial_count = len(self.df)

        # 1. Clean current data (remove roles from keys)
        self.df["canonical_person_key"] = self.df["canonical_person_key"].apply(
            SpeakerNormalizer.robust_person_normalization
        )
        # Drop rows that became empty after normalization
        self.df = self.df[self.df["canonical_person_key"] != ""].copy()

        # 2. Iterative merge
        # We sort by status to ensure resolved/pending rows are the "base" for merges
        status_priority = {"resolved_manual": 0, "pending_manual_review": 1, "provisional_cargo": 2}
        self.df["_sort"] = self.df["status"].map(lambda x: status_priority.get(str(x), 99))
        self.df = self.df.sort_values("_sort").drop(columns=["_sort"])

        new_rows = []
        processed_indices = set()

        data = self.df.to_dict("records")
        for i in range(len(data)):
            if i in processed_indices:
                continue

            base = data[i]
            base_key = str(base["canonical_person_key"])
            base_tokens = set(base_key.split())

            # Find candidates to merge into this one
            for j in range(i + 1, len(data)):
                if j in processed_indices:
                    continue

                target = data[j]
                target_key = str(target["canonical_person_key"])
                target_tokens = set(target_key.split())

                should_merge = False

                # Rule A: Same token set (swap)
                if base_tokens == target_tokens and len(base_tokens) > 1:
                    should_merge = True

                # Rule B: One is subset of another
                elif (base_tokens.issubset(target_tokens) or target_tokens.issubset(base_tokens)) and len(
                    base_tokens.intersection(target_tokens)
                ) >= 2:
                    should_merge = True

                # Rule C: Fuzzy match (OCR errors or joined words)
                else:
                    # Sort words to handle name swapping in fuzzy match
                    s_base = " ".join(sorted(base_key.split()))
                    s_target = " ".join(sorted(target_key.split()))
                    similarity = SequenceMatcher(None, s_base, s_target).ratio()
                    if similarity > 0.85:  # Slightly lowered for better OCR resilience
                        should_merge = True

                # Rule D: Provisional row matches person row via hint
                if (
                    not should_merge
                    and base["status"] != "provisional_cargo"
                    and target["status"] == "provisional_cargo"
                ):
                    hint = SpeakerNormalizer.robust_person_normalization(str(target["preferred_name_hint"]))
                    if hint == base_key:
                        should_merge = True

                if should_merge:
                    # Merge target into base
                    processed_indices.add(j)
                    # Aggregate aliases
                    best_aliases = set(str(base["aliases"]).split("; "))
                    target_aliases = set(str(target["aliases"]).split("; "))
                    best_aliases.update([a.strip() for a in target_aliases if a.strip()])
                    base["aliases"] = "; ".join(sorted([a for a in best_aliases if a]))

                    # Carry over missing hints/cargos
                    if not base["preferred_name_hint"] and target["preferred_name_hint"]:
                        base["preferred_name_hint"] = target["preferred_name_hint"]
                    if not base["preferred_cargo"] and target["preferred_cargo"]:
                        base["preferred_cargo"] = target["preferred_cargo"]

                    # Prefer person key over cargo key
                    if not SpeakerNormalizer.is_probably_person_name(
                        base_key
                    ) and SpeakerNormalizer.is_probably_person_name(target_key):
                        base["canonical_person_key"] = target_key
                        base_key = target_key
                        base_tokens = target_tokens

            new_rows.append(base)
            processed_indices.add(i)

        self.df = pd.DataFrame(new_rows)
        # Final cleanup: ensure Diaz Perez is correct if found
        for idx, row in self.df.iterrows():
            if "DIAZ PEREZ" in str(row["preferred_cargo"]) and "VICEPRESIDENTA" in str(row["canonical_person_key"]):
                # Swap
                self.df.at[idx, "canonical_person_key"] = "DIAZ PEREZ"
                self.df.at[idx, "preferred_name_hint"] = "DIAZ PEREZ"
                self.df.at[idx, "preferred_cargo"] = row["canonical_person_key"]

        self._modified = True
        logger.info(f"Consolidation complete: {initial_count} -> {len(self.df)} rows.")

    def save_if_modified(self) -> None:
        # Always run consolidation before saving to ensure quality
        if self._modified:
            self.consolidate_entries()
            try:
                self.df.to_csv(self.file_path, index=False, encoding="utf-8")
                logger.info(f"Manual dictionary saved to {self.file_path}")
                self._modified = False
            except Exception as e:
                logger.error(f"Error saving manual dictionary: {e}")


class SpeakerResolver:
    """
    Phases 3 and 4: Classification and Matching.
    Manages resolution by delegating to deputies or the manual Government dictionary.
    """

    def __init__(self, df_deputies: pd.DataFrame, manual_dict_path: str):
        self.df_deputies = df_deputies
        self._prepare_deputy_index()
        self.manual_manager = GovernmentManualManager(manual_dict_path)

    def _prepare_deputy_index(self) -> None:
        """Pre-calculate normalized versions of deputy names."""
        self.deputy_names = []
        for name in self.df_deputies["name"].dropna():
            norm = SpeakerNormalizer.robust_person_normalization(name)
            self.deputy_names.append(norm)

    def _find_match(self, name_hint: str) -> Tuple[Optional[str], float, str]:
        """
        Search for a match in the deputies dataset using exact and fuzzy strategies.
        """
        if not name_hint:
            return None, 0.0, "none"

        # 1. Exact Match (normalized)
        if name_hint in self.deputy_names:
            idx = self.deputy_names.index(name_hint)
            return self.df_deputies.iloc[idx]["name"], 1.0, "exact_match"

        # 2. Fuzzy Match
        import difflib

        matches = difflib.get_close_matches(name_hint, self.deputy_names, n=1, cutoff=0.85)
        if matches:
            best_match = matches[0]
            idx = self.deputy_names.index(best_match)
            ratio = SequenceMatcher(None, name_hint, best_match).ratio()
            return self.df_deputies.iloc[idx]["name"], float(round(ratio, 2)), "fuzzy_match"

        return None, 0.0, "no_match_found"

    def _classify(self, label: str) -> Tuple[SpeakerStatus, str]:
        """
        Classify the label into a category (MP, GOV, INSTITUTIONAL, etc.)
        """
        label_upper = label.upper()
        norm = SpeakerNormalizer.normalize_text(label)

        # 1. Institutional Roles (Pure)
        pure_roles = ["PRESIDENTA", "PRESIDENTE", "PRESIDENCIA", "SECRETARIA", "SECRETARIO", "MESA"]
        clean_for_role = SpeakerNormalizer.clean_treatment(label)
        if any(r == clean_for_role.upper() for r in pure_roles):
            return SpeakerStatus.INSTITUTIONAL, "INSTITUTIONAL"

        # 2. Government Members
        gov_keywords = ["MINISTRO", "MINISTRA", "VICEPRESIDENTE", "VICEPRESIDENTA", "PRESIDENTE DEL GOBIERNO"]
        if any(kw in label_upper for kw in gov_keywords):
            return SpeakerStatus.GOV, "GOV"

        # 3. Regional Representatives
        if (
            "REPRESENTANTE DEL PARLAMENTO" in norm
            or "REPRESENTANTE DE LAS CORTES" in norm
            or "REPRESENTANTE DE LA ASAMBLEA" in norm
        ):
            return SpeakerStatus.REGIONAL, "REGIONAL"

        # 4. Other roles (Ombudsman, etc.)
        other_inst = ["DEFENSOR DEL PUEBLO", "PRESIDENTE DE LA JUNTA", "PRESIDENTE DE LA REGIÓN"]
        if any(kw in label_upper for kw in other_inst):
            return SpeakerStatus.OTHER_INST, "OTHER_INST"

        # 5. Default: Member of Parliament (MP)
        return SpeakerStatus.MATCHED_DEPUTY, "MP"

    def resolver(self, df_interventions: pd.DataFrame) -> pd.DataFrame:
        """
        Complete speaker identification process.
        """
        results = []
        for _, row in df_interventions.iterrows():
            label = str(row["speaker_label"])

            # PHASE 1: Structural validation
            if not SpeakerValidator.is_likely_speaker(label + ":"):
                status = SpeakerStatus.INVALID
                speaker_type = "INVALID"
                name_hint = ""
                matched_name = None
                confidence = 0.0
                method = "validator_fail"
            else:
                # PHASE 2: Normalization and PHASE 3: Classification
                status, speaker_type = self._classify(label)

                # PHASE 4: Advanced Matching (A/B Candidate Strategy)
                matched_name = None
                confidence = 0.0
                method = "no_match_found"

                # Pre-calculate candidates A and B
                match_paren = re.search(r"\(([^)]+)\)", label)
                cand_a = ""
                if match_paren:
                    cand_a = SpeakerNormalizer.robust_person_normalization(match_paren.group(1))

                cand_b_raw = re.sub(r"\(.*?\)", "", label).strip()
                cand_b = SpeakerNormalizer.robust_person_normalization(cand_b_raw)
                name_hint = cand_b  # default fallback

                if status in [
                    SpeakerStatus.MATCHED_DEPUTY,
                    SpeakerStatus.GOV,
                    SpeakerStatus.REGIONAL,
                    SpeakerStatus.OTHER_INST,
                ]:
                    # 1. Attempt matching with A (if exists)
                    if cand_a:
                        matched_name, confidence, method = self._find_match(cand_a)
                        name_hint = cand_a

                    # 2. If A fails or doesn't exist, attempt with B
                    if not matched_name and cand_b:
                        matched_name, confidence, method = self._find_match(cand_b)
                        # Only update name_hint to cand_b if we found a match or if we don't have cand_a
                        if matched_name or not cand_a:
                            name_hint = cand_b

                    # Specific logic for GOV (Phase 4.1: Fallback to manual dictionary)
                    if status == SpeakerStatus.GOV:
                        if not matched_name:
                            # Use robust normalization for person key with strict priority
                            person_key = ""

                            # Priority 1: Parentheses if they look like a name
                            if cand_a and SpeakerNormalizer.is_probably_person_name(cand_a):
                                person_key = SpeakerNormalizer.robust_person_normalization(cand_a)

                            # Priority 2: Outside text if it looks like a name
                            if not person_key and cand_b and SpeakerNormalizer.is_probably_person_name(cand_b):
                                person_key = SpeakerNormalizer.robust_person_normalization(cand_b)

                            # Priority 3: Fallback to provisional cargo key
                            if not person_key:
                                person_key = SpeakerNormalizer.robust_person_normalization(cand_b)

                            if person_key:
                                manual_entry = self.manual_manager.find_entry(person_key)

                                if manual_entry:
                                    if manual_entry["status"] == SpeakerStatus.RESOLVED_MANUAL.value:
                                        matched_name = manual_entry["matched_person_name"]
                                        status = SpeakerStatus.RESOLVED_MANUAL
                                        method = "manual_match"
                                        confidence = 1.0
                                    else:
                                        method = "manual_pending"
                                else:
                                    # Auto-populate manual dictionary
                                    self.manual_manager.add_or_update_entry(
                                        person_key=person_key,
                                        label_variant=label,
                                        name_hint=cand_a if SpeakerNormalizer.is_probably_person_name(cand_a) else "",
                                        cargo=SpeakerNormalizer.normalize_text(cand_b),
                                    )
                                    method = "manual_added"
                            else:
                                method = "manual_fail_nomkey"

                    # If still no match but it's an MP, mark as UNRESOLVED
                    if not matched_name and status == SpeakerStatus.MATCHED_DEPUTY:
                        status = SpeakerStatus.UNRESOLVED
                else:
                    matched_name, confidence, method = None, 0.0, "none"

            results.append(
                {
                    **row.to_dict(),
                    "speaker_clean": SpeakerNormalizer.clean_treatment(label),
                    "speaker_status": status.value if hasattr(status, "value") else status,
                    "speaker_type": speaker_type,
                    "matched_name": matched_name,
                    "match_confidence": confidence,
                    "match_method": method,
                    "name_hint": name_hint,
                }
            )

        return pd.DataFrame(results)

    def generate_review_report(self, df_resolved: pd.DataFrame, output_path: str) -> None:
        """
        Phase 5: Generate report of UNIDENTIFIED speakers.
        Deduplicates by person/canonical_key to avoid redundant entries.
        """
        # 1. Identify unresolved cases
        mask_no_match = df_resolved["matched_name"].isna() | (df_resolved["matched_name"] == "")
        to_review = df_resolved[mask_no_match].copy()

        if to_review.empty:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write("# No speakers pending review.\n")
            return

        # 2. Filter: Exclude pure institutional roles
        to_review = to_review[to_review["speaker_status"] != SpeakerStatus.INSTITUTIONAL.value]

        if to_review.empty:
            with open(output_path, "w", encoding="utf-8") as f:
                f.write("# No non-institutional speakers pending review.\n")
            return

        # 3. Add canonical_person_key for grouping (for GOV/UNRESOLVED)
        def get_group_key(row: "pd.Series[object]") -> str:
            label = str(row["speaker_label"])
            # Try to resolve via manual dictionary first to use consolidated keys
            match_paren = re.search(r"\(([^)]+)\)", label)
            p_name = ""
            if match_paren:
                p_name = SpeakerNormalizer.robust_person_normalization(match_paren.group(1))

            if p_name:
                m_entry = self.manual_manager.find_entry(p_name)
                if m_entry:
                    return str(m_entry["canonical_person_key"])
                return p_name

            # Fallback for when there's no parenthesis
            cand_b_raw = re.sub(r"\(.*?\)", "", label).strip()
            p_name_b = SpeakerNormalizer.robust_person_normalization(cand_b_raw)
            m_entry_b = self.manual_manager.find_entry(p_name_b)
            if m_entry_b:
                return str(m_entry_b["canonical_person_key"])
            return str(p_name_b)

        to_review["canonical_person_key"] = to_review.apply(get_group_key, axis=1)

        # 4. Group by canonical key and take representative representative
        # We keep the most recent document_id and the original labels as aliases
        grouped = (
            to_review.groupby("canonical_person_key")
            .agg(
                {
                    "document_id": "last",
                    "speaker_label": lambda x: " | ".join(sorted(set(x))),
                    "speaker_clean": "first",
                    "speaker_status": "first",
                    "speaker_type": "first",
                    "match_method": "first",
                }
            )
            .reset_index()
        )

        grouped = grouped.sort_values(["speaker_status", "canonical_person_key"])

        try:
            with open(output_path, "w", encoding="utf-8") as f:
                header = "PERSON_KEY | STATUS | TYPE | DOC_EXAMPLE | LABELS_SEEN\n"
                f.write(header)
                f.write("-" * len(header) + "\n")

                for _, row in grouped.iterrows():
                    line = (
                        f"{row['canonical_person_key']} | "
                        f"{row['speaker_status']} | "
                        f"{row['speaker_type']} | "
                        f"{row['document_id']} | "
                        f"{row['speaker_label']}\n"
                    )
                    f.write(line)

            logger.info(f"Operational report generated at: {output_path}")
            logger.info(f"  - Total unique persons in report: {len(grouped)}")

        except Exception as e:
            logger.error(f"Error writing report: {e}")

    def save_manual_dictionary(self) -> None:
        """Persist changes to the government dictionary."""
        self.manual_manager.save_if_modified()
