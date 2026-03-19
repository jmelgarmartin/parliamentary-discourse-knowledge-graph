import logging
from typing import Any, Dict, List, Optional, Set

import pandas as pd
from congress_analysis.processing.filters import is_admin_block, should_keep_for_graph
from congress_analysis.processing.roles import SpeakerRole, detect_role_by_regex, normalize_person_name

logger = logging.getLogger(__name__)


def enrich_and_filter_interventions(
    df: pd.DataFrame,
    roster_mps: Optional[Set[str]] = None,
    roster_gov: Optional[Dict[str, str]] = None,
    config: Optional[Dict[str, Any]] = None,
) -> pd.DataFrame:
    """
    Apply enrichment and filtering to a dataframe of interventions.
    """
    conf = {
        "include_chair_speech": False,
        "keep_unknown": False,
        "admin_block_min_namelike_lines": 3,
    }
    if config:
        conf.update(config)

    results: List[Dict[str, Any]] = []

    for _, row in df.iterrows():
        row_dict = row.to_dict()

        speaker_raw = str(row_dict.get("speaker_raw", ""))
        speaker_label = str(row_dict.get("speaker_label", ""))
        text_raw = str(row_dict.get("text_raw", ""))

        # 1. Normalize Name
        name_norm = normalize_person_name(speaker_label)
        row_dict["speaker_name_norm"] = name_norm

        # 2. Detect Role
        role = detect_role_by_regex(speaker_label)

        # Refine role with rosters if available
        if role == SpeakerRole.UNKNOWN:
            if roster_gov and name_norm in roster_gov:
                role = SpeakerRole.GOV_MEMBER
            elif roster_mps and name_norm in roster_mps:
                role = SpeakerRole.MP

        # Fallback for Table items if they look like names
        if role == SpeakerRole.UNKNOWN:
            # If it's a very short name and starts with El/La...
            if speaker_label.upper() == speaker_label and len(speaker_label) < 100:
                if role not in [SpeakerRole.GOV_MEMBER, SpeakerRole.CHAIR]:
                    role = SpeakerRole.MP

        row_dict["speaker_role"] = role.value

        # 3. Apply Filtering
        is_admin = is_admin_block(text_raw)
        row_dict["is_admin_block"] = is_admin

        keep = should_keep_for_graph(
            role,
            is_admin,
            include_chair_speech=bool(conf.get("include_chair_speech", False)),
            keep_unknown=bool(conf.get("keep_unknown", False)),
        )
        row_dict["keep_for_graph"] = keep

        if role == SpeakerRole.UNKNOWN:
            logger.debug(f"Unknown role for speaker: {speaker_raw}")

        results.append(row_dict)

    return pd.DataFrame(results)
