import hashlib
import logging
import pathlib
from typing import Any, Dict, List, Optional

import pandas as pd
from bs4 import BeautifulSoup
from congreso_analisis.processing.speaker_detector import SpeakerDetector
from congreso_analisis.processing.speaker_validation import SpeakerValidator

logger = logging.getLogger(__name__)


class InterventionsExtractor:
    """
    Extraction of parliamentary interventions using a state machine
    to detect speaker turn changes (Standard and Embedded headers).
    """

    def __init__(self, term: str):
        self.term = term
        self.input_dir = pathlib.Path(f"data/raw/sessions/legislature={term}")
        self.output_file = pathlib.Path(f"data/silver/interventions/legislature={term}/interventions_raw.parquet")

    def run(self, file_list: Optional[List[pathlib.Path]] = None) -> pd.DataFrame:
        logger.info(f"InterventionsExtractor: Starting extraction for legislature {self.term}")

        files_to_process = file_list if file_list is not None else list(self.input_dir.glob("*.html"))
        if not files_to_process:
            logger.warning("No files found to process.")
            return pd.DataFrame()

        new_records = []
        for file in files_to_process:
            extracted_records = self._process_file(file)
            new_records.extend(extracted_records)

        df_new = pd.DataFrame(new_records)

        if not df_new.empty:
            if self.output_file.exists():
                df_old = pd.read_parquet(self.output_file)
                doc_ids_new = df_new["document_id"].unique()
                df_old = df_old[~df_old["document_id"].isin(doc_ids_new)]
                df_final = pd.concat([df_old, df_new], ignore_index=True)
            else:
                df_final = df_new

            self.output_file.parent.mkdir(parents=True, exist_ok=True)
            df_final.sort_values(["document_id", "intervention_order"], inplace=True)
            df_final.to_parquet(self.output_file, index=False)
            return df_final

        return pd.DataFrame()

    def _process_file(self, file_path: pathlib.Path) -> List[Dict[str, Any]]:
        """
        Processes an HTML file implementing the State Machine.
        """
        doc_name = file_path.name
        doc_id = file_path.stem

        try:
            with open(file_path, "rb") as file:
                content = file.read()
            soup = BeautifulSoup(content, "html.parser")
        except Exception as e:
            logger.error(f"Error loading {file_path}: {e}")
            return []

        for tag in soup(["script", "style"]):
            tag.decompose()

        text_blocks = [s.strip() for s in soup.get_text(separator="\n").splitlines() if s.strip()]

        # State Machine Variables
        records = []
        current_speaker = None
        current_text_fragments = []
        pending_speaker = None
        order_counter = 0

        for text in text_blocks:
            # 1. Skip empty lines or very short titles (without colon)
            # The standard detector will validate if it's a speaker
            if not text or (len(text) < 10 and not text.endswith(":")):
                continue

            # 2. Look for Standard Header (Priority 1)
            standard_match = SpeakerDetector.find_standard_speaker(text)
            if standard_match:
                speaker_header, initial_text = standard_match

                # Save previous intervention if exists
                if current_speaker:
                    records.append(
                        self._create_record(
                            doc_name, doc_id, order_counter, current_speaker, " ".join(current_text_fragments)
                        )
                    )
                    order_counter += 1

                # Activate new speaker
                current_speaker = speaker_header
                current_text_fragments = [initial_text] if initial_text else []
                pending_speaker = None  # Reset pending if there was one
                continue

            # 3. Handle pending speaker from a previous line (Embedded rescue)
            if pending_speaker:
                # If it's a pure annotation (stage direction), accumulate and keep waiting for real speech
                if SpeakerDetector.is_pure_acotacion(text):
                    current_text_fragments.append(text)
                    continue

                # If it's pure narrative (according to validator but without being a header),
                # accumulate but don't open the turn yet (could be noise between annotation and speech)
                if SpeakerValidator.is_likely_speaker(text):
                    current_text_fragments.append(text)
                    continue

                # Standard text reached. Close previous and open the new one.
                if current_speaker:
                    records.append(
                        self._create_record(
                            doc_name, doc_id, order_counter, current_speaker, " ".join(current_text_fragments)
                        )
                    )
                    order_counter += 1

                current_speaker = pending_speaker
                pending_speaker = None
                current_text_fragments = [text]
                continue

            # 4. Look for Embedded Rescue (Speaker at the end of an annotation)
            embedded_rescue = SpeakerDetector.find_embedded_speaker(text)
            if embedded_rescue:
                pending_speaker = embedded_rescue
                # The current line is accumulated to the active speaker
                current_text_fragments.append(text)
                continue

            # 5. Normal accumulation
            if current_speaker:
                current_text_fragments.append(text)

        # Last block
        if current_speaker:
            records.append(
                self._create_record(doc_name, doc_id, order_counter, current_speaker, " ".join(current_text_fragments))
            )

        return records

    def _create_record(self, doc_name: str, doc_id: str, order: int, speaker_raw: str, text_raw: str) -> Dict[str, Any]:
        """Creates a structured record with a unique ID."""
        speaker_label = speaker_raw.rstrip(":").strip()
        unique_str = f"{doc_id}_{order}_{speaker_raw}"
        intervention_id = hashlib.sha256(unique_str.encode()).hexdigest()
        return {
            "document_name": doc_name,
            "document_id": doc_id,
            "intervention_order": order,
            "speaker_raw": speaker_raw,
            "speaker_label": speaker_label,
            "intervention_id": intervention_id,
            "text_raw": text_raw,
        }
