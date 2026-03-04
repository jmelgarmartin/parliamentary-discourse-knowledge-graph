import hashlib
import logging
import pathlib
import re
from typing import Any, Dict, List, Optional

import pandas as pd
from bs4 import BeautifulSoup


class InterventionsExtractor:
    """
    Módulo para extraer intervenciones parlamentarias desde HTML Bronze.
    Salida: Parquet Silver.
    """

    def __init__(self, term: str):
        self.term = term
        self.input_dir = pathlib.Path(f"data/raw/sessions/legislature={term}")
        self.output_file = pathlib.Path(f"data/silver/interventions/legislature={term}/interventions_raw.parquet")
        self.dataset = "congreso-intervenciones"
        self.base_url = "https://www.congreso.es"

    def run(self, file_list: Optional[List[pathlib.Path]] = None) -> pd.DataFrame:
        """
        Versión incremental. Procesa solo file_list si se provee,
        si no, procesa todo el directorio input_dir.
        """
        logger.info(f"InterventionsExtractor: Iniciando extracción para legislatura {self.term}")

        # Decidir qué archivos procesar
        if file_list is not None:
            files_to_process = [f for f in file_list if f.suffix == ".html"]
            logger.info(f"Modo incremental: {len(files_to_process)} ficheros a procesar.")
        else:
            if not self.input_dir.exists():
                logger.error(f"Directorio de entrada no existe: {self.input_dir}")
                return pd.DataFrame()
            files_to_process = list(self.input_dir.glob("*.html"))
            logger.info(f"Modo completo: {len(files_to_process)} ficheros encontrados.")

        if not files_to_process:
            logger.warning("No hay ficheros para procesar.")
            return pd.DataFrame()

        # Extraer registros nuevos
        new_records = []
        for f in files_to_process:
            recs = self._process_file(f)
            new_records.extend(recs)

        df_new = pd.DataFrame(new_records)

        # Cargar existente para mergear si estamos en modo incremental
        if self.output_file.exists():
            df_old = pd.read_parquet(self.output_file)
            # Eliminar registros de los documentos que acabamos de re-procesar (evitar duplicados)
            doc_ids_new = df_new["document_id"].unique()
            df_old = df_old[~df_old["document_id"].isin(doc_ids_new)]
            df_final = pd.concat([df_old, df_new], ignore_index=True)
        else:
            df_final = df_new

        if not df_final.empty:
            self.output_file.parent.mkdir(parents=True, exist_ok=True)
            df_final.sort_values(["document_id", "intervention_order"], inplace=True)
            df_final.to_parquet(self.output_file, index=False)
            logger.info(
                f"Extracción completada: {len(df_new)} nuevas intervenciones. "
                f"Total: {len(df_final)} en {self.output_file}"
            )
            return df_final
        else:
            logger.warning("No se extrajeron intervenciones.")
            return pd.DataFrame()

    def _process_file(self, file_path: pathlib.Path) -> List[Dict[str, Any]]:
        """
        Parsea un solo archivo HTML y devuelve lista de intervenciones.
        """
        doc_name = file_path.name
        doc_id = file_path.stem

        with open(file_path, "r", encoding="utf-8") as f:
            html = f.read()

        soup = BeautifulSoup(html, "html.parser")
        # El portlet recortado suele tener el texto en tags <p> con clase 'texto_parlamentario'
        # o similar, pero por ahora buscaremos todos los párrafos significativos.
        paragraphs = soup.find_all("p")

        records: List[Dict[str, Any]] = []
        current_speaker_raw: Optional[str] = None
        current_text_fragments: List[str] = []
        order = 0

        # Patrón para detectar hablante: "La señora PRESIDENTA (Narbona Ruiz):"
        # Debe empezar por "El señor" o "La señora" y terminar en ":"
        speaker_pattern = re.compile(r"^(El|La)\s+señor(a)?\s+.*:", re.IGNORECASE)

        # Flag para ignorar todo hasta el primer speaker válido
        found_first_speaker = False

        for p in paragraphs:
            text = p.get_text().strip()
            if not text:
                continue

            # Eliminar ruido: "Página 123"
            if text.startswith("Página "):
                continue

            # Detectar cambio de speaker
            if speaker_pattern.match(text):
                # 1. Antes de cambiar, guardar la intervención anterior si existe
                if current_speaker_raw:
                    records.append(
                        self._create_record(
                            doc_name,
                            doc_id,
                            order,
                            current_speaker_raw,
                            " ".join(current_text_fragments),
                        )
                    )
                    order += 1

                # 2. Iniciar nueva intervención
                # SEGURO: speaker es hasta el primer ":"
                parts = text.split(":", 1)
                current_speaker_raw = parts[0].strip() + ":"
                found_first_speaker = True

                # Si hay texto tras el ":", es el primer fragmento del discurso
                if len(parts) > 1 and parts[1].strip():
                    current_text_fragments = [parts[1].strip()]
                else:
                    current_text_fragments = []
            else:
                # Es texto de discurso
                if found_first_speaker:
                    current_text_fragments.append(text)

        # Guardar la última del fichero
        if current_speaker_raw:
            records.append(
                self._create_record(doc_name, doc_id, order, current_speaker_raw, " ".join(current_text_fragments))
            )

        return records

    def _create_record(self, doc_name: str, doc_id: str, order: int, speaker_raw: str, text_raw: str) -> Dict[str, Any]:
        """
        Helper para formatear el diccionario de salida.
        """
        # speaker_label es el speaker_raw sin el ":" final y limpio
        speaker_label = speaker_raw.rstrip(":").strip()

        # Generar intervention_id (SHA256 del contenido para unicidad)
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


def main() -> None:
    """
    Interfaz de línea de comandos para el extractor.
    """
    import argparse

    parser = argparse.ArgumentParser(description="Extrae intervenciones desde HTML Bronze.")
    parser.add_argument("--term", type=str, required=True, help="Número de legislatura (ej: 15)")

    args = parser.parse_args()

    extractor = InterventionsExtractor(args.term)
    extractor.run()


if __name__ == "__main__":
    setup_logging = logging.getLogger("congreso_analisis")
    setup_logging.setLevel(logging.INFO)
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter("%(asctime)s - %(levelname)s - %(message)s"))
    setup_logging.addHandler(handler)

    logger = setup_logging
    main()
