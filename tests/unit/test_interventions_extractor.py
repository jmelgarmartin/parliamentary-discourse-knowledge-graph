import unittest
from unittest.mock import MagicMock, mock_open, patch

from congress_analysis.silver.interventions_extractor import InterventionsExtractor


class TestInterventionsExtractor(unittest.TestCase):
    def setUp(self) -> None:
        self.extractor = InterventionsExtractor(term="15")

    def test_extract_from_content_basic(self) -> None:
        """Test basic extraction from HTML content."""
        html_content = """
        <html>
            <body>
                <section id="portlet_publicaciones">
                    <p>El señor PRESIDENTE: Se abre la sesión.</p>
                    <p>El señor GONZÁLEZ LÓPEZ: Muchas gracias.</p>
                </section>
            </body>
        </html>
        """
        doc_id = "DSCD-15-PL-1"
        doc_name = "DSCD-15-PL-1.html"

        records = self.extractor.extract_from_content(html_content, doc_id, doc_name)

        self.assertEqual(len(records), 2)
        self.assertEqual(records[0]["speaker_label"], "El señor PRESIDENTE")
        self.assertEqual(records[0]["text_raw"], "Se abre la sesión.")
        self.assertEqual(records[1]["speaker_label"], "El señor GONZÁLEZ LÓPEZ")
        self.assertEqual(records[1]["text_raw"], "Muchas gracias.")

    @patch(
        "builtins.open", new_callable=mock_open, read_data=b"<html><body><p>El senor PRESIDENTE: Hola</p></body></html>"
    )
    @patch("pathlib.Path.name", new_callable=MagicMock)
    @patch("pathlib.Path.stem", new_callable=MagicMock)
    def test_process_file_wrapper(self, mock_stem: MagicMock, mock_name: MagicMock, mock_file: MagicMock) -> None:
        """Test that _process_file correctly delegates to extract_from_content."""
        # Setup mocks
        mock_name.__get__ = MagicMock(return_value="test.html")
        mock_stem.__get__ = MagicMock(return_value="test")

        from pathlib import Path

        file_path = Path("test.html")

        with patch.object(InterventionsExtractor, "extract_from_content") as mock_extract:
            mock_extract.return_value = [{"record": "test"}]
            records = self.extractor._process_file(file_path)

            self.assertEqual(records, [{"record": "test"}])
            # Check that it was called with content (decoded), id, and name
            mock_extract.assert_called_once_with(
                "<html><body><p>El senor PRESIDENTE: Hola</p></body></html>", file_path.stem, file_path.name
            )


if __name__ == "__main__":
    unittest.main()
