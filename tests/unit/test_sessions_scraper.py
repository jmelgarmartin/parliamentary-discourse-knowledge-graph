import pathlib
import unittest
from unittest.mock import MagicMock, mock_open, patch

from congress_analysis.ingestion.scrappers.sessions_scraper import SessionsScraper


class TestSessionsScraper(unittest.TestCase):
    def setUp(self) -> None:
        self.scraper = SessionsScraper(
            term="15",
            raw_root="data/raw",
            bronze_root="data/bronze",
            state_path="state/test_bronze.duckdb",
            headless=True,
        )

    @patch("congress_analysis.ingestion.scrappers.sessions_scraper.time.sleep", return_value=None)
    def test_extract_pleno_content_success(self, mock_sleep: MagicMock) -> None:
        """Test that _extract_pleno_content correctly extracts HTML using the driver."""
        mock_driver = MagicMock()
        mock_wait = MagicMock()
        self.scraper.driver = mock_driver
        self.scraper.wait = mock_wait

        mock_portlet = MagicMock()
        mock_portlet.get_attribute.return_value = "<html><body>Useful content</body></html>"
        mock_driver.find_element.return_value = mock_portlet

        # Mock window handles
        mock_driver.window_handles = ["main", "new"]

        url = "https://example.com/document"
        content = self.scraper._extract_pleno_content(url)

        self.assertEqual(content, "<html><body>Useful content</body></html>")
        mock_driver.execute_script.assert_called_with("window.open(arguments[0]);", url)
        mock_driver.switch_to.window.assert_called()
        mock_driver.close.assert_called()

    def test_save_pleno_content(self) -> None:
        """Test that _save_pleno_content saves content to the correct path."""
        document_id = "DSCD-15-PL-12"
        html_content = "<html>test</html>"

        with patch("pathlib.Path.mkdir") as mock_mkdir, patch("builtins.open", mock_open()) as m_open:
            path = self.scraper._save_pleno_content(document_id, html_content)

            expected_path = str(pathlib.Path("data/raw/sessions/legislature=15/DSCD-15-PL-12.html"))
            self.assertEqual(path, expected_path)
            mock_mkdir.assert_called_with(parents=True, exist_ok=True)
            m_open.assert_called_with(pathlib.Path(expected_path), "w", encoding="utf-8")
            m_open().write.assert_called_with(html_content)

    @patch.object(SessionsScraper, "_get_document_state", return_value=None)
    @patch.object(SessionsScraper, "_extract_pleno_content", return_value="<html>extracted</html>")
    @patch.object(SessionsScraper, "_save_pleno_content", return_value="path/to/file.html")
    @patch.object(SessionsScraper, "_update_document_state")
    def test_process_row_new_document(
        self, mock_update_state: MagicMock, mock_save: MagicMock, mock_extract: MagicMock, mock_get_state: MagicMock
    ) -> None:
        """Test the full flow of _process_row for a new document."""
        mock_row = MagicMock()
        mock_cell = MagicMock()
        mock_cell.text = "DSCD-15-PL-12 01/01/2024"
        mock_row.find_elements.return_value = [mock_cell]
        mock_row.text = "DSCD-15-PL-12 Pleno Congreso 01/01/2024"

        mock_link = MagicMock()
        mock_link.get_attribute.return_value = "https://example.com/doc"
        mock_row.find_element.return_value = mock_link

        record = self.scraper._process_row(mock_row)

        self.assertIsNotNone(record)
        self.assertEqual(record["document_id"], "DSCD-15-PL-12")
        self.assertEqual(record["raw_path"], "path/to/file.html")
        self.assertTrue(record["is_new"])

        mock_extract.assert_called_once_with("https://example.com/doc")
        mock_save.assert_called_once_with("DSCD-15-PL-12", "<html>extracted</html>")
        mock_update_state.assert_called_once()


if __name__ == "__main__":
    unittest.main()
