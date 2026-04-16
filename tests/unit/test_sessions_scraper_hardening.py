import unittest

from congress_analysis.ingestion.scrappers.sessions_scraper import SessionsScraper


class TestSessionsScraperHardening(unittest.TestCase):
    def setUp(self):
        # Initialize scraper with minimal dependencies for unit testing logic
        self.scraper = SessionsScraper(term="15", headless=True)

    def test_build_canonical_publication_url_adds_codi(self):
        raw_url = "https://www.congreso.es/busqueda-de-publicaciones?_publicaciones_id_texto=DSCD-15-PL-176"
        expected = "https://www.congreso.es/busqueda-de-publicaciones?_publicaciones_id_texto=DSCD-15-PL-176.CODI.&_publicaciones_legislatura=XV"
        canonical = self.scraper._build_canonical_publication_url(raw_url)
        self.assertIn("DSCD-15-PL-176.CODI.", canonical)
        self.assertIn("_publicaciones_legislatura=XV", canonical)

    def test_build_canonical_publication_url_preserves_codi(self):
        raw_url = "https://www.congreso.es/busqueda-de-publicaciones?_publicaciones_id_texto=DSCD-15-PL-176.CODI.&_publicaciones_legislatura=XV"
        canonical = self.scraper._build_canonical_publication_url(raw_url)
        self.assertEqual(raw_url, canonical)

    def test_build_canonical_publication_url_corrects_legislature(self):
        raw_url = "https://www.congreso.es/busqueda-de-publicaciones?_publicaciones_id_texto=DSCD-15-PL-176.CODI.&_publicaciones_legislatura=15"
        canonical = self.scraper._build_canonical_publication_url(raw_url)
        self.assertIn("_publicaciones_legislatura=XV", canonical)
        self.assertNotIn("_publicaciones_legislatura=15", canonical)

    def test_is_valid_plenary_html_detects_de_placeholder(self):
        html = '<section id="portlet_publicaciones">de</section>'
        is_valid, reason = self.scraper._is_valid_plenary_html(html)
        self.assertFalse(is_valid)
        self.assertEqual(reason, "Detected 'de' placeholder (incomplete page)")

    def test_is_valid_plenary_html_detects_short_content(self):
        html = '<section id="portlet_publicaciones">Short content but no placeholder</section>'
        is_valid, reason = self.scraper._is_valid_plenary_html(html)
        self.assertFalse(is_valid)
        self.assertIn("suspiciously short", reason)

    def test_is_valid_plenary_html_detects_missing_header(self):
        # Long enough but missing mandatory header
        html = '<section id="portlet_publicaciones">' + "A" * 40000 + "</section>"
        is_valid, reason = self.scraper._is_valid_plenary_html(html)
        self.assertFalse(is_valid)
        self.assertEqual(reason, "Missing 'DIARIO DE SESIONES' header")

    def test_is_valid_plenary_html_accepts_valid_content(self):
        html = (
            '<section id="portlet_publicaciones">DIARIO DE SESIONES DE LAS CORTES GENERALES '
            + "Content " * 5000
            + "</section>"
        )
        is_valid, reason = self.scraper._is_valid_plenary_html(html)
        self.assertTrue(is_valid)


if __name__ == "__main__":
    unittest.main()
