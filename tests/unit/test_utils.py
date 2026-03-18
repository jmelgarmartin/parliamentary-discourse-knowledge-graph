"""
Tests unitarios de las utilidades.
"""
from congreso_analisis.utils.hashing import text_to_hash
from congreso_analisis.utils.logging_utils import setup_logger
from congreso_analisis.utils.time_utils import get_current_partition_date


def test_hashing() -> None:
    """Prueba hashing stub."""
    assert text_to_hash("test") == ""


def test_time_utils() -> None:
    """Prueba time_utils stub."""
    assert get_current_partition_date() == ""


def test_logging_utils() -> None:
    """Prueba logging_utils stub."""
    assert setup_logger("test") is None


def test_is_last_page_from_text() -> None:
    """Pruebas para comprobar la lógica is_last_page_from_text."""
    from congreso_analisis.utils.selenium_utils import is_last_page_from_text

    assert is_last_page_from_text("Resultados 21 a 40 de 113") is False
    assert is_last_page_from_text("Resultados 101 a 113 de 113") is True
    assert is_last_page_from_text("Resultados 113 a 113 de 113") is True
    assert is_last_page_from_text("No match") is False
    assert is_last_page_from_text(None) is False


def test_parse_pagination_text() -> None:
    """Prueba el ayudante privado de parseo de texto de paginación."""
    from congreso_analisis.utils.selenium_utils import _parse_pagination_text

    f, t, z = _parse_pagination_text("Resultados 1 a 20 de 100")
    assert f == 1
    assert t == 20
    assert z == 100

    f, t, z = _parse_pagination_text("Resultados 81 a 100 de 100")
    assert f == 81
    assert t == 100
    assert z == 100

    f, t, z = _parse_pagination_text("Algo distinto")
    assert f is None
    assert t is None
    assert z is None


def test_paginate_table_no_progress() -> None:
    """
    Prueba que paginate_table rompe el bucle si detecta que la página no avanza
    usando la firma de la página.
    """
    from unittest.mock import MagicMock

    from congreso_analisis.utils.selenium_utils import paginate_table

    mock_driver = MagicMock()
    mock_wait = MagicMock()

    # Mock elements return the same signatures over and over
    mock_row = MagicMock()
    mock_row.text = "Fila idéntica constante"

    mock_footer = MagicMock()
    mock_footer.text = "Resultados 1 a 20 de 100"

    # Mocking wait.until so it throws a TimeoutException to simulate "did not advance"
    from selenium.common.exceptions import TimeoutException

    # Calls:
    # 1. wait_for_spinner (322)
    # 2. wait.until (323)
    # 3. wait_for_spinner (372)
    # 4. wait.until (385) -> Timeout
    mock_wait.until.side_effect = [True, True, True, TimeoutException("did not advance either")]

    def find_elements_side_effect(by: str, selector: str = "") -> list[MagicMock]:
        return [mock_row]

    def find_element_side_effect(by: str, identifier: str) -> MagicMock:
        if identifier == "test_footer":
            return mock_footer
        if identifier == "//a_next":
            next_btn = MagicMock()
            next_btn.is_displayed.return_value = True
            next_btn.get_attribute.return_value = ""
            return next_btn
        return MagicMock()

    mock_driver.find_elements.side_effect = find_elements_side_effect
    mock_driver.find_element.side_effect = find_element_side_effect

    generator = paginate_table(
        driver=mock_driver,
        wait=mock_wait,
        row_selector="table tr",
        paginator_id="test_footer",
        next_btn_xpath="//a_next",
        max_pages=10,
    )

    # El generador debe retornar solo 1 página porque el wait falló en la firma de cambio
    pages = list(generator)
    assert len(pages) == 1
    assert len(pages[0]) == 1
    assert pages[0][0].text == "Fila idéntica constante"


def test_click_siguiente_pagina_no_button() -> None:
    """Prueba que click_siguiente_pagina devuelve False si no encuentra el botón."""
    from unittest.mock import MagicMock

    from congreso_analisis.utils.selenium_utils import click_siguiente_pagina

    mock_driver = MagicMock()
    mock_wait = MagicMock()

    # simulate find_element raising NoSuchElementException
    from selenium.common.exceptions import NoSuchElementException

    mock_driver.find_element.side_effect = NoSuchElementException()

    result = click_siguiente_pagina(
        driver=mock_driver, wait=mock_wait, next_xpath="//any_xpath", table_by="css selector", table_selector="tr"
    )
    assert result is False


def test_click_siguiente_pagina_disabled() -> None:
    """Prueba que click_siguiente_pagina devuelve False si el botón está deshabilitado."""
    from unittest.mock import MagicMock

    from congreso_analisis.utils.selenium_utils import click_siguiente_pagina

    mock_driver = MagicMock()
    mock_wait = MagicMock()

    mock_btn = MagicMock()
    mock_btn.get_attribute.return_value = "btn disabled"
    mock_btn.is_displayed.return_value = True

    # We need to make sure find_element returns the button
    mock_driver.find_element.return_value = mock_btn

    result = click_siguiente_pagina(
        driver=mock_driver, wait=mock_wait, next_xpath="//any_xpath", table_by="css selector", table_selector="tr"
    )
    assert result is False


def test_click_siguiente_pagina_no_progress() -> None:
    """Prueba que click_siguiente_pagina devuelve False si la firma no cambia tras el click."""
    from unittest.mock import MagicMock

    from congreso_analisis.utils.selenium_utils import click_siguiente_pagina

    mock_driver = MagicMock()
    mock_wait = MagicMock()

    mock_btn = MagicMock()
    mock_btn.get_attribute.return_value = ""
    mock_btn.is_displayed.return_value = True
    mock_driver.find_element.return_value = mock_btn

    # Signature remains the same (footer text doesn't change)
    mock_footer = MagicMock()
    mock_footer.text = "Page 1"

    # Overriding side_effect for find_element to return different things
    def find_element_side(by: str, val: str) -> MagicMock:
        if val == "paginator":
            return mock_footer
        return mock_btn

    mock_driver.find_element.side_effect = find_element_side

    # simulate wait.until timeout
    from selenium.common.exceptions import TimeoutException

    mock_wait.until.side_effect = TimeoutException()

    result = click_siguiente_pagina(
        driver=mock_driver,
        wait=mock_wait,
        next_xpath="//any_xpath",
        table_by="css selector",
        table_selector="tr",
        paginator_id="paginator",
    )
    assert result is False


def test_click_siguiente_pagina_success() -> None:
    """Prueba que click_siguiente_pagina devuelve True si avanza correctamente."""
    from unittest.mock import MagicMock

    from congreso_analisis.utils.selenium_utils import click_siguiente_pagina

    mock_driver = MagicMock()
    mock_wait = MagicMock()

    mock_btn = MagicMock()
    mock_btn.get_attribute.return_value = ""
    mock_btn.is_displayed.return_value = True

    mock_footer = MagicMock()
    mock_footer.text = "Page 1"

    def find_element_side(by: str, val: str) -> MagicMock:
        if val == "paginator":
            return mock_footer
        return mock_btn

    mock_driver.find_element.side_effect = find_element_side
    mock_wait.until.return_value = True  # Signature change wait succeeds

    result = click_siguiente_pagina(
        driver=mock_driver,
        wait=mock_wait,
        next_xpath="//any_xpath",
        table_by="css selector",
        table_selector="tr",
        paginator_id="paginator",
    )
    assert result is True
