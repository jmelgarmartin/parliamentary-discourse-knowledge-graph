"""
Selenium utilities for web scraping operations.
"""

import logging
import re
import time
from typing import Any, Generator, Optional, Tuple

from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait
from webdriver_manager.chrome import ChromeDriverManager

logger = logging.getLogger(__name__)


def init_driver(
    driver_path: Optional[str] = None, headless: bool = False
) -> Tuple[webdriver.Chrome, WebDriverWait[webdriver.Chrome]]:
    """
    Initializes a Chrome driver with WebDriverWait.

    :param driver_path: Path to the ChromeDriver executable.
        If None, uses DriverManager.
    :param headless: If True, runs Chrome in headless mode.
    :return: A tuple containing the Chrome driver and a WebDriverWait instance.
    """
    options = Options()
    if headless:
        options.add_argument("--headless=new")
    options.add_argument("--start-maximized")

    if driver_path:
        service = Service(driver_path)
    else:
        service = Service(ChromeDriverManager().install())

    driver = webdriver.Chrome(service=service, options=options)
    wait = WebDriverWait(driver, 20)
    return driver, wait


def accept_cookies(driver: webdriver.Chrome, wait: WebDriverWait[webdriver.Chrome]) -> None:
    """
    Accepts the cookies banner if present on the page.

    :param driver: Chrome browser instance.
    :param wait: WebDriverWait instance.
    """
    try:
        wait.until(EC.element_to_be_clickable((By.XPATH, "//a[normalize-space(text())='Aceptar todas']"))).click()
        logger.info("Cookies accepted.")
    except Exception as e:
        logger.error(f"Could not accept cookies: {e}")


def select_option_by_value(select_element: Any, value: str) -> bool:
    """
    Selects a specific option in an HTML <select> element given a value.

    :param select_element: WebElement corresponding to the select.
    :param value: Value of the 'value' attribute of the option to select.
    :return: True if the option was selected successfully, False otherwise.
    """
    try:
        Select(select_element).select_by_value(value)
        return True
    except Exception as e:
        logger.error(f"Error selecting value '{value}': {e}")
        return False


def wait_for_spinner(wait: WebDriverWait[webdriver.Chrome]) -> None:
    """
    Waits until the loading spinner disappears from the screen.

    :param wait: WebDriverWait instance.
    """
    wait.until(EC.invisibility_of_element_located((By.CLASS_NAME, "spinner-border")))


def wait_for_table_load(wait: WebDriverWait[webdriver.Chrome], selector: str) -> None:
    """
    Waits until the table specified by the selector is loaded on the page.

    :param wait: WebDriverWait instance.
    :param selector: CSS Selector for the table.
    """
    wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))


def click_with_wait(
    driver: webdriver.Chrome,
    wait: WebDriverWait[webdriver.Chrome],
    by: str,
    selector: str,
) -> None:
    """
    Waits for an element to be clickable and then clicks it using JavaScript.

    :param driver: Chrome browser instance.
    :param wait: WebDriverWait instance.
    :param by: Method to locate elements (By.XPATH, By.CSS_SELECTOR, etc.).
    :param selector: Selector to locate the element.
    """
    element = wait.until(EC.element_to_be_clickable((by, selector)))
    driver.execute_script("arguments[0].click();", element)


def is_last_page(driver: webdriver.Chrome, element_id: str) -> bool:
    """
    Determines if the current page is the last one according to the paginator text.

    :param driver: Chrome browser instance.
    :param element_id: ID of the element containing the paginator information.
    :return: True if it is the last page, False otherwise.
    """
    try:
        text = driver.find_element(By.ID, element_id).text
        match = re.search(r"Resultados (\d+) a (\d+) de (\d+)", text)
        if match:
            to_val = int(match.group(2))
            total_val = int(match.group(3))
            return to_val >= total_val
    except Exception:
        pass
    return False


def click_siguiente_pagina(
    driver: webdriver.Chrome,
    wait: WebDriverWait[webdriver.Chrome],
    next_xpath: str,
    table_by: str,
    table_selector: str,
    paginator_id: Optional[str] = None,
) -> bool:
    """
    Attempts to click the next page button and waits for the content to update.
    Uses a page signature to detect real progress.

    :param driver: Selenium browser driver.
    :param wait: WebDriverWait object.
    :param next_xpath: XPath of the pagination button.
    :param table_by: Method to locate table elements (By.XPATH, By.CSS_SELECTOR, etc.).
    :param table_selector: Selector for the results table rows.
    :param paginator_id: ID of the element showing the results range.
    :return: True if it advances the page, False if there are no more pages or it fails to advance.
    """
    try:
        # Capture current page signature
        footer_text_before = ""
        if paginator_id:
            try:
                footer_text_before = driver.find_element(By.ID, paginator_id).text.strip()
            except Exception:
                pass

        first_row_text_before = ""
        try:
            rows_before = driver.find_elements(table_by, table_selector)
            if rows_before:
                first_row_text_before = rows_before[0].text.strip()
        except Exception:
            pass

        page_signature_before = f"{footer_text_before}|{first_row_text_before}"

        # Find next button
        next_btn = None
        try:
            next_btn = driver.find_element(By.XPATH, next_xpath)
        except Exception:
            # Fallback search within the same container if possible
            # We assume next_xpath usually points to something like //ul[...]//a[...]
            # Let's try to extract the base container from xpath if simple enough, or just log info
            logger.info(f"Next button not found with xpath: {next_xpath}. Assuming last page.")
            return False

        # Check if disabled
        class_attr = (next_btn.get_attribute("class") or "").lower()
        aria_disabled = next_btn.get_attribute("aria-disabled") == "true"
        is_visible = next_btn.is_displayed()

        if "disabled" in class_attr or aria_disabled or not is_visible:
            logger.info("Next button is disabled or invisible. Reached last page.")
            return False

        # Click Next
        try:
            # Hide common blockers (cookies)
            driver.execute_script(
                "let banner = document.getElementById('pop-up-cookies'); if(banner) banner.style.display='none';"
            )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_btn)
            time.sleep(0.5)
            driver.execute_script("arguments[0].click();", next_btn)
        except Exception as e:
            logger.warning(f"Failed to click next button: {e}")
            return False

        wait_for_spinner(wait)

        # Wait for signature change
        def signature_changed(d: Any) -> bool:
            try:
                foot_t = ""
                if paginator_id:
                    foot_t = d.find_element(By.ID, paginator_id).text.strip()

                rows = d.find_elements(table_by, table_selector)
                row_t = rows[0].text.strip() if rows else ""

                new_sig = f"{foot_t}|{row_t}"
                return new_sig != page_signature_before
            except Exception:
                return False

        try:
            wait.until(signature_changed)
            return True
        except Exception:
            logger.warning("Pagination click did not result in a page advancement (signature remained identical).")
            return False

    except Exception as e:
        logger.error(f"Unexpected error in click_next_page: {e}")
        return False


def get_results_range(driver: webdriver.Chrome, element_id: str) -> Tuple[Optional[int], Optional[int]]:
    """
    Gets the current results range and the total from the paginator text.

    :param driver: Chrome browser instance.
    :param element_id: ID of the element containing the paginator text.
    :return: Tuple with (to, total) results. (None, None) if it fails.
    """
    try:
        text = driver.find_element(By.ID, element_id).text
        match = re.search(r"Resultados (\d+) a (\d+) de (\d+)", text)
        if match:
            return int(match.group(2)), int(match.group(3))
    except Exception:
        pass
    return None, None


def save_html_content(
    driver: webdriver.Chrome,
    wait: WebDriverWait[webdriver.Chrome],
    selector: str,
    file_path: str,
) -> bool:
    """
    Saves the HTML content of a specific selector to a file.

    :param driver: Chrome browser instance.
    :param wait: WebDriverWait instance.
    :param selector: CSS Selector of the element whose HTML content will be saved.
    :param file_path: Full file path where the content will be saved.
    :return: True if the content was saved successfully, False if not found.
    """
    try:
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, selector)))
        soup = BeautifulSoup(driver.page_source, "html.parser")
        content = soup.select_one(selector)
        if content:
            with open(file_path, "w", encoding="utf-8") as f:
                f.write(str(content))
            return True
    except Exception as e:
        logger.error(f"Error saving the HTML content: {e}")
    return False


def is_last_page_from_text(text: str) -> bool:
    """
    Checks if the given paginator text indicates the last page.
    E.g. "Resultados 11 a 15 de 15" -> True

    :param text: Text containing the pagination info.
    :return: True if Y == Z in "Resultados X a Y de Z", False otherwise.
    """
    if not text:
        return False
    match = re.search(r"Resultados \d+ a (\d+) de (\d+)", text)
    if match:
        to_val = int(match.group(1))
        total_val = int(match.group(2))
        return to_val >= total_val
    return False


def paginate_table(
    driver: webdriver.Chrome,
    wait: WebDriverWait[webdriver.Chrome],
    row_selector: str,
    paginator_id: str,
    next_btn_xpath: str,
    max_pages: int = 500,
) -> Generator[list[Any], None, None]:
    """
    Robustly paginates a table, yielding elements for each page.
    Uses a page signature to avoid infinite loops.

    :param driver: Chrome browser instance.
    :param wait: WebDriverWait instance.
    :param row_selector: CSS selector for the rows to extract.
    :param paginator_id: HTML ID of the paginator text element (Resultados X a Y de Z).
    :param next_btn_xpath: XPath for the 'Next' button.
    :param max_pages: Maximum number of pages to process.
    :yields: A list of WebElement rows for the current page.
    """
    for page_num in range(1, max_pages + 1):
        wait_for_spinner(wait)
        wait.until(EC.presence_of_element_located((By.CSS_SELECTOR, row_selector)))

        # Capture current page elements
        current_rows = driver.find_elements(By.CSS_SELECTOR, row_selector)

        # Read paginator text if available
        footer_text = ""
        try:
            footer_text = driver.find_element(By.ID, paginator_id).text.strip()
        except Exception:
            pass

        # Create page signature
        first_row_text = current_rows[0].text.strip() if current_rows else ""
        page_signature = f"{footer_text}|{first_row_text}"

        logger.info(f"Page {page_num} Signature: {page_signature} | Rows extracted this page: {len(current_rows)}")

        # Yield the elements for the caller to parse immediately (before they go stale)
        yield current_rows

        # End Criterion 1: Footer says Y == Z
        if is_last_page_from_text(footer_text):
            logger.info("Last page detected via footer Y == Z.")
            break

        # Try finding next button
        try:
            next_btn = driver.find_element(By.XPATH, next_btn_xpath)
            is_disabled = (
                "disabled" in (next_btn.get_attribute("class") or "")
                or next_btn.get_attribute("aria-disabled") == "true"
            )
            if not next_btn.is_displayed() or is_disabled:
                logger.info("Last page detected via next button state (disabled/invisible).")
                break
        except Exception:
            logger.info("Last page detected because next button was not found.")
            break

        # Click Next
        try:
            # Fix ElementClickInterceptedException by forcing JS click and hiding the cookie banner
            driver.execute_script(
                "let banner = document.getElementById('pop-up-cookies'); if(banner) banner.style.display='none';"
            )
            driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", next_btn)
            time.sleep(1)
            driver.execute_script("arguments[0].click();", next_btn)
        except Exception as e:
            logger.warning(f"Failed to click next button: {e}")
            break

        wait_for_spinner(wait)

        # Wait for signature to change to ensure we advanced
        def signature_changed(d: Any) -> bool:
            try:
                new_f_text = d.find_element(By.ID, paginator_id).text.strip()
                new_r_text = d.find_elements(By.CSS_SELECTOR, row_selector)[0].text.strip()
                new_sig = f"{new_f_text}|{new_r_text}"
                return new_sig != page_signature
            except Exception:
                return False

        try:
            wait.until(signature_changed)
        except Exception:
            # Did not advance
            new_footer = ""
            try:
                new_footer = driver.find_element(By.ID, paginator_id).text.strip()
            except Exception:
                pass
            new_first_row = ""
            try:
                new_first_row = driver.find_elements(By.CSS_SELECTOR, row_selector)[0].text.strip()
            except Exception:
                pass
            new_sig = f"{new_footer}|{new_first_row}"

            logger.warning(
                f"Pagination did not advance!\n"
                f"  Previous Signature: {page_signature}\n"
                f"  Current Signature:  {new_sig}\n"
                f"  Next Button state:  Found/Clicked previously, but DOM didn't update.\n"
                f"Breaking loop to prevent infinite retry."
            )
            break

    else:
        # Hit max_pages
        logger.error(f"HARD LIMIT REACHED! Stopped pagination after hitting max_pages={max_pages}.")
