"""BCRP Weekly Report PDF scraper for Peru GDP RTD pipeline.

This module provides functions to download Weekly Reports (WR) from the
Central Reserve Bank of Peru (BCRP) website using Selenium-based web scraping.

The scraper:
- Uses browser automation to navigate the BCRP website
- Downloads PDFs with retry logic and error handling
- Tracks downloaded files to avoid duplicates
- Respects rate limits to mimic human behavior
- Provides audio alerts for long-running downloads
"""

import os
import re
import time
from pathlib import Path
from typing import Optional, Set, List, Tuple

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import StaleElementReferenceException

# Browser-specific imports
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.chrome.service import Service as ChromeService
from webdriver_manager.chrome import ChromeDriverManager

from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service as FirefoxService
from webdriver_manager.firefox import GeckoDriverManager

from selenium.webdriver.edge.options import Options as EdgeOptions
from selenium.webdriver.edge.service import Service as EdgeService
from webdriver_manager.microsoft import EdgeChromiumDriverManager

# Local imports
from peru_gdp_rtd.scrapers.utils import get_http_session, random_wait
from peru_gdp_rtd.utils.alerts import (
    init_audio,
    load_alert_track,
    play_alert_track,
    stop_alert_track,
)
from peru_gdp_rtd.config import Settings


def init_driver(browser: str = "chrome", headless: bool = False, page_load_timeout: int = 30):
    """Initialize and return a Selenium WebDriver instance.

    Args:
        browser: Engine to use. Supported: 'chrome' (default), 'firefox', 'edge', 'safari'
        headless: Run the browser in headless mode if True (no GUI)
        page_load_timeout: Maximum seconds to wait for page loads

    Returns:
        Configured WebDriver instance

    Raises:
        ValueError: If browser type is not supported

    Example:
        >>> driver = init_driver(browser="chrome", headless=True)
        >>> driver.get("https://www.bcrp.gob.pe")
        >>> driver.quit()
    """
    b = browser.lower()

    if b == "chrome":
        options = ChromeOptions()
        if headless:
            options.add_argument("--headless=new")  # Modern headless mode
        options.add_argument("--no-sandbox")  # Stability in containerized envs
        options.add_argument("--disable-dev-shm-usage")  # Avoid /dev/shm issues
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)

    elif b == "firefox":
        fopts = FirefoxOptions()
        if headless:
            fopts.add_argument("-headless")  # Firefox headless flag
        service = FirefoxService(GeckoDriverManager().install())
        driver = webdriver.Firefox(service=service, options=fopts)

    elif b == "edge":
        eopts = EdgeOptions()
        if headless:
            eopts.add_argument("--headless=new")
        eopts.add_argument("--no-sandbox")
        eopts.add_argument("--disable-dev-shm-usage")
        service = EdgeService(EdgeChromiumDriverManager().install())
        driver = webdriver.Edge(service=service, options=eopts)

    elif b == "safari":
        if headless:
            print("⚠️  Headless mode is not supported for Safari. Running in normal mode.")
        driver = webdriver.Safari()  # Safari driver bundled with macOS

    else:
        raise ValueError(
            f"Unsupported browser: '{browser}'. "
            "Supported browsers are: 'chrome', 'firefox', 'edge', 'safari'."
        )

    driver.set_page_load_timeout(page_load_timeout)
    return driver


def download_pdf(
    driver,
    pdf_link,
    wait: WebDriverWait,
    download_counter: int,
    raw_pdf_folder: str,
    download_record_folder: str,
    download_record_txt: str,
    chunk_size: int = 128,
    timeout: int = 60,
) -> bool:
    """Download a single PDF and update the chronological record.

    Args:
        driver: Active Selenium WebDriver instance
        pdf_link: Anchor element pointing to the PDF
        wait: Explicit wait helper bound to the driver
        download_counter: Ordinal used in progress messages
        raw_pdf_folder: Destination directory for the downloaded PDF
        download_record_folder: Folder containing the record text file
        download_record_txt: Record filename (e.g., 'downloaded_pdfs.txt')
        chunk_size: Bytes per chunk when streaming downloads
        timeout: Seconds for connect + read timeouts

    Returns:
        True if the file was successfully downloaded and recorded; False otherwise

    Example:
        >>> driver = init_driver()
        >>> wait = WebDriverWait(driver, 60)
        >>> # ... get pdf_link element ...
        >>> success = download_pdf(driver, pdf_link, wait, 1, "raw/", "record/", "downloads.txt")
    """
    # Click via JS (handles covered/overlayed links)
    driver.execute_script("arguments[0].click();", pdf_link)

    # Wait for a new tab to open (2 windows in total)
    wait.until(EC.number_of_windows_to_be(2))
    windows = driver.window_handles
    driver.switch_to.window(windows[1])  # Focus the new tab

    # Get final PDF URL after any redirects
    new_url = driver.current_url
    file_name = os.path.basename(new_url)  # Use server-provided filename
    destination_path = os.path.join(raw_pdf_folder, file_name)

    # Download with retry logic
    session = get_http_session()
    try:
        # Stream to avoid loading large files in RAM
        response = session.get(new_url, stream=True, timeout=timeout)

        if response.status_code == 200:
            os.makedirs(raw_pdf_folder, exist_ok=True)
            with open(destination_path, "wb") as fh:
                for chunk in response.iter_content(chunk_size=chunk_size):
                    if chunk:  # Skip keep-alive chunks
                        fh.write(chunk)
        else:
            print(
                f"{download_counter}. ❌ Error downloading {file_name}. "
                f"HTTP {response.status_code}"
            )
            driver.close()
            driver.switch_to.window(windows[0])
            return False

    except Exception as ex:
        print(f"{download_counter}. ❌ Network error downloading {file_name}: {ex}")
        driver.close()
        driver.switch_to.window(windows[0])
        return False

    # Update the record log in chronological order (year -> issue)
    record_path = os.path.join(download_record_folder, download_record_txt)
    records: List[str] = []

    if os.path.exists(record_path):
        with open(record_path, "r", encoding="utf-8") as f:
            records = [ln.strip() for ln in f if ln.strip()]

    if file_name not in records:
        records.append(file_name)

    def _ns_key(s: str) -> Tuple[int, int, str]:
        """Sort key for Weekly Report filenames (ns-XX-YYYY.pdf)."""
        base = os.path.splitext(os.path.basename(s))[0]
        m = re.search(r"ns-(\d{2})-(\d{4})", base, re.I)
        if not m:
            return (9999, 9999, base)  # Unknown pattern -> sort last
        issue, year = int(m.group(1)), int(m.group(2))
        return (year, issue, base)

    records.sort(key=_ns_key)  # Chronological order

    os.makedirs(download_record_folder, exist_ok=True)
    with open(record_path, "w", encoding="utf-8") as f:
        f.write("\n".join(records) + ("\n" if records else ""))

    print(f"{download_counter}. ✔️ Downloaded: {file_name}")

    # Close child tab and go back to main
    driver.close()
    driver.switch_to.window(windows[0])

    return True


def pdf_downloader(
    settings: Settings,
    max_downloads: Optional[int] = None,
    downloads_per_batch: Optional[int] = None,
    headless: Optional[bool] = None,
) -> None:
    """Download BCRP Weekly Reports by crawling the monthly listing page.

    This function:
    1. Opens the WR listing page and locates one anchor per month (the first/"latest")
    2. Reverses the order to download from oldest -> newest for stable local numbering
    3. Skips any file already present in the record file (no re-download)
    4. Streams each PDF to disk and appends its filename to the record (chronological)
    5. Optionally pauses after each batch with a short alert track and user prompt
    6. Prints a final summary (total links, skipped, new, elapsed time)

    Assumptions:
    - The site structures monthly WR links under the CSS selector from settings
    - Within each month block, the first <a> is the latest WR of that month
    - The record file contains one filename per line (e.g., ns-07-2019.pdf)

    Args:
        settings: Configuration settings object
        max_downloads: Upper bound on new downloads; None means no cap (overrides config)
        downloads_per_batch: Number of files between pause prompts (overrides config)
        headless: If True, runs the browser in headless mode (overrides config)

    Example:
        >>> from peru_gdp_rtd.config import get_settings
        >>> settings = get_settings()
        >>> pdf_downloader(settings, max_downloads=10, headless=True)
    """
    start_time = time.time()

    # Use provided arguments or fall back to config
    if max_downloads is None:
        max_downloads = settings.scraper.max_downloads
    if downloads_per_batch is None:
        downloads_per_batch = settings.scraper.downloads_per_batch
    if headless is None:
        headless = settings.scraper.headless

    # Get configuration values
    bcrp_url = settings.scraper.bcrp_url
    browser = settings.scraper.browser
    css_selector = settings.scraper.css_selectors["report_list"]

    raw_pdf_folder = str(settings.paths.pdf_raw)
    record_folder = str(settings.paths.record)
    alert_folder = str(settings.paths.alert_track)
    record_txt = settings.record_files["downloaded_pdfs"]

    min_wait = settings.scraper.min_wait
    max_wait = settings.scraper.max_wait

    page_timeout = settings.scraper.selenium["page_load_timeout"]
    explicit_timeout = settings.scraper.selenium["explicit_wait_timeout"]

    chunk_size = settings.scraper.http["chunk_size"]
    http_timeout = settings.scraper.http["timeout"]

    enable_alerts = settings.features["enable_alerts"]

    print("\n📥 Starting PDF downloader for BCRP WR...\n")

    # Initialize audio if enabled
    _last_alert = None
    if enable_alerts:
        init_audio()
        alert_track_path = load_alert_track(alert_folder, _last_alert)
        _last_alert = alert_track_path

    # Load existing download record
    record_path = os.path.join(record_folder, record_txt)
    downloaded_files: Set[str] = set()

    if os.path.exists(record_path):
        with open(record_path, "r", encoding="utf-8") as f:
            downloaded_files = set(f.read().splitlines())

    # Initialize browser
    driver = init_driver(browser=browser, headless=headless, page_load_timeout=page_timeout)
    wait = WebDriverWait(driver, explicit_timeout)

    new_counter = 0  # Count new files successfully downloaded
    skipped_files: List[str] = []  # Filenames skipped due to record matches
    new_downloads = []  # Queue of (selenium_element, filename)
    pdf_links = []  # Full set of month-leading anchors for summary

    try:
        driver.get(bcrp_url)
        print("🌐 BCRP site opened successfully.")

        # Wait for all month containers to appear
        month_ul_elems = wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, css_selector))
        )
        print(f"🔎 Found {len(month_ul_elems)} WR blocks on page (one per month).\n")

        # Select exactly one link per month (business rule: the first anchor inside the block)
        for ul in month_ul_elems:
            try:
                anchors = ul.find_elements(By.TAG_NAME, "a")
            except Exception:
                anchors = []  # Conservative fallback if DOM changes mid-run

            if not anchors:
                continue

            pdf_links.append(anchors[0])  # Keep only the first anchor (latest monthly WR)

        pdf_links = pdf_links[::-1]  # Oldest -> newest for stable local ordering

        # Build a work queue, skipping any file already recorded
        for link in pdf_links:
            try:
                file_url = link.get_attribute("href")
                file_name = os.path.basename(file_url)
            except Exception:
                continue  # Defensive skip if attributes are momentarily unavailable

            if file_name in downloaded_files:
                skipped_files.append(file_name)
            else:
                new_downloads.append((link, file_name))

        # Download queue (chronological), with optional batch pauses and pacing
        for i, (link, file_name) in enumerate(new_downloads, start=1):
            # Load a new random alert for each batch start
            if enable_alerts and i % downloads_per_batch == 1:
                alert_track_path = load_alert_track(alert_folder, _last_alert)
                _last_alert = alert_track_path

            ok = download_pdf(
                driver=driver,
                pdf_link=link,
                wait=wait,
                download_counter=i,
                raw_pdf_folder=raw_pdf_folder,
                download_record_folder=record_folder,
                download_record_txt=record_txt,
                chunk_size=chunk_size,
                timeout=http_timeout,
            )

            if ok:
                downloaded_files.add(file_name)
                new_counter += 1

            # Optional checkpoint every N downloads
            if enable_alerts and (i % downloads_per_batch == 0) and alert_track_path:
                play_alert_track()
                user_input = input("⏸️ Continue? (y = yes, any other key = stop): ")
                stop_alert_track()

                if user_input.lower() != "y":
                    print("🛑 Download stopped by user.")
                    break

            # Respect a global cap if provided
            if max_downloads and new_counter >= max_downloads:
                print(f"🏁 Download limit of {max_downloads} new PDFs reached.")
                break

            # Gentle pacing to mimic a human user
            random_wait(min_wait, max_wait)

    except StaleElementReferenceException:
        print("⚠️ StaleElementReferenceException encountered. Consider re-running.")
    finally:
        driver.quit()
        print("\n👋 Browser closed.")

    # Maintain the record file in chronological order (idempotent)
    try:
        if os.path.exists(record_path):
            with open(record_path, "r", encoding="utf-8") as f:
                records = [ln.strip() for ln in f if ln.strip()]

            def _ns_key(s: str) -> Tuple[int, int, str]:
                base = os.path.splitext(os.path.basename(s))[0]
                m = re.search(r"ns-(\d{2})-(\d{4})", base, re.I)
                if not m:
                    return (9999, 9999, base)
                issue, year = int(m.group(1)), int(m.group(2))
                return (year, issue, base)

            records = sorted(set(records), key=_ns_key)

            os.makedirs(record_folder, exist_ok=True)
            with open(record_path, "w", encoding="utf-8") as f:
                f.write("\n".join(records) + ("\n" if records else ""))

    except Exception as e:
        print(f"⚠️ Unable to re-sort record file: {e}")

    # Final summary for the session
    elapsed_time = round(time.time() - start_time)
    total_links = len(pdf_links)

    print("\n📊 Summary:")
    print(f"\n🔗 Total monthly links kept: {total_links}")

    if skipped_files:
        print(f"🗂️ {len(skipped_files)} already downloaded PDFs were skipped.")

    print(f"➕ Newly downloaded: {new_counter}")
    print(f"⏱️ {elapsed_time} seconds")
