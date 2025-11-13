
# *********************************************************************************************
#  Pipelines for old_gdp_rtd.ipynb and new_gdp_rtd.ipynb
# *********************************************************************************************
#
#   Program       : gdp_rtd_pipeline.py
#   Project       : Building Real-Time Dataset (RTD) for GDP Revisions
#   Author        : Jason Cruz
#   Last updated  : 11/13/2025
#   Python        : 3.12
#
#   Overview: Helper functions used (together as a module) by the new_gdp_rtd.ipynb workflow.
#
#   Sections:
#       1. Downloading PDFs ...................................................................
#       2. Generating input PDFs with key tables ..............................................
#       3. Cleaning tables and building RTD ...................................................
#           3.1 Creating functions for extracting, parsing and cleaning-up input tables .......
#           3.2 Building friendly pipelines for running cleaners and RTD transformers .........
#       4. Concatenating RTD across years by frequency ........................................
#       5. Metadata ...........................................................................
#           5.1 Generating adjusted RTDs by removing revisions affected by base years (based on
#           metadata) .........................................................................
#           5.2 Generating benchmark RTD for revisions affected by benchmarking procedures
#           (based on metadata) ...............................................................
#       6. Converting RTD to releases dataset .................................................
# 
#   Notes:
#       "NS/ns" (Nota Semanal, Spanish) is equivalent to "WR/wr" (Weekly Report).
#
# *********************************************************************************************



# ##############################################################################################
# SECTION 1 Downloading PDFs
# ##############################################################################################

# In this section we build an automated downloader for BCRP's Weekly Reports (WR) using
# Selenium-based web scraping to mimic a human browser session and avoid duplicate downloads.


# ++++++++++++++++++++++++++++++++++++++++++++++++
# Libraries
# ++++++++++++++++++++++++++++++++++++++++++++++++

import os                                                                   # Path utilities & directory management (paths, env, makedirs)
import re                                                                   # Filename parsing & robust pattern matching (e.g., NS codes)
import time                                                                 # Execution timing, sleeps for rate limiting/backoff
import random                                                               # Jittered waits to mimic human behavior and reduce rate spikes
import shutil                                                               # High-level file ops: move/copy/rename/delete

import requests                                                             # HTTP client for GET/HEAD with sessions and streaming downloads
from requests.adapters import HTTPAdapter                                   # Mount retry-enabled adapters on requests.Session
from urllib3.util.retry import Retry                                        # Exponential backoff strategy (status_forcelist, total, factor)

import pygame                                                               # Lightweight audio playback for desktop alerts (mp3/wav)

from selenium import webdriver                                              # WebDriver controllers (Chrome/Edge/Firefox)
from selenium.webdriver.common.by import By                                 # DOM locator strategies (id/xpath/css/name)
from selenium.webdriver.support.ui import WebDriverWait                     # Explicit waits for async/JS-heavy pages
from selenium.webdriver.support import expected_conditions as EC            # Wait predicates (visibility/clickable/presence)
from selenium.common.exceptions import StaleElementReferenceException       # Handle dynamic DOM reattachment/detachment

from webdriver_manager.chrome import ChromeDriverManager                    # Auto-provision ChromeDriver (version-matched)
from selenium.webdriver.chrome.options import Options as ChromeOptions      # Chrome flags (headless, prefs, download dir)
from selenium.webdriver.chrome.service import Service as ChromeService      # ChromeDriver service wrapper

from webdriver_manager.microsoft import EdgeChromiumDriverManager           # Auto-provision EdgeDriver (Chromium)
from selenium.webdriver.edge.options import Options as EdgeOptions          # Edge flags (headless, prefs)
from selenium.webdriver.edge.service import Service as EdgeService          # EdgeDriver service wrapper

from webdriver_manager.firefox import GeckoDriverManager                    # Auto-provision GeckoDriver (Firefox)
from selenium.webdriver.firefox.options import Options as FirefoxOptions    # Firefox flags (headless, prefs)
from selenium.webdriver.firefox.service import Service as FirefoxService    # GeckoDriver service wrapper


# ++++++++++++++++++++++++++++++++++++++++++++++++
# Module-level setting-up
# ++++++++++++++++++++++++++++++++++++++++++++++++

# HTTP
REQUEST_CHUNK_SIZE  = 128                       # Bytes per chunk when streaming downloads
REQUEST_TIMEOUT     = 60                        # Seconds for connect + read timeouts
DEFAULT_RETRIES     = 3                         # Total retries for transient HTTP errors
DEFAULT_BACKOFF     = 0.5                       # Exponential backoff factor (0.5, 1.0, 2.0, ...)
RETRY_STATUSES      = (429, 500, 502, 503, 504) # Retry on rate limits and server errors

# Selenium / Browser
PAGE_LOAD_TIMEOUT       = 30                    # Seconds to wait for page loads
EXPLICIT_WAIT_TIMEOUT   = 60                    # Seconds for WebDriverWait

# Downloader pacing
DEFAULT_MIN_WAIT    = 5.0                       # Lower bound for random delay between downloads (seconds)
DEFAULT_MAX_WAIT    = 10.0                      # Upper bound for random delay between downloads (seconds)


# ++++++++++++++++++++++++++++++++++++++++++++++++
# Functions
# ++++++++++++++++++++++++++++++++++++++++++++++++

# _________________________________________________________________________
# Function to create a retry-enabled HTTP session for resilient downloads
def get_http_session(
    total: int = DEFAULT_RETRIES,
    backoff: float = DEFAULT_BACKOFF,
    statuses: tuple = RETRY_STATUSES,
) -> requests.Session:
    """
    Create a persistent HTTP session configured with retries and exponential backoff
    for transient HTTP errors (e.g., 429/5xx). Safe drop-in replacement for plain GETs.

    Args:
        total (int): Max retries for connect/read/status failures.
        backoff (float): Backoff factor (sleep grows as 0.5, 1.0, 2.0, ...).
        statuses (tuple): HTTP status codes that should trigger a retry.

    Returns:
        requests.Session: Session with mounted retry-enabled adapters.
    """
    retry = Retry(
        total=total,
        read=total,
        connect=total,
        backoff_factor=backoff,                         # Controls exponential sleep between retries
        status_forcelist=statuses,                      # Retry only on these HTTP status codes
        allowed_methods=frozenset(["GET", "HEAD"]),     # Retry idempotent methods only
        raise_on_status=False,                          # Do not raise; let caller inspect status_code
    )
    sess = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)                     # Apply retry policy to HTTPS
    sess.mount("http://", adapter)                      # Apply retry policy to HTTP
    return sess

# _________________________________________________________________________
# Function to load a random .mp3 alert track without repeating the last one
def load_alert_track(alert_track_folder: str, last_alert: str | None) -> str | None:
    """
    Load a random .mp3 file from `alert_track_folder` for audio alerts, avoiding
    immediate repetition of the previous selection when possible.

    Args:
        alert_track_folder (str): Directory expected to contain one or more .mp3 files.
        last_alert (str | None): The filename of the last track played.

    Returns:
        str | None: Absolute path to the selected .mp3 file, or None if no .mp3 is found.
    """
    os.makedirs(alert_track_folder, exist_ok=True)                          # Ensure folder exists (no error if present)

    tracks = [f for f in os.listdir(alert_track_folder)                     # Collect only .mp3 filenames (case-insensitive)
              if f.lower().endswith(".mp3")]
    if not tracks:
        print("🔇 No .mp3 files found in 'alert_track/'. Continuing without audio alerts.")
        return None

    choices = [t for t in tracks if t != last_alert] or tracks              # Prefer any file ≠ last; fallback to all if single
    track   = random.choice(choices)                                        # Uniform random selection among candidates

    alert_track_path = os.path.join(alert_track_folder, track)              # Build absolute path to the chosen file
    pygame.mixer.music.load(alert_track_path)                               # Preload into pygame mixer for instant playback
    return track                                                            # Return the selected track instead of the path

# _________________________________________________________________________
# Function to start playback of the loaded alert track
def play_alert_track() -> None:
    """Start playback of the currently loaded alert track."""
    pygame.mixer.music.play()                                               # Non-blocking playback

# _________________________________________________________________________
# Function to stop playback of the alert track immediately
def stop_alert_track() -> None:
    """Stop playback of the current alert track."""
    pygame.mixer.music.stop()                                               # Immediate stop

# _________________________________________________________________________
# Function to wait a random interval to mimic human pacing
def random_wait(min_time: float, max_time: float) -> None:
    """
    Pause execution for a random duration within [min_time, max_time].

    Args:
        min_time (float): Lower bound for waiting time (seconds).
        max_time (float): Upper bound for waiting time (seconds).
    """
    wait_time = random.uniform(min_time, max_time)                          # Inclusive random delay
    print(f"⏳ Waiting {wait_time:.2f} seconds...")
    time.sleep(wait_time)                                                   # Sleep for the computed duration

# _________________________________________________________________________
# Function to initialize a Selenium WebDriver for the chosen browser
def init_driver(browser: str = "chrome", headless: bool = False):
    """
    Initialize and return a Selenium WebDriver instance.

    Args:
        browser (str): Engine to use. Supported: 'chrome' (default), 'firefox', 'edge'.
        headless (bool): Run the browser in headless mode if True.

    Returns:
        selenium.webdriver: Configured WebDriver instance.
    """
    b = browser.lower()

    if b == "chrome":
        options = ChromeOptions()
        if headless:
            options.add_argument("--headless=new")                          # Modern headless mode
        options.add_argument("--no-sandbox")                                # Stability in containerized envs
        options.add_argument("--disable-dev-shm-usage")                     # Avoid /dev/shm issues
        service = ChromeService(ChromeDriverManager().install())            # Provision ChromeDriver automatically
        driver = webdriver.Chrome(service=service, options=options)

    elif b == "firefox":
        fopts = FirefoxOptions()
        if headless:
            fopts.add_argument("-headless")                                 # Firefox headless flag
        service = FirefoxService(GeckoDriverManager().install())            # Provision GeckoDriver automatically
        driver = webdriver.Firefox(service=service, options=fopts)

    elif b == "edge":
        eopts = EdgeOptions()
        if headless:
            eopts.add_argument("--headless=new")
        eopts.add_argument("--no-sandbox")
        eopts.add_argument("--disable-dev-shm-usage")
        service = EdgeService(EdgeChromiumDriverManager().install())        # Provision EdgeDriver automatically
        driver = webdriver.Edge(service=service, options=eopts)

    elif b == "safari":
        if headless:
            print("⚠️  Headless mode is not supported for Safari. Running in normal mode.")
        driver = webdriver.Safari()                                         # Safari driver bundled with macOS

    else:
        raise ValueError("Supported browsers are: 'chrome', 'firefox', 'edge', 'safari'.")

    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)                         # Hard limit for page loads
    return driver

# _________________________________________________________________________
# Function to download a single PDF and update the chronological record
def download_pdf(
    driver,
    pdf_link,
    wait: WebDriverWait,
    download_counter: int,
    raw_pdf_folder: str,
    download_record_folder: str,
    download_record_txt: str,
) -> bool:
    """
    Download a single PDF referenced by a Selenium link element and update the record log.

    Args:
        driver (WebDriver): Active Selenium WebDriver instance.
        pdf_link (WebElement): Anchor element pointing to the PDF.
        wait (WebDriverWait): Explicit wait helper bound to the driver.
        download_counter (int): Ordinal used in progress messages.
        raw_pdf_folder (str): Destination directory for the downloaded PDF.
        download_record_folder (str): Folder containing the record text file.
        download_record_txt (str): Record filename (e.g., 'downloaded_pdf.txt').

    Returns:
        bool: True if the file was successfully downloaded and recorded; False otherwise.
    """
    driver.execute_script("arguments[0].click();", pdf_link)                    # Click via JS (handles covered/overlayed links)
    wait.until(EC.number_of_windows_to_be(2))                                   # Wait for a new tab to open (2 windows in total)
    windows = driver.window_handles                                             # Get both window handles
    driver.switch_to.window(windows[1])                                         # Focus the new tab

    new_url   = driver.current_url                                              # Final PDF URL after any redirects
    file_name = os.path.basename(new_url)                                       # Use server-provided filename
    destination_path = os.path.join(raw_pdf_folder, file_name)                  # Local destination

    session = get_http_session()                                                # Session with retry/backoff
    try:
        response = session.get(new_url, stream=True, timeout=REQUEST_TIMEOUT)   # Stream to avoid loading large files in RAM
        if response.status_code == 200:                                         # Successful request; proceed to fetch other codes
            os.makedirs(raw_pdf_folder, exist_ok=True)                          # Ensure output folder exists
            with open(destination_path, "wb") as fh:
                for chunk in response.iter_content(chunk_size=REQUEST_CHUNK_SIZE):
                    if chunk:                                                   # Skip keep-alive chunks
                        fh.write(chunk)
        else:
            print(f"{download_counter}. ❌ Error downloading {file_name}. HTTP {response.status_code}")
            success = False
            driver.close(); driver.switch_to.window(windows[0])                 # Close child tab and return focus
            return success
    except requests.RequestException as ex:
        print(f"{download_counter}. ❌ Network error downloading {file_name}: {ex}")
        success = False
        driver.close(); driver.switch_to.window(windows[0])                     # Close child tab and return focus
        return success

    # Update the record log in chronological order (year -> issue)
    record_path = os.path.join(download_record_folder, download_record_txt)
    records: list[str] = []
    if os.path.exists(record_path):
        with open(record_path, "r", encoding="utf-8") as f:
            records = [ln.strip() for ln in f if ln.strip()]                    # Keep non-empty lines only

    if file_name not in records:
        records.append(file_name)                                               # Append if not present

    def _ns_key(s: str):
        base = os.path.splitext(os.path.basename(s))[0]                         # Strip extension
        m = re.search(r"ns-(\d{2})-(\d{4})", base, re.I)                        # Expect ns-<issue>-<year>
        if not m:
            return (9999, 9999, base)                                           # Unknown pattern -> sort last
        issue, year = int(m.group(1)), int(m.group(2))
        return (year, issue)

    records.sort(key=_ns_key)                                                   # Chronological order
    os.makedirs(download_record_folder, exist_ok=True)
    with open(record_path, "w", encoding="utf-8") as f:
        f.write("\n".join(records) + ("\n" if records else ""))                 # Trailing newline if non-empty

    print(f"{download_counter}. ✔️ Downloaded: {file_name}")
    success = True

    driver.close(); driver.switch_to.window(windows[0])                         # Close child tab and go back to main
    return success

# _________________________________________________________________________
# Function to orchestrate monthly-link crawling, WR downloads, pacing, and summary
def pdf_downloader(
    bcrp_url: str,
    raw_pdf_folder: str,
    download_record_folder: str,
    download_record_txt: str,
    alert_track_folder: str,
    max_downloads: int | None = None,
    downloads_per_batch: int = 12,
    headless: bool = False,
) -> None:
    """
    Download BCRP Weekly Reports (WR) by crawling the monthly listing page, selecting the
    first link inside each month block (business rule: the first anchor corresponds to the
    latest WR of that month), and saving files in chronological order while avoiding
    duplicates via a persistent record text file. Optionally pauses every N downloads with
    an audible prompt and prints a concise run summary.

    What this function does:
      1) Opens the WR listing page and locates one anchor per month (the first/“latest”).
      2) Reverses the order to download from oldest -> newest for stable local numbering.
      3) Skips any file already present in the record file (no re-download).
      4) Streams each PDF to disk and appends its filename to the record (chronological).
      5) Optionally pauses after each batch with a short alert track and user prompt.
      6) Prints a final summary (total links, skipped, new, elapsed time).

    Assumptions:
      - The site structures monthly WR links under `#rightside ul.listado-bot-std-claros`.
      - Within each month block, the first <a> is the latest WR of that month.
      - The record file contains one filename per line (e.g., ns-07-2019.pdf).

    Args:
        bcrp_url (str): URL of the BCRP WR listing page.
        raw_pdf_folder (str): Destination folder for downloaded PDFs.
        download_record_folder (str): Folder containing the record file.
        download_record_txt (str): Record filename tracking downloaded PDFs (one per line).
        alert_track_folder (str): Folder with .mp3 files (optional audio prompt between batches).
        max_downloads (int | None): Upper bound on new downloads; None means no cap.
        downloads_per_batch (int): Number of files between optional pause prompts.
        headless (bool): If True, runs the browser in headless mode.

    Returns:
        None
    """
    start_time = time.time()                                                # Wall-clock start (seconds since epoch)

    print("\n📥 Starting PDF downloader for BCRP WR...\n")
    pygame.mixer.init()                                                     # Ready the audio mixer for alerts

    _last_alert = None                                                      # Initialize memory of last alert
    alert_track_path = load_alert_track(alert_track_folder, _last_alert)    # Load a random .mp3 if available
    _last_alert = alert_track_path                                          # Store last alert name (filename only)

    record_path = os.path.join(download_record_folder, download_record_txt) # State file: prevents duplicates
    downloaded_files = set()
    if os.path.exists(record_path):
        with open(record_path, "r", encoding="utf-8") as f:
            downloaded_files = set(f.read().splitlines())                   # Preload known filenames into a set

    driver = init_driver(headless=headless)                                 # Start chosen browser engine
    wait = WebDriverWait(driver, EXPLICIT_WAIT_TIMEOUT)                     # Explicit wait helper bound to driver

    new_counter  = 0                                                        # Count new files successfully downloaded
    skipped_files: list[str] = []                                           # Filenames skipped due to record matches
    new_downloads = []                                                      # Queue of (selenium_element, filename)
    pdf_links = []                                                          # Full set of month-leading anchors for summary

    try:
        driver.get(bcrp_url)                                                # Open WR listing page
        print("🌐 BCRP site opened successfully.")

        month_ul_elems = wait.until(                                        # Wait for all month containers to appear
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "#rightside ul.listado-bot-std-claros"))
        )
        print(f"🔎 Found {len(month_ul_elems)} WR blocks on page (one per month).\n")

        # Select exactly one link per month (business rule: the first anchor inside the block)
        for ul in month_ul_elems:
            try:
                anchors = ul.find_elements(By.TAG_NAME, "a")                # All anchors within this month block
            except Exception:
                anchors = []                                                # Conservative fallback if DOM changes mid-run
            if not anchors:
                continue
            pdf_links.append(anchors[0])                                    # Keep only the first anchor (latest monthly WR)

        pdf_links = pdf_links[::-1]                                         # Oldest -> newest for stable local ordering

        # Build a work queue, skipping any file already recorded
        for link in pdf_links:
            try:
                file_url  = link.get_attribute("href")                      # Resolve the URL bound to the anchor
                file_name = os.path.basename(file_url)                      # Server-provided filename (e.g., ns-07-2019.pdf)
            except Exception:
                continue                                                    # Defensive skip if attributes are momentarily unavailable

            if file_name in downloaded_files:
                skipped_files.append(file_name)                             # Already captured in prior runs -> skip
            else:
                new_downloads.append((link, file_name))                     # Will download in chronological pass

        # Download queue (chronological), with optional batch pauses and pacing
        for i, (link, file_name) in enumerate(new_downloads, start=1):
            # Load a new random alert for each batch start
            if i % downloads_per_batch == 1:                                # New batch
                alert_track_path = load_alert_track(alert_track_folder, _last_alert)
                _last_alert = alert_track_path                              # Update memory of last alert

            ok = download_pdf(
                driver=driver,
                pdf_link=link,
                wait=wait,
                download_counter=i,
                raw_pdf_folder=raw_pdf_folder,
                download_record_folder=download_record_folder,
                download_record_txt=download_record_txt,
            )
            if ok:
                downloaded_files.add(file_name)                             # Update in-memory record immediately
                new_counter += 1

            # Optional checkpoint every N downloads — useful for long sessions
            if (i % downloads_per_batch == 0) and alert_track_path:
                play_alert_track()
                user_input = input("⏸️ Continue? (y = yes, any other key = stop): ")
                stop_alert_track()
                if user_input.lower() != "y":
                    print("🛑 Download stopped by user.")
                    break

            # Respect a global cap if provided (e.g., first 20 new files only)
            if max_downloads and new_counter >= max_downloads:
                print(f"🏁 Download limit of {max_downloads} new PDFs reached.")
                break

            random_wait(DEFAULT_MIN_WAIT, DEFAULT_MAX_WAIT)                 # Gentle pacing to mimic a human user

    except StaleElementReferenceException:
        print("⚠️ StaleElementReferenceException encountered. Consider re-running.")  
    finally:
        driver.quit()                                                       # Ensure the browser is closed in all cases
        print("\n👋 Browser closed.")

    # Maintain the record file in chronological order (idempotent)
    try:
        if os.path.exists(record_path):
            with open(record_path, "r", encoding="utf-8") as f:
                records = [ln.strip() for ln in f if ln.strip()]            # Compact to non-empty, shortened lines

            def _ns_key(s: str):
                base = os.path.splitext(os.path.basename(s))[0]
                m = re.search(r"ns-(\d{2})-(\d{4})", base, re.I)            # Expect pattern ns-<issue>-<year>
                if not m:
                    return (9999, 9999, base)                               # Unknown names sorted last (stable by base)
                issue, year = int(m.group(1)), int(m.group(2))
                return (year, issue)

            records = sorted(set(records), key=_ns_key)                     # De-dup then sort by (year, issue)
            os.makedirs(download_record_folder, exist_ok=True)
            with open(record_path, "w", encoding="utf-8") as f:
                f.write("\n".join(records) + ("\n" if records else ""))     # Trailing newline for POSIX-friendly files
    except Exception as _e:
        print(f"⚠️ Unable to re-sort record file: {_e}")                   

    # Final summary for the session
    elapsed_time = round(time.time() - start_time)                          # Seconds elapsed
    total_links  = len(pdf_links)                                           # Count of month-leading anchors discovered
    print("\n📊 Summary:")
    print(f"\n🔗 Total monthly links kept: {total_links}")
    if skipped_files:
        print(f"🗂️ {len(skipped_files)} already downloaded PDFs were skipped.")
    print(f"➕ Newly downloaded: {new_counter}")
    print(f"⏱️ {elapsed_time} seconds")

# _________________________________________________________________________
# Function to move PDFs into year-based subfolders inferred from filenames
def organize_files_by_year(raw_pdf_folder: str) -> None:
    """
    Move PDFs in `raw_pdf_folder` into subfolders named by year.
    The year is inferred from the first 4-digit token in the filename.

    Args:
        raw_pdf_folder (str): Directory containing the downloaded PDFs.
    """
    files = os.listdir(raw_pdf_folder)                                      # List immediate children in the folder

    for file in files:
        name, _ext = os.path.splitext(file)                                 # Separate stem and extension
        year = None

        for part in name.split("-"):                                        # Heuristic: look for any 4-digit token
            if part.isdigit() and len(part) == 4:
                year = part
                break

        if year:
            dest = os.path.join(raw_pdf_folder, year)                       # Year subfolder path
            os.makedirs(dest, exist_ok=True)                                # Create if absent
            shutil.move(os.path.join(raw_pdf_folder, file), dest)           # Move file into its year folder
        else:
            print(f"⚠️ No 4-digit year detected in filename: {file}")      

# _________________________________________________________________________
# Function to replace defective WR PDFs (NS files) and update the record safely
def replace_defective_pdfs(
    items: list[tuple[str, str, str]],
    root_folder: str,
    record_folder: str,
    download_record_txt: str,
    quarantine: str | None = None,
    verbose: bool = True,
) -> tuple[int, int]:
    """
    Replace defective WR PDFs (BCRP Nota Semanal, 'ns-XX-YYYY.pdf') stored under year subfolders.
    Keeps the download record consistent so the downloader will not re-fetch defective files.

    Args:
        items: List of triples (year, defective_pdf, replacement_code).
               Example: [("2017","ns-08-2017.pdf","ns-07-2017"), ("2019","ns-23-2019.pdf","ns-22-2019")]
        root_folder: Base path containing year folders (e.g., 'raw_pdf').
        record_folder: Folder holding the download record TXT.
        download_record_txt: Record filename (e.g., 'downloaded_pdfs.txt').
        quarantine: If set, move defective PDFs there; if None, delete them.
        verbose: If True, prints a clear summary at the end.

    Returns:
        tuple[int, int]: (ok, fail)
            ok   = number of PDFs successfully replaced
            fail = total failures = not_found + download_errors + file_op_errors

    Notes:
        - Defective entries are intentionally NOT removed from the record file to prevent re-downloads.
        - Replacement filenames ARE appended to the record file in chronological order.
    """
    pat = re.compile(r"^ns-(\d{1,2})-(\d{4})(?:\.pdf)?$", re.I)             # Accept 'ns-7-2019' or 'ns-07-2019[.pdf]'

    def norm(c: str) -> str:
        m = pat.match(os.path.basename(c).lower())                          # Validate and extract (issue, year)
        if not m:
            raise ValueError(f"Bad NS code: {c}")
        return f"ns-{int(m.group(1)):02d}-{m.group(2)}"                     # Zero-pad issue (e.g., 7 -> 07)

    def url(c: str) -> str:
        cc = norm(c)                                                        # Normalized 'ns-xx-yyyy'
        return f"https://www.bcrp.gob.pe/docs/Publicaciones/Nota-Semanal/{cc[-4:]}/{cc}.pdf"  # Year-coded path

    def _ns_key(name: str) -> tuple[int, int, str]:
        base = os.path.splitext(os.path.basename(name))[0]                  # Remove dir and extension
        m = re.search(r"ns-(\d{2})-(\d{4})", base, re.I)                    # Extract issue/year for sorting
        if not m:
            return (9999, 9999, base)                                       # Unknowns last, stable by base
        issue, year = int(m.group(1)), int(m.group(2))
        return (year, issue, base)                                          # Sort by (year -> issue -> name)

    def update_record(add: str | None = None, remove: str | None = None) -> None:
        p = os.path.join(record_folder, download_record_txt)                # Record file path
        s: set[str] = set()
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                s = {x.strip() for x in f if x.strip()}                     # Read, trim, de-duplicate
        if add:
            s.add(add)                                                      # Append replacement filename
        records = sorted(s, key=_ns_key)                                    # Chronological order (year -> issue)
        os.makedirs(record_folder, exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            f.write("\n".join(records) + ("\n" if records else ""))         # Trailing newline if non-empty

    if quarantine:
        os.makedirs(quarantine, exist_ok=True)                              # Ensure quarantine folder exists

    ok = 0
    not_found = 0
    download_errors = 0
    file_op_errors = 0

    replaced_names: list[str] = []                                          # Keep a small preview list
    failed_items: list[tuple[str, str, str, str]] = []                      # (year, bad_pdf, repl_code, reason)

    for year, bad_pdf, repl_code in items:
        year = str(year)                                                    # Normalize to string for joins
        ydir = os.path.join(root_folder, year)                              # e.g., raw_pdf/2019
        bad_path = os.path.join(ydir, bad_pdf)                              # Existing defective file path
        new_name = f"{norm(repl_code)}.pdf"                                 # Normalized replacement filename
        new_path = os.path.join(ydir, new_name)                             # Destination for replacement

        if not os.path.exists(bad_path):
            not_found += 1
            failed_items.append((year, bad_pdf, repl_code, "not found"))
            continue

        # Download replacement first (ensures we only remove old file after we have a good replacement)
        try:
            os.makedirs(ydir, exist_ok=True)
            with requests.get(url(repl_code), stream=True, timeout=60) as r:
                r.raise_for_status()                                        # Non-2xx -> raise HTTPError
                with open(new_path, "wb") as fh:
                    for ch in r.iter_content(131072):                       # Stream in 128 KiB chunks
                        if ch:
                            fh.write(ch)
        except Exception as e:
            try:
                if os.path.exists(new_path):
                    os.remove(new_path)                                     # Remove partial download (if any)
            except Exception:
                pass
            download_errors += 1
            failed_items.append((year, bad_pdf, repl_code, f"download: {e.__class__.__name__}"))
            continue

        # Quarantine or delete the defective file; ensure folder consistency on failure
        try:
            if quarantine:
                shutil.move(bad_path, os.path.join(quarantine, bad_pdf))    # Preserve evidence under quarantine
            else:
                os.remove(bad_path)                                         # Permanent removal
        except Exception as e:
            file_op_errors += 1
            failed_items.append((year, bad_pdf, repl_code, f"file-op: {e.__class__.__name__}"))
            try:
                if os.path.exists(new_path):
                    os.remove(new_path)                                     # Roll back replacement to keep state clean
            except Exception:
                pass
            continue

        update_record(add=new_name, remove=bad_pdf)                         # Keep defective entry; append replacement
        replaced_names.append(new_name)
        ok += 1

    fail = not_found + download_errors + file_op_errors

    if verbose:
        print("\n📊 PDF replacement summary")
        print(f"   • Succeeded: {ok}")
        print(f"   • Failed:    {fail} "
              f"(not found: {not_found}, download errors: {download_errors}, file ops: {file_op_errors})")
        if replaced_names:
            preview = ", ".join(replaced_names[:10])
            suffix = "…" if len(replaced_names) > 10 else ""
            print(f"   • New files: {preview}{suffix}")
        if failed_items:
            print("   • Failed items (sample):")
            for y, bad, rep, reason in failed_items[:5]:
                print(f"     - {bad} [{y}] ← {rep}  ({reason})")
            if len(failed_items) > 5:
                print(f"     … and {len(failed_items) - 5} more")

    return ok, fail



# ##############################################################################################
# SECTION 2 Generating input PDFs with key tables
# ##############################################################################################

# In this section we build an automated input PDF generator for WR PDFs. It searches pages by
# keywords, produces shortened “input” PDFs, and (for 4-page outputs) keeps only pages 1 and 3
# where the key tables usually appear. A simple record prevents re-processing.


# ++++++++++++++++++++++++++++++++++++++++++++++++
# Libraries
# ++++++++++++++++++++++++++++++++++++++++++++++++

# import os                                                                 # [already imported and documented in section 1]
# import re                                                                 # [already imported and documented in section 1]
# import time                                                               # [already imported and documented in section 1]
# import shutil                                                             # [already imported and documented in section 1]
# import requests                                                           # [already imported and documented in section 1]
import fitz                                                                 # PyMuPDF: fast PDF I/O, page extraction, lightweight edits
import ipywidgets as widgets                                                # Jupyter UI widgets (controls/progress/inputs for workflows)
from IPython.display import display                                         # Render widgets/HTML/images inline in notebooks
from tqdm.notebook import tqdm                                              # Jupyter-friendly progress bar for iterative tasks
from PyPDF2 import PdfReader, PdfWriter                                     # Page-level edits: split/merge/select/rotate pages


# ++++++++++++++++++++++++++++++++++++++++++++++++
# Functions
# ++++++++++++++++++++++++++++++++++++++++++++++++

# _________________________________________________________________________
# Function to find page indices in a PDF that contain any of the given keywords
def search_keywords(pdf_file, keywords):
    """
    Return 0-indexed page numbers containing any keyword (case-sensitive).

    Args:
        pdf_file (str): Path to the WR PDF.
        keywords (list[str]): Keywords to search for (case-sensitive).

    Returns:
        list[int]: Page indices where any keyword appears.
    """
    pages_with_keywords = []
    with fitz.open(pdf_file) as doc:
        for page_num in range(doc.page_count):
            page_text = doc.load_page(page_num).get_text()                 # Extract text for this page
            if any(k in page_text for k in keywords):                      # True if at least one keyword is present
                pages_with_keywords.append(page_num)
    return pages_with_keywords

# _________________________________________________________________________
# Function to create a new PDF that retains only selected pages from a source PDF
def shortened_pdf(pdf_file, pages, output_folder):
    """
    Create a compact PDF containing only `pages` from `pdf_file` and save it to `output_folder`.

    Args:
        pdf_file (str): Path to the source WR PDF.
        pages (list[int]): 0-indexed pages to retain.
        output_folder (str): Destination folder for the shortened PDF.

    Returns:
        int: Number of pages in the shortened PDF (0 if no pages were selected).
    """
    if not pages:
        return 0                                                           # Nothing to keep -> skip

    os.makedirs(output_folder, exist_ok=True)                              # Ensure target folder exists
    new_pdf_file = os.path.join(output_folder, os.path.basename(pdf_file)) # Output path mirrors source filename
    with fitz.open(pdf_file) as doc:
        new_doc = fitz.open()                                              # Empty in-memory PDF
        for p in pages:
            new_doc.insert_pdf(doc, from_page=p, to_page=p)                # Copy exactly page p
        new_doc.save(new_pdf_file)                                         # Persist shortened PDF
        count = new_doc.page_count                                         # Capture page count before closing
        new_doc.close()
    return count

# _________________________________________________________________________
# Function to read the record of WR PDFs that already have input PDFs generated
def read_input_pdf_files(input_pdf_record_folder, input_pdf_record_txt):
    """
    Read filenames of previously processed WR PDFs from the record file.

    Args:
        input_pdf_record_folder (str): Folder where the record file lives.
        input_pdf_record_txt (str): Record filename.

    Returns:
        set[str]: Filenames already processed.
    """
    record_path = os.path.join(input_pdf_record_folder, input_pdf_record_txt)
    if not os.path.exists(record_path):
        return set()
    with open(record_path, "r", encoding="utf-8") as f:
        return set(ln.strip() for ln in f if ln.strip())                   # Remove blanks and deduplicate via set

# _________________________________________________________________________
# Function to write/update the record of WR PDFs with generated input PDFs
def write_input_pdf_files(input_pdf_files, input_pdf_record_folder, input_pdf_record_txt):
    """
    Persist processed WR PDF filenames to the record file (one per line, sorted for determinism).

    Args:
        input_pdf_files (set[str]): Filenames to write.
        input_pdf_record_folder (str): Folder for the record file.
        input_pdf_record_txt (str): Record filename.
    """
    record_path = os.path.join(input_pdf_record_folder, input_pdf_record_txt)
    os.makedirs(input_pdf_record_folder, exist_ok=True)                    # Ensure folder exists
    with open(record_path, "w", encoding="utf-8") as f:
        for fn in sorted(input_pdf_files):                                 # Stable, predictable order
            f.write(fn + "\n")

# _________________________________________________________________________
# Function to prompt for a yes/no decision in console environments
def ask_continue_input(message):
    """
    Ask the operator whether to continue (yes/no).

    Args:
        message (str): Prompt to show.

    Returns:
        bool: True for 'y', False for 'n'.
    """
    while True:
        ans = input(f"{message} (y = yes / n = no): ").strip().lower()
        if ans in ("y", "n"):
            return ans == "y"                                              # Repeat until a valid response is given

# _________________________________________________________________________
# Function to generate shortened input PDFs from raw WR PDFs using keyword hits
def pdf_input_generator(
    raw_pdf_folder,
    input_pdf_folder,
    input_pdf_record_folder,
    input_pdf_record_txt,
    keywords
):
    """
    Generate input PDFs from raw WR PDFs by extracting only pages that match `keywords`.
    If a shortened PDF has 4 pages, keep only pages 1 and 3 (common location of key tables).
    Updates the record to avoid re-processing.

    Args:
        raw_pdf_folder (str): Folder containing yearly subfolders of raw WR PDFs.
        input_pdf_folder (str): Folder to save the input PDFs.
        input_pdf_record_folder (str): Folder to store the record file.
        input_pdf_record_txt (str): Record filename (e.g., 'input_pdfs.txt').
        keywords (list[str]): Keywords used to select relevant pages.
    """
    start_time = time.time()

    input_pdf_files = read_input_pdf_files(input_pdf_record_folder, input_pdf_record_txt)   # Previously processed set
    skipped_years = {}                                                                      # Map year -> count already processed
    new_counter = 0
    skipped_counter = 0

    for folder in sorted(os.listdir(raw_pdf_folder)):                                       # Iterate years in order
        if folder == "_quarantine":                                                         # Skip quarantine area
            continue

        folder_path = os.path.join(raw_pdf_folder, folder)
        if not os.path.isdir(folder_path):
            continue

        pdf_files = [f for f in os.listdir(folder_path) if f.endswith(".pdf")]
        if not pdf_files:
            continue

        already = [f for f in pdf_files if f in input_pdf_files]                            # Files in this year already processed
        if len(already) == len(pdf_files):                                                  # Entire year already processed
            skipped_years[folder] = len(already)
            skipped_counter += len(already)
            continue

        print(f"\n📂 Processing folder: {folder}\n")
        folder_new_count = 0
        folder_skipped_count = 0

        pbar = tqdm(                                                                        # Year-level progress bar
            pdf_files,
            desc=f"Generating input PDFs with key tables in {folder}",
            unit="PDF",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
            colour="#E6004C"
        )

        for filename in pbar:
            pdf_file = os.path.join(folder_path, filename)
            if filename in input_pdf_files:
                folder_skipped_count += 1
                continue

            pages_with_keywords = search_keywords(pdf_file, keywords)                       # Candidate page indices
            num_pages = shortened_pdf(pdf_file, pages_with_keywords, output_folder=input_pdf_folder)

            short_pdf_file = os.path.join(input_pdf_folder, os.path.basename(pdf_file))
            reader = PdfReader(short_pdf_file)                                              # Inspect the shortened output

            # Using the keyword "economic sectors" typically yields 4 pages — corresponding to 4 GDP tables:
            # 2 in levels and 2 in percentage variations. We only need the latter (percentage variations).   
            if len(reader.pages) == 4:                                                      # Special case: retain 1st and 3rd pages
                writer = PdfWriter()
                writer.add_page(reader.pages[0])                                            # Keep page 1 (monthly GDP percentage variations)
                writer.add_page(reader.pages[2])                                            # Keep page 3 (quarterly/annual GDP percentage variations)
                with open(short_pdf_file, "wb") as f_out:
                    writer.write(f_out)

            if num_pages > 0:                                                               # Only mark successful extractions
                input_pdf_files.add(filename)
                folder_new_count += 1

        # Attempt to recolor the bar to indicate completion (may be unsupported in some envs)
        try:
            pbar.colour = "#3366FF"                                                         # Finished color
            pbar.refresh()
        except Exception:
            pass
        finally:
            pbar.close()

        # Chronological record order: (year, issue) inferred from 'ns-XX-YYYY'
        def _ns_key(s):
            base = os.path.splitext(os.path.basename(s))[0]                                 # Strip extension
            m = re.search(r"ns-(\d{2})-(\d{4})", base, re.I)
            if not m:
                return (9999, 9999, base)                                                   # Unknown names at the end
            issue = int(m.group(1)); year = int(m.group(2))
            return (year, issue)

        ordered_records = sorted(input_pdf_files, key=_ns_key)                              # Deterministic write order
        os.makedirs(input_pdf_record_folder, exist_ok=True)
        record_path = os.path.join(input_pdf_record_folder, input_pdf_record_txt)
        with open(record_path, "w", encoding="utf-8") as f_rec:
            for name in ordered_records:
                f_rec.write(name + "\n")

        print(f"✔️ Shortened PDFs saved in '{input_pdf_folder}' "
              f"({folder_new_count} new, {folder_skipped_count} skipped)")

        new_counter += folder_new_count
        skipped_counter += folder_skipped_count

        if not ask_continue_input(f"Do you want to continue to the next folder after '{folder}'?"):
            print("🛑 Process stopped by user.")
            break

    if skipped_years:
        years_summary = ", ".join(skipped_years.keys())
        total_skipped = sum(skipped_years.values())
        print(f"\n⏩ {total_skipped} input PDFs already generated for years: {years_summary}")

    elapsed_time = round(time.time() - start_time)
    print(f"\n📊 Summary:\n")
    print(f"📂 {len(os.listdir(raw_pdf_folder))} folders (years) found containing raw PDFs")
    print(f"🗃️ Already generated input PDFs: {skipped_counter}")
    print(f"➕ Newly generated input PDFs: {new_counter}")
    print(f"⏱️ {elapsed_time} seconds")



# ##############################################################################################
# SECTION 3 Cleaning tables and building RTD
# ##############################################################################################

# DISCLAIMER: This is the longest section, as it contains the core and essence of our RTD construction process. 

# ==============================================================================================
# SECTION 3.1  Creating functions for extracting, parsing and cleaning-up input tables 
# ==============================================================================================
# In this section we prepare reusable helpers to parse WR-derived tables (Table 1 and Table 2)
# extracted from input PDFs. Utilities include text normalization, column/row fixes, selective
# splitting, roman–arabic conversions, and tailored corrections for specific WR issues.


# ++++++++++++++++++++++++++++++++++++++++++++++++
# Libraries
# ++++++++++++++++++++++++++++++++++++++++++++++++

# import re                                                                 # [already imported and documented in section 1]
import unicodedata                                                          # Unicode normalization (strip accents/compat forms, NFC/NFKD)
import pandas as pd                                                         # Tabular data structures, vectorized ops, IO (CSV/Parquet)
import numpy as np                                                          # Numerical helpers (arrays, NaNs, dtype ops, vector math)
import roman                                                                # Roman ↔ integer conversion (e.g., parsing section headings)


# ++++++++++++++++++++++++++++++++++++++++++++++++
# Ancillary utilities for upcoming cleanup
# functions
# ++++++++++++++++++++++++++++++++++++++++++++++++

# _________________________________________________________________________
# Function to normalize first-row text and keep only letters/digits/hyphens
def remove_rare_characters_first_row(texto):
    """
    Remove spaces around hyphens and drop non-alphanumeric characters (except hyphens).
    Intended for first-row cleanups where headers are later derived.
    """
    texto = re.sub(r'\s*-\s*', '-', texto)                           # Normalize "a - b" -> "a-b"
    texto = re.sub(r'[^a-zA-Z0-9\s-]', '', texto)                    # Keep letters/digits/spaces/hyphens
    return texto

# _________________________________________________________________________
# Function to strip all non-letters from arbitrary text
def remove_rare_characters(texto):
    """Remove any character that is not a letter or space."""
    return re.sub(r'[^a-zA-Z\s]', '', texto)

# _________________________________________________________________________
# Function to strip diacritics (tildes) from text
def remove_tildes(texto):
    """Return text without diacritics using Unicode decomposition."""
    return ''.join((c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn'))

# _________________________________________________________________________
# Function to find Roman numerals (I to X) in text
def find_roman_numerals(text):
    """Return a list of Roman numerals (I–X) found in text."""
    pattern = r'\b(?:I{1,3}|IV|V|VI{0,3}|IX|X)\b'
    matches = re.findall(pattern, text)
    return matches

# _________________________________________________________________________
# Function to split the third-from-last column into multiple columns (Table 2 helper)
def split_values(df):
    """
    Split whitespace-separated tokens in the third-from-last column into new columns,
    inserting them before the last two columns.
    """
    column_to_expand = df.columns[-3]                                  # Target column (3rd from the end)
    new_columns = df[column_to_expand].str.split(expand=True)          # Expand tokens into separate columns
    new_columns.columns = [f'{column_to_expand}_{i+1}'                 # Name as <col>_1, <col>_2, ...
                               for i in range(new_columns.shape[1])]
    insertion_position = len(df.columns) - 2                           # Insert before the final two columns
    for col in reversed(new_columns.columns):                          # Insert in reverse to keep order
        df.insert(insertion_position, col, new_columns[col])
    df.drop(columns=[column_to_expand], inplace=True)                  # Remove original combined column
    return df


# ++++++++++++++++++++++++++++++++++++++++++++++++
# Common utilities for cleaning both Table 1
# and Table 2
# ++++++++++++++++++++++++++++++++++++++++++++++++

# _________________________________________________________________________
# Function to drop rows where all values are NaN
def drop_nan_rows(df):
    """Drop any row that is entirely NaN."""
    df = df.dropna(how='all')                                               # Drop rows with all NaN values
    return df

# _________________________________________________________________________
# Function to drop columns where all values are NaN
def drop_nan_columns(df):
    """Drop any column that is entirely NaN."""
    return df.dropna(axis=1, how='all')                                     # Drop columns with all NaN values

# _________________________________________________________________________
# Function to swap first and second rows in both the first and last columns
def swap_first_second_row(df):
    """Swap [row0,row1] in first and last columns to fix misplaced headers."""
    temp = df.iloc[0, 0]                                                    # Temporary store first cell of the first column
    df.iloc[0, 0] = df.iloc[1, 0]                                           # Swap first row of the first column
    df.iloc[1, 0] = temp

    temp = df.iloc[0, -1]                                                   # Temporary store first cell of the last column
    df.iloc[0, -1] = df.iloc[1, -1]                                         # Swap first row of the last column
    df.iloc[1, -1] = temp
    return df

# _________________________________________________________________________
# Function to reset the DataFrame index
def reset_index(df):
    """Reset index to a simple RangeIndex after row drops/reorders."""
    df.reset_index(drop=True, inplace=True)                              # Reset the index of the DataFrame, removing old index
    return df

# _________________________________________________________________________
# Function to remove digit-slash prefixes in first and last two columns
def remove_digit_slash(df):
    """Strip patterns like '12/' at the start of values in [first, penultimate, last] columns."""
    df.iloc[:, [0, -2, -1]] = df.iloc[:, [0, -2, -1]].apply(                # Apply the transformation on the relevant columns
        lambda x: x.str.replace(r'\d+/', '', regex=True)                    # Remove digit-slash patterns
    )
    return df

# _________________________________________________________________________
# Function to split alphanumeric tokens into text and numeric parts (penultimate column)
def separate_text_digits(df):
    """
    If the penultimate column mixes letters and digits, split into:
    - text-only part (moved into the last column when it is NaN)
    - numeric part (kept in the penultimate column; decimal separator harmonized)
    """
    for index, row in df.iterrows():                                                            # Iterate over each row in the DataFrame
        token = str(row.iloc[-2])                                                               # Convert token from the penultimate column to string
        if any(char.isdigit() for char in token) and any(char.isalpha() for char in token):     # Check for mixed content
            if pd.isnull(row.iloc[-1]):                                                         # Only split if target column is empty
                df.loc[index, df.columns[-1]] = ''.join(                                        # Assign letters to the last column
                    filter(lambda x: x.isalpha() or x == ' ', token)
                )
                df.loc[index, df.columns[-2]] = ''.join(                                        # Assign digits to the penultimate column
                    filter(lambda x: not (x.isalpha() or x == ' '), token)
                )

            # Detect decimal separator (',' preferred; fallback '.')
            if ',' in token:
                parts = token.split(',')
            elif '.' in token:
                parts = token.split('.')
            else:
                parts = [token, '']

            cleaned_integer = ''.join(filter(lambda x: x.isdigit() or x == '-', parts[0]))      # Clean the integer part
            cleaned_decimal = ''.join(filter(lambda x: x.isdigit(), parts[1]))                  # Clean the decimal part
            cleaned_numeric = f"{cleaned_integer},{cleaned_decimal}" if cleaned_decimal else cleaned_integer
            df.loc[index, df.columns[-2]] = cleaned_numeric
    return df

# _________________________________________________________________________
# Function to list columns that are 4-digit years
def extract_years(df):
    """Return a list of column names that are pure 4-digit years."""
    year_columns = [col for col in df.columns if re.match(r'\b\d{4}\b', col)]  # Find columns with exactly 4 digits
    return year_columns

# _________________________________________________________________________
# Function to promote first row to header
def first_row_columns(df):
    """Set first row as header and drop it from the data area."""
    df.columns = df.iloc[0]                                      # Set the first row as the header
    df = df.drop(df.index[0])                                    # Drop the first row from the data area
    return df

# _________________________________________________________________________
# Function to clean column names and string values across the DataFrame
def clean_columns_values(df):
    """
    Normalize headers to lowercase ASCII with underscores and replace 'ano'->'year'.
    Convert string numeric commas to dots across textual and numeric columns.
    Lowercase and sanitize the sector label columns.
    """
    df.columns = df.columns.str.lower()                                                             # Convert column headers to lowercase
    df.columns = [
        unicodedata.normalize('NFKD', col).encode('ASCII', 'ignore').decode('utf-8')                # Normalize Unicode characters
        if isinstance(col, str) else col
        for col in df.columns
    ]
    df.columns = df.columns.str.replace(' ', '_').str.replace('ano', 'year').str.replace('-', '_')

    text_columns = df.select_dtypes(include='object').columns                                       # Capture all textual columns
    for col in df.columns:
        df.loc[:, col] = df[col].apply(lambda x: remove_tildes(x) if isinstance(x, str) else x)     # Remove tildes
        df.loc[:, col] = df[col].apply(lambda x: str(x).replace(',', '.') if isinstance(x, (int, float, str)) else x)  # Replace commas with dots

    df.loc[:, 'sectores_economicos'] = df['sectores_economicos'].str.lower()                        # Lowercase the economic sector columns
    df.loc[:, 'economic_sectors']   = df['economic_sectors'].str.lower()
    df.loc[:, 'sectores_economicos'] = df['sectores_economicos'].apply(remove_rare_characters)      # Clean sector columns
    df.loc[:, 'economic_sectors']    = df['economic_sectors'].apply(remove_rare_characters)
    return df

# _________________________________________________________________________
# Function to convert all non-excluded columns to numeric
def convert_float(df):
    """Convert all columns except sector-label columns to numeric (coerce on failure)."""
    excluded_columns   = ['sectores_economicos', 'economic_sectors']                        # Do not convert sector label columns
    columns_to_convert = [col for col in df.columns if col not in excluded_columns]
    df[columns_to_convert] = df[columns_to_convert].apply(pd.to_numeric, errors='coerce')   # Convert to numeric, set errors to NaN
    return df

# _________________________________________________________________________
# Function to move the last column into second position
def relocate_last_column(df):
    """Relocate the last column to position index 1, preserving order of the rest."""
    last_column = df.pop(df.columns[-1])                                        # Remove last column and store it
    df.insert(1, last_column.name, last_column)                                 # Insert it at second position
    return df

# _________________________________________________________________________
# Function to normalize the very first row of the DataFrame
def clean_first_row(df):
    """
    Lowercase, remove tildes and rare characters for first-row cells,
    and translate 'ano'->'year' in-place.
    """
    for col in df.columns:
        if df[col].dtype == 'object':                                               # Only process string columns
            if isinstance(df.at[0, col], str):                                      # Apply transformations only on string values
                df.at[0, col] = df.at[0, col].lower()                               # Lowercase
                df.at[0, col] = remove_tildes(df.at[0, col])                        # Remove tildes
                df.at[0, col] = remove_rare_characters_first_row(df.at[0, col])     # Remove rare characters
                df.at[0, col] = df.at[0, col].replace('ano', 'year')                # Replace 'ano' with 'year'
    return df

# _________________________________________________________________________
# Function to rename month 'set'->'sep' in headings
def replace_set_sep(df):
    """Rename any column containing 'set' to use 'sep' instead."""
    columns = df.columns
    for column in columns:
        if 'set' in column:                                                     # Check if 'set' is in the column name
            new_column = column.replace('set', 'sep')                           # Replace 'set' with 'sep'
            df.rename(columns={column: new_column}, inplace=True)               # Rename the column
    return df

# _________________________________________________________________________
# Function to strip extra spaces in sector label columns
def spaces_se_es(df):
    """Remove surrounding spaces from sector label columns in ES/EN."""
    df['sectores_economicos'] = df['sectores_economicos'].str.strip()        # Remove spaces from 'sectores_economicos'
    df['economic_sectors']    = df['economic_sectors'].str.strip()           # Remove spaces from 'economic_sectors'
    return df

# _________________________________________________________________________
# Function to unify 'services' naming across ES/EN sector labels
def replace_services(df):
    """Replace 'servicios'->'otros servicios' and 'services'->'other services' when both columns contain those tokens."""
    if ('servicios' in df['sectores_economicos'].values) and ('services' in df['economic_sectors'].values):
        df['sectores_economicos'].replace({'servicios': 'otros servicios'}, inplace=True)   # Replace 'servicios' with 'otros servicios'
        df['economic_sectors'].replace({'services': 'other services'}, inplace=True)        # Replace 'services' with 'other services'
    return df

# _________________________________________________________________________
# Function to unify 'mineria' naming in ES sector labels
def replace_mineria(df):
    """
    Replace 'mineria'->'mineria e hidrocarburos' when the latter is otherwise absent.
    This function ensures that the label 'mineria' is standardized to 'mineria e hidrocarburos'
    for consistency in sector labeling in the 'sectores_economicos' column.
    """
    if ('mineria' in df['sectores_economicos'].values) and ('mineria e hidrocarburos' not in df['sectores_economicos'].values):  
        # Check if 'mineria' exists and 'mineria e hidrocarburos' does not exist in the 'sectores_economicos' column
        df['sectores_economicos'].replace({'mineria': 'mineria e hidrocarburos'}, inplace=True)  # Replace 'mineria' with 'mineria e hidrocarburos'
    return df

# _________________________________________________________________________
# Function to unify 'mining and fuels' naming in EN sector labels
def replace_mining(df):
    """
    Replace 'mining and fuels'->'mining and fuel' in EN sector labels.
    This function standardizes the sector name 'mining and fuels' to 'mining and fuel' 
    in the 'economic_sectors' column for consistency.
    """
    if ('mining and fuels' in df['economic_sectors'].values):  
        # Check if 'mining and fuels' exists in the 'economic_sectors' column
        df['economic_sectors'].replace({'mining and fuels': 'mining and fuel'}, inplace=True)  # Replace 'mining and fuels' with 'mining and fuel'
    return df

# _________________________________________________________________________
# Function to round all float64 columns to the given number of decimals
def rounding_values(df, decimals=1):
    """
    Round float64 columns to a specified number of decimal places.
    This function ensures that all columns with the 'float64' dtype are rounded to the given
    number of decimal places (default is 1 decimal place).
    """
    for col in df.columns:  
        if df[col].dtype == 'float64':  
            # Check if the column is of type float64
            df[col] = df[col].round(decimals)  # Round the values in the column to the specified number of decimal places
    return df


# ++++++++++++++++++++++++++++++++++++++++++++++++
# Utilities only for cleaning Table 1 
# ++++++++++++++++++++++++++++++++++++++++++++++++

# _________________________________________________________________________
# Function to convert column names to lowercase and remove accents [exclusive to the OLD dataset]
def clean_column_names(df):
    """
    Converts all column names to lowercase and removes any accents.
    """
    df.columns = df.columns.str.lower()  # Convert all column names to lowercase
    df.columns = [unicodedata.normalize('NFKD', col).encode('ASCII', 'ignore').decode('utf-8') if isinstance(col, str) else col for col in df.columns]  # Normalize and remove accents from string column names
    return df  # Return the modified DataFrame

# _________________________________________________________________________
# Function to adjust column names [exclusive to the OLD dataset]
def adjust_column_names(df):
    """
    Adjust column names if the first and last columns have NaN values and specific conditions are met.
    """
    if pd.isna(df.iloc[0, 0]) and pd.isna(df.iloc[0, -1]):  # Check if the first observation in the first and last columns are NaN
        if "sectores economicos" in df.columns[0] and "economic sectors" in df.columns[-1]:  # Verify column names in the first and last columns
            df.iloc[0, 0] = "sectores economicos"  # Replace NaN in the first column with the correct name
            df.iloc[0, -1] = "economic sectors"  # Replace NaN in the last column with the correct name
    return df  # Return the modified DataFrame

# _________________________________________________________________________
# Function to relocate trailing values when the last column is already filled
def relocate_last_columns(df):
    """
    If the last column's second row is non-null, create a helper column and relocate
    the penultimate header value to the last column header, clearing the original spot.
    """
    if not pd.isna(df.iloc[1, -1]):                                                 # Check if the second row of the last column is non-null
        new_column = 'col_' + ''.join(map(str, np.random.randint(1, 5, size=1)))    # Create a temporary helper column name
        df[new_column] = np.nan                                                     # Add the new column with NaN values

        insert_value_1 = df.iloc[0, -2]                                             # Get value to transfer into the last column header
        insert_value_1 = str(insert_value_1)                                        # Ensure the value is a string
        df.iloc[:, -1] = df.iloc[:, -1].astype('object')                            # Ensure the last column is treated as a string
        df.iloc[0, -1] = insert_value_1                                             # Set the penultimate header value in the last column header

        df.iloc[0, -2] = np.nan                                                     # Clear the original spot in the penultimate column header
    return df

# _________________________________________________________________________
# Function to build composite month headers from the first row and year columns
def get_months_sublist_list(df, year_columns):
    """
    Parse the first row to collect month tokens and compose headers as <year>_<month>.
    Preserve the first two original elements if they are not present in the new header list.
    """
    first_row = df.iloc[0]                                                      # Get the first row for processing
    months_sublist_list = []                                                    # Initialize list for months
    months_sublist = []                                                         # Temporary list for a month group

    for item in first_row:
        if len(str(item)) == 3:                                                 # Likely month abbreviations (e.g., 'jan')
            months_sublist.append(item)
        elif '-' in item or str(item) == 'year':                                # Boundary markers such as 'year' or 'year-month'
            months_sublist.append(item)
            months_sublist_list.append(months_sublist)                          # Add completed month group
            months_sublist = []                                                 # Reset for next group

    if months_sublist:                                                          # Append the last group if not empty
        months_sublist_list.append(months_sublist)

    new_elements = []                                                           # Initialize list for new header elements
    if year_columns:                                                            # Only process if year columns are provided
        for i, year in enumerate(year_columns):
            if i < len(months_sublist_list):
                for element in months_sublist_list[i]:
                    new_elements.append(f"{year}_{element}")                    # Combine year and month into one string

    two_first_elements = df.iloc[0][:2].tolist()                                # Safeguard first two elements
    for index in range(len(two_first_elements) - 1, -1, -1):
        if two_first_elements[index] not in new_elements:                       # Ensure the first two elements are added if not present
            new_elements.insert(0, two_first_elements[index])

    while len(new_elements) < len(df.columns):                                  # Fill remaining spots with None if necessary
        new_elements.append(None)

    temp_df = pd.DataFrame([new_elements], columns=df.columns)                  # Create temporary DataFrame for alignment
    df.iloc[0] = temp_df.iloc[0]                                                # Assign the new header to the DataFrame
    return df

# _________________________________________________________________________
# Function to infer/correct a year header based on the position of 'year' token
def find_year_column(df):
    """
    Detect 4-digit year columns; if a single year is present and a 'year' token appears
    in a different column header position, rename that token to the adjacent year (±1).
    """
    found_years = []                                                                        # List to store detected years

    for column in df.columns:
        if column.isdigit() and len(column) == 4:                                           # Check for columns with a 4-digit year
            found_years.append(column)

    if len(found_years) > 1:                                                                # If multiple years are found, do nothing
        pass
    elif len(found_years) == 1:                                                             # If one year is found, proceed to fix year-related header
        year_name = found_years[0]                                                          # Extract the detected year
        first_row = df.iloc[0]

        column_contains_year = first_row[first_row.astype(str).str.contains(r'\byear\b')]   # Find 'year' token in header

        if not column_contains_year.empty:
            column_contains_year_name = column_contains_year.index[0]                       # Get the column name containing 'year'
            column_contains_year_index = df.columns.get_loc(column_contains_year_name)
            year_name_index = df.columns.get_loc(year_name)

            if column_contains_year_index < year_name_index:
                new_year = str(int(year_name) - 1)                                          # If 'year' is to the left, assign previous year
                df.rename(columns={column_contains_year_name: new_year}, inplace=True)
            elif column_contains_year_index > year_name_index:
                new_year = str(int(year_name) + 1)                                          # If 'year' is to the right, assign next year
                df.rename(columns={column_contains_year_name: new_year}, inplace=True)
            else:
                pass
        else:
            pass
    else:
        pass

    return df

# _________________________________________________________________________
# Function to swap values between the last two columns when NaNs appear at the end
def exchange_values(df):
    """
    If the last column has NaNs, swap those cells with the corresponding penultimate
    column values (row-wise) to restore completeness.
    """
    if len(df.columns) < 2:                                                                 # Ensure DataFrame has more than one column
        print("The DataFrame has less than two columns. Values cannot be exchanged.")
        return df

    if df.iloc[:, -1].isnull().any():                                                       # Check if the last column contains NaNs
        last_column_rows_nan = df[df.iloc[:, -1].isnull()].index

        for idx in last_column_rows_nan:                                                    # Iterate over rows with NaNs
            if -2 >= -len(df.columns):                                                      # Ensure the penultimate column exists
                df.iloc[idx, -1], df.iloc[idx, -2] = df.iloc[idx, -2], df.iloc[idx, -1]     # Swap values between last and penultimate columns

    return df

# _________________________________________________________________________
# Function to standardize "Var. %" tokens in the first column (ES)
def replace_var_perc_first_column(df):
    """Replace 'Var.%' variants with 'variacion porcentual' in the first column."""
    regex = re.compile(r'Var\. ?%')                                                 # Regular expression to match 'Var. %'

    for index, row in df.iterrows():                                                # Iterate through the rows of the DataFrame
        value = str(row.iloc[0])                                                    # Get the first column value as string
        if regex.search(value):                                                     # If 'Var. %' is found, replace it
            df.at[index, df.columns[0]] = regex.sub("variacion porcentual", value)  # Replace with 'variacion porcentual'
    return df

# _________________________________________________________________________
# Function to normalize moving-average descriptors in the last column
number_moving_average = 'three'  # Keep a space at the end
def replace_number_moving_average(df):
    """
    Replace patterns like '2 -' at the start of tokens in the last column with a
    normalized text (e.g., 'three-'), using the global `number_moving_average`.
    """
    for index, row in df.iterrows():                                                    # Iterate over rows in the DataFrame
        if pd.notnull(row.iloc[-1]) and re.search(r'(\d\s*-)', str(row.iloc[-1])):      # Check for numeric pattern
            df.at[index, df.columns[-1]] = re.sub(
                r'(\d\s*-)', f'{number_moving_average}-', str(row.iloc[-1])             # Replace numeric pattern with the moving average descriptor
            )
    return df

# _________________________________________________________________________
# Function to standardize "Var. %" tokens in the last two columns (EN)
def replace_var_perc_last_columns(df):
    """Replace 'Var.%' variants with 'percent change' in the last two columns."""
    regex = re.compile(r'(Var\. ?%)(.*)')                                           # Regular expression to match 'Var. %'

    for index, row in df.iterrows():                                                # Iterate over the rows of the DataFrame
        if isinstance(row.iloc[-2], str) and regex.search(row.iloc[-2]):            # Check if the penultimate column contains 'Var. %'
            replaced_text = regex.sub(r'\2 percent change', row.iloc[-2])           # Replace with 'percent change'
            df.at[index, df.columns[-2]] = replaced_text.strip()

        if isinstance(row.iloc[-1], str) and regex.search(row.iloc[-1]):            # Check if the last column contains 'Var. %'
            replaced_text = regex.sub(r'\2 percent change', row.iloc[-1])           # Replace with 'percent change'
            df.at[index, df.columns[-1]] = replaced_text.strip()
    return df

# _________________________________________________________________________
# Function to change the first dot in row 2 into a hyphen pattern across columns
def replace_first_dot(df):
    """If a cell on the second row matches 'Word.Word', replace the first dot with a hyphen."""
    second_row = df.iloc[1]                                                                         # Get the second row for processing

    if any(isinstance(cell, str) and re.match(r'^\w+\.\s?\w+', cell) for cell in second_row):       # Check if any cell matches 'Word.Word'
        for col in df.columns:                                                                      # Iterate over all columns
            if isinstance(second_row[col], str):                                                    # Ensure the cell is a string
                if re.match(r'^\w+\.\s?\w+', second_row[col]):                                      # Check for 'Word.Word' pattern
                    df.at[1, col] = re.sub(r'(\w+)\.(\s?\w+)', r'\1-\2', second_row[col], count=1)  # Replace first dot with hyphen
    return df

# _________________________________________________________________________
# Function to drop rows containing the '}' character anywhere
def drop_rare_caracter_row(df):
    """Remove any row where the '}' character appears in any cell."""
    rare_caracter_row = df.apply(lambda row: '}' in row.values, axis=1)         # Find rows containing '}' character
    df = df[~rare_caracter_row]                                                 # Remove rows with rare character
    return df

# _________________________________________________________________________
# Function to split a column into two if second-row pattern 'Word.Word' is found
def split_column_by_pattern(df):
    """
    For columns whose second row matches 'Title.Title', split the column by whitespace
    and insert the second token into a new '<col>_split' column immediately to the right.
    """
    for col in df.columns:
        if re.match(r'^[A-Z][a-z]+\.?\s[A-Z][a-z]+\.?$', str(df.iloc[1][col])):     # Check for 'Word.Word' pattern in second row
            split_values = df[col].str.split(expand=True)                           # Split the column by whitespace
            df[col] = split_values[0]                                               # Assign first token to original column
            new_col_name = col + '_split'                                           # Create new column name
            df.insert(df.columns.get_loc(col) + 1, new_col_name, split_values[1])   # Insert second token in new column
    return df


# ++++++++++++++++++++++++++++++++++++++++++++++++
# Utilities for cleaning WR-specified issues in
# Table 1
# ++++++++++++++++++++++++++++++++++++++++++++++++
# WR below are examples where issues are fixed by functions. They are not unique cases,
# but examples where issues occur and the fix is encoded for reuse.


# 𝑛𝑠_2014_07
#...............................................................................................................................

# _________________________________________________________________________
# Function to swap NaN and 'SECTORES ECONÓMICOS' in the first row, then drop the empty column
def swap_nan_se(df):
    """Place 'SECTORES ECONÓMICOS' in the first column header when it drifted to the second."""
    if pd.isna(df.iloc[0, 0]) and df.iloc[0, 1] == "SECTORES ECONÓMICOS":       # Check if 'SECTORES ECONÓMICOS' is in the second column and the first column is NaN
        column_1_value = df.iloc[0, 1]                                          # Store the value of 'SECTORES ECONÓMICOS' from the second column
        df.iloc[0, 0] = column_1_value                                          # Place it in the first column
        df.iloc[0, 1] = np.nan                                                  # Set the second column to NaN
        df = df.drop(df.columns[1], axis=1)                                     # Drop the now empty second column
    return df


# 𝑛𝑠_2014_08
#...............................................................................................................................

# _________________________________________________________________________
# Function to replace NaNs in the first row with synthetic names, then promote as headers
def replace_first_row_with_columns(df):
    """
    If the first row contains 4-digit year tokens, fill NaN cells with 'column_<idx>'
    and promote the first row to be the header.
    """
    if any(isinstance(element, str) and element.isdigit() and len(element) == 4 for element in df.iloc[0]):     # Check for 4-digit year tokens in the first row
        for col_index, value in enumerate(df.iloc[0]):                                                          # Iterate through the first row
            if pd.isna(value):                                                                                  # If a value is NaN
                df.iloc[0, col_index] = f"column_{col_index + 1}"                                               # Replace with synthetic column name like 'column_1'
        df.columns = df.iloc[0]                                                                                 # Promote the first row as the new header
        df = df.drop(df.index[0])                                                                               # Drop the first row since it's now the header
    return df

# _________________________________________________________________________
# Function to expand a column by fixing 'a-b' into 'a b' and splitting trailing text out
def expand_column(df):
    """
    For the penultimate column, normalize 'word-word' to 'word word', move trailing text to
    the last column (row-wise), and keep only numeric/textual parts separated.
    """
    column_to_expand = df.columns[-2]                                                                               # Identify the penultimate column
    
    def replace_hyphens(match_obj):
        return match_obj.group(1) + ' ' + match_obj.group(2)                                                        # Replace hyphen between words with a space

    if df[column_to_expand].str.contains(r'\d').any() and df[column_to_expand].str.contains(r'[a-zA-Z]').any():     # Check for mixed numeric and textual data
        df[column_to_expand] = df[column_to_expand].apply(
            lambda x: re.sub(r'([a-zA-Z]+)\s*-\s*([a-zA-Z]+)', replace_hyphens, str(x)) if pd.notnull(x) else x     # Apply regex to normalize hyphen
        )
        
        pattern = re.compile(r'[a-zA-Z\s]+$')                                                                       # Regex to match trailing text (letters or spaces)

        def extract_replace(row):
            if pd.notnull(row[column_to_expand]) and isinstance(row[column_to_expand], str):                        # If there's a valid string to process
                if row.name != 0:                                                                                   # Ensure this isn't the first row
                    value_to_replace = pattern.search(row[column_to_expand])                                        # Search for trailing text
                    if value_to_replace:
                        value_to_replace = value_to_replace.group().strip()                                         # Clean the extracted value
                        row[df.columns[-1]] = value_to_replace                                                      # Place the extracted value in the last column
                        row[column_to_expand] = re.sub(pattern, '', row[column_to_expand]).strip()                  # Remove the trailing text from the original column
            return row

        df = df.apply(extract_replace, axis=1)                                                                      # Apply the changes row-wise

    return df

# _________________________________________________________________________
# Function to split penultimate column into multiple columns (whitespace)
def split_values_1(df):
    """Split the penultimate column by whitespace and insert the parts before the last column."""
    column_to_expand = df.columns[-2]                                                           # Identify the penultimate column
    new_columns = df[column_to_expand].str.split(expand=True)                                   # Split the values in the column by whitespace
    new_columns.columns = [f'{column_to_expand}_{i+1}' for i in range(new_columns.shape[1])]    # Rename new columns with an index suffix
    insertion_position = len(df.columns) - 1                                                    # Determine the position before the last column
    for col in reversed(new_columns.columns):                                                   # Insert each new column in reverse order to avoid overwriting
        df.insert(insertion_position, col, new_columns[col])
    df.drop(columns=[column_to_expand], inplace=True)                                           # Drop the original penultimate column
    return df


# 𝑛𝑠_2015_11
#...............................................................................................................................

# _________________________________________________________________________
# Function to re-label year-pair headers and backfill single-year cells in row 0
def check_first_row(df):
    """
    Detect cells like '2018 2019' in row 0; rename such columns with synthetic names and backfill
    year tokens into the first two columns if missing.
    """
    first_row = df.iloc[0]                                                      # Get the first row for processing
    
    for i, (col, value) in enumerate(first_row.items()):                        # Iterate through the columns in the first row
        if re.search(r'\b\d{4}\s\d{4}\b', str(value)):                          # Detect year pair patterns (e.g., '2018 2019')
            years = value.split()                                               # Split the value into two separate years
            first_year  = years[0]                                              # First year
            second_year = years[1]                                              # Second year
            
            original_column_name = f'col_{i}'                                   # Create synthetic column name for this entry
            df.at[0, col] = original_column_name                                # Replace the header with the synthetic name
            
            if pd.isna(df.iloc[0, 0]):                                          # If the first header cell is NaN
                df.iloc[0, 0] = first_year                                      # Fill the first header cell with the first year
            
            if pd.isna(df.iloc[0, 1]):                                          # If the second header cell is NaN
                df.iloc[0, 1] = second_year                                     # Fill the second header cell with the second year
    
    return df

# _________________________________________________________________________
# Function to swap values between adjacent columns when right column has NaNs
def replace_nan_with_previous_column_3(df):
    """
    For any pair of adjacent columns where the right column has NaNs and does not end with '_year',
    swap values with the left column that ends with '_year' and is fully non-null.
    """
    columns = df.columns                                                                                                    # Get all the columns in the DataFrame
    
    for i in range(len(columns) - 1):                                                                                       # Iterate over the columns, except the last one
        if i != len(columns) - 1 and (columns[i].endswith('_year') and not df[columns[i]].isnull().any()):                  # Check if the current column ends with '_year' and has no NaNs
            if df[columns[i+1]].isnull().any() and not columns[i+1].endswith('_year'):                                      # Check if the next column has NaNs and is not a year column
                nan_indices = df[columns[i+1]].isnull()                                                                     # Get the indices with NaNs in the next column
                df.loc[nan_indices, [columns[i], columns[i+1]]] = df.loc[nan_indices, [columns[i+1], columns[i]]].values    # Swap the values
    return df


# 𝑛𝑠_2016_15
#...............................................................................................................................

# _________________________________________________________________________
# Function to fill first-row year tokens from trailing columns when leading cells are NaN
def check_first_row_1(df):
    """
    If first or second header cells are NaN and the trailing cells contain 4-digit years,
    move those years forward and clear their original positions.
    """
    if pd.isnull(df.iloc[0, 0]):                                                                                    # Check if the first cell in the header is NaN
        penultimate_column = df.iloc[0, -2]                                                                         # Get the penultimate column value
        if isinstance(penultimate_column, str) and len(penultimate_column) == 4 and penultimate_column.isdigit():   # If it's a 4-digit year
            df.iloc[0, 0] = penultimate_column                                                                      # Move it to the first header cell
            df.iloc[0, -2] = np.nan                                                                                 # Clear the penultimate column header
    
    if pd.isnull(df.iloc[0, 1]):                                                                                    # Check if the second cell in the header is NaN
        last_column = df.iloc[0, -1]                                                                                # Get the last column value
        if isinstance(last_column, str) and len(last_column) == 4 and last_column.isdigit():                        # If it's a 4-digit year
            df.iloc[0, 1] = last_column                                                                             # Move it to the second header cell
            df.iloc[0, -1] = np.nan                                                                                 # Clear the last column header
    
    return df

# _________________________________________________________________________
# Function to split the 4th-from-last column into multiple columns (whitespace)
def split_values_2(df):
    """Split the fourth-from-last column and insert new parts before the last three columns."""
    column_to_expand = df.columns[-4]                                                           # Identify the fourth-from-last column
    new_columns = df[column_to_expand].str.split(expand=True)                                   # Split the values in the column by whitespace
    new_columns.columns = [f'{column_to_expand}_{i+1}' for i in range(new_columns.shape[1])]    # Rename new columns with an index suffix
    insertion_position = len(df.columns) - 3                                                    # Determine the position before the last three columns
    for col in reversed(new_columns.columns):                                                   # Insert each new column in reverse order to avoid overwriting
        df.insert(insertion_position, col, new_columns[col])
    df.drop(columns=[column_to_expand], inplace=True)                                           # Drop the original column
    return df


# 𝑛𝑠_2016_19
#...............................................................................................................................

# _________________________________________________________________________
# Function to split the third-from-last column into multiple columns (whitespace)
def split_values_3(df):
    """Split the third-from-last column and insert new parts before the last two columns."""
    column_to_expand = df.columns[-3]                                                           # Identify the third-from-last column
    new_columns = df[column_to_expand].str.split(expand=True)                                   # Split the values in the column by whitespace
    new_columns.columns = [f'{column_to_expand}_{i+1}' for i in range(new_columns.shape[1])]    # Rename new columns with an index suffix
    insertion_position = len(df.columns) - 2                                                    # Determine the position before the last two columns
    for col in reversed(new_columns.columns):                                                   # Insert each new column in reverse order to avoid overwriting
        df.insert(insertion_position, col, new_columns[col])
    df.drop(columns=[column_to_expand], inplace=True)                                           # Drop the original column
    return df

# _________________________________________________________________________
# Function to swap with previous column when the right column has NaNs (variant 1)
def replace_nan_with_previous_column_1(df):
    """Swap values with the previous column if the right one contains NaNs and is not a '_year' column."""
    columns = df.columns                                                                                                    # Get all columns in the DataFrame
    
    for i in range(len(columns) - 1):                                                                                       # Iterate over the columns, except the last one
        if i != len(columns) - 2 and not (columns[i].endswith('_year') and df[columns[i]].isnull().any()):                  # Check for '_year' columns with no NaNs
            if df[columns[i+1]].isnull().any() and not columns[i+1].endswith('_year'):                                      # Check if the next column has NaNs and is not a year column
                nan_indices = df[columns[i+1]].isnull()                                                                     # Get the indices of NaN values in the next column
                df.loc[nan_indices, [columns[i], columns[i+1]]] = df.loc[nan_indices, [columns[i+1], columns[i]]].values    # Swap the values
    return df

# _________________________________________________________________________
# Function to swap with previous column when the right column has NaNs (variant 2)
def replace_nan_with_previous_column_2(df):
    """Same as variant 1; included for WR-specific patterns that require a second pass."""
    columns = df.columns                                                                                                    # Get all columns in the DataFrame
    
    for i in range(len(columns) - 1):                                                                                       # Iterate over the columns, except the last one
        if i != len(columns) - 2 and not (columns[i].endswith('_year') and df[columns[i]].isnull().any()):                  # Check for '_year' columns with no NaNs
            if df[columns[i+1]].isnull().any() and not columns[i+1].endswith('_year'):                                      # Check if the next column has NaNs and is not a year column
                nan_indices = df[columns[i+1]].isnull()                                                                     # Get the indices of NaN values in the next column
                df.loc[nan_indices, [columns[i], columns[i+1]]] = df.loc[nan_indices, [columns[i+1], columns[i]]].values    # Swap the values
    return df


# ++++++++++++++++++++++++++++++++++++++++++++++++
# Utilities only for cleaning Table 2
# ++++++++++++++++++++++++++++++++++++++++++++++++

# _________________________________________________________________________
# Function to replace 'TOTAL' with 'YEAR' in the first row [exclusive to the OLD dataset]
def replace_total_with_year(df):
    """
    If the first row contains 'TOTAL', replace it with 'YEAR'.
    """
    df.iloc[0] = df.iloc[0].apply(lambda x: 'YEAR' if "TOTAL" in str(x) else x)  # Apply lambda function to replace 'TOTAL' with 'YEAR' in the first row
    return df  # Return the modified DataFrame

# _________________________________________________________________________
# Function to split two space-separated years in the penultimate column into two columns
def separate_years(df):
    """
    If the penultimate header cell contains 'YYYY YYYY', keep the first in place and
    insert the second as a new column immediately before the last column.
    """
    df = df.copy()                                                                                  # Create a copy to avoid modifying the original DataFrame
    if isinstance(df.iloc[0, -2], str) and len(df.iloc[0, -2].split()) == 2:                        # Check if the penultimate column has two space-separated years
        years = df.iloc[0, -2].split()                                                              # Split the string into two parts
        if all(len(year) == 4 for year in years):                                                   # Ensure both parts are 4-digit years
            second_year = years[1]                                                                  # Get the second year
            df.iloc[0, -2] = years[0]                                                               # Assign the first year back to the penultimate column
            df.insert(len(df.columns) - 1, 'new_column', [second_year] + [None] * (len(df) - 1))    # Insert the second year as a new column
    return df

# _________________________________________________________________________
# Function to move Roman numerals from the last cell in row 2 into a new column
def relocate_roman_numerals(df):
    """
    Detect Roman numerals in the third row's last column, strip them from that cell,
    move them into 'new_column', and set the original cell to NaN.
    """
    roman_numerals = find_roman_numerals(df.iloc[2, -1])                        # Find Roman numerals in the last cell of row 2
    if roman_numerals:
        original_text = df.iloc[2, -1]                                          # Store the original content
        for roman_numeral in roman_numerals:                                    # For each Roman numeral found
            original_text = original_text.replace(roman_numeral, '').strip()    # Remove Roman numerals from the text
        df.iloc[2, -1] = original_text                                          # Update the original cell with the cleaned text
        df.at[2, 'new_column'] = ', '.join(roman_numerals)                      # Move the Roman numerals to a new column
        df.iloc[2, -1] = np.nan                                                 # Set the original cell to NaN
    return df

# _________________________________________________________________________
# Function to extract '<number,text>' pairs from third-last column into the second-last column
def extract_mixed_values(df):
    """
    If the third-from-last column contains patterns like '-1,2 text', move that token into
    the penultimate column (when empty) and clean the source cell.
    """
    df = df.copy()                                                                      # Create a copy to avoid modifying the original DataFrame
    regex_pattern = r'(-?\d+,\d [a-zA-Z\s]+)'                                           # Define the regex pattern to capture the mixed numeric-textual tokens
    for index, row in df.iterrows():                                                    # Iterate over each row in the DataFrame
        third_last_obs  = row.iloc[-3]                                                  # Get the value from the third-to-last column
        second_last_obs = row.iloc[-2]                                                  # Get the value from the second-to-last column

        if isinstance(third_last_obs, str) and pd.notnull(third_last_obs):              # If the value is a valid string and not NaN
            match = re.search(regex_pattern, third_last_obs)                            # Try to match the regex pattern
            if match:
                extracted_part = match.group(0)                                         # Extract the matched portion
                if pd.isna(second_last_obs) or pd.isnull(second_last_obs):              # If the second-to-last column is empty
                    df.iloc[index, -2] = extracted_part                                 # Place the extracted value in the second-to-last column
                    third_last_obs = re.sub(regex_pattern, '', third_last_obs).strip()  # Remove the extracted part from the original cell
                    df.iloc[index, -3] = third_last_obs                                 # Update the third-to-last column with the cleaned text
    return df

# _________________________________________________________________________
# Function to fill NaN values in the first row with their column names
def replace_first_row_nan(df):
    """Replace NaNs in the first row with the corresponding column name."""
    for col in df.columns:                              # Iterate over the columns
        if pd.isna(df.iloc[0][col]):                    # Check if the cell in the first row is NaN
            df.iloc[0, df.columns.get_loc(col)] = col   # Replace NaN with the column name
    return df

# _________________________________________________________________________
# Function to convert Roman numerals in the first row to Arabic numerals
def roman_arabic(df):
    """Convert any Roman numeral tokens in the first row into Arabic numerals."""
    first_row = df.iloc[0]                                              # Get the first row
    
    def convert_roman_number(number):
        try:
            return str(roman.fromRoman(number))                         # Convert Roman numeral to Arabic
        except roman.InvalidRomanNumeralError:                          # Handle invalid Roman numerals
            return number                                               # Return the original value if conversion fails

    converted_first_row = []                                            # Prepare an empty list to store the converted values
    for value in first_row:                                             # Iterate over the values in the first row
        if isinstance(value, str) and not pd.isna(value):               # If the value is a string and not NaN
            converted_first_row.append(convert_roman_number(value))     # Convert Roman numeral to Arabic
        else:
            converted_first_row.append(value)                           # If not a Roman numeral, keep the original value

    df.iloc[0] = converted_first_row                                    # Update the first row with the converted values
    return df

# _________________________________________________________________________
# Function to fix duplicate numeric headers in the first row by incrementing duplicates
def fix_duplicates(df):
    """
    Ensure strictly increasing numeric tokens across the first row when duplicates appear.
    Subsequent duplicates are incremented in sequence.
    """
    second_row = df.iloc[0].copy()                                          # Copy the first row to avoid modifying the original DataFrame
    prev_num = None                                                         # Initialize previous number tracker
    first_one_index = None                                                  # Track the first occurrence of the number 1

    for i, num in enumerate(second_row):                                    # Iterate through the first row
        try:
            num = int(num)                                                  # Try to convert the value to an integer
            prev_num = int(prev_num) if prev_num is not None else None      # If a previous number exists, cast to integer

            if num == prev_num:                                             # If the current number is equal to the previous one
                if num == 1:                                                # If the number is 1, handle as a special case
                    if first_one_index is None:
                        first_one_index = i - 1                             # Track the first occurrence of 1
                    next_num = int(second_row[i - 1]) + 1                   # Increment the previous value
                    for j in range(i, len(second_row)):                     # Iterate over the remaining values
                        if str(second_row.iloc[j]).isdigit():               # If the value is a digit
                            second_row.iloc[j] = str(next_num)              # Assign the incremented value
                            next_num += 1                                   # Increment for the next duplicate
                elif i - 1 >= 0:                                            # If not the first index, increment the previous value
                    second_row.iloc[i] = str(int(second_row.iloc[i - 1]) + 1)

            prev_num = num                                                  # Update the previous number tracker
        except ValueError:
            pass                                                            # Skip non-numeric values

    df.iloc[0] = second_row                                                 # Update the first row with the new values
    return df

# _________________________________________________________________________
# Function to build composite quarter headers per year: <year>_<q>
def get_quarters_sublist_list(df, year_columns):
    """
    Parse the first row to collect single-char quarter labels and compose headers as <year>_<q>.
    Preserve the first two original elements if they are not present in the result.
    """
    first_row = df.iloc[0]                                      # Get the first row to extract quarter labels
    quarters_sublist_list = []                                  # List of quarter groups per year
    quarters_sublist = []                                       # Temporary list to hold quarters for the current year

    for item in first_row:                                      # Iterate over each element in the first row
        if len(str(item)) == 1:                                 # Single-character quarter label (e.g., '1', '2', '3', '4')
            quarters_sublist.append(item)
        elif str(item) == 'year':                               # Marker for year columns
            quarters_sublist.append(item)                       # Add the 'year' token to the current sublist
            quarters_sublist_list.append(quarters_sublist)      # Add the current sublist to the list of sublists
            quarters_sublist = []                               # Reset for the next year

    if quarters_sublist:                                        # If there are any remaining quarters, add them to the list
        quarters_sublist_list.append(quarters_sublist)

    new_elements = []                                           # List to hold the new column names
    if year_columns:                                            # If there are year columns provided
        for i, year in enumerate(year_columns):                 # Iterate through the year columns
            if i < len(quarters_sublist_list):                  # If there's a corresponding quarter list
                for element in quarters_sublist_list[i]:        # Append the year-quarter combinations
                    new_elements.append(f"{year}_{element}")

    two_first_elements = df.iloc[0][:2].tolist()                # Preserve the first two elements from the original header
    for index in range(len(two_first_elements) - 1, -1, -1):    # Ensure the first two elements are included in the new headers
        if two_first_elements[index] not in new_elements:
            new_elements.insert(0, two_first_elements[index])

    while len(new_elements) < len(df.columns):                  # Pad with None values to match the number of columns
        new_elements.append(None)

    temp_df = pd.DataFrame([new_elements], columns=df.columns)  # Create a temporary DataFrame with the new headers
    df.iloc[0] = temp_df.iloc[0]                                # Update the original DataFrame with the new headers
    return df


# ++++++++++++++++++++++++++++++++++++++++++++++++
# Utilities for cleaning WR-specified issues in
# Table 2
# ++++++++++++++++++++++++++++++++++++++++++++++++
# WR below are examples where issues are fixed by functions. They are not unique cases,
# but examples where issues occur and the fix is encoded for reuse.


# 𝑛𝑠_2016_20
#...............................................................................................................................

# _________________________________________________________________________
# Function to drop the first row if it is entirely NaN
def drop_nan_row(df):
    """If the first row is all NaN, drop it and reset the index."""
    if df.iloc[0].isnull().all():                   # Check if the first row has all NaN values
        df = df.drop(index=0)                       # Drop the first row
        df.reset_index(drop=True, inplace=True)     # Reset the index after dropping the row
    return df


# 𝑛𝑠_2019_17
#...............................................................................................................................

# _________________________________________________________________________
# Function to move header content when the last column begins with 'ECONOMIC SECTORS'
def last_column_es(df):
    """
    If the last column header is 'ECONOMIC SECTORS' and the second row in that column is non-null,
    insert a new helper column and relocate the adjacent header value into the last column header.
    """
    if df[df.columns[-1]].iloc[0] == 'ECONOMIC SECTORS':        # Check if the last column header is 'ECONOMIC SECTORS'
        if pd.notnull(df[df.columns[-1]].iloc[1]):              # Ensure that the second row in the column is not NaN
            new_column_name = f"col_{len(df.columns)}"          # Name the new helper column based on the current number of columns
            df[new_column_name] = np.nan                        # Add the new column with NaN values

            insert_value = df.iloc[0, -2]                       # Get the value from the penultimate column header to be moved
            insert_value = str(insert_value)
            df.iloc[:, -1] = df.iloc[:, -1].astype('object')    # Ensure the last column can hold string values
            df.iloc[0, -1] = insert_value                       # Move the penultimate column header to the last column header

            df.iloc[0, -2] = np.nan                             # Clear the original position of the penultimate header
    return df


# 𝑛𝑠_2019_26
#...............................................................................................................................

# _________________________________________________________________________
# Function to swap two column names when a NaN-only year column is adjacent to a non-year column
def exchange_columns(df):
    """
    Find a year-like column (4 digits) that is fully NaN and swap its name with the immediate
    left neighbor if that neighbor is not year-like.
    """
    nan_column = None                                                                                   # Initialize variable to store the column with NaN values
    for column in df.columns:                                                                           # Iterate through each column to find a fully NaN year column
        if df[column].isnull().all() and len(column) == 4 and column.isdigit():                         # Check for a 4-digit column with all NaN values
            nan_column = column  # Store the column name
            break

    if nan_column:  # If a NaN column is found
        column_index = df.columns.get_loc(nan_column)                                                   # Get the index of the NaN column
        if column_index > 0:                                                                            # Ensure there is a left neighbor
            left_column = df.columns[column_index - 1]                                                  # Get the left neighbor column
            if not (len(left_column) == 4 and left_column.isdigit()):                                   # If the left column is not a year column
                df.rename(columns={nan_column: left_column, left_column: nan_column}, inplace=True)     # Swap the columns' names
    return df


# 𝑛𝑠_2019_29
#...............................................................................................................................

# _________________________________________________________________________
# Function to swap values when a Roman numeral or 'AÑO' appears next to an empty cell in row 2
def exchange_roman_nan(df):
    """
    For each cell in row 2, if it is 'AÑO' or a valid Roman numeral and the next cell is NaN,
    swap those two row-2 values when the column below is empty (except the header row).
    """
    for col_idx, value in enumerate(df.iloc[1]):                                                                        # Iterate through each value in row 2
        if isinstance(value, str):                                                                                      # Check if the value is a string
            if value.upper() == 'AÑO' or (value.isalpha() and roman.fromRoman(value.upper())):                          # Check for 'AÑO' or Roman numeral
                next_col_idx = col_idx + 1                                                                              # Get the index of the next column
                if next_col_idx < len(df.columns) and pd.isna(df.iloc[1, next_col_idx]):                                # Ensure the next cell is NaN
                    current_col = df.iloc[:, col_idx].drop(index=1)                                                     # Exclude header row from emptiness check
                    next_col = df.iloc[:, next_col_idx].drop(index=1)                                                   # Exclude header row from emptiness check
                    if current_col.isna().all():                                                                        # If the current column is all NaN
                        df.iloc[1, col_idx], df.iloc[1, next_col_idx] = df.iloc[1, next_col_idx], df.iloc[1, col_idx]   # Swap the values
    return df


# ==============================================================================================
# SECTION 3.2 Building friendly pipelines for running cleaners and RTD transformers
# ==============================================================================================
# In this section we define utility functions that support the processing of OLD and NEW
# WR-derived data. These pipelines run previous tools for extracting metadata from filenames,
# reading/writing records, extracting tables from CSV and PDFs, and saving cleaned data to disk in
# appropriate formats.


# ++++++++++++++++++++++++++++++++++++++++++++++++
# Libraries
# ++++++++++++++++++++++++++++++++++++++++++++++++

# import os                                                                 # [already imported and documented in section 1]
# import re                                                                 # [already imported and documented in section 1]
# import time                                                               # [already imported and documented in section 1]
# import pandas as pd                                                       # [already imported and documented in section 3.1]
# from tqdm.notebook import tqdm                                            # [already imported and documented in section 2]
import hashlib                                                              # SHA-256/MD5 hashing for file fingerprints & integrity checks
import tabula                                                               # tabula-py: Java-backed PDF table extraction via Tabula


# ++++++++++++++++++++++++++++++++++++++++++++++++
# Utility functions for handling OLD and NEW WR
# file metadata, records, and table extraction
# ++++++++++++++++++++++++++++++++++++++++++++++++

# _________________________________________________________________________
# Function to extract issue and year from filenames like 'ns-07-2017.pdf'
def parse_ns_meta(file_name: str) -> tuple[str | None, str | None]:
    """
    Extract issue number and year from WR-style filenames of the form 'ns-xx-yyyy.*'.

    Args:
        file_name (str): Path or name of the WR file (OLD CSV or NEW PDF).

    Returns:
        tuple[str | None, str | None]: (issue, year) extracted from the filename,
        or (None, None) when the filename does not match the WR pattern.
    """
    m = re.search(
        r"ns-(\d{1,2})-(\d{4})",
        os.path.basename(file_name).lower()
    )                                                                           # Capture issue (1–2 digits) and year (4 digits)
    return (m.group(1), m.group(2)) if m else (None, None)                      # Return extracted tokens or (None, None) if no match

# _________________________________________________________________________
# Function to generate sorting key based on (year, issue) for stable file ordering
def _ns_sort_key(s: str) -> tuple[int, int, str]:
    """
    Build a sorting key for WR filenames ('ns-xx-yyyy.*') so that both OLD and NEW
    files are ordered chronologically by year and issue number.

    Args:
        s (str): Full path or basename of a WR file.

    Returns:
        tuple[int, int, str]: (year, issue, basename) used for stable ordering.
    """
    base = os.path.splitext(os.path.basename(s))[0]                            # Remove extension and keep basename
    m    = re.search(r"ns-(\d{1,2})-(\d{4})", base, re.I)                      # Look for 'ns-<issue>-<year>' pattern
    if not m:
        return (9999, 9999, base)                                              # Non-matching files are sent to the end
    issue, year = int(m.group(1)), int(m.group(2))                             # Cast to integers for numeric sorting
    return (year, issue, base)                                                 # Sort primarily by year, then by issue

# _________________________________________________________________________
# Function to read existing records from a file and return them as a sorted list
def _read_records(record_folder: str, record_txt: str) -> list[str]:
    """
    Load previously processed WR filenames from a record file, deduplicate them,
    and return them sorted by chronological WR order.

    Args:
        record_folder (str): Folder path where the record file is stored.
        record_txt    (str): Name of the record file (e.g., 'table_1_records.txt').

    Returns:
        list[str]: Sorted and deduplicated list of WR filenames.
    """
    path = os.path.join(record_folder, record_txt)                             # Build full path to record file
    if not os.path.exists(path):                                               # If record file does not exist yet
        return []                                                              # Start with an empty list of records
    with open(path, "r", encoding="utf-8") as f:
        items = [ln.strip() for ln in f if ln.strip()]                         # Strip whitespace and drop empty lines
    return sorted(set(items), key=_ns_sort_key)                                # Deduplicate and sort using WR sort key

# _________________________________________________________________________
# Function to write records to a text file, maintaining chronological order
def _write_records(record_folder: str, record_txt: str, items: list[str]) -> None:
    """
    Persist the set of processed WR filenames into a record file, ensuring that
    duplicates are removed and the list is kept in WR chronological order.

    Args:
        record_folder (str): Folder where the record file is saved.
        record_txt    (str): Record filename to store processed WR filenames.
        items         (list[str]): List of WR filenames to persist.
    """
    os.makedirs(record_folder, exist_ok=True)                                  # Ensure that the record folder exists
    items = sorted(set(items), key=_ns_sort_key)                               # Deduplicate and sort by WR order
    path  = os.path.join(record_folder, record_txt)                            # Full record path
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(items) + ("\n" if items else ""))                    # One filename per line (optional trailing newline)

# _________________________________________________________________________
# Function to extract a table from a PDF page using Tabula
def _extract_table(pdf_path: str, page: int) -> pd.DataFrame | None:
    """
    Extract a single table from a specific page in a NEW WR PDF using Tabula.

    Args:
        pdf_path (str): Full path to the NEW WR PDF.
        page     (int): 1-based index of the PDF page containing the table.

    Returns:
        pd.DataFrame | None: Extracted table as DataFrame, or None when Tabula
        does not return any table.
    """
    tables = tabula.read_pdf(
        pdf_path,
        pages=page,
        multiple_tables=False,
        stream=True,
    )                                                                           # Run Tabula on the requested page
    if tables is None:                                                          # Tabula returned nothing
        return None
    if isinstance(tables, list) and len(tables) == 0:                           # Tabula returned an empty list
        return None
    return tables[0] if isinstance(tables, list) else tables                    # Normal case: return the first detected table

# _________________________________________________________________________
# Function to save a DataFrame to either Parquet or CSV format
def _save_df(df: pd.DataFrame, out_path: str) -> tuple[str, int, int]:
    """
    Save an OLD or NEW cleaned/vintage DataFrame to disk, preferring Parquet and
    falling back to CSV if Parquet is unavailable.

    Args:
        df       (pd.DataFrame): DataFrame to persist (cleaned or vintage).
        out_path (str): Target path suggested by the caller (extension adjusted as needed).

    Returns:
        tuple[str, int, int]: (final_output_path, n_rows, n_cols).
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)                      # Ensure that the parent folder exists
    try:
        if not out_path.endswith(".parquet"):                                  # Normalize extension to '.parquet'
            out_path = os.path.splitext(out_path)[0] + ".parquet"
        df.to_parquet(out_path, index=False)                                   # Write Parquet (requires backend engine)
    except Exception:
        out_path = os.path.splitext(out_path)[0] + ".csv"                      # On failure, switch to CSV
        df.to_csv(out_path, index=False)                                       # Write CSV using default encoding
    return out_path, int(df.shape[0]), int(df.shape[1])                        # Report path and table shape


# °°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°
# 3.2.1 Class for OLD Table 1 and Table 2 pipeline cleaning
# °°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°
# This class encapsulates the cleaning pipeline for WR tables coming from the OLD dataset
# (CSV-based WR files). It exposes two main methods:
# - old_clean_table_1(df): Clean OLD Table 1 (monthly growth).
# - old_clean_table_2(df): Clean OLD Table 2 (quarterly/annual growth).
# These methods apply the common cleaners defined in Section 3 and return harmonized tables.

class old_tables_cleaner:
    """
    Pipelines for OLD WR tables cleaning (CSV-based source).

    Exposes:
        - old_clean_table_1(df): Monthly (Table 1) OLD pipeline.
        - old_clean_table_2(df): Quarterly/annual (Table 2) OLD pipeline.

    Note:
        The helper functions referenced below (drop_nan_rows, clean_columns_values, etc.)
        are defined in Section 3.1 and reused here for the OLD dataset.
    """

    # _____________________________________________________________________
    # Function to clean and process Table 1 (monthly data) from the OLD db 
    def old_clean_table_1(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean a raw DataFrame extracted from OLD WR Table 1 (monthly growth rates).

        Args:
            df (pd.DataFrame): Raw OLD Table 1 DataFrame as read from CSV.

        Returns:
            pd.DataFrame: Cleaned OLD Table 1 DataFrame, ready for reshaping into vintages.
        """
        d = df.copy()                                                          # Work on a copy to preserve the original DataFrame

        # Branch A — header already has sector columns in the expected position
        if d.columns[1] == 'economic_sectors':
            d = drop_nan_rows(d)                                               #  1. Drop rows where all entries are NaN
            d = drop_nan_columns(d)                                            #  2. Drop columns where all entries are NaN
            d = clean_columns_values(d)                                        #  3. Normalize column names and textual values
            d = convert_float(d)                                               #  4. Convert numeric columns using coercion on errors
            d = replace_set_sep(d)                                             #  5. Standardize 'set' month labels to 'sep'
            d = spaces_se_es(d)                                                #  6. Strip spaces in ES/EN sector label columns
            d = replace_mineria(d)                                             #  7. Harmonize 'mineria' naming (ES)
            d = replace_mining(d)                                              #  8. Harmonize 'mining and fuels' naming (EN)
            d = rounding_values(d, decimals=1)                                 #  9. Round float columns to one decimal place
            return d                                                           # Return the cleaned OLD Table 1 DataFrame
        else:
            # Branch B — headers are more irregular and require structural fixes
            d = clean_column_names(d)                                          #  1. Standardize raw column name casing/diacritics
            d = adjust_column_names(d)                                         #  2. Apply WR-specific column name adjustments
            d = drop_rare_caracter_row(d)                                      #  3. Remove rows containing rare character '}'
            d = drop_nan_rows(d)                                               #  4. Drop rows where all entries are NaN
            d = drop_nan_columns(d)                                            #  5. Drop columns where all entries are NaN
            d = reset_index(d)                                                 #  6. Reset index after dropping rows/cols
            d = remove_digit_slash(d)                                          #  7. Strip '<digits>/' prefixes in edge columns
            d = replace_var_perc_first_column(d)                               #  8. Normalize 'Var. %' labels in first column
            d = replace_var_perc_last_columns(d)                               #  9. Normalize 'Var. %' labels in last columns
            d = replace_number_moving_average(d)                               # 10. Normalize moving-average descriptors
            d = relocate_last_column(d)                                        # 11. Move last column into position 1
            d = clean_first_row(d)                                             # 12. Normalize header row text content
            d = find_year_column(d)                                            # 13. Align textual 'year' tokens with numeric years
            years = extract_years(d)                                           # 14. Identify year-labelled columns for WR
            d = get_months_sublist_list(d, years)                              # 15. Build '<year>_<month>' composite headers
            d = first_row_columns(d)                                           # 16. Promote first row to header row
            d = clean_columns_values(d)                                        # 17. Normalize resulting header and body values
            d = convert_float(d)                                               # 18. Convert non-label columns to numeric
            d = replace_set_sep(d)                                             # 19. Standardize 'set' into 'sep'
            d = spaces_se_es(d)                                                # 20. Strip spaces in ES/EN sector label columns
            d = replace_mineria(d)                                             # 21. Harmonize 'mineria' naming (ES)
            d = replace_mining(d)                                              # 22. Harmonize 'mining and fuels' naming (EN)
            d = rounding_values(d, decimals=1)                                 # 23. Round float columns to one decimal place
            return d                                                           # Return the cleaned OLD Table 1 DataFrame

    # _____________________________________________________________________
    # Function to clean and process Table 2 (quarterly/annual data) from OLD db
    def old_clean_table_2(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean a raw DataFrame extracted from OLD WR Table 2 (quarterly/annual growth).

        Args:
            df (pd.DataFrame): Raw OLD Table 2 DataFrame as read from CSV.

        Returns:
            pd.DataFrame: Cleaned OLD Table 2 DataFrame, ready for reshaping into vintages.
        """
        d = df.copy()                                                          # Work on a copy to preserve the original DataFrame

        # Branch A — header already has sector columns in the expected position
        if d.columns[1] == 'economic_sectors':
            d = drop_nan_rows(d)                                               #  1. Drop rows where all entries are NaN
            d = drop_nan_columns(d)                                            #  2. Drop columns where all entries are NaN
            d = clean_columns_values(d)                                        #  3. Normalize column names and textual values
            d = convert_float(d)                                               #  4. Convert numeric columns using coercion on errors
            d = replace_set_sep(d)                                             #  5. Standardize 'set' month labels to 'sep'
            d = spaces_se_es(d)                                                #  6. Strip spaces in ES/EN sector label columns
            d = replace_mineria(d)                                             #  7. Harmonize 'mineria' naming (ES)
            d = replace_mining(d)                                              #  8. Harmonize 'mining and fuels' naming (EN)
            d = rounding_values(d, decimals=1)                                 #  9. Round float columns to one decimal place
            return d                                                           # Return the cleaned OLD Table 2 DataFrame
        else:
            # Branch B — headers are more irregular and require structural fixes
            d = replace_total_with_year(d)                                     #  1. Convert 'TOTAL' into 'year' header tokens
            d = drop_nan_rows(d)                                               #  2. Drop rows where all entries are NaN
            d = drop_nan_columns(d)                                            #  3. Drop columns where all entries are NaN
            years = extract_years(d)                                           #  4. Identify year-labelled columns
            d = roman_arabic(d)                                                #  5. Convert Roman numeral headers into Arabic
            d = fix_duplicates(d)                                              #  6. Fix duplicated numeric header tokens
            d = relocate_last_column(d)                                        #  7. Move last column into position 1
            d = replace_first_row_nan(d)                                       #  8. Fill NaNs in the first row with column names
            d = clean_first_row(d)                                             #  9. Normalize header row text content
            d = get_quarters_sublist_list(d, years)                            # 10. Build '<year>_<quarter>' composite headers
            d = reset_index(d)                                                 # 11. Reset index after structural changes
            d = first_row_columns(d)                                           # 12. Promote first row to header row
            d = clean_columns_values(d)                                        # 13. Normalize columns and values
            d = reset_index(d)                                                 # 14. Reset index after additional cleaning
            d = convert_float(d)                                               # 15. Convert non-label columns to numeric
            d = replace_set_sep(d)                                             # 16. Standardize 'set' into 'sep'
            d = spaces_se_es(d)                                                # 17. Strip spaces in ES/EN sector label columns
            d = replace_mineria(d)                                             # 18. Harmonize 'mineria' naming (ES)
            d = replace_mining(d)                                              # 19. Harmonize 'mining and fuels' naming (EN)
            d = rounding_values(d, decimals=1)                                 # 20. Round float columns to one decimal place
            return d                                                           # Return the cleaned OLD Table 2 DataFrame


# °°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°
# 3.2.2 Class for NEW Table 1 and Table 2 pipeline cleaning
# °°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°
# This class encapsulates the cleaning pipeline for WR tables coming from the NEW dataset
# (PDF-based WR files). It exposes two main methods:
# - new_clean_table_1(df): Clean NEW Table 1 (monthly growth).
# - new_clean_table_2(df): Clean NEW Table 2 (quarterly/annual growth).
# These methods rely on the same cleaning helpers but are tailored to NEW PDF layouts.

class new_tables_cleaner:
    """
    Pipelines for NEW WR tables cleaning (PDF-based source).

    Exposes:
        - new_clean_table_1(df): Monthly (Table 1) NEW pipeline.
        - new_clean_table_2(df): Quarterly/annual (Table 2) NEW pipeline.

    Note:
        The helper functions referenced below (drop_nan_rows, split_column_by_pattern, etc.)
        are defined in Section 3.1 and reused here for the NEW dataset.
    """

    # _____________________________________________________________________
    # Function to clean and process Table 1 (monthly data) from the NEW db
    def new_clean_table_1(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean a raw DataFrame extracted from NEW WR Table 1 (monthly growth rates).

        Args:
            df (pd.DataFrame): Raw NEW Table 1 DataFrame as extracted from PDF.

        Returns:
            pd.DataFrame: Cleaned NEW Table 1 DataFrame, ready for reshaping into vintages.
        """
        d = df.copy()                                                          # Work on a copy to preserve the original DataFrame

        # Branch A — at least one header already matches a 'YYYY' pattern
        if any(isinstance(c, str) and c.isdigit() and len(c) == 4 for c in d.columns):
            d = swap_nan_se(d)                                                 #  1. Fix misplaced 'SECTORES ECONÓMICOS' header
            d = split_column_by_pattern(d)                                     #  2. Split 'Word. Word' headers into two columns
            d = drop_rare_caracter_row(d)                                      #  3. Remove rows containing rare character '}'
            d = drop_nan_rows(d)                                               #  4. Drop rows where all entries are NaN
            d = drop_nan_columns(d)                                            #  5. Drop columns where all entries are NaN
            d = relocate_last_columns(d)                                       #  6. Relocate text in the last columns if needed
            d = replace_first_dot(d)                                           #  7. Replace the first '.' with '-' in second row cells
            d = swap_first_second_row(d)                                       #  8. Swap first/second rows at first and last columns
            d = drop_nan_rows(d)                                               #  9. Clean residual empty rows
            d = reset_index(d)                                                 # 10. Reset index after structural changes
            d = remove_digit_slash(d)                                          # 11. Strip '<digits>/' prefixes in edge columns
            d = replace_var_perc_first_column(d)                               # 12. Normalize 'Var. %' labels in the first column
            d = replace_var_perc_last_columns(d)                               # 13. Normalize 'Var. %' labels in the last columns
            d = replace_number_moving_average(d)                               # 14. Normalize moving-average descriptors
            d = separate_text_digits(d)                                        # 15. Split mixed text-numeric tokens in penultimate column
            d = exchange_values(d)                                             # 16. Swap last two columns when NaNs appear in the last
            d = relocate_last_column(d)                                        # 17. Move last column into position 1
            d = clean_first_row(d)                                             # 18. Normalize header row text
            d = find_year_column(d)                                            # 19. Align 'year' tokens with numeric year columns
            years = extract_years(d)                                           # 20. Identify year-labelled columns
            d = get_months_sublist_list(d, years)                              # 21. Build '<year>_<month>' composite headers
            d = first_row_columns(d)                                           # 22. Promote first row to header row
            d = clean_columns_values(d)                                        # 23. Normalize column names and values
            d = convert_float(d)                                               # 24. Convert non-label columns to numeric
            d = replace_set_sep(d)                                             # 25. Standardize 'set' into 'sep'
            d = spaces_se_es(d)                                                # 26. Strip spaces in ES/EN sector label columns
            d = replace_services(d)                                            # 27. Harmonize 'services' naming
            d = replace_mineria(d)                                             # 28. Harmonize 'mineria' naming (ES)
            d = replace_mining(d)                                              # 29. Harmonize 'mining and fuels' naming (EN)
            d = rounding_values(d, decimals=1)                                 # 30. Round float columns to one decimal place
            return d                                                           # Return the cleaned NEW Table 1 DataFrame

        # Branch B — no 'YYYY' header yet, additional reconstruction needed
        d = check_first_row(d)                                                 #  1. Handle 'YYYY YYYY' patterns in the first row
        d = check_first_row_1(d)                                               #  2. Fill missing first-row year tokens from edges
        d = replace_first_row_with_columns(d)                                  #  3. Replace NaNs in row 0 with synthetic column names
        d = swap_nan_se(d)                                                     #  4. Fix misplaced 'SECTORES ECONÓMICOS' header
        d = split_column_by_pattern(d)                                         #  5. Split 'Word. Word' headers into two columns
        d = drop_rare_caracter_row(d)                                          #  6. Remove rows containing rare character '}'
        d = drop_nan_rows(d)                                                   #  7. Drop rows where all entries are NaN
        d = drop_nan_columns(d)                                                #  8. Drop columns where all entries are NaN
        d = relocate_last_columns(d)                                           #  9. Relocate trailing values in last columns if needed
        d = swap_first_second_row(d)                                           # 10. Swap first/second rows at first and last columns
        d = drop_nan_rows(d)                                                   # 11. Clean residual empty rows
        d = reset_index(d)                                                     # 12. Reset index after structural changes
        d = remove_digit_slash(d)                                              # 13. Strip '<digits>/' prefixes in edge columns
        d = replace_var_perc_first_column(d)                                   # 14. Normalize 'Var. %' labels in the first column
        d = replace_var_perc_last_columns(d)                                   # 15. Normalize 'Var. %' labels in the last columns
        d = replace_number_moving_average(d)                                   # 16. Normalize moving-average descriptors
        d = expand_column(d)                                                   # 17. Expand hyphenated text within the penultimate column
        d = split_values_1(d)                                                  # 18. Split expanded column (variant 1)
        d = split_values_2(d)                                                  # 19. Split expanded column (variant 2)
        d = split_values_3(d)                                                  # 20. Split expanded column (variant 3)
        d = separate_text_digits(d)                                            # 21. Split mixed text-numeric tokens in penultimate column
        d = exchange_values(d)                                                 # 22. Swap last two columns when NaNs appear in the last
        d = relocate_last_column(d)                                            # 23. Move last column into position 1
        d = clean_first_row(d)                                                 # 24. Normalize header row text
        d = find_year_column(d)                                                # 25. Align 'year' tokens with numeric year columns
        years = extract_years(d)                                               # 26. Identify year-labelled columns
        d = get_months_sublist_list(d, years)                                  # 27. Build '<year>_<month>' composite headers
        d = first_row_columns(d)                                               # 28. Promote first row to header row
        d = clean_columns_values(d)                                            # 29. Normalize column names and values
        d = convert_float(d)                                                   # 30. Convert non-label columns to numeric
        d = replace_nan_with_previous_column_1(d)                              # 31. Fill NaNs using neighboring columns (variant 1)
        d = replace_nan_with_previous_column_2(d)                              # 32. Fill NaNs using neighboring columns (variant 2)
        d = replace_nan_with_previous_column_3(d)                              # 33. Fill NaNs using neighboring columns (variant 3)
        d = replace_set_sep(d)                                                 # 34. Standardize 'set' into 'sep'
        d = spaces_se_es(d)                                                    # 35. Strip spaces in ES/EN sector label columns
        d = replace_services(d)                                                # 36. Harmonize 'services' naming
        d = replace_mineria(d)                                                 # 37. Harmonize 'mineria' naming (ES)
        d = replace_mining(d)                                                  # 38. Harmonize 'mining and fuels' naming (EN)
        d = rounding_values(d, decimals=1)                                     # 39. Round float columns to one decimal place
        return d                                                               # Return the cleaned NEW Table 1 DataFrame

    # _____________________________________________________________________
    # Function to clean and process Table 2 (quarterly/annual data) from NEW db
    def new_clean_table_2(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean a raw DataFrame extracted from NEW WR Table 2 (quarterly/annual growth).

        Args:
            df (pd.DataFrame): Raw NEW Table 2 DataFrame as extracted from PDF.

        Returns:
            pd.DataFrame: Cleaned NEW Table 2 DataFrame, ready for reshaping into vintages.
        """
        d = df.copy()                                                          # Work on a copy to preserve the original DataFrame

        # Branch A — header starts with NaN in the first cell (specific NEW layout)
        if pd.isna(d.iloc[0, 0]):
            d = drop_nan_columns(d)                                            #  1. Drop fully-NaN columns
            d = separate_years(d)                                              #  2. Split 'YYYY YYYY' combined header into two columns
            d = relocate_roman_numerals(d)                                     #  3. Move Roman numerals into a dedicated column
            d = extract_mixed_values(d)                                        #  4. Extract mixed numeric/text tokens from third-last column
            d = replace_first_row_nan(d)                                       #  5. Replace NaNs in row 0 with their column names
            d = first_row_columns(d)                                           #  6. Promote the first row to column headers
            d = swap_first_second_row(d)                                       #  7. Swap first/second rows at first and last columns
            d = reset_index(d)                                                 #  8. Reset index after structural changes
            d = drop_nan_row(d)                                                #  9. Drop row 0 if still fully NaN
            years = extract_years(d)                                           # 10. Identify year-labelled columns
            d = split_values(d)                                                # 11. Split target mixed column into several columns
            d = separate_text_digits(d)                                        # 12. Split mixed text-numeric tokens in penultimate column
            d = roman_arabic(d)                                                # 13. Convert Roman numerals in row 0 to Arabic numerals
            d = fix_duplicates(d)                                              # 14. Fix duplicated numeric header tokens
            d = relocate_last_column(d)                                        # 15. Move last column into position 1
            d = clean_first_row(d)                                             # 16. Normalize header row text
            d = get_quarters_sublist_list(d, years)                            # 17. Build '<year>_<quarter>' composite headers
            d = first_row_columns(d)                                           # 18. Promote first row to column headers again
            d = clean_columns_values(d)                                        # 19. Normalize column names and values
            d = reset_index(d)                                                 # 20. Reset index after additional cleaning
            d = convert_float(d)                                               # 21. Convert non-label columns to numeric
            d = replace_set_sep(d)                                             # 22. Standardize 'set' into 'sep'
            d = spaces_se_es(d)                                                # 23. Strip spaces in ES/EN sector label columns
            d = replace_services(d)                                            # 24. Harmonize 'services' naming
            d = replace_mineria(d)                                             # 25. Harmonize 'mineria' naming (ES)
            d = replace_mining(d)                                              # 26. Harmonize 'mining and fuels' naming (EN)
            d = rounding_values(d, decimals=1)                                 # 27. Round float columns to one decimal place
            return d                                                           # Return the cleaned NEW Table 2 DataFrame

        # Branch B — standard NEW layout without NaN at (0, 0)
        d = exchange_roman_nan(d)                                              #  1. Swap Roman numerals/'AÑO' vs NaN in second row
        d = exchange_columns(d)                                                #  2. Swap year-like empty column names with neighbors
        d = drop_nan_columns(d)                                                #  3. Drop fully-NaN columns
        d = remove_digit_slash(d)                                              #  4. Strip '<digits>/' prefixes in edge columns
        d = last_column_es(d)                                                  #  5. Fix 'ECONOMIC SECTORS' placement in the last column
        d = swap_first_second_row(d)                                           #  6. Swap first/second rows at first and last columns
        d = drop_nan_rows(d)                                                   #  7. Drop rows where all entries are NaN
        d = reset_index(d)                                                     #  8. Reset index after structural changes
        years = extract_years(d)                                               #  9. Identify year-labelled columns
        d = separate_text_digits(d)                                            # 10. Split mixed text-numeric tokens in penultimate column
        d = roman_arabic(d)                                                    # 11. Convert Roman numerals in row 0 to Arabic numerals
        d = fix_duplicates(d)                                                  # 12. Fix duplicated numeric header tokens
        d = relocate_last_column(d)                                            # 13. Move last column into position 1
        d = clean_first_row(d)                                                 # 14. Normalize header row text
        d = get_quarters_sublist_list(d, years)                                # 15. Build '<year>_<quarter>' composite headers
        d = first_row_columns(d)                                               # 16. Promote first row to column headers
        d = clean_columns_values(d)                                            # 17. Normalize column names and values
        d = reset_index(d)                                                     # 18. Reset index after additional cleaning
        d = convert_float(d)                                                   # 19. Convert non-label columns to numeric
        d = replace_set_sep(d)                                                 # 20. Standardize 'set' into 'sep'
        d = spaces_se_es(d)                                                    # 21. Strip spaces in ES/EN sector label columns
        d = replace_services(d)                                                # 22. Harmonize 'services' naming
        d = replace_mineria(d)                                                 # 23. Harmonize 'mineria' naming (ES)
        d = replace_mining(d)                                                  # 24. Harmonize 'mining and fuels' naming (EN)
        d = rounding_values(d, decimals=1)                                     # 25. Round float columns to one decimal place
        return d                                                               # Return the cleaned NEW Table 2 DataFrame


# °°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°
# 3.2.3 Class to reshape cleaned tables into “vintages”
# °°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°
# This class reshapes cleaned OLD/NEW tables into a tidy 'vintage' format that is ready for
# concatenation across years and frequencies. It provides helpers to:
# - Infer the WR month order within a year based on WR issue day (ns-dd-yyyy).
# - Reshape cleaned tables into row-based vintages with industry/vintage/target-period columns.

class vintages_preparator:
    """
    Helpers that:
      - Infer month order within a year from WR issue numbers (ns-dd-yyyy.pdf -> dd -> month index 1..12).
      - Reshape cleaned OLD/NEW tables into tidy 'vintage' DataFrames for concatenation.
    """
    
    # _____________________________________________________________________
    # Function to build month order mapping from WR filenames
    def build_month_order_map(self, year_folder: str, extensions: tuple[str, ...] = (".pdf", ".csv")) -> dict[str, int]:
        """
        Create a mapping {filename: month_order} for files in a given WR year folder.

        Args:
            year_folder (str): Folder path containing WR files for a single year (OLD CSV or NEW PDF).
            extensions (tuple[str, ...], optional): Allowed file extensions. Defaults to (".pdf", ".csv").

        Returns:
            dict[str, int]: Mapping from filename to month_order in {1..12}, inferred from 'ns-dd-yyyy.ext'.
        """
        files = [
            f for f in os.listdir(year_folder)
            if f.lower().endswith(extensions)
        ]                                                                           # Filter by desired extensions

        pairs: list[tuple[str, int]] = []                                           # List of (filename, issue_day) tuples
        for f in files:
            m = re.search(
                r"ns-(\d{2})-\d{4}\.[a-zA-Z0-9]+$",
                f,
                re.IGNORECASE,
            )                                                                       # Match 'ns-07-2017.pdf' or similar
            if m:
                pairs.append((f, int(m.group(1))))                                  # Use WR issue day as ordering anchor

        sorted_files = sorted(pairs, key=lambda x: x[1])                            # Sort by issue day in ascending order
        return {fname: i + 1 for i, (fname, _) in enumerate(sorted_files)}          # Month order is implied by sorted position

    # _____________________________________________________________________
    # Function to prepare Table 1 data into *row-based* vintage format
    def prepare_table_1(self, df: pd.DataFrame, filename: str, month_order_map: dict[str, int]) -> pd.DataFrame:
        """
        Prepare a cleaned Table 1 (monthly) from OLD/NEW WR into a tidy *row-based* vintage format.

        Output columns:
            - industry (str)
            - vintage  (str, e.g. '2017m1')
            - tp_YYYYmM (float) for every target period present in the WR.
        """

        # 1) Work on a copy of the cleaned table
        d = df.copy()

        # 2) Determine WR month index (1..12) from filename
        wr_month = month_order_map.get(filename)                                    # Single month index for this WR file
        d["month"] = wr_month                                                       # Attach WR month to all rows

        # 3) Drop columns that are not needed for the vintage layout
        d = d.drop(columns=["wr", "sectores_economicos"], errors="ignore")          # Keep 'year' and 'economic_sectors' plus periods

        # 4) Build sector remapping into canonical industry labels
        sector_map = {
            "agriculture and livestock": "agriculture",
            "fishing": "fishing",
            "mining and fuel": "mining",
            "manufacturing": "manufacturing",
            "electricity and water": "electricity",
            "construction": "construction",
            "commerce": "commerce",
            "other services": "services",
            "gdp": "gdp",
        }

        # Normalize sector column name for OLD/NEW variations
        if "economic_sectors" not in d.columns:
            if "economic_sector" in d.columns:
                d["economic_sectors"] = d["economic_sector"]                        # Fallback for alternative naming
            else:
                raise ValueError(
                    "Expected 'economic_sectors' column not found in cleaned Table 1 dataframe."
                )

        d["industry"] = d["economic_sectors"].map(sector_map)                       # Map to canonical industry codes
        d = d[d["industry"].notna()].copy()                                         # Keep only rows with recognized industries

        # 5) Build vintage identifier = year + 'm' + WR month index
        d["vintage"] = (
            d["year"].astype(int).astype(str)
            + "m"
            + d["month"].astype(int).astype(str)
        )                                                                           # Example: '2017m1'

        # 6) Detect monthly target-period columns like '2015_ene', '2015_jul', ...
        pat = re.compile(
            r"^\d{4}_(ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)$",
            re.IGNORECASE,
        )                                                                           # Year + Spanish 3-letter month
        period_cols = [c for c in d.columns if pat.match(str(c))]                   # All candidate target period columns

        # 7) Rename period columns into 'tp_YYYYmM'
        month_map = {
            "ene": 1, "feb": 2, "mar": 3, "abr": 4, "may": 5, "jun": 6,
            "jul": 7, "ago": 8, "sep": 9, "oct": 10, "nov": 11, "dic": 12,
        }

        def period_to_tp(col: str) -> str:
            m = re.match(r"^(\d{4})_(\w{3})$", col, re.IGNORECASE)                 # Split 'YYYY_mmm'
            if not m:
                return col
            yy  = m.group(1)
            mmm = m.group(2).lower()
            mm  = month_map.get(mmm, 1)                                            # Default to month 1 if unknown
            return f"tp_{yy}m{mm}"                                                 # Build 'tp_YYYYmM' label

        rename_dict = {c: period_to_tp(c) for c in period_cols}                    # Build mapping for renaming
        d = d.rename(columns=rename_dict)                                          # Apply new target-period column names

        # 8) Order columns in chronological target-period order
        tp_cols = [rename_dict[c] for c in period_cols]                            # List of new 'tp_...' column names

        def _tp_key(c: str):
            # Convert 'tp_2016m10' into sorting key (2016, 10)
            s = c[3:]                                                              # Remove 'tp_'
            y, m = s.split("m")
            return (int(y), int(m))

        tp_cols_sorted = sorted(tp_cols, key=_tp_key)                              # Sort by (year, month)
        final_cols    = ["industry", "vintage"] + tp_cols_sorted                   # Final column order

        d_out = d[final_cols].reset_index(drop=True)                               # New vintage DataFrame

        # 9) Enforce dtypes:
        #    - 'industry' and 'vintage' as strings
        #    - all 'tp_*' columns as float
        d_out["industry"] = d_out["industry"].astype(str)
        d_out["vintage"]  = d_out["vintage"].astype(str)

        for col in tp_cols_sorted:
            d_out[col] = pd.to_numeric(d_out[col], errors="coerce").astype(float)  # Coerce invalid entries to NaN

        return d_out                                                               # Return tidy vintage Table 1

    # _____________________________________________________________________
    # Function to prepare Table 2 data into *row-based* vintage format
    def prepare_table_2(self, df: pd.DataFrame, filename: str, month_order_map: dict[str, int]) -> pd.DataFrame:
        """
        Prepare a cleaned Table 2 (quarterly/annual) from OLD/NEW WR into a tidy *row-based* vintage format.

        Output columns:
            - industry (str)
            - vintage  (str, e.g. '2017m1')
            - tp_YYYYqN (float) for quarterly targets
            - tp_YYYY   (float) for annual targets.
        """

        # 1) Work on a copy of the cleaned table
        d = df.copy()

        # 2) Drop columns that are not needed for the vintage layout
        d = d.drop(columns=["wr", "sectores_economicos"], errors="ignore")         # Keep 'year', 'economic_sectors', and target periods

        # 3) Sector remapping into canonical industry labels
        sector_map = {
            "agriculture and livestock": "agriculture",
            "fishing": "fishing",
            "mining and fuel": "mining",
            "manufacturing": "manufacturing",
            "electricity and water": "electricity",
            "construction": "construction",
            "commerce": "commerce",
            "other services": "services",
            "gdp": "gdp",
        }

        # Normalize sector column name for OLD/NEW variations
        if "economic_sectors" not in d.columns:
            if "economic_sector" in d.columns:
                d["economic_sectors"] = d["economic_sector"]                      # Fallback for alternative naming
            else:
                raise ValueError(
                    "Expected 'economic_sectors' column not found in cleaned Table 2 dataframe."
                )

        d["industry"] = d["economic_sectors"].map(sector_map)                     # Map to canonical industry codes
        d = d[d["industry"].notna()].copy()                                       # Keep only rows with recognized industries

        # 4) Build vintage identifier from 'year' and WR month index (consistent with Table 1)
        wr_month = month_order_map.get(filename)                                  # Single month index for this WR file
        d["month"]   = wr_month
        d["vintage"] = (
            d["year"].astype(int).astype(str)
            + "m"
            + d["month"].astype(int).astype(str)
        )                                                                         # Example: '2017m1'

        # 5) Detect quarterly/annual period columns: '2020_1', '2020_2', '2020_3', '2020_4', '2020_year'
        pat = re.compile(r"^\d{4}_(1|2|3|4|year)$", re.IGNORECASE)
        period_cols = [c for c in d.columns if pat.match(str(c))]                 # All candidate target period columns

        # 6) Rename to 'tp_YYYYqN' for quarters and 'tp_YYYY' for annuals
        def quarter_to_tp(col: str) -> str:
            m = re.match(r"^(\d{4})_(\d)$", col, re.IGNORECASE)                   # Match quarter pattern 'YYYY_q'
            if m:
                yy = m.group(1)
                q  = m.group(2)
                return f"tp_{yy}q{q}"                                             # Quarterly target period
            m2 = re.match(r"^(\d{4})_year$", col, re.IGNORECASE)                  # Match annual pattern 'YYYY_year'
            if m2:
                yy = m2.group(1)
                return f"tp_{yy}"                                                 # Annual target period
            return col

        rename_dict = {c: quarter_to_tp(c) for c in period_cols}                  # Build mapping for renaming
        d = d.rename(columns=rename_dict)                                         # Apply new target-period column names

        # 7) Order columns so that quarterlies precede annual for each year
        tp_cols = [rename_dict[c] for c in period_cols]                           # List of new 'tp_...' column names

        def _tp_key(c: str):
            # Sorting key: (year, is_annual_flag, quarter)
            assert c.startswith("tp_")
            body = c[3:]                                                          # 'YYYYqN' or 'YYYY'
            if "q" in body:
                yy, q = body.split("q")                                           # Quarterly: first four digits are year
                return (int(yy), 0, int(q))                                       # Quarterly rows come before annual
            return (int(body), 1, 0)                                              # Annual row for that year

        tp_cols_sorted = sorted(tp_cols, key=_tp_key)                             # Chronological within year
        final_cols    = ["industry", "vintage"] + tp_cols_sorted                  # Final column order
        d_out         = d[final_cols].reset_index(drop=True)                      # New vintage DataFrame

        # 8) Enforce dtypes:
        #    - 'industry' and 'vintage' as strings
        #    - all 'tp_*' columns as float
        d_out["industry"] = d_out["industry"].astype(str)
        d_out["vintage"]  = d_out["vintage"].astype(str)

        for col in tp_cols_sorted:
            d_out[col] = pd.to_numeric(d_out[col], errors="coerce").astype(float) # Coerce invalid entries to NaN

        return d_out                                                              # Return tidy vintage Table 2


# °°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°
# 3.2.4 Runners: single-call functions per table 
# °°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°
# These runner functions coordinate the entire workflow for OLD and NEW WR:
# - Walk year folders.
# - Skip already-recorded WR files.
# - Extract tables (CSV for OLD, PDF for NEW).
# - Clean tables using the appropriate pipeline class.
# - Reshape into vintages and optionally persist vintage datasets.

# _________________________________________________________________________ 
# Function to clean and process Table 1 from all OLD WR (CSV files) in a folder
def old_table_1_cleaner(
    input_csv_folder: str,
    record_folder: str,
    record_txt: str,
    persist: bool = False,
    persist_folder: str | None = None,
    pipeline_version: str = "s3.0.0",
    sep: str = ';',  # Separator argument to allow flexibility in separator choice
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    """
    Process each OLD WR CSV file in a folder, run the OLD Table 1 cleaning pipeline,
    update the record of processed files, optionally persist vintages (Parquet/CSV),
    and print a concise run summary.

    Returns:
        tuple of dictionaries containing raw tables, cleaned tables, and vintages
        (see function body for keys and structure).
    """
    start_time = time.time()                                                    # Capture overall start time
    print("\n🧹 Starting Table 1 cleaning...\n")

    cleaner   = old_tables_cleaner()                                            # OLD Table 1/2 cleaner for CSV-based WR
    records   = _read_records(record_folder, record_txt)                        # Load previously processed WR filenames
    processed = set(records)                                                    # Convert to set for O(1) membership checks

    raw_tables_dict_1: dict[str, pd.DataFrame]   = {}                           # Store raw OLD Table 1 extractions
    clean_tables_dict_1: dict[str, pd.DataFrame] = {}                           # Store cleaned OLD Table 1 DataFrames

    new_counter      = 0                                                        # Counter of newly cleaned WR files
    skipped_counter  = 0                                                        # Counter of already-processed WR files
    skipped_years: dict[str, int] = {}                                          # Per-year skipped WR counts

    # List all year folders except '_quarantine'
    years = [
        d for d in sorted(os.listdir(input_csv_folder))
        if os.path.isdir(os.path.join(input_csv_folder, d)) and d != "_quarantine"
    ]
    total_year_folders = len(years)                                             # Total number of year folders with input

    prep            = vintages_preparator()                                     # Helper to build vintages from cleaned tables
    vintages_dict_1: dict[str, pd.DataFrame] = {}                               # Store row-based vintage outputs for Table 1

    # Prepare output folders if persistence is enabled
    if persist:
        base_out = persist_folder or os.path.join("data", "input")              # Root for persisted inputs
        out_root = os.path.join(base_out, "table_1")                            # Subfolder for Table 1 vintages
        os.makedirs(out_root, exist_ok=True)                                    # Ensure that the output directory exists

    # Iterate through year folders in chronological order
    for year in years:
        folder_path = os.path.join(input_csv_folder, year)                      # Full path to current OLD year folder
        csv_files   = sorted(
            [f for f in os.listdir(folder_path) if f.endswith(".csv")],
            key=_ns_sort_key,
        )                                                                       # Order WR files using WR sort key

        month_order_map = prep.build_month_order_map(folder_path)               # Map filename -> WR month index (1..12)

        if not csv_files:
            continue                                                            # Skip empty year folders

        # Skip if all CSVs in this year are already processed
        already = [f for f in csv_files if f in processed]
        if len(already) == len(csv_files):
            skipped_years[year] = len(already)                                  # Record full-year skip
            skipped_counter    += len(already)
            continue

        print(f"\n📂 Processing Table 1 in {year}\n")
        folder_new_count    = 0                                                 # Newly processed WR for this year
        folder_skipped_count = 0                                                # Skipped WR for this year

        # Progress bar for OLD CSVs in the current year
        pbar = tqdm(
            csv_files,
            desc=f"🧹 {year}",
            unit="CSV",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
            colour="#E6004C",
            leave=False,
            position=0,
            dynamic_ncols=True,
        )

        for filename in pbar:
            if filename in processed:
                folder_skipped_count += 1                                       # WR already processed earlier
                continue

            issue, yr = parse_ns_meta(filename)                                 # Extract WR issue and year from file name
            if not issue:                                                       # Skip if filename does not follow WR pattern
                folder_skipped_count += 1
                continue

            csv_path = os.path.join(folder_path, filename)                      # Full path to OLD WR CSV
            try:
                raw = pd.read_csv(csv_path, sep=sep)                            # Read OLD Table 1 directly from CSV
                if raw is None:
                    folder_skipped_count += 1                                   # Defensive: unexpected None from reader
                    continue

                key = f"{os.path.splitext(filename)[0].replace('-', '_')}_1"    # Unique key per WR for Table 1
                raw_tables_dict_1[key] = raw.copy()                             # Store raw OLD Table 1 for inspection

                clean = cleaner.old_clean_table_1(raw)                          # Run OLD Table 1 cleaning pipeline
                clean.insert(0, "year", yr)                                     # Insert 'year' column as first column
                clean.insert(1, "wr", issue)                                    # Insert WR issue (ns code) as second column
                clean.attrs["pipeline_version"] = pipeline_version              # Stamp pipeline version on the DataFrame

                clean_tables_dict_1[key] = clean.copy()                         # Keep in-memory copy of cleaned table

                # Prepare and, if requested, persist the vintage (final reshaped output)
                vintage = prep.prepare_table_1(clean, filename, month_order_map)
                vintage.attrs["pipeline_version"] = pipeline_version
                vintages_dict_1[key] = vintage                                  # Store vintage in memory (optional)

                if persist:                                                     # Only vintages are persisted to disk
                    ns_code  = os.path.splitext(filename)[0]                    # Example: 'ns-07-2017'
                    out_dir  = os.path.join(out_root, str(yr))                  # Folder per year
                    out_path = os.path.join(out_dir, f"{ns_code}.parquet")      # Prefer Parquet extension
                    _save_df(vintage, out_path)                                 # Persist vintage (Parquet/CSV)

                processed.add(filename)                                         # Mark this WR as processed
                folder_new_count += 1                                           # Increment new WR counter
            except Exception as e:
                print(f"⚠️  {filename}: {e}")
                folder_skipped_count += 1                                       # Record failure as skipped

        pbar.clear(); pbar.close()                                              # Clear progress bar after loop

        # Completion bar for the current year
        fb = tqdm(
            total=len(csv_files),
            desc=f"✔️ {year}",
            unit="CSV",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
            colour="#3366FF",
            leave=True,
            position=0,
            dynamic_ncols=True,
        )
        fb.update(len(csv_files))                                               # Mark full completion for year
        fb.close()

        new_counter     += folder_new_count                                     # Accumulate new WR count across years
        skipped_counter += folder_skipped_count                                 # Accumulate skipped WR count
        _write_records(record_folder, record_txt, list(processed))              # Persist updated records file

    # Summary of skipped years for OLD Table 1
    if skipped_years:
        years_summary = ", ".join(skipped_years.keys())
        total_skipped = sum(skipped_years.values())
        print(f"\n⏩ {total_skipped} cleaned tables already generated for years: {years_summary}")

    elapsed_time = round(time.time() - start_time)                              # Total runtime in seconds
    print(f"\n📊 Summary:\n")
    print(f"📂 {total_year_folders} folders (years) found containing input CSVs")
    print(f"🗃️ Already cleaned tables: {skipped_counter}")
    print(f"✨ Newly cleaned tables: {new_counter}")
    print(f"⏱️ {elapsed_time} seconds")

    return raw_tables_dict_1, clean_tables_dict_1, vintages_dict_1              # Return raw, cleaned, and vintage tables

# _________________________________________________________________________
# Function to clean and process Table 1 from all NEW WR (PDF files) in a folder
def new_table_1_cleaner(
    input_pdf_folder: str,
    record_folder: str,
    record_txt: str,
    persist: bool = False,
    persist_folder: str | None = None,
    pipeline_version: str = "s3.0.0",
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    """
    Extract page 1 from each NEW WR PDF, run the NEW Table 1 cleaning pipeline,
    update the record of processed files, optionally persist vintages (Parquet/CSV),
    and print a concise run summary.

    Returns:
        tuple of dictionaries containing raw tables, cleaned tables, and vintages
        (see function body for keys and structure).
    """
    start_time = time.time()                                                    # Capture overall start time
    print("\n🧹 Starting Table 1 cleaning...\n")

    cleaner   = new_tables_cleaner()                                            # NEW Table 1/2 cleaner for PDF-based WR
    records   = _read_records(record_folder, record_txt)                        # Load previously processed WR filenames
    processed = set(records)                                                    # Convert to set for O(1) membership checks

    raw_tables_dict_1: dict[str, pd.DataFrame]   = {}                           # Store raw NEW Table 1 extractions
    clean_tables_dict_1: dict[str, pd.DataFrame] = {}                           # Store cleaned NEW Table 1 DataFrames

    new_counter      = 0                                                        # Counter of newly cleaned WR files
    skipped_counter  = 0                                                        # Counter of already-processed WR files
    skipped_years: dict[str, int] = {}                                          # Per-year skipped WR counts

    # List all year folders except '_quarantine'
    years = [
        d for d in sorted(os.listdir(input_pdf_folder))
        if os.path.isdir(os.path.join(input_pdf_folder, d)) and d != "_quarantine"
    ]
    total_year_folders = len(years)                                             # Total number of year folders with input

    prep            = vintages_preparator()                                     # Helper to build vintages from cleaned tables
    vintages_dict_1: dict[str, pd.DataFrame] = {}                               # Store row-based vintage outputs for Table 1

    # Prepare output folders if persistence is enabled
    if persist:
        base_out = persist_folder or os.path.join("data", "input")              # Root for persisted inputs
        out_root = os.path.join(base_out, "table_1")                            # Subfolder for Table 1 vintages
        os.makedirs(out_root, exist_ok=True)                                    # Ensure that the output directory exists

    # Iterate through year folders in chronological order
    for year in years:
        folder_path = os.path.join(input_pdf_folder, year)                      # Full path to current NEW year folder
        pdf_files   = sorted(
            [f for f in os.listdir(folder_path) if f.endswith(".pdf")],
            key=_ns_sort_key,
        )                                                                       # Order WR PDFs using WR sort key
        
        month_order_map = prep.build_month_order_map(folder_path)               # Map filename -> WR month index (1..12)

        if not pdf_files:
            continue                                                            # Skip empty year folders

        # Skip if all PDFs in this year are already processed
        already = [f for f in pdf_files if f in processed]
        if len(already) == len(pdf_files):
            skipped_years[year] = len(already)                                  # Record full-year skip
            skipped_counter    += len(already)
            continue

        print(f"\n📂 Processing Table 1 in {year}\n")
        folder_new_count     = 0                                                # Newly processed WR for this year
        folder_skipped_count = 0                                                # Skipped WR for this year

        # Progress bar for NEW PDFs in the current year
        pbar = tqdm(
            pdf_files,
            desc=f"🧹 {year}",
            unit="PDF",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
            colour="#E6004C",
            leave=False,
            position=0,
            dynamic_ncols=True,
        )

        for filename in pbar:
            if filename in processed:
                folder_skipped_count += 1                                       # WR already processed earlier
                continue

            issue, yr = parse_ns_meta(filename)                                 # Extract WR issue and year from file name
            if not issue:                                                       # Skip if filename does not follow WR pattern
                folder_skipped_count += 1
                continue

            pdf_path = os.path.join(folder_path, filename)                      # Full path to NEW WR PDF
            try:
                raw = _extract_table(pdf_path, page=1)                          # Extract NEW Table 1 from page 1
                if raw is None:
                    folder_skipped_count += 1                                   # Nothing to process for this WR
                    continue

                key = f"{os.path.splitext(filename)[0].replace('-', '_')}_1"    # Unique key per WR for Table 1
                raw_tables_dict_1[key] = raw.copy()                             # Store raw NEW Table 1 for inspection

                clean = cleaner.new_clean_table_1(raw)                          # Run NEW Table 1 cleaning pipeline
                clean.insert(0, "year", yr)                                     # Insert 'year' column as first column
                clean.insert(1, "wr", issue)                                    # Insert WR issue (ns code) as second column
                clean.attrs["pipeline_version"] = pipeline_version              # Stamp pipeline version on the DataFrame

                clean_tables_dict_1[key] = clean.copy()                         # Keep in-memory copy of cleaned table

                # Prepare and, if requested, persist the vintage (final reshaped output)
                vintage = prep.prepare_table_1(clean, filename, month_order_map)
                vintage.attrs["pipeline_version"] = pipeline_version
                vintages_dict_1[key] = vintage                                  # Store vintage in memory (optional)
                
                if persist:                                                     # Only vintages are persisted to disk
                    ns_code  = os.path.splitext(filename)[0]                    # Example: 'ns-07-2017'
                    out_dir  = os.path.join(out_root, str(yr))                  # Folder per year
                    out_path = os.path.join(out_dir, f"{ns_code}.parquet")      # Prefer Parquet extension
                    _save_df(vintage, out_path)                                 # Persist vintage (Parquet/CSV)

                processed.add(filename)                                         # Mark this WR as processed
                folder_new_count += 1                                           # Increment new WR counter
            except Exception as e:
                print(f"⚠️  {filename}: {e}")
                folder_skipped_count += 1                                       # Record failure as skipped

        pbar.clear(); pbar.close()                                              # Clear progress bar after loop

        # Completion bar for the current year
        fb = tqdm(
            total=len(pdf_files),
            desc=f"✔️ {year}",
            unit="PDF",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
            colour="#3366FF",
            leave=True,
            position=0,
            dynamic_ncols=True,
        )
        fb.update(len(pdf_files))                                               # Mark full completion for year
        fb.close()

        new_counter     += folder_new_count                                     # Accumulate new WR count across years
        skipped_counter += folder_skipped_count                                 # Accumulate skipped WR count
        _write_records(record_folder, record_txt, list(processed))              # Persist updated records file

    # Summary of skipped years for NEW Table 1
    if skipped_years:
        years_summary = ", ".join(skipped_years.keys())
        total_skipped = sum(skipped_years.values())
        print(f"\n⏩ {total_skipped} cleaned tables already generated for years: {years_summary}")

    elapsed_time = round(time.time() - start_time)                              # Total runtime in seconds
    print(f"\n📊 Summary:\n")
    print(f"📂 {total_year_folders} folders (years) found containing input PDFs")
    print(f"🗃️ Already cleaned tables: {skipped_counter}")
    print(f"✨ Newly cleaned tables: {new_counter}")
    print(f"⏱️ {elapsed_time} seconds")

    return raw_tables_dict_1, clean_tables_dict_1, vintages_dict_1              # Return raw, cleaned, and vintage tables

# _________________________________________________________________________ 
# Function to clean and process Table 2 from all OLD WR (CSV files) in a folder
def old_table_2_cleaner(
    input_csv_folder: str,
    record_folder: str,
    record_txt: str,
    persist: bool = False,
    persist_folder: str | None = None,
    pipeline_version: str = "s3.0.0",
    sep: str = ';',  # Separator argument to allow flexibility in separator choice
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    """
    Process each OLD WR CSV file in a folder, run the OLD Table 2 cleaning pipeline,
    update the record of processed files, optionally persist vintages (Parquet/CSV),
    and print a concise run summary.

    Returns:
        tuple of dictionaries containing raw tables, cleaned tables, and vintages
        (see function body for keys and structure).
    """
    start_time = time.time()                                                    # Capture overall start time
    print("\n🧹 Starting Table 2 cleaning...\n")

    cleaner   = old_tables_cleaner()                                            # OLD Table 1/2 cleaner for CSV-based WR
    records   = _read_records(record_folder, record_txt)                        # Load previously processed WR filenames
    processed = set(records)                                                    # Convert to set for O(1) membership checks

    raw_tables_dict_2: dict[str, pd.DataFrame]   = {}                           # Store raw OLD Table 2 extractions
    clean_tables_dict_2: dict[str, pd.DataFrame] = {}                           # Store cleaned OLD Table 2 DataFrames

    new_counter      = 0                                                        # Counter of newly cleaned WR files
    skipped_counter  = 0                                                        # Counter of already-processed WR files
    skipped_years: dict[str, int] = {}                                          # Per-year skipped WR counts

    # List all year folders except '_quarantine'
    years = [
        d for d in sorted(os.listdir(input_csv_folder))
        if os.path.isdir(os.path.join(input_csv_folder, d)) and d != "_quarantine"
    ]
    total_year_folders = len(years)                                             # Total number of year folders with input
    
    prep            = vintages_preparator()                                     # Helper to build vintages from cleaned tables
    vintages_dict_2: dict[str, pd.DataFrame] = {}                               # Store row-based vintage outputs for Table 2

    # Prepare output folders if persistence is enabled
    if persist:
        base_out = persist_folder or os.path.join("data", "input")              # Root for persisted inputs
        out_root = os.path.join(base_out, "table_2")                            # Subfolder for Table 2 vintages
        os.makedirs(out_root, exist_ok=True)                                    # Ensure that the output directory exists

    # Iterate through year folders in chronological order
    for year in years:
        folder_path = os.path.join(input_csv_folder, year)                      # Full path to current OLD year folder
        csv_files   = sorted(
            [f for f in os.listdir(folder_path) if f.endswith(".csv")],
            key=_ns_sort_key,
        )                                                                       # Order WR files using WR sort key
        
        month_order_map = prep.build_month_order_map(folder_path)               # Map filename -> WR month index (1..12)

        if not csv_files:
            continue                                                            # Skip empty year folders

        # Skip if all CSVs in this year are already processed
        already = [f for f in csv_files if f in processed]
        if len(already) == len(csv_files):
            skipped_years[year] = len(already)                                  # Record full-year skip
            skipped_counter    += len(already)
            continue

        print(f"\n📂 Processing Table 2 in {year}\n")
        folder_new_count     = 0                                                # Newly processed WR for this year
        folder_skipped_count = 0                                                # Skipped WR for this year

        # Progress bar for OLD CSVs in the current year
        pbar = tqdm(
            csv_files,
            desc=f"🧹 {year}",
            unit="CSV",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
            colour="#E6004C",
            leave=False,
            position=0,
            dynamic_ncols=True,
        )

        for filename in pbar:
            if filename in processed:
                folder_skipped_count += 1                                       # WR already processed earlier
                continue

            issue, yr = parse_ns_meta(filename)                                 # Extract WR issue and year from file name
            if not issue:                                                       # Skip if filename does not follow WR pattern
                folder_skipped_count += 1
                continue

            csv_path = os.path.join(folder_path, filename)                      # Full path to OLD WR CSV
            try:
                raw = pd.read_csv(csv_path, sep=sep)                            # Read OLD Table 2 directly from CSV
                if raw is None:
                    folder_skipped_count += 1                                   # Defensive: unexpected None from reader
                    continue

                key = f"{os.path.splitext(filename)[0].replace('-', '_')}_2"    # Unique key per WR for Table 2
                raw_tables_dict_2[key] = raw.copy()                             # Store raw OLD Table 2 for inspection

                clean = cleaner.old_clean_table_2(raw)                          # Run OLD Table 2 cleaning pipeline
                clean.insert(0, "year", yr)                                     # Insert 'year' column as first column
                clean.insert(1, "wr", issue)                                    # Insert WR issue (ns code) as second column
                clean.attrs["pipeline_version"] = pipeline_version              # Stamp pipeline version on the DataFrame

                clean_tables_dict_2[key] = clean.copy()                         # Keep in-memory copy of cleaned table

                # Prepare and, if requested, persist the vintage (final reshaped output)
                vintage = prep.prepare_table_2(clean, filename, month_order_map)
                vintage.attrs["pipeline_version"] = pipeline_version
                vintages_dict_2[key] = vintage                                  # Store vintage in memory (optional)
                
                if persist:                                                     # Only vintages are persisted to disk
                    ns_code  = os.path.splitext(filename)[0]                    # Example: 'ns-07-2017'
                    out_dir  = os.path.join(out_root, str(yr))                  # Folder per year
                    out_path = os.path.join(out_dir, f"{ns_code}.parquet")      # Prefer Parquet extension
                    _save_df(vintage, out_path)                                 # Persist vintage (Parquet/CSV)

                processed.add(filename)                                         # Mark this WR as processed
                folder_new_count += 1                                           # Increment new WR counter
            except Exception as e:
                print(f"⚠️  {filename}: {e}")
                folder_skipped_count += 1                                       # Record failure as skipped

        pbar.clear(); pbar.close()                                              # Clear progress bar after loop

        # Completion bar for the current year
        fb = tqdm(
            total=len(csv_files),
            desc=f"✔️ {year}",
            unit="CSV",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
            colour="#3366FF",
            leave=True,
            position=0,
            dynamic_ncols=True,
        )
        fb.update(len(csv_files))                                               # Mark full completion for year
        fb.close()

        new_counter     += folder_new_count                                     # Accumulate new WR count across years
        skipped_counter += folder_skipped_count                                 # Accumulate skipped WR count
        _write_records(record_folder, record_txt, list(processed))              # Persist updated records file

    # Summary of skipped years for OLD Table 2
    if skipped_years:
        years_summary = ", ".join(skipped_years.keys())
        total_skipped = sum(skipped_years.values())
        print(f"\n⏩ {total_skipped} cleaned tables already generated for years: {years_summary}")

    elapsed_time = round(time.time() - start_time)                              # Total runtime in seconds
    print(f"\n📊 Summary:\n")
    print(f"📂 {total_year_folders} folders (years) found containing input CSVs")
    print(f"🗃️ Already cleaned tables: {skipped_counter}")
    print(f"✨ Newly cleaned tables: {new_counter}")
    print(f"⏱️ {elapsed_time} seconds")

    return raw_tables_dict_2, clean_tables_dict_2, vintages_dict_2              # Return raw, cleaned, and vintage tables

# _________________________________________________________________________
# Function to clean and process Table 2 from all NEW WR PDF files in a folder
def new_table_2_cleaner(
    input_pdf_folder: str,
    record_folder: str,
    record_txt: str,
    persist: bool = False,
    persist_folder: str | None = None,
    pipeline_version: str = "s3.0.0",
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    """
    Extract page 2 from each NEW WR PDF, run the NEW Table 2 cleaning pipeline,
    update the record of processed files, optionally persist vintages (Parquet/CSV),
    and print a concise run summary.

    Returns:
        tuple of dictionaries containing raw tables, cleaned tables, and vintages
        (see function body for keys and structure).
    """
    start_time = time.time()                                                    # Capture overall start time
    print("\n🧹 Starting Table 2 cleaning...\n")

    cleaner   = new_tables_cleaner()                                            # NEW Table 1/2 cleaner for PDF-based WR
    records   = _read_records(record_folder, record_txt)                        # Load previously processed WR filenames
    processed = set(records)                                                    # Convert to set for O(1) membership checks

    raw_tables_dict_2: dict[str, pd.DataFrame] = {}                             # Store raw NEW Table 2 extractions
    clean_tables_dict_2: dict[str, pd.DataFrame] = {}                           # Store cleaned NEW Table 2 DataFrames

    new_counter      = 0                                                        # Counter of newly cleaned WR files
    skipped_counter  = 0                                                        # Counter of already-processed WR files
    skipped_years: dict[str, int] = {}                                          # Per-year skipped WR counts

    # List year directories except '_quarantine'
    years = [
        d for d in sorted(os.listdir(input_pdf_folder))
        if os.path.isdir(os.path.join(input_pdf_folder, d)) and d != "_quarantine"
    ]
    total_year_folders = len(years)                                             # Total number of year folders with input
    
    prep            = vintages_preparator()                                     # Helper to build vintages from cleaned tables
    vintages_dict_2: dict[str, pd.DataFrame] = {}                               # Store row-based vintage outputs for Table 2

    # Create persistence folder if enabled
    if persist:
        base_out = persist_folder or os.path.join("data", "input")              # Root for persisted inputs
        out_root = os.path.join(base_out, "table_2")                            # Subfolder for Table 2 vintages
        os.makedirs(out_root, exist_ok=True)                                    # Ensure that the output directory exists

    # Iterate through each year's folder
    for year in years:
        folder_path = os.path.join(input_pdf_folder, year)                      # Full path to current NEW year folder
        pdf_files   = sorted(
            [f for f in os.listdir(folder_path) if f.endswith(".pdf")],
            key=_ns_sort_key,
        )                                                                       # Order WR PDFs using WR sort key
        month_order_map = prep.build_month_order_map(folder_path)               # Map filename -> WR month index (1..12)

        if not pdf_files:
            continue                                                            # Skip empty year folders

        # Skip if all PDFs already processed
        already = [f for f in pdf_files if f in processed]
        if len(already) == len(pdf_files):
            skipped_years[year] = len(already)                                  # Record full-year skip
            skipped_counter    += len(already)
            continue

        print(f"\n📂 Processing Table 2 in {year}\n")
        folder_new_count     = 0                                                # Newly processed WR for this year
        folder_skipped_count = 0                                                # Skipped WR for this year

        # Display progress bar for current year
        pbar = tqdm(
            pdf_files,
            desc=f"🧹 {year}",
            unit="PDF",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
            colour="#E6004C",
            leave=False,
            position=0,
            dynamic_ncols=True,
        )

        for filename in pbar:
            if filename in processed:
                folder_skipped_count += 1                                       # WR already processed earlier
                continue

            issue, yr = parse_ns_meta(filename)                                 # Extract WR issue and year from file name
            if not issue:                                                       # Skip if filename does not follow WR pattern
                folder_skipped_count += 1
                continue

            pdf_path = os.path.join(folder_path, filename)                      # Full path to NEW WR PDF
            try:
                raw = _extract_table(pdf_path, page=2)                          # Extract NEW Table 2 from page 2
                if raw is None:
                    folder_skipped_count += 1                                   # Nothing to process for this WR
                    continue

                key = f"{os.path.splitext(filename)[0].replace('-', '_')}_2"    # Unique key per WR for Table 2
                raw_tables_dict_2[key] = raw.copy()                             # Store raw NEW Table 2 for inspection

                clean = cleaner.new_clean_table_2(raw)                          # Run NEW Table 2 cleaning pipeline
                clean.insert(0, "year", yr)                                     # Add 'year' column at position 0
                clean.insert(1, "wr", issue)                                    # Add WR issue code at position 1
                clean.attrs["pipeline_version"] = pipeline_version              # Stamp pipeline version on the DataFrame

                clean_tables_dict_2[key] = clean.copy()                         # Keep in-memory copy of cleaned table

                # Build + persist the vintage (reshaped final output)
                vintage = prep.prepare_table_2(clean, filename, month_order_map)
                vintage.attrs["pipeline_version"] = pipeline_version
                vintages_dict_2[key] = vintage                                  # Keep vintage in-memory (optional)

                if persist:                                                     # Persist NEW vintages only
                    ns_code  = os.path.splitext(filename)[0]                    # Example: 'ns-07-2017'
                    out_dir  = os.path.join(out_root, str(yr))                  # Folder per year
                    out_path = os.path.join(out_dir, f"{ns_code}.parquet")      # Prefer Parquet extension
                    _save_df(vintage, out_path)                                 # Persist vintage (Parquet/CSV)

                processed.add(filename)                                         # Record this WR as processed
                folder_new_count += 1                                           # Increment new WR counter
            except Exception as e:
                print(f"⚠️  {filename}: {e}")
                folder_skipped_count += 1                                       # Record failure as skipped

        pbar.clear(); pbar.close()                                              # Close progress bar after processing

        # Completion bar for the current year
        fb = tqdm(
            total=len(pdf_files),
            desc=f"✔️ {year}",
            unit="PDF",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
            colour="#3366FF",
            leave=True,
            position=0,
            dynamic_ncols=True,
        )
        fb.update(len(pdf_files))                                               # Mark full completion for year
        fb.close()

        new_counter     += folder_new_count                                     # Accumulate new WR count across years
        skipped_counter += folder_skipped_count                                 # Accumulate skipped WR count
        _write_records(record_folder, record_txt, list(processed))              # Persist updated records file

    # Summary of skipped years for NEW Table 2
    if skipped_years:
        years_summary = ", ".join(skipped_years.keys())
        total_skipped = sum(skipped_years.values())
        print(f"\n⏩ {total_skipped} cleaned tables already generated for years: {years_summary}")

    elapsed_time = round(time.time() - start_time)                              # Total runtime in seconds
    print(f"\n📊 Summary:\n")
    print(f"📂 {total_year_folders} folders (years) found containing input PDFs")
    print(f"🗃️ Already cleaned tables: {skipped_counter}")
    print(f"✨ Newly cleaned tables: {new_counter}")
    print(f"⏱️ {elapsed_time} seconds")

    return raw_tables_dict_2, clean_tables_dict_2, vintages_dict_2              # Return raw, cleaned, and vintage tables



# ##############################################################################################
# SECTION 4 Concatenating RTD across years by frequency
# ##############################################################################################

# This section contains functions for concatenating Table 1 and Table 2 (monthly and quarterly/annual data) 
# from different years into a unified Real-Time Data (RTD) DataFrame. It processes the data, 
# ensuring that all tables are aligned by frequency and year, and supports saving the concatenated 
# results into a persistent format (CSV or Parquet).


# ++++++++++++++++++++++++++++++++++++++++++++++++
# Libraries
# ++++++++++++++++++++++++++++++++++++++++++++++++

# import os                                                                 # [already imported and documented in section 1]
# import re                                                                 # [already imported and documented in section 1]
# import time                                                               # [already imported and documented in section 3.2]
# import pandas as pd                                                       # [already imported and documented in section 3.1]


# _________________________________________________________________________
# Helper function (you already had this for month-style tp_)
def target_period_sort_key(tp: str):
    """
    Convert 'tp_YYYYmM' or 'YYYYmM' to (year, month) for sorting.

    Args:
        tp (str): Target period in the format 'tp_YYYYmM' or 'YYYYmM'.

    Returns:
        tuple[int, int]: A tuple containing the year and month for sorting.
    """
    if tp.startswith("tp_"):                                                # Remove the 'tp_' prefix if present
        tp = tp[3:]
    m = re.match(r"(\d{4})m(\d+)", tp)                                      # Match the year and month from the string
    if m:
        return (int(m.group(1)), int(m.group(2)))                           # Return as a tuple of year and month
    return (9999, 0)                                                        # Default return for unmatched patterns

# _________________________________________________________________________
# Function to concatenate Table 1 CSVs into ONE unified RTD (row-based)
def concatenate_table_1(
    input_data_subfolder: str,
    record_folder: str,
    record_txt: str,
    persist: bool,
    persist_folder: str,
    csv_file_label: str | None = None,
) -> pd.DataFrame:
    """
    Row-based concatenation for Table 1 (monthly).
    - Reads all year subfolders under .../table_1
    - Loads all CSVs not in record
    - Builds union of tp_* columns
    - Sorts tp_* chronologically
    - Reindexes each df to that full column set (in one shot)
    - Vertical concatenation
    - Enforces dtypes
    - If persist: saves to persist_folder / <csv_file_label or default>.csv

    Args:
        input_data_subfolder (str): Path to the data subfolder.
        record_folder (str): Path where the processed records are stored.
        record_txt (str): Name of the record file containing processed files.
        persist (bool): Whether to persist the output or not.
        persist_folder (str): Folder where the concatenated file will be saved.
        csv_file_label (str | None, optional): Custom name for the output file.

    Returns:
        pd.DataFrame: Unified RTD DataFrame containing concatenated data.
    """
    start_time = time.time()                                                                    # Record the start time
    print("\n⛓️📜 Starting Table 1 concatenation (row-based)...")

    processed_files = _read_records(record_folder, record_txt)                                  # Read the processed files from the record
    table_1_folder  = os.path.join(input_data_subfolder, "table_1")                             # Get the path to the 'table_1' folder
    year_folders    = sorted([f for f in os.listdir(table_1_folder) if f.isdigit()], key=int)   # Sort year folders numerically

    loaded_dfs      = []                                                                        # Initialize list to store loaded DataFrames
    skipped_counter = 0                                                                         # Initialize counter for skipped files
    new_counter     = 0                                                                         # Initialize counter for newly processed files

    # 1) Load CSV files from each year folder
    for year in year_folders:
        year_folder = os.path.join(table_1_folder, year)                                        # Get the path for the current year's folder
        csv_files   = sorted([f for f in os.listdir(year_folder) if f.endswith(".csv")])        # Get all CSV files in the folder
        for csv_file in csv_files:
            if csv_file in processed_files:                                                     # Skip files that have already been processed
                skipped_counter += 1
                continue
            full_path = os.path.join(year_folder, csv_file)                                     # Get the full path of the CSV file
            df = pd.read_csv(full_path)                                                         # Read the CSV into a DataFrame
            loaded_dfs.append(df)                                                               # Add the DataFrame to the list
            processed_files.append(csv_file)                                                    # Add the file to the processed list
            _write_records(record_folder, record_txt, processed_files)                          # Update the record with the processed file
            new_counter += 1                                                                    # Increment the new counter

    if not loaded_dfs:                                                                          # Check if no new files were loaded
        print("No new CSV files to concatenate.")
        return pd.DataFrame()                                                                   # Return an empty DataFrame

    # 2) Base columns and union of tp_* columns from all DataFrames
    base_cols = ["industry", "vintage"]                                                         # Define base columns for the final DataFrame
    all_tp_cols = set()                                                                         # Initialize a set for target period columns
    for df in loaded_dfs:                                                                       # Iterate over all loaded DataFrames
        for col in df.columns:
            if col.startswith("tp_"):                                                           # Check if column name starts with 'tp_'
                all_tp_cols.add(col)                                                            # Add the column to the set

    # 3) Sort tp_* columns chronologically
    tp_cols_sorted = sorted(list(all_tp_cols), key=target_period_sort_key)                      # Sort the target period columns

    # 4) Final column schema (base + tp_* columns)
    final_cols = base_cols + tp_cols_sorted                                                     # Final column order

    # 5) Reindex each DataFrame to match the final column schema
    aligned_dfs = []                                                                            # List to store reindexed DataFrames
    for df in loaded_dfs:
        if "industry" not in df.columns:                                                        # Check if 'industry' column is missing
            raise ValueError("Missing 'industry' column in a Table 1 CSV.")
        if "vintage" not in df.columns:                                                         # Check if 'vintage' column is missing
            raise ValueError("Missing 'vintage' column in a Table 1 CSV.")

        df = df.reindex(columns=final_cols)                                                     # Reindex DataFrame to match the final schema
        aligned_dfs.append(df)                                                                  # Append the reindexed DataFrame to the list

    # 6) Concatenate all DataFrames vertically
    unified_df = pd.concat(aligned_dfs, axis=0, ignore_index=True)                              # Concatenate all DataFrames into a single one

    # 7) Enforce consistent dtypes
    unified_df["industry"] = unified_df["industry"].astype(str)                                 # Ensure 'industry' column is of type string
    unified_df["vintage"]  = unified_df["vintage"].astype(str)                                  # Ensure 'vintage' column is of type string
    for col in tp_cols_sorted:                                                                  # Iterate over target period columns
        unified_df[col] = pd.to_numeric(unified_df[col], errors="coerce").astype(float)         # Convert to numeric, coercing errors to NaN

    # 8) Persist the unified DataFrame if required
    if persist:
        os.makedirs(persist_folder, exist_ok=True)                                              # Ensure the persistence folder exists
        fname = csv_file_label if csv_file_label else "new_gdp_rtd_table_1_unified.csv"         # Set the output filename
        out_path = os.path.join(persist_folder, fname)                                          # Get the full path for saving
        unified_df.to_csv(out_path, index=False)                                                # Save the DataFrame as a CSV file
        print(f"📦 Unified RTD (Table 1) saved to {out_path}")                             

    # 9) Summary of the concatenation process
    elapsed_time = round(time.time() - start_time)                                              # Calculate the elapsed time
    print(f"\n📊 Summary (Table 1):")
    print(f"📂 {len(year_folders)} year folders found")
    print(f"🗃️ Already processed files: {skipped_counter}")
    print(f"🔹 Newly concatenated files: {new_counter}")
    print(f"⏱️ {elapsed_time} seconds")

    return unified_df                                                                           # Return the concatenated DataFrame

# _________________________________________________________________________
# Function to concatenate Table 2 CSVs into ONE unified RTD (row-based)
def concatenate_table_2(
    input_data_subfolder: str,
    record_folder: str,
    record_txt: str,
    persist: bool,
    persist_folder: str,
    csv_file_label: str | None = None,
) -> pd.DataFrame:
    """
    Row-based concatenation for Table 2 (quarterly/annual).
    - Reads all year subfolders under .../table_2
    - Loads all CSVs not in record
    - Builds union of tp_* columns (tp_YYYYqN, tp_YYYY)
    - Sorts them: quarters first, then annual for each year
    - Reindexes in one shot
    - Vertical concatenation
    - Enforces dtypes
    - If persist: saves to persist_folder / <csv_file_label or default>.csv

    Args:
        input_data_subfolder (str): Path to the data subfolder.
        record_folder (str): Path where the processed records are stored.
        record_txt (str): Name of the record file containing processed files.
        persist (bool): Whether to persist the output or not.
        persist_folder (str): Folder where the concatenated file will be saved.
        csv_file_label (str | None, optional): Custom name for the output file.

    Returns:
        pd.DataFrame: Unified RTD DataFrame containing concatenated data.
    """
    start_time = time.time()                                                                    # Record the start time
    print("\n⛓️📜 Starting Table 2 concatenation (row-based)...")

    processed_files = _read_records(record_folder, record_txt)                                  # Read the processed files from the record
    table_2_folder  = os.path.join(input_data_subfolder, "table_2")                             # Get the path to the 'table_2' folder
    year_folders    = sorted([f for f in os.listdir(table_2_folder) if f.isdigit()], key=int)   # Sort year folders numerically

    loaded_dfs      = []                                                                        # Initialize list to store loaded DataFrames
    skipped_counter = 0                                                                         # Initialize counter for skipped files
    new_counter     = 0                                                                         # Initialize counter for newly processed files

    # 1) Load CSV files from each year folder
    for year in year_folders:
        year_folder = os.path.join(table_2_folder, year)                                        # Get the path for the current year's folder
        csv_files   = sorted([f for f in os.listdir(year_folder) if f.endswith(".csv")])        # Get all CSV files in the folder
        for csv_file in csv_files:
            if csv_file in processed_files:                                                     # Skip files that have already been processed
                skipped_counter += 1
                continue
            full_path = os.path.join(year_folder, csv_file)                                     # Get the full path of the CSV file
            df = pd.read_csv(full_path)                                                         # Read the CSV into a DataFrame
            loaded_dfs.append(df)                                                               # Add the DataFrame to the list
            processed_files.append(csv_file)                                                    # Add the file to the processed list
            _write_records(record_folder, record_txt, processed_files)                          # Update the record with the processed file
            new_counter += 1                                                                    # Increment the new counter

    if not loaded_dfs:                                                                          # Check if no new files were loaded
        print("No new CSV files to concatenate.")
        return pd.DataFrame()                                                                   # Return an empty DataFrame

    # 2) Base columns and union of tp_* columns from all DataFrames
    base_cols = ["industry", "vintage"]                                                         # Define base columns for the final DataFrame
    all_tp_cols = set()                                                                         # Initialize a set for target period columns
    for df in loaded_dfs:                                                                       # Iterate over all loaded DataFrames
        for col in df.columns:
            if col.startswith("tp_"):                                                           # Check if column name starts with 'tp_'
                all_tp_cols.add(col)                                                            # Add the column to the set

    # 3) Sort tp_* columns: quarters first, then annual
    def tp_quarter_year_sort_key(col: str):
        if not col.startswith("tp_"):                                                           # Ensure the column starts with 'tp_'
            return (9999, 9, col)
        body = col[3:]  # '1994q1' or '1994'
        m = re.match(r"^(\d{4})q(\d)$", body)                                                   # Match quarterly columns (e.g., '1994q1')
        if m:
            year = int(m.group(1))
            q    = int(m.group(2))
            return (year, 0, q)  # Quarters first
        m2 = re.match(r"^(\d{4})$", body)                                                       # Match annual columns (e.g., '1994')
        if m2:
            year = int(m2.group(1))
            return (year, 1, 0)  # Annual after quarters
        return (9999, 9, col)

    tp_cols_sorted = sorted(list(all_tp_cols), key=tp_quarter_year_sort_key)                    # Sort the target period columns

    # 4) Final column schema
    final_cols = base_cols + tp_cols_sorted                                                     # Final column order

    # 5) Reindex each DataFrame to match the final column schema
    aligned_dfs = []                                                                            # List to store reindexed DataFrames
    for df in loaded_dfs:
        if "industry" not in df.columns:                                                        # Check if 'industry' column is missing
            raise ValueError("Missing 'industry' column in a Table 2 CSV.")
        if "vintage" not in df.columns:                                                         # Check if 'vintage' column is missing
            raise ValueError("Missing 'vintage' column in a Table 2 CSV.")

        df = df.reindex(columns=final_cols)                                                     # Reindex DataFrame to match the final schema
        aligned_dfs.append(df)                                                                  # Append the reindexed DataFrame to the list

    # 6) Concatenate all DataFrames vertically
    unified_df = pd.concat(aligned_dfs, axis=0, ignore_index=True)                              # Concatenate all DataFrames into a single one

    # 7) Enforce consistent dtypes
    unified_df["industry"] = unified_df["industry"].astype(str)                                 # Ensure 'industry' column is of type string
    unified_df["vintage"]  = unified_df["vintage"].astype(str)                                  # Ensure 'vintage' column is of type string
    for col in tp_cols_sorted:                                                                  # Iterate over target period columns
        unified_df[col] = pd.to_numeric(unified_df[col], errors="coerce").astype(float)         # Convert to numeric, coercing errors to NaN

    # 8) Persist the unified DataFrame if required
    if persist:
        os.makedirs(persist_folder, exist_ok=True)                                              # Ensure the persistence folder exists
        fname = csv_file_label if csv_file_label else "new_gdp_rtd_table_2_unified.csv"         # Set the output filename
        out_path = os.path.join(persist_folder, fname)                                          # Get the full path for saving
        unified_df.to_csv(out_path, index=False)                                                # Save the DataFrame as a CSV file
        print(f"📦 Unified RTD (Table 2) saved to {out_path}")                                  

    # 9) Summary of the concatenation process
    elapsed_time = round(time.time() - start_time)                                              # Calculate the elapsed time
    print(f"\n📊 Summary (Table 2):")
    print(f"📂 {len(year_folders)} year folders found containing input CSVs")
    print(f"🗃️ Already processed files: {skipped_counter}")
    print(f"🔹 Newly concatenated files: {new_counter}")
    print(f"⏱️ {elapsed_time} seconds")

    return unified_df                                                                           # Return the concatenated DataFrame



# ##############################################################################################
# SECTION 5 Metadata
# ##############################################################################################
# This section handles the reading, writing, and updating of metadata related to GDP revisions.
# It includes functions to track processed files, extract revision data from PDFs, and apply base-year adjustments.


# ++++++++++++++++++++++++++++++++++++++++++++++++
# Libraries
# ++++++++++++++++++++++++++++++++++++++++++++++++

# import os                                                                 # [already imported and documented in section 1]
# import re                                                                 # [already imported and documented in section 1]
# import time                                                               # [already imported and documented in section 3.2]
# import pandas as pd                                                       # [already imported and documented in section 3.1]
# import numpy as np                                                        # [already imported and documented in section 3.1]
# import fitz                                                               # [already imported and documented in section 2]


# _________________________________________________________________________
# Function to read records of processed years from a text file
def _read_records_2(record_folder: str, record_txt: str) -> list[str]:
    """
    Reads previously processed years from a record text file.
    
    Args:
        record_folder (str): Folder path where the record file is stored.
        record_txt (str): Name of the record file.
    
    Returns:
        list[str]: List of processed years.
    """
    record_path = os.path.join(record_folder, record_txt)                   # Get the full path to the record file
    if not os.path.exists(record_path):                                     # Check if the record file exists
        return []                                                           # Return an empty list if the file doesn't exist
    
    with open(record_path, "r", encoding="utf-8") as f:                     # Open the record file for reading
        return [line.strip() for line in f if line.strip()]                 # Read all lines and return as a list

# _________________________________________________________________________
# Function to write records of processed years to a text file
def _write_records_2(record_folder: str, record_txt: str, years: list[str]) -> None:
    """
    Writes the list of processed years to a record text file.
    
    Args:
        record_folder (str): Folder path where the record file is stored.
        record_txt (str): Name of the record file.
        years (list[str]): List of processed years.
    """
    os.makedirs(record_folder, exist_ok=True)                               # Ensure the record folder exists
    with open(os.path.join(record_folder, record_txt), "w", encoding="utf-8") as f:
        f.write("\n".join(sorted(years)) + "\n")                            # Write the sorted list of years to the file

# _________________________________________________________________________
# Function to extract revision numbers from a PDF
def _extract_wr_update_from_pdf(pdf_path: str) -> tuple[str, str]:
    """
    Extracts the revision numbers (wr IDs) from the first and second pages of a PDF.
    
    Args:
        pdf_path (str): Path to the PDF file.
    
    Returns:
        tuple[str, str]: Extracted revision numbers from the first and second pages.
    """
    doc = fitz.open(pdf_path)                                               # Open the PDF using PyMuPDF
    
    page_1_text = doc[0].get_text()                                         # Extract text from the first page
    revision_calendar_tab_1 = _extract_dd_from_text(page_1_text)            # Extract revision info from the first page text
    
    page_2_text = doc[1].get_text()                                         # Extract text from the second page
    revision_calendar_tab_2 = _extract_dd_from_text(page_2_text)            # Extract revision info from the second page text
    
    return revision_calendar_tab_1, revision_calendar_tab_2                 # Return both revision numbers

# _________________________________________________________________________
# Function to extract the revision ID from a given text
def _extract_dd_from_text(text: str) -> str:
    """
    Extracts a one or two-digit number (wr ID) that follows the pattern "en la Nota N° dd" from the text.
    
    Args:
        text (str): Text content of the PDF page.
    
    Returns:
        str: The extracted "dd" number (1 or 2 digits) or NaN if no match is found.
    """
    match = re.search(r"en la Nota N°\s*(\d{1,2})", text)                 # Search for the revision ID in the text
    return match.group(1) if match else "NaN"                             # Return the revision ID or NaN if no match

# _________________________________________________________________________
# Function to apply base-year block mapping to the DataFrame
def apply_base_years_block(df: pd.DataFrame, base_year_list: list[dict]) -> pd.DataFrame:
    """
    Given a DataFrame with at least ['year', 'wr'], fill 'base_year' according to an ordered list of change points.
    The function assigns a 'base_year' based on defined change points in the base_year_list.

    Args:
        df (pd.DataFrame): The DataFrame with GDP revision data.
        base_year_list (list[dict]): List of change points, each being a dictionary with year, wr, and base_year.

    Returns:
        pd.DataFrame: The DataFrame with 'base_year' filled according to the provided change points.
    """
    df = df.copy()                                                          # Make a copy of the DataFrame to avoid modifying the original

    if "base_year" not in df.columns:                                       # Ensure the 'base_year' column exists
        df["base_year"] = pd.NA                                             # Initialize the 'base_year' column with NaN

    points = sorted(base_year_list, key=lambda x: (x["year"], x["wr"]))     # Sort the base-year change points by year and wr

    def geq(df, Y, W):                                                      # Helper function to build a mask for (year, wr) >= (Y, W)
        return (df["year"] > Y) | ((df["year"] == Y) & (df["wr"] >= W))

    def lt(df, Y, W):                                                       # Helper function to build a mask for (year, wr) < (Y, W)
        return (df["year"] < Y) | ((df["year"] == Y) & (df["wr"] < W))

    # Handle everything before the first change point
    first = points[0]
    first_by = first["base_year"]
    first_y, first_w = first["year"], first["wr"]
    mask_before = lt(df, first_y, first_w)
    df.loc[mask_before & df["base_year"].isna(), "base_year"] = first_by

    # Handle each interval between points
    for i, pt in enumerate(points):
        y, w, by = pt["year"], pt["wr"], pt["base_year"]
        if i < len(points) - 1:
            nxt = points[i + 1]
            ny, nw = nxt["year"], nxt["wr"]
            mask_interval = geq(df, y, w) & lt(df, ny, nw)                      # Mask for rows between two points
        else:
            mask_interval = geq(df, y, w)                                       # Last point: from here to the end

        df.loc[mask_interval & df["base_year"].isna(), "base_year"] = by        # Apply the base-year mapping

    return df                                                                   # Return the updated DataFrame

# _________________________________________________________________________
# Function to mark rows where the base-year has changed
def mark_base_year_affected(df: pd.DataFrame) -> pd.DataFrame:
    """
    Creates or overwrites 'base_year_affected' as 1 whenever 'base_year' changes within a block; 0 otherwise.
    The first row is always marked as 0.

    Args:
        df (pd.DataFrame): The DataFrame with the base-year data.

    Returns:
        pd.DataFrame: The updated DataFrame with the 'base_year_affected' column.
    """
    df = df.sort_values(["year", "wr"]).reset_index(drop=True).copy()           # Sort the DataFrame by year and wr

    if "base_year_affected" not in df.columns:                                  # Ensure the 'base_year_affected' column exists
        df["base_year_affected"] = 0                                            # Initialize the column with zeros

    changed = df["base_year"].ne(df["base_year"].shift())                       # Identify where the base_year has changed
    df.loc[:, "base_year_affected"] = changed.astype(int)                       # Mark the change with a 1, otherwise 0
    df.loc[0, "base_year_affected"] = 0                                         # Ensure the first row is 0
    return df                                                                   # Return the updated DataFrame

# _________________________________________________________________________
# Function to update metadata with new revisions and base-year changes
def update_metadata(
    metadata_folder: str,
    input_pdf_folder: str,
    record_folder: str,
    record_txt: str,
    wr_metadata_csv: str,
    base_year_list: list[dict],
) -> pd.DataFrame:
    """
    Updates metadata by processing new revision PDF files and applying base-year changes.
    - Reads current metadata from CSV
    - Lists folders (years) to process
    - Extracts revision data from PDF files
    - Applies base-year mapping only to new rows
    - Marks where base-year has changed in the new rows
    - Concatenates the updated rows to the existing metadata
    - Saves the updated metadata to CSV

    Args:
        metadata_folder: Folder where the metadata CSV is stored
        input_pdf_folder: Folder where the revision PDFs are stored
        record_folder: Folder where the processed records are stored
        record_txt: Record file to keep track of processed files
        wr_metadata_csv: The metadata CSV file name
        base_year_list: List of base-year mappings

    Returns:
        Updated pandas DataFrame containing the metadata with new revisions
    """
    start_time = time.time()  # Timer to measure elapsed time
    print("\n🔄📋 Starting metadata update process...")

    # 1) Read current metadata
    metadata_path = os.path.join(metadata_folder, wr_metadata_csv)
    if os.path.exists(metadata_path):
        metadata = pd.read_csv(metadata_path)                               # Load existing metadata
    else:
        metadata = pd.DataFrame(                                            # Create an empty DataFrame if no metadata exists
            columns=[
                "year",
                "wr",
                "month",
                "revision_calendar_tab_1",
                "revision_calendar_tab_2",
                "benchmark_revision",
                "base_year",
                "base_year_affected",
            ]
        )

    # 2) Read processed years from record
    processed_years = _read_records_2(record_folder, record_txt)

    # 3) List years to process
    years = [
        d
        for d in sorted(os.listdir(input_pdf_folder))
        if os.path.isdir(os.path.join(input_pdf_folder, d))
        and d != "_quarantine"
    ]
    years_to_process = [y for y in years if y not in processed_years]

    new_rows = []

    # 4) Extract revision data from PDF files
    for year in years_to_process:
        year_folder = os.path.join(input_pdf_folder, year)
        pdf_files = sorted(
            [f for f in os.listdir(year_folder) if f.endswith(".pdf")],
            key=lambda x: int(re.search(r"ns-(\d+)-", x).group(1)),
        )

        for month_idx, pdf_filename in enumerate(pdf_files, start=1):
            pdf_path = os.path.join(year_folder, pdf_filename)

            # Extract wr_number and year from the PDF filename
            m = re.search(r"ns-(\d{1,2})-(\d{4})", pdf_filename)
            if not m:
                continue
            wr_number = int(m.group(1))
            year_int = int(m.group(2))

            # Extract numbers from PDF pages
            rev1, rev2 = _extract_wr_update_from_pdf(pdf_path)

            # Build the row for new data (base_year will be filled later)
            new_rows.append(
                {
                    "year": year_int,
                    "wr": wr_number,
                    "month": month_idx,
                    "revision_calendar_tab_1": int(rev1) if str(rev1).isdigit() else np.nan,
                    "revision_calendar_tab_2": int(rev2) if str(rev2).isdigit() else np.nan,
                    "benchmark_revision": (
                        1 if (str(rev1).isdigit() and str(rev2).isdigit() and int(rev1) == int(rev2)) else 0
                    ),
                    # Temp placeholders, will fill base_year later for all new rows together
                    "base_year": np.nan,
                    "base_year_affected": 0,
                }
            )

    # 5) If there are no new rows, return the current metadata
    if not new_rows:
        print("No new revisions to process.")
        return metadata

    # 6) Build a DataFrame for the new rows
    new_df = pd.DataFrame(new_rows)

    # 7) Apply base-year mapping only to new rows (new_df)
    new_df = apply_base_years_block(new_df, base_year_list)

    # 8) Mark where base_year has changed only inside new_df
    new_df = mark_base_year_affected(new_df)

    # 9) Concatenate the old metadata with the new rows
    updated = pd.concat([metadata, new_df], ignore_index=True)

    # 10) Enforce dtypes: use Int64 for integer columns that allow NA
    int_cols = [
        "year",
        "wr",
        "month",
        "benchmark_revision",
        "base_year",
        "base_year_affected",
    ]
    for col in int_cols:
        if col in updated.columns:
            updated[col] = updated[col].astype("Int64")

    # 11) For revision columns, allow NA and convert to Int64
    for col in ["revision_calendar_tab_1", "revision_calendar_tab_2"]:
        if col in updated.columns:
            updated[col] = pd.to_numeric(updated[col], errors="coerce").astype("Int64")

    # 12) Save the updated metadata back to the CSV file
    updated.to_csv(metadata_path, index=False)
    print(f"💾📋 Updated metadata saved to {metadata_path}")

    # 13) Update the processed records
    _write_records_2(record_folder, record_txt, processed_years + years_to_process)

    # 14) Summary
    elapsed_time = round(time.time() - start_time)
    print(f"\n📊 Summary (Metadata Update):")
    print(f"📂 {len(years)} years found for processing")
    print(f"🗃️ Already processed years: {len(processed_years)}")
    print(f"🔹 New years processed: {len(years_to_process)}")
    print(f"⏱️ Total elapsed time: {elapsed_time} seconds")

    return updated


# ==============================================================================================
# SECTION 5.1 Generating adjusted RTDs by removing revisions affected by base years (based on metadata)
# ==============================================================================================
# This section applies base-year adjustments to real-time GDP data. 
# It replaces non-NaN values above mapped rows for relevant target-period columns and marks values as "invalid" when base-year changes are detected.

# ++++++++++++++++++++++++++++++++++++++++++++++++
# Libraries
# ++++++++++++++++++++++++++++++++++++++++++++++++

# import os                                                                 # [already imported and documented in section 1]
# import time                                                               # [already imported and documented in section 3.2]
# import pandas as pd                                                       # [already imported and documented in section 3.1]

# _________________________________________________________________________
# Function to apply base-year sentinel
def apply_base_year_sentinel(
    base_year_list: list[str],
    sentinel: float = -999999.0,
    output_data_subfolder: str = ".",
    csv_file_labels: list[str] = None,                                                  # List of CSV file labels (monthly and quarterly files)
) -> dict:
    """
    Efficiently applies the base-year adjustment:
    - Replaces non-NaN values above mapped rows in the relevant tp_* columns.
    - Ensures columns to the left of the mapped value are not replaced.
    - Leaves NaN values untouched.

    Args:
        base_year_list: List of vintage strings (e.g., ["2000m7", "2014m3"]).
        sentinel: Numeric value to mark “invalid due to base-year change”.
        output_data_subfolder: Folder where the concatenated CSV files are located.
        csv_file_labels: List of CSV file labels to process (e.g., ["monthly_gdp_rtd.csv", "quarterly_annual_gdp_rtd.csv"]).

    Returns:
        A dictionary with filenames as keys and updated pandas DataFrames as values.
    """
    start_time = time.time()  # Timer to measure elapsed time
    print("\n🔧 Starting base-year sentinel application...")

    if csv_file_labels is None:
        csv_file_labels = ["monthly_gdp_rtd.csv", "quarterly_annual_gdp_rtd.csv"]        # Default file labels

    processed_data = {}                                                                  # Dictionary to store the processed data

    # Iterate over all CSV file labels
    for csv_file_label in csv_file_labels:
        # 1) Load the CSV
        csv_path = os.path.join(output_data_subfolder, csv_file_label)
        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"File not found at: {csv_path}")                   # Raise error if file not found

        df = pd.read_csv(csv_path)                                                      # Read the CSV into a DataFrame

        # 2) Basic validation for required columns
        if "industry" not in df.columns or "vintage" not in df.columns:
            raise ValueError("CSV must have 'industry' and 'vintage' columns.")         # Ensure the necessary columns are present

        df["industry"] = df["industry"].astype(str)                                     # Ensure 'industry' column is of type string
        df["vintage"]  = df["vintage"].astype(str)                                      # Ensure 'vintage' column is of type string

        # 3) Identify all tp_* columns in the dataset
        tp_cols = [col for col in df.columns if col.startswith("tp_")]                  # Filter columns that start with 'tp_'

        # 4) Process each base-year vintage in the base_year_list
        for by_vintage in base_year_list:
            by_vintage = str(by_vintage)                                                # Ensure vintage is a string

            # Find rows where the vintage matches the base-year
            mask_v = df["vintage"] == by_vintage
            if not mask_v.any():                                                        # Skip if no matching rows are found
                continue

            # Get the index of the first base-year row
            base_idx = df.index[mask_v].tolist()[0]

            # 5) Identify relevant tp_* columns based on non-NaN values in the base-year row
            relevant_cols = []
            for col in tp_cols:
                if pd.notna(df.loc[base_idx, col]):                                     # Check if the value in the base year row is not NaN
                    relevant_cols.append(col)                                           # Add to the relevant columns list

            # 6) Replace non-NaN values above the base-row in the relevant columns
            for col in relevant_cols:
                for i in range(base_idx):
                    if pd.notna(df.loc[i, col]):                                        # Only replace non-NaN values
                        df.loc[i, col] = sentinel                                       # Apply the sentinel value for invalid entries

        # 7) Enforce dtypes at the end: industry/vintage str, tp_* float
        for col in tp_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")                           # Convert tp_* columns to numeric type, coerce errors to NaN

        # 8) Save the updated DataFrame to a new CSV file
        adjusted_csv_file_label = f"by_adjusted_{csv_file_label}"
        adjusted_csv_path = os.path.join(output_data_subfolder, adjusted_csv_file_label)
        df.to_csv(adjusted_csv_path, index=False)                                       # Save the adjusted data to a new CSV file

        # Store the processed DataFrame in the dictionary
        processed_data[adjusted_csv_file_label] = df

    # 9) Summary
    elapsed_time = round(time.time() - start_time)
    print(f"\n📊 Summary (Base-Year Sentinel Application):")
    print(f"📂 {len(csv_file_labels)} files processed")
    print(f"⏱️ Total elapsed time: {elapsed_time} seconds")

    return processed_data                                                               # Return the dictionary of processed data


# ==============================================================================================
# SECTION 5.2 Generating benchmark RTD for revisions affected by benchmarking procedures (based on metadata)
# ==============================================================================================
# This section generates adjusted Real-Time Data (RTD) for GDP, removing growth rates affected by base-year changes.
# It processes the metadata and applies the appropriate adjustments to the target period columns in the dataset.

# ++++++++++++++++++++++++++++++++++++++++++++++++
# Libraries
# ++++++++++++++++++++++++++++++++++++++++++++++++

# import os                                                                 # [already imported and documented in section 1]
# import re                                                                 # [already imported and documented in section 1]
# import time                                                               # [already imported and documented in section 3.2]
# import pandas as pd                                                       # [already imported and documented in section 3.1]

# _________________________________________________________________________
# Function to generate benchmark datasets from real-time GDP releases
def convert_to_benchmark_dataset(
    output_data_subfolder: str,
    csv_file_labels: list[str],
    metadata_folder: str,
    wr_metadata_csv: str,
    record_folder: str,
    record_txt: str,
    benchmark_dataset_csv: list[str],
) -> dict:
    """
    Generates benchmark datasets by applying the benchmark revision mapping
    to existing real-time GDP releases.

    - Reads filtered metadata to identify benchmark vintages
    - Normalizes vintage identifiers to ensure matching consistency
    - Replaces non-NaN growth rate values with:
        * 1.0 for vintages identified as benchmark
        * 0.0 for all other vintages
    - Ensures all target-period columns are of float type
    - Saves the resulting benchmark dataset(s) to CSV
    - Updates record files for processed datasets

    Args:
        output_data_subfolder: Folder where the input and output CSV files are stored
        csv_file_labels: List of base dataset file labels (without .csv extension)
        metadata_folder: Folder where the metadata CSV file is located
        wr_metadata_csv: File name of the metadata CSV
        record_folder: Folder where processed file records are stored
        record_txt: Record file name tracking processed datasets
        benchmark_dataset_csv: List of output file labels for benchmark datasets

    Returns:
        Dictionary where keys are benchmark dataset labels
        and values are the corresponding processed pandas DataFrames.
    """

    start_time = time.time()  # Timer to measure elapsed time
    print("\n🔄📊 Starting benchmark dataset generation...")

    # 1) Validate input length
    if len(csv_file_labels) != len(benchmark_dataset_csv):
        raise ValueError("csv_file_labels and benchmark_dataset_csv must have the same length.")    # Ensure matching lengths

    processed_files = _read_records_2(record_folder, record_txt)
    processed_results = {}

    # 2) Load and filter metadata
    metadata_path = os.path.join(metadata_folder, wr_metadata_csv)
    if not os.path.exists(metadata_path):
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}")                        # Raise error if metadata file is missing

    metadata = pd.read_csv(metadata_path)
    metadata_filtered = metadata[metadata["benchmark_revision"] == 1]                               # Filter metadata for benchmark revisions

    # 3) Build benchmark mapping dictionary
    benchmark_map = {
        f"{str(year)}m{str(month).zfill(2)}": True
        for year, month in zip(
            metadata_filtered["year"].astype(str),
            metadata_filtered["month"].astype(int),
        )
    }

    # 4) Normalize benchmark keys (e.g., 1997m02 -> 1997m2)
    def normalize_key(v):
        match = re.match(r"^(\d{4})m0?(\d{1,2})$", v)
        if match:
            y, m = match.groups()
            return f"{y}m{int(m)}"
        return v

    benchmark_map = {normalize_key(k): v for k, v in benchmark_map.items()}

    # 5) Process each dataset
    for csv_label, benchmark_label in zip(csv_file_labels, benchmark_dataset_csv):
        csv_path = os.path.join(output_data_subfolder, f"{csv_label}.csv")

        if csv_label in processed_files:
            print(f"⏭️ Skipping already processed file: {csv_label}.csv")
            continue

        if not os.path.exists(csv_path):
            raise FileNotFoundError(f"File not found: {csv_path}")                                  # Raise error if dataset file is missing

        print(f"\n🔹 Processing dataset: {csv_label}.csv")
        df = pd.read_csv(csv_path)

        if "vintage" not in df.columns:
            raise ValueError(f"The file '{csv_label}.csv' must contain a 'vintage' column.")        # Ensure 'vintage' column exists

        # 5.1 Normalize vintage identifiers
        df["vintage"] = (
            df["vintage"]
            .astype(str)
            .str.strip()
            .str.lower()
            .str.replace(r"[^0-9m]", "", regex=True)
        )  # Normalize vintage strings to consistent format

        # 5.2 Identify columns to process
        tp_cols = [c for c in df.columns if c.startswith("tp_")]                                    # Identify target period columns
        if not tp_cols:
            tp_cols = [c for c in df.columns if c not in ["industry", "vintage"]]                   # Fallback to non-'industry', 'vintage' columns

        # Ensure numeric type
        df[tp_cols] = df[tp_cols].astype(float)                                                     # Convert tp_* columns to float

        # 5.3 Diagnostic report: vintage matching
        vintages_df = set(df["vintage"].unique())                                                   # Get unique vintages from the data
        vintages_map = set(benchmark_map.keys())                                                    # Get benchmark vintages from the map
        matched = vintages_map.intersection(vintages_df)                                            # Identify matched vintages
        unmatched = vintages_map - vintages_df                                                      # Identify unmatched vintages

        print(f"🔍 Matching diagnostics for {csv_label}.csv")
        print(f"   Total vintages in data: {len(vintages_df)}")
        print(f"   Total benchmark vintages: {len(vintages_map)}")
        print(f"   ✅ Matched vintages: {len(matched)} / {len(vintages_map)}")
        print(f"   ⚠️ Unmatched vintages: {len(unmatched)}")
        if unmatched:
            print(f"   Example unmatched vintages: {list(sorted(unmatched))[:10]}")

        # 5.4 Apply vectorized replacement logic
        mask_benchmark = df["vintage"].isin(vintages_map)                                           # Mask for benchmark vintages

        # Replace all non-NaN values with 0.0
        df[tp_cols] = df[tp_cols].where(df[tp_cols].isna(), 0.0)                                    # Replace non-NaN values with 0.0

        # Replace benchmark vintages with 1.0
        if mask_benchmark.any():
            df.loc[mask_benchmark, tp_cols] = df.loc[mask_benchmark, tp_cols].where(
                df.loc[mask_benchmark, tp_cols].isna(), 1.0
            )

        # Enforce float type
        df[tp_cols] = df[tp_cols].astype(float)                                                     # Ensure tp_* columns are float type

        # 5.5 Save processed dataset
        output_path = os.path.join(output_data_subfolder, f"{benchmark_label}.csv")
        df.to_csv(output_path, index=False)                                                         # Save the adjusted dataset to a new file
        processed_results[benchmark_label] = df

        processed_files.append(csv_label)                                                           # Add to processed files
        print(f"💾 Saved benchmark dataset: {output_path}")
        print(f"   Rows: {len(df)}, Columns: {len(df.columns)}")

    # 6) Update records and summarize
    _write_records_2(record_folder, record_txt, processed_files)                                    # Update the record file

    elapsed_time = round(time.time() - start_time)                                                  # Measure elapsed time
    print(f"\n📊 Summary (Benchmark Dataset Generation):")
    print(f"📂 {len(csv_file_labels)} files processed")
    print(f"🗃️ Record updated: {record_txt}")
    print(f"⏱️ Total elapsed time: {elapsed_time} seconds")

    return processed_results                                                                        # Return the dictionary of processed benchmark datasets



# ##############################################################################################
# SECTION 6 Converting RTD to releases dataset
# ##############################################################################################

# This section is responsible for converting real-time GDP growth rate datasets into releases datasets.
# It processes each dataset, aligns the non-NaN values for each 'industry' and 'vintage', and removes 
# growth rates affected by base-year changes based on metadata. The final output is saved as a new dataset 
# where each industry has its corresponding release values.

# ++++++++++++++++++++++++++++++++++++++++++++++++
# Libraries
# ++++++++++++++++++++++++++++++++++++++++++++++++

# import os                                                                 # [already imported and documented in section 1]
# import time                                                               # [already imported and documented in section 3.2]
# import pandas as pd                                                       # [already imported and documented in section 3.1]
# import numpy as np                                                        # [already imported and documented in section 3.1]


# _________________________________________________________________________
# Function to convert real-time GDP growth rates dataset(s) into releases dataset(s)
def convert_to_releases_dataset(
    output_data_subfolder: str,
    csv_file_labels: list[str],           # Input CSV files (without .csv extension)
    record_folder: str,                   # Folder where the record txt is stored
    record_txt: str,                      # Name of record txt
    releases_dataset_csv: list[str],      # Output CSV filenames for releases datasets
) -> dict:
    """
    Converts real-time GDP growth rates dataset(s) into releases dataset(s).
    - Reads dataset for each file in csv_file_labels.
    - Sorts by 'industry' and 'vintage'.
    - For each 'industry', aligns the first, second, ... non-NaN values across tp_* columns.
    - Adds a 'release' column (1, 2, 3, ...).
    - Drops fully NaN rows.
    - Removes the 'vintage' column (no longer needed).
    - Saves the converted dataset with its corresponding release file name.

    Args:
        output_data_subfolder: Folder where the CSV files are located and saved.
        csv_file_labels: List of input CSV filenames (without .csv extension).
        record_folder: Folder where record files are kept.
        record_txt: File name for the record of processed files.
        releases_dataset_csv: List of output CSV filenames (without .csv extension).

    Returns:
        Dictionary of output file names (key) and their corresponding DataFrames (value).
    """
    start_time = time.time()  # Timer to measure elapsed time
    print("\n🧮 Starting conversion to releases dataset(s)...")

    # 1) Validation: ensure both lists match in length
    if len(csv_file_labels) != len(releases_dataset_csv):                                                               # Check if input and output files match in length
        raise ValueError("csv_file_labels and releases_dataset_csv must have the same length.")

    processed_files = _read_records_2(record_folder, record_txt)                                                        # Load processed files from record
    processed_results = {}

    # 2) Iterate over each pair of input/output filenames
    for csv_label, release_label in zip(csv_file_labels, releases_dataset_csv):
        csv_path = os.path.join(output_data_subfolder, f"{csv_label}.csv")                                              # Construct path for the input CSV
        
        # Check if file has already been processed
        if csv_label in processed_files:                                                                                # Skip if file has already been processed
            print(f"⏭️📄 Skipping already processed file: {csv_label}.csv")
            continue  # Skip to the next file if this one has been processed

        if not os.path.exists(csv_path):                                                                                # Raise error if file does not exist
            raise FileNotFoundError(f"File not found: {csv_path}")

        print(f"\n🔄 Processing file: {csv_label}.csv")

        df = pd.read_csv(csv_path)                                                                                      # Read the input CSV file

        # 3) Ensure required columns exist
        if "industry" not in df.columns or "vintage" not in df.columns:                                                 # Ensure 'industry' and 'vintage' columns are present
            raise ValueError(f"CSV file '{csv_label}.csv' must have 'industry' and 'vintage' columns.")

        df["industry"] = df["industry"].astype(str)                                                                     # Convert 'industry' column to string type
        df["vintage"]  = df["vintage"].astype(str)                                                                      # Convert 'vintage' column to string type

        # 4) Extract year and month from vintage and sort by chronological order
        df["year"] = df["vintage"].str.extract(r"(\d{4})").astype(int)                                                  # Extract year from 'vintage'
        df["month"] = df["vintage"].str.extract(r"m(\d{1,2})").astype(int)                                              # Extract month from 'vintage'
        df = df.sort_values(["industry", "year", "month"], ignore_index=True)                                           # Sort by 'industry', 'year', and 'month'

        # 5) Identify tp_* columns
        tp_cols = [col for col in df.columns if col.startswith("tp_")]                                                  # Identify columns starting with 'tp_'
        releases_df_list = []                                                                                           # List to hold processed data for each industry

        # 6) Process each industry group independently
        for industry, group in df.groupby("industry"):                                                                  # Group data by 'industry'
            group = group.reset_index(drop=True)                                                                        # Reset index for each group

            # Convert tp_* data into NumPy for efficient column-wise processing
            tp_values = group[tp_cols].to_numpy()                                                                       # Convert target period columns to NumPy array

            # Determine the maximum possible number of releases
            max_releases = np.max([np.count_nonzero(~np.isnan(tp_values[:, i])) for i in range(tp_values.shape[1])])    # Find max number of releases

            # Create a structure for the release-aligned data
            industry_releases = np.full((max_releases, len(tp_cols)), np.nan)                                           # Create empty array for releases data

            # Align non-NaN values vertically (release by release)
            for j, col in enumerate(tp_cols):
                non_nan_vals = tp_values[~np.isnan(tp_values[:, j]), j]                                                 # Extract non-NaN values for the column
                industry_releases[:len(non_nan_vals), j] = non_nan_vals                                                 # Align values vertically in the array

            # Build the DataFrame for this industry
            industry_df = pd.DataFrame(industry_releases, columns=tp_cols)                                              # Convert array back to DataFrame
            industry_df.insert(0, "release", range(1, len(industry_df) + 1))                                            # Insert 'release' column
            industry_df.insert(0, "industry", industry)                                                                 # Insert 'industry' column

            # Drop rows that are completely NaN (after alignment)
            industry_df.dropna(how="all", subset=tp_cols, inplace=True)                                                 # Remove rows where all 'tp_*' columns are NaN

            releases_df_list.append(industry_df)                                                                        # Add the industry DataFrame to the list

        # 7) Concatenate all industries
        releases_df = pd.concat(releases_df_list, ignore_index=True)                                                    # Concatenate all industry DataFrames

        # 8) Transpose the dataset: Create target_period and rearrange industries with releases
        releases_df = releases_df.melt(id_vars=["industry", "release"], value_vars=tp_cols,
                                        var_name="target_period", value_name="value")                                   # Reshape DataFrame

        # Clean target_period by removing the 'tp_' prefix
        releases_df["target_period"] = releases_df["target_period"].str.replace("tp_", "", regex=False)                 # Remove 'tp_' from 'target_period'

        # 9) Pivot the table to get each industry with its corresponding releases in columns
        releases_df_pivot = releases_df.pivot_table(index="target_period", columns=["industry", "release"], values="value")

        # Flatten the multi-level columns and rename them
        releases_df_pivot.columns = [f"{industry}_{release}" for industry, release in releases_df_pivot.columns]        # Flatten column names
        releases_df_pivot.reset_index(inplace=True)                                                                     # Reset the index to make 'target_period' a column

        # 10) Sort target_period to match the chronological order (same as vintage sorting)
        releases_df_pivot["year"] = releases_df_pivot["target_period"].str.extract(r"(\d{4})").astype("Int64")          # Extract year
        releases_df_pivot["month"] = releases_df_pivot["target_period"].str.extract(r"m(\d{1,2})").astype("Int64")      # Extract month
        releases_df_pivot = releases_df_pivot.sort_values(["year", "month"], ignore_index=True)                         # Sort by year and month

        # Remove "year" and "month" columns
        releases_df_pivot.drop(columns=["year", "month"], inplace=True)                                                 # Drop the year and month columns after sorting

        # 11) Save the release dataset
        release_path = os.path.join(output_data_subfolder, f"{release_label}.csv")                                      # Construct path for output CSV
        releases_df_pivot.to_csv(release_path, index=False)                                                             # Save the DataFrame as a CSV
        processed_results[release_label] = releases_df_pivot                                                            # Store the result in the dictionary

        # 12) Update processed record
        processed_files.append(csv_label)                                                                               # Add the processed file to the record
        _write_records_2(record_folder, record_txt, processed_files)                                                    # Update the record of processed files

        print(f"💾 Saved release dataset: {release_path}")
        print(f"   🧱 Rows: {len(releases_df_pivot)}, Industries: {releases_df_pivot['target_period'].nunique()}")

    # 13) Summary
    elapsed_time = round(time.time() - start_time)                                                                      # Measure elapsed time
    print(f"\n📊 Summary (Conversion to Releases Dataset):")
    print(f"📂 {len(csv_file_labels)} files processed")                                                             
    print(f"🗃️ Records updated: {record_txt}")                                                                     
    print(f"⏱️ Total elapsed time: {elapsed_time} seconds")                                                          

    return processed_results                                                                                            # Returning dictionary of processed results (DataFrames)

