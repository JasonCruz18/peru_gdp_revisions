#*********************************************************************************************
# Functions for new_gdp_dataset.ipynb
#*********************************************************************************************

################################################################################################
# Section 1. PDF Downloader
################################################################################################

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# LIBRARIES
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

import os                                                               # Path utilities and directory management
import re                                                               # Filename parsing and natural sorting helpers
import time                                                             # Execution timing and simple profiling
import random                                                           # Randomized backoff/wait durations
import shutil                                                           # High-level file operations (move/copy/rename/delete)

import logging                                                          # Unified logging to console and file
from logging.handlers import RotatingFileHandler                        # Log rotation

import requests                                                         # HTTP client for downloading files
from requests.adapters import HTTPAdapter                               # Attach retry/backoff to requests
from urllib3.util.retry import Retry                                    # Exponential backoff policy

import pygame                                                           # Audio playback for notification sounds

from selenium import webdriver                                          # Browser automation
from selenium.webdriver.common.by import By                             # Element location strategies
from selenium.webdriver.support.ui import WebDriverWait                 # Explicit waits
from selenium.webdriver.support import expected_conditions as EC        # Wait conditions
from selenium.common.exceptions import StaleElementReferenceException   # Dynamic DOM handling

from webdriver_manager.chrome import ChromeDriverManager                # ChromeDriver provisioning
from selenium.webdriver.chrome.options import Options as ChromeOptions  # Chrome options
from selenium.webdriver.chrome.service import Service as ChromeService  # Chrome service

from webdriver_manager.firefox import GeckoDriverManager                  # GeckoDriver provisioning
from selenium.webdriver.firefox.options import Options as FirefoxOptions  # Firefox options
from selenium.webdriver.firefox.service import Service as FirefoxService  # Firefox service

from webdriver_manager.microsoft import EdgeChromiumDriverManager   # EdgeDriver provisioning
from selenium.webdriver.edge.options import Options as EdgeOptions   # Edge options
from selenium.webdriver.edge.service import Service as EdgeService   # Edge service


# --------------------------
# Module-level configuration
# --------------------------

# HTTP
REQUEST_CHUNK_SIZE  = 128                                     # Bytes per chunk when streaming downloads
REQUEST_TIMEOUT     = 60                                      # Seconds for connect+read timeouts
DEFAULT_RETRIES     = 3                                       # Total retries for transient HTTP errors
DEFAULT_BACKOFF     = 0.5                                     # Exponential backoff factor (0.5, 1.0, 2.0, ...)
RETRY_STATUSES      = (429, 500, 502, 503, 504)               # Retry on rate limits and server errors

# Selenium / Browser
PAGE_LOAD_TIMEOUT       = 30                                  # Seconds to wait for page loads
EXPLICIT_WAIT_TIMEOUT   = 60                                  # Seconds for WebDriverWait

# Downloader pacing
DEFAULT_MIN_WAIT    = 5.0                                     # Lower bound for random delay between downloads (seconds)
DEFAULT_MAX_WAIT    = 10.0                                    # Upper bound for random delay between downloads (seconds)

# Logging
LOG_PATH        = "logs/1_pdf_downloader.log"                   # Rotating log file
LOG_MAX_BYTES   = 1_000_000                                   # ~1 MB per log segment
LOG_BACKUPS     = 3                                           # Keep last N rotated log files


# --------------------------------
# Logging setup (console + file)
# --------------------------------

os.makedirs(os.path.dirname(LOG_PATH), exist_ok=True)
_logger = logging.getLogger(__name__)
_logger.setLevel(logging.INFO)

_file_handler = RotatingFileHandler(
    LOG_PATH, maxBytes=LOG_MAX_BYTES, backupCount=LOG_BACKUPS, encoding="utf-8"
)
_fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
_file_handler.setFormatter(_fmt)
_logger.addHandler(_file_handler)

def log_info(msg: str) -> None:
    """Log informational message to console and rotating log file."""
    print(msg)
    _logger.info(msg)

def log_warn(msg: str) -> None:
    """Log warning message to console and rotating log file."""
    print(msg)
    _logger.warning(msg)

def log_error(msg: str) -> None:
    """Log error message to console and rotating log file."""
    print(msg)
    _logger.error(msg)


# --------------------------------
# Function: get_http_session
# HTTP session with retry/backoff
# --------------------------------

def get_http_session(
    total: int = DEFAULT_RETRIES,
    backoff: float = DEFAULT_BACKOFF,
    statuses: tuple = RETRY_STATUSES,
) -> requests.Session:
    """
    Create a requests.Session configured with retries and exponential backoff
    for transient HTTP errors (e.g., 429/5xx). Safe drop-in for GET requests.
    """
    retry = Retry(
        total=total,
        read=total,
        connect=total,
        backoff_factor=backoff,                         # 0.5s, 1.0s, 2.0s, ... between retries
        status_forcelist=statuses,                      # Retry on these HTTP status codes
        allowed_methods=frozenset(["GET", "HEAD"]),     # Idempotent methods only
        raise_on_status=False,                          # Do not raise; let caller inspect status_code
    )
    sess = requests.Session()
    adapter = HTTPAdapter(max_retries=retry)
    sess.mount("https://", adapter)                     # Apply retry policy to HTTPS
    sess.mount("http://", adapter)                      # Apply retry policy to HTTP
    return sess


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# FUNCTIONS
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# _________________________________________________________________________
# Function: load_alert_track
def load_alert_track(alert_track_folder: str) -> str | None:
    """
    Load a random .mp3 file from the given folder for audio alerts.
    If no .mp3 files are present, proceed without an alert track.

    Args:
        alert_track_folder (str): Directory expected to contain one or more .mp3 files.

    Returns:
        str | None: Absolute path to a randomly selected .mp3 file, or None if unavailable.
    """
    os.makedirs(alert_track_folder, exist_ok=True)      # Ensure folder exists

    tracks = [f for f in os.listdir(alert_track_folder) if f.lower().endswith(".mp3")]  # Filter .mp3 files
    if not tracks:
        log_warn("🔇 No .mp3 files found in 'alert_track/'. Continuing without audio alerts.")
        return None

    alert_track_path = os.path.join(alert_track_folder, random.choice(tracks))  # Random track selection
    pygame.mixer.music.load(alert_track_path)           # Preload into mixer
    return alert_track_path


# _________________________________________________________________________
# Function: play_alert_track
def play_alert_track() -> None:
    """Start playback of the currently loaded alert track."""
    pygame.mixer.music.play()                           # Non-blocking playback


# _________________________________________________________________________
# Function: stop_alert_track
def stop_alert_track() -> None:
    """Stop playback of the current alert track."""
    pygame.mixer.music.stop()                           # Immediate stop


# _________________________________________________________________________
# Function: random_wait
def random_wait(min_time: float, max_time: float) -> None:
    """
    Pause execution for a random duration within [min_time, max_time].

    Args:
        min_time (float): Minimum wait time in seconds.
        max_time (float): Maximum wait time in seconds.
    """
    wait_time = random.uniform(min_time, max_time)      # Inclusive random delay
    log_info(f"⏳ Waiting {wait_time:.2f} seconds...")
    time.sleep(wait_time)                               # Sleep for the computed duration


# _________________________________________________________________________
# Function: init_driver
def init_driver(browser: str = "chrome", headless: bool = False):
    """
    Initialize and return a Selenium WebDriver instance.

    Args:
        browser (str): Browser engine to use. Supported: 'chrome' (default), 'firefox', 'edge'.
        headless (bool): Run the browser in headless mode if True.

    Returns:
        WebDriver: Configured Selenium WebDriver instance.
    """
    b = browser.lower()

    if b == "chrome":
        options = ChromeOptions()
        if headless:
            options.add_argument("--headless=new")      # Modern headless mode
        options.add_argument("--no-sandbox")            # Container stability
        options.add_argument("--disable-dev-shm-usage") # Avoid /dev/shm issues in containers
        service = ChromeService(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)

    elif b == "firefox":
        fopts = FirefoxOptions()
        if headless:
            fopts.add_argument("-headless")             # Firefox headless flag
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

    else:
        raise ValueError("Supported browsers are: 'chrome', 'firefox', 'edge'.")

    driver.set_page_load_timeout(PAGE_LOAD_TIMEOUT)     # Hard limit for page loads
    return driver


# _________________________________________________________________________
# Function: download_pdf
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
        driver: Active Selenium WebDriver instance.
        pdf_link: Selenium WebElement (anchor) pointing to the PDF.
        wait (WebDriverWait): Explicit wait instance for window events.
        download_counter (int): Ordinal for progress messages.
        raw_pdf_folder (str): Output directory for the downloaded PDF file.
        download_record_folder (str): Directory containing the record text file.
        download_record_txt (str): Filename of the record log (e.g., 'downloaded_pdfs.txt').

    Returns:
        bool: True if the file was successfully downloaded and recorded; False otherwise.
    """
    driver.execute_script("arguments[0].click();", pdf_link)         # Open link via JS (handles hidden/overlayed links)
    wait.until(EC.number_of_windows_to_be(2))                         # Wait for new tab (2 windows total)
    windows = driver.window_handles                                   # Capture window handles
    driver.switch_to.window(windows[1])                               # Focus new tab

    new_url = driver.current_url                                      # Direct PDF URL (after any redirects)
    file_name = os.path.basename(new_url)                             # Use server-provided filename
    destination_path = os.path.join(raw_pdf_folder, file_name)        # Local path to save

    session = get_http_session()                                      # Session with retries/backoff
    try:
        response = session.get(new_url, stream=True, timeout=REQUEST_TIMEOUT)  # Stream to avoid large memory use
        if response.status_code == 200:
            os.makedirs(raw_pdf_folder, exist_ok=True)                # Ensure destination exists
            with open(destination_path, "wb") as fh:
                for chunk in response.iter_content(chunk_size=REQUEST_CHUNK_SIZE):
                    if chunk:                                         # Ignore keep-alive chunks
                        fh.write(chunk)
        else:
            log_error(f"{download_counter}. ❌ Error downloading {file_name}. HTTP {response.status_code}")
            success = False
            driver.close(); driver.switch_to.window(windows[0])       # Cleanup: close tab and refocus
            return success
    except requests.RequestException as ex:
        log_error(f"{download_counter}. ❌ Network error downloading {file_name}: {ex}")
        success = False
        driver.close(); driver.switch_to.window(windows[0])           # Cleanup: close tab and refocus
        return success

    # Update the record log (chronologically: year → issue)
    record_path = os.path.join(download_record_folder, download_record_txt)
    records: list[str] = []
    if os.path.exists(record_path):
        with open(record_path, "r", encoding="utf-8") as f:
            records = [ln.strip() for ln in f if ln.strip()]          # Strip blanks and newlines

    if file_name not in records:
        records.append(file_name)                                     # Append if not already present

    def _ns_key(s: str):
        base = os.path.splitext(os.path.basename(s))[0]               # Drop extension
        m = re.search(r"ns-(\d{2})-(\d{4})", base, re.I)              # Capture issue+year (ns-XX-YYYY)
        if not m:                                                     # Unknown pattern → sort last, stable by name
            return (9999, 9999, base)
        issue, year = int(m.group(1)), int(m.group(2))
        return (year, issue)                                          # Primary sort by year, then issue

    records.sort(key=_ns_key)                                         # Stable chronological order
    os.makedirs(download_record_folder, exist_ok=True)
    with open(record_path, "w", encoding="utf-8") as f:
        f.write("\n".join(records) + ("\n" if records else ""))       # Ensure trailing newline if non-empty

    log_info(f"{download_counter}. ✅ Downloaded: {file_name}")
    success = True

    driver.close(); driver.switch_to.window(windows[0])               # Return focus to main window
    return success


# _________________________________________________________________________
# Function: pdf_downloader
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
    Download BCRP Weekly Report PDFs (first link per month), keep clean numbering,
    play batch alerts, and print a summary.

    Args:
        bcrp_url (str): URL of the BCRP Weekly Reports page.
        raw_pdf_folder (str): Destination folder for PDFs.
        download_record_folder (str): Folder containing the record file.
        download_record_txt (str): Record filename tracking downloaded PDFs.
        alert_track_folder (str): Folder with .mp3 files for notifications.
        max_downloads (int | None): Upper limit on new downloads (None = no limit).
        downloads_per_batch (int): Number of files between alert prompts.
        headless (bool): Run browser headless if True.
    """
    start_time = time.time()

    log_info("\n📥 Starting PDF downloader for BCRP Weekly Reports...\n")
    pygame.mixer.init()                                               # Initialize audio mixer
    alert_track_path = load_alert_track(alert_track_folder)           # Optional alert sound

    record_path = os.path.join(download_record_folder, download_record_txt)
    downloaded_files = set()
    if os.path.exists(record_path):
        with open(record_path, "r", encoding="utf-8") as f:
            downloaded_files = set(f.read().splitlines())             # Prior downloads (one per line)

    driver = init_driver(headless=headless)
    wait = WebDriverWait(driver, EXPLICIT_WAIT_TIMEOUT)               # Explicit wait helper

    new_counter  = 0
    skipped_files: list[str] = []
    new_downloads = []                                                # (WebElement, filename)
    pdf_links = []                                                    # Keep for summary

    try:
        driver.get(bcrp_url)
        log_info("🌐 BCRP site opened successfully.")

        # Capture the UL containers that hold monthly links; we only take the first link per month
        month_ul_elems = wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, "#rightside ul.listado-bot-std-claros"))
        )
        log_info(f"🔎 Found {len(month_ul_elems)} WR blocks on page (one per month).\n")

        for ul in month_ul_elems:
            anchors = []
            try:
                anchors = ul.find_elements(By.TAG_NAME, "a")          # All anchors within this month block
            except Exception:
                pass
            if not anchors:
                continue
            pdf_links.append(anchors[0])                               # Take the first anchor only

        pdf_links = pdf_links[::-1]                                    # Oldest → newest for stable local order

        for link in pdf_links:
            try:
                file_url  = link.get_attribute("href")                 # Direct link to PDF file
                file_name = os.path.basename(file_url)                 # Server filename
            except Exception:
                continue

            if file_name in downloaded_files:
                skipped_files.append(file_name)                        # Already downloaded earlier
            else:
                new_downloads.append((link, file_name))                # Queue for download

        for i, (link, file_name) in enumerate(new_downloads, start=1):
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
                downloaded_files.add(file_name)
                new_counter += 1

            if (i % downloads_per_batch == 0) and alert_track_path:   # Batch checkpoint
                play_alert_track()
                user_input = input("⏸️ Continue? (y = yes, any other key = stop): ")  # Operator confirmation
                stop_alert_track()
                if user_input.lower() != "y":
                    log_warn("🛑 Download stopped by user.")
                    break

            if max_downloads and new_counter >= max_downloads:        # Respect cap if provided
                log_info(f"🏁 Download limit of {max_downloads} new PDFs reached.")
                break

            random_wait(DEFAULT_MIN_WAIT, DEFAULT_MAX_WAIT)           # Gentle pacing

    except StaleElementReferenceException:
        log_warn("⚠️ StaleElementReferenceException encountered. Consider re-running.")
    finally:
        driver.quit()
        log_info("\n👋 Browser closed.")

    # Keep the record file chronologically ordered (year → issue)
    try:
        if os.path.exists(record_path):
            with open(record_path, "r", encoding="utf-8") as f:
                records = [ln.strip() for ln in f if ln.strip()]      # Compact existing entries

            def _ns_key(s: str):
                base = os.path.splitext(os.path.basename(s))[0]
                m = re.search(r"ns-(\d{2})-(\d{4})", base, re.I)      # Expect ns-XX-YYYY pattern
                if not m:
                    return (9999, 9999, base)                          # Unknown pattern → sort last
                issue, year = int(m.group(1)), int(m.group(2))
                return (year, issue)

            records = sorted(set(records), key=_ns_key)               # De-dup + chronological sort
            os.makedirs(download_record_folder, exist_ok=True)
            with open(record_path, "w", encoding="utf-8") as f:
                f.write("\n".join(records) + ("\n" if records else ""))  # Trailing newline if non-empty
    except Exception as _e:
        log_warn(f"⚠️ Unable to re-sort record file: {_e}")

    elapsed_time = round(time.time() - start_time)
    total_links  = len(pdf_links)
    log_info("\n📊 Summary:")
    log_info(f"\n🔗 Total monthly links kept: {total_links}")
    if skipped_files:
        log_info(f"🗂️ {len(skipped_files)} already downloaded PDFs were skipped.")
    log_info(f"➕ Newly downloaded: {new_counter}")
    log_info(f"⏱️ {elapsed_time} seconds")


# _________________________________________________________________________
# Function: organize_files_by_year
def organize_files_by_year(raw_pdf_folder: str) -> None:
    """
    Move PDFs in `raw_pdf_folder` into subfolders named by year.
    The year is inferred from the first 4-digit token in the filename.

    Args:
        raw_pdf_folder (str): Directory containing the downloaded PDFs.
    """
    files = os.listdir(raw_pdf_folder)                   # Enumerate files in the root folder

    for file in files:
        name, _ext = os.path.splitext(file)              # Separate stem and extension
        year = None

        for part in name.split("-"):                      # Heuristic: look for any 4-digit token
            if part.isdigit() and len(part) == 4:
                year = part
                break

        if year:
            dest = os.path.join(raw_pdf_folder, year)    # Ensure a year subfolder exists
            os.makedirs(dest, exist_ok=True)
            shutil.move(os.path.join(raw_pdf_folder, file), dest)  # Move file into its year folder
        else:
            log_warn(f"⚠️ No 4-digit year detected in filename: {file}")

            
            
################################################################################################
# Section 2. Generate PDF input with key tables
################################################################################################

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# LIBRARIES
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

import os                                                     # Path utilities and directory management
import re                                                     # Pattern matching for NS codes and sorting
import time                                                   # Execution timing
import shutil                                                 # File moves for quarantine handling
import logging                                                # Logging to console and file
from logging.handlers import RotatingFileHandler              # Log rotation

import requests                                               # HTTP client for replacement downloads
import fitz                                                   # Lightweight PDF editing (PyMuPDF)
import ipywidgets as widgets                                  # Jupyter UI elements (used in this section's workflow)
from IPython.display import display                           # Render widgets in notebooks
from tqdm.notebook import tqdm                                # Progress bars in Jupyter
import pdfplumber                                             # Rich PDF text extraction (not heavily used here)
from PyPDF2 import PdfReader, PdfWriter                       # Page-level PDF edits (keep/select pages)

# --------------------------
# Module-level configuration
# --------------------------

LOG2_PATH       = "logs/2_input_pdfs_generator.log"               # Section 2 log file path (no extension by design)
LOG2_MAX_BYTES  = 1_000_000                                   # ~1 MB per segment
LOG2_BACKUPS    = 3                                           # Keep last N rotated logs

# --------------------------------
# Logging setup (console + file)
# --------------------------------

os.makedirs(os.path.dirname(LOG2_PATH), exist_ok=True)
_logger_input_pdfs = logging.getLogger("input_pdfs_generator")
_logger_input_pdfs.setLevel(logging.INFO)

_file_handler2 = RotatingFileHandler(
    LOG2_PATH, maxBytes=LOG2_MAX_BYTES, backupCount=LOG2_BACKUPS, encoding="utf-8"
)
_fmt2 = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
_file_handler2.setFormatter(_fmt2)
_logger_input_pdfs.addHandler(_file_handler2)

def log2_info(msg: str) -> None:
    print(msg)                                                # Mirror to console for notebooks/CLIs
    _logger_input_pdfs.info(msg)

def log2_warn(msg: str) -> None:
    print(msg)
    _logger_input_pdfs.warning(msg)

def log2_error(msg: str) -> None:
    print(msg)
    _logger_input_pdfs.error(msg)


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# FUNCTIONS
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# _________________________________________________________________________
# Function: replace_ns_pdfs
def replace_ns_pdfs(items, root_folder, record_folder, download_record_txt, quarantine=None):
    """
    Replace defective PDFs stored under year subfolders, keeping a consistent
    download record. Defective entries are intentionally kept in the TXT so the
    downloader will not fetch them again.

    Args:
        items (list[tuple[str, str, str]]): Triples of (year, defective_pdf, replacement_code).
            Example: [("2017","ns-08-2017.pdf","ns-07-2017"), ("2019","ns-23-2019.pdf","ns-22-2019")]
        root_folder (str): Base path containing year folders (e.g., raw_pdf).
        record_folder (str): Folder holding the download record TXT.
        download_record_txt (str): Record filename (e.g., 'downloaded_pdfs.txt').
        quarantine (str | None): Folder to move defective PDFs; if None, delete them.
    """
    pat = re.compile(r"^ns-(\d{1,2})-(\d{4})(?:\.pdf)?$", re.I)            # Flexible matcher for ns-<issue>-<year>[.pdf]

    def norm(c):
        m = pat.match(os.path.basename(c).lower())
        if not m:
            raise ValueError(f"Bad NS code: {c}")                          # Validate expected NS format
        return f"ns-{int(m.group(1)):02d}-{m.group(2)}"                    # Zero-pad issue (e.g., 7 → 07)

    def url(c):
        cc = norm(c)
        return f"https://www.bcrp.gob.pe/docs/Publicaciones/Nota-Semanal/{cc[-4:]}/{cc}.pdf"  # Year-coded path on server

    def _ns_key(name):
        base = os.path.splitext(os.path.basename(name))[0]
        m = re.search(r"ns-(\d{2})-(\d{4})", base, re.I)
        if not m:
            return (9999, 9999, base)                                      # Unknown names go last (stable by base)
        issue, year = int(m.group(1)), int(m.group(2))
        return (year, issue, base)                                         # Sort by year → issue → name

    def update_record(add=None, remove=None):
        # Intentionally DO NOT remove defective entries from the TXT.
        # This prevents the downloader from re-fetching them in future runs.
        p = os.path.join(record_folder, download_record_txt)
        s = set()
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                s = {x.strip() for x in f if x.strip()}                   # De-duplicate and strip blanks
        if add:
            s.add(add)                                                    # Only add the replacement
        records = sorted(s, key=_ns_key)                                  # Chronological: year → issue
        os.makedirs(record_folder, exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            f.write("\n".join(records) + ("\n" if records else ""))       # Ensure trailing newline if non-empty

    log2_info(f"\n🧩 Replacing {len(items)} PDF(s) under: {root_folder}")
    if quarantine:
        os.makedirs(quarantine, exist_ok=True)
        log2_info(f"🦠 Quarantine enabled → {quarantine}")
    ok, fail = 0, 0

    for year, bad_pdf, repl_code in items:
        year = str(year)
        ydir = os.path.join(root_folder, year)                             # Year directory (e.g., raw_pdf/2019)
        bad_path = os.path.join(ydir, bad_pdf)                             # Full path to defective file
        new_name = f"{norm(repl_code)}.pdf"                                # Normalized replacement filename
        new_path = os.path.join(ydir, new_name)                            # Destination path for replacement

        if not os.path.exists(bad_path):
            log2_warn(f"⚠️  {year}: not found → {bad_pdf} (skipped)")
            fail += 1
            continue

        # Download replacement first to avoid leaving gaps if removal fails later
        try:
            os.makedirs(ydir, exist_ok=True)
            log2_info(f"⬇️  {year}: downloading {norm(repl_code)} …")
            with requests.get(url(repl_code), stream=True, timeout=60) as r:
                r.raise_for_status()                                       # Raise on non-2xx to enter except block
                with open(new_path, "wb") as fh:
                    for ch in r.iter_content(131072):                      # 128 KiB chunks for efficiency
                        if ch:
                            fh.write(ch)
        except Exception as e:
            if os.path.exists(new_path):
                try:
                    os.remove(new_path)                                    # Clean partial file on failure
                except:
                    pass
            log2_error(f"❌  {year}: download failed for {norm(repl_code)} → {e}")
            fail += 1
            continue

        # Move defective to quarantine or delete permanently
        try:
            if quarantine:
                shutil.move(bad_path, os.path.join(quarantine, bad_pdf))   # Preserve evidence in quarantine
                moved_msg = f"moved to {os.path.basename(quarantine)}"
            else:
                os.remove(bad_path)                                        # Hard delete
                moved_msg = "deleted"
        except Exception as e:
            log2_error(f"❌  {year}: could not remove old file {bad_pdf} → {e}")
            fail += 1
            continue

        update_record(add=new_name, remove=bad_pdf)                        # Keep defective entry; add replacement
        log2_info(f"✅  {year}: {bad_pdf} → {new_name} ({moved_msg})")
        ok += 1

    log2_info(f"\n📊 Summary: ✅ {ok} done · ❌ {fail} failed")


# _________________________________________________________________________
# Function: search_keywords
def search_keywords(pdf_file, keywords):
    """
    Return 0-indexed page numbers containing any keyword.

    Args:
        pdf_file (str): Path to the PDF file.
        keywords (list[str]): Keywords to search for (case-sensitive).

    Returns:
        list[int]: Page indices where any keyword appears.
    """
    pages_with_keywords = []
    with fitz.open(pdf_file) as doc:
        for page_num in range(doc.page_count):
            page_text = doc.load_page(page_num).get_text()                 # Extract page text (layout-agnostic)
            if any(k in page_text for k in keywords):                      # Simple containment check
                pages_with_keywords.append(page_num)
    return pages_with_keywords


# _________________________________________________________________________
# Function: shortened_pdf
def shortened_pdf(pdf_file, pages, output_folder):
    """
    Create a compact PDF with only the provided pages and save it to output_folder.

    Args:
        pdf_file (str): Path to the source PDF.
        pages (list[int]): 0-indexed pages to retain.
        output_folder (str): Destination folder for the shortened PDF.

    Returns:
        int: Number of pages in the shortened PDF (0 if no pages were selected).
    """
    if not pages:
        return 0                                                           # Nothing to keep → skip

    os.makedirs(output_folder, exist_ok=True)
    new_pdf_file = os.path.join(output_folder, os.path.basename(pdf_file))
    with fitz.open(pdf_file) as doc:
        new_doc = fitz.open()
        for p in pages:
            new_doc.insert_pdf(doc, from_page=p, to_page=p)                # Insert page p only
        new_doc.save(new_pdf_file)                                         # Write compact copy to disk
        count = new_doc.page_count                                         # Capture page count before closing
        new_doc.close()
    return count


# _________________________________________________________________________
# Function: read_input_pdf_files
def read_input_pdf_files(input_pdf_record_folder, input_pdf_record_txt):
    """
    Read filenames of previously processed PDFs.

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
        return set(ln.strip() for ln in f if ln.strip())                   # Trim blanks and deduplicate via set


# _________________________________________________________________________
# Function: write_input_pdf_files
def write_input_pdf_files(input_pdf_files, input_pdf_record_folder, input_pdf_record_txt):
    """
    Persist processed PDF filenames to the record file (one per line).

    Args:
        input_pdf_files (set[str]): Filenames to write.
        input_pdf_record_folder (str): Folder for the record file.
        input_pdf_record_txt (str): Record filename.
    """
    record_path = os.path.join(input_pdf_record_folder, input_pdf_record_txt)
    os.makedirs(input_pdf_record_folder, exist_ok=True)
    with open(record_path, "w", encoding="utf-8") as f:
        for fn in sorted(input_pdf_files):                                  # Lexicographic write (stable baseline)
            f.write(fn + "\n")


# _________________________________________________________________________
# Function: ask_continue_input
def ask_continue_input(message):
    """
    Prompt the operator for a yes/no response in console environments.

    Args:
        message (str): Prompt to show.

    Returns:
        bool: True for 'y', False for 'n'.
    """
    while True:
        ans = input(f"{message} (y = yes / n = no): ").strip().lower()
        if ans in ("y", "n"):
            return ans == "y"                                              # Loop until valid response is given


# _________________________________________________________________________
# Function: input_pdfs_generator
def input_pdfs_generator(
    raw_pdf_folder,
    input_pdf_folder,
    input_pdf_record_folder,
    input_pdf_record_txt,
    keywords
):
    """
    Generate input PDFs containing key pages found by keyword search.
    For 4-page outputs, keep only the 1st and 3rd pages (tables of interest).
    Updates the record of processed PDFs.

    Args:
        raw_pdf_folder (str): Folder containing yearly subfolders of raw PDFs.
        input_pdf_folder (str): Folder to save the input PDFs.
        input_pdf_record_folder (str): Folder to store the record file.
        input_pdf_record_txt (str): Record filename (e.g., 'input_pdfs.txt').
        keywords (list[str]): Keywords used to select relevant pages.
    """
    start_time = time.time()

    input_pdf_files = read_input_pdf_files(input_pdf_record_folder, input_pdf_record_txt)  # Already processed
    skipped_years = {}                                                  # year → count already processed
    new_counter = 0
    skipped_counter = 0

    for folder in sorted(os.listdir(raw_pdf_folder)):                   # Yearly iteration
        if folder == "_quarantine":                                     # Skip quarantine area
            continue

        folder_path = os.path.join(raw_pdf_folder, folder)
        if not os.path.isdir(folder_path):
            continue

        pdf_files = [f for f in os.listdir(folder_path) if f.endswith(".pdf")]
        if not pdf_files:
            continue

        already = [f for f in pdf_files if f in input_pdf_files]        # Files already processed in this year
        if len(already) == len(pdf_files):                              # Nothing new in this year
            skipped_years[folder] = len(already)
            skipped_counter += len(already)
            continue

        log2_info(f"\n📂 Processing folder: {folder}\n")
        folder_new_count = 0
        folder_skipped_count = 0

        pbar = tqdm(                                                    # Visual progress for this year
            pdf_files,
            desc=f"Generating input PDFs in {folder}",
            unit="PDF",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
            colour="#E6004C"
        )

        for filename in pbar:
            pdf_file = os.path.join(folder_path, filename)
            if filename in input_pdf_files:
                folder_skipped_count += 1
                continue

            pages_with_keywords = search_keywords(pdf_file, keywords)    # Find candidate pages
            num_pages = shortened_pdf(pdf_file, pages_with_keywords, output_folder=input_pdf_folder)

            short_pdf_file = os.path.join(input_pdf_folder, os.path.basename(pdf_file))
            reader = PdfReader(short_pdf_file)                           # Re-open compact file

            if len(reader.pages) == 4:                                   # If 4 pages, keep 1st and 3rd only
                writer = PdfWriter()
                writer.add_page(reader.pages[0])                         # Page 1
                writer.add_page(reader.pages[2])                         # Page 3
                with open(short_pdf_file, "wb") as f_out:
                    writer.write(f_out)

            if num_pages > 0:                                            # Only mark successful extractions
                input_pdf_files.add(filename)
                folder_new_count += 1

        # Try to recolor the finished bar (some envs may not support)
        try:
            pbar.colour = "#3366FF"                                      # Finished color
            pbar.refresh()
        except Exception:
            pass
        finally:
            pbar.close()

        # Chronological write: (year, issue)
        def _ns_key(s):
            base = os.path.splitext(os.path.basename(s))[0]              # Strip .pdf
            m = re.search(r"ns-(\d{2})-(\d{4})", base, re.I)
            if not m:
                return (9999, 9999, base)
            issue = int(m.group(1)); year = int(m.group(2))
            return (year, issue)

        ordered_records = sorted(input_pdf_files, key=_ns_key)           # Deterministic order for the record
        os.makedirs(input_pdf_record_folder, exist_ok=True)
        record_path = os.path.join(input_pdf_record_folder, input_pdf_record_txt)
        with open(record_path, "w", encoding="utf-8") as f_rec:
            for name in ordered_records:
                f_rec.write(name + "\n")

        log2_info(f"✅ Shortened PDFs saved in '{input_pdf_folder}' "
                  f"({folder_new_count} new, {folder_skipped_count} skipped)")

        new_counter += folder_new_count
        skipped_counter += folder_skipped_count

        if not ask_continue_input(f"Do you want to continue to the next folder after '{folder}'?"):
            log2_warn("🛑 Process stopped by user.")
            break

    if skipped_years:
        years_summary = ", ".join(skipped_years.keys())
        total_skipped = sum(skipped_years.values())
        log2_info(f"\n⏩ {total_skipped} input PDFs already generated for years: {years_summary}")

    elapsed_time = round(time.time() - start_time)
    log2_info(f"\n📊 Summary:\n")
    log2_info(f"📂 {len(os.listdir(raw_pdf_folder))} folders (years) found containing raw PDFs")
    log2_info(f"🗃️ Already generated input PDFs: {skipped_counter}")
    log2_info(f"➕ Newly generated input PDFs: {new_counter}")
    log2_info(f"⏱️ {elapsed_time} seconds")




################################################################################################
# Section 3. Data cleaning
################################################################################################


#**********************************************************************************************
# Section 3.1.  Extracting tables and data cleanup 
#----------------------------------------------------------------------------------------------

#+++++++++++++++
# LIBRARIES
#+++++++++++++++

# Auxiliary functions (used within other functions)

import re  # Para funciones que utilizan expresiones regulares
import unicodedata  # Para funciones que manejan caracteres Unicode

# Functions for both Table 1 and Table 2

import pandas as pd  # Para funciones que trabajan con DataFrames

# Functions only for Table 1 

import numpy as np  # Para operaciones numéricas


# Functions only for Table 2

import roman  # Para conversión de números romanos a arábigos



#...............................................................................................
#...............................................................................................
# Auxiliary functions (used within other functions)
# ______________________________________________________________________________________________
#...............................................................................................


# 1. Removes spaces around hyphens and rare characters except letters, digits, and hyphens
def remove_rare_characters_first_row(texto):
    texto = re.sub(r'\s*-\s*', '-', texto)  # Removes spaces around hyphens
    texto = re.sub(r'[^a-zA-Z0-9\s-]', '', texto)  # Removes rare characters except letters, digits, and hyphens
    return texto

# 2. Removes rare characters, allowing only letters and spaces
def remove_rare_characters(texto):
    return re.sub(r'[^a-zA-Z\s]', '', texto)

# 3. Removes tildes and other diacritical marks
def remove_tildes(texto):
    return ''.join((c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn'))

# 4. Finds Roman numerals (I to X) in a given text.
def find_roman_numerals(text):
    pattern = r'\b(?:I{1,3}|IV|V|VI{0,3}|IX|X)\b'
    matches = re.findall(pattern, text)
    return matches

# 5. Auxiliary function used directly only in Table 2 to split values in a specific column.
def split_values(df):
    column_to_expand = df.columns[-3]
    new_columns = df[column_to_expand].str.split(expand=True)
    new_columns.columns = [f'{column_to_expand}_{i+1}' for i in range(new_columns.shape[1])]
    insertion_position = len(df.columns) - 2
    for col in reversed(new_columns.columns):
        df.insert(insertion_position, col, new_columns[col])
    df.drop(columns=[column_to_expand], inplace=True)
    return df



#...............................................................................................
#...............................................................................................
# Functions for both Table 1 and Table 2
# ______________________________________________________________________________________________
#...............................................................................................

# 1. Drops rows where all elements are NaN
def drop_nan_rows(df):
    df = df.dropna(how='all')
    return df

# 2. Drops columns where all elements are NaN
def drop_nan_columns(df):
    return df.dropna(axis=1, how='all')

# 3. Swaps the first and second rows in the first and last columns
def swap_first_second_row(df):
    temp = df.iloc[0, 0]
    df.iloc[0, 0] = df.iloc[1, 0]
    df.iloc[1, 0] = temp

    temp = df.iloc[0, -1]
    df.iloc[0, -1] = df.iloc[1, -1]
    df.iloc[1, -1] = temp
    return df

# 4. Resets the index of the DataFrame
def reset_index(df):
    df.reset_index(drop=True, inplace=True)
    return df

# 5. Removes digits followed by a slash in the first and last two columns
def remove_digit_slash(df):
    df.iloc[:, [0, -2, -1]] = df.iloc[:, [0, -2, -1]].apply(lambda x: x.str.replace(r'\d+/', '', regex=True))
    return df

# 6. Separates text and digits in the second to last column, handling potential numeric and decimal values
def separate_text_digits(df):
    for index, row in df.iterrows():
        if any(char.isdigit() for char in str(row.iloc[-2])) and any(char.isalpha() for char in str(row.iloc[-2])):
            if pd.isnull(row.iloc[-1]):
                df.loc[index, df.columns[-1]] = ''.join(filter(lambda x: x.isalpha() or x == ' ', str(row.iloc[-2])))
                df.loc[index, df.columns[-2]] = ''.join(filter(lambda x: not (x.isalpha() or x == ' '), str(row.iloc[-2])))
            
            # Check if comma or dot is used as decimal separator
            if ',' in str(row.iloc[-2]):
                split_values = str(row.iloc[-2]).split(',')
            elif '.' in str(row.iloc[-2]):
                split_values = str(row.iloc[-2]).split('.')
            else:
                split_values = [str(row.iloc[-2]), '']
                
            cleaned_integer = ''.join(filter(lambda x: x.isdigit() or x == '-', split_values[0]))
            cleaned_decimal = ''.join(filter(lambda x: x.isdigit(), split_values[1]))
            if cleaned_decimal:
                cleaned_numeric = cleaned_integer + ',' + cleaned_decimal
            else:
                cleaned_numeric = cleaned_integer
            df.loc[index, df.columns[-2]] = cleaned_numeric
    return df

# 7. Extracts columns with 4-digit years in their names
def extract_years(df):
    year_columns = [col for col in df.columns if re.match(r'\b\d{4}\b', col)]
    return year_columns

# 8. Sets the first row as column names and drops the first row
def first_row_columns(df):
    df.columns = df.iloc[0]
    df = df.drop(df.index[0])
    return df

# 9. Cleans column names and values by normalizing, removing tildes, and making them lowercase
def clean_columns_values(df):
    df.columns = df.columns.str.lower()
    df.columns = [unicodedata.normalize('NFKD', col).encode('ASCII', 'ignore').decode('utf-8') if isinstance(col, str) else col for col in df.columns]
    df.columns = df.columns.str.replace(' ', '_').str.replace('ano', 'year').str.replace('-', '_')
    
    text_columns = df.select_dtypes(include='object').columns
    for col in df.columns:
        df.loc[:, col] = df[col].apply(lambda x: remove_tildes(x) if isinstance(x, str) else x)
        df.loc[:, col] = df[col].apply(lambda x: str(x).replace(',', '.') if isinstance(x, (int, float, str)) else x)
    df.loc[:, 'sectores_economicos'] = df['sectores_economicos'].str.lower()
    df.loc[:, 'economic_sectors'] = df['economic_sectors'].str.lower()
    df.loc[:, 'sectores_economicos'] = df['sectores_economicos'].apply(remove_rare_characters)
    df.loc[:, 'economic_sectors'] = df['economic_sectors'].apply(remove_rare_characters)
    return df

# 10. Converts specified columns to numeric, ignoring specified columns
def convert_float(df):
    excluded_columns = ['sectores_economicos', 'economic_sectors']
    columns_to_convert = [col for col in df.columns if col not in excluded_columns]
    df[columns_to_convert] = df[columns_to_convert].apply(pd.to_numeric, errors='coerce')
    return df

# 11. Moves the last column to be the second column
def relocate_last_column(df):
    last_column = df.pop(df.columns[-1])
    df.insert(1, last_column.name, last_column)
    return df

# 12. Cleans the first row of the DataFrame by converting to lowercase, removing tildes, rare characters, and replacing 'ano' with 'year'
def clean_first_row(df):
    for col in df.columns:
        if df[col].dtype == 'object':
            if isinstance(df.at[0, col], str):
                df.at[0, col] = df.at[0, col].lower()  # Convert to lowercase if it is a string
                df.at[0, col] = remove_tildes(df.at[0, col])
                df.at[0, col] = remove_rare_characters_first_row(df.at[0, col])
                # Replace 'ano' with 'year'
                df.at[0, col] = df.at[0, col].replace('ano', 'year')

    return df

# 13. Replace 'set' with 'sep' in column names.
def replace_set_sep(df):
    # Get the column names of the DataFrame
    columns = df.columns
    
    # Iterate over the columns
    for column in columns:
        # Check if the column contains the expression 'set'
        if 'set' in column:
            # Replace 'set' with 'sep' in the column name
            new_column = column.replace('set', 'sep')
            # Rename the column in the DataFrame
            df.rename(columns={column: new_column}, inplace=True)
    
    return df

# 14. Strip spaces from specific columns.
def spaces_se_es(df):
    # Apply strip to the columns 'sectores_economicos' and 'economic_sectors'
    df['sectores_economicos'] = df['sectores_economicos'].str.strip()
    df['economic_sectors'] = df['economic_sectors'].str.strip()
    return df

# 15. Replace specific values in two columns.
def replace_services(df):
    # Check if 'servicios' and 'services' are present in the columns 'sectores_economicos' and 'economic_sectors'
    if ('servicios' in df['sectores_economicos'].values) and ('services' in df['economic_sectors'].values):
        # Replace the values
        df['sectores_economicos'].replace({'servicios': 'otros servicios'}, inplace=True)
        df['economic_sectors'].replace({'services': 'other services'}, inplace=True)
    return df

# 16. Replace specific values in two columns.
def replace_mineria(df):
    # Check if 'mineria' is present and 'mineria e hidrocarburos' is not present in the column 'sectores_economicos'
    if ('mineria' in df['sectores_economicos'].values) and ('mineria e hidrocarburos' not in df['sectores_economicos'].values):
        # Replace the value
        df['sectores_economicos'].replace({'mineria': 'mineria e hidrocarburos'}, inplace=True)
    return df

# 17. Replace specific values in two columns.
def replace_mining(df):
    # Check if 'mining and fuels' is present in the columns 'economic_sectors'
    if ('mining and fuels' in df['economic_sectors'].values):
        # Replace the value
        df['economic_sectors'].replace({'mining and fuels': 'mining and fuel'}, inplace=True)
    return df

# 18. Round float values to the specified number of decimals.
def rounding_values(df, decimals=1):
    # Iterate over all columns of the DataFrame
    for col in df.columns:
        # Check if the column is of type float
        if df[col].dtype == 'float64':
            # Round the values in the column to the specified number of decimals
            df[col] = df[col].round(decimals)
    return df



#...............................................................................................
#...............................................................................................
# Functions only for Table 1 
# ______________________________________________________________________________________________
#...............................................................................................

# 1. Relocates values in the last columns of the DataFrame
def relocate_last_columns(df):
    if not pd.isna(df.iloc[1, -1]):
        # Create a new column with NaN
        new_column = 'col_' + ''.join(map(str, np.random.randint(1, 5, size=1)))
        df[new_column] = np.nan
        
        # Get 'ECONOMIC SECTORS' and relocate
        insert_value_1 = df.iloc[0, -2]
        insert_value_1 = str(insert_value_1)
        df.iloc[:, -1] = df.iloc[:, -1].astype('object')
        df.iloc[0, -1] = insert_value_1
        
        # NaN first obs
        df.iloc[0,-2] = np.nan
    return df

# 2. Gets a sublist of months based on specified year columns
def get_months_sublist_list(df, year_columns):
    first_row = df.iloc[0]
    months_sublist_list = []
    months_sublist = []

    for item in first_row:
        if len(str(item)) == 3:
            months_sublist.append(item)
        elif '-' in item or str(item) == 'year':
            months_sublist.append(item)
            months_sublist_list.append(months_sublist)
            months_sublist = []

    if months_sublist:
        months_sublist_list.append(months_sublist)

    new_elements = []

    if year_columns:
        for i, year in enumerate(year_columns):
            if i < len(months_sublist_list):
                for element in months_sublist_list[i]:
                    new_elements.append(f"{year}_{element}")
                    
    two_first_elements = df.iloc[0][:2].tolist()

    for index in range(len(two_first_elements) - 1, -1, -1):
        if two_first_elements[index] not in new_elements:
            new_elements.insert(0, two_first_elements[index])

    while len(new_elements) < len(df.columns):
        new_elements.append(None)

    temp_df = pd.DataFrame([new_elements], columns=df.columns)
    df.iloc[0] = temp_df.iloc[0]

    return df

# 3. Finds and processes the year column in the DataFrame
def find_year_column(df):
    found_years = []

    for column in df.columns:
        if column.isdigit() and len(column) == 4:
            found_years.append(column)

    if len(found_years) > 1:
        pass
    elif len(found_years) == 1:
        year_name = found_years[0]
        first_row = df.iloc[0]

        column_contains_year = first_row[first_row.astype(str).str.contains(r'\byear\b')]

        if not column_contains_year.empty:
            column_contains_year_name = column_contains_year.index[0]

            column_contains_year_index = df.columns.get_loc(column_contains_year_name)
            year_name_index = df.columns.get_loc(year_name)

            if column_contains_year_index < year_name_index:
                new_year = str(int(year_name) - 1)
                df.rename(columns={column_contains_year_name: new_year}, inplace=True)
            elif column_contains_year_index > year_name_index:
                new_year = str(int(year_name) + 1)
                df.rename(columns={column_contains_year_name: new_year}, inplace=True)
            else:
                pass
        else:
            pass
    else:
        pass
    
    return df

# 4. Exchanges values between the last two columns where NaNs are present in the last column
def exchange_values(df):
    if len(df.columns) < 2:
        print("The DataFrame has less than two columns. Values cannot be exchanged.")
        return df

    if df.iloc[:, -1].isnull().any():
        last_column_rows_nan = df[df.iloc[:, -1].isnull()].index

        for idx in last_column_rows_nan:
            if -2 >= -len(df.columns):
                df.iloc[idx, -1], df.iloc[idx, -2] = df.iloc[idx, -2], df.iloc[idx, -1]

    return df

# 5. Replaces "Var. %" or "Var.%" in the first column with "variacion porcentual"
def replace_var_perc_first_column(df):
    regex = re.compile(r'Var\. ?%')

    for index, row in df.iterrows():
        value = str(row.iloc[0])

        if regex.search(value):
            df.at[index, df.columns[0]] = regex.sub("variacion porcentual", value)
    
    return df

# 6. Replaces a numeric pattern in the last column with a specified moving average number

number_moving_average = 'three' # Keep a space at the end
def replace_number_moving_average(df):
    for index, row in df.iterrows():
        if pd.notnull(row.iloc[-1]) and re.search(r'(\d\s*-)', str(row.iloc[-1])):
            df.at[index, df.columns[-1]] = re.sub(r'(\d\s*-)', f'{number_moving_average}-', str(row.iloc[-1]))

    return df

# 7. Replaces "Var. %" or "Var.%" in the last two columns with "percent change"
def replace_var_perc_last_columns(df):
    regex = re.compile(r'(Var\. ?%)(.*)')

    for index, row in df.iterrows():
        if isinstance(row.iloc[-2], str) and regex.search(row.iloc[-2]):
            replaced_text = regex.sub(r'\2 percent change', row.iloc[-2])
            df.at[index, df.columns[-2]] = replaced_text.strip()
        
        if isinstance(row.iloc[-1], str) and regex.search(row.iloc[-1]):
            replaced_text = regex.sub(r'\2 percent change', row.iloc[-1])
            df.at[index, df.columns[-1]] = replaced_text.strip()
    
    return df

# 8. Replaces the first dot in the second row of each column with a hyphen
def replace_first_dot(df):
    second_row = df.iloc[1]

    if any(isinstance(cell, str) and re.match(r'^\w+\.\s?\w+', cell) for cell in second_row):
        for col in df.columns:
            if isinstance(second_row[col], str):
                if re.match(r'^\w+\.\s?\w+', second_row[col]):
                    df.at[1, col] = re.sub(r'(\w+)\.(\s?\w+)', r'\1-\2', second_row[col], count=1)
    return df

# 9. Drops rows containing the rare character "}"
def drop_rare_caracter_row(df):
    rare_caracter_row = df.apply(lambda row: '}' in row.values, axis=1)
    df = df[~rare_caracter_row]
    return df

# 10. Splits columns based on a specified pattern in the second row
def split_column_by_pattern(df):
    for col in df.columns:
        if re.match(r'^[A-Z][a-z]+\.?\s[A-Z][a-z]+\.?$', str(df.iloc[1][col])):
            split_values = df[col].str.split(expand=True)
            df[col] = split_values[0]
            new_col_name = col + '_split'
            df.insert(df.columns.get_loc(col) + 1, new_col_name, split_values[1])
    return df


#++++++++++++++++++++++++++++++++++++++++++++++++
# Functions created for wr-specified issues
#++++++++++++++++++++++++++++++++++++++++++++++++

# WR below are examples where issues are fixed by functions. They are not unique cases, but at least examples where issues occur. Take into account that "ns" (Nota Semanal, Spanish) is just the same as wr (Weekly Report)

# 𝑛𝑠_2014_07
#...............................................................................................................................

# 1. Swaps NaN and "SECTORES ECONÓMICOS" in the first row of the DataFrame
def swap_nan_se(df):
    if pd.isna(df.iloc[0, 0]) and df.iloc[0, 1] == "SECTORES ECONÓMICOS":
        column_1_value = df.iloc[0, 1]
        df.iloc[0, 0] = column_1_value
        df.iloc[0, 1] = np.nan
        df = df.drop(df.columns[1], axis=1)
    return df


# 𝑛𝑠_2014_08
#...............................................................................................................................

# 1. Replaces NaN values in the first row with column names and sets them as column headers
def replace_first_row_with_columns(df):
    if any(isinstance(element, str) and element.isdigit() and len(element) == 4 for element in df.iloc[0]):
        for col_index, value in enumerate(df.iloc[0]):
            if pd.isna(value):
                df.iloc[0, col_index] = f"column_{col_index + 1}"
        df.columns = df.iloc[0]
        df = df.drop(df.index[0])
    return df

# 2. Expands a column based on a hyphen pattern and splits into new columns
def expand_column(df):
    column_to_expand = df.columns[-2]
    
    def replace_hyphens(match_obj):
        return match_obj.group(1) + ' ' + match_obj.group(2)

    if df[column_to_expand].str.contains(r'\d').any() and df[column_to_expand].str.contains(r'[a-zA-Z]').any():
        df[column_to_expand] = df[column_to_expand].apply(lambda x: re.sub(r'([a-zA-Z]+)\s*-\s*([a-zA-Z]+)', replace_hyphens, str(x)) if pd.notnull(x) else x)
        
        pattern = re.compile(r'[a-zA-Z\s]+$')

        def extract_replace(row):
            if pd.notnull(row[column_to_expand]) and isinstance(row[column_to_expand], str):
                if row.name != 0:
                    value_to_replace = pattern.search(row[column_to_expand])
                    if value_to_replace:
                        value_to_replace = value_to_replace.group().strip()
                        row[df.columns[-1]] = value_to_replace
                        row[column_to_expand] = re.sub(pattern, '', row[column_to_expand]).strip()
            return row

        df = df.apply(extract_replace, axis=1)

    return df

# 3. Splits values in a column into multiple columns based on a separator
def split_values_1(df):
    column_to_expand = df.columns[-2]
    new_columns = df[column_to_expand].str.split(expand=True)
    new_columns.columns = [f'{column_to_expand}_{i+1}' for i in range(new_columns.shape[1])]
    insertion_position = len(df.columns) - 1
    for col in reversed(new_columns.columns):
        df.insert(insertion_position, col, new_columns[col])
    df.drop(columns=[column_to_expand], inplace=True)
    return df


# 𝑛𝑠_2015_11
#...............................................................................................................................

# 1. Checks and updates the first row with specific patterns and values
def check_first_row(df):
    first_row = df.iloc[0]
    
    for i, (col, value) in enumerate(first_row.items()):
        if re.search(r'\b\d{4}\s\d{4}\b', str(value)):
            years = value.split()
            first_year = years[0]
            second_year = years[1]
            
            original_column_name = f'col_{i}'
            df.at[0, col] = original_column_name
            
            if pd.isna(df.iloc[0, 0]):
                df.iloc[0, 0] = first_year
            
            if pd.isna(df.iloc[0, 1]):
                df.iloc[0, 1] = second_year
    
    return df

# 2. Replaces NaN values in specific columns with values from adjacent columns
def replace_nan_with_previous_column_3(df):
    columns = df.columns
    
    for i in range(len(columns) - 1):
        if i != len(columns) - 1 and (columns[i].endswith('_year') and not df[columns[i]].isnull().any()):
            if df[columns[i+1]].isnull().any() and not columns[i+1].endswith('_year'):
                nan_indices = df[columns[i+1]].isnull()
                df.loc[nan_indices, [columns[i], columns[i+1]]] = df.loc[nan_indices, [columns[i+1], columns[i]]].values
    
    return df


# 𝑛𝑠_2016_15
#...............................................................................................................................

# 1. Checks and updates specific cells in the first row based on patterns and conditions
def check_first_row_1(df):
    if pd.isnull(df.iloc[0, 0]):
        penultimate_column = df.iloc[0, -2]
        if isinstance(penultimate_column, str) and len(penultimate_column) == 4 and penultimate_column.isdigit():
            df.iloc[0, 0] = penultimate_column
            df.iloc[0, -2] = np.nan
    
    if pd.isnull(df.iloc[0, 1]):
        last_column = df.iloc[0, -1]
        if isinstance(last_column, str) and len(last_column) == 4 and last_column.isdigit():
            df.iloc[0, 1] = last_column
            df.iloc[0, -1] = np.nan
    
    return df

# 2. Splits values in a column into multiple columns based on a separator
def split_values_2(df):
    column_to_expand = df.columns[-4]
    new_columns = df[column_to_expand].str.split(expand=True)
    new_columns.columns = [f'{column_to_expand}_{i+1}' for i in range(new_columns.shape[1])]
    insertion_position = len(df.columns) - 3
    for col in reversed(new_columns.columns):
        df.insert(insertion_position, col, new_columns[col])
    df.drop(columns=[column_to_expand], inplace=True)
    return df


# 𝑛𝑠_2016_19
#...............................................................................................................................

# 1. Split values in a specified column into multiple columns.
def split_values_3(df):
    column_to_expand = df.columns[-3]
    new_columns = df[column_to_expand].str.split(expand=True)
    new_columns.columns = [f'{column_to_expand}_{i+1}' for i in range(new_columns.shape[1])]
    insertion_position = len(df.columns) - 2
    for col in reversed(new_columns.columns):
        df.insert(insertion_position, col, new_columns[col])
    df.drop(columns=[column_to_expand], inplace=True)
    return df

# 2. Replace NaN values in consecutive columns with values from the previous column.
def replace_nan_with_previous_column_1(df):
    columns = df.columns
    
    for i in range(len(columns) - 1):
        # Add condition to check if the current column is not the last one
        if i != len(columns) - 2 and not (columns[i].endswith('_year') and df[columns[i]].isnull().any()):
            # Check if the column to the right has at least one NaN and does not end with '_year'
            if df[columns[i+1]].isnull().any() and not columns[i+1].endswith('_year'):
                nan_indices = df[columns[i+1]].isnull()
                df.loc[nan_indices, [columns[i], columns[i+1]]] = df.loc[nan_indices, [columns[i+1], columns[i]]].values
    
    return df

# 3. Replace NaN values in consecutive columns with values from the previous column (version 2).
def replace_nan_with_previous_column_2(df):
    columns = df.columns
    
    for i in range(len(columns) - 1):
        # Add condition to check if the current column is not the last one
        if i != len(columns) - 2 and not (columns[i].endswith('_year') and df[columns[i]].isnull().any()):
            # Check if the column to the right has at least one NaN and does not end with '_year'
            if df[columns[i+1]].isnull().any() and not columns[i+1].endswith('_year'):
                nan_indices = df[columns[i+1]].isnull()
                df.loc[nan_indices, [columns[i], columns[i+1]]] = df.loc[nan_indices, [columns[i+1], columns[i]]].values
    
    return df



#...............................................................................................
#...............................................................................................
# Functions only for Table 2
# ______________________________________________________________________________________________
#...............................................................................................

# 1. Separate years in the second last column of the DataFrame.
def separate_years(df):
    df = df.copy()  # Create a copy of the DataFrame to avoid SettingWithCopyWarning
    if isinstance(df.iloc[0, -2], str) and len(df.iloc[0, -2].split()) == 2:
        years = df.iloc[0, -2].split()
        if all(len(year) == 4 for year in years):
            second_year = years[1]
            df.iloc[0, -2] = years[0]
            df.insert(len(df.columns) - 1, 'new_column', [second_year] + [None] * (len(df) - 1))
    return df

# 2. Relocate Roman numerals found in the last row's last column and store them in a new column.
def relocate_roman_numerals(df):
    roman_numerals = find_roman_numerals(df.iloc[2, -1])
    if roman_numerals:
        original_text = df.iloc[2, -1]
        for roman_numeral in roman_numerals:
            original_text = original_text.replace(roman_numeral, '').strip()
        df.iloc[2, -1] = original_text
        df.at[2, 'new_column'] = ', '.join(roman_numerals)
        df.iloc[2, -1] = np.nan
    return df

# 3. Extract mixed numeric and textual values from the third last column and move them to the second last column.
def extract_mixed_values(df):
    df = df.copy()  # Create a copy of the DataFrame to avoid SettingWithCopyWarning
    regex_pattern = r'(-?\d+,\d [a-zA-Z\s]+)'
    for index, row in df.iterrows():
        third_last_obs = row.iloc[-3]
        second_last_obs = row.iloc[-2]

        if isinstance(third_last_obs, str) and pd.notnull(third_last_obs):
            match = re.search(regex_pattern, third_last_obs)
            if match:
                extracted_part = match.group(0)
                if pd.isna(second_last_obs) or pd.isnull(second_last_obs):
                    df.iloc[index, -2] = extracted_part
                    third_last_obs = re.sub(regex_pattern, '', third_last_obs).strip()
                    df.iloc[index, -3] = third_last_obs
    return df

# 4. Replace NaN values in the first row with corresponding column names.
def replace_first_row_nan(df):
    for col in df.columns:
        if pd.isna(df.iloc[0][col]):
            df.iloc[0, df.columns.get_loc(col)] = col
    return df

# 5. Convert Roman numerals in the first row of the DataFrame to Arabic numerals.
def roman_arabic(df):
    first_row = df.iloc[0]
    
    def convert_roman_number(number):
        try:
            return str(roman.fromRoman(number))
        except roman.InvalidRomanNumeralError:
            return number

    converted_first_row = []
    for value in first_row:
        if isinstance(value, str) and not pd.isna(value):
            converted_first_row.append(convert_roman_number(value))
        else:
            converted_first_row.append(value)

    df.iloc[0] = converted_first_row
    return df

# 6. Fix duplicate numeric values in the first row by incrementing subsequent duplicates.
def fix_duplicates(df):
    second_row = df.iloc[0].copy()
    prev_num = None
    first_one_index = None

    for i, num in enumerate(second_row):
        try:
            num = int(num)
            prev_num = int(prev_num) if prev_num is not None else None

            if num == prev_num:
                if num == 1:
                    if first_one_index is None:
                        first_one_index = i - 1
                    next_num = int(second_row[i - 1]) + 1
                    for j in range(i, len(second_row)):
                        if str(second_row.iloc[j]).isdigit():
                            second_row.iloc[j] = str(next_num)
                            next_num += 1
                elif i - 1 >= 0:
                    second_row.iloc[i] = str(int(second_row.iloc[i - 1]) + 1)

            prev_num = num
        except ValueError:
            pass

    df.iloc[0] = second_row
    return df

# 7. Extract quarters and create new column names based on year_columns.
def get_quarters_sublist_list(df, year_columns):
    first_row = df.iloc[0]
    # Initialize the list of sublists
    quarters_sublist_list = []

    # Initialize the current sublist
    quarters_sublist = []

    # Iterate over the elements of the first row
    for item in first_row:
        # Check if the item meets the requirements
        if len(str(item)) == 1:
            quarters_sublist.append(item)
        elif str(item) == 'year':
            quarters_sublist.append(item)
            quarters_sublist_list.append(quarters_sublist)
            quarters_sublist = []

    # Add the last sublist if it's not empty
    if quarters_sublist:
        quarters_sublist_list.append(quarters_sublist)

    new_elements = []

    # Check if year_columns is not empty
    if year_columns:
        for i, year in enumerate(year_columns):
            # Check if index i is valid for quarters_sublist_list
            if i < len(quarters_sublist_list):
                for element in quarters_sublist_list[i]:
                    new_elements.append(f"{year}_{element}")

    two_first_elements = df.iloc[0][:2].tolist()

    # Ensure that the two_first_elements are added if they are not in new_elements
    for index in range(len(two_first_elements) - 1, -1, -1):
        if two_first_elements[index] not in new_elements:
            new_elements.insert(0, two_first_elements[index])

    # Ensure that the length of new_elements matches the number of columns in df
    while len(new_elements) < len(df.columns):
        new_elements.append(None)

    temp_df = pd.DataFrame([new_elements], columns=df.columns)
    df.iloc[0] = temp_df.iloc[0]

    return df


#++++++++++++++++++++++++++++++++++++++++++++++++
# Functions created for wr-specified issues 
#++++++++++++++++++++++++++++++++++++++++++++++++

# WR below are examples where issues are fixed by functions. They are not unique cases, but at least examples where issues occur. Take into account that "ns" (Nota Semanal, Spanish) is just the same as wr (Weekly Report)

# 𝑛𝑠_2016_20
#...............................................................................................................................

# 1. Drop the first row if all values are NaN.
def drop_nan_row(df):
    if df.iloc[0].isnull().all():
        df = df.drop(index=0)
        df.reset_index(drop=True, inplace=True)
    return df


# 𝑛𝑠_2019_17
#...............................................................................................................................

# 1. Move values based on 'ECONOMIC SECTORS' observation in the last column.
def last_column_es(df):
    # Check if the first observation of the last column is 'ECONOMIC SECTORS'
    if df[df.columns[-1]].iloc[0] == 'ECONOMIC SECTORS':
        # Check if the second observation of the last column is not empty
        if pd.notnull(df[df.columns[-1]].iloc[1]):
            # Create a new column with NaN values
            new_column_name = f"col_{len(df.columns)}"
            df[new_column_name] = np.nan

            # Get 'ECONOMIC SECTORS' and relocate
            insert_value = df.iloc[0, -2]
            # Convert the value to string before assignment
            insert_value = str(insert_value)
            # Ensure the dtype of the last column is object (string) to accommodate string values
            df.iloc[:, -1] = df.iloc[:, -1].astype('object')
            df.iloc[0, -1] = insert_value

            # NaN first observation
            df.iloc[0, -2] = np.nan
    
    return df


# 𝑛𝑠_2019_26
#...............................................................................................................................

# 1. Exchange columns based on conditions.
def exchange_columns(df):
    # Find a column with all NaN values
    nan_column = None
    for column in df.columns:
        if df[column].isnull().all() and len(column) == 4 and column.isdigit():
            nan_column = column
            break
    
    if nan_column:
        # Check the column to the left
        column_index = df.columns.get_loc(nan_column)
        if column_index > 0:
            left_column = df.columns[column_index - 1]
            # Check if it is not a year (does not have 4 digits)
            if not (len(left_column) == 4 and left_column.isdigit()):
                # Swap column names
                df.rename(columns={nan_column: left_column, left_column: nan_column}, inplace=True)
    
    return df


# 𝑛𝑠_2019_29
#...............................................................................................................................

# 1. Exchange values between columns based on specific conditions.
def exchange_roman_nan(df):
    for col_idx, value in enumerate(df.iloc[1]):
        if isinstance(value, str):
            if value.upper() == 'AÑO' or (value.isalpha() and roman.fromRoman(value.upper())):
                next_col_idx = col_idx + 1
                if next_col_idx < len(df.columns) and pd.isna(df.iloc[1, next_col_idx]):
                    current_col = df.iloc[:, col_idx].drop(index=1)
                    next_col = df.iloc[:, next_col_idx].drop(index=1)
                    if current_col.isna().all():
                        df.iloc[1, col_idx], df.iloc[1, next_col_idx] = df.iloc[1, next_col_idx], df.iloc[1, col_idx]
    return df




# =============================================================================================
# Section 3. pipelines — table 1 and table 2 cleaning runners
# =============================================================================================

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# libraries (top-only imports)
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
import os
import re
import time
import csv
import hashlib
import logging
import pandas as pd
from tqdm.notebook import tqdm                      # Jupyter-native progress bars (gray background)
import tabula                                       # PDF table extractor (Java backend)

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# constants — colors, bar style, defaults
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
PROG_COLOR_ACTIVE = "#E6004C"                       # in-progress color (magenta/red)
PROG_COLOR_DONE   = "#3366FF"                       # finished color (blue)
BAR_FORMAT        = "{l_bar}{bar}| {n_fmt}/{total_fmt}"

DEFAULT_LOG_FOLDER = "logs"                         # default folder for .log files
LOG_TXT_T1         = "3_cleaner_1.log"              # table 1 log filename
LOG_TXT_T2         = "3_cleaner_2.log"              # table 2 log filename

RECORD_SUFFIX_1    = "new_generated_dataframes_1.txt"  # record txt for table 1
RECORD_SUFFIX_2    = "new_generated_dataframes_2.txt"  # record txt for table 2

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# logger — file-only loggers per table (no console echo from logger)
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
_SECTION3_LOGGERS: dict[str, logging.Logger] = {}   # cache of named loggers


# _________________________________________________________________________
# Function: init_section3_logger
def init_section3_logger(name: str,
                         log_folder: str,
                         log_txt: str) -> logging.Logger:
    """
    Create a file-only logger for Section 3 (per table).

    Args:
        name (str): Internal logger key (e.g., 't1' or 't2').
        log_folder (str): Folder for log files.
        log_txt (str): Log filename (e.g., '3_cleaner_1.log').

    Returns:
        logging.Logger: Configured file-only logger.
    """
    os.makedirs(log_folder, exist_ok=True)                                   # Ensure folder exists
    log_path = os.path.join(log_folder, log_txt)

    logger = logging.getLogger(f"section3.{name}")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    logger.propagate = False                                                 # No parent echo

    fh = logging.FileHandler(log_path, encoding="utf-8")                     # File sink
    fh.setFormatter(logging.Formatter("%(asctime)s — %(levelname)s — %(message)s"))
    logger.addHandler(fh)
    return logger


# _________________________________________________________________________
# Function: _ensure_logger
def _ensure_logger(name: str, log_folder: str, log_txt: str) -> logging.Logger:
    """
    Return a cached logger per table; initialize if missing.

    Args:
        name (str): Logger key ('t1' or 't2').
        log_folder (str): Folder for the log file.
        log_txt (str): Log filename.

    Returns:
        logging.Logger: Ready-to-use logger.
    """
    if name not in _SECTION3_LOGGERS:
        _SECTION3_LOGGERS[name] = init_section3_logger(name, log_folder, log_txt)
    return _SECTION3_LOGGERS[name]


# _________________________________________________________________________
# Function: log_info
def log_info(logger: logging.Logger, msg: str) -> None:
    """
    Write info into .log and show the same line in the notebook output.

    Args:
        logger (logging.Logger): Logger instance.
        msg (str): Message to record.
    """
    logger.info(msg)                                                         # File only
    print(msg)                                                               # Notebook line


# _________________________________________________________________________
# Function: log_warn
def log_warn(logger: logging.Logger, msg: str) -> None:
    """
    Write warning into .log and show the same line in the notebook output.

    Args:
        logger (logging.Logger): Logger instance.
        msg (str): Message to record.
    """
    logger.warning(msg)                                                      # File only
    print(msg)                                                               # Notebook line


# =============================================================================================
# utilities — parsing, sorting, records, extraction, persistence
# =============================================================================================

# _________________________________________________________________________
# Function: parse_ns_meta
def parse_ns_meta(file_name: str) -> tuple[str | None, str | None]:
    """
    Extract (issue, year) from filename like 'ns-07-2017.pdf'.

    Args:
        file_name (str): Filename or path.

    Returns:
        tuple[str | None, str | None]: (issue, year) if matched; else (None, None).
    """
    m = re.search(r"ns-(\d{1,2})-(\d{4})", os.path.basename(file_name).lower())
    return (m.group(1), m.group(2)) if m else (None, None)


# _________________________________________________________________________
# Function: _ns_sort_key
def _ns_sort_key(s: str) -> tuple[int, int, str]:
    """
    Chronological sort key (year, issue) for names 'ns-xx-yyyy(.pdf)'.

    Args:
        s (str): Filename or basename.

    Returns:
        tuple[int, int, str]: (year, issue, basename) for stable ordering.
    """
    base = os.path.splitext(os.path.basename(s))[0]
    m = re.search(r"ns-(\d{1,2})-(\d{4})", base, re.I)
    if not m:
        return (9999, 9999, base)
    issue, year = int(m.group(1)), int(m.group(2))
    return (year, issue, base)


# _________________________________________________________________________
# Function: _read_records
def _read_records(record_folder: str, record_txt: str) -> list[str]:
    """
    Read existing records and return a unique, chronological list.

    Args:
        record_folder (str): Folder for the record file.
        record_txt (str): Record filename.

    Returns:
        list[str]: Filenames ordered by (year, issue).
    """
    path = os.path.join(record_folder, record_txt)
    if not os.path.exists(path):
        return []
    with open(path, "r", encoding="utf-8") as f:
        items = [ln.strip() for ln in f if ln.strip()]
    return sorted(set(items), key=_ns_sort_key)


# _________________________________________________________________________
# Function: _write_records
def _write_records(record_folder: str, record_txt: str, items: list[str]) -> None:
    """
    Persist records chronologically with a trailing newline.

    Args:
        record_folder (str): Folder of the record file.
        record_txt (str): Filename of the record.
        items (list[str]): Filenames to persist.
    """
    os.makedirs(record_folder, exist_ok=True)                                 # Ensure folder exists
    items = sorted(set(items), key=_ns_sort_key)                              # De-dup + sort
    path = os.path.join(record_folder, record_txt)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(items) + ("\n" if items else ""))                   # Trailing newline


# _________________________________________________________________________
# Function: _extract_table
def _extract_table(pdf_path: str, page: int) -> pd.DataFrame | None:
    """
    Extract a single table from the specified PDF page.

    Args:
        pdf_path (str): Full path to the PDF.
        page (int): 1-based page index to read.

    Returns:
        pd.DataFrame | None: Extracted table or None if not found.
    """
    tables = tabula.read_pdf(pdf_path, pages=page, multiple_tables=False, stream=True)
    if tables is None:
        return None
    if isinstance(tables, list) and len(tables) == 0:
        return None
    return tables[0] if isinstance(tables, list) else tables


# _________________________________________________________________________
# Function: _compute_sha256
def _compute_sha256(file_path: str, chunk: int = 1 << 20) -> str:
    """
    Compute SHA-256 of a file in chunks (default 1MB).

    Args:
        file_path (str): Path to file.
        chunk (int): Chunk size.

    Returns:
        str: Hex digest of SHA-256.
    """
    h = hashlib.sha256()
    with open(file_path, "rb") as fh:
        while True:
            b = fh.read(chunk)
            if not b:
                break
            h.update(b)
    return h.hexdigest()


# _________________________________________________________________________
# Function: _save_df
def _save_df(df: pd.DataFrame, out_path: str) -> tuple[str, int, int]:
    """
    Save a DataFrame to Parquet (preferred) or CSV (fallback).

    Args:
        df (pd.DataFrame): DataFrame to persist.
        out_path (str): Suggested full path (extension adjusted if needed).

    Returns:
        tuple[str, int, int]: (saved_path, n_rows, n_cols).
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)                      # Ensure folder exists
    try:
        if not out_path.endswith(".parquet"):
            out_path = os.path.splitext(out_path)[0] + ".parquet"             # Force .parquet
        df.to_parquet(out_path, index=False)                                   # Requires pyarrow/fastparquet
    except Exception:
        out_path = os.path.splitext(out_path)[0] + ".csv"                      # Fallback to CSV
        df.to_csv(out_path, index=False)
    return out_path, int(df.shape[0]), int(df.shape[1])


# _________________________________________________________________________
# Function: _append_manifest
def _append_manifest(manifest_path: str,
                     ns_code: str,
                     table_name: str,
                     year: str,
                     issue: str,
                     file_path: str,
                     n_rows: int,
                     n_cols: int,
                     pipeline_version: str) -> None:
    """
    Append or update a row in 'manifest.csv' for ns_code.

    Columns:
        ns, table, year, issue, path, rows, cols, sha256, pipeline_version, timestamp
    """
    os.makedirs(os.path.dirname(manifest_path), exist_ok=True)                 # Ensure folder exists

    rows: list[dict] = []
    if os.path.exists(manifest_path):
        with open(manifest_path, "r", encoding="utf-8", newline="") as fh:
            reader = csv.DictReader(fh)
            rows = list(reader)

    now_str = time.strftime("%Y-%m-%d %H:%M:%S")
    sha = _compute_sha256(file_path)
    new_row = {
        "ns": ns_code,
        "table": table_name,
        "year": year,
        "issue": issue,
        "path": file_path,
        "rows": str(n_rows),
        "cols": str(n_cols),
        "sha256": sha,
        "pipeline_version": pipeline_version,
        "timestamp": now_str,
    }

    idx = next((i for i, r in enumerate(rows)
                if r.get("ns") == ns_code and r.get("table") == table_name), None)
    if idx is None:
        rows.append(new_row)                                                  # Append
    else:
        rows[idx] = new_row                                                   # Replace

    with open(manifest_path, "w", encoding="utf-8", newline="") as fh:
        writer = csv.DictWriter(fh, fieldnames=list(new_row.keys()))
        writer.writeheader()
        writer.writerows(rows)



# =============================================================================================
# pipelines — class wrapper for Table 1 and Table 2 cleaning
# =============================================================================================
class gdpwr_cleaner:
    """
    Pipelines for WR tables.

    Exposes:
        - clean_table1(df): Monthly (table 1) pipeline.
        - clean_table2(df): Quarterly/annual (table 2) pipeline.

    Note:
        The helper functions referenced below (drop_nan_rows, split_column_by_pattern, …)
        must exist in this module (your Section 3 cleaning helpers).
    """

    # _____________________________________________________________________
    # Function: clean_table1
    def clean_table1(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean a raw DataFrame extracted from WR Table 1 (monthly growth).

        Args:
            df (pd.DataFrame): Raw table 1 dataframe.

        Returns:
            pd.DataFrame: Cleaned table 1 dataframe.
        """
        d = df.copy()                                                            # Work on a copy

        # Branch A — at least one column already looks like 'YYYY'
        if any(isinstance(c, str) and c.isdigit() and len(c) == 4 for c in d.columns):
            d = swap_nan_se(d)                                                   # Fix misplaced 'SECTORES ECONÓMICOS'
            d = split_column_by_pattern(d)                                       # Split 'Word. Word' headers
            d = drop_rare_caracter_row(d)                                        # Remove rows with rare char '}'
            d = drop_nan_rows(d)                                                 # Drop fully-NaN rows
            d = drop_nan_columns(d)                                              # Drop fully-NaN columns
            d = relocate_last_columns(d)                                         # Move trailing text if needed
            d = replace_first_dot(d)                                             # Replace first dot with hyphen in row 2
            d = swap_first_second_row(d)                                         # Swap first/second rows at edges
            d = drop_nan_rows(d)                                                 # Clean residual empty rows
            d = reset_index(d)                                                   # Reset DataFrame index
            d = remove_digit_slash(d)                                            # Clean 'digits/' on edge columns
            d = replace_var_perc_first_column(d)                                 # Normalize 'Var. %' in first column
            d = replace_var_perc_last_columns(d)                                 # Normalize 'Var. %' in last columns
            d = replace_number_moving_average(d)                                 # Normalize moving-average label
            d = separate_text_digits(d)                                          # Separate text and digits in penultimate col
            d = exchange_values(d)                                               # Swap last two columns when needed
            d = relocate_last_column(d)                                          # Move last column to position 2
            d = clean_first_row(d)                                               # Normalize header row text (lowercase, etc.)
            d = find_year_column(d)                                              # Align 'year' vs numeric-year column
            years = extract_years(d)                                             # Collect year columns
            d = get_months_sublist_list(d, years)                                # Build month headers per year
            d = first_row_columns(d)                                             # Promote first row to headers
            d = clean_columns_values(d)                                          # Normalize columns and values
            d = convert_float(d)                                                 # Coerce numeric columns
            d = replace_set_sep(d)                                               # 'set' → 'sep' in headers
            d = spaces_se_es(d)                                                  # Trim spaces in ES/EN sector columns
            d = replace_services(d)                                              # Harmonize 'services' labels
            d = replace_mineria(d)                                               # Harmonize 'mineria' labels (ES)
            d = replace_mining(d)                                                # Harmonize 'mining' labels (EN)
            d = rounding_values(d, decimals=1)                                   # Round float columns to 1 decimal
            return d

        # Branch B — no 'YYYY' columns yet
        d = check_first_row(d)                                                   # Split 'YYYY YYYY' patterns in header
        d = check_first_row_1(d)                                                 # Fill missing first-row years from edges
        d = replace_first_row_with_columns(d)                                    # Replace NaNs with placeholder names
        d = swap_nan_se(d)                                                       # Fix misplaced 'SECTORES ECONÓMICOS'
        d = split_column_by_pattern(d)                                           # Split 'Word. Word' headers
        d = drop_rare_caracter_row(d)                                            # Remove rows with rare char '}'
        d = drop_nan_rows(d)                                                     # Drop fully-NaN rows
        d = drop_nan_columns(d)                                                  # Drop fully-NaN columns
        d = relocate_last_columns(d)                                             # Move trailing text if needed
        d = swap_first_second_row(d)                                             # Swap first/second rows at edges
        d = drop_nan_rows(d)                                                     # Clean residual empty rows
        d = reset_index(d)                                                       # Reset DataFrame index
        d = remove_digit_slash(d)                                                # Clean 'digits/' on edge columns
        d = replace_var_perc_first_column(d)                                     # Normalize 'Var. %' in first column
        d = replace_var_perc_last_columns(d)                                     # Normalize 'Var. %' in last columns
        d = replace_number_moving_average(d)                                     # Normalize moving-average label
        d = expand_column(d)                                                     # Expand hyphenated text in penultimate col
        d = split_values_1(d)                                                    # Split expanded col (variant 1)
        d = split_values_2(d)                                                    # Split expanded col (variant 2)
        d = split_values_3(d)                                                    # Split expanded col (variant 3)
        d = separate_text_digits(d)                                              # Separate text and digits in penultimate col
        d = exchange_values(d)                                                   # Swap last two columns when needed
        d = relocate_last_column(d)                                              # Move last column to position 2
        d = clean_first_row(d)                                                   # Normalize header row text
        d = find_year_column(d)                                                  # Align 'year' vs numeric-year column
        years = extract_years(d)                                                 # Collect year columns
        d = get_months_sublist_list(d, years)                                    # Build month headers per year
        d = first_row_columns(d)                                                 # Promote first row to headers
        d = clean_columns_values(d)                                              # Normalize columns and values
        d = convert_float(d)                                                     # Coerce numeric columns
        d = replace_nan_with_previous_column_1(d)                                # Fill NaNs from neighbor (v1)
        d = replace_nan_with_previous_column_2(d)                                # Fill NaNs from neighbor (v2)
        d = replace_nan_with_previous_column_3(d)                                # Fill NaNs from neighbor (v3)
        d = replace_set_sep(d)                                                   # 'set' → 'sep' in headers
        d = spaces_se_es(d)                                                      # Trim spaces in ES/EN sector columns
        d = replace_services(d)                                                  # Harmonize 'services' labels
        d = replace_mineria(d)                                                   # Harmonize 'mineria' labels (ES)
        d = replace_mining(d)                                                    # Harmonize 'mining' labels (EN)
        d = rounding_values(d, decimals=1)                                       # Round float columns to 1 decimal
        return d

    # _____________________________________________________________________
    # Function: clean_table2
    def clean_table2(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean a raw DataFrame extracted from WR Table 2 (quarterly/annual).

        Args:
            df (pd.DataFrame): Raw table 2 dataframe.

        Returns:
            pd.DataFrame: Cleaned table 2 dataframe.
        """
        d = df.copy()                                                            # Work on a copy

        # Branch A — header starts with NaN (specific layout)
        if pd.isna(d.iloc[0, 0]):
            d = drop_nan_columns(d)                                              # Drop fully-NaN columns
            d = separate_years(d)                                                # Split 'YYYY YYYY' cell into two
            d = relocate_roman_numerals(d)                                       # Move Roman numerals to new column
            d = extract_mixed_values(d)                                          # Extract mixed numeric/text values
            d = replace_first_row_nan(d)                                         # Fill NaNs in first row with col names
            d = first_row_columns(d)                                             # Promote first row to header
            d = swap_first_second_row(d)                                         # Swap first/second rows at edges
            d = reset_index(d)                                                   # Reset DataFrame index
            d = drop_nan_row(d)                                                  # Drop first row if fully NaN
            years = extract_years(d)                                             # Collect year columns
            d = split_values(d)                                                  # Split target mixed column
            d = separate_text_digits(d)                                          # Separate text and digits in penultimate col
            d = roman_arabic(d)                                                  # Convert Roman numerals to Arabic
            d = fix_duplicates(d)                                                # Fix duplicate numeric headers
            d = relocate_last_column(d)                                          # Move last column to position 2
            d = clean_first_row(d)                                               # Normalize header row text
            d = get_quarters_sublist_list(d, years)                              # Build quarter headers per year
            d = first_row_columns(d)                                             # Promote first row to headers again
            d = clean_columns_values(d)                                          # Normalize columns and values
            d = reset_index(d)                                                   # Reset DataFrame index
            d = convert_float(d)                                                 # Coerce numeric columns
            d = replace_set_sep(d)                                               # 'set' → 'sep' in headers
            d = spaces_se_es(d)                                                  # Trim spaces in ES/EN sector columns
            d = replace_services(d)                                              # Harmonize 'services' labels
            d = replace_mineria(d)                                               # Harmonize 'mineria' labels (ES)
            d = replace_mining(d)                                                # Harmonize 'mining' labels (EN)
            d = rounding_values(d, decimals=1)                                   # Round float columns to 1 decimal
            return d

        # Branch B — standard layout
        d = exchange_roman_nan(d)                                                # Swap Roman vs NaN when needed
        d = exchange_columns(d)                                                  # Swap year vs non-year columns if shifted
        d = drop_nan_columns(d)                                                  # Drop fully-NaN columns
        d = remove_digit_slash(d)                                                # Clean 'digits/' on edge columns
        d = last_column_es(d)                                                    # Fix last-column 'ECONOMIC SECTORS' placement
        d = swap_first_second_row(d)                                             # Swap first/second rows at edges
        d = drop_nan_rows(d)                                                     # Drop fully-NaN rows
        d = reset_index(d)                                                       # Reset DataFrame index
        years = extract_years(d)                                                 # Collect year columns
        d = separate_text_digits(d)                                              # Separate text and digits in penultimate col
        d = roman_arabic(d)                                                      # Convert Roman numerals to Arabic
        d = fix_duplicates(d)                                                    # Fix duplicate numeric headers
        d = relocate_last_column(d)                                              # Move last column to position 2
        d = clean_first_row(d)                                                   # Normalize header row text
        d = get_quarters_sublist_list(d, years)                                  # Build quarter headers per year
        d = first_row_columns(d)                                                 # Promote first row to headers
        d = clean_columns_values(d)                                              # Normalize columns and values
        d = reset_index(d)                                                       # Reset DataFrame index
        d = convert_float(d)                                                     # Coerce numeric columns
        d = replace_set_sep(d)                                                   # 'set' → 'sep' in headers
        d = spaces_se_es(d)                                                      # Trim spaces in ES/EN sector columns
        d = replace_services(d)                                                  # Harmonize 'services' labels
        d = replace_mineria(d)                                                   # Harmonize 'mineria' labels (ES)
        d = replace_mining(d)                                                    # Harmonize 'mining' labels (EN)
        d = rounding_values(d, decimals=1)                                       # Round float columns to 1 decimal
        return d


# =============================================================================================
# runners — single-call functions per table (raw+clean dicts, records, logs, bars, summary)
# =============================================================================================

# _________________________________________________________________________
# Function: table_1_cleaner
def table_1_cleaner(
    input_pdf_folder: str,
    record_folder: str,
    record_txt: str = RECORD_SUFFIX_1,
    log_folder: str = DEFAULT_LOG_FOLDER,
    log_txt: str = LOG_TXT_T1,
    persist: bool = False,
    persist_folder: str | None = None,
    pipeline_version: str = "s3.0.0",
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    """
    Extract page 1 from each WR PDF, run the table 1 pipeline, update the record,
    optionally persist cleaned tables (Parquet/CSV) + manifest, and show a concise summary.

    Args:
        input_pdf_folder (str): Root folder containing year subfolders (skips '_quarantine').
        record_folder (str): Folder where the record text file is stored.
        record_txt (str): Record filename, default 'new_generated_dataframes_1.txt'.
        log_folder (str): Folder where the .log file will be written.
        log_txt (str): Log filename (default '3_cleaner_1.log').
        persist (bool): If True, save cleaned DataFrames + manifest.
        persist_folder (str | None): Base output folder (default './data/clean').
        pipeline_version (str): Version tag recorded in manifest.

    Returns:
        tuple:
            - raw_tables_dict_1 (dict[str, pd.DataFrame]): Raw tables keyed as 'ns_xx_yyyy_1'.
            - new_dataframes_dict_1 (dict[str, pd.DataFrame]): Cleaned tables keyed as 'ns_xx_yyyy_1'.
    """
    logger = _ensure_logger("t1", log_folder, log_txt)                         # File-only logger
    start_time = time.time()                                                    # Timer start

    log_info(logger, "\n🧹 Starting Table 1 cleaning...\n")

    cleaner   = gdpwr_cleaner()                                                # Pipeline runner
    records   = _read_records(record_folder, record_txt)                        # Load existing records
    processed = set(records)                                                    # Fast membership test

    raw_tables_dict_1: dict[str, pd.DataFrame]   = {}                           # Raw tables store
    new_dataframes_dict_1: dict[str, pd.DataFrame] = {}                         # Cleaned tables store

    new_counter = 0                                                             # Newly cleaned
    skipped_counter = 0                                                         # Already cleaned (per file)
    skipped_years: dict[str, int] = {}                                          # year → count already cleaned

    # Year folders (skip quarantine)
    years = [d for d in sorted(os.listdir(input_pdf_folder))
             if os.path.isdir(os.path.join(input_pdf_folder, d)) and d != "_quarantine"]
    total_year_folders = len(years)                                             # For final summary

    # Persistence layout
    if persist:
        base_out      = persist_folder or os.path.join("data", "clean")         # Default base
        out_root      = os.path.join(base_out, "table_1")                       # e.g., data/clean/table_1
        manifest_path = os.path.join(out_root, "manifest.csv")                  # table-level manifest
        os.makedirs(out_root, exist_ok=True)

    for year in years:
        folder_path = os.path.join(input_pdf_folder, year)                       # Year folder
        pdf_files   = sorted([f for f in os.listdir(folder_path) if f.endswith(".pdf")], key=_ns_sort_key)
        if not pdf_files:
            continue

        # If this year is fully processed, skip bar and remember
        already = [f for f in pdf_files if f in processed]
        if len(already) == len(pdf_files):
            skipped_years[year] = len(already)                                   # Whole year already done
            skipped_counter += len(already)
            continue

        log_info(logger, f"\n📂 Processing Table 1 in {year}\n")
        folder_new_count = 0
        folder_skipped_count = 0

        pbar = tqdm(
            pdf_files,
            desc=f"🚧 {year}",
            unit="PDF",
            bar_format=BAR_FORMAT,
            colour=PROG_COLOR_ACTIVE,
            leave=False,                 # remove row when finished
            position=0,                  # use the same row for both bars
            dynamic_ncols=True
        )

        for filename in pbar:
            if filename in processed:
                folder_skipped_count += 1
                continue

            issue, yr = parse_ns_meta(filename)
            if not issue:
                folder_skipped_count += 1
                continue

            pdf_path = os.path.join(folder_path, filename)
            try:
                raw = _extract_table(pdf_path, page=1)
                if raw is None:
                    folder_skipped_count += 1
                    continue

                key = f"{os.path.splitext(filename)[0].replace('-', '_')}_1"
                raw_tables_dict_1[key] = raw.copy()

                clean = cleaner.clean_table1(raw)
                clean.insert(0, "year", yr)
                clean.insert(1, "wr", issue)
                new_dataframes_dict_1[key] = clean

                if persist:
                    ns_code  = os.path.splitext(filename)[0]
                    out_dir  = os.path.join(out_root, str(yr))
                    out_path = os.path.join(out_dir, f"{ns_code}.parquet")
                    saved_path, n_rows, n_cols = _save_df(clean, out_path)
                    _append_manifest(
                        manifest_path=manifest_path,
                        ns_code=ns_code,
                        table_name="table_1",
                        year=yr,
                        issue=issue,
                        file_path=saved_path,
                        n_rows=n_rows,
                        n_cols=n_cols,
                        pipeline_version=pipeline_version,
                    )

                processed.add(filename)
                folder_new_count += 1
            except Exception as e:
                log_warn(logger, f"⚠️  {filename}: {e}")
                folder_skipped_count += 1

        # IMPORTANT: no prints/logs between these two blocks
        pbar.clear(); pbar.close()        # remove the '🚧' row

        # Finished bar (blue) — same row, looks like it “replaced” the first
        fb = tqdm(
            total=len(pdf_files),
            desc=f"✔️ {year}",
            unit="PDF",
            bar_format=BAR_FORMAT,
            colour=PROG_COLOR_DONE,
            leave=True,                   # keep visible
            position=0,                   # same row as pbar
            dynamic_ncols=True
        )
        fb.update(len(pdf_files))
        fb.close()

        # Update counters after the year is done
        new_counter += folder_new_count
        skipped_counter += folder_skipped_count

        # Persist updated record (chronological) after each year
        _write_records(record_folder, record_txt, list(processed))

    # Summary of fully skipped years (same style as Section 2)
    if skipped_years:
        years_summary = ", ".join(skipped_years.keys())
        total_skipped = sum(skipped_years.values())
        log_info(logger, f"\n⏩ {total_skipped} cleaned tables already generated for years: {years_summary}")

    # Final summary — mimic Section 2 format
    elapsed_time = round(time.time() - start_time)
    log_info(logger, f"\n📊 Summary:\n")
    log_info(logger, f"📂 {total_year_folders} folders (years) found containing input PDFs")
    log_info(logger, f"🗃️ Already cleaned tables: {skipped_counter}")
    log_info(logger, f"✨ Newly cleaned tables: {new_counter}")
    log_info(logger, f"⏱️ {elapsed_time} seconds")

    return raw_tables_dict_1, new_dataframes_dict_1


# _________________________________________________________________________
# Function: table_2_cleaner
def table_2_cleaner(
    input_pdf_folder: str,
    record_folder: str,
    record_txt: str = RECORD_SUFFIX_2,
    log_folder: str = DEFAULT_LOG_FOLDER,
    log_txt: str = LOG_TXT_T2,
    persist: bool = False,
    persist_folder: str | None = None,
    pipeline_version: str = "s3.0.0",
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    """
    Extract page 2 from each WR PDF, run the table 2 pipeline, update the record,
    optionally persist cleaned tables (Parquet/CSV) + manifest, and show a concise summary.

    Args:
        input_pdf_folder (str): Root folder containing year subfolders (skips '_quarantine').
        record_folder (str): Folder where the record text file is stored.
        record_txt (str): Record filename, default 'new_generated_dataframes_2.txt'.
        log_folder (str): Folder where the .log file will be written.
        log_txt (str): Log filename (default '3_cleaner_2.log').
        persist (bool): If True, save cleaned DataFrames + manifest.
        persist_folder (str | None): Base output folder (default './data/clean').
        pipeline_version (str): Version tag recorded in manifest.

    Returns:
        tuple:
            - raw_tables_dict_2 (dict[str, pd.DataFrame]): Raw tables keyed as 'ns_xx_yyyy_2'.
            - new_dataframes_dict_2 (dict[str, pd.DataFrame]): Cleaned tables keyed as 'ns_xx_yyyy_2'.
    """
    logger = _ensure_logger("t2", log_folder, log_txt)                          # File-only logger
    start_time = time.time()                                                    # Timer start

    log_info(logger, "\n🧹 Starting Table 2 cleaning...\n")

    cleaner   = gdpwr_cleaner()                                                # Pipeline runner
    records   = _read_records(record_folder, record_txt)                        # Load existing records
    processed = set(records)                                                    # Fast membership test

    raw_tables_dict_2: dict[str, pd.DataFrame]   = {}                           # Raw tables store
    new_dataframes_dict_2: dict[str, pd.DataFrame] = {}                         # Cleaned tables store

    new_counter = 0                                                             # Newly cleaned
    skipped_counter = 0                                                         # Already cleaned (per file)
    skipped_years: dict[str, int] = {}                                          # year → count already cleaned

    # Year folders (skip quarantine)
    years = [d for d in sorted(os.listdir(input_pdf_folder))
             if os.path.isdir(os.path.join(input_pdf_folder, d)) and d != "_quarantine"]
    total_year_folders = len(years)                                             # For final summary

    # Persistence layout
    if persist:
        base_out      = persist_folder or os.path.join("data", "clean")         # Default base
        out_root      = os.path.join(base_out, "table_2")                       # e.g., data/clean/table_2
        manifest_path = os.path.join(out_root, "manifest.csv")                  # table-level manifest
        os.makedirs(out_root, exist_ok=True)

    for year in years:
        folder_path = os.path.join(input_pdf_folder, year)                       # Year folder
        pdf_files   = sorted([f for f in os.listdir(folder_path) if f.endswith(".pdf")], key=_ns_sort_key)
        if not pdf_files:
            continue

        # If this year is fully processed, skip bar and remember
        already = [f for f in pdf_files if f in processed]
        if len(already) == len(pdf_files):
            skipped_years[year] = len(already)                                   # Whole year already done
            skipped_counter += len(already)
            continue

        log_info(logger, f"\n📂 Processing Table 2 in {year}\n")
        folder_new_count = 0
        folder_skipped_count = 0

        pbar = tqdm(
            pdf_files,
            desc=f"🚧 {year}",
            unit="PDF",
            bar_format=BAR_FORMAT,
            colour=PROG_COLOR_ACTIVE,
            leave=False,                 # remove row when finished
            position=0,                  # use the same row for both bars
            dynamic_ncols=True
        )

        for filename in pbar:
            if filename in processed:
                folder_skipped_count += 1
                continue

            issue, yr = parse_ns_meta(filename)
            if not issue:
                folder_skipped_count += 1
                continue

            pdf_path = os.path.join(folder_path, filename)
            try:
                raw = _extract_table(pdf_path, page=2)
                if raw is None:
                    folder_skipped_count += 1
                    continue

                key = f"{os.path.splitext(filename)[0].replace('-', '_')}_2"
                raw_tables_dict_2[key] = raw.copy()

                clean = cleaner.clean_table2(raw)
                clean.insert(0, "year", yr)
                clean.insert(1, "wr", issue)
                new_dataframes_dict_2[key] = clean

                if persist:
                    ns_code  = os.path.splitext(filename)[0]
                    out_dir  = os.path.join(out_root, str(yr))
                    out_path = os.path.join(out_dir, f"{ns_code}.parquet")
                    saved_path, n_rows, n_cols = _save_df(clean, out_path)
                    _append_manifest(
                        manifest_path=manifest_path,
                        ns_code=ns_code,
                        table_name="table_2",
                        year=yr,
                        issue=issue,
                        file_path=saved_path,
                        n_rows=n_rows,
                        n_cols=n_cols,
                        pipeline_version=pipeline_version,
                    )

                processed.add(filename)
                folder_new_count += 1
            except Exception as e:
                log_warn(logger, f"⚠️  {filename}: {e}")
                folder_skipped_count += 1

        # IMPORTANT: no prints/logs between these two blocks
        pbar.clear(); pbar.close()        # remove the '🚧' row

        # Finished bar (blue) — same row, looks like it “replaced” the first
        fb = tqdm(
            total=len(pdf_files),
            desc=f"✔️ {year}",
            unit="PDF",
            bar_format=BAR_FORMAT,
            colour=PROG_COLOR_DONE,
            leave=True,                   # keep visible
            position=0,                   # same row as pbar
            dynamic_ncols=True
        )
        fb.update(len(pdf_files))
        fb.close()

        # Update counters after the year is done
        new_counter += folder_new_count
        skipped_counter += folder_skipped_count

        # Persist updated record (chronological) after each year
        _write_records(record_folder, record_txt, list(processed))

    # Summary of fully skipped years (same style as Section 2)
    if skipped_years:
        years_summary = ", ".join(skipped_years.keys())
        total_skipped = sum(skipped_years.values())
        log_info(logger, f"\n⏩ {total_skipped} cleaned tables already generated for years: {years_summary}")

    # Final summary — mimic Section 2 format
    elapsed_time = round(time.time() - start_time)
    log_info(logger, f"\n📊 Summary:\n")
    log_info(logger, f"📂 {total_year_folders} folders (years) found containing input PDFs")
    log_info(logger, f"🗃️ Already cleaned tables: {skipped_counter}")
    log_info(logger, f"✨ Newly cleaned tables: {new_counter}")
    log_info(logger, f"⏱️ {elapsed_time} seconds")

    return raw_tables_dict_2, new_dataframes_dict_2





################################################################################################
# Section 4. Concatenated CSV Export
################################################################################################

# +++++++++++++++
# LIBRARIES
# +++++++++++++++

import os
import pandas as pd
import tkinter as tk  # GUI for choosing sector/frequency (if you use these helpers)
from tkinter import simpledialog

# Define the options and their mappings
options = [
    "gdp", 
    "agriculture",  # agriculture and livestock
    "fishing",
    "mining",       # mining and fuel
    "manufacturing",
    "electricity",  # electricity and water
    "construction",
    "commerce",
    "services"      # other services
]

# Mapping each option with its Spanish and English counterparts
option_mapping = {
    "gdp": ("pbi", "gdp"),
    "agriculture": ("agropecuario", "agriculture and livestock"),
    "fishing": ("pesca", "fishing"),
    "mining": ("mineria e hidrocarburos", "mining and fuel"),
    "manufacturing": ("manufactura", "manufacturing"),
    "electricity": ("electricidad y agua", "electricity and water"),
    "construction": ("construccion", "construction"),
    "commerce": ("comercio", "commerce"),
    "services": ("otros servicios", "other services")
}

# ------------------------------
# Small helper to save to CSV
# ------------------------------
def _save_concat_to_csv(df: pd.DataFrame, sector: str, frequency: str, out_dir: str = "data/clean/concatenated"):
    """
    Saves the provided DataFrame to CSV under:
        data/clean/concatenated/{frequency}/{sector}_{frequency}_growth_rates.csv
    """
    os.makedirs(os.path.join(out_dir, frequency), exist_ok=True)
    fname = f"{sector}_{frequency}_growth_rates.csv"
    fpath = os.path.join(out_dir, frequency, fname)
    df.to_csv(fpath, index=False, encoding="utf-8")
    print(f"Saved CSV: {fpath}")

# Function to show the option window
def show_option_window():
    """
    Displays a Tkinter window to select an option, and returns the corresponding 
    selected values for 'selected_spanish', 'selected_english', and 'sector'.
    """
    selected_spanish = None
    selected_english = None
    sector = None

    def save_option():
        nonlocal selected_spanish, selected_english, sector
        sector = selected_option.get()
        selected_spanish, selected_english = option_mapping[sector]
        root.destroy()

    root = tk.Tk()
    root.title("Select Option")

    selected_option = tk.StringVar(root)
    selected_option.set(options[0])  # Default

    menu = tk.OptionMenu(root, selected_option, *options)
    menu.pack(pady=10)

    confirm_button = tk.Button(root, text="Confirm", command=save_option)
    confirm_button.pack()

    root.update_idletasks()
    root.wait_window()
    return selected_spanish, selected_english, sector

# Function to show frequency window
def show_frequency_window():
    frequencies = ["monthly", "quarterly", "annual"]

    def save_frequency():
        root.destroy()

    root = tk.Tk()
    root.title("Select Frequency")

    selected_frequency = tk.StringVar(root)
    selected_frequency.set(frequencies[0])

    menu = tk.OptionMenu(root, selected_frequency, *frequencies)
    menu.pack(pady=10)

    confirm_button = tk.Button(root, text="Confirm", command=save_frequency)
    confirm_button.pack()

    root.update_idletasks()
    root.wait_window()
    return selected_frequency.get()

# **********************************************************************************************
# Section 4.1. Annual Concatenation (and related helpers)
# ----------------------------------------------------------------------------------------------

# Concatenate Table 2 (annual)
def concatenate_annual_df(dataframes_dict, sector_economico, economic_sector):
    dataframes_ending_with_2 = []
    dataframes_to_concatenate = []

    for df_name in dataframes_dict.keys():
        if df_name.endswith('_2'):
            dataframes_ending_with_2.append(df_name)
            dataframes_to_concatenate.append(dataframes_dict[df_name])

    if dataframes_to_concatenate:
        annual_growth_rates = pd.concat(
            [
                df[(df['sectores_economicos'] == sector_economico) | (df['economic_sectors'] == economic_sector)]
                for df in dataframes_to_concatenate
                if 'sectores_economicos' in df.columns and 'economic_sectors' in df.columns
            ],
            ignore_index=True
        )

        columns_to_keep = ['year', 'wr', 'date'] + [col for col in annual_growth_rates.columns if col.endswith('_year')]
        annual_growth_rates = annual_growth_rates[columns_to_keep]
        annual_growth_rates = annual_growth_rates.loc[:, ~annual_growth_rates.columns.duplicated()]
        annual_growth_rates.columns = [
            (col.split('_')[1] + '_' + col.split('_')[0]) if '_' in col and idx >= 3 else col
            for idx, col in enumerate(annual_growth_rates.columns)
        ]
        print("Number of rows in the concatenated dataframe:", len(annual_growth_rates))
        return annual_growth_rates
    else:
        print("No dataframes were found to concatenate.")
        return None

# Concatenate Table 1 (quarterly)
def concatenate_quarterly_df(dataframes_dict, sector_economico, economic_sector):
    dataframes_ending_with_2 = []
    dataframes_to_concatenate = []

    for df_name in dataframes_dict.keys():
        if df_name.endswith('_2'):
            dataframes_ending_with_2.append(df_name)
            dataframes_to_concatenate.append(dataframes_dict[df_name])

    if dataframes_to_concatenate:
        quarterly_growth_rates = pd.concat(
            [
                df[(df['sectores_economicos'] == sector_economico) | (df['economic_sectors'] == economic_sector)]
                for df in dataframes_to_concatenate
                if 'sectores_economicos' in df.columns and 'economic_sectors' in df.columns
            ],
            ignore_index=True
        )

        columns_to_keep = ['year', 'wr', 'date'] + [col for col in quarterly_growth_rates.columns if not col.endswith('_year')]
        quarterly_growth_rates = quarterly_growth_rates[columns_to_keep]
        quarterly_growth_rates.drop(columns=['sectores_economicos', 'economic_sectors'], inplace=True)
        quarterly_growth_rates = quarterly_growth_rates.loc[:, ~quarterly_growth_rates.columns.duplicated()]
        print("Number of rows in the concatenated dataframe:", len(quarterly_growth_rates))
        return quarterly_growth_rates
    else:
        print("No dataframes were found to concatenate.")
        return None

# Concatenate Table 1 (monthly)
def concatenate_monthly_df(dataframes_dict, sector_economico, economic_sector):
    dataframes_ending_with_1 = []
    dataframes_to_concatenate = []

    for df_name in dataframes_dict.keys():
        if df_name.endswith('_1'):
            dataframes_ending_with_1.append(df_name)
            dataframes_to_concatenate.append(dataframes_dict[df_name])

    if dataframes_to_concatenate:
        monthly_growth_rates = pd.concat(
            [
                df[(df['sectores_economicos'] == sector_economico) | (df['economic_sectors'] == economic_sector)]
                for df in dataframes_to_concatenate
                if 'sectores_economicos' in df.columns and 'economic_sectors' in df.columns
            ],
            ignore_index=True
        )

        columns_to_keep = ['year', 'wr', 'date'] + [
            col for col in monthly_growth_rates.columns if not (col.endswith('_year') or col.endswith('_mean'))
        ]
        monthly_growth_rates = monthly_growth_rates[columns_to_keep]
        monthly_growth_rates.drop(columns=['sectores_economicos', 'economic_sectors'], inplace=True)
        monthly_growth_rates = monthly_growth_rates.loc[:, ~monthly_growth_rates.columns.duplicated()]

        # Drop columns with at least two underscores in their names
        columns_to_drop = [col for col in monthly_growth_rates.columns if col.count('_') >= 2]
        monthly_growth_rates.drop(columns=columns_to_drop, inplace=True)

        monthly_growth_rates.columns = [
            (col.split('_')[1] + '_' + col.split('_')[0]) if '_' in col and idx >= 3 else col
            for idx, col in enumerate(monthly_growth_rates.columns)
        ]
        print("Number of rows in the concatenated dataframe:", len(monthly_growth_rates))
        return monthly_growth_rates
    else:
        print("No dataframes were found to concatenate.")
        return None

# ------------------------------
# Optional: one-call export helper
# ------------------------------
def concatenate_and_save(
    dataframes_dict,
    sector_key: str,
    frequency: str,
    out_dir: str = "data/clean/concatenated"
):
    """
    Runs the appropriate concatenate_* function and writes the CSV.
    """
    if sector_key not in option_mapping:
        raise ValueError(f"Unknown sector: {sector_key}")

    sector_es, sector_en = option_mapping[sector_key]

    func_map = {
        "monthly": concatenate_monthly_df,
        "quarterly": concatenate_quarterly_df,
        "annual": concatenate_annual_df,
    }
    if frequency not in func_map:
        raise ValueError(f"Unknown frequency: {frequency}")

    df = func_map[frequency](dataframes_dict, sector_es, sector_en)
    if df is not None:
        _save_concat_to_csv(df, sector_key, frequency, out_dir)
    return df

