
# *********************************************************************************************
#  Pipelines for new_gdp_rtd.ipynb
# *********************************************************************************************
#
#   Program       : new_gdp_rtd_pipeline.py
#   Project       : Peruvian GDP Revisions
#   Author        : Jason Cruz
#   Last updated  : 08/13/2025
#   Python        : 3.12
#
#   Overview: Helper functions used (together as a module) by the new_gdp_rtd.ipynb workflow.
#
#   Sections:
#       1. Downloading PDFs ...................................................................
#       2. Generating input PDFs ..............................................................
#       3. Cleaning data and building vintages ................................................
#       4. Concatenate vintages across years by frequency......................................
# 
#   Notes:
#       "NS/ns" (Nota Semanal, Spanish) is equivalent to "WR/wr" (Weekly Report).
# *********************************************************************************************



# ##############################################################################################
# SECTION 1 Downloading PDFs
# ##############################################################################################

# In this section we build an automated downloader for BCRP's Weekly Reports (WR) using
# Selenium-based web scraping to mimic a human browser session and avoid duplicate downloads.


# ++++++++++++++++++++++++++++++++++++++++++++++++
# Libraries
# ++++++++++++++++++++++++++++++++++++++++++++++++

import os                                                                   # Path utilities and directory management
import re                                                                   # Filename parsing and natural sorting helpers
import time                                                                 # Execution timing and simple profiling
import random                                                               # Randomized backoff/wait durations
import shutil                                                               # High-level file operations (move/copy/rename/delete)

import requests                                                             # HTTP client for downloading files
from requests.adapters import HTTPAdapter                                   # Attach retry/backoff to requests
from urllib3.util.retry import Retry                                        # Exponential backoff policy

import pygame                                                               # Audio playback for notification sounds

from selenium import webdriver                                              # Browser automation
from selenium.webdriver.common.by import By                                 # Element location strategies
from selenium.webdriver.support.ui import WebDriverWait                     # Explicit waits
from selenium.webdriver.support import expected_conditions as EC            # Wait conditions
from selenium.common.exceptions import StaleElementReferenceException       # Dynamic DOM handling

from webdriver_manager.chrome import ChromeDriverManager                    # ChromeDriver provisioning
from selenium.webdriver.chrome.options import Options as ChromeOptions      # Chrome options
from selenium.webdriver.chrome.service import Service as ChromeService      # Chrome service

from webdriver_manager.firefox import GeckoDriverManager                    # GeckoDriver provisioning
from selenium.webdriver.firefox.options import Options as FirefoxOptions    # Firefox options
from selenium.webdriver.firefox.service import Service as FirefoxService    # Firefox service

from webdriver_manager.microsoft import EdgeChromiumDriverManager           # EdgeDriver provisioning
from selenium.webdriver.edge.options import Options as EdgeOptions          # Edge options
from selenium.webdriver.edge.service import Service as EdgeService          # Edge service


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

    # Update the record log in chronological order (year → issue)
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
            return (9999, 9999, base)                                           # Unknown pattern → sort last
        issue, year = int(m.group(1)), int(m.group(2))
        return (year, issue)

    records.sort(key=_ns_key)                                                   # Chronological order
    os.makedirs(download_record_folder, exist_ok=True)
    with open(record_path, "w", encoding="utf-8") as f:
        f.write("\n".join(records) + ("\n" if records else ""))                 # Trailing newline if non-empty

    print(f"{download_counter}. ✅ Downloaded: {file_name}")
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
      2) Reverses the order to download from oldest → newest for stable local numbering.
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

        pdf_links = pdf_links[::-1]                                         # Oldest → newest for stable local ordering

        # Build a work queue, skipping any file already recorded
        for link in pdf_links:
            try:
                file_url  = link.get_attribute("href")                      # Resolve the URL bound to the anchor
                file_name = os.path.basename(file_url)                      # Server-provided filename (e.g., ns-07-2019.pdf)
            except Exception:
                continue                                                    # Defensive skip if attributes are momentarily unavailable

            if file_name in downloaded_files:
                skipped_files.append(file_name)                             # Already captured in prior runs → skip
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
        return f"ns-{int(m.group(1)):02d}-{m.group(2)}"                     # Zero-pad issue (e.g., 7 → 07)

    def url(c: str) -> str:
        cc = norm(c)                                                        # Normalized 'ns-xx-yyyy'
        return f"https://www.bcrp.gob.pe/docs/Publicaciones/Nota-Semanal/{cc[-4:]}/{cc}.pdf"  # Year-coded path

    def _ns_key(name: str) -> tuple[int, int, str]:
        base = os.path.splitext(os.path.basename(name))[0]                  # Remove dir and extension
        m = re.search(r"ns-(\d{2})-(\d{4})", base, re.I)                    # Extract issue/year for sorting
        if not m:
            return (9999, 9999, base)                                       # Unknowns last, stable by base
        issue, year = int(m.group(1)), int(m.group(2))
        return (year, issue, base)                                          # Sort by (year → issue → name)

    def update_record(add: str | None = None, remove: str | None = None) -> None:
        p = os.path.join(record_folder, download_record_txt)                # Record file path
        s: set[str] = set()
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                s = {x.strip() for x in f if x.strip()}                     # Read, trim, de-duplicate
        if add:
            s.add(add)                                                      # Append replacement filename
        records = sorted(s, key=_ns_key)                                    # Chronological order (year → issue)
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
                r.raise_for_status()                                        # Non-2xx → raise HTTPError
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
        print("\n✅ PDF replacement summary")
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
# SECTION 2 Generating input PDFs
# ##############################################################################################

# In this section we build an automated input PDF generator for WR PDFs. It searches pages by
# keywords, produces shortened “input” PDFs, and (for 4-page outputs) keeps only pages 1 and 3
# where the key tables usually appear. A simple record prevents re-processing.


# ++++++++++++++++++++++++++++++++++++++++++++++++
# Libraries
# ++++++++++++++++++++++++++++++++++++++++++++++++

import os                                                     # Path utilities and directory management
import re                                                     # Pattern matching for NS codes and sorting
import time                                                   # Execution timing
import shutil                                                 # File moves for quarantine handling

import requests                                               # HTTP client for replacement downloads
import fitz                                                   # Lightweight PDF editing (PyMuPDF)
import ipywidgets as widgets                                  # Jupyter UI elements (used in this section's workflow)
from IPython.display import display                           # Render widgets in notebooks
from tqdm.notebook import tqdm                                # Progress bars in Jupyter
import pdfplumber                                             # Rich PDF text extraction (not heavily used here)
from PyPDF2 import PdfReader, PdfWriter                       # Page-level PDF edits (keep/select pages)


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
        return 0                                                           # Nothing to keep → skip

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
        return set(ln.strip() for ln in f if ln.strip())                   # Trim blanks and deduplicate via set

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
        for fn in sorted(input_pdf_files):                                  # Stable, predictable order
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
    skipped_years = {}                                                                      # Map year → count already processed
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

        print(f"✅ Shortened PDFs saved in '{input_pdf_folder}' "
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
# SECTION 3 Cleaning data and building vintages
# ##############################################################################################

# ==============================================================================================
# SECTION 3.1  Creating functions for extracting, parsing and cleaning-up data from input PDFs 
# ==============================================================================================
# In this section we prepare reusable helpers to parse WR-derived tables (Table 1 and Table 2)
# extracted from input PDFs. Utilities include text normalization, column/row fixes, selective
# splitting, roman–arabic conversions, and tailored corrections for specific WR issues.


# ++++++++++++++++++++++++++++++++++++++++++++++++
# Libraries
# ++++++++++++++++++++++++++++++++++++++++++++++++

# Auxiliary functions (used within other functions)
import re                                                            # Regular expressions for pattern matching
import unicodedata                                                   # Unicode normalization (strip diacritics)

# Functions for both Table 1 and Table 2
import pandas as pd                                                  # DataFrame manipulation

# Functions only for Table 1 
import numpy as np                                                   # Numerical helpers

# Functions only for Table 2
import roman                                                         # Roman → Arabic numeral conversion


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
    texto = re.sub(r'\s*-\s*', '-', texto)                           # Normalize "a - b" → "a-b"
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
    df = df.dropna(how='all')
    return df

# _________________________________________________________________________
# Function to drop columns where all values are NaN
def drop_nan_columns(df):
    """Drop any column that is entirely NaN."""
    return df.dropna(axis=1, how='all')

# _________________________________________________________________________
# Function to swap first and second rows in both the first and last columns
def swap_first_second_row(df):
    """Swap [row0,row1] in first and last columns to fix misplaced headers."""
    temp = df.iloc[0, 0]
    df.iloc[0, 0] = df.iloc[1, 0]
    df.iloc[1, 0] = temp

    temp = df.iloc[0, -1]
    df.iloc[0, -1] = df.iloc[1, -1]
    df.iloc[1, -1] = temp
    return df

# _________________________________________________________________________
# Function to reset the DataFrame index
def reset_index(df):
    """Reset index to a simple RangeIndex after row drops/reorders."""
    df.reset_index(drop=True, inplace=True)
    return df

# _________________________________________________________________________
# Function to remove digit-slash prefixes in first and last two columns
def remove_digit_slash(df):
    """Strip patterns like '12/' at the start of values in [first, penultimate, last] columns."""
    df.iloc[:, [0, -2, -1]] = df.iloc[:, [0, -2, -1]].apply(
        lambda x: x.str.replace(r'\d+/', '', regex=True)
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
    for index, row in df.iterrows():
        token = str(row.iloc[-2])
        if any(char.isdigit() for char in token) and any(char.isalpha() for char in token):
            if pd.isnull(row.iloc[-1]):                                   # Only split if target is empty
                df.loc[index, df.columns[-1]] = ''.join(                  # Letters + spaces → last column
                    filter(lambda x: x.isalpha() or x == ' ', token)
                )
                df.loc[index, df.columns[-2]] = ''.join(                  # Non-letters → penultimate column
                    filter(lambda x: not (x.isalpha() or x == ' '), token)
                )

            # Detect decimal separator (',' preferred; fallback '.')
            if ',' in token:
                parts = token.split(',')
            elif '.' in token:
                parts = token.split('.')
            else:
                parts = [token, '']

            cleaned_integer = ''.join(filter(lambda x: x.isdigit() or x == '-', parts[0]))
            cleaned_decimal = ''.join(filter(lambda x: x.isdigit(), parts[1]))
            cleaned_numeric = f"{cleaned_integer},{cleaned_decimal}" if cleaned_decimal else cleaned_integer
            df.loc[index, df.columns[-2]] = cleaned_numeric
    return df

# _________________________________________________________________________
# Function to list columns that are 4-digit years
def extract_years(df):
    """Return a list of column names that are pure 4-digit years."""
    year_columns = [col for col in df.columns if re.match(r'\b\d{4}\b', col)]
    return year_columns

# _________________________________________________________________________
# Function to promote first row to header
def first_row_columns(df):
    """Set first row as header and drop it from the data area."""
    df.columns = df.iloc[0]
    df = df.drop(df.index[0])
    return df

# _________________________________________________________________________
# Function to clean column names and string values across the DataFrame
def clean_columns_values(df):
    """
    Normalize headers to lowercase ASCII with underscores and replace 'ano'→'year'.
    Convert string numeric commas to dots across textual and numeric columns.
    Lowercase and sanitize the sector label columns.
    """
    df.columns = df.columns.str.lower()                                   # Lowercase headings
    df.columns = [
        unicodedata.normalize('NFKD', col).encode('ASCII', 'ignore').decode('utf-8')
        if isinstance(col, str) else col
        for col in df.columns
    ]
    df.columns = df.columns.str.replace(' ', '_').str.replace('ano', 'year').str.replace('-', '_')

    text_columns = df.select_dtypes(include='object').columns             # Capture textual columns (not strictly required)

    for col in df.columns:
        df.loc[:, col] = df[col].apply(lambda x: remove_tildes(x) if isinstance(x, str) else x)
        df.loc[:, col] = df[col].apply(lambda x: str(x).replace(',', '.') if isinstance(x, (int, float, str)) else x)

    df.loc[:, 'sectores_economicos'] = df['sectores_economicos'].str.lower()
    df.loc[:, 'economic_sectors']   = df['economic_sectors'].str.lower()
    df.loc[:, 'sectores_economicos'] = df['sectores_economicos'].apply(remove_rare_characters)
    df.loc[:, 'economic_sectors']    = df['economic_sectors'].apply(remove_rare_characters)
    return df

# _________________________________________________________________________
# Function to convert all non-excluded columns to numeric
def convert_float(df):
    """Convert all columns except sector-label columns to numeric (coerce on failure)."""
    excluded_columns   = ['sectores_economicos', 'economic_sectors']
    columns_to_convert = [col for col in df.columns if col not in excluded_columns]
    df[columns_to_convert] = df[columns_to_convert].apply(pd.to_numeric, errors='coerce')
    return df

# _________________________________________________________________________
# Function to move the last column into second position
def relocate_last_column(df):
    """Relocate the last column to position index 1, preserving order of the rest."""
    last_column = df.pop(df.columns[-1])
    df.insert(1, last_column.name, last_column)
    return df

# _________________________________________________________________________
# Function to normalize the very first row of the DataFrame
def clean_first_row(df):
    """
    Lowercase, remove tildes and rare characters for first-row cells,
    and translate 'ano'→'year' in-place.
    """
    for col in df.columns:
        if df[col].dtype == 'object':
            if isinstance(df.at[0, col], str):
                df.at[0, col] = df.at[0, col].lower()
                df.at[0, col] = remove_tildes(df.at[0, col])
                df.at[0, col] = remove_rare_characters_first_row(df.at[0, col])
                df.at[0, col] = df.at[0, col].replace('ano', 'year')
    return df

# _________________________________________________________________________
# Function to rename month 'set'→'sep' in headings
def replace_set_sep(df):
    """Rename any column containing 'set' to use 'sep' instead."""
    columns = df.columns
    for column in columns:
        if 'set' in column:
            new_column = column.replace('set', 'sep')
            df.rename(columns={column: new_column}, inplace=True)
    return df

# _________________________________________________________________________
# Function to strip extra spaces in sector label columns
def spaces_se_es(df):
    """Trim surrounding spaces from sector label columns in ES/EN."""
    df['sectores_economicos'] = df['sectores_economicos'].str.strip()
    df['economic_sectors']    = df['economic_sectors'].str.strip()
    return df

# _________________________________________________________________________
# Function to unify 'services' naming across ES/EN sector labels
def replace_services(df):
    """Replace 'servicios'→'otros servicios' and 'services'→'other services' when both columns contain those tokens."""
    if ('servicios' in df['sectores_economicos'].values) and ('services' in df['economic_sectors'].values):
        df['sectores_economicos'].replace({'servicios': 'otros servicios'}, inplace=True)
        df['economic_sectors'].replace({'services': 'other services'}, inplace=True)
    return df

# _________________________________________________________________________
# Function to unify 'mineria' naming in ES sector labels
def replace_mineria(df):
    """Replace 'mineria'→'mineria e hidrocarburos' when the latter is otherwise absent."""
    if ('mineria' in df['sectores_economicos'].values) and ('mineria e hidrocarburos' not in df['sectores_economicos'].values):
        df['sectores_economicos'].replace({'mineria': 'mineria e hidrocarburos'}, inplace=True)
    return df

# _________________________________________________________________________
# Function to unify 'mining and fuels' naming in EN sector labels
def replace_mining(df):
    """Replace 'mining and fuels'→'mining and fuel' in EN sector labels."""
    if ('mining and fuels' in df['economic_sectors'].values):
        df['economic_sectors'].replace({'mining and fuels': 'mining and fuel'}, inplace=True)
    return df

# _________________________________________________________________________
# Function to round all float64 columns to the given number of decimals
def rounding_values(df, decimals=1):
    """Round float64 columns to `decimals` places."""
    for col in df.columns:
        if df[col].dtype == 'float64':
            df[col] = df[col].round(decimals)
    return df


# ++++++++++++++++++++++++++++++++++++++++++++++++
# Utilities only for cleaning Table 1 
# ++++++++++++++++++++++++++++++++++++++++++++++++

# _________________________________________________________________________
# Function to relocate trailing values when the last column is already filled
def relocate_last_columns(df):
    """
    If the last column's second row is non-null, create a helper column and relocate
    the penultimate header value to the last column header, clearing the original spot.
    """
    if not pd.isna(df.iloc[1, -1]):
        new_column = 'col_' + ''.join(map(str, np.random.randint(1, 5, size=1)))  # Temporary helper column name
        df[new_column] = np.nan

        insert_value_1 = df.iloc[0, -2]                                   # Value to transfer into last column header
        insert_value_1 = str(insert_value_1)
        df.iloc[:, -1] = df.iloc[:, -1].astype('object')                  # Ensure string-capable dtype
        df.iloc[0, -1] = insert_value_1

        df.iloc[0, -2] = np.nan                                           # Clear original position
    return df

# _________________________________________________________________________
# Function to build composite month headers from the first row and year columns
def get_months_sublist_list(df, year_columns):
    """
    Parse the first row to collect month tokens and compose headers as <year>_<month>.
    Preserve the first two original elements if they are not present in the new header list.
    """
    first_row = df.iloc[0]
    months_sublist_list = []
    months_sublist = []

    for item in first_row:
        if len(str(item)) == 3:                                           # Likely month abbreviations (e.g., 'jan')
            months_sublist.append(item)
        elif '-' in item or str(item) == 'year':                          # Group boundary markers
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

    two_first_elements = df.iloc[0][:2].tolist()                          # Safeguard leading identifiers
    for index in range(len(two_first_elements) - 1, -1, -1):
        if two_first_elements[index] not in new_elements:
            new_elements.insert(0, two_first_elements[index])

    while len(new_elements) < len(df.columns):                            # Pad to full width if needed
        new_elements.append(None)

    temp_df = pd.DataFrame([new_elements], columns=df.columns)            # Single-row temporary DF for aligned assignment
    df.iloc[0] = temp_df.iloc[0]
    return df

# _________________________________________________________________________
# Function to infer/correct a year header based on the position of 'year' token
def find_year_column(df):
    """
    Detect 4-digit year columns; if a single year is present and a 'year' token appears
    in a different column header position, rename that token to the adjacent year (±1).
    """
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
            column_contains_year_name  = column_contains_year.index[0]
            column_contains_year_index = df.columns.get_loc(column_contains_year_name)
            year_name_index            = df.columns.get_loc(year_name)

            if column_contains_year_index < year_name_index:
                new_year = str(int(year_name) - 1)                        # If 'year' is to the left, assign previous year
                df.rename(columns={column_contains_year_name: new_year}, inplace=True)
            elif column_contains_year_index > year_name_index:
                new_year = str(int(year_name) + 1)                        # If 'year' is to the right, assign next year
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
    if len(df.columns) < 2:
        print("The DataFrame has less than two columns. Values cannot be exchanged.")
        return df

    if df.iloc[:, -1].isnull().any():
        last_column_rows_nan = df[df.iloc[:, -1].isnull()].index

        for idx in last_column_rows_nan:
            if -2 >= -len(df.columns):
                df.iloc[idx, -1], df.iloc[idx, -2] = df.iloc[idx, -2], df.iloc[idx, -1]

    return df

# _________________________________________________________________________
# Function to standardize "Var. %" tokens in the first column (ES)
def replace_var_perc_first_column(df):
    """Replace 'Var.%' variants with 'variacion porcentual' in the first column."""
    regex = re.compile(r'Var\. ?%')

    for index, row in df.iterrows():
        value = str(row.iloc[0])
        if regex.search(value):
            df.at[index, df.columns[0]] = regex.sub("variacion porcentual", value)
    return df

# _________________________________________________________________________
# Function to normalize moving-average descriptors in the last column
number_moving_average = 'three'  # Keep a space at the end
def replace_number_moving_average(df):
    """
    Replace patterns like '2 -' at the start of tokens in the last column with a
    normalized text (e.g., 'three-'), using the global `number_moving_average`.
    """
    for index, row in df.iterrows():
        if pd.notnull(row.iloc[-1]) and re.search(r'(\d\s*-)', str(row.iloc[-1])):
            df.at[index, df.columns[-1]] = re.sub(
                r'(\d\s*-)', f'{number_moving_average}-', str(row.iloc[-1])
            )
    return df

# _________________________________________________________________________
# Function to standardize "Var. %" tokens in the last two columns (EN)
def replace_var_perc_last_columns(df):
    """Replace 'Var.%' variants with 'percent change' in the last two columns."""
    regex = re.compile(r'(Var\. ?%)(.*)')

    for index, row in df.iterrows():
        if isinstance(row.iloc[-2], str) and regex.search(row.iloc[-2]):
            replaced_text = regex.sub(r'\2 percent change', row.iloc[-2])
            df.at[index, df.columns[-2]] = replaced_text.strip()
        
        if isinstance(row.iloc[-1], str) and regex.search(row.iloc[-1]):
            replaced_text = regex.sub(r'\2 percent change', row.iloc[-1])
            df.at[index, df.columns[-1]] = replaced_text.strip()
    return df

# _________________________________________________________________________
# Function to change the first dot in row 2 into a hyphen pattern across columns
def replace_first_dot(df):
    """If a cell on the second row matches 'Word.Word', replace the first dot with a hyphen."""
    second_row = df.iloc[1]

    if any(isinstance(cell, str) and re.match(r'^\w+\.\s?\w+', cell) for cell in second_row):
        for col in df.columns:
            if isinstance(second_row[col], str):
                if re.match(r'^\w+\.\s?\w+', second_row[col]):
                    df.at[1, col] = re.sub(r'(\w+)\.(\s?\w+)', r'\1-\2', second_row[col], count=1)
    return df

# _________________________________________________________________________
# Function to drop rows containing the '}' character anywhere
def drop_rare_caracter_row(df):
    """Remove any row where the '}' character appears in any cell."""
    rare_caracter_row = df.apply(lambda row: '}' in row.values, axis=1)
    df = df[~rare_caracter_row]
    return df

# _________________________________________________________________________
# Function to split a column into two if second-row pattern 'Word.Word' is found
def split_column_by_pattern(df):
    """
    For columns whose second row matches 'Title.Title', split the column by whitespace
    and insert the second token into a new '<col>_split' column immediately to the right.
    """
    for col in df.columns:
        if re.match(r'^[A-Z][a-z]+\.?\s[A-Z][a-z]+\.?$', str(df.iloc[1][col])):
            split_values = df[col].str.split(expand=True)
            df[col] = split_values[0]
            new_col_name = col + '_split'
            df.insert(df.columns.get_loc(col) + 1, new_col_name, split_values[1])
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
    if pd.isna(df.iloc[0, 0]) and df.iloc[0, 1] == "SECTORES ECONÓMICOS":
        column_1_value = df.iloc[0, 1]
        df.iloc[0, 0] = column_1_value
        df.iloc[0, 1] = np.nan
        df = df.drop(df.columns[1], axis=1)
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
    if any(isinstance(element, str) and element.isdigit() and len(element) == 4 for element in df.iloc[0]):
        for col_index, value in enumerate(df.iloc[0]):
            if pd.isna(value):
                df.iloc[0, col_index] = f"column_{col_index + 1}"
        df.columns = df.iloc[0]
        df = df.drop(df.index[0])
    return df

# _________________________________________________________________________
# Function to expand a column by fixing 'a-b' into 'a b' and splitting trailing text out
def expand_column(df):
    """
    For the penultimate column, normalize 'word-word' to 'word word', move trailing text to
    the last column (row-wise), and keep only numeric/textual parts separated.
    """
    column_to_expand = df.columns[-2]
    
    def replace_hyphens(match_obj):
        return match_obj.group(1) + ' ' + match_obj.group(2)

    if df[column_to_expand].str.contains(r'\d').any() and df[column_to_expand].str.contains(r'[a-zA-Z]').any():
        df[column_to_expand] = df[column_to_expand].apply(
            lambda x: re.sub(r'([a-zA-Z]+)\s*-\s*([a-zA-Z]+)', replace_hyphens, str(x)) if pd.notnull(x) else x
        )
        
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

# _________________________________________________________________________
# Function to split penultimate column into multiple columns (whitespace)
def split_values_1(df):
    """Split the penultimate column by whitespace and insert the parts before the last column."""
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

# _________________________________________________________________________
# Function to re-label year-pair headers and backfill single-year cells in row 0
def check_first_row(df):
    """
    Detect cells like '2018 2019' in row 0; rename such columns with synthetic names and backfill
    year tokens into the first two columns if missing.
    """
    first_row = df.iloc[0]
    
    for i, (col, value) in enumerate(first_row.items()):
        if re.search(r'\b\d{4}\s\d{4}\b', str(value)):
            years = value.split()
            first_year  = years[0]
            second_year = years[1]
            
            original_column_name = f'col_{i}'
            df.at[0, col] = original_column_name
            
            if pd.isna(df.iloc[0, 0]):
                df.iloc[0, 0] = first_year
            
            if pd.isna(df.iloc[0, 1]):
                df.iloc[0, 1] = second_year
    
    return df

# _________________________________________________________________________
# Function to swap values between adjacent columns when right column has NaNs
def replace_nan_with_previous_column_3(df):
    """
    For any pair of adjacent columns where the right column has NaNs and does not end with '_year',
    swap values with the left column that ends with '_year' and is fully non-null.
    """
    columns = df.columns
    
    for i in range(len(columns) - 1):
        if i != len(columns) - 1 and (columns[i].endswith('_year') and not df[columns[i]].isnull().any()):
            if df[columns[i+1]].isnull().any() and not columns[i+1].endswith('_year'):
                nan_indices = df[columns[i+1]].isnull()
                df.loc[nan_indices, [columns[i], columns[i+1]]] = df.loc[nan_indices, [columns[i+1], columns[i]]].values
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

# _________________________________________________________________________
# Function to split the 4th-from-last column into multiple columns (whitespace)
def split_values_2(df):
    """Split the fourth-from-last column and insert new parts before the last three columns."""
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

# _________________________________________________________________________
# Function to split the third-from-last column into multiple columns (whitespace)
def split_values_3(df):
    """Split the third-from-last column and insert new parts before the last two columns."""
    column_to_expand = df.columns[-3]
    new_columns = df[column_to_expand].str.split(expand=True)
    new_columns.columns = [f'{column_to_expand}_{i+1}' for i in range(new_columns.shape[1])]
    insertion_position = len(df.columns) - 2
    for col in reversed(new_columns.columns):
        df.insert(insertion_position, col, new_columns[col])
    df.drop(columns=[column_to_expand], inplace=True)
    return df

# _________________________________________________________________________
# Function to swap with previous column when the right column has NaNs (variant 1)
def replace_nan_with_previous_column_1(df):
    """Swap values with the previous column if the right one contains NaNs and is not a '_year' column."""
    columns = df.columns
    
    for i in range(len(columns) - 1):
        if i != len(columns) - 2 and not (columns[i].endswith('_year') and df[columns[i]].isnull().any()):
            if df[columns[i+1]].isnull().any() and not columns[i+1].endswith('_year'):
                nan_indices = df[columns[i+1]].isnull()
                df.loc[nan_indices, [columns[i], columns[i+1]]] = df.loc[nan_indices, [columns[i+1], columns[i]]].values
    return df

# _________________________________________________________________________
# Function to swap with previous column when the right column has NaNs (variant 2)
def replace_nan_with_previous_column_2(df):
    """Same as variant 1; included for WR-specific patterns that require a second pass."""
    columns = df.columns
    
    for i in range(len(columns) - 1):
        if i != len(columns) - 2 and not (columns[i].endswith('_year') and df[columns[i]].isnull().any()):
            if df[columns[i+1]].isnull().any() and not columns[i+1].endswith('_year'):
                nan_indices = df[columns[i+1]].isnull()
                df.loc[nan_indices, [columns[i], columns[i+1]]] = df.loc[nan_indices, [columns[i+1], columns[i]]].values
    return df


# ++++++++++++++++++++++++++++++++++++++++++++++++
# Utilities only for cleaning Table 2
# ++++++++++++++++++++++++++++++++++++++++++++++++

# _________________________________________________________________________
# Function to split two space-separated years in the penultimate column into two columns
def separate_years(df):
    """
    If the penultimate header cell contains 'YYYY YYYY', keep the first in place and
    insert the second as a new column immediately before the last column.
    """
    df = df.copy()
    if isinstance(df.iloc[0, -2], str) and len(df.iloc[0, -2].split()) == 2:
        years = df.iloc[0, -2].split()
        if all(len(year) == 4 for year in years):
            second_year = years[1]
            df.iloc[0, -2] = years[0]
            df.insert(len(df.columns) - 1, 'new_column', [second_year] + [None] * (len(df) - 1))
    return df

# _________________________________________________________________________
# Function to move Roman numerals from the last cell in row 2 into a new column
def relocate_roman_numerals(df):
    """
    Detect Roman numerals in the third row's last column, strip them from that cell,
    move them into 'new_column', and set the original cell to NaN.
    """
    roman_numerals = find_roman_numerals(df.iloc[2, -1])
    if roman_numerals:
        original_text = df.iloc[2, -1]
        for roman_numeral in roman_numerals:
            original_text = original_text.replace(roman_numeral, '').strip()
        df.iloc[2, -1] = original_text
        df.at[2, 'new_column'] = ', '.join(roman_numerals)
        df.iloc[2, -1] = np.nan
    return df

# _________________________________________________________________________
# Function to extract '<number,text>' pairs from third-last column into the second-last column
def extract_mixed_values(df):
    """
    If the third-from-last column contains patterns like '-1,2 text', move that token into
    the penultimate column (when empty) and clean the source cell.
    """
    df = df.copy()
    regex_pattern = r'(-?\d+,\d [a-zA-Z\s]+)'
    for index, row in df.iterrows():
        third_last_obs  = row.iloc[-3]
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

# _________________________________________________________________________
# Function to fill NaN values in the first row with their column names
def replace_first_row_nan(df):
    """Replace NaNs in the first row with the corresponding column name."""
    for col in df.columns:
        if pd.isna(df.iloc[0][col]):
            df.iloc[0, df.columns.get_loc(col)] = col
    return df

# _________________________________________________________________________
# Function to convert Roman numerals in the first row to Arabic numerals
def roman_arabic(df):
    """Convert any Roman numeral tokens in the first row into Arabic numerals."""
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

# _________________________________________________________________________
# Function to fix duplicate numeric headers in the first row by incrementing duplicates
def fix_duplicates(df):
    """
    Ensure strictly increasing numeric tokens across the first row when duplicates appear.
    Subsequent duplicates are incremented in sequence.
    """
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

# _________________________________________________________________________
# Function to build composite quarter headers per year: <year>_<q>
def get_quarters_sublist_list(df, year_columns):
    """
    Parse the first row to collect single-char quarter labels and compose headers as <year>_<q>.
    Preserve the first two original elements if they are not present in the result.
    """
    first_row = df.iloc[0]
    quarters_sublist_list = []                                            # List of quarter groups per year
    quarters_sublist = []

    for item in first_row:
        if len(str(item)) == 1:                                           # Single-char quarter (e.g., '1','2','3','4')
            quarters_sublist.append(item)
        elif str(item) == 'year':
            quarters_sublist.append(item)
            quarters_sublist_list.append(quarters_sublist)
            quarters_sublist = []

    if quarters_sublist:
        quarters_sublist_list.append(quarters_sublist)

    new_elements = []
    if year_columns:
        for i, year in enumerate(year_columns):
            if i < len(quarters_sublist_list):
                for element in quarters_sublist_list[i]:
                    new_elements.append(f"{year}_{element}")

    two_first_elements = df.iloc[0][:2].tolist()                          # Preserve first two header tokens
    for index in range(len(two_first_elements) - 1, -1, -1):
        if two_first_elements[index] not in new_elements:
            new_elements.insert(0, two_first_elements[index])

    while len(new_elements) < len(df.columns):                            # Pad to full width if needed
        new_elements.append(None)

    temp_df = pd.DataFrame([new_elements], columns=df.columns)
    df.iloc[0] = temp_df.iloc[0]
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
    if df.iloc[0].isnull().all():
        df = df.drop(index=0)
        df.reset_index(drop=True, inplace=True)
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
    if df[df.columns[-1]].iloc[0] == 'ECONOMIC SECTORS':
        if pd.notnull(df[df.columns[-1]].iloc[1]):
            new_column_name = f"col_{len(df.columns)}"                    # Name helper column based on width
            df[new_column_name] = np.nan

            insert_value = df.iloc[0, -2]                                 # Move penultimate header into last
            insert_value = str(insert_value)
            df.iloc[:, -1] = df.iloc[:, -1].astype('object')
            df.iloc[0, -1] = insert_value

            df.iloc[0, -2] = np.nan                                       # Clear original position
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
    nan_column = None
    for column in df.columns:
        if df[column].isnull().all() and len(column) == 4 and column.isdigit():
            nan_column = column
            break
    
    if nan_column:
        column_index = df.columns.get_loc(nan_column)
        if column_index > 0:
            left_column = df.columns[column_index - 1]
            if not (len(left_column) == 4 and left_column.isdigit()):
                df.rename(columns={nan_column: left_column, left_column: nan_column}, inplace=True)
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
    for col_idx, value in enumerate(df.iloc[1]):
        if isinstance(value, str):
            if value.upper() == 'AÑO' or (value.isalpha() and roman.fromRoman(value.upper())):
                next_col_idx = col_idx + 1
                if next_col_idx < len(df.columns) and pd.isna(df.iloc[1, next_col_idx]):
                    current_col = df.iloc[:, col_idx].drop(index=1)        # Exclude header row from emptiness check
                    next_col    = df.iloc[:, next_col_idx].drop(index=1)
                    if current_col.isna().all():
                        df.iloc[1, col_idx], df.iloc[1, next_col_idx] = df.iloc[1, next_col_idx], df.iloc[1, col_idx]
    return df



# ==============================================================================================
# SECTION 3.2 Building friendly pipelines for running data cleaning for both Table 1 and Table 2
# ==============================================================================================
# In this section we define utility functions that support the processing of WR-derived data.
# These pipelines run previous tools for extracting metadata from filenames, reading/writing records,
# extracting tables from PDFs, and saving cleaned data to disk in appropriate formats.

# ++++++++++++++++++++++++++++++++++++++++++++++++
# Libraries
# ++++++++++++++++++++++++++++++++++++++++++++++++

import os                                                              # Operating system utilities (e.g., file path handling)
import re                                                              # Regular expressions for pattern matching
import time                                                            # Time handling (e.g., for timeouts and delays)
import hashlib                                                         # SHA-256 hash generation for file verification
import pandas as pd                                                    # DataFrame manipulation (table operations)
from tqdm.notebook import tqdm                                         # Progress bar for Jupyter notebooks (UI)
import tabula                                                          # PDF table extraction (Java backend, requires tabula-py)


# ++++++++++++++++++++++++++++++++++++++++++++++++
# Utility functions for handling WR file metadata,
# records, and table extraction
# ++++++++++++++++++++++++++++++++++++++++++++++++

# _________________________________________________________________________
# Function to extract issue and year from filenames like 'ns-07-2017.pdf'
def parse_ns_meta(file_name: str) -> tuple[str | None, str | None]:
    """
    Extracts issue number and year from the filename of the format 'ns-xx-yyyy.pdf'.
    
    Args:
        file_name (str): Path or name of the PDF file.

    Returns:
        tuple[str | None, str | None]: Tuple containing issue number and year, 
        or (None, None) if the filename format is invalid.
    """
    m = re.search(r"ns-(\d{1,2})-(\d{4})", os.path.basename(file_name).lower())     # Regular expression to capture issue and year
    return (m.group(1), m.group(2)) if m else (None, None)                          # Return extracted values or None

# _________________________________________________________________________
# Function to generate sorting key based on (year, issue) for stable file ordering
def _ns_sort_key(s: str) -> tuple[int, int, str]:
    """
    Generates a sorting key for filenames of the form 'ns-xx-yyyy.pdf',
    prioritizing chronological sorting by year and issue number.

    Args:
        s (str): Filename or basename of a PDF file.

    Returns:
        tuple[int, int, str]: Tuple containing year, issue, and basename for stable ordering.
    """
    base = os.path.splitext(os.path.basename(s))[0]                             # Strip file extension and extract basename
    m = re.search(r"ns-(\d{1,2})-(\d{4})", base, re.I)                          # Extract issue and year using regex
    if not m:
        return (9999, 9999, base)                                               # If pattern doesn't match, place it last
    issue, year = int(m.group(1)), int(m.group(2))                              # Convert issue and year to integers
    return (year, issue, base)                                                  # Return for stable sorting

# _________________________________________________________________________
# Function to read existing records from a file and return them as a sorted list
def _read_records(record_folder: str, record_txt: str) -> list[str]:
    """
    Reads previously processed file records from a specified text file, ensuring no duplicates 
    and that filenames are sorted by year and issue number.

    Args:
        record_folder (str): Folder path where record file is stored.
        record_txt (str): Record file name containing previously processed filenames.

    Returns:
        list[str]: Sorted and deduplicated list of filenames, ordered by year and issue.
    """
    path = os.path.join(record_folder, record_txt)                              # Combine folder and filename to get full path
    if not os.path.exists(path):                                                # If the record file doesn't exist, return empty list
        return []
    with open(path, "r", encoding="utf-8") as f:
        items = [ln.strip() for ln in f if ln.strip()]                          # Read and strip empty lines
    return sorted(set(items), key=_ns_sort_key)                                 # Sort records by issue and year using the helper

# _________________________________________________________________________
# Function to write records to a text file, maintaining chronological order
def _write_records(record_folder: str, record_txt: str, items: list[str]) -> None:
    """
    Writes the list of processed file records to a specified text file, ensuring no duplicates 
    and sorting by year and issue number.

    Args:
        record_folder (str): Folder where the record file should be saved.
        record_txt (str): Record filename to persist processed file names.
        items (list[str]): List of filenames to write to the record file.
    """
    os.makedirs(record_folder, exist_ok=True)                                 # Ensure that the folder exists
    items = sorted(set(items), key=_ns_sort_key)                              # Remove duplicates and sort by year/issue
    path = os.path.join(record_folder, record_txt)
    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(items) + ("\n" if items else ""))                   # Write sorted records with trailing newline

# _________________________________________________________________________
# Function to extract a table from a PDF page using Tabula
def _extract_table(pdf_path: str, page: int) -> pd.DataFrame | None:
    """
    Extracts a table from a specific page of a PDF file using the Tabula library.

    Args:
        pdf_path (str): Full path to the PDF file.
        page (int): 1-based index of the page from which to extract the table.

    Returns:
        pd.DataFrame | None: Extracted table as a DataFrame, or None if no table is found.
    """
    tables = tabula.read_pdf(pdf_path, pages=page, multiple_tables=False, stream=True)  # Extract table from PDF
    if tables is None:
        return None
    if isinstance(tables, list) and len(tables) == 0:
        return None
    return tables[0] if isinstance(tables, list) else tables                            # Return the first table found

# _________________________________________________________________________
# Function to compute the SHA-256 checksum of a file
def _compute_sha256(file_path: str, chunk: int = 1 << 20) -> str:
    """
    Compute the SHA-256 hash of a file in chunks, used for file integrity verification.

    Args:
        file_path (str): Path to the file to compute the hash.
        chunk (int): Size of each read chunk (default: 1MB).

    Returns:
        str: Hexadecimal SHA-256 hash digest of the file.
    """
    h = hashlib.sha256()                                # Initialize SHA-256 hash object
    with open(file_path, "rb") as fh:                   # Open the file in binary mode
        while True:
            b = fh.read(chunk)                          # Read a chunk of the file
            if not b:
                break
            h.update(b)                                 # Update the hash with the chunk
    return h.hexdigest()                                # Return the hexadecimal digest

# _________________________________________________________________________
# Function to save a DataFrame to either Parquet or CSV format
def _save_df(df: pd.DataFrame, out_path: str) -> tuple[str, int, int]:
    """
    Save a DataFrame to Parquet format (preferred), falling back to CSV if necessary.

    Args:
        df (pd.DataFrame): The DataFrame to save.
        out_path (str): Suggested file path for saving the DataFrame.

    Returns:
        tuple[str, int, int]: (output path, number of rows, number of columns).
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)               # Ensure the output directory exists
    try:
        if not out_path.endswith(".parquet"):                           # Force output to Parquet if extension is missing
            out_path = os.path.splitext(out_path)[0] + ".parquet"
        df.to_parquet(out_path, index=False)                            # Save as Parquet (requires pyarrow/fastparquet)
    except Exception:
        out_path = os.path.splitext(out_path)[0] + ".csv"               # Fallback to CSV if Parquet fails
        df.to_csv(out_path, index=False)                                # Save as CSV
    return out_path, int(df.shape[0]), int(df.shape[1])                 # Return path and shape (rows, columns)


# °°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°
# 3.2.1 Class for Table 1 and Table 2 pipeline cleaning
# °°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°
# In this section we define a class called `tables_cleaner` that encapsulates the pipeline for 
# cleaning data extracted from WR tables. The class exposes two main functions:
# - `clean_table_1(df)`: For cleaning data from Table 1 (monthly growth).
# - `clean_table_2(df)`: For cleaning data from Table 2 (quarterly/annual growth).
# These pipelines ensure that the raw data is properly cleaned, normalized, and formatted.

class tables_cleaner:
    """
    Pipelines for WR tables cleaning.

    Exposes:
        - clean_table_1(df): Monthly (table 1) pipeline.
        - clean_table_2(df): Quarterly/annual (table 2) pipeline.

    Note:
        The helper functions referenced below (drop_nan_rows, split_column_by_pattern, …)
        must exist in this module (Section 3 cleaning helpers).
    """

    # _____________________________________________________________________
    # Function to clean and process Table 1 (monthly data)
    def clean_table_1(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean a raw DataFrame extracted from WR Table 1 (monthly growth).

        Args:
            df (pd.DataFrame): Raw table 1 dataframe.

        Returns:
            pd.DataFrame: Cleaned table 1 dataframe.
        """
        d = df.copy()  # Work on a copy to avoid modifying the original DataFrame

        # Branch A — at least one column already looks like 'YYYY'
        if any(isinstance(c, str) and c.isdigit() and len(c) == 4 for c in d.columns):
            d = swap_nan_se(d)                      # 1. Fix misplaced 'SECTORES ECONÓMICOS'
            d = split_column_by_pattern(d)          # 2. Split 'Word. Word' headers into separate columns
            d = drop_rare_caracter_row(d)           # 3. Remove rows containing rare character '}'
            d = drop_nan_rows(d)                    # 4. Drop rows where all values are NaN
            d = drop_nan_columns(d)                 # 5. Drop columns where all values are NaN
            d = relocate_last_columns(d)            # 6. Move trailing text (e.g., sector names) if needed
            d = replace_first_dot(d)                # 7. Replace the first dot with a hyphen in the second row
            d = swap_first_second_row(d)            # 8. Swap the first and second rows in the first and last columns
            d = drop_nan_rows(d)                    # 9. Clean residual empty rows
            d = reset_index(d)                      # 10. Reset DataFrame index after cleaning
            d = remove_digit_slash(d)               # 11. Remove digits followed by a slash in the edge columns
            d = replace_var_perc_first_column(d)    # 12. Normalize 'Var. %' in the first column
            d = replace_var_perc_last_columns(d)    # 13. Normalize 'Var. %' in the last columns
            d = replace_number_moving_average(d)    # 14. Normalize moving-average label
            d = separate_text_digits(d)             # 15. Separate text and digits in the penultimate column
            d = exchange_values(d)                  # 16. Swap the last two columns when needed
            d = relocate_last_column(d)             # 17. Move the last column to position 2
            d = clean_first_row(d)                  # 18. Normalize text in the header row (e.g., lowercase)
            d = find_year_column(d)                 # 19. Align the 'year' column with the numeric-year column
            years = extract_years(d)                # 20. Collect year columns for use in future steps
            d = get_months_sublist_list(d, years)   # 21. Create month headers for each year
            d = first_row_columns(d)                # 22. Promote the first row to column headers
            d = clean_columns_values(d)             # 23. Normalize column names and values (e.g., remove tildes)
            d = convert_float(d)                    # 24. Convert columns to numeric, handling errors gracefully
            d = replace_set_sep(d)                  # 25. Replace 'set' with 'sep' in column names
            d = spaces_se_es(d)                     # 26. Trim spaces in the 'sectores_economicos' and 'economic_sectors' columns
            d = replace_services(d)                 # 27. Harmonize 'services' labels
            d = replace_mineria(d)                  # 28. Harmonize 'mineria' labels (ES)
            d = replace_mining(d)                   # 29. Harmonize 'mining' labels (EN)
            d = rounding_values(d, decimals=1)      # 30. Round float values to 1 decimal place
            return d                                # Return the cleaned DataFrame

        # Branch B — no 'YYYY' columns yet
        d = check_first_row(d)                      # 1. Split 'YYYY YYYY' patterns in header and assign columns
        d = check_first_row_1(d)                    # 2. Fill missing first-row years from edge columns
        d = replace_first_row_with_columns(d)       # 3. Replace NaNs in the first row with placeholder names
        d = swap_nan_se(d)                          # 4. Fix misplaced 'SECTORES ECONÓMICOS'
        d = split_column_by_pattern(d)              # 5. Split 'Word. Word' headers into separate columns
        d = drop_rare_caracter_row(d)               # 6. Remove rows containing rare character '}'
        d = drop_nan_rows(d)                        # 7. Drop rows where all values are NaN
        d = drop_nan_columns(d)                     # 8. Drop columns where all values are NaN
        d = relocate_last_columns(d)                # 9. Move trailing text (e.g., sector names) if needed
        d = swap_first_second_row(d)                # 10. Swap the first and second rows in the first and last columns
        d = drop_nan_rows(d)                        # 11. Clean residual empty rows
        d = reset_index(d)                          # 12. Reset DataFrame index after cleaning
        d = remove_digit_slash(d)                   # 13. Remove digits followed by a slash in the edge columns
        d = replace_var_perc_first_column(d)        # 14. Normalize 'Var. %' in the first column
        d = replace_var_perc_last_columns(d)        # 15. Normalize 'Var. %' in the last columns
        d = replace_number_moving_average(d)        # 16. Normalize moving-average label
        d = expand_column(d)                        # 17. Expand hyphenated text in the penultimate column
        d = split_values_1(d)                       # 18. Split expanded column (variant 1)
        d = split_values_2(d)                       # 19. Split expanded column (variant 2)
        d = split_values_3(d)                       # 20. Split expanded column (variant 3)
        d = separate_text_digits(d)                 # 21. Separate text and digits in the penultimate column
        d = exchange_values(d)                      # 22. Swap the last two columns when needed
        d = relocate_last_column(d)                 # 23. Move the last column to position 2
        d = clean_first_row(d)                      # 24. Normalize text in the header row (e.g., lowercase)
        d = find_year_column(d)                     # 25. Align the 'year' column with the numeric-year column
        years = extract_years(d)                    # 26. Collect year columns for use in future steps
        d = get_months_sublist_list(d, years)       # 27. Create month headers for each year
        d = first_row_columns(d)                    # 28. Promote the first row to column headers
        d = clean_columns_values(d)                 # 29. Normalize column names and values (e.g., remove tildes)
        d = convert_float(d)                        # 30. Convert columns to numeric, handling errors gracefully
        d = replace_nan_with_previous_column_1(d)   # 31. Fill NaNs from neighboring columns (v1)
        d = replace_nan_with_previous_column_2(d)   # 32. Fill NaNs from neighboring columns (v2)
        d = replace_nan_with_previous_column_3(d)   # 33. Fill NaNs from neighboring columns (v3)
        d = replace_set_sep(d)                      # 34. Replace 'set' with 'sep' in column names
        d = spaces_se_es(d)                         # 35. Trim spaces in the 'sectores_economicos' and 'economic_sectors' columns
        d = replace_services(d)                     # 36. Harmonize 'services' labels
        d = replace_mineria(d)                      # 37. Harmonize 'mineria' labels (ES)
        d = replace_mining(d)                       # 38. Harmonize 'mining' labels (EN)
        d = rounding_values(d, decimals=1)          # 39. Round float values to 1 decimal place
        return d                                    # Return the cleaned DataFrame

    # _____________________________________________________________________
    # Function to clean and process Table 2 (quarterly/annual data)
    def clean_table_2(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean a raw DataFrame extracted from WR Table 2 (quarterly/annual).

        Args:
            df (pd.DataFrame): Raw table 2 dataframe.

        Returns:
            pd.DataFrame: Cleaned table 2 dataframe.
        """
        d = df.copy()  # Work on a copy to avoid modifying the original DataFrame

        # Branch A — header starts with NaN (specific layout)
        if pd.isna(d.iloc[0, 0]):
            d = drop_nan_columns(d)                     # 1. Drop fully-NaN columns
            d = separate_years(d)                       # 2. Split 'YYYY YYYY' cell into two
            d = relocate_roman_numerals(d)              # 3. Move Roman numerals to a new column
            d = extract_mixed_values(d)                 # 4. Extract mixed numeric/text values from the column
            d = replace_first_row_nan(d)                # 5. Fill NaNs in the first row with column names
            d = first_row_columns(d)                    # 6. Promote the first row to header
            d = swap_first_second_row(d)                # 7. Swap the first and second rows at the edges
            d = reset_index(d)                          # 8. Reset DataFrame index
            d = drop_nan_row(d)                         # 9. Drop first row if fully NaN
            years = extract_years(d)                    # 10. Collect year columns
            d = split_values(d)                         # 11. Split the target mixed column into separate columns
            d = separate_text_digits(d)                 # 12. Separate text and digits in the penultimate column
            d = roman_arabic(d)                         # 13. Convert Roman numerals to Arabic numerals
            d = fix_duplicates(d)                       # 14. Fix duplicate numeric headers
            d = relocate_last_column(d)                 # 15. Move the last column to position 2
            d = clean_first_row(d)                      # 16. Normalize header row text
            d = get_quarters_sublist_list(d, years)     # 17. Build quarter headers per year
            d = first_row_columns(d)                    # 18. Promote the first row to headers again
            d = clean_columns_values(d)                 # 19. Normalize columns and values
            d = reset_index(d)                          # 20. Reset DataFrame index
            d = convert_float(d)                        # 21. Coerce numeric columns
            d = replace_set_sep(d)                      # 22. Replace 'set' with 'sep' in column names
            d = spaces_se_es(d)                         # 23. Trim spaces in the ES/EN sector columns
            d = replace_services(d)                     # 24. Harmonize 'services' labels
            d = replace_mineria(d)                      # 25. Harmonize 'mineria' labels (ES)
            d = replace_mining(d)                       # 26. Harmonize 'mining' labels (EN)
            d = rounding_values(d, decimals=1)          # 27. Round float values to 1 decimal
            return d                                    # Return the cleaned DataFrame

        # Branch B — standard layout
        d = exchange_roman_nan(d)                       # 1. Swap Roman numerals vs NaN when needed
        d = exchange_columns(d)                         # 2. Swap year columns with non-year columns if misaligned
        d = drop_nan_columns(d)                         # 3. Drop fully-NaN columns
        d = remove_digit_slash(d)                       # 4. Clean 'digits/' from edge columns
        d = last_column_es(d)                           # 5. Fix 'ECONOMIC SECTORS' placement in the last column
        d = swap_first_second_row(d)                    # 6. Swap first and second rows in the first and last columns
        d = drop_nan_rows(d)                            # 7. Drop rows where all values are NaN
        d = reset_index(d)                              # 8. Reset DataFrame index
        years = extract_years(d)                        # 9. Collect year columns
        d = separate_text_digits(d)                     # 10. Separate text and digits in the penultimate column
        d = roman_arabic(d)                             # 11. Convert Roman numerals to Arabic numerals
        d = fix_duplicates(d)                           # 12. Fix duplicate numeric headers in the first row
        d = relocate_last_column(d)                     # 13. Move the last column to position 2
        d = clean_first_row(d)                          # 14. Normalize header row text
        d = get_quarters_sublist_list(d, years)         # 15. Build quarter headers per year
        d = first_row_columns(d)                        # 16. Promote the first row to column headers
        d = clean_columns_values(d)                     # 17. Normalize columns and values (e.g., remove tildes)
        d = reset_index(d)                              # 18. Reset DataFrame index
        d = convert_float(d)                            # 19. Coerce numeric columns
        d = replace_set_sep(d)                          # 20. Replace 'set' with 'sep' in column names
        d = spaces_se_es(d)                             # 21. Trim spaces in ES/EN sector columns
        d = replace_services(d)                         # 22. Harmonize 'services' labels
        d = replace_mineria(d)                          # 23. Harmonize 'mineria' labels (ES)
        d = replace_mining(d)                           # 24. Harmonize 'mining' labels (EN)
        d = rounding_values(d, decimals=1)              # 25. Round float values to 1 decimal
        return d  # Return the cleaned DataFrame


# °°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°
# 3.2.2 Class to reshape cleaned tables into “vintages”
# °°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°
# In this section we define the `vintages_preparator` class which reshapes the cleaned tables into a tidy 'vintage' format
# that is ready for concatenation across years and frequencies. The helper functions allow the cleaning pipeline to:
# - Infer the month order within a year based on the WR issue number.
# - Reshape the data from cleaned WR tables into tidy 'vintage' DataFrames that can be stored in the desired format.
#
# This class uses helper functions like `build_month_order_map`, `prepare_table_1`, and `prepare_table_2` to process the 
# tables and generate the final reshaped data.

class vintages_preparator:
    """
    Helpers to:
      - Infer month order within a year from WR issue number (ns-dd-yyyy.pdf → dd → month 1..12).
      - Reshape cleaned tables into tidy 'vintages' ready for concatenation across years/frequencies.
    """
    
    # _____________________________________________________________________
    # Function to build month order mapping from WR filenames
    def build_month_order_map(self, year_folder: str) -> dict[str, int]:
        """
        Create {filename: month_order} mapping for files under a given year folder.

        Args:
            year_folder (str): Folder path containing the year's PDFs.

        Returns:
            dict[str, int]: filename → month_order (1..12) inferred from ns-dd-yyyy.pdf (dd).
        """
        files = [f for f in os.listdir(year_folder) if f.endswith(".pdf")]  # Get list of PDF files in the year folder
        pairs = []                                                           # Initialize list for filename-month pairs
        for f in files:
            m = re.search(r"ns-(\d{2})-\d{4}\.pdf$", f, re.IGNORECASE)        # Match filenames like 'ns-07-2017.pdf'
            if m:
                pairs.append((f, int(m.group(1))))                             # Extract day (dd) as month order
        sorted_files = sorted(pairs, key=lambda x: x[1])                        # Sort filenames by the extracted month
        return {fname: i + 1 for i, (fname, _) in enumerate(sorted_files)}      # Create month order mapping (1..12)

    # _____________________________________________________________________
    # Function to prepare Table 1 data into vintage format
    def prepare_table_1(self, df: pd.DataFrame, filename: str, month_order_map: dict[str, int]) -> pd.DataFrame:
        """
        Prepare a cleaned Table 1 (monthly) into tidy vintage format.

        Args:
            df (pd.DataFrame): Cleaned Table 1 dataframe (already has 'year' and 'wr').
            filename (str): Original PDF filename (ns-xx-yyyy.pdf) to pick its month order.
            month_order_map (dict[str,int]): filename → month order (1..12).

        Returns:
            pd.DataFrame: Tidy vintage dataframe with:
                - index (row id removed)
                - 'target_period' like '2019m7'
                - one column per vintage_id (economic_sector_year_month)
        """
        d = df.copy()  # Work on a copy of the dataframe to avoid modifying the original

        # 1) Determine month from filename
        d["month"] = month_order_map.get(filename)  # Map filename to month order using the month_order_map

        # 2) Drop unused columns (retain only 'economic_sector')
        d = d.drop(columns=["wr", "sectores_economicos"], errors="ignore")  # Drop 'wr' and 'sectores_economicos' columns

        # 3) Map economic sector names to shorter labels
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
        d["economic_sector"] = d["economic_sectors"].map(sector_map)  # Map full sector names to short labels
        d = d[d["economic_sector"].notna()].copy()  # Keep only rows with valid economic sectors

        # 4) Build vintage_id (sector_year_month)
        d["vintage_id"] = d["economic_sector"] + "_" + d["year"].astype(str) + "_" + d["month"].astype(str)

        # 5) Retain only the 'yyyy_mmm' columns based on month names (e.g., 2020_ene, 2020_feb)
        pat = re.compile(r"^\d{4}_(ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)$", re.IGNORECASE)
        keep = ["vintage_id"] + [c for c in d.columns if pat.match(str(c))]  # Select only relevant columns
        d = d[keep]  # Keep the selected columns

        # 6) Reshape the dataframe to a tidy format (rows: target_period, columns: vintage_id)
        t = d.set_index("vintage_id").T.reset_index().rename(columns={"index": "target_period"})

        # 7) Convert 'YYYY_mmm' → 'yyyymX' (e.g., '2020_ene' → '2020m1')
        month_map = {"ene":"01","feb":"02","mar":"03","abr":"04","may":"05","jun":"06",
                     "jul":"07","ago":"08","sep":"09","oct":"10","nov":"11","dic":"12"}
        def _to_yyyymx(s: str) -> str:
            m = re.match(r"^(\d{4})_(\w{3})$", s, re.IGNORECASE)  # Match the 'YYYY_mmm' format
            if not m:
                return s
            y = m.group(1)  # Extract year
            mmm = m.group(2).lower()  # Extract month abbreviation
            mm = month_map.get(mmm, "01")  # Convert month abbreviation to month number
            return f"{y}m{int(mm)}"  # Return 'yyyymX' format (no leading zero in month)

        t["target_period"] = t["target_period"].astype(str).map(_to_yyyymx)  # Apply the conversion
        return t  # Return the reshaped (tidy) dataframe

    # _____________________________________________________________________
    # Function to prepare Table 2 data into vintage format
    def prepare_table_2(self, df: pd.DataFrame, filename: str, month_order_map: dict[str, int]) -> pd.DataFrame:
        """
        Prepare a cleaned Table 2 (quarterly/annual) into tidy vintage format.

        Args:
            df (pd.DataFrame): Cleaned Table 2 dataframe (already has 'year' and 'wr').
            filename (str): Original PDF filename (ns-xx-yyyy.pdf).
            month_order_map (dict[str,int]): filename → month order (not used but kept for symmetry).

        Returns:
            pd.DataFrame: Tidy vintage dataframe with 'target_period' as 'yyyyqN' or 'yyyy'.
        """
        d = df.copy()  # Work on a copy to avoid modifying the original DataFrame

        # 1) Drop unused columns (retain only relevant columns)
        d = d.drop(columns=["wr", "sectores_economicos"], errors="ignore")  # Drop 'wr' and 'sectores_economicos'

        # 2) Map economic sector names to shorter labels
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
        d["economic_sector"] = d["economic_sectors"].map(sector_map)
        d = d[d["economic_sector"].notna()].copy()  # Keep only valid rows

        # 3) Build vintage_id (sector_year_monthorder) — keeps the same structure as table_1 for later concatenation
        d["month"] = month_order_map.get(filename)  # Get month from the filename using the month_order_map
        d["vintage_id"] = d["economic_sector"] + "_" + d["year"].astype(str) + "_" + d["month"].astype(str)

        # 4) Retain only the relevant columns (e.g., year and quarterly/year columns)
        pat = re.compile(r"^\d{4}_(1|2|3|4|year)$", re.IGNORECASE)  # Match year and quarter columns
        keep = ["vintage_id"] + [c for c in d.columns if pat.match(str(c))]  # Keep only necessary columns
        d = d[keep]  # Retain selected columns

        # 5) Reshape the DataFrame to a tidy format (rows: target_period, columns: vintage_id)
        t = d.set_index("vintage_id").T.reset_index().rename(columns={"index": "target_period"})

        # 6) Convert 'YYYY_1..4' → 'yyyyqN' (e.g., '2020_1' → '2020q1')
        t["target_period"] = (
            t["target_period"].astype(str)
            .str.replace(r"^(\d{4})_(\d)$", r"\1q\2", regex=True)  # Convert to 'yyyyqN'
            .str.replace(r"^(\d{4})_year$", r"\1", regex=True)  # Convert 'yyyy_year' to 'yyyy'
        )
        return t  # Return the reshaped (tidy) vintage dataframe


# °°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°
# 3.2.3 Runners: single-call functions per table (raw + clean dicts, records, bars, summary)
# °°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°°
# In this section, we define two functions to clean and process all WR PDFs from a folder:
# 1) `table_1_cleaner`: Extracts page 1 from each WR PDF, applies the Table 1 pipeline, updates the record, and optionally 
#    persists cleaned tables (Parquet/CSV) while showing a concise summary.
# 2) `table_2_cleaner`: Similar to `table_1_cleaner` but for Table 2 (quarterly/annual).

# _________________________________________________________________________
# Function to clean and process Table 1 from all WR (PDF files) in a folder
def table_1_cleaner(
    input_pdf_folder: str,
    record_folder: str,
    record_txt: str,
    persist: bool = False,
    persist_folder: str | None = None,
    pipeline_version: str = "s3.0.0",
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    """
    Extract page 1 from each WR PDF, run the table 1 pipeline, update the record,
    optionally persist cleaned tables (Parquet/CSV), and show a concise summary.
    """
    start_time = time.time()                                 # Record the start time
    print("\n🧹 Starting Table 1 cleaning...\n")               # Print section start message

    cleaner   = tables_cleaner()                             # Initialize the cleaner for Table 1
    records   = _read_records(record_folder, record_txt)     # Read existing record of processed files
    processed = set(records)                                 # Convert record list to set for faster lookup

    raw_tables_dict_1: dict[str, pd.DataFrame]   = {}        # Store raw tables extracted from PDFs
    new_dataframes_dict_1: dict[str, pd.DataFrame] = {}      # Store cleaned dataframes

    new_counter = 0                                          # Counter for newly cleaned tables
    skipped_counter = 0                                      # Counter for skipped tables
    skipped_years: dict[str, int] = {}                       # Track skipped years and counts

    # List all year folders except '_quarantine'
    years = [d for d in sorted(os.listdir(input_pdf_folder))
             if os.path.isdir(os.path.join(input_pdf_folder, d)) and d != "_quarantine"]
    total_year_folders = len(years)                          # Total number of year folders found
    
    prep = vintages_preparator()                             # Initialize vintages helper
    vintages_dict_1: dict[str, pd.DataFrame] = {}            # Store tidy (prepared) vintage data

    # Prepare output folders if persistence is enabled
    if persist:
        base_out = persist_folder or os.path.join("data", "clean")
        out_root = os.path.join(base_out, "table_1")
        os.makedirs(out_root, exist_ok=True)                 # Ensure the output directory exists

    # Iterate through year folders
    for year in years:
        folder_path = os.path.join(input_pdf_folder, year)   # Full path to current year folder
        pdf_files = sorted([f for f in os.listdir(folder_path) if f.endswith(".pdf")],
                           key=_ns_sort_key)                 # Sort PDF files by NS code
        
        month_order_map = prep.build_month_order_map(folder_path)   # Map filename → month (1..12)

        if not pdf_files:
            continue                                         # Skip if no PDFs found

        # Skip if all PDFs already processed
        already = [f for f in pdf_files if f in processed]
        if len(already) == len(pdf_files):
            skipped_years[year] = len(already)
            skipped_counter += len(already)
            continue

        print(f"\n📂 Processing Table 1 in {year}\n")        # Indicate the year being processed
        folder_new_count = 0                                 # Count new tables for this year
        folder_skipped_count = 0                             # Count skipped tables for this year

        # Progress bar for PDFs in the current year
        pbar = tqdm(pdf_files, desc=f"🚧 {year}", unit="PDF",
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}", colour="#E6004C",
                    leave=False, position=0, dynamic_ncols=True)

        for filename in pbar:
            if filename in processed:
                folder_skipped_count += 1                    # Skip if already processed
                continue

            issue, yr = parse_ns_meta(filename)              # Extract WR issue and year from filename
            if not issue:
                folder_skipped_count += 1
                continue

            pdf_path = os.path.join(folder_path, filename)   # Build full PDF path
            try:
                raw = _extract_table(pdf_path, page=1)       # Extract table 1 from page 1
                if raw is None:
                    folder_skipped_count += 1
                    continue

                key = f"{os.path.splitext(filename)[0].replace('-', '_')}_1"
                raw_tables_dict_1[key] = raw.copy()          # Store raw table

                clean = cleaner.clean_table_1(raw)            # Clean the raw table
                clean.insert(0, "year", yr)                  # Insert 'year' column at the start
                clean.insert(1, "wr", issue)                 # Insert 'wr' column (weekly report code)
                clean.attrs["pipeline_version"] = pipeline_version

                # Keep a copy of the cleaned table in-memory for inspection
                new_dataframes_dict_1[key] = clean.copy()

                # Prepare and persist the vintage (final output)
                vintage = prep.prepare_table_1(clean, filename, month_order_map)
                vintage.attrs["pipeline_version"] = pipeline_version
                vintages_dict_1[key] = vintage               # Store vintage data in-memory (optional)
                
                if persist:                                  # Persist only the vintage (not raw data)
                    ns_code  = os.path.splitext(filename)[0] # e.g., ns-07-2017
                    out_dir  = os.path.join(out_root, str(yr))
                    out_path = os.path.join(out_dir, f"{ns_code}.parquet")
                    _save_df(vintage, out_path)              # Save vintage (Parquet/CSV)

                processed.add(filename)                      # Mark the file as processed
                folder_new_count += 1
            except Exception as e:
                print(f"⚠️  {filename}: {e}")                # Handle and print any extraction/cleaning errors
                folder_skipped_count += 1

        pbar.clear(); pbar.close()                           # Clear progress bar after processing

        # Display the completion bar for the current year
        fb = tqdm(total=len(pdf_files), desc=f"✔️ {year}", unit="PDF",
                  bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}", colour="#3366FF",
                  leave=True, position=0, dynamic_ncols=True)
        fb.update(len(pdf_files))  # Update completion status
        fb.close()

        new_counter += folder_new_count                      # Update the total count of new cleaned tables
        skipped_counter += folder_skipped_count              # Update the total skipped table count
        _write_records(record_folder, record_txt, list(processed))  # Update the processed records list

    # Summary of skipped years
    if skipped_years:
        years_summary = ", ".join(skipped_years.keys())
        total_skipped = sum(skipped_years.values())
        print(f"\n⏩ {total_skipped} cleaned tables already generated for years: {years_summary}")

    elapsed_time = round(time.time() - start_time)            # Calculate total elapsed time
    print(f"\n📊 Summary:\n")
    print(f"📂 {total_year_folders} folders (years) found containing input PDFs")
    print(f"🗃️ Already cleaned tables: {skipped_counter}")
    print(f"✨ Newly cleaned tables: {new_counter}")
    print(f"⏱️ {elapsed_time} seconds")

    return raw_tables_dict_1, new_dataframes_dict_1, vintages_dict_1           # Return raw, cleaned, and vintages

# _________________________________________________________________________
# Function to clean and process Table 2 from all WR PDF files in a folder
def table_2_cleaner(
    input_pdf_folder: str,
    record_folder: str,
    record_txt: str,
    persist: bool = False,
    persist_folder: str | None = None,
    pipeline_version: str = "s3.0.0",
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    """
    Extract page 2 from each WR PDF, run the table 2 pipeline, update the record,
    optionally persist cleaned tables (Parquet/CSV), and show a concise summary.
    """
    start_time = time.time()                                # Record script start time
    print("\n🧹 Starting Table 2 cleaning...\n")             # Print section start message

    cleaner = tables_cleaner()                              # Initialize table cleaner object
    records = _read_records(record_folder, record_txt)       # Load processed record file
    processed = set(records)                                 # Convert to set for fast lookup

    raw_tables_dict_2: dict[str, pd.DataFrame] = {}          # Store extracted raw tables
    new_dataframes_dict_2: dict[str, pd.DataFrame] = {}      # Store cleaned tables

    new_counter = 0                                          # Count new cleaned files
    skipped_counter = 0                                      # Count skipped files
    skipped_years: dict[str, int] = {}                       # Track skipped counts per year

    # List year directories except '_quarantine'
    years = [d for d in sorted(os.listdir(input_pdf_folder))
             if os.path.isdir(os.path.join(input_pdf_folder, d)) and d != "_quarantine"]
    total_year_folders = len(years)                          # Total number of valid year folders
    
    prep = vintages_preparator()                             # Initialize vintages helper
    vintages_dict_2: dict[str, pd.DataFrame] = {}            # Store tidy (prepared) vintage data

    # Create persistence folder if enabled
    if persist:
        base_out = persist_folder or os.path.join("data", "clean")
        out_root = os.path.join(base_out, "table_2")
        os.makedirs(out_root, exist_ok=True)

    # Iterate through each year's folder
    for year in years:
        folder_path = os.path.join(input_pdf_folder, year)   # Full path to current year folder
        pdf_files = sorted([f for f in os.listdir(folder_path) if f.endswith(".pdf")],
                           key=_ns_sort_key)                 # Sort PDFs by NS code order
        month_order_map = prep.build_month_order_map(folder_path)   # Map filename → month (1..12)

        if not pdf_files:
            continue                                         # Skip if no PDFs present

        # Skip if all PDFs already processed
        already = [f for f in pdf_files if f in processed]
        if len(already) == len(pdf_files):
            skipped_years[year] = len(already)
            skipped_counter += len(already)
            continue

        print(f"\n📂 Processing Table 2 in {year}\n")        # Indicate the year being processed
        folder_new_count = 0                                 # Count new tables for this year
        folder_skipped_count = 0                             # Count skipped tables for this year

        # Display progress bar for current year
        pbar = tqdm(pdf_files, desc=f"🚧 {year}", unit="PDF",
                    bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}", colour="#E6004C",
                    leave=False, position=0, dynamic_ncols=True)

        for filename in pbar:
            if filename in processed:
                folder_skipped_count += 1                    # Skip if already processed
                continue

            issue, yr = parse_ns_meta(filename)              # Extract WR issue and year from filename
            if not issue:
                folder_skipped_count += 1
                continue

            pdf_path = os.path.join(folder_path, filename)   # Build full PDF path
            try:
                raw = _extract_table(pdf_path, page=2)       # Extract table 2 from page 2
                if raw is None:
                    folder_skipped_count += 1
                    continue

                key = f"{os.path.splitext(filename)[0].replace('-', '_')}_2"
                raw_tables_dict_2[key] = raw.copy()          # Store raw table with unique key

                clean = cleaner.clean_table_2(raw)            # Clean the extracted table
                clean.insert(0, "year", yr)                  # Add 'year' column first
                clean.insert(1, "wr", issue)                  
                clean.attrs["pipeline_version"] = pipeline_version

                # ▶ keep a copy in-memory so you can inspect `clean_2`
                new_dataframes_dict_2[key] = clean.copy()

                # build + persist the vintage (what we save/record)
                vintage = prep.prepare_table_2(clean, filename, month_order_map)
                vintage.attrs["pipeline_version"] = pipeline_version
                vintages_dict_2[key] = vintage               # Keep vintage in-memory (optional)

                if persist:                                  # Persist **vintage** only
                    ns_code  = os.path.splitext(filename)[0] # e.g., ns-07-2017
                    out_dir  = os.path.join(out_root, str(yr))
                    out_path = os.path.join(out_dir, f"{ns_code}.parquet")
                    _save_df(vintage, out_path)              # Save vintage (Parquet/CSV)

                processed.add(filename)                      # Record processed **by vintage**
                folder_new_count += 1
            except Exception as e:
                print(f"⚠️  {filename}: {e}")                # Display any extraction/cleaning error
                folder_skipped_count += 1

        pbar.clear(); pbar.close()                           # Close progress bar after processing

        # Display the completion bar for the current year
        fb = tqdm(total=len(pdf_files), desc=f"✔️ {year}", unit="PDF",
                  bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}", colour="#3366FF",
                  leave=True, position=0, dynamic_ncols=True)
        fb.update(len(pdf_files))  # Update completion status
        fb.close()

        new_counter += folder_new_count                      # Update the total count of new cleaned tables
        skipped_counter += folder_skipped_count              # Update the total skipped table count
        _write_records(record_folder, record_txt, list(processed))  # Update processed records list

    # Summary of skipped years
    if skipped_years:
        years_summary = ", ".join(skipped_years.keys())
        total_skipped = sum(skipped_years.values())
        print(f"\n⏩ {total_skipped} cleaned tables already generated for years: {years_summary}")

    elapsed_time = round(time.time() - start_time)            # Calculate total elapsed time
    print(f"\n📊 Summary:\n")
    print(f"📂 {total_year_folders} folders (years) found containing input PDFs")
    print(f"🗃️ Already cleaned tables: {skipped_counter}")
    print(f"✨ Newly cleaned tables: {new_counter}")
    print(f"⏱️ {elapsed_time} seconds")

    return raw_tables_dict_2, new_dataframes_dict_2, vintages_dict_2          # Return raw, cleaned, and vintages



# ##############################################################################################
# SECTION 4 Concatenate vintages across years by frequency
# ##############################################################################################



