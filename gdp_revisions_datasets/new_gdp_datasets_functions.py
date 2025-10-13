
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
#       1. PDF Downloader .....................................................................
#       2. Generate PDF input with key tables .................................................
#       3. 
#       4. 
# 
# *********************************************************************************************



# ##############################################################################################
# 1 PDF Downloader
# ##############################################################################################

# In this section we build an automated downloader for BCRP's Weekly Reports (WR) using
# Selenium-based web scraping to mimic a human browser session and avoid duplicate downloads.


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Libraries
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

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


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Module-level setting-up
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

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


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Functions
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

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
_last_alert = None                                                          # Remember the last chosen filename across calls

def load_alert_track(alert_track_folder: str) -> str | None:
    """
    Load a random .mp3 file from `alert_track_folder` for audio alerts, avoiding
    immediate repetition of the previous selection when possible.

    Args:
        alert_track_folder (str): Directory expected to contain one or more .mp3 files.

    Returns:
        str | None: Absolute path to the selected .mp3 file, or None if no .mp3 is found.
    """
    global _last_alert                                                      # Access the module-level last-choice guard
    os.makedirs(alert_track_folder, exist_ok=True)                          # Ensure folder exists (no error if present)

    tracks = [f for f in os.listdir(alert_track_folder)                     # Collect only .mp3 filenames (case-insensitive)
              if f.lower().endswith(".mp3")]
    if not tracks:
        print("🔇 No .mp3 files found in 'alert_track/'. Continuing without audio alerts.")
        return None

    choices = [t for t in tracks if t != _last_alert] or tracks             # Prefer any file ≠ last; fallback to all if single
    track   = random.choice(choices)                                        # Uniform random selection among candidates
    _last_alert = track                                                     # Update guard to prevent immediate repetition

    alert_track_path = os.path.join(alert_track_folder, track)              # Build absolute path to the chosen file
    pygame.mixer.music.load(alert_track_path)                               # Preload into pygame mixer for instant playback
    return alert_track_path


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
    alert_track_path = load_alert_track(alert_track_folder)                 # Load a random .mp3 if available

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
                records = [ln.strip() for ln in f if ln.strip()]            # Compact to non-empty, trimmed lines

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
def replace_ns_pdfs(items, root_folder, record_folder, download_record_txt, quarantine=None):
    """
    Replace defective WR PDFs (BCRP Nota Semanal, 'ns-XX-YYYY.pdf') stored under year subfolders.
    Keeps the download record consistent so the downloader will not re-fetch defective files.

    Args:
        items (list[tuple[str, str, str]]): Triples of (year, defective_pdf, replacement_code).
            Example: [("2017","ns-08-2017.pdf","ns-07-2017"), ("2019","ns-23-2019.pdf","ns-22-2019")]
        root_folder (str): Base path containing year folders (e.g., raw_pdf).
        record_folder (str): Folder holding the download record TXT.
        download_record_txt (str): Record filename (e.g., 'downloaded_pdfs.txt').
        quarantine (str | None): Folder to move defective PDFs; if None, delete them.
    """
    pat = re.compile(r"^ns-(\d{1,2})-(\d{4})(?:\.pdf)?$", re.I)             # Match 'ns-<issue>-<year>[.pdf]' (issue can be 1–2 digits)

    def norm(c):
        m = pat.match(os.path.basename(c).lower())                          # Validate and normalize the code using the regex
        if not m:
            raise ValueError(f"Bad NS code: {c}")                           # Enforce expected 'ns-xx-yyyy' structure
        return f"ns-{int(m.group(1)):02d}-{m.group(2)}"                     # Zero-pad issue (e.g., 7 → 07) and keep year

    def url(c):
        cc = norm(c)                                                        # Normalized 'ns-xx-yyyy'
        return f"https://www.bcrp.gob.pe/docs/Publicaciones/Nota-Semanal/{cc[-4:]}/{cc}.pdf"  # Year-coded server path

    def _ns_key(name):
        base = os.path.splitext(os.path.basename(name))[0]                  # Drop directory and extension
        m = re.search(r"ns-(\d{2})-(\d{4})", base, re.I)                    # Extract issue/year for ordering
        if not m:
            return (9999, 9999, base)                                       # Unknown pattern → stable, last
        issue, year = int(m.group(1)), int(m.group(2))
        return (year, issue, base)                                          # Sort by (year, issue, base)

    def update_record(add=None, remove=None):
        # Intentionally DO NOT remove defective entries from the TXT.
        # This prevents the downloader from re-fetching them in future runs.
        p = os.path.join(record_folder, download_record_txt)                # Absolute path to record file
        s = set()
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                s = {x.strip() for x in f if x.strip()}                     # Read, trim blanks, and de-duplicate
        if add:
            s.add(add)                                                      # Add the replacement filename
        records = sorted(s, key=_ns_key)                                    # Chronological order by (year → issue)
        os.makedirs(record_folder, exist_ok=True)                           # Ensure destination exists
        with open(p, "w", encoding="utf-8") as f:
            f.write("\n".join(records) + ("\n" if records else ""))         # Write with trailing newline if non-empty

    if quarantine:
        os.makedirs(quarantine, exist_ok=True)

    ok, fail = 0, 0

    for year, bad_pdf, repl_code in items:
        year = str(year)                                                    # Normalize year to string for path joins
        ydir = os.path.join(root_folder, year)                              # Year directory (e.g., raw_pdf/2019)
        bad_path = os.path.join(ydir, bad_pdf)                              # Full path to the defective file
        new_name = f"{norm(repl_code)}.pdf"                                 # Normalized replacement filename
        new_path = os.path.join(ydir, new_name)                             # Destination path for the replacement

        if not os.path.exists(bad_path):
            fail += 1
            continue

        # Download the replacement first to ensure availability before removing the defective file
        try:
            os.makedirs(ydir, exist_ok=True)                                # Ensure year folder exists
            with requests.get(url(repl_code), stream=True, timeout=60) as r:
                r.raise_for_status()                                        # Fail fast on non-2xx
                with open(new_path, "wb") as fh:
                    for ch in r.iter_content(131072):                       # Stream in 128 KiB chunks
                        if ch:
                            fh.write(ch)
        except Exception as e:
            if os.path.exists(new_path):
                try:
                    os.remove(new_path)                                     # Remove partial/incomplete file
                except:
                    pass
            fail += 1
            continue

        # Quarantine the defective file (if configured) or delete it permanently
        try:
            if quarantine:
                shutil.move(bad_path, os.path.join(quarantine, bad_pdf))    # Preserve original artifact under quarantine
            else:
                os.remove(bad_path)                                         # Hard delete when no quarantine path is provided
        except Exception:
            fail += 1
            continue

        update_record(add=new_name, remove=bad_pdf)                         # Keep defective entry; append replacement
        ok += 1

    return ok, fail



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



# *++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*
# ##############################################################################################
# Section 3. Data cleaning
# ##############################################################################################
# *++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++*

#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# Section 3.1.  Extracting tables and data cleanup 
#+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

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
# Section 3.2 pipelines — table 1 and table 2 cleaning runners
# =============================================================================================

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# libraries (top-only imports)
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
import os
import re
import time
import hashlib
import pandas as pd
from tqdm.notebook import tqdm                      # Jupyter-native progress bars (gray background)
import tabula                                       # PDF table extractor (Java backend)

# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# constants — colors, bar style, defaults
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
PROG_COLOR_ACTIVE = "#E6004C"                       # in-progress color (magenta/red)
PROG_COLOR_DONE   = "#3366FF"                       # finished color (blue)
BAR_FORMAT        = "{l_bar}{bar}| {n_fmt}/{total_fmt}"

RECORD_SUFFIX_1    = "new_generated_dataframes_1.txt"  # record txt for table 1
RECORD_SUFFIX_2    = "new_generated_dataframes_2.txt"  # record txt for table 2


# =============================================================================================
# UTILITIES: parsing, sorting, records, extraction, persistence
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


# =============================================================================================
# PIPELINES: class wrapper for Table 1 and Table 2 cleaning
# =============================================================================================

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
    # Function: clean_table_1
    def clean_table_1(self, df: pd.DataFrame) -> pd.DataFrame:
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
    # Function: clean_table_2
    def clean_table_2(self, df: pd.DataFrame) -> pd.DataFrame:
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
# PREPARATION: class to build month order and reshape cleaned tables into “vintages”
# =============================================================================================

class vintages_preparator:
    """
    Helpers to:
      - infer month order within a year from WR issue number (ns-dd-yyyy.pdf → dd → month 1..12)
      - reshape cleaned tables into tidy 'vintages' ready for concatenation across years/frequencies
    """

    # _____________________________________________________________________
    # Function: build_month_order_map
    def build_month_order_map(self, year_folder: str) -> dict[str, int]:
        """
        Create {filename: month_order} mapping for files under a given year folder.

        Args:
            year_folder (str): Folder path containing the year's PDFs.

        Returns:
            dict[str, int]: filename → month_order (1..12) inferred from ns-dd-yyyy.pdf (dd).
        """
        files = [f for f in os.listdir(year_folder) if f.endswith(".pdf")]              # PDFs in year folder
        pairs = []
        for f in files:
            m = re.search(r"ns-(\d{2})-\d{4}\.pdf$", f, re.IGNORECASE)                  # capture dd
            if m:
                pairs.append((f, int(m.group(1))))
        sorted_files = sorted(pairs, key=lambda x: x[1])                                 # order by dd
        return {fname: i + 1 for i, (fname, _) in enumerate(sorted_files)}              # 1..12

    # _____________________________________________________________________
    # Function: prepare_table_1
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
        d = df.copy()                                                                    # work on a copy

        # 1) month from filename
        d["month"] = month_order_map.get(filename)                                       # 1..12 (int/None)

        # 2) drop unused columns (keep EN sector)
        d = d.drop(columns=["wr", "sectores_economicos"], errors="ignore")

        # 3) short sector labels
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
        d["economic_sector"] = d["economic_sectors"].map(sector_map)                     # map to short labels
        d = d[d["economic_sector"].notna()].copy()                                       # keep valid rows

        # 4) build vintage_id (sector_year_month)
        d["vintage_id"] = d["economic_sector"] + "_" + d["year"].astype(str) + "_" + d["month"].astype(str)

        # 5) keep only yyyy_mmm columns
        pat = re.compile(r"^\d{4}_(ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)$", re.IGNORECASE)
        keep = ["vintage_id"] + [c for c in d.columns if pat.match(str(c))]
        d = d[keep]

        # 6) reshape to tidy (target_period rows)
        t = d.set_index("vintage_id").T.reset_index().rename(columns={"index": "target_period"})

        # 7) convert 'YYYY_mmm' → 'yyyymX'
        month_map = {"ene":"01","feb":"02","mar":"03","abr":"04","may":"05","jun":"06",
                     "jul":"07","ago":"08","sep":"09","oct":"10","nov":"11","dic":"12"}
        def _to_yyyymx(s: str) -> str:
            m = re.match(r"^(\d{4})_(\w{3})$", s, re.IGNORECASE)
            if not m:
                return s
            y = m.group(1); mmm = m.group(2).lower()
            mm = month_map.get(mmm, "01")
            return f"{y}m{int(mm)}"                                                     # no leading zero in m

        t["target_period"] = t["target_period"].astype(str).map(_to_yyyymx)
        return t

    # _____________________________________________________________________
    # Function: prepare_table_2
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
        d = df.copy()                                                                    # work on a copy

        # 1) drop unused columns (keep EN sector)
        d = d.drop(columns=["wr", "sectores_economicos"], errors="ignore")

        # 2) short sector labels
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
        d = d[d["economic_sector"].notna()].copy()

        # 3) vintage_id (sector_year_monthorder) — keep same shape as table_1 for concatenation later
        #    month_order not strictly needed here but kept for a homogeneous ID design
        d["month"] = month_order_map.get(filename)
        d["vintage_id"] = d["economic_sector"] + "_" + d["year"].astype(str) + "_" + d["month"].astype(str)

        # 4) keep only yyyy_(1|2|3|4|year) columns
        pat = re.compile(r"^\d{4}_(1|2|3|4|year)$", re.IGNORECASE)
        keep = ["vintage_id"] + [c for c in d.columns if pat.match(str(c))]
        d = d[keep]

        # 5) reshape to tidy (target_period rows)
        t = d.set_index("vintage_id").T.reset_index().rename(columns={"index": "target_period"})

        # 6) convert 'YYYY_1..4' → 'yyyyqN' and 'YYYY_year' → 'yyyy'
        t["target_period"] = (
            t["target_period"].astype(str)
            .str.replace(r"^(\d{4})_(\d)$", r"\1q\2", regex=True)
            .str.replace(r"^(\d{4})_year$", r"\1", regex=True)
        )
        return t


# =============================================================================================
# RUNNERS: single-call functions per table (raw + clean dicts, records, bars, summary)
# =============================================================================================

# _________________________________________________________________________
# Function to clean and process Table 1 from all WR (PDF files) in a folder
def table_1_cleaner(
    input_pdf_folder: str,
    record_folder: str,
    record_txt: str = RECORD_SUFFIX_1,
    persist: bool = False,
    persist_folder: str | None = None,
    pipeline_version: str = "s3.0.0",
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    """
    Extract page 1 from each WR PDF, run the table 1 pipeline, update the record,
    optionally persist cleaned tables (Parquet/CSV), and show a concise summary.
    """
    start_time = time.time()                                 # Record the start time
    print("\n🧹 Starting Table 1 cleaning...\n")

    cleaner   = tables_cleaner()                             # Initialize the GDP WR cleaner
    records   = _read_records(record_folder, record_txt)     # Read existing record of processed files
    processed = set(records)                                 # Convert record list to a set for faster lookup

    raw_tables_dict_1: dict[str, pd.DataFrame]   = {}        # Store extracted raw tables
    new_dataframes_dict_1: dict[str, pd.DataFrame] = {}      # Store cleaned dataframes

    new_counter = 0                                          # Counter for new cleaned tables
    skipped_counter = 0                                      # Counter for skipped tables
    skipped_years: dict[str, int] = {}                       # Track skipped years and their counts

    # List all year folders except '_quarantine'
    years = [d for d in sorted(os.listdir(input_pdf_folder))
             if os.path.isdir(os.path.join(input_pdf_folder, d)) and d != "_quarantine"]
    total_year_folders = len(years)                          # Total number of year folders found
    
    prep = vintages_preparator()                             # helper for vintages
    vintages_dict_1: dict[str, pd.DataFrame] = {}            # tidy (prepared) outputs

    # Prepare output folders if persistence is enabled
    if persist:
        base_out = persist_folder or os.path.join("data", "clean")
        out_root = os.path.join(base_out, "table_1")
        os.makedirs(out_root, exist_ok=True)                 # Ensure output directory exists

    # Iterate through year folders
    for year in years:
        folder_path = os.path.join(input_pdf_folder, year)   # Full path to the year's folder
        pdf_files = sorted([f for f in os.listdir(folder_path) if f.endswith(".pdf")],
                           key=_ns_sort_key)                 # Sort PDF files by NS code
        
        month_order_map = prep.build_month_order_map(folder_path)   # <-- add: filename → month (1..12)

        if not pdf_files:
            continue                                         # Skip if no PDFs found

        # Skip if all PDFs already processed
        already = [f for f in pdf_files if f in processed]
        if len(already) == len(pdf_files):
            skipped_years[year] = len(already)
            skipped_counter += len(already)
            continue

        print(f"\n📂 Processing Table 1 in {year}\n")        # Display year being processed
        folder_new_count = 0                                 # New tables for this year
        folder_skipped_count = 0                             # Skipped tables for this year

        # Progress bar for PDFs in this year
        pbar = tqdm(pdf_files, desc=f"🚧 {year}", unit="PDF",
                    bar_format=BAR_FORMAT, colour=PROG_COLOR_ACTIVE,
                    leave=False, position=0, dynamic_ncols=True)

        for filename in pbar:
            if filename in processed:
                folder_skipped_count += 1                    # Skip already processed PDFs
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

                clean = cleaner.clean_table_1(raw)            # Apply table 1 cleaning routine
                clean.insert(0, "year", yr)                  # Insert 'year' column at start
                clean.insert(1, "wr", issue)                 # Insert 'wr' (weekly report code) column
                clean.attrs["pipeline_version"] = pipeline_version

                # ▶ keep a copy in-memory so you can inspect `clean_1`
                new_dataframes_dict_1[key] = clean.copy()

                # build + persist the vintage (what we save/record)
                vintage = prep.prepare_table_1(clean, filename, month_order_map)
                vintage.attrs["pipeline_version"] = pipeline_version
                vintages_dict_1[key] = vintage               # Keep vintage in-memory (optional)
                
                if persist:                                  # Persist **vintage** only
                    ns_code  = os.path.splitext(filename)[0] # e.g., ns-07-2017
                    out_dir  = os.path.join(out_root, str(yr))
                    out_path = os.path.join(out_dir, f"{ns_code}.parquet")
                    _save_df(vintage, out_path)              # Save vintage (Parquet/CSV)

                processed.add(filename)                      # Record processed **by vintage**
                folder_new_count += 1
            except Exception as e:
                print(f"⚠️  {filename}: {e}")                # Handle and display any errors
                folder_skipped_count += 1

        pbar.clear(); pbar.close()                           # Clear progress bar

        # Display completion bar for the year
        fb = tqdm(total=len(pdf_files), desc=f"✔️ {year}", unit="PDF",
                  bar_format=BAR_FORMAT, colour=PROG_COLOR_DONE,
                  leave=True, position=0, dynamic_ncols=True)
        fb.update(len(pdf_files))
        fb.close()

        new_counter += folder_new_count                      # Update overall new counter
        skipped_counter += folder_skipped_count              # Update overall skipped counter
        _write_records(record_folder, record_txt, list(processed))  # Update record file

    # Display summary for skipped years
    if skipped_years:
        years_summary = ", ".join(skipped_years.keys())
        total_skipped = sum(skipped_years.values())
        print(f"\n⏩ {total_skipped} cleaned tables already generated for years: {years_summary}")

    elapsed_time = round(time.time() - start_time)            # Compute total elapsed time
    print(f"\n📊 Summary:\n")
    print(f"📂 {total_year_folders} folders (years) found containing input PDFs")
    print(f"🗃️ Already cleaned tables: {skipped_counter}")
    print(f"✨ Newly cleaned tables: {new_counter}")
    print(f"⏱️ {elapsed_time} seconds")

    return raw_tables_dict_1, new_dataframes_dict_1, vintages_dict_1           # Return both raw and cleaned dataframes

# _________________________________________________________________________
# Function to clean and process Table 2 from all WR PDF files in a folder
def table_2_cleaner(
    input_pdf_folder: str,
    record_folder: str,
    record_txt: str = RECORD_SUFFIX_2,
    persist: bool = False,
    persist_folder: str | None = None,
    pipeline_version: str = "s3.0.0",
) -> tuple[dict[str, pd.DataFrame], dict[str, pd.DataFrame]]:
    """
    Extract page 2 from each WR PDF, run the table 2 pipeline, update the record,
    optionally persist cleaned tables (Parquet/CSV), and show a concise summary.
    """
    start_time = time.time()                                # Record script start time
    print("\n🧹 Starting Table 2 cleaning...\n")             # Display section start message

    cleaner = tables_cleaner()                              # Initialize table cleaner object
    records = _read_records(record_folder, record_txt)       # Load processed record file
    processed = set(records)                                 # Convert to set for fast lookup

    raw_tables_dict_2: dict[str, pd.DataFrame] = {}          # Store extracted raw tables
    new_dataframes_dict_2: dict[str, pd.DataFrame] = {}      # Store cleaned tables

    new_counter = 0                                          # Count new cleaned files
    skipped_counter = 0                                      # Count skipped files
    skipped_years: dict[str, int] = {}                       # Keep skipped counts per year

    # List year directories except '_quarantine'
    years = [d for d in sorted(os.listdir(input_pdf_folder))
             if os.path.isdir(os.path.join(input_pdf_folder, d)) and d != "_quarantine"]
    total_year_folders = len(years)                          # Total number of valid year folders
    
    prep = vintages_preparator()                             # helper for vintages
    vintages_dict_2: dict[str, pd.DataFrame] = {}            # tidy (prepared) outputs

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
        month_order_map = prep.build_month_order_map(folder_path)   # <-- add: filename → month (1..12)

        if not pdf_files:
            continue                                         # Skip if no PDFs present

        # Skip if all PDFs already processed
        already = [f for f in pdf_files if f in processed]
        if len(already) == len(pdf_files):
            skipped_years[year] = len(already)
            skipped_counter += len(already)
            continue

        print(f"\n📂 Processing Table 2 in {year}\n")        # Indicate which year is running
        folder_new_count = 0                                 # New tables within this year
        folder_skipped_count = 0                             # Skipped tables within this year

        # Display progress bar for current year
        pbar = tqdm(pdf_files, desc=f"🚧 {year}", unit="PDF",
                    bar_format=BAR_FORMAT, colour=PROG_COLOR_ACTIVE,
                    leave=False, position=0, dynamic_ncols=True)

        for filename in pbar:
            if filename in processed:
                folder_skipped_count += 1                    # Skip if already processed
                continue

            issue, yr = parse_ns_meta(filename)              # Extract WR issue and year
            if not issue:
                folder_skipped_count += 1
                continue

            pdf_path = os.path.join(folder_path, filename)   # Build full PDF path
            try:
                raw = _extract_table(pdf_path, page=2)       # Extract table 2 (page 2)
                if raw is None:
                    folder_skipped_count += 1
                    continue

                key = f"{os.path.splitext(filename)[0].replace('-', '_')}_2"
                raw_tables_dict_2[key] = raw.copy()          # Save raw table with unique key

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

        pbar.clear(); pbar.close()                           # Close progress bar cleanly

        # Display finished bar for current year
        fb = tqdm(total=len(pdf_files), desc=f"✔️ {year}", unit="PDF",
                  bar_format=BAR_FORMAT, colour=PROG_COLOR_DONE,
                  leave=True, position=0, dynamic_ncols=True)
        fb.update(len(pdf_files))
        fb.close()

        new_counter += folder_new_count                      # Update total new count
        skipped_counter += folder_skipped_count              # Update total skipped count
        _write_records(record_folder, record_txt, list(processed))  # Update processed record file

    # Summary of skipped years
    if skipped_years:
        years_summary = ", ".join(skipped_years.keys())
        total_skipped = sum(skipped_years.values())
        print(f"\n⏩ {total_skipped} cleaned tables already generated for years: {years_summary}")

    elapsed_time = round(time.time() - start_time)            # Compute total elapsed time
    print(f"\n📊 Summary:\n")
    print(f"📂 {total_year_folders} folders (years) found containing input PDFs")
    print(f"🗃️ Already cleaned tables: {skipped_counter}")
    print(f"✨ Newly cleaned tables: {new_counter}")
    print(f"⏱️ {elapsed_time} seconds")

    return raw_tables_dict_2, new_dataframes_dict_2, vintages_dict_2          # Return both raw and cleaned tables

