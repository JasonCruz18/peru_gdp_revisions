
#*********************************************************************************************
# Functions for new_gdp_dataset.ipynb 
#*********************************************************************************************



################################################################################################
# Section 1. PDF Downloader
################################################################################################


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# LIBRARIES
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

import os  # for file and directory manipulation
import random  # for generating random numbers and random delays
import time  # to pause execution and manage timing
import requests  # to make HTTP requests to download PDFs or MP3s
import pygame  # to handle alert track notifications (play/stop .mp3 files)
from selenium import webdriver  # to automate web browser actions
from selenium.webdriver.common.by import By  # to locate elements on a webpage
from selenium.webdriver.support.ui import WebDriverWait  # to wait until elements are present
from selenium.webdriver.support import expected_conditions as EC  # to define wait conditions
from selenium.common.exceptions import StaleElementReferenceException  # handle dynamic web elements
from webdriver_manager.chrome import ChromeDriverManager  # auto-install ChromeDriver
from selenium.webdriver.chrome.options import Options  # set Chrome browser options
from selenium.webdriver.chrome.service import Service  # define ChromeDriver service
import shutil # used for high-level file operations, such as copying, moving, renaming, and deleting files and directories.


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# FUNCTIONS
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# Function to load an alert track
# _________________________________________________________________________
def load_alert_track(alert_track_folder):
    """
    Load a random .mp3 file from the alert track folder.
    If no .mp3 exists, continue without using alert track.
    
    Returns:
        str or None: Path to the loaded alert track, or None if unavailable
    """
    # Ensure the folder exists
    os.makedirs(alert_track_folder, exist_ok=True)

    # Collect all available .mp3 files
    available_alert_tracks = [f for f in os.listdir(alert_track_folder) if f.lower().endswith(".mp3")]

    # Handle case with no available tracks
    if not available_alert_tracks:
        print("🔇 No .mp3 files found in 'alert_track/' folder. Continuing without alert track.")
        return None

    # Select one track at random and load it
    alert_track_path = os.path.join(alert_track_folder, random.choice(available_alert_tracks))
    pygame.mixer.music.load(alert_track_path)
    return alert_track_path

# Function to play alert track
# _________________________________________________________________________
def play_alert_track():
    """Start playing the currently loaded alert track."""
    # Trigger playback of the loaded audio
    pygame.mixer.music.play()

# Function to stop alert track
# _________________________________________________________________________
def stop_alert_track():
    """Stop the currently playing alert track."""
    # Stop audio playback immediately
    pygame.mixer.music.stop()

# Function to wait random seconds
# _________________________________________________________________________
def random_wait(min_time, max_time):
    """
    Pause execution for a random duration between min_time and max_time.
    
    Args:
        min_time (float): Minimum wait time in seconds
        max_time (float): Maximum wait time in seconds
    """
    # Generate a random wait duration
    wait_time = random.uniform(min_time, max_time)

    # Print and apply the delay
    print(f"⏳ Waiting {wait_time:.2f} seconds...")
    time.sleep(wait_time)

# Function to initialize web driver
# _________________________________________________________________________
def init_driver(browser="chrome", headless=False):
    """
    Initialize a Selenium WebDriver.
    
    Args:
        browser (str): Browser to use (currently only 'chrome' supported)
        headless (bool): Whether to run in headless mode
    
    Returns:
        webdriver.Chrome: Initialized Chrome WebDriver
    """
    # Handle Chrome driver initialization
    if browser.lower() == "chrome":
        options = Options()
        
        # Enable headless mode if requested
        if headless:
            options.add_argument("--headless=new")
        
        # Add recommended Chrome options for stability
        options.add_argument("--no-sandbox")
        options.add_argument("--disable-dev-shm-usage")

        # Install and start ChromeDriver
        service = Service(ChromeDriverManager().install())
        driver = webdriver.Chrome(service=service, options=options)
        driver.set_page_load_timeout(30)
        return driver
    
    # Raise error if unsupported browser requested
    else:
        raise ValueError("Currently only Chrome is supported. Future versions may add Firefox/Edge.")

# Function to download a single PDF (WR)
# _________________________________________________________________________
def download_pdf(driver, pdf_link, wait, download_counter, raw_pdf_folder, download_record_folder, download_record_txt):
    """
    Download a single PDF from the selenium link element.
    
    Args:
        driver (webdriver.Chrome): Selenium Chrome WebDriver
        pdf_link (WebElement): Selenium element containing PDF link
        wait (WebDriverWait): WebDriverWait object for explicit waits
        download_counter (int): Counter for display purposes
        raw_pdf_folder (str): Folder to save raw PDFs
        download_record_folder (str): Folder to save download records
    
    Returns:
        bool: True if download succeeded, False otherwise
    """
    # Click the PDF link and switch to the new browser tab
    driver.execute_script("arguments[0].click();", pdf_link)
    wait.until(EC.number_of_windows_to_be(2))
    windows = driver.window_handles
    driver.switch_to.window(windows[1])

    # Extract file name and define local save path
    new_url = driver.current_url
    file_name = os.path.basename(new_url)
    destination_path = os.path.join(raw_pdf_folder, file_name)

    # Download the PDF file and save it locally
    response = requests.get(new_url, stream=True)
    if response.status_code == 200:
        with open(destination_path, 'wb') as pdf_file:
            for chunk in response.iter_content(chunk_size=128):
                pdf_file.write(chunk)

        # Record the file in the download log (chronologically: year → issue)
        import re
        record_path = os.path.join(download_record_folder, download_record_txt)
        records = []
        if os.path.exists(record_path):
            with open(record_path, "r", encoding="utf-8") as f:
                records = [ln.strip() for ln in f if ln.strip()]
        if file_name not in records:
            records.append(file_name)

        def _ns_key(s):
            base = os.path.splitext(os.path.basename(s))[0]
            m = re.search(r"ns-(\d{2})-(\d{4})", base, re.I)
            if not m:  # push unknowns last, stable by name
                return (9999, 9999, base)
            issue, year = int(m.group(1)), int(m.group(2))
            return (year, issue)

        records.sort(key=_ns_key)
        os.makedirs(download_record_folder, exist_ok=True)
        with open(record_path, "w", encoding="utf-8") as f:
            f.write("\n".join(records) + ("\n" if records else ""))

        print(f"{download_counter}. ✅ Downloaded: {file_name}")
        success = True
    else:
        # Handle failed download attempt
        print(f"{download_counter}. ❌ Error downloading {file_name}. HTTP {response.status_code}")
        success = False

    # Close the tab and return to the main window
    driver.close()
    driver.switch_to.window(windows[0])
    return success

# Function to download all PDFs (WR) 
# _________________________________________________________________________
def download_pdfs(
    bcrp_url,
    raw_pdf_folder,
    download_record_folder,
    download_record_txt,
    alert_track_folder,
    max_downloads=None,
    downloads_per_batch=12,
    headless=False
):
    """
    Download BCRP Weekly Report PDFs (latest link per month) with clean numbering, 
    batch alerts, and summary reporting.
    
    Args:
        bcrp_url (str): URL of BCRP Weekly Reports
        raw_pdf_folder (str): Folder to save raw PDFs
        download_record_folder (str): Folder to save download record file
        download_record_txt (str): Record filename for downloaded PDFs log
        alert_track_folder (str): Folder containing notification MP3s
        max_downloads (int, optional): Maximum number of new PDFs to download
        downloads_per_batch (int): Number of PDFs between user prompts
        headless (bool): Whether to run browser in headless mode
    """
    # Start timer for total execution time
    import time, re
    start_time = time.time()

    # Start the downloader and prepare audio alerts
    print("\n📥 Starting PDF Downloader for BCRP Weekly Reports...\n")
    pygame.mixer.init()
    alert_track_path = load_alert_track(alert_track_folder)

    # Load record of already downloaded files
    record_path = os.path.join(download_record_folder, download_record_txt)
    downloaded_files = set()
    if os.path.exists(record_path):
        with open(record_path, "r", encoding="utf-8") as f:
            downloaded_files = set(f.read().splitlines())

    # Initialize browser session
    driver = init_driver(headless=headless)
    wait = WebDriverWait(driver, 60)
    new_counter = 0
    skipped_files = []   # Store names of previously downloaded PDFs
    new_downloads = []   # Store links to new PDFs for downloading

    try:
        # Open the BCRP webpage
        driver.get(bcrp_url)
        print("🌐 BCRP site opened successfully.")

        # Locate monthly blocks and select the first link (latest per month)
        month_ul_elems = wait.until(
            EC.presence_of_all_elements_located((By.CSS_SELECTOR, '#rightside ul.listado-bot-std-claros'))
        )
        print(f"🔎 Found {len(month_ul_elems)} WR on page (one per month).\n")

        # Extract the first anchor from each block
        pdf_links = []
        for ul in month_ul_elems:
            try:
                anchors = ul.find_elements(By.TAG_NAME, "a")
            except Exception:
                anchors = []
            if not anchors:
                continue
            pdf_links.append(anchors[0])

        # Reverse order to start downloading from the oldest
        pdf_links = pdf_links[::-1]

        # Separate already downloaded files from new ones
        for pdf_link in pdf_links:
            try:
                file_url = pdf_link.get_attribute("href")
                file_name = os.path.basename(file_url)
            except Exception:
                continue
            if file_name in downloaded_files:
                skipped_files.append(file_name)
            else:
                new_downloads.append((pdf_link, file_name))

        # Process each new download
        for i, (pdf_link, file_name) in enumerate(new_downloads, start=1):
            # Attempt the download
            if download_pdf(driver, pdf_link, wait, i, raw_pdf_folder, download_record_folder, download_record_txt):
                downloaded_files.add(file_name)
                new_counter += 1

            # Handle batch alerts and optional user prompt
            if i % downloads_per_batch == 0 and alert_track_path:
                play_alert_track()
                user_input = input("⏸️ Continue? (y = yes, any other key = stop): ")
                stop_alert_track()
                if user_input.lower() != 'y':
                    print("🛑 Download stopped by user.")
                    break

            # Stop if maximum number of new downloads reached
            if max_downloads and new_counter >= max_downloads:
                print(f"🏁 Download limit of {max_downloads} new PDFs reached.")
                break

            # Wait randomly between downloads
            random_wait(5, 10)

    except StaleElementReferenceException:
        # Retry if selenium loses reference
        print("⚠️ StaleElementReferenceException occurred. Retrying...")
    finally:
        # Always close the browser
        driver.quit()
        print("\n👋 Browser closed.")

    # Ensure the record file is chronologically ordered (year → issue)
    try:
        if os.path.exists(record_path):
            with open(record_path, "r", encoding="utf-8") as f:
                records = [ln.strip() for ln in f if ln.strip()]

            def _ns_key(s):
                base = os.path.splitext(os.path.basename(s))[0]
                m = re.search(r"ns-(\d{2})-(\d{4})", base, re.I)
                if not m:
                    return (9999, 9999, base)
                issue, year = int(m.group(1)), int(m.group(2))
                return (year, issue)

            records = sorted(set(records), key=_ns_key)
            os.makedirs(download_record_folder, exist_ok=True)
            with open(record_path, "w", encoding="utf-8") as f:
                f.write("\n".join(records) + ("\n" if records else ""))
    except Exception as _e:
        # Non-fatal: if sorting fails, keep proceeding
        pass

    # Final summary of operations
    elapsed_time = round(time.time() - start_time)
    total_links = len(pdf_links)
    print(f"\n📊 Summary:")
    print(f"\n🔗 Total monthly links kept: {total_links}")
    if skipped_files:
        print(f"🗂️ {len(skipped_files)} already downloaded PDFs were skipped.")
    print(f"➕ Newly downloaded: {new_counter}")
    print(f"⏱️ Time: {elapsed_time} seconds")

# Function to organize PDFs by year
# _________________________________________________________________________
def organize_files_by_year(raw_pdf_folder):
    """
    Organize PDF files in the given folder into subfolders by year.
    The year is extracted from the file name (expects a 4-digit number).
    
    Args:
        raw_pdf_folder (str): Path to the folder containing raw PDFs
    """
    # Get the list of files in the directory
    files = os.listdir(raw_pdf_folder)

    # Iterate over each file in the folder
    for file in files:
        # Extract year candidate from the file name
        name, extension = os.path.splitext(file)
        year = None
        name_parts = name.split('-')
        for part in name_parts:
            if part.isdigit() and len(part) == 4:
                year = part
                break

        # If a year is found, move the file into the corresponding year subfolder
        if year:
            destination_folder = os.path.join(raw_pdf_folder, year)

            # Create the year subfolder if it does not exist
            if not os.path.exists(destination_folder):
                os.makedirs(destination_folder)

            # Move the file to the destination folder
            shutil.move(os.path.join(raw_pdf_folder, file), destination_folder)
            
            
            
################################################################################################
# Section 2. Generate PDF input with key tables
################################################################################################


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# LIBRARIES
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

import os  # File/folder manipulation
import fitz  # PDF manipulation
import ipywidgets as widgets  # Interactive widgets for Jupyter
from IPython.display import display  # Display widgets in Jupyter
import time
from tqdm.notebook import tqdm
import pdfplumber
from PyPDF2 import PdfReader, PdfWriter


# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++
# FUNCTIONS
# +++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++

# Function to replace defective PDFs with specific issues (year-aware)
# _________________________________________________________________________
def replace_ns_pdfs(items, root_folder, record_folder, download_record_txt, quarantine=None):
    """
    Replace defective PDFs stored under year subfolders.
    items: list of (year, defective_pdf, replacement_code), e.g.
           [("2017","ns-08-2017.pdf","ns-07-2017"), ("2019","ns-23-2019.pdf","ns-22-2019")]
    root_folder: base path containing year folders (e.g., raw_pdf)
    record_folder: path holding the download record TXT
    download_record_txt: record filename (e.g., 'downloaded_pdfs.txt')
    quarantine: optional folder to move defective PDFs (else they’re deleted)
    """
    # Helpers: normalize code, build URL, update record (keep defective entries!)
    pat = re.compile(r'^ns-(\d{1,2})-(\d{4})(?:\.pdf)?$', re.I)
    def norm(c):
        m = pat.match(os.path.basename(c).lower())
        if not m: raise ValueError(f"Bad NS code: {c}")
        return f"ns-{int(m.group(1)):02d}-{m.group(2)}"
    def url(c): 
        cc = norm(c); return f"https://www.bcrp.gob.pe/docs/Publicaciones/Nota-Semanal/{cc[-4:]}/{cc}.pdf"
    def _ns_key(name):
        base = os.path.splitext(os.path.basename(name))[0]
        m = re.search(r"ns-(\d{2})-(\d{4})", base, re.I)
        if not m: return (9999, 9999, base)
        issue, year = int(m.group(1)), int(m.group(2))
        return (year, issue, base)
    def update_record(add=None, remove=None):
        # Intentionally DO NOT remove defective entries from the TXT, so the downloader won’t fetch them again.
        p = os.path.join(record_folder, download_record_txt)
        s = set()
        if os.path.exists(p):
            with open(p, "r", encoding="utf-8") as f:
                s = {x.strip() for x in f if x.strip()}
        if add:
            s.add(add)
        records = sorted(s, key=_ns_key)  # chronological: year → issue
        os.makedirs(record_folder, exist_ok=True)
        with open(p, "w", encoding="utf-8") as f:
            f.write("\n".join(records) + ("\n" if records else ""))

    # Friendly header
    print(f"\n🧩 Replacing {len(items)} PDF(s) under: {root_folder}")
    if quarantine:
        os.makedirs(quarantine, exist_ok=True)
        print(f"🦠 Quarantine enabled → {quarantine}")
    ok, fail = 0, 0

    # Process each (year, defective_pdf, replacement_code)
    for year, bad_pdf, repl_code in items:
        year = str(year)
        ydir = os.path.join(root_folder, year)
        bad_path = os.path.join(ydir, bad_pdf)
        new_name = f"{norm(repl_code)}.pdf"
        new_path = os.path.join(ydir, new_name)

        # Check defective file presence
        if not os.path.exists(bad_path):
            print(f"⚠️  {year}: not found → {bad_pdf} (skipped)")
            fail += 1
            continue

        # Try downloading replacement first (safer)
        try:
            os.makedirs(ydir, exist_ok=True)
            print(f"⬇️  {year}: downloading {norm(repl_code)} …")
            with requests.get(url(repl_code), stream=True, timeout=60) as r:
                r.raise_for_status()
                with open(new_path, "wb") as fh:
                    for ch in r.iter_content(131072):
                        if ch: fh.write(ch)
        except Exception as e:
            if os.path.exists(new_path):
                try: os.remove(new_path)
                except: pass
            print(f"❌  {year}: download failed for {norm(repl_code)} → {e}")
            fail += 1
            continue

        # Move defective to quarantine or delete
        try:
            if quarantine:
                shutil.move(bad_path, os.path.join(quarantine, bad_pdf))
                moved_msg = f"moved to {os.path.basename(quarantine)}"
            else:
                os.remove(bad_path); moved_msg = "deleted"
        except Exception as e:
            print(f"❌  {year}: could not remove old file {bad_pdf} → {e}")
            fail += 1
            continue

        # Update record and report (keep defective in TXT; just add the replacement)
        update_record(add=new_name, remove=bad_pdf)
        print(f"✅  {year}: {bad_pdf} → {new_name} ({moved_msg})")
        ok += 1

    # Friendly footer
    print(f"\n📊 Summary: ✅ {ok} done · ❌ {fail} failed")
    
# Function to search for pages containing specified keywords in a PDF file
# _________________________________________________________________________
def search_keywords(pdf_file, keywords):
    """Search pages in PDF that contain any of the keywords.

    Args:
        pdf_file (str): Path to PDF.
        keywords (list of str): Keywords to search.

    Returns:
        List[int]: Pages containing keywords (0-indexed).
    """
    pages_with_keywords = []
    with fitz.open(pdf_file) as doc:
        for page_num in range(doc.page_count):
            page_text = doc.load_page(page_num).get_text()
            if any(k in page_text for k in keywords):
                pages_with_keywords.append(page_num)
    return pages_with_keywords


# Function to shorten a PDF based on specified pages
# _________________________________________________________________________
def shortened_pdf(pdf_file, pages, output_folder):
    """Shorten PDF to pages of interest and save to output folder.

    Args:
        pdf_file (str): Path to source PDF.
        pages (list[int]): Pages to retain.
        output_folder (str): Folder to save input PDF.

    Returns:
        int: Number of pages in the input PDF (0 if skipped).
    """
    if not pages:
        return 0

    os.makedirs(output_folder, exist_ok=True)
    new_pdf_file = os.path.join(output_folder, os.path.basename(pdf_file))
    with fitz.open(pdf_file) as doc:
        new_doc = fitz.open()
        for p in pages:
            new_doc.insert_pdf(doc, from_page=p, to_page=p)
        new_doc.save(new_pdf_file)
    return new_doc.page_count

# Function to read input PDF filenames from record file
# _________________________________________________________________________
def read_input_pdf_files(input_pdf_record_folder, input_pdf_record_txt):
    """Read filenames of previously processed PDFs.

    Returns:
        set[str]: Set of filenames.
    """
    record_path = os.path.join(input_pdf_record_folder, input_pdf_record_txt)
    if not os.path.exists(record_path):
        return set()
    with open(record_path, "r") as f:
        return set(f.read().splitlines())

# Function to write input PDF filenames to record file
# _________________________________________________________________________
def write_input_pdf_files(input_pdf_files, input_pdf_record_folder, input_pdf_record_txt):
    """Write processed PDF filenames to record file."""
    record_path = os.path.join(input_pdf_record_folder, input_pdf_record_txt)
    os.makedirs(input_pdf_record_folder, exist_ok=True)
    with open(record_path, "w") as f:
        for fn in sorted(input_pdf_files):
            f.write(fn + "\n")

# Function to ask user yes/no in Jupyter or server
# _________________________________________________________________________
def ask_continue_input(message):
    """Ask the user whether to continue using a simple input prompt."""
    while True:
        ans = input(f"{message} (y = yes / n = no): ").strip().lower()
        if ans in ("y", "n"):
            return ans == "y"
        
# Function to generate input PDFs (from raw PDFs)
# _________________________________________________________________________
def generate_input_pdfs(
    raw_pdf_folder,
    input_pdf_folder,
    input_pdf_record_folder,
    input_pdf_record_txt,
    keywords
):
    """
    Generate input PDFs containing key pages by searching for keywords
    in raw PDFs. Produces input PDFs and updates a processing record.

    Args:
        raw_pdf_folder (str): Folder containing yearly subfolders of raw PDFs.
        input_pdf_folder (str): Folder to save input input PDFs.
        input_pdf_record_folder (str): Folder for the record file.
        input_pdf_record_txt (str): Record filename.
        keywords (list[str]): Keywords to search for.
    """

    start_time = time.time()

    # Read previously processed PDFs
    input_pdf_files = read_input_pdf_files(input_pdf_record_folder, input_pdf_record_txt)

    # Track years with already processed PDFs
    skipped_years = {}
    new_counter = 0
    skipped_counter = 0

    # Loop over yearly folders
    for folder in sorted(os.listdir(raw_pdf_folder)):
        # Skip quarantine folder created by replacement routine
        if folder == "_quarantine":
            continue

        folder_path = os.path.join(raw_pdf_folder, folder)
        if not os.path.isdir(folder_path):
            continue

        pdf_files = [f for f in os.listdir(folder_path) if f.endswith(".pdf")]
        if not pdf_files:
            continue

        # Count how many PDFs in this folder are already processed
        already = [f for f in pdf_files if f in input_pdf_files]
        if len(already) == len(pdf_files):
            skipped_years[folder] = len(already)
            skipped_counter += len(already)
            continue

        # Process only new PDFs in this folder
        print(f"\n📂 Processing folder: {folder}\n")
        folder_new_count = 0
        folder_skipped_count = 0

        # Progress bar with custom colors: active=#E6004C, finished=#3366FF
        pbar = tqdm(
            pdf_files,
            desc=f"Generating input PDFs in {folder}",
            unit="PDF",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
            colour="#E6004C"  # in-progress color
        )

        for filename in pbar:
            pdf_file = os.path.join(folder_path, filename)
            if filename in input_pdf_files:
                folder_skipped_count += 1
                continue

            pages_with_keywords = search_keywords(pdf_file, keywords)

            num_pages = shortened_pdf(pdf_file, pages_with_keywords, output_folder=input_pdf_folder)
            
            # keep only first and third pages if PDF has 4 pages (they contain the key tables)            
            short_pdf_file = os.path.join(input_pdf_folder, os.path.basename(pdf_file))
            reader = PdfReader(short_pdf_file)

            # Only apply if it has 4 pages
            if len(reader.pages) == 4:
                writer = PdfWriter()
                writer.add_page(reader.pages[0])  # first page
                writer.add_page(reader.pages[2])  # third page
                with open(short_pdf_file, "wb") as f_out:
                    writer.write(f_out)
                    
            # Now update processed PDF list        
            if num_pages > 0:
                input_pdf_files.add(filename)
                folder_new_count += 1

        # Set finished color and refresh the bar (leave it visible)
        try:
            pbar.colour = "#3366FF"  # finished color
            pbar.refresh()
        except Exception:
            pass
        finally:
            pbar.close()

        # Chronological write: sort by (year, issue) and write exactly in that order
        import re
        def _ns_key(s):
            base = os.path.splitext(os.path.basename(s))[0]  # strip .pdf if present
            m = re.search(r"ns-(\d{2})-(\d{4})", base, re.I)
            if not m:
                return (9999, 9999, base)  # push unknowns to the end
            issue = int(m.group(1))
            year  = int(m.group(2))
            return (year, issue)

        ordered_records = sorted(input_pdf_files, key=_ns_key)
        os.makedirs(input_pdf_record_folder, exist_ok=True)
        record_path = os.path.join(input_pdf_record_folder, input_pdf_record_txt)
        with open(record_path, "w", encoding="utf-8") as f_rec:
            for name in ordered_records:
                f_rec.write(name + "\n")
                
        # Folder summary
        print(f"✅ Shortened PDFs saved in '{input_pdf_folder}' "
              f"({folder_new_count} new, {folder_skipped_count} skipped)")

        new_counter += folder_new_count
        skipped_counter += folder_skipped_count

        # Ask user if they want to continue
        if not ask_continue_input(f"Do you want to continue to the next folder after '{folder}'?"):
            print("🛑 Process stopped by user.")
            break

    # Print summary of already processed years
    if skipped_years:
        years_summary = ", ".join(skipped_years.keys())
        total_skipped = sum(skipped_years.values())
        print(f"\n⏩ {total_skipped} input PDFs already generated for years: {years_summary}")

    # Final summary
    elapsed_time = round(time.time() - start_time)
    print(f"\n📊 Summary:\n")
    print(f"📂 {len(os.listdir(raw_pdf_folder))} folders (years) found containing raw PDFs")
    print(f"🗂️ Already generated input PDFs: {skipped_counter}")
    print(f"➕ Newly generated input PDFs: {new_counter}")
    print(f"⏱️ Time: {elapsed_time} seconds")



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


#+++++++++++++++
# By WR
#+++++++++++++++


# 𝑛𝑠_2014_07 sectores económicos
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


#+++++++++++++++
# By WR
#+++++++++++++++

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



################################################################################################
# Section 4. SQL Tables
################################################################################################


#+++++++++++++++
# LIBRARIES
#+++++++++++++++

import tkinter as tk  # Import the tkinter library for creating the GUI
from tkinter import simpledialog  # Import the simpledialog module from tkinter for creating dialog boxes



# Define the options and their mappings
options = [
    "gdp", 
    "agriculture",  # agriculture and livestock
    "fishing",
    "mining",  # mining and fuel
    "manufacturing",
    "electricity",  # electricity and water
    "construction",
    "commerce",
    "services"  # other services
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

# Function to show the option window
def show_option_window():
    """
    Displays a Tkinter window to select an option, and returns the corresponding 
    selected values for 'selected_spanish', 'selected_english', and 'sector'.
    """
    # Variables to store the selected options
    selected_spanish = None
    selected_english = None
    sector = None

    def save_option():
        nonlocal selected_spanish, selected_english, sector
        sector = selected_option.get()
        selected_spanish, selected_english = option_mapping[sector]
        root.destroy()  # Close the window after selecting an option

    # Create the popup window
    root = tk.Tk()
    root.title("Select Option")

    # Variable to store the selected option
    selected_option = tk.StringVar(root)
    selected_option.set(options[0])  # Default option

    # Create the option menu
    menu = tk.OptionMenu(root, selected_option, *options)
    menu.pack(pady=10)

    # Button to confirm the selection
    confirm_button = tk.Button(root, text="Confirm", command=save_option)
    confirm_button.pack()

    # Show the window
    root.update_idletasks()
    root.wait_window()

    # Return the selected values
    return selected_spanish, selected_english, sector

# Function to show option window
#________________________________________________________________

# def show_option_window():
#     # Define the list of options
#     options = [
#         "gdp", 
#         "agriculture",  # agriculture and livestock
#         "fishing",
#         "mining",  # mining and fuel
#         "manufacturing",
#         "electricity",  # electricity and water
#         "construction",
#         "commerce",
#         "services"  # other services
#     ]

#     # Function to save the selected option and close the window
#     def save_option():
#         global sector
#         sector = selected_option.get()
#         root.destroy()  # Close the window after selecting an option

#     # Create the popup window
#     root = tk.Tk()
#     root.title("Select Option")

#     # Variable to store the selected option
#     selected_option = tk.StringVar(root)
#     selected_option.set(options[0])  # Default option

#     # Create the option menu
#     menu = tk.OptionMenu(root, selected_option, *options)
#     menu.pack(pady=10)

#     # Button to confirm the selection
#     confirm_button = tk.Button(root, text="Confirm", command=save_option)
#     confirm_button.pack()

#     # Show the window
#     root.update_idletasks()
#     root.wait_window()

#     return selected_option.get()

# Function to show frequency window
#________________________________________________________________
def show_frequency_window():
    # Define the list of options
    frequencies = [
        "monthly", 
        "quarterly",
        "annual"
    ]

    # Function to save the selected option and close the window
    def save_frequency():
        global frequency
        frequency = selected_frequency.get()
        root.destroy()  # Close the window after selecting an option

    # Create the popup window
    root = tk.Tk()
    root.title("Select Frequency")

    # Variable to store the selected option
    selected_frequency = tk.StringVar(root)
    selected_frequency.set(frequencies[0])  # Default option

    # Create the option menu
    menu = tk.OptionMenu(root, selected_frequency, *frequencies)
    menu.pack(pady=10)

    # Button to confirm the selection
    confirm_button = tk.Button(root, text="Confirm", command=save_frequency)
    confirm_button.pack()

    # Show the window
    root.update_idletasks()
    root.wait_window()
    
    return selected_frequency.get()



#**********************************************************************************************
# Section 4.1. Annual Concatenation
#----------------------------------------------------------------------------------------------


# Function to concatenate Table 2 (annual)
# _________________________________________________________________________
def concatenate_annual_df(dataframes_dict, sector_economico, economic_sector):
    # List to store the names of dataframes that meet the criterion of ending in '_2'
    dataframes_ending_with_2 = []

    # List to store the names of dataframes to be concatenated
    dataframes_to_concatenate = []

    # Iterate over the dataframe names in the all_dataframes dictionary
    for df_name in dataframes_dict.keys():
        # Check if the dataframe name ends with '_2' and add it to the corresponding list
        if df_name.endswith('_2'):
            dataframes_ending_with_2.append(df_name)
            dataframes_to_concatenate.append(dataframes_dict[df_name])

    # Print the names of dataframes that meet the criterion of ending in '_2'
    #print("DataFrames ending with '_2' that will be concatenated:")
    #for df_name in dataframes_ending_with_2:
    #    print(df_name)

    # Concatenate all dataframes in the 'dataframes_to_concatenate' list
    if dataframes_to_concatenate:
        # Concatenate only rows that meet the specified conditions
        annual_growth_rates = pd.concat([df[(df['sectores_economicos'] == sector_economico) | (df['economic_sectors'] == economic_sector)] 
                                    for df in dataframes_to_concatenate 
                                    if 'sectores_economicos' in df.columns and 'economic_sectors' in df.columns], 
                                    ignore_index=True)

        # Keep only columns that start with 'year' and the 'id_ns', 'year', and 'date' columns
        columns_to_keep = ['year', 'id_ns', 'date'] + [col for col in annual_growth_rates.columns if col.endswith('_year')]

        # Drop unwanted columns
        annual_growth_rates = annual_growth_rates[columns_to_keep]
        
        # Remove duplicate columns if any
        annual_growth_rates = annual_growth_rates.loc[:,~annual_growth_rates.columns.duplicated()]
    
        # Cambia el nombre de las columnas a partir de la cuarta columna
        annual_growth_rates.columns = [col.split('_')[1] + '_' + col.split('_')[0] if '_' in col and idx >= 3 else col for idx, col in enumerate(annual_growth_rates.columns)]

        # Print the number of rows in the concatenated dataframe
        print("Number of rows in the concatenated dataframe:", len(annual_growth_rates))
        
        return annual_growth_rates
    else:
        print("No dataframes were found to concatenate.")
        return None
    
# Function to concatenate Table 1 (quarterly)
# _________________________________________________________________________
    
def concatenate_quarterly_df(dataframes_dict, sector_economico, economic_sector):
    # List to store the names of dataframes that meet the criterion of ending in '_2'
    dataframes_ending_with_2 = []

    # List to store the names of dataframes to be concatenated
    dataframes_to_concatenate = []

    # Iterate over the dataframe names in the all_dataframes dictionary
    for df_name in dataframes_dict.keys():
        # Check if the dataframe name ends with '_2' and add it to the corresponding list
        if df_name.endswith('_2'):
            dataframes_ending_with_2.append(df_name)
            dataframes_to_concatenate.append(dataframes_dict[df_name])

    # Print the names of dataframes that meet the criterion of ending in '_2'
    #print("DataFrames ending with '_2' that will be concatenated:")
    #for df_name in dataframes_ending_with_2:
    #    print(df_name)

    # Concatenate all dataframes in the 'dataframes_to_concatenate' list
    if dataframes_to_concatenate:
        # Concatenate only rows that meet the specified conditions
        quarterly_growth_rates = pd.concat([df[(df['sectores_economicos'] == sector_economico) | (df['economic_sectors'] == economic_sector)] 
                                    for df in dataframes_to_concatenate 
                                    if 'sectores_economicos' in df.columns and 'economic_sectors' in df.columns], 
                                    ignore_index=True)

        # Keep all columns except those starting with 'year_', in addition to the 'id_ns', 'year', and 'date' columns
        columns_to_keep = ['year', 'id_ns', 'date'] + [col for col in quarterly_growth_rates.columns if not col.endswith('_year')]

        # Select unwanted columns
        quarterly_growth_rates = quarterly_growth_rates[columns_to_keep]

        # Drop the 'sectores_economicos' and 'economic_sectors' columns
        quarterly_growth_rates.drop(columns=['sectores_economicos', 'economic_sectors'], inplace=True)

        # Remove duplicate columns if any
        quarterly_growth_rates = quarterly_growth_rates.loc[:, ~quarterly_growth_rates.columns.duplicated()]
        
        # Print the number of rows in the concatenated dataframe
        print("Number of rows in the concatenated dataframe:", len(quarterly_growth_rates))
        
        return quarterly_growth_rates
    else:
        print("No dataframes were found to concatenate.")
        return None
    
# Function to concatenate Table 1 (monthly)
# _________________________________________________________________________
    
def concatenate_monthly_df(dataframes_dict, sector_economico, economic_sector):
    # List to store the names of dataframes that meet the criterion of ending in '_1'
    dataframes_ending_with_1 = []

    # List to store the names of dataframes to be concatenated
    dataframes_to_concatenate = []

    # Iterate over the dataframe names in the all_dataframes dictionary
    for df_name in dataframes_dict.keys():
        # Check if the dataframe name ends with '_1' and add it to the corresponding list
        if df_name.endswith('_1'):
            dataframes_ending_with_1.append(df_name)
            dataframes_to_concatenate.append(dataframes_dict[df_name])

    # Print the names of dataframes that meet the criterion of ending with '_1'
    #print("DataFrames ending with '_1' that will be concatenated:")
    #for df_name in dataframes_ending_with_1:
    #    print(df_name)

    # Concatenate all dataframes in the 'dataframes_to_concatenate' list
    if dataframes_to_concatenate:
        # Concatenate only rows that meet the specified conditions
        monthly_growth_rates = pd.concat([df[(df['sectores_economicos'] == sector_economico) | (df['economic_sectors'] == economic_sector)] 
                                    for df in dataframes_to_concatenate 
                                    if 'sectores_economicos' in df.columns and 'economic_sectors' in df.columns], 
                                    ignore_index=True)

        # Keep all columns except those starting with 'year_', in addition to the 'id_ns', 'year', and 'date' columns
        columns_to_keep = ['year', 'id_ns', 'date'] + [col for col in monthly_growth_rates.columns if not (col.endswith('_year') or col.endswith('_mean'))]

        # Select unwanted columns
        monthly_growth_rates = monthly_growth_rates[columns_to_keep]

        # Drop the 'sectores_economicos' and 'economic_sectors' columns
        monthly_growth_rates.drop(columns=['sectores_economicos', 'economic_sectors'], inplace=True)

        # Remove duplicate columns if any
        monthly_growth_rates = monthly_growth_rates.loc[:,~monthly_growth_rates.columns.duplicated()]

        # Drop columns with at least two underscores in their names
        columns_to_drop = [col for col in monthly_growth_rates.columns if col.count('_') >= 2]
        monthly_growth_rates.drop(columns=columns_to_drop, inplace=True)
        
        # Cambia el nombre de las columnas a partir de la cuarta columna
        monthly_growth_rates.columns = [col.split('_')[1] + '_' + col.split('_')[0] if '_' in col and idx >= 3 else col for idx, col in enumerate(monthly_growth_rates.columns)]
        
        # Print the number of rows in the concatenated dataframe
        print("Number of rows in the concatenated dataframe:", len(monthly_growth_rates))

        return monthly_growth_rates
    else:
        print("No dataframes were found to concatenate.")
        return None
