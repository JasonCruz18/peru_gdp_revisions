#*********************************************************************************************
# Functions for gdp_revisions_datasets 
#*********************************************************************************************



#----------------------------------------------------------------
# 1. PDF Downloader
#----------------------------------------------------------------


#+++++++++++++++
# LIBRARIES
#+++++++++++++++

import os  # for file and directory manipulation
import requests  # to make HTTP requests to web servers
import random  # to generate random numbers
import time  # to manage time and take breaks in the script
from selenium import webdriver  # for automating web browsers
from selenium.webdriver.common.by import By  # to locate elements on a webpage
from selenium.webdriver.support.ui import WebDriverWait  # to wait until certain conditions are met on a webpage.
from selenium.webdriver.support import expected_conditions as EC  # to define expected conditions
from selenium.common.exceptions import StaleElementReferenceException  # To handle exceptions related to elements on the webpage that are no longer available.
import pygame # Allows you to handle graphics, sounds and input events.

import shutil # used for high-level file operations, such as copying, moving, renaming, and deleting files and directories.


# Function to play the sound
#________________________________________________________________
def play_sound():
    pygame.mixer.music.play()
    
    
# Function to wait random seconds
#________________________________________________________________
def random_wait(min_time, max_time):
    wait_time = random.uniform(min_time, max_time)
    print(f"Waiting randomly for {wait_time:.2f} seconds")
    time.sleep(wait_time)


# Function to download PDFs
#________________________________________________________________
def download_pdf(driver, pdf_link, wait, download_counter, raw_pdf, download_record):
    # Click the link using JavaScript
    driver.execute_script("arguments[0].click();", pdf_link)

    # Wait for the new page to fully open (adjust timing as necessary)
    wait.until(EC.number_of_windows_to_be(2))

    # Switch to the new window or tab
    windows = driver.window_handles
    driver.switch_to.window(windows[1])

    # Get the current URL (may vary based on site-specific logic)
    new_url = driver.current_url
    print(f"{download_counter}. New URL: {new_url}")

    # Get the file name from the URL
    file_name = new_url.split("/")[-1]

    # Form the full destination path
    destination_path = os.path.join(raw_pdf, file_name)

    # Download the PDF
    response = requests.get(new_url, stream=True)

    # Check if the request was successful (status code 200)
    if response.status_code == 200:
        # Save the PDF content to the local file
        with open(destination_path, 'wb') as pdf_file:
            for chunk in response.iter_content(chunk_size=128):
                pdf_file.write(chunk)

        print(f"PDF downloaded successfully at: {destination_path}")

        # Save the file name in the record
        with open(os.path.join(download_record, "downloaded_files.txt"), "a") as f:
            f.write(file_name + "\n")

    else:
        print(f"Error downloading the PDF. Response code: {response.status_code}")

    # Close the new window or tab
    driver.close()

    # Switch back to the main window
    driver.switch_to.window(windows[0])
    

# Function to organize PDFs by year
#________________________________________________________________       
def organize_files_by_year(raw_pdf):
    # Get the list of files in the directory
    files = os.listdir(raw_pdf)

    # Iterate over each file
    for file in files:
        # Get the year from the file name
        name, extension = os.path.splitext(file)
        year = None
        name_parts = name.split('-')
        for part in name_parts:
            if part.isdigit() and len(part) == 4:
                year = part
                break

        # If the year was found, move the file to the corresponding folder
        if year:
            destination_folder = os.path.join(raw_pdf, year)
            # Create the folder if it doesn't exist
            if not os.path.exists(destination_folder):
                os.makedirs(destination_folder)
            # Move the file to the destination folder
            shutil.move(os.path.join(raw_pdf, file), destination_folder)
            
            
            
#----------------------------------------------------------------
# 2. Data cleaning
#----------------------------------------------------------------


#+++++++++++++++
# LIBRARIES
#+++++++++++++++

import os
import shutil
import fitz  # for working with PDFs
import tkinter as tk  # for creating popup windows


# Folder path to download the trimmed PDF files (these are PDF inputs for the extraction and cleanup code)
input_pdf = 'input_pdf'

# Folder path to save PDF files containing only the pages of interest (where the GDP growth rate tables are located)
input_pdf_record = 'input_pdf_record'
    
# Folder path to save the record of trimmed PDFs (input PDF)    
input_pdf_record_txt = 'input_pdf_record.txt'


# Function to search for pages containing specified keywords in a PDF file
# _________________________________________________________________________
def search_keywords(pdf_file, keywords):
    """Searches for pages containing specified keywords in a PDF file.

    Args:
    - pdf_file (str): Path to the PDF file to search.
    - keywords (list): List of keywords to search for.

    Returns:
    - pages_with_keywords (list): List of page numbers containing any of the keywords.
    """
    pages_with_keywords = []
    with fitz.open(pdf_file) as doc:
        for page_num in range(doc.page_count):
            page = doc.load_page(page_num)
            text = page.get_text()
            if any(keyword in text for keyword in keywords):
                pages_with_keywords.append(page_num)
    return pages_with_keywords

# Function to trim a PDF file based on specified pages
# _________________________________________________________________________
def trim_pdf(pdf_file, pages):
    """Trims a PDF file based on specified pages containing keywords.

    Args:
    - pdf_file (str): Path to the PDF file to trim.
    - pages (list): List of page numbers to retain.

    Returns:
    - num_pages_new_pdf (int): Number of pages in the trimmed PDF.
    """
    if not pages:
        print(f"No pages found with keywords in {pdf_file}")
        return 0
    
    new_pdf_file = os.path.join(input_pdf, os.path.basename(pdf_file))
    
    with fitz.open(pdf_file) as doc:
        new_doc = fitz.open()
        new_doc.insert_pdf(doc, from_page=0, to_page=0)
        for page_num in pages:
            new_doc.insert_pdf(doc, from_page=page_num, to_page=page_num)
        new_doc.save(new_pdf_file)
    
    num_pages_new_pdf = new_doc.page_count
    print(f"The trimmed PDF '{new_pdf_file}' has {num_pages_new_pdf} pages.")

    if num_pages_new_pdf == 5:
        final_doc = fitz.open()
        final_doc.insert_pdf(new_doc, from_page=0, to_page=0)
        final_doc.insert_pdf(new_doc, from_page=1, to_page=1)
        final_doc.insert_pdf(new_doc, from_page=3, to_page=3)
        final_doc.save(new_pdf_file)

        num_pages_new_pdf = final_doc.page_count
        print(f"Only the cover and pages with 2 tables of interest are retained in the trimmed PDF '{new_pdf_file}'.")
    else:
        print(f"All pages are retained in the trimmed PDF '{new_pdf_file}'.")

    return num_pages_new_pdf

# Function to read input PDF files from a record file
# _________________________________________________________________________
def read_input_pdf_files():
    """Reads input PDF filenames from a record file.

    Returns:
    - set of input PDF filenames.
    """
    input_pdf_files_path = os.path.join(input_pdf_record, input_pdf_record_txt)
    if not os.path.exists(input_pdf_files_path):
        return set()
    
    with open(input_pdf_files_path, 'r') as file:
        return set(file.read().splitlines())

# Function to write input PDF filenames to a record file
# _________________________________________________________________________
def write_input_pdf_files(input_pdf_files):
    """Writes input PDF filenames to a record file.

    Args:
    - input_pdf_files (set): Set of input PDF filenames to write.
    """
    input_pdf_files_path = os.path.join(input_pdf_record, input_pdf_record_txt)
    sorted_filenames = sorted(input_pdf_files)  # Sort the filenames
    with open(input_pdf_files_path, 'w') as file:
        for filename in sorted_filenames:
            file.write(filename + '\n')

