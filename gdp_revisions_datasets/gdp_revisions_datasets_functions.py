#*********************************************************************************************
# Functions for gdp_revisions_datasets 
#*********************************************************************************************



################################################################################################
# Section 1. PDF Downloader
################################################################################################


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
            
            
            
################################################################################################
# Section 2. Generate PDF input with key tables
################################################################################################


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



################################################################################################
# Section 3. Data cleaning
################################################################################################


#----------------------------------------------------------------------------------------------
# Section 3.1. A brief documentation on issus in the table information of the PDFs. 
#----------------------------------------------------------------------------------------------

#+++++++++++++++
# LIBRARIES
#+++++++++++++++

from PIL import Image  # Used for opening, manipulating, and saving image files.
import matplotlib.pyplot as plt  # Used for creating static, animated, and interactive visualizations.


# Function to 
# _________________________________________________________________________


#----------------------------------------------------------------------------------------------
# Section 3.1. A brief documentation on issus in the table information of the PDFs. 
#----------------------------------------------------------------------------------------------

#+++++++++++++++
# LIBRARIES
#+++++++++++++++

from PIL import Image  # Used for opening, manipulating, and saving image files.
import matplotlib.pyplot as plt  # Used for creating static, animated, and interactive visualizations.


#+++++++++++++++
# Cleaning up
#+++++++++++++++


# Auxiliary functions (used within other functions)
# _________________________________________________________________________

def remove_rare_characters_first_row(texto):
    texto = re.sub(r'\s*-\s*', '-', texto)  # Removes spaces around hyphens
    texto = re.sub(r'[^a-zA-Z0-9\s-]', '', texto)  # Removes rare characters except letters, digits, and hyphens
    return texto

def remove_rare_characters(texto):
    return re.sub(r'[^a-zA-Z\s]', '', texto)  # Removes rare characters, allowing only letters and spaces

def remove_tildes(texto):
    return ''.join((c for c in unicodedata.normalize('NFD', texto) if unicodedata.category(c) != 'Mn'))  # Removes tildes and other diacritical marks


# Functions for both Table 1 and Table 2
# _________________________________________________________________________

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
