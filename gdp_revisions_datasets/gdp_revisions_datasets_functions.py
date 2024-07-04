#*********************************************************************************************
#*********************************************************************************************
# Functions for gdp_revisions_datasets 
#*********************************************************************************************
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


#**********************************************************************************************
# Section 3.1. A brief documentation on issus in the table information of the PDFs. 
#----------------------------------------------------------------------------------------------

#+++++++++++++++
# LIBRARIES
#+++++++++++++++

from PIL import Image  # Used for opening, manipulating, and saving image files.
import matplotlib.pyplot as plt  # Used for creating static, animated, and interactive visualizations.


# Function to PENDING
# _________________________________________________________________________


#**********************************************************************************************
# Section 3.2.  Extracting tables and data cleanup 
#----------------------------------------------------------------------------------------------

#+++++++++++++++
# LIBRARIES
#+++++++++++++++

from PIL import Image  # Used for opening, manipulating, and saving image files.
import matplotlib.pyplot as plt  # Used for creating static, animated, and interactive visualizations.



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
# By NS
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
            if df[columns[i+1]].isnull() and not columns[i+1].endswith('_year'):
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
# By NS
#+++++++++++++++

# 𝑛𝑠_20
#...............................................................................................................................