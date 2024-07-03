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
# Function to download PDFs
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
# 2. 
#----------------------------------------------------------------