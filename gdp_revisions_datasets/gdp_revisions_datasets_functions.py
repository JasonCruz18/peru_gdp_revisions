#*********************************************************************************************
# Functions for gdp_revisions_datasets 
#*********************************************************************************************



#----------------------------------------------------------------
# 1. PDF Downloader
#----------------------------------------------------------------

#+++++++++++++++
# LIBRARIES
#+++++++++++++++

import random  # to generate random numbers
import time  # to manage time and take breaks in the script

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
def download_pdf(pdf_link):
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

    else:
        print(f"Error downloading the PDF. Response code: {response.status_code}")

    # Close the new window or tab
    driver.close()

    # Switch back to the main window
    driver.switch_to.window(windows[0])
    
    
    
#----------------------------------------------------------------
# 2. 
#----------------------------------------------------------------