#*********************************************************************************************
# Functions for gdp_revisions_datasets 
#*********************************************************************************************

#----------------------------------------------------------------
# 1. PDF Downloader
#----------------------------------------------------------------

# Function to play the sound
def play_sound():
    pygame.mixer.music.play()
    
# Function to wait random seconds
def random_wait(min_time, max_time):
wait_time = random.uniform(min_time, max_time)
print(f"Waiting randomly for {wait_time:.2f} seconds")
time.sleep(wait_time)