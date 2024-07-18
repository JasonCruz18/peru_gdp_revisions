#*********************************************************************************************
#*********************************************************************************************
# Functions for gdp_inter_revisions_datasets.ipynb 
#*********************************************************************************************
#*********************************************************************************************



################################################################################################
# Section 1. Economic sector and data frequency selector
################################################################################################


#+++++++++++++++
# LIBRARIES
#+++++++++++++++


# 1.1. Economic sector

import tkinter as tk


# Function to show option window
#________________________________________________________________

def show_option_window():
    # Define the list of options
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

    # Function to save the selected option and close the window
    def save_option():
        global sector
        sector = selected_option.get()
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

    return selected_option.get()

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