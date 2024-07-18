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


################################################################################################
# Section 2. Create horizon datasets
################################################################################################


#+++++++++++++++
# LIBRARIES
#+++++++++++++++

import tkinter as tk
import pandas as pd
import re

# Window fo h_initial

import tkinter as tk
from tkinter import simpledialog


# Show h_initial window to ask for user to enter a number
#________________________________________________________________
def show_h_initial_window():
    root = tk.Tk()
    root.withdraw()  # Hide the root window
    h_initial = simpledialog.askinteger("Input", "Please enter an initial horizon value (h_initial):",
                                        minvalue=1)  # Only accept positive integers
    root.destroy()
    return h_initial

# Show start_row window to ask for user to enter a number
#________________________________________________________________
def show_start_row_window():
    root = tk.Tk()
    root.withdraw()  # Hide the root window
    start_row = simpledialog.askinteger("Input", "Please enter an initial horizon value (h_initial):",
                                        minvalue=0)  # Only accept positive integers
    root.destroy()
    return start_row

# Setting main horizon rows on growth rates datasets
#________________________________________________________________
def replace_horizon(df, start_row, h_initial, h_counter):
    # Cast DataFrame to object dtype to allow storing strings
    df = df.astype(object)

    def replace_row(row, last_non_nan_indices, h_initial):
        new_row = row.copy()  # Create a copy of the current row
        last_non_nan_index = row.last_valid_index()  # Get the index of the last non-NaN value
        h = h_initial  # Initialize the horizon counter

        if last_non_nan_indices:
            if last_non_nan_indices[-1] != last_non_nan_index:
                new_row[last_non_nan_index] = "t+1"  # Set the last non-NaN value to "t+1"
                h = 1  # Reset horizon counter
            else:
                h += h_counter  # Increment horizon counter
        else:
            new_row[last_non_nan_index] = "t+1"  # Set the last non-NaN value to "t+1" if no previous indices

        for i in range(len(row) - 1, -1, -1):
            if pd.notnull(row.iloc[i]) and (not last_non_nan_indices or last_non_nan_indices[-1] != last_non_nan_index):
                new_row.iloc[i] = f"t+{h}"  # Replace value with horizon string
                h += h_counter  # Increment horizon counter by h_counter

        last_non_nan_indices.append(last_non_nan_index)  # Store the last non-NaN index
        return new_row

    first_part = df.iloc[:start_row]  # Get the first part of the DataFrame up to start_row
    last_non_nan_indices = []  # Initialize the list to store indices of last non-NaN values
    second_part = df.iloc[start_row:].apply(lambda x: replace_row(x, last_non_nan_indices, h_initial), axis=1)  # Apply replace_row to the second part
    return pd.concat([first_part, second_part])  # Concatenate the first and second parts and return the result

# Converting columns to string type
#________________________________________________________________
def columns_str(df):
    # Aplicar la conversión a partir de la cuarta columna del dataframe
    return df.apply(lambda x: x if x.name in df.columns[:3] else x.map(lambda y: str(y) if pd.notnull(y) else ''))

# Filling the remaining rows with horizon 't+h' values 
#________________________________________________________________
def replace_horizon_1(df):
    df = df.copy()
    df['date'] = pd.to_datetime(df['date'])
    columns = df.columns[3:]

    for col in columns:
        base_t = None

        for i in range(len(df)):
            current_value = df.at[i, col]

            if pd.isna(current_value) or str(current_value) == '' or re.match(r't\+\d+', str(current_value)):
                if re.match(r't\+\d+', str(current_value)):
                    base_t = int(current_value.split('+')[1])
                continue

            if base_t is not None:
                prev_date = df.at[i-1, 'date']
                current_date = df.at[i, 'date']
                month_diff = (current_date.year - prev_date.year) * 12 + (current_date.month - prev_date.month)
                base_t += month_diff
            else:
                base_t = 0  # In case base_t was not set, we start with t+0 for the first replacement.

            if re.match(r'[-+]?\d+\.\d+', str(current_value)):
                df.at[i, col] = f't+{base_t}'

    return df


################################################################################################
# Section 3. Create horizon datasets
################################################################################################


#+++++++++++++++
# LIBRARIES
#+++++++++++++++


# 
#________________________________________________________________