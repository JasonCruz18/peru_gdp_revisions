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
    start_row = simpledialog.askinteger("Input", "Please enter a start row number (start_row):",
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
# Section 3. Create intermediate revisions datasets
################################################################################################


#+++++++++++++++
# LIBRARIES
#+++++++++++++++

import numpy as np

# Getting last row index for t+h value for each column
#________________________________________________________________
def get_last_index_h(df):
    # Create a dictionary to store the records
    records = {}

    # Iterate over each column in the DataFrame, excluding the 'year', 'date', and 'id_ns' columns
    for column in df.columns.drop(['year', 'date', 'id_ns']):
        # Create a dictionary to store the row indices of each 't+\d' value in the current column
        column_records = {}
        # Iterate over each unique value in the current column, excluding NaN
        for value in df[column].dropna().unique():
            # Use regular expressions to find values that contain 't+\d'
            if re.search(r't\+\d', value):
                # Find the index of the last occurrence of a value containing 't+\d' in the column
                last_index = df[df[column] == value].index.max()
                # Add the value and its index to the column records dictionary
                column_records[value] = last_index
        # Add the column records dictionary to the main dictionary
        records[column] = column_records

    # Return the records
    return records

#  Computting intermediate revisions
#________________________________________________________________
def computing_inter_revisions(df, records):
    # Extract columns from df, excluding 'year', 'date', and 'id_ns'
    columns = df.columns.drop(['year', 'date', 'id_ns'])
    
    # Get the maximum value of h to determine the number of rows in the revisions DataFrame
    max_h = max([int(value.split('+')[1]) for column in records.values() for value in column.keys()])
    num_rows = max_h - 1
    
    # Create an empty DataFrame to store the intermediate revisions
    intermediate_revisions = pd.DataFrame(columns=columns, index=range(num_rows))
    
    # Iterate over each value of h
    for h in range(2, max_h + 1):
        # Calculate the column name in the new DataFrame
        revision_column = f"t+{h} - t+1"
        
        # Iterate over each column in df
        for column in columns:
            # Get the indices corresponding to t+h and t+1
            index_h = float(records[column].get(f"t+{h}", float('nan')))
            index_t1 = float(records[column].get(f"t+1", float('nan')))

            # Check if the indices are valid
            if np.isnan(index_h) or np.isnan(index_t1):
                # If either index is NaN, assign NaN to the result
                result = np.nan
            else:
                # Perform the subtraction and store the result in the corresponding column
                result = df.at[int(index_h), column] - df.at[int(index_t1), column]
                # Save the result in the corresponding row of intermediate_revisions
                intermediate_revisions.at[h - 2, column] = result
    
    return intermediate_revisions

#  Transpose intermediate revisions dataset
#________________________________________________________________
def transpose_inter_revisions(intermediate_revisions):
    # Transpose the DataFrame
    transposed_revisions = intermediate_revisions.T
    
    # Set the name of the first column as 'intermediate_revision_date'
    transposed_revisions.columns.name = 'intermediate_revision_date'
    
    # Rename the columns
    transposed_revisions.columns = [f'{sector}_revision_{i+1}' for i in range(len(transposed_revisions.columns))]
    
    # Reset the index
    transposed_revisions = transposed_revisions.reset_index()
    
    # Rename the index column
    transposed_revisions = transposed_revisions.rename(columns={'index': 'inter_revision_date'})
    
    return transposed_revisions

#  Convert columns to float and round decimal values
#________________________________________________________________
def convert_to_float_and_round(df):
    for col in df.columns:
        if col != 'inter_revision_date':
            # Convert the column to float, forcing errors to NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    # Apply rounding to one decimal place only to columns that are now float
    float_cols = df.select_dtypes(include='float64').columns
    df[float_cols] = df[float_cols].round(1)
    
    return df