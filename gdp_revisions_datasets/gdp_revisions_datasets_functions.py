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
# def show_h_initial_window():
#     root = tk.Tk()
#     root.withdraw()  # Hide the root window
#     h_initial = simpledialog.askinteger("Input", "Please enter an initial horizon value (h_initial):",
#                                         minvalue=1)  # Only accept positive integers
#     root.destroy()
#     return h_initial

# Show start_row window to ask for user to enter a number
#________________________________________________________________
# def show_start_row_window():
#     root = tk.Tk()
#     root.withdraw()  # Hide the root window
#     start_row = simpledialog.askinteger("Input", "Please enter a start row number (start_row):",
#                                         minvalue=0)  # Only accept positive integers
#     root.destroy()
#     return start_row

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
        last_valid_t = None
        last_valid_date = None

        for i in range(len(df)):
            current_value = df.at[i, col]

            if pd.isna(current_value) or str(current_value) == '' or re.match(r't\+\d+', str(current_value)):
                if re.match(r't\+\d+', str(current_value)):
                    base_t = int(current_value.split('+')[1])
                    last_valid_t = base_t
                    last_valid_date = df.at[i, 'date']
                continue

            if last_valid_t is not None:
                prev_date = last_valid_date if last_valid_date is not None else df.at[i-1, 'date']
                current_date = df.at[i, 'date']
                month_diff = (current_date.year - prev_date.year) * 12 + (current_date.month - prev_date.month)
                base_t = last_valid_t + month_diff
            else:
                base_t = 0  # In case base_t was not set, we start with t+0 for the first replacement.

            last_valid_t = base_t
            last_valid_date = df.at[i, 'date']

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
import pandas as pd

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

#  Keep dataframe for last dates by month (monthly vintages)
#________________________________________________________________
def create_vintages(df):
    # Check that the 'date' column exists and is of type datetime64[ns]
    if 'date' not in df.columns:
        raise ValueError("The DataFrame does not contain the 'date' column.")
    if not pd.api.types.is_datetime64_any_dtype(df['date']):
        raise TypeError("The 'date' column is not of type datetime64[ns].")

    # Keep the DataFrame only for the last dates of each month
    df = df.sort_values(by='date')
    df['year_month'] = df['date'].dt.to_period('M')
    last_dates = df.groupby('year_month')['date'].transform('max') == df['date']
    filtered_df = df[last_dates].drop(columns='year_month')

    # Create the 'vintages' column
    filtered_df['aux'] = 'ns_' + filtered_df['year'].astype(str) + 'm' + filtered_df['date'].dt.strftime('%m')

    # Drop the 'year', 'id_ns', and 'date' columns
    filtered_df = filtered_df.drop(columns=['year', 'id_ns', 'date'])

    # Transpose filtered_df
    filtered_df = filtered_df.set_index('aux').T # We will use 'vintages' as columns and keep the original columns as rows

    # Reset index to have a default integer index
    filtered_df.reset_index(inplace=True)
    filtered_df.rename(columns={'index': 'vintages_date'}, inplace=True)

    return filtered_df

#  Convert columns to float and round decimal values
#________________________________________________________________
def convert_to_float_and_round(df):
    for col in df.columns:
        if col != 'vintages_date':
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    float_cols = df.select_dtypes(include='float64').columns
    df[float_cols] = df[float_cols].round(1)
    
    return df


#  Clean up monthly dataset
#________________________________________________________________
def process_monthly(df):
    df['month'] = df['vintages_date'].str.split('_').str[0]
    df['year'] = df['vintages_date'].str.split('_').str[1]
    
    month_mapping = {
        'ene': '01', 'feb': '02', 'mar': '03', 'abr': '04',
        'may': '05', 'jun': '06', 'jul': '07', 'ago': '08',
        'sep': '09', 'oct': '10', 'nov': '11', 'dic': '12'
    }
    
    df['month'] = df['month'].map(month_mapping)
    df['vintages_date'] = df['year'] + '-' + df['month']
    df['vintages_date'] = pd.to_datetime(df['vintages_date'], format='%Y-%m')
    
    df.drop(['month', 'year'], axis=1, inplace=True)
    
    return convert_to_float_and_round(df)

def process_monthly_services(df):
    
    # Ensure 'vintages_date' is a string
    df['vintages_date'] = df['vintages_date'].astype(str)
    
    # Remove duplicate columns, keeping the last occurrence
    df = df.loc[:, ~df.columns.duplicated(keep='last')] # serives last and mining first

    # Remove the first 47 rows
    #df = df.iloc[47:].reset_index(drop=True)
    
    # Extract month and year from 'vintages_date'
    df['month'] = df['vintages_date'].str.split('_').str[0]
    df['year'] = df['vintages_date'].str.split('_').str[1]
    
    # Map Spanish month abbreviations to numerical format
    month_mapping = {
        'ene': '01', 'feb': '02', 'mar': '03', 'abr': '04',
        'may': '05', 'jun': '06', 'jul': '07', 'ago': '08',
        'sep': '09', 'oct': '10', 'nov': '11', 'dic': '12'
    }
    
    df['month'] = df['month'].map(month_mapping)
    
    # Combine year and month into a single date column
    df['vintages_date'] = df['year'] + '-' + df['month']
    df['vintages_date'] = pd.to_datetime(df['vintages_date'], format='%Y-%m')
    
    # Drop the temporary 'month' and 'year' columns
    df.drop(['month', 'year'], axis=1, inplace=True)
    
    # Convert columns to float and round as needed
    return convert_to_float_and_round(df)

#  Clean up quarterly dataset
#________________________________________________________________
def process_quarterly(df):
    df['year'] = df['vintages_date'].str.split('_').str[0]
    df['month'] = df['vintages_date'].str.split('_').str[1]
    
    month_mapping = {
        '1': '03', '2': '06', '3': '09', '4': '12'
    }
    
    df['month'] = df['month'].map(month_mapping)
    df['vintages_date'] = df['year'] + '-' + df['month']
    df['vintages_date'] = pd.to_datetime(df['vintages_date'], format='%Y-%m')
    
    df.drop(['month', 'year'], axis=1, inplace=True)
    
    return convert_to_float_and_round(df)

#  Clean up annual dataset
#________________________________________________________________
def process_annual(df):
    df['year'] = df['vintages_date'].str.split('_').str[1]
    df['vintages_date'] = pd.to_datetime(df['year'], format='%Y')
    
    df.drop(['year'], axis=1, inplace=True)
    
    return convert_to_float_and_round(df)

# Generate horizon dataset (vintages as observations)
#________________________________________________________________
def filter_df_by_indices(df, records):
    # Create a dictionary to store the filtered columns
    filtered_columns = {}

    # Iterate over the records dictionary
    for column, value_indices in records.items():
        # Get the indices to keep for the current column
        indices = list(value_indices.values())
        # Filter the DataFrame to keep only the rows with the specified indices for the current column
        filtered_values = df.loc[indices, column]
        # Add the filtered values to the dictionary, resetting the index
        filtered_columns[column] = filtered_values.reset_index(drop=True)

    # Create a new DataFrame from the filtered columns dictionary
    filtered_df = pd.DataFrame(filtered_columns)

    # Add the 'horizon' column
    max_index = len(filtered_df) - 1
    filtered_df.insert(0, 'horizon', [f't+{i}' for i in range(1, max_index + 2)])

    # Return the filtered DataFrame
    return filtered_df

#  Generate releases dataset (firs, second, ..., most recent)
#________________________________________________________________

def create_releases(df, sector):
    # Filter columns of type float
    float_columns = df.select_dtypes(include=[np.float64]).columns

    # Find the maximum number of non-NaN values in any row
    max_non_nan = df[float_columns].notna().sum(axis=1).max()

    # Create a new DataFrame to store the results
    result_df = df[['vintages_date']].copy()

    for i in range(max_non_nan):
        # Create column name for the new column
        col_name = f'{sector}_release_{i+1}'
        
        # Extract the first, second, ..., nth non-NaN value in each row
        result_df[col_name] = df[float_columns].apply(
            lambda row: [x for x in row if not pd.isna(x)][i] if len([x for x in row if not pd.isna(x)]) > i else np.nan,
            axis=1
        )
    
    # Add column with the most recent value for each row
    result_df[f'{sector}_most_recent'] = df[float_columns].apply(
        lambda row: [x for x in row if not pd.isna(x)][-1] if len([x for x in row if not pd.isna(x)]) > 0 else np.nan,
        axis=1
    )
    
    return result_df



################################################################################################
# Section X. Base Year
################################################################################################

# Function to Replace Float Values with Base Year in DataFrame
#________________________________________________________________
def replace_floats_with_base_year(df_1, df_2):
    """
    Replace float values in df_2 with the corresponding base_year from df_1 based on common columns.
    
    Parameters:
    - df_1: DataFrame containing 'year', 'id_ns', 'date', and 'base_year' columns.
    - df_2: DataFrame where float values will be replaced with the base_year from df_1.
    
    Returns:
    - A DataFrame with float values replaced by base_year from df_1 where applicable.
    """
    
    # Ensure 'year' and 'id_ns' columns are of the same type
    df_1['year'] = df_1['year'].astype(str)
    df_1['id_ns'] = df_1['id_ns'].astype(str)
    df_2['year'] = df_2['year'].astype(str)
    df_2['id_ns'] = df_2['id_ns'].astype(str)
    
    # Merge the two dataframes on the common columns
    merged_df = pd.merge(df_2, df_1[['year', 'id_ns', 'date', 'base_year']], on=['year', 'id_ns', 'date'], how='left')
    
    # List of columns to exclude from replacement
    exclude_columns = ['year', 'id_ns', 'date', 'base_year']
    
    # Get the list of columns where replacements should be made
    columns_to_replace = [col for col in df_2.columns if col not in exclude_columns]
    
    # Replace all float values in the specified columns with base_year if they are not NaN
    for col in columns_to_replace:
        merged_df[col] = merged_df.apply(
            lambda row: row['base_year'] if pd.notnull(row[col]) and isinstance(row[col], float) else row[col], axis=1
        )
    
    # Drop the base_year column since it's no longer needed
    merged_df = merged_df.drop(columns=['base_year'])
    
    return merged_df

# Function to Create a Dictionary Mapping Columns to Base Year Indices (r)
#________________________________________________________________
def r_create_dic_base_year(df):
    """
    Create a dictionary where each key is a column name and each value is a set of indices.
    The indices correspond to the observations where the second unique value is observed,
    including subsequent values with the same 'year_month' as the first index of the second unique value.
    
    Parameters:
    - df: DataFrame containing columns for which the second unique value will be identified.
    
    Returns:
    - A dictionary mapping column names to sets of indices.
    """
    
    # Create the 'year_month' column
    df['year_month'] = df['date'].dt.to_period('M')
    
    # Exclude 'year', 'id_ns', 'date' from the columns to process
    exclude_columns = ['year', 'id_ns', 'date', 'year_month']
    columns_to_check = [col for col in df.columns if col not in exclude_columns]
    
    # Initialize the dictionary
    dic_base_year = {}
    
    # Iterate over the columns to check
    for col in columns_to_check:
        # Get the unique values in the column, excluding NaN
        unique_values = df[col].dropna().unique()
        
        # If there are at least two unique values
        if len(unique_values) >= 2:
            # Sort the unique values to find the second unique value
            unique_values.sort()
            second_unique_value = unique_values[1]
            
            # Find the first index of the second unique value
            initial_index = df.index[df[col] == second_unique_value][0]
            
            # Get the corresponding year_month value
            base_year_month = df.loc[initial_index, 'year_month']
            
            # Find all indices with the same 'year_month'
            matching_indices = df.index[(df[col] == second_unique_value) & (df['year_month'] == base_year_month)].tolist()
            
            # Add to the dictionary if indices are found
            if matching_indices:
                dic_base_year[col] = set(matching_indices)
    
    return dic_base_year

# Function to Create a Dictionary Mapping Columns to Base Year Indices
#________________________________________________________________
def create_dic_base_year(df):
    """
    Create a dictionary where each key is a column name and each value is a set of indices where the second unique 
    value (if available) in that column is observed. Columns with fewer than two unique values are excluded.
    
    Parameters:
    - df: DataFrame containing columns for which the second unique value will be identified.
    
    Returns:
    - A dictionary mapping column names to sets of indices where the second unique value is observed.
    """
    
    # Exclude 'year', 'id_ns', 'date' from the columns to process
    exclude_columns = ['year', 'id_ns', 'date']
    columns_to_check = [col for col in df.columns if col not in exclude_columns]
    
    # Initialize the dictionary
    dic_base_year = {}
    
    # Iterate over the columns to check
    for col in columns_to_check:
        # Get the unique values in the column, excluding NaN
        unique_values = df[col].dropna().unique()
        
        # If there are at least two unique values
        if len(unique_values) >= 2:
            # Sort the unique values to find the second unique value
            unique_values.sort()
            second_unique_value = unique_values[1]
            
            # Find the indices of the observations that contain the second unique value
            indices = df.index[df[col] == second_unique_value].tolist()
            
            # Add to the dictionary if indices are found
            if indices:
                dic_base_year[col] = set(indices)
    
    return dic_base_year

# Function to Remove Observations Affected by Base Year Indices
#________________________________________________________________
def remove_base_year_affected_obs(dic_base_year, df):
    """
    Remove observations from the DataFrame that are affected by the base year indices specified in the dictionary.
    Values at the specified indices in each column are replaced with NaN if they are not already NaN.
    
    Parameters:
    - dic_base_year: Dictionary where each key is a column name and each value is a set of indices to be updated.
    - df: DataFrame from which the specified observations will be removed.
    
    Returns:
    - A DataFrame with specified observations replaced by NaN.
    """
    
    # Iterate over the dictionary
    for col, indices in dic_base_year.items():
        # Check if the column exists in the dataframe
        if col in df.columns:
            # Replace the values at the specified indices with NaN if they are not already NaN
            df.loc[list(indices), col] = df.loc[list(indices), col].apply(lambda x: np.nan if pd.notnull(x) else x)
    
    return df

# Function to Replace Observations with Base Year Dummies
#________________________________________________________________
def replace_base_year_with_dummies(dic_base_year, df):
    """
    Replace observations from the DataFrame that are affected by the base year indices specified in the dictionary.
    Values at the specified indices in each column are replaced with 1 if they meet the criteria,
    and with 0 if they don't meet the criteria and are not already NaN.
    
    Parameters:
    - dic_base_year: Dictionary where each key is a column name and each value is a set of indices to be updated.
    - df: DataFrame from which the specified observations will be updated.
    
    Returns:
    - A DataFrame with specified observations replaced by 1 or 0.
    """
    
    # Iterate over the dictionary
    for col, indices in dic_base_year.items():
        # Check if the column exists in the dataframe
        if col in df.columns:
            # Iterate over each index in the DataFrame
            for i in range(len(df)):
                value = df.loc[i, col]
                # If the index is in the dic_base_year indices, replace the value with 1
                if i in indices:
                    df.loc[i, col] = 1
                # If the value is of type string and starts with 't+' (e.g., t+1, t+2) and is not in the indices, replace with 0
                elif isinstance(value, str) and value.startswith('t+'):
                    df.loc[i, col] = 0

    # Final pass to replace all 't+\d' values not handled previously
    for col in df.columns:
        for i in range(len(df)):
            value = df.loc[i, col]
            if isinstance(value, str) and vuale.startswith('t+'):
                df.loc[i, col] = 0

    return df


# Function to replace 't+\d' with dummies
#________________________________________________________________
def replace_strings_with_dummies(df_1, df_2):
    """
    Matches rows from df_1 to df_2 based on 'year' and 'id_ns' columns and replaces 't+\\d' values 
    in rows of df_2 with either 1 or 0 based on the match criteria, checking if the row below has 
    matching 't+\\d' values in corresponding columns.

    Args:
        df_1 (pd.DataFrame): DataFrame containing 'year' and 'id_ns' columns.
        df_2 (pd.DataFrame): DataFrame containing 'year' and 'id_ns' columns along with other string columns.

    Returns:
        pd.DataFrame: Modified df_2 with 't+\\d' values replaced by 1 or 0 based on the conditions.
    """
    
    # Ensure 'year' and 'id_ns' columns are of the same type
    df_1['year'] = df_1['year'].astype(str)
    df_1['id_ns'] = df_1['id_ns'].astype(str)
    df_2['year'] = df_2['year'].astype(str)
    df_2['id_ns'] = df_2['id_ns'].astype(str)
    
    # Create a set of (year, id_ns) pairs from df_1 for fast lookup
    df_1_pairs = set(zip(df_1['year'], df_1['id_ns']))
    
    # Add a boolean column to df_2 to indicate if the (year, id_ns) pair exists in df_1
    df_2['matched'] = df_2[['year', 'id_ns']].apply(lambda row: (row['year'], row['id_ns']) in df_1_pairs, axis=1)

    # Define a regex pattern to identify 't+<number>' strings
    pattern = re.compile(r't\+\d+')

    # Process only the rows in df_2 where 'matched' is True
    matched_indices = df_2.index[df_2['matched']].tolist()

    # Loop through matched indices and apply the exact logic to replace strings with 1 or 0
    for i in matched_indices:
        # Identify 't+<number>' columns in the row
        columns_to_check = [col for col in df_2.columns if isinstance(df_2.at[i, col], str) and pattern.match(df_2.at[i, col])]
        
        # Check if the next row has the same values in these columns
        if i + 1 < len(df_2) and all(df_2.at[i + 1, col] == df_2.at[i, col] for col in columns_to_check):
            # Replace both the identified row and the next row with 1 for the matched 't+<number>' columns
            for col in columns_to_check:
                df_2.at[i, col] = 1
                df_2.at[i + 1, col] = 1
        else:
            # Replace only the identified row with 1 and the next row's matched columns with 0
            for col in columns_to_check:
                df_2.at[i, col] = 1
                if i + 1 < len(df_2) and pd.notna(df_2.at[i + 1, col]):
                    df_2.at[i + 1, col] = 0

    # Replace any remaining 't+<number>' values with 0 in df_2 for columns to check
    for col in df_2.columns:
        if df_2[col].dtype == object:  # Ensures we only check string columns
            df_2[col] = df_2[col].apply(lambda x: 0 if isinstance(x, str) and pattern.match(x) else x)

    # Drop the auxiliary column 'matched'
    df_2.drop(columns=['matched'], inplace=True)
    
    return df_2

# Function to convert columns to integer type
#________________________________________________________________
def convert_columns_to_float(df):
    columns_to_exclude = ['year', 'id_ns', 'date']
    # Reemplazamos valores vacíos '' por NaN antes de la conversión
    df.replace('', np.nan, inplace=True)
    
    for col in df.columns:
        if col not in columns_to_exclude:
            # Convertimos la columna a float con pd.to_numeric
            df[col] = pd.to_numeric(df[col], errors='coerce').astype(float)
    
    return df
