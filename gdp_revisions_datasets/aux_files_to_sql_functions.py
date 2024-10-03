#*********************************************************************************************
#*********************************************************************************************
# Functions for aux_files_to_sql.ipynb 
#*********************************************************************************************
#*********************************************************************************************



#+++++++++++++++
# LIBRARIES
#+++++++++++++++

import pandas as pd
import os
from sqlalchemy import create_engine
import re



################################################################################################
# Section 7. Create cumulative revisions
################################################################################################

# Function to calculate cumulative revisions in a DataFrame based on release versions.
#________________________________________________________________
def calculate_cumulative_revisions(df):
    # Step 1: Identify all release numbers from the column names
    release_numbers = []
    
    for col in df.columns:
        # Search for columns that follow the pattern '_release_' and extract the number
        match = re.search(r'_release_(\d+)', col)
        if match:
            release_numbers.append(int(match.group(1)))
    
    # Step 2: Determine the maximum release number
    max_release = max(release_numbers) if release_numbers else 0
    
    # Step 3: Extract the base variable names by removing the suffixes '_release_' and '_most_recent'
    variable_names = [col.replace(f'_release_{i}', '').replace('_most_recent', '') 
                      for i in range(1, max_release + 1)
                      for col in df.columns if f'_release_{i}' in col or '_most_recent' in col]
    
    # Step 4: Remove any duplicate variable names
    variable_names = list(set(variable_names))
    
    # Step 5: Prepare to create new columns for cumulative revisions
    new_columns = []
    
    # Step 6: Create revision variables for each identified variable, comparing releases
    for variable in variable_names:
        # Iterate through the release versions, excluding the last one
        for i in range(1, max_release + 1):
            release_col = f"{variable}_release_{i}"  # Example: var_release_1
            most_recent_col = f"{variable}_most_recent"  # Example: var_most_recent
            if release_col in df.columns and most_recent_col in df.columns:
                new_column_name = f"r_{i}_{variable}"  # Naming the new revision column
                # Subtract release value from the most recent value
                new_columns.append((new_column_name, df[most_recent_col] - df[release_col]))
    
    # Step 7: Add all newly created revision columns to the DataFrame
    if new_columns:
        df_new_columns = pd.DataFrame(dict(new_columns))
        df = pd.concat([df, df_new_columns], axis=1)

    # Step 8: Return the modified DataFrame along with the maximum release number
    return df, max_release


# Function to extract year or year_month based on the specified frequency
#________________________________________________________________
def destring_date_column(df, frequency):
    # Step 1: Check if 'vintages_date' column exists in the DataFrame
    if 'vintages_date' not in df.columns:
        raise KeyError("'vintages_date' column not found in the dataframe.")
    
    # Step 2: Convert 'vintages_date' column to datetime, handling errors by converting invalid dates to NaT
    df['vintages_date'] = pd.to_datetime(df['vintages_date'], errors='coerce')
    
    # Step 3: Determine which column to create based on the provided frequency argument
    if frequency == 'monthly' or frequency == 'quarterly':
        # Step 3a: If monthly or quarterly, extract the year and month as 'year_month'
        year_month_column = pd.DataFrame({'year_month': df['vintages_date'].dt.to_period('M').astype(str)})
        # Concatenate the new column to the original DataFrame
        df = pd.concat([df, year_month_column], axis=1)
    
    elif frequency == 'annual':
        # Step 3b: If annual, extract only the year as 'year'
        year_column = pd.DataFrame({'year': df['vintages_date'].dt.year})
        # Concatenate the new column to the original DataFrame
        df = pd.concat([df, year_column], axis=1)
    
    else:
        # Step 4: Raise an error if the frequency argument is invalid
        raise ValueError("Invalid frequency. Please choose 'annual', 'quarterly', or 'monthly'.")
    
    # Step 5: Return the modified DataFrame with the new column
    return df


# Function to retain only 'vintages_date' and columns that start with 'r_' for revisions
#________________________________________________________________
def keep_revisions(df, frequency):
    # Step 1: Determine which columns to keep based on the frequency
    if frequency in ['quarterly', 'monthly']:
        # Step 1a: If frequency is quarterly or monthly, keep 'year_month' and all columns starting with 'r_'
        cols_to_keep = ['year_month'] + [col for col in df.columns if col.startswith('r_')]
    
    elif frequency == 'annual':
        # Step 1b: If frequency is annual, keep 'year' and all columns starting with 'r_'
        cols_to_keep = ['year'] + [col for col in df.columns if col.startswith('r_')]
    
    else:
        # Step 2: Raise an error if the frequency argument is invalid
        raise ValueError("Invalid frequency value. Please use 'quarterly', 'monthly', or 'annual'.")
    
    # Step 3: Return the DataFrame with only the selected columns
    return df[cols_to_keep]


# Function to transpose data by sector based on the given frequency
#________________________________________________________________
def transpose_df(df, frequency):
    # Step 1: Determine whether to use 'year_month' or 'year' based on the frequency
    if frequency in ['monthly', 'quarterly']:
        time_col = 'year_month'
    elif frequency == 'annual':
        time_col = 'year'
    else:
        raise ValueError("frequency must be 'monthly', 'quarterly', or 'annual'")
    
    # Step 2: Extract sector categories (e.g., 'gdp', 'services', etc.)
    sectores = sorted(set(col.split('_')[2] for col in df.columns if col.startswith('r_')))
    
    # Step 3: Create a dictionary to store the transposed data
    data_transpuesta = {}
    
    # Step 4: For each sector, gather all columns related to that sector
    for sector in sectores:
        # Step 4a: Filter the columns corresponding to the current sector
        cols_sector = [col for col in df.columns if f'_{sector}' in col]
        
        # Step 4b: Transpose the rows into columns, using 'year_month' or 'year' for the column names
        for i, row in df.iterrows():
            time_value = row[time_col]  # Get the value of 'year_month' or 'year'
            
            # Convert 'time_value' to an integer if the frequency is annual
            if frequency == 'annual':
                time_value = int(time_value)
            
            sector_data = row[cols_sector]  # Get the data for the current sector
            
            # Step 4c: Create dynamic column names using the format "{sector}_{time_value}"
            for col, value in zip(cols_sector, sector_data):
                new_col_name = f"{sector}_{time_value}"
                
                # If the column does not exist in the dictionary, initialize it with NaN
                if new_col_name not in data_transpuesta:
                    data_transpuesta[new_col_name] = [value]
                else:
                    data_transpuesta[new_col_name].append(value)
    
    # Step 5: Ensure that all lists in the dictionary have the same length
    max_len = max(len(v) for v in data_transpuesta.values())
    for key in data_transpuesta:
        if len(data_transpuesta[key]) < max_len:
            # Fill with NaN to match the maximum length
            data_transpuesta[key] += [np.nan] * (max_len - len(data_transpuesta[key]))
    
    # Step 6: Convert the dictionary into a DataFrame
    df_transpuesto = pd.DataFrame(data_transpuesta)
    
    # Step 7: Reset the index and return the transposed DataFrame
    return df_transpuesto.reset_index(drop=True)



################################################################################################
# Section 8. Create intermediate revisions
################################################################################################


# Function to calculate intermediate revisions between data releases
#________________________________________________________________
def calculate_intermediate_revisions(df):
    # Step 1: Find the highest number following the pattern '_release_'
    release_numbers = []
    
    # Step 1a: Loop through the columns to identify release numbers
    for col in df.columns:
        match = re.search(r'_release_(\d+)', col)  # Search for release numbers in the column names
        if match:
            release_numbers.append(int(match.group(1)))  # Store the release numbers as integers
    
    # Step 2: Determine the maximum release number
    max_release = max(release_numbers) if release_numbers else 0  # Get the highest release number
    
    # Step 3: Extract the names of variables without the '_release_' or '_most_recent' suffix
    variable_names = [col.replace(f'_release_{i}', '').replace('_most_recent', '') 
                      for i in range(1, max_release + 1)
                      for col in df.columns if f'_release_{i}' in col or '_most_recent' in col]
    
    # Step 3a: Remove duplicates from the list of variable names
    variable_names = list(set(variable_names))
    
    # Step 4: Create a list to store the new columns for intermediate revisions
    new_columns = []
    
    # Step 5: Create new variables for each intermediate revision
    for variable in variable_names:
        # Step 5a: Loop through releases from release_2 to the maximum release
        for i in range(2, max_release + 1):
            previous_release_col = f"{variable}_release_{i-1}"  # Previous release
            current_release_col = f"{variable}_release_{i}"      # Current release
            
            # Step 5b: Check if both the previous and current releases exist in the DataFrame
            if previous_release_col in df.columns and current_release_col in df.columns:
                new_column_name = f"r_{i-1}_{i}_{variable}"  # Create new column name for the difference
                # Calculate the difference between the current release and the previous release
                new_columns.append((new_column_name, df[current_release_col] - df[previous_release_col]))
        
        # Step 6: For the last revision, calculate the difference between the most recent release and the last release
        most_recent_col = f"{variable}_most_recent"
        last_release_col = f"{variable}_release_{max_release}"
        
        # Step 6a: Check if both the last release and most recent columns exist
        if last_release_col in df.columns and most_recent_col in df.columns:
            new_column_name = f"r_{max_release}_{most_recent_col.replace(variable + '_', '')}"
            # Calculate the difference between most recent and the last release
            new_columns.append((new_column_name, df[most_recent_col] - df[last_release_col]))
    
    # Step 7: Concatenate all new columns into the DataFrame
    if new_columns:
        df_new_columns = pd.DataFrame(dict(new_columns))  # Convert the list of new columns into a DataFrame
        df = pd.concat([df, df_new_columns], axis=1)      # Concatenate the new columns to the original DataFrame

    # Step 8: Return the modified DataFrame and the maximum release value
    return df, max_release

