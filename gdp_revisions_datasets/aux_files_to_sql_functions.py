#*********************************************************************************************
#*********************************************************************************************
# Functions for aux_files_to_sql.ipynb 
#*********************************************************************************************
#*********************************************************************************************



#+++++++++++++++
# LIBRARIES
#+++++++++++++++

import pandas as pd
import numpy as np
import os
from sqlalchemy import create_engine
import re


# Function to merge sectors dataframes 
#________________________________________________________________
def releases_datasets_merge(frequency, *dataframes):
    """
    Merges multiple dataframes on the 'vintages_date' column.

    Parameters:
    frequency (str): The frequency to be used in the resulting dataframe name.
    *dataframes (pd.DataFrame): DataFrames to be merged.

    Returns:
    pd.DataFrame: The merged dataframe with the name 'sectorial_gdp_{frequency}_releases'.
    """
    # Initialize the merged dataframe with the first dataframe
    merged_df = dataframes[0]

    # Merge each dataframe on 'vintages_date'
    for df in dataframes[1:]:
        merged_df = pd.merge(merged_df, df, on='vintages_date', how='outer')

    # Define the name of the resulting dataframe
    result_name = f'sectorial_gdp_{frequency}_releases'
    
    # Assign the name to the dataframe (this is for reference, actual DataFrame doesn't have a 'name' attribute)
    merged_df.name = result_name

    return merged_df

# Function to sort merged sectors dataframes by vintages_date 
#________________________________________________________________
def sort_by_date(df):
    """
    Sorts the DataFrame by the 'vintages_date' column in ascending order.

    Parameters:
    df (pd.DataFrame): Input DataFrame with a datetime64[ns] column named 'vintages_date'.

    Returns:
    pd.DataFrame: Sorted DataFrame.
    """
    if 'vintages_date' not in df.columns:
        raise ValueError("The DataFrame does not contain a 'vintages_date' column.")
    
    if not pd.api.types.is_datetime64_ns_dtype(df['vintages_date']):
        raise TypeError("'vintages_date' column is not of type datetime64[ns].")

    return df.sort_values(by='vintages_date').reset_index(drop=True)

# Function to convert releases to to data panel
#________________________________________________________________
def releases_convert_to_panel(df):
    # Obtener todas las columnas del dataframe
    columns = df.columns
    
    # Conjunto para almacenar los sectores únicos
    sectors = set()

    # Expresión regular para identificar sectores y sus releases
    pattern = re.compile(r'(.+?)_release_(\d+)')

    # Identificar todos los sectores a partir de las columnas
    for col in columns:
        match = pattern.match(col)
        if match:
            sectors.add(match.group(1))  # Extraer el nombre del sector
    
    # Inicializar el DataFrame resultante en formato panel
    df_panel = pd.DataFrame()

    # Para cada sector, transformar y fusionar los datos
    for sector in sectors:
        # Filtrar las columnas que pertenecen a este sector
        sector_columns = [col for col in columns if col.startswith(f'{sector}_release_')]
        
        # Convertir las columnas del sector al formato largo
        sector_melted = pd.melt(df, 
                                id_vars=['vintages_date'], 
                                value_vars=sector_columns, 
                                var_name='horizon', 
                                value_name=f'release_{sector}')
        
        # Extraer el número de revisión y eliminar el nombre del sector del campo 'horizon'
        sector_melted['horizon'] = sector_melted['horizon'].str.extract(r'release_(\d+)')[0].astype(int)

        # Si es el primer sector, inicializar df_panel
        if df_panel.empty:
            df_panel = sector_melted
        else:
            # Fusionar el sector actual con el panel general
            df_panel = pd.merge(df_panel, sector_melted, on=['vintages_date', 'horizon'], how='outer')

    return df_panel



################################################################################################
# r
################################################################################################

# Function to calculate revisions
#________________________________________________________________
def calculate_r(df):
    # Find the highest number following the pattern '_release_'
    release_numbers = []
    
    for col in df.columns:
        match = re.search(r'_release_(\d+)', col)
        if match:
            release_numbers.append(int(match.group(1)))
    
    # Determine the maximum number of releases
    max_release = max(release_numbers) if release_numbers else 0
    
    # Extract sector names with specified suffixes
    sector_names = [col.replace(f'_release_{i}', '').replace('_most_recent', '') 
                      for i in range(1, max_release + 1)
                      for col in df.columns if f'_release_{i}' in col or '_most_recent' in col]
    
    # Remove duplicate sector names
    sector_names = list(set(sector_names))
    
    # Create a list to store the new columns
    new_columns = []
    
    # Create intermediate revision columns for each sector found
    for sector in sector_names:
        for i in range(2, max_release + 1):  # Iterate from release_2 to the highest release + 1
            previous_release_col = f"{sector}_release_{i-1}"
            current_release_col = f"{sector}_release_{i}"
            
            if previous_release_col in df.columns and current_release_col in df.columns:
                new_column_name = f"r_{i}_{sector}"
                # Calculate the difference between the current and previous release
                new_columns.append((new_column_name, df[current_release_col] - df[previous_release_col]))
        
        # For the last difference, between most_recent and the latest release
        most_recent_col = f"{sector}_most_recent"
        last_release_col = f"{sector}_release_{max_release}"
        
        if last_release_col in df.columns and most_recent_col in df.columns:
            # Modify the column name
            new_column_name = f"r_{max_release + 1}_{sector}"
            new_columns.append((new_column_name, df[most_recent_col] - df[last_release_col]))
    
    # Concatenate all new columns to the DataFrame
    if new_columns:
        df_new_columns = pd.DataFrame(dict(new_columns))
        df = pd.concat([df, df_new_columns], axis=1)

    # Return the modified DataFrame
    return df, max_release

# Function to calculate revision dummies
#________________________________________________________________
def calculate_r_dummies(df):
    # Find the highest number following the pattern '_release_'
    release_numbers = []
    
    for col in df.columns:
        match = re.search(r'_release_(\d+)', col)
        if match:
            release_numbers.append(int(match.group(1)))
    
    # Determine the maximum number of releases
    max_release = max(release_numbers) if release_numbers else 0
    
    # Extract sector names with specified suffixes
    sector_names = [col.replace(f'_release_{i}', '').replace('_most_recent', '') 
                      for i in range(1, max_release + 1)
                      for col in df.columns if f'_release_{i}' in col or '_most_recent' in col]
    
    # Remove duplicate sector names
    sector_names = list(set(sector_names))
    
    # Create a list to store the new columns
    new_columns = []
    
    # Create intermediate revision columns for each sector found
    for sector in sector_names:
        for i in range(2, max_release + 1):  # Iterate from release_2 to the highest release
            previous_release_col = f"{sector}_release_{i-1}"
            current_release_col = f"{sector}_release_{i}"
            
            if previous_release_col in df.columns and current_release_col in df.columns:
                new_column_name = f"r_{i}_{sector}"
                new_columns.append((new_column_name, df[current_release_col]))
        
        # For the last difference, between most_recent and the latest release
        most_recent_col = f"{sector}_most_recent"
        last_release_col = f"{sector}_release_{max_release}"
        
        if last_release_col in df.columns and most_recent_col in df.columns:
            new_column_name = f"r_{max_release + 1}_{sector}"
            new_columns.append((new_column_name, df[most_recent_col]))
    
    # Concatenate all new columns to the DataFrame
    if new_columns:
        df_new_columns = pd.DataFrame(dict(new_columns))
        df = pd.concat([df, df_new_columns], axis=1)

    # Return the modified DataFrame
    return df, max_release


# Function to convert r to panel data
#________________________________________________________________
def r_to_panel(df):
    # Get all columns from the dataframe
    columns = df.columns
    
    # Set to store unique sectors
    sectors = set()

    # Regular expression for the pattern 'r_{i}_{sector}'
    pattern = re.compile(r'r_(\d+)_(.+)')

    # Identify all sectors from the columns
    for col in columns:
        match = pattern.search(col)
        if match:
            sectors.add(match.group(2))  # Extract the sector and add it to the set

    # Initialize the resulting DataFrame in panel format
    df_panel = pd.DataFrame()

    # For each sector, transform and merge the data
    for sector in sectors:
        # Filter columns that belong to this sector
        sector_columns = [col for col in columns if f'_{sector}' in col]
        
        # Convert the sector's columns to long format
        sector_melted = pd.melt(df, id_vars=['vintages_date'], 
                                value_vars=sector_columns, 
                                var_name='horizon', 
                                value_name=f'r_{sector}')
        
        # Extract the revision number and remove the sector name from the 'horizon' field
        sector_melted['horizon'] = sector_melted['horizon'].str.extract(r'r_(\d+)_')[0].astype(int)

        # If it's the first sector, initialize df_panel
        if df_panel.empty:
            df_panel = sector_melted
        else:
            # Merge the current sector with the general panel
            df_panel = pd.merge(df_panel, sector_melted, on=['vintages_date', 'horizon'], how='outer')

    return df_panel


# Function to convert r dummies to panel data
#________________________________________________________________
def r_dummies_to_panel(df):
    # Get all columns from the dataframe
    columns = df.columns
    
    # Set to store unique sectors
    sectors = set()

    # Regular expression for the pattern 'r_(\d+)_(.+)'
    pattern = re.compile(r'r_(\d+)_(.+)')

    # Identify all sectors from the columns
    for col in columns:
        match = pattern.search(col)
        if match:
            sectors.add(match.group(2))  # Extract the sector and add it to the set

    # Initialize the resulting DataFrame in panel format
    df_panel = pd.DataFrame()

    # For each sector, transform and merge the data
    for sector in sectors:
        # Filter columns that belong to this sector
        sector_columns = [col for col in columns if f'_{sector}' in col]
        
        # Convert the sector's columns to long format
        sector_melted = pd.melt(df, id_vars=['vintages_date'], 
                                value_vars=sector_columns, 
                                var_name='horizon', 
                                value_name=f'r_dummy_{sector}')
        
        # Extract the revision number and remove the sector name from the 'horizon' field
        sector_melted['horizon'] = sector_melted['horizon'].str.extract(r'r_(\d+)_')[0].astype(int)

        # If it's the first sector, initialize df_panel
        if df_panel.empty:
            df_panel = sector_melted
        else:
            # Merge the current sector with the general panel
            df_panel = pd.merge(df_panel, sector_melted, on=['vintages_date', 'horizon'], how='outer')

    return df_panel



################################################################################################
# e
################################################################################################

# Function to calculate errors
#________________________________________________________________
def calculate_e(df):
    # Step 1: Identify all release numbers from the column names
    release_numbers = []
    
    for col in df.columns:
        # Search for columns that follow the pattern '_release_' and extract the number
        match = re.search(r'_release_(\d+)', col)
        if match:
            release_numbers.append(int(match.group(1)))
    
    # Step 2: Determine the maximum release number
    max_release = max(release_numbers) if release_numbers else 0
    
    # Step 3: Extract the sector names by removing the suffixes '_release_' and '_most_recent'
    sector_names = [col.replace(f'_release_{i}', '').replace('_most_recent', '') 
                      for i in range(1, max_release + 1)
                      for col in df.columns if f'_release_{i}' in col or '_most_recent' in col]
    
    # Step 4: Remove any duplicate sector names
    sector_names = list(set(sector_names))
    
    # Step 5: Prepare to create new columns for nowcast errors
    new_columns = []
    
    # Step 6: Create errors from releases for each identified sector 
    for sector in sector_names:
        # Iterate through the horizon
        for i in range(1, max_release + 1):
            release_col = f"{sector}_release_{i}"  # Example: sector_release_1
            most_recent_col = f"{sector}_most_recent"  # Example: sector_most_recent
            if release_col in df.columns and most_recent_col in df.columns:
                new_column_name = f"e_{i}_{sector}"  # Naming the new revision column
                # Compute revisions
                new_columns.append((new_column_name, df[most_recent_col] - df[release_col]))
    
    # Step 7: Add all newly created e columns to the DataFrame
    if new_columns:
        df_new_columns = pd.DataFrame(dict(new_columns))
        df = pd.concat([df, df_new_columns], axis=1)

    # Step 8: Return the modified DataFrame along with the maximum release number
    return df, max_release

# Function to calculate errors dummies
#________________________________________________________________
def calculate_e_dummies(df):
    # Step 1: Identify all release numbers from the column names
    release_numbers = []
    
    for col in df.columns:
        # Search for columns that follow the pattern '_release_' and extract the number
        match = re.search(r'_release_(\d+)', col)
        if match:
            release_numbers.append(int(match.group(1)))
    
    # Step 2: Determine the maximum release number
    max_release = max(release_numbers) if release_numbers else 0
    
    # Step 3: Extract the sector names by removing the suffixes '_release_' and '_most_recent'
    sector_names = [col.replace(f'_release_{i}', '').replace('_most_recent', '') 
                      for i in range(1, max_release + 1)
                      for col in df.columns if f'_release_{i}' in col or '_most_recent' in col]
    
    # Step 4: Remove any duplicate sector names
    sector_names = list(set(sector_names))
    
    # Step 5: Prepare to create new columns for nowcast errors
    new_columns = []
    
    # Step 6: Create errors (e) from releases for each identified sector 
    for sector in sector_names:
        # Iterate through the horizon
        for i in range(1, max_release + 1):
            release_col = f"{sector}_release_{i}"  # Example: sector_release_1
            most_recent_col = f"{sector}_most_recent"  # Example: sector_most_recent
            if release_col in df.columns and most_recent_col in df.columns:
                new_column_name = f"e_{i}_{sector}"  # Naming the new revision column
                # Subtract release affected by base year
                new_columns.append((new_column_name, df[release_col])) # in e_t(h) = y_t - y_t(h), y_t(h) is affected by base year 
                
    # Step 7: Add all newly created e columns to the DataFrame
    if new_columns:
        df_new_columns = pd.DataFrame(dict(new_columns))
        df = pd.concat([df, df_new_columns], axis=1)

    # Step 8: Return the modified DataFrame along with the maximum release number
    return df, max_release

# Function to convert e to data panel
#________________________________________________________________
def e_to_panel(df):
    # Get all columns from the dataframe
    columns = df.columns
    
    # Set to store unique sectors
    sectors = set()

    # Regular expression for the pattern 'e_{i}_{sector}'
    pattern = re.compile(r'e_(\d+)_(.+)')

    # Identify all sectors from the columns
    for col in columns:
        match = pattern.search(col)
        if match:
            sectors.add(match.group(2))  # Extract the sector and add it to the set

    # Initialize the resulting DataFrame in panel format
    df_panel = pd.DataFrame()

    # For each sector, transform and merge the data
    for sector in sectors:
        # Filter columns that belong to this sector
        sector_columns = [col for col in columns if f'_{sector}' in col]
        
        # Convert the sector's columns to long format
        sector_melted = pd.melt(df, id_vars=['vintages_date'], 
                                value_vars=sector_columns, 
                                var_name='horizon', 
                                value_name=f'e_{sector}')
        
        # Extract the revision number and remove the sector name from the 'horizon' field
        sector_melted['horizon'] = sector_melted['horizon'].str.extract(r'e_(\d+)_')[0].astype(int)

        # If it's the first sector, initialize df_panel
        if df_panel.empty:
            df_panel = sector_melted
        else:
            # Merge the current sector with the general panel
            df_panel = pd.merge(df_panel, sector_melted, on=['vintages_date', 'horizon'], how='outer')

    return df_panel

# Function to convert e dummies to data panel
#________________________________________________________________
def e_dummies_to_panel(df):
    # Get all columns from the dataframe
    columns = df.columns
    
    # Set to store unique sectors
    sectors = set()

    # Regular expression for the pattern 'e_{i}_{sector}'
    pattern = re.compile(r'e_(\d+)_(.+)')

    # Identify all sectors from the columns
    for col in columns:
        match = pattern.search(col)
        if match:
            sectors.add(match.group(2))  # Extract the sector and add it to the set

    # Initialize the resulting DataFrame in panel format
    df_panel = pd.DataFrame()

    # For each sector, transform and merge the data
    for sector in sectors:
        # Filter columns that belong to this sector
        sector_columns = [col for col in columns if f'_{sector}' in col]
        
        # Convert the sector's columns to long format
        sector_melted = pd.melt(df, id_vars=['vintages_date'], 
                                value_vars=sector_columns, 
                                var_name='horizon', 
                                value_name=f'e_dummy_{sector}')
        
        # Extract the revision number and remove the sector name from the 'horizon' field
        sector_melted['horizon'] = sector_melted['horizon'].str.extract(r'e_(\d+)_')[0].astype(int)

        # If it's the first sector, initialize df_panel
        if df_panel.empty:
            df_panel = sector_melted
        else:
            # Merge the current sector with the general panel
            df_panel = pd.merge(df_panel, sector_melted, on=['vintages_date', 'horizon'], how='outer')

    return df_panel



################################################################################################
# z
################################################################################################

# Function to calculate cumulative revisions up to h
#________________________________________________________________
def calculate_z(df):
    # Step 1: Identify all release numbers from the column names
    release_numbers = []
    
    for col in df.columns:
        # Search for columns that follow the pattern '_release_' and extract the number
        match = re.search(r'_release_(\d+)', col)
        if match:
            release_numbers.append(int(match.group(1)))
    
    # Step 2: Determine the maximum release number
    max_release = max(release_numbers) if release_numbers else 0
    
    # Step 3: Extract the sector names by removing the suffixes '_release_' and '_most_recent'
    sector_names = [col.replace(f'_release_{i}', '').replace('_most_recent', '') 
                      for i in range(1, max_release + 1)
                      for col in df.columns if f'_release_{i}' in col or '_most_recent' in col]
    
    # Step 4: Remove any duplicate sector names
    sector_names = list(set(sector_names))
    
    # Step 5: Prepare to create new columns for nowcast errors
    new_columns = []
    
    # Step 6: Create cumulative revisions up to h from releases for each identified sector 
    for sector in sector_names:
        # Iterate through the horizon
        for i in range(2, max_release + 1): # first "z" starts from horizon 2
            release_col = f"{sector}_release_{i}"  # Example: sector_release_2
            first_release_col = f"{sector}_release_1"  # Example: sector_release_1
            if release_col in df.columns and first_release_col in df.columns:
                new_column_name = f"z_{i}_{sector}"  # Naming the new revision column
                # Compute revisions
                new_columns.append((new_column_name, df[release_col] - df[first_release_col]))
    
    # Step 7: Add all newly created e columns to the DataFrame
    if new_columns:
        df_new_columns = pd.DataFrame(dict(new_columns))
        df = pd.concat([df, df_new_columns], axis=1)

    # Step 8: Return the modified DataFrame along with the maximum release number
    return df, max_release

# Function to calculate dummies cumulative revisions up to h 
#________________________________________________________________
def calculate_z_dummies(df):
    # Step 1: Identify all release numbers from the column names
    release_numbers = []
    
    for col in df.columns:
        # Search for columns that follow the pattern '_release_' and extract the number
        match = re.search(r'_release_(\d+)', col)
        if match:
            release_numbers.append(int(match.group(1)))
    
    # Step 2: Determine the maximum release number
    max_release = max(release_numbers) if release_numbers else 0
    
    # Step 3: Extract the sector names by removing the suffixes '_release_' and '_most_recent'
    sector_names = [col.replace(f'_release_{i}', '').replace('_most_recent', '') 
                      for i in range(1, max_release + 1)
                      for col in df.columns if f'_release_{i}' in col or '_most_recent' in col]
    
    # Step 4: Remove any duplicate sector names
    sector_names = list(set(sector_names))
    
    # Step 5: Prepare to create new columns for nowcast errors
    new_columns = []
    
    # Step 6: Create cumulative revisions up to h from releases for each identified sector 
    for sector in sector_names:
        # Iterate through the horizon
        for i in range(2, max_release + 1): # first "z" starts from horizon 2
            release_col = f"{sector}_release_{i}"  # Example: sector_release_2
            first_release_col = f"{sector}_release_1"  # Example: _release_1
            if release_col in df.columns and first_release_col in df.columns:
                new_column_name = f"z_{i}_{sector}"  # Naming the new revision column
                # Compute revisions
                new_columns.append((new_column_name, df[release_col]))
    
    # Step 7: Add all newly created e columns to the DataFrame
    if new_columns:
        df_new_columns = pd.DataFrame(dict(new_columns))
        df = pd.concat([df, df_new_columns], axis=1)

    # Step 8: Return the modified DataFrame along with the maximum release number
    return df, max_release

# Function to convert z to data panel
#________________________________________________________________
def z_to_panel(df):
    # Get all columns from the dataframe
    columns = df.columns
    
    # Set to store unique sectors
    sectors = set()

    # Regular expression for the pattern 'z_{i}_{sector}'
    pattern = re.compile(r'z_(\d+)_(.+)')

    # Identify all sectors from the columns
    for col in columns:
        match = pattern.search(col)
        if match:
            sectors.add(match.group(2))  # Extract the sector and add it to the set

    # Initialize the resulting DataFrame in panel format
    df_panel = pd.DataFrame()

    # For each sector, transform and merge the data
    for sector in sectors:
        # Filter columns that belong to this sector
        sector_columns = [col for col in columns if f'_{sector}' in col]
        
        # Convert the sector's columns to long format
        sector_melted = pd.melt(df, id_vars=['vintages_date'], 
                                value_vars=sector_columns, 
                                var_name='horizon', 
                                value_name=f'z_{sector}')
        
        # Extract the revision number and remove the sector name from the 'horizon' field
        sector_melted['horizon'] = sector_melted['horizon'].str.extract(r'z_(\d+)_')[0].astype(int)

        # If it's the first sector, initialize df_panel
        if df_panel.empty:
            df_panel = sector_melted
        else:
            # Merge the current sector with the general panel
            df_panel = pd.merge(df_panel, sector_melted, on=['vintages_date', 'horizon'], how='outer')

    return df_panel

# Function to convert z dummies to data panel
#________________________________________________________________
def z_dummies_to_panel(df):
    # Get all columns from the dataframe
    columns = df.columns
    
    # Set to store unique sectors
    sectors = set()

    # Regular expression for the pattern 'z_{i}_{sector}'
    pattern = re.compile(r'z_(\d+)_(.+)')

    # Identify all sectors from the columns
    for col in columns:
        match = pattern.search(col)
        if match:
            sectors.add(match.group(2))  # Extract the sector and add it to the set

    # Initialize the resulting DataFrame in panel format
    df_panel = pd.DataFrame()

    # For each sector, transform and merge the data
    for sector in sectors:
        # Filter columns that belong to this sector
        sector_columns = [col for col in columns if f'_{sector}' in col]
        
        # Convert the sector's columns to long format
        sector_melted = pd.melt(df, id_vars=['vintages_date'], 
                                value_vars=sector_columns, 
                                var_name='horizon', 
                                value_name=f'z_dummy_{sector}')
        
        # Extract the revision number and remove the sector name from the 'horizon' field
        sector_melted['horizon'] = sector_melted['horizon'].str.extract(r'z_(\d+)_')[0].astype(int)

        # If it's the first sector, initialize df_panel
        if df_panel.empty:
            df_panel = sector_melted
        else:
            # Merge the current sector with the general panel
            df_panel = pd.merge(df_panel, sector_melted, on=['vintages_date', 'horizon'], how='outer')

    return df_panel