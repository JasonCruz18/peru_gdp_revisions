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



################################################################################################
# Section 7. Create cumulative revisions
################################################################################################

# Function to calculate cumulative revisions (1/2).
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


# Function to calculate cumulative revisions (2/2).
#________________________________________________________________

def calculate_specific_cumulative_revisions(df, frequency):
    # Obtén los nombres de las variables con los sufijos especificados
    suffixes = ['_release_1', '_release_6', '_release_12', '_release_24', '_most_recent']
    variable_names = [col.replace(suffix, '') for col in df.columns for suffix in suffixes if col.endswith(suffix)]
    variable_names = list(set(variable_names))  # Eliminar duplicados

    # Crear nuevas variables de revisión según la frecuencia
    for variable in variable_names:
        try:
            if frequency == 'monthly':
                # Verificar si las columnas existen para monthly
                if all(f'{variable}{suffix}' in df.columns for suffix in ['_most_recent', '_release_12', '_release_6', '_release_1']):
                    df[f'r_12_H_{variable}'] = df[f'{variable}_most_recent'] - df[f'{variable}_release_12']
                    df[f'r_6_H_{variable}'] = df[f'{variable}_most_recent'] - df[f'{variable}_release_6']
                    df[f'r_1_H_{variable}'] = df[f'{variable}_most_recent'] - df[f'{variable}_release_1']
                else:
                    print(f"Skipping {variable}: not all expected columns for 'monthly' are present.")
            elif frequency in ['annual', 'quarterly']:
                # Verificar si las columnas existen para annual o quarterly
                if all(f'{variable}{suffix}' in df.columns for suffix in ['_most_recent', '_release_24', '_release_12', '_release_1']):
                    df[f'r_24_H_{variable}'] = df[f'{variable}_most_recent'] - df[f'{variable}_release_24']
                    df[f'r_12_H_{variable}'] = df[f'{variable}_most_recent'] - df[f'{variable}_release_12']
                    df[f'r_1_H_{variable}'] = df[f'{variable}_most_recent'] - df[f'{variable}_release_1']
                else:
                    print(f"Skipping {variable}: not all expected columns for 'annual' or 'quarterly' are present.")
            else:
                print(f"Frequency {frequency} is not supported.")
        except KeyError as e:
            print(f"Error processing {variable}: {e}")
    
    return df


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
        cols_to_keep = ['vintages_date'] + ['year_month'] + [col for col in df.columns if col.startswith('r_')]
    
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
    sectors = sorted(set(col.split('_')[-1] for col in df.columns if col.startswith('r_')))
    
    # Step 3: Create a dictionary to store the transposed data
    data_transpuesta = {}
    
    # Step 4: For each sector, gather all columns related to that sector
    for sector in sectors:
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


# Function to add time trend column
#________________________________________________________________
def add_time_trend(df):
    # Número de observaciones en el dataframe
    n = len(df)
    
    # Columna 'horizon' que va de 1 hasta n
    df['horizon'] = range(1, n + 1)
    
    # Columna 'target_date' que contiene el valor máximo (n)
    df['target_date'] = n
    
    # Columna 'time_trend' que es la diferencia entre target_date y horizon
    df['time_trend'] = df['target_date'] - df['horizon']
    
    # Convertir las columnas al tipo entero
    df['horizon'] = df['horizon'].astype(int)
    df['target_date'] = df['target_date'].astype(int)
    df['time_trend'] = df['time_trend'].astype(int)
    
    return df


# Function to remove NaN or zero columns
#________________________________________________________________
def remove_nan_or_zero_float_columns(df):
    # Filtrar solo las columnas de tipo float
    float_columns = df.select_dtypes(include=['float'])
    
    # Identificar las columnas a eliminar (todas NaN, todas 0.0 o ambas)
    columns_to_drop = []
    for col in float_columns.columns:
        all_nan = float_columns[col].isna().all()         # Condición 1: todas NaN
        all_zero = (float_columns[col] == 0.0).all()      # Condición 2: todas 0.0
        nan_count = float_columns[col].isna().sum()        # Contar NaN
        zero_count = (float_columns[col] == 0.0).sum()     # Contar 0.0
        total_count = len(float_columns[col])               # Total de elementos en la columna
        
        # Condición 3: parte NaN y parte 0.0 y total debe ser igual a la longitud de la columna
        complementary_nan_and_zero = (nan_count + zero_count == total_count) and (nan_count > 0 and zero_count > 0)

        if all_nan or all_zero or complementary_nan_and_zero:
            columns_to_drop.append(col)
    
    # Eliminar las columnas identificadas
    df_filtered = df.drop(columns=columns_to_drop)
    
    return df_filtered


# Function to converto to data panel
#________________________________________________________________
def convert_to_panel(df):
    # Encontrar todos los sectores de las columnas
    columns = df.columns
    sectors = set()
    
    # Utilizar expresiones regulares para encontrar patrones de columnas como "r_1_2_{sector}" o "r_1_{sector}"
    pattern = re.compile(r'r_(?:\d+(_\d+)?)?_(.+)')

    for col in columns:
        match = pattern.search(col)
        if match:
            sectors.add(match.group(2))  # Extraer el nombre del sector

    # Inicializar el DataFrame resultante en formato panel
    df_panel = pd.DataFrame()

    # Para cada sector, hacer la conversión al formato largo (panel) y luego fusionar los resultados
    for sector in sectors:
        # Extraer las columnas que pertenecen a este sector
        sector_columns = [col for col in columns if sector in col]
        
        # Convertir las columnas del sector en formato largo
        sector_melted = pd.melt(df, id_vars=['vintages_date'], 
                                value_vars=sector_columns, 
                                var_name='revision', 
                                value_name=sector)
        
        # Limpiar la columna 'revision' para que contenga solo la revisión (ej: "r_1" o "r_1_2")
        sector_melted['revision'] = sector_melted['revision'].str.replace(f'_{sector}', '', regex=False)

        # Si es el primer sector, inicializar df_panel
        if df_panel.empty:
            df_panel = sector_melted
        else:
            # Fusionar el sector actual con el panel general
            df_panel = pd.merge(df_panel, sector_melted, on=['vintages_date', 'revision'], how='outer')


# Function to replace "-" by "_" in columns
#________________________________________________________________
def replace_hyphen_in_columns(df):
    # Reemplaza "-" por "_" en los nombres de las columnas
    df.columns = df.columns.str.replace("-", "_", regex=False)
    return df


################################################################################################
# Section 8. Create intermediate revisions
################################################################################################


# Function to calculate intermediate revisions (1/2)
#________________________________________________________________
def calculate_intermediate_revisions(df):
    # Encontrar el número más alto que sigue el patrón '_release_'
    release_numbers = []
    
    for col in df.columns:
        match = re.search(r'_release_(\d+)', col)
        if match:
            release_numbers.append(int(match.group(1)))
    
    # Determinar el número máximo de releases
    max_release = max(release_numbers) if release_numbers else 0
    
    # Extraer los nombres de las variables con los sufijos especificados
    variable_names = [col.replace(f'_release_{i}', '').replace('_most_recent', '') 
                      for i in range(1, max_release + 1)
                      for col in df.columns if f'_release_{i}' in col or '_most_recent' in col]
    
    # Eliminar duplicados de los nombres de variables
    variable_names = list(set(variable_names))
    
    # Crear una lista para contener las nuevas columnas
    new_columns = []
    
    # Crear nuevas variables de revisión intermedia para cada variable encontrada
    for variable in variable_names:
        for i in range(2, max_release + 1):  # Iterar desde release_2 hasta el release más alto
            previous_release_col = f"{variable}_release_{i-1}"
            current_release_col = f"{variable}_release_{i}"
            
            if previous_release_col in df.columns and current_release_col in df.columns:
                new_column_name = f"r_{i-1}_{i}_{variable}"
                # Realizar la resta de la revisión actual menos la revisión anterior
                new_columns.append((new_column_name, df[current_release_col] - df[previous_release_col]))
        
        # Para la última diferencia, entre most_recent y el release más reciente - 1
        most_recent_col = f"{variable}_most_recent"
        last_release_col = f"{variable}_release_{max_release}"
        
        if last_release_col in df.columns and most_recent_col in df.columns:
            # Modificar el nombre de la columna según lo solicitado
            new_column_name = f"r_{max_release}_most_recent_{variable}"
            new_columns.append((new_column_name, df[most_recent_col] - df[last_release_col]))
    
    # Concatenar todas las nuevas columnas al DataFrame
    if new_columns:
        df_new_columns = pd.DataFrame(dict(new_columns))
        df = pd.concat([df, df_new_columns], axis=1)

    # Retornar el DataFrame modificado
    return df, max_release

# Function to calculate intermediate revisions (2/2)
#________________________________________________________________
def calculate_specific_intermediate_revisions(df, frequency):
    # Obtén los nombres de las variables con los sufijos especificados
    suffixes = ['_release_1', '_release_6', '_release_12', '_release_24', '_most_recent']
    variable_names = [col.replace(suffix, '') for col in df.columns for suffix in suffixes if col.endswith(suffix)]
    variable_names = list(set(variable_names))  # Eliminar duplicados

    # Crear nuevas variables de revisión según la frecuencia
    for variable in variable_names:
        try:
            if frequency == 'monthly':
                # Verificar si las columnas existen para monthly
                if all(f'{variable}{suffix}' in df.columns for suffix in ['_most_recent', '_release_12', '_release_6', '_release_1']):
                    df[f'r_12_H_{variable}'] = df[f'{variable}_most_recent'] - df[f'{variable}_release_12']
                    df[f'r_6_12_{variable}'] = df[f'{variable}_release_12'] - df[f'{variable}_release_6']
                    df[f'r_1_6_{variable}'] = df[f'{variable}_release_6'] - df[f'{variable}_release_1']
                else:
                    print(f"Skipping {variable}: not all expected columns for 'monthly' are present.")
            elif frequency in ['annual', 'quarterly']:
                # Verificar si las columnas existen para annual o quarterly
                if all(f'{variable}{suffix}' in df.columns for suffix in ['_most_recent', '_release_24', '_release_12', '_release_1']):
                    df[f'r_24_H_{variable}'] = df[f'{variable}_most_recent'] - df[f'{variable}_release_24']
                    df[f'r_12_24_{variable}'] = df[f'{variable}_release_24'] - df[f'{variable}_release_12']
                    df[f'r_1_12_{variable}'] = df[f'{variable}_release_12'] - df[f'{variable}_release_1']
                else:
                    print(f"Skipping {variable}: not all expected columns for 'annual' or 'quarterly' are present.")
            else:
                print(f"Frequency {frequency} is not supported.")
        except KeyError as e:
            print(f"Error processing {variable}: {e}")
    
    return df

