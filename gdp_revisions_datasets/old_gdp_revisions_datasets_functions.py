#*********************************************************************************************
#*********************************************************************************************
# Functions for old_gdp_revisions_datasets.ipynb 
#*********************************************************************************************
#*********************************************************************************************



################################################################################################
# Section 1. Duplicate tables for all other NS ids
################################################################################################


#+++++++++++++++
# LIBRARIES
#+++++++++++++++


# 1.1. Table 1

import os  # Importar os para funcionalidades del sistema operativo
import shutil  # Importar shutil para operaciones de alto nivel en archivos y directorios
import psycopg2  # Importar psycopg2 para interactuar con bases de datos PostgreSQL
import pandas as pd  # Importar pandas con alias pd para manipulación de datos estructurados


# Function to duplicate files
#________________________________________________________________
def duplicate_files(year, df_year, base_path):
    year_path = os.path.join(base_path, str(year))
    files = sorted([f for f in os.listdir(year_path) if f.endswith('.csv')])

    # Get existing files
    existing_files = {int(f.split('-')[1]): f for f in files}
    
    # Get the last file of the previous year
    if year > 1994:
        prev_year_path = os.path.join(base_path, str(year - 1))
        prev_files = sorted([f for f in os.listdir(prev_year_path) if f.endswith('.csv')])
        if prev_files:
            last_prev_file = prev_files[-1]
            last_prev_id_ns = int(last_prev_file.split('-')[1])
        else:
            last_prev_file = None
            last_prev_id_ns = None
    else:
        last_prev_file = None
        last_prev_id_ns = None

    # Initialize variables for duplication
    last_existing_file = last_prev_file
    last_existing_id_ns = last_prev_id_ns

    for index, row in df_year.iterrows():
        id_ns = row['id_ns']
        if row['delivered_1'] == 1:
            last_existing_file = existing_files[id_ns]
            last_existing_id_ns = id_ns
        else:
            # Create name of new duplicate file
            new_file_name = f"ns-{id_ns:02d}-{year}.csv"
            new_file_path = os.path.join(year_path, new_file_name)

            # Duplicate file
            if last_existing_file:
                if last_existing_file in existing_files.values():
                    src_file_path = os.path.join(year_path, last_existing_file)
                else:
                    src_file_path = os.path.join(prev_year_path, last_existing_file)
                shutil.copy(src_file_path, new_file_path)
                print(f"Duplicated {last_existing_file} to {new_file_name}")
            else:
                print(f"No existing file to duplicate for {new_file_name}")



################################################################################################
# Section 2. Data cleaning
################################################################################################
                

# Function to ... PENDING
# _________________________________________________________________________


#**********************************************************************************************
# Section 2.1.  Extracting tables and data cleanup 
#----------------------------------------------------------------------------------------------

#+++++++++++++++
# LIBRARIES
#+++++++++++++++





#...............................................................................................
#...............................................................................................
# Functions for tables delivered by Central Bank
# ______________________________________________________________________________________________
#...............................................................................................

# 1. Convert column names to lowercase and remove accents
def clean_column_names(df):
    # Convert column names to lowercase
    df.columns = df.columns.str.lower()
    # Normalize string column names to remove accents
    df.columns = [unicodedata.normalize('NFKD', col).encode('ASCII', 'ignore').decode('utf-8') if isinstance(col, str) else col for col in df.columns]
    return df

# 2. Adjust column names
def adjust_column_names(df):
    # Check if the first observation in the first column is NaN
    if pd.isna(df.iloc[0, 0]) and pd.isna(df.iloc[0, -1]):
        # Verify column names
        if "sectores economicos" in df.columns[0] and "economic sectors" in df.columns[-1]:
            # Replace NaN with corresponding column names
            df.iloc[0, 0] = "sectores economicos"
            df.iloc[0, -1] = "economic sectors"
    return df

# 3. Round values in DataFrame columns
def rounding_values(df, decimals=1):
    # Iterate over all columns in the DataFrame
    for col in df.columns:
        # Check if the column is of type float
        if df[col].dtype == 'float64':
            # Round the values in the column to the specified number of decimals
            df[col] = df[col].round(decimals)
    return df



#...............................................................................................
#...............................................................................................
# Functions for table 1
# ______________________________________________________________________________________________
#...............................................................................................

# 1. Replace 'TOTAL' with 'YEAR' in the first row
def replace_total_with_year(df):
    # Replace 'TOTAL' with 'YEAR' in the first row
    df.iloc[0] = df.iloc[0].apply(lambda x: 'YEAR' if "TOTAL" in str(x) else x)
    return df