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
