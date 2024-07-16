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


# Function to 
#________________________________________________________________