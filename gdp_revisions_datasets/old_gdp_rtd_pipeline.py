#*********************************************************************************************
#*********************************************************************************************
# Functions for old_gdp_revisions_datasets.ipynb 
#*********************************************************************************************
#*********************************************************************************************


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

import unicodedata  # For manipulating Unicode data



#...............................................................................................
#...............................................................................................
#  Pre-2013-specified cleansing function
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


#...............................................................................................
#...............................................................................................
# Functions for table 2
# ______________________________________________________________________________________________
#...............................................................................................

# 1. Replace 'TOTAL' with 'YEAR' in the first row
def replace_total_with_year(df):
    # Replace 'TOTAL' with 'YEAR' in the first row
    df.iloc[0] = df.iloc[0].apply(lambda x: 'YEAR' if "TOTAL" in str(x) else x)
    return df