"""
Table 1 (monthly GDP) specific cleaning utilities.

This module provides functions specific to cleaning Table 1 (monthly GDP percentage
variations) from both OLD (CSV) and NEW (PDF) BCRP Weekly Reports. These functions
handle header construction, year column inference, and WR-specific data quality issues.
"""

import re
import unicodedata
from typing import List

import numpy as np
import pandas as pd

from peru_gdp_rtd.cleaners.text_cleaners import remove_rare_characters_first_row


def clean_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert column names to lowercase and remove accents.

    Exclusive to OLD dataset processing.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with normalized column names.
    """
    df.columns = df.columns.str.lower()
    df.columns = [
        unicodedata.normalize('NFKD', col).encode('ASCII', 'ignore').decode('utf-8')
        if isinstance(col, str)
        else col
        for col in df.columns
    ]
    return df


def adjust_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adjust column names when first and last columns have NaN headers.

    If first observation in first/last columns are NaN and their column names
    contain sector labels, replaces the NaN with the appropriate label.

    Exclusive to OLD dataset processing.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with adjusted column names.
    """
    if pd.isna(df.iloc[0, 0]) and pd.isna(df.iloc[0, -1]):
        if "sectores economicos" in df.columns[0] and "economic sectors" in df.columns[-1]:
            df.iloc[0, 0] = "sectores economicos"
            df.iloc[0, -1] = "economic sectors"
    return df


def relocate_last_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Create helper column and relocate penultimate header when last column is filled.

    If last column's second row is non-null, creates a temporary helper column,
    moves penultimate header value to last column header, and clears original spot.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with relocated columns.
    """
    if not pd.isna(df.iloc[1, -1]):
        new_column = 'col_' + ''.join(map(str, np.random.randint(1, 5, size=1)))
        df[new_column] = np.nan

        insert_value_1 = df.iloc[0, -2]
        insert_value_1 = str(insert_value_1)
        df.iloc[:, -1] = df.iloc[:, -1].astype('object')
        df.iloc[0, -1] = insert_value_1

        df.iloc[0, -2] = np.nan
    return df


def get_months_sublist_list(df: pd.DataFrame, year_columns: List[str]) -> pd.DataFrame:
    """
    Build composite month headers from first row and year columns.

    Parse first row to collect month tokens and compose headers as <year>_<month>.
    Preserves first two original elements if not present in new header list.

    Args:
        df: DataFrame to modify.
        year_columns: List of 4-digit year column names.

    Returns:
        DataFrame with composite month headers.
    """
    first_row = df.iloc[0]
    months_sublist_list = []
    months_sublist = []

    for item in first_row:
        if len(str(item)) == 3:  # Month abbreviations (e.g., 'jan')
            months_sublist.append(item)
        elif '-' in str(item) or str(item) == 'year':  # Boundary markers
            months_sublist.append(item)
            months_sublist_list.append(months_sublist)
            months_sublist = []

    if months_sublist:
        months_sublist_list.append(months_sublist)

    new_elements = []
    if year_columns:
        for i, year in enumerate(year_columns):
            if i < len(months_sublist_list):
                for element in months_sublist_list[i]:
                    new_elements.append(f"{year}_{element}")

    two_first_elements = df.iloc[0][:2].tolist()
    for index in range(len(two_first_elements) - 1, -1, -1):
        if two_first_elements[index] not in new_elements:
            new_elements.insert(0, two_first_elements[index])

    while len(new_elements) < len(df.columns):
        new_elements.append(None)

    temp_df = pd.DataFrame([new_elements], columns=df.columns)
    df.iloc[0] = temp_df.iloc[0]
    return df


def find_year_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Infer/correct year header based on position of 'year' token.

    Detects 4-digit year columns; if single year present and 'year' token appears
    in different column header position, renames that token to adjacent year (±1).

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with corrected year column.
    """
    found_years = []

    for column in df.columns:
        if str(column).isdigit() and len(str(column)) == 4:
            found_years.append(column)

    if len(found_years) == 1:
        year_name = found_years[0]
        first_row = df.iloc[0]

        column_contains_year = first_row[first_row.astype(str).str.contains(r'\byear\b')]

        if not column_contains_year.empty:
            column_contains_year_name = column_contains_year.index[0]
            column_contains_year_index = df.columns.get_loc(column_contains_year_name)
            year_name_index = df.columns.get_loc(year_name)

            if column_contains_year_index < year_name_index:
                new_year = str(int(year_name) - 1)
                df.rename(columns={column_contains_year_name: new_year}, inplace=True)
            elif column_contains_year_index > year_name_index:
                new_year = str(int(year_name) + 1)
                df.rename(columns={column_contains_year_name: new_year}, inplace=True)

    return df


def replace_var_perc_first_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize 'Var. %' tokens in first column.

    Replaces 'Var.%' variants with 'variacion porcentual' in Spanish sector column.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with standardized variation labels.
    """
    regex = re.compile(r'Var\. ?%')

    for index, row in df.iterrows():
        value = str(row.iloc[0])
        if regex.search(value):
            df.at[index, df.columns[0]] = regex.sub("variacion porcentual", value)
    return df


def replace_number_moving_average(df: pd.DataFrame, number_moving_average: str = 'three') -> pd.DataFrame:
    """
    Normalize moving-average descriptors in last column.

    Replaces patterns like '2 -' at start of tokens with normalized text (e.g., 'three-').

    Args:
        df: DataFrame to modify.
        number_moving_average: Text to replace numeric prefix (default: 'three').

    Returns:
        DataFrame with normalized moving average labels.
    """
    for index, row in df.iterrows():
        if pd.notnull(row.iloc[-1]) and re.search(r'(\d\s*-)', str(row.iloc[-1])):
            df.at[index, df.columns[-1]] = re.sub(
                r'(\d\s*-)', f'{number_moving_average}-', str(row.iloc[-1])
            )
    return df


def replace_var_perc_last_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Standardize 'Var. %' tokens in last two columns.

    Replaces 'Var.%' variants with 'percent change' in English columns.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with standardized variation labels.
    """
    regex = re.compile(r'(Var\. ?%)(.*)')

    for index, row in df.iterrows():
        if isinstance(row.iloc[-2], str) and regex.search(row.iloc[-2]):
            replaced_text = regex.sub(r'\2 percent change', row.iloc[-2])
            df.at[index, df.columns[-2]] = replaced_text.strip()

        if isinstance(row.iloc[-1], str) and regex.search(row.iloc[-1]):
            replaced_text = regex.sub(r'\2 percent change', row.iloc[-1])
            df.at[index, df.columns[-1]] = replaced_text.strip()
    return df


def replace_first_dot(df: pd.DataFrame) -> pd.DataFrame:
    """
    Change first dot in row 2 into hyphen pattern across columns.

    If cell on second row matches 'Word.Word', replaces first dot with hyphen.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with dot-to-hyphen replacements.
    """
    second_row = df.iloc[1]

    if any(isinstance(cell, str) and re.match(r'^\w+\.\s?\w+', cell) for cell in second_row):
        for col in df.columns:
            if isinstance(second_row[col], str):
                if re.match(r'^\w+\.\s?\w+', second_row[col]):
                    df.at[1, col] = re.sub(r'(\w+)\.(\s?\w+)', r'\1-\2', second_row[col], count=1)
    return df


def swap_nan_se(df: pd.DataFrame) -> pd.DataFrame:
    """
    Swap NaN and 'SECTORES ECONÓMICOS' in first row, then drop empty column.

    Places 'SECTORES ECONÓMICOS' in first column header when it drifted to second.

    Specific to WR ns_2014_07 pattern.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with corrected sector column.
    """
    if pd.isna(df.iloc[0, 0]) and df.iloc[0, 1] == "SECTORES ECONÓMICOS":
        column_1_value = df.iloc[0, 1]
        df.iloc[0, 0] = column_1_value
        df.iloc[0, 1] = np.nan
        df = df.drop(df.columns[1], axis=1)
    return df


def replace_first_row_with_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace NaNs in first row with synthetic names, then promote as headers.

    If first row contains 4-digit year tokens, fills NaN cells with 'column_<idx>'
    and promotes first row to be the header.

    Specific to WR ns_2014_08 pattern.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with synthetic column names promoted to header.
    """
    if any(isinstance(element, str) and element.isdigit() and len(element) == 4 for element in df.iloc[0]):
        for col_index, value in enumerate(df.iloc[0]):
            if pd.isna(value):
                df.iloc[0, col_index] = f"column_{col_index + 1}"
        df.columns = df.iloc[0]
        df = df.drop(df.index[0])
    return df


def check_first_row(df: pd.DataFrame) -> pd.DataFrame:
    """
    Re-label year-pair headers and backfill single-year cells in row 0.

    Detects cells like '2018 2019' in row 0; renames with synthetic names and
    backfills year tokens into first two columns if missing.

    Specific to WR ns_2015_11 pattern.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with year pairs separated.
    """
    first_row = df.iloc[0]

    for i, (col, value) in enumerate(first_row.items()):
        if re.search(r'\b\d{4}\s\d{4}\b', str(value)):
            years = value.split()
            first_year = years[0]
            second_year = years[1]

            original_column_name = f'col_{i}'
            df.at[0, col] = original_column_name

            if pd.isna(df.iloc[0, 0]):
                df.iloc[0, 0] = first_year

            if pd.isna(df.iloc[0, 1]):
                df.iloc[0, 1] = second_year

    return df


def check_first_row_1(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill first-row year tokens from trailing columns when leading cells are NaN.

    If first or second header cells are NaN and trailing cells contain 4-digit years,
    moves those years forward and clears original positions.

    Specific to WR ns_2016_15 pattern.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with year headers moved to front.
    """
    if pd.isnull(df.iloc[0, 0]):
        penultimate_column = df.iloc[0, -2]
        if isinstance(penultimate_column, str) and len(penultimate_column) == 4 and penultimate_column.isdigit():
            df.iloc[0, 0] = penultimate_column
            df.iloc[0, -2] = np.nan

    if pd.isnull(df.iloc[0, 1]):
        last_column = df.iloc[0, -1]
        if isinstance(last_column, str) and len(last_column) == 4 and last_column.isdigit():
            df.iloc[0, 1] = last_column
            df.iloc[0, -1] = np.nan

    return df
