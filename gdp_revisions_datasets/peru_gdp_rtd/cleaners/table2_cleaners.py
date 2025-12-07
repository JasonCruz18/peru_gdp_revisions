"""
Table 2 (quarterly/annual GDP) specific cleaning utilities.

This module provides functions specific to cleaning Table 2 (quarterly and annual GDP
percentage variations) from both OLD (CSV) and NEW (PDF) BCRP Weekly Reports. These
functions handle quarter header construction, Roman numeral conversion, and WR-specific
data quality issues.
"""

import re
from typing import List

import numpy as np
import pandas as pd
import roman

from peru_gdp_rtd.cleaners.text_cleaners import find_roman_numerals


def replace_total_with_year(df: pd.DataFrame) -> pd.DataFrame:
    """
    Replace 'TOTAL' with 'YEAR' in first row.

    Exclusive to OLD dataset processing.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with 'TOTAL' replaced by 'YEAR'.
    """
    df.iloc[0] = df.iloc[0].apply(lambda x: "YEAR" if "TOTAL" in str(x) else x)
    return df


def separate_years(df: pd.DataFrame) -> pd.DataFrame:
    """
    Split two space-separated years in penultimate column into two columns.

    If penultimate header cell contains 'YYYY YYYY', keeps first in place and
    inserts second as new column immediately before last column.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with separated year columns.
    """
    df = df.copy()
    if isinstance(df.iloc[0, -2], str) and len(df.iloc[0, -2].split()) == 2:
        years = df.iloc[0, -2].split()
        if all(len(year) == 4 for year in years):
            second_year = years[1]
            df.iloc[0, -2] = years[0]
            df.insert(len(df.columns) - 1, "new_column", [second_year] + [None] * (len(df) - 1))
    return df


def relocate_roman_numerals(df: pd.DataFrame) -> pd.DataFrame:
    """
    Move Roman numerals from last cell in row 2 into new column.

    Detects Roman numerals in third row's last column, strips them from that cell,
    moves them into 'new_column', and sets original cell to NaN.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with Roman numerals relocated.
    """
    roman_numerals = find_roman_numerals(str(df.iloc[2, -1]))
    if roman_numerals:
        original_text = df.iloc[2, -1]
        for roman_numeral in roman_numerals:
            original_text = str(original_text).replace(roman_numeral, "").strip()
        df.iloc[2, -1] = original_text
        df.at[2, "new_column"] = ", ".join(roman_numerals)
        df.iloc[2, -1] = np.nan
    return df


def extract_mixed_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Extract '<number,text>' pairs from third-last column into second-last column.

    If third-from-last column contains patterns like '-1,2 text', moves that token
    into penultimate column (when empty) and cleans source cell.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with mixed values extracted.
    """
    df = df.copy()
    regex_pattern = r"(-?\d+,\d [a-zA-Z\s]+)"

    for index, row in df.iterrows():
        third_last_obs = row.iloc[-3]
        second_last_obs = row.iloc[-2]

        if isinstance(third_last_obs, str) and pd.notnull(third_last_obs):
            match = re.search(regex_pattern, third_last_obs)
            if match:
                extracted_part = match.group(0)
                if pd.isna(second_last_obs) or pd.isnull(second_last_obs):
                    df.iloc[index, -2] = extracted_part
                    third_last_obs = re.sub(regex_pattern, "", third_last_obs).strip()
                    df.iloc[index, -3] = third_last_obs
    return df


def replace_first_row_nan(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fill NaN values in first row with their column names.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with NaN header cells filled.
    """
    for col in df.columns:
        if pd.isna(df.iloc[0][col]):
            df.iloc[0, df.columns.get_loc(col)] = col
    return df


def roman_arabic(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert Roman numerals in first row to Arabic numerals.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with Roman numerals converted to Arabic.
    """
    first_row = df.iloc[0]

    def convert_roman_number(number):
        try:
            return str(roman.fromRoman(str(number)))
        except (roman.InvalidRomanNumeralError, AttributeError):
            return number

    converted_first_row = []
    for value in first_row:
        if isinstance(value, str) and not pd.isna(value):
            converted_first_row.append(convert_roman_number(value))
        else:
            converted_first_row.append(value)

    df.iloc[0] = converted_first_row
    return df


def fix_duplicates(df: pd.DataFrame) -> pd.DataFrame:
    """
    Fix duplicate numeric headers in first row by incrementing duplicates.

    Ensures strictly increasing numeric tokens across first row when duplicates appear.
    Subsequent duplicates are incremented in sequence.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with duplicate numbers fixed.
    """
    second_row = df.iloc[0].copy()
    prev_num = None
    first_one_index = None

    for i, num in enumerate(second_row):
        try:
            num = int(num)
            prev_num = int(prev_num) if prev_num is not None else None

            if num == prev_num:
                if num == 1:
                    if first_one_index is None:
                        first_one_index = i - 1
                    next_num = int(second_row[i - 1]) + 1
                    for j in range(i, len(second_row)):
                        if str(second_row.iloc[j]).isdigit():
                            second_row.iloc[j] = str(next_num)
                            next_num += 1
                elif i - 1 >= 0:
                    second_row.iloc[i] = str(int(second_row.iloc[i - 1]) + 1)

            prev_num = num
        except ValueError:
            pass

    df.iloc[0] = second_row
    return df


def get_quarters_sublist_list(df: pd.DataFrame, year_columns: List[str]) -> pd.DataFrame:
    """
    Build composite quarter headers per year: <year>_<q>.

    Parses first row to collect single-char quarter labels and composes headers as <year>_<q>.
    Preserves first two original elements if not present in result.

    Args:
        df: DataFrame to modify.
        year_columns: List of 4-digit year column names.

    Returns:
        DataFrame with composite quarter headers.
    """
    first_row = df.iloc[0]
    quarters_sublist_list = []
    quarters_sublist = []

    for item in first_row:
        if len(str(item)) == 1:  # Single-character quarter label (e.g., '1', '2', '3', '4')
            quarters_sublist.append(item)
        elif str(item) == "year":  # Marker for year columns
            quarters_sublist.append(item)
            quarters_sublist_list.append(quarters_sublist)
            quarters_sublist = []

    if quarters_sublist:
        quarters_sublist_list.append(quarters_sublist)

    new_elements = []
    if year_columns:
        for i, year in enumerate(year_columns):
            if i < len(quarters_sublist_list):
                for element in quarters_sublist_list[i]:
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


def drop_nan_row(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop first row if it is entirely NaN.

    Specific to WR ns_2016_20 pattern.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with first row dropped if all NaN.
    """
    if df.iloc[0].isnull().all():
        df = df.drop(index=0)
        df.reset_index(drop=True, inplace=True)
    return df


def last_column_es(df: pd.DataFrame) -> pd.DataFrame:
    """
    Move header content when last column begins with 'ECONOMIC SECTORS'.

    If last column header is 'ECONOMIC SECTORS' and second row in that column is non-null,
    inserts new helper column and relocates adjacent header value into last column header.

    Specific to WR ns_2019_17 pattern.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with header relocated.
    """
    if df[df.columns[-1]].iloc[0] == "ECONOMIC SECTORS":
        if pd.notnull(df[df.columns[-1]].iloc[1]):
            new_column_name = f"col_{len(df.columns)}"
            df[new_column_name] = np.nan

            insert_value = df.iloc[0, -2]
            insert_value = str(insert_value)
            df.iloc[:, -1] = df.iloc[:, -1].astype("object")
            df.iloc[0, -1] = insert_value

            df.iloc[0, -2] = np.nan
    return df


def exchange_columns(df: pd.DataFrame) -> pd.DataFrame:
    """
    Swap two column names when NaN-only year column is adjacent to non-year column.

    Finds year-like column (4 digits) that is fully NaN and swaps its name with
    immediate left neighbor if that neighbor is not year-like.

    Specific to WR ns_2019_26 pattern.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with column names swapped.
    """
    nan_column = None
    for column in df.columns:
        if df[column].isnull().all() and len(str(column)) == 4 and str(column).isdigit():
            nan_column = column
            break

    if nan_column:
        column_index = df.columns.get_loc(nan_column)
        if column_index > 0:
            left_column = df.columns[column_index - 1]
            if not (len(str(left_column)) == 4 and str(left_column).isdigit()):
                df.rename(columns={nan_column: left_column, left_column: nan_column}, inplace=True)
    return df


def exchange_roman_nan(df: pd.DataFrame) -> pd.DataFrame:
    """
    Swap values when Roman numeral or 'AÑO' appears next to empty cell in row 2.

    For each cell in row 2, if it is 'AÑO' or valid Roman numeral and next cell is NaN,
    swaps those two row-2 values when column below is empty (except header row).

    Specific to WR ns_2019_29 pattern.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with Roman numeral values swapped.
    """
    for col_idx, value in enumerate(df.iloc[1]):
        if isinstance(value, str):
            try:
                is_roman = value.upper() == "AÑO" or (
                    value.isalpha() and roman.fromRoman(value.upper())
                )
            except (roman.InvalidRomanNumeralError, AttributeError):
                is_roman = False

            if is_roman:
                next_col_idx = col_idx + 1
                if next_col_idx < len(df.columns) and pd.isna(df.iloc[1, next_col_idx]):
                    current_col = df.iloc[:, col_idx].drop(index=1)
                    next_col = df.iloc[:, next_col_idx].drop(index=1)
                    if current_col.isna().all():
                        df.iloc[1, col_idx], df.iloc[1, next_col_idx] = (
                            df.iloc[1, next_col_idx],
                            df.iloc[1, col_idx],
                        )
    return df
