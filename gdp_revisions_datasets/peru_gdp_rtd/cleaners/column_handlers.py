"""
Column manipulation utilities for GDP table cleaning.

This module provides specialized functions for splitting, swapping, relocating,
and transforming columns in DataFrames extracted from BCRP Weekly Reports.
"""

import re
from typing import List

import numpy as np
import pandas as pd


def split_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Split third-from-last column into multiple columns by whitespace.

    Splits whitespace-separated tokens and inserts new columns before the last two.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with split column.
    """
    column_to_expand = df.columns[-3]
    new_columns = df[column_to_expand].str.split(expand=True)
    new_columns.columns = [f'{column_to_expand}_{i+1}' for i in range(new_columns.shape[1])]

    insertion_position = len(df.columns) - 2
    for col in reversed(new_columns.columns):
        df.insert(insertion_position, col, new_columns[col])

    df.drop(columns=[column_to_expand], inplace=True)
    return df


def split_values_1(df: pd.DataFrame) -> pd.DataFrame:
    """
    Split penultimate column by whitespace and insert parts before last column.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with split column.
    """
    column_to_expand = df.columns[-2]
    new_columns = df[column_to_expand].str.split(expand=True)
    new_columns.columns = [f'{column_to_expand}_{i+1}' for i in range(new_columns.shape[1])]

    insertion_position = len(df.columns) - 1
    for col in reversed(new_columns.columns):
        df.insert(insertion_position, col, new_columns[col])

    df.drop(columns=[column_to_expand], inplace=True)
    return df


def split_values_2(df: pd.DataFrame) -> pd.DataFrame:
    """
    Split fourth-from-last column by whitespace and insert parts before last three columns.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with split column.
    """
    column_to_expand = df.columns[-4]
    new_columns = df[column_to_expand].str.split(expand=True)
    new_columns.columns = [f'{column_to_expand}_{i+1}' for i in range(new_columns.shape[1])]

    insertion_position = len(df.columns) - 3
    for col in reversed(new_columns.columns):
        df.insert(insertion_position, col, new_columns[col])

    df.drop(columns=[column_to_expand], inplace=True)
    return df


def split_values_3(df: pd.DataFrame) -> pd.DataFrame:
    """
    Split third-from-last column by whitespace and insert parts before last two columns.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with split column.
    """
    column_to_expand = df.columns[-3]
    new_columns = df[column_to_expand].str.split(expand=True)
    new_columns.columns = [f'{column_to_expand}_{i+1}' for i in range(new_columns.shape[1])]

    insertion_position = len(df.columns) - 2
    for col in reversed(new_columns.columns):
        df.insert(insertion_position, col, new_columns[col])

    df.drop(columns=[column_to_expand], inplace=True)
    return df


def split_column_by_pattern(df: pd.DataFrame) -> pd.DataFrame:
    """
    Split columns where second row matches 'Title.Title' pattern.

    For matching columns, splits by whitespace and inserts second token into
    a new '<col>_split' column immediately to the right.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with pattern-matched columns split.
    """
    for col in df.columns:
        if re.match(r'^[A-Z][a-z]+\.?\s[A-Z][a-z]+\.?$', str(df.iloc[1][col])):
            split_values = df[col].str.split(expand=True)
            df[col] = split_values[0]
            new_col_name = col + '_split'
            df.insert(df.columns.get_loc(col) + 1, new_col_name, split_values[1])
    return df


def exchange_values(df: pd.DataFrame) -> pd.DataFrame:
    """
    Swap values between last two columns when last column has NaNs.

    If the last column contains NaNs, swaps those cells with corresponding
    penultimate column values (row-wise).

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with exchanged values.
    """
    if len(df.columns) < 2:
        print("The DataFrame has less than two columns. Values cannot be exchanged.")
        return df

    if df.iloc[:, -1].isnull().any():
        last_column_rows_nan = df[df.iloc[:, -1].isnull()].index

        for idx in last_column_rows_nan:
            if -2 >= -len(df.columns):
                df.iloc[idx, -1], df.iloc[idx, -2] = df.iloc[idx, -2], df.iloc[idx, -1]

    return df


def replace_nan_with_previous_column_1(df: pd.DataFrame) -> pd.DataFrame:
    """
    Swap values with previous column when right column has NaNs (variant 1).

    Swaps with the previous column if the right one contains NaNs and is not
    a '_year' column.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with swapped values.
    """
    columns = df.columns

    for i in range(len(columns) - 1):
        if i != len(columns) - 2 and not (
            str(columns[i]).endswith('_year') and df[columns[i]].isnull().any()
        ):
            if df[columns[i + 1]].isnull().any() and not str(columns[i + 1]).endswith('_year'):
                nan_indices = df[columns[i + 1]].isnull()
                df.loc[nan_indices, [columns[i], columns[i + 1]]] = df.loc[
                    nan_indices, [columns[i + 1], columns[i]]
                ].values

    return df


def replace_nan_with_previous_column_2(df: pd.DataFrame) -> pd.DataFrame:
    """
    Swap values with previous column when right column has NaNs (variant 2).

    Identical to variant 1; included for WR-specific patterns requiring a second pass.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with swapped values.
    """
    columns = df.columns

    for i in range(len(columns) - 1):
        if i != len(columns) - 2 and not (
            str(columns[i]).endswith('_year') and df[columns[i]].isnull().any()
        ):
            if df[columns[i + 1]].isnull().any() and not str(columns[i + 1]).endswith('_year'):
                nan_indices = df[columns[i + 1]].isnull()
                df.loc[nan_indices, [columns[i], columns[i + 1]]] = df.loc[
                    nan_indices, [columns[i + 1], columns[i]]
                ].values

    return df


def replace_nan_with_previous_column_3(df: pd.DataFrame) -> pd.DataFrame:
    """
    Swap adjacent columns when right column has NaNs and left ends with '_year'.

    For pairs where:
    - Left column ends with '_year' and is fully non-null
    - Right column has NaNs and doesn't end with '_year'
    Swaps values to restore proper alignment.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with swapped values.
    """
    columns = df.columns

    for i in range(len(columns) - 1):
        if i != len(columns) - 1 and (
            str(columns[i]).endswith('_year') and not df[columns[i]].isnull().any()
        ):
            if df[columns[i + 1]].isnull().any() and not str(columns[i + 1]).endswith('_year'):
                nan_indices = df[columns[i + 1]].isnull()
                df.loc[nan_indices, [columns[i], columns[i + 1]]] = df.loc[
                    nan_indices, [columns[i + 1], columns[i]]
                ].values

    return df


def expand_column(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expand penultimate column by fixing 'a-b' into 'a b' and splitting trailing text.

    Normalizes 'word-word' to 'word word', moves trailing text to last column
    (row-wise), keeping numeric and textual parts separated.

    Args:
        df: DataFrame to modify.

    Returns:
        DataFrame with expanded column.
    """
    column_to_expand = df.columns[-2]

    def replace_hyphens(match_obj):
        return match_obj.group(1) + ' ' + match_obj.group(2)

    if (
        df[column_to_expand].str.contains(r'\d').any()
        and df[column_to_expand].str.contains(r'[a-zA-Z]').any()
    ):
        df[column_to_expand] = df[column_to_expand].apply(
            lambda x: re.sub(r'([a-zA-Z]+)\s*-\s*([a-zA-Z]+)', replace_hyphens, str(x))
            if pd.notnull(x)
            else x
        )

        pattern = re.compile(r'[a-zA-Z\s]+$')

        def extract_replace(row):
            if pd.notnull(row[column_to_expand]) and isinstance(row[column_to_expand], str):
                if row.name != 0:
                    value_to_replace = pattern.search(row[column_to_expand])
                    if value_to_replace:
                        value_to_replace = value_to_replace.group().strip()
                        row[df.columns[-1]] = value_to_replace
                        row[column_to_expand] = re.sub(
                            pattern, '', row[column_to_expand]
                        ).strip()
            return row

        df = df.apply(extract_replace, axis=1)

    return df
