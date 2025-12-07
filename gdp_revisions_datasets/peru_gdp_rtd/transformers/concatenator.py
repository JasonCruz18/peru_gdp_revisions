"""
Concatenation utilities for merging vintage datasets into unified RTD.

This module provides functions to concatenate vintage-format GDP tables across
years and sources (OLD + NEW) into a unified Real-Time Dataset. Handles column
alignment, chronological ordering, and type enforcement.
"""

import os
import re
import time
from typing import List, Optional

import pandas as pd

from peru_gdp_rtd.processors.metadata import read_records, write_records


def target_period_monthly_sort_key(tp: str) -> tuple:
    """
    Convert monthly target period to (year, month) tuple for sorting.

    Args:
        tp: Target period in format 'tp_YYYYmM' or 'YYYYmM'.

    Returns:
        Tuple of (year, month) for sorting, or (9999, 0) for unmatched patterns.

    Example:
        >>> target_period_monthly_sort_key("tp_2017m7")
        (2017, 7)
        >>> target_period_monthly_sort_key("2017m12")
        (2017, 12)
    """
    if tp.startswith("tp_"):
        tp = tp[3:]
    m = re.match(r"(\d{4})m(\d+)", tp)
    if m:
        return (int(m.group(1)), int(m.group(2)))
    return (9999, 0)


def target_period_quarterly_sort_key(col: str) -> tuple:
    """
    Convert quarterly/annual target period to (year, type, quarter) for sorting.

    Quarterly periods are sorted before annual periods within each year.

    Args:
        col: Target period in format 'tp_YYYYqN' or 'tp_YYYY'.

    Returns:
        Tuple of (year, type_flag, quarter) for sorting.
        - type_flag: 0 for quarters, 1 for annual
        - Returns (9999, 9, col) for unmatched patterns

    Example:
        >>> target_period_quarterly_sort_key("tp_2017q1")
        (2017, 0, 1)
        >>> target_period_quarterly_sort_key("tp_2017")
        (2017, 1, 0)
    """
    if not col.startswith("tp_"):
        return (9999, 9, col)

    body = col[3:]  # Remove 'tp_' prefix

    # Match quarterly pattern: '2017q1'
    m = re.match(r"^(\d{4})q(\d)$", body)
    if m:
        year = int(m.group(1))
        q = int(m.group(2))
        return (year, 0, q)  # Quarters first (type_flag=0)

    # Match annual pattern: '2017'
    m2 = re.match(r"^(\d{4})$", body)
    if m2:
        year = int(m2.group(1))
        return (year, 1, 0)  # Annual after quarters (type_flag=1)

    return (9999, 9, col)


def concatenate_table_1(
    input_data_subfolder: str,
    record_folder: str,
    record_txt: str,
    persist: bool = False,
    persist_folder: Optional[str] = None,
    csv_file_label: Optional[str] = None,
) -> pd.DataFrame:
    """
    Concatenate all Table 1 (monthly GDP) vintage files into unified RTD.

    Performs row-based concatenation of monthly GDP vintages:
    1. Loads all CSV/Parquet files from year subfolders under table_1/
    2. Builds union of all tp_* columns across files
    3. Sorts tp_* columns chronologically
    4. Reindexes all DataFrames to match full column schema
    5. Vertically concatenates all data
    6. Enforces consistent dtypes
    7. Optionally persists unified RTD to disk

    Args:
        input_data_subfolder: Root folder containing table_1/ subfolder with year folders.
        record_folder: Folder for storing concatenation records.
        record_txt: Filename for record file (e.g., 'concatenate_table_1.txt').
        persist: If True, save unified RTD to disk.
        persist_folder: Folder for saving output (default: same as input_data_subfolder).
        csv_file_label: Custom filename for output (default: 'gdp_rtd_table_1_unified.csv').

    Returns:
        Unified RTD DataFrame with columns: industry, vintage, tp_YYYYmM...

    Example:
        >>> unified = concatenate_table_1(
        ...     "./data/input",
        ...     "./record",
        ...     "concat_table_1.txt",
        ...     persist=True
        ... )
        >>> print(f"Unified RTD shape: {unified.shape}")
    """
    start_time = time.time()
    print("\n⛓️  Starting Table 1 concatenation (row-based)...")

    processed_files = read_records(record_folder, record_txt)
    table_1_folder = os.path.join(input_data_subfolder, "table_1")

    if not os.path.exists(table_1_folder):
        print(f"⚠️  Table 1 folder not found: {table_1_folder}")
        return pd.DataFrame()

    year_folders = sorted([f for f in os.listdir(table_1_folder) if f.isdigit()], key=int)

    loaded_dfs: List[pd.DataFrame] = []
    skipped_counter = 0
    new_counter = 0

    # Load files from each year folder
    for year in year_folders:
        year_folder = os.path.join(table_1_folder, year)
        files = sorted([f for f in os.listdir(year_folder) if f.endswith((".csv", ".parquet"))])

        for file in files:
            if file in processed_files:
                skipped_counter += 1
                continue

            full_path = os.path.join(year_folder, file)
            try:
                if file.endswith(".parquet"):
                    df = pd.read_parquet(full_path)
                else:
                    df = pd.read_csv(full_path)

                loaded_dfs.append(df)
                processed_files.append(file)
                write_records(record_folder, record_txt, processed_files)
                new_counter += 1
            except Exception as e:
                print(f"⚠️  Error loading {file}: {e}")
                continue

    if not loaded_dfs:
        print("No new files to concatenate.")
        return pd.DataFrame()

    # Build union of all tp_* columns
    base_cols = ["industry", "vintage"]
    all_tp_cols = set()

    for df in loaded_dfs:
        for col in df.columns:
            if col.startswith("tp_"):
                all_tp_cols.add(col)

    # Sort tp_* columns chronologically
    tp_cols_sorted = sorted(list(all_tp_cols), key=target_period_monthly_sort_key)

    # Final column schema
    final_cols = base_cols + tp_cols_sorted

    # Reindex each DataFrame to match full schema
    aligned_dfs = []
    for df in loaded_dfs:
        if "industry" not in df.columns or "vintage" not in df.columns:
            print("⚠️  Skipping DataFrame missing 'industry' or 'vintage' column")
            continue

        df = df.reindex(columns=final_cols)
        aligned_dfs.append(df)

    if not aligned_dfs:
        print("No valid DataFrames to concatenate.")
        return pd.DataFrame()

    # Vertical concatenation
    unified_df = pd.concat(aligned_dfs, axis=0, ignore_index=True)

    # Enforce consistent dtypes
    unified_df["industry"] = unified_df["industry"].astype(str)
    unified_df["vintage"] = unified_df["vintage"].astype(str)

    for col in tp_cols_sorted:
        unified_df[col] = pd.to_numeric(unified_df[col], errors="coerce").astype(float)

    # Persist if requested
    if persist:
        persist_folder = persist_folder or input_data_subfolder
        os.makedirs(persist_folder, exist_ok=True)
        fname = csv_file_label or "gdp_rtd_table_1_unified.csv"
        out_path = os.path.join(persist_folder, fname)
        unified_df.to_csv(out_path, index=False)
        print(f"📦 Unified RTD (Table 1) saved to {out_path}")

    # Summary
    elapsed_time = round(time.time() - start_time)
    print(f"\n📊 Summary (Table 1):")
    print(f"📂 {len(year_folders)} year folders found")
    print(f"🗃️  Already processed: {skipped_counter}")
    print(f"✨ Newly concatenated: {new_counter}")
    print(f"📏 Final shape: {unified_df.shape}")
    print(f"⏱️  {elapsed_time} seconds")

    return unified_df


def concatenate_table_2(
    input_data_subfolder: str,
    record_folder: str,
    record_txt: str,
    persist: bool = False,
    persist_folder: Optional[str] = None,
    csv_file_label: Optional[str] = None,
) -> pd.DataFrame:
    """
    Concatenate all Table 2 (quarterly/annual GDP) vintage files into unified RTD.

    Similar to concatenate_table_1 but handles quarterly and annual target periods.
    Sorts tp_* columns with quarters before annuals within each year.

    Args:
        input_data_subfolder: Root folder containing table_2/ subfolder with year folders.
        record_folder: Folder for storing concatenation records.
        record_txt: Filename for record file (e.g., 'concatenate_table_2.txt').
        persist: If True, save unified RTD to disk.
        persist_folder: Folder for saving output.
        csv_file_label: Custom filename for output (default: 'gdp_rtd_table_2_unified.csv').

    Returns:
        Unified RTD DataFrame with columns: industry, vintage, tp_YYYYqN, tp_YYYY...

    Example:
        >>> unified = concatenate_table_2(
        ...     "./data/input",
        ...     "./record",
        ...     "concat_table_2.txt",
        ...     persist=True
        ... )
    """
    start_time = time.time()
    print("\n⛓️  Starting Table 2 concatenation (row-based)...")

    processed_files = read_records(record_folder, record_txt)
    table_2_folder = os.path.join(input_data_subfolder, "table_2")

    if not os.path.exists(table_2_folder):
        print(f"⚠️  Table 2 folder not found: {table_2_folder}")
        return pd.DataFrame()

    year_folders = sorted([f for f in os.listdir(table_2_folder) if f.isdigit()], key=int)

    loaded_dfs: List[pd.DataFrame] = []
    skipped_counter = 0
    new_counter = 0

    # Load files from each year folder
    for year in year_folders:
        year_folder = os.path.join(table_2_folder, year)
        files = sorted([f for f in os.listdir(year_folder) if f.endswith((".csv", ".parquet"))])

        for file in files:
            if file in processed_files:
                skipped_counter += 1
                continue

            full_path = os.path.join(year_folder, file)
            try:
                if file.endswith(".parquet"):
                    df = pd.read_parquet(full_path)
                else:
                    df = pd.read_csv(full_path)

                loaded_dfs.append(df)
                processed_files.append(file)
                write_records(record_folder, record_txt, processed_files)
                new_counter += 1
            except Exception as e:
                print(f"⚠️  Error loading {file}: {e}")
                continue

    if not loaded_dfs:
        print("No new files to concatenate.")
        return pd.DataFrame()

    # Build union of all tp_* columns
    base_cols = ["industry", "vintage"]
    all_tp_cols = set()

    for df in loaded_dfs:
        for col in df.columns:
            if col.startswith("tp_"):
                all_tp_cols.add(col)

    # Sort tp_* columns: quarters first, then annual for each year
    tp_cols_sorted = sorted(list(all_tp_cols), key=target_period_quarterly_sort_key)

    # Final column schema
    final_cols = base_cols + tp_cols_sorted

    # Reindex each DataFrame to match full schema
    aligned_dfs = []
    for df in loaded_dfs:
        if "industry" not in df.columns or "vintage" not in df.columns:
            print("⚠️  Skipping DataFrame missing 'industry' or 'vintage' column")
            continue

        df = df.reindex(columns=final_cols)
        aligned_dfs.append(df)

    if not aligned_dfs:
        print("No valid DataFrames to concatenate.")
        return pd.DataFrame()

    # Vertical concatenation
    unified_df = pd.concat(aligned_dfs, axis=0, ignore_index=True)

    # Enforce consistent dtypes
    unified_df["industry"] = unified_df["industry"].astype(str)
    unified_df["vintage"] = unified_df["vintage"].astype(str)

    for col in tp_cols_sorted:
        unified_df[col] = pd.to_numeric(unified_df[col], errors="coerce").astype(float)

    # Persist if requested
    if persist:
        persist_folder = persist_folder or input_data_subfolder
        os.makedirs(persist_folder, exist_ok=True)
        fname = csv_file_label or "gdp_rtd_table_2_unified.csv"
        out_path = os.path.join(persist_folder, fname)
        unified_df.to_csv(out_path, index=False)
        print(f"📦 Unified RTD (Table 2) saved to {out_path}")

    # Summary
    elapsed_time = round(time.time() - start_time)
    print(f"\n📊 Summary (Table 2):")
    print(f"📂 {len(year_folders)} year folders found")
    print(f"🗃️  Already processed: {skipped_counter}")
    print(f"✨ Newly concatenated: {new_counter}")
    print(f"📏 Final shape: {unified_df.shape}")
    print(f"⏱️  {elapsed_time} seconds")

    return unified_df
