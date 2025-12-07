"""
Metadata Handler Module

This module handles GDP revision metadata extraction, base-year mapping,
and benchmark dataset generation for Peru's Real-Time Dataset (RTD).

Key responsibilities:
- Extract revision metadata from Weekly Report PDFs
- Apply base-year change point mappings
- Mark observations affected by base-year changes
- Generate sentinel-adjusted datasets for base-year discontinuities
- Convert RTDs to benchmark revision indicators

Author: Peru GDP RTD Team
Version: 1.0.0
"""

import os
import re
import time
from typing import Dict, List, Optional, Tuple

import fitz  # PyMuPDF
import numpy as np
import pandas as pd
from tqdm import tqdm

from peru_gdp_rtd.utils.data_manager import RecordManager


# ==============================================================================================
# PDF Metadata Extraction
# ==============================================================================================


def extract_dd_from_text(text: str) -> str:
    """
    Extract revision ID (1-2 digit number) from Spanish "en la Nota N° dd" pattern.

    Args:
        text: Text content from PDF page

    Returns:
        Extracted revision ID (1-2 digits) or "NaN" if not found

    Example:
        >>> extract_dd_from_text("...en la Nota N° 12...")
        "12"
    """
    match = re.search(r"en la Nota N°\s*(\d{1,2})", text)
    return match.group(1) if match else "NaN"


def extract_wr_update_from_pdf(pdf_path: str) -> Tuple[str, str]:
    """
    Extract revision numbers from first and second pages of a Weekly Report PDF.

    Each PDF page contains a revision calendar reference like "en la Nota N° 12".
    This function extracts these references from both Table 1 (page 1) and
    Table 2 (page 2) to track which estimates were revised.

    Args:
        pdf_path: Absolute path to Weekly Report PDF file

    Returns:
        Tuple of (revision_tab_1, revision_tab_2) as strings

    Example:
        >>> extract_wr_update_from_pdf("data/2017/ns-1-2017.pdf")
        ("12", "12")  # Both tables reference revision 12
    """
    doc = fitz.open(pdf_path)

    page_1_text = doc[0].get_text()
    revision_calendar_tab_1 = extract_dd_from_text(page_1_text)

    page_2_text = doc[1].get_text()
    revision_calendar_tab_2 = extract_dd_from_text(page_2_text)

    doc.close()
    return revision_calendar_tab_1, revision_calendar_tab_2


# ==============================================================================================
# Base-Year Mapping and Change Detection
# ==============================================================================================


def apply_base_years_block(
    df: pd.DataFrame, base_year_list: List[Dict[str, int]]
) -> pd.DataFrame:
    """
    Apply base-year mapping to DataFrame based on ordered change points.

    The function assigns 'base_year' values according to (year, wr) intervals
    defined in base_year_list. This handles Peru's GDP base-year changes
    (e.g., 1994, 2007, 2017 base years).

    Args:
        df: DataFrame with at least ['year', 'wr'] columns
        base_year_list: List of change point dictionaries with keys:
            - 'year' (int): Year of change
            - 'wr' (int): Weekly Report number of change
            - 'base_year' (int): Base year to apply from this point

    Returns:
        DataFrame with 'base_year' column filled

    Example:
        >>> df = pd.DataFrame({'year': [2017, 2017], 'wr': [1, 25]})
        >>> change_points = [
        ...     {'year': 2017, 'wr': 1, 'base_year': 2007},
        ...     {'year': 2017, 'wr': 20, 'base_year': 2017}
        ... ]
        >>> apply_base_years_block(df, change_points)
           year  wr  base_year
        0  2017   1       2007
        1  2017  25       2017
    """
    df = df.copy()

    if "base_year" not in df.columns:
        df["base_year"] = pd.NA

    points = sorted(base_year_list, key=lambda x: (x["year"], x["wr"]))

    def geq(df_inner, Y, W):
        """Check if (year, wr) >= (Y, W)"""
        return (df_inner["year"] > Y) | ((df_inner["year"] == Y) & (df_inner["wr"] >= W))

    def lt(df_inner, Y, W):
        """Check if (year, wr) < (Y, W)"""
        return (df_inner["year"] < Y) | ((df_inner["year"] == Y) & (df_inner["wr"] < W))

    # Handle everything before the first change point
    first = points[0]
    first_by = first["base_year"]
    first_y, first_w = first["year"], first["wr"]
    mask_before = lt(df, first_y, first_w)
    df.loc[mask_before & df["base_year"].isna(), "base_year"] = first_by

    # Handle each interval between points
    for i, pt in enumerate(points):
        y, w, by = pt["year"], pt["wr"], pt["base_year"]
        if i < len(points) - 1:
            nxt = points[i + 1]
            ny, nw = nxt["year"], nxt["wr"]
            mask_interval = geq(df, y, w) & lt(df, ny, nw)
        else:
            mask_interval = geq(df, y, w)

        df.loc[mask_interval & df["base_year"].isna(), "base_year"] = by

    return df


def mark_base_year_affected(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mark rows where base_year changes from previous row.

    Creates 'base_year_affected' column:
    - 1 if base_year differs from previous row
    - 0 otherwise (including first row)

    This identifies vintages where GDP series experienced base-year rebasing,
    making comparisons with previous vintages invalid.

    Args:
        df: DataFrame with 'base_year' column

    Returns:
        DataFrame with 'base_year_affected' column added

    Example:
        >>> df = pd.DataFrame({
        ...     'year': [2017, 2017, 2017],
        ...     'wr': [1, 19, 20],
        ...     'base_year': [2007, 2007, 2017]
        ... })
        >>> mark_base_year_affected(df)
           year  wr  base_year  base_year_affected
        0  2017   1       2007                   0
        1  2017  19       2007                   0
        2  2017  20       2017                   1
    """
    df = df.sort_values(["year", "wr"]).reset_index(drop=True).copy()

    if "base_year_affected" not in df.columns:
        df["base_year_affected"] = 0

    changed = df["base_year"].ne(df["base_year"].shift())
    df.loc[:, "base_year_affected"] = changed.astype(int)
    df.loc[0, "base_year_affected"] = 0  # First row is always 0

    return df


def update_metadata(
    metadata_folder: str,
    input_pdf_folder: str,
    record_folder: str,
    record_txt: str,
    wr_metadata_csv: str,
    base_year_list: List[Dict[str, int]],
) -> pd.DataFrame:
    """
    Update metadata by processing new Weekly Report PDFs and applying base-year mappings.

    Workflow:
    1. Load existing metadata CSV (or create empty if none exists)
    2. Identify unprocessed year folders
    3. Extract revision data from PDF files
    4. Detect benchmark revisions (both tables reference same revision)
    5. Apply base-year mapping to new rows
    6. Mark base-year affected rows
    7. Concatenate with existing metadata
    8. Save updated metadata to CSV

    Args:
        metadata_folder: Folder where metadata CSV is stored
        input_pdf_folder: Folder where Weekly Report PDFs are organized by year
        record_folder: Folder for processing records
        record_txt: Record file name for tracking processed years
        wr_metadata_csv: Metadata CSV file name
        base_year_list: List of base-year change point dictionaries

    Returns:
        Updated DataFrame with columns:
        - year: Publication year
        - wr: Weekly Report number (1-52)
        - month: Month index (1-12)
        - revision_calendar_tab_1: Revision referenced in Table 1
        - revision_calendar_tab_2: Revision referenced in Table 2
        - benchmark_revision: 1 if both tables reference same revision, 0 otherwise
        - base_year: Applicable base year
        - base_year_affected: 1 if base_year changed from previous row

    Example:
        >>> base_year_list = [
        ...     {'year': 2011, 'wr': 1, 'base_year': 2007},
        ...     {'year': 2017, 'wr': 20, 'base_year': 2017}
        ... ]
        >>> metadata = update_metadata(
        ...     metadata_folder="data/metadata",
        ...     input_pdf_folder="data/inputs",
        ...     record_folder="data/records",
        ...     record_txt="metadata_processed.txt",
        ...     wr_metadata_csv="wr_metadata.csv",
        ...     base_year_list=base_year_list
        ... )
    """
    start_time = time.time()
    print("\n🔄📋 Starting metadata update process...")

    # 1) Read or initialize metadata
    metadata_path = os.path.join(metadata_folder, wr_metadata_csv)
    if os.path.exists(metadata_path):
        metadata = pd.read_csv(metadata_path)
    else:
        metadata = pd.DataFrame(
            columns=[
                "year",
                "wr",
                "month",
                "revision_calendar_tab_1",
                "revision_calendar_tab_2",
                "benchmark_revision",
                "base_year",
                "base_year_affected",
            ]
        )

    # 2) Read processed years from record
    record_manager = RecordManager(record_folder=record_folder, record_file=record_txt)
    processed_years = record_manager.read_records()

    # 3) List years to process
    years = [
        d
        for d in sorted(os.listdir(input_pdf_folder))
        if os.path.isdir(os.path.join(input_pdf_folder, d)) and d != "_quarantine"
    ]
    years_to_process = [y for y in years if y not in processed_years]

    if not years_to_process:
        print("✅ No new revisions to process.")
        return metadata

    new_rows = []

    # 4) Extract revision data from PDF files
    print(f"📂 Processing {len(years_to_process)} new year(s)...")
    for year in tqdm(years_to_process, desc="Extracting metadata", colour="blue"):
        year_folder = os.path.join(input_pdf_folder, year)
        pdf_files = sorted(
            [f for f in os.listdir(year_folder) if f.endswith(".pdf")],
            key=lambda x: int(re.search(r"ns-(\d+)-", x).group(1)),
        )

        for month_idx, pdf_filename in enumerate(pdf_files, start=1):
            pdf_path = os.path.join(year_folder, pdf_filename)

            # Extract wr_number and year from filename
            m = re.search(r"ns-(\d{1,2})-(\d{4})", pdf_filename)
            if not m:
                continue
            wr_number = int(m.group(1))
            year_int = int(m.group(2))

            # Extract revision numbers from PDF pages
            rev1, rev2 = extract_wr_update_from_pdf(pdf_path)

            # Build row (base_year will be filled later)
            new_rows.append(
                {
                    "year": year_int,
                    "wr": wr_number,
                    "month": month_idx,
                    "revision_calendar_tab_1": (
                        int(rev1) if str(rev1).isdigit() else np.nan
                    ),
                    "revision_calendar_tab_2": (
                        int(rev2) if str(rev2).isdigit() else np.nan
                    ),
                    "benchmark_revision": (
                        1
                        if (
                            str(rev1).isdigit()
                            and str(rev2).isdigit()
                            and int(rev1) == int(rev2)
                        )
                        else 0
                    ),
                    "base_year": np.nan,
                    "base_year_affected": 0,
                }
            )

    # 5) Build DataFrame for new rows
    new_df = pd.DataFrame(new_rows)

    # 6) Apply base-year mapping to new rows
    new_df = apply_base_years_block(new_df, base_year_list)

    # 7) Mark base-year changes within new rows
    new_df = mark_base_year_affected(new_df)

    # 8) Concatenate with existing metadata
    updated = pd.concat([metadata, new_df], ignore_index=True)

    # 9) Enforce dtypes
    int_cols = [
        "year",
        "wr",
        "month",
        "benchmark_revision",
        "base_year",
        "base_year_affected",
    ]
    for col in int_cols:
        if col in updated.columns:
            updated[col] = updated[col].astype("Int64")

    for col in ["revision_calendar_tab_1", "revision_calendar_tab_2"]:
        if col in updated.columns:
            updated[col] = pd.to_numeric(updated[col], errors="coerce").astype("Int64")

    # 10) Save updated metadata
    os.makedirs(metadata_folder, exist_ok=True)
    updated.to_csv(metadata_path, index=False)
    print(f"💾 Updated metadata saved to {metadata_path}")

    # 11) Update processed records
    record_manager.write_records(processed_years + years_to_process)

    # 12) Summary
    elapsed_time = round(time.time() - start_time)
    print(f"\n📊 Summary (Metadata Update):")
    print(f"📂 {len(years)} total years in input folder")
    print(f"🗃️ {len(processed_years)} already processed years")
    print(f"🔹 {len(years_to_process)} new years processed")
    print(f"📝 {len(new_rows)} new metadata rows extracted")
    print(f"⏱️ Total elapsed time: {elapsed_time} seconds")

    return updated


# ==============================================================================================
# Base-Year Sentinel Application
# ==============================================================================================


def apply_base_year_sentinel(
    base_year_vintages: List[str],
    sentinel: float = -999999.0,
    output_data_subfolder: str = ".",
    csv_file_labels: Optional[List[str]] = None,
) -> Dict[str, pd.DataFrame]:
    """
    Apply sentinel value to mark GDP estimates invalid due to base-year changes.

    When Peru's GDP undergoes base-year rebasing, estimates published before
    the change become incomparable with post-change estimates. This function
    marks such invalid comparisons by replacing values with a sentinel.

    Algorithm:
    1. For each base-year vintage (e.g., "2017m20"):
       - Find the row where vintage == base_year_vintage
       - Identify tp_* columns with non-NaN values in that row
       - Replace non-NaN values ABOVE that row with sentinel
    2. This preserves NaN structure while marking invalid values

    Args:
        base_year_vintages: List of vintage strings where base-year changed
            Example: ["2007m1", "2014m3", "2017m20"]
        sentinel: Numeric value to mark invalid entries (default: -999999.0)
        output_data_subfolder: Folder containing RTD CSV files
        csv_file_labels: List of CSV file labels to process
            If None, defaults to ["monthly_gdp_rtd.csv", "quarterly_annual_gdp_rtd.csv"]

    Returns:
        Dictionary mapping adjusted filenames to DataFrames
        Keys: "by_adjusted_{original_filename}"

    Example:
        >>> base_year_vintages = ["2017m20"]  # Base year changed in WR 20/2017
        >>> adjusted = apply_base_year_sentinel(
        ...     base_year_vintages=base_year_vintages,
        ...     output_data_subfolder="data/outputs",
        ...     csv_file_labels=["monthly_gdp_rtd"]
        ... )
        >>> # Vintages before 2017m20 will have sentinel for tp_* columns
        >>> # that first appeared in 2017m20
    """
    start_time = time.time()
    print("\n🔧 Starting base-year sentinel application...")

    if csv_file_labels is None:
        csv_file_labels = ["monthly_gdp_rtd", "quarterly_annual_gdp_rtd"]

    # Ensure labels don't have .csv extension
    csv_file_labels = [lbl.replace(".csv", "") for lbl in csv_file_labels]

    processed_data = {}

    for csv_file_label in tqdm(
        csv_file_labels, desc="Applying sentinel", colour="yellow"
    ):
        # 1) Load CSV
        csv_path = os.path.join(output_data_subfolder, f"{csv_file_label}.csv")
        if not os.path.exists(csv_path):
            print(f"⚠️ File not found, skipping: {csv_path}")
            continue

        df = pd.read_csv(csv_path)

        # 2) Validate required columns
        if "industry" not in df.columns or "vintage" not in df.columns:
            raise ValueError(
                f"CSV must have 'industry' and 'vintage' columns: {csv_path}"
            )

        df["industry"] = df["industry"].astype(str)
        df["vintage"] = df["vintage"].astype(str)

        # 3) Identify tp_* columns
        tp_cols = [col for col in df.columns if col.startswith("tp_")]

        # 4) Process each base-year vintage
        for by_vintage in base_year_vintages:
            by_vintage = str(by_vintage)

            # Find rows matching the base-year vintage
            mask_v = df["vintage"] == by_vintage
            if not mask_v.any():
                continue

            # Get index of first base-year row
            base_idx = df.index[mask_v].tolist()[0]

            # 5) Identify relevant tp_* columns (non-NaN in base-year row)
            relevant_cols = []
            for col in tp_cols:
                if pd.notna(df.loc[base_idx, col]):
                    relevant_cols.append(col)

            # 6) Replace non-NaN values above base-row
            for col in relevant_cols:
                for i in range(base_idx):
                    if pd.notna(df.loc[i, col]):
                        df.loc[i, col] = sentinel

        # 7) Enforce dtypes
        for col in tp_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # 8) Save adjusted dataset
        adjusted_csv_label = f"by_adjusted_{csv_file_label}"
        adjusted_csv_path = os.path.join(
            output_data_subfolder, f"{adjusted_csv_label}.csv"
        )
        df.to_csv(adjusted_csv_path, index=False)

        processed_data[adjusted_csv_label] = df
        print(f"💾 Saved: {adjusted_csv_label}.csv")

    # 9) Summary
    elapsed_time = round(time.time() - start_time)
    print(f"\n📊 Summary (Base-Year Sentinel Application):")
    print(f"📂 {len(csv_file_labels)} files processed")
    print(f"🔹 Sentinel value: {sentinel}")
    print(f"⏱️ Total elapsed time: {elapsed_time} seconds")

    return processed_data


# ==============================================================================================
# Benchmark Dataset Generation
# ==============================================================================================


def convert_to_benchmark_dataset(
    output_data_subfolder: str,
    csv_file_labels: List[str],
    metadata_folder: str,
    wr_metadata_csv: str,
    record_folder: str,
    record_txt: str,
    benchmark_dataset_labels: List[str],
) -> Dict[str, pd.DataFrame]:
    """
    Generate benchmark revision indicator datasets from RTDs.

    Converts GDP growth rate RTDs into binary indicator datasets where:
    - 1.0 indicates a benchmark revision vintage
    - 0.0 indicates a non-benchmark vintage
    - NaN remains NaN

    Benchmark revisions occur when BCRP revises both monthly (Table 1) and
    quarterly/annual (Table 2) estimates simultaneously, indicating a
    methodological update or major data source incorporation.

    Args:
        output_data_subfolder: Folder containing input RTD CSV files
        csv_file_labels: List of input CSV filenames (without .csv)
        metadata_folder: Folder where metadata CSV is stored
        wr_metadata_csv: Metadata CSV filename
        record_folder: Folder for processing records
        record_txt: Record filename for tracking processed files
        benchmark_dataset_labels: List of output filenames (without .csv)

    Returns:
        Dictionary mapping benchmark dataset labels to DataFrames

    Example:
        >>> benchmark_data = convert_to_benchmark_dataset(
        ...     output_data_subfolder="data/outputs",
        ...     csv_file_labels=["monthly_gdp_rtd", "quarterly_annual_gdp_rtd"],
        ...     metadata_folder="data/metadata",
        ...     wr_metadata_csv="wr_metadata.csv",
        ...     record_folder="data/records",
        ...     record_txt="benchmark_processed.txt",
        ...     benchmark_dataset_labels=["monthly_benchmark", "quarterly_benchmark"]
        ... )
    """
    start_time = time.time()
    print("\n🔄📊 Starting benchmark dataset generation...")

    # 1) Validate input lengths
    if len(csv_file_labels) != len(benchmark_dataset_labels):
        raise ValueError(
            "csv_file_labels and benchmark_dataset_labels must have same length"
        )

    # 2) Load processing records
    record_manager = RecordManager(record_folder=record_folder, record_file=record_txt)
    processed_files = record_manager.read_records()
    processed_results = {}

    # 3) Load and filter metadata
    metadata_path = os.path.join(metadata_folder, wr_metadata_csv)
    if not os.path.exists(metadata_path):
        raise FileNotFoundError(f"Metadata file not found: {metadata_path}")

    metadata = pd.read_csv(metadata_path)
    metadata_filtered = metadata[metadata["benchmark_revision"] == 1]

    # 4) Build benchmark mapping dictionary
    benchmark_map = {
        f"{str(year)}m{str(month).zfill(2)}": True
        for year, month in zip(
            metadata_filtered["year"].astype(str),
            metadata_filtered["month"].astype(int),
        )
    }

    # 5) Normalize benchmark keys (e.g., 1997m02 -> 1997m2)
    def normalize_key(v):
        match = re.match(r"^(\d{4})m0?(\d{1,2})$", v)
        if match:
            y, m = match.groups()
            return f"{y}m{int(m)}"
        return v

    benchmark_map = {normalize_key(k): v for k, v in benchmark_map.items()}

    # 6) Process each dataset
    for csv_label, benchmark_label in zip(csv_file_labels, benchmark_dataset_labels):
        # Ensure labels don't have .csv extension
        csv_label_clean = csv_label.replace(".csv", "")
        benchmark_label_clean = benchmark_label.replace(".csv", "")

        csv_path = os.path.join(output_data_subfolder, f"{csv_label_clean}.csv")

        if csv_label_clean in processed_files:
            print(f"⏭️ Skipping already processed: {csv_label_clean}.csv")
            continue

        if not os.path.exists(csv_path):
            print(f"⚠️ File not found, skipping: {csv_path}")
            continue

        print(f"\n🔹 Processing dataset: {csv_label_clean}.csv")
        df = pd.read_csv(csv_path)

        if "vintage" not in df.columns:
            raise ValueError(f"File must contain 'vintage' column: {csv_label_clean}")

        # 6.1) Normalize vintage identifiers
        df["vintage"] = (
            df["vintage"]
            .astype(str)
            .str.strip()
            .str.lower()
            .str.replace(r"[^0-9m]", "", regex=True)
        )

        # 6.2) Identify tp_* columns
        tp_cols = [c for c in df.columns if c.startswith("tp_")]
        if not tp_cols:
            tp_cols = [c for c in df.columns if c not in ["industry", "vintage"]]

        # Ensure numeric type
        df[tp_cols] = df[tp_cols].astype(float)

        # 6.3) Diagnostic report
        vintages_df = set(df["vintage"].unique())
        vintages_map = set(benchmark_map.keys())
        matched = vintages_map.intersection(vintages_df)
        unmatched = vintages_map - vintages_df

        print(f"🔍 Matching diagnostics for {csv_label_clean}.csv:")
        print(f"   Total vintages in data: {len(vintages_df)}")
        print(f"   Total benchmark vintages: {len(vintages_map)}")
        print(f"   ✅ Matched vintages: {len(matched)} / {len(vintages_map)}")
        print(f"   ⚠️ Unmatched vintages: {len(unmatched)}")
        if unmatched:
            print(f"   Example unmatched: {list(sorted(unmatched))[:5]}")

        # 6.4) Apply vectorized replacement logic
        mask_benchmark = df["vintage"].isin(vintages_map)

        # Replace all non-NaN values with 0.0
        df[tp_cols] = df[tp_cols].where(df[tp_cols].isna(), 0.0)

        # Replace benchmark vintages with 1.0
        if mask_benchmark.any():
            df.loc[mask_benchmark, tp_cols] = df.loc[mask_benchmark, tp_cols].where(
                df.loc[mask_benchmark, tp_cols].isna(), 1.0
            )

        # Enforce float type
        df[tp_cols] = df[tp_cols].astype(float)

        # 6.5) Save processed dataset
        output_path = os.path.join(
            output_data_subfolder, f"{benchmark_label_clean}.csv"
        )
        df.to_csv(output_path, index=False)
        processed_results[benchmark_label_clean] = df

        processed_files.append(csv_label_clean)
        print(f"💾 Saved benchmark dataset: {benchmark_label_clean}.csv")
        print(f"   Rows: {len(df)}, Columns: {len(df.columns)}")

    # 7) Update records and summarize
    record_manager.write_records(processed_files)

    elapsed_time = round(time.time() - start_time)
    print(f"\n📊 Summary (Benchmark Dataset Generation):")
    print(f"📂 {len(csv_file_labels)} input files")
    print(f"🔹 {len(processed_results)} benchmark datasets created")
    print(f"🗃️ Record updated: {record_txt}")
    print(f"⏱️ Total elapsed time: {elapsed_time} seconds")

    return processed_results
