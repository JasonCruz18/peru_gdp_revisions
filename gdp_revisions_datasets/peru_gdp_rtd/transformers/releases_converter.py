"""
Releases Converter Module

This module converts vintage-format Real-Time Datasets (RTDs) into
release-format datasets for revision analysis.

Vintage format: Each row is an industry-vintage pair with tp_* columns
Release format: Each row is a target period with industry_release columns

The transformation aligns non-NaN values chronologically, creating a
release sequence (1st, 2nd, 3rd estimate) for each industry-target period.

Author: Peru GDP RTD Team
Version: 1.0.0
"""

import os
import time
from typing import Dict, List

import numpy as np
import pandas as pd
from tqdm import tqdm

from peru_gdp_rtd.utils.data_manager import RecordManager


def convert_to_releases_dataset(
    output_data_subfolder: str,
    csv_file_labels: List[str],
    record_folder: str,
    record_txt: str,
    releases_dataset_labels: List[str],
) -> Dict[str, pd.DataFrame]:
    """
    Convert vintage-format RTDs into release-format datasets.

    Transformation logic:
    1. For each industry, sort vintages chronologically
    2. For each tp_* column, extract non-NaN values in vintage order
    3. Align these values vertically to create release sequences
       - 1st non-NaN = release 1 (first estimate)
       - 2nd non-NaN = release 2 (first revision)
       - 3rd non-NaN = release 3 (second revision), etc.
    4. Reshape to create industry_release columns

    Input format (vintage):
        | industry | vintage | tp_2017m1 | tp_2017m2 | ...
        |----------|---------|-----------|-----------|
        | agr      | 2017m2  | NaN       | 1.2       |
        | agr      | 2017m3  | 1.5       | 1.3       |
        | agr      | 2017m4  | 1.4       | 1.2       |

    Output format (releases):
        | target_period | agr_1 | agr_2 | agr_3 | ...
        |---------------|-------|-------|-------|
        | 2017m1        | 1.5   | 1.4   | NaN   |  # 1st=2017m3, 2nd=2017m4
        | 2017m2        | 1.2   | 1.3   | 1.2   |  # 1st=2017m2, 2nd=2017m3, 3rd=2017m4

    Args:
        output_data_subfolder: Folder containing input RTD CSV files
        csv_file_labels: List of input CSV filenames (without .csv)
        record_folder: Folder for processing records
        record_txt: Record filename for tracking processed files
        releases_dataset_labels: List of output CSV filenames (without .csv)

    Returns:
        Dictionary mapping output labels to release-format DataFrames

    Example:
        >>> releases = convert_to_releases_dataset(
        ...     output_data_subfolder="data/outputs",
        ...     csv_file_labels=["monthly_gdp_rtd", "quarterly_annual_gdp_rtd"],
        ...     record_folder="data/records",
        ...     record_txt="releases_processed.txt",
        ...     releases_dataset_labels=["monthly_releases", "quarterly_releases"]
        ... )
    """
    start_time = time.time()
    print("\n🧮 Starting conversion to releases dataset(s)...")

    # 1) Validate input lengths
    if len(csv_file_labels) != len(releases_dataset_labels):
        raise ValueError(
            "csv_file_labels and releases_dataset_labels must have same length"
        )

    # 2) Load processing records
    record_manager = RecordManager(record_folder=record_folder, record_file=record_txt)
    processed_files = record_manager.read_records()
    processed_results = {}

    # 3) Process each dataset
    for csv_label, release_label in zip(csv_file_labels, releases_dataset_labels):
        # Ensure labels don't have .csv extension
        csv_label_clean = csv_label.replace(".csv", "")
        release_label_clean = release_label.replace(".csv", "")

        csv_path = os.path.join(output_data_subfolder, f"{csv_label_clean}.csv")

        # Check if already processed
        if csv_label_clean in processed_files:
            print(f"⏭️ Skipping already processed: {csv_label_clean}.csv")
            continue

        if not os.path.exists(csv_path):
            print(f"⚠️ File not found, skipping: {csv_path}")
            continue

        print(f"\n🔄 Processing file: {csv_label_clean}.csv")
        df = pd.read_csv(csv_path)

        # 4) Validate required columns
        if "industry" not in df.columns or "vintage" not in df.columns:
            raise ValueError(
                f"CSV must have 'industry' and 'vintage' columns: {csv_label_clean}"
            )

        df["industry"] = df["industry"].astype(str)
        df["vintage"] = df["vintage"].astype(str)

        # 5) Extract year and month from vintage for chronological sorting
        df["year"] = df["vintage"].str.extract(r"(\d{4})").astype(int)
        df["month"] = df["vintage"].str.extract(r"m(\d{1,2})").astype(int)
        df = df.sort_values(["industry", "year", "month"], ignore_index=True)

        # 6) Identify tp_* columns
        tp_cols = [col for col in df.columns if col.startswith("tp_")]
        if not tp_cols:
            raise ValueError(f"No tp_* columns found in {csv_label_clean}.csv")

        releases_df_list = []

        # 7) Process each industry independently
        print(f"🏭 Processing {df['industry'].nunique()} industries...")
        for industry, group in tqdm(
            df.groupby("industry"), desc="Converting to releases", colour="cyan"
        ):
            group = group.reset_index(drop=True)

            # Convert tp_* data to NumPy for efficient column-wise processing
            tp_values = group[tp_cols].to_numpy()

            # Determine maximum number of releases (max non-NaN count in any column)
            max_releases = np.max(
                [
                    np.count_nonzero(~np.isnan(tp_values[:, i]))
                    for i in range(tp_values.shape[1])
                ]
            )

            # Create structure for release-aligned data
            industry_releases = np.full((max_releases, len(tp_cols)), np.nan)

            # Align non-NaN values vertically (release by release)
            for j, col in enumerate(tp_cols):
                non_nan_vals = tp_values[~np.isnan(tp_values[:, j]), j]
                industry_releases[: len(non_nan_vals), j] = non_nan_vals

            # Build DataFrame for this industry
            industry_df = pd.DataFrame(industry_releases, columns=tp_cols)
            industry_df.insert(0, "release", range(1, len(industry_df) + 1))
            industry_df.insert(0, "industry", industry)

            # Drop rows that are completely NaN
            industry_df.dropna(how="all", subset=tp_cols, inplace=True)

            releases_df_list.append(industry_df)

        # 8) Concatenate all industries
        releases_df = pd.concat(releases_df_list, ignore_index=True)

        # 9) Reshape: Melt to long format
        releases_df = releases_df.melt(
            id_vars=["industry", "release"],
            value_vars=tp_cols,
            var_name="target_period",
            value_name="value",
        )

        # Clean target_period by removing 'tp_' prefix
        releases_df["target_period"] = releases_df["target_period"].str.replace(
            "tp_", "", regex=False
        )

        # 10) Pivot to create industry_release columns
        releases_df_pivot = releases_df.pivot_table(
            index="target_period", columns=["industry", "release"], values="value"
        )

        # Flatten multi-level columns
        releases_df_pivot.columns = [
            f"{industry}_{release}" for industry, release in releases_df_pivot.columns
        ]
        releases_df_pivot.reset_index(inplace=True)

        # 11) Sort target_period chronologically
        # Extract year and month for sorting
        releases_df_pivot["year"] = (
            releases_df_pivot["target_period"].str.extract(r"(\d{4})").astype("Int64")
        )
        releases_df_pivot["month"] = (
            releases_df_pivot["target_period"]
            .str.extract(r"m(\d{1,2})")
            .astype("Int64")
        )
        releases_df_pivot = releases_df_pivot.sort_values(
            ["year", "month"], ignore_index=True
        )

        # Drop temporary sorting columns
        releases_df_pivot.drop(columns=["year", "month"], inplace=True)

        # 12) Save release dataset
        release_path = os.path.join(
            output_data_subfolder, f"{release_label_clean}.csv"
        )
        releases_df_pivot.to_csv(release_path, index=False)
        processed_results[release_label_clean] = releases_df_pivot

        # Update processed records
        processed_files.append(csv_label_clean)

        print(f"💾 Saved release dataset: {release_label_clean}.csv")
        print(
            f"   📏 Rows: {len(releases_df_pivot)}, "
            f"Columns: {len(releases_df_pivot.columns)}"
        )

    # 13) Update record file
    record_manager.write_records(processed_files)

    # 14) Summary
    elapsed_time = round(time.time() - start_time)
    print(f"\n📊 Summary (Conversion to Releases Dataset):")
    print(f"📂 {len(csv_file_labels)} input files")
    print(f"🔹 {len(processed_results)} release datasets created")
    print(f"🗃️ Record updated: {record_txt}")
    print(f"⏱️ Total elapsed time: {elapsed_time} seconds")

    return processed_results
