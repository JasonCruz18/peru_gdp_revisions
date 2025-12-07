"""
Workflow orchestration runners for OLD and NEW dataset processing.

This module provides high-level orchestrator functions that coordinate the entire
processing workflow for GDP tables from both OLD (CSV-based) and NEW (PDF-based)
datasets. Each runner:
1. Walks through year folders
2. Loads processing records for idempotency
3. Extracts tables from source files
4. Cleans tables using appropriate cleaner classes
5. Reshapes into vintage format
6. Optionally persists vintages to disk
7. Provides progress tracking and summary reporting
"""

import os
import time
from typing import Dict, Optional, Tuple

import pandas as pd
from tqdm import tqdm

from peru_gdp_rtd.cleaners import NewTableCleaner, OldTableCleaner
from peru_gdp_rtd.processors.metadata import (
    extract_table,
    ns_sort_key,
    parse_ns_meta,
    read_records,
    save_df,
    write_records,
)
from peru_gdp_rtd.transformers import VintagesPreparator


def old_table_1_runner(
    input_csv_folder: str,
    record_folder: str,
    record_txt: str,
    persist: bool = False,
    persist_folder: Optional[str] = None,
    pipeline_version: str = "v1.0.0",
    sep: str = ";",
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]:
    """
    Process all OLD WR CSV files for Table 1 (monthly GDP).

    Coordinates complete workflow: record management, CSV reading, cleaning,
    vintage reshaping, and optional persistence.

    Args:
        input_csv_folder: Root folder containing year subfolders with CSV files.
        record_folder: Folder for storing processing records.
        record_txt: Filename for record file (e.g., 'table_1_old.txt').
        persist: If True, persist vintages to disk.
        persist_folder: Root folder for persisted outputs (default: './data/input').
        pipeline_version: Version stamp for processed tables.
        sep: CSV separator (default: ';' for OLD dataset).

    Returns:
        Tuple of (raw_tables_dict, clean_tables_dict, vintages_dict).

    Example:
        >>> raw, clean, vintages = old_table_1_runner(
        ...     "./data/old/csv",
        ...     "./record",
        ...     "table_1_old.txt",
        ...     persist=True
        ... )
        >>> print(f"Processed {len(vintages)} tables")
    """
    start_time = time.time()
    print("\n>u Starting OLD Table 1 processing...\n")

    cleaner = OldTableCleaner()
    prep = VintagesPreparator()
    records = read_records(record_folder, record_txt)
    processed = set(records)

    raw_tables: Dict[str, pd.DataFrame] = {}
    clean_tables: Dict[str, pd.DataFrame] = {}
    vintages: Dict[str, pd.DataFrame] = {}

    new_counter = 0
    skipped_counter = 0
    skipped_years: Dict[str, int] = {}

    years = [
        d
        for d in sorted(os.listdir(input_csv_folder))
        if os.path.isdir(os.path.join(input_csv_folder, d)) and d != "_quarantine"
    ]
    total_year_folders = len(years)

    if persist:
        base_out = persist_folder or os.path.join("data", "input")
        out_root = os.path.join(base_out, "old_table_1")
        os.makedirs(out_root, exist_ok=True)

    for year in years:
        folder_path = os.path.join(input_csv_folder, year)
        csv_files = sorted(
            [f for f in os.listdir(folder_path) if f.endswith(".csv")],
            key=ns_sort_key,
        )

        month_order_map = prep.build_month_order_map(folder_path)

        if not csv_files:
            continue

        already = [f for f in csv_files if f in processed]
        if len(already) == len(csv_files):
            skipped_years[year] = len(already)
            skipped_counter += len(already)
            continue

        print(f"\n=Â Processing Table 1 in {year}\n")
        folder_new_count = 0
        folder_skipped_count = 0

        pbar = tqdm(
            csv_files,
            desc=f">u {year}",
            unit="CSV",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
            colour="#E6004C",
            leave=False,
        )

        for filename in pbar:
            if filename in processed:
                folder_skipped_count += 1
                continue

            issue, yr = parse_ns_meta(filename)
            if not issue:
                folder_skipped_count += 1
                continue

            csv_path = os.path.join(folder_path, filename)
            try:
                raw = pd.read_csv(csv_path, sep=sep)
                if raw is None:
                    folder_skipped_count += 1
                    continue

                key = f"{os.path.splitext(filename)[0].replace('-', '_')}_1"
                raw_tables[key] = raw.copy()

                clean = cleaner.clean_table_1(raw)
                clean.insert(0, "year", yr)
                clean.insert(1, "wr", issue)
                clean.attrs["pipeline_version"] = pipeline_version

                clean_tables[key] = clean.copy()

                vintage = prep.prepare_table_1(clean, filename, month_order_map)
                vintage.attrs["pipeline_version"] = pipeline_version
                vintages[key] = vintage

                if persist:
                    ns_code = os.path.splitext(filename)[0]
                    out_dir = os.path.join(out_root, str(yr))
                    out_path = os.path.join(out_dir, f"{ns_code}.parquet")
                    save_df(vintage, out_path)

                processed.add(filename)
                folder_new_count += 1
            except Exception as e:
                print(f"   {filename}: {e}")
                folder_skipped_count += 1

        pbar.clear()
        pbar.close()

        fb = tqdm(
            total=len(csv_files),
            desc=f" {year}",
            unit="CSV",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
            colour="#3366FF",
            leave=True,
        )
        fb.update(len(csv_files))
        fb.close()

        new_counter += folder_new_count
        skipped_counter += folder_skipped_count
        write_records(record_folder, record_txt, list(processed))

    if skipped_years:
        years_summary = ", ".join(skipped_years.keys())
        total_skipped = sum(skipped_years.values())
        print(f"\né {total_skipped} tables already processed for years: {years_summary}")

    elapsed_time = round(time.time() - start_time)
    print(f"\n=Ê Summary:\n")
    print(f"=Â {total_year_folders} year folders found")
    print(f"=Ã  Already processed: {skipped_counter}")
    print(f"( Newly processed: {new_counter}")
    print(f"ñ  {elapsed_time} seconds")

    return raw_tables, clean_tables, vintages


def old_table_2_runner(
    input_csv_folder: str,
    record_folder: str,
    record_txt: str,
    persist: bool = False,
    persist_folder: Optional[str] = None,
    pipeline_version: str = "v1.0.0",
    sep: str = ";",
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]:
    """
    Process all OLD WR CSV files for Table 2 (quarterly/annual GDP).

    Similar to old_table_1_runner but for Table 2 data.

    Args:
        input_csv_folder: Root folder containing year subfolders with CSV files.
        record_folder: Folder for storing processing records.
        record_txt: Filename for record file (e.g., 'table_2_old.txt').
        persist: If True, persist vintages to disk.
        persist_folder: Root folder for persisted outputs.
        pipeline_version: Version stamp for processed tables.
        sep: CSV separator (default: ';').

    Returns:
        Tuple of (raw_tables_dict, clean_tables_dict, vintages_dict).
    """
    start_time = time.time()
    print("\n>u Starting OLD Table 2 processing...\n")

    cleaner = OldTableCleaner()
    prep = VintagesPreparator()
    records = read_records(record_folder, record_txt)
    processed = set(records)

    raw_tables: Dict[str, pd.DataFrame] = {}
    clean_tables: Dict[str, pd.DataFrame] = {}
    vintages: Dict[str, pd.DataFrame] = {}

    new_counter = 0
    skipped_counter = 0
    skipped_years: Dict[str, int] = {}

    years = [
        d
        for d in sorted(os.listdir(input_csv_folder))
        if os.path.isdir(os.path.join(input_csv_folder, d)) and d != "_quarantine"
    ]
    total_year_folders = len(years)

    if persist:
        base_out = persist_folder or os.path.join("data", "input")
        out_root = os.path.join(base_out, "old_table_2")
        os.makedirs(out_root, exist_ok=True)

    for year in years:
        folder_path = os.path.join(input_csv_folder, year)
        csv_files = sorted(
            [f for f in os.listdir(folder_path) if f.endswith(".csv")],
            key=ns_sort_key,
        )

        month_order_map = prep.build_month_order_map(folder_path)

        if not csv_files:
            continue

        already = [f for f in csv_files if f in processed]
        if len(already) == len(csv_files):
            skipped_years[year] = len(already)
            skipped_counter += len(already)
            continue

        print(f"\n=Â Processing Table 2 in {year}\n")
        folder_new_count = 0
        folder_skipped_count = 0

        pbar = tqdm(
            csv_files,
            desc=f">u {year}",
            unit="CSV",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
            colour="#E6004C",
            leave=False,
        )

        for filename in pbar:
            if filename in processed:
                folder_skipped_count += 1
                continue

            issue, yr = parse_ns_meta(filename)
            if not issue:
                folder_skipped_count += 1
                continue

            csv_path = os.path.join(folder_path, filename)
            try:
                raw = pd.read_csv(csv_path, sep=sep)
                if raw is None:
                    folder_skipped_count += 1
                    continue

                key = f"{os.path.splitext(filename)[0].replace('-', '_')}_2"
                raw_tables[key] = raw.copy()

                clean = cleaner.clean_table_2(raw)
                clean.insert(0, "year", yr)
                clean.insert(1, "wr", issue)
                clean.attrs["pipeline_version"] = pipeline_version

                clean_tables[key] = clean.copy()

                vintage = prep.prepare_table_2(clean, filename, month_order_map)
                vintage.attrs["pipeline_version"] = pipeline_version
                vintages[key] = vintage

                if persist:
                    ns_code = os.path.splitext(filename)[0]
                    out_dir = os.path.join(out_root, str(yr))
                    out_path = os.path.join(out_dir, f"{ns_code}.parquet")
                    save_df(vintage, out_path)

                processed.add(filename)
                folder_new_count += 1
            except Exception as e:
                print(f"   {filename}: {e}")
                folder_skipped_count += 1

        pbar.clear()
        pbar.close()

        fb = tqdm(
            total=len(csv_files),
            desc=f" {year}",
            unit="CSV",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
            colour="#3366FF",
            leave=True,
        )
        fb.update(len(csv_files))
        fb.close()

        new_counter += folder_new_count
        skipped_counter += folder_skipped_count
        write_records(record_folder, record_txt, list(processed))

    if skipped_years:
        years_summary = ", ".join(skipped_years.keys())
        total_skipped = sum(skipped_years.values())
        print(f"\né {total_skipped} tables already processed for years: {years_summary}")

    elapsed_time = round(time.time() - start_time)
    print(f"\n=Ê Summary:\n")
    print(f"=Â {total_year_folders} year folders found")
    print(f"=Ã  Already processed: {skipped_counter}")
    print(f"( Newly processed: {new_counter}")
    print(f"ñ  {elapsed_time} seconds")

    return raw_tables, clean_tables, vintages


def new_table_1_runner(
    input_pdf_folder: str,
    record_folder: str,
    record_txt: str,
    persist: bool = False,
    persist_folder: Optional[str] = None,
    pipeline_version: str = "v1.0.0",
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]:
    """
    Process all NEW WR PDF files for Table 1 (monthly GDP from page 1).

    Coordinates complete workflow: record management, PDF extraction, cleaning,
    vintage reshaping, and optional persistence.

    Args:
        input_pdf_folder: Root folder containing year subfolders with PDF files.
        record_folder: Folder for storing processing records.
        record_txt: Filename for record file (e.g., 'table_1_new.txt').
        persist: If True, persist vintages to disk.
        persist_folder: Root folder for persisted outputs.
        pipeline_version: Version stamp for processed tables.

    Returns:
        Tuple of (raw_tables_dict, clean_tables_dict, vintages_dict).

    Example:
        >>> raw, clean, vintages = new_table_1_runner(
        ...     "./data/new/pdfs",
        ...     "./record",
        ...     "table_1_new.txt",
        ...     persist=True
        ... )
    """
    start_time = time.time()
    print("\n>u Starting NEW Table 1 processing...\n")

    cleaner = NewTableCleaner()
    prep = VintagesPreparator()
    records = read_records(record_folder, record_txt)
    processed = set(records)

    raw_tables: Dict[str, pd.DataFrame] = {}
    clean_tables: Dict[str, pd.DataFrame] = {}
    vintages: Dict[str, pd.DataFrame] = {}

    new_counter = 0
    skipped_counter = 0
    skipped_years: Dict[str, int] = {}

    years = [
        d
        for d in sorted(os.listdir(input_pdf_folder))
        if os.path.isdir(os.path.join(input_pdf_folder, d)) and d != "_quarantine"
    ]
    total_year_folders = len(years)

    if persist:
        base_out = persist_folder or os.path.join("data", "input")
        out_root = os.path.join(base_out, "new_table_1")
        os.makedirs(out_root, exist_ok=True)

    for year in years:
        folder_path = os.path.join(input_pdf_folder, year)
        pdf_files = sorted(
            [f for f in os.listdir(folder_path) if f.endswith(".pdf")],
            key=ns_sort_key,
        )

        month_order_map = prep.build_month_order_map(folder_path)

        if not pdf_files:
            continue

        already = [f for f in pdf_files if f in processed]
        if len(already) == len(pdf_files):
            skipped_years[year] = len(already)
            skipped_counter += len(already)
            continue

        print(f"\n=Â Processing Table 1 in {year}\n")
        folder_new_count = 0
        folder_skipped_count = 0

        pbar = tqdm(
            pdf_files,
            desc=f">u {year}",
            unit="PDF",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
            colour="#E6004C",
            leave=False,
        )

        for filename in pbar:
            if filename in processed:
                folder_skipped_count += 1
                continue

            issue, yr = parse_ns_meta(filename)
            if not issue:
                folder_skipped_count += 1
                continue

            pdf_path = os.path.join(folder_path, filename)
            try:
                raw = extract_table(pdf_path, page=1)
                if raw is None:
                    folder_skipped_count += 1
                    continue

                key = f"{os.path.splitext(filename)[0].replace('-', '_')}_1"
                raw_tables[key] = raw.copy()

                clean = cleaner.clean_table_1(raw)
                clean.insert(0, "year", yr)
                clean.insert(1, "wr", issue)
                clean.attrs["pipeline_version"] = pipeline_version

                clean_tables[key] = clean.copy()

                vintage = prep.prepare_table_1(clean, filename, month_order_map)
                vintage.attrs["pipeline_version"] = pipeline_version
                vintages[key] = vintage

                if persist:
                    ns_code = os.path.splitext(filename)[0]
                    out_dir = os.path.join(out_root, str(yr))
                    out_path = os.path.join(out_dir, f"{ns_code}.parquet")
                    save_df(vintage, out_path)

                processed.add(filename)
                folder_new_count += 1
            except Exception as e:
                print(f"   {filename}: {e}")
                folder_skipped_count += 1

        pbar.clear()
        pbar.close()

        fb = tqdm(
            total=len(pdf_files),
            desc=f" {year}",
            unit="PDF",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
            colour="#3366FF",
            leave=True,
        )
        fb.update(len(pdf_files))
        fb.close()

        new_counter += folder_new_count
        skipped_counter += folder_skipped_count
        write_records(record_folder, record_txt, list(processed))

    if skipped_years:
        years_summary = ", ".join(skipped_years.keys())
        total_skipped = sum(skipped_years.values())
        print(f"\né {total_skipped} tables already processed for years: {years_summary}")

    elapsed_time = round(time.time() - start_time)
    print(f"\n=Ê Summary:\n")
    print(f"=Â {total_year_folders} year folders found")
    print(f"=Ã  Already processed: {skipped_counter}")
    print(f"( Newly processed: {new_counter}")
    print(f"ñ  {elapsed_time} seconds")

    return raw_tables, clean_tables, vintages


def new_table_2_runner(
    input_pdf_folder: str,
    record_folder: str,
    record_txt: str,
    persist: bool = False,
    persist_folder: Optional[str] = None,
    pipeline_version: str = "v1.0.0",
) -> Tuple[Dict[str, pd.DataFrame], Dict[str, pd.DataFrame], Dict[str, pd.DataFrame]]:
    """
    Process all NEW WR PDF files for Table 2 (quarterly/annual GDP from page 2).

    Similar to new_table_1_runner but extracts from page 2 and processes Table 2 data.

    Args:
        input_pdf_folder: Root folder containing year subfolders with PDF files.
        record_folder: Folder for storing processing records.
        record_txt: Filename for record file (e.g., 'table_2_new.txt').
        persist: If True, persist vintages to disk.
        persist_folder: Root folder for persisted outputs.
        pipeline_version: Version stamp for processed tables.

    Returns:
        Tuple of (raw_tables_dict, clean_tables_dict, vintages_dict).
    """
    start_time = time.time()
    print("\n>u Starting NEW Table 2 processing...\n")

    cleaner = NewTableCleaner()
    prep = VintagesPreparator()
    records = read_records(record_folder, record_txt)
    processed = set(records)

    raw_tables: Dict[str, pd.DataFrame] = {}
    clean_tables: Dict[str, pd.DataFrame] = {}
    vintages: Dict[str, pd.DataFrame] = {}

    new_counter = 0
    skipped_counter = 0
    skipped_years: Dict[str, int] = {}

    years = [
        d
        for d in sorted(os.listdir(input_pdf_folder))
        if os.path.isdir(os.path.join(input_pdf_folder, d)) and d != "_quarantine"
    ]
    total_year_folders = len(years)

    if persist:
        base_out = persist_folder or os.path.join("data", "input")
        out_root = os.path.join(base_out, "new_table_2")
        os.makedirs(out_root, exist_ok=True)

    for year in years:
        folder_path = os.path.join(input_pdf_folder, year)
        pdf_files = sorted(
            [f for f in os.listdir(folder_path) if f.endswith(".pdf")],
            key=ns_sort_key,
        )

        month_order_map = prep.build_month_order_map(folder_path)

        if not pdf_files:
            continue

        already = [f for f in pdf_files if f in processed]
        if len(already) == len(pdf_files):
            skipped_years[year] = len(already)
            skipped_counter += len(already)
            continue

        print(f"\n=Â Processing Table 2 in {year}\n")
        folder_new_count = 0
        folder_skipped_count = 0

        pbar = tqdm(
            pdf_files,
            desc=f">u {year}",
            unit="PDF",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
            colour="#E6004C",
            leave=False,
        )

        for filename in pbar:
            if filename in processed:
                folder_skipped_count += 1
                continue

            issue, yr = parse_ns_meta(filename)
            if not issue:
                folder_skipped_count += 1
                continue

            pdf_path = os.path.join(folder_path, filename)
            try:
                raw = extract_table(pdf_path, page=2)
                if raw is None:
                    folder_skipped_count += 1
                    continue

                key = f"{os.path.splitext(filename)[0].replace('-', '_')}_2"
                raw_tables[key] = raw.copy()

                clean = cleaner.clean_table_2(raw)
                clean.insert(0, "year", yr)
                clean.insert(1, "wr", issue)
                clean.attrs["pipeline_version"] = pipeline_version

                clean_tables[key] = clean.copy()

                vintage = prep.prepare_table_2(clean, filename, month_order_map)
                vintage.attrs["pipeline_version"] = pipeline_version
                vintages[key] = vintage

                if persist:
                    ns_code = os.path.splitext(filename)[0]
                    out_dir = os.path.join(out_root, str(yr))
                    out_path = os.path.join(out_dir, f"{ns_code}.parquet")
                    save_df(vintage, out_path)

                processed.add(filename)
                folder_new_count += 1
            except Exception as e:
                print(f"   {filename}: {e}")
                folder_skipped_count += 1

        pbar.clear()
        pbar.close()

        fb = tqdm(
            total=len(pdf_files),
            desc=f" {year}",
            unit="PDF",
            bar_format="{l_bar}{bar}| {n_fmt}/{total_fmt}",
            colour="#3366FF",
            leave=True,
        )
        fb.update(len(pdf_files))
        fb.close()

        new_counter += folder_new_count
        skipped_counter += folder_skipped_count
        write_records(record_folder, record_txt, list(processed))

    if skipped_years:
        years_summary = ", ".join(skipped_years.keys())
        total_skipped = sum(skipped_years.values())
        print(f"\né {total_skipped} tables already processed for years: {years_summary}")

    elapsed_time = round(time.time() - start_time)
    print(f"\n=Ê Summary:\n")
    print(f"=Â {total_year_folders} year folders found")
    print(f"=Ã  Already processed: {skipped_counter}")
    print(f"( Newly processed: {new_counter}")
    print(f"ñ  {elapsed_time} seconds")

    return raw_tables, clean_tables, vintages
