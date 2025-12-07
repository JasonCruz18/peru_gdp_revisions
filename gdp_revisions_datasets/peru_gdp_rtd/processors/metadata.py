"""
Metadata extraction and table processing utilities.

This module provides utilities for parsing WR filenames, extracting tables from PDFs,
and managing table processing records.
"""

import os
import re
from pathlib import Path
from typing import List, Optional, Tuple

import pandas as pd
import tabula


def parse_ns_meta(file_name: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Extract issue number and year from WR-style filenames.

    Parses filenames of the form 'ns-XX-YYYY.*' to extract issue and year.

    Args:
        file_name: Path or name of the WR file (OLD CSV or NEW PDF).

    Returns:
        Tuple of (issue, year) extracted from filename, or (None, None) if no match.

    Example:
        >>> parse_ns_meta("ns-07-2017.pdf")
        ('07', '2017')
        >>> parse_ns_meta("unknown-file.pdf")
        (None, None)
    """
    m = re.search(r"ns-(\d{1,2})-(\d{4})", os.path.basename(file_name).lower())
    return (m.group(1), m.group(2)) if m else (None, None)


def ns_sort_key(s: str) -> Tuple[int, int, str]:
    """
    Build sorting key for WR filenames for chronological ordering.

    Sorts WR filenames ('ns-XX-YYYY.*') chronologically by year and issue number.
    Both OLD and NEW files are ordered together.

    Args:
        s: Full path or basename of a WR file.

    Returns:
        Tuple of (year, issue, basename) used for stable ordering.

    Example:
        >>> ns_sort_key("ns-01-2020.pdf")
        (2020, 1, 'ns-01-2020')
        >>> ns_sort_key("unknown.pdf")
        (9999, 9999, 'unknown')
    """
    base = os.path.splitext(os.path.basename(s))[0]
    m = re.search(r"ns-(\d{1,2})-(\d{4})", base, re.I)
    if not m:
        return (9999, 9999, base)  # Non-matching files sent to end
    issue, year = int(m.group(1)), int(m.group(2))
    return (year, issue, base)


def read_records(record_folder: str, record_txt: str) -> List[str]:
    """
    Load previously processed WR filenames from record file.

    Reads record file, deduplicates entries, and returns them sorted
    by chronological WR order.

    Args:
        record_folder: Folder path where record file is stored.
        record_txt: Name of record file (e.g., 'table_1_records.txt').

    Returns:
        Sorted and deduplicated list of WR filenames.
    """
    path = os.path.join(record_folder, record_txt)
    if not os.path.exists(path):
        return []

    with open(path, "r", encoding="utf-8") as f:
        items = [ln.strip() for ln in f if ln.strip()]

    return sorted(set(items), key=ns_sort_key)


def write_records(record_folder: str, record_txt: str, items: List[str]) -> None:
    """
    Persist processed WR filenames to record file in chronological order.

    Ensures duplicates are removed and list is kept in WR chronological order.

    Args:
        record_folder: Folder where record file is saved.
        record_txt: Record filename to store processed WR filenames.
        items: List of WR filenames to persist.
    """
    os.makedirs(record_folder, exist_ok=True)
    items = sorted(set(items), key=ns_sort_key)
    path = os.path.join(record_folder, record_txt)

    with open(path, "w", encoding="utf-8") as f:
        f.write("\n".join(items) + ("\n" if items else ""))


def extract_table(pdf_path: str, page: int) -> Optional[pd.DataFrame]:
    """
    Extract single table from specific PDF page using Tabula.

    Args:
        pdf_path: Full path to the NEW WR PDF.
        page: 1-based index of PDF page containing the table.

    Returns:
        Extracted table as DataFrame, or None if Tabula returns no table.

    Example:
        >>> df = extract_table("ns-01-2020.pdf", page=1)
        >>> if df is not None:
        ...     print(f"Extracted {len(df)} rows")
    """
    tables = tabula.read_pdf(pdf_path, pages=page, multiple_tables=False, stream=True)

    if tables is None:
        return None
    if isinstance(tables, list) and len(tables) == 0:
        return None

    return tables[0] if isinstance(tables, list) else tables


def save_df(df: pd.DataFrame, out_path: str) -> Tuple[str, int, int]:
    """
    Save DataFrame to disk, preferring Parquet with CSV fallback.

    Saves cleaned or vintage DataFrame to disk. Attempts Parquet format first,
    falls back to CSV if Parquet engine unavailable.

    Args:
        df: DataFrame to persist (cleaned or vintage).
        out_path: Target path (extension adjusted as needed).

    Returns:
        Tuple of (final_output_path, n_rows, n_cols).

    Example:
        >>> path, rows, cols = save_df(df, "output/table.parquet")
        >>> print(f"Saved {rows}x{cols} table to {path}")
    """
    os.makedirs(os.path.dirname(out_path), exist_ok=True)

    try:
        if not out_path.endswith(".parquet"):
            out_path = os.path.splitext(out_path)[0] + ".parquet"
        df.to_parquet(out_path, index=False)
    except Exception:
        out_path = os.path.splitext(out_path)[0] + ".csv"
        df.to_csv(out_path, index=False)

    return out_path, int(df.shape[0]), int(df.shape[1])
