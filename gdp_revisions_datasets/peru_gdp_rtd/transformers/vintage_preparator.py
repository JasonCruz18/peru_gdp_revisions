"""
Vintage format reshaping for Real-Time Dataset construction.

This module provides the VintagesPreparator class that reshapes cleaned GDP tables
into tidy 'vintage' format ready for concatenation across years and frequencies.
A vintage represents a snapshot of GDP estimates as published at a specific point in time.
"""

import os
import re
from typing import Dict, List, Tuple

import pandas as pd


class VintagesPreparator:
    """
    Reshape cleaned OLD/NEW tables into tidy vintage format.

    This class provides methods to:
    1. Infer month order within a year from WR issue numbers (ns-DD-YYYY ’ month 1..12)
    2. Reshape cleaned tables into row-based vintages with industry/vintage/target-period columns

    A vintage DataFrame contains:
    - industry: Canonical sector code (agriculture, fishing, mining, etc.)
    - vintage: Publication date as 'YYYYmM' (e.g., '2017m7' for July 2017)
    - tp_*: Target period columns with GDP percentage variations

    Example:
        >>> prep = VintagesPreparator()
        >>> month_map = prep.build_month_order_map("./data/raw/2017")
        >>> vintage = prep.prepare_table_1(cleaned_df, "ns-07-2017.csv", month_map)
    """

    # Canonical sector mapping used across both Table 1 and Table 2
    SECTOR_MAP = {
        "agriculture and livestock": "agriculture",
        "fishing": "fishing",
        "mining and fuel": "mining",
        "manufacturing": "manufacturing",
        "electricity and water": "electricity",
        "construction": "construction",
        "commerce": "commerce",
        "other services": "services",
        "gdp": "gdp",
    }

    # Spanish month abbreviation to numeric month mapping
    MONTH_MAP = {
        "ene": 1,
        "feb": 2,
        "mar": 3,
        "abr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "ago": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dic": 12,
    }

    def build_month_order_map(
        self, year_folder: str, extensions: Tuple[str, ...] = (".pdf", ".csv")
    ) -> Dict[str, int]:
        """
        Create mapping from filename to month order (1-12) for files in year folder.

        The month order is inferred from the WR issue day (DD in 'ns-DD-YYYY'). Files are
        sorted by issue day, and the resulting order gives each file a month index 1-12.

        Args:
            year_folder: Folder path containing WR files for a single year.
            extensions: Allowed file extensions (default: .pdf and .csv).

        Returns:
            Dictionary mapping filename to month_order in {1..12}.

        Example:
            >>> prep = VintagesPreparator()
            >>> month_map = prep.build_month_order_map("./data/raw/2017")
            >>> month_map["ns-07-2017.pdf"]
            7
        """
        files = [f for f in os.listdir(year_folder) if f.lower().endswith(extensions)]

        pairs: List[Tuple[str, int]] = []
        for f in files:
            m = re.search(r"ns-(\d{2})-\d{4}\.[a-zA-Z0-9]+$", f, re.IGNORECASE)
            if m:
                pairs.append((f, int(m.group(1))))  # (filename, issue_day)

        sorted_files = sorted(pairs, key=lambda x: x[1])  # Sort by issue day
        return {fname: i + 1 for i, (fname, _) in enumerate(sorted_files)}

    def prepare_table_1(
        self, df: pd.DataFrame, filename: str, month_order_map: Dict[str, int]
    ) -> pd.DataFrame:
        """
        Reshape cleaned Table 1 (monthly GDP) into tidy vintage format.

        Output columns:
        - industry (str): Canonical sector code
        - vintage (str): Publication date as 'YYYYmM' (e.g., '2017m7')
        - tp_YYYYmM (float): Target period columns for each month in the table

        Args:
            df: Cleaned Table 1 DataFrame with monthly GDP data.
            filename: WR filename (e.g., 'ns-07-2017.csv').
            month_order_map: Mapping from filename to month order (1-12).

        Returns:
            Tidy vintage DataFrame ready for concatenation.

        Example:
            >>> vintage = prep.prepare_table_1(cleaned_df, "ns-07-2017.csv", month_map)
            >>> print(vintage.columns)
            Index(['industry', 'vintage', 'tp_2016m1', 'tp_2016m2', ...], dtype='object')
        """
        d = df.copy()

        # Determine WR month index from filename
        wr_month = month_order_map.get(filename)
        d["month"] = wr_month

        # Drop columns not needed for vintage layout
        d = d.drop(columns=["wr", "sectores_economicos"], errors="ignore")

        # Normalize sector column name (handle OLD/NEW variations)
        if "economic_sectors" not in d.columns:
            if "economic_sector" in d.columns:
                d["economic_sectors"] = d["economic_sector"]
            else:
                raise ValueError(
                    "Expected 'economic_sectors' column not found in cleaned Table 1 dataframe."
                )

        # Map to canonical industry codes
        d["industry"] = d["economic_sectors"].map(self.SECTOR_MAP)
        d = d[d["industry"].notna()].copy()

        # Build vintage identifier: year + 'm' + WR month index
        d["vintage"] = (
            d["year"].astype(int).astype(str) + "m" + d["month"].astype(int).astype(str)
        )

        # Detect monthly target-period columns like '2015_ene', '2015_jul'
        pat = re.compile(
            r"^\d{4}_(ene|feb|mar|abr|may|jun|jul|ago|sep|oct|nov|dic)$", re.IGNORECASE
        )
        period_cols = [c for c in d.columns if pat.match(str(c))]

        # Rename period columns to 'tp_YYYYmM'
        def period_to_tp(col: str) -> str:
            m = re.match(r"^(\d{4})_(\w{3})$", col, re.IGNORECASE)
            if not m:
                return col
            yy = m.group(1)
            mmm = m.group(2).lower()
            mm = self.MONTH_MAP.get(mmm, 1)
            return f"tp_{yy}m{mm}"

        rename_dict = {c: period_to_tp(c) for c in period_cols}
        d = d.rename(columns=rename_dict)

        # Order columns in chronological target-period order
        tp_cols = [rename_dict[c] for c in period_cols]

        def _tp_key(c: str):
            s = c[3:]  # Remove 'tp_'
            y, m = s.split("m")
            return (int(y), int(m))

        tp_cols_sorted = sorted(tp_cols, key=_tp_key)
        final_cols = ["industry", "vintage"] + tp_cols_sorted

        d_out = d[final_cols].reset_index(drop=True)

        # Enforce dtypes
        d_out["industry"] = d_out["industry"].astype(str)
        d_out["vintage"] = d_out["vintage"].astype(str)

        for col in tp_cols_sorted:
            d_out[col] = pd.to_numeric(d_out[col], errors="coerce").astype(float)

        return d_out

    def prepare_table_2(
        self, df: pd.DataFrame, filename: str, month_order_map: Dict[str, int]
    ) -> pd.DataFrame:
        """
        Reshape cleaned Table 2 (quarterly/annual GDP) into tidy vintage format.

        Output columns:
        - industry (str): Canonical sector code
        - vintage (str): Publication date as 'YYYYmM' (e.g., '2017m7')
        - tp_YYYYqN (float): Quarterly target period columns
        - tp_YYYY (float): Annual target period columns

        Args:
            df: Cleaned Table 2 DataFrame with quarterly/annual GDP data.
            filename: WR filename (e.g., 'ns-07-2017.csv').
            month_order_map: Mapping from filename to month order (1-12).

        Returns:
            Tidy vintage DataFrame ready for concatenation.

        Example:
            >>> vintage = prep.prepare_table_2(cleaned_df, "ns-07-2017.csv", month_map)
            >>> print(vintage.columns)
            Index(['industry', 'vintage', 'tp_2016q1', 'tp_2016q2', 'tp_2016', ...], dtype='object')
        """
        d = df.copy()

        # Drop columns not needed for vintage layout
        d = d.drop(columns=["wr", "sectores_economicos"], errors="ignore")

        # Normalize sector column name (handle OLD/NEW variations)
        if "economic_sectors" not in d.columns:
            if "economic_sector" in d.columns:
                d["economic_sectors"] = d["economic_sector"]
            else:
                raise ValueError(
                    "Expected 'economic_sectors' column not found in cleaned Table 2 dataframe."
                )

        # Map to canonical industry codes
        d["industry"] = d["economic_sectors"].map(self.SECTOR_MAP)
        d = d[d["industry"].notna()].copy()

        # Build vintage identifier from year and WR month index
        wr_month = month_order_map.get(filename)
        d["month"] = wr_month
        d["vintage"] = (
            d["year"].astype(int).astype(str) + "m" + d["month"].astype(int).astype(str)
        )

        # Detect quarterly/annual period columns: '2020_1', '2020_2', '2020_3', '2020_4', '2020_year'
        pat = re.compile(r"^\d{4}_(1|2|3|4|year)$", re.IGNORECASE)
        period_cols = [c for c in d.columns if pat.match(str(c))]

        # Rename to 'tp_YYYYqN' for quarters and 'tp_YYYY' for annuals
        def quarter_to_tp(col: str) -> str:
            m = re.match(r"^(\d{4})_(\d)$", col, re.IGNORECASE)
            if m:
                yy = m.group(1)
                q = m.group(2)
                return f"tp_{yy}q{q}"
            m2 = re.match(r"^(\d{4})_year$", col, re.IGNORECASE)
            if m2:
                yy = m2.group(1)
                return f"tp_{yy}"
            return col

        rename_dict = {c: quarter_to_tp(c) for c in period_cols}
        d = d.rename(columns=rename_dict)

        # Order columns so quarterlies precede annual for each year
        tp_cols = [rename_dict[c] for c in period_cols]

        def _tp_key(c: str):
            assert c.startswith("tp_")
            body = c[3:]  # 'YYYYqN' or 'YYYY'
            if "q" in body:
                yy, q = body.split("q")
                return (int(yy), 0, int(q))  # Quarterly rows come before annual
            return (int(body), 1, 0)  # Annual row for that year

        tp_cols_sorted = sorted(tp_cols, key=_tp_key)
        final_cols = ["industry", "vintage"] + tp_cols_sorted
        d_out = d[final_cols].reset_index(drop=True)

        # Enforce dtypes
        d_out["industry"] = d_out["industry"].astype(str)
        d_out["vintage"] = d_out["vintage"].astype(str)

        for col in tp_cols_sorted:
            d_out[col] = pd.to_numeric(d_out[col], errors="coerce").astype(float)

        return d_out
