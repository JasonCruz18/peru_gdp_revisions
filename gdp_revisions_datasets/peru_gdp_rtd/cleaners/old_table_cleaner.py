"""
OLD dataset (CSV-based) table cleaning pipeline orchestrator.

This module provides the OldTableCleaner class that encapsulates the full cleaning
pipeline for WR tables from the OLD dataset (scanned PDFs converted to CSV).
Handles both Table 1 (monthly GDP) and Table 2 (quarterly/annual GDP) cleaning.
"""

import pandas as pd

from peru_gdp_rtd.cleaners.column_handlers import exchange_values
from peru_gdp_rtd.cleaners.table1_cleaners import (
    clean_column_names,
    adjust_column_names,
    find_year_column,
    get_months_sublist_list,
    relocate_last_columns,
    replace_number_moving_average,
    replace_var_perc_first_column,
    replace_var_perc_last_columns,
)
from peru_gdp_rtd.cleaners.table2_cleaners import (
    fix_duplicates,
    get_quarters_sublist_list,
    replace_first_row_nan,
    replace_total_with_year,
    roman_arabic,
)
from peru_gdp_rtd.cleaners.table_cleaners import (
    clean_columns_values,
    clean_first_row,
    convert_float,
    drop_nan_columns,
    drop_nan_rows,
    drop_rare_caracter_row,
    extract_years,
    first_row_columns,
    relocate_last_column,
    remove_digit_slash,
    replace_mineria,
    replace_mining,
    replace_set_sep,
    reset_index,
    rounding_values,
    spaces_se_es,
)


class OldTableCleaner:
    """
    Orchestrator for OLD WR tables cleaning (CSV-based source).

    This class provides comprehensive cleaning pipelines for OLD dataset tables:
    - Table 1: Monthly GDP percentage variations
    - Table 2: Quarterly and annual GDP percentage variations

    The OLD dataset consists of scanned PDF reports converted to CSV files.
    Tables require extensive structural cleaning due to OCR inconsistencies
    and varying report formats over time.

    Methods:
        clean_table_1: Clean OLD Table 1 (monthly data).
        clean_table_2: Clean OLD Table 2 (quarterly/annual data).

    Example:
        >>> cleaner = OldTableCleaner()
        >>> df_raw = pd.read_csv("old/ns-07-2017-table1.csv")
        >>> df_clean = cleaner.clean_table_1(df_raw)
    """

    def clean_table_1(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean raw DataFrame extracted from OLD WR Table 1 (monthly growth rates).

        Applies appropriate cleaning pipeline based on table structure detection.
        Two branches:
        - Branch A: Well-formed tables with sector columns in expected position
        - Branch B: Irregular tables requiring extensive structural reconstruction

        Args:
            df: Raw OLD Table 1 DataFrame as read from CSV.

        Returns:
            Cleaned OLD Table 1 DataFrame ready for reshaping into vintages.

        Example:
            >>> cleaner = OldTableCleaner()
            >>> df_clean = cleaner.clean_table_1(df_raw)
            >>> print(df_clean.columns[:3])
            Index(['sectores_economicos', 'economic_sectors', '2017_jan'], dtype='object')
        """
        d = df.copy()

        # Branch A — header already has sector columns in expected position
        if d.columns[1] == "economic_sectors":
            d = drop_nan_rows(d)  # 1. Drop rows where all entries are NaN
            d = drop_nan_columns(d)  # 2. Drop columns where all entries are NaN
            d = clean_columns_values(d)  # 3. Normalize column names and textual values
            d = convert_float(d)  # 4. Convert numeric columns using coercion on errors
            d = replace_set_sep(d)  # 5. Standardize 'set' month labels to 'sep'
            d = spaces_se_es(d)  # 6. Strip spaces in ES/EN sector label columns
            d = replace_mineria(d)  # 7. Harmonize 'mineria' naming (ES)
            d = replace_mining(d)  # 8. Harmonize 'mining and fuels' naming (EN)
            d = rounding_values(d, decimals=1)  # 9. Round float columns to one decimal place
            return d

        # Branch B — headers are more irregular and require structural fixes
        d = clean_column_names(d)  # 1. Standardize raw column name casing/diacritics
        d = adjust_column_names(d)  # 2. Apply WR-specific column name adjustments
        d = drop_rare_caracter_row(d)  # 3. Remove rows containing rare character '}'
        d = drop_nan_rows(d)  # 4. Drop rows where all entries are NaN
        d = drop_nan_columns(d)  # 5. Drop columns where all entries are NaN
        d = reset_index(d)  # 6. Reset index after dropping rows/cols
        d = remove_digit_slash(d)  # 7. Strip '<digits>/' prefixes in edge columns
        d = replace_var_perc_first_column(d)  # 8. Normalize 'Var. %' labels in first column
        d = replace_var_perc_last_columns(d)  # 9. Normalize 'Var. %' labels in last columns
        d = replace_number_moving_average(d)  # 10. Normalize moving-average descriptors
        d = relocate_last_column(d)  # 11. Move last column into position 1
        d = clean_first_row(d)  # 12. Normalize header row text content
        d = find_year_column(d)  # 13. Align textual 'year' tokens with numeric years
        years = extract_years(d)  # 14. Identify year-labelled columns for WR
        d = get_months_sublist_list(d, years)  # 15. Build '<year>_<month>' composite headers
        d = first_row_columns(d)  # 16. Promote first row to header row
        d = clean_columns_values(d)  # 17. Normalize resulting header and body values
        d = convert_float(d)  # 18. Convert non-label columns to numeric
        d = replace_set_sep(d)  # 19. Standardize 'set' into 'sep'
        d = spaces_se_es(d)  # 20. Strip spaces in ES/EN sector label columns
        d = replace_mineria(d)  # 21. Harmonize 'mineria' naming (ES)
        d = replace_mining(d)  # 22. Harmonize 'mining and fuels' naming (EN)
        d = rounding_values(d, decimals=1)  # 23. Round float columns to one decimal place
        return d

    def clean_table_2(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean raw DataFrame extracted from OLD WR Table 2 (quarterly/annual growth).

        Applies appropriate cleaning pipeline based on table structure detection.
        Two branches:
        - Branch A: Well-formed tables with sector columns in expected position
        - Branch B: Irregular tables requiring extensive structural reconstruction

        Args:
            df: Raw OLD Table 2 DataFrame as read from CSV.

        Returns:
            Cleaned OLD Table 2 DataFrame ready for reshaping into vintages.

        Example:
            >>> cleaner = OldTableCleaner()
            >>> df_clean = cleaner.clean_table_2(df_raw)
            >>> print(df_clean.columns[:3])
            Index(['sectores_economicos', 'economic_sectors', '2017_1'], dtype='object')
        """
        d = df.copy()

        # Branch A — header already has sector columns in expected position
        if d.columns[1] == "economic_sectors":
            d = drop_nan_rows(d)  # 1. Drop rows where all entries are NaN
            d = drop_nan_columns(d)  # 2. Drop columns where all entries are NaN
            d = clean_columns_values(d)  # 3. Normalize column names and textual values
            d = convert_float(d)  # 4. Convert numeric columns using coercion on errors
            d = replace_set_sep(d)  # 5. Standardize 'set' month labels to 'sep'
            d = spaces_se_es(d)  # 6. Strip spaces in ES/EN sector label columns
            d = replace_mineria(d)  # 7. Harmonize 'mineria' naming (ES)
            d = replace_mining(d)  # 8. Harmonize 'mining and fuels' naming (EN)
            d = rounding_values(d, decimals=1)  # 9. Round float columns to one decimal place
            return d

        # Branch B — headers are more irregular and require structural fixes
        d = replace_total_with_year(d)  # 1. Convert 'TOTAL' into 'year' header tokens
        d = drop_nan_rows(d)  # 2. Drop rows where all entries are NaN
        d = drop_nan_columns(d)  # 3. Drop columns where all entries are NaN
        years = extract_years(d)  # 4. Identify year-labelled columns
        d = roman_arabic(d)  # 5. Convert Roman numeral headers into Arabic
        d = fix_duplicates(d)  # 6. Fix duplicated numeric header tokens
        d = relocate_last_column(d)  # 7. Move last column into position 1
        d = replace_first_row_nan(d)  # 8. Fill NaNs in the first row with column names
        d = clean_first_row(d)  # 9. Normalize header row text content
        d = get_quarters_sublist_list(d, years)  # 10. Build '<year>_<quarter>' composite headers
        d = reset_index(d)  # 11. Reset index after structural changes
        d = first_row_columns(d)  # 12. Promote first row to header row
        d = clean_columns_values(d)  # 13. Normalize columns and values
        d = reset_index(d)  # 14. Reset index after additional cleaning
        d = convert_float(d)  # 15. Convert non-label columns to numeric
        d = replace_set_sep(d)  # 16. Standardize 'set' into 'sep'
        d = spaces_se_es(d)  # 17. Strip spaces in ES/EN sector label columns
        d = replace_mineria(d)  # 18. Harmonize 'mineria' naming (ES)
        d = replace_mining(d)  # 19. Harmonize 'mining and fuels' naming (EN)
        d = rounding_values(d, decimals=1)  # 20. Round float columns to one decimal place
        return d
