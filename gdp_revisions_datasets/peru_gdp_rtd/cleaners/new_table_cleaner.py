"""
NEW dataset (PDF-based) table cleaning pipeline orchestrator.

This module provides the NewTableCleaner class that encapsulates the full cleaning
pipeline for WR tables from the NEW dataset (editable PDFs extracted via Tabula).
Handles both Table 1 (monthly GDP) and Table 2 (quarterly/annual GDP) cleaning.
"""

import pandas as pd

from peru_gdp_rtd.cleaners.column_handlers import (
    exchange_values,
    expand_column,
    replace_nan_with_previous_column_1,
    replace_nan_with_previous_column_2,
    replace_nan_with_previous_column_3,
    split_column_by_pattern,
    split_values,
    split_values_1,
    split_values_2,
    split_values_3,
)
from peru_gdp_rtd.cleaners.table1_cleaners import (
    check_first_row,
    check_first_row_1,
    find_year_column,
    get_months_sublist_list,
    relocate_last_columns,
    replace_first_dot,
    replace_first_row_with_columns,
    replace_number_moving_average,
    replace_var_perc_first_column,
    replace_var_perc_last_columns,
    swap_nan_se,
)
from peru_gdp_rtd.cleaners.table2_cleaners import (
    drop_nan_row,
    exchange_columns,
    exchange_roman_nan,
    extract_mixed_values,
    fix_duplicates,
    get_quarters_sublist_list,
    last_column_es,
    relocate_roman_numerals,
    replace_first_row_nan,
    roman_arabic,
    separate_years,
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
    replace_services,
    replace_set_sep,
    reset_index,
    rounding_values,
    separate_text_digits,
    spaces_se_es,
    swap_first_second_row,
)


class NewTableCleaner:
    """
    Orchestrator for NEW WR tables cleaning (PDF-based source).

    This class provides comprehensive cleaning pipelines for NEW dataset tables:
    - Table 1: Monthly GDP percentage variations
    - Table 2: Quarterly and annual GDP percentage variations

    The NEW dataset consists of editable PDF reports with tables extracted via Tabula.
    Tables have different structural issues compared to OLD dataset (OCR-based),
    requiring specialized cleaning sequences.

    Methods:
        clean_table_1: Clean NEW Table 1 (monthly data).
        clean_table_2: Clean NEW Table 2 (quarterly/annual data).

    Example:
        >>> cleaner = NewTableCleaner()
        >>> df_raw = extract_table("new/ns-07-2020.pdf", page=1)
        >>> df_clean = cleaner.clean_table_1(df_raw)
    """

    def clean_table_1(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean raw DataFrame extracted from NEW WR Table 1 (monthly growth rates).

        Applies appropriate cleaning pipeline based on table structure detection.
        Two branches:
        - Branch A: At least one header matches 'YYYY' pattern
        - Branch B: No 'YYYY' header yet, requires additional reconstruction

        Args:
            df: Raw NEW Table 1 DataFrame as extracted from PDF.

        Returns:
            Cleaned NEW Table 1 DataFrame ready for reshaping into vintages.

        Example:
            >>> cleaner = NewTableCleaner()
            >>> df_clean = cleaner.clean_table_1(df_raw)
            >>> print(df_clean.columns[:3])
            Index(['sectores_economicos', 'economic_sectors', '2020_jan'], dtype='object')
        """
        d = df.copy()

        # Branch A — at least one header already matches a 'YYYY' pattern
        if any(isinstance(c, str) and c.isdigit() and len(c) == 4 for c in d.columns):
            d = swap_nan_se(d)  # 1. Fix misplaced 'SECTORES ECONÓMICOS' header
            d = split_column_by_pattern(d)  # 2. Split 'Word. Word' headers into two columns
            d = drop_rare_caracter_row(d)  # 3. Remove rows containing rare character '}'
            d = drop_nan_rows(d)  # 4. Drop rows where all entries are NaN
            d = drop_nan_columns(d)  # 5. Drop columns where all entries are NaN
            d = relocate_last_columns(d)  # 6. Relocate text in last columns if needed
            d = replace_first_dot(d)  # 7. Replace first '.' with '-' in second row cells
            d = swap_first_second_row(d)  # 8. Swap first/second rows at first and last columns
            d = drop_nan_rows(d)  # 9. Clean residual empty rows
            d = reset_index(d)  # 10. Reset index after structural changes
            d = remove_digit_slash(d)  # 11. Strip '<digits>/' prefixes in edge columns
            d = replace_var_perc_first_column(d)  # 12. Normalize 'Var. %' labels in first column
            d = replace_var_perc_last_columns(d)  # 13. Normalize 'Var. %' labels in last columns
            d = replace_number_moving_average(d)  # 14. Normalize moving-average descriptors
            d = separate_text_digits(d)  # 15. Split mixed text-numeric tokens in penultimate column
            d = exchange_values(d)  # 16. Swap last two columns when NaNs appear in last
            d = relocate_last_column(d)  # 17. Move last column into position 1
            d = clean_first_row(d)  # 18. Normalize header row text
            d = find_year_column(d)  # 19. Align 'year' tokens with numeric year columns
            years = extract_years(d)  # 20. Identify year-labelled columns
            d = get_months_sublist_list(d, years)  # 21. Build '<year>_<month>' composite headers
            d = first_row_columns(d)  # 22. Promote first row to header row
            d = clean_columns_values(d)  # 23. Normalize column names and values
            d = convert_float(d)  # 24. Convert non-label columns to numeric
            d = replace_set_sep(d)  # 25. Standardize 'set' into 'sep'
            d = spaces_se_es(d)  # 26. Strip spaces in ES/EN sector label columns
            d = replace_services(d)  # 27. Harmonize 'services' naming
            d = replace_mineria(d)  # 28. Harmonize 'mineria' naming (ES)
            d = replace_mining(d)  # 29. Harmonize 'mining and fuels' naming (EN)
            d = rounding_values(d, decimals=1)  # 30. Round float columns to one decimal place
            return d

        # Branch B — no 'YYYY' header yet, additional reconstruction needed
        d = check_first_row(d)  # 1. Handle 'YYYY YYYY' patterns in first row
        d = check_first_row_1(d)  # 2. Fill missing first-row year tokens from edges
        d = replace_first_row_with_columns(d)  # 3. Replace NaNs in row 0 with synthetic names
        d = swap_nan_se(d)  # 4. Fix misplaced 'SECTORES ECONÓMICOS' header
        d = split_column_by_pattern(d)  # 5. Split 'Word. Word' headers into two columns
        d = drop_rare_caracter_row(d)  # 6. Remove rows containing rare character '}'
        d = drop_nan_rows(d)  # 7. Drop rows where all entries are NaN
        d = drop_nan_columns(d)  # 8. Drop columns where all entries are NaN
        d = relocate_last_columns(d)  # 9. Relocate trailing values in last columns if needed
        d = swap_first_second_row(d)  # 10. Swap first/second rows at first and last columns
        d = drop_nan_rows(d)  # 11. Clean residual empty rows
        d = reset_index(d)  # 12. Reset index after structural changes
        d = remove_digit_slash(d)  # 13. Strip '<digits>/' prefixes in edge columns
        d = replace_var_perc_first_column(d)  # 14. Normalize 'Var. %' labels in first column
        d = replace_var_perc_last_columns(d)  # 15. Normalize 'Var. %' labels in last columns
        d = replace_number_moving_average(d)  # 16. Normalize moving-average descriptors
        d = expand_column(d)  # 17. Expand hyphenated text within penultimate column
        d = split_values_1(d)  # 18. Split expanded column (variant 1)
        d = split_values_2(d)  # 19. Split expanded column (variant 2)
        d = split_values_3(d)  # 20. Split expanded column (variant 3)
        d = separate_text_digits(d)  # 21. Split mixed text-numeric tokens in penultimate column
        d = exchange_values(d)  # 22. Swap last two columns when NaNs appear in last
        d = relocate_last_column(d)  # 23. Move last column into position 1
        d = clean_first_row(d)  # 24. Normalize header row text
        d = find_year_column(d)  # 25. Align 'year' tokens with numeric year columns
        years = extract_years(d)  # 26. Identify year-labelled columns
        d = get_months_sublist_list(d, years)  # 27. Build '<year>_<month>' composite headers
        d = first_row_columns(d)  # 28. Promote first row to header row
        d = clean_columns_values(d)  # 29. Normalize column names and values
        d = convert_float(d)  # 30. Convert non-label columns to numeric
        d = replace_nan_with_previous_column_1(d)  # 31. Fill NaNs using neighboring columns (variant 1)
        d = replace_nan_with_previous_column_2(d)  # 32. Fill NaNs using neighboring columns (variant 2)
        d = replace_nan_with_previous_column_3(d)  # 33. Fill NaNs using neighboring columns (variant 3)
        d = replace_set_sep(d)  # 34. Standardize 'set' into 'sep'
        d = spaces_se_es(d)  # 35. Strip spaces in ES/EN sector label columns
        d = replace_services(d)  # 36. Harmonize 'services' naming
        d = replace_mineria(d)  # 37. Harmonize 'mineria' naming (ES)
        d = replace_mining(d)  # 38. Harmonize 'mining and fuels' naming (EN)
        d = rounding_values(d, decimals=1)  # 39. Round float columns to one decimal place
        return d

    def clean_table_2(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Clean raw DataFrame extracted from NEW WR Table 2 (quarterly/annual growth).

        Applies appropriate cleaning pipeline based on table structure detection.
        Two branches:
        - Branch A: Header starts with NaN in first cell (specific NEW layout)
        - Branch B: Standard NEW layout without NaN at (0, 0)

        Args:
            df: Raw NEW Table 2 DataFrame as extracted from PDF.

        Returns:
            Cleaned NEW Table 2 DataFrame ready for reshaping into vintages.

        Example:
            >>> cleaner = NewTableCleaner()
            >>> df_clean = cleaner.clean_table_2(df_raw)
            >>> print(df_clean.columns[:3])
            Index(['sectores_economicos', 'economic_sectors', '2020_1'], dtype='object')
        """
        d = df.copy()

        # Branch A — header starts with NaN in first cell (specific NEW layout)
        if pd.isna(d.iloc[0, 0]):
            d = drop_nan_columns(d)  # 1. Drop fully-NaN columns
            d = separate_years(d)  # 2. Split 'YYYY YYYY' combined header into two columns
            d = relocate_roman_numerals(d)  # 3. Move Roman numerals into dedicated column
            d = extract_mixed_values(d)  # 4. Extract mixed numeric/text tokens from third-last column
            d = replace_first_row_nan(d)  # 5. Replace NaNs in row 0 with their column names
            d = first_row_columns(d)  # 6. Promote first row to column headers
            d = swap_first_second_row(d)  # 7. Swap first/second rows at first and last columns
            d = reset_index(d)  # 8. Reset index after structural changes
            d = drop_nan_row(d)  # 9. Drop row 0 if still fully NaN
            years = extract_years(d)  # 10. Identify year-labelled columns
            d = split_values(d)  # 11. Split target mixed column into several columns
            d = separate_text_digits(d)  # 12. Split mixed text-numeric tokens in penultimate column
            d = roman_arabic(d)  # 13. Convert Roman numerals in row 0 to Arabic numerals
            d = fix_duplicates(d)  # 14. Fix duplicated numeric header tokens
            d = relocate_last_column(d)  # 15. Move last column into position 1
            d = clean_first_row(d)  # 16. Normalize header row text
            d = get_quarters_sublist_list(d, years)  # 17. Build '<year>_<quarter>' composite headers
            d = first_row_columns(d)  # 18. Promote first row to column headers again
            d = clean_columns_values(d)  # 19. Normalize column names and values
            d = reset_index(d)  # 20. Reset index after additional cleaning
            d = convert_float(d)  # 21. Convert non-label columns to numeric
            d = replace_set_sep(d)  # 22. Standardize 'set' into 'sep'
            d = spaces_se_es(d)  # 23. Strip spaces in ES/EN sector label columns
            d = replace_services(d)  # 24. Harmonize 'services' naming
            d = replace_mineria(d)  # 25. Harmonize 'mineria' naming (ES)
            d = replace_mining(d)  # 26. Harmonize 'mining and fuels' naming (EN)
            d = rounding_values(d, decimals=1)  # 27. Round float columns to one decimal place
            return d

        # Branch B — standard NEW layout without NaN at (0, 0)
        d = exchange_roman_nan(d)  # 1. Swap Roman numerals/'AÑO' vs NaN in second row
        d = exchange_columns(d)  # 2. Swap year-like empty column names with neighbors
        d = drop_nan_columns(d)  # 3. Drop fully-NaN columns
        d = remove_digit_slash(d)  # 4. Strip '<digits>/' prefixes in edge columns
        d = last_column_es(d)  # 5. Fix 'ECONOMIC SECTORS' placement in last column
        d = swap_first_second_row(d)  # 6. Swap first/second rows at first and last columns
        d = drop_nan_rows(d)  # 7. Drop rows where all entries are NaN
        d = reset_index(d)  # 8. Reset index after structural changes
        years = extract_years(d)  # 9. Identify year-labelled columns
        d = separate_text_digits(d)  # 10. Split mixed text-numeric tokens in penultimate column
        d = roman_arabic(d)  # 11. Convert Roman numerals in row 0 to Arabic numerals
        d = fix_duplicates(d)  # 12. Fix duplicated numeric header tokens
        d = relocate_last_column(d)  # 13. Move last column into position 1
        d = clean_first_row(d)  # 14. Normalize header row text
        d = get_quarters_sublist_list(d, years)  # 15. Build '<year>_<quarter>' composite headers
        d = first_row_columns(d)  # 16. Promote first row to column headers
        d = clean_columns_values(d)  # 17. Normalize column names and values
        d = reset_index(d)  # 18. Reset index after additional cleaning
        d = convert_float(d)  # 19. Convert non-label columns to numeric
        d = replace_set_sep(d)  # 20. Standardize 'set' into 'sep'
        d = spaces_se_es(d)  # 21. Strip spaces in ES/EN sector label columns
        d = replace_services(d)  # 22. Harmonize 'services' naming
        d = replace_mineria(d)  # 23. Harmonize 'mineria' naming (ES)
        d = replace_mining(d)  # 24. Harmonize 'mining and fuels' naming (EN)
        d = rounding_values(d, decimals=1)  # 25. Round float columns to one decimal place
        return d
