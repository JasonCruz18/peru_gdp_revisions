"""Data cleaning and normalization functions."""

# Cleaner orchestrator classes
from peru_gdp_rtd.cleaners.new_table_cleaner import NewTableCleaner
from peru_gdp_rtd.cleaners.old_table_cleaner import OldTableCleaner

# Text cleaning utilities
from peru_gdp_rtd.cleaners.text_cleaners import (
    find_roman_numerals,
    remove_rare_characters,
    remove_rare_characters_first_row,
    remove_tildes,
)

# Table cleaning utilities
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

# Column manipulation utilities
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

# Table 1 (monthly GDP) specific cleaners
from peru_gdp_rtd.cleaners.table1_cleaners import (
    adjust_column_names,
    check_first_row,
    check_first_row_1,
    clean_column_names,
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

# Table 2 (quarterly/annual GDP) specific cleaners
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
    replace_total_with_year,
    roman_arabic,
    separate_years,
)

__all__ = [
    # Cleaner classes
    "NewTableCleaner",
    "OldTableCleaner",
    # Text cleaners
    "find_roman_numerals",
    "remove_rare_characters",
    "remove_rare_characters_first_row",
    "remove_tildes",
    # Table cleaners
    "clean_columns_values",
    "clean_first_row",
    "convert_float",
    "drop_nan_columns",
    "drop_nan_rows",
    "drop_rare_caracter_row",
    "extract_years",
    "first_row_columns",
    "relocate_last_column",
    "remove_digit_slash",
    "replace_mineria",
    "replace_mining",
    "replace_services",
    "replace_set_sep",
    "reset_index",
    "rounding_values",
    "separate_text_digits",
    "spaces_se_es",
    "swap_first_second_row",
    # Column handlers
    "exchange_values",
    "expand_column",
    "replace_nan_with_previous_column_1",
    "replace_nan_with_previous_column_2",
    "replace_nan_with_previous_column_3",
    "split_column_by_pattern",
    "split_values",
    "split_values_1",
    "split_values_2",
    "split_values_3",
    # Table 1 cleaners
    "adjust_column_names",
    "check_first_row",
    "check_first_row_1",
    "clean_column_names",
    "find_year_column",
    "get_months_sublist_list",
    "relocate_last_columns",
    "replace_first_dot",
    "replace_first_row_with_columns",
    "replace_number_moving_average",
    "replace_var_perc_first_column",
    "replace_var_perc_last_columns",
    "swap_nan_se",
    # Table 2 cleaners
    "drop_nan_row",
    "exchange_columns",
    "exchange_roman_nan",
    "extract_mixed_values",
    "fix_duplicates",
    "get_quarters_sublist_list",
    "last_column_es",
    "relocate_roman_numerals",
    "replace_first_row_nan",
    "replace_total_with_year",
    "roman_arabic",
    "separate_years",
]
