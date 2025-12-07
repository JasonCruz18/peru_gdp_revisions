"""PDF processing and table extraction."""

from peru_gdp_rtd.processors.file_organizer import (
    organize_files_by_year,
    replace_defective_pdfs,
)
from peru_gdp_rtd.processors.metadata import (
    extract_table,
    ns_sort_key,
    parse_ns_meta,
    read_records,
    save_df,
    write_records,
)
from peru_gdp_rtd.processors.pdf_processor import (
    ask_continue_input,
    pdf_input_generator,
    read_input_pdf_files,
    search_keywords,
    shortened_pdf,
    write_input_pdf_files,
)

__all__ = [
    "organize_files_by_year",
    "replace_defective_pdfs",
    "search_keywords",
    "shortened_pdf",
    "read_input_pdf_files",
    "write_input_pdf_files",
    "ask_continue_input",
    "pdf_input_generator",
    "extract_table",
    "ns_sort_key",
    "parse_ns_meta",
    "read_records",
    "save_df",
    "write_records",
]
