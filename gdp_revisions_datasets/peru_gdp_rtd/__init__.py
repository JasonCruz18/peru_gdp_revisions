"""Peru GDP Real-Time Dataset Construction Pipeline.

This package provides tools for automated construction of real-time GDP datasets
from BCRP (Central Reserve Bank of Peru) Weekly Reports.

Main components:
- scrapers: Web scraping and PDF downloading
- processors: PDF processing and table extraction
- cleaners: Data cleaning and normalization
- transformers: Data transformation and RTD construction
- orchestration: Pipeline orchestration and workflow management
- utils: Shared utilities and helpers
"""

__version__ = "1.0.0"
__author__ = "Jason Cruz"
__email__ = "jj.cruza@up.edu.pe"

# Package metadata
__all__ = [
    "__version__",
    "__author__",
    "__email__",
]
