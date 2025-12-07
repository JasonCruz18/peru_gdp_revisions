# Peru GDP Real-Time Dataset

> Automated construction pipeline for Peruvian GDP Real-Time Dataset from BCRP Weekly Reports

[![Python](https://img.shields.io/badge/python-3.10%2B-blue.svg)](https://www.python.org/downloads/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

## Overview

This project provides a comprehensive, production-ready pipeline for building Real-Time Datasets (RTD) of Peruvian GDP revisions. It automatically scrapes, processes, cleans, and transforms data from the Central Reserve Bank of Peru (BCRP) Weekly Reports into analysis-ready datasets.

**Key Features:**
- **Automated web scraping** from BCRP website using Selenium
- **Robust PDF processing** with table extraction from scanned and digital PDFs
- **Comprehensive data cleaning** with 70+ specialized transformation functions
- **Real-time dataset construction** tracking GDP revisions over time
- **Metadata management** for base-year changes and benchmark revisions
- **One-button update** script for effortless dataset updates
- **Configuration-driven** - no hardcoded values
- **Fully replicable** - works from scratch for any user

---

## Quick Start

### Installation

```bash
# Clone the repository
git clone https://github.com/JasonCruz18/peru_gdp_revisions.git
cd peru_gdp_revisions/gdp_revisions_datasets

# Install dependencies
pip install -r requirements.txt

# Or install in development mode
pip install -e .
```

### Configuration

```bash
# Copy example configuration
cp config/config.example.yaml config/config.yaml

# Edit configuration if needed (optional - defaults work out of the box)
# nano config/config.yaml
```

### Run Pipeline

```bash
# One-button update - runs complete pipeline
python scripts/update_rtd.py

# Run specific steps only
python scripts/update_rtd.py --steps 3,4,5,6

# Skip PDF download (useful for testing)
python scripts/update_rtd.py --skip-download

# Verbose output for debugging
python scripts/update_rtd.py --verbose
```

That's it! The complete GDP RTD will be generated in `data/output/`.

---

## Project Structure

```
gdp_revisions_datasets/
├── peru_gdp_rtd/              # Main Python package
│   ├── config/                # Configuration management
│   ├── scrapers/              # Web scraping and PDF downloading
│   ├── processors/            # PDF processing and table extraction
│   ├── cleaners/              # Data cleaning functions
│   ├── transformers/          # Data transformation and RTD construction
│   ├── orchestration/         # Pipeline orchestration
│   └── utils/                 # Shared utilities
│
├── scripts/
│   └── update_rtd.py          # One-button update script ⭐
│
├── notebooks/                 # Educational Jupyter notebooks
│   ├── new_gdp_rtd.ipynb      # Complete pipeline walkthrough
│   └── [Step-by-step guides]  # Individual step tutorials
│
├── config/
│   ├── config.yaml            # User configuration
│   └── config.example.yaml    # Configuration template
│
├── data/                      # Generated datasets (gitignored)
│   ├── input/                 # Intermediate data
│   └── output/                # Final RTD datasets ⭐
│
├── metadata/
│   └── wr_metadata.csv        # Metadata (tracked in git)
│
├── tests/                     # Test suite
├── docs/                      # Documentation
├── pyproject.toml             # Modern Python packaging
├── requirements.txt           # Dependencies
└── README.md                  # This file
```

---

## Pipeline Steps

The pipeline consists of 6 sequential steps:

### Step 1: Download PDFs
- Automated web scraping from [BCRP Weekly Reports](https://www.bcrp.gob.pe/publicaciones/nota-semanal.html)
- Selenium-based browser automation
- Rate limiting to mimic human behavior
- Retry logic with exponential backoff

### Step 2: Generate Input PDFs
- Extracts key pages containing GDP tables
- Reduces file size by removing irrelevant pages
- Keyword-based page detection

### Step 3: Clean Tables & Build RTD
- Extracts tables from PDFs using Tabula
- Applies 70+ cleaning transformations
- Handles both OLD (CSV) and NEW (PDF) data sources
- Normalizes sector names, removes noise, converts data types
- Transforms to vintage format

### Step 4: Concatenate RTD
- Merges data across multiple years
- Aligns columns by target period
- Maintains chronological order

### Step 5: Metadata & Benchmarking
- Tracks base-year changes
- Marks revisions affected by base-year updates
- Generates benchmark datasets

### Step 6: Convert to Releases
- Transforms RTD to releases format
- Aligns first, second, third... releases
- Enables econometric analysis

---

## Output Datasets

All datasets are saved in `data/output/`:

**Real-Time Datasets (RTD):**
- `monthly_gdp_rtd.csv` - Monthly GDP growth rates
- `quarterly_annual_gdp_rtd.csv` - Quarterly/annual GDP growth rates

**Base-Year Adjusted:**
- `by_adjusted_monthly_gdp_rtd.csv`
- `by_adjusted_quarterly_annual_gdp_rtd.csv`

**Benchmark Datasets:**
- `monthly_gdp_benchmark.csv`
- `quarterly_annual_gdp_benchmark.csv`
- (+ base-year adjusted versions)

**Releases Datasets:**
- `monthly_gdp_releases.csv`
- `quarterly_annual_gdp_releases.csv`
- (+ benchmark and adjusted versions)

---

## Usage Examples

### Command-Line Interface

```bash
# Run complete pipeline
python scripts/update_rtd.py

# Run steps 3-6 only (skip download and input generation)
python scripts/update_rtd.py --steps 3,4,5,6

# Use custom configuration
python scripts/update_rtd.py --config path/to/custom_config.yaml

# Dry run (see what would be executed)
python scripts/update_rtd.py --dry-run

# Verbose mode for debugging
python scripts/update_rtd.py --verbose
```

### Python API (Future)

```python
from peru_gdp_rtd.config import get_settings
from peru_gdp_rtd.orchestration.pipeline import GDPRTDPipeline

# Initialize pipeline
settings = get_settings('config/config.yaml')
pipeline = GDPRTDPipeline(settings)

# Run all steps
pipeline.run_all()

# Or run individual steps
pipeline.run_step_1_download()
pipeline.run_step_2_generate_inputs()
pipeline.run_step_3_clean_and_build()
# ...
```

### Jupyter Notebooks

Open `notebooks/new_gdp_rtd.ipynb` for a complete, step-by-step walkthrough with explanations and visualizations.

---

## Configuration

All pipeline settings are managed in `config/config.yaml`. Key settings:

```yaml
scraper:
  browser: "chrome"        # Browser: chrome, firefox, edge
  headless: false          # Run in background
  max_downloads: 60        # Maximum PDFs to download

cleaning:
  decimal_places: 1        # Precision for growth rates

features:
  enable_alerts: true      # Play audio alerts
  persist_format: "csv"    # Output format: csv or parquet
```

See `config/config.example.yaml` for all available options.

---

## Requirements

- **Python**: 3.10 or higher
- **Java Runtime Environment (JRE)**: Required for tabula-py (PDF processing)
- **Browser**: Chrome, Firefox, or Edge (for Selenium web scraping)

All Python dependencies are listed in `requirements.txt` and will be installed automatically.

---

## Data Sources

**Main Source**: [BCRP Weekly Reports](https://www.bcrp.gob.pe/publicaciones/nota-semanal.html)

The pipeline processes two types of data:
- **NEW data** (2013+): Digital PDFs with editable tables
- **OLD data** (pre-2013): Scanned PDFs converted to CSV

No external datasets required - everything is downloaded automatically!

---

## Development

### Install Development Dependencies

```bash
pip install -r requirements-dev.txt
```

### Code Formatting

```bash
# Format code with Black
black peru_gdp_rtd/

# Sort imports
isort peru_gdp_rtd/

# Lint
flake8 peru_gdp_rtd/
```

### Running Tests

```bash
pytest tests/
```

---

## Documentation

- **Installation Guide**: See [docs/INSTALLATION.md](docs/INSTALLATION.md) *(coming soon)*
- **Usage Guide**: See [docs/USAGE.md](docs/USAGE.md) *(coming soon)*
- **API Documentation**: See [docs/API.md](docs/API.md) *(coming soon)*
- **Plan File**: See refactoring plan at `.claude/plans/witty-gathering-snail.md`

---

## Project Status

**Current Status**: ✅ Foundation Complete (Week 1)
- [x] Package structure created
- [x] Configuration system implemented
- [x] One-button update script created
- [x] Dependencies and packaging configured
- [ ] Pipeline modules (Weeks 2-5) - *In progress*
- [ ] Documentation (Week 7)
- [ ] Testing (Week 8)

See the [refactoring plan](.claude/plans/witty-gathering-snail.md) for complete roadmap.

---

## Research Context

This project supports the research paper:

**"Rationality and Nowcasting on Peruvian GDP Revisions"**
by Jason Cruz

The datasets generated by this pipeline enable analysis of:
- GDP revision patterns in emerging markets
- Nowcasting accuracy using real-time data
- Information content of preliminary releases

---

## Contributing

Contributions are welcome! Please:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/amazing-feature`)
3. Format code with Black (`black .`)
4. Run tests (`pytest`)
5. Commit changes (`git commit -m 'Add amazing feature'`)
6. Push to branch (`git push origin feature/amazing-feature`)
7. Open a Pull Request

---

## Citation

If you use this dataset or code in your research, please cite:

```bibtex
@article{cruz2024gdp,
  title={Rationality and Nowcasting on Peruvian GDP Revisions},
  author={Cruz, Jason},
  year={2024},
  institution={Universidad del Pac\'ifico - CIUP}
}
```

---

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## Acknowledgments

- **Central Reserve Bank of Peru (BCRP)** for providing public access to Weekly Reports
- **Universidad del Pacífico - CIUP** for research support

---

## Contact

**Jason Cruz**
Email: jj.cruza@up.edu.pe
GitHub: [@JasonCruz18](https://github.com/JasonCruz18)

---

## Troubleshooting

### Common Issues

**Problem**: `FileNotFoundError: config/config.yaml not found`
**Solution**: Copy the example config: `cp config/config.example.yaml config/config.yaml`

**Problem**: `tabula-py` errors
**Solution**: Install Java Runtime Environment (JRE)

**Problem**: Selenium browser errors
**Solution**: Ensure Chrome/Firefox/Edge is installed and up to date

For more help, open an issue on GitHub.

---

**Note**: This README reflects the new modular architecture. The refactoring from the legacy monolithic structure is currently in progress (Week 1 complete). See the plan file for details.
