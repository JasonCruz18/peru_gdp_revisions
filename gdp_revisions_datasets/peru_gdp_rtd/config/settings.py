"""Configuration management for Peru GDP RTD pipeline.

This module provides configuration loading and validation from YAML files.
All pipeline settings are centralized here to eliminate hardcoding.
"""

import os
from pathlib import Path
from typing import Any, Dict, List, Optional
import yaml
from dataclasses import dataclass, field


@dataclass
class PathConfig:
    """Path configuration container."""

    data_root: Path
    data_input: Path
    data_output: Path
    pdf_root: Path
    pdf_raw: Path
    pdf_input: Path
    old_weekly_reports: Path
    metadata: Path
    record: Path
    alert_track: Path


@dataclass
class HTTPConfig:
    """HTTP request configuration."""

    chunk_size: int = 128
    timeout: int = 60
    retries: int = 3
    backoff_factor: float = 0.5
    retry_statuses: List[int] = field(default_factory=lambda: [429, 500, 502, 503, 504])


@dataclass
class SeleniumConfig:
    """Selenium WebDriver configuration."""

    page_load_timeout: int = 30
    explicit_wait_timeout: int = 60


@dataclass
class ScraperConfig:
    """Web scraper configuration."""

    bcrp_url: str
    browser: str
    headless: bool
    max_downloads: int
    downloads_per_batch: int
    min_wait: float
    max_wait: float
    css_selectors: Dict[str, str]
    http: HTTPConfig
    selenium: SeleniumConfig


@dataclass
class PDFProcessingConfig:
    """PDF processing configuration."""

    keywords: List[str]
    pages_to_extract: List[int]


@dataclass
class CleaningConfig:
    """Data cleaning configuration."""

    decimal_places: int
    pipeline_version: str
    sector_mappings_spanish: Dict[str, str]
    sector_mappings_english: Dict[str, str]
    month_mappings: Dict[str, int]


@dataclass
class BaseYearInfo:
    """Base year change information."""

    year: int
    wr: int
    base_year: int


@dataclass
class MetadataConfig:
    """Metadata configuration."""

    filename: str
    base_years: List[BaseYearInfo]


@dataclass
class BenchmarkConfig:
    """Benchmark dataset configuration."""

    sentinel_value: float
    base_year_periods: List[str]


@dataclass
class LoggingConfig:
    """Logging configuration."""

    level: str
    format: str
    file: str
    console: bool


@dataclass
class FeaturesConfig:
    """Feature flags configuration."""

    enable_alerts: bool
    persist_intermediate: bool
    persist_format: str
    validate_data: bool


@dataclass
class Settings:
    """Main settings container for Peru GDP RTD pipeline.

    This class encapsulates all configuration settings loaded from YAML files.
    It provides type-safe access to configuration values and eliminates hardcoding.

    Attributes:
        project: Project metadata (name, version, root directory)
        paths: File system paths for data, PDFs, metadata, etc.
        scraper: Web scraping and download settings
        pdf_processing: PDF processing settings
        cleaning: Data cleaning settings
        metadata: Metadata management settings
        benchmark: Benchmark dataset settings
        record_files: Record file names for tracking processed files
        output_files: Output file names for generated datasets
        logging: Logging configuration
        features: Feature flags
    """

    project: Dict[str, Any]
    paths: PathConfig
    scraper: ScraperConfig
    pdf_processing: PDFProcessingConfig
    cleaning: CleaningConfig
    metadata: MetadataConfig
    benchmark: BenchmarkConfig
    record_files: Dict[str, str]
    output_files: Dict[str, str]
    logging: LoggingConfig
    features: FeaturesConfig

    @classmethod
    def from_yaml(cls, config_path: Optional[str] = None) -> "Settings":
        """Load settings from YAML configuration file.

        Args:
            config_path: Path to YAML configuration file.
                        If None, uses default config/config.yaml

        Returns:
            Settings instance with loaded configuration

        Raises:
            FileNotFoundError: If config file doesn't exist
            yaml.YAMLError: If config file has invalid YAML syntax
            KeyError: If required configuration keys are missing
        """
        if config_path is None:
            # Try to find config.yaml in expected locations
            possible_paths = [
                Path("config/config.yaml"),
                Path("../config/config.yaml"),
                Path(__file__).parent.parent.parent / "config" / "config.yaml",
            ]
            config_path = None
            for p in possible_paths:
                if p.exists():
                    config_path = p
                    break

            if config_path is None:
                raise FileNotFoundError(
                    "Could not find config/config.yaml. "
                    "Please create it or specify path explicitly."
                )
        else:
            config_path = Path(config_path)

        if not config_path.exists():
            raise FileNotFoundError(f"Configuration file not found: {config_path}")

        # Load YAML configuration
        with open(config_path, "r", encoding="utf-8") as f:
            config = yaml.safe_load(f)

        # Determine root directory
        root_dir = Path(config["project"].get("root_dir", "."))
        if not root_dir.is_absolute():
            # Make relative to project root (parent of config directory)
            # If root_dir is ".", use parent of config directory
            if str(root_dir) == ".":
                root_dir = config_path.parent.parent.resolve()
            else:
                root_dir = (config_path.parent.parent / root_dir).resolve()

        # Parse paths
        paths_dict = config["paths"]
        path_config = PathConfig(
            data_root=root_dir / paths_dict["data_root"],
            data_input=root_dir / paths_dict["data_input"],
            data_output=root_dir / paths_dict["data_output"],
            pdf_root=root_dir / paths_dict["pdf_root"],
            pdf_raw=root_dir / paths_dict["pdf_raw"],
            pdf_input=root_dir / paths_dict["pdf_input"],
            old_weekly_reports=root_dir / paths_dict["old_weekly_reports"],
            metadata=root_dir / paths_dict["metadata"],
            record=root_dir / paths_dict["record"],
            alert_track=root_dir / paths_dict["alert_track"],
        )

        # Parse scraper config
        scraper_dict = config["scraper"]
        http_config = HTTPConfig(**scraper_dict["http"])
        selenium_config = SeleniumConfig(**scraper_dict["selenium"])

        scraper_config = ScraperConfig(
            bcrp_url=scraper_dict["bcrp_url"],
            browser=scraper_dict["browser"],
            headless=scraper_dict["headless"],
            max_downloads=scraper_dict["max_downloads"],
            downloads_per_batch=scraper_dict["downloads_per_batch"],
            min_wait=scraper_dict["min_wait"],
            max_wait=scraper_dict["max_wait"],
            css_selectors=scraper_dict["css_selectors"],
            http=http_config,
            selenium=selenium_config,
        )

        # Parse PDF processing config
        pdf_config = PDFProcessingConfig(**config["pdf_processing"])

        # Parse cleaning config
        cleaning_config = CleaningConfig(**config["cleaning"])

        # Parse metadata config
        metadata_dict = config["metadata"]
        base_years = [BaseYearInfo(**by) for by in metadata_dict["base_years"]]
        metadata_config = MetadataConfig(filename=metadata_dict["filename"], base_years=base_years)

        # Parse benchmark config
        benchmark_config = BenchmarkConfig(**config["benchmark"])

        # Parse logging config
        logging_config = LoggingConfig(**config["logging"])

        # Parse features config
        features_config = FeaturesConfig(**config["features"])

        return cls(
            project=config["project"],
            paths=path_config,
            scraper=scraper_config,
            pdf_processing=pdf_config,
            cleaning=cleaning_config,
            metadata=metadata_config,
            benchmark=benchmark_config,
            record_files=config["record_files"],
            output_files=config["output_files"],
            logging=logging_config,
            features=features_config,
        )

    def ensure_directories(self) -> None:
        """Create all required directories if they don't exist."""
        directories = [
            self.paths.data_root,
            self.paths.data_input,
            self.paths.data_output,
            self.paths.pdf_root,
            self.paths.pdf_raw,
            self.paths.pdf_input,
            self.paths.metadata,
            self.paths.record,
            self.paths.alert_track,
        ]

        for directory in directories:
            directory.mkdir(parents=True, exist_ok=True)


# Global settings instance
_settings: Optional[Settings] = None


def get_settings(config_path: Optional[str] = None, force_reload: bool = False) -> Settings:
    """Get or create global settings instance.

    This function implements a singleton pattern for settings,
    ensuring configuration is loaded only once unless explicitly reloaded.

    Args:
        config_path: Path to YAML configuration file. If None, uses default.
        force_reload: If True, reload settings even if already loaded.

    Returns:
        Settings instance with loaded configuration

    Example:
        >>> settings = get_settings()
        >>> print(settings.scraper.bcrp_url)
        https://www.bcrp.gob.pe/publicaciones/nota-semanal.html
    """
    global _settings

    if _settings is None or force_reload:
        _settings = Settings.from_yaml(config_path)
        _settings.ensure_directories()

    return _settings
