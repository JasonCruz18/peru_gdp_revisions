#!/usr/bin/env python
"""One-button GDP RTD update script.

This script provides a simple command-line interface for updating the
Peru GDP Real-Time Dataset. It orchestrates the complete pipeline or
individual steps based on user preferences.

Usage:
    # Run complete pipeline (all steps)
    python scripts/update_rtd.py

    # Use custom configuration file
    python scripts/update_rtd.py --config path/to/config.yaml

    # Run specific steps only
    python scripts/update_rtd.py --steps 1,2,3

    # Skip PDF download step (useful for testing)
    python scripts/update_rtd.py --skip-download

    # Verbose output for debugging
    python scripts/update_rtd.py --verbose

Examples:
    # Update everything with default settings
    python scripts/update_rtd.py

    # Only clean and build RTD (skip download and input generation)
    python scripts/update_rtd.py --steps 3,4,5,6

    # Run in verbose mode to see detailed logs
    python scripts/update_rtd.py --verbose
"""

import argparse
import sys
import logging
from pathlib import Path
from typing import List

# Add package to path (allows running without installation)
SCRIPT_DIR = Path(__file__).parent
PROJECT_ROOT = SCRIPT_DIR.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Note: These imports will work once we create the pipeline module
# For now, this script provides the structure


def setup_logging(verbose: bool = False) -> logging.Logger:
    """Configure logging for the update script.

    Args:
        verbose: If True, set log level to DEBUG. Otherwise, INFO.

    Returns:
        Configured logger instance
    """
    log_level = logging.DEBUG if verbose else logging.INFO

    logging.basicConfig(
        level=log_level,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logger = logging.getLogger("update_rtd")
    return logger


def parse_args() -> argparse.Namespace:
    """Parse command-line arguments.

    Returns:
        Parsed arguments
    """
    parser = argparse.ArgumentParser(
        description="Update Peru GDP Real-Time Dataset",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  %(prog)s                          # Run all steps
  %(prog)s --steps 3,4,5,6          # Run steps 3-6 only
  %(prog)s --skip-download          # Skip PDF download
  %(prog)s --verbose                # Verbose output
  %(prog)s --config custom.yaml     # Use custom config
        """,
    )

    parser.add_argument(
        "--config",
        "-c",
        type=str,
        default="config/config.yaml",
        help="Path to configuration file (default: config/config.yaml)",
    )

    parser.add_argument(
        "--steps",
        "-s",
        type=str,
        help="Comma-separated list of steps to run (e.g., '1,2,3'). "
        "Steps: 1=Download PDFs, 2=Generate inputs, 3=Clean & build RTD, "
        "4=Concatenate, 5=Metadata, 6=Releases",
    )

    parser.add_argument(
        "--skip-download",
        action="store_true",
        help="Skip PDF download step (step 1)",
    )

    parser.add_argument(
        "--verbose",
        "-v",
        action="store_true",
        help="Enable verbose logging (DEBUG level)",
    )

    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Show what would be done without executing",
    )

    return parser.parse_args()


def print_banner() -> None:
    """Print welcome banner."""
    print("=" * 70)
    print(" " * 15 + "Peru GDP Real-Time Dataset Update")
    print("=" * 70)
    print()


def print_step_header(step_num: int, step_name: str) -> None:
    """Print formatted step header.

    Args:
        step_num: Step number (1-6)
        step_name: Step description
    """
    print()
    print("=" * 70)
    print(f"  STEP {step_num}: {step_name}")
    print("=" * 70)
    print()


def print_completion_summary() -> None:
    """Print completion banner."""
    print()
    print("=" * 70)
    print(" " * 20 + "✓ Pipeline Completed Successfully!")
    print("=" * 70)
    print()
    print("Next steps:")
    print("  - Check data/output/ for generated datasets")
    print("  - Review logs/ for execution details")
    print("  - Use notebooks/ for data exploration")
    print()


def run_pipeline(args: argparse.Namespace, logger: logging.Logger) -> int:
    """Run the GDP RTD pipeline.

    Args:
        args: Parsed command-line arguments
        logger: Logger instance

    Returns:
        Exit code (0 for success, 1 for failure)
    """
    try:
        # Import settings (will fail if config is invalid)
        from peru_gdp_rtd.config import get_settings

        logger.info(f"Loading configuration from: {args.config}")
        settings = get_settings(args.config)

        logger.info(f"Project: {settings.project['name']} v{settings.project['version']}")
        logger.info(f"Root directory: {settings.paths.data_root.parent}")

        # Determine which steps to run
        if args.steps:
            steps = [int(s.strip()) for s in args.steps.split(",")]
            logger.info(f"Running selected steps: {steps}")
        else:
            steps = list(range(1, 7))  # All steps
            logger.info("Running all pipeline steps (1-6)")

        # Remove step 1 if --skip-download
        if args.skip_download and 1 in steps:
            steps.remove(1)
            logger.info("Skipping PDF download (step 1)")

        # Validate step numbers
        invalid_steps = [s for s in steps if s < 1 or s > 6]
        if invalid_steps:
            logger.error(f"Invalid step numbers: {invalid_steps}. Must be 1-6.")
            return 1

        if args.dry_run:
            logger.info("DRY RUN MODE - No changes will be made")
            logger.info(f"Would run steps: {steps}")
            return 0

        # Define step descriptions
        step_descriptions = {
            1: "Download PDFs from BCRP website",
            2: "Generate input PDFs (extract key pages)",
            3: "Clean tables and build RTD",
            4: "Concatenate RTD across years",
            5: "Update metadata and create benchmarks",
            6: "Convert RTD to releases dataset",
        }

        # Execute pipeline steps
        print_banner()

        for step_num in steps:
            step_name = step_descriptions[step_num]
            print_step_header(step_num, step_name)

            logger.info(f"Executing step {step_num}: {step_name}")

            # STEP 1: Download PDFs from BCRP website
            if step_num == 1:
                from peru_gdp_rtd.scrapers.bcrp_scraper import pdf_downloader

                pdf_downloader(
                    start_year=2011,  # TODO: Add to config
                    end_year=2025,  # TODO: Add to config
                    output_folder=str(settings.paths.pdf_raw),
                    record_folder=str(settings.paths.record),
                    record_txt=settings.record_files["downloaded_pdfs"],
                    max_retries=settings.scraper.http.retries,
                    retry_delay=settings.scraper.http.backoff_factor,
                    download_delay=settings.scraper.min_wait,
                )

            # STEP 2: Generate input PDFs (extract key pages)
            elif step_num == 2:
                from peru_gdp_rtd.processors.pdf_processor import pdf_input_generator

                pdf_input_generator(
                    search_pdf_folder=str(settings.paths.pdf_raw),
                    output_pdf_folder=str(settings.paths.pdf_input),
                    keywords=settings.pdf_processing.keywords,
                    record_folder=str(settings.paths.record),
                    record_txt=settings.record_files["generated_inputs"],
                    max_hits=2,  # Extract 2 pages per PDF
                )

            # STEP 3: Clean tables and build vintage-format RTD
            elif step_num == 3:
                from peru_gdp_rtd.orchestration.runners import (
                    old_table_1_runner,
                    old_table_2_runner,
                    new_table_1_runner,
                    new_table_2_runner,
                )

                # OLD CSV Tables (2011-2020)
                logger.info("Processing OLD CSV Table 1 (monthly GDP)...")
                old_table_1_runner(
                    input_csv_folder=str(settings.paths.old_weekly_reports),
                    record_folder=str(settings.paths.record),
                    record_txt=settings.record_files["created_old_rtd_tab_1"],
                    persist=True,
                    persist_folder=str(settings.paths.data_output / "vintages" / "old"),
                    pipeline_version=settings.project["version"],
                    sep=";",
                )

                logger.info("Processing OLD CSV Table 2 (quarterly/annual GDP)...")
                old_table_2_runner(
                    input_csv_folder=str(settings.paths.old_weekly_reports),
                    record_folder=str(settings.paths.record),
                    record_txt=settings.record_files["created_old_rtd_tab_2"],
                    persist=True,
                    persist_folder=str(settings.paths.data_output / "vintages" / "old"),
                    pipeline_version=settings.project["version"],
                    sep=";",
                )

                # NEW PDF Tables (2021+)
                logger.info("Processing NEW PDF Table 1 (monthly GDP)...")
                new_table_1_runner(
                    input_pdf_folder=str(settings.paths.pdf_input),
                    record_folder=str(settings.paths.record),
                    record_txt=settings.record_files["created_new_rtd_tab_1"],
                    persist=True,
                    persist_folder=str(settings.paths.data_output / "vintages" / "new"),
                    pipeline_version=settings.project["version"],
                )

                logger.info("Processing NEW PDF Table 2 (quarterly/annual GDP)...")
                new_table_2_runner(
                    input_pdf_folder=str(settings.paths.pdf_input),
                    record_folder=str(settings.paths.record),
                    record_txt=settings.record_files["created_new_rtd_tab_2"],
                    persist=True,
                    persist_folder=str(settings.paths.data_output / "vintages" / "new"),
                    pipeline_version=settings.project["version"],
                )

            # STEP 4: Concatenate vintages across all years
            elif step_num == 4:
                from peru_gdp_rtd.transformers.concatenator import (
                    concatenate_table_1,
                    concatenate_table_2,
                )

                logger.info("Concatenating Table 1 vintages (monthly GDP)...")
                concatenate_table_1(
                    input_data_subfolder=str(settings.paths.data_output / "vintages"),
                    record_folder=str(settings.paths.record),
                    record_txt=settings.record_files["concatenated_tab_1"],
                    persist=True,
                    persist_folder=str(settings.paths.data_output),
                    csv_file_label=settings.output_files["monthly_rtd"].replace(".csv", ""),
                )

                logger.info("Concatenating Table 2 vintages (quarterly/annual GDP)...")
                concatenate_table_2(
                    input_data_subfolder=str(settings.paths.data_output / "vintages"),
                    record_folder=str(settings.paths.record),
                    record_txt=settings.record_files["concatenated_tab_2"],
                    persist=True,
                    persist_folder=str(settings.paths.data_output),
                    csv_file_label=settings.output_files["quarterly_annual_rtd"].replace(
                        ".csv", ""
                    ),
                )

            # STEP 5: Update metadata and create adjusted datasets
            elif step_num == 5:
                from peru_gdp_rtd.transformers.metadata_handler import (
                    update_metadata,
                    apply_base_year_sentinel,
                    convert_to_benchmark_dataset,
                )

                # Convert base_years to dict format for update_metadata
                base_year_list = [
                    {"year": by.year, "wr": by.wr, "base_year": by.base_year}
                    for by in settings.metadata.base_years
                ]

                # Update metadata with new revisions
                logger.info("Updating metadata from Weekly Report PDFs...")
                update_metadata(
                    metadata_folder=str(settings.paths.metadata),
                    input_pdf_folder=str(settings.paths.pdf_input),
                    record_folder=str(settings.paths.record),
                    record_txt=settings.record_files["metadata_updated"],
                    wr_metadata_csv=settings.metadata.filename,
                    base_year_list=base_year_list,
                )

                # Apply base-year sentinel to mark invalid comparisons
                logger.info("Applying base-year sentinel to RTDs...")
                apply_base_year_sentinel(
                    base_year_vintages=settings.benchmark.base_year_periods,
                    sentinel=settings.benchmark.sentinel_value,
                    output_data_subfolder=str(settings.paths.data_output),
                    csv_file_labels=[
                        settings.output_files["monthly_rtd"].replace(".csv", ""),
                        settings.output_files["quarterly_annual_rtd"].replace(".csv", ""),
                    ],
                )

                # Generate benchmark revision indicator datasets
                logger.info("Converting to benchmark datasets...")
                convert_to_benchmark_dataset(
                    output_data_subfolder=str(settings.paths.data_output),
                    csv_file_labels=[
                        settings.output_files["monthly_rtd"].replace(".csv", ""),
                        settings.output_files["quarterly_annual_rtd"].replace(".csv", ""),
                    ],
                    metadata_folder=str(settings.paths.metadata),
                    wr_metadata_csv=settings.metadata.filename,
                    record_folder=str(settings.paths.record),
                    record_txt=settings.record_files["benchmark_converted"],
                    benchmark_dataset_labels=[
                        settings.output_files["monthly_benchmark"].replace(".csv", ""),
                        settings.output_files["quarterly_benchmark"].replace(".csv", ""),
                    ],
                )

            # STEP 6: Convert RTD to releases format
            elif step_num == 6:
                from peru_gdp_rtd.transformers.releases_converter import (
                    convert_to_releases_dataset,
                )

                logger.info("Converting RTDs to releases format...")
                convert_to_releases_dataset(
                    output_data_subfolder=str(settings.paths.data_output),
                    csv_file_labels=[
                        settings.output_files["monthly_rtd"].replace(".csv", ""),
                        settings.output_files["quarterly_annual_rtd"].replace(".csv", ""),
                        settings.output_files["by_adjusted_monthly"].replace(".csv", ""),
                        settings.output_files["by_adjusted_quarterly"].replace(".csv", ""),
                    ],
                    record_folder=str(settings.paths.record),
                    record_txt=settings.record_files["releases_converted"],
                    releases_dataset_labels=[
                        settings.output_files["monthly_releases"].replace(".csv", ""),
                        settings.output_files["quarterly_releases"].replace(".csv", ""),
                        settings.output_files["by_adjusted_monthly_releases"].replace(".csv", ""),
                        settings.output_files["by_adjusted_quarterly_releases"].replace(".csv", ""),
                    ],
                )

            logger.info(f"Step {step_num} completed successfully")

        print_completion_summary()
        return 0

    except FileNotFoundError as e:
        logger.error(f"Configuration error: {e}")
        logger.error("Please ensure config/config.yaml exists")
        return 1

    except ImportError as e:
        logger.error(f"Import error: {e}")
        logger.error(
            "Pipeline modules not yet implemented. "
            "This script provides the structure for future implementation."
        )
        return 0  # Return 0 for now since we're still building

    except Exception as e:
        logger.exception(f"Pipeline failed with error: {e}")
        return 1


def main() -> int:
    """Main entry point.

    Returns:
        Exit code (0 for success, non-zero for failure)
    """
    args = parse_args()
    logger = setup_logging(verbose=args.verbose)

    try:
        return run_pipeline(args, logger)
    except KeyboardInterrupt:
        logger.warning("\nPipeline interrupted by user")
        return 130  # Standard exit code for SIGINT


if __name__ == "__main__":
    sys.exit(main())
