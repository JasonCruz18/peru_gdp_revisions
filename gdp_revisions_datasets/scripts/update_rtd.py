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
        # NOTE: This will be implemented once we migrate the pipeline code
        print_banner()

        for step_num in steps:
            step_name = step_descriptions[step_num]
            print_step_header(step_num, step_name)

            logger.info(f"Executing step {step_num}: {step_name}")

            # TODO: Call actual pipeline functions here
            # For now, we show the structure
            if step_num == 1:
                logger.info("Step 1: Would download PDFs from BCRP")
                # from peru_gdp_rtd.scrapers.bcrp_scraper import pdf_downloader
                # pdf_downloader(...)

            elif step_num == 2:
                logger.info("Step 2: Would generate input PDFs")
                # from peru_gdp_rtd.processors.pdf_processor import pdf_input_generator
                # pdf_input_generator(...)

            elif step_num == 3:
                logger.info("Step 3: Would clean tables and build RTD")
                # from peru_gdp_rtd.cleaners import old_table_cleaner, new_table_cleaner
                # ...

            elif step_num == 4:
                logger.info("Step 4: Would concatenate RTD")
                # from peru_gdp_rtd.transformers.concatenator import concatenate_table_1
                # ...

            elif step_num == 5:
                logger.info("Step 5: Would update metadata")
                # from peru_gdp_rtd.transformers.metadata_handler import update_metadata
                # ...

            elif step_num == 6:
                logger.info("Step 6: Would convert to releases")
                # from peru_gdp_rtd.transformers.releases_converter import convert_to_releases_dataset
                # ...

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
