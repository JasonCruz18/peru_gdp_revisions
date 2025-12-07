#!/usr/bin/env python
"""Quick test script to verify Peru GDP RTD setup.

Run this script to quickly verify that everything is working correctly.

Usage:
    python quick_test.py
"""

import sys
from pathlib import Path


def print_header(text):
    """Print formatted header."""
    print()
    print("=" * 70)
    print(f"  {text}")
    print("=" * 70)
    print()


def print_test(test_name, status="PASS", details=None):
    """Print test result."""
    symbol = "[OK]" if status == "PASS" else "[FAIL]"
    print(f"{symbol} {test_name}")
    if details:
        for line in details:
            print(f"     {line}")


def main():
    """Run quick tests."""
    print_header("Peru GDP RTD - Quick Setup Test")

    failed_tests = []

    # Test 1: Package import
    try:
        import peru_gdp_rtd

        print_test(
            "Package import",
            details=[
                f"Version: {peru_gdp_rtd.__version__}",
                f"Author: {peru_gdp_rtd.__author__}",
            ],
        )
    except Exception as e:
        print_test("Package import", "FAIL", [str(e)])
        failed_tests.append("Package import")

    # Test 2: Configuration loading
    try:
        from peru_gdp_rtd.config import get_settings

        settings = get_settings("config/config.yaml")
        print_test(
            "Configuration loading",
            details=[
                f"Project: {settings.project['name']}",
                f"Version: {settings.project['version']}",
            ],
        )
    except Exception as e:
        print_test("Configuration loading", "FAIL", [str(e)])
        failed_tests.append("Configuration loading")
        return 1

    # Test 3: Path resolution
    try:
        data_root = str(settings.paths.data_root)
        if "gdp_revisions_datasets" in data_root:
            print_test(
                "Path resolution",
                details=[f"Data root: ...{data_root[-50:]}"],
            )
        else:
            print_test(
                "Path resolution",
                "FAIL",
                ["Data root doesn't contain 'gdp_revisions_datasets'"],
            )
            failed_tests.append("Path resolution")
    except Exception as e:
        print_test("Path resolution", "FAIL", [str(e)])
        failed_tests.append("Path resolution")

    # Test 4: Configuration values
    try:
        assert settings.scraper.browser in ["chrome", "firefox", "edge"]
        assert settings.cleaning.decimal_places == 1
        assert len(settings.cleaning.month_mappings) == 12
        assert len(settings.metadata.base_years) >= 4

        print_test(
            "Configuration values",
            details=[
                f"Browser: {settings.scraper.browser}",
                f"Decimal places: {settings.cleaning.decimal_places}",
                f"Month mappings: {len(settings.cleaning.month_mappings)}",
                f"Base years: {len(settings.metadata.base_years)}",
            ],
        )
    except Exception as e:
        print_test("Configuration values", "FAIL", [str(e)])
        failed_tests.append("Configuration values")

    # Test 5: All modules import
    try:
        modules = [
            "peru_gdp_rtd.config",
            "peru_gdp_rtd.scrapers",
            "peru_gdp_rtd.processors",
            "peru_gdp_rtd.cleaners",
            "peru_gdp_rtd.transformers",
            "peru_gdp_rtd.orchestration",
            "peru_gdp_rtd.utils",
        ]

        for module_name in modules:
            __import__(module_name)

        print_test("All modules import", details=[f"{len(modules)} modules imported"])
    except Exception as e:
        print_test("All modules import", "FAIL", [str(e)])
        failed_tests.append("All modules import")

    # Test 6: Update script exists
    try:
        script_path = Path("scripts/update_rtd.py")
        assert script_path.exists()
        size = script_path.stat().st_size

        print_test("Update script", details=[f"Size: {size:,} bytes"])
    except Exception as e:
        print_test("Update script", "FAIL", [str(e)])
        failed_tests.append("Update script")

    # Test 7: Dependencies
    try:
        deps = ["yaml", "pandas", "numpy", "requests", "selenium"]
        missing = []

        for dep in deps:
            try:
                __import__(dep)
            except ImportError:
                missing.append(dep)

        if not missing:
            print_test("Dependencies", details=[f"{len(deps)} packages installed"])
        else:
            print_test(
                "Dependencies",
                "FAIL",
                [f"Missing: {', '.join(missing)}"],
            )
            failed_tests.append("Dependencies")
    except Exception as e:
        print_test("Dependencies", "FAIL", [str(e)])
        failed_tests.append("Dependencies")

    # Summary
    print()
    print("=" * 70)
    total_tests = 7
    passed = total_tests - len(failed_tests)

    if not failed_tests:
        print(f"  SUCCESS! All {total_tests} tests passed!")
        print("=" * 70)
        print()
        print("Your setup is working perfectly!")
        print()
        print("Next steps:")
        print("  1. Run: python tests/test_smoke.py")
        print("  2. Run: python scripts/update_rtd.py --dry-run")
        print("  3. You're ready for Week 2 (module migration)")
        print()
        return 0
    else:
        print(f"  {passed}/{total_tests} tests passed, {len(failed_tests)} failed")
        print("=" * 70)
        print()
        print("Failed tests:")
        for test in failed_tests:
            print(f"  - {test}")
        print()
        print("See HOW_TO_TEST.md for troubleshooting help")
        print()
        return 1


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        print("\nTest interrupted by user")
        sys.exit(130)
    except Exception as e:
        print(f"\nUnexpected error: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)
