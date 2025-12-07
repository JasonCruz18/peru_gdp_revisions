"""Smoke tests for Peru GDP RTD package.

These tests verify basic functionality and integration between components.
"""

import sys
from pathlib import Path

# Add package to path
sys.path.insert(0, str(Path(__file__).parent.parent))


def test_package_import():
    """Test that the package can be imported."""
    import peru_gdp_rtd

    assert peru_gdp_rtd.__version__ == "1.0.0"
    assert peru_gdp_rtd.__author__ == "Jason Cruz"


def test_config_loading():
    """Test that configuration can be loaded."""
    from peru_gdp_rtd.config import get_settings

    settings = get_settings("config/config.yaml")

    assert settings.project["name"] == "Peru GDP RTD"
    assert settings.project["version"] == "1.0.0"
    assert settings.scraper.browser in ["chrome", "firefox", "edge"]
    assert settings.cleaning.decimal_places == 1
    assert settings.cleaning.pipeline_version == "s3.0.0"


def test_config_paths():
    """Test that configuration paths are resolved correctly."""
    from peru_gdp_rtd.config import get_settings

    settings = get_settings("config/config.yaml", force_reload=True)

    # Check that paths exist
    assert settings.paths.data_root.parent.exists()
    assert "gdp_revisions_datasets" in str(settings.paths.data_root)

    # Check path names
    assert settings.paths.data_root.name == "data"
    assert settings.paths.metadata.name == "metadata"
    assert settings.paths.record.name == "record"


def test_sector_mappings():
    """Test that sector mappings are loaded correctly."""
    from peru_gdp_rtd.config import get_settings

    settings = get_settings("config/config.yaml", force_reload=True)

    # Check English mappings
    assert "agriculture" in settings.cleaning.sector_mappings_english.values()
    assert "fishing" in settings.cleaning.sector_mappings_english.values()
    assert "mining" in settings.cleaning.sector_mappings_english.values()

    # Check Spanish mappings
    assert "agropecuario" in settings.cleaning.sector_mappings_spanish.values()
    assert "pesca" in settings.cleaning.sector_mappings_spanish.values()


def test_month_mappings():
    """Test that month mappings are loaded correctly."""
    from peru_gdp_rtd.config import get_settings

    settings = get_settings("config/config.yaml", force_reload=True)

    assert settings.cleaning.month_mappings["ene"] == 1
    assert settings.cleaning.month_mappings["feb"] == 2
    assert settings.cleaning.month_mappings["dic"] == 12


def test_base_years():
    """Test that base year information is loaded correctly."""
    from peru_gdp_rtd.config import get_settings

    settings = get_settings("config/config.yaml", force_reload=True)

    assert len(settings.metadata.base_years) >= 4

    # Check first base year entry
    first_by = settings.metadata.base_years[0]
    assert first_by.year == 1994
    assert first_by.wr == 1
    assert first_by.base_year == 1990


def test_all_modules_importable():
    """Test that all submodules can be imported."""
    modules = [
        "peru_gdp_rtd",
        "peru_gdp_rtd.config",
        "peru_gdp_rtd.scrapers",
        "peru_gdp_rtd.processors",
        "peru_gdp_rtd.cleaners",
        "peru_gdp_rtd.transformers",
        "peru_gdp_rtd.orchestration",
        "peru_gdp_rtd.utils",
    ]

    for module_name in modules:
        module = __import__(module_name)
        assert module is not None


if __name__ == "__main__":
    print("Running smoke tests...")
    print()

    tests = [
        ("Package import", test_package_import),
        ("Config loading", test_config_loading),
        ("Config paths", test_config_paths),
        ("Sector mappings", test_sector_mappings),
        ("Month mappings", test_month_mappings),
        ("Base years", test_base_years),
        ("All modules importable", test_all_modules_importable),
    ]

    passed = 0
    failed = 0

    for test_name, test_func in tests:
        try:
            test_func()
            print(f"[PASS] {test_name}")
            passed += 1
        except AssertionError as e:
            print(f"[FAIL] {test_name}: {e}")
            failed += 1
        except Exception as e:
            print(f"[ERROR] {test_name}: {e}")
            failed += 1

    print()
    print(f"Results: {passed} passed, {failed} failed")

    if failed == 0:
        print()
        print("[OK] All smoke tests passed!")
        sys.exit(0)
    else:
        sys.exit(1)
