# How to Test Your Setup - Step-by-Step Guide

This guide will help you verify that the Week 1 foundation is working correctly on your system.

---

## Prerequisites

- Python 3.10+ installed
- Terminal/Command Prompt access
- You're in the correct directory

---

## Step 1: Navigate to Project Directory

Open your terminal and navigate to the project:

```bash
cd "C:\Users\Jason Cruz\OneDrive\Documentos\RA\CIUP\GDP Revisions\GitHub\peru_gdp_revisions\gdp_revisions_datasets"
```

Verify you're in the right place:
```bash
# Windows Command Prompt
dir

# You should see:
# - peru_gdp_rtd/ folder
# - config/ folder
# - scripts/ folder
# - README.md
# - pyproject.toml
```

---

## Step 2: Check Python Version

Verify your Python version:

```bash
python --version
```

**Expected output**: `Python 3.12.1` (or 3.10+)

If you see a different version or error:
- Make sure you're using the correct conda environment
- Activate it: `conda activate gdp_revisions`

---

## Step 3: Test Package Import

Test that the package can be imported:

```bash
python -c "import peru_gdp_rtd; print('✓ Package imported successfully'); print('  Version:', peru_gdp_rtd.__version__); print('  Author:', peru_gdp_rtd.__author__)"
```

**Expected output**:
```
✓ Package imported successfully
  Version: 1.0.0
  Author: Jason Cruz
```

**If you get an error**:
- Check that you're in the `gdp_revisions_datasets` directory
- The error will tell you what's wrong

---

## Step 4: Test Configuration System

Test that configuration loads correctly:

```bash
python -c "from peru_gdp_rtd.config import get_settings; s = get_settings(); print('✓ Configuration loaded'); print('  Project:', s.project['name']); print('  Browser:', s.scraper.browser); print('  Data root:', s.paths.data_root)"
```

**Expected output** (paths will match your system):
```
✓ Configuration loaded
  Project: Peru GDP RTD
  Browser: chrome
  Data root: C:\Users\Jason Cruz\OneDrive\...\gdp_revisions_datasets\data
```

**What to check**:
- ✅ No errors
- ✅ Data root path ends with `gdp_revisions_datasets\data`
- ✅ Browser is set to your preference (chrome/firefox/edge)

---

## Step 5: Test Configuration Details

Get more detailed configuration info:

```bash
python -c "from peru_gdp_rtd.config import get_settings; s = get_settings(); print('Configuration Details:'); print('  BCRP URL:', s.scraper.bcrp_url); print('  Max downloads:', s.scraper.max_downloads); print('  Decimal places:', s.cleaning.decimal_places); print('  Pipeline version:', s.cleaning.pipeline_version); print('  Month mappings:', len(s.cleaning.month_mappings), 'months'); print('  Base years:', len(s.metadata.base_years), 'entries')"
```

**Expected output**:
```
Configuration Details:
  BCRP URL: https://www.bcrp.gob.pe/publicaciones/nota-semanal.html
  Max downloads: 60
  Decimal places: 1
  Pipeline version: s3.0.0
  Month mappings: 12 months
  Base years: 4 entries
```

---

## Step 6: Test Update Script

Test the one-button update script:

### 6a. Show Help

```bash
python scripts/update_rtd.py --help
```

**Expected**: You should see usage information with all options explained.

### 6b. Dry Run (No Changes)

```bash
python scripts/update_rtd.py --dry-run
```

**Expected output**:
```
2025-12-06 ... - update_rtd - INFO - Loading configuration from: config/config.yaml
2025-12-06 ... - update_rtd - INFO - Project: Peru GDP RTD v1.0.0
2025-12-06 ... - update_rtd - INFO - Root directory: C:\Users\Jason Cruz\...\gdp_revisions_datasets
2025-12-06 ... - update_rtd - INFO - Running all pipeline steps (1-6)
2025-12-06 ... - update_rtd - INFO - DRY RUN MODE - No changes will be made
2025-12-06 ... - update_rtd - INFO - Would run steps: [1, 2, 3, 4, 5, 6]
```

**What to check**:
- ✅ No errors
- ✅ Shows "DRY RUN MODE"
- ✅ Root directory is correct

### 6c. Test Step Selection

```bash
python scripts/update_rtd.py --dry-run --steps 3,4,5
```

**Expected**: Should show "Would run steps: [3, 4, 5]"

### 6d. Test Verbose Mode

```bash
python scripts/update_rtd.py --dry-run --verbose
```

**Expected**: Same output but with DEBUG level logging (if implemented)

---

## Step 7: Run Smoke Tests

Run the automated test suite:

```bash
python tests/test_smoke.py
```

**Expected output**:
```
Running smoke tests...

[PASS] Package import
[PASS] Config loading
[PASS] Config paths
[PASS] Sector mappings
[PASS] Month mappings
[PASS] Base years
[PASS] All modules importable

Results: 7 passed, 0 failed

[OK] All smoke tests passed!
```

**If any tests fail**:
- Read the error message carefully
- It will tell you exactly what's wrong
- Common issues: wrong directory, missing files, config errors

---

## Step 8: Check File Structure

Verify all files were created:

```bash
python -c "import os; from pathlib import Path; files = ['peru_gdp_rtd/__init__.py', 'peru_gdp_rtd/config/settings.py', 'config/config.yaml', 'scripts/update_rtd.py', 'pyproject.toml', 'requirements.txt', 'README.md']; missing = [f for f in files if not Path(f).exists()]; print('Checking files...'); [print(f'  ✓ {f}') for f in files if Path(f).exists()]; print(f'\n{len(files) - len(missing)}/{len(files)} files present'); missing and print(f'Missing: {missing}')"
```

**Expected**: All files should be present (✓ for each)

---

## Step 9: Verify Dependencies

Check if required packages are installed:

```bash
python -c "import sys; packages = ['yaml', 'pandas', 'numpy', 'requests', 'selenium']; print('Checking dependencies...'); results = []; [results.append((pkg, __import__(pkg))) for pkg in packages]; print('\n'.join([f'  ✓ {pkg}' for pkg, _ in results])); print(f'\n{len(results)}/{len(packages)} packages installed')"
```

**Expected output**:
```
Checking dependencies...
  ✓ yaml
  ✓ pandas
  ✓ numpy
  ✓ requests
  ✓ selenium

5/5 packages installed
```

**If packages are missing**:
```bash
# Install from requirements.txt
pip install -r requirements.txt
```

---

## Step 10: Test YAML Configuration

Verify YAML files are valid:

```bash
python -c "import yaml; files = ['config/config.yaml', 'config/config.example.yaml']; print('Validating YAML files...'); [print(f'  ✓ {f} - Valid YAML ({len(yaml.safe_load(open(f)))} sections)') for f in files if open(f)]; print('\nAll YAML files are valid!')"
```

**Expected**: Both files should be valid YAML

---

## Step 11: Check Directory Creation

The configuration system should auto-create directories:

```bash
python -c "from peru_gdp_rtd.config import get_settings; s = get_settings(); s.ensure_directories(); print('✓ All directories created'); dirs = [s.paths.data_root, s.paths.data_input, s.paths.data_output, s.paths.metadata, s.paths.record]; print('\nDirectories:'); [print(f'  ✓ {d.name}/ exists') for d in dirs if d.exists()]; missing = [d for d in dirs if not d.exists()]; missing and print(f'\nMissing: {[d.name for d in missing]}')"
```

**Expected**: All required directories exist

---

## Step 12: Full Integration Test

Run everything together:

```bash
python -c "
print('='*60)
print('  Peru GDP RTD - Full Integration Test')
print('='*60)
print()

# Test 1: Package import
import peru_gdp_rtd
print('[1/5] ✓ Package import')

# Test 2: Configuration
from peru_gdp_rtd.config import get_settings
settings = get_settings()
print(f'[2/5] ✓ Configuration loaded ({settings.project[\"name\"]})')

# Test 3: Paths
assert 'gdp_revisions_datasets' in str(settings.paths.data_root)
print('[3/5] ✓ Path resolution')

# Test 4: Settings validation
assert settings.scraper.browser in ['chrome', 'firefox', 'edge']
assert settings.cleaning.decimal_places == 1
assert len(settings.cleaning.month_mappings) == 12
print('[4/5] ✓ Settings validation')

# Test 5: All modules
modules = ['config', 'scrapers', 'processors', 'cleaners', 'transformers', 'orchestration', 'utils']
for mod in modules:
    __import__(f'peru_gdp_rtd.{mod}')
print('[5/5] ✓ All modules import')

print()
print('='*60)
print('  ✓ ALL TESTS PASSED!')
print('='*60)
print()
print('Your setup is working perfectly!')
print()
"
```

**Expected output**:
```
============================================================
  Peru GDP RTD - Full Integration Test
============================================================

[1/5] ✓ Package import
[2/5] ✓ Configuration loaded (Peru GDP RTD)
[3/5] ✓ Path resolution
[4/5] ✓ Settings validation
[5/5] ✓ All modules import

============================================================
  ✓ ALL TESTS PASSED!
============================================================

Your setup is working perfectly!
```

---

## Troubleshooting Common Issues

### Issue: "No module named 'peru_gdp_rtd'"

**Solution**: Make sure you're in the correct directory
```bash
cd "C:\Users\Jason Cruz\OneDrive\Documentos\RA\CIUP\GDP Revisions\GitHub\peru_gdp_revisions\gdp_revisions_datasets"
```

### Issue: "FileNotFoundError: config/config.yaml not found"

**Solution**: Check if the file exists
```bash
# Windows
dir config\config.yaml

# If missing, copy from example
copy config\config.example.yaml config\config.yaml
```

### Issue: "No module named 'yaml'"

**Solution**: Install dependencies
```bash
pip install -r requirements.txt
```

### Issue: Wrong Python version

**Solution**: Activate your conda environment
```bash
conda activate gdp_revisions
python --version  # Should show 3.12.1
```

### Issue: Import errors with unicode characters

**Solution**: This is a Windows console encoding issue, not a code issue. The tests will still work.

---

## Quick Test (All-in-One)

If you want to run everything at once:

```bash
# Save this as test_all.bat (Windows) or test_all.sh (Mac/Linux)
echo Running all tests...
python tests/test_smoke.py
python scripts/update_rtd.py --dry-run
echo.
echo All tests complete!
```

---

## What Success Looks Like

✅ **All smoke tests pass** (7/7)
✅ **Configuration loads** without errors
✅ **Paths resolve correctly** to your project directory
✅ **Update script runs** in dry-run mode
✅ **All modules import** successfully

---

## After Testing

Once all tests pass, you're ready for:
1. ✅ Week 1 is complete and verified
2. 🚀 Ready to start Week 2 (module migration)
3. 📝 You can commit these changes to git

---

## Git Commands (Optional)

If everything works, you can commit:

```bash
git add .
git commit -m "feat: Week 1 foundation - package structure, config system, one-button script"
git push
```

---

## Need Help?

If any test fails:
1. Read the error message carefully
2. Check the troubleshooting section above
3. Verify you're in the correct directory
4. Make sure conda environment is activated
5. Check that all files were created properly

---

**Testing Guide Created**: December 6, 2025
**Status**: Ready for your verification
**Next Step**: Run these tests and let me know if you see any issues!
