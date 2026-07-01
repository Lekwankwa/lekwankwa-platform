"""
Sanity Check for Hive-Partitioned Vault

Validates data quality in the Hive-partitioned lekwankwa-historical-vault structure.
Runs the same critical checks as sanity_check.py but adapted for Parquet files.

CRITICAL VALIDATION CHECKS:
  1. Schema Check: Verify Parquet schema matches expected format
  2. Null Check: Flag any rows with missing price values
  3. Outlier Detection: Flag price changes > 50% month-over-month
  4. Empty Payload Trap: Detect empty/undersized Parquet files
  5. Duplicate Key Detection: Check primary key violations
  6. Currency Shift Detection: Flag 99%+ price drops

USAGE:
    python sanity_check_hive_vault.py
    
OUTPUT:
    - Console report with summary statistics
    - hive_sanity_check_report.txt with detailed failures
    - hive_sanity_check_failures.json with structured failure data

Author: Lekwankwa Corporation
Date: May 31, 2026
"""

import pandas as pd
import json
from pathlib import Path
from collections import defaultdict
from typing import Dict, List, Tuple, Any
import logging

# Setup logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('hive_sanity_check.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURATION
# ══════════════════════════════════════════════════════════════════════════════

VAULT_DIR = Path("lekwankwa-historical-vault")
PRODUCT = "food_micropricing"
COUNTRY = "USA"
SOURCES = ["bls", "usda_ers"]
SOURCE = "bls"   # kept for backwards compat — main loop uses SOURCES

SOURCE_FILES = {
    "bls":      "food_pricing_data.parquet",
    "usda_ers": "food_pricing_data.parquet",
}

# Per-source price and name column names
PRICE_COL = {
    "bls":      "observed_price_local",
    "usda_ers": "observed_price_local",
}
NAME_COL = {
    "bls":      "standard_name",
    "usda_ers": "standard_name",
}

REPORT_PATH = Path("hive_sanity_check_report.txt")
FAILURES_JSON = Path("hive_sanity_check_failures.json")

# Validation thresholds
MOM_THRESHOLD_PCT = 50.0
MIN_REASONABLE_PRICE = 0.01
MAX_REASONABLE_PRICE = 10000.0
MIN_ROWS_PER_YEAR = 5
CURRENCY_SHIFT_THRESHOLD_PCT = 99.0

# Common required columns present in both BLS and ERS schemas
REQUIRED_COLUMNS = [
    "record_id", "country_code", "category", "currency",
    "pct_change_mom", "data_timestamp", "published_date", "as_of_date",
    "conversion_timestamp", "revision_number", "data_quality_certified",
    "source", "extraction_method",
]

PRIMARY_KEY_FIELDS = ["country_code", "source_series_id", "data_timestamp"]

# Known outliers (from sanity_check.py)
KNOWN_OUTLIERS = [
    ("Onions", 1984, 1, "Agricultural supply shock"),
    ("Onions", 1990, 1, "Agricultural supply shock"),
    ("Potatoes", 1985, 1, "Agricultural supply shock"),
    ("Potatoes", 1987, 12, "Agricultural supply shock"),
    ("Potatoes", 1991, 11, "Agricultural supply shock"),
    ("Potatoes", 1995, 4, "Agricultural supply shock"),
    ("Potatoes", 1998, 1, "Agricultural supply shock"),
    ("Tomatoes", 1990, 1, "Agricultural supply shock"),
    ("Tomatoes", 1990, 4, "Agricultural supply shock recovery"),
]


# ══════════════════════════════════════════════════════════════════════════════
#  VALIDATION FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def _validate_file(parquet_file: Path, year: str,
                   price_col: str = "observed_price_local",
                   name_col: str = "standard_name") -> Dict[str, Any]:
    """Validate a single Parquet partition file."""
    results = {
        "file": str(parquet_file.relative_to(VAULT_DIR)),
        "year": year,
        "checks_passed": [],
        "checks_failed": [],
        "warnings": [],
        "row_count": 0,
        "file_size_kb": 0
    }
    
    try:
        # Load Parquet file
        df = pd.read_parquet(parquet_file)
        results["row_count"] = len(df)
        results["file_size_kb"] = parquet_file.stat().st_size / 1024
        
        # CHECK 1: Empty Payload Trap
        if len(df) < MIN_ROWS_PER_YEAR:
            results["checks_failed"].append(f"Empty payload: only {len(df)} rows (min: {MIN_ROWS_PER_YEAR})")
        else:
            results["checks_passed"].append(f"Row count: {len(df)} rows")
        
        # CHECK 2: Schema Validation
        missing_cols = [col for col in REQUIRED_COLUMNS if col not in df.columns]
        if missing_cols:
            results["checks_failed"].append(f"Missing columns: {', '.join(missing_cols)}")
        else:
            results["checks_passed"].append(f"Schema valid: {len(df.columns)} columns")
        
        # CHECK 3: Null Price Check
        if price_col in df.columns:
            null_count = df[price_col].isnull().sum()
            if null_count > 0:
                results["checks_failed"].append(f"Null prices: {null_count} rows")
            else:
                results["checks_passed"].append("No null prices")
        
        # CHECK 4: Price Range Check
        if price_col in df.columns:
            invalid_prices = df[
                (df[price_col] < MIN_REASONABLE_PRICE) | 
                (df[price_col] > MAX_REASONABLE_PRICE)
            ]
            if len(invalid_prices) > 0:
                results["warnings"].append(f"Suspicious prices: {len(invalid_prices)} rows outside range")
        
        # CHECK 5: Duplicate Primary Key Check
        if all(col in df.columns for col in PRIMARY_KEY_FIELDS):
            duplicates = df.duplicated(subset=PRIMARY_KEY_FIELDS).sum()
            if duplicates > 0:
                results["checks_failed"].append(f"Duplicate keys: {duplicates} rows")
            else:
                results["checks_passed"].append("No duplicate keys")
        
        # CHECK 6: Outlier Detection (simplified - just flag extreme values)
        if price_col in df.columns and len(df) > 1:
            # Calculate month-over-month changes per item
            df_sorted = df.sort_values([name_col, "data_timestamp"])
            df_sorted["price_change_pct"] = df_sorted.groupby(name_col)[price_col].pct_change() * 100
            
            extreme_changes = df_sorted[
                (df_sorted["price_change_pct"].abs() > MOM_THRESHOLD_PCT) & 
                (df_sorted["price_change_pct"].notna())
            ]
            
            if len(extreme_changes) > 0:
                results["warnings"].append(f"Outliers detected: {len(extreme_changes)} price spikes > {MOM_THRESHOLD_PCT}%")
                
                # Filter out known outliers
                year_int = int(year)
                unknown_outliers = []
                for _, row in extreme_changes.iterrows():
                    item = row.get(name_col, "")
                    month = pd.to_datetime(row.get("data_timestamp", "")).month if "data_timestamp" in row else 0
                    
                    if not any(
                        item == known[0] and year_int == known[1] and month == known[2]
                        for known in KNOWN_OUTLIERS
                    ):
                        unknown_outliers.append(item)
                
                if unknown_outliers:
                    results["checks_failed"].append(f"Unknown outliers: {len(unknown_outliers)} items")
        
        # CHECK 7: Currency Shift Detection
        if price_col in df.columns and len(df) > 1:
            df_sorted = df.sort_values([name_col, "data_timestamp"])
            df_sorted["price_drop_pct"] = df_sorted.groupby(name_col)[price_col].pct_change() * -100
            
            severe_drops = df_sorted[df_sorted["price_drop_pct"] > CURRENCY_SHIFT_THRESHOLD_PCT]
            
            if len(severe_drops) > 0:
                results["checks_failed"].append(f"Currency shift detected: {len(severe_drops)} drops > {CURRENCY_SHIFT_THRESHOLD_PCT}%")
        
    except Exception as e:
        results["checks_failed"].append(f"File read error: {str(e)}")
    
    return results


def validate_vault():
    """Validate all Parquet files across both BLS and ERS sources."""
    all_results = []
    total_checks_passed = 0
    total_checks_failed = 0
    total_warnings = 0

    for src in SOURCES:
        fname    = SOURCE_FILES[src]
        src_path = VAULT_DIR / f"product={PRODUCT}" / f"country={COUNTRY}" / f"source={src}"
        price_col = PRICE_COL[src]
        name_col  = NAME_COL[src]

        if not src_path.exists():
            logger.warning(f"Vault path not found for source={src} — skipping")
            continue

        year_folders = sorted([d for d in src_path.iterdir() if d.is_dir() and d.name.startswith("year=")])
        if not year_folders:
            logger.warning(f"No year folders found for source={src}")
            continue

        logger.info("=" * 70)
        logger.info(f"HIVE VAULT SANITY CHECK — source={src}")
        logger.info("=" * 70)
        logger.info(f"Vault: {src_path.relative_to(VAULT_DIR)}")
        logger.info(f"Years to validate: {len(year_folders)}")
        logger.info("=" * 70)

        src_results_list = []

        for year_folder in year_folders:
            year = year_folder.name.split("=")[1]
            month_folders = sorted([d for d in year_folder.iterdir()
                                    if d.is_dir() and d.name.startswith("month=")])
            if not month_folders:
                logger.warning(f"No month folders in {year_folder.name}")
                continue

            last_results = None
            for month_folder in month_folders:
                month = month_folder.name.split("=")[1]
                parquet_file = month_folder / fname

                if not parquet_file.exists():
                    logger.warning(f"Missing {fname} in {year_folder.name}/{month_folder.name}")
                    continue

                logger.info(f"\nValidating {year_folder.name}/{month_folder.name} [{src}]...")
                # Patch column names into validate_parquet_file via module-level globals
                results = _validate_file(parquet_file, year, price_col, name_col)
                all_results.append(results)
                src_results_list.append(results)
                last_results = results

            if last_results:
                logger.info(f"  Rows: {last_results['row_count']}, "
                            f"Size: {last_results['file_size_kb']:.1f} KB")
                if last_results["checks_failed"]:
                    logger.error(f"  [FAIL] FAILED: {len(last_results['checks_failed'])} checks")
                    for failure in last_results["checks_failed"]:
                        logger.error(f"     - {failure}")
                if last_results["warnings"]:
                    logger.warning(f"  [WARN] WARNINGS: {len(last_results['warnings'])}")
                if last_results["checks_passed"] and not last_results["checks_failed"]:
                    logger.info(f"  [PASS] PASSED: {len(last_results['checks_passed'])} checks")

        src_passed  = sum(len(r["checks_passed"])  for r in src_results_list)
        src_failed  = sum(len(r["checks_failed"])  for r in src_results_list)
        src_warned  = sum(len(r["warnings"])       for r in src_results_list)
        logger.info(f"\n  source={src} total: {src_passed} passed, "
                    f"{src_failed} failed, {src_warned} warnings")
        total_checks_passed += src_passed
        total_checks_failed += src_failed
        total_warnings      += src_warned
    
    # Summary
    logger.info("\n" + "=" * 70)
    logger.info("VALIDATION SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Years validated: {len(all_results)}")
    logger.info(f"Total checks passed: {total_checks_passed}")
    logger.info(f"Total checks failed: {total_checks_failed}")
    logger.info(f"Total warnings: {total_warnings}")
    
    # Save detailed report
    with open(REPORT_PATH, 'w') as f:
        f.write("HIVE VAULT SANITY CHECK REPORT\n")
        f.write("=" * 70 + "\n\n")
        
        for result in all_results:
            f.write(f"File: {result['file']}\n")
            f.write(f"Year: {result['year']}\n")
            f.write(f"Rows: {result['row_count']}\n")
            f.write(f"Size: {result['file_size_kb']:.1f} KB\n\n")
            
            if result["checks_failed"]:
                f.write("FAILURES:\n")
                for failure in result["checks_failed"]:
                    f.write(f"  - {failure}\n")
                f.write("\n")
            
            if result["warnings"]:
                f.write("WARNINGS:\n")
                for warning in result["warnings"]:
                    f.write(f"  - {warning}\n")
                f.write("\n")
            
            f.write("-" * 70 + "\n\n")
    
    # Save JSON report
    with open(FAILURES_JSON, 'w') as f:
        json.dump(all_results, f, indent=2)
    
    logger.info(f"\nDetailed report: {REPORT_PATH}")
    logger.info(f"JSON report: {FAILURES_JSON}")
    logger.info("=" * 70)
    
    return total_checks_failed == 0


if __name__ == "__main__":
    import sys
    
    success = validate_vault()
    sys.exit(0 if success else 1)
