"""
Sanity Check for Macro Employment Hive Vault

Validates BLS CES and JOLTS data quality across all partitions.

VALIDATION CHECKS:
  1. Empty File Detection: No empty/undersized Parquet files
  2. Duplicate Detection: No duplicate record_ids
  3. Schema Check: Required columns present
  4. Null Value Check: Critical fields not null
  5. Metric Value Range: Numeric values within expected bounds
  6. Timestamp Validation: Dates in expected range
  7. PIT Field Integrity: published_date >= data_timestamp

Author: Lekwankwa Corporation
Date: 2026-06-07
"""

import sys
import pandas as pd
import json
from pathlib import Path
from typing import Dict, List, Any
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('employment_sanity_check.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _vault_root import VAULT_ROOT, vault_exists, vault_glob_paths as vault_glob, vault_subdirs, vault_read_parquet  # noqa: E402

# =============================================================================
# CONFIGURATION
# =============================================================================

VAULT_DIR = VAULT_ROOT
PRODUCT = "wages_and_employment"
COUNTRY = "USA"
SOURCES = ["bls_ces", "bls_cps"]

REPORT_PATH = Path("employment_sanity_check_report.txt")
FAILURES_JSON = Path("employment_sanity_check_failures.json")

MIN_ROWS_PER_PARTITION = 1
MIN_METRIC_VALUE = 0.0
MAX_METRIC_VALUE = 1_000_000_000.0

REQUIRED_COLUMNS = [
    "record_id", "country_code", "industry_code", "metric_value",
    "data_timestamp", "source", "published_date", "as_of_date",
    "revision_number"
]

# =============================================================================
# VALIDATION FUNCTIONS
# =============================================================================

def validate_partition(parquet_file: Path, source: str, year: str, month: str) -> Dict[str, Any]:
    result = {
        "file": str(parquet_file),
        "source": source,
        "year": year,
        "month": month,
        "checks_passed": [],
        "checks_failed": [],
        "warnings": [],
        "row_count": 0,
    }

    try:
        df = vault_read_parquet(parquet_file)
        result["row_count"] = len(df)

        # CHECK 1: Empty payload
        if len(df) < MIN_ROWS_PER_PARTITION:
            result["checks_failed"].append(f"Empty file: {len(df)} rows")
        else:
            result["checks_passed"].append(f"Row count OK: {len(df)}")

        # CHECK 2: Schema
        missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        if missing:
            result["checks_failed"].append(f"Missing columns: {missing}")
        else:
            result["checks_passed"].append("Schema valid")

        # CHECK 3: Null critical fields
        null_violations = {}
        for col in ["record_id", "source", "data_timestamp", "published_date", "as_of_date"]:
            if col in df.columns:
                n = df[col].isna().sum()
                if n > 0:
                    null_violations[col] = int(n)
        if null_violations:
            result["checks_failed"].append(f"Null values: {null_violations}")
        else:
            result["checks_passed"].append("No nulls in critical fields")

        # CHECK 4: Metric value range
        if "metric_value" in df.columns:
            numeric = pd.to_numeric(df["metric_value"], errors="coerce")
            out_of_range = ((numeric < MIN_METRIC_VALUE) | (numeric > MAX_METRIC_VALUE)).sum()
            if out_of_range > 0:
                result["warnings"].append(f"Metric value out of range: {out_of_range} rows")
            else:
                result["checks_passed"].append("Metric value range OK")

        # CHECK 5: Timestamp range
        if "data_timestamp" in df.columns:
            dates = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True)
            invalid = dates.isna().sum()
            if invalid > 0:
                result["checks_failed"].append(f"Invalid timestamps: {invalid}")
            else:
                years = dates.dt.year
                if years.min() < 1939 or years.max() > 2027:
                    result["warnings"].append(f"Timestamp year outside range: {years.min()}-{years.max()}")
                else:
                    result["checks_passed"].append(f"Timestamps valid: {years.min()}-{years.max()}")

        # CHECK 6: PIT integrity - published_date >= data_timestamp
        if "published_date" in df.columns and "data_timestamp" in df.columns:
            pub = pd.to_datetime(df["published_date"], errors="coerce", utc=True)
            dat = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True)
            violations = (pub < dat).sum()
            if violations > 0:
                result["checks_failed"].append(f"PIT violation: published_date < data_timestamp ({violations} rows)")
            else:
                result["checks_passed"].append("PIT temporal order valid")

        # CHECK 7: Duplicate record_ids
        if "record_id" in df.columns:
            dupes = df["record_id"].duplicated().sum()
            if dupes > 0:
                result["checks_failed"].append(f"Duplicate record_ids: {dupes}")
            else:
                result["checks_passed"].append("No duplicate record_ids")

    except Exception as e:
        result["checks_failed"].append(f"File read error: {e}")

    return result


def run_sanity_check():
    logger.info("=" * 70)
    logger.info("MACRO EMPLOYMENT - HIVE VAULT SANITY CHECK")
    logger.info("=" * 70)

    all_results = []
    total_passed = 0
    total_failed = 0
    total_warnings = 0
    total_records = 0

    for source in SOURCES:
        source_path = f"{VAULT_DIR}/product={PRODUCT}/country={COUNTRY}/source={source}"

        if not vault_exists(source_path):
            logger.error(f"Source path not found: {source_path}")
            continue

        year_folders = vault_subdirs(source_path, "year=")
        logger.info(f"\nSource: {source} | Years: {len(year_folders)}")

        for year_folder in year_folders:
            year = year_folder.name.split("=")[1]
            month_folders = vault_subdirs(str(year_folder), "month=")

            for month_folder in month_folders:
                month = month_folder.name.split("=")[1]
                parquet_files = list(vault_glob(str(month_folder), "*.parquet"))
                parquet_files = [f for f in parquet_files if "outliers" not in f.name and "changelog" not in f.name]

                for pf in parquet_files:
                    result = validate_partition(pf, source, year, month)
                    all_results.append(result)
                    total_passed += len(result["checks_passed"])
                    total_failed += len(result["checks_failed"])
                    total_warnings += len(result["warnings"])
                    total_records += result["row_count"]

                    if result["checks_failed"]:
                        logger.error(f"  [FAIL] {source}/year={year}/month={month}: {result['checks_failed']}")
                    elif result["warnings"]:
                        logger.warning(f"  [WARN] {source}/year={year}/month={month}: {result['warnings']}")

    logger.info("\n" + "=" * 70)
    logger.info("SANITY CHECK SUMMARY")
    logger.info("=" * 70)
    logger.info(f"Total partitions checked: {len(all_results)}")
    logger.info(f"Total records: {total_records:,}")
    logger.info(f"Checks passed: {total_passed}")
    logger.info(f"Checks failed: {total_failed}")
    logger.info(f"Warnings: {total_warnings}")

    overall = "[PASS]" if total_failed == 0 else "[FAIL]"
    logger.info(f"Overall status: {overall}")

    # Save report
    with open(REPORT_PATH, "w") as f:
        f.write("MACRO EMPLOYMENT SANITY CHECK REPORT\n")
        f.write("=" * 70 + "\n\n")
        for r in all_results:
            if r["checks_failed"] or r["warnings"]:
                f.write(f"Source: {r['source']} | Year: {r['year']} | Month: {r['month']}\n")
                for fail in r["checks_failed"]:
                    f.write(f"  [FAIL] {fail}\n")
                for warn in r["warnings"]:
                    f.write(f"  [WARN] {warn}\n")
                f.write("\n")
        f.write(f"\nSUMMARY: {total_passed} passed, {total_failed} failed, {total_warnings} warnings\n")
        f.write(f"Overall: {overall}\n")

    with open(FAILURES_JSON, "w") as f:
        failed = [r for r in all_results if r["checks_failed"]]
        json.dump(failed, f, indent=2, default=str)

    logger.info(f"Report saved: {REPORT_PATH}")
    return total_failed == 0


if __name__ == "__main__":
    success = run_sanity_check()
    exit(0 if success else 1)
