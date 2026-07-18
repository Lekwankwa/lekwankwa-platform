"""
Sanity Check — Trade Flows Hive Vault (US Census FT-900)

FILE-LEVEL VALIDATION CHECKS:
  1. Empty File Detection    — no empty / undersized Parquet partitions
  2. Schema Check            — all required columns present
  3. Null Critical Fields    — record_id, source, commodity_code, trade_flow non-null
  4. Trade Value Range       — observed_value in [0, 1_000_000] USD_MILLIONS
  5. Timestamp Validity      — data_timestamp parseable, year in [1989, 2030]
  6. PIT Temporal Order      — published_date >= data_timestamp
  7. Trade Flow Vocabulary   — only 'Export' / 'Import'
  8. Duplicate record_ids    — no duplicates within each partition

Author: Lekwankwa Corporation
Date: 2026-06-13
"""

import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("trade_flows_sanity_check.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _vault_root import VAULT_ROOT, vault_glob_paths as vault_glob, vault_read_parquet  # noqa: E402

# =============================================================================
# CONFIGURATION
# =============================================================================

VAULT_DIR  = VAULT_ROOT
PRODUCT    = "trade_flows"
COUNTRY    = "USA"
SOURCES    = ["census_ft900"]

REPORT_TXT    = Path("trade_flows_sanity_check_report.txt")
FAILURES_JSON = Path("trade_flows_sanity_check_failures.json")

MIN_ROWS_PER_PARTITION = 1
MIN_TRADE_VALUE        = 0.0
MAX_TRADE_VALUE        = 1_000_000.0   # $1 trillion in USD_MILLIONS
VALID_TRADE_FLOWS      = {"Export", "Import"}

REQUIRED_COLUMNS = [
    "record_id", "country_code", "commodity_code", "trade_flow",
    "observed_value", "trade_value", "unit_of_measure", "currency",
    "data_timestamp", "source", "published_date", "as_of_date",
    "revision_number", "sovereign_series_id",
]


# =============================================================================
# PARTITION VALIDATOR
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

        # CHECK 1: Row count
        if len(df) < MIN_ROWS_PER_PARTITION:
            result["checks_failed"].append(f"Empty partition: {len(df)} rows")
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
        for col in ["record_id", "source", "commodity_code", "trade_flow",
                    "data_timestamp", "published_date", "as_of_date"]:
            if col in df.columns:
                n = int(df[col].isna().sum())
                if n > 0:
                    null_violations[col] = n
        if null_violations:
            result["checks_failed"].append(f"Null critical fields: {null_violations}")
        else:
            result["checks_passed"].append("No nulls in critical fields")

        # CHECK 4: Trade value range
        for vcol in ["observed_value", "trade_value"]:
            if vcol in df.columns:
                numeric = pd.to_numeric(df[vcol], errors="coerce")
                out_low  = int((numeric < MIN_TRADE_VALUE).sum())
                out_high = int((numeric > MAX_TRADE_VALUE).sum())
                if out_low > 0:
                    result["warnings"].append(f"{vcol}: {out_low} rows below {MIN_TRADE_VALUE}")
                if out_high > 0:
                    result["warnings"].append(
                        f"{vcol}: {out_high} rows above {MAX_TRADE_VALUE:,} USD_MILLIONS "
                        f"(implausible — check raw data)"
                    )
                if out_low == 0 and out_high == 0:
                    result["checks_passed"].append(f"{vcol} range OK")

        # CHECK 5: Timestamp validity
        if "data_timestamp" in df.columns:
            dates = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True)
            invalid = int(dates.isna().sum())
            if invalid > 0:
                result["checks_failed"].append(f"Invalid data_timestamp: {invalid} rows")
            else:
                years = dates.dt.year
                if years.min() < 1989 or years.max() > 2030:
                    result["warnings"].append(
                        f"Timestamp year outside 1989-2030: {years.min()}-{years.max()}"
                    )
                else:
                    result["checks_passed"].append(f"Timestamps valid: year={years.min()}-{years.max()}")

        # CHECK 6: PIT temporal order
        if "published_date" in df.columns and "data_timestamp" in df.columns:
            pub = pd.to_datetime(df["published_date"], errors="coerce", utc=True)
            dat = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True)
            v = int((pub < dat).sum())
            if v > 0:
                result["checks_failed"].append(
                    f"PIT violation: published_date < data_timestamp ({v} rows)"
                )
            else:
                result["checks_passed"].append("PIT temporal order valid")

        # CHECK 7: Trade flow vocabulary
        if "trade_flow" in df.columns:
            found   = set(df["trade_flow"].dropna().unique())
            invalid = found - VALID_TRADE_FLOWS
            if invalid:
                result["checks_failed"].append(f"Invalid trade_flow values: {invalid}")
            else:
                result["checks_passed"].append(f"Trade flow values valid: {sorted(found)}")

        # CHECK 8: Duplicate record_ids
        if "record_id" in df.columns:
            dupes = int(df["record_id"].duplicated().sum())
            if dupes > 0:
                result["checks_failed"].append(f"Duplicate record_ids: {dupes}")
            else:
                result["checks_passed"].append("No duplicate record_ids")

    except Exception as exc:
        result["checks_failed"].append(f"File read error: {exc}")

    return result


# =============================================================================
# MAIN RUNNER
# =============================================================================

def run_sanity_check():
    logger.info("=" * 70)
    logger.info("TRADE FLOWS — HIVE VAULT SANITY CHECK")
    logger.info("=" * 70)

    all_results  = []
    total_files  = 0
    total_passed = 0
    total_failed = 0
    failures     = []

    for source in SOURCES:
        source_path = f"{VAULT_DIR}/product={PRODUCT}/country={COUNTRY}/source={source}"
        parquet_files = [
            f for f in vault_glob(source_path, "*.parquet")
            if "outliers" not in f.name and "changelog" not in f.name
        ]
        logger.info(f"  Source={source}: {len(parquet_files)} partition files")

        for pf in sorted(parquet_files):
            parts   = str(pf).split("/")
            year    = next((p.split("=")[1] for p in parts if p.startswith("year=")),  "unknown")
            month   = next((p.split("=")[1] for p in parts if p.startswith("month=")), "unknown")
            result  = validate_partition(pf, source, year, month)

            total_files += 1
            if result["checks_failed"]:
                total_failed += 1
                failures.append(result)
                for msg in result["checks_failed"]:
                    logger.error(f"  [{year}-{month}] FAIL: {msg}")
            else:
                total_passed += 1
                if result["warnings"]:
                    for w in result["warnings"]:
                        logger.warning(f"  [{year}-{month}] WARN: {w}")

            all_results.append(result)

    # Persist failures
    with open(FAILURES_JSON, "w") as f:
        json.dump(failures, f, indent=2, default=str)

    # Persist text report
    with open(REPORT_TXT, "w") as f:
        f.write("TRADE FLOWS — HIVE VAULT SANITY CHECK REPORT\n")
        f.write("=" * 70 + "\n\n")
        f.write(f"Total partitions scanned : {total_files}\n")
        f.write(f"Partitions passed        : {total_passed}\n")
        f.write(f"Partitions failed        : {total_failed}\n\n")
        for r in failures:
            f.write(f"FAIL [{r['source']} y={r['year']} m={r['month']}]\n")
            for msg in r["checks_failed"]:
                f.write(f"  {msg}\n")
            f.write("\n")

    overall = "PASS" if total_failed == 0 else "FAIL"
    logger.info("")
    logger.info(f"Summary: {total_passed}/{total_files} partitions passed [{overall}]")
    if total_failed > 0:
        logger.error(f"  {total_failed} failures written to {FAILURES_JSON}")

    return total_failed == 0


if __name__ == "__main__":
    success = run_sanity_check()
    sys.exit(0 if success else 1)
