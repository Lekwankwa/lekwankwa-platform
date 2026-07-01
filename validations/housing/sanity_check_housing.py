"""
Sanity Check for Housing Supply & Shelter Inflation Vault

Partition-level quality checks for both housing sub-datasets:
  - source=bls_cpi_shelter  (BLS CPI Shelter)
  - source=census_bps        (Census Building Permits Survey)

VALIDATION CHECKS (per partition):
  1. Empty File Detection   — no empty/undersized Parquet files
  2. Schema Check           — required gold-standard columns present
  3. Null Critical Fields   — record_id, source, data_timestamp, published_date non-null
  4. Duplicate record_ids   — no duplicate UUIDs within a partition
  5. Metric Value Range     — observed_value > 0 and within source-specific bounds
  6. Timestamp Range        — data_timestamp within valid historical range for source
  7. PIT Temporal Order     — published_date ≥ data_timestamp
  8. Source Label Integrity — source column matches expected value for this partition

OUTPUT:
  - housing_sanity_check_report.txt
  - housing_sanity_check_failures.json

Author: Lekwankwa Corporation
Date: 2026-06-12
"""

import json
import logging
from datetime import datetime
from pathlib import Path
from typing import Any

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("housing_sanity_check.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

VAULT_DIR = Path("lekwankwa-historical-vault")
PRODUCT   = "Housing_Supply_and_Shelter_Inflation"
COUNTRY   = "USA"
SOURCES   = ["bls_cpi_shelter", "census_bps"]

REPORT_TXT    = Path("housing_sanity_check_report.txt")
FAILURES_JSON = Path("housing_sanity_check_failures.json")

MIN_ROWS_PER_PARTITION = 1

# Source-specific value bounds
VALUE_BOUNDS = {
    "bls_cpi_shelter": (1.0,       1_500.0),      # CPI index: historical range ~1-1500
    "census_bps":      (1.0,       9_999_999.0),  # permits count + valuation
}

# Timestamp year bounds
YEAR_BOUNDS = {
    "bls_cpi_shelter": (1914, 2027),
    "census_bps":      (1959, 2027),
}

# Minimum required gold-standard columns (shared)
REQUIRED_COLUMNS = [
    "record_id",
    "sovereign_series_id",
    "data_vintage_id",
    "confidence_tier",
    "macro_metric_name",
    "reporting_date",
    "observed_value",
    "unit_of_measure",
    "is_revised_figure",
    "source",
    "published_date",
    "as_of_date",
    "revision_number",
    "portal_url",
    "source_agency",
]


# =============================================================================
# PARTITION VALIDATOR
# =============================================================================

def validate_partition(f: Path, source: str, year: str, month: str) -> dict[str, Any]:
    result: dict[str, Any] = {
        "file":           str(f),
        "source":         source,
        "year":           year,
        "month":          month,
        "checks_passed":  [],
        "checks_failed":  [],
        "warnings":       [],
        "row_count":      0,
    }

    try:
        df = pd.read_parquet(f)
        result["row_count"] = len(df)

        # ── 1. Empty file ────────────────────────────────────────────────
        if len(df) < MIN_ROWS_PER_PARTITION:
            result["checks_failed"].append(f"Empty file: {len(df)} rows")
        else:
            result["checks_passed"].append(f"Row count OK: {len(df)}")

        # ── 2. Schema ────────────────────────────────────────────────────
        missing_cols = [c for c in REQUIRED_COLUMNS if c not in df.columns]
        if missing_cols:
            result["checks_failed"].append(f"Missing columns: {missing_cols}")
        else:
            result["checks_passed"].append("Gold-standard schema present")

        # ── 3. Null critical fields ──────────────────────────────────────
        critical = ["record_id", "source", "reporting_date", "published_date",
                    "as_of_date", "sovereign_series_id"]
        null_issues: dict[str, int] = {}
        for col in critical:
            if col in df.columns:
                n = int(df[col].isna().sum())
                if n:
                    null_issues[col] = n
        if null_issues:
            result["checks_failed"].append(f"Null in critical fields: {null_issues}")
        else:
            result["checks_passed"].append("No nulls in critical fields")

        # ── 4. Duplicate record_ids ──────────────────────────────────────
        if "record_id" in df.columns:
            dupes = int(df["record_id"].duplicated().sum())
            if dupes:
                result["checks_failed"].append(f"Duplicate record_ids: {dupes}")
            else:
                result["checks_passed"].append("No duplicate record_ids")

        # ── 5. Metric value range ────────────────────────────────────────
        val_col = "observed_value" if "observed_value" in df.columns else "metric_value"
        if val_col in df.columns:
            lo, hi = VALUE_BOUNDS.get(source, (0, 1e15))
            vals = pd.to_numeric(df[val_col], errors="coerce")
            out_range = int(((vals < lo) | (vals > hi)).sum())
            nulls     = int(vals.isna().sum())
            if out_range:
                result["warnings"].append(
                    f"Metric value out of range [{lo}, {hi}]: {out_range} rows"
                )
            else:
                result["checks_passed"].append(f"Metric value range OK (nulls={nulls})")

        # ── 6. Timestamp range ───────────────────────────────────────────
        ts_col = "reporting_date" if "reporting_date" in df.columns else "data_timestamp"
        if ts_col in df.columns:
            ts = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
            invalid_ts = int(ts.isna().sum())
            if invalid_ts:
                result["checks_failed"].append(f"Unparseable {ts_col}: {invalid_ts}")
            else:
                yr_min, yr_max = YEAR_BOUNDS.get(source, (1900, 2027))
                years = ts.dt.year
                out   = int(((years < yr_min) | (years > yr_max)).sum())
                if out:
                    result["warnings"].append(
                        f"{ts_col} year outside [{yr_min}, {yr_max}]: {out} rows"
                    )
                else:
                    result["checks_passed"].append(
                        f"{ts_col} range OK ({years.min()}-{years.max()})"
                    )

        # ── 7. PIT temporal order ─────────────────────────────────────────
        if "published_date" in df.columns and ts_col in df.columns:
            pub = pd.to_datetime(df["published_date"], errors="coerce", utc=True)
            dat = pd.to_datetime(df[ts_col],           errors="coerce", utc=True)
            viol = int((pub < dat).sum())
            if viol:
                result["checks_failed"].append(
                    f"PIT violation: published_date < reporting_date ({viol} rows)"
                )
            else:
                result["checks_passed"].append("PIT temporal order valid")

        # ── 8. Source label integrity ────────────────────────────────────
        if "source" in df.columns:
            wrong_src = int((df["source"] != source).sum())
            if wrong_src:
                result["checks_failed"].append(
                    f"Source label mismatch: {wrong_src} records have wrong source value"
                )
            else:
                result["checks_passed"].append(f"Source label correct: {source}")

    except Exception as exc:
        result["checks_failed"].append(f"File read error: {exc}")

    return result


# =============================================================================
# RUNNER
# =============================================================================

def run_sanity_check():
    logger.info("=" * 70)
    logger.info("HOUSING — VAULT SANITY CHECK")
    logger.info("=" * 70)
    logger.info(f"Run timestamp: {datetime.utcnow().isoformat()}Z")

    all_results: list[dict] = []
    summary = {
        "total_files":     0,
        "total_rows":      0,
        "files_passed":    0,
        "files_with_failures": 0,
        "files_with_warnings": 0,
        "by_source":       {},
    }

    for source in SOURCES:
        src_path = VAULT_DIR / f"product={PRODUCT}" / f"country={COUNTRY}" / f"source={source}"
        files = sorted(src_path.rglob("*_data.parquet"))  # Only _data.parquet files
        logger.info(f"\nSource: {source} — {len(files)} partition(s)")

        src_stats = {"files": len(files), "rows": 0, "passed": 0, "failed": 0}

        for f in files:
            parts = f.parts
            year  = next((p.split("=")[1] for p in parts if p.startswith("year=")), "?")
            month = next((p.split("=")[1] for p in parts if p.startswith("month=")), "?")

            result = validate_partition(f, source, year, month)
            all_results.append(result)
            summary["total_files"] += 1
            summary["total_rows"]  += result["row_count"]
            src_stats["rows"]      += result["row_count"]

            if result["checks_failed"]:
                summary["files_with_failures"] += 1
                src_stats["failed"] += 1
                logger.warning(f"  FAIL {source}/{year}/{month}: {result['checks_failed']}")
            elif result["warnings"]:
                summary["files_with_warnings"] += 1
                src_stats["passed"] += 1
                logger.info(f"  WARN {source}/{year}/{month}: {result['warnings']}")
            else:
                summary["files_passed"] += 1
                src_stats["passed"]     += 1

        summary["by_source"][source] = src_stats

    # ── Write outputs ─────────────────────────────────────────────────────────
    failures = [r for r in all_results if r["checks_failed"]]

    with open(FAILURES_JSON, "w") as fh:
        json.dump(failures, fh, indent=2, default=str)

    with open(REPORT_TXT, "w") as fh:
        fh.write(f"Housing Sanity Check Report — {datetime.utcnow().isoformat()}Z\n")
        fh.write("=" * 70 + "\n\n")
        for src, stats in summary["by_source"].items():
            fh.write(f"Source: {src}\n")
            fh.write(f"  Files: {stats['files']}, Rows: {stats['rows']:,}, "
                     f"Passed: {stats['passed']}, Failed: {stats['failed']}\n\n")
        fh.write(f"TOTAL FILES:    {summary['total_files']}\n")
        fh.write(f"TOTAL ROWS:     {summary['total_rows']:,}\n")
        fh.write(f"FILES CLEAN:    {summary['files_passed']}\n")
        fh.write(f"FILES WITH FAILURES: {summary['files_with_failures']}\n")
        fh.write(f"FILES WITH WARNINGS: {summary['files_with_warnings']}\n")
        if failures:
            fh.write("\nFAILURE DETAILS:\n")
            for r in failures[:20]:
                fh.write(f"  {r['source']}/{r['year']}/{r['month']}: "
                         f"{'; '.join(r['checks_failed'])}\n")

    logger.info("\n" + "=" * 70)
    logger.info(f"SUMMARY — Files: {summary['total_files']} | "
                f"Rows: {summary['total_rows']:,} | "
                f"Clean: {summary['files_passed']} | "
                f"Failed: {summary['files_with_failures']} | "
                f"Warned: {summary['files_with_warnings']}")
    logger.info("=" * 70)
    logger.info(f"Reports: {REPORT_TXT}, {FAILURES_JSON}")

    return summary["files_with_failures"] == 0


if __name__ == "__main__":
    import sys
    sys.exit(0 if run_sanity_check() else 1)
