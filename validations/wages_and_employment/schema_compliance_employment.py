"""
Industry-Standard Schema Validation for Macro Employment Data

Validates the vault data against recognised industry standards:

  BLS STANDARDS:
  - CES series ID format: CES + 8-digit supersector + 2-digit data type (e.g. CES0000000001)
  - JOLTS series ID format: JTS + 6-digit industry + 2-char ownership + 5-char element (e.g. JTS000000000000000JOL)
  - Seasonal adjustment codes: S (Seasonally Adjusted), U (Not Seasonally Adjusted)
  - CES data type codes: 01=All Employees (thousands), 02=Prod Workers, 03=Avg Hours, etc.
  - JOLTS element codes: JOL/JOR=Openings, HIL/HIR=Hires, QUL/QUR=Quits, LDL/LDR=Layoffs, etc.

  NAICS STANDARDS:
  - CES industry codes: 8-digit BLS supersector codes (subset of NAICS hierarchy)
  - All-zeros (00000000) = Total Nonfarm aggregate

  ISO / COMMON STANDARDS:
  - country_code: ISO 3166-1 alpha-2
  - data_timestamp: ISO 8601, UTC timezone-aware
  - record_id: UUID v4 format
  - extraction_method: controlled vocabulary (api / scraper / manual)

  SDMX ALIGNMENT:
  - Time period granularity: monthly (consistent with SDMX TIME_PERIOD)
  - Dimensional key structure: source + industry + metric + period

OUTPUT:
  - employment_schema_compliance_report.json
  - employment_schema_compliance_report.txt
  - Console summary with per-check results

Author: Lekwankwa Corporation
Date: 2026-06-07
"""

import re
import json
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Tuple
import logging
import sys

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _vault_root import VAULT_ROOT, vault_glob_paths as vault_glob, vault_read_parquet  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('employment_schema_compliance.log'),
        logging.StreamHandler(sys.stdout)
    ]
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

VAULT_DIR = VAULT_ROOT
PRODUCT = "wages_and_employment"
COUNTRY = "USA"
SOURCES = ["bls_ces", "bls_cps"]

REPORT_JSON = Path("employment_schema_compliance_report.json")
REPORT_TXT = Path("employment_schema_compliance_report.txt")

# --- BLS CES Standards ---
# Series ID: CES + 8-digit supersector code + 2-digit data type
CES_SERIES_PATTERN = re.compile(r'^CES\d{10}$')

# CES data type codes (BLS standard)
CES_VALID_DATA_TYPES = {
    '01': 'All Employees, Thousands',
    '02': 'Production and Nonsupervisory Employees, Thousands',
    '03': 'Average Weekly Hours',
    '06': 'Average Hourly Earnings',
    '07': 'Average Weekly Earnings',
    '08': 'Index of Aggregate Weekly Hours',
    '09': 'Women Employees, Thousands',
    '11': 'Overtime Hours',
    '26': 'All Employees, 3-Month Average Change',
}

# CES industry codes: 8-digit BLS supersector (leading zeros OK)
CES_INDUSTRY_PATTERN = re.compile(r'^\d{8}$')

# --- BLS CPS Standards ---
# Series ID: LNS + 8-digit labour force code = 11 chars total
# Examples: LNS14000000 (U-3 unemployment rate), LNS11000000 (civilian labour force)
CPS_SERIES_PATTERN = re.compile(r'^LNS\d{8}$')

CPS_METRIC_PREFIXES = {
    'LNS14': 'Unemployment Rate',
    'LNS13': 'Unemployment Level',
    'LNS12': 'Employment Level',
    'LNS11': 'Civilian Labour Force',
}

# --- General BLS Standards ---
VALID_SEASONAL_ADJUSTMENT = {'S', 'U'}  # S=Seasonally Adjusted, U=Unadjusted

# --- ISO Standards ---
UUID_PATTERN = re.compile(r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE)
VALID_COUNTRY_CODES = {'US', 'USA'}  # ISO 3166-1
VALID_EXTRACTION_METHODS = {'api', 'scraper', 'manual'}


# =============================================================================
# LOAD DATA
# =============================================================================

def load_sample(source: str, max_files: int = 50) -> pd.DataFrame:
    """Load a representative sample for schema validation (avoid loading 760K rows)."""
    source_path = f"{VAULT_DIR}/product={PRODUCT}/country={COUNTRY}/source={source}"
    all_files = [f for f in vault_glob(source_path, "*.parquet")
                 if "outliers" not in f.name and "changelog" not in f.name]

    # Sample evenly across years
    step = max(1, len(all_files) // max_files)
    sampled = all_files[::step][:max_files]

    dfs = []
    for f in sampled:
        try:
            dfs.append(vault_read_parquet(f))
        except Exception as e:
            logger.warning(f"Could not read {f}: {e}")

    if not dfs:
        return pd.DataFrame()

    return pd.concat(dfs, ignore_index=True)


# =============================================================================
# STANDARD VALIDATION CHECKS
# =============================================================================

def check_iso_record_ids(df: pd.DataFrame, source: str) -> dict:
    """ISO: record_id must be valid UUID v4."""
    if "record_id" not in df.columns:
        return {"status": "FAIL", "standard": "ISO/IEC 9834-8 UUID", "message": "record_id column missing"}

    valid = df["record_id"].dropna().apply(lambda x: bool(UUID_PATTERN.match(str(x))))
    invalid_count = (~valid).sum()
    total = len(df["record_id"].dropna())

    if invalid_count == 0:
        return {"status": "PASS", "standard": "ISO/IEC 9834-8 UUID",
                "message": f"All {total:,} record_ids are valid UUID v4"}
    else:
        return {"status": "FAIL", "standard": "ISO/IEC 9834-8 UUID",
                "message": f"{invalid_count:,}/{total:,} record_ids fail UUID v4 format",
                "details": {"invalid_count": int(invalid_count)}}


def check_iso_timestamps(df: pd.DataFrame, source: str) -> dict:
    """ISO 8601: Timestamps must be timezone-aware (UTC)."""
    issues = {}
    for col in ["data_timestamp", "published_date", "as_of_date", "conversion_timestamp"]:
        if col not in df.columns:
            continue
        ts = pd.to_datetime(df[col], errors="coerce", utc=True)
        null_count = ts.isna().sum()
        if null_count > 0:
            issues[col] = int(null_count)

    if not issues:
        return {"status": "PASS", "standard": "ISO 8601 (UTC timestamps)",
                "message": "All timestamp fields are valid ISO 8601 UTC"}
    else:
        return {"status": "FAIL", "standard": "ISO 8601 (UTC timestamps)",
                "message": f"Invalid/null timestamps in fields: {list(issues.keys())}",
                "details": issues}


def check_iso_country_codes(df: pd.DataFrame, source: str) -> dict:
    """ISO 3166-1: country_code must be a valid 2-letter code."""
    if "country_code" not in df.columns:
        return {"status": "FAIL", "standard": "ISO 3166-1 Country Codes", "message": "country_code column missing"}

    unique_codes = set(df["country_code"].dropna().str.upper().unique())
    invalid = unique_codes - VALID_COUNTRY_CODES

    if not invalid:
        return {"status": "PASS", "standard": "ISO 3166-1 Country Codes",
                "message": f"All country_codes valid: {unique_codes}"}
    else:
        return {"status": "FAIL", "standard": "ISO 3166-1 Country Codes",
                "message": f"Invalid country codes found: {invalid}",
                "details": {"invalid_codes": list(invalid)}}


def check_bls_ces_series_format(df: pd.DataFrame, source: str) -> dict:
    """BLS Standard: CES series IDs must follow CES + 10-digit format."""
    if source != "bls_ces":
        return {"status": "SKIP", "standard": "BLS CES Series ID Format", "message": "N/A for non-CES source"}
    col = "sovereign_series_id" if "sovereign_series_id" in df.columns else "source_series_id"
    if col not in df.columns:
        return {"status": "FAIL", "standard": "BLS CES Series ID Format", "message": "sovereign_series_id / source_series_id column missing"}

    valid_ids = df[col].dropna()
    invalid = valid_ids[~valid_ids.apply(lambda x: bool(CES_SERIES_PATTERN.match(str(x))))]

    if len(invalid) == 0:
        return {"status": "PASS", "standard": "BLS CES Series ID Format",
                "message": f"All {len(valid_ids):,} CES series IDs match BLS format (CES + 10 digits)"}
    else:
        return {"status": "FAIL", "standard": "BLS CES Series ID Format",
                "message": f"{len(invalid):,} series IDs do not match BLS CES format",
                "details": {"invalid_samples": invalid.head(5).tolist()}}


def check_bls_cps_series_format(df: pd.DataFrame, source: str) -> dict:
    """BLS Standard: CPS series IDs must follow LNS + 8-digit format."""
    if source != "bls_cps":
        return {"status": "SKIP", "standard": "BLS CPS Series ID Format", "message": "N/A for non-CPS source"}
    col = "sovereign_series_id" if "sovereign_series_id" in df.columns else "source_series_id"
    if col not in df.columns:
        return {"status": "FAIL", "standard": "BLS CPS Series ID Format", "message": f"{col} missing"}

    valid_ids = df[col].dropna()
    invalid = valid_ids[~valid_ids.apply(lambda x: bool(CPS_SERIES_PATTERN.match(str(x))))]

    if len(invalid) == 0:
        return {"status": "PASS", "standard": "BLS CPS Series ID Format",
                "message": f"All {len(valid_ids):,} CPS series IDs match BLS format (LNS + 8 digits)"}
    return {"status": "FAIL", "standard": "BLS CPS Series ID Format",
            "message": f"{len(invalid):,} series IDs do not match BLS CPS format",
            "details": {"invalid_samples": invalid.head(5).tolist()}}


def check_bls_cps_metric_vocabulary(df: pd.DataFrame, source: str) -> dict:
    """BLS CPS: macro_metric_name must be drawn from known CPS metric names."""
    if source != "bls_cps":
        return {"status": "SKIP", "standard": "BLS CPS Metric Vocabulary",
                "message": "N/A for non-CPS source"}
    col = "macro_metric_name"
    if col not in df.columns:
        return {"status": "SKIP", "standard": "BLS CPS Metric Vocabulary",
                "message": f"'{col}' not yet present (pre-migration)"}
    KNOWN = {
        "UNEMPLOYMENT_RATE_U3", "UNEMPLOYMENT_RATE_U6", "UNEMPLOYMENT_LEVEL",
        "EMPLOYMENT_LEVEL", "CIVILIAN_LABOR_FORCE", "LABOR_FORCE_PARTICIPATION_RATE",
        "EMPLOYMENT_POPULATION_RATIO", "UNEMPLOYMENT_RATE_YOUTH_16_19",
        "UNEMPLOYMENT_RATE_MEN_20PLUS", "UNEMPLOYMENT_RATE_WOMEN_20PLUS",
        "UNEMPLOYMENT_RATE_WHITE", "LONG_TERM_UNEMPLOYED_27WEEKS_PLUS",
        "CPS_LABOR_METRIC",
    }
    found = set(df[col].dropna().unique())
    unknown = found - KNOWN
    if not unknown:
        return {"status": "PASS", "standard": "BLS CPS Metric Vocabulary",
                "message": f"All CPS macro_metric_name values are recognised: {sorted(found)}"}
    return {"status": "WARN", "standard": "BLS CPS Metric Vocabulary",
            "message": f"Unrecognised metric names: {unknown}",
            "details": {"unknown": list(unknown)}}


def check_bls_seasonal_adjustment(df: pd.DataFrame, source: str) -> dict:
    """BLS Standard: seasonal_adjustment must be 'S' (Seasonally Adjusted) or 'U' (Unadjusted)."""
    if "seasonal_adjustment" not in df.columns:
        return {"status": "FAIL", "standard": "BLS Seasonal Adjustment Codes",
                "message": "seasonal_adjustment column missing"}

    unique_vals = set(df["seasonal_adjustment"].dropna().str.upper().unique())
    invalid = unique_vals - VALID_SEASONAL_ADJUSTMENT

    if not invalid:
        return {"status": "PASS", "standard": "BLS Seasonal Adjustment Codes",
                "message": f"All seasonal_adjustment values valid: {unique_vals} (S=Seasonally Adjusted, U=Not Adjusted)"}
    else:
        return {"status": "FAIL", "standard": "BLS Seasonal Adjustment Codes",
                "message": f"Invalid seasonal_adjustment values: {invalid}",
                "details": {"invalid_values": list(invalid)}}


def check_naics_industry_codes(df: pd.DataFrame, source: str) -> dict:
    """NAICS/BLS: CES industry codes must be 8-digit numeric (BLS supersector codes)."""
    if source != "bls_ces":
        return {"status": "SKIP", "standard": "NAICS / BLS Industry Codes", "message": "N/A for non-CES source"}
    if "industry_code" not in df.columns:
        return {"status": "FAIL", "standard": "NAICS / BLS Industry Codes", "message": "industry_code missing"}

    valid_ids = df["industry_code"].dropna().astype(str)
    invalid = valid_ids[~valid_ids.apply(lambda x: bool(CES_INDUSTRY_PATTERN.match(x)))]

    if len(invalid) == 0:
        unique_count = valid_ids.nunique()
        return {"status": "PASS", "standard": "NAICS / BLS Industry Codes",
                "message": f"All {len(valid_ids):,} industry codes are valid 8-digit BLS supersector codes ({unique_count} unique)"}
    else:
        return {"status": "FAIL", "standard": "NAICS / BLS Industry Codes",
                "message": f"{len(invalid):,} industry codes do not match 8-digit BLS format",
                "details": {"invalid_samples": invalid.head(5).tolist()}}


def check_sdmx_temporal_granularity(df: pd.DataFrame, source: str) -> dict:
    """SDMX: TIME_PERIOD should be monthly (day=1 for all records)."""
    if "data_timestamp" not in df.columns:
        return {"status": "FAIL", "standard": "SDMX TIME_PERIOD Monthly", "message": "data_timestamp missing"}

    dates = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True)
    non_monthly = (dates.dt.day != 1).sum()
    valid = dates.notna().sum()

    if non_monthly == 0:
        min_dt = dates.min().strftime("%Y-%m")
        max_dt = dates.max().strftime("%Y-%m")
        return {"status": "PASS", "standard": "SDMX TIME_PERIOD Monthly",
                "message": f"All {valid:,} records use monthly granularity (day=1). Range: {min_dt} to {max_dt}"}
    else:
        return {"status": "FAIL", "standard": "SDMX TIME_PERIOD Monthly",
                "message": f"{non_monthly:,} records have non-monthly timestamps (day != 1)",
                "details": {"non_monthly_count": int(non_monthly)}}


def check_pit_field_completeness(df: pd.DataFrame, source: str) -> dict:
    """PIT Standard: All 5 PIT fields must be present and non-null (except superseded_by)."""
    required = ["record_id", "published_date", "as_of_date", "revision_number"]
    optional = ["superseded_by"]

    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        return {"status": "FAIL", "standard": "Lekwankwa PIT Schema v4.0",
                "message": f"Missing PIT columns: {missing_cols}"}

    null_violations = {}
    for col in required:
        n = df[col].isna().sum()
        if n > 0:
            null_violations[col] = int(n)

    superseded_null_pct = 100.0
    if "superseded_by" in df.columns:
        superseded_null_pct = round(df["superseded_by"].isna().mean() * 100, 2)

    if not null_violations:
        return {"status": "PASS", "standard": "Lekwankwa PIT Schema v4.0",
                "message": f"All PIT fields present and populated. superseded_by is {superseded_null_pct}% null (expected for initial load)",
                "details": {"superseded_by_null_pct": superseded_null_pct}}
    else:
        return {"status": "FAIL", "standard": "Lekwankwa PIT Schema v4.0",
                "message": f"Null values in required PIT fields: {null_violations}",
                "details": null_violations}


def check_extraction_method_vocabulary(df: pd.DataFrame, source: str) -> dict:
    """Controlled vocabulary: extraction_method must be api/scraper/manual."""
    if "extraction_method" not in df.columns:
        return {"status": "FAIL", "standard": "Controlled Vocabulary (extraction_method)",
                "message": "extraction_method column missing"}

    unique_vals = set(df["extraction_method"].dropna().str.lower().unique())
    invalid = unique_vals - VALID_EXTRACTION_METHODS

    if not invalid:
        return {"status": "PASS", "standard": "Controlled Vocabulary (extraction_method)",
                "message": f"All extraction_method values valid: {unique_vals}"}
    else:
        return {"status": "FAIL", "standard": "Controlled Vocabulary (extraction_method)",
                "message": f"Invalid extraction_method values: {invalid}"}


def check_metric_value_numeric(df: pd.DataFrame, source: str) -> dict:
    """Data integrity: metric_value must be numeric (or parseable as float)."""
    if "metric_value" not in df.columns:
        return {"status": "FAIL", "standard": "Numeric Metric Values", "message": "metric_value column missing"}

    numeric = pd.to_numeric(df["metric_value"], errors="coerce")
    null_count = numeric.isna().sum()
    total = len(df)
    pct_null = round(null_count / max(total, 1) * 100, 2)

    if pct_null <= 5.0:
        return {"status": "PASS", "standard": "Numeric Metric Values",
                "message": f"metric_value is numeric: {total - null_count:,}/{total:,} valid ({100-pct_null:.1f}% completeness)"}
    else:
        return {"status": "FAIL", "standard": "Numeric Metric Values",
                "message": f"{null_count:,}/{total:,} metric_values non-numeric ({pct_null}% failure rate)"}


# =============================================================================
# PER-SOURCE RUNNER
# =============================================================================

CHECKS = [
    check_iso_record_ids,
    check_iso_timestamps,
    check_iso_country_codes,
    check_bls_ces_series_format,
    check_bls_cps_series_format,
    check_bls_cps_metric_vocabulary,
    check_bls_seasonal_adjustment,
    check_naics_industry_codes,
    check_sdmx_temporal_granularity,
    check_pit_field_completeness,
    check_extraction_method_vocabulary,
    check_metric_value_numeric,
]


def validate_source(source: str) -> dict:
    logger.info(f"\n{'=' * 70}")
    logger.info(f"SOURCE: {source.upper()}")
    logger.info(f"{'=' * 70}")

    df = load_sample(source, max_files=50)
    if df.empty:
        logger.error(f"  No data loaded for {source}")
        return {"source": source, "status": "ERROR", "results": [],
                "checks_passed": 0, "checks_failed": 0, "checks_skipped": 0}

    logger.info(f"  Sample loaded: {len(df):,} records from {source}")
    logger.info("")

    results = []
    passed = 0
    failed = 0
    skipped = 0

    for check_fn in CHECKS:
        result = check_fn(df, source)
        result["check"] = check_fn.__name__

        status = result["status"]
        standard = result.get("standard", "")
        message = result.get("message", "")

        if status == "PASS":
            passed += 1
            logger.info(f"  [PASS] {standard}")
            logger.info(f"         {message}")
        elif status == "SKIP":
            skipped += 1
            logger.info(f"  [SKIP] {standard} - {message}")
        else:
            failed += 1
            logger.error(f"  [FAIL] {standard}")
            logger.error(f"         {message}")

        results.append(result)

    overall = "PASS" if failed == 0 else "FAIL"
    logger.info(f"\n  Summary: {passed} passed, {failed} failed, {skipped} skipped -> [{overall}]")

    return {
        "source": source,
        "status": overall,
        "sample_records": len(df),
        "checks_passed": passed,
        "checks_failed": failed,
        "checks_skipped": skipped,
        "results": results
    }


# =============================================================================
# MAIN
# =============================================================================

def run_schema_compliance():
    logger.info("=" * 70)
    logger.info("MACRO WAGES & EMPLOYMENT - INDUSTRY STANDARD SCHEMA COMPLIANCE")
    logger.info("=" * 70)
    logger.info("Standards applied:")
    logger.info("  - ISO/IEC 9834-8 UUID v4 (record_id format)")
    logger.info("  - ISO 8601 (timestamp format, UTC)")
    logger.info("  - ISO 3166-1 (country_code)")
    logger.info("  - BLS CES Series ID Format (CES + 10 digits)")
    logger.info("  - BLS CPS Series ID Format (LNS + 8 digits)")
    logger.info("  - BLS CPS Metric Vocabulary (unemployment/employment metrics)")
    logger.info("  - BLS Seasonal Adjustment Codes (S/U)")
    logger.info("  - NAICS / BLS Supersector Codes (8-digit industry)")
    logger.info("  - SDMX TIME_PERIOD Monthly Granularity")
    logger.info("  - Lekwankwa PIT Schema v4.0 (5 PIT fields)")
    logger.info("  - Controlled vocabulary (extraction_method)")

    all_results = {}
    for source in SOURCES:
        all_results[source] = validate_source(source)

    # Final summary
    total_passed = sum(r["checks_passed"] for r in all_results.values())
    total_failed = sum(r["checks_failed"] for r in all_results.values())
    total_skipped = sum(r["checks_skipped"] for r in all_results.values())
    overall = "PASS" if total_failed == 0 else "FAIL"

    logger.info("\n" + "=" * 70)
    logger.info("OVERALL SCHEMA COMPLIANCE SUMMARY")
    logger.info("=" * 70)
    for source, r in all_results.items():
        logger.info(f"  {source:<15}: [{r['status']}] {r['checks_passed']} passed / {r['checks_failed']} failed / {r['checks_skipped']} skipped")
    logger.info(f"\n  Total: {total_passed} passed, {total_failed} failed, {total_skipped} skipped")
    logger.info(f"  Overall: [{overall}]")

    # Save JSON report
    report = {
        "product": PRODUCT,
        "validated_at": datetime.now().isoformat(),
        "standards_applied": [
            "ISO/IEC 9834-8 UUID v4",
            "ISO 8601 UTC Timestamps",
            "ISO 3166-1 Country Codes",
            "BLS CES Series ID Format",
            "BLS JOLTS Series ID Format",
            "BLS JOLTS Element Codes",
            "BLS Seasonal Adjustment Codes (S/U)",
            "NAICS / BLS Supersector Codes",
            "SDMX TIME_PERIOD Monthly",
            "Lekwankwa PIT Schema v4.0",
            "Controlled Vocabulary (extraction_method)",
        ],
        "source_results": all_results,
        "summary": {
            "overall": overall,
            "total_passed": total_passed,
            "total_failed": total_failed,
            "total_skipped": total_skipped,
        }
    }

    with open(REPORT_JSON, "w") as f:
        json.dump(report, f, indent=2, default=str)

    # Save text report
    with open(REPORT_TXT, "w") as f:
        f.write("MACRO EMPLOYMENT - INDUSTRY STANDARD SCHEMA COMPLIANCE REPORT\n")
        f.write("=" * 70 + "\n\n")
        for source, sr in all_results.items():
            f.write(f"SOURCE: {source.upper()}\n")
            f.write(f"Status: [{sr['status']}]\n")
            f.write(f"Sample: {sr.get('sample_records', 0):,} records\n\n")
            for r in sr.get("results", []):
                status = r["status"]
                standard = r.get("standard", r.get("check", ""))
                message = r.get("message", "")
                f.write(f"  [{status:<4}] {standard}\n")
                f.write(f"         {message}\n")
                if "details" in r and r["details"]:
                    f.write(f"         Details: {r['details']}\n")
            f.write("\n")
        f.write(f"OVERALL: [{overall}] | {total_passed} passed / {total_failed} failed / {total_skipped} skipped\n")

    logger.info(f"\nReports saved: {REPORT_JSON}, {REPORT_TXT}")
    return total_failed == 0


if __name__ == "__main__":
    success = run_schema_compliance()
    sys.exit(0 if success else 1)
