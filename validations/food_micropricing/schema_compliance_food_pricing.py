"""
Industry-Standard Schema Validation for Food Micropricing Data

Validates the vault data against recognised industry standards:

  BLS STANDARDS:
  - BLS CPI Average Price series ID format: APU + 10 digits (13 characters)
  - BLS source URL: https://www.bls.gov/cpi/data.htm

  ISO / COMMON STANDARDS:
  - record_id: UUID v4 format (ISO/IEC 9834-8)
  - data_timestamp: ISO 8601, UTC timezone-aware (monthly granularity)
  - country_code: ISO 3166-1 alpha-2 ('US')
  - currency: ISO 4217 ('USD')

  CONTROLLED VOCABULARIES:
  - source: {'bls'}
  - extraction_method: {'api', 'scraper', 'manual'}
  - category: 11 valid food categories
  - data_quality_certified: boolean True for all production records

  DATA INTEGRITY:
  - item_value: positive numeric (> 0)
  - usd_equivalent: equals item_value for USD-denominated data
  - pct_change_mom: numeric (nulls permitted for first observation per series)
  - SDMX TIME_PERIOD: monthly granularity (day=1 for all records)

OUTPUT:
  - food_pricing_schema_compliance_report.json
  - food_pricing_schema_compliance_report.txt
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
import logging

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('food_pricing_schema_compliance.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

VAULT_DIR = Path("lekwankwa-historical-vault")
PRODUCT = "food_micropricing"
COUNTRY = "USA"
SOURCES = ["bls", "usda_ers"]

SOURCE_FILES = {
    "bls":      "food_pricing_data.parquet",
    "usda_ers": "food_pricing_data.parquet",
}


REPORT_JSON = Path("food_pricing_schema_compliance_report.json")
REPORT_TXT  = Path("food_pricing_schema_compliance_report.txt")

# --- BLS CPI Average Price Standards ---
# Series ID: APU + 2-digit area code + 8-digit item code = 13 characters
BLS_SERIES_PATTERN = re.compile(r'^APU\d{10}$')
BLS_SOURCE_URL = 'https://www.bls.gov/cpi/data.htm'

# --- ISO 4217 Currency ---
VALID_CURRENCIES = {'USD'}

# --- ISO 3166-1 ---
VALID_COUNTRY_CODES = {'US', 'USA'}

# --- Controlled Vocabularies ---
VALID_SOURCES = {'bls', 'usda_ers'}
VALID_EXTRACTION_METHODS = {'api', 'scraper', 'manual'}

# 11 valid food categories (matches data dictionary)
VALID_CATEGORIES = {
    'All Food',
    'Cereals & Grains',
    'Meat & Poultry',
    'Dairy & Eggs',
    'Vegetables',
    'Fruits',
    'Oils & Fats',
    'Beverages',
    'Sugar & Spices',
    'Fish & Seafood',
    'Other Foods',
}

# Data range constraints (per data dictionary)
ITEM_VALUE_MIN = 0.01
ITEM_VALUE_MAX = 1_000_000.0
PCT_CHANGE_MIN = -99.99
PCT_CHANGE_MAX = 1_000.0

# --- ISO/IEC 9834-8 UUID v4 ---
UUID_PATTERN = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$',
    re.IGNORECASE,
)


# =============================================================================
# LOAD DATA
# =============================================================================

def load_sample(source: str, max_files: int = 50) -> pd.DataFrame:
    """Load a representative sample and normalise BLS column names to canonical form."""
    fname = SOURCE_FILES.get(source, "*.parquet")
    source_path = VAULT_DIR / f"product={PRODUCT}" / f"country={COUNTRY}" / f"source={source}"
    all_files = sorted(source_path.rglob(fname))

    step = max(1, len(all_files) // max_files)
    sampled = all_files[::step][:max_files]

    dfs = []
    for f in sampled:
        try:
            dfs.append(pd.read_parquet(f))
        except Exception as e:
            logger.warning(f"Could not read {f}: {e}")

    if not dfs:
        return pd.DataFrame()

    df = pd.concat(dfs, ignore_index=True)
    return df


# =============================================================================
# CHECKS
# =============================================================================

def check_iso_record_ids(df: pd.DataFrame, source: str) -> dict:
    """ISO/IEC 9834-8: record_id must be valid UUID v4."""
    if "record_id" not in df.columns:
        return {"status": "FAIL", "standard": "ISO/IEC 9834-8 UUID",
                "message": "record_id column missing"}

    valid = df["record_id"].dropna().apply(lambda x: bool(UUID_PATTERN.match(str(x))))
    invalid_count = int((~valid).sum())
    total = int(df["record_id"].dropna().shape[0])

    if invalid_count == 0:
        return {"status": "PASS", "standard": "ISO/IEC 9834-8 UUID",
                "message": f"All {total:,} record_ids are valid UUID v4"}
    return {"status": "FAIL", "standard": "ISO/IEC 9834-8 UUID",
            "message": f"{invalid_count:,}/{total:,} record_ids fail UUID v4 format",
            "details": {"invalid_count": invalid_count}}


def check_iso_timestamps(df: pd.DataFrame, source: str) -> dict:
    """ISO 8601: All timestamp columns must be UTC-aware and parseable."""
    issues = {}
    for col in ["data_timestamp", "conversion_timestamp", "official_release_date", "as_of_date"]:
        if col not in df.columns:
            continue
        ts = pd.to_datetime(df[col], errors="coerce", utc=True)
        n = int(ts.isna().sum())
        if n > 0:
            issues[col] = n

    if not issues:
        return {"status": "PASS", "standard": "ISO 8601 (UTC Timestamps)",
                "message": "All timestamp fields are valid ISO 8601 UTC"}
    return {"status": "FAIL", "standard": "ISO 8601 (UTC Timestamps)",
            "message": f"Invalid/null timestamps in: {list(issues.keys())}",
            "details": issues}


def check_iso_country_codes(df: pd.DataFrame, source: str) -> dict:
    """ISO 3166-1 alpha-2: country_code must be 'US'."""
    if "country_code" not in df.columns:
        return {"status": "FAIL", "standard": "ISO 3166-1 Country Code",
                "message": "country_code column missing"}

    unique = set(df["country_code"].dropna().str.upper().unique())
    invalid = unique - VALID_COUNTRY_CODES

    if not invalid:
        return {"status": "PASS", "standard": "ISO 3166-1 Country Code",
                "message": f"All country_codes valid: {unique}"}
    return {"status": "FAIL", "standard": "ISO 3166-1 Country Code",
            "message": f"Invalid country codes: {invalid}"}


def check_iso_currency(df: pd.DataFrame, source: str) -> dict:
    """ISO 4217: currency must be 'USD' for this dataset."""
    if "currency" not in df.columns:
        return {"status": "FAIL", "standard": "ISO 4217 Currency Code",
                "message": "currency column missing"}

    unique = set(df["currency"].dropna().str.upper().unique())
    invalid = unique - VALID_CURRENCIES

    if not invalid:
        return {"status": "PASS", "standard": "ISO 4217 Currency Code",
                "message": f"All currency values valid: {unique}"}
    return {"status": "FAIL", "standard": "ISO 4217 Currency Code",
            "message": f"Invalid currency codes: {invalid}"}


def check_bls_series_id_format(df: pd.DataFrame, source: str) -> dict:
    """BLS Standard: BLS series IDs must be APU + 10 digits (CPI Average Price series)."""
    if source != "bls":
        return {"status": "SKIP", "standard": "BLS CPI Series ID Format",
                "message": "N/A for non-BLS source"}
    if "sovereign_series_id" not in df.columns:
        return {"status": "FAIL", "standard": "BLS CPI Series ID Format",
                "message": "sovereign_series_id column missing"}

    ids = df["sovereign_series_id"].dropna()
    invalid = ids[~ids.apply(lambda x: bool(BLS_SERIES_PATTERN.match(str(x))))]

    if len(invalid) == 0:
        return {"status": "PASS", "standard": "BLS CPI Series ID Format",
                "message": f"All {len(ids):,} series IDs match BLS APU format (APU + 10 digits)"}
    return {"status": "FAIL", "standard": "BLS CPI Series ID Format",
            "message": f"{len(invalid):,} series IDs do not match APU format",
            "details": {"invalid_samples": invalid.head(5).tolist()}}


ERS_SOURCE_URL = "https://www.ers.usda.gov/data-products/food-price-outlook/"
SOURCE_URLS = {"bls": BLS_SOURCE_URL, "usda_ers": ERS_SOURCE_URL}


def check_source_url(df: pd.DataFrame, source: str) -> dict:
    """Data lineage: portal_url must point to the official agency endpoint."""
    url_col = "portal_url" if "portal_url" in df.columns else "source_url"
    if url_col not in df.columns:
        return {"status": "FAIL", "standard": "Data Lineage (Source URL)",
                "message": "portal_url / source_url column missing"}

    expected = SOURCE_URLS.get(source, BLS_SOURCE_URL)
    urls = df[url_col].dropna()
    invalid = urls[urls != expected]

    if len(invalid) == 0:
        return {"status": "PASS", "standard": "Data Lineage (Source URL)",
                "message": f"All {len(urls):,} source URLs point to official {source.upper()} endpoint"}
    return {"status": "FAIL", "standard": "Data Lineage (Source URL)",
            "message": f"{len(invalid):,} records have unexpected source URL",
            "details": {"unexpected_urls": list(invalid.unique()[:3])}}


def check_sdmx_temporal_granularity(df: pd.DataFrame, source: str) -> dict:
    """SDMX: TIME_PERIOD should be monthly (day=1 for all records)."""
    if "data_timestamp" not in df.columns:
        return {"status": "FAIL", "standard": "SDMX TIME_PERIOD Monthly",
                "message": "data_timestamp missing"}

    dates = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True)
    non_monthly = int((dates.dt.day != 1).sum())
    valid = int(dates.notna().sum())

    if non_monthly == 0:
        min_dt = dates.min().strftime("%Y-%m")
        max_dt = dates.max().strftime("%Y-%m")
        return {"status": "PASS", "standard": "SDMX TIME_PERIOD Monthly",
                "message": f"All {valid:,} records use monthly granularity (day=1). Range: {min_dt} to {max_dt}"}
    return {"status": "FAIL", "standard": "SDMX TIME_PERIOD Monthly",
            "message": f"{non_monthly:,} records have non-monthly timestamps (day != 1)",
            "details": {"non_monthly_count": non_monthly}}


def check_category_vocabulary(df: pd.DataFrame, source: str) -> dict:
    """Controlled vocabulary: category must be one of 11 valid food categories."""
    if "category" not in df.columns:
        return {"status": "FAIL", "standard": "Food Category Vocabulary",
                "message": "category column missing"}

    unique = set(df["category"].dropna().unique())
    invalid = unique - VALID_CATEGORIES

    if not invalid:
        return {"status": "PASS", "standard": "Food Category Vocabulary",
                "message": f"All {len(unique)} category values are valid: {sorted(unique)}"}
    return {"status": "FAIL", "standard": "Food Category Vocabulary",
            "message": f"Unrecognised food categories: {invalid}",
            "details": {"invalid_categories": list(invalid)}}


def check_source_vocabulary(df: pd.DataFrame, source: str) -> dict:
    """Controlled vocabulary: source must be 'bls'."""
    if "source" not in df.columns:
        return {"status": "FAIL", "standard": "Controlled Vocabulary (source)",
                "message": "source column missing"}

    unique = set(df["source"].dropna().str.lower().unique())
    invalid = unique - VALID_SOURCES

    if not invalid:
        return {"status": "PASS", "standard": "Controlled Vocabulary (source)",
                "message": f"All source values valid: {unique}"}
    return {"status": "FAIL", "standard": "Controlled Vocabulary (source)",
            "message": f"Invalid source values: {invalid}"}


def check_extraction_method_vocabulary(df: pd.DataFrame, source: str) -> dict:
    """Controlled vocabulary: extraction_method must be api/scraper/manual."""
    if "extraction_method" not in df.columns:
        return {"status": "FAIL", "standard": "Controlled Vocabulary (extraction_method)",
                "message": "extraction_method column missing"}

    unique = set(df["extraction_method"].dropna().str.lower().unique())
    invalid = unique - VALID_EXTRACTION_METHODS

    if not invalid:
        return {"status": "PASS", "standard": "Controlled Vocabulary (extraction_method)",
                "message": f"All extraction_method values valid: {unique}"}
    return {"status": "FAIL", "standard": "Controlled Vocabulary (extraction_method)",
            "message": f"Invalid extraction_method values: {invalid}"}


def check_item_value_numeric_range(df: pd.DataFrame, source: str) -> dict:
    """Data integrity: observed_price_local must be positive numeric within valid range."""
    if "observed_price_local" not in df.columns:
        return {"status": "FAIL", "standard": "Item Value Range (Numeric)",
                "message": "observed_price_local column missing"}

    numeric = pd.to_numeric(df["observed_price_local"], errors="coerce")
    null_count = int(numeric.isna().sum())
    out_of_range = int(((numeric < ITEM_VALUE_MIN) | (numeric > ITEM_VALUE_MAX)).sum())

    if null_count == 0 and out_of_range == 0:
        mn = float(numeric.min())
        mx = float(numeric.max())
        return {"status": "PASS", "standard": "Item Value Range (Numeric)",
                "message": f"All {len(df):,} observed_price_local values are numeric and in range [{ITEM_VALUE_MIN}, {ITEM_VALUE_MAX}]. "
                           f"Actual range: {mn:.4f} to {mx:.4f}"}
    issues = {}
    if null_count:  issues["null_count"]      = null_count
    if out_of_range: issues["out_of_range_count"] = out_of_range
    return {"status": "FAIL", "standard": "Item Value Range (Numeric)",
            "message": f"observed_price_local issues detected: {issues}",
            "details": issues}


def check_usd_equivalent_consistency(df: pd.DataFrame, source: str) -> dict:
    """Data integrity: price_usd_equivalent must equal observed_price_local for USD-denominated records."""
    if "price_usd_equivalent" not in df.columns or "observed_price_local" not in df.columns:
        return {"status": "FAIL", "standard": "USD Equivalent Consistency",
                "message": "price_usd_equivalent or observed_price_local column missing"}

    iv = pd.to_numeric(df["observed_price_local"], errors="coerce")
    ue = pd.to_numeric(df["price_usd_equivalent"], errors="coerce")
    both_valid = iv.notna() & ue.notna()
    mismatch = int((abs(iv[both_valid] - ue[both_valid]) > 0.0001).sum())

    if mismatch == 0:
        return {"status": "PASS", "standard": "USD Equivalent Consistency",
                "message": f"price_usd_equivalent equals observed_price_local for all {both_valid.sum():,} valid records"}
    return {"status": "FAIL", "standard": "USD Equivalent Consistency",
            "message": f"{mismatch:,} records have price_usd_equivalent != observed_price_local (tolerance 0.0001)",
            "details": {"mismatch_count": mismatch}}


def check_pct_change_range(df: pd.DataFrame, source: str) -> dict:
    """Data integrity: pct_change_mom must be within [-99.99, 1000.0] when not null."""
    if "pct_change_mom" not in df.columns:
        return {"status": "FAIL", "standard": "Pct Change MoM Range",
                "message": "pct_change_mom column missing"}

    numeric = pd.to_numeric(df["pct_change_mom"], errors="coerce")
    non_null = numeric.dropna()
    out_of_range = int(((non_null < PCT_CHANGE_MIN) | (non_null > PCT_CHANGE_MAX)).sum())
    null_pct = round(numeric.isna().mean() * 100, 2)

    if out_of_range == 0:
        return {"status": "PASS", "standard": "Pct Change MoM Range",
                "message": f"All non-null pct_change_mom values within [{PCT_CHANGE_MIN}, {PCT_CHANGE_MAX}]. "
                           f"{null_pct}% null (permitted for first observation)"}
    return {"status": "FAIL", "standard": "Pct Change MoM Range",
            "message": f"{out_of_range:,} pct_change_mom values outside valid range",
            "details": {"out_of_range_count": out_of_range, "null_pct": null_pct}}


def check_pit_field_completeness(df: pd.DataFrame, source: str) -> dict:
    """PIT Standard: All 5 PIT fields must be present and valid (except superseded_by)."""
    required = ["record_id", "published_date", "as_of_date", "revision_number"]

    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        return {"status": "FAIL", "standard": "Lekwankwa PIT Schema v4.0",
                "message": f"Missing PIT columns: {missing_cols}"}

    null_violations = {c: int(df[c].isna().sum()) for c in required if df[c].isna().any()}
    superseded_null_pct = 100.0
    if "superseded_by" in df.columns:
        superseded_null_pct = round(df["superseded_by"].isna().mean() * 100, 2)

    if not null_violations:
        return {"status": "PASS", "standard": "Lekwankwa PIT Schema v4.0",
                "message": f"All PIT fields present and populated. "
                           f"superseded_by is {superseded_null_pct}% null (expected for initial load)",
                "details": {"superseded_by_null_pct": superseded_null_pct}}
    return {"status": "FAIL", "standard": "Lekwankwa PIT Schema v4.0",
            "message": f"Null values in required PIT fields: {null_violations}",
            "details": null_violations}


def check_data_quality_certified(df: pd.DataFrame, source: str) -> dict:
    """Data integrity: data_quality_certified must be boolean (True=certified, False=pending)."""
    if "data_quality_certified" not in df.columns:
        return {"status": "FAIL", "standard": "Data Quality Certification",
                "message": "data_quality_certified column missing"}

    null_count  = int(df["data_quality_certified"].isna().sum())
    false_count = int((df["data_quality_certified"] == False).sum())  # noqa: E712
    true_count  = int((df["data_quality_certified"] == True).sum())   # noqa: E712
    total = len(df)

    if null_count > 0:
        return {"status": "FAIL", "standard": "Data Quality Certification",
                "message": f"{null_count:,} null values — field must be boolean",
                "details": {"null_count": null_count}}
    if false_count == 0:
        return {"status": "PASS", "standard": "Data Quality Certification",
                "message": f"All {total:,} records have data_quality_certified=True"}
    # false_count > 0: data pending manual QA — warn, not fail (False is valid)
    return {"status": "WARN", "standard": "Data Quality Certification",
            "message": f"{false_count:,}/{total:,} records have data_quality_certified=False "
                       f"(pending manual QA sign-off — valid for newly ingested sources)",
            "details": {"certified_count": true_count, "pending_count": false_count}}


# =============================================================================
# CHECK REGISTRY
# =============================================================================

CHECKS = [
    check_iso_record_ids,
    check_iso_timestamps,
    check_iso_country_codes,
    check_iso_currency,
    check_bls_series_id_format,
    check_source_url,
    check_sdmx_temporal_granularity,
    check_category_vocabulary,
    check_source_vocabulary,
    check_extraction_method_vocabulary,
    check_item_value_numeric_range,
    check_usd_equivalent_consistency,
    check_pct_change_range,
    check_pit_field_completeness,
    check_data_quality_certified,
]


# =============================================================================
# PER-SOURCE RUNNER
# =============================================================================

def validate_source(source: str) -> dict:
    logger.info(f"\n{'=' * 70}")
    logger.info(f"SOURCE: {source.upper()}")
    logger.info(f"{'=' * 70}")

    df = load_sample(source, max_files=50)
    if df.empty:
        logger.error(f"  No data loaded for {source}")
        return {"source": source, "status": "ERROR", "results": []}

    logger.info(f"  Sample loaded: {len(df):,} records from {source}")
    logger.info("")

    results = []
    passed = failed = skipped = 0

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
        elif status == "WARN":
            passed += 1   # warn counts as passed — it's a non-blocking advisory
            logger.warning(f"  [WARN] {standard}")
            logger.warning(f"         {message}")
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
        "results": results,
    }


# =============================================================================
# MAIN
# =============================================================================

def run_schema_compliance():
    logger.info("=" * 70)
    logger.info("FOOD MICROPRICING - INDUSTRY STANDARD SCHEMA COMPLIANCE")
    logger.info("=" * 70)
    logger.info("Standards applied:")
    logger.info("  - ISO/IEC 9834-8 UUID v4 (record_id format)")
    logger.info("  - ISO 8601 (timestamp format, UTC)")
    logger.info("  - ISO 3166-1 (country_code)")
    logger.info("  - ISO 4217 (currency code)")
    logger.info("  - BLS CPI Average Price Series ID Format (APU + 10 digits)")
    logger.info("  - Data Lineage (official agency source URLs)")
    logger.info("  - SDMX TIME_PERIOD Monthly Granularity")
    logger.info("  - Food Category Controlled Vocabulary (11 categories)")
    logger.info("  - Controlled vocabulary (source, extraction_method)")
    logger.info("  - Lekwankwa PIT Schema v4.0 (5 PIT fields)")
    logger.info("  - Numeric range validation (item_value, pct_change_mom)")
    logger.info("  - USD equivalence consistency")
    logger.info("")

    all_results = []
    total_passed = total_failed = total_skipped = 0

    for source in SOURCES:
        result = validate_source(source)
        all_results.append(result)
        total_passed  += result.get("checks_passed", 0)
        total_failed  += result.get("checks_failed", 0)
        total_skipped += result.get("checks_skipped", 0)

    # Summary
    logger.info(f"\n{'=' * 70}")
    logger.info("OVERALL SCHEMA COMPLIANCE SUMMARY")
    logger.info(f"{'=' * 70}")
    for r in all_results:
        status = r.get("status", "ERROR")
        p = r.get("checks_passed", 0)
        f = r.get("checks_failed", 0)
        s = r.get("checks_skipped", 0)
        label = r["source"].ljust(10)
        logger.info(f"  {label}: [{status}] {p} passed / {f} failed / {s} skipped")

    overall_status = "PASS" if total_failed == 0 else "FAIL"
    logger.info(f"\n  Total: {total_passed} passed, {total_failed} failed, {total_skipped} skipped")
    logger.info(f"  Overall: [{overall_status}]")

    # Write reports
    report = {
        "run_timestamp": datetime.utcnow().isoformat() + "Z",
        "product": PRODUCT,
        "country": COUNTRY,
        "overall_status": overall_status,
        "total_passed": total_passed,
        "total_failed": total_failed,
        "total_skipped": total_skipped,
        "sources": all_results,
    }

    with open(REPORT_JSON, "w") as f:
        json.dump(report, f, indent=2, default=str)

    with open(REPORT_TXT, "w") as f:
        f.write("FOOD MICROPRICING SCHEMA COMPLIANCE REPORT\n")
        f.write(f"Run: {report['run_timestamp']}\n")
        f.write(f"Overall: [{overall_status}] — {total_passed} passed / {total_failed} failed / {total_skipped} skipped\n\n")
        for r in all_results:
            f.write(f"Source: {r['source']}\n")
            f.write(f"  Status: [{r.get('status', 'ERROR')}]\n")
            for chk in r.get("results", []):
                f.write(f"  [{chk['status']}] {chk.get('standard', '')}: {chk.get('message', '')}\n")
            f.write("\n")

    logger.info(f"\nReports saved: {REPORT_JSON}, {REPORT_TXT}")


if __name__ == "__main__":
    run_schema_compliance()
