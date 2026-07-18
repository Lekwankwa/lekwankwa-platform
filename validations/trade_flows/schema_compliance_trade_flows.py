"""
Industry Standard Schema Compliance — Trade Flows (US Census FT-900)

STANDARDS APPLIED:
  - ISO/IEC 9834-8  : UUID v4 format for record_id
  - ISO 8601        : UTC-aware timestamps
  - ISO 3166-1      : country_code (US / USA)
  - UN HS 2022      : Harmonized System 2-digit chapter code format
  - Census FIPS     : Partner country code (4-digit)
  - Lekwankwa PIT   : 5 PIT fields present and populated
  - Controlled vocab: trade_flow, unit_of_measure, extraction_method, currency, source

OUTPUT:
  trade_flows_schema_compliance_report.json
  trade_flows_schema_compliance_report.txt

Author: Lekwankwa Corporation
Date: 2026-06-13
"""

import json
import logging
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import List

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("trade_flows_schema_compliance.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _vault_root import VAULT_ROOT, vault_glob_paths as vault_glob, vault_read_parquet  # noqa: E402

# =============================================================================
# CONFIGURATION
# =============================================================================

VAULT_DIR = VAULT_ROOT
PRODUCT   = "trade_flows"
COUNTRY   = "USA"
SOURCES   = ["census_ft900"]

REPORT_JSON = Path("trade_flows_schema_compliance_report.json")
REPORT_TXT  = Path("trade_flows_schema_compliance_report.txt")

# Standards
UUID_PATTERN        = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
)
HS2_CODE_PATTERN    = re.compile(r"^\d{2}$")           # 2-digit numeric
SERIES_ID_PATTERN   = re.compile(r"^HS\d{2}_(EXP|IMP)$")  # e.g. HS01_EXP
CENSUS_CTY_PATTERN  = re.compile(r"^\d{4}$")           # 4-digit FIPS country code

VALID_COUNTRY_CODES       = {"US", "USA"}
VALID_TRADE_FLOWS         = {"Export", "Import"}
VALID_UNITS               = {"USD_MILLIONS"}
VALID_CURRENCIES          = {"USD"}
VALID_EXTRACTION_METHODS  = {"api", "scraper", "manual"}
VALID_SOURCES             = {"census_ft900"}
CENSUS_PORTAL             = "https://www.census.gov/foreign-trade/"


# =============================================================================
# DATA LOADER
# =============================================================================

def load_sample(source: str, max_files: int = 60) -> pd.DataFrame:
    """Load a representative sample (avoid loading all ~85K records)."""
    source_path = f"{VAULT_DIR}/product={PRODUCT}/country={COUNTRY}/source={source}"
    all_files   = [
        f for f in vault_glob(source_path, "*.parquet")
        if "outliers" not in f.name and "changelog" not in f.name
    ]
    step    = max(1, len(all_files) // max_files)
    sampled = all_files[::step][:max_files]

    dfs = []
    for f in sampled:
        try:
            dfs.append(vault_read_parquet(f))
        except Exception as exc:
            logger.warning(f"Could not read {f}: {exc}")

    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


# =============================================================================
# CHECKS
# =============================================================================

def check_iso_record_ids(df: pd.DataFrame, source: str) -> dict:
    """ISO/IEC 9834-8: record_id must be valid UUID v4."""
    if "record_id" not in df.columns:
        return {"status": "FAIL", "standard": "ISO/IEC 9834-8 UUID v4",
                "message": "record_id column missing"}
    valid   = df["record_id"].dropna().apply(lambda x: bool(UUID_PATTERN.match(str(x))))
    invalid = int((~valid).sum())
    total   = int(valid.count())
    if invalid == 0:
        return {"status": "PASS", "standard": "ISO/IEC 9834-8 UUID v4",
                "message": f"All {total:,} record_ids are valid UUID v4"}
    return {"status": "FAIL", "standard": "ISO/IEC 9834-8 UUID v4",
            "message": f"{invalid:,}/{total:,} record_ids fail UUID v4 format",
            "details": {"invalid_count": invalid}}


def check_iso_timestamps(df: pd.DataFrame, source: str) -> dict:
    """ISO 8601: All timestamp columns must be parseable as UTC-aware datetimes."""
    issues = {}
    for col in ["data_timestamp", "published_date", "as_of_date",
                "conversion_timestamp", "official_release_date"]:
        if col not in df.columns:
            continue
        ts    = pd.to_datetime(df[col], errors="coerce", utc=True)
        nulls = int(ts.isna().sum())
        if nulls > 0:
            issues[col] = nulls
    if not issues:
        return {"status": "PASS", "standard": "ISO 8601 (UTC timestamps)",
                "message": "All timestamp fields are valid ISO 8601 UTC"}
    return {"status": "FAIL", "standard": "ISO 8601 (UTC timestamps)",
            "message": f"Invalid/null timestamps in: {list(issues.keys())}",
            "details": issues}


def check_iso_country_codes(df: pd.DataFrame, source: str) -> dict:
    """ISO 3166-1: country_code must be US or USA."""
    if "country_code" not in df.columns:
        return {"status": "FAIL", "standard": "ISO 3166-1 Country Code",
                "message": "country_code column missing"}
    found   = set(df["country_code"].dropna().str.upper().unique())
    invalid = found - VALID_COUNTRY_CODES
    if not invalid:
        return {"status": "PASS", "standard": "ISO 3166-1 Country Code",
                "message": f"All country_codes valid: {found}"}
    return {"status": "FAIL", "standard": "ISO 3166-1 Country Code",
            "message": f"Invalid country_codes: {invalid}",
            "details": {"invalid": list(invalid)}}


def check_hs_commodity_code_format(df: pd.DataFrame, source: str) -> dict:
    """UN HS 2022: commodity_code must be a 2-digit numeric string."""
    if "commodity_code" not in df.columns:
        return {"status": "FAIL", "standard": "UN HS Commodity Code Format",
                "message": "commodity_code column missing"}
    valid_ids = df["commodity_code"].dropna()
    invalid   = valid_ids[~valid_ids.apply(lambda x: bool(HS2_CODE_PATTERN.match(str(x))))]
    if len(invalid) == 0:
        n_unique = valid_ids.nunique()
        return {"status": "PASS", "standard": "UN HS Commodity Code Format",
                "message": f"All {len(valid_ids):,} commodity_codes match HS2 format ({n_unique} unique chapters)"}
    return {"status": "FAIL", "standard": "UN HS Commodity Code Format",
            "message": f"{len(invalid):,} commodity_codes do not match 2-digit HS2 format",
            "details": {"invalid_samples": invalid.head(5).tolist()}}


def check_series_id_format(df: pd.DataFrame, source: str) -> dict:
    """Series ID: sovereign_series_id must match HS{2d}_(EXP|IMP) pattern."""
    col = "sovereign_series_id"
    if col not in df.columns:
        return {"status": "FAIL", "standard": "Census Series ID Format",
                "message": f"{col} column missing"}
    valid_ids = df[col].dropna()
    invalid   = valid_ids[~valid_ids.apply(lambda x: bool(SERIES_ID_PATTERN.match(str(x))))]
    if len(invalid) == 0:
        return {"status": "PASS", "standard": "Census Series ID Format",
                "message": f"All {len(valid_ids):,} series IDs match HS{{2d}}_(EXP|IMP) format"}
    return {"status": "FAIL", "standard": "Census Series ID Format",
            "message": f"{len(invalid):,} series IDs do not match expected format",
            "details": {"invalid_samples": invalid.head(5).tolist()}}


def check_partner_country_code_format(df: pd.DataFrame, source: str) -> dict:
    """Census FIPS: partner_country_code must be 4-digit numeric."""
    if "partner_country_code" not in df.columns:
        return {"status": "SKIP", "standard": "Census Partner Country Code (FIPS 4-digit)",
                "message": "partner_country_code column not present"}
    valid_ids = df["partner_country_code"].dropna().astype(str)
    invalid   = valid_ids[~valid_ids.apply(lambda x: bool(CENSUS_CTY_PATTERN.match(x)))]
    if len(invalid) == 0:
        return {"status": "PASS", "standard": "Census Partner Country Code (FIPS 4-digit)",
                "message": f"All {len(valid_ids):,} partner_country_codes are 4-digit FIPS"}
    return {"status": "FAIL", "standard": "Census Partner Country Code (FIPS 4-digit)",
            "message": f"{len(invalid):,} partner_country_codes do not match 4-digit FIPS format",
            "details": {"invalid_samples": invalid.head(5).tolist()}}


def check_trade_flow_vocabulary(df: pd.DataFrame, source: str) -> dict:
    """Controlled vocabulary: trade_flow must be 'Export' or 'Import'."""
    if "trade_flow" not in df.columns:
        return {"status": "FAIL", "standard": "Trade Flow Vocabulary",
                "message": "trade_flow column missing"}
    found   = set(df["trade_flow"].dropna().unique())
    invalid = found - VALID_TRADE_FLOWS
    if not invalid:
        return {"status": "PASS", "standard": "Trade Flow Vocabulary",
                "message": f"All trade_flow values valid: {sorted(found)}"}
    return {"status": "FAIL", "standard": "Trade Flow Vocabulary",
            "message": f"Invalid trade_flow values: {invalid}",
            "details": {"invalid": list(invalid)}}


def check_unit_of_measure_vocabulary(df: pd.DataFrame, source: str) -> dict:
    """Controlled vocabulary: unit_of_measure must be USD_MILLIONS."""
    if "unit_of_measure" not in df.columns:
        return {"status": "FAIL", "standard": "Unit of Measure Vocabulary",
                "message": "unit_of_measure column missing"}
    found   = set(df["unit_of_measure"].dropna().unique())
    invalid = found - VALID_UNITS
    if not invalid:
        return {"status": "PASS", "standard": "Unit of Measure Vocabulary",
                "message": f"All unit_of_measure values valid: {found}"}
    return {"status": "FAIL", "standard": "Unit of Measure Vocabulary",
            "message": f"Invalid unit_of_measure values: {invalid}",
            "details": {"invalid": list(invalid)}}


def check_currency_vocabulary(df: pd.DataFrame, source: str) -> dict:
    """Controlled vocabulary: currency must be USD."""
    if "currency" not in df.columns:
        return {"status": "FAIL", "standard": "Currency Vocabulary",
                "message": "currency column missing"}
    found   = set(df["currency"].dropna().unique())
    invalid = found - VALID_CURRENCIES
    if not invalid:
        return {"status": "PASS", "standard": "Currency Vocabulary",
                "message": f"All currency values valid: {found}"}
    return {"status": "FAIL", "standard": "Currency Vocabulary",
            "message": f"Invalid currency values: {invalid}",
            "details": {"invalid": list(invalid)}}


def check_extraction_method_vocabulary(df: pd.DataFrame, source: str) -> dict:
    """Controlled vocabulary: extraction_method must be api/scraper/manual."""
    if "extraction_method" not in df.columns:
        return {"status": "FAIL", "standard": "Controlled Vocabulary (extraction_method)",
                "message": "extraction_method column missing"}
    found   = set(df["extraction_method"].dropna().str.lower().unique())
    invalid = found - VALID_EXTRACTION_METHODS
    if not invalid:
        return {"status": "PASS", "standard": "Controlled Vocabulary (extraction_method)",
                "message": f"All extraction_method values valid: {found}"}
    return {"status": "FAIL", "standard": "Controlled Vocabulary (extraction_method)",
            "message": f"Invalid extraction_method values: {invalid}"}


def check_source_vocabulary(df: pd.DataFrame, source: str) -> dict:
    """Controlled vocabulary: source must be census_ft900."""
    if "source" not in df.columns:
        return {"status": "FAIL", "standard": "Source Vocabulary",
                "message": "source column missing"}
    found   = set(df["source"].dropna().unique())
    invalid = found - VALID_SOURCES
    if not invalid:
        return {"status": "PASS", "standard": "Source Vocabulary",
                "message": f"All source values valid: {found}"}
    return {"status": "FAIL", "standard": "Source Vocabulary",
            "message": f"Invalid source values: {invalid}",
            "details": {"invalid": list(invalid)}}


def check_portal_url(df: pd.DataFrame, source: str) -> dict:
    """All portal_url values must point to the Census foreign trade portal."""
    if "portal_url" not in df.columns:
        return {"status": "FAIL", "standard": "Census Portal URL",
                "message": "portal_url column missing"}
    found   = df["portal_url"].dropna().unique()
    invalid = [u for u in found if CENSUS_PORTAL not in str(u)]
    if not invalid:
        return {"status": "PASS", "standard": "Census Portal URL",
                "message": f"All portal_urls reference Census foreign trade portal"}
    return {"status": "FAIL", "standard": "Census Portal URL",
            "message": f"{len(invalid)} non-Census portal URLs found",
            "details": {"invalid_samples": invalid[:3]}}


def check_trade_value_positive(df: pd.DataFrame, source: str) -> dict:
    """Data integrity: observed_value and trade_value must be >= 0."""
    issues = {}
    for col in ["observed_value", "trade_value"]:
        if col not in df.columns:
            continue
        numeric = pd.to_numeric(df[col], errors="coerce")
        neg     = int((numeric < 0).sum())
        if neg > 0:
            issues[col] = neg
    if not issues:
        total = int(pd.to_numeric(df.get("observed_value", pd.Series()), errors="coerce").notna().sum())
        return {"status": "PASS", "standard": "Trade Value Non-Negative",
                "message": f"All {total:,} trade values are >= 0"}
    return {"status": "FAIL", "standard": "Trade Value Non-Negative",
            "message": f"Negative trade values found: {issues}",
            "details": issues}


def check_pit_field_completeness(df: pd.DataFrame, source: str) -> dict:
    """Lekwankwa PIT Schema v4.0: required PIT fields present and non-null."""
    required = ["record_id", "published_date", "as_of_date", "revision_number"]
    missing_cols = [c for c in required if c not in df.columns]
    if missing_cols:
        return {"status": "FAIL", "standard": "Lekwankwa PIT Schema v4.0",
                "message": f"Missing PIT columns: {missing_cols}"}
    null_violations = {
        col: int(df[col].isna().sum()) for col in required if df[col].isna().sum() > 0
    }
    sup_null_pct = round(df["superseded_by"].isna().mean() * 100, 2) if "superseded_by" in df.columns else 100.0
    if not null_violations:
        return {"status": "PASS", "standard": "Lekwankwa PIT Schema v4.0",
                "message": f"All PIT fields present and populated. superseded_by: {sup_null_pct}% null (expected for initial load)",
                "details": {"superseded_by_null_pct": sup_null_pct}}
    return {"status": "FAIL", "standard": "Lekwankwa PIT Schema v4.0",
            "message": f"Null values in required PIT fields: {null_violations}",
            "details": null_violations}


def check_sdmx_monthly_granularity(df: pd.DataFrame, source: str) -> dict:
    """SDMX TIME_PERIOD: data_timestamp must have day=1 (monthly granularity)."""
    if "data_timestamp" not in df.columns:
        return {"status": "FAIL", "standard": "SDMX TIME_PERIOD Monthly",
                "message": "data_timestamp column missing"}
    dates    = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True)
    bad      = int((dates.dt.day != 1).sum())
    valid    = int(dates.notna().sum())
    if bad == 0:
        return {"status": "PASS", "standard": "SDMX TIME_PERIOD Monthly",
                "message": f"All {valid:,} records use monthly granularity (day=1). "
                           f"Range: {dates.min().strftime('%Y-%m')} to {dates.max().strftime('%Y-%m')}"}
    return {"status": "FAIL", "standard": "SDMX TIME_PERIOD Monthly",
            "message": f"{bad:,} records have non-monthly timestamps (day != 1)",
            "details": {"non_monthly_count": bad}}


# =============================================================================
# PER-SOURCE RUNNER
# =============================================================================

CHECKS = [
    check_iso_record_ids,
    check_iso_timestamps,
    check_iso_country_codes,
    check_hs_commodity_code_format,
    check_series_id_format,
    check_partner_country_code_format,
    check_trade_flow_vocabulary,
    check_unit_of_measure_vocabulary,
    check_currency_vocabulary,
    check_extraction_method_vocabulary,
    check_source_vocabulary,
    check_portal_url,
    check_trade_value_positive,
    check_pit_field_completeness,
    check_sdmx_monthly_granularity,
]


def validate_source(source: str) -> dict:
    logger.info(f"\n{'=' * 70}")
    logger.info(f"SOURCE: {source.upper()}")
    logger.info(f"{'=' * 70}")

    df = load_sample(source, max_files=60)
    if df.empty:
        logger.error(f"  No data loaded for {source}")
        return {"source": source, "status": "ERROR", "results": [],
                "checks_passed": 0, "checks_failed": 0, "checks_skipped": 0}

    logger.info(f"  Sample loaded: {len(df):,} records from {source}")

    passed = failed = skipped = 0
    results = []
    for check_fn in CHECKS:
        result  = check_fn(df, source)
        result["check"] = check_fn.__name__
        status  = result["status"]
        std     = result.get("standard", "")
        msg     = result.get("message", "")

        if status == "PASS":
            passed += 1
            logger.info(f"  [PASS] {std}")
            logger.info(f"         {msg}")
        elif status == "SKIP":
            skipped += 1
            logger.info(f"  [SKIP] {std} - {msg}")
        else:
            failed += 1
            logger.error(f"  [FAIL] {std}")
            logger.error(f"         {msg}")
        results.append(result)

    overall = "PASS" if failed == 0 else "FAIL"
    logger.info(f"\n  Summary: {passed} passed, {failed} failed, {skipped} skipped -> [{overall}]")

    return {
        "source": source,
        "status": overall,
        "sample_records": len(df),
        "checks_passed":  passed,
        "checks_failed":  failed,
        "checks_skipped": skipped,
        "results": results,
    }


def run_schema_compliance() -> bool:
    logger.info("=" * 70)
    logger.info("TRADE FLOWS — INDUSTRY STANDARD SCHEMA COMPLIANCE")
    logger.info("=" * 70)

    all_results = {src: validate_source(src) for src in SOURCES}

    total_passed  = sum(r["checks_passed"]  for r in all_results.values())
    total_failed  = sum(r["checks_failed"]  for r in all_results.values())
    total_skipped = sum(r["checks_skipped"] for r in all_results.values())
    overall       = "PASS" if total_failed == 0 else "FAIL"

    logger.info("\n" + "=" * 70)
    logger.info("OVERALL SCHEMA COMPLIANCE SUMMARY")
    logger.info("=" * 70)
    for src, r in all_results.items():
        logger.info(f"  {src:<20}: [{r['status']}] "
                    f"{r['checks_passed']} passed / {r['checks_failed']} failed / "
                    f"{r['checks_skipped']} skipped")
    logger.info(f"\n  Total  : {total_passed} passed, {total_failed} failed, {total_skipped} skipped")
    logger.info(f"  Overall: [{overall}]")

    report = {
        "product": PRODUCT,
        "validated_at": datetime.now().isoformat(),
        "standards_applied": [
            "ISO/IEC 9834-8 UUID v4",
            "ISO 8601 UTC Timestamps",
            "ISO 3166-1 Country Codes",
            "UN HS 2022 Commodity Code Format",
            "Census Series ID Format (HS{2d}_{EXP|IMP})",
            "Census Partner Country Code (FIPS 4-digit)",
            "Trade Flow Vocabulary (Export/Import)",
            "Unit of Measure Vocabulary (USD_MILLIONS)",
            "Currency Vocabulary (USD)",
            "Controlled Vocabulary (extraction_method)",
            "Source Vocabulary (census_ft900)",
            "Census Portal URL",
            "Trade Value Non-Negative",
            "Lekwankwa PIT Schema v4.0",
            "SDMX TIME_PERIOD Monthly Granularity",
        ],
        "source_results": all_results,
        "summary": {
            "overall": overall,
            "total_passed":  total_passed,
            "total_failed":  total_failed,
            "total_skipped": total_skipped,
        },
    }
    with open(REPORT_JSON, "w") as f:
        json.dump(report, f, indent=2, default=str)

    with open(REPORT_TXT, "w") as f:
        f.write("TRADE FLOWS — INDUSTRY STANDARD SCHEMA COMPLIANCE REPORT\n")
        f.write("=" * 70 + "\n\n")
        for src, sr in all_results.items():
            f.write(f"SOURCE: {src.upper()}\n")
            f.write(f"Status : [{sr['status']}]\n")
            f.write(f"Sample : {sr.get('sample_records', 0):,} records\n\n")
            for r in sr.get("results", []):
                st  = r["status"]
                std = r.get("standard", r.get("check", ""))
                msg = r.get("message", "")
                f.write(f"  [{st:<4}] {std}\n         {msg}\n")
                if r.get("details"):
                    f.write(f"         Details: {r['details']}\n")
            f.write("\n")
        f.write(f"OVERALL: [{overall}] | {total_passed} passed / "
                f"{total_failed} failed / {total_skipped} skipped\n")

    logger.info(f"\nReports saved: {REPORT_JSON}, {REPORT_TXT}")
    return total_failed == 0


if __name__ == "__main__":
    success = run_schema_compliance()
    sys.exit(0 if success else 1)
