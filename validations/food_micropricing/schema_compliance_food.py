"""
Industry-Standard Schema Validation for Food Micropricing Data

Validates vault data against the gold-standard schema:
  food_pricing.json — BLS APU food retail prices (avg price per unit, USD)

DATASET: BLS Average Retail Food Prices (source=bls)
  - Series ID format: APU0000XXXXXX  (BLS Average Price series)
  - source_agency:     BLS
  - source_sub_category: CPI
  - extraction_method: api
  - portal_url: https://www.bls.gov/cpi/data.htm

  ISO / COMMON STANDARDS:
  - record_id:        UUID v4 format (ISO/IEC 9834-8)
  - data_timestamp:   ISO 8601, UTC timezone-aware (monthly granularity)
  - country_code:     ISO 3166-1 alpha-2 ('US')
  - published_date:   ISO 8601, UTC
  - as_of_date:       ISO 8601, UTC
  - item_code:        COICOP-style code (e.g. 01.1.6.3)

  CONTROLLED VOCABULARIES:
  - source:              {'bls', 'usda'}
  - extraction_method:   {'api', 'scraper', 'manual'}
  - currency:            {'USD'}
  - country_code:        {'US'}
  - category:            8 COICOP-aligned categories
  - data_quality_certified: {True, False}

OUTPUT:
  - food_pricing_schema_compliance_report.json
  - food_pricing_schema_compliance_report.txt

Author: Lekwankwa Corporation
Date: 2026-06-13
"""

import re
import json
import logging
import pandas as pd
from pathlib import Path
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("food_pricing_schema_compliance.log"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

VAULT_DIR = Path("lekwankwa-historical-vault")
PRODUCT   = "food_micropricing"
COUNTRY   = "USA"
SOURCES   = ["bls"]

REPORT_JSON = Path("food_pricing_schema_compliance_report.json")
REPORT_TXT  = Path("food_pricing_schema_compliance_report.txt")

# ── BLS APU series standards ──────────────────────────────────────────────────
# Average Price series: APU + 4-digit area (0000 = national) + 6-char item code
BLS_SERIES_PATTERN = re.compile(r"^APU\d{4}[0-9A-Z]{4,8}$")
BLS_PORTAL_URL     = "https://www.bls.gov/cpi/data.htm"

# ── COICOP item code (dot-separated 4-level hierarchy) ───────────────────────
COICOP_PATTERN = re.compile(r"^\d{2}\.\d{1,2}\.\d{1,2}\.\d{1,2}$")

# ── Controlled vocabularies ───────────────────────────────────────────────────
VALID_SOURCES            = {"bls", "usda"}
VALID_EXTRACTION_METHODS = {"api", "scraper", "manual"}
VALID_CURRENCIES         = {"USD"}
VALID_COUNTRY_CODES      = {"US"}
VALID_CATEGORIES = {
    "Fruits", "Meat & Poultry", "Cereals & Grains", "Fish & Seafood",
    "Beverages", "Dairy & Eggs", "Vegetables", "Sugar & Spices",
}
REQUIRED_COLUMNS = [
    "country_code", "item_name", "item_description", "item_code",
    "category", "item_value", "unit", "currency", "usd_equivalent",
    "data_quality_certified", "data_timestamp", "conversion_timestamp",
    "source", "source_series_id", "extraction_method", "source_url",
    "record_id", "published_date", "as_of_date", "revision_number",
]

UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$",
    re.IGNORECASE,
)


# =============================================================================
# DATA LOADING
# =============================================================================

def load_sample(source: str, max_files: int = 80) -> pd.DataFrame:
    source_path = VAULT_DIR / f"product={PRODUCT}" / f"country={COUNTRY}" / f"source={source}"
    if not source_path.exists():
        logger.warning(f"Vault path not found: {source_path}")
        return pd.DataFrame()
    all_files = sorted(
        f for f in source_path.rglob("*.parquet")
        if "outliers" not in f.name and "changelog" not in f.name
    )
    step    = max(1, len(all_files) // max_files)
    sampled = all_files[::step][:max_files]
    dfs = []
    for f in sampled:
        try:
            dfs.append(pd.read_parquet(f))
        except Exception as exc:
            logger.warning(f"  Cannot read {f}: {exc}")
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


# =============================================================================
# CHECKS
# =============================================================================

def check_required_columns(df: pd.DataFrame) -> dict:
    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if not missing:
        return {"status": "PASS", "check": "Required columns present",
                "message": f"All {len(REQUIRED_COLUMNS)} required columns present"}
    return {"status": "FAIL", "check": "Required columns present",
            "message": f"{len(missing)} columns missing: {missing}"}


def check_uuid_record_ids(df: pd.DataFrame) -> dict:
    if "record_id" not in df.columns:
        return {"status": "FAIL", "check": "UUID record_id",
                "message": "record_id column missing"}
    ids     = df["record_id"].dropna().astype(str)
    valid   = ids.apply(lambda x: bool(UUID_PATTERN.match(x)))
    n_fail  = int((~valid).sum())
    if n_fail == 0:
        return {"status": "PASS", "check": "UUID record_id",
                "message": f"All {len(ids):,} record_ids are valid UUID v4"}
    return {"status": "FAIL", "check": "UUID record_id",
            "message": f"{n_fail:,}/{len(ids):,} record_ids fail UUID v4 format",
            "sample_invalid": ids[~valid].head(5).tolist()}


def check_iso_timestamps(df: pd.DataFrame) -> dict:
    for col in ("data_timestamp", "published_date", "as_of_date", "conversion_timestamp"):
        if col not in df.columns:
            continue
        ts      = pd.to_datetime(df[col], errors="coerce", utc=True)
        n_bad   = int(ts.isna().sum())
        if n_bad > 0:
            return {"status": "FAIL", "check": f"ISO 8601 timestamps",
                    "message": f"{n_bad:,} non-parseable values in {col}"}
        years = ts.dt.year
        out   = int(((years < 1970) | (years > 2030)).sum())
        if out:
            return {"status": "WARN", "check": "ISO 8601 timestamps",
                    "message": f"{out:,} {col} values outside expected 1970-2030 range"}
    ts = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True)
    return {"status": "PASS", "check": "ISO 8601 timestamps",
            "message": f"All timestamp columns valid UTC ISO 8601 "
                       f"({ts.dt.year.min()}-{ts.dt.year.max()})"}


def check_country_code(df: pd.DataFrame) -> dict:
    if "country_code" not in df.columns:
        return {"status": "FAIL", "check": "ISO 3166-1 country_code",
                "message": "country_code column missing"}
    invalid = ~df["country_code"].isin(VALID_COUNTRY_CODES)
    n = int(invalid.sum())
    if n == 0:
        return {"status": "PASS", "check": "ISO 3166-1 country_code",
                "message": f"All {len(df):,} records have valid country_code (US)"}
    return {"status": "FAIL", "check": "ISO 3166-1 country_code",
            "message": f"{n:,} records have invalid country_code",
            "values": df.loc[invalid, "country_code"].value_counts().head(5).to_dict()}


def check_source_vocabulary(df: pd.DataFrame) -> dict:
    if "source" not in df.columns:
        return {"status": "FAIL", "check": "Source vocabulary",
                "message": "source column missing"}
    invalid = ~df["source"].isin(VALID_SOURCES)
    n = int(invalid.sum())
    if n == 0:
        return {"status": "PASS", "check": "Source vocabulary",
                "message": f"All source values in valid set {VALID_SOURCES}"}
    return {"status": "FAIL", "check": "Source vocabulary",
            "message": f"{n:,} records have invalid source",
            "values": df.loc[invalid, "source"].value_counts().head(5).to_dict()}


def check_extraction_method(df: pd.DataFrame) -> dict:
    if "extraction_method" not in df.columns:
        return {"status": "WARN", "check": "Extraction method vocabulary",
                "message": "extraction_method column missing"}
    invalid = ~df["extraction_method"].isin(VALID_EXTRACTION_METHODS)
    n = int(invalid.sum())
    if n == 0:
        return {"status": "PASS", "check": "Extraction method vocabulary",
                "message": f"All extraction_method values in {VALID_EXTRACTION_METHODS}"}
    return {"status": "FAIL", "check": "Extraction method vocabulary",
            "message": f"{n:,} records have invalid extraction_method",
            "values": df.loc[invalid, "extraction_method"].value_counts().head(5).to_dict()}


def check_currency(df: pd.DataFrame) -> dict:
    if "currency" not in df.columns:
        return {"status": "WARN", "check": "Currency vocabulary",
                "message": "currency column missing"}
    invalid = ~df["currency"].isin(VALID_CURRENCIES)
    n = int(invalid.sum())
    if n == 0:
        return {"status": "PASS", "check": "Currency vocabulary",
                "message": "All records have currency=USD (gold standard)"}
    return {"status": "FAIL", "check": "Currency vocabulary",
            "message": f"{n:,} records have non-USD currency",
            "values": df.loc[invalid, "currency"].value_counts().head(5).to_dict()}


def check_category_vocabulary(df: pd.DataFrame) -> dict:
    if "category" not in df.columns:
        return {"status": "FAIL", "check": "Category vocabulary",
                "message": "category column missing"}
    invalid = ~df["category"].isin(VALID_CATEGORIES)
    n = int(invalid.sum())
    if n == 0:
        return {"status": "PASS", "check": "Category vocabulary",
                "message": f"All categories in {len(VALID_CATEGORIES)} valid COICOP categories"}
    return {"status": "FAIL", "check": "Category vocabulary",
            "message": f"{n:,} records have unrecognised category values",
            "values": df.loc[invalid, "category"].value_counts().head(10).to_dict()}


def check_coicop_item_codes(df: pd.DataFrame) -> dict:
    if "item_code" not in df.columns:
        return {"status": "FAIL", "check": "COICOP item_code format",
                "message": "item_code column missing"}
    codes   = df["item_code"].dropna().astype(str)
    valid   = codes.apply(lambda x: bool(COICOP_PATTERN.match(x)))
    n_fail  = int((~valid).sum())
    if n_fail == 0:
        return {"status": "PASS", "check": "COICOP item_code format",
                "message": f"All {len(codes):,} item_codes match XX.X.X.X pattern",
                "unique_codes": codes.nunique()}
    return {"status": "FAIL", "check": "COICOP item_code format",
            "message": f"{n_fail:,} item_codes do not match COICOP pattern",
            "sample": codes[~valid].head(5).tolist()}


def check_bls_series_ids(df: pd.DataFrame) -> dict:
    if "source_series_id" not in df.columns:
        return {"status": "FAIL", "check": "BLS APU series ID format",
                "message": "source_series_id column missing"}
    bls_df = df[df["source"] == "bls"] if "source" in df.columns else df
    ids    = bls_df["source_series_id"].dropna().astype(str)
    if len(ids) == 0:
        return {"status": "SKIP", "check": "BLS APU series ID format",
                "message": "No BLS records in sample"}
    valid  = ids.apply(lambda x: bool(BLS_SERIES_PATTERN.match(x)))
    n_fail = int((~valid).sum())
    if n_fail == 0:
        return {"status": "PASS", "check": "BLS APU series ID format",
                "message": f"All {len(ids):,} BLS source_series_ids match APU pattern",
                "unique_series": ids.nunique()}
    return {"status": "FAIL", "check": "BLS APU series ID format",
            "message": f"{n_fail:,} BLS series IDs fail APU pattern",
            "sample": ids[~valid].head(5).tolist()}


def check_portal_url(df: pd.DataFrame) -> dict:
    if "source_url" not in df.columns:
        return {"status": "WARN", "check": "BLS portal URL",
                "message": "source_url column missing"}
    bls_df = df[df["source"] == "bls"] if "source" in df.columns else df
    wrong  = bls_df[bls_df["source_url"] != BLS_PORTAL_URL]
    if len(wrong) == 0:
        return {"status": "PASS", "check": "BLS portal URL",
                "message": f"All BLS records have source_url={BLS_PORTAL_URL!r}"}
    return {"status": "WARN", "check": "BLS portal URL",
            "message": f"{len(wrong):,} BLS records have non-standard source_url",
            "values": wrong["source_url"].value_counts().head(5).to_dict()}


def check_item_value_positive(df: pd.DataFrame) -> dict:
    for col in ("item_value", "usd_equivalent"):
        if col not in df.columns:
            continue
        numeric    = pd.to_numeric(df[col], errors="coerce")
        zero_neg   = int((numeric <= 0).sum())
        nulls      = int(numeric.isna().sum())
        if zero_neg > 0:
            return {"status": "WARN", "check": "Item value positive",
                    "message": f"{zero_neg:,} records have {col} <= 0 (nulls: {nulls})"}
    return {"status": "PASS", "check": "Item value positive",
            "message": "All item_value and usd_equivalent values > 0"}


def check_pit_ordering(df: pd.DataFrame) -> dict:
    """published_date should be >= data_timestamp (release after observation)."""
    for ts_col in ("data_timestamp",):
        if ts_col not in df.columns or "published_date" not in df.columns:
            return {"status": "SKIP", "check": "PIT ordering (published >= observed)",
                    "message": "Missing data_timestamp or published_date"}
    dt   = pd.to_datetime(df["data_timestamp"],  errors="coerce", utc=True)
    pub  = pd.to_datetime(df["published_date"],  errors="coerce", utc=True)
    both = dt.notna() & pub.notna()
    bad  = int((pub[both] < dt[both]).sum())
    if bad == 0:
        return {"status": "PASS", "check": "PIT ordering (published >= observed)",
                "message": f"All {both.sum():,} records have published_date >= data_timestamp"}
    return {"status": "FAIL", "check": "PIT ordering (published >= observed)",
            "message": f"{bad:,} records have published_date < data_timestamp"}


# =============================================================================
# RUNNER
# =============================================================================

def run_source(source: str) -> list:
    logger.info(f"\n{'-' * 60}")
    logger.info(f"  SOURCE: {source}")
    logger.info("-" * 60)
    df = load_sample(source)
    if df.empty:
        logger.warning(f"  No data found for source={source}")
        return [{"status": "SKIP", "check": "All checks", "source": source,
                 "message": f"No vault data for source={source}"}]

    logger.info(f"  Sample loaded: {len(df):,} records, {df.columns.nunique()} columns")

    results = [
        check_required_columns(df),
        check_uuid_record_ids(df),
        check_iso_timestamps(df),
        check_country_code(df),
        check_source_vocabulary(df),
        check_extraction_method(df),
        check_currency(df),
        check_category_vocabulary(df),
        check_coicop_item_codes(df),
        check_bls_series_ids(df),
        check_portal_url(df),
        check_item_value_positive(df),
        check_pit_ordering(df),
    ]

    for r in results:
        r["source"] = source
        icon = {"PASS": "[PASS]", "FAIL": "[FAIL]", "WARN": "[WARN]", "SKIP": "[SKIP]"}.get(r["status"], "[?]")
        logger.info(f"  {icon} {r['check']}: {r['message']}")

    return results


def run():
    logger.info("=" * 70)
    logger.info("FOOD MICROPRICING - SCHEMA COMPLIANCE VALIDATION")
    logger.info("=" * 70)
    logger.info(f"Run timestamp: {datetime.utcnow().isoformat()}Z")
    logger.info(f"Gold standard: schema gold standards/food_pricing.json")

    all_results = {}
    for src in SOURCES:
        all_results[src] = run_source(src)

    total  = sum(len(v) for v in all_results.values())
    passed = sum(r["status"] == "PASS" for v in all_results.values() for r in v)
    failed = sum(r["status"] == "FAIL" for v in all_results.values() for r in v)
    warned = sum(r["status"] == "WARN" for v in all_results.values() for r in v)

    logger.info("\n" + "=" * 70)
    logger.info(f"SUMMARY: {passed} PASS / {failed} FAIL / {warned} WARN / {total} total")
    logger.info("=" * 70)

    report = {
        "product": PRODUCT,
        "country": COUNTRY,
        "gold_standard": "schema gold standards/food_pricing.json",
        "run_timestamp": datetime.utcnow().isoformat() + "Z",
        "summary": {"total": total, "passed": passed, "failed": failed, "warned": warned},
        "results_by_source": all_results,
    }

    with open(REPORT_JSON, "w") as fh:
        json.dump(report, fh, indent=2, default=str)

    with open(REPORT_TXT, "w") as fh:
        fh.write(f"Food Pricing Schema Compliance Report - {datetime.utcnow().isoformat()}Z\n")
        fh.write("=" * 70 + "\n")
        for src, results in all_results.items():
            fh.write(f"\nSOURCE: {src}\n")
            for r in results:
                icon = {"PASS": "PASS", "FAIL": "FAIL", "WARN": "WARN", "SKIP": "SKIP"}.get(r["status"], "?")
                fh.write(f"  [{icon}] {r['check']}: {r['message']}\n")
        fh.write(f"\nSUMMARY: {passed} PASS / {failed} FAIL / {warned} WARN\n")

    logger.info(f"Reports written: {REPORT_JSON}, {REPORT_TXT}")
    return failed == 0


if __name__ == "__main__":
    import sys
    sys.exit(0 if run() else 1)
