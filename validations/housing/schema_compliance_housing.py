"""
Industry-Standard Schema Validation for Housing Supply & Shelter Inflation Data

Validates vault data against gold-standard schemas:

  DATASET 1: BLS CPI Shelter (source=bls_cpi_shelter)
  - Series ID format: C[UW][US][RS]0000[A-Z]{2,4}[0-9A-Z]*
    (e.g. CUUR0000SEHA, CUSR0000SEHB, CUUR0000SAH1)
  - source_agency: BLS  |  source_sub_category: CPI_URBAN
  - unit_of_measure: INDEX
  - portal_url: https://www.bls.gov/cpi/

  DATASET 2: Census BPS Building Permits (source=census_bps)
  - sovereign_series_id vocabulary: {PERMIT, PERMIT1, PERMIT2, PERMIT3_4, PERMIT5, BLDGS, VALUE}
  - source_agency: CENSUS  |  source_sub_category: HOUSING
  - unit_of_measure: UNITS_SAAR | COUNT | THOUSANDS_USD
  - portal_url: https://www.census.gov/construction/bps/

  ISO / COMMON STANDARDS:
  - record_id: UUID v4 format (ISO/IEC 9834-8)
  - data_timestamp: ISO 8601, UTC timezone-aware (monthly granularity)
  - country_code: ISO 3166-1 alpha-2 ('US')
  - data_vintage_id: BLS-{series}-{YYYY-MM}-v1 | CENSUS-PERMIT-USA-{YYYY-MM}-v1

  CONTROLLED VOCABULARIES:
  - source: {'bls_cpi_shelter', 'census_bps'}
  - extraction_method: {'api', 'scraper', 'manual'}
  - confidence_tier: {'PRIMARY', 'SECONDARY', 'ESTIMATED'}
  - seasonal_adjustment: {'S', 'U'}  (shelter only)

OUTPUT:
  - housing_schema_compliance_report.json
  - housing_schema_compliance_report.txt

Author: Lekwankwa Corporation
Date: 2026-06-12
"""

import re
import json
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("housing_schema_compliance.log"),
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
PRODUCT   = "Housing_Supply_and_Shelter_Inflation"
COUNTRY   = "USA"
SOURCES   = ["bls_cpi_shelter", "census_bps"]

REPORT_JSON = Path("housing_schema_compliance_report.json")
REPORT_TXT  = Path("housing_schema_compliance_report.txt")

# ── BLS CPI Shelter standards ─────────────────────────────────────────────────
# CPI series: C + U|W + U|S + R|S + 4-digit-area + item-code
SHELTER_SERIES_PATTERN = re.compile(r"^C[UW][US][RS]\d{4}S[AEH][A-Z0-9]{0,6}$")
SHELTER_PORTAL_URL     = "https://www.bls.gov/cpi/"
SHELTER_VALID_SERIES   = {
    "CUUR0000SEHA", "CUUR0000SEHB", "CUUR0000SAH1", "CUUR0000SEHC",
    "CUSR0000SEHA", "CUSR0000SEHB", "CUSR0000SAH1",
}

# ── Census BPS standards ──────────────────────────────────────────────────────
PERMITS_PORTAL_URL  = "https://www.census.gov/construction/bps/"
BPS_VALID_VARIABLES = {"PERMIT", "PERMIT1", "PERMIT2", "PERMIT3_4", "PERMIT5", "BLDGS", "VALUE"}
BPS_VALID_UNITS     = {"UNITS_SAAR", "COUNT", "THOUSANDS_USD"}

# ── Shared controlled vocabularies ───────────────────────────────────────────
VALID_SOURCES            = {"bls_cpi_shelter", "census_bps"}
VALID_EXTRACTION_METHODS = {"api", "scraper", "manual"}
VALID_CONFIDENCE_TIERS   = {"PRIMARY", "SECONDARY", "ESTIMATED"}
VALID_SEASONAL_ADJ       = {"S", "U"}
VALID_COUNTRY_CODES      = {"US", "USA"}

UUID_PATTERN = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$", re.IGNORECASE
)
VINTAGE_SHELTER_PATTERN = re.compile(r"^BLS-[A-Z0-9]+-\d{4}-\d{2}-v\d+$")
VINTAGE_PERMITS_PATTERN = re.compile(r"^CENSUS-(?:PERMIT|BPS)-[A-Z0-9_]+-\d{4}-\d{2}-v\d+$")


# =============================================================================
# LOAD DATA
# =============================================================================

def load_sample(source: str, max_files: int = 60) -> pd.DataFrame:
    source_path = f"{VAULT_DIR}/product={PRODUCT}/country={COUNTRY}/source={source}"
    all_files   = sorted(vault_glob(source_path, "*_data.parquet"))
    step        = max(1, len(all_files) // max_files)
    sampled     = all_files[::step][:max_files]
    dfs = []
    for f in sampled:
        try:
            dfs.append(vault_read_parquet(f))
        except Exception as exc:
            logger.warning(f"  Cannot read {f}: {exc}")
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


# =============================================================================
# STANDARD CHECKS
# =============================================================================

def check_uuid_record_ids(df: pd.DataFrame, source: str) -> dict:
    if "record_id" not in df.columns:
        return {"status": "FAIL", "check": "UUID record_id",
                "message": "record_id column missing"}
    valid   = df["record_id"].dropna().apply(lambda x: bool(UUID_PATTERN.match(str(x))))
    invalid = int((~valid).sum())
    total   = len(df["record_id"].dropna())
    if invalid == 0:
        return {"status": "PASS", "check": "UUID record_id",
                "message": f"All {total:,} record_ids are valid UUID v4"}
    return {"status": "FAIL", "check": "UUID record_id",
            "message": f"{invalid:,}/{total:,} record_ids fail UUID v4 format",
            "sample_invalid": df.loc[~valid, "record_id"].head(5).tolist()}


def check_iso_timestamps(df: pd.DataFrame, source: str,
                         min_year: int, max_year: int = 2027) -> dict:
    for col in ("data_timestamp", "reporting_date"):
        if col not in df.columns:
            continue
        ts = pd.to_datetime(df[col], errors="coerce", utc=True)
        invalid = int(ts.isna().sum())
        if invalid:
            return {"status": "FAIL", "check": f"ISO 8601 {col}",
                    "message": f"{invalid:,} non-parseable values in {col}"}
        years = ts.dt.year
        out   = int(((years < min_year) | (years > max_year)).sum())
        if out:
            return {"status": "WARN", "check": f"ISO 8601 {col}",
                    "message": f"{out:,} records outside expected year range {min_year}-{max_year}"}
        return {"status": "PASS", "check": f"ISO 8601 {col}",
                "message": f"All {len(df):,} {col} values are valid UTC ISO 8601 "
                           f"({years.min()}-{years.max()})"}
    return {"status": "SKIP", "check": "ISO 8601 timestamp",
            "message": "No timestamp column found"}


def check_country_code(df: pd.DataFrame) -> dict:
    col = "country_code" if "country_code" in df.columns else "iso_alpha3"
    if col not in df.columns:
        return {"status": "WARN", "check": "ISO 3166-1 country_code",
                "message": "country_code / iso_alpha3 column missing"}
    invalid = df[col].dropna().apply(lambda x: x not in VALID_COUNTRY_CODES)
    n = int(invalid.sum())
    if n == 0:
        return {"status": "PASS", "check": "ISO 3166-1 country_code",
                "message": f"All {len(df):,} records have valid country code"}
    return {"status": "FAIL", "check": "ISO 3166-1 country_code",
            "message": f"{n:,} records have invalid country codes",
            "sample": df.loc[invalid, col].value_counts().head(5).to_dict()}


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


def check_confidence_tier(df: pd.DataFrame) -> dict:
    if "confidence_tier" not in df.columns:
        return {"status": "WARN", "check": "Confidence tier vocabulary",
                "message": "confidence_tier column missing"}
    invalid = ~df["confidence_tier"].isin(VALID_CONFIDENCE_TIERS)
    n = int(invalid.sum())
    if n == 0:
        return {"status": "PASS", "check": "Confidence tier vocabulary",
                "message": f"All confidence_tier values valid: {VALID_CONFIDENCE_TIERS}"}
    return {"status": "FAIL", "check": "Confidence tier vocabulary",
            "message": f"{n:,} records have invalid confidence_tier",
            "values": df.loc[invalid, "confidence_tier"].value_counts().head(5).to_dict()}


def check_observed_value_positive(df: pd.DataFrame) -> dict:
    col = "observed_value" if "observed_value" in df.columns else "metric_value"
    if col not in df.columns:
        return {"status": "WARN", "check": "Observed value positive",
                "message": "observed_value / metric_value column missing"}
    numeric = pd.to_numeric(df[col], errors="coerce")
    zero_or_neg = int((numeric <= 0).sum())
    nulls = int(numeric.isna().sum())
    total = len(df)
    if zero_or_neg == 0:
        return {"status": "PASS", "check": "Observed value positive",
                "message": f"All {total - nulls:,} non-null values > 0 (nulls: {nulls})"}
    return {"status": "WARN", "check": "Observed value positive",
            "message": f"{zero_or_neg:,} records have zero/negative observed_value"}


# =============================================================================
# DATASET-SPECIFIC CHECKS
# =============================================================================

def check_shelter_series_ids(df: pd.DataFrame) -> dict:
    col = "sovereign_series_id"
    if col not in df.columns:
        return {"status": "FAIL", "check": "BLS CPI Shelter series ID format",
                "message": "sovereign_series_id column missing"}
    ids = df[col].dropna()
    in_catalogue  = ids.isin(SHELTER_VALID_SERIES)
    out_catalogue = int((~in_catalogue).sum())
    if out_catalogue == 0:
        return {"status": "PASS", "check": "BLS CPI Shelter series ID format",
                "message": f"All {len(ids):,} series IDs in validated catalogue",
                "series_found": ids.unique().tolist()}
    pattern_ok  = ids[~in_catalogue].apply(lambda x: bool(SHELTER_SERIES_PATTERN.match(str(x))))
    pattern_fail = int((~pattern_ok).sum())
    return {"status": "WARN" if pattern_fail == 0 else "FAIL",
            "check": "BLS CPI Shelter series ID format",
            "message": f"{out_catalogue} IDs not in catalogue "
                       f"({pattern_fail} also fail BLS pattern)",
            "unknown_ids": ids[~in_catalogue].value_counts().head(10).to_dict()}


def check_shelter_vintage_id(df: pd.DataFrame) -> dict:
    if "data_vintage_id" not in df.columns:
        return {"status": "FAIL", "check": "Shelter data_vintage_id format",
                "message": "data_vintage_id column missing"}
    ids   = df["data_vintage_id"].dropna()
    valid = ids.apply(lambda x: bool(VINTAGE_SHELTER_PATTERN.match(str(x))))
    n_fail = int((~valid).sum())
    if n_fail == 0:
        return {"status": "PASS", "check": "Shelter data_vintage_id format",
                "message": f"All {len(ids):,} data_vintage_ids match BLS-{{series}}-{{YYYY-MM}}-v1"}
    return {"status": "FAIL", "check": "Shelter data_vintage_id format",
            "message": f"{n_fail:,} data_vintage_ids fail expected BLS pattern",
            "sample": ids[~valid].head(5).tolist()}


def check_shelter_unit(df: pd.DataFrame) -> dict:
    if "unit_of_measure" not in df.columns:
        return {"status": "WARN", "check": "Shelter unit_of_measure",
                "message": "unit_of_measure column missing"}
    wrong = df[df["unit_of_measure"] != "INDEX"]
    if len(wrong) == 0:
        return {"status": "PASS", "check": "Shelter unit_of_measure",
                "message": "All shelter records have unit_of_measure=INDEX (gold standard)"}
    return {"status": "FAIL", "check": "Shelter unit_of_measure",
            "message": f"{len(wrong):,} records do not have unit_of_measure=INDEX",
            "values": wrong["unit_of_measure"].value_counts().to_dict()}


def check_shelter_seasonal_adj(df: pd.DataFrame) -> dict:
    if "seasonal_adjustment" not in df.columns:
        return {"status": "WARN", "check": "Shelter seasonal_adjustment",
                "message": "seasonal_adjustment column missing"}
    invalid = ~df["seasonal_adjustment"].isin(VALID_SEASONAL_ADJ)
    n = int(invalid.sum())
    if n == 0:
        return {"status": "PASS", "check": "Shelter seasonal_adjustment",
                "message": f"All seasonal_adjustment values in {VALID_SEASONAL_ADJ}"}
    return {"status": "FAIL", "check": "Shelter seasonal_adjustment",
            "message": f"{n:,} records have invalid seasonal_adjustment"}


def check_permits_bps_vocabulary(df: pd.DataFrame) -> dict:
    col = "sovereign_series_id"
    if col not in df.columns:
        col = "bps_variable"
    if col not in df.columns:
        return {"status": "FAIL", "check": "BPS variable vocabulary",
                "message": "sovereign_series_id / bps_variable column missing"}
    ids   = df[col].dropna()
    invalid = ~ids.isin(BPS_VALID_VARIABLES)
    n = int(invalid.sum())
    if n == 0:
        return {"status": "PASS", "check": "BPS variable vocabulary",
                "message": f"All BPS sovereign_series_ids in canonical set: {BPS_VALID_VARIABLES}",
                "variables_found": ids.unique().tolist()}
    return {"status": "FAIL", "check": "BPS variable vocabulary",
            "message": f"{n:,} records have unknown BPS variable codes",
            "unknown": ids[invalid].value_counts().head(10).to_dict()}


def check_permits_vintage_id(df: pd.DataFrame) -> dict:
    if "data_vintage_id" not in df.columns:
        return {"status": "FAIL", "check": "Permits data_vintage_id format",
                "message": "data_vintage_id column missing"}
    ids   = df["data_vintage_id"].dropna()
    valid = ids.apply(lambda x: bool(VINTAGE_PERMITS_PATTERN.match(str(x))))
    n_fail = int((~valid).sum())
    if n_fail == 0:
        return {"status": "PASS", "check": "Permits data_vintage_id format",
                "message": f"All {len(ids):,} data_vintage_ids match CENSUS-PERMIT-USA-{{YYYY-MM}}-v1"}
    return {"status": "FAIL", "check": "Permits data_vintage_id format",
            "message": f"{n_fail:,} data_vintage_ids fail expected CENSUS pattern",
            "sample": ids[~valid].head(5).tolist()}


def check_permits_unit_vocabulary(df: pd.DataFrame) -> dict:
    if "unit_of_measure" not in df.columns:
        return {"status": "WARN", "check": "BPS unit_of_measure vocabulary",
                "message": "unit_of_measure column missing"}
    invalid = ~df["unit_of_measure"].isin(BPS_VALID_UNITS)
    n = int(invalid.sum())
    if n == 0:
        return {"status": "PASS", "check": "BPS unit_of_measure vocabulary",
                "message": f"All unit_of_measure values in {BPS_VALID_UNITS}"}
    return {"status": "FAIL", "check": "BPS unit_of_measure vocabulary",
            "message": f"{n:,} records have invalid unit_of_measure",
            "values": df.loc[invalid, "unit_of_measure"].value_counts().to_dict()}


def check_source_agency_alignment(df: pd.DataFrame, source: str) -> dict:
    """source_agency and source_sub_category must match the dataset."""
    expected = {
        "bls_cpi_shelter": ("BLS",    "CPI_URBAN"),
        "census_bps":      ("CENSUS", "HOUSING"),
    }.get(source)
    if not expected:
        return {"status": "SKIP", "check": "Source agency alignment",
                "message": f"No expectation defined for {source}"}
    agency_col  = "source_agency"
    sub_col     = "source_sub_category"
    issues = []
    if agency_col in df.columns:
        wrong_agency = int((df[agency_col] != expected[0]).sum())
        if wrong_agency:
            issues.append(f"{wrong_agency} records have source_agency != {expected[0]}")
    if sub_col in df.columns:
        wrong_sub = int((df[sub_col] != expected[1]).sum())
        if wrong_sub:
            issues.append(f"{wrong_sub} records have source_sub_category != {expected[1]}")
    if issues:
        return {"status": "FAIL", "check": "Source agency alignment",
                "message": "; ".join(issues)}
    return {"status": "PASS", "check": "Source agency alignment",
            "message": f"source_agency={expected[0]}, source_sub_category={expected[1]} consistent"}


# =============================================================================
# RUNNER
# =============================================================================

def run_source(source: str) -> list:
    logger.info(f"\n{'-' * 60}")
    logger.info(f"  SOURCE: {source}")
    logger.info(f"{'-' * 60}")
    df = load_sample(source)
    if df.empty:
        logger.warning(f"  No data found for source={source}")
        return [{"status": "SKIP", "check": "All checks", "source": source,
                 "message": f"No vault data for source={source}"}]

    logger.info(f"  Sample loaded: {len(df):,} records")

    common = [
        check_uuid_record_ids(df, source),
        check_country_code(df),
        check_extraction_method(df),
        check_confidence_tier(df),
        check_observed_value_positive(df),
        check_source_agency_alignment(df, source),
    ]

    if source == "bls_cpi_shelter":
        min_year = 1914
        specific = [
            check_iso_timestamps(df, source, min_year),
            check_shelter_series_ids(df),
            check_shelter_vintage_id(df),
            check_shelter_unit(df),
            check_shelter_seasonal_adj(df),
        ]
    else:  # census_bps
        min_year = 1959
        specific = [
            check_iso_timestamps(df, source, min_year),
            check_permits_bps_vocabulary(df),
            check_permits_vintage_id(df),
            check_permits_unit_vocabulary(df),
        ]

    results = common + specific
    for r in results:
        r["source"] = source
        icon = {"PASS": "[+]", "FAIL": "[!]", "WARN": "[!]", "SKIP": "[-]"}.get(r["status"], "[?]")
        logger.info(f"  [{icon}] {r['check']}: {r['message']}")
    return results


def run():
    logger.info("=" * 70)
    logger.info("HOUSING — SCHEMA COMPLIANCE VALIDATION")
    logger.info("=" * 70)
    logger.info(f"Run timestamp: {datetime.utcnow().isoformat()}Z")

    all_results = {}
    for src in SOURCES:
        all_results[src] = run_source(src)

    # ── Summary ──────────────────────────────────────────────────────────────
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
        "run_timestamp": datetime.utcnow().isoformat() + "Z",
        "summary": {"total": total, "passed": passed, "failed": failed, "warned": warned},
        "results_by_source": all_results,
    }

    with open(REPORT_JSON, "w") as fh:
        json.dump(report, fh, indent=2, default=str)
    with open(REPORT_TXT, "w") as fh:
        fh.write(f"Housing Schema Compliance Report — {datetime.utcnow().isoformat()}Z\n")
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
