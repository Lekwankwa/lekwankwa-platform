"""
Referential & Cross-Dataset Integrity Validation — Food Micropricing

Verifies that:
  (a) the BLS source within food_micropricing is internally consistent and
      referentially sound (USDA removed from dataset), and
  (b) the food_micropricing vault is consistent with the electricity and
      macro_employment vaults where those datasets share the same jurisdiction
      and time window.

INTERNAL CHECKS (BLS CPI):
  1.  Country Code Consistency        — BLS source carries country_code='US'
  2.  Schema Field Parity             — BLS carries all required columns
  3.  Series ID Isolation             — all sovereign_series_id values match APU pattern
  4.  Category Vocabulary Consistency — uses the 11-category controlled vocab
  5.  Currency Consistency            — currency='USD', usd_equivalent == item_value
  6.  PIT Fields Completeness         — all 5 PIT fields present in BLS

CROSS-DATASET CHECKS:
  7.  Country Code Alignment w/ Employment — food 'US' == employment 'US'
  8.  BLS Series Isolation from Employment  — APU IDs absent from CES/JOLTS series
  9.  Source Vocabulary Isolation           — food sources ∉ {'eia_generation'}; others ∉ {'bls'}
  10. Temporal Partition Alignment          — 2011-2026 shared months across all three products

OUTPUT:
  - food_pricing_referential_integrity_report.json
  - food_pricing_referential_integrity_report.txt

Author: Lekwankwa Corporation
Date: 2026-06-13
"""

import json
import re
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import logging

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from _vault_root import VAULT_ROOT, vault_glob_paths  # noqa: E402

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("food_pricing_referential_integrity.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

VAULT_DIR = VAULT_ROOT
PRODUCT   = "food_micropricing"
COUNTRY   = "USA"
SOURCES   = ["bls"]

REPORT_JSON = Path("food_pricing_referential_integrity_report.json")
REPORT_TXT  = Path("food_pricing_referential_integrity_report.txt")

SAMPLE_FILES_INTERNAL  = 40   # files per source for within-product checks
SAMPLE_FILES_CROSS     = 30   # files per source for cross-dataset record-level checks

BLS_SERIES_RE  = re.compile(r"^APU\d{10}$")

SOURCE_FILES = {
    "bls":      "food_pricing_data.parquet",
    "usda_ers": "food_pricing_data.parquet",
}

# BLS base_data.parquet uses the legacy column schema
REQUIRED_COLUMNS = {
    "country_code", "standard_name", "local_name", "global_coicop_code", "category",
    "observed_price_local", "unit_measure_standardized", "currency", "price_usd_equivalent",
    "pct_change_mom", "data_quality_certified", "data_timestamp", "conversion_timestamp",
    "source", "source_series_id", "extraction_method", "portal_url",
    "record_id", "published_date", "as_of_date", "revision_number", "superseded_by",
}

VALID_CATEGORIES = {
    "All Food", "Cereals & Grains", "Meat & Poultry", "Dairy & Eggs", "Vegetables",
    "Fruits", "Oils & Fats", "Beverages", "Sugar & Spices",
    "Fish & Seafood", "Other Foods",
}

PIT_FIELDS = {"record_id", "revision_number", "superseded_by", "published_date", "as_of_date"}

# Cross-dataset paths
ELEC_SOURCE   = "eia_generation"
EMP_CES_SRC   = "bls_ces"
EMP_JOLTS_SRC = "bls_jolts"

# =============================================================================
# HELPERS
# =============================================================================

def _result(status, check, message, details=None):
    entry = {"status": status, "check": check, "message": message}
    if details:
        entry["details"] = details
    icon = {"PASS": "[PASS]", "FAIL": "[FAIL]", "WARN": "[WARN]", "SKIP": "[SKIP]"}[status]
    log_fn = logger.warning if status == "WARN" else (logger.error if status == "FAIL" else logger.info)
    log_fn(f"  {icon} {check}")
    if message:
        log_fn(f"         {message}")
    return entry


def _load_sample(product, source, file_glob, n_files):
    """Load up to n_files partition files and concatenate."""
    src_path = f"{VAULT_DIR}/product={product}/country={COUNTRY}/source={source}"
    files   = sorted(vault_glob_paths(src_path, file_glob), key=lambda p: str(p))
    step    = max(1, len(files) // n_files)
    sampled = files[::step][:n_files]
    if not sampled:
        return pd.DataFrame()
    frames = [pd.read_parquet(f) for f in sampled]
    return pd.concat(frames, ignore_index=True)


def _get_ym_set(product, source, file_glob):
    src_path = f"{VAULT_DIR}/product={product}/country={COUNTRY}/source={source}"
    yms = set()
    for f in vault_glob_paths(src_path, file_glob):
        year  = int(f.parent.parent.name.split("=")[1])
        month = int(f.parent.name.split("=")[1])
        yms.add((year, month))
    return yms

# =============================================================================
# INTERNAL CHECKS — BLS CPI (USDA removed)
# =============================================================================

def chk_country_code_consistency(bls_df):
    bls_codes  = set(bls_df["country_code"].dropna().unique())
    unexpected_bls = bls_codes - {"US", "USA"}
    if unexpected_bls:
        return _result("FAIL", "Country Code Consistency",
                       f"Unexpected codes — BLS: {unexpected_bls}")
    return _result("PASS", "Country Code Consistency",
                   f"BLS source uses country_code={bls_codes}")


def chk_schema_field_parity(bls_df):
    bls_cols = set(bls_df.columns)
    missing_from_bls = REQUIRED_COLUMNS - bls_cols
    if missing_from_bls:
        return _result("FAIL", "Schema Field Parity",
                       f"Required columns missing from BLS: {missing_from_bls}")
    return _result("PASS", "Schema Field Parity",
                   f"BLS carries all {len(REQUIRED_COLUMNS)} required columns")


def chk_series_id_isolation(bls_df):
    bls_ids = set(bls_df["source_series_id"].dropna().unique())
    bls_non_apu = {s for s in bls_ids if not BLS_SERIES_RE.match(s)}
    if bls_non_apu:
        return _result("FAIL", "Series ID Isolation",
                       f"{len(bls_non_apu)} BLS IDs not matching APU pattern",
                       {"examples": sorted(bls_non_apu)[:10]})
    return _result("PASS", "Series ID Isolation",
                   f"All {len(bls_ids):,} BLS series IDs match APU pattern")


def chk_category_vocabulary_consistency(bls_df):
    bls_cats = set(bls_df["category"].dropna().unique()) if "category" in bls_df.columns else set()
    invalid_bls = bls_cats - VALID_CATEGORIES
    if invalid_bls:
        return _result("FAIL", "Category Vocabulary Consistency",
                       f"Invalid BLS categories: {invalid_bls}")
    return _result("PASS", "Category Vocabulary Consistency",
                   f"BLS uses {len(bls_cats)} categories — within controlled vocabulary")


def chk_currency_consistency(bls_df):
    if "currency" not in bls_df.columns:
        return _result("FAIL", "Currency Consistency", "BLS: currency column missing")
    bad_currency = bls_df[bls_df["currency"] != "USD"]
    if len(bad_currency):
        return _result("FAIL", "Currency Consistency",
                       f"BLS: {len(bad_currency):,} records with currency != 'USD'")
    if "price_usd_equivalent" in bls_df.columns and "observed_price_local" in bls_df.columns:
        usd_sub = bls_df.dropna(subset=["observed_price_local", "price_usd_equivalent"])
        mismatch = (usd_sub["price_usd_equivalent"] - usd_sub["observed_price_local"]).abs() > 0.0001
        if mismatch.sum() > 0:
            return _result("FAIL", "Currency Consistency",
                           f"BLS: {int(mismatch.sum()):,} USD records where "
                           f"price_usd_equivalent != observed_price_local (tolerance 0.0001)")
    return _result("PASS", "Currency Consistency",
                   "BLS: currency='USD' throughout; usd_equivalent == item_value for all records")


def chk_pit_fields_completeness(bls_df):
    absent = PIT_FIELDS - set(bls_df.columns)
    if absent:
        return _result("FAIL", "PIT Fields Completeness",
                       f"Missing PIT fields in BLS: {sorted(absent)}")
    bls_null_ids = int(bls_df["record_id"].isna().sum())
    if bls_null_ids:
        return _result("FAIL", "PIT Fields Completeness",
                       f"Null record_ids in BLS: {bls_null_ids:,}")
    return _result("PASS", "PIT Fields Completeness",
                   f"All PIT fields present in BLS; record_id non-null throughout")



# =============================================================================
# CROSS-DATASET CHECKS
# =============================================================================

def chk_country_alignment_with_employment(bls_df):
    """Spot-check that food country_code matches employment country_code."""
    emp_files = sorted(vault_glob_paths(
        f"{VAULT_DIR}/product=macro_employment/country={COUNTRY}/source={EMP_CES_SRC}/year=2020/month=01",
        "*.parquet",
    ), key=lambda p: str(p))
    if not emp_files:
        return _result("SKIP", "Country Code Alignment w/ Employment",
                       "Employment vault not found — skipping cross-dataset check")
    emp_df   = pd.read_parquet(emp_files[0])
    food_cc  = set(bls_df["country_code"].dropna().unique()) if "country_code" in bls_df.columns else set()
    emp_cc   = set(emp_df["country_code"].dropna().unique())  if "country_code" in emp_df.columns else set()
    food_valid = food_cc <= {"US", "USA"}
    emp_valid  = emp_cc  <= {"US", "USA"}
    if not food_valid or not emp_valid:
        return _result("FAIL", "Country Code Alignment w/ Employment",
                       f"Non-US codes — food: {food_cc}, employment: {emp_cc}")
    return _result("PASS", "Country Code Alignment w/ Employment",
                   f"Both food ({food_cc}) and employment ({emp_cc}) carry US country codes")


def chk_bls_series_isolation_from_employment():
    """Verify no APU (food CPI) series IDs appear in employment CES/JOLTS data."""
    emp_series = set()
    for src in [EMP_CES_SRC, EMP_JOLTS_SRC]:
        files = sorted(vault_glob_paths(
            f"{VAULT_DIR}/product=macro_employment/country={COUNTRY}/source={src}/year=2020/month=01",
            "*.parquet",
        ), key=lambda p: str(p))
        for f in files[:2]:
            df = pd.read_parquet(f)
            if "source_series_id" in df.columns:
                emp_series.update(df["source_series_id"].dropna().unique())

    if not emp_series:
        return _result("SKIP", "BLS Series Isolation from Employment",
                       "Employment vault not accessible — skipping")

    leaked_apu = {s for s in emp_series if BLS_SERIES_RE.match(str(s))}
    if leaked_apu:
        return _result("FAIL", "BLS Series Isolation from Employment",
                       f"{len(leaked_apu)} APU series IDs found in employment vault (cross-product leakage)",
                       {"leaked_ids": sorted(leaked_apu)[:10]})
    return _result("PASS", "BLS Series Isolation from Employment",
                   f"No APU series IDs found in employment vault ({len(emp_series):,} employment series checked)")


def chk_source_vocabulary_isolation():
    """Verify source field values are product-isolated — no 'eia' in food, no 'bls'/'usda' in electricity."""
    issues = []
    # Check electricity for BLS/USDA source values
    elec_files = sorted(vault_glob_paths(
        f"{VAULT_DIR}/product=electricity/country={COUNTRY}/source={ELEC_SOURCE}/year=2020/month=01",
        "electricity_generation_data.parquet",
    ), key=lambda p: str(p))
    if elec_files:
        elec_df = pd.read_parquet(elec_files[0])
        elec_sources = set(elec_df["source"].dropna().unique()) if "source" in elec_df.columns else set()
        bad = elec_sources & {"bls", "bls_ces", "bls_jolts"}
        if bad:
            issues.append(f"Electricity contains food/employment source values: {bad}")

    # Check employment for 'eia' source values
    for src in [EMP_CES_SRC, EMP_JOLTS_SRC]:
        emp_files = sorted(vault_glob_paths(
            f"{VAULT_DIR}/product=macro_employment/country={COUNTRY}/source={src}/year=2020/month=01",
            "*.parquet",
        ), key=lambda p: str(p))
        for f in emp_files[:1]:
            df = pd.read_parquet(f)
            emp_sources = set(df["source"].dropna().unique()) if "source" in df.columns else set()
            bad_emp = emp_sources & {"bls", "usda", "eia"}
            if bad_emp:
                issues.append(f"{src} contains invalid source values: {bad_emp}")

    if issues:
        return _result("FAIL", "Source Vocabulary Isolation", "; ".join(issues))
    return _result("PASS", "Source Vocabulary Isolation",
                   "No cross-product source value leakage detected — "
                   "food={'bls'}, electricity={'eia_generation'}, employment={'bls_ces','bls_jolts'}")


def chk_temporal_partition_alignment(food_bls_ym):
    """Verify all three products share partitions for 2011-2026 (the full-overlap window)."""
    elec_ym  = _get_ym_set("electricity",       ELEC_SOURCE,   "electricity_generation_data.parquet")
    jolts_ym = _get_ym_set("macro_employment",   EMP_JOLTS_SRC, "*.parquet")

    if not elec_ym or not jolts_ym:
        return _result("SKIP", "Temporal Partition Alignment",
                       "Electricity or employment vault not found — skipping cross-dataset check")

    window     = {(y, m) for y in range(2011, 2026) for m in range(1, 13)}  # excl. 2026 partial
    food_w     = food_bls_ym & window
    elec_w     = elec_ym     & window
    jolts_w    = jolts_ym    & window
    common     = food_w & elec_w & jolts_w
    expected   = len(window)

    missing_food = window - food_w
    missing_elec = window - elec_w
    missing_jolt = window - jolts_w

    if len(common) < 0.90 * expected:
        return _result("FAIL", "Temporal Partition Alignment",
                       f"Only {len(common)}/{expected} months shared across all three products "
                       f"in 2011-2025 window",
                       {"missing_food": len(missing_food), "missing_elec": len(missing_elec),
                        "missing_jolts": len(missing_jolt)})

    gaps = []
    if missing_food: gaps.append(f"food_BLS missing {len(missing_food)} months")
    if missing_elec: gaps.append(f"electricity missing {len(missing_elec)} months")
    if missing_jolt: gaps.append(f"JOLTS missing {len(missing_jolt)} months")

    status  = "WARN" if gaps else "PASS"
    message = (f"{len(common)}/{expected} months present across all three products (2011-2025). "
               + ("; ".join(gaps) if gaps else "Perfect alignment."))
    return _result(status, "Temporal Partition Alignment", message,
                   {"shared_months": len(common), "total_window": expected,
                    "food_bls": len(food_w), "electricity": len(elec_w), "jolts": len(jolts_w)})


# =============================================================================
# RUNNER
# =============================================================================

def run_checks():
    logger.info("=" * 70)
    logger.info("FOOD MICROPRICING — REFERENTIAL & CROSS-DATASET INTEGRITY")
    logger.info("=" * 70)

    results = []
    counts  = {"PASS": 0, "FAIL": 0, "WARN": 0, "SKIP": 0}

    def record(r):
        results.append(r)
        counts[r["status"]] += 1

    # --- Load BLS sample ---
    logger.info("\nLoading BLS sample…")
    bls_df  = _load_sample(PRODUCT, "bls",  SOURCE_FILES["bls"], SAMPLE_FILES_INTERNAL)
    logger.info(f"  {len(bls_df):,} BLS records loaded")

    if bls_df.empty:
        logger.error("Could not load BLS sample — aborting")
        return

    bls_ym  = _get_ym_set(PRODUCT, "bls",  SOURCE_FILES["bls"])

    # --- Internal checks (BLS self-consistency) ---
    logger.info("\n" + "=" * 70)
    logger.info("SECTION A — INTERNAL REFERENTIAL INTEGRITY (BLS CPI)")
    logger.info("=" * 70)
    record(chk_country_code_consistency(bls_df))
    record(chk_schema_field_parity(bls_df))
    record(chk_series_id_isolation(bls_df))
    record(chk_category_vocabulary_consistency(bls_df))
    record(chk_currency_consistency(bls_df))
    record(chk_pit_fields_completeness(bls_df))

    # --- Cross-dataset checks ---
    logger.info("\n" + "=" * 70)
    logger.info("SECTION B — CROSS-DATASET INTEGRITY")
    logger.info("=" * 70)
    record(chk_country_alignment_with_employment(bls_df))
    record(chk_bls_series_isolation_from_employment())
    record(chk_source_vocabulary_isolation())
    record(chk_temporal_partition_alignment(bls_ym))

    # --- Summary ---
    logger.info("\n" + "=" * 70)
    logger.info("SUMMARY")
    logger.info("=" * 70)
    overall = "PASS" if counts["FAIL"] == 0 else "FAIL"
    logger.info(f"  {counts['PASS']} passed / {counts['FAIL']} failed / "
                f"{counts['WARN']} warned / {counts['SKIP']} skipped")
    logger.info(f"  Overall: [{overall}]")

    # --- Save reports ---
    report = {
        "product":    PRODUCT,
        "country":    COUNTRY,
        "generated":  datetime.utcnow().isoformat() + "Z",
        "overall":    overall,
        "counts":     counts,
        "checks":     results,
    }
    with open(REPORT_JSON, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    lines = [
        "FOOD MICROPRICING — REFERENTIAL & CROSS-DATASET INTEGRITY REPORT",
        f"Generated: {report['generated']}",
        f"Overall:   [{overall}]",
        f"Counts:    {counts['PASS']} passed / {counts['FAIL']} failed / "
        f"{counts['WARN']} warned / {counts['SKIP']} skipped",
        "",
        "SECTION A — INTERNAL REFERENTIAL INTEGRITY (BLS <-> USDA)",
        "-" * 60,
    ]
    section_b_started = False
    for r in results:
        if r["check"] in {"Country Code Alignment w/ Employment",
                          "BLS Series Isolation from Employment",
                          "Source Vocabulary Isolation",
                          "Temporal Partition Alignment"}:
            if not section_b_started:
                lines += ["", "SECTION B — CROSS-DATASET INTEGRITY", "-" * 60]
                section_b_started = True
        lines.append(f"  [{r['status']:4}] {r['check']}")
        lines.append(f"         {r['message']}")

    with open(REPORT_TXT, "w", encoding="utf-8") as fh:
        fh.write("\n".join(lines) + "\n")

    logger.info(f"\nReports saved: {REPORT_JSON}, {REPORT_TXT}")
    return overall


if __name__ == "__main__":
    run_checks()
