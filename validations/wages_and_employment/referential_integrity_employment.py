"""
Referential & Cross-Dataset Integrity Validation — Macro Wages & Employment

Verifies that:
  (a) the two internal sources (BLS CES and BLS CPS) within the
      macro_employment vault are mutually consistent and referentially sound, and
  (b) the macro_employment vault is consistent with the food_micropricing and
      electricity vaults for the shared jurisdiction and time window.

INTERNAL CHECKS (CES ↔ CPS):
  1.  Country Code Consistency       — both sources carry country_code='US'
  2.  Schema Field Parity            — both share required core columns; distinct cols noted
  3.  Series ID Isolation            — CES IDs start with 'CES'; CPS with 'LNS'; no overlap
  4.  Industry Code Scope            — CES: 8-digit supersector codes; CPS: national aggregate '00000000'
  5.  CES↔CPS Temporal Overlap       — shared coverage 2011-2026 window (CPS extends to 1948)
  6.  Metric Value Non-Negativity    — employment headcount/rate values >= 0
  7.  PIT Fields Consistency         — all 5 PIT fields present in both sources

CROSS-DATASET CHECKS:
  8.  Country Code Alignment w/ Food — employment 'US' == food 'US'
  9.  Employment Series Isolation    — no CES/LNS IDs in food series
  10. Source Vocabulary Isolation    — no 'bls_ces'/'bls_cps' in food/electricity source fields
  11. Temporal Partition Alignment   — 2011-2025 months shared across CPS, food_BLS, electricity

OUTPUT:
  - macro_employment_referential_integrity_report.json
  - macro_employment_referential_integrity_report.txt

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
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("macro_employment_referential_integrity.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

VAULT_DIR = Path("lekwankwa-historical-vault")
PRODUCT   = "wages_and_employment"
COUNTRY   = "USA"
SOURCES   = ["bls_ces", "bls_cps"]

REPORT_JSON = Path("macro_employment_referential_integrity_report.json")
REPORT_TXT  = Path("macro_employment_referential_integrity_report.txt")

SAMPLE_FILES = 40   # per source

CES_SERIES_RE   = re.compile(r"^CES\d{10}$")
CPS_SERIES_RE   = re.compile(r"^LNS\d{8}$")

CES_INDUSTRY_CODE_RE   = re.compile(r"^\d{8}$")   # 8 digits
CPS_NATIONAL_CODE      = "00000000"                # CPS is national aggregate

REQUIRED_SHARED_COLUMNS = {
    "record_id", "country_code", "industry_code", "metric_value",
    "seasonal_adjustment", "data_timestamp", "conversion_timestamp",
    "source", "source_series_id", "source_url", "extraction_method",
    "data_quality_certified", "published_date", "as_of_date",
    "revision_number", "superseded_by",
}

PIT_FIELDS = {"record_id", "revision_number", "superseded_by", "published_date", "as_of_date"}

# Cross-dataset
FOOD_BLS_SRC    = "bls"
ELEC_SRC        = "eia"
FOOD_APU_RE     = re.compile(r"^APU\d{10}$")

# =============================================================================
# HELPERS
# =============================================================================

def _result(status, check, message, details=None):
    entry = {"status": status, "check": check, "message": message}
    if details:
        entry["details"] = details
    icon   = {"PASS": "[PASS]", "FAIL": "[FAIL]", "WARN": "[WARN]", "SKIP": "[SKIP]"}[status]
    log_fn = logger.warning if status == "WARN" else (logger.error if status == "FAIL" else logger.info)
    log_fn(f"  {icon} {check}")
    if message:
        log_fn(f"         {message}")
    return entry


def _load_sample(source, n_files):
    pattern = VAULT_DIR / f"product={PRODUCT}" / f"country={COUNTRY}" / f"source={source}" / "year=*" / "month=*" / "*.parquet"
    files   = sorted(Path(".").glob(str(pattern)))
    step    = max(1, len(files) // n_files)
    sampled = files[::step][:n_files]
    if not sampled:
        return pd.DataFrame()
    return pd.concat([pd.read_parquet(f) for f in sampled], ignore_index=True)


def _get_ym_set(product, source, file_glob):
    pattern = VAULT_DIR / f"product={product}" / f"country={COUNTRY}" / f"source={source}" / "year=*" / "month=*" / file_glob
    yms = set()
    for f in Path(".").glob(str(pattern)):
        year  = int(f.parent.parent.name.split("=")[1])
        month = int(f.parent.name.split("=")[1])
        yms.add((year, month))
    return yms

# =============================================================================
# INTERNAL CHECKS — CES ↔ CPS
# =============================================================================

def chk_country_code_consistency(ces_df, cps_df):
    ces_codes = set(ces_df["country_code"].dropna().unique()) if "country_code" in ces_df.columns else set()
    cps_codes = set(cps_df["country_code"].dropna().unique()) if "country_code" in cps_df.columns else set()
    bad_ces = ces_codes - {"US", "USA"}
    bad_cps = cps_codes - {"US", "USA"}
    if bad_ces or bad_cps:
        return _result("FAIL", "Country Code Consistency",
                       f"Unexpected codes — CES: {bad_ces}, CPS: {bad_cps}")
    if ces_codes != cps_codes:
        return _result("WARN", "Country Code Consistency",
                       f"Different but valid sets — CES: {ces_codes}, CPS: {cps_codes}")
    return _result("PASS", "Country Code Consistency",
                   f"Both CES and CPS carry country_code={ces_codes}")


def chk_schema_field_parity(ces_df, cps_df):
    ces_cols = set(ces_df.columns)
    cps_cols = set(cps_df.columns)
    # Use v2 column names if present, fall back to v1
    shared_check = REQUIRED_SHARED_COLUMNS.copy()
    if "sovereign_series_id" in ces_cols:
        shared_check = (shared_check - {"source_series_id", "source_url"})
        shared_check |= {"sovereign_series_id", "portal_url"}
    missing_from_ces = shared_check - ces_cols
    missing_from_cps = shared_check - cps_cols
    if missing_from_ces or missing_from_cps:
        return _result("FAIL", "Schema Field Parity",
                       f"Required columns absent — CES: {missing_from_ces}, CPS: {missing_from_cps}")
    ces_only = ces_cols - cps_cols
    cps_only = cps_cols - ces_cols
    if ces_only or cps_only:
        return _result("WARN", "Schema Field Parity",
                       f"Source-exclusive columns — CES-only: {ces_only}, CPS-only: {cps_only}",
                       {"ces_only": sorted(ces_only), "cps_only": sorted(cps_only)})
    return _result("PASS", "Schema Field Parity",
                   f"Both sources carry all required columns with identical schema")


def chk_series_id_isolation(ces_df, cps_df):
    sid_col = "sovereign_series_id" if "sovereign_series_id" in ces_df.columns else "source_series_id"
    ces_ids = set(ces_df[sid_col].dropna().unique()) if sid_col in ces_df.columns else set()
    cps_ids = set(cps_df[sid_col].dropna().unique()) if sid_col in cps_df.columns else set()

    bad_ces = {s for s in ces_ids if not CES_SERIES_RE.match(str(s))}
    bad_cps = {s for s in cps_ids if not CPS_SERIES_RE.match(str(s))}
    cross   = ces_ids & cps_ids

    issues = []
    if bad_ces:
        issues.append(f"{len(bad_ces)} CES IDs not matching CES pattern")
    if bad_cps:
        issues.append(f"{len(bad_cps)} CPS IDs not matching LNS pattern")
    if cross:
        issues.append(f"{len(cross)} series IDs shared between CES and CPS (leakage)")

    if issues:
        return _result("FAIL", "Series ID Isolation",
                       "; ".join(issues),
                       {"bad_ces_sample": sorted(bad_ces)[:5], "bad_cps_sample": sorted(bad_cps)[:5],
                        "cross_sample": sorted(cross)[:5]})
    return _result("PASS", "Series ID Isolation",
                   f"CES ({len(ces_ids):,} IDs, all matching 'CES\\d{{10}}') and "
                   f"CPS ({len(cps_ids):,} IDs, all matching 'LNS\\d{{8}}') "
                   f"are fully isolated — no ID overlap")


def chk_industry_code_scope(ces_df, cps_df):
    """CES uses 8-digit supersector codes; CPS is national-only (00000000)."""
    issues = []
    if "industry_code" in ces_df.columns:
        ces_bad = ces_df["industry_code"].dropna().apply(
            lambda x: not CES_INDUSTRY_CODE_RE.match(str(x))
        )
        if ces_bad.sum():
            sample = ces_df.loc[ces_bad, "industry_code"].unique()[:5]
            issues.append(f"{int(ces_bad.sum()):,} CES records with non-8-digit industry_code: {list(sample)}")
    if "industry_code" in cps_df.columns:
        non_national = cps_df["industry_code"].dropna()
        non_national = non_national[non_national.astype(str) != CPS_NATIONAL_CODE]
        if len(non_national) > 0:
            issues.append(f"{len(non_national):,} CPS records with non-national industry_code (expected '{CPS_NATIONAL_CODE}')")
    if issues:
        return _result("FAIL", "Industry Code Scope", "; ".join(issues))
    return _result("PASS", "Industry Code Scope",
                   f"CES uses 8-digit supersector codes; "
                   f"CPS uses national aggregate code '{CPS_NATIONAL_CODE}' — correct by program design")


def chk_temporal_overlap(ces_ym, cps_ym):
    window  = {(y, m) for y in range(2011, 2027) for m in range(1, 13)}
    ces_w   = ces_ym & window
    cps_w   = cps_ym & window
    if not ces_w or not cps_w:
        return _result("FAIL", "CES-CPS Temporal Overlap",
                       f"Insufficient data in 2011-2026 window — CES: {len(ces_w)}, CPS: {len(cps_w)}")
    common = ces_w & cps_w
    pct = 100 * len(common) / max(len(ces_w), len(cps_w))
    if pct < 80:
        return _result("FAIL", "CES-CPS Temporal Overlap",
                       f"Only {len(common)} shared months ({pct:.0f}%) in 2011-2026 window",
                       {"ces_months": len(ces_w), "cps_months": len(cps_w), "shared": len(common)})
    return _result("PASS", "CES-CPS Temporal Overlap",
                   f"{len(common)} shared months in 2011-2026 window ({pct:.0f}% overlap). "
                   f"CES={len(ces_w)}, CPS={len(cps_w)}",
                   {"shared_months": len(common), "ces_in_window": len(ces_w), "cps_in_window": len(cps_w)})


def chk_metric_value_non_negativity(ces_df, cps_df):
    """Employment levels and rates should be >= 0 (negative values are data errors)."""
    issues = []
    for label, df in [("CES", ces_df), ("CPS", cps_df)]:
        if "metric_value" not in df.columns:
            issues.append(f"{label}: metric_value column missing")
            continue
        numeric = pd.to_numeric(df["metric_value"], errors="coerce").dropna()
        neg_count = int((numeric < 0).sum())
        if neg_count:
            issues.append(f"{label}: {neg_count:,} records with metric_value < 0")
    if issues:
        return _result("FAIL", "Metric Value Non-Negativity", "; ".join(issues))
    return _result("PASS", "Metric Value Non-Negativity",
                   "All numeric metric_value entries are >= 0 in both CES and CPS samples")


def chk_pit_fields_consistency(ces_df, cps_df):
    missing = {}
    for label, df in [("CES", ces_df), ("CPS", cps_df)]:
        absent = PIT_FIELDS - set(df.columns)
        if absent:
            missing[label] = sorted(absent)
    if missing:
        return _result("FAIL", "PIT Fields Consistency",
                       f"Missing PIT fields: {missing}")
    ces_null = int(ces_df["record_id"].isna().sum())
    cps_null = int(cps_df["record_id"].isna().sum())
    if ces_null or cps_null:
        return _result("FAIL", "PIT Fields Consistency",
                       f"Null record_ids — CES: {ces_null:,}, CPS: {cps_null:,}")
    return _result("PASS", "PIT Fields Consistency",
                   f"All 5 PIT fields present in both CES and CPS; record_id non-null throughout")


# =============================================================================
# CROSS-DATASET CHECKS
# =============================================================================

def chk_country_alignment_with_food(ces_df):
    """Spot-check employment country_code matches food country_code."""
    food_files = sorted(Path(".").glob(str(
        VAULT_DIR / "product=food_micropricing" / f"country={COUNTRY}"
        / f"source={FOOD_BLS_SRC}" / "year=2020" / "month=01" / "base_data.parquet"
    )))
    if not food_files:
        return _result("SKIP", "Country Code Alignment w/ Food",
                       "Food vault not found — skipping cross-dataset check")
    food_df  = pd.read_parquet(food_files[0])
    emp_cc   = set(ces_df["country_code"].dropna().unique())
    food_cc  = set(food_df["country_code"].dropna().unique()) if "country_code" in food_df.columns else set()
    if not (emp_cc <= {"US", "USA"}) or not (food_cc <= {"US", "USA"}):
        return _result("FAIL", "Country Code Alignment w/ Food",
                       f"Non-US codes — employment: {emp_cc}, food: {food_cc}")
    return _result("PASS", "Country Code Alignment w/ Food",
                   f"Employment ({emp_cc}) and food ({food_cc}) both carry US country codes")


def chk_employment_series_isolation_from_food():
    """Verify no CES or CPS series IDs appear in food_micropricing data."""
    food_series = set()
    food_files = sorted(Path(".").glob(str(
        VAULT_DIR / "product=food_micropricing" / f"country={COUNTRY}"
        / f"source={FOOD_BLS_SRC}" / "year=2020" / "month=01" / "base_data.parquet"
    )))
    for f in food_files[:2]:
        df = pd.read_parquet(f)
        col = "sovereign_series_id" if "sovereign_series_id" in df.columns else "source_series_id"
        if col in df.columns:
            food_series.update(df[col].dropna().unique())

    if not food_series:
        return _result("SKIP", "Employment Series Isolation from Food",
                       "Food vault not accessible — skipping")

    leaked_ces = {s for s in food_series if CES_SERIES_RE.match(str(s))}
    leaked_cps = {s for s in food_series if CPS_SERIES_RE.match(str(s))}

    if leaked_ces or leaked_cps:
        return _result("FAIL", "Employment Series Isolation from Food",
                       f"CES IDs in food: {len(leaked_ces)}, CPS IDs in food: {len(leaked_cps)}",
                       {"leaked_ces": sorted(leaked_ces)[:5], "leaked_cps": sorted(leaked_cps)[:5]})
    return _result("PASS", "Employment Series Isolation from Food",
                   f"No CES or CPS series IDs found in food vault "
                   f"({len(food_series):,} food series checked)")


def chk_source_vocabulary_isolation():
    """Verify no employment source labels ('bls_ces','bls_cps') appear in food/electricity."""
    issues = []

    # Check food BLS
    food_files = sorted(Path(".").glob(str(
        VAULT_DIR / "product=food_micropricing" / f"country={COUNTRY}"
        / f"source={FOOD_BLS_SRC}" / "year=2020" / "month=01" / "base_data.parquet"
    )))
    if food_files:
        df = pd.read_parquet(food_files[0])
        bad = set(df.get("source", pd.Series()).dropna().unique()) & {"bls_ces", "bls_cps", "eia"}
        if bad:
            issues.append(f"Food contains employment/electricity source values: {bad}")

    # Check electricity
    elec_files = sorted(Path(".").glob(str(
        VAULT_DIR / "product=electricity" / f"country={COUNTRY}"
        / f"source={ELEC_SRC}" / "year=2020" / "month=01" / "generation_data.parquet"
    )))
    if elec_files:
        df = pd.read_parquet(elec_files[0])
        bad = set(df.get("source", pd.Series()).dropna().unique()) & {"bls_ces", "bls_cps", "bls", "usda"}
        if bad:
            issues.append(f"Electricity contains employment/food source values: {bad}")

    if issues:
        return _result("FAIL", "Source Vocabulary Isolation", "; ".join(issues))
    return _result("PASS", "Source Vocabulary Isolation",
                   "Source vocabulary is product-isolated: employment={'bls_ces','bls_cps'}, "
                   "food={'bls','usda'}, electricity={'eia'} — no leakage detected")


def chk_temporal_partition_alignment(cps_ym):
    """Verify CPS + food_BLS + electricity share partitions for 2011-2025."""
    food_ym = _get_ym_set("food_micropricing", FOOD_BLS_SRC, "base_data.parquet")
    elec_ym = _get_ym_set("electricity",        ELEC_SRC,     "generation_data.parquet")

    if not food_ym or not elec_ym:
        return _result("SKIP", "Temporal Partition Alignment",
                       "Food or electricity vault not found — skipping cross-dataset check")

    window  = {(y, m) for y in range(2011, 2026) for m in range(1, 13)}
    cps_w   = cps_ym  & window
    food_w  = food_ym & window
    elec_w  = elec_ym & window
    common  = cps_w & food_w & elec_w
    expected = len(window)

    missing_cps  = window - cps_w
    missing_food = window - food_w
    missing_elec = window - elec_w

    if len(common) < 0.90 * expected:
        return _result("FAIL", "Temporal Partition Alignment",
                       f"Only {len(common)}/{expected} months shared across CPS, food_BLS, "
                       f"and electricity for 2011-2025",
                       {"missing_cps": len(missing_cps), "missing_food": len(missing_food),
                        "missing_elec": len(missing_elec)})

    gaps = []
    if missing_cps:  gaps.append(f"CPS missing {len(missing_cps)}")
    if missing_food: gaps.append(f"food_BLS missing {len(missing_food)}")
    if missing_elec: gaps.append(f"electricity missing {len(missing_elec)}")

    status  = "WARN" if gaps else "PASS"
    message = (f"{len(common)}/{expected} months present across all three products (2011-2025). "
               + ("; ".join(gaps) if gaps else "Perfect alignment."))
    return _result(status, "Temporal Partition Alignment", message,
                   {"shared_months": len(common), "total_window": expected,
                    "cps": len(cps_w), "food_bls": len(food_w), "electricity": len(elec_w)})


# =============================================================================
# RUNNER
# =============================================================================

def run_checks():
    logger.info("=" * 70)
    logger.info("MACRO EMPLOYMENT — REFERENTIAL & CROSS-DATASET INTEGRITY")
    logger.info("=" * 70)

    results = []
    counts  = {"PASS": 0, "FAIL": 0, "WARN": 0, "SKIP": 0}

    def record(r):
        results.append(r)
        counts[r["status"]] += 1

    # --- Load samples ---
    logger.info("\nLoading CES sample…")
    ces_df   = _load_sample("bls_ces",   SAMPLE_FILES)
    logger.info(f"  {len(ces_df):,} CES records loaded")
    logger.info("Loading CPS sample…")
    cps_df = _load_sample("bls_cps", SAMPLE_FILES)
    logger.info(f"  {len(cps_df):,} CPS records loaded")

    if ces_df.empty or cps_df.empty:
        logger.error("Could not load samples — aborting")
        return

    ces_ym   = _get_ym_set(PRODUCT, "bls_ces",  "*.parquet")
    cps_ym   = _get_ym_set(PRODUCT, "bls_cps",  "*.parquet")

    # --- Internal checks ---
    logger.info("\n" + "=" * 70)
    logger.info("SECTION A — INTERNAL REFERENTIAL INTEGRITY (CES ↔ CPS)")
    logger.info("=" * 70)
    record(chk_country_code_consistency(ces_df, cps_df))
    record(chk_schema_field_parity(ces_df, cps_df))
    record(chk_series_id_isolation(ces_df, cps_df))
    record(chk_industry_code_scope(ces_df, cps_df))
    record(chk_temporal_overlap(ces_ym, cps_ym))
    record(chk_metric_value_non_negativity(ces_df, cps_df))
    record(chk_pit_fields_consistency(ces_df, cps_df))

    # --- Cross-dataset checks ---
    logger.info("\n" + "=" * 70)
    logger.info("SECTION B — CROSS-DATASET INTEGRITY")
    logger.info("=" * 70)
    record(chk_country_alignment_with_food(ces_df))
    record(chk_employment_series_isolation_from_food())
    record(chk_source_vocabulary_isolation())
    record(chk_temporal_partition_alignment(cps_ym))

    # --- Summary ---
    logger.info("\n" + "=" * 70)
    logger.info("SUMMARY")
    logger.info("=" * 70)
    overall = "PASS" if counts["FAIL"] == 0 else "FAIL"
    logger.info(f"  {counts['PASS']} passed / {counts['FAIL']} failed / "
                f"{counts['WARN']} warned / {counts['SKIP']} skipped")
    logger.info(f"  Overall: [{overall}]")

    report = {
        "product":   PRODUCT,
        "country":   COUNTRY,
        "generated": datetime.utcnow().isoformat() + "Z",
        "overall":   overall,
        "counts":    counts,
        "checks":    results,
    }
    with open(REPORT_JSON, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, default=str)

    lines = [
        "MACRO EMPLOYMENT — REFERENTIAL & CROSS-DATASET INTEGRITY REPORT",
        f"Generated: {report['generated']}",
        f"Overall:   [{overall}]",
        f"Counts:    {counts['PASS']} passed / {counts['FAIL']} failed / "
        f"{counts['WARN']} warned / {counts['SKIP']} skipped",
        "",
        "SECTION A — INTERNAL REFERENTIAL INTEGRITY (CES <-> JOLTS)",
        "-" * 60,
    ]
    cross_checks = {"Country Code Alignment w/ Food", "Employment Series Isolation from Food",
                    "Source Vocabulary Isolation", "Temporal Partition Alignment"}
    section_b_started = False
    for r in results:
        if r["check"] in cross_checks and not section_b_started:
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
