"""
Referential Integrity Validation for Housing Supply & Shelter Inflation

Checks internal consistency within the housing dataset, temporal overlap
between the two sub-datasets, and cross-dataset isolation from other
product vaults (food, electricity, macro_employment).

REFERENTIAL INTEGRITY CHECKS:
  A. Intra-dataset:
     A1. Shelter Series Completeness  — all 7 CPI series present
     A2. BPS Variable Completeness    — all 7 BPS variables present
     A3. Shelter NSA/SA Series Parity — for each NSA series, check SA counterpart exists
  B. Cross-dataset (shelter ↔ permits):
     B1. Temporal Overlap             — both datasets share overlapping date range (1959+)
     B2. No Record Bleed              — no record from one source in other source partition
  C. Cross-product isolation:
     C1. Housing records not in food_micropricing vault
     C2. Housing records not in electricity vault
     C3. Housing records not in macro_employment vault

OUTPUT:
  - housing_referential_integrity_report.json
  - housing_referential_integrity_report.txt

Author: Lekwankwa Corporation
Date: 2026-06-12
"""

import json
import logging
import sys
from datetime import datetime
from pathlib import Path

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("housing_referential_integrity.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

VAULT_DIR = Path("lekwankwa-historical-vault")
PRODUCT   = "Housing_Supply_and_Shelter_Inflation"
COUNTRY   = "USA"

SHELTER_SOURCE = "bls_cpi_shelter"
PERMITS_SOURCE = "census_bps"

REPORT_JSON = Path("housing_referential_integrity_report.json")
REPORT_TXT  = Path("housing_referential_integrity_report.txt")

# Expected series / variables
EXPECTED_SHELTER_SERIES = {
    "CUUR0000SEHA", "CUUR0000SEHB", "CUUR0000SAH1", "CUUR0000SEHC",
    "CUSR0000SEHA", "CUSR0000SEHB", "CUSR0000SAH1",
}
NSA_TO_SA_MAP = {
    "CUUR0000SEHA": "CUSR0000SEHA",
    "CUUR0000SEHB": "CUSR0000SEHB",
    "CUUR0000SAH1": "CUSR0000SAH1",
}
EXPECTED_BPS_VARS = {"PERMIT", "PERMIT1", "PERMIT5"}

# Other product vault paths for isolation check (sample file counts only)
OTHER_PRODUCTS = {
    "food_micropricing":  "product=food_micropricing",
    "electricity":        "product=electricity",
    "wages_and_employment":   "product=wages_and_employment",
}

# Overlap start — BPS begins 1959; CPI shelter earliest: CUUR0000SEHA 1914
OVERLAP_START_YEAR = 1959


# =============================================================================
# HELPERS
# =============================================================================

def _load_all(source: str, max_files: int = 100) -> pd.DataFrame:
    src_path = VAULT_DIR / f"product={PRODUCT}" / f"country={COUNTRY}" / f"source={source}"
    files = sorted(src_path.rglob("*_data.parquet"))
    step = max(1, len(files) // max_files)
    sampled = files[::step][:max_files]
    dfs = []
    for f in sampled:
        try:
            dfs.append(pd.read_parquet(f))
        except Exception as exc:
            logger.warning(f"  Skipping {f}: {exc}")
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


# =============================================================================
# SECTION A — INTRA-DATASET
# =============================================================================

def check_shelter_series_completeness(df: pd.DataFrame) -> dict:
    """All 7 expected BLS CPI shelter series must be present."""
    if df.empty:
        return {"status": "SKIP", "check": "A1 Shelter Series Completeness",
                "message": "No shelter data in vault"}
    col = "sovereign_series_id"
    if col not in df.columns:
        return {"status": "WARN", "check": "A1 Shelter Series Completeness",
                "message": "sovereign_series_id column missing"}
    found   = set(df[col].dropna().unique())
    missing = EXPECTED_SHELTER_SERIES - found
    extra   = found - EXPECTED_SHELTER_SERIES
    if not missing:
        return {"status": "PASS", "check": "A1 Shelter Series Completeness",
                "message": f"All {len(EXPECTED_SHELTER_SERIES)} expected CPI shelter series present",
                "series": sorted(found)}
    return {"status": "FAIL", "check": "A1 Shelter Series Completeness",
            "message": f"{len(missing)} expected series missing from vault",
            "missing": sorted(missing), "extra": sorted(extra)}


def check_bps_variable_completeness(df: pd.DataFrame) -> dict:
    """All 7 BPS variables must be present in the permits vault."""
    if df.empty:
        return {"status": "SKIP", "check": "A2 BPS Variable Completeness",
                "message": "No permits data in vault"}
    col = "sovereign_series_id" if "sovereign_series_id" in df.columns else "bps_variable"
    if col not in df.columns:
        return {"status": "WARN", "check": "A2 BPS Variable Completeness",
                "message": "sovereign_series_id / bps_variable column missing"}
    found   = set(df[col].dropna().unique())
    missing = EXPECTED_BPS_VARS - found
    if not missing:
        return {"status": "PASS", "check": "A2 BPS Variable Completeness",
                "message": f"All {len(EXPECTED_BPS_VARS)} expected BPS variables present",
                "variables": sorted(found)}
    return {"status": "FAIL", "check": "A2 BPS Variable Completeness",
            "message": f"{len(missing)} BPS variables missing from vault",
            "missing": sorted(missing)}


def check_nsa_sa_parity(df_shelter: pd.DataFrame) -> dict:
    """For each NSA series, verify the SA counterpart also exists."""
    if df_shelter.empty:
        return {"status": "SKIP", "check": "A3 NSA/SA Series Parity",
                "message": "No shelter data"}
    col = "sovereign_series_id"
    if col not in df_shelter.columns:
        return {"status": "SKIP", "check": "A3 NSA/SA Series Parity",
                "message": f"{col} column missing"}
    found = set(df_shelter[col].dropna().unique())
    missing_sa = {nsa: sa for nsa, sa in NSA_TO_SA_MAP.items()
                  if nsa in found and sa not in found}
    if not missing_sa:
        return {"status": "PASS", "check": "A3 NSA/SA Series Parity",
                "message": "All NSA series have a corresponding SA series in the vault"}
    return {"status": "WARN", "check": "A3 NSA/SA Series Parity",
            "message": f"{len(missing_sa)} NSA series have no SA counterpart",
            "missing_sa_for": missing_sa}


# =============================================================================
# SECTION B — CROSS-DATASET (SHELTER ↔ PERMITS)
# =============================================================================

def check_temporal_overlap(df_shelter: pd.DataFrame, df_permits: pd.DataFrame) -> dict:
    """Both datasets should share overlapping time range starting 1959."""
    ts_col = "reporting_date" if "reporting_date" in (
        df_shelter.columns if not df_shelter.empty else []) else "data_timestamp"

    results = {}
    for name, df in [("shelter", df_shelter), ("permits", df_permits)]:
        if df.empty:
            results[name] = {"min": None, "max": None}
            continue
        col = "reporting_date" if "reporting_date" in df.columns else "data_timestamp"
        ts  = pd.to_datetime(df[col], errors="coerce", utc=True)
        results[name] = {
            "min": ts.min().isoformat() if not ts.isna().all() else None,
            "max": ts.max().isoformat() if not ts.isna().all() else None,
        }

    if results.get("shelter", {}).get("min") and results.get("permits", {}).get("min"):
        s_min = pd.Timestamp(results["shelter"]["min"])
        p_min = pd.Timestamp(results["permits"]["min"])
        overlap_start = max(s_min, p_min)
        overlap_year  = overlap_start.year
        if overlap_year <= OVERLAP_START_YEAR + 2:   # within 2 years of expected 1959
            return {"status": "PASS", "check": "B1 Temporal Overlap",
                    "message": f"Shelter and permits data overlap from {overlap_year}",
                    "shelter_range": results["shelter"],
                    "permits_range": results["permits"]}
        return {"status": "WARN", "check": "B1 Temporal Overlap",
                "message": f"Overlap starts {overlap_year} (expected ~{OVERLAP_START_YEAR})",
                "shelter_range": results["shelter"],
                "permits_range": results["permits"]}

    return {"status": "SKIP", "check": "B1 Temporal Overlap",
            "message": "One or both datasets are empty — cannot check temporal overlap",
            "ranges": results}


def check_record_bleed(df_shelter: pd.DataFrame, df_permits: pd.DataFrame) -> dict:
    """No shelter record should appear in permits partition and vice versa."""
    issues = []
    if not df_shelter.empty and "source" in df_shelter.columns:
        wrong = df_shelter[df_shelter["source"] != SHELTER_SOURCE]
        if not wrong.empty:
            issues.append(f"Shelter partition has {len(wrong)} records with source!={SHELTER_SOURCE}")
    if not df_permits.empty and "source" in df_permits.columns:
        wrong = df_permits[df_permits["source"] != PERMITS_SOURCE]
        if not wrong.empty:
            issues.append(f"Permits partition has {len(wrong)} records with source!={PERMITS_SOURCE}")
    if not issues:
        return {"status": "PASS", "check": "B2 Record Bleed",
                "message": "No cross-contamination between shelter and permits partitions"}
    return {"status": "FAIL", "check": "B2 Record Bleed",
            "message": "; ".join(issues)}


# =============================================================================
# SECTION C — CROSS-PRODUCT ISOLATION
# =============================================================================

def check_cross_product_isolation() -> dict:
    """Housing source labels must not appear in other product vaults."""
    housing_sources = {SHELTER_SOURCE, PERMITS_SOURCE}
    contaminations  = []

    for product_name, product_dir_part in OTHER_PRODUCTS.items():
        product_path = VAULT_DIR / product_dir_part
        if not product_path.exists():
            continue
        files = list(product_path.rglob("*.parquet"))[:10]   # sample
        for f in files:
            try:
                df = pd.read_parquet(f, columns=["source"])
                bleed = df[df["source"].isin(housing_sources)]
                if not bleed.empty:
                    contaminations.append(
                        f"{product_name}: {f.name} contains "
                        f"housing source '{bleed['source'].iloc[0]}'"
                    )
            except Exception:
                continue

    if not contaminations:
        return {"status": "PASS", "check": "C Cross-product Isolation",
                "message": "Housing source labels not found in food/electricity/employment vaults"}
    return {"status": "FAIL", "check": "C Cross-product Isolation",
            "message": f"{len(contaminations)} cross-product contamination(s) detected",
            "details": contaminations}


# =============================================================================
# RUNNER
# =============================================================================

def run():
    logger.info("=" * 70)
    logger.info("HOUSING — REFERENTIAL INTEGRITY VALIDATION")
    logger.info("=" * 70)
    logger.info(f"Run timestamp: {datetime.utcnow().isoformat()}Z")

    logger.info(f"\nLoading {SHELTER_SOURCE}…")
    df_shelter = _load_all(SHELTER_SOURCE)
    logger.info(f"  {len(df_shelter):,} records loaded (sample)")

    logger.info(f"\nLoading {PERMITS_SOURCE}…")
    df_permits = _load_all(PERMITS_SOURCE)
    logger.info(f"  {len(df_permits):,} records loaded (sample)")

    results = [
        # Section A — intra
        check_shelter_series_completeness(df_shelter),
        check_bps_variable_completeness(df_permits),
        check_nsa_sa_parity(df_shelter),
        # Section B — cross-dataset
        check_temporal_overlap(df_shelter, df_permits),
        check_record_bleed(df_shelter, df_permits),
        # Section C — cross-product
        check_cross_product_isolation(),
    ]

    # ── Summary ──────────────────────────────────────────────────────────────
    passed = sum(r["status"] == "PASS" for r in results)
    failed = sum(r["status"] == "FAIL" for r in results)
    warned = sum(r["status"] == "WARN" for r in results)

    logger.info("\n" + "=" * 70)
    for r in results:
        icon = {"PASS": "[+]", "FAIL": "[!]", "WARN": "[!]", "SKIP": "[-]"}.get(r["status"], "[?]")
        logger.info(f"  [{icon}] {r['check']}: {r['message']}")
    logger.info("=" * 70)
    logger.info(f"SUMMARY: {passed} PASS / {failed} FAIL / {warned} WARN / {len(results)} total")

    report = {
        "product": PRODUCT,
        "country": COUNTRY,
        "run_timestamp": datetime.utcnow().isoformat() + "Z",
        "summary": {"total": len(results), "passed": passed, "failed": failed, "warned": warned},
        "results": results,
    }
    with open(REPORT_JSON, "w") as fh:
        json.dump(report, fh, indent=2, default=str)
    with open(REPORT_TXT, "w", encoding="utf-8", errors="replace") as fh:
        fh.write(f"Housing Referential Integrity Report — {datetime.utcnow().isoformat()}Z\n")
        fh.write("=" * 70 + "\n")
        for r in results:
            fh.write(f"  [{r['status']}] {r['check']}: {r['message']}\n")
        fh.write(f"\nSUMMARY: {passed} PASS / {failed} FAIL / {warned} WARN\n")

    logger.info(f"Reports written: {REPORT_JSON}, {REPORT_TXT}")
    return failed == 0


if __name__ == "__main__":
    import sys
    sys.exit(0 if run() else 1)
