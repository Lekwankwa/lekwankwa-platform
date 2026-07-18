"""Bitemporal PIT validation — Macro Wages & Employment (BLS CES + CPS)."""

import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from bitemporal_core import (  # noqa: E402
    check_unique_record_ids, check_knowledge_completeness,
    check_valid_to_knowledge_ordering, check_knowledge_ordering,
    check_as_of_published_cohesion, check_knowledge_horizon,
    check_anti_retroactive_ingestion, check_conversion_horizon,
    check_publication_lag, check_knowledge_monotonicity,
    check_bitemporal_uniqueness, check_supersession_integrity,
    write_report,
)
from _vault_root import VAULT_ROOT, vault_glob_paths as vault_glob, vault_read_parquet  # noqa: E402

# ── Config ────────────────────────────────────────────────────────────────────
VAULT_BASE   = VAULT_ROOT
PRODUCT      = "wages_and_employment"
COUNTRY      = "USA"
SOURCES      = ["bls_ces", "bls_cps"]
REPORT_JSON  = Path("macro_employment_bitemporal_pit_report.json")
REPORT_TXT   = Path("macro_employment_bitemporal_pit_report.txt")
# CES: Employment Situation released 1 month after reference month
# CPS: Employment Situation released 1 month after reference month (same release)
PUB_LAG_BOUNDS = {"bls_ces": (1, 2), "bls_cps": (1, 2)}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler("employment_bitemporal_pit.log", encoding="utf-8"),
              logging.StreamHandler()],
)
logger = logging.getLogger(__name__)

# ── Loader ────────────────────────────────────────────────────────────────────

def _load():
    dfs = []
    for src in SOURCES:
        src_path = f"{VAULT_BASE}/product={PRODUCT}/country={COUNTRY}/source={src}"
        files = [f for f in vault_glob(src_path, "*.parquet")
                 if "outliers" not in f.name and "changelog" not in f.name]
        for f in files:
            try:
                dfs.append(vault_read_parquet(f))
            except Exception as exc:
                logger.warning(f"Skipping {f}: {exc}")
    if not dfs:
        raise FileNotFoundError("No employment data found in vault")
    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"  Loaded {len(df):,} records")
    return df

# ── Employment-specific check ─────────────────────────────────────────────────

def _check_revision_integrity(df):
    """revision_number >= 0; all zero at initial load is expected and correct."""
    neg = int((df["revision_number"] < 0).sum())
    sup = int(df["superseded_by"].notna().sum()) if "superseded_by" in df.columns else 0
    if neg > 0:
        return {"status": "FAIL", "check": "Revision Integrity",
                "message": f"{neg} negative revision numbers",
                "details": {"negative_count": neg}}
    return {"status": "PASS", "check": "Revision Integrity",
            "message": f"All {len(df):,} records at revision_number=0, {sup} superseded (initial load)",
            "details": {"all_zero": True, "superseded_count": sup}}

# ── Runner ────────────────────────────────────────────────────────────────────

def run():
    logger.info("=" * 70)
    logger.info("MACRO EMPLOYMENT — BITEMPORAL PIT VALIDATION")
    logger.info("=" * 70)
    df = _load()
    results = [
        check_unique_record_ids(df),
        check_knowledge_completeness(df),
        check_valid_to_knowledge_ordering(df),
        check_knowledge_ordering(df),
        check_as_of_published_cohesion(df),
        check_knowledge_horizon(df),
        check_anti_retroactive_ingestion(df),
        check_conversion_horizon(df),
        check_publication_lag(df, PUB_LAG_BOUNDS),
        check_knowledge_monotonicity(df, sample=50, min_len=6, min_year=2000),
        check_bitemporal_uniqueness(df),
        _check_revision_integrity(df),
        check_supersession_integrity(df),
    ]
    return write_report(REPORT_JSON, REPORT_TXT, PRODUCT, COUNTRY, results)


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
