"""Bitemporal PIT validation — Trade Flows (US Census FT-900)."""

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
PRODUCT      = "trade_flows"
COUNTRY      = "USA"
SOURCES      = ["census_ft900"]
REPORT_JSON  = Path("trade_flows_bitemporal_pit_report.json")
REPORT_TXT   = Path("trade_flows_bitemporal_pit_report.txt")
# FT-900 released ~5 weeks after reference month end ≈ 1-2 calendar months lag
PUB_LAG_BOUNDS = {"census_ft900": (1, 3)}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("trade_flows_bitemporal_pit.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)


# ── Loader ────────────────────────────────────────────────────────────────────

def _load():
    dfs = []
    for src in SOURCES:
        base = f"{VAULT_BASE}/product={PRODUCT}/country={COUNTRY}/source={src}"
        files = [f for f in vault_glob(base, "*.parquet")
                 if "outliers" not in f.name and "changelog" not in f.name]
        for f in files:
            try:
                dfs.append(vault_read_parquet(f))
            except Exception as exc:
                logger.warning(f"Skipping {f}: {exc}")
    if not dfs:
        raise FileNotFoundError("No trade_flows data found in vault. Run scraper first.")
    df = pd.concat(dfs, ignore_index=True)
    logger.info(f"  Loaded {len(df):,} records across {len(dfs)} partition files")
    return df


# ── Trade-specific checks ─────────────────────────────────────────────────────

def _check_trade_value_positive(df):
    """All observed_value / trade_value must be >= 0."""
    if "observed_value" not in df.columns:
        return {"status": "FAIL", "check": "Trade Value Non-Negative",
                "message": "observed_value column missing", "details": {}}
    numeric = pd.to_numeric(df["observed_value"], errors="coerce")
    neg     = int((numeric < 0).sum())
    nulls   = int(numeric.isna().sum())
    if neg == 0:
        return {"status": "PASS", "check": "Trade Value Non-Negative",
                "message": (f"All {int(numeric.notna().sum()):,} observed_values >= 0 "
                            f"({nulls} null, expected for missing commodity-months)"),
                "details": {"negative_count": 0, "null_count": nulls}}
    return {"status": "FAIL", "check": "Trade Value Non-Negative",
            "message": f"{neg} records with negative trade value",
            "details": {"negative_count": neg}}


def _check_trade_flow_vocabulary(df):
    """trade_flow must be exactly 'Export' or 'Import'."""
    if "trade_flow" not in df.columns:
        return {"status": "FAIL", "check": "Trade Flow Vocabulary",
                "message": "trade_flow column missing", "details": {}}
    valid   = {"Export", "Import"}
    found   = set(df["trade_flow"].dropna().unique())
    invalid = found - valid
    if not invalid:
        counts = {v: int((df["trade_flow"] == v).sum()) for v in found}
        return {"status": "PASS", "check": "Trade Flow Vocabulary",
                "message": f"All trade_flow values valid: {sorted(found)}",
                "details": {"counts": counts}}
    return {"status": "FAIL", "check": "Trade Flow Vocabulary",
            "message": f"Invalid trade_flow values: {invalid}",
            "details": {"invalid": list(invalid)}}


def _check_both_flows_present(df):
    """Both Export and Import flows must be present in the vault."""
    if "trade_flow" not in df.columns:
        return {"status": "SKIP", "check": "Both Flows Present",
                "message": "trade_flow column missing", "details": {}}
    flows = set(df["trade_flow"].dropna().unique())
    if {"Export", "Import"}.issubset(flows):
        n_exp = int((df["trade_flow"] == "Export").sum())
        n_imp = int((df["trade_flow"] == "Import").sum())
        return {"status": "PASS", "check": "Both Flows Present",
                "message": f"Exports: {n_exp:,} records, Imports: {n_imp:,} records",
                "details": {"exports": n_exp, "imports": n_imp}}
    missing = {"Export", "Import"} - flows
    return {"status": "FAIL", "check": "Both Flows Present",
            "message": f"Missing trade flows: {missing}",
            "details": {"missing": list(missing)}}


# ── Runner ────────────────────────────────────────────────────────────────────

def run():
    logger.info("=" * 70)
    logger.info("TRADE FLOWS — BITEMPORAL PIT VALIDATION")
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
        _check_trade_value_positive(df),
        _check_trade_flow_vocabulary(df),
        _check_both_flows_present(df),
        check_supersession_integrity(df),
    ]
    return write_report(REPORT_JSON, REPORT_TXT, PRODUCT, COUNTRY, results)


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
