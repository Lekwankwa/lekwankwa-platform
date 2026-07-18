"""Bitemporal PIT validation — Housing Supply & Shelter Inflation (BLS CPI + Census BPS)."""

import logging
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from bitemporal_core import (  # noqa: E402
    check_unique_record_ids,
    check_knowledge_completeness,
    check_valid_to_knowledge_ordering,
    check_knowledge_ordering,
    check_as_of_published_cohesion,
    check_knowledge_horizon,
    check_anti_retroactive_ingestion,
    check_conversion_horizon,
    check_publication_lag,
    check_knowledge_monotonicity,
    check_bitemporal_uniqueness,
    check_supersession_integrity,
    write_report,
)
from _vault_root import VAULT_ROOT, vault_glob_since as vault_glob, vault_read_parquet  # noqa: E402

# ── Config ────────────────────────────────────────────────────────────────────
VAULT_BASE   = VAULT_ROOT
PRODUCT      = "Housing_Supply_and_Shelter_Inflation"
COUNTRY      = "USA"
SOURCES      = ["bls_cpi_shelter", "census_bps"]
REPORT_JSON  = Path("housing_bitemporal_pit_report.json")
REPORT_TXT   = Path("housing_bitemporal_pit_report.txt")

# BLS CPI shelter: CPI released ~12-16 days after reference month end
#   → data for month M is published early in month M+1 (~1-2 months lag)
# Census BPS: Building Permits released ~6 weeks after reference month end
#   → lag range 1.5-3 months
PUB_LAG_BOUNDS = {
    "bls_cpi_shelter": (1, 2),   # months — BLS releases CPI ~12-16 days after reference month
    "census_bps":      (1, 2),   # months — FRED stores published_date as first of next month (~1 mo)
}

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("housing_bitemporal_pit.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)


# ── Loader ────────────────────────────────────────────────────────────────────

def _load() -> pd.DataFrame:
    dfs = []
    for src in SOURCES:
        src_path = f"{VAULT_BASE}/product={PRODUCT}/country={COUNTRY}/source={src}"
        files = sorted(vault_glob(src_path, "*_data.parquet"))  # Only _data.parquet files
        for f in files:
            try:
                df = vault_read_parquet(f)
                df["__source__"] = src
                dfs.append(df)
            except Exception as exc:
                logger.warning(f"  Skipping {f}: {exc}")
    if not dfs:
        raise FileNotFoundError(
            f"No housing data found in vault under product={PRODUCT}/country={COUNTRY}"
        )
    df = pd.concat(dfs, ignore_index=True)
    # Normalise column names for bitemporal_core compatibility
    # bitemporal_core expects: data_timestamp, published_date, as_of_date,
    #   conversion_timestamp, revision_number, superseded_by, record_id
    if "reporting_date" in df.columns and "data_timestamp" not in df.columns:
        df["data_timestamp"] = df["reporting_date"]
    # bitemporal_core expects source_series_id; housing uses sovereign_series_id
    if "sovereign_series_id" in df.columns and "source_series_id" not in df.columns:
        df["source_series_id"] = df["sovereign_series_id"]
    logger.info(f"  Loaded {len(df):,} records from {len(dfs)} partitions")
    return df


# ── Housing-specific check ────────────────────────────────────────────────────

def _check_revision_integrity(df: pd.DataFrame) -> dict:
    """revision_number ≥ 0; zero at initial load is expected."""
    neg = int((df["revision_number"] < 0).sum()) if "revision_number" in df.columns else 0
    sup = int(df["superseded_by"].notna().sum()) if "superseded_by" in df.columns else 0
    if neg > 0:
        return {
            "status": "FAIL",
            "check": "Revision Integrity",
            "message": f"{neg} negative revision_number values",
            "details": {"negative_count": neg},
        }
    return {
        "status": "PASS",
        "check": "Revision Integrity",
        "message": (
            f"All {len(df):,} records have revision_number ≥ 0 "
            f"(superseded: {sup}; all-zero at initial load is expected)"
        ),
        "details": {"all_non_negative": True, "superseded_count": sup},
    }


def _check_shelter_cpi_index_range(df: pd.DataFrame) -> dict:
    """BLS CPI Shelter INDEX values should be within plausible historical range (1-1500)."""
    shelter = df[df.get("__source__", pd.Series(dtype=str)) == "bls_cpi_shelter"] if "__source__" in df.columns else pd.DataFrame()
    if shelter.empty or df.empty:
        # Fall back to source_agency
        shelter = df[df.get("source", pd.Series(dtype=str)) == "bls_cpi_shelter"] if "source" in df.columns else pd.DataFrame()
    if shelter.empty:
        return {"status": "SKIP", "check": "Shelter CPI Index Range",
                "message": "No shelter CPI records found in dataset"}

    val_col = "observed_value" if "observed_value" in shelter.columns else "metric_value"
    if val_col not in shelter.columns:
        return {"status": "SKIP", "check": "Shelter CPI Index Range",
                "message": f"{val_col} column missing"}

    vals = pd.to_numeric(shelter[val_col], errors="coerce").dropna()
    out_low  = int((vals < 1.0).sum())
    out_high = int((vals > 1_500.0).sum())
    if out_low == 0 and out_high == 0:
        return {
            "status": "PASS",
            "check": "Shelter CPI Index Range",
            "message": f"All shelter INDEX values within [1, 1500] "
                       f"(min={vals.min():.2f}, max={vals.max():.2f})",
        }
    return {
        "status": "WARN",
        "check": "Shelter CPI Index Range",
        "message": f"{out_low} below 1.0, {out_high} above 1500 in shelter CPI",
        "actual_min": float(vals.min()),
        "actual_max": float(vals.max()),
    }


# ── Runner ────────────────────────────────────────────────────────────────────

def run() -> bool:
    logger.info("=" * 70)
    logger.info("HOUSING — BITEMPORAL PIT VALIDATION")
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
        check_knowledge_monotonicity(df, sample=50, min_len=6, min_year=1980),
        check_bitemporal_uniqueness(df),
        _check_revision_integrity(df),
        check_supersession_integrity(df),
        _check_shelter_cpi_index_range(df),
    ]

    return write_report(REPORT_JSON, REPORT_TXT, PRODUCT, COUNTRY, results)


if __name__ == "__main__":
    sys.exit(0 if run() else 1)
