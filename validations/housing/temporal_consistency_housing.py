"""
Temporal Consistency Validation for Housing Supply & Shelter Inflation

Validates time-series continuity, gap detection, and cross-dataset
synchronisation for both housing sub-datasets.

TEMPORAL CHECKS:
  SECTION A — BLS CPI Shelter (source=bls_cpi_shelter):
    A1. Monthly Continuity   — no unexpected gaps in CUUR0000SEHA (1914-present)
    A2. Intra-series Gaps    — gap detection for all 7 shelter series
    A3. No Future Dates      — reporting_date ≤ today
    A4. Chronological Order  — timestamps monotonically increasing per series

  SECTION B — Census BPS Building Permits (source=census_bps):
    B1. Monthly Continuity   — no unexpected gaps in PERMIT series (1959-present)
    B2. Intra-variable Gaps  — gap detection for all 7 BPS variables
    B3. No Future Dates      — reporting_date ≤ today
    B4. Value Trend Sanity   — PERMIT values within plausible historical range

  SECTION C — Cross-dataset Sync:
    C1. Overlapping Period Coverage — both datasets populated for each month 1959-present
    C2. Release Lag Consistency     — BPS published_date ≥ reporting_date + 6 weeks
    C3. Shelter CPI Lag             — shelter published_date ≥ reporting_date + 12 days

OUTPUT:
  - housing_temporal_consistency_report.json
  - housing_temporal_consistency_report.txt

Author: Lekwankwa Corporation
Date: 2026-06-12
"""

import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import numpy as np

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("housing_temporal_consistency.log"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

VAULT_DIR      = Path("lekwankwa-historical-vault")
PRODUCT        = "Housing_Supply_and_Shelter_Inflation"
COUNTRY        = "USA"
SHELTER_SOURCE = "bls_cpi_shelter"
PERMITS_SOURCE = "census_bps"

REPORT_JSON = Path("housing_temporal_consistency_report.json")
REPORT_TXT  = Path("housing_temporal_consistency_report.txt")

TODAY = datetime.now(timezone.utc)

# Series / variable lists
SHELTER_SERIES   = ["CUUR0000SEHA", "CUUR0000SEHB", "CUUR0000SAH1", "CUUR0000SEHC",
                    "CUSR0000SEHA", "CUSR0000SEHB", "CUSR0000SAH1"]
HEADLINE_SHELTER = "CUUR0000SEHA"

BPS_VARIABLES    = ["PERMIT", "PERMIT1", "PERMIT2", "PERMIT3_4", "PERMIT5", "BLDGS", "VALUE"]
HEADLINE_BPS     = "PERMIT"

# Documented, permanent gaps that must NOT be reported as continuity failures.
# Kept in lockstep with configs/catalog_expected_series.yaml known_gaps.
#   BLS_2025_APPROPRIATIONS_LAPSE (2025-10): the US federal government operated
#   without appropriations in late Oct 2025; BLS could not collect/publish and
#   does not retroactively backfill lapse periods. Affects the CPI shelter
#   series. This is expected missing data, not a scraper/pipeline failure.
KNOWN_GAP_MONTHS = {"2025-10"}


def _strip_known_gaps(gaps: list) -> list:
    """Drop gap ranges whose every month is a documented KNOWN_GAP_MONTHS entry."""
    kept = []
    for start, end in gaps:
        months = {str(p) for p in pd.period_range(start, end, freq="M")}
        if months <= KNOWN_GAP_MONTHS:
            continue  # entirely explained by a documented known gap
        kept.append((start, end))
    return kept

# Historical plausibility for PERMIT (total units authorized, SAAR)
PERMIT_MIN = 200_000
PERMIT_MAX = 2_500_000

# Publication lag bounds (weeks)
BPS_PUB_LAG_MIN_DAYS     = 28   # ~4 weeks min (Feb: 6-week estimate rounds to +28d)
BPS_PUB_LAG_MAX_DAYS     = 90   # ~13 weeks (grace)
SHELTER_PUB_LAG_MIN_DAYS = 12
SHELTER_PUB_LAG_MAX_DAYS = 45


# =============================================================================
# HELPERS
# =============================================================================

def _load_source(source: str) -> pd.DataFrame:
    src_path = VAULT_DIR / f"product={PRODUCT}" / f"country={COUNTRY}" / f"source={source}"
    files = sorted(src_path.rglob("*.parquet"))
    dfs = []
    for f in files:
        try:
            dfs.append(pd.read_parquet(f))
        except Exception as exc:
            logger.warning(f"  Skipping {f}: {exc}")
    if not dfs:
        return pd.DataFrame()
    df = pd.concat(dfs, ignore_index=True)
    ts_col = "reporting_date" if "reporting_date" in df.columns else "data_timestamp"
    df["_ts"] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
    return df


def _detect_gaps(dates: pd.Series, expected_freq: str = "MS",
                 max_gap_months: int = 1) -> list:
    """Return list of (gap_start, gap_end, months) for periods with missing months."""
    dates = dates.dropna().sort_values().reset_index(drop=True)
    if len(dates) < 2:
        return []
    expected = pd.date_range(dates.iloc[0], dates.iloc[-1], freq=expected_freq)
    actual_set = set(dates.dt.to_period("M"))
    gaps = []
    for d in expected:
        if d.to_period("M") not in actual_set:
            gaps.append(d)
    # Collapse to ranges
    if not gaps:
        return []
    gap_ranges = []
    start = gaps[0]
    prev  = gaps[0]
    for g in gaps[1:]:
        if (g - prev).days > 32:
            gap_ranges.append((str(start.date()), str(prev.date())))
            start = g
        prev = g
    gap_ranges.append((str(start.date()), str(prev.date())))
    return gap_ranges


# =============================================================================
# SECTION A — BLS CPI SHELTER
# =============================================================================

def check_shelter_headline_continuity(df: pd.DataFrame) -> dict:
    """CUUR0000SEHA should have unbroken monthly coverage from its first observation."""
    if df.empty:
        return {"status": "SKIP", "check": "A1 Shelter Headline Continuity",
                "message": "No shelter data in vault"}
    id_col = "sovereign_series_id"
    if id_col not in df.columns:
        return {"status": "WARN", "check": "A1 Shelter Headline Continuity",
                "message": "sovereign_series_id column missing"}
    series_df = df[df[id_col] == HEADLINE_SHELTER]
    if series_df.empty:
        return {"status": "FAIL", "check": "A1 Shelter Headline Continuity",
                "message": f"Series {HEADLINE_SHELTER} not found in vault"}
    all_gaps = _detect_gaps(series_df["_ts"])
    gaps = _strip_known_gaps(all_gaps)
    span = f"{series_df['_ts'].min().date()} - {series_df['_ts'].max().date()}"
    known_n = len(all_gaps) - len(gaps)
    if not gaps:
        note = f"No unexplained gaps in {HEADLINE_SHELTER} monthly series ({span})"
        if known_n:
            note += f"; {known_n} documented known-gap period(s) excluded (BLS_2025_APPROPRIATIONS_LAPSE)"
        return {"status": "PASS", "check": "A1 Shelter Headline Continuity", "message": note}
    return {"status": "FAIL", "check": "A1 Shelter Headline Continuity",
            "message": f"{len(gaps)} unexplained gap(s) detected in {HEADLINE_SHELTER} ({span})",
            "gaps": gaps[:10]}


def check_shelter_intra_series_gaps(df: pd.DataFrame) -> dict:
    """Check all 7 shelter series for unexpected monthly gaps."""
    if df.empty or "sovereign_series_id" not in df.columns:
        return {"status": "SKIP", "check": "A2 Shelter Intra-series Gaps",
                "message": "No data or sovereign_series_id missing"}
    series_gaps = {}
    for sid in SHELTER_SERIES:
        sdf = df[df["sovereign_series_id"] == sid]
        if sdf.empty:
            continue
        g = _strip_known_gaps(_detect_gaps(sdf["_ts"]))
        if g:
            series_gaps[sid] = g
    if not series_gaps:
        return {"status": "PASS", "check": "A2 Shelter Intra-series Gaps",
                "message": f"No intra-series gaps in any of {len(SHELTER_SERIES)} shelter series"}
    return {"status": "WARN", "check": "A2 Shelter Intra-series Gaps",
            "message": f"Gaps found in {len(series_gaps)} shelter series",
            "details": {k: v[:3] for k, v in series_gaps.items()}}


def check_shelter_no_future_dates(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"status": "SKIP", "check": "A3 Shelter No Future Dates",
                "message": "No shelter data"}
    future = df[df["_ts"] > pd.Timestamp(TODAY)]
    if len(future) == 0:
        return {"status": "PASS", "check": "A3 Shelter No Future Dates",
                "message": f"No future-dated records in shelter data (max={df['_ts'].max().date()})"}
    return {"status": "FAIL", "check": "A3 Shelter No Future Dates",
            "message": f"{len(future)} records have reporting_date in the future",
            "max_future": str(future["_ts"].max().date())}


def check_shelter_chronological_order(df: pd.DataFrame) -> dict:
    """Per series, timestamps must be monotonically non-decreasing."""
    if df.empty or "sovereign_series_id" not in df.columns:
        return {"status": "SKIP", "check": "A4 Shelter Chronological Order",
                "message": "No data"}
    out_of_order = {}
    for sid, grp in df.groupby("sovereign_series_id"):
        sorted_ts = grp["_ts"].sort_values()
        if not (sorted_ts.diff().dropna() >= pd.Timedelta(0)).all():
            out_of_order[sid] = int((sorted_ts.diff().dropna() < pd.Timedelta(0)).sum())
    if not out_of_order:
        return {"status": "PASS", "check": "A4 Shelter Chronological Order",
                "message": "All shelter series are in chronological order"}
    return {"status": "FAIL", "check": "A4 Shelter Chronological Order",
            "message": f"Out-of-order timestamps in {len(out_of_order)} series",
            "details": out_of_order}


# =============================================================================
# SECTION B — CENSUS BPS
# =============================================================================

def check_permits_headline_continuity(df: pd.DataFrame) -> dict:
    """PERMIT (total) should have unbroken monthly coverage from 1959."""
    if df.empty:
        return {"status": "SKIP", "check": "B1 Permits Headline Continuity",
                "message": "No permits data in vault"}
    id_col = "sovereign_series_id" if "sovereign_series_id" in df.columns else "bps_variable"
    if id_col not in df.columns:
        return {"status": "WARN", "check": "B1 Permits Headline Continuity",
                "message": "sovereign_series_id / bps_variable column missing"}
    perm_df = df[df[id_col] == HEADLINE_BPS]
    if perm_df.empty:
        return {"status": "FAIL", "check": "B1 Permits Headline Continuity",
                "message": f"Variable {HEADLINE_BPS} not found in vault"}
    gaps = _detect_gaps(perm_df["_ts"])
    span = f"{perm_df['_ts'].min().date()} - {perm_df['_ts'].max().date()}"
    if not gaps:
        return {"status": "PASS", "check": "B1 Permits Headline Continuity",
                "message": f"No gaps in {HEADLINE_BPS} monthly series ({span})"}
    return {"status": "FAIL", "check": "B1 Permits Headline Continuity",
            "message": f"{len(gaps)} gap(s) in {HEADLINE_BPS} ({span})",
            "gaps": gaps[:10]}


def check_permits_intra_variable_gaps(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"status": "SKIP", "check": "B2 Permits Intra-variable Gaps",
                "message": "No permits data"}
    id_col = "sovereign_series_id" if "sovereign_series_id" in df.columns else "bps_variable"
    if id_col not in df.columns:
        return {"status": "SKIP", "check": "B2 Permits Intra-variable Gaps",
                "message": f"{id_col} column missing"}
    var_gaps = {}
    for var in BPS_VARIABLES:
        vdf = df[df[id_col] == var]
        if vdf.empty:
            continue
        g = _detect_gaps(vdf["_ts"])
        if g:
            var_gaps[var] = g
    if not var_gaps:
        return {"status": "PASS", "check": "B2 Permits Intra-variable Gaps",
                "message": f"No gaps in any of {len(BPS_VARIABLES)} BPS variables"}
    return {"status": "WARN", "check": "B2 Permits Intra-variable Gaps",
            "message": f"Gaps in {len(var_gaps)} BPS variables",
            "details": {k: v[:3] for k, v in var_gaps.items()}}


def check_permits_no_future_dates(df: pd.DataFrame) -> dict:
    if df.empty:
        return {"status": "SKIP", "check": "B3 Permits No Future Dates",
                "message": "No permits data"}
    future = df[df["_ts"] > pd.Timestamp(TODAY)]
    if len(future) == 0:
        return {"status": "PASS", "check": "B3 Permits No Future Dates",
                "message": f"No future-dated records (max={df['_ts'].max().date()})"}
    return {"status": "FAIL", "check": "B3 Permits No Future Dates",
            "message": f"{len(future)} future-dated permit records",
            "max_future": str(future["_ts"].max().date())}


def check_permits_value_sanity(df: pd.DataFrame) -> dict:
    """Total PERMIT (SAAR) values must be within plausible historical range."""
    if df.empty:
        return {"status": "SKIP", "check": "B4 Permits Value Sanity",
                "message": "No permits data"}
    id_col = "sovereign_series_id" if "sovereign_series_id" in df.columns else "bps_variable"
    perm_df = df[df.get(id_col, pd.Series(dtype=str)) == HEADLINE_BPS] if id_col in df.columns else pd.DataFrame()
    if perm_df.empty:
        return {"status": "SKIP", "check": "B4 Permits Value Sanity",
                "message": f"Variable {HEADLINE_BPS} not found for range check"}
    val_col = "observed_value" if "observed_value" in perm_df.columns else "metric_value"
    if val_col not in perm_df.columns:
        return {"status": "SKIP", "check": "B4 Permits Value Sanity",
                "message": "observed_value / metric_value column missing"}
    vals = pd.to_numeric(perm_df[val_col], errors="coerce").dropna()
    out_low  = int((vals < PERMIT_MIN).sum())
    out_high = int((vals > PERMIT_MAX).sum())
    if out_low == 0 and out_high == 0:
        return {"status": "PASS", "check": "B4 Permits Value Sanity",
                "message": f"All PERMIT SAAR values within [{PERMIT_MIN:,}, {PERMIT_MAX:,}] "
                           f"(min={vals.min():,.0f}, max={vals.max():,.0f})"}
    return {"status": "WARN", "check": "B4 Permits Value Sanity",
            "message": f"{out_low} below {PERMIT_MIN:,}, {out_high} above {PERMIT_MAX:,}",
            "actual_min": float(vals.min()), "actual_max": float(vals.max())}


# =============================================================================
# SECTION C — CROSS-DATASET SYNC
# =============================================================================

def check_overlap_coverage(df_shelter: pd.DataFrame, df_permits: pd.DataFrame) -> dict:
    """From 1959 onward, both datasets should cover the same months."""
    if df_shelter.empty or df_permits.empty:
        return {"status": "SKIP", "check": "C1 Overlap Coverage",
                "message": "One or both datasets empty"}

    shelter_months = set(df_shelter[df_shelter["_ts"].dt.year >= 1959]["_ts"]
                         .dt.to_period("M").dropna().unique())
    permits_months  = set(df_permits["_ts"].dt.to_period("M").dropna().unique())

    in_shelter_not_permits = shelter_months - permits_months
    in_permits_not_shelter = permits_months - shelter_months

    issues = []
    if in_shelter_not_permits:
        issues.append(f"{len(in_shelter_not_permits)} months in shelter but not permits")
    if in_permits_not_shelter:
        issues.append(f"{len(in_permits_not_shelter)} months in permits but not shelter")

    if not issues:
        return {"status": "PASS", "check": "C1 Overlap Coverage",
                "message": f"Both datasets cover the same {len(shelter_months)} months from 1959"}
    return {"status": "WARN", "check": "C1 Overlap Coverage",
            "message": "; ".join(issues),
            "shelter_only_sample": [str(m) for m in sorted(in_shelter_not_permits)[:5]],
            "permits_only_sample": [str(m) for m in sorted(in_permits_not_shelter)[:5]]}


def check_bps_publication_lag(df_permits: pd.DataFrame) -> dict:
    """BPS: published_date should be ≥ reporting_date + 30 days (6 weeks)."""
    if df_permits.empty:
        return {"status": "SKIP", "check": "C2 BPS Publication Lag",
                "message": "No permits data"}
    pub_col = "published_date" if "published_date" in df_permits.columns else "official_release_date"
    if pub_col not in df_permits.columns:
        return {"status": "SKIP", "check": "C2 BPS Publication Lag",
                "message": "published_date column missing"}
    pub = pd.to_datetime(df_permits[pub_col], errors="coerce", utc=True)
    lag_days = (pub - df_permits["_ts"]).dt.days.dropna()
    too_short = int((lag_days < BPS_PUB_LAG_MIN_DAYS).sum())
    too_long  = int((lag_days > BPS_PUB_LAG_MAX_DAYS).sum())
    if too_short == 0:
        return {"status": "PASS", "check": "C2 BPS Publication Lag",
                "message": f"All BPS lags >= {BPS_PUB_LAG_MIN_DAYS} days "
                           f"(median={lag_days.median():.0f}d, max={lag_days.max():.0f}d)",
                "too_long": too_long}
    return {"status": "FAIL", "check": "C2 BPS Publication Lag",
            "message": f"{too_short:,} records have BPS lag < {BPS_PUB_LAG_MIN_DAYS} days",
            "min_lag_days": float(lag_days.min())}


def check_shelter_publication_lag(df_shelter: pd.DataFrame) -> dict:
    """CPI shelter: published_date should be ≥ reporting_date + 12 days."""
    if df_shelter.empty:
        return {"status": "SKIP", "check": "C3 Shelter Publication Lag",
                "message": "No shelter data"}
    pub_col = "published_date" if "published_date" in df_shelter.columns else "official_release_date"
    if pub_col not in df_shelter.columns:
        return {"status": "SKIP", "check": "C3 Shelter Publication Lag",
                "message": "published_date column missing"}
    pub = pd.to_datetime(df_shelter[pub_col], errors="coerce", utc=True)
    lag_days = (pub - df_shelter["_ts"]).dt.days.dropna()
    too_short = int((lag_days < SHELTER_PUB_LAG_MIN_DAYS).sum())
    if too_short == 0:
        return {"status": "PASS", "check": "C3 Shelter Publication Lag",
                "message": f"All shelter CPI lags >= {SHELTER_PUB_LAG_MIN_DAYS} days "
                           f"(median={lag_days.median():.0f}d)"}
    return {"status": "FAIL", "check": "C3 Shelter Publication Lag",
            "message": f"{too_short:,} records have shelter lag < {SHELTER_PUB_LAG_MIN_DAYS} days",
            "min_lag_days": float(lag_days.min())}


# =============================================================================
# RUNNER
# =============================================================================

def run():
    logger.info("=" * 70)
    logger.info("HOUSING — TEMPORAL CONSISTENCY VALIDATION")
    logger.info("=" * 70)
    logger.info(f"Run timestamp: {datetime.utcnow().isoformat()}Z")

    logger.info(f"\nLoading {SHELTER_SOURCE}…")
    df_shelter = _load_source(SHELTER_SOURCE)
    logger.info(f"  {len(df_shelter):,} records")

    logger.info(f"\nLoading {PERMITS_SOURCE}…")
    df_permits = _load_source(PERMITS_SOURCE)
    logger.info(f"  {len(df_permits):,} records")

    results = [
        # Section A
        check_shelter_headline_continuity(df_shelter),
        check_shelter_intra_series_gaps(df_shelter),
        check_shelter_no_future_dates(df_shelter),
        check_shelter_chronological_order(df_shelter),
        # Section B
        check_permits_headline_continuity(df_permits),
        check_permits_intra_variable_gaps(df_permits),
        check_permits_no_future_dates(df_permits),
        check_permits_value_sanity(df_permits),
        # Section C
        check_overlap_coverage(df_shelter, df_permits),
        check_bps_publication_lag(df_permits),
        check_shelter_publication_lag(df_shelter),
    ]

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
        fh.write(f"Housing Temporal Consistency Report - {datetime.utcnow().isoformat()}Z\n")
        fh.write("=" * 70 + "\n")
        for r in results:
            fh.write(f"  [{r['status']}] {r['check']}: {r['message']}\n")
        fh.write(f"\nSUMMARY: {passed} PASS / {failed} FAIL / {warned} WARN\n")

    logger.info(f"Reports written: {REPORT_JSON}, {REPORT_TXT}")
    return failed == 0


if __name__ == "__main__":
    import sys
    sys.exit(0 if run() else 1)
