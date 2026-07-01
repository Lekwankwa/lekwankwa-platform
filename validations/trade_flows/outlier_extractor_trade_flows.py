"""
Outlier Extractor — Trade Flows (US Census FT-900)

Detects anomalies in monthly HS2-chapter trade values and writes
outliers.parquet to each year partition.

DETECTION METHODOLOGY (mirrors housing outlier approach):
  - Load full history per HS series before computing statistics
    (prevents year-boundary MoM breaks)
  - Compute MoM % change per series: (value - prev) / prev * 100
  - Compute per-series Z-score on MoM % change (NOT on raw levels)
  - Flag records exceeding EITHER:
      * Hard threshold: |MoM%| > MOM_THRESHOLD_PCT (30%)
      * Statistical:    Z-score > Z_SCORE_THRESHOLD (3.5σ)

Known major trade events are tagged for context (not excluded).

OUTPUT:
  lekwankwa-historical-vault/product=trade_flows/.../year=YYYY/outliers.parquet

Author: Lekwankwa Corporation
Date: 2026-06-13
"""

import logging
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Dict

import numpy as np
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[
        logging.FileHandler("trade_flows_outlier_extraction.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONFIGURATION
# =============================================================================

VAULT_DIR    = Path("lekwankwa-historical-vault")
PRODUCT      = "trade_flows"
COUNTRY      = "USA"
SOURCE       = "census_ft900"

VALUE_COL    = "observed_value"
SERIES_COL   = "sovereign_series_id"

MOM_THRESHOLD_PCT = 30.0   # hard threshold: |MoM%| > 30% flags an outlier
Z_SCORE_THRESHOLD = 3.5    # statistical: Z-score > 3.5σ on MoM% distribution

OUTLIER_COLUMNS = [
    "record_id", "source", "commodity_code", "trade_flow",
    VALUE_COL, "data_timestamp", SERIES_COL,
    "outlier_type", "outlier_severity", "outlier_reason",
    "value_change_pct", "previous_value",
    "outlier_detected_at", "outlier_detection_method",
]

# Major trade events for context annotation
KNOWN_EVENTS = {
    1998: "Asian Financial Crisis — trade contraction",
    2001: "September 11 + tech recession — trade disruption",
    2002: "Post-9/11 recovery + trade policy shifts",
    2008: "Global Financial Crisis onset — trade collapse",
    2009: "Global Financial Crisis — largest post-war trade contraction",
    2015: "Oil price collapse — major shift in HS27 (Mineral Fuels)",
    2020: "COVID-19 pandemic — historic trade shock",
    2021: "COVID-19 recovery — supply chain disruptions",
    2022: "Russia-Ukraine war — commodity price shocks",
}


# =============================================================================
# CORE DETECTION
# =============================================================================

def _load_source_df() -> pd.DataFrame:
    """Load the COMPLETE vault history for the source (required for accurate MoM statistics)."""
    base  = VAULT_DIR / f"product={PRODUCT}" / f"country={COUNTRY}" / f"source={SOURCE}"
    files = [f for f in base.rglob("*.parquet")
             if "outliers" not in f.name and "changelog" not in f.name]
    dfs   = []
    for f in files:
        try:
            dfs.append(pd.read_parquet(f))
        except Exception as exc:
            logger.warning(f"  Skipping {f}: {exc}")
    if not dfs:
        return pd.DataFrame()
    return pd.concat(dfs, ignore_index=True)


def _build_mom_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Compute per-series MoM% change and Z-score on the full history.

    Returns df with added columns:
      _mom_pct  : month-over-month % change
      _z        : Z-score of _mom_pct within that series
    """
    df = df.copy()
    df[VALUE_COL] = pd.to_numeric(df[VALUE_COL], errors="coerce")

    ts_col = "data_timestamp"
    df[ts_col] = pd.to_datetime(df[ts_col], errors="coerce", utc=True)
    df = df.sort_values([SERIES_COL, ts_col])

    df["_prev"] = df.groupby(SERIES_COL)[VALUE_COL].shift(1)
    df["_mom_pct"] = np.where(
        (df["_prev"].notna()) & (df["_prev"] != 0),
        (df[VALUE_COL] - df["_prev"]) / df["_prev"].abs() * 100,
        np.nan,
    )

    def z_score(x: pd.Series) -> pd.Series:
        std = x.std(ddof=1)
        if std == 0 or pd.isna(std) or len(x.dropna()) < 4:
            return pd.Series(np.nan, index=x.index)
        return (x - x.mean()) / std

    df["_z"] = df.groupby(SERIES_COL)["_mom_pct"].transform(z_score)
    return df


def detect_value_spikes(df: pd.DataFrame, year: int) -> List[Dict]:
    """
    Detect MoM spikes using both hard threshold and Z-score.
    Operates on the FULL history df (pre-computed by _build_mom_df).
    """
    outliers = []
    flagged  = df[
        (
            (df["_mom_pct"].abs() > MOM_THRESHOLD_PCT) |
            (df["_z"].abs()       > Z_SCORE_THRESHOLD)
        ) &
        df["_mom_pct"].notna()
        & (pd.to_datetime(df["data_timestamp"], utc=True).dt.year == year)
    ]

    known_event = KNOWN_EVENTS.get(year, "")

    for _, row in flagged.iterrows():
        pct       = row["_mom_pct"]
        z         = row["_z"]
        otype     = "trade_spike" if pct > 0 else "trade_drop"
        severity  = "critical" if abs(z) > 5.0 else "high" if abs(z) > 4.0 else "medium"
        reason    = f"MoM {pct:+.1f}% (Z={z:.2f}σ)"
        if known_event:
            reason += f" — {known_event}"
        detection = "mom_pct_change" if abs(pct) > MOM_THRESHOLD_PCT else "z_score_3_5sigma"
        if abs(pct) > MOM_THRESHOLD_PCT and abs(z) > Z_SCORE_THRESHOLD:
            detection = "mom_pct_and_z_score"

        outliers.append({
            "record_id":               row.get("record_id"),
            "source":                  row.get("source"),
            "commodity_code":          row.get("commodity_code"),
            "trade_flow":              row.get("trade_flow"),
            VALUE_COL:                 row.get(VALUE_COL),
            "data_timestamp":          row.get("data_timestamp"),
            SERIES_COL:                row.get(SERIES_COL),
            "outlier_type":            otype,
            "outlier_severity":        severity,
            "outlier_reason":          reason,
            "value_change_pct":        round(float(pct), 4) if pd.notna(pct) else None,
            "previous_value":          float(row["_prev"]) if pd.notna(row.get("_prev")) else None,
            "outlier_detected_at":     datetime.now().isoformat(),
            "outlier_detection_method": detection,
        })
    return outliers


def detect_null_values(df: pd.DataFrame, year: int) -> List[Dict]:
    """Flag records with null / non-numeric observed_value in the given year."""
    outliers = []
    ts = pd.to_datetime(df["data_timestamp"], errors="coerce", utc=True)
    year_mask = ts.dt.year == year
    null_rows = df[year_mask & pd.to_numeric(df[VALUE_COL], errors="coerce").isna()]
    for _, row in null_rows.iterrows():
        outliers.append({
            "record_id":               row.get("record_id"),
            "source":                  row.get("source"),
            "commodity_code":          row.get("commodity_code"),
            "trade_flow":              row.get("trade_flow"),
            VALUE_COL:                 None,
            "data_timestamp":          row.get("data_timestamp"),
            SERIES_COL:                row.get(SERIES_COL),
            "outlier_type":            "null_value",
            "outlier_severity":        "medium",
            "outlier_reason":          f"{VALUE_COL} is null or non-numeric",
            "value_change_pct":        None,
            "previous_value":          None,
            "outlier_detected_at":     datetime.now().isoformat(),
            "outlier_detection_method": "null_check",
        })
    return outliers


# =============================================================================
# VAULT WRITER
# =============================================================================

# =============================================================================
# VAULT WRITER
# =============================================================================

def write_outliers_for_year(year: int, outlier_list: List[Dict]) -> None:
    if not outlier_list:
        return
    df_out = pd.DataFrame(outlier_list)
    for col in OUTLIER_COLUMNS:
        if col not in df_out.columns:
            df_out[col] = None
    
    # Group outliers by month and write to month partitions
    df_out['_month'] = pd.to_datetime(df_out['data_timestamp']).dt.month
    for month, group in df_out.groupby('_month'):
        path = (VAULT_DIR / f"product={PRODUCT}" / f"country={COUNTRY}"
                / f"source={SOURCE}" / f"year={year}" / f"month={month:02d}" / "outliers.parquet")
        path.parent.mkdir(parents=True, exist_ok=True)
        group[OUTLIER_COLUMNS].to_parquet(path, engine="pyarrow", index=False)
    logger.info(f"  year={year}: {len(df_out)} outliers written to month partitions")


# =============================================================================
# MAIN
# =============================================================================

def run():
    logger.info("=" * 70)
    logger.info("TRADE FLOWS — OUTLIER EXTRACTION")
    logger.info("=" * 70)
    logger.info(f"  Thresholds: MoM |%| > {MOM_THRESHOLD_PCT}% OR Z-score > {Z_SCORE_THRESHOLD}σ")

    full_df = _load_source_df()
    if full_df.empty:
        logger.warning("  No trade_flows data in vault. Run scraper first.")
        return True   # not a failure — just nothing to process

    logger.info(f"  Full vault loaded: {len(full_df):,} records")

    # Build MoM% and Z-score on the COMPLETE history
    mom_df = _build_mom_df(full_df)

    years = sorted(
        pd.to_datetime(mom_df["data_timestamp"], utc=True).dt.year.dropna().unique()
    )
    total_outliers = 0

    for year in years:
        year_outliers  = detect_value_spikes(mom_df, year)
        year_outliers += detect_null_values(full_df, year)

        if year_outliers:
            write_outliers_for_year(year, year_outliers)
            total_outliers += len(year_outliers)
        # else: silently skip clean years

    logger.info("")
    logger.info(f"  Total outliers detected: {total_outliers}")
    if total_outliers > 0:
        logger.info(f"  outliers.parquet written to each affected year partition")
    return True


if __name__ == "__main__":
    success = run()
    sys.exit(0 if success else 1)
