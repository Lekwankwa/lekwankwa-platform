"""
ALFRED API Client — St. Louis Fed Archival FRED

Fetches complete vintage/revision history for any FRED series.
One API call returns all observation-vintage pairs for the full history.

ALFRED observation response columns:
  realtime_start  — date this value first appeared in FRED (= official_release_date)
  realtime_end    — date this value was superseded (9999-12-31 = current)
  date            — data period date (= data_timestamp)
  value           — reported value for that vintage

Author: Lekwankwa Corporation
Date: 2026-06-16
"""

import logging
import os
import time
from typing import Optional

import pandas as pd
import requests
import urllib3

urllib3.disable_warnings()

logger = logging.getLogger(__name__)

FRED_BASE = "https://api.stlouisfed.org/fred"
ALFRED_KEY = os.getenv("ALFRED_API_KEY") or os.getenv("FRED_API_KEY", "136178f657b4aba7ad9e55938a1473bd")
REALTIME_START = "1900-01-01"
REALTIME_END   = "9999-12-31"
REQUEST_DELAY  = 0.35   # seconds between calls — FRED rate limit is ~120/min


def _get(endpoint: str, params: dict, retries: int = 3) -> Optional[dict]:
    params = {**params, "api_key": ALFRED_KEY, "file_type": "json"}
    url = f"{FRED_BASE}/{endpoint}"
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, verify=False, timeout=30)
            if r.status_code == 200:
                return r.json()
            if r.status_code == 400:
                logger.warning(f"Series not on ALFRED: {params.get('series_id')} (400)")
                return None
            logger.warning(f"HTTP {r.status_code} for {params.get('series_id')}, attempt {attempt+1}")
            time.sleep(2 ** attempt)
        except Exception as exc:
            logger.warning(f"Request error ({exc}), attempt {attempt+1}")
            time.sleep(2 ** attempt)
    return None


def fetch_all_vintages(series_id: str) -> pd.DataFrame:
    """
    Return full revision history for series_id as a DataFrame.

    Columns: date, realtime_start, realtime_end, value
      date            — data period (YYYY-MM-DD, first of month for monthly)
      realtime_start  — vintage date (official_release_date)
      realtime_end    — date superseded
      value           — reported value at that vintage
    """
    time.sleep(REQUEST_DELAY)
    data = _get("series/observations", {
        "series_id":      series_id,
        "realtime_start": REALTIME_START,
        "realtime_end":   REALTIME_END,
        # output_type omitted → default (1): returns one row per
        # (data_date, realtime_period) pair — full revision history
    })
    if not data or "observations" not in data:
        logger.warning(f"No ALFRED data for {series_id}")
        return pd.DataFrame()

    df = pd.DataFrame(data["observations"])
    if df.empty:
        return df

    df["date"]           = pd.to_datetime(df["date"], errors="coerce")
    df["realtime_start"] = pd.to_datetime(df["realtime_start"], errors="coerce")
    df["realtime_end"]   = pd.to_datetime(df["realtime_end"],   errors="coerce")
    df["value"]          = pd.to_numeric(df["value"], errors="coerce")

    # Drop rows with missing date or missing value (marked "." by FRED for missing)
    df = df.dropna(subset=["date", "value"])
    df = df.sort_values(["date", "realtime_start"]).reset_index(drop=True)
    return df


def build_vintage_rows(
    series_id:        str,
    fred_df:          pd.DataFrame,
    source_prefix:    str,
    schema_fields:    dict,
    vintage_id_fn=    None,
) -> pd.DataFrame:
    """
    Convert raw ALFRED observations into vault-ready vintage rows.

    Args:
        series_id       ALFRED/FRED series ID
        fred_df         output of fetch_all_vintages()
        source_prefix   prefix for data_vintage_id (e.g. "BLS", "CENSUS")
        schema_fields   dict of constant fields to attach to every row
        vintage_id_fn   optional callable(series_id, date, n) -> str
                        overrides default data_vintage_id pattern

    Returns DataFrame with one row per (date, vintage) pair, conforming
    to the gold-standard schema fields required for PIT tracking.
    """
    if fred_df.empty:
        return pd.DataFrame()

    rows = []
    for date, grp in fred_df.groupby("date"):
        grp = grp.sort_values("realtime_start").reset_index(drop=True)
        for n, (_, obs) in enumerate(grp.iterrows(), start=1):
            yyyy_mm = date.strftime("%Y-%m")
            if vintage_id_fn:
                vid = vintage_id_fn(series_id, date, n)
            else:
                vid = f"{source_prefix}-{series_id}-{yyyy_mm}-v{n}"

            row = {
                **schema_fields,
                "sovereign_series_id":  series_id,
                "data_vintage_id":      vid,
                "reporting_date":       date.strftime("%Y-%m-%d"),
                "data_timestamp":       date.isoformat(),
                "official_release_date": obs["realtime_start"].strftime("%Y-%m-%d"),
                "as_of_date":           obs["realtime_start"].isoformat() + "Z",
                "published_date":       obs["realtime_start"].strftime("%Y-%m-%d"),
                "observed_value":       obs["value"],
                "is_revised_figure":    (n > 1),
                "confidence_tier":      "PRIMARY",
                "revision_number":      n,
                "source_system":        "ALFRED",
            }
            rows.append(row)

    return pd.DataFrame(rows)


def series_on_alfred(series_id: str) -> bool:
    """Quick check — returns True if the series has ALFRED vintage data."""
    time.sleep(REQUEST_DELAY)
    data = _get("series/vintagedates", {"series_id": series_id})
    return data is not None and len(data.get("vintage_dates", [])) > 0
