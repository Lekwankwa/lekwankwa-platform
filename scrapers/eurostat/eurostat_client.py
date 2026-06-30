"""
Eurostat Statistics REST API client.

Endpoint:
  https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data/{dataset}
  ?{dim}={val}&{dim}={val}&format=JSON&lang=en

Response format: JSON-stat (compact JSON with flat value array).

Public API:
  fetch_dataset(dataset_id, filters, geo_list, start_period, end_period)
      → pd.DataFrame  (columns = dimension IDs + "value" + "status")

  The caller is responsible for all business-logic transformations.
  This module handles only HTTP + JSON-stat parsing.

Rate-limiting:
  Eurostat recommends ≤ 1 large request/second.
  A 0.4 s sleep is inserted between consecutive calls by default.
  For robustness, up to MAX_RETRIES exponential-backoff retries are attempted.
"""

from __future__ import annotations

import itertools
import logging
import time
from typing import Any, Optional

import requests
import urllib3
import pandas as pd

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = logging.getLogger(__name__)

BASE_URL    = "https://ec.europa.eu/eurostat/api/dissemination/statistics/1.0/data"
MAX_RETRIES = 4
RETRY_SLEEP = [2, 5, 15, 30]   # seconds per attempt
CALL_SLEEP  = 0.4               # polite pause between successive requests


# ---------------------------------------------------------------------------
# Internal: JSON-stat parser
# ---------------------------------------------------------------------------

def _parse_jsonstat(raw: dict) -> pd.DataFrame:
    """
    Convert a Eurostat JSON-stat response to a flat DataFrame.

    Columns: one per dimension ID (values are string codes) + "value" (float)
    and optionally "status" (e.g., "e"=estimated, "p"=provisional, "b"=break).
    Rows with null/missing values are dropped.
    """
    ids    = raw.get("id", [])
    sizes  = raw.get("size", [])
    dims   = raw.get("dimension", {})
    values = raw.get("value", {})
    status = raw.get("status", {})

    if not ids or not values:
        return pd.DataFrame()

    # Build position → code lookup for every dimension
    dim_pos_to_code: list[dict[int, str]] = []
    for dim_id in ids:
        cat = dims.get(dim_id, {}).get("category", {})
        idx = cat.get("index", {})
        # idx is {code: position}
        pos_to_code: dict[int, str] = {int(pos): code for code, pos in idx.items()}
        dim_pos_to_code.append(pos_to_code)

    rows: list[dict] = []
    for flat_key_str, val in values.items():
        if val is None:
            continue
        flat_idx = int(flat_key_str)

        # Decompose flat index into per-dimension positions (right-to-left)
        positions: list[int] = []
        remaining = flat_idx
        for s in reversed(sizes):
            positions.insert(0, remaining % s)
            remaining //= s

        row: dict[str, Any] = {}
        for i, dim_id in enumerate(ids):
            row[dim_id] = dim_pos_to_code[i].get(positions[i], str(positions[i]))
        row["value"]  = float(val)
        row["status"] = status.get(flat_key_str)
        rows.append(row)

    return pd.DataFrame(rows)


# ---------------------------------------------------------------------------
# Public: fetch one dataset
# ---------------------------------------------------------------------------

def fetch_dataset(
    dataset_id:   str,
    filters:      dict[str, Any],   # dimension → value(s); value may be str or list[str]
    geo_list:     list[str],         # Eurostat geo codes (AT, BE, …, EL for Greece)
    start_period: Optional[str] = None,
    end_period:   Optional[str]  = None,
) -> pd.DataFrame:
    """
    Fetch a Eurostat dataset for all countries in geo_list at once.

    Parameters
    ----------
    dataset_id    Eurostat dataflow ID, e.g. "prc_hicp_midx"
    filters       Dimension filters, e.g. {"unit": "INX_A_AVG", "freq": "M"}
                  Values may be a list for multi-select: {"coicop": ["CP0111","CP0112"]}
    geo_list      Eurostat 2-letter geo codes, e.g. ["AT","DE","EL"]
    start_period  ISO period string: "2000-01" (monthly) or "2000-Q1" (quarterly)
    end_period    ISO period string; if None, fetch up to the latest available

    Returns
    -------
    DataFrame with dimension-code columns + "value" (float) + "status".
    Empty DataFrame if the API returns nothing or the call fails.
    """
    # Build query params (list of (key, value) tuples to allow repeated keys)
    params: list[tuple[str, str]] = [("format", "JSON"), ("lang", "en")]

    for dim, val in filters.items():
        if isinstance(val, (list, tuple)):
            for v in val:
                params.append((dim, str(v)))
        else:
            params.append((dim, str(val)))

    for geo in geo_list:
        params.append(("geo", geo))

    if start_period:
        params.append(("sinceTimePeriod", start_period))
    if end_period:
        params.append(("untilTimePeriod", end_period))

    url = f"{BASE_URL}/{dataset_id}"

    for attempt, sleep_sec in enumerate(RETRY_SLEEP[:MAX_RETRIES]):
        try:
            log.info("Eurostat GET %s attempt %d  params=%s", dataset_id, attempt + 1,
                     dict(params[:6]))
            resp = requests.get(url, params=params, timeout=120, verify=False)

            if resp.status_code == 200:
                raw = resp.json()
                df  = _parse_jsonstat(raw)
                log.info("  -> %d observations", len(df))
                time.sleep(CALL_SLEEP)
                return df

            if resp.status_code in (429, 503):
                # Rate-limited or temporarily unavailable — wait and retry
                log.warning("  HTTP %d — sleeping %ds before retry", resp.status_code, sleep_sec)
                time.sleep(sleep_sec)
                continue

            # Non-retryable error
            log.error("  HTTP %d for %s: %s", resp.status_code, dataset_id,
                      resp.text[:300])
            return pd.DataFrame()

        except requests.Timeout:
            log.warning("  Timeout on attempt %d — sleeping %ds", attempt + 1, sleep_sec)
            time.sleep(sleep_sec)
        except Exception as exc:
            log.error("  Unexpected error: %s", exc)
            return pd.DataFrame()

    log.error("All %d retries exhausted for %s", MAX_RETRIES, dataset_id)
    return pd.DataFrame()


# ---------------------------------------------------------------------------
# Utility: period string normalisation
# ---------------------------------------------------------------------------

def period_to_date(period_str: str) -> Optional[pd.Timestamp]:
    """
    Convert a Eurostat period string to the first calendar date of that period.

    Supports:
      "2024-01"   → 2024-01-01  (monthly)
      "2024-Q1"   → 2024-01-01  (quarterly)
      "2024-Q2"   → 2024-04-01
      "2024-Q3"   → 2024-07-01
      "2024-Q4"   → 2024-10-01
      "2024"      → 2024-01-01  (annual)
    """
    s = period_str.strip()
    try:
        # Quarterly: "YYYY-QN"
        if "-Q" in s:
            year, q = s.split("-Q")
            month_start = (int(q) - 1) * 3 + 1
            return pd.Timestamp(int(year), month_start, 1)
        # Monthly: "YYYY-MM"
        if len(s) == 7 and s[4] == "-":
            return pd.Timestamp(s + "-01")
        # Annual: "YYYY"
        if len(s) == 4 and s.isdigit():
            return pd.Timestamp(int(s), 1, 1)
    except Exception:
        pass
    return None
