"""
Statistics Canada NDM CSV download client.

WDS REST API entity endpoints (/getCubeMetadata, /getDataFromVectorsAndLatestNPeriods,
etc.) return HTTP 404 for all PIDs/vectors — confirmed broken 2026-06-18.
Fallback: direct ZIP/CSV download from the NDM table download endpoint.

Download URL pattern:
    https://www150.statcan.gc.ca/n1/tbl/csv/{table_id}-eng.zip
Each ZIP contains {table_id}.csv with columns:
    REF_DATE, GEO, ..., VECTOR, COORDINATE, VALUE, ...

pit_coverage_type: RELEASE_DATE_ONLY/accumulating
  CSV snapshots contain current values only; no historical vintage retrieval.
  Release date estimated from obs_date + release_lag_days (series-level lag).

Public API:
    fetch_vector(table_id, vector_str)  ->  pd.DataFrame(obs_date, value)
"""

from __future__ import annotations

import io
import logging
import zipfile
from functools import lru_cache

import pandas as pd
import requests
import urllib3

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = logging.getLogger(__name__)

_NDM_BASE = "https://www150.statcan.gc.ca/n1/tbl/csv"
TIMEOUT   = 120

_session: requests.Session | None = None


def _sess() -> requests.Session:
    global _session
    if _session is None:
        s = requests.Session()
        s.verify = False
        s.headers.update({"User-Agent": "lekwankwa-vault/1.0"})
        _session = s
    return _session


@lru_cache(maxsize=8)
def _download_table(table_id: str) -> bytes | None:
    """Download and cache a StatCan NDM ZIP in memory."""
    url = f"{_NDM_BASE}/{table_id}-eng.zip"
    try:
        r = _sess().get(url, timeout=TIMEOUT)
        if r.status_code != 200:
            log.warning("StatCan %s: HTTP %d", table_id, r.status_code)
            return None
        return r.content
    except Exception as e:
        log.warning("StatCan %s: download error: %s", table_id, e)
        return None


def fetch_vector(table_id: str, vector_str: str) -> pd.DataFrame:
    """
    Download a StatCan NDM table and extract one vector's time series.

    table_id:   e.g. "18100004" (without "-eng.zip")
    vector_str: e.g. "v41690974"

    Returns DataFrame with columns: obs_date (Timestamp), value (float).
    REF_DATE values like "2024-01" or "2024-01-01" are coerced to
    first-of-period Timestamps.
    """
    zip_bytes = _download_table(table_id)
    if zip_bytes is None:
        return pd.DataFrame()

    try:
        z = zipfile.ZipFile(io.BytesIO(zip_bytes))
        csv_name = f"{table_id}.csv"
        with z.open(csv_name) as f:
            rows: list[dict] = []
            for chunk in pd.read_csv(f, chunksize=5000, low_memory=False):
                hits = chunk[chunk["VECTOR"] == vector_str]
                for _, row in hits.iterrows():
                    val = row.get("VALUE")
                    if pd.isna(val):
                        continue
                    ref = str(row.get("REF_DATE", ""))
                    try:
                        obs_date = _parse_ref_date(ref)
                    except Exception:
                        continue
                    if obs_date:
                        rows.append({"obs_date": obs_date, "value": float(val)})
    except Exception as e:
        log.warning("StatCan %s/%s: parse error: %s", table_id, vector_str, e)
        return pd.DataFrame()

    if not rows:
        log.warning("StatCan %s/%s: 0 obs after filter", table_id, vector_str)
        return pd.DataFrame()

    df = pd.DataFrame(rows).sort_values("obs_date").reset_index(drop=True)
    return df


def _parse_ref_date(ref: str) -> pd.Timestamp | None:
    """Parse StatCan REF_DATE: '2024-01-01', '2024-01', '2024Q1', '2024'."""
    ref = ref.strip()
    if not ref:
        return None
    # Full date
    if len(ref) == 10 and ref[4] == "-":
        return pd.Timestamp(ref)
    # Year-month: "2024-01"
    if len(ref) == 7 and ref[4] == "-":
        return pd.Timestamp(ref + "-01")
    # Quarterly: "2024Q1" — unlikely but handle
    if "Q" in ref:
        yr, q = ref.split("Q")
        return pd.Timestamp(int(yr), (int(q) - 1) * 3 + 1, 1)
    # Annual
    if len(ref) == 4:
        return pd.Timestamp(int(ref), 1, 1)
    return None
