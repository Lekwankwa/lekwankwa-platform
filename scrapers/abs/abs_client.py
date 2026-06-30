"""
Australian Bureau of Statistics SDMX REST API client.
Base URL: https://api.data.abs.gov.au

All ABS data is RELEASE_DATE_ONLY/structural_ceiling:
  The includeHistory parameter is unsupported (HTTP 500 on all tested
  dataflows). Only current-value series available.

Dimension key format: MEASURE.DIM2.DIM3...FREQ  (dataflow-specific)
Confirmed working dataflows:
  ANA_AGG   GDP quarterly — key: MEASURE.DATA_ITEM.TSEST.REGION.FREQ
  CPI_Q     CPI quarterly — key: MEASURE.INDEX.TSEST.REGION.FREQ
  LF        Labour Force  — key: MEASURE.SEX.AGE.TSEST.REGION.FREQ
  BOP       Balance of Payments — key: MEASURE.DATA_ITEM.TSEST.FREQ
  RPPI      Residential Property Price Index — fetched as all-series

Public API:
    fetch_sdmx(dataflow, key, start_period)  →  pd.DataFrame(obs_date, value)
    fetch_sdmx_all(dataflow, start_period)   →  pd.DataFrame with series_key col
"""

from __future__ import annotations

import logging

import requests
import urllib3
import pandas as pd

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = logging.getLogger(__name__)

BASE    = "https://api.data.abs.gov.au"
TIMEOUT = 60

_session: requests.Session | None = None


def _sess() -> requests.Session:
    global _session
    if _session is None:
        s = requests.Session()
        s.verify = False
        s.headers.update({
            "Accept": "application/vnd.sdmx.data+json;version=1.0",
            "User-Agent": "lekwankwa-vault/1.0",
        })
        _session = s
    return _session


def fetch_sdmx(
    dataflow: str,
    key: str,
    start_period: str | None = "2000",
    series_key_filter: str | None = None,
) -> pd.DataFrame:
    """
    Fetch a specific SDMX key from an ABS dataflow.

    key: e.g. "M1.GPM.20.AUS.Q" or "ALL" to fetch all series.
    series_key_filter: when key="ALL", extract only the series with this
        integer-index key (e.g. "0:0:8:0" for RPPI national houses).
    Returns DataFrame: obs_date (Timestamp), value (float).
    """
    url = f"{BASE}/data/{dataflow}/{key}"
    params: dict[str, str] = {"detail": "dataonly"}
    if start_period:
        params["startPeriod"] = start_period

    try:
        r = _sess().get(url, params=params, timeout=TIMEOUT)
        if r.status_code == 422:
            log.warning("ABS %s/%s: HTTP 422 (wrong key format)", dataflow, key)
            return pd.DataFrame()
        if r.status_code == 404:
            log.warning("ABS %s/%s: HTTP 404 (no data found)", dataflow, key)
            return pd.DataFrame()
        if r.status_code != 200:
            log.warning("ABS %s/%s: HTTP %d", dataflow, key, r.status_code)
            return pd.DataFrame()
        data = r.json()
    except Exception as e:
        log.warning("ABS %s/%s: %s", dataflow, key, e)
        return pd.DataFrame()

    return _parse_sdmx_json(
        data, dataflow=dataflow, key=key, series_key_filter=series_key_filter
    )


def _parse_sdmx_json(
    data: dict, *, dataflow: str = "", key: str = "",
    series_key_filter: str | None = None,
) -> pd.DataFrame:
    """Parse SDMX-JSON 1.0 response into flat DataFrame.

    ABS returns data.structure (singular), not data.structures (plural).
    The TIME_PERIOD dimension is in structure.dimensions.observation.
    """
    try:
        ds_list = data.get("data", {}).get("dataSets", [])
        if not ds_list:
            return pd.DataFrame()

        # ABS uses singular "structure", not plural "structures"
        structure = data.get("data", {}).get("structure", {})
        obs_dims  = structure.get("dimensions", {}).get("observation", [])
        time_dim  = next((d for d in obs_dims if d.get("id") == "TIME_PERIOD"), None)
        if not time_dim:
            log.warning("ABS %s: no TIME_PERIOD obs dimension", dataflow)
            return pd.DataFrame()

        time_values = time_dim.get("values", [])
        rows: list[dict] = []

        for ds in ds_list:
            for _sk, series_obj in ds.get("series", {}).items():
                if series_key_filter and _sk != series_key_filter:
                    continue
                for obs_idx_str, obs_vals in series_obj.get("observations", {}).items():
                    try:
                        period = time_values[int(obs_idx_str)]["id"]
                        val    = obs_vals[0]
                        if val is None:
                            continue
                        obs_date = _parse_period(period)
                        if obs_date:
                            rows.append({"obs_date": obs_date, "value": float(val)})
                    except (IndexError, TypeError, ValueError):
                        continue

        return pd.DataFrame(rows)
    except Exception as e:
        log.warning("ABS JSON parse error (%s/%s): %s", dataflow, key, e)
        return pd.DataFrame()


def _parse_period(period: str) -> pd.Timestamp | None:
    """Convert ABS period strings to first-of-period Timestamp."""
    try:
        if "-Q" in period:
            year, q = period.split("-Q")
            return pd.Timestamp(int(year), (int(q) - 1) * 3 + 1, 1)
        if len(period) == 7 and "-" in period:   # "2024-01"
            return pd.Timestamp(period + "-01")
        if len(period) == 4:                      # "2024"
            return pd.Timestamp(int(period), 1, 1)
        return pd.Timestamp(period)
    except Exception:
        return None
