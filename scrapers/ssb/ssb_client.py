"""
Statistics Norway (SSB) PX-Web API client.
Base URL: https://data.ssb.no/api/v0/en/table/{tableId}

SSB provides current-value series only (RELEASE_DATE_ONLY/accumulating).
API uses the JSON-stat2 format via POST requests.

Confirmed tables (probed 2026-06-18):
  08183  Consumer Price Index, by contents and month
         ContentsCode=KpiIndMnd → CPI 2015=100 monthly
  03013  Consumer Price Index, by consumption group, contents and month
         Konsumgrp=01 (Food) → CPI food sub-components monthly
  09190  Quarterly National Accounts
         Makrost=bnpb.nr23_9, ContentsCode=Faste → GDP quarterly
  10644  International investment position and balance of payments (quarterly)

Public API:
    fetch_table(table_id, query_body)  →  pd.DataFrame(obs_date, value)
    get_table_meta(table_id)           →  dict (variable codes and values)
"""

from __future__ import annotations

import logging
import warnings

import requests
import pandas as pd

log = logging.getLogger(__name__)

BASE    = "https://data.ssb.no/api/v0/en/table"
TIMEOUT = 30

_session: requests.Session | None = None


def _sess() -> requests.Session:
    global _session
    if _session is None:
        s = requests.Session()
        s.headers.update({
            "Content-Type": "application/json",
            "User-Agent": "lekwankwa-vault/1.0",
        })
        _session = s
    return _session


def get_table_meta(table_id: str) -> dict:
    """Return variable metadata for an SSB table (GET)."""
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            r = _sess().get(f"{BASE}/{table_id}", timeout=TIMEOUT, verify=False)
        if r.status_code != 200:
            log.warning("SSB meta %s: HTTP %d", table_id, r.status_code)
            return {}
        return r.json()
    except Exception as e:
        log.warning("SSB meta %s: %s", table_id, e)
        return {}


def fetch_table(
    table_id: str,
    query_body: dict,
    dedup_dim: str | None = None,
) -> pd.DataFrame:
    """
    POST a JSON-stat2 query to an SSB table.

    dedup_dim: when a dimension has multiple revision values (e.g.
    PubliseringMnd), pass its code here.  The parser will keep the
    latest non-null observation per Tid period.
    """
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            r = _sess().post(
                f"{BASE}/{table_id}", json=query_body, timeout=TIMEOUT, verify=False
            )
        if r.status_code == 400:
            log.warning("SSB table %s: HTTP 400 (bad variable code)", table_id)
            return pd.DataFrame()
        if r.status_code != 200:
            log.warning("SSB table %s: HTTP %d", table_id, r.status_code)
            return pd.DataFrame()
        data = r.json()
    except Exception as e:
        log.warning("SSB table %s: %s", table_id, e)
        return pd.DataFrame()

    return _parse_jsonstat2(data, table_id=table_id, dedup_dim=dedup_dim)


def _parse_jsonstat2(
    data: dict,
    *,
    table_id: str = "",
    dedup_dim: str | None = None,
) -> pd.DataFrame:
    """
    Parse a JSON-stat2 response.

    All non-time dimensions should have exactly one selected value,
    except the optional dedup_dim which may have multiple values
    representing publication revisions.  When dedup_dim is set,
    for each Tid period the latest non-null value is kept.
    """
    try:
        dims     = data.get("id", [])
        sizes    = data.get("size", [])
        dim_info = data.get("dimension", {})
        values   = data.get("value", [])

        tid_pos = next((i for i, d in enumerate(dims) if d.lower() == "tid"), None)
        if tid_pos is None:
            log.warning("SSB %s: no Tid dimension", table_id)
            return pd.DataFrame()

        dedup_pos = None
        if dedup_dim:
            dedup_pos = next((i for i, d in enumerate(dims) if d == dedup_dim), None)

        tid_cats  = dim_info["Tid"]["category"]
        tid_index = tid_cats.get("index", {})
        if isinstance(tid_index, list):
            tid_codes = tid_index
        else:
            tid_codes = sorted(tid_index, key=lambda k: tid_index[k])

        # Compute per-dimension strides (row-major order)
        strides = [1] * len(dims)
        for i in range(len(dims) - 2, -1, -1):
            strides[i] = strides[i + 1] * sizes[i + 1]

        n_dedup = sizes[dedup_pos] if dedup_pos is not None else 1

        rows: list[dict] = []
        for t, tc in enumerate(tid_codes):
            best_val = None
            for d in range(n_dedup):
                idx = t * strides[tid_pos]
                if dedup_pos is not None:
                    idx += d * strides[dedup_pos]
                if 0 <= idx < len(values) and values[idx] is not None:
                    best_val = values[idx]  # overwrite → latest d wins
            if best_val is not None:
                obs_date = _parse_tid(tc)
                if obs_date:
                    rows.append({"obs_date": obs_date, "value": float(best_val)})

        return pd.DataFrame(rows)
    except Exception as e:
        log.warning("SSB JSON-stat2 parse error (%s): %s", table_id, e)
        return pd.DataFrame()


def _parse_tid(tid: str) -> pd.Timestamp | None:
    """
    Parse SSB Tid codes:
      '2024M12' → 2024-12-01 (monthly)
      '2024K1'  → 2024-01-01 (quarterly, K=kvartal)
      '2024H1'  → 2024-01-01 (half-yearly)
      '2024'    → 2024-01-01 (annual)
    """
    try:
        if "M" in tid and len(tid) == 7:
            return pd.Timestamp(int(tid[:4]), int(tid[5:]), 1)
        if "K" in tid:
            q = int(tid.split("K")[1])
            return pd.Timestamp(int(tid[:4]), (q - 1) * 3 + 1, 1)
        if "H" in tid:
            h = int(tid.split("H")[1])
            return pd.Timestamp(int(tid[:4]), (h - 1) * 6 + 1, 1)
        if len(tid) == 4:
            return pd.Timestamp(int(tid), 1, 1)
        return None
    except Exception:
        return None
