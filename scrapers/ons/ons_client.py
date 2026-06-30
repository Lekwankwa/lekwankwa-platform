"""
ONS Beta API client.

Search: https://api.beta.ons.gov.uk/v1  (CDID lookup)
Data:   https://www.ons.gov.uk{uri}/data  (timeseries download)

Strategy:
  1. resolve_cdid(cdid)  ->  ONS web URI string via /search endpoint
  2. fetch_timeseries(cdid, uri)  ->  DataFrame(obs_date, value, freq)

The /data endpoint on www.ons.gov.uk returns JSON with months/quarters/years
arrays and is the confirmed working path for economic time series.
"""

from __future__ import annotations

import logging

import requests
import urllib3
import pandas as pd

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

log = logging.getLogger(__name__)

SEARCH_BASE = "https://api.beta.ons.gov.uk/v1"
DATA_BASE   = "https://www.ons.gov.uk"
_TIMEOUT    = 30

_session: requests.Session | None = None


def _sess() -> requests.Session:
    global _session
    if _session is None:
        s = requests.Session()
        s.verify = False
        s.headers.update({
            "Accept":     "application/json",
            "User-Agent": "lekwankwa-vault/1.0",
        })
        _session = s
    return _session


def resolve_cdid(cdid: str) -> str | None:
    """
    Resolve a CDID to its full ONS web URI via the search API.

    The beta search endpoint returns items with:
      {"cdid": "ABMI", "dataset_id": "pn2",
       "uri": "/economy/grossdomesticproductgdp/timeseries/abmi/pn2"}

    We match cdid case-insensitively and return the uri field so that
    fetch_timeseries can build https://www.ons.gov.uk{uri}/data.
    """
    try:
        r = _sess().get(
            f"{SEARCH_BASE}/search",
            params={"query": cdid, "content_type": "timeseries", "limit": 10},
            timeout=_TIMEOUT,
        )
        if r.status_code != 200:
            log.warning("ONS search %s: HTTP %d", cdid, r.status_code)
            return None

        items  = r.json().get("items", [])
        target = cdid.upper()
        for item in items:
            if item.get("cdid", "").upper() == target:
                return item.get("uri")

        log.warning("ONS: CDID %s not found in %d search results", cdid, len(items))
        return None
    except Exception as e:
        log.warning("ONS resolve_cdid %s: %s", cdid, e)
        return None


def fetch_timeseries(cdid: str, uri: str) -> pd.DataFrame:
    """
    Fetch all observations for a CDID using its ONS web URI.

    url = https://www.ons.gov.uk{uri}/data
    Response: {"months": [...], "quarters": [...], "years": [...]}

    Returns DataFrame: obs_date (Timestamp), value (float), freq (str).
    """
    url = f"{DATA_BASE}{uri}/data"
    try:
        r = _sess().get(url, timeout=_TIMEOUT)
        if r.status_code != 200:
            log.warning("ONS fetch %s: HTTP %d (%s)", cdid, r.status_code, url)
            return pd.DataFrame()
        data = r.json()
    except Exception as e:
        log.warning("ONS fetch %s: %s", cdid, e)
        return pd.DataFrame()

    rows: list[dict] = []

    for m in data.get("months", []):
        try:
            val_str = str(m.get("value", "")).replace(",", "")
            if not val_str or val_str in ("-", ".."):
                continue
            obs_date = pd.to_datetime(m["date"], format="%Y %b")
            rows.append({"obs_date": obs_date, "value": float(val_str), "freq": "M"})
        except (ValueError, KeyError):
            continue

    for q in data.get("quarters", []):
        try:
            val_str = str(q.get("value", "")).replace(",", "")
            if not val_str or val_str in ("-", ".."):
                continue
            year_str, qn = q["date"].split(" Q")
            obs_date = pd.Timestamp(int(year_str), (int(qn) - 1) * 3 + 1, 1)
            rows.append({"obs_date": obs_date, "value": float(val_str), "freq": "Q"})
        except (ValueError, KeyError):
            continue

    if not rows:
        log.warning("ONS %s: 0 observations in response", cdid)
    return pd.DataFrame(rows)
