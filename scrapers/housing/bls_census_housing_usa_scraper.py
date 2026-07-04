"""
bls_census_housing_usa_scraper.py
Lekwankwa Corporation Pty Ltd

Combined scraper for the "Housing Supply & Shelter Inflation" dataset:
  ┌─────────────────────────────────────────────────────────────────────┐
  │  SHELTER  — BLS Consumer Price Index, Shelter components            │
  │  Source:   BLS REST API  /publicAPI/v2/timeseries/data/             │
  │  Series:   CUUR/CUSR prefix, area=0000 (national)                   │
  │  Coverage: 1914-01 → present  (monthly)                             │
  │  Vault:    product=housing/country=USA/source=bls_cpi_shelter       │
  │  File:     shelter_inflation_data.parquet                           │
  │  Gold std: schema gold standards/housing_shelter_inflation.json     │
  ├─────────────────────────────────────────────────────────────────────┤
  │  PERMITS  — US Census Bureau Building Permits Survey (BPS)          │
  │  Source:   Census timeseries API  /data/timeseries/bps              │
  │  Variables: PERMIT, PERMIT1, PERMIT2, PERMIT3_4, PERMIT5, BLDGS,   │
  │             VALUE                                                    │
  │  Coverage: 1959-01 → present  (monthly, national SAAR + actuals)   │
  │  Vault:    product=housing/country=USA/source=census_bps            │
  │  File:     housing_permits_data.parquet                             │
  │  Gold std: schema gold standards/housing_building_permits.json      │
  └─────────────────────────────────────────────────────────────────────┘

Usage:
    # Incremental (default — cloud scheduler):
    python3.10 scrapers/housing/bls_census_housing_usa_scraper.py

    # Override start point:
    python3.10 scrapers/housing/bls_census_housing_usa_scraper.py --since 2025-01

    # Full historical backfill:
    python3.10 scrapers/housing/bls_census_housing_usa_scraper.py --mode full --dataset permits

Environment variables (optional — increase rate limits):
    BLS_API_KEY=<key>        — from .env or shell
    CENSUS_API_KEY=<key>     — from .env or shell

Author: Lekwankwa Corporation
Date: 2026-06-12
"""

import argparse
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from scrapers.utilities.vault_io import get_vault_root

import pandas as pd
import requests

# Load .env if present
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
except ImportError:
    pass

# Incremental utilities
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scrapers.utilities.incremental import (
    compute_scrape_range, compute_scrape_range_monthly,
    revision_upsert, BLS_KNOWN_GAPS,
)

_LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
_LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(_LOG_DIR / "housing_scraper.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONSTANTS — SHARED
# =============================================================================

VAULT_ROOT = get_vault_root("lekwankwa-historical-vault/product=Housing_Supply_and_Shelter_Inflation/country=USA")
REQUEST_DELAY = 0.4   # seconds between API calls

# =============================================================================
# CONSTANTS — BLS CPI SHELTER
# =============================================================================

BLS_API_URL   = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BLS_PORTAL    = "https://www.bls.gov/cpi/"
SHELTER_SOURCE    = "bls_cpi_shelter"
SHELTER_FILE_NAME = "shelter_inflation_data.parquet"

SHELTER_START_YEAR = 1914   # CUUR0000SEHA begins 1914; later series start clamped per-series

MAX_YEARS_PER_CALL        = 20
MAX_SERIES_PER_CALL_KEYED = 50
MAX_SERIES_PER_CALL_OPEN  = 25

# BLS CPI Shelter series catalogue
# Key: series_id → (macro_metric_name, unit_of_measure, first_available_year)
SHELTER_SERIES_META = {
    # Not seasonally adjusted (NSA) — unadjusted (CUUR prefix)
    "CUUR0000SEHA": ("CPI_RENT_OF_PRIMARY_RESIDENCE",    "INDEX", 1914),
    "CUUR0000SEHB": ("CPI_OWNERS_EQUIVALENT_RENT",       "INDEX", 1983),
    "CUUR0000SAH1": ("CPI_SHELTER",                      "INDEX", 1940),
    "CUUR0000SEHC": ("CPI_RENT_OF_SHELTER",              "INDEX", 1983),
    # Seasonally adjusted (SA) — CUSR prefix
    "CUSR0000SEHA": ("CPI_RENT_OF_PRIMARY_RESIDENCE_SA", "INDEX", 1947),
    "CUSR0000SEHB": ("CPI_OWNERS_EQUIVALENT_RENT_SA",    "INDEX", 1983),
    "CUSR0000SAH1": ("CPI_SHELTER_SA",                   "INDEX", 1983),
}

SHELTER_SERIES = list(SHELTER_SERIES_META.keys())


def _is_sa_shelter(series_id: str) -> str:
    """BLS CPI convention: position 2 is 'S' for SA, 'U' for NSA."""
    if len(series_id) >= 3:
        return "S" if series_id[2].upper() == "S" else "U"
    return "U"


# =============================================================================
# CONSTANTS — CENSUS BPS
# =============================================================================

CENSUS_BPS_BASE   = "https://api.census.gov/data/timeseries/bps"
CENSUS_PORTAL     = "https://www.census.gov/construction/bps/"
PERMITS_SOURCE    = "census_bps"
PERMITS_FILE_NAME = "housing_permits_data.parquet"

PERMITS_START_YEAR = 1959   # BPS data available from 1959

# BPS variables → (macro_metric_name, unit_of_measure)
BPS_VARIABLES = {
    "PERMIT":    ("AUTHORIZED_PERMITS_TOTAL_UNITS",          "UNITS_SAAR"),
    "PERMIT1":   ("AUTHORIZED_PERMITS_SINGLE_FAMILY",        "UNITS_SAAR"),
    "PERMIT2":   ("AUTHORIZED_PERMITS_2_UNIT_STRUCTURES",    "UNITS_SAAR"),
    "PERMIT3_4": ("AUTHORIZED_PERMITS_3_4_UNIT_STRUCTURES",  "UNITS_SAAR"),
    "PERMIT5":   ("AUTHORIZED_PERMITS_5PLUS_MULTIFAMILY",    "UNITS_SAAR"),
    "BLDGS":     ("AUTHORIZED_BUILDINGS_COUNT",              "COUNT"),
    "VALUE":     ("AUTHORIZED_CONSTRUCTION_VALUE",           "THOUSANDS_USD"),
}


# =============================================================================
# BLS API CLIENT
# =============================================================================

class BLSAPIClient:
    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.environ.get("BLS_API_KEY", "")
        self.max_series = (MAX_SERIES_PER_CALL_KEYED if self.api_key
                           else MAX_SERIES_PER_CALL_OPEN)
        if not self.api_key:
            logger.warning(
                "BLS_API_KEY not set — using public tier (25 series/call, "
                "500 queries/day). Set BLS_API_KEY in .env to lift limits."
            )

    def fetch(self, series_ids: list, start_year: int, end_year: int) -> dict:
        payload = {
            "seriesid":      series_ids,
            "startyear":     str(start_year),
            "endyear":       str(end_year),
            "catalog":       True,
            "calculations":  False,
            "annualaverage": False,
        }
        if self.api_key:
            payload["registrationkey"] = self.api_key

        resp = requests.post(
            BLS_API_URL,
            json=payload,
            headers={"Content-type": "application/json"},
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()

    def fetch_all_years(self, series_ids: list, start_year: int, end_year: int) -> list:
        """Paginate over year windows and series batches; return flat list of obs dicts."""
        all_items = []
        year_windows = [
            (y, min(y + MAX_YEARS_PER_CALL - 1, end_year))
            for y in range(start_year, end_year + 1, MAX_YEARS_PER_CALL)
        ]
        batches = [
            series_ids[i: i + self.max_series]
            for i in range(0, len(series_ids), self.max_series)
        ]
        total_calls = len(year_windows) * len(batches)
        call_n = 0
        for y_start, y_end in year_windows:
            for batch in batches:
                call_n += 1
                logger.info(f"  BLS call {call_n}/{total_calls}: "
                            f"{len(batch)} series, {y_start}-{y_end}")
                try:
                    raw = self.fetch(batch, y_start, y_end)
                    status = raw.get("status", "")
                    if status != "REQUEST_SUCCEEDED":
                        logger.warning(f"  BLS status={status}: "
                                       f"{raw.get('message', '')}")
                    for series in raw.get("Results", {}).get("series", []):
                        for obs in series.get("data", []):
                            obs["seriesID"] = series["seriesID"]
                            all_items.append(obs)
                except requests.HTTPError as exc:
                    logger.error(f"  HTTP error: {exc}")
                except Exception as exc:
                    logger.error(f"  Unexpected error: {exc}")
                time.sleep(REQUEST_DELAY)
        return all_items


# =============================================================================
# CENSUS BPS API CLIENT
# =============================================================================

class CensusBPSClient:
    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.environ.get("CENSUS_API_KEY", "")
        if not self.api_key:
            logger.warning(
                "CENSUS_API_KEY not set — using unauthenticated Census API "
                "(rate limits apply). Set CENSUS_API_KEY in .env."
            )

    def fetch_month(self, year: int, month: int) -> list[dict] | None:
        """Fetch one year-month at national level. Returns list of row dicts or None on error."""
        var_list = ",".join(["MONTH", "YEAR"] + list(BPS_VARIABLES.keys()))
        params = {
            "get":   var_list,
            "YEAR":  str(year),
            "MONTH": f"{month:02d}",
            "for":   "us:*",
        }
        if self.api_key:
            params["key"] = self.api_key
        try:
            resp = requests.get(CENSUS_BPS_BASE, params=params, timeout=30)
            if resp.status_code == 204:
                return []
            resp.raise_for_status()
            raw = resp.json()
            if not raw or len(raw) < 2:
                return []
            headers = raw[0]
            return [dict(zip(headers, row)) for row in raw[1:]]
        except requests.HTTPError as exc:
            status = exc.response.status_code if exc.response is not None else "?"
            if status in (400, 404):
                return []   # period not yet available
            logger.error(f"  Census HTTP {status} for {year}-{month:02d}: {exc}")
            return None
        except Exception as exc:
            logger.error(f"  Census error for {year}-{month:02d}: {exc}")
            return None


# =============================================================================
# DATA TRANSFORMERS
# =============================================================================

def _period_to_date(year: str, period: str) -> str | None:
    """BLS period (M01-M12) → ISO date YYYY-MM-01. Returns None for annual averages."""
    if not period.startswith("M") or period == "M13":
        return None
    month = int(period[1:])
    return f"{year}-{month:02d}-01"


def transform_shelter(items: list, extraction_ts: str) -> pd.DataFrame:
    """Convert raw BLS API observations to housing_shelter_inflation gold-standard schema."""
    rows = []
    for obs in items:
        date_str = _period_to_date(obs.get("year", ""), obs.get("period", ""))
        if date_str is None:
            continue
        sid = obs["seriesID"]
        try:
            value = float(obs.get("value", ""))
        except (ValueError, TypeError):
            value = None

        meta = SHELTER_SERIES_META.get(sid, ("CPI_SHELTER_METRIC", "INDEX", 1914))
        metric_name, unit, _ = meta

        # BLS publishes CPI ~12-14 days after reference month ends (mid-following month)
        try:
            ref_dt   = pd.Timestamp(date_str, tz="UTC")
            released = (ref_dt + pd.DateOffset(months=1)).replace(day=12).strftime("%Y-%m-%d")
        except Exception:
            released = date_str

        data_ts = pd.Timestamp(date_str, tz="UTC").isoformat()

        rows.append({
            # ── Identity ──────────────────────────────────────────────
            "record_id":              str(uuid.uuid4()),
            # ── Gold standard: housing_shelter_inflation.json ─────────
            "iso_alpha3":             "USA",
            "country_name":           "United States",
            "country_code":           "US",
            "market_tier":            "Developed",
            "source_agency":          "BLS",
            "source_sub_category":    "CPI_URBAN",
            "portal_url":             BLS_PORTAL,
            "sovereign_series_id":    sid,
            "data_vintage_id":        f"BLS-{sid}-{date_str[:7]}-v1",
            "confidence_tier":        "PRIMARY",
            "macro_metric_name":      metric_name,
            "reporting_date":         data_ts,
            "data_timestamp":         data_ts,
            "official_release_date":  pd.Timestamp(released, tz="UTC").isoformat(),
            "published_date":         pd.Timestamp(released, tz="UTC").isoformat(),
            "observed_value":         value,
            "metric_value":           value,
            "unit_of_measure":        unit,
            "is_revised_figure":      bool(obs.get("revised", False)),
            # ── Operational ──────────────────────────────────────────
            "seasonal_adjustment":    _is_sa_shelter(sid),
            "source":                 SHELTER_SOURCE,
            "extraction_method":      "api",
            "data_quality_certified": False,
            "conversion_timestamp":   extraction_ts,
            "as_of_date":             pd.Timestamp(released, tz="UTC").isoformat(),
            "revision_number":        0,
            "superseded_by":          None,
            # ── Raw metadata ─────────────────────────────────────────
            "bls_footnotes":          str(obs.get("footnotes", "")),
        })
    return pd.DataFrame(rows)


def transform_permits(rows_raw: list[dict], extraction_ts: str) -> pd.DataFrame:
    """Convert Census BPS API rows to housing_building_permits gold-standard schema."""
    records = []
    for row in rows_raw:
        try:
            year  = int(row.get("YEAR",  0))
            month = int(row.get("MONTH", 0))
            if year == 0 or month == 0:
                continue
            date_str = f"{year}-{month:02d}-01"
            data_ts  = pd.Timestamp(date_str, tz="UTC").isoformat()

            # Census BPS published ~6 weeks after reference month
            ref_dt   = pd.Timestamp(date_str, tz="UTC")
            released = (ref_dt + pd.DateOffset(weeks=6)).replace(day=1).strftime("%Y-%m-01")

            for var, (metric_name, unit) in BPS_VARIABLES.items():
                raw_val = row.get(var)
                if raw_val is None or str(raw_val).strip() in ("-", "(X)", "N", ""):
                    continue
                try:
                    value = float(str(raw_val).replace(",", ""))
                except ValueError:
                    continue

                records.append({
                    # ── Identity ────────────────────────────────────────
                    "record_id":              str(uuid.uuid4()),
                    # ── Gold standard: housing_building_permits.json ────
                    "iso_alpha3":             "USA",
                    "country_name":           "United States",
                    "country_code":           "US",
                    "market_tier":            "Developed",
                    "source_agency":          "CENSUS",
                    "source_sub_category":    "HOUSING",
                    "portal_url":             CENSUS_PORTAL,
                    "sovereign_series_id":    var,
                    "data_vintage_id":        f"CENSUS-PERMIT-USA-{date_str[:7]}-v1",
                    "confidence_tier":        "PRIMARY",
                    "macro_metric_name":      metric_name,
                    "reporting_date":         data_ts,
                    "data_timestamp":         data_ts,
                    "official_release_date":  pd.Timestamp(released, tz="UTC").isoformat(),
                    "published_date":         pd.Timestamp(released, tz="UTC").isoformat(),
                    "observed_value":         value,
                    "metric_value":           value,
                    "unit_of_measure":        unit,
                    "is_revised_figure":      False,
                    # ── Geography ───────────────────────────────────────
                    "geo_level":              "national",
                    "geo_id":                 "US",
                    "bps_variable":           var,
                    # ── Operational ─────────────────────────────────────
                    "source":                 PERMITS_SOURCE,
                    "extraction_method":      "api",
                    "data_quality_certified": False,
                    "conversion_timestamp":   extraction_ts,
                    "as_of_date":             pd.Timestamp(released, tz="UTC").isoformat(),
                    "revision_number":        0,
                    "superseded_by":          None,
                })
        except Exception as exc:
            logger.warning(f"  Skipping malformed BPS row: {exc}")
    return pd.DataFrame(records)


# =============================================================================
# VAULT WRITER
# =============================================================================

def save_to_vault(df: pd.DataFrame, source: str, file_name: str,
                  key_cols: list | None = None) -> tuple[int, int]:
    """Partition by year/month and revision-upsert to vault. Returns (partitions, revisions)."""
    if df.empty:
        logger.warning("  No data to save for source=%s", source)
        return 0, 0

    if key_cols is None:
        key_cols = ["sovereign_series_id", "reporting_date"]

    df = df.copy()
    df["data_timestamp"] = pd.to_datetime(df["data_timestamp"], utc=True, errors="coerce")
    df["_year"]  = df["data_timestamp"].dt.year
    df["_month"] = df["data_timestamp"].dt.month
    df = df.dropna(subset=["_year", "_month"])

    written    = 0
    total_revs = 0
    for (year, month), group in df.groupby(["_year", "_month"]):
        year, month = int(year), int(month)
        path = (VAULT_ROOT / f"source={source}" / f"year={year}"
                / f"month={month:02d}" / file_name)
        out = group.drop(columns=["_year", "_month"], errors="ignore")
        added, revs = revision_upsert(path, out, key_cols=key_cols, value_col="observed_value")
        if added:
            written    += 1
            total_revs += revs

    logger.info("  source=%s → %d partitions, %d revisions", source, written, total_revs)
    return written, total_revs


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description=(
            "Scrape BLS CPI Shelter and/or Census BPS Building Permits "
            "into the Lekwankwa housing vault"
        )
    )
    parser.add_argument(
        "--mode", choices=["incremental", "full"], default="incremental",
        help="incremental: auto-detect vault latest and append; full: re-scrape everything",
    )
    parser.add_argument(
        "--since", type=str, default=None, metavar="YYYY-MM",
        help="Override incremental start point (e.g. 2025-01)",
    )
    parser.add_argument(
        "--dataset", choices=["shelter", "permits", "both"], default="both",
        help="Which dataset to scrape (default: both)"
    )
    parser.add_argument(
        "--start-year", type=int, default=1959,
        help="Start year for full mode (BPS: 1959; BLS shelter: 1914)"
    )
    parser.add_argument(
        "--end-year", type=int, default=None,
        help="End year for full mode (default: current year)"
    )
    parser.add_argument("--bls-api-key",    default=None)
    parser.add_argument("--census-api-key", default=None)
    args = parser.parse_args()

    end_year      = args.end_year or datetime.now(timezone.utc).year
    extraction_ts = datetime.now(timezone.utc).isoformat()

    logger.info("=" * 70)
    logger.info("HOUSING SCRAPER — BLS Shelter + Census BPS")
    logger.info("Mode: %s  |  Dataset: %s", args.mode, args.dataset)
    logger.info("=" * 70)

    # ── DATASET 1: BLS CPI Shelter ────────────────────────────────────────────
    if args.dataset in ("shelter", "both"):
        logger.info("")
        logger.info("DATASET: BLS CPI Shelter — Housing Shelter Inflation")

        bls_client    = BLSAPIClient(api_key=args.bls_api_key)
        shelter_vault = VAULT_ROOT / f"source={SHELTER_SOURCE}"

        if args.mode == "incremental":
            shelter_start, end_year = compute_scrape_range(
                shelter_vault, default_start_year=SHELTER_START_YEAR, since=args.since,
            )
        else:
            shelter_start = max(args.start_year, SHELTER_START_YEAR)

        logger.info("  Series: %d, years: %d-%d", len(SHELTER_SERIES), shelter_start, end_year)
        items = bls_client.fetch_all_years(SHELTER_SERIES, shelter_start, end_year)
        # Filter known BLS gap months
        items = [
            obs for obs in items
            if not (
                obs.get("period", "").startswith("M") and
                obs.get("period") != "M13" and
                (int(obs.get("year", 0)), int(obs.get("period", "M0")[1:])) in BLS_KNOWN_GAPS
            )
        ]
        logger.info("  Raw observations fetched: %d", len(items))

        if items:
            df_shelter = transform_shelter(items, extraction_ts)
            logger.info("  Records transformed: %d", len(df_shelter))
            save_to_vault(df_shelter, SHELTER_SOURCE, SHELTER_FILE_NAME,
                          key_cols=["sovereign_series_id", "reporting_date"])
        else:
            logger.warning("  No shelter CPI data returned by BLS API")

    # ── DATASET 2: Census BPS Building Permits ────────────────────────────────
    if args.dataset in ("permits", "both"):
        logger.info("")
        logger.info("DATASET: Census BPS — US Building Permits Survey")

        census_client = CensusBPSClient(api_key=args.census_api_key)
        permits_vault = VAULT_ROOT / f"source={PERMITS_SOURCE}"

        if args.mode == "incremental":
            p_sy, p_sm, p_ey, p_em = compute_scrape_range_monthly(
                permits_vault, default_start_year=PERMITS_START_YEAR, since=args.since,
            )
        else:
            p_sy, p_sm = max(args.start_year, PERMITS_START_YEAR), 1
            p_ey, p_em = end_year, 12

        logger.info("  Variables: %d, range: %d-%02d → %d-%02d",
                    len(BPS_VARIABLES), p_sy, p_sm, p_ey, p_em)

        total_records    = 0
        total_partitions = 0
        fetch_errors     = 0

        for year in range(p_sy, p_ey + 1):
            m_start = p_sm if year == p_sy else 1
            m_end   = p_em if year == p_ey else 12
            year_records = 0

            for month in range(m_start, m_end + 1):
                rows = census_client.fetch_month(year, month)
                if rows is None:
                    fetch_errors += 1
                    continue
                if not rows:
                    continue

                df_permits = transform_permits(rows, extraction_ts)
                if df_permits.empty:
                    continue

                written, _ = save_to_vault(
                    df_permits, PERMITS_SOURCE, PERMITS_FILE_NAME,
                    key_cols=["sovereign_series_id", "reporting_date", "geo_id"],
                )
                total_partitions += written
                year_records     += len(df_permits)
                time.sleep(REQUEST_DELAY)

            if year_records:
                logger.info("  %d: %d records", year, year_records)
                total_records += year_records

        logger.info("  Census BPS complete: %d records, %d partitions (%d errors)",
                    total_records, total_partitions, fetch_errors)

    logger.info("")
    logger.info("Housing scrape complete.")


if __name__ == "__main__":
    main()
