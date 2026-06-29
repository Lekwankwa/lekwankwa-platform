"""
census_housing_permits_usa_scraper.py
Lekwankwa Corporation Pty Ltd

Scraper for US Census Bureau Building Permits Survey (BPS) via FRED API.
  ┌─────────────────────────────────────────────────────────────────────────┐
  │  DATASET: Building Permits Survey (BPS)                                 │
  │  Source:  FRED (Federal Reserve Economic Data) mirrors Census BPS       │
  │  API:     https://api.stlouisfed.org/fred/series/observations           │
  │  Note:    Census timeseries/bps API is no longer available; FRED is     │
  │           the active route as documented in .env                        │
  │  Coverage: 1959-01 → present  (monthly, SAAR)                          │
  │  Vault:   product=Housing_Supply_and_Shelter_Inflation/country=USA      │
  │           /source=census_bps/year=YYYY/month=MM/                       │
  │           building_permits_data.parquet                                 │
  │  Gold std: schema gold standards/housing_building_permits.json          │
  └─────────────────────────────────────────────────────────────────────────┘

FRED series fetched (all monthly, seasonally adjusted):
  PERMIT   — Total housing units authorised (SAAR, all structures)
  PERMIT1  — Single-family units authorised
  PERMIT2  — 2-unit structures
  PERMIT3  — 3-4 unit structures
  PERMIT5  — 5+ unit structures (multifamily)

Authentication:
  Set env var FRED_API_KEY in .env or pass --api-key flag.
  Register free at: https://fred.stlouisfed.org/docs/api/api_key.html

Usage:
  python3.10 scrapers/housing/census_housing_permits_usa_scraper.py
  python3.10 scrapers/housing/census_housing_permits_usa_scraper.py --start-year 1990
  python3.10 scrapers/housing/census_housing_permits_usa_scraper.py --api-key <key>

Author: Lekwankwa Corporation
Date: 2026-06-15
"""

import argparse
import logging
import os
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import urllib3
import pandas as pd
import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler("census_housing_permits_scraper.log", encoding="utf-8"),
        logging.StreamHandler(),
    ],
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONSTANTS
# =============================================================================

FRED_BASE_URL  = "https://api.stlouisfed.org/fred/series/observations"
CENSUS_PORTAL  = "https://www.census.gov/construction/bps/"
VAULT_ROOT     = Path("lekwankwa-historical-vault/product=Housing_Supply_and_Shelter_Inflation/country=USA")
SOURCE         = "census_bps"
FILE_NAME      = "building_permits_data.parquet"

REQUEST_DELAY  = 0.5   # seconds between FRED API calls

# FRED series → (macro_metric_name, unit_of_measure)
# Note: PERMIT2/PERMIT3 (2-unit and 3-4-unit breakdown) are not available in FRED.
# Covered by: PERMIT (total), PERMIT1 (single-family), PERMIT5 (5+ multifamily).
FRED_SERIES = {
    "PERMIT":  ("AUTHORIZED_PERMITS_TOTAL_UNITS",        "UNITS_SAAR"),
    "PERMIT1": ("AUTHORIZED_PERMITS_SINGLE_FAMILY",      "UNITS_SAAR"),
    "PERMIT5": ("AUTHORIZED_PERMITS_5PLUS_MULTIFAMILY",  "UNITS_SAAR"),
}


# =============================================================================
# FRED API CLIENT
# =============================================================================

class FREDClient:
    def __init__(self, api_key: str | None = None):
        self.api_key = api_key or os.environ.get("FRED_API_KEY", "")
        if not self.api_key:
            logger.warning(
                "FRED_API_KEY not set — requests will fail. "
                "Set FRED_API_KEY in .env."
            )

    def fetch_series(self, series_id: str, start_year: int, end_year: int) -> list[dict]:
        """Fetch all monthly observations for a FRED series. Returns list of raw obs dicts."""
        params = {
            "series_id":          series_id,
            "api_key":            self.api_key,
            "file_type":          "json",
            "frequency":          "m",
            "observation_start":  f"{start_year}-01-01",
            "observation_end":    f"{end_year}-12-31",
        }
        try:
            resp = requests.get(FRED_BASE_URL, params=params, timeout=30, verify=False)
            resp.raise_for_status()
            data = resp.json()
            return data.get("observations", [])
        except Exception as exc:
            logger.error(f"  FRED fetch failed for {series_id}: {exc}")
            return []


# =============================================================================
# TRANSFORMER
# =============================================================================

def transform(series_id: str, metric_name: str, unit: str,
              observations: list[dict], extraction_ts: str) -> pd.DataFrame:
    """Convert FRED observations to housing gold-standard schema rows."""
    records = []
    for obs in observations:
        date_str = obs.get("date", "")
        raw_val  = obs.get("value", ".")
        if not date_str or raw_val in (".", ""):
            continue
        try:
            value = float(raw_val)
        except ValueError:
            continue

        try:
            ts = pd.Timestamp(date_str, tz="UTC")
        except Exception:
            continue

        data_ts = ts.isoformat()
        # BPS published ~6 weeks after reference month
        released = (ts + pd.DateOffset(weeks=6)).replace(day=1).isoformat()

        vintage_id = f"CENSUS-BPS-{series_id}-{date_str[:7]}-v1"

        records.append({
            # ── Identity ──────────────────────────────────────────────────
            "record_id":              str(uuid.uuid4()),
            # ── Gold standard (housing_building_permits.json) ─────────────
            "iso_alpha3":             "USA",
            "country_name":           "United States",
            "country_code":           "US",
            "market_tier":            "Developed",
            "source_agency":          "CENSUS",
            "source_sub_category":    "HOUSING",
            "portal_url":             CENSUS_PORTAL,
            "sovereign_series_id":    series_id,
            "data_vintage_id":        vintage_id,
            "confidence_tier":        "PRIMARY",
            "macro_metric_name":      metric_name,
            "reporting_date":         data_ts,
            "data_timestamp":         data_ts,
            "official_release_date":  released,
            "published_date":         released,
            "observed_value":         value,
            "metric_value":           value,
            "unit_of_measure":        unit,
            "is_revised_figure":      False,
            # ── Geography ─────────────────────────────────────────────────
            "geo_level":              "national",
            "geo_id":                 "US",
            "bps_variable":           series_id,
            # ── Operational ───────────────────────────────────────────────
            "source":                 SOURCE,
            "extraction_method":      "api",
            "data_quality_certified": False,
            "conversion_timestamp":   extraction_ts,
            "as_of_date":             extraction_ts,
            "revision_number":        0,
            "superseded_by":          None,
        })

    return pd.DataFrame(records)


# =============================================================================
# VAULT WRITER
# =============================================================================

def save_to_vault(df: pd.DataFrame) -> int:
    if df.empty:
        return 0
    df = df.copy()
    df["data_timestamp"] = pd.to_datetime(df["data_timestamp"], utc=True, errors="coerce")
    df["_year"]  = df["data_timestamp"].dt.year
    df["_month"] = df["data_timestamp"].dt.month
    df = df.dropna(subset=["_year", "_month"])

    written = 0
    for (year, month), group in df.groupby(["_year", "_month"]):
        year, month = int(year), int(month)
        path = (VAULT_ROOT / f"source={SOURCE}" / f"year={year}"
                / f"month={month:02d}" / FILE_NAME)
        path.parent.mkdir(parents=True, exist_ok=True)
        out = group.drop(columns=["_year", "_month"], errors="ignore")

        if path.exists():
            existing = pd.read_parquet(path)
            out = pd.concat([existing, out], ignore_index=True).drop_duplicates(
                subset=["sovereign_series_id", "data_timestamp"], keep="last"
            )
        out.to_parquet(path, engine="pyarrow", index=False)
        written += 1

    return written


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Scrape Census BPS building permits via FRED into the Lekwankwa housing vault"
    )
    parser.add_argument("--start-year", type=int, default=1980,
                        help="Start year (BPS data available from 1959; default: 1980)")
    parser.add_argument("--end-year",   type=int,
                        default=datetime.now(timezone.utc).year,
                        help="End year (default: current year)")
    parser.add_argument("--api-key",    default=None,
                        help="FRED API key (overrides FRED_API_KEY env var)")
    args = parser.parse_args()

    extraction_ts = datetime.now(timezone.utc).isoformat()
    client = FREDClient(api_key=args.api_key)

    logger.info("=" * 70)
    logger.info("DATASET: Census BPS — US Building Permits (via FRED)")
    logger.info("=" * 70)
    logger.info(f"  Years:   {args.start_year} – {args.end_year}")
    logger.info(f"  Series:  {list(FRED_SERIES.keys())}")
    logger.info(f"  Output:  {VAULT_ROOT / ('source=' + SOURCE)}")
    logger.info(f"  API key: {'set' if client.api_key else 'NOT SET'}")

    total_records    = 0
    total_partitions = 0

    for series_id, (metric_name, unit) in FRED_SERIES.items():
        logger.info(f"\n  Fetching {series_id} ({metric_name})...")
        obs = client.fetch_series(series_id, args.start_year, args.end_year)
        if not obs:
            logger.warning(f"  No data for {series_id} — skipping")
            continue

        df = transform(series_id, metric_name, unit, obs, extraction_ts)
        logger.info(f"  Transformed {len(df):,} records")

        written = save_to_vault(df)
        total_records    += len(df)
        total_partitions += written

        time.sleep(REQUEST_DELAY)

    logger.info("")
    logger.info(
        f"Scrape complete: {total_records:,} records across "
        f"{total_partitions} vault partitions."
    )


if __name__ == "__main__":
    main()
