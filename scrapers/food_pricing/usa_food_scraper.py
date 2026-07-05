"""
usa_food_scraper.py
Lekwankwa Corporation Pty Ltd

USA food pricing scraper — BLS CPI Average Price series only.
Outputs gold-standard Parquet directly to the Lekwankwa historical vault.

  Source:    Bureau of Labor Statistics (BLS) Public Data API v2
  Series:    APU* (CPI Average Retail Prices)
  Coverage:  1980-01 → present  (monthly)
  Vault:     lekwankwa-historical-vault/product=food_micropricing/country=USA/source=bls
             /year=YYYY/month=MM/food_pricing_data.parquet
  Gold std:  schema gold standards/food_pricing.json

Usage:
    # Incremental (default — cloud scheduler calls this):
    python3.10 scrapers/food_pricing/usa_food_scraper.py

    # Override start point:
    python3.10 scrapers/food_pricing/usa_food_scraper.py --since 2025-01

    # Full historical backfill:
    python3.10 scrapers/food_pricing/usa_food_scraper.py --mode full --start-year 1980

Modes:
    incremental (default)  Reads vault for latest month, re-fetches last 2 years
                           to capture BLS benchmark revisions, then appends new months.
    full                   Re-scrapes entire range from --start-year to today.

Environment variable (loads from .env or OS):
    BLS_API_KEY=<key>

Author: Lekwankwa Corporation
Date: 2026-06-15
"""

import calendar
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path
from scrapers.utilities.vault_io import get_vault_root

import urllib3
import pandas as pd
import requests

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# ── Load .env ────────────────────────────────────────────────────────────────
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
except ImportError:
    env_path = Path(__file__).resolve().parents[2] / ".env"
    if env_path.exists():
        for line in env_path.read_text().splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                k, v = line.split("=", 1)
                os.environ.setdefault(k.strip(), v.strip())

# ── Incremental utilities ────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scrapers.utilities.incremental import (
    compute_scrape_range, revision_upsert, BLS_KNOWN_GAPS,
)

# ── Logging (logs/ dir, not root) ────────────────────────────────────────────
_LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
_LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(_LOG_DIR / "food_scraper.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

BLS_API_KEY = os.environ.get("BLS_API_KEY", "")
BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BLS_PORTAL  = "https://www.bls.gov/cpi/data.htm"

VAULT_ROOT  = get_vault_root("lekwankwa-historical-vault/product=food_micropricing/country=USA/source=bls")
FILE_NAME   = "food_pricing_data.parquet"
SOURCE      = "bls"
VERSION     = "3.0-GOLD"

MAX_YEARS_PER_CALL   = 20
REQUEST_DELAY        = 0.5   # seconds between BLS API calls

# ── BLS series → COICOP mapping (17 core items) ───────────────────────────────
# Format: (series_id, coicop_code, item_name, item_description, category, bls_unit, factor_to_std_unit, std_unit)
BLS_SERIES = [
    # Cereals & Grains
    ("APU0000701111", "01.1.1.1", "Rice",           "white, long grain",    "Cereals & Grains", "lb",  0.453592, "kg"),
    ("APU0000702111", "01.1.1.2", "Wheat Flour",    "all purpose",          "Cereals & Grains", "5lb", 2.268,    "kg"),
    ("APU0000702421", "01.1.1.3", "Bread",          "white, sliced",        "Cereals & Grains", "lb",  0.453592, "kg"),

    # Meat & Poultry
    ("APU0000703112", "01.1.2.2", "Beef",           "minced/ground",        "Meat & Poultry",   "lb",  0.453592, "kg"),
    ("APU0000706111", "01.1.2.3", "Chicken",        "whole, fresh",         "Meat & Poultry",   "lb",  0.453592, "kg"),
    ("APU0000706211", "01.1.2.4", "Chicken Breast", "boneless",             "Meat & Poultry",   "lb",  0.453592, "kg"),

    # Dairy & Eggs
    ("APU0000708111", "01.1.4.3", "Eggs",           "hen, medium/large",    "Dairy & Eggs",     "doz", 0.72,     "kg"),
    ("APU0000709112", "01.1.4.1", "Milk",           "whole, pasteurised",   "Dairy & Eggs",     "gal", 3.78541,  "litre"),
    ("APU0000712111", "01.1.4.5", "Butter",         "unsalted",             "Dairy & Eggs",     "lb",  0.453592, "kg"),
    ("APU0000711111", "01.1.4.4", "Cheese",         "cheddar",              "Dairy & Eggs",     "lb",  0.453592, "kg"),

    # Oils & Fats
    ("APU0000714229", "01.1.5.1", "Vegetable Oil",  "corn/blended",         "Oils & Fats",      "qt",  0.946353, "litre"),

    # Vegetables
    ("APU0000720111", "01.1.7.1", "Tomatoes",       "fresh, round",         "Vegetables",       "lb",  0.453592, "kg"),
    ("APU0000720211", "01.1.7.3", "Potatoes",       "white, loose",         "Vegetables",       "lb",  0.453592, "kg"),
    ("APU0000720311", "01.1.7.4", "Lettuce",        "iceberg",              "Vegetables",       "lb",  0.453592, "kg"),

    # Sugar & Spices
    ("APU0000711412", "01.1.8.1", "Sugar",          "white, granulated",    "Sugar & Spices",   "lb",  0.453592, "kg"),

    # Beverages
    ("APU0000717311", "01.2.1.2", "Coffee",         "ground, roasted",      "Beverages",        "lb",  0.453592, "kg"),
    ("APU0000717111", "01.2.1.1", "Tea",            "black, loose leaf",    "Beverages",        "lb",  0.453592, "kg"),
]

ITEM_META = {row[0]: row[1:] for row in BLS_SERIES}  # sid → (coicop, name, desc, cat, bls_unit, factor, std_unit)

BLS_ITEM_IDS = {
    "APU0000701111": "GRAIN-01",
    "APU0000702111": "GRAIN-02",
    "APU0000702421": "GRAIN-03",
    "APU0000703112": "MEAT-01",
    "APU0000706111": "MEAT-02",
    "APU0000706211": "MEAT-03",
    "APU0000708111": "DAIRY-01",
    "APU0000709112": "DAIRY-02",
    "APU0000712111": "DAIRY-03",
    "APU0000711111": "DAIRY-04",
    "APU0000714229": "OILS-01",
    "APU0000720111": "VEG-01",
    "APU0000720211": "VEG-02",
    "APU0000720311": "VEG-03",
    "APU0000711412": "SUGAR-01",
    "APU0000717311": "BEV-01",
    "APU0000717111": "BEV-02",
}


def fetch_bls_data(series_ids: list[str], start_year: int, end_year: int) -> dict:
    """
    Fetch data from BLS API v2, paginating over 20-year windows.
    Returns: {series_id: [(year, month, value), ...]} sorted chronologically.
    Known BLS gap months (e.g. Oct-2025 funding lapse) are silently skipped.
    """
    if not BLS_API_KEY:
        raise RuntimeError("BLS_API_KEY not found. Check your .env file.")

    combined: dict[str, list] = {sid: [] for sid in series_ids}

    year_windows = [
        (y, min(y + MAX_YEARS_PER_CALL - 1, end_year))
        for y in range(start_year, end_year + 1, MAX_YEARS_PER_CALL)
    ]

    batch_size = 50
    batches = [series_ids[i: i + batch_size] for i in range(0, len(series_ids), batch_size)]

    total_calls = len(year_windows) * len(batches)
    call_n = 0

    for y_start, y_end in year_windows:
        for batch in batches:
            call_n += 1
            logger.info("  API call %d/%d: %d-%d ...", call_n, total_calls, y_start, y_end)
            payload = {
                "seriesid":        batch,
                "startyear":       str(y_start),
                "endyear":         str(y_end),
                "registrationkey": BLS_API_KEY,
                "catalog":         False,
                "calculations":    False,
                "annualaverage":   False,
            }
            resp = requests.post(BLS_API_URL, json=payload, timeout=30, verify=False)
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") != "REQUEST_SUCCEEDED":
                logger.warning("  BLS API warn: %s", data.get("message", ""))
            else:
                n = 0
                for series in data.get("Results", {}).get("series", []):
                    sid = series["seriesID"]
                    for obs in series.get("data", []):
                        period = obs.get("period", "")
                        if not period.startswith("M") or period == "M13":
                            continue
                        yr, mo = int(obs["year"]), int(period[1:])
                        if (yr, mo) in BLS_KNOWN_GAPS:
                            continue   # skip known funding-lapse months
                        try:
                            val = float(obs["value"])
                        except (ValueError, KeyError):
                            continue
                        combined[sid].append((yr, mo, val))
                        n += 1
                logger.info("  → %d observations", n)
            time.sleep(REQUEST_DELAY)

    return {sid: sorted(rows) for sid, rows in combined.items()}


# =============================================================================
# TRANSFORMER — gold-standard schema
# =============================================================================

def transform(all_data: dict, extraction_ts: str) -> pd.DataFrame:
    """
    Convert BLS observations to gold-standard food_pricing Parquet schema.
    Computes pct_change_mom inline per series.
    Includes both gold-standard field names AND PIT-compat aliases.
    """
    rows = []
    for sid, obs_list in all_data.items():
        if not obs_list:
            continue
        meta = ITEM_META.get(sid)
        if not meta:
            continue
        coicop, name, description, category, bls_unit, factor, std_unit = meta
        internal_id = BLS_ITEM_IDS.get(sid, "BLS-UNKNOWN")

        prev_price: float | None = None

        for (year, month, raw_price) in obs_list:
            price_std = round(raw_price / factor, 4)

            pct_change_mom: float | None = None
            if prev_price is not None and prev_price > 0:
                pct_change_mom = round((price_std - prev_price) / prev_price * 100, 4)
            prev_price = price_std

            date_str  = f"{year:04d}-{month:02d}-01"
            last_day  = calendar.monthrange(year, month)[1]
            fx_date   = f"{year:04d}-{month:02d}-{last_day:02d}"
            data_ts   = pd.Timestamp(date_str, tz="UTC").isoformat()

            # BLS CPI published ~14th of the following month
            rel_month = month + 1 if month < 12 else 1
            rel_year  = year if month < 12 else year + 1
            release_ts = pd.Timestamp(f"{rel_year:04d}-{rel_month:02d}-14", tz="UTC").isoformat()

            rows.append({
                # ── Identity ──────────────────────────────────────────────
                "record_id":                   str(uuid.uuid4()),
                # ── Country ───────────────────────────────────────────────
                "country_code":                "US",
                "iso_alpha3":                  "USA",
                "market_tier":                 "Developed",
                # ── Source metadata (gold standard) ───────────────────────
                "source":                      SOURCE,
                "source_agency":               "BLS",
                "source_sub_category":         "CPI",
                "portal_url":                  BLS_PORTAL,
                "sovereign_series_id":         sid,
                "source_series_id":            sid,   # PIT validation compat
                "dataset_id":                  sid,
                "release_frequency":           "Monthly",
                "extraction_method":           "api",
                # ── Item identity ─────────────────────────────────────────
                "internal_item_id":            internal_id,
                "data_vintage_id":             f"BLS-{sid}-{year:04d}-{month:02d}-v1",
                "confidence_tier":             "PRIMARY",
                "global_coicop_code":          coicop,
                "standard_name":               name,
                "local_name":                  description,
                "category":                    category,
                # ── Observation period ────────────────────────────────────
                "observation_period":          f"{year:04d}-{month:02d}",
                "data_timestamp":              data_ts,
                "official_release_date":       release_ts,
                "published_date":              release_ts,   # PIT validation compat
                "as_of_date":                  release_ts,
                "conversion_timestamp":        extraction_ts,
                "is_revised_figure":           False,
                # ── Price ─────────────────────────────────────────────────
                "observed_price_local":        price_std,
                "price_usd_equivalent":        price_std,
                "currency":                    "USD",
                "fx_rate_applied":             1.0,
                "fx_rate_date":                fx_date,
                "unit_quantity_standardized":  factor,
                "unit_measure_standardized":   std_unit,
                "pct_change_mom":              pct_change_mom,
                # ── Raw provenance ────────────────────────────────────────
                "bls_raw_price":               raw_price,
                "bls_unit":                    bls_unit,
                # ── Bitemporal PIT ────────────────────────────────────────
                "revision_number":             0,
                "superseded_by":               None,
                "data_quality_certified":      True,
                "data_version":                VERSION,
            })

    return pd.DataFrame(rows)


# =============================================================================
# VAULT WRITER
# =============================================================================

def save_to_vault(df: pd.DataFrame) -> tuple[int, int]:
    """
    Partition by year/month and write to vault using revision-aware upsert.
    Returns (partitions_written, revisions_detected).
    """
    if df.empty:
        logger.info("  No data to save.")
        return 0, 0

    df = df.copy()
    df["data_timestamp"] = pd.to_datetime(df["data_timestamp"], utc=True, errors="coerce")
    df["_year"]  = df["data_timestamp"].dt.year
    df["_month"] = df["data_timestamp"].dt.month
    df = df.dropna(subset=["_year", "_month"])

    written    = 0
    total_revs = 0
    for (year, month), group in df.groupby(["_year", "_month"]):
        year, month = int(year), int(month)
        path = VAULT_ROOT / f"year={year}" / f"month={month:02d}" / FILE_NAME
        out  = group.drop(columns=["_year", "_month"], errors="ignore")
        added, revs = revision_upsert(
            path, out,
            key_cols=["sovereign_series_id", "data_timestamp"],
            value_col="observed_price_local",
        )
        if added:
            written    += 1
            total_revs += revs

    return written, total_revs


# =============================================================================
# MAIN
# =============================================================================

def scrape_usa_food_pricing(
    mode: str = "incremental",
    since: str | None = None,
    start_year: int = 1980,
    end_year: int | None = None,
) -> int:
    """
    Run the food pricing scraper.

    mode="incremental"  Detect vault latest month; re-fetch last 2 years for
                        revision capture; append new months only.
    mode="full"         Re-scrape from start_year to end_year (backfill).
    since="YYYY-MM"     Override incremental start point.
    """
    if mode == "incremental":
        start_year, end_year = compute_scrape_range(
            VAULT_ROOT, default_start_year=1980, since=since,
        )
    else:
        if end_year is None:
            end_year = datetime.now(timezone.utc).year

    logger.info("=" * 60)
    logger.info("  Lekwankwa USA Food Pricing Scraper — BLS CPI Only")
    logger.info("  Mode     : %s", mode)
    logger.info("  Coverage : %d – %d", start_year, end_year)
    logger.info("  Items    : %d", len(BLS_SERIES))
    logger.info("  Source   : BLS (API v2)")
    logger.info("  Output   : %s", VAULT_ROOT)
    logger.info("=" * 60)

    extraction_ts = datetime.now(timezone.utc).isoformat()
    series_ids    = [row[0] for row in BLS_SERIES]

    logger.info("  Fetching %d-%d from BLS API...", start_year, end_year)
    try:
        all_data  = fetch_bls_data(series_ids, start_year, end_year)
        total_obs = sum(len(v) for v in all_data.values())
        logger.info("  Fetched %d observations across %d series", total_obs, len(series_ids))
    except Exception as exc:
        logger.error("  FAILED: %s", exc)
        return 0

    logger.info("  Transforming to gold-standard schema...")
    df = transform(all_data, extraction_ts)
    logger.info("  Transformed %d records", len(df))

    # Snapshot this run's transformed rows as a "delta" file for the
    # post-write Live Feed Audit (tools/live_feed_audit.py) to check —
    # it needs a parquet snapshot of just what was scraped THIS run, not
    # the full vault. Filename must be exactly {product}_{YYYYMMDD}_{HHMMSS}
    # (see check_filename_content_match's C5a regex). Written to local
    # disk since the audit runs later in this same container instance —
    # no GCS persistence needed for a same-run scratch file.
    try:
        delta_dir = Path("audit_logs")
        delta_dir.mkdir(parents=True, exist_ok=True)
        delta_ts   = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        delta_path = delta_dir / f"food_micropricing_{delta_ts}.parquet"
        df.to_parquet(delta_path, engine="pyarrow", index=False)
        logger.info("  Delta snapshot written: %s (%d rows)", delta_path, len(df))
    except Exception as exc:
        logger.warning("  Could not write delta snapshot for live feed audit: %s", exc)

    logger.info("  Writing to vault...")
    partitions, revisions = save_to_vault(df)

    logger.info("=" * 60)
    logger.info("  COMPLETE")
    logger.info("  Records    : %d", len(df))
    logger.info("  Partitions : %d", partitions)
    logger.info("  Revisions  : %d", revisions)
    logger.info("  Vault      : %s", VAULT_ROOT)
    logger.info("=" * 60)
    return partitions


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="USA food pricing scraper — BLS CPI only, gold-standard Parquet vault"
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
        "--start-year", type=int, default=1980,
        help="Start year for full mode (default: 1980)",
    )
    parser.add_argument(
        "--end-year", type=int, default=None,
        help="End year for full mode (default: current year)",
    )
    args = parser.parse_args()

    scrape_usa_food_pricing(
        mode=args.mode,
        since=args.since,
        start_year=args.start_year,
        end_year=args.end_year,
    )
