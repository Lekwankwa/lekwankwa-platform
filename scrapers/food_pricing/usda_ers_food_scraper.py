"""
usda_ers_food_scraper.py
Lekwankwa Corporation Pty Ltd

USA food pricing scraper — USDA ERS Food Price Outlook (monthly CPI food categories).

  Source:    USDA Economic Research Service (ERS) Food Price Outlook
  Underlying data: BLS CPI food-category series (CU* — Consumer Price Index by category)
  Coverage:  1980-01 → present  (monthly)
  Vault:     lekwankwa-historical-vault/product=food_micropricing/country=USA/source=usda_ers
             /year=YYYY/month=MM/food_pricing_data.parquet
  Gold std:  Identical column structure to source=bls (PIT v4.0, 22-field)

DATA PROVENANCE NOTE:
  ERS does not collect retail food prices independently. Their Food Price Outlook
  uses BLS CPI food-category series (CU*) as the underlying data source.
  This scraper fetches those same BLS CPI series and stores them as source=usda_ers,
  reflecting that ERS is the publishing authority for the Food Price Outlook product.

  item_value = BLS CPI index level (1982–84 = 100).  This is not a dollar price;
  it is a relative price level.  To get dollar prices, see source=bls (APU series).

CRITICAL — Release-date discipline (PIT sequence check will flag violations if wrong):
  BLS CPI Average Prices (source=bls, APU series):
      published ~14th of FOLLOWING month      (~2–3 week lag)
  USDA ERS Food Price Outlook (source=usda_ers, CU series):
      published ~70 days after reference month  (~8–12 week lag)

  Example: January 2024 data
    source=bls  official_release_date → 2024-02-14
    source=usda_ers official_release_date → 2024-04-10  (Jan 31 + 70 days)

  PIT check: reporting_date must be < official_release_date.
  Lag check  expects {"usda_ers": (2, 3)} months — never apply BLS 14-day offset here.

Usage:
    python scrapers/food_pricing/usda_ers_food_scraper.py
    python scrapers/food_pricing/usda_ers_food_scraper.py --start-year 1980 --end-year 2026

Environment variables (loads from .env or OS):
    BLS_API_KEY=<key>       (ERS uses BLS CPI data; existing BLS key is reused)
    SSL_VERIFY=false        (set on Windows environments with corporate CA)

Author: Lekwankwa Corporation
Date: 2026-06-15
"""

import calendar
import os
import time
import uuid
import warnings as _warnings
from datetime import datetime, timedelta, timezone
from pathlib import Path
from scrapers.utilities.vault_io import get_vault_root

import pandas as pd
import requests
import urllib3

# ── SSL / env ─────────────────────────────────────────────────────────────────
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

_SSL_VERIFY = os.environ.get("SSL_VERIFY", "true").lower() not in ("false", "0", "no")
if not _SSL_VERIFY:
    urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

BLS_API_KEY = os.environ.get("BLS_API_KEY", "")
BLS_API_URL = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
ERS_PORTAL  = "https://www.ers.usda.gov/data-products/food-price-outlook/"

VAULT_ROOT = Path(
    "lekwankwa-historical-vault/product=food_micropricing/country=USA/source=usda_ers"
)
FILE_NAME = "food_pricing_data.parquet"
SOURCE    = "usda_ers"
VERSION   = "1.0-GOLD"

# ERS Food Price Outlook lag: ~10 weeks (70 days) after reference month ends
ERS_RELEASE_LAG_DAYS = 70

MAX_YEARS_PER_CALL = 20
REQUEST_DELAY      = 0.5   # seconds between BLS API calls

# ── ERS/BLS CPI food-category series catalogue ────────────────────────────────
# All series verified to return data from the BLS API.
# item_value = CPI index level (1982–84 = 100), not a dollar price.
#
# Format: (bls_series_id, coicop_code, item_name, item_description, category)
ERS_SERIES = [
    # ── Aggregate food categories ────────────────────────────────────────────
    ("CUUR0000SAF",    "01.1",   "All Food & Beverages", "CPI-U all food and beverages",     "All Food"),
    ("CUUR0000SAF1",   "01.1",   "All Food",             "CPI-U all food (at home + away)",  "All Food"),
    ("CUUR0000SAF11",  "01.1",   "Food at Home",         "CPI-U grocery/retail food total",  "All Food"),

    # ── Cereals & Grains ─────────────────────────────────────────────────────
    ("CUUR0000SAF111", "01.1.1", "Cereals & Bakery",     "cereals and bakery products",      "Cereals & Grains"),
    ("CUUR0000SEFA01", "01.1.1.2","Wheat Flour",         "flour and prepared flour mixes",   "Cereals & Grains"),
    ("CUUR0000SEFA02", "01.1.1.1","Breakfast Cereal",    "breakfast cereal",                 "Cereals & Grains"),
    ("CUUR0000SEFB01", "01.1.1.3","Bread",               "fresh bread",                      "Cereals & Grains"),
    ("CUUR0000SEFB02", "01.1.1.3","Rolls & Biscuits",    "biscuits, rolls, muffins",         "Cereals & Grains"),
    ("CUUR0000SEFB03", "01.1.1.5","Cakes & Cookies",     "cakes, cupcakes, and cookies",     "Cereals & Grains"),

    # ── Meat & Poultry ───────────────────────────────────────────────────────
    ("CUUR0000SAF112", "01.1.2", "Meats, Poultry, Fish & Eggs", "aggregate meat/poultry/fish/eggs", "Meat & Poultry"),
    ("CUUR0000SEFC01", "01.1.2.2","Beef (Ground)",        "uncooked ground beef",             "Meat & Poultry"),
    ("CUUR0000SEFC02", "01.1.2.2","Beef (Roasts)",        "uncooked beef roasts",             "Meat & Poultry"),
    ("CUUR0000SEFC03", "01.1.2.2","Beef (Steaks)",        "uncooked beef steaks",             "Meat & Poultry"),
    ("CUUR0000SEFD01", "01.1.2.1","Bacon",                "bacon, breakfast sausage",         "Meat & Poultry"),
    ("CUUR0000SEFD02", "01.1.2.1","Ham",                  "ham (cured pork)",                 "Meat & Poultry"),
    ("CUUR0000SEFD03", "01.1.2.1","Pork Chops",           "pork chops",                       "Meat & Poultry"),
    ("CUUR0000SEFF01", "01.1.2.3","Chicken",              "chicken (all cuts)",               "Meat & Poultry"),
    ("CUUR0000SEFF02", "01.1.2.4","Turkey",               "turkey and other poultry",         "Meat & Poultry"),

    # ── Fish & Seafood ───────────────────────────────────────────────────────
    ("CUUR0000SEFG01", "01.1.3.1","Fish (Fresh)",         "fresh fish and seafood",           "Fish & Seafood"),
    ("CUUR0000SEFG02", "01.1.3.2","Fish (Processed)",     "processed fish and seafood",       "Fish & Seafood"),

    # ── Dairy & Eggs ─────────────────────────────────────────────────────────
    ("CUUR0000SEFJ01", "01.1.4.1","Milk",                 "fluid milk",                       "Dairy & Eggs"),
    ("CUUR0000SEFJ02", "01.1.4.4","Cheese",               "cheese and related products",      "Dairy & Eggs"),
    ("CUUR0000SEFJ03", "01.1.4.6","Ice Cream",            "ice cream and related products",   "Dairy & Eggs"),
    ("CUUR0000SEFJ04", "01.1.4.9","Other Dairy",          "other dairy and related products", "Dairy & Eggs"),
    ("CUUR0000SEFS01", "01.1.4.5","Butter & Margarine",   "butter and margarine",             "Dairy & Eggs"),

    # ── Oils & Fats ──────────────────────────────────────────────────────────
    ("CUUR0000SEFS02", "01.1.5.2","Salad Dressing",       "salad dressing",                   "Oils & Fats"),
    ("CUUR0000SEFS03", "01.1.5.1","Other Fats & Oils",    "other fats and oils, peanut butter","Oils & Fats"),

    # ── Fruits ───────────────────────────────────────────────────────────────
    ("CUUR0000SEFK01", "01.1.6.1","Apples",               "fresh apples",                     "Fruits"),
    ("CUUR0000SEFK02", "01.1.6.2","Bananas",              "fresh bananas",                    "Fruits"),
    ("CUUR0000SEFK03", "01.1.6.3","Citrus Fruits",        "fresh citrus (oranges, lemons)",   "Fruits"),
    ("CUUR0000SEFK04", "01.1.6.9","Other Fresh Fruits",   "other fresh fruits",               "Fruits"),

    # ── Vegetables ───────────────────────────────────────────────────────────
    ("CUUR0000SEFL01", "01.1.7.3","Potatoes",             "fresh potatoes",                   "Vegetables"),
    ("CUUR0000SEFL02", "01.1.7.4","Lettuce",              "fresh lettuce",                    "Vegetables"),
    ("CUUR0000SEFL03", "01.1.7.1","Tomatoes",             "fresh tomatoes",                   "Vegetables"),
    ("CUUR0000SEFL04", "01.1.7.9","Other Fresh Veg",      "other fresh vegetables",           "Vegetables"),
    ("CUUR0000SEFM01", "01.1.7.8","Canned Fruits & Veg",  "canned fruits and vegetables",     "Vegetables"),

    # ── Sugar & Sweets ───────────────────────────────────────────────────────
    ("CUUR0000SEFR01", "01.1.8.1","Sugar",                "sugar and sugar substitutes",      "Sugar & Spices"),
    ("CUUR0000SEFR02", "01.1.8.2","Candy",                "candy and chewing gum",            "Sugar & Spices"),

    # ── Beverages ────────────────────────────────────────────────────────────
    ("CUUR0000SEFP01", "01.2.1.2","Coffee",               "ground and instant coffee",        "Beverages"),
    ("CUUR0000SEFP02", "01.2.1.1","Tea",                  "tea and other beverage materials", "Beverages"),
    ("CUUR0000SEFN01", "01.2.2.2","Carbonated Drinks",    "carbonated soft drinks",           "Beverages"),
]

ITEM_META = {row[0]: row[1:] for row in ERS_SERIES}

ERS_ITEM_IDS = {row[0]: f"ERS-{row[0].replace('CUUR0000','')}" for row in ERS_SERIES}


# =============================================================================
# RELEASE DATE HELPER
# =============================================================================

def _release_date(year: int, month: int) -> str:
    """
    ERS Food Price Outlook is published ~70 days after reference month ends.

    This is the PIT-critical function — do NOT use the BLS 14-day offset here.
    BLS lag : ~14 days after month-end.
    ERS lag : ~70 days after month-end (8–12 week publication schedule).

    Mixing these lags causes false PIT violations:
      - Reporting_date too close to official_release_date → false PASS
      - backtesting window opens too early for ERS data
    """
    last_day  = calendar.monthrange(year, month)[1]
    month_end = datetime(year, month, last_day, tzinfo=timezone.utc)
    return (month_end + timedelta(days=ERS_RELEASE_LAG_DAYS)).isoformat()


# =============================================================================
# BLS API FETCHER  (reuses BLS_API_KEY; ERS publishes the same CPI data)
# =============================================================================

def fetch_cpi_data(
    series_ids: list[str], start_year: int, end_year: int
) -> dict[str, list[tuple[int, int, float]]]:
    """
    Fetch BLS CPI food-category series via the BLS Public Data API v2.
    Returns {series_id: [(year, month, cpi_index_value), ...]} sorted chronologically.

    CPI index values use 1982–84 = 100 as base.
    These are the same series ERS uses for their Food Price Outlook.
    """
    if not BLS_API_KEY:
        raise RuntimeError("BLS_API_KEY not found. Check your .env file.")

    combined: dict[str, list] = {sid: [] for sid in series_ids}

    year_windows = [
        (y, min(y + MAX_YEARS_PER_CALL - 1, end_year))
        for y in range(start_year, end_year + 1, MAX_YEARS_PER_CALL)
    ]
    batch_size = 50
    batches    = [series_ids[i: i + batch_size] for i in range(0, len(series_ids), batch_size)]

    total_calls = len(year_windows) * len(batches)
    call_n = 0

    for y_start, y_end in year_windows:
        for batch in batches:
            call_n += 1
            payload = {
                "seriesid":        batch,
                "startyear":       str(y_start),
                "endyear":         str(y_end),
                "registrationkey": BLS_API_KEY,
                "catalog":         False,
                "calculations":    False,
                "annualaverage":   False,
            }
            resp = requests.post(
                BLS_API_URL, json=payload, timeout=30, verify=_SSL_VERIFY
            )
            resp.raise_for_status()
            data = resp.json()
            if data.get("status") != "REQUEST_SUCCEEDED":
                print(f"  BLS warning call {call_n}/{total_calls}: {data.get('message', '')}")

            for series in data.get("Results", {}).get("series", []):
                sid = series["seriesID"]
                for obs in series.get("data", []):
                    period = obs.get("period", "")
                    if not period.startswith("M") or period == "M13":
                        continue
                    try:
                        val = float(obs["value"])
                    except (ValueError, KeyError):
                        continue
                    combined[sid].append((int(obs["year"]), int(period[1:]), val))

            time.sleep(REQUEST_DELAY)

    return {sid: sorted(rows) for sid, rows in combined.items()}


# =============================================================================
# TRANSFORMER
# =============================================================================

def transform(
    all_data: dict[str, list[tuple[int, int, float]]], extraction_ts: str
) -> pd.DataFrame:
    """
    Convert BLS CPI food-category observations to gold-standard Parquet schema.
    Stored as source=usda_ers with the ERS 70-day release lag.
    item_value = CPI index (1982–84 = 100), not a dollar price.
    pct_change_mom computed from consecutive CPI index levels.
    """
    rows = []

    for sid, obs_list in all_data.items():
        if not obs_list:
            continue
        meta = ITEM_META.get(sid)
        if not meta:
            continue
        coicop, item_name, description, category = meta
        internal_id = ERS_ITEM_IDS.get(sid, "ERS-UNKNOWN")

        prev_idx: float | None = None

        for (year, month, cpi_val) in obs_list:
            pct_change_mom: float | None = None
            if prev_idx is not None and prev_idx > 0:
                pct_change_mom = round((cpi_val - prev_idx) / prev_idx * 100, 4)
            prev_idx = cpi_val

            date_str  = f"{year:04d}-{month:02d}-01"
            last_day  = calendar.monthrange(year, month)[1]
            fx_date   = f"{year:04d}-{month:02d}-{last_day:02d}"
            data_ts   = pd.Timestamp(date_str, tz="UTC").isoformat()

            # ── CRITICAL PIT FIELD ──────────────────────────────────────────
            # ERS lag = 70 days after month-end, NOT 14 days like BLS APU.
            # The BLS APU series (source=bls) is published ~14 days after month-end.
            # ERS Food Price Outlook takes an additional ~8 weeks, so ~70 days total.
            # Using the BLS offset here would cause false PIT violations.
            release_ts = _release_date(year, month)

            # Skip months whose ERS release date hasn't passed yet.
            # Ingesting a month before ERS publishes it violates PIT anti-retroactive
            # ingestion: conversion_timestamp (now) < published_date (future).
            if pd.Timestamp(release_ts) > pd.Timestamp.utcnow():
                prev_idx = cpi_val  # still track for next pct_change_mom
                continue

            rows.append({
                # ── Identity ───────────────────────────────────────────────
                "record_id":                   str(uuid.uuid4()),
                # ── Country ────────────────────────────────────────────────
                "country_code":                "US",
                "iso_alpha3":                  "USA",
                "market_tier":                 "Developed",
                # ── Source metadata ────────────────────────────────────────
                "source":                      SOURCE,
                "source_agency":               "USDA ERS",
                "source_sub_category":         "Food Price Outlook CPI",
                "portal_url":                  ERS_PORTAL,
                "sovereign_series_id":         sid,
                "source_series_id":            sid,
                "dataset_id":                  sid,
                "release_frequency":           "Monthly",
                "extraction_method":           "api",
                # ── Item identity ──────────────────────────────────────────
                "internal_item_id":            internal_id,
                "data_vintage_id":             f"ERS-{sid}-{year:04d}-{month:02d}-v1",
                "confidence_tier":             "PRIMARY",
                "global_coicop_code":          coicop,
                "standard_name":               item_name,
                "local_name":                  description,
                "category":                    category,
                # ── Observation period ─────────────────────────────────────
                "observation_period":          f"{year:04d}-{month:02d}",
                "reporting_date":              data_ts,
                "data_timestamp":              data_ts,
                # ERS 70-day lag — NEVER apply BLS 14-day offset to ERS rows
                "official_release_date":       release_ts,
                "published_date":              release_ts,
                "as_of_date":                  release_ts,
                "conversion_timestamp":        extraction_ts,
                "is_revised_figure":           False,
                # ── Price (CPI index, not dollar price) ────────────────────
                # CPI index (1982-84=100); use source=bls APU for dollar prices
                "observed_price_local":        cpi_val,
                "price_usd_equivalent":        cpi_val,
                "currency":                    "USD",
                "fx_rate_applied":             1.0,
                "fx_rate_date":                fx_date,
                "unit_quantity_standardized":  1.0,
                "unit_measure_standardized":   "CPI index (1982-84=100)",
                "pct_change_mom":              pct_change_mom,
                # ── ERS/BLS provenance ─────────────────────────────────────
                "bls_raw_price":               cpi_val,
                "bls_unit":                    "CPI index (1982-84=100)",
                # ── Bitemporal PIT ─────────────────────────────────────────
                "revision_number":             0,
                "superseded_by":               None,
                "data_quality_certified":      False,
                "data_version":                VERSION,
            })

    return pd.DataFrame(rows)


# =============================================================================
# VAULT WRITER
# =============================================================================

def save_to_vault(df: pd.DataFrame) -> int:
    """Partition by year/month and write Parquet to vault. Returns partition count."""
    if df.empty:
        print("  No data to save.")
        return 0

    df = df.copy()
    df["data_timestamp"] = pd.to_datetime(df["data_timestamp"], utc=True, errors="coerce")
    df["_year"]  = df["data_timestamp"].dt.year
    df["_month"] = df["data_timestamp"].dt.month
    df = df.dropna(subset=["_year", "_month"])

    written = 0
    for (year, month), group in df.groupby(["_year", "_month"]):
        year, month = int(year), int(month)
        path = VAULT_ROOT / f"year={year}" / f"month={month:02d}" / FILE_NAME
        path.parent.mkdir(parents=True, exist_ok=True)
        out = group.drop(columns=["_year", "_month"], errors="ignore")

        if path.exists():
            existing = pd.read_parquet(path)
            out = (
                pd.concat([existing, out], ignore_index=True)
                .drop_duplicates(
                    subset=["sovereign_series_id", "reporting_date"], keep="last"
                )
            )

        out.to_parquet(path, engine="pyarrow", index=False)
        written += 1

    return written


# =============================================================================
# MAIN
# =============================================================================

def scrape_usa_ers_food_pricing(
    start_year: int = 1980, end_year: int | None = None
) -> int:
    if end_year is None:
        end_year = datetime.now(timezone.utc).year

    series_ids = [row[0] for row in ERS_SERIES]

    print(f"\n{'='*60}")
    print(f"  Lekwankwa — USDA ERS Food Price Outlook Scraper")
    print(f"  Coverage    : {start_year} – {end_year}")
    print(f"  Series      : {len(series_ids)} BLS CPI food-category series")
    print(f"  Data type   : CPI index (1982–84=100), monthly")
    print(f"  Source tag  : usda_ers  (ERS publishes via Food Price Outlook)")
    print(f"  Release lag : {ERS_RELEASE_LAG_DAYS} days after month-end [8–12 wk ERS schedule]")
    print(f"  Output      : {VAULT_ROOT}")
    print(f"{'='*60}\n")

    extraction_ts = datetime.now(timezone.utc).isoformat()

    print(f"  Fetching {start_year}–{end_year} from BLS API (CU* series)...", flush=True)
    try:
        all_data  = fetch_cpi_data(series_ids, start_year, end_year)
        total_obs = sum(len(v) for v in all_data.values())
        nodata    = [s for s, v in all_data.items() if not v]
        print(f"  Fetched {total_obs:,} observations")
        if nodata:
            print(f"  WARNING: {len(nodata)} series returned no data: {nodata}")
    except Exception as exc:
        print(f"  FAILED: {exc}")
        return 0

    print("  Transforming to gold-standard schema...", flush=True)
    df = transform(all_data, extraction_ts)
    print(f"  Transformed {len(df):,} records")

    print("  Writing to vault...", flush=True)
    partitions = save_to_vault(df)

    print(f"\n{'='*60}")
    print(f"  COMPLETE")
    print(f"  Records    : {len(df):,}")
    print(f"  Partitions : {partitions}")
    print(f"  Vault      : {VAULT_ROOT}")
    print(f"{'='*60}\n")
    return partitions


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="USDA ERS Food Price Outlook scraper (BLS CPI food categories, gold-standard vault)"
    )
    parser.add_argument(
        "--start-year", type=int, default=1980,
        help="Start year (default: 1980)"
    )
    parser.add_argument(
        "--end-year", type=int,
        default=datetime.now(timezone.utc).year,
        help="End year (default: current year)"
    )
    args = parser.parse_args()

    scrape_usa_ers_food_pricing(start_year=args.start_year, end_year=args.end_year)
