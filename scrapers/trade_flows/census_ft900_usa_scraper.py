"""
census_ft900_usa_scraper.py
Lekwankwa Corporation Pty Ltd

Scraper for US Census Bureau FT-900 International Trade in Goods — monthly.

  ┌──────────────────────────────────────────────────────────────────────────┐
  │  FT-900  — U.S. International Trade in Goods (Census Bureau)            │
  │  API:       https://api.census.gov/data/timeseries/intltrade/           │
  │  Coverage:  1989-01 → present  (monthly, HS 2-digit chapter level)      │
  │  Scope:     World total (all partner countries aggregated)               │
  │  Flows:     Exports (FOB) and Imports (General/CIF)                     │
  │  Vault:     product=trade_flows/country=USA/source=census_ft900         │
  │  Gold std:  schema gold standards/trade_flows.json                      │
  └──────────────────────────────────────────────────────────────────────────┘

Usage:
    # Incremental (default — cloud scheduler):
    python3.10 scrapers/trade_flows/census_ft900_usa_scraper.py

    # Override start point:
    python3.10 scrapers/trade_flows/census_ft900_usa_scraper.py --since 2025-01

    # Full historical backfill:
    python3.10 scrapers/trade_flows/census_ft900_usa_scraper.py --mode full --start-year 2010

Environment variable (optional — increases rate limits):
    CENSUS_API_KEY=<your-key>

Author: Lekwankwa Corporation
Date: 2026-06-13
"""

import argparse
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import requests

# Load .env so CENSUS_API_KEY is available when running directly
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
except ImportError:
    pass

# Incremental utilities
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scrapers.utilities.incremental import (
    compute_scrape_range_monthly, revision_upsert,
)

_LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
_LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(_LOG_DIR / "trade_scraper.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONSTANTS
# =============================================================================

CENSUS_BASE      = "https://api.census.gov/data/timeseries/intltrade"
EXPORTS_URL      = f"{CENSUS_BASE}/exports/hs"
IMPORTS_URL      = f"{CENSUS_BASE}/imports/hs"
CENSUS_PORTAL    = "https://www.census.gov/foreign-trade/"

VAULT_ROOT       = Path("lekwankwa-historical-vault/product=trade_flows/country=USA/source=census_ft900")
DATA_FILE_NAME   = "trade_flows_data.parquet"

TRADE_START_YEAR = 2010   # Census timeseries/intltrade API earliest available month
WORLD_CTY_CODE   = "0000" # Census code for all-country aggregate
REQUEST_DELAY    = 0.5    # seconds between API calls

# HS 2-digit chapter names (HS 2022 nomenclature)
HS2_NAMES = {
    "01": "Live Animals",
    "02": "Meat and Offal",
    "03": "Fish and Seafood",
    "04": "Dairy and Eggs",
    "05": "Other Animal Products",
    "06": "Live Trees and Plants",
    "07": "Vegetables",
    "08": "Fruits and Nuts",
    "09": "Coffee Tea and Spices",
    "10": "Cereals",
    "11": "Milling Products",
    "12": "Oil Seeds",
    "13": "Lac Gums and Resins",
    "14": "Vegetable Plaiting Materials",
    "15": "Animal and Vegetable Fats",
    "16": "Prepared Meat and Fish",
    "17": "Sugars",
    "18": "Cocoa",
    "19": "Cereal Preparations",
    "20": "Vegetable Preparations",
    "21": "Miscellaneous Food Preparations",
    "22": "Beverages and Spirits",
    "23": "Food Industry Residues",
    "24": "Tobacco",
    "25": "Salt Sulfur and Minerals",
    "26": "Ores Slag and Ash",
    "27": "Mineral Fuels and Oils",
    "28": "Inorganic Chemicals",
    "29": "Organic Chemicals",
    "30": "Pharmaceutical Products",
    "31": "Fertilizers",
    "32": "Tanning and Dyeing Extracts",
    "33": "Essential Oils and Perfumes",
    "34": "Soap and Waxes",
    "35": "Albuminoidal Substances",
    "36": "Explosives",
    "37": "Photographic Goods",
    "38": "Miscellaneous Chemical Products",
    "39": "Plastics",
    "40": "Rubber",
    "41": "Raw Hides and Skins",
    "42": "Leather Articles",
    "43": "Furskins",
    "44": "Wood and Articles of Wood",
    "45": "Cork",
    "46": "Straw Manufactures",
    "47": "Pulp of Wood",
    "48": "Paper and Paperboard",
    "49": "Printed Books and Newspapers",
    "50": "Silk",
    "51": "Wool and Animal Hair",
    "52": "Cotton",
    "53": "Other Vegetable Textiles",
    "54": "Man-Made Filaments",
    "55": "Man-Made Staple Fibres",
    "56": "Wadding and Felt",
    "57": "Carpets and Floor Coverings",
    "58": "Special Woven Fabrics",
    "59": "Impregnated Textiles",
    "60": "Knitted Fabrics",
    "61": "Knitted Apparel",
    "62": "Woven Apparel",
    "63": "Other Textile Articles",
    "64": "Footwear",
    "65": "Headgear",
    "66": "Umbrellas",
    "67": "Feathers and Artificial Flowers",
    "68": "Stone and Plaster Articles",
    "69": "Ceramic Products",
    "70": "Glass",
    "71": "Precious Stones and Metals",
    "72": "Iron and Steel",
    "73": "Iron and Steel Articles",
    "74": "Copper",
    "75": "Nickel",
    "76": "Aluminum",
    "78": "Lead",
    "79": "Zinc",
    "80": "Tin",
    "81": "Other Base Metals",
    "82": "Tools and Cutlery",
    "83": "Miscellaneous Metal Articles",
    "84": "Machinery and Mechanical Appliances",
    "85": "Electrical Machinery",
    "86": "Railway Equipment",
    "87": "Vehicles",
    "88": "Aircraft and Spacecraft",
    "89": "Ships and Boats",
    "90": "Optical and Precision Instruments",
    "91": "Clocks and Watches",
    "92": "Musical Instruments",
    "93": "Arms and Ammunition",
    "94": "Furniture and Bedding",
    "95": "Toys and Sports Equipment",
    "96": "Miscellaneous Manufactures",
    "97": "Works of Art",
    "98": "Special Import Provisions",
    "99": "Special Classification Provisions",
}


# =============================================================================
# API CLIENT
# =============================================================================

def _api_key() -> str:
    return os.environ.get("CENSUS_API_KEY", "")


def fetch_month(flow: str, year: int, month: int, api_key: str) -> list:
    """
    Fetch HS2-chapter trade data for one month, world total.

    Returns list of dicts (one per HS chapter). Empty list on error.
    """
    if flow == "exports":
        url            = EXPORTS_URL
        commodity_fld  = "E_COMMODITY"
        value_fld      = "ALL_VAL_MO"
    else:
        url            = IMPORTS_URL
        commodity_fld  = "I_COMMODITY"
        value_fld      = "GEN_VAL_MO"

    params = {
        "get":      f"{commodity_fld},{value_fld}",
        "COMM_LVL": "HS2",
        "time":     f"{year}-{month:02d}",
    }
    if api_key:
        params["key"] = api_key

    try:
        resp = requests.get(url, params=params, timeout=120)
        resp.raise_for_status()
        data = resp.json()
        if not data or len(data) < 2:
            return []
        headers = data[0]
        return [dict(zip(headers, row)) for row in data[1:]]
    except requests.HTTPError as exc:
        if exc.response is not None and exc.response.status_code == 404:
            logger.debug(f"  404 for {flow} {year}-{month:02d} (data not yet available)")
        else:
            logger.warning(f"  HTTP error {flow} {year}-{month:02d}: {exc}")
        return []
    except Exception as exc:
        logger.warning(f"  Error {flow} {year}-{month:02d}: {exc}")
        return []


# =============================================================================
# TRANSFORMER
# =============================================================================

def _release_date(year: int, month: int) -> str:
    """
    Approximate FT-900 release date: ~5-6 weeks after reference month end.
    Convention: day 5 of the month two months after the reference month.
    """
    rm = month + 2
    ry = year
    if rm > 12:
        rm -= 12
        ry += 1
    return f"{ry}-{rm:02d}-05T13:30:00+00:00"


def transform_rows(
    rows: list,
    flow: str,
    year: int,
    month: int,
    extraction_ts: str,
) -> pd.DataFrame:
    """Convert raw Census API rows into the vault schema."""
    if not rows:
        return pd.DataFrame()

    flow_label     = "Export" if flow == "exports" else "Import"
    flow_prefix    = "EXP"    if flow == "exports" else "IMP"
    commodity_fld  = "E_COMMODITY" if flow == "exports" else "I_COMMODITY"
    value_fld      = "ALL_VAL_MO"  if flow == "exports" else "GEN_VAL_MO"
    flow_metric_pfx = "EXPORTS_FOB" if flow == "exports" else "IMPORTS_CIF"
    source_url     = EXPORTS_URL if flow == "exports" else IMPORTS_URL

    reporting_date   = f"{year}-{month:02d}-01T00:00:00+00:00"
    official_release = _release_date(year, month)

    records = []
    for row in rows:
        hs_raw   = str(row.get(commodity_fld, "")).strip()
        hs_code  = hs_raw.zfill(2) if hs_raw.isdigit() else hs_raw
        time_str = row.get("time", "")

        # Skip non-chapter codes (e.g. totals coded as "00" or empty)
        if not hs_code or not hs_code.isdigit() or hs_code == "00":
            continue

        raw_val = row.get(value_fld)
        try:
            value_usd      = float(raw_val) if raw_val is not None else None
            value_millions = round(value_usd / 1_000_000.0, 6) if value_usd is not None else None
        except (TypeError, ValueError):
            value_millions = None

        commodity_name = HS2_NAMES.get(hs_code, f"HS Chapter {hs_code}")
        series_id      = f"HS{hs_code}_{flow_prefix}"
        safe_name      = (commodity_name.upper()
                          .replace(" AND ", "_")
                          .replace(" ", "_")
                          .replace("-", "_")[:24])
        macro_name     = f"{flow_metric_pfx}_{safe_name}_HS{hs_code}"

        records.append({
            "record_id":              str(uuid.uuid4()),
            "iso_alpha3":             "USA",
            "country_name":           "United States",
            "country_code":           "US",
            "market_tier":            "Developed",
            "partner_country_code":   WORLD_CTY_CODE,   # world total (no CTY_CODE filter)
            "partner_country_name":   "WORLD",
            "commodity_code":         hs_code,
            "commodity_name":         commodity_name,
            "trade_flow":             flow_label,
            "sovereign_series_id":    series_id,
            "source_series_id":       series_id,   # alias for bitemporal_core compatibility
            "data_vintage_id":        f"CENSUS-{series_id}-{year}-{month:02d}-v1",
            "macro_metric_name":      macro_name,
            "reporting_date":         reporting_date,
            "data_timestamp":         reporting_date,
            "official_release_date":  official_release,
            "published_date":         official_release,
            "as_of_date":             extraction_ts,
            "conversion_timestamp":   extraction_ts,
            "observed_value":         value_millions,
            "trade_value":            value_millions,
            "unit_of_measure":        "USD_MILLIONS",
            "currency":               "USD",
            "is_revised_figure":      False,
            "confidence_tier":        "PRIMARY",
            "source":                 "census_ft900",
            "source_agency":          "CENSUS",
            "source_sub_category":    "TRADE",
            "portal_url":             CENSUS_PORTAL,
            "source_url":             source_url,
            "extraction_method":      "api",
            "data_quality_certified": True,
            "revision_number":        0,
            "superseded_by":          None,
            "year":                   year,
            "month":                  month,
        })

    return pd.DataFrame(records)


# =============================================================================
# VAULT WRITER
# =============================================================================

def save_to_vault(df: pd.DataFrame) -> tuple[int, int]:
    """
    Revision-aware Hive-partitioned vault write.
    Exports and imports can be written in separate passes — rows for a different
    trade_flow direction are preserved from existing partitions.
    Returns (partitions_written, revisions_detected).
    """
    if df.empty:
        return 0, 0

    partitions_written = 0
    total_revisions    = 0
    out_cols = [c for c in df.columns if c not in ("year", "month")]

    for (year, month), group in df.groupby(["year", "month"]):
        year, month = int(year), int(month)
        path = VAULT_ROOT / f"year={year}" / f"month={month:02d}" / DATA_FILE_NAME
        new_rows = group[out_cols].copy()
        incoming_flows = set(new_rows["trade_flow"].unique())

        # Preserve existing rows for the OTHER trade flow direction
        if path.exists():
            existing = pd.read_parquet(path, engine="pyarrow")
            keep = existing[~existing["trade_flow"].isin(incoming_flows)]
            if not keep.empty:
                # Write keeper rows back first, then upsert incoming
                keep.to_parquet(path, engine="pyarrow", index=False)

        added, revs = revision_upsert(
            path, new_rows,
            key_cols=["sovereign_series_id", "reporting_date"],
            value_col="observed_value",
        )
        if added:
            partitions_written += 1
            total_revisions    += revs

    return partitions_written, total_revisions


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Scrape US Census Bureau FT-900 goods trade data (HS2 monthly, world total)"
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
        "--start-year", type=int, default=TRADE_START_YEAR,
        help=f"Start year for full mode (default: {TRADE_START_YEAR})",
    )
    parser.add_argument(
        "--end-year", type=int, default=None,
        help="End year for full mode (default: current year)",
    )
    parser.add_argument(
        "--flow", choices=["exports", "imports", "both"], default="both",
        help="Trade flow direction to scrape (default: both)",
    )
    parser.add_argument(
        "--api-key", default=None,
        help="Census API key (or set CENSUS_API_KEY env var)",
    )
    args = parser.parse_args()

    api_key       = args.api_key or _api_key()
    flows         = ["exports", "imports"] if args.flow == "both" else [args.flow]
    extraction_ts = datetime.now(timezone.utc).isoformat()

    # FT-900 lags ~5 weeks; cap at 2 months before today
    today     = datetime.now(timezone.utc)
    max_year  = today.year
    max_month = today.month - 2 if today.month > 2 else 12 + (today.month - 2)
    if today.month <= 2:
        max_year -= 1

    if args.mode == "incremental":
        start_year, start_month, end_year, end_month = compute_scrape_range_monthly(
            VAULT_ROOT, default_start_year=TRADE_START_YEAR, since=args.since,
        )
    else:
        start_year, start_month = max(args.start_year, TRADE_START_YEAR), 1
        end_year   = args.end_year or today.year
        end_month  = 12

    logger.info("=" * 70)
    logger.info("US CENSUS BUREAU FT-900 — INTERNATIONAL TRADE IN GOODS (HS2)")
    logger.info("=" * 70)
    logger.info("  Mode      : %s", args.mode)
    logger.info("  Range     : %d-%02d → %d-%02d", start_year, start_month, end_year, end_month)
    logger.info("  Flows     : %s", ", ".join(flows))
    logger.info("  API key   : %s", "YES" if api_key else "NO (public rate limits)")
    logger.info("  Data cap  : %d-%02d (FT-900 release lag)", max_year, max_month)
    logger.info("  Vault     : %s", VAULT_ROOT)
    logger.info("")

    total_records    = 0
    total_partitions = 0
    total_revisions  = 0

    for year in range(start_year, end_year + 1):
        m_start = start_month if year == start_year else 1
        m_end   = min(end_month, 12) if year == end_year else 12
        year_records = year_parts = 0

        for month in range(m_start, m_end + 1):
            if year > max_year or (year == max_year and month > max_month):
                continue   # data not yet published

            month_frames = []
            for flow in flows:
                rows = fetch_month(flow, year, month, api_key)
                if rows:
                    df_m = transform_rows(rows, flow, year, month, extraction_ts)
                    if not df_m.empty:
                        month_frames.append(df_m)
                time.sleep(REQUEST_DELAY)

            if month_frames:
                month_df        = pd.concat(month_frames, ignore_index=True)
                parts, revs     = save_to_vault(month_df)
                year_records   += len(month_df)
                year_parts     += parts
                total_revisions += revs
                logger.info("  %d-%02d: %d records → %d partitions (revisions=%d)",
                            year, month, len(month_df), parts, revs)
            else:
                logger.debug("  %d-%02d: No data", year, month)
            sys.stdout.flush()

        if year_records:
            logger.info("  ── %d total: %d records, %d partitions ──",
                        year, year_records, year_parts)
        total_records    += year_records
        total_partitions += year_parts

    logger.info("")
    logger.info("Trade scrape complete. Total: %d records, %d partitions, %d revisions",
                total_records, total_partitions, total_revisions)


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logger.error(f"Fatal error in main: {exc}", exc_info=True)
    sys.exit(0)
