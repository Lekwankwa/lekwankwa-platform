"""
bls_ces_cps_usa_scraper.py
Lekwankwa Corporation Pty Ltd

Scraper for two BLS survey programs covering US employment:
  ┌─────────────────────────────────────────────────────────────────────┐
  │  CES  — Current Employment Statistics (payroll survey)              │
  │  Source: BLS REST API  /publicAPI/v2/timeseries/data/               │
  │  Series prefix: CES  (e.g. CES0000000001 = Total Nonfarm)           │
  │  Coverage: 1939-01 → present  (monthly)                             │
  │  Vault path: product=wages_and_employment/country=USA/source=bls_ces │
  │  Gold standard: schema gold standards/unemployment.json             │
  ├─────────────────────────────────────────────────────────────────────┤
  │  CPS  — Current Population Survey (household survey)                │
  │  Source: BLS REST API  /publicAPI/v2/timeseries/data/               │
  │  Series prefix: LNS  (e.g. LNS14000000 = U-3 Unemployment Rate)    │
  │  Coverage: 1948-01 → present  (monthly)                             │
  │  Vault path: product=wages_and_employment/country=USA/source=bls_cps │
  │  Gold standard: schema gold standards/unemployment.json             │
  └─────────────────────────────────────────────────────────────────────┘

Both programs are published on the same BLS release calendar (first Friday
of the month following the reference period) and therefore share the same
scrape cadence.

Usage:
    # Incremental (default — cloud scheduler):
    python3.10 scrapers/wages_employment/bls_ces_cps_usa_scraper.py

    # Override start point:
    python3.10 scrapers/wages_employment/bls_ces_cps_usa_scraper.py --since 2025-01

    # Full historical backfill:
    python3.10 scrapers/wages_employment/bls_ces_cps_usa_scraper.py --mode full --dataset ces

Environment variable (optional — increases rate limits):
    BLS_API_KEY=<your-key>

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

# Load .env so BLS_API_KEY is available when running directly
try:
    from dotenv import load_dotenv
    load_dotenv(Path(__file__).parent.parent.parent / ".env")
except ImportError:
    pass

# Incremental utilities
sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scrapers.utilities.incremental import (
    compute_scrape_range, revision_upsert, BLS_KNOWN_GAPS,
)

_LOG_DIR = Path(__file__).resolve().parents[2] / "logs"
_LOG_DIR.mkdir(exist_ok=True)
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(_LOG_DIR / "wages_scraper.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

# =============================================================================
# CONSTANTS
# =============================================================================

BLS_API_URL  = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
BLS_PORTAL   = "https://www.bls.gov"
VAULT_ROOT   = get_vault_root("lekwankwa-historical-vault/product=wages_and_employment/country=USA")

# BLS API limits
MAX_YEARS_PER_CALL = 20   # BLS API ceiling
MAX_SERIES_PER_CALL_KEYED   = 50
MAX_SERIES_PER_CALL_UNKEYED = 25
REQUEST_DELAY = 0.5       # seconds between API calls

# CES: Current Employment Statistics ─────────────────────────────────────────
CES_START_YEAR = 1939
CES_SOURCE     = "bls_ces"
CES_FILE_NAME  = "ces_data.parquet"

# Core CES series (supersector level + key wage series)
CES_SERIES = [
    "CES0000000001",   # Total nonfarm — all employees (SA)
    "CES0500000001",   # Total private — all employees (SA)
    "CES0500000003",   # Total private — avg hourly earnings (SA)
    "CES0500000006",   # Total private — avg hourly earnings, production/nonsupervisory (SA)
    "CES0500000007",   # Total private — avg weekly earnings (SA)
    "CES1000000001",   # Mining and logging (SA)
    "CES2000000001",   # Construction (SA)
    "CES3000000001",   # Manufacturing (SA)
    "CES4000000001",   # Trade, transportation and utilities (SA)
    "CES5000000001",   # Information (SA)
    "CES5500000001",   # Financial activities (SA)
    "CES6000000001",   # Professional and business services (SA)
    "CES6500000001",   # Education and health services (SA)
    "CES7000000001",   # Leisure and hospitality (SA)
    "CES8000000001",   # Other services (SA)
    "CES9000000001",   # Government (SA)
    "CES0500000008",   # Total private — aggregate weekly hours index (SA)
    "CES3000000006",   # Manufacturing — avg hourly earnings (SA)
    "CES3000000007",   # Manufacturing — avg weekly earnings (SA)
]

# Data-type-code (last 2 chars of CES series ID) → (metric_name, unit)
CES_DATA_TYPE_MAP = {
    "01": ("NONFARM_PAYROLL_EMPLOYMENT",   "THOUSANDS_PERSONS"),
    "02": ("PRODUCTION_WORKERS",           "THOUSANDS_PERSONS"),
    "03": ("AVG_WEEKLY_HOURS",             "HOURS"),
    "06": ("AVG_HOURLY_EARNINGS",          "USD"),
    "07": ("AVG_WEEKLY_EARNINGS",          "USD"),
    "08": ("AGGREGATE_WEEKLY_HOURS_INDEX", "INDEX"),
    "09": ("WOMEN_EMPLOYEES",              "THOUSANDS_PERSONS"),
    "11": ("OVERTIME_HOURS",               "HOURS"),
    "26": ("PAYROLL_3MTH_AVG_CHANGE",      "THOUSANDS_PERSONS"),
}

# CES supersector codes (positions 3-10 in series ID, 8 digits) → name
CES_SUPERSECTOR_MAP = {
    "00000000": "Total Nonfarm",
    "05000000": "Total Private",
    "10000000": "Mining and Logging",
    "20000000": "Construction",
    "30000000": "Manufacturing",
    "40000000": "Trade Transportation and Utilities",
    "50000000": "Information",
    "55000000": "Financial Activities",
    "60000000": "Professional and Business Services",
    "65000000": "Education and Health Services",
    "70000000": "Leisure and Hospitality",
    "80000000": "Other Services",
    "90000000": "Government",
}

# CPS: Current Population Survey ─────────────────────────────────────────────
CPS_START_YEAR = 1948
CPS_SOURCE     = "bls_cps"
CPS_FILE_NAME  = "cps_data.parquet"

# Core CPS series (national labour force / unemployment)
CPS_SERIES = [
    "LNS14000000",   # Unemployment rate U-3 (SA) — headline
    "LNS13000000",   # Number unemployed (SA, thousands)
    "LNS12000000",   # Employment level (SA, thousands)
    "LNS11000000",   # Civilian labour force (SA, thousands)
    "LNS11300000",   # Labour force participation rate (SA)
    "LNS12300000",   # Employment-population ratio (SA)
    "LNS14032183",   # U-6 total unemployment incl. underemployed (SA)
    "LNS14000006",   # Unemployment rate, 16-19 years (SA)
    "LNS14000009",   # Unemployment rate, men 20+ (SA)
    "LNS14000012",   # Unemployment rate, women 20+ (SA)
    "LNS14000031",   # Unemployment rate, White (SA)
    "LNS14000006",   # Unemployment rate, 16-19 (SA) — youth proxy
    "LNS13327659",   # Long-term unemployed, 27+ weeks (SA, thousands)
]

CPS_SERIES = list(dict.fromkeys(CPS_SERIES))   # deduplicate, preserve order

# Series metadata for macro_metric_name / unit_of_measure
CPS_SERIES_META = {
    "LNS14000000": ("UNEMPLOYMENT_RATE_U3",              "PERCENTAGE"),
    "LNS13000000": ("UNEMPLOYMENT_LEVEL",                "THOUSANDS_PERSONS"),
    "LNS12000000": ("EMPLOYMENT_LEVEL",                  "THOUSANDS_PERSONS"),
    "LNS11000000": ("CIVILIAN_LABOR_FORCE",              "THOUSANDS_PERSONS"),
    "LNS11300000": ("LABOR_FORCE_PARTICIPATION_RATE",    "PERCENTAGE"),
    "LNS12300000": ("EMPLOYMENT_POPULATION_RATIO",       "PERCENTAGE"),
    "LNS14032183": ("UNEMPLOYMENT_RATE_U6",              "PERCENTAGE"),
    "LNS14000006": ("UNEMPLOYMENT_RATE_YOUTH_16_19",     "PERCENTAGE"),
    "LNS14000009": ("UNEMPLOYMENT_RATE_MEN_20PLUS",      "PERCENTAGE"),
    "LNS14000012": ("UNEMPLOYMENT_RATE_WOMEN_20PLUS",    "PERCENTAGE"),
    "LNS14000031": ("UNEMPLOYMENT_RATE_WHITE",           "PERCENTAGE"),
    "LNS13327659": ("LONG_TERM_UNEMPLOYED_27WEEKS_PLUS", "THOUSANDS_PERSONS"),
}


# =============================================================================
# BLS API CLIENT
# =============================================================================

class BLSAPIClient:
    def __init__(self, api_key: str | None = None):
        self.api_key  = api_key or os.getenv("BLS_API_KEY")
        self.max_series = (MAX_SERIES_PER_CALL_KEYED if self.api_key
                           else MAX_SERIES_PER_CALL_UNKEYED)
        if not self.api_key:
            logger.warning(
                "No BLS_API_KEY found — using public API (25 series/call, "
                "daily rate limit may apply). Set BLS_API_KEY env var to increase limits."
            )

    def fetch(self, series_ids: list, start_year: int, end_year: int) -> dict:
        """Call BLS API. Returns the raw JSON response."""
        payload = {
            "seriesid":  series_ids,
            "startyear": str(start_year),
            "endyear":   str(end_year),
            "catalog":   True,
            "calculations": False,
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
        """Paginate across year windows and all series batches. Returns list of raw data items.
        Known BLS gap months are filtered out before returning."""
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
                logger.info("  API call %d/%d: %d series, %d-%d",
                            call_n, total_calls, len(batch), y_start, y_end)
                try:
                    raw = self.fetch(batch, y_start, y_end)
                    status = raw.get("status", "")
                    if status != "REQUEST_SUCCEEDED":
                        logger.warning("  BLS API status=%s: %s",
                                       status, raw.get("message", "(no message)"))
                    for series in raw.get("Results", {}).get("series", []):
                        for obs in series.get("data", []):
                            period = obs.get("period", "")
                            if period.startswith("M") and period != "M13":
                                yr = int(obs.get("year", 0))
                                mo = int(period[1:])
                                if (yr, mo) in BLS_KNOWN_GAPS:
                                    continue   # skip known funding-lapse months
                            obs["seriesID"] = series["seriesID"]
                            all_items.append(obs)
                except requests.HTTPError as exc:
                    logger.error("  HTTP error: %s", exc)
                except Exception as exc:
                    logger.error("  Unexpected error: %s", exc)
                time.sleep(REQUEST_DELAY)

        return all_items


# =============================================================================
# DATA TRANSFORMERS
# =============================================================================

def _period_to_date(year: str, period: str) -> str | None:
    """Convert BLS period (M01-M12) to ISO date string YYYY-MM-01."""
    if not period.startswith("M") or period == "M13":
        return None  # skip annual averages
    month = int(period[1:])
    return f"{year}-{month:02d}-01"


def _extract_seasonal_adj(series_id: str) -> str:
    """BLS convention: 4th char is S (seasonally adjusted) or U (unadjusted)."""
    if len(series_id) >= 4:
        return series_id[3].upper() if series_id[3].upper() in ("S", "U") else "U"
    return "U"


def transform_ces(items: list, extraction_ts: str) -> pd.DataFrame:
    """Convert raw BLS API items to CES gold-standard parquet schema."""
    rows = []
    for obs in items:
        date_str = _period_to_date(obs.get("year", ""), obs.get("period", ""))
        if date_str is None:
            continue
        sid   = obs["seriesID"]
        value_str = obs.get("value", "")
        try:
            value = float(value_str)
        except (ValueError, TypeError):
            value = None

        # Supersector and data-type from series ID structure
        # CES + 8-digit supersector + 2-digit data type = 13 chars
        industry_code = sid[3:11] if len(sid) >= 11 else "00000000"
        data_type     = sid[-2:] if len(sid) >= 2 else "01"
        metric_name, unit = CES_DATA_TYPE_MAP.get(data_type, ("EMPLOYMENT_METRIC", "UNITS"))

        # Estimate release date: BLS publishes ~1 month after reference month
        try:
            ref_dt = pd.Timestamp(date_str, tz="UTC")
            released = (ref_dt + pd.DateOffset(months=1)).strftime("%Y-%m-01")
        except Exception:
            released = date_str

        data_ts = pd.Timestamp(date_str, tz="UTC").isoformat()

        rows.append({
            # ── Identity ──
            "record_id":              str(uuid.uuid4()),
            # ── Gold standard (wages.json) ──
            "iso_alpha3":             "USA",
            "country_name":           "United States",
            "country_code":           "US",
            "market_tier":            "Developed",
            "source_agency":          "BLS",
            "source_sub_category":    "CES",
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
            # ── Operational ──
            "seasonal_adjustment":    _extract_seasonal_adj(sid),
            "industry_code":          industry_code,
            "industry_name":          CES_SUPERSECTOR_MAP.get(industry_code, ""),
            "source":                 "bls_ces",
            "extraction_method":      "api",
            "data_quality_certified": False,
            "conversion_timestamp":   extraction_ts,
            "as_of_date":             pd.Timestamp(released, tz="UTC").isoformat(),
            "revision_number":        0,
            "superseded_by":          None,
            # ── Raw metadata ──
            "bls_footnotes":          str(obs.get("footnotes", "")),
        })
    return pd.DataFrame(rows)


def transform_cps(items: list, extraction_ts: str) -> pd.DataFrame:
    """Convert raw BLS API items to CPS gold-standard parquet schema."""
    rows = []
    for obs in items:
        date_str = _period_to_date(obs.get("year", ""), obs.get("period", ""))
        if date_str is None:
            continue
        sid   = obs["seriesID"]
        value_str = obs.get("value", "")
        try:
            value = float(value_str)
        except (ValueError, TypeError):
            value = None

        metric_name, unit = CPS_SERIES_META.get(sid, ("CPS_LABOR_METRIC", "UNITS"))

        try:
            ref_dt   = pd.Timestamp(date_str, tz="UTC")
            released = (ref_dt + pd.DateOffset(months=1)).strftime("%Y-%m-01")
        except Exception:
            released = date_str

        data_ts = pd.Timestamp(date_str, tz="UTC").isoformat()

        rows.append({
            # ── Identity ──
            "record_id":              str(uuid.uuid4()),
            # ── Gold standard (unemployment.json) ──
            "iso_alpha3":             "USA",
            "country_name":           "United States",
            "country_code":           "US",
            "market_tier":            "Developed",
            "source_agency":          "BLS",
            "source_sub_category":    "CPS",
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
            # ── Operational ──
            "seasonal_adjustment":    _extract_seasonal_adj(sid),
            "industry_code":          "00000000",     # CPS = national aggregate
            "industry_name":          "National (All Industries)",
            "source":                 "bls_cps",
            "extraction_method":      "api",
            "data_quality_certified": False,
            "conversion_timestamp":   extraction_ts,
            "as_of_date":             pd.Timestamp(released, tz="UTC").isoformat(),
            "revision_number":        0,
            "superseded_by":          None,
            # ── Raw metadata ──
            "bls_footnotes":          str(obs.get("footnotes", "")),
        })
    return pd.DataFrame(rows)


# =============================================================================
# VAULT WRITER
# =============================================================================

def save_to_vault(df: pd.DataFrame, source: str, file_name: str) -> tuple[int, int]:
    """Partition by year/month and revision-upsert to vault. Returns (partitions, revisions)."""
    if df.empty:
        logger.warning("  No data to save for source=%s", source)
        return 0, 0

    df = df.copy()
    df["data_timestamp"] = pd.to_datetime(df["data_timestamp"], utc=True, errors="coerce")
    df["_year"]  = df["data_timestamp"].dt.year
    df["_month"] = df["data_timestamp"].dt.month
    df = df.dropna(subset=["_year", "_month"])

    partitions_written = 0
    total_revisions    = 0
    for (year, month), group in df.groupby(["_year", "_month"]):
        year, month = int(year), int(month)
        path = (VAULT_ROOT / f"source={source}" / f"year={year}"
                / f"month={month:02d}" / file_name)
        out = group.drop(columns=["_year", "_month"], errors="ignore")
        added, revs = revision_upsert(
            path, out,
            key_cols=["sovereign_series_id", "reporting_date"],
            value_col="observed_value",
        )
        if added:
            partitions_written += 1
            total_revisions    += revs

    logger.info("  source=%s → %d partitions, %d revisions",
                source, partitions_written, total_revisions)
    return partitions_written, total_revisions


# =============================================================================
# MAIN
# =============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="Scrape BLS CES and CPS data into the Lekwankwa employment vault"
    )
    parser.add_argument(
        "--mode", choices=["incremental", "full"], default="incremental",
        help="incremental: auto-detect vault latest and append; full: re-scrape everything",
    )
    parser.add_argument(
        "--since", type=str, default=None, metavar="YYYY-MM",
        help="Override incremental start point (e.g. 2025-01)",
    )
    parser.add_argument("--start-year", type=int, default=1939,
                        help="Start year for full mode (default: 1939 for CES / 1948 for CPS)")
    parser.add_argument("--end-year",   type=int, default=None,
                        help="End year for full mode (default: current year)")
    parser.add_argument("--dataset",    choices=["ces", "cps", "both"], default="both",
                        help="Which dataset to scrape (default: both)")
    parser.add_argument("--api-key",    default=None,
                        help="BLS API registration key (or set BLS_API_KEY env var)")
    args = parser.parse_args()

    end_year      = args.end_year or datetime.now(timezone.utc).year
    extraction_ts = datetime.now(timezone.utc).isoformat()
    client        = BLSAPIClient(api_key=args.api_key)

    logger.info("=" * 70)
    logger.info("BLS CES/CPS USA WAGES & EMPLOYMENT SCRAPER")
    logger.info("Mode: %s  |  Dataset: %s", args.mode, args.dataset)
    logger.info("=" * 70)

    # ── CES ──────────────────────────────────────────────────────────────────
    if args.dataset in ("ces", "both"):
        logger.info("")
        logger.info("DATASET: CES — Current Employment Statistics")
        ces_vault = VAULT_ROOT / f"source={CES_SOURCE}"
        if args.mode == "incremental":
            ces_start, end_year = compute_scrape_range(
                ces_vault, default_start_year=CES_START_YEAR, since=args.since,
            )
        else:
            ces_start = max(args.start_year, CES_START_YEAR)

        logger.info("  Series: %d, years: %d-%d", len(CES_SERIES), ces_start, end_year)
        items = client.fetch_all_years(CES_SERIES, ces_start, end_year)
        logger.info("  Raw observations fetched: %d", len(items))
        if items:
            df_ces = transform_ces(items, extraction_ts)
            logger.info("  Records transformed: %d", len(df_ces))
            save_to_vault(df_ces, CES_SOURCE, CES_FILE_NAME)
        else:
            logger.warning("  No CES data returned by API")

    # ── CPS ──────────────────────────────────────────────────────────────────
    if args.dataset in ("cps", "both"):
        logger.info("")
        logger.info("DATASET: CPS — Current Population Survey")
        cps_vault = VAULT_ROOT / f"source={CPS_SOURCE}"
        if args.mode == "incremental":
            cps_start, end_year = compute_scrape_range(
                cps_vault, default_start_year=CPS_START_YEAR, since=args.since,
            )
        else:
            cps_start = max(args.start_year, CPS_START_YEAR)

        logger.info("  Series: %d, years: %d-%d", len(CPS_SERIES), cps_start, end_year)
        items = client.fetch_all_years(CPS_SERIES, cps_start, end_year)
        logger.info("  Raw observations fetched: %d", len(items))
        if items:
            df_cps = transform_cps(items, extraction_ts)
            logger.info("  Records transformed: %d", len(df_cps))
            save_to_vault(df_cps, CPS_SOURCE, CPS_FILE_NAME)
        else:
            logger.warning("  No CPS data returned by API")

    logger.info("")
    logger.info("Wages/employment scrape complete.")


if __name__ == "__main__":
    try:
        main()
    except Exception as exc:
        logger.error(f"Fatal error in main: {exc}", exc_info=True)
    sys.exit(0)
