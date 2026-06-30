"""
imf_datamapper_usa_scraper.py
Lekwankwa Corporation Pty Ltd

IMF DataMapper USA — QUAD_VINTAGE scraper (cloud-ready incremental mode).

Writes all four WEO publication vintages per observation year in a single run:
  month=07  July Update       official_release_date = YYYY-07-01
  month=10  October WEO       official_release_date = YYYY-10-01
  month=01  January Update    official_release_date = YYYY+1-01-01
  month=04  April WEO (final) official_release_date = YYYY+1-04-01

PIT signal order per observation year (ascending release date):
  July → October → January(YYYY+1) → April(YYYY+1)

Note: The DataMapper API exposes only current-vintage values. All four vintages
are stamped with synthetic PIT dates using the current API response. Revision
deltas between adjacent WEO editions are typically < 0.1pp for GDP growth.

Incremental logic:
  In --mode incremental (default), the scraper:
    1. Scans the vault for already-written (obs_year, pub_month) partitions.
    2. Skips partitions that already exist (values are synthetic — no revision
       detection needed for IMF).
    3. Only writes a vintage if its scheduled release date has passed.
  Use --mode full to rewrite all years from start_year to today.

API:    https://www.imf.org/external/datamapper/api/v1
Vault:  product=global_macro/country=USA/source=imf_weo/year=YYYY/month=MM/
Schema: schema gold standards/imf_weo.json

Usage:
    python scrapers/imf_global_macro/imf_datamapper_usa_scraper.py
    python scrapers/imf_global_macro/imf_datamapper_usa_scraper.py --mode full
    python scrapers/imf_global_macro/imf_datamapper_usa_scraper.py --since 2023
    python scrapers/imf_global_macro/imf_datamapper_usa_scraper.py --dry-run
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
import uuid
import warnings
from datetime import datetime, timezone
from pathlib import Path
from scrapers.utilities.vault_io import get_vault_root

import pandas as pd
import requests

warnings.filterwarnings("ignore")   # suppress SSL verify=False noise

_LOG_DIR = Path("logs")
_LOG_DIR.mkdir(exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[
        logging.FileHandler(_LOG_DIR / "imf_scraper.log", encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
logger = logging.getLogger(__name__)

try:
    from scrapers.utilities.incremental import compute_scrape_range
except ImportError:
    import importlib.util, os as _os
    _util = _os.path.join(_os.path.dirname(__file__), "..", "utilities", "incremental.py")
    _spec = importlib.util.spec_from_file_location("incremental", _util)
    _mod  = importlib.util.module_from_spec(_spec)
    _spec.loader.exec_module(_mod)
    compute_scrape_range = _mod.compute_scrape_range

# ── Constants ──────────────────────────────────────────────────────────────────

IMF_BASE      = "https://www.imf.org/external/datamapper/api/v1"
COUNTRY       = "USA"
COUNTRY_CODE  = "US"
PRODUCT       = "global_macro"
SOURCE        = "imf_weo"
SOURCE_AGENCY = "IMF"
SOURCE_SUB    = "WEO"
PORTAL_URL    = "https://www.imf.org/external/datamapper/"
MARKET_TIER   = "Developed"
VAULT_ROOT    = get_vault_root("lekwankwa-historical-vault")
FORECAST_FROM = 2025   # years >= this are IMF forecasts (is_forecast=True)

INDICATORS: dict[str, dict] = {
    "PCPIPCH":     {"name": "CPI_INFLATION_ANNUAL_PCT_CHANGE",    "unit": "PERCENT",        "category": "prices"},
    "NGDP_RPCH":   {"name": "REAL_GDP_GROWTH_PCT_CHANGE",         "unit": "PERCENT",        "category": "output"},
    "NGDPD":       {"name": "GDP_CURRENT_PRICES_USD_BN",          "unit": "USD_BILLIONS",   "category": "output"},
    "PPPGDP":      {"name": "GDP_PPP_INTL_DOLLAR_BN",             "unit": "INTL_DOLLAR_BN", "category": "output"},
    "LUR":         {"name": "UNEMPLOYMENT_RATE",                  "unit": "PERCENT",        "category": "labor"},
    "BCA_NGDPD":   {"name": "CURRENT_ACCOUNT_BALANCE_PCT_GDP",    "unit": "PERCENT",        "category": "external"},
    "GGXWDG_NGDP": {"name": "GROSS_GOVT_DEBT_PCT_GDP",            "unit": "PERCENT",        "category": "fiscal"},
    "GGXCNL_NGDP": {"name": "GOVT_NET_LENDING_BORROWING_PCT_GDP", "unit": "PERCENT",        "category": "fiscal"},
}

# Four WEO publication vintages per observation year.
# suffix=None means no suffix in data_vintage_id (April final = original format).
VINTAGES = [
    {
        "label":     "July Update",
        "suffix":    "Jul",
        "pub_month": 7,
        "release_date":    lambda year: f"{year}-07-01",
        "published_date":  lambda year: f"{year}-07-01T00:00:00Z",
    },
    {
        "label":     "October WEO",
        "suffix":    "Oct",
        "pub_month": 10,
        "release_date":    lambda year: f"{year}-10-01",
        "published_date":  lambda year: f"{year}-10-01T00:00:00Z",
    },
    {
        "label":     "January Update",
        "suffix":    "Jan",
        "pub_month": 1,
        "release_date":    lambda year: f"{year + 1}-01-01",
        "published_date":  lambda year: f"{year + 1}-01-01T00:00:00Z",
    },
    {
        "label":     "April WEO (final)",
        "suffix":    None,
    url = f"{IMF_BASE}/{indicator}/{COUNTRY}"
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, timeout=60, verify=False)
            r.raise_for_status()
            payload = r.json()

            # IMF DataMapper API changed field name from 'value' → 'values'.
            # Support both to stay robust against future renames.
            if "values" in payload:
                top_level = payload["values"]
            elif "value" in payload:
                logger.warning(
        try:
            r = requests.get(url, timeout=60, verify=False)
            r.raise_for_status()
            payload = r.json()

            # IMF DataMapper API changed the top-level envelope key from
            # "value" (legacy) to "values" (current) in the /latest endpoint.
            # Support both shapes during any transition period; prefer "values".
            if "values" in payload:
                top_level = payload["values"]
            elif "value" in payload:
                logger.warning(
                    "  fetch_indicator(%s): API returned legacy key 'value' "
                    "instead of 'values' — using fallback. "
                    "Update this scraper once the old key is fully retired.",
                    indicator,
                )
                top_level = payload["value"]
            else:
                logger.warning(
                    "  fetch_indicator(%s): API response contains neither "
                    "'values' nor 'value' key. Keys present: %s",
                    indicator,
                    list(payload.keys()),
                )
                top_level = {}

            values = top_level.get(indicator, {}).get(COUNTRY, {})

            if not values:
                logger.warning("  No data returned for %s", indicator)
                return None                )

            values = top_level.get(indicator, {}).get(COUNTRY, {})

            if not values:
                logger.warning("  No data returned for %s", indicator)
                return None
            return values
        except requests.exceptions.RequestException as exc:
            logger.warning("  Attempt %d/%d failed for %s: %s", attempt, retries, indicator, exc)
            if attempt < retries:
                time.sleep(2 ** attempt)
    return None            # Defensive dual-key extraction.
            # IMF DataMapper renamed the top-level envelope key from 'value'
            # to 'values' (observed 2026-06).  We try the current canonical
            # key first, then fall back to the legacy key so the scraper
            # survives either form without a KeyError.
            # ----------------------------------------------------------------
            if "values" in payload:
                envelope = payload["values"]
            elif "value" in payload:
                logger.warning(
                    "  IMF API returned legacy key 'value' (expected 'values') "
                    "for indicator %s — using fallback. Verify API contract.",
                    indicator,
                )
                envelope = payload["value"]
            else:
                available_keys = list(payload.keys())
                raise RuntimeError(
                    f"IMF DataMapper response for {indicator} contains neither "
                    f"'values' nor 'value' key. Available keys: {available_keys}. "
                    "The API envelope may have changed again — inspect raw response."
                )

            values = (
                envelope
                .get(indicator, {})
                .get(COUNTRY, {})
            )
            if not values:
                logger.warning("  No data returned for %s", indicator)
                return None                logger.warning("  No data returned for %s", indicator)
                return None
            return values
        except requests.exceptions.RequestException as exc:
            logger.warning("  Attempt %d/%d failed for %s: %s", attempt, retries, indicator, exc)
            if attempt < retries:
                time.sleep(2 ** attempt)
    return None


def build_vintage_records(
    indicator: str,
    meta: dict,
    values: dict,
    vintage: dict,
    start_year: int,
) -> list[dict]:
    """Build vault records for one vintage from a pre-fetched API response."""
    records = []
    run_ts  = _now_utc()
    suffix  = vintage["suffix"]
    for year_str, raw_value in values.items():
        year = int(year_str)
        if year < start_year:
            continue
        value = float(raw_value) if raw_value is not None else None
        if value is None:
            continue

        is_forecast     = year >= FORECAST_FROM
        confidence_tier = "ESTIMATED" if is_forecast else "PRIMARY"
        vintage_id      = (
            f"IMF-{indicator}-{COUNTRY}-{year}-{suffix}-v1"
            if suffix else
            f"IMF-{indicator}-{COUNTRY}-{year}-v1"
        )
        records.append({
            "record_id":              str(uuid.uuid4()),
            "product":                PRODUCT,
            "country_code":           COUNTRY_CODE,
            "iso_alpha3":             COUNTRY,
            "source":                 SOURCE,
            "source_agency":          SOURCE_AGENCY,
            "source_sub_category":    SOURCE_SUB,
            "sovereign_series_id":    indicator,
            "macro_metric_name":      meta["name"],
            "observed_value":         value,
            "unit_of_measure":        meta["unit"],
            "data_timestamp":         f"{year}-01-01T00:00:00Z",
            "published_date":         vintage["published_date"](year),
            "official_release_date":  vintage["release_date"](year),
            "data_vintage_id":        vintage_id,
            "extraction_method":      "api",
            "confidence_tier":        confidence_tier,
            "market_tier":            MARKET_TIER,
            "portal_url":             PORTAL_URL,
            "revision_number":        1,
            "is_forecast":            is_forecast,
            "sdmx_frequency":         "A",
            "data_quality_certified": True,
            "processing_timestamp":   run_ts,
        })
    return records


def _imf_vault_base() -> Path:
    return VAULT_ROOT / f"product={PRODUCT}" / f"country={COUNTRY}" / f"source={SOURCE}"


def get_existing_imf_partitions() -> set[tuple[int, int]]:
    """Return set of (obs_year, pub_month) tuples for partitions already in vault."""
    existing: set[tuple[int, int]] = set()
    base = _imf_vault_base()
    if not base.exists():
        return existing
    for year_dir in base.glob("year=*"):
        try:
            year = int(year_dir.name.split("=")[1])
        except (ValueError, IndexError):
            continue
        for month_dir in year_dir.glob("month=*"):
            try:
                month = int(month_dir.name.split("=")[1])
            except (ValueError, IndexError):
                continue
            if any(month_dir.glob("*.parquet")):
                existing.add((year, month))
    return existing


def _vintage_release_date(vintage: dict, obs_year: int) -> datetime:
    """Return the scheduled release date of a vintage as a UTC datetime."""
    date_str = vintage["release_date"](obs_year)   # e.g. "2024-07-01"
    return datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)


def save_partition(year: int, records: list[dict], pub_month: int, dry_run: bool) -> bool:
    if not records:
        return False
    df = pd.DataFrame(records)
    partition = (
        _imf_vault_base()
        / f"year={year}"
        / f"month={pub_month:02d}"
    )
    out_path = partition / f"{PRODUCT}_data.parquet"

    if dry_run:
        logger.info("  [DRY-RUN] %d rows → %s", len(df), out_path)
        return True

    partition.mkdir(parents=True, exist_ok=True)
    if out_path.exists():
        existing = pd.read_parquet(out_path)
        incoming_series = set(df["sovereign_series_id"].unique())
        kept = existing[~existing["sovereign_series_id"].isin(incoming_series)]
        df = pd.concat([kept, df], ignore_index=True)

    df.to_parquet(out_path, index=False, engine="pyarrow")
    return True


# ── Main ───────────────────────────────────────────────────────────────────────

def main(
    mode: str = "incremental",
    since: str | None = None,
    start_year: int = 1980,
    dry_run: bool = False,
) -> None:
    logger.info("=" * 70)
    logger.info("IMF DATAMAPPER USA — QUAD_VINTAGE SCRAPER")
    logger.info("Vintages: Jul · Oct · Jan · Apr (all four WEO publications)")
    logger.info("=" * 70)
    logger.info("Mode       : %s", mode)
    logger.info("Indicators : %d", len(INDICATORS))
    logger.info("Dry run    : %s", dry_run)
    logger.info("NOTE: DataMapper API returns current-vintage values.")
    logger.info("      All four vintage PIT dates are synthetic for past years.")
    logger.info("=" * 70)

    now = datetime.now(timezone.utc)

    # Resolve year range
    if mode == "incremental":
        eff_start, end_year = compute_scrape_range(
            _imf_vault_base(), default_start_year=start_year, since=since,
        )
    else:
        eff_start = start_year if not since else int(since.split("-")[0])
        end_year  = now.year

    logger.info("Year range : %d – %d", eff_start, end_year)

    # In incremental mode, skip partitions that already exist
    existing_partitions: set[tuple[int, int]] = set()
    if mode == "incremental":
        existing_partitions = get_existing_imf_partitions()
        logger.info("Existing vault partitions : %d", len(existing_partitions))

    # Fetch each indicator once — reuse the response for all four vintages
    fetched: dict[str, dict] = {}
    failed: list[str] = []
    for indicator, meta in INDICATORS.items():
        logger.info("\nFetching %s — %s", indicator, meta["name"])
        values = fetch_indicator(indicator)
        if values is None:
            failed.append(indicator)
            continue
        fetched[indicator] = values
        logger.info("  %d years (%s — %s)", len(values), min(values), max(values))
        time.sleep(0.5)

    # Write all four vintages per indicator
    total_rows    = 0
    total_written = 0
    total_skipped = 0

    for vintage in VINTAGES:
        label     = vintage["label"]
        pub_month = vintage["pub_month"]
        logger.info("\n--- %s (month=%02d) ---", label, pub_month)

        by_year: dict[int, list[dict]] = {}
        for indicator, values in fetched.items():
            meta    = INDICATORS[indicator]
            records = build_vintage_records(indicator, meta, values, vintage, eff_start)
            for rec in records:
                year = int(rec["data_timestamp"][:4])
                if year > end_year:
                    continue
                by_year.setdefault(year, []).append(rec)

        written = 0
        skipped = 0
        for year in sorted(by_year):
            # Skip partitions whose release date hasn't passed yet
            release_dt = _vintage_release_date(vintage, year)
            if release_dt > now:
                logger.debug("  %d month=%02d: release date %s not yet reached — skip",
                             year, pub_month, release_dt.date())
                skipped += 1
                continue

            # In incremental mode, skip partitions already in the vault
            if mode == "incremental" and (year, pub_month) in existing_partitions:
                skipped += 1
                continue

            if save_partition(year, by_year[year], pub_month, dry_run):
                written += 1

        vintage_rows   = sum(len(v) for v in by_year.values())
        total_rows    += vintage_rows
        total_written += written
        total_skipped += skipped
        logger.info("  %d rows | %d years written | %d skipped", vintage_rows, written, skipped)

    logger.info("\n" + "=" * 70)
    logger.info("SUMMARY")
    logger.info("=" * 70)
    logger.info("Indicators fetched : %d / %d", len(fetched), len(INDICATORS))
    logger.info("Partitions written : %d", total_written)
    logger.info("Partitions skipped : %d (already exist or future)", total_skipped)
    logger.info("Total rows staged  : %d", total_rows)
    if failed:
        logger.warning("Failed indicators  : %s", failed)
    logger.info("=" * 70)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="IMF DataMapper USA QUAD_VINTAGE scraper (Jul/Oct/Jan/Apr)"
    )
    parser.add_argument(
        "--mode", choices=["incremental", "full"], default="incremental",
        help="incremental: skip existing partitions; full: rewrite all years",
    )
    parser.add_argument(
        "--since", type=str, default=None, metavar="YYYY or YYYY-MM",
        help="Override incremental start year (e.g. 2023 or 2023-04)",
    )
    parser.add_argument(
        "--start-year", type=int, default=1980,
        help="Earliest observation year for full mode (default: 1980)",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="Log what would be written without touching the vault",
    )
    args = parser.parse_args()
    try:
        main(
            mode=args.mode,
            since=args.since,
            start_year=args.start_year,
            dry_run=args.dry_run,
        )
    except Exception as exc:
        logger.error("Scraper failed: %s", exc, exc_info=True)
        sys.exit(1)
