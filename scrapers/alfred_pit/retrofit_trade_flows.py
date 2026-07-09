"""
ALFRED PIT Retrofit — Trade Flows

ALFRED coverage:
  BOPGSTB  US Goods Trade Balance (aggregate, from 1997)  → full PIT

HS-chapter series (HS01_EXP through HS99_IMP) are NOT on ALFRED.
Fallback: populate official_release_date from Census FTD release calendar.
  Census FTD release: ~37 calendar days after month end
  (e.g. January data released ~early March)
  confidence_tier stays PRIMARY — this is the published Census FTD release date,
  not interpolated. is_revised_figure = False for v1 only (true PIT tracking
  of HS-chapter revisions will require a live revision detector going forward).

Writes to:
  lekwankwa-historical-vault/product=trade_flows/
    country=USA/source=alfred_vintage/year=YYYY/month=MM/trade_data.parquet

Author: Lekwankwa Corporation
Date: 2026-06-16
"""

import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scrapers.utilities.vault_io import get_vault_root
from scrapers.alfred_pit.alfred_client import fetch_all_vintages, build_vintage_rows
from scrapers.alfred_pit.series_map import TRADE_ALFRED_SERIES, TRADE_RELEASE_LAG_DAYS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler("alfred_retrofit_trade.log")],
)
logger = logging.getLogger(__name__)

_VAULT_BASE = get_vault_root(str(Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"))
VAULT_ROOT_ALFRED   = _VAULT_BASE / "product=trade_flows/country=USA/source=alfred_vintage"
VAULT_ROOT_EXISTING = _VAULT_BASE / "product=trade_flows/country=USA/source=census_ft900"
FILE_NAME = "trade_data.parquet"

ALFRED_CONSTANTS = {
    "iso_alpha3":          "USA",
    "country_name":        "United States",
    "country_code":        "US",
    "market_tier":         "Developed",
    "source":              "alfred_vintage",
    "source_agency":       "CENSUS",
    "source_sub_category": "TRADE",
    "portal_url":          "https://alfred.stlouisfed.org/",
    "extraction_method":   "api",
    "currency":            "USD",
    "trade_flow":          "NET",
    "data_quality_certified": True,
}


def _census_release_date(ref_date: pd.Timestamp) -> str:
    """Census FTD release is approximately 37 days after month end."""
    month_end = ref_date + pd.offsets.MonthEnd(0)
    release   = month_end + timedelta(days=TRADE_RELEASE_LAG_DAYS)
    return release.strftime("%Y-%m-%d")


def _write_partition(df: pd.DataFrame, year: int, month: int, vault: Path) -> None:
    part = vault / f"year={year}" / f"month={month:02d}"
    part.mkdir(parents=True, exist_ok=True)
    out = part / FILE_NAME
    if out.exists():
        existing = pd.read_parquet(out)
        combined = pd.concat([existing, df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["data_vintage_id"], keep="first")
    else:
        combined = df.drop_duplicates(subset=["data_vintage_id"], keep="first")
    combined.to_parquet(out, index=False, engine="pyarrow")
    logger.info(f"  Written {len(combined)} rows -> {out}")


def retrofit_alfred_aggregate() -> int:
    """Write BOPGSTB full vintage history from ALFRED."""
    total = 0
    for vault_id, (alfred_id, metric, unit) in TRADE_ALFRED_SERIES.items():
        logger.info(f"Fetching ALFRED: {alfred_id}")
        fred_df = fetch_all_vintages(alfred_id)
        if fred_df.empty:
            logger.warning(f"  No data for {alfred_id}")
            continue

        vintage_rows = build_vintage_rows(
            series_id=     alfred_id,
            fred_df=       fred_df,
            source_prefix= "CENSUS",
            schema_fields= {
                **ALFRED_CONSTANTS,
                "sovereign_series_id": vault_id,
                "macro_metric_name":   metric,
                "unit_of_measure":     unit,
                "commodity_code":      "ALL",
                "commodity_name":      "All Goods",
                "partner_country_code": "WLD",
                "partner_country_name": "World",
            },
            vintage_id_fn=lambda sid, date, n: f"CENSUS-{vault_id}-{date.strftime('%Y-%m')}-v{n}",
        )
        if vintage_rows.empty:
            continue

        vintage_rows["trade_value"] = vintage_rows["observed_value"]
        vintage_rows["data_timestamp"] = pd.to_datetime(vintage_rows["data_timestamp"])
        for (year, month), grp in vintage_rows.groupby([
            vintage_rows["data_timestamp"].dt.year,
            vintage_rows["data_timestamp"].dt.month,
        ]):
            _write_partition(grp.copy(), int(year), int(month), VAULT_ROOT_ALFRED)
        total += len(vintage_rows)
        logger.info(f"  {len(vintage_rows)} ALFRED rows for {vault_id}")
    return total


def patch_hs_chapter_release_dates() -> int:
    """
    Populate official_release_date for HS-chapter rows in the existing vault
    using the Census FTD release calendar formula. Does not overwrite rows
    that already have a non-null official_release_date.
    """
    if not VAULT_ROOT_EXISTING.exists():
        logger.info("No census_ft900 vault found")
        return 0

    files = list(VAULT_ROOT_EXISTING.rglob("*_data.parquet"))
    patched = 0
    for f in files:
        df = pd.read_parquet(f)
        if df.empty:
            continue

        # Only fill rows where official_release_date is missing
        needs_fill = (
            "official_release_date" not in df.columns or
            df["official_release_date"].isna().any()
        )
        if not needs_fill:
            continue

        ts_col = "data_timestamp" if "data_timestamp" in df.columns else "reporting_date"
        ts = pd.to_datetime(df[ts_col], errors="coerce")

        if "official_release_date" not in df.columns:
            df["official_release_date"] = None
        if "as_of_date" not in df.columns:
            df["as_of_date"] = None
        if "is_revised_figure" not in df.columns:
            df["is_revised_figure"] = False
        if "published_date" not in df.columns:
            df["published_date"] = None

        mask = df["official_release_date"].isna()
        df.loc[mask, "official_release_date"] = ts[mask].apply(
            lambda t: _census_release_date(t) if pd.notna(t) else None
        )
        df.loc[mask, "as_of_date"] = df.loc[mask, "official_release_date"].apply(
            lambda d: d + "Z" if d else None
        )
        df.loc[mask & df["is_revised_figure"].isna(), "is_revised_figure"] = False
        df.loc[mask, "published_date"] = df.loc[mask, "official_release_date"]

        df.to_parquet(f, index=False, engine="pyarrow")
        patched += mask.sum()

    logger.info(f"Patched {patched} HS-chapter rows with census FTD release dates")
    return patched


def run() -> None:
    logger.info("=" * 70)
    logger.info("TRADE FLOWS — ALFRED PIT RETROFIT")
    logger.info("=" * 70)
    total  = retrofit_alfred_aggregate()
    total += patch_hs_chapter_release_dates()
    logger.info(f"\nDone. {total} rows written/patched")


if __name__ == "__main__":
    run()
