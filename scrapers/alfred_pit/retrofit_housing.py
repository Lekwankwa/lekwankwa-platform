"""
ALFRED PIT Retrofit — Housing Supply & Shelter Inflation

Fetches complete vintage/revision history from ALFRED for:
  PERMIT, PERMIT1, PERMIT5     (Census Building Permits — from 1999)
  CUUR0000SEHA, CUUR0000SEHB  (BLS CPI Rent NSA — from 2011)
  CUUR0000SAH1                 (BLS CPI Housing NSA — from 2011)
  CUSR0000SEHA, CUSR0000SAH1  (SA variants — from 2011)

Writes vintage rows to:
  lekwankwa-historical-vault/product=Housing_Supply_and_Shelter_Inflation/
    country=USA/source=alfred_vintage/year=YYYY/month=MM/{permits|shelter}_data.parquet

Author: Lekwankwa Corporation
Date: 2026-06-16
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scrapers.utilities.vault_io import get_vault_root
from scrapers.alfred_pit.alfred_client import fetch_all_vintages, build_vintage_rows
from scrapers.alfred_pit.series_map import HOUSING_SERIES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler("alfred_retrofit_housing.log")],
)
logger = logging.getLogger(__name__)

VAULT_ROOT = get_vault_root(str(Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault/product=Housing_Supply_and_Shelter_Inflation/country=USA/source=alfred_vintage"))
PERMIT_SERIES  = {"PERMIT", "PERMIT1", "PERMIT5"}
SHELTER_SERIES = {"CUUR0000SEHA", "CUUR0000SEHB", "CUUR0000SAH1",
                   "CUSR0000SEHA", "CUSR0000SEHB", "CUSR0000SAH1"}

PERMITS_CONSTANTS = {
    "iso_alpha3":          "USA",
    "country_name":        "United States",
    "country_code":        "US",
    "market_tier":         "Developed",
    "source":              "alfred_vintage",
    "source_agency":       "CENSUS",
    "source_sub_category": "HOUSING",
    "portal_url":          "https://alfred.stlouisfed.org/",
    "extraction_method":   "api",
    "seasonal_adjustment": "SAAR",
    "data_quality_certified": True,
}

SHELTER_CONSTANTS = {
    "iso_alpha3":          "USA",
    "country_name":        "United States",
    "country_code":        "US",
    "market_tier":         "Developed",
    "source":              "alfred_vintage",
    "source_agency":       "BLS",
    "source_sub_category": "CPI_URBAN",
    "portal_url":          "https://alfred.stlouisfed.org/",
    "extraction_method":   "api",
    "data_quality_certified": True,
}


def _write_partition(df: pd.DataFrame, year: int, month: int, fname: str) -> None:
    part = VAULT_ROOT / f"year={year}" / f"month={month:02d}"
    part.mkdir(parents=True, exist_ok=True)
    out_path = part / fname
    if out_path.exists():
        existing = pd.read_parquet(out_path)
        combined = pd.concat([existing, df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["data_vintage_id"], keep="first")
    else:
        combined = df.drop_duplicates(subset=["data_vintage_id"], keep="first")
    combined.to_parquet(out_path, index=False, engine="pyarrow")
    logger.info(f"  Written {len(combined)} rows -> {out_path}")


def retrofit_series(vault_id: str, alfred_id: str, metric: str, unit: str) -> int:
    is_permit = vault_id in PERMIT_SERIES
    constants = PERMITS_CONSTANTS if is_permit else SHELTER_CONSTANTS
    fname = "building_permits_data.parquet" if is_permit else "shelter_data.parquet"
    prefix = "CENSUS" if is_permit else "BLS"

    logger.info(f"Fetching ALFRED: {alfred_id}")
    fred_df = fetch_all_vintages(alfred_id)
    if fred_df.empty:
        logger.warning(f"  No data for {alfred_id}")
        return 0

    vintage_rows = build_vintage_rows(
        series_id=     alfred_id,
        fred_df=       fred_df,
        source_prefix= prefix,
        schema_fields= {
            **constants,
            "sovereign_series_id": vault_id,
            "macro_metric_name":   metric,
            "unit_of_measure":     unit,
        },
        vintage_id_fn=lambda sid, date, n: f"{prefix}-{vault_id}-{date.strftime('%Y-%m')}-v{n}",
    )
    if vintage_rows.empty:
        return 0

    vintage_rows["data_timestamp"] = pd.to_datetime(vintage_rows["data_timestamp"])
    for (year, month), grp in vintage_rows.groupby([
        vintage_rows["data_timestamp"].dt.year,
        vintage_rows["data_timestamp"].dt.month,
    ]):
        _write_partition(grp.copy(), int(year), int(month), fname)

    logger.info(f"  {len(vintage_rows)} vintage rows for {vault_id}")
    return len(vintage_rows)


def run() -> None:
    logger.info("=" * 70)
    logger.info("HOUSING — ALFRED PIT RETROFIT")
    logger.info("=" * 70)
    total = 0
    for vault_id, (alfred_id, metric, unit) in HOUSING_SERIES.items():
        total += retrofit_series(vault_id, alfred_id, metric, unit)
    logger.info(f"\nDone. {total} vintage rows written")


if __name__ == "__main__":
    run()
