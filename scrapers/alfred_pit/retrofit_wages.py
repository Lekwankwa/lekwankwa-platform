"""
ALFRED PIT Retrofit — Wages & Employment

Fetches complete vintage/revision history from ALFRED for:
  PAYEMS   (Total Nonfarm Payrolls)
  USPRIV   (Total Private Payrolls)
  UNRATE   (Unemployment Rate)
  CES0500000003 (Avg Hourly Earnings — Private)
  USCONS, MANEMP, USTRADE, USINFO, USFIRE, USEHS, USLAH, USGOVT

Writes vintage rows to:
  lekwankwa-historical-vault/product=wages_and_employment/
    country=USA/source=alfred_vintage/year=YYYY/month=MM/wages_data.parquet

Never overwrites existing records. Deduplicates on data_vintage_id before write.

Author: Lekwankwa Corporation
Date: 2026-06-16
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scrapers.alfred_pit.alfred_client import fetch_all_vintages, build_vintage_rows
from scrapers.alfred_pit.series_map import WAGES_SERIES

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler("alfred_retrofit_wages.log")],
)
logger = logging.getLogger(__name__)

VAULT_ROOT = Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault/product=wages_and_employment/country=USA/source=alfred_vintage"
FILE_NAME  = "wages_data.parquet"

SCHEMA_CONSTANTS = {
    "iso_alpha3":         "USA",
    "country_name":       "United States",
    "country_code":       "USA",
    "market_tier":        "Developed",
    "source":             "alfred_vintage",
    "source_agency":      "BLS",
    "source_sub_category": "CES_CPS",
    "portal_url":         "https://alfred.stlouisfed.org/",
    "extraction_method":  "ALFRED_API",
    "data_quality_certified": True,
}


def _write_partition(df: pd.DataFrame, year: int, month: int) -> None:
    part = VAULT_ROOT / f"year={year}" / f"month={month:02d}"
    part.mkdir(parents=True, exist_ok=True)
    out_path = part / FILE_NAME

    if out_path.exists():
        existing = pd.read_parquet(out_path)
        combined = pd.concat([existing, df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["data_vintage_id"], keep="first")
    else:
        combined = df.drop_duplicates(subset=["data_vintage_id"], keep="first")

    combined.to_parquet(out_path, index=False, engine="pyarrow")
    logger.info(f"  Written {len(combined)} rows -> {out_path}")


def retrofit_series(vault_series_id: str, alfred_id: str, metric_name: str, unit: str) -> int:
    logger.info(f"Fetching ALFRED vintages: {alfred_id} (vault: {vault_series_id})")
    fred_df = fetch_all_vintages(alfred_id)
    if fred_df.empty:
        logger.warning(f"  No ALFRED data for {alfred_id}, skipping")
        return 0

    vintage_rows = build_vintage_rows(
        series_id=     alfred_id,
        fred_df=       fred_df,
        source_prefix= "BLS",
        schema_fields= {
            **SCHEMA_CONSTANTS,
            "sovereign_series_id": vault_series_id,
            "macro_metric_name":   metric_name,
            "unit_of_measure":     unit,
            "seasonal_adjustment": "SA",
            "industry_code":       vault_series_id,
            "industry_name":       metric_name,
        },
        # Use vault series ID in the vintage_id so it matches existing rows
        vintage_id_fn=lambda sid, date, n: f"BLS-{vault_series_id}-{date.strftime('%Y-%m')}-v{n}",
    )
    if vintage_rows.empty:
        return 0

    vintage_rows["data_timestamp"] = pd.to_datetime(vintage_rows["data_timestamp"])
    total_written = 0
    for (year, month), grp in vintage_rows.groupby([
        vintage_rows["data_timestamp"].dt.year,
        vintage_rows["data_timestamp"].dt.month,
    ]):
        _write_partition(grp.copy(), int(year), int(month))
        total_written += len(grp)

    logger.info(f"  {len(fred_df['date'].unique())} data dates × vintages = {len(vintage_rows)} rows written for {vault_series_id}")
    return len(vintage_rows)


def run() -> None:
    logger.info("=" * 70)
    logger.info("WAGES & EMPLOYMENT — ALFRED PIT RETROFIT")
    logger.info("=" * 70)
    start = datetime.now(timezone.utc)
    total = 0
    for vault_id, (alfred_id, metric, unit) in WAGES_SERIES.items():
        total += retrofit_series(vault_id, alfred_id, metric, unit)

    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    logger.info(f"\nDone. {total} vintage rows written in {elapsed:.1f}s")


if __name__ == "__main__":
    run()
