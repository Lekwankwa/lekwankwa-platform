"""
ALFRED PIT Retrofit — Food Pricing

ALFRED coverage for BLS APU* average-price series starts 2019-07-11.

Strategy:
  1. 2019-07 onward   → fetch all vintages from ALFRED (genuine PIT)
  2. Pre-2019         → BLS release calendar fallback:
                        official_release_date = 2nd Tuesday of month
                        following the reference month
                        confidence_tier = PRIMARY (actual BLS release schedule)
                        is_revised_figure = False (BLS avg prices rarely revised)

Writes vintage rows to:
  lekwankwa-historical-vault/product=food_micropricing/
    country=USA/source=alfred_vintage/year=YYYY/month=MM/food_pricing_data.parquet

Author: Lekwankwa Corporation
Date: 2026-06-16
"""

import logging
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scrapers.alfred_pit.alfred_client import fetch_all_vintages, build_vintage_rows
from scrapers.alfred_pit.series_map import FOOD_SERIES, FOOD_ALFRED_START

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler("alfred_retrofit_food.log")],
)
logger = logging.getLogger(__name__)

VAULT_ROOT = Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault/product=food_micropricing/country=USA/source=alfred_vintage"
FILE_NAME  = "food_pricing_data.parquet"

SCHEMA_CONSTANTS = {
    "iso_alpha3":              "USA",
    "country_code":            "USA",
    "market_tier":             "Developed",
    "source":                  "alfred_vintage",
    "source_agency":           "BLS",
    "source_sub_category":     "CPI",
    "portal_url":              "https://alfred.stlouisfed.org/",
    "extraction_method":       "ALFRED_API",
    "currency":                "USD",
    "fx_rate_applied":         1.0,
    "price_usd_equivalent":    None,   # will be set per row
    "data_quality_certified":  True,
    "global_coicop_code":      "01.1",
    "category":                "Food",
    "observation_period":      None,   # will be set per row
}


def _bls_release_date(ref_date: pd.Timestamp) -> str:
    """Return the 2nd Tuesday of the month following ref_date."""
    first_of_next = (ref_date + pd.offsets.MonthBegin(1))
    # Find first Tuesday
    dow = first_of_next.dayofweek   # 0=Mon, 1=Tue
    days_to_tuesday = (1 - dow) % 7
    first_tuesday = first_of_next + timedelta(days=days_to_tuesday)
    second_tuesday = first_tuesday + timedelta(days=7)
    return second_tuesday.strftime("%Y-%m-%d")


def _write_partition(df: pd.DataFrame, year: int, month: int) -> None:
    part = VAULT_ROOT / f"year={year}" / f"month={month:02d}"
    part.mkdir(parents=True, exist_ok=True)
    out = part / FILE_NAME
    if out.exists():
        existing = pd.read_parquet(out)
        combined = pd.concat([existing, df], ignore_index=True)
        combined = combined.drop_duplicates(subset=["data_vintage_id"], keep="first")
    else:
        combined = df.drop_duplicates(subset=["data_vintage_id"], keep="first")
    combined.to_parquet(out, index=False, engine="pyarrow")


def retrofit_series(
    vault_id: str,
    alfred_id: str,
    metric: str,
    unit: str,
) -> int:
    logger.info(f"Food pricing: {alfred_id}")
    alfred_cutoff = pd.Timestamp(FOOD_ALFRED_START, tz="UTC")

    # ── ALFRED vintages (2019-07 onward) ─────────────────────────────────────
    fred_df = fetch_all_vintages(alfred_id)
    alfred_rows = pd.DataFrame()
    if not fred_df.empty:
        dates = pd.to_datetime(fred_df["date"], utc=True)
        recent = fred_df[dates >= alfred_cutoff]
        if not recent.empty:
            alfred_rows = build_vintage_rows(
                series_id=     alfred_id,
                fred_df=       recent,
                source_prefix= "BLS",
                schema_fields= {
                    **SCHEMA_CONSTANTS,
                    "sovereign_series_id": vault_id,
                    "macro_metric_name":   metric,
                    "unit_of_measure":     unit,
                    "standard_name":       metric,
                    "local_name":          metric,
                    "source_system":       "ALFRED",
                },
                vintage_id_fn=lambda sid, date, n: f"BLS-{vault_id}-{date.strftime('%Y-%m')}-v{n}",
            )

    # ── Pre-ALFRED period: read existing vault rows and add official_release_date ─
    existing_vault = Path(f"lekwankwa-historical-vault/product=food_micropricing/country=USA/source=bls")
    pre_alfred_rows = []
    if existing_vault.exists():
        for f in sorted(existing_vault.rglob("*_data.parquet")):
            df = pd.read_parquet(f)
            if "sovereign_series_id" not in df.columns:
                continue
            series_rows = df[df["sovereign_series_id"] == vault_id].copy()
            if series_rows.empty:
                continue
            ts = pd.to_datetime(series_rows.get("data_timestamp", series_rows.get("reporting_date")), errors="coerce", utc=True)
            pre = series_rows[ts < alfred_cutoff].copy()
            if pre.empty:
                continue

            # Populate official_release_date from BLS release calendar
            for idx, row in pre.iterrows():
                try:
                    ref = pd.Timestamp(row.get("data_timestamp") or row.get("reporting_date"))
                    rel_date = _bls_release_date(ref)
                    pre.at[idx, "official_release_date"] = rel_date
                    if pd.isna(pre.at[idx, "as_of_date"] if "as_of_date" in pre.columns else None):
                        pre.at[idx, "as_of_date"] = rel_date + "T00:00:00Z"
                    if "is_revised_figure" not in pre.columns or pd.isna(pre.at[idx, "is_revised_figure"]):
                        pre.at[idx, "is_revised_figure"] = False
                    pre.at[idx, "confidence_tier"] = "PRIMARY"
                    pre.at[idx, "source_system"] = "BLS_CALENDAR_FALLBACK"
                except Exception:
                    continue
            pre_alfred_rows.append(pre)

    # ── Combine and write ─────────────────────────────────────────────────────
    all_parts = [alfred_rows] + pre_alfred_rows
    all_parts = [p for p in all_parts if not p.empty]
    if not all_parts:
        return 0

    combined = pd.concat(all_parts, ignore_index=True)
    combined["data_timestamp"] = pd.to_datetime(combined["data_timestamp"], errors="coerce", utc=True)
    combined = combined.dropna(subset=["data_timestamp"])

    for (year, month), grp in combined.groupby([
        combined["data_timestamp"].dt.year,
        combined["data_timestamp"].dt.month,
    ]):
        if pd.isna(year) or pd.isna(month):
            continue
        _write_partition(grp.copy(), int(year), int(month))

    logger.info(f"  {len(alfred_rows)} ALFRED rows + {sum(len(r) for r in pre_alfred_rows)} calendar rows for {vault_id}")
    return len(combined)


def run() -> None:
    logger.info("=" * 70)
    logger.info("FOOD PRICING — ALFRED PIT RETROFIT")
    logger.info("=" * 70)
    total = 0
    for vault_id, (alfred_id, metric, unit) in FOOD_SERIES.items():
        total += retrofit_series(vault_id, alfred_id, metric, unit)
    logger.info(f"\nDone. {total} rows written/enriched")


if __name__ == "__main__":
    run()
