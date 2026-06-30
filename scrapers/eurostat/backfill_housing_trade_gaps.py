"""
Housing and Trade Flows Gap Backfill — EU27

Detects missing observation periods in the existing eurostat_sdmx vault data
for Housing_Supply_and_Shelter_Inflation and trade_flows, then re-fetches and
writes only the missing months/quarters.

Algorithm:
  1. Load all existing reporting_dates from vault for each product × country
  2. Derive expected periods (quarterly for trade, quarterly for housing HPI/permits)
  3. Identify gaps = expected - actual
  4. Fetch the missing periods from Eurostat SDMX API
  5. Write only the gap partitions (write_partition deduplicates on vintage_id)

After this runs, each country should have continuous quarterly coverage with
no missing quarter-start months for housing and trade datasets.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Optional

import pandas as pd

_SCRAPER_ROOT = get_vault_root(str(Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"))
sys.path.insert(0, str(_SCRAPER_ROOT))
from scrapers.utilities.vault_io import get_vault_root

from scrapers.eurostat.country_map import ALL_GEO2, ALL_ISO3, GEO2_TO_ISO3, ISO3_TO_GEO2, ISO3_TO_NAME
from scrapers.eurostat.eurostat_client import fetch_dataset, period_to_date
from scrapers.eurostat.revision_tracker import write_partition, build_vintage_id, _estimate_release_date
from scrapers.eurostat.series_map import HOUSING_CONFIGS, TRADE_CONFIGS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

_VAULT_BASE = get_vault_root(str(Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"))
SOURCE      = "eurostat_sdmx"


# ---------------------------------------------------------------------------
# Gap detection
# ---------------------------------------------------------------------------

def _load_reporting_dates(product: str, iso3: str, filenames: list[str]) -> set[str]:
    """Return set of reporting_date values (YYYY-MM-DD strings) for one country."""
    base = _VAULT_BASE / f"product={product}" / f"country={iso3}" / f"source={SOURCE}"
    if not base.exists():
        return set()
    dates: set[str] = set()
    for f in base.rglob("*.parquet"):
        if f.name not in filenames:
            continue
        try:
            df = pd.read_parquet(f, columns=["reporting_date"])
            dates.update(df["reporting_date"].dropna().astype(str).tolist())
        except Exception:
            pass
    return dates


def _expected_quarterly_periods(min_date: str, max_date: str) -> list[str]:
    """
    Return list of YYYY-MM-DD strings for Q1/Q2/Q3/Q4 start months
    covering the full range [min_date, max_date] inclusive.
    """
    start = pd.Timestamp(min_date)
    end   = pd.Timestamp(max_date)
    # Snap start to previous quarter boundary
    q_start_month = ((start.month - 1) // 3) * 3 + 1
    start = pd.Timestamp(start.year, q_start_month, 1)

    periods = []
    cur = start
    while cur <= end:
        periods.append(cur.strftime("%Y-%m-%d"))
        # Advance one quarter
        if cur.month == 10:
            cur = pd.Timestamp(cur.year + 1, 1, 1)
        else:
            cur = pd.Timestamp(cur.year, cur.month + 3, 1)
    return periods


def _expected_monthly_periods(min_date: str, max_date: str) -> list[str]:
    """Return list of YYYY-MM-DD strings for each month in [min_date, max_date]."""
    start = pd.Timestamp(min_date)
    end   = pd.Timestamp(max_date)
    start = pd.Timestamp(start.year, start.month, 1)
    periods = []
    cur = start
    while cur <= end:
        periods.append(cur.strftime("%Y-%m-%d"))
        if cur.month == 12:
            cur = pd.Timestamp(cur.year + 1, 1, 1)
        else:
            cur = pd.Timestamp(cur.year, cur.month + 1, 1)
    return periods


def _find_gaps(actual: set[str], expected: list[str]) -> list[str]:
    return sorted(d for d in expected if d not in actual)


# ---------------------------------------------------------------------------
# Housing backfill
# ---------------------------------------------------------------------------

def _backfill_housing_country(iso3: str, gaps: list[str]) -> int:
    """Re-fetch housing data for a specific country and write only gap months."""
    if not gaps:
        return 0

    geo2      = ISO3_TO_GEO2.get(iso3)
    if not geo2:
        return 0

    # Convert gap dates to Eurostat quarterly period strings for API filter
    gap_periods_q = set()
    for d in gaps:
        ts = pd.Timestamp(d)
        q  = (ts.month - 1) // 3 + 1
        gap_periods_q.add(f"{ts.year}-Q{q}")

    gap_min = min(gaps)[:7]   # YYYY-MM for sinceTimePeriod
    gap_max = max(gaps)[:7]

    total = 0
    for cfg in HOUSING_CONFIGS:
        dataflow    = cfg["dataflow"]
        metric_code = cfg["metric_code"]
        metric_name = cfg["macro_metric_name"]
        unit_label  = cfg["unit_of_measure"]
        lag         = cfg["release_lag_days"]
        freq        = cfg["freq"]
        source_sub  = cfg["source_sub_category"]

        # Convert month range to Eurostat period format
        start_p = f"{gap_min[:4]}-Q{(int(gap_min[5:7])-1)//3+1}" if "Q" in gap_min else gap_min
        end_p   = f"{gap_max[:4]}-Q{(int(gap_max[5:7])-1)//3+1}" if "Q" in gap_max else gap_max

        df_raw = fetch_dataset(
            dataset_id=  dataflow,
            filters=      cfg["static_filters"],
            geo_list=     [geo2],
            start_period= start_p,
            end_period=   end_p,
        )
        if df_raw.empty:
            continue

        geo_col = next((c for c in df_raw.columns if c.lower().startswith("geo")), "geo")
        vault_root = (
            _VAULT_BASE
            / f"product=Housing_Supply_and_Shelter_Inflation"
            / f"country={iso3}"
            / f"source={SOURCE}"
        )

        for _, r in df_raw.iterrows():
            geo_val  = str(r.get(geo_col, ""))
            if GEO2_TO_ISO3.get(geo_val) != iso3:
                continue
            period   = str(r.get("time", ""))
            obs_date = period_to_date(period)
            if obs_date is None:
                continue
            # Only write if this date is in the gap list
            if obs_date.strftime("%Y-%m-%d") not in gaps:
                continue

            val = r.get("value")
            if pd.isna(val) if isinstance(val, float) else val is None:
                continue

            cpa_suffix = ""
            if "cpa2_1" in df_raw.columns:
                cpa_val = str(r.get("cpa2_1", "")).replace("CPA_", "").replace("-", "_")
                if cpa_val and cpa_val != "nan":
                    cpa_suffix = f"_{cpa_val}"

            vid_code = f"{metric_code}{cpa_suffix}"
            vid      = build_vintage_id(iso3, vid_code, obs_date, 1)
            sid      = f"{metric_code}{cpa_suffix}_{iso3}"
            rdate    = _estimate_release_date(obs_date, lag)

            row_df = pd.DataFrame([{
                "data_vintage_id":       vid,
                "confidence_tier":       "PRIMARY",
                "sovereign_series_id":   sid,
                "macro_metric_name":     metric_name,
                "reporting_date":        obs_date.strftime("%Y-%m-%d"),
                "official_release_date": rdate,
                "as_of_date":            rdate + "T00:00:00Z",
                "observed_value":        float(val),
                "unit_of_measure":       unit_label,
                "is_revised_figure":     False,
                "data_timestamp":        obs_date.isoformat() + "Z",
                "revision_number":       1,
                "iso_alpha3":            iso3,
                "country_name":          ISO3_TO_NAME.get(iso3, iso3),
                "source":                SOURCE,
                "source_agency":         "EUROSTAT",
                "source_sub_category":   source_sub,
                "sdmx_frequency":        freq,
                "published_date":        rdate,
                "data_quality_certified": True,
                "is_forecast":           False,
            }])

            write_partition(
                row_df, vault_root,
                obs_date.year, obs_date.month,
                "housing_data.parquet",
            )
            total += 1

    return total


# ---------------------------------------------------------------------------
# Trade backfill
# ---------------------------------------------------------------------------

def _backfill_trade_country(iso3: str, gaps: list[str]) -> int:
    if not gaps:
        return 0

    geo2 = ISO3_TO_GEO2.get(iso3)
    if not geo2:
        return 0

    gap_min = min(gaps)[:7]
    gap_max = max(gaps)[:7]

    # Convert to quarterly period strings
    def to_q(yyyy_mm: str) -> str:
        y, m = yyyy_mm[:4], int(yyyy_mm[5:7])
        return f"{y}-Q{(m-1)//3+1}"

    start_p = to_q(gap_min)
    end_p   = to_q(gap_max)

    total = 0
    for cfg in TRADE_CONFIGS:
        dataflow    = cfg["dataflow"]
        metric_code = cfg["metric_code"]
        metric_name = cfg["macro_metric_name"]
        unit_label  = cfg["unit_of_measure"]
        lag         = cfg["release_lag_days"]
        na_item     = cfg.get("na_item", "")
        trade_flow  = "EXPORTS" if na_item == "P6" else "IMPORTS"

        df_raw = fetch_dataset(
            dataset_id=  dataflow,
            filters=      cfg["static_filters"],
            geo_list=     [geo2],
            start_period= start_p,
            end_period=   end_p,
        )
        if df_raw.empty:
            continue

        geo_col = next((c for c in df_raw.columns if c.lower().startswith("geo")), "geo")
        vault_root = (
            _VAULT_BASE
            / f"product=trade_flows"
            / f"country={iso3}"
            / f"source={SOURCE}"
        )

        for _, r in df_raw.iterrows():
            if GEO2_TO_ISO3.get(str(r.get(geo_col, ""))) != iso3:
                continue
            period   = str(r.get("time", ""))
            obs_date = period_to_date(period)
            if obs_date is None:
                continue
            if obs_date.strftime("%Y-%m-%d") not in gaps:
                continue

            val = r.get("value")
            if pd.isna(val) if isinstance(val, float) else val is None:
                continue

            vid   = build_vintage_id(iso3, metric_code, obs_date, 1)
            sid   = f"{metric_code}_{iso3}"
            rdate = _estimate_release_date(obs_date, lag)

            row_df = pd.DataFrame([{
                "data_vintage_id":       vid,
                "confidence_tier":       "PRIMARY",
                "sovereign_series_id":   sid,
                "macro_metric_name":     metric_name,
                "reporting_date":        obs_date.strftime("%Y-%m-%d"),
                "official_release_date": rdate,
                "as_of_date":            rdate + "T00:00:00Z",
                "observed_value":        float(val),
                "unit_of_measure":       unit_label,
                "is_revised_figure":     False,
                "data_timestamp":        obs_date.isoformat() + "Z",
                "revision_number":       1,
                "commodity_code":        na_item,
                "commodity_name":        "Goods and Services",
                "partner_country_code":  "WLD",
                "partner_country_name":  "World",
                "trade_flow":            trade_flow,
                "currency":              "EUR",
                "iso_alpha3":            iso3,
                "country_name":          ISO3_TO_NAME.get(iso3, iso3),
                "source":                SOURCE,
                "source_agency":         "EUROSTAT",
                "source_sub_category":   "NATIONAL_ACCOUNTS_TRADE",
                "sdmx_frequency":        "Q",
                "published_date":        rdate,
                "data_quality_certified": True,
                "is_forecast":           False,
            }])

            write_partition(
                row_df, vault_root,
                obs_date.year, obs_date.month,
                "trade_data.parquet",
            )
            total += 1

    return total


# ---------------------------------------------------------------------------
# Main runner
# ---------------------------------------------------------------------------

def run() -> dict[str, int]:
    log.info("=" * 70)
    log.info("EU27 Housing + Trade — Gap Detection and Backfill")
    log.info("=" * 70)

    summary = {"housing_gaps_fixed": 0, "trade_gaps_fixed": 0,
               "housing_countries_with_gaps": 0, "trade_countries_with_gaps": 0}

    # ── HOUSING ─────────────────────────────────────────────────────────────
    log.info("\n-- Housing gap scan --")
    h_filenames = ["housing_data.parquet"]

    for iso3 in ALL_ISO3:
        actual = _load_reporting_dates("Housing_Supply_and_Shelter_Inflation", iso3, h_filenames)
        if not actual:
            continue

        sorted_actual = sorted(actual)
        expected = _expected_quarterly_periods(sorted_actual[0], sorted_actual[-1])
        gaps     = _find_gaps(actual, expected)

        if gaps:
            log.info(f"  {iso3}: {len(gaps)} gap quarters -> {gaps[:4]}{'...' if len(gaps)>4 else ''}")
            n = _backfill_housing_country(iso3, gaps)
            summary["housing_gaps_fixed"] += n
            summary["housing_countries_with_gaps"] += 1
        else:
            log.info(f"  {iso3}: no gaps")

    # ── TRADE ────────────────────────────────────────────────────────────────
    log.info("\n-- Trade gap scan --")
    t_filenames = ["trade_data.parquet"]

    for iso3 in ALL_ISO3:
        actual = _load_reporting_dates("trade_flows", iso3, t_filenames)
        if not actual:
            continue

        sorted_actual = sorted(actual)
        expected = _expected_quarterly_periods(sorted_actual[0], sorted_actual[-1])
        gaps     = _find_gaps(actual, expected)

        if gaps:
            log.info(f"  {iso3}: {len(gaps)} gap quarters -> {gaps[:4]}{'...' if len(gaps)>4 else ''}")
            n = _backfill_trade_country(iso3, gaps)
            summary["trade_gaps_fixed"] += n
            summary["trade_countries_with_gaps"] += 1
        else:
            log.info(f"  {iso3}: no gaps")

    log.info("\n" + "=" * 70)
    log.info(f"Gap backfill complete:")
    log.info(f"  Housing: {summary['housing_gaps_fixed']} rows added across "
             f"{summary['housing_countries_with_gaps']} countries")
    log.info(f"  Trade:   {summary['trade_gaps_fixed']} rows added across "
             f"{summary['trade_countries_with_gaps']} countries")
    log.info("=" * 70)
    return summary


if __name__ == "__main__":
    run()
