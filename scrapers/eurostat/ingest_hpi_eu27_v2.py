"""
EU27 House Price Index Ingestor — Gold Standard conformant

Pulls prc_hpi_q (quarterly House Price Index) for all 27 EU member states
using purchase=PURCHASE (purchase transactions only, excludes inheritance/gift)
and writes rows conforming to the housing gold standard schema.

Gold standard field mapping:
  macro_metric_name  -> "HOUSE_PRICE_INDEX_PURCHASE_ONLY"
  unit_of_measure    -> "INDEX_2015_100"
  sovereign_series_id -> "EUROSTAT_HPI_PURCH_{ISO3}"
  data_vintage_id    -> "EUROSTAT-HPI-{ISO3}-{YYYY-MM}-v1"

Falls back to purchase=TOTAL (all transaction types) if purchase=PURCHASE
returns no data for a given country, recording macro_metric_name as
"HOUSE_PRICE_INDEX_ALL_DWELLINGS" for transparency.

Revision note: Eurostat SDMX API does not publish a revision archive.
All rows are version-1 initial ingestions. is_revised_figure=False.

Output: product=Housing_Supply_and_Shelter_Inflation/country={ISO3}/
            source=eurostat_sdmx/year=YYYY/month=MM/hpi_purchase_data.parquet
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

_SCRAPER_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_SCRAPER_ROOT))

from scrapers.eurostat.country_map import ALL_GEO2, ALL_ISO3, GEO2_TO_ISO3, ISO3_TO_GEO2, ISO3_TO_NAME
from scrapers.eurostat.eurostat_client import fetch_dataset, period_to_date
from scrapers.eurostat.revision_tracker import write_partition, _estimate_release_date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

_VAULT_BASE   = Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"
VAULT_PRODUCT = "Housing_Supply_and_Shelter_Inflation"
SOURCE        = "eurostat_sdmx"
FILENAME      = "hpi_purchase_data.parquet"

RELEASE_LAG_DAYS = 90   # HPI published ~90 days after reference quarter
START_PERIOD     = "2005-Q1"

# Fetch configs — try purchase-only first, fall back to total
FETCH_CONFIGS = [
    {
        "filters":        {"freq": "Q", "purchase": "PURCHASE", "unit": "I15_Q"},
        "metric_name":    "HOUSE_PRICE_INDEX_PURCHASE_ONLY",
        "sid_prefix":     "EUROSTAT_HPI_PURCH",
        "vid_code":       "HPI_PURCH",
    },
    {
        "filters":        {"freq": "Q", "purchase": "TOTAL", "unit": "I15_Q"},
        "metric_name":    "HOUSE_PRICE_INDEX_ALL_DWELLINGS",
        "sid_prefix":     "EUROSTAT_HPI_TOTAL",
        "vid_code":       "HPI_TOTAL",
    },
]


def _build_vintage_id(iso3: str, vid_code: str,
                      obs_date: pd.Timestamp, version: int = 1) -> str:
    period = obs_date.strftime("%Y-%m")
    return f"EUROSTAT-{vid_code}-{iso3}-{period}-v{version}"


def _build_rows(df_raw: pd.DataFrame, metric_name: str,
                sid_prefix: str, vid_code: str) -> pd.DataFrame:
    if df_raw.empty:
        return pd.DataFrame()

    geo_col = next((c for c in df_raw.columns if c.lower().startswith("geo")), "geo")
    rows = []

    for _, r in df_raw.iterrows():
        geo  = str(r.get(geo_col, ""))
        iso3 = GEO2_TO_ISO3.get(geo)
        if not iso3:
            continue

        period   = str(r.get("time", ""))
        obs_date = period_to_date(period)
        if obs_date is None:
            continue

        val = r.get("value")
        if pd.isna(val) if isinstance(val, float) else val is None:
            continue

        rdate = _estimate_release_date(obs_date, RELEASE_LAG_DAYS)
        vid   = _build_vintage_id(iso3, vid_code, obs_date, 1)
        sid   = f"{sid_prefix}_{iso3}"

        rows.append({
            # housing gold standard fields
            "data_vintage_id":       vid,
            "confidence_tier":       "PRIMARY",
            "sovereign_series_id":   sid,
            "macro_metric_name":     metric_name,
            "reporting_date":        obs_date.strftime("%Y-%m-%d"),
            "official_release_date": rdate,
            "as_of_date":            rdate + "T00:00:00Z",
            "observed_value":        float(val),
            "unit_of_measure":       "INDEX_2015_100",
            "is_revised_figure":     False,

            # PIT mandatory
            "data_timestamp":        obs_date.isoformat() + "Z",
            "revision_number":       1,

            # Provenance
            "iso_alpha3":            iso3,
            "country_name":          ISO3_TO_NAME.get(iso3, iso3),
            "source":                SOURCE,
            "source_agency":         "EUROSTAT",
            "source_sub_category":   "HOUSING",
            "sdmx_dataflow":         "prc_hpi_q",
            "observation_period":    obs_date.strftime("%Y-%m"),
            "sdmx_frequency":        "Q",
            "published_date":        rdate,
            "data_quality_certified": True,
            "is_forecast":           False,
            **({"data_status": r["status"]} if r.get("status") else {}),
        })

    return pd.DataFrame(rows)


def run() -> int:
    log.info("=" * 70)
    log.info("EU27 HPI (prc_hpi_q) -> housing gold standard schema")
    log.info(f"Countries: {len(ALL_GEO2)} | Start: {START_PERIOD}")
    log.info("=" * 70)

    # Try purchase-only first; fall back to total if too few rows
    df_raw_purch = fetch_dataset(
        dataset_id=  "prc_hpi_q",
        filters=      FETCH_CONFIGS[0]["filters"],
        geo_list=     ALL_GEO2,
        start_period= START_PERIOD,
    )
    df_raw_total = fetch_dataset(
        dataset_id=  "prc_hpi_q",
        filters=      FETCH_CONFIGS[1]["filters"],
        geo_list=     ALL_GEO2,
        start_period= START_PERIOD,
    )

    # Determine which countries have purchase-only data
    geo_col_p = next((c for c in df_raw_purch.columns if c.lower().startswith("geo")), "geo") \
        if not df_raw_purch.empty else "geo"
    geo_col_t = next((c for c in df_raw_total.columns if c.lower().startswith("geo")), "geo") \
        if not df_raw_total.empty else "geo"

    geos_with_purch = set(df_raw_purch[geo_col_p].dropna().unique()) \
        if not df_raw_purch.empty else set()

    log.info(f"  purchase=PURCHASE rows: {len(df_raw_purch):,} | geos: {len(geos_with_purch)}")
    log.info(f"  purchase=TOTAL rows:    {len(df_raw_total):,}")

    all_rows: list[pd.DataFrame] = []

    # Rows for countries with purchase-only data
    if not df_raw_purch.empty:
        df_p = _build_rows(
            df_raw_purch,
            metric_name= FETCH_CONFIGS[0]["metric_name"],
            sid_prefix=  FETCH_CONFIGS[0]["sid_prefix"],
            vid_code=    FETCH_CONFIGS[0]["vid_code"],
        )
        if not df_p.empty:
            all_rows.append(df_p)
            log.info(f"  HOUSE_PRICE_INDEX_PURCHASE_ONLY rows mapped: {len(df_p):,}")

    # Rows for countries that only have total-dwellings data (fallback)
    iso3_with_purch = {GEO2_TO_ISO3.get(g) for g in geos_with_purch if GEO2_TO_ISO3.get(g)}
    iso3_fallback   = [iso3 for iso3 in ALL_ISO3 if iso3 not in iso3_with_purch]

    if iso3_fallback and not df_raw_total.empty:
        fallback_geos = {ISO3_TO_GEO2[iso3] for iso3 in iso3_fallback if iso3 in ISO3_TO_GEO2}
        df_total_sub  = df_raw_total[df_raw_total[geo_col_t].isin(fallback_geos)]
        df_t = _build_rows(
            df_total_sub,
            metric_name= FETCH_CONFIGS[1]["metric_name"],
            sid_prefix=  FETCH_CONFIGS[1]["sid_prefix"],
            vid_code=    FETCH_CONFIGS[1]["vid_code"],
        )
        if not df_t.empty:
            all_rows.append(df_t)
            log.info(f"  HOUSE_PRICE_INDEX_ALL_DWELLINGS (fallback) rows mapped: {len(df_t):,}")
            log.info(f"  Fallback countries ({len(iso3_fallback)}): {sorted(iso3_fallback)}")

    if not all_rows:
        log.error("HPI mapping produced 0 rows — aborting")
        return 0

    df_vault = pd.concat(all_rows, ignore_index=True)
    log.info(f"Total vault rows: {len(df_vault):,}")

    total_written = 0
    df_vault["_obs_ts"] = pd.to_datetime(
        df_vault["data_timestamp"], errors="coerce", utc=True
    )

    for iso3, grp_iso in df_vault.groupby("iso_alpha3"):
        vault_root = (
            _VAULT_BASE
            / f"product={VAULT_PRODUCT}"
            / f"country={iso3}"
            / f"source={SOURCE}"
        )
        for (year, month), grp_period in grp_iso.groupby([
            grp_iso["_obs_ts"].dt.year,
            grp_iso["_obs_ts"].dt.month,
        ]):
            out = grp_period.drop(columns=["_obs_ts"])
            write_partition(out, vault_root, int(year), int(month), FILENAME)
            total_written += len(out)

    log.info(f"EU27 HPI ingestion complete: {total_written:,} rows written")
    return total_written


if __name__ == "__main__":
    run()
