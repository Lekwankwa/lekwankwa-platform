"""
EU27 HICP Rent Ingestor — Housing_Supply_and_Shelter_Inflation vault

Pulls prc_hicp_minr COICOP code CP041 (Actual Rentals for Housing) for all
27 EU member states. CP041 is the HICP sub-index for rents paid by tenants
and is the Eurostat equivalent of BLS CPI Rent-of-Primary-Residence (USA).

Gold standard field mapping (housing_shelter_inflation.json):
  macro_metric_name   -> "CPI_RENT_OF_PRIMARY_RESIDENCE"
  unit_of_measure     -> "INDEX_2015_100"
  sovereign_series_id -> "EUROSTAT_HICP_CP041_{ISO3}"
  data_vintage_id     -> "EUROSTAT-{ISO3}-HICP_RENT_CP041-{YYYY-MM}-v1"
  source_sub_category -> "HICP_CPI"
  source_agency       -> "EUROSTAT"
  confidence_tier     -> "PRIMARY"

Dataflow:  prc_hicp_minr
Filters:   unit=I15 (2015=100 monthly index), freq=M, coicop=CP041
Start:     2000-01
Release lag: 30 days (HICP flash published ~mid-month following reference)

Output: product=Housing_Supply_and_Shelter_Inflation/country={ISO3}/
            source=eurostat_sdmx/year=YYYY/month=MM/housing_hicp_rent_data.parquet

Idempotent: write_partition deduplicates on data_vintage_id.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

_SCRAPER_ROOT = get_vault_root(str(Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"))
sys.path.insert(0, str(_SCRAPER_ROOT))
from scrapers.utilities.vault_io import get_vault_root

from scrapers.eurostat.country_map import ALL_GEO2, GEO2_TO_ISO3, ISO3_TO_NAME
from scrapers.eurostat.eurostat_client import fetch_dataset, period_to_date
from scrapers.eurostat.revision_tracker import write_partition, _estimate_release_date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

_VAULT_BASE = get_vault_root(str(Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"))
VAULT_PRODUCT = "Housing_Supply_and_Shelter_Inflation"
SOURCE        = "eurostat_sdmx"
FILENAME      = "housing_hicp_rent_data.parquet"

MACRO_METRIC_NAME = "CPI_RENT_OF_PRIMARY_RESIDENCE"
METRIC_CODE       = "HICP_RENT_CP041"
RELEASE_LAG_DAYS  = 30
START_PERIOD      = "2000-01"


def _build_vintage_id(iso3: str, obs_date: pd.Timestamp, version: int = 1) -> str:
    period = obs_date.strftime("%Y-%m")
    return f"EUROSTAT-{iso3}-{METRIC_CODE}-{period}-v{version}"


def run(start_period: str = START_PERIOD) -> int:
    log.info("=" * 70)
    log.info("EU27 HICP Rent CP041 (prc_hicp_minr) -> housing gold standard")
    log.info(f"Countries: {len(ALL_GEO2)} | Start: {start_period} | Metric: {MACRO_METRIC_NAME}")
    log.info("=" * 70)

    df_raw = fetch_dataset(
        dataset_id=  "prc_hicp_minr",
        filters=      {"unit": "I15", "freq": "M", "coicop18": "CP041"},   # prc_hicp_minr uses coicop18
        geo_list=     ALL_GEO2,
        start_period= start_period,
    )

    if df_raw.empty:
        log.error("prc_hicp_minr CP041 returned 0 rows — aborting")
        return 0

    log.info(f"Raw rows from API: {len(df_raw):,}")

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
        vid   = _build_vintage_id(iso3, obs_date, 1)
        sid   = f"EUROSTAT_HICP_CP041_{iso3}"

        rows.append({
            "data_vintage_id":        vid,
            "confidence_tier":        "PRIMARY",
            "sovereign_series_id":    sid,
            "macro_metric_name":      MACRO_METRIC_NAME,
            "reporting_date":         obs_date.strftime("%Y-%m-%d"),
            "official_release_date":  rdate,
            "as_of_date":             rdate + "T00:00:00Z",
            "observed_value":         float(val),
            "unit_of_measure":        "INDEX_2015_100",
            "is_revised_figure":      False,
            "data_timestamp":         obs_date.isoformat() + "Z",
            "revision_number":        1,
            "iso_alpha3":             iso3,
            "country_name":           ISO3_TO_NAME.get(iso3, iso3),
            "source":                 SOURCE,
            "source_agency":          "EUROSTAT",
            "source_sub_category":    "HICP_CPI",
            "sdmx_dataflow":          "prc_hicp_minr",
            "sdmx_coicop":            "CP041",
            "observation_period":     obs_date.strftime("%Y-%m"),
            "sdmx_frequency":         "M",
            "published_date":         rdate,
            "data_quality_certified": True,
            "is_forecast":            False,
            **({"data_status": r["status"]} if r.get("status") else {}),
        })

    if not rows:
        log.error("Schema mapping produced 0 rows")
        return 0

    df_vault = pd.DataFrame(rows)
    log.info(f"Vault rows mapped: {len(df_vault):,} across "
             f"{df_vault['iso_alpha3'].nunique()} countries")

    # Group by country and log per-country counts before writing
    for iso3, grp in df_vault.groupby("iso_alpha3"):
        log.info(f"  {iso3}: {len(grp):,} rows ({grp['reporting_date'].min()} to "
                 f"{grp['reporting_date'].max()})")

    df_vault["_obs_ts"] = pd.to_datetime(
        df_vault["data_timestamp"], errors="coerce", utc=True
    )

    total_written = 0
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

    log.info(f"EU27 HICP Rent CP041 ingestion complete: {total_written:,} rows written")
    return total_written


if __name__ == "__main__":
    run()
