"""
EU27 Unemployment Rate Ingestor — Gold Standard conformant

Pulls une_rt_m (monthly harmonised unemployment rate, seasonally adjusted,
all ages, both sexes) for all 27 EU member states and writes rows that match
unemployment.json gold standard schema field names exactly.

Key schema differences vs. the existing wages_data.parquet ingest:
  macro_metric_name  -> "UNEMPLOYMENT_RATE_U3"  (harmonised U3 equivalent)
  unit_of_measure    -> "PERCENTAGE"
  sovereign_series_id -> "EUROSTAT_UNE_{ISO3}"
  data_vintage_id    -> "EUROSTAT-UNEMP-{ISO3}-{YYYY-MM}-v1"

Revision note:  Eurostat's public SDMX API does not publish a full revision
archive equivalent to ALFRED.  All rows are ingested as version-1 (initial
publication estimate).  is_revised_figure=False for all records.
A future EurostatRevisionDetector can detect changed values via the
updatedAfter API parameter and write v2+ rows.

Output: product=wages_and_employment/country={ISO3}/source=eurostat_sdmx/
            year=YYYY/month=MM/unemployment_data.parquet
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

from scrapers.utilities.vault_io import get_vault_root
_SCRAPER_ROOT = get_vault_root(str(Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"))
sys.path.insert(0, str(_SCRAPER_ROOT))

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
VAULT_PRODUCT = "wages_and_employment"
SOURCE        = "eurostat_sdmx"
FILENAME      = "unemployment_data.parquet"

# une_rt_m dimension filters — seasonally adjusted, total ages, both sexes
UNEMP_FILTERS = {
    "freq":   "M",
    "age":    "TOTAL",
    "sex":    "T",
    "unit":   "PC_ACT",
    "s_adj":  "SA",
}

METRIC_CODE       = "UNEMP_U3"
MACRO_METRIC_NAME = "UNEMPLOYMENT_RATE_U3"
UNIT_OF_MEASURE   = "PERCENTAGE"
RELEASE_LAG_DAYS  = 45   # LFS flash ~6 weeks after reference month
START_PERIOD      = "1998-01"


def _build_vintage_id(iso3: str, obs_date: pd.Timestamp, version: int = 1) -> str:
    period = obs_date.strftime("%Y-%m")
    return f"EUROSTAT-UNEMP-{iso3}-{period}-v{version}"


def _build_rows(df_raw: pd.DataFrame) -> pd.DataFrame:
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
        vid   = _build_vintage_id(iso3, obs_date, 1)
        sid   = f"EUROSTAT_UNE_{iso3}"

        rows.append({
            # unemployment.json gold standard fields
            "data_vintage_id":       vid,
            "confidence_tier":       "PRIMARY",
            "sovereign_series_id":   sid,
            "macro_metric_name":     MACRO_METRIC_NAME,
            "reporting_date":        obs_date.strftime("%Y-%m-%d"),
            "official_release_date": rdate,
            "as_of_date":            rdate + "T00:00:00Z",
            "observed_value":        float(val),
            "unit_of_measure":       UNIT_OF_MEASURE,
            "is_revised_figure":     False,

            # PIT mandatory
            "data_timestamp":        obs_date.isoformat() + "Z",
            "revision_number":       1,

            # Provenance
            "iso_alpha3":            iso3,
            "country_name":          ISO3_TO_NAME.get(iso3, iso3),
            "source":                SOURCE,
            "source_agency":         "EUROSTAT",
            "source_sub_category":   "LFS",
            "observation_period":    obs_date.strftime("%Y-%m"),
            "sdmx_frequency":        "M",
            "published_date":        rdate,
            "data_quality_certified": True,
            "is_forecast":           False,
            **({"data_status": r["status"]} if r.get("status") else {}),
        })

    return pd.DataFrame(rows)


def run() -> int:
    log.info("=" * 70)
    log.info("EU27 Unemployment Rate (une_rt_m) -> unemployment.json schema")
    log.info(f"Countries: {len(ALL_GEO2)} | Start: {START_PERIOD}")
    log.info("=" * 70)

    df_raw = fetch_dataset(
        dataset_id=  "une_rt_m",
        filters=      UNEMP_FILTERS,
        geo_list=     ALL_GEO2,
        start_period= START_PERIOD,
    )

    if df_raw.empty:
        log.error("No data returned from une_rt_m")
        return 0

    log.info(f"Raw rows fetched: {len(df_raw):,}")
    df_vault = _build_rows(df_raw)

    if df_vault.empty:
        log.error("Schema mapping produced 0 rows")
        return 0

    log.info(f"Vault rows mapped: {len(df_vault):,}")

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

    log.info(f"EU27 unemployment ingestion complete: {total_written:,} rows written")
    return total_written


if __name__ == "__main__":
    run()
