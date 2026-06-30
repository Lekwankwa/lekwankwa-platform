"""
Eurostat Housing & Credit Ingestor

Sources:
  prc_hpi_q    House Price Index (quarterly, all dwellings, 2015=100)
  sts_cobp_q   Building permits issued (quarterly, dwellings, number, NSA)

Output: lekwankwa-historical-vault/product=Housing_Supply_and_Shelter_Inflation/
            country={ISO3}/source=eurostat_sdmx/year=YYYY/month=MM/
            housing_data.parquet

Schema matches SCHEMA_GOLD_STANDARD housing_building_permits.json field names.
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
from scrapers.eurostat.series_map import HOUSING_CONFIGS
from scrapers.eurostat.eurostat_client import fetch_dataset, period_to_date
from scrapers.eurostat.revision_tracker import (
    write_partition, build_vintage_id, _estimate_release_date,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

_VAULT_BASE = get_vault_root(str(Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"))
VAULT_PRODUCT = "Housing_Supply_and_Shelter_Inflation"
SOURCE        = "eurostat_sdmx"
FILENAME      = "housing_data.parquet"


def _build_housing_rows(df_raw: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    """Map raw API observations to housing schema."""
    if df_raw.empty:
        return pd.DataFrame()

    metric_code = cfg["metric_code"]
    metric_name = cfg["macro_metric_name"]
    unit        = cfg["unit_of_measure"]
    lag         = cfg["release_lag_days"]
    freq        = cfg["freq"]
    source_sub  = cfg["source_sub_category"]

    geo_col = next((c for c in df_raw.columns if c.lower().startswith("geo")), "geo")

    rows = []
    for _, r in df_raw.iterrows():
        geo      = str(r.get(geo_col, ""))
        iso3     = GEO2_TO_ISO3.get(geo)
        if not iso3:
            continue

        period   = str(r.get("time", ""))
        obs_date = period_to_date(period)
        if obs_date is None:
            continue

        val = r.get("value")
        if pd.isna(val) if isinstance(val, float) else val is None:
            continue

        # For building permits, cpa2_1 varies by country — encode in series ID
        cpa_suffix = ""
        if "cpa2_1" in df_raw.columns:
            cpa_val = str(r.get("cpa2_1", "")).replace("CPA_", "").replace("-", "_")
            if cpa_val and cpa_val != "nan":
                cpa_suffix = f"_{cpa_val}"

        sid   = f"{metric_code}{cpa_suffix}_{iso3}"
        vid_code = f"{metric_code}{cpa_suffix}"
        vid   = build_vintage_id(iso3, vid_code, obs_date, 1)
        rdate = _estimate_release_date(obs_date, lag)

        rows.append({
            # Gold-standard housing_building_permits.json fields
            "data_vintage_id":       vid,
            "confidence_tier":       "PRIMARY",
            "sovereign_series_id":   sid,
            "macro_metric_name":     metric_name,
            "reporting_date":        obs_date.strftime("%Y-%m-%d"),
            "official_release_date": rdate,
            "as_of_date":            rdate + "T00:00:00Z",
            "observed_value":        float(val),
            "unit_of_measure":       unit,
            "is_revised_figure":     False,

            # PIT mandatory
            "data_timestamp":        obs_date.isoformat() + "Z",
            "revision_number":       1,

            # Provenance
            "iso_alpha3":            iso3,
            "country_name":          ISO3_TO_NAME.get(iso3, iso3),
            "source":                SOURCE,
            "source_agency":         "EUROSTAT",
            "source_sub_category":   source_sub,
            "sdmx_frequency":        freq,
            "published_date":        rdate,
            "data_quality_certified": True,
            "is_forecast":           False,
            **({"data_status": r["status"]} if r.get("status") else {}),
        })

    return pd.DataFrame(rows)


def _ingest_one(cfg: dict) -> int:
    dataflow    = cfg["dataflow"]
    metric_code = cfg["metric_code"]
    start       = cfg.get("start_period", "2000-Q1")

    log.info(f"  Fetching {dataflow} ({metric_code}) from {start}")
    df_raw = fetch_dataset(
        dataset_id=   dataflow,
        filters=       cfg["static_filters"],
        geo_list=      ALL_GEO2,
        start_period=  start,
    )

    if df_raw.empty:
        log.warning(f"  No data for {dataflow}")
        return 0

    log.info(f"  Raw rows: {len(df_raw):,}")
    df_vault = _build_housing_rows(df_raw, cfg)

    if df_vault.empty:
        log.warning(f"  Schema mapping produced 0 rows for {dataflow}")
        return 0

    total_written = 0
    df_vault["_obs_date"] = pd.to_datetime(
        df_vault["data_timestamp"], errors="coerce", utc=True
    )

    for iso3, iso_grp in df_vault.groupby("iso_alpha3"):
        vault_root = (
            _VAULT_BASE
            / f"product={VAULT_PRODUCT}"
            / f"country={iso3}"
            / f"source={SOURCE}"
        )
        for (year, month), period_grp in iso_grp.groupby([
            iso_grp["_obs_date"].dt.year,
            iso_grp["_obs_date"].dt.month,
        ]):
            out_df = period_grp.drop(columns=["_obs_date"])
            write_partition(out_df, vault_root, int(year), int(month), FILENAME)
            total_written += len(out_df)

    return total_written


def run() -> int:
    log.info("=" * 70)
    log.info("EUROSTAT — Housing & Credit")
    log.info(f"Series: {len(HOUSING_CONFIGS)} | Countries: {len(ALL_GEO2)}")
    log.info("=" * 70)

    total = 0
    for cfg in HOUSING_CONFIGS:
        total += _ingest_one(cfg)

    log.info(f"\nHousing & credit ingestion complete: {total:,} rows")
    return total


if __name__ == "__main__":
    run()
