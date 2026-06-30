"""
Eurostat Trade Flows Ingestor

Source: namq_10_gdp (Quarterly National Accounts)
        NA items: P6 (exports), P7 (imports) — current prices, million EUR, SCA

Using National Accounts trade data avoids the COMEXT bulk-download requirement
while still providing PIT-compatible quarterly trade series for all 27 countries.

Output: lekwankwa-historical-vault/product=trade_flows/
            country={ISO3}/source=eurostat_sdmx/year=YYYY/month=MM/
            trade_data.parquet

Schema matches SCHEMA_GOLD_STANDARD trade_flows.json field names.
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
from scrapers.eurostat.series_map import TRADE_CONFIGS
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
VAULT_PRODUCT = "trade_flows"
SOURCE        = "eurostat_sdmx"
FILENAME      = "trade_data.parquet"


def _build_trade_rows(df_raw: pd.DataFrame, cfg: dict) -> pd.DataFrame:
    if df_raw.empty:
        return pd.DataFrame()

    metric_code = cfg["metric_code"]
    metric_name = cfg["macro_metric_name"]
    unit        = cfg["unit_of_measure"]
    lag         = cfg["release_lag_days"]
    freq        = cfg["freq"]
    source_sub  = cfg["source_sub_category"]
    na_item     = cfg.get("na_item", "")

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

        sid   = f"{metric_code}_{iso3}"
        vid   = build_vintage_id(iso3, metric_code, obs_date, 1)
        rdate = _estimate_release_date(obs_date, lag)

        rows.append({
            # Gold-standard trade_flows.json fields
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

            # Trade-specific
            "commodity_code":        na_item,
            "commodity_name":        "Goods and Services",
            "partner_country_code":  "WLD",
            "partner_country_name":  "World",
            "trade_flow":            "EXPORTS" if na_item == "P6" else "IMPORTS",
            "currency":              "EUR",

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

    log.info(f"  Fetching {dataflow} na_item={cfg.get('na_item')} ({metric_code})")
    df_raw = fetch_dataset(
        dataset_id=   dataflow,
        filters=       cfg["static_filters"],
        geo_list=      ALL_GEO2,
        start_period=  start,
    )

    if df_raw.empty:
        log.warning(f"  No data for {dataflow} ({metric_code})")
        return 0

    log.info(f"  Raw rows: {len(df_raw):,}")
    df_vault = _build_trade_rows(df_raw, cfg)

    if df_vault.empty:
        log.warning(f"  Schema mapping produced 0 rows")
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
    log.info("EUROSTAT — Trade Flows (P6 exports + P7 imports from namq_10_gdp)")
    log.info(f"Series: {len(TRADE_CONFIGS)} | Countries: {len(ALL_GEO2)}")
    log.info("=" * 70)

    total = 0
    for cfg in TRADE_CONFIGS:
        total += _ingest_one(cfg)

    log.info(f"\nTrade flows ingestion complete: {total:,} rows")
    return total


if __name__ == "__main__":
    run()
