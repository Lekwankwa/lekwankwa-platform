"""
Eurostat Food Pricing Ingestor

Source  : prc_hicp_minr (HICP monthly data, index 2015=100) — replaces discontinued prc_hicp_midx
Coverage: 27 EU member states × 11 COICOP food codes × 2000-present
Output  : lekwankwa-historical-vault/product=food_micropricing/
              country={ISO3}/source=eurostat_sdmx/year=YYYY/month=MM/
              food_pricing_data.parquet

Schema conforms to SCHEMA_GOLD_STANDARD food_pricing.json field names.

Note: HICP values are indices (2015=100), not actual prices.
      observed_price_local carries the index value;
      unit_measure_standardized = "2015=100".
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
from scrapers.eurostat.series_map import FOOD_CONFIG, FOOD_COICOP_CODES, FOOD_COICOP_NAMES
from scrapers.eurostat.eurostat_client import fetch_dataset, period_to_date
from scrapers.eurostat.revision_tracker import write_partition, build_vintage_id, _estimate_release_date

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

_VAULT_BASE = get_vault_root(str(Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"))
VAULT_PRODUCT = "food_micropricing"
SOURCE        = "eurostat_sdmx"
FILENAME      = "food_pricing_data.parquet"


def _build_food_rows(df: pd.DataFrame) -> pd.DataFrame:
    """
    Convert raw HICP API observations → food_pricing schema rows.

    Each row in df has columns: freq, unit, coicop18, geo, time, value, status
    (prc_hicp_minr uses coicop18 dimension; prc_hicp_midx used coicop)
    Output schema matches SCHEMA_GOLD_STANDARD food_pricing.json.
    """
    if df.empty:
        return pd.DataFrame()

    rows = []
    cfg  = FOOD_CONFIG
    lag  = cfg["release_lag_days"]

    for _, r in df.iterrows():
        geo      = r.get("geo", "")
        iso3     = GEO2_TO_ISO3.get(str(geo))
        if not iso3:
            continue

        coicop   = str(r.get("coicop18") or r.get("coicop", ""))
        period   = str(r.get("time", ""))
        obs_date = period_to_date(period)
        if obs_date is None:
            continue

        val = r.get("value")
        if pd.isna(val) if isinstance(val, float) else val is None:
            continue

        coicop_name = FOOD_COICOP_NAMES.get(coicop, coicop)
        metric_code = f"HICP_{coicop}"
        vid         = build_vintage_id(iso3, metric_code, obs_date, 1)
        rdate       = _estimate_release_date(obs_date, lag)
        period_str  = obs_date.strftime("%Y-%m")

        rows.append({
            # Gold-standard food_pricing.json fields
            "internal_item_id":        vid,
            "data_vintage_id":         vid,
            "confidence_tier":         "PRIMARY",
            "global_coicop_code":      coicop,
            "standard_name":           coicop_name,
            "local_name":              coicop_name,
            "category":                "FOOD" if coicop != "CP012" else "BEVERAGES_NON_ALCOHOLIC",
            "observation_period":      period_str,
            "official_release_date":   rdate,
            "as_of_date":              rdate + "T00:00:00Z",
            "is_revised_figure":       False,
            "observed_price_local":    float(val),
            "price_usd_equivalent":    None,
            "fx_rate_applied":         None,
            "fx_rate_date":            None,
            "unit_quantity_standardized": "INDEX",
            "unit_measure_standardized":  "2015=100",

            # PIT mandatory
            "sovereign_series_id":     f"EUROSTAT_HICP_{coicop}",
            "data_timestamp":          obs_date.isoformat() + "Z",
            "revision_number":         1,

            # Provenance
            "iso_alpha3":              iso3,
            "country_name":            ISO3_TO_NAME.get(iso3, iso3),
            "source":                  SOURCE,
            "source_agency":           "EUROSTAT",
            "source_sub_category":     "HICP_CPI",
            "sdmx_frequency":          "M",
            "unit_of_measure":         "INDEX_2015_100",
            "published_date":          rdate,
            "data_quality_certified":  True,
            "is_forecast":             False,
            **({"data_status": r["status"]} if r.get("status") else {}),
        })

    return pd.DataFrame(rows)


def run(start_period: str = "2000-01") -> int:
    """Fetch HICP food data for all 27 EU countries and write to vault."""
    log.info("=" * 70)
    log.info("EUROSTAT — Food Pricing (HICP prc_hicp_minr)")
    log.info(f"Countries: {len(ALL_GEO2)} | COICOP codes: {len(FOOD_COICOP_CODES)}")
    log.info("=" * 70)

    filters = {
        **FOOD_CONFIG["static_filters"],
        "coicop18": FOOD_COICOP_CODES,   # prc_hicp_minr uses coicop18 dimension (ECOICOP)
    }

    df_raw = fetch_dataset(
        dataset_id=   "prc_hicp_minr",
        filters=       filters,
        geo_list=      ALL_GEO2,
        start_period=  start_period,
    )

    if df_raw.empty:
        log.error("No data returned from Eurostat API")
        return 0

    log.info(f"Raw rows fetched: {len(df_raw):,}")
    df_vault = _build_food_rows(df_raw)

    if df_vault.empty:
        log.error("Schema mapping produced 0 rows")
        return 0

    log.info(f"Schema rows built: {len(df_vault):,}")

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

    countries_written = df_vault["iso_alpha3"].nunique()
    log.info(
        f"\nFood pricing ingestion complete: {total_written:,} rows across "
        f"{countries_written} countries"
    )
    return total_written


if __name__ == "__main__":
    run()
