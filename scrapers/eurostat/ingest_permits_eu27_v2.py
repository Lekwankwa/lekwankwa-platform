"""
EU27 Building Permits Monthly Ingestor — Gold Standard conformant

Pulls sts_cobp_m (monthly building permits, residential dwellings) for all
27 EU member states and writes rows matching housing_building_permits.json
gold standard schema exactly.

Primary source:  sts_cobp_m (monthly) with unit=NR (actual permit count)
Fallback:        sts_cobp_m with unit=I15 (index 2015=100) if NR unavailable
                 sts_cobp_q (quarterly) if sts_cobp_m returns no data at all

This gives true monthly coverage (12 data points per year) rather than the
quarterly coverage of the original sts_cobp_q ingest in housing_data.parquet.

Vintage-ID format (per user spec): EUROSTAT-PERMIT-{ISO3}-{YYYY-MM}-v{N}

Revision note: Eurostat SDMX API does not publish revision archives.
All rows are version-1 initial ingestions. is_revised_figure=False.

Output: product=Housing_Supply_and_Shelter_Inflation/country={ISO3}/
            source=eurostat_sdmx/year=YYYY/month=MM/permits_eu27_data.parquet
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
FILENAME      = "permits_eu27_data.parquet"

MACRO_METRIC_NAME = "AUTHORIZED_PERMITS_TOTAL_UNITS"
RELEASE_LAG_DAYS  = 60   # Monthly permits published ~2 months after reference month
START_PERIOD_M    = "2000-01"
START_PERIOD_Q    = "2000-Q1"

# Attempt order: monthly NR (counts) -> monthly I15 (index) -> quarterly I15
FETCH_ATTEMPTS = [
    {
        "dataflow":    "sts_cobp_m",
        "filters":     {"freq": "M", "s_adj": "NSA", "indic_bt": "BPRM_DW", "unit": "NR"},
        "freq":        "M",
        "unit_label":  "PERMITS_COUNT",
        "start":       START_PERIOD_M,
    },
    {
        "dataflow":    "sts_cobp_m",
        "filters":     {"freq": "M", "s_adj": "NSA", "indic_bt": "BPRM_DW", "unit": "I15"},
        "freq":        "M",
        "unit_label":  "INDEX_2015_100",
        "start":       START_PERIOD_M,
    },
    {
        "dataflow":    "sts_cobp_q",
        "filters":     {"freq": "Q", "s_adj": "NSA", "indic_bt": "BPRM_DW", "unit": "I15"},
        "freq":        "Q",
        "unit_label":  "INDEX_2015_100",
        "start":       START_PERIOD_Q,
    },
]


def _build_vintage_id(iso3: str, obs_date: pd.Timestamp, version: int = 1) -> str:
    period = obs_date.strftime("%Y-%m")
    return f"EUROSTAT-PERMIT-{iso3}-{period}-v{version}"


def _build_rows(df_raw: pd.DataFrame, freq: str, unit_label: str,
                source_dataflow: str) -> pd.DataFrame:
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

        # Embed cpa2_1 sub-category if present (e.g., CPA_F410011)
        cpa_suffix = ""
        if "cpa2_1" in df_raw.columns:
            cpa_val = str(r.get("cpa2_1", "")).replace("CPA_", "").replace("-", "_")
            if cpa_val and cpa_val != "nan":
                cpa_suffix = f"_{cpa_val}"

        rdate = _estimate_release_date(obs_date, RELEASE_LAG_DAYS)
        vid   = _build_vintage_id(iso3, obs_date, 1)
        sid   = f"EUROSTAT_PERMIT{cpa_suffix}_{iso3}"

        rows.append({
            # housing_building_permits.json gold standard fields
            "data_vintage_id":       vid,
            "confidence_tier":       "PRIMARY",
            "sovereign_series_id":   sid,
            "macro_metric_name":     MACRO_METRIC_NAME,
            "reporting_date":        obs_date.strftime("%Y-%m-%d"),
            "official_release_date": rdate,
            "as_of_date":            rdate + "T00:00:00Z",
            "observed_value":        float(val),
            "unit_of_measure":       unit_label,
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
            "sdmx_dataflow":         source_dataflow,
            "observation_period":    obs_date.strftime("%Y-%m"),
            "sdmx_frequency":        freq,
            "published_date":        rdate,
            "data_quality_certified": True,
            "is_forecast":           False,
            **({"data_status": r["status"]} if r.get("status") else {}),
        })

    return pd.DataFrame(rows)


def run() -> int:
    log.info("=" * 70)
    log.info("EU27 Building Permits -> housing_building_permits.json schema")
    log.info(f"Countries: {len(ALL_GEO2)} | Target: AUTHORIZED_PERMITS_TOTAL_UNITS")
    log.info("=" * 70)

    df_raw = pd.DataFrame()
    used_attempt = None

    for attempt in FETCH_ATTEMPTS:
        log.info(f"Trying {attempt['dataflow']} unit={attempt['filters'].get('unit')} freq={attempt['freq']}")
        df_raw = fetch_dataset(
            dataset_id=  attempt["dataflow"],
            filters=      attempt["filters"],
            geo_list=     ALL_GEO2,
            start_period= attempt["start"],
        )
        if not df_raw.empty and len(df_raw) > 100:
            used_attempt = attempt
            log.info(f"  Success: {len(df_raw):,} raw rows from {attempt['dataflow']}")
            break
        log.warning(f"  {attempt['dataflow']} returned {len(df_raw)} rows — trying next")

    if df_raw.empty or used_attempt is None:
        log.error("All permit data sources returned empty — aborting")
        return 0

    df_vault = _build_rows(
        df_raw,
        freq=            used_attempt["freq"],
        unit_label=      used_attempt["unit_label"],
        source_dataflow= used_attempt["dataflow"],
    )

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

    log.info(f"EU27 permits ingestion complete: {total_written:,} rows written")
    return total_written


if __name__ == "__main__":
    run()
