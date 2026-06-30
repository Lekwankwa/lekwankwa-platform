"""
EU27 Wages & Salaries Ingestor — wages_and_employment vault

Pulls nama_10_a10 (National Accounts by Industry) na_item=D11 (Wages and
Salaries, i.e. total compensation excluding employers' social contributions)
for all 27 EU member states. This is the closest Eurostat equivalent to
BLS Average Hourly/Weekly Earnings for the full economy.

Source: Annual national accounts (freq=A), current prices, millions EUR,
        total economy (nace_r2=TOTAL).

Gold standard field mapping (wages.json):
  macro_metric_name   -> "WAGES_AND_SALARIES_TOTAL"
  unit_of_measure     -> "MIO_EUR"
  sovereign_series_id -> "EUROSTAT_WAGES_D11_{ISO3}"
  data_vintage_id     -> "EUROSTAT-{ISO3}-WAGES_D11-{YYYY}-v1"
  source_sub_category -> "NATIONAL_ACCOUNTS"
  source_agency       -> "EUROSTAT"
  confidence_tier     -> "PRIMARY"

Note: D11 (Wages and Salaries) is sourced from Eurostat National Accounts
      (not LFS). It is annual (freq=A). Quarterly LFS earnings dataflows
      (lc_lci_r2, earn_gr_nace2) are not available on the SDMX v1.0 API.

Vintage-ID uses YYYY (not YYYY-MM) because the observation period is annual.
The reporting_date is set to {YYYY}-01-01 (first day of the reference year).
The official_release_date is {YYYY+1}-03-31 (~90 days after year-end, flash).

Output: product=wages_and_employment/country={ISO3}/
            source=eurostat_sdmx/year={YYYY}/month=01/wages_compensation_data.parquet

Idempotent: write_partition deduplicates on data_vintage_id.
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

_SCRAPER_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(_SCRAPER_ROOT))

from scrapers.eurostat.country_map import ALL_GEO2, GEO2_TO_ISO3, ISO3_TO_NAME
from scrapers.eurostat.eurostat_client import fetch_dataset
from scrapers.eurostat.revision_tracker import write_partition

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

_VAULT_BASE   = Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"
VAULT_PRODUCT = "wages_and_employment"
SOURCE        = "eurostat_sdmx"
FILENAME      = "wages_compensation_data.parquet"

MACRO_METRIC_NAME = "WAGES_AND_SALARIES_TOTAL"
METRIC_CODE       = "WAGES_D11"
START_PERIOD      = "1995"


def _build_vintage_id(iso3: str, year: int, version: int = 1) -> str:
    return f"EUROSTAT-{iso3}-{METRIC_CODE}-{year}-v{version}"


def _estimate_annual_release(year: int) -> str:
    """Annual accounts first estimate published ~Q1 of following year."""
    return f"{year + 1}-03-31"


def run() -> int:
    log.info("=" * 70)
    log.info("EU27 Wages & Salaries D11 (nama_10_a10) -> wages gold standard")
    log.info(f"Countries: {len(ALL_GEO2)} | Start: {START_PERIOD} | Metric: {MACRO_METRIC_NAME}")
    log.info("=" * 70)

    df_raw = fetch_dataset(
        dataset_id=  "nama_10_a10",
        filters=      {
            "freq":      "A",
            "unit":      "CP_MEUR",
            "nace_r2":   "TOTAL",
            "na_item":   "D11",
        },
        geo_list=     ALL_GEO2,
        start_period= START_PERIOD,
    )

    if df_raw.empty:
        log.error("nama_10_a10 D11 returned 0 rows — aborting")
        return 0

    log.info(f"Raw rows from API: {len(df_raw):,}")

    geo_col = next((c for c in df_raw.columns if c.lower().startswith("geo")), "geo")

    rows = []
    for _, r in df_raw.iterrows():
        geo  = str(r.get(geo_col, ""))
        iso3 = GEO2_TO_ISO3.get(geo)
        if not iso3:
            continue

        time_val = str(r.get("time", ""))
        try:
            year = int(time_val[:4])
        except ValueError:
            continue

        val = r.get("value")
        if pd.isna(val) if isinstance(val, float) else val is None:
            continue

        obs_date  = pd.Timestamp(year, 1, 1)        # first day of reference year
        rdate     = _estimate_annual_release(year)
        vid       = _build_vintage_id(iso3, year, 1)
        sid       = f"EUROSTAT_WAGES_D11_{iso3}"

        rows.append({
            "data_vintage_id":        vid,
            "confidence_tier":        "PRIMARY",
            "sovereign_series_id":    sid,
            "macro_metric_name":      MACRO_METRIC_NAME,
            "reporting_date":         obs_date.strftime("%Y-%m-%d"),
            "official_release_date":  rdate,
            "as_of_date":             rdate + "T00:00:00Z",
            "observed_value":         float(val),
            "unit_of_measure":        "MIO_EUR",
            "is_revised_figure":      False,
            "data_timestamp":         obs_date.isoformat() + "Z",
            "revision_number":        1,
            "iso_alpha3":             iso3,
            "country_name":           ISO3_TO_NAME.get(iso3, iso3),
            "source":                 SOURCE,
            "source_agency":          "EUROSTAT",
            "source_sub_category":    "NATIONAL_ACCOUNTS",
            "sdmx_dataflow":          "nama_10_a10",
            "sdmx_na_item":           "D11",
            "observation_period":     str(year),
            "sdmx_frequency":         "A",
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

    for iso3, grp in df_vault.groupby("iso_alpha3"):
        log.info(f"  {iso3}: {len(grp):,} rows ({grp['reporting_date'].min()[:4]}"
                 f"–{grp['reporting_date'].max()[:4]})")

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

    log.info(f"EU27 Wages Compensation ingestion complete: {total_written:,} rows written")
    return total_written


if __name__ == "__main__":
    run()
