"""
Statistics Canada CAN full ingestion — all 5 vault products.

Downloads NDM CSV tables, extracts specific vectors, builds PIT-compliant
vault rows, and writes to Hive-partitioned parquet vault.

Usage:
    python -m scrapers.statcan.ingest_all
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

import pandas as pd

_SCRAPER_ROOT = get_vault_root(str(Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"))
sys.path.insert(0, str(_SCRAPER_ROOT))
from scrapers.utilities.vault_io import get_vault_root

from scrapers.statcan.statcan_client import fetch_vector
from scrapers.statcan.series_map import (
    ISO3, PIT_COVERAGE, SERIES, SOURCE, SOURCE_AGENCY, VAULT_PRODUCT_MAP,
)
from scrapers.shared_pit_tracker import build_vault_row, write_partition

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

_VAULT_BASE = get_vault_root(str(Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"))
def _ingest_vector(
    table_id: str,
    vector_str: str,
    metric_code: str,
    vault_product: str,
    macro_metric_name: str,
    unit: str,
    lag: int,
    freq: str,
    sub_cat: str,
) -> int:
    log.info("  StatCan %s/%s (%s) -> %s", table_id, vector_str, metric_code, vault_product)

    df_raw = fetch_vector(table_id, vector_str)
    if df_raw.empty:
        log.warning("  SKIP %s/%s: no data", table_id, vector_str)
        return 0

    log.info("  %s/%s: %d obs", table_id, vector_str, len(df_raw))

    vault_rows = []
    for _, row in df_raw.iterrows():
        vault_rows.append(
            build_vault_row(
                source_prefix=SOURCE_AGENCY,
                iso3=ISO3,
                metric_code=metric_code,
                sovereign_series_id=f"{metric_code}_{ISO3}_{vector_str}",
                macro_metric_name=macro_metric_name,
                obs_date=row["obs_date"],
                observed_value=row["value"],
                unit_of_measure=unit,
                release_lag_days=lag,
                freq=freq,
                source=SOURCE,
                source_agency=SOURCE_AGENCY,
                source_sub_category=sub_cat,
                pit_coverage_type=PIT_COVERAGE,
                extra_fields={
                    "statcan_table_id": table_id,
                    "statcan_vector":   vector_str,
                },
            )
        )

    if not vault_rows:
        return 0

    df_vault = pd.DataFrame(vault_rows)
    df_vault["_obs_date"] = pd.to_datetime(df_vault["reporting_date"])

    vault_root = (
        _VAULT_BASE
        / f"product={vault_product}"
        / f"country={ISO3}"
        / f"source={SOURCE}"
    )
    filename = VAULT_PRODUCT_MAP[vault_product]

    for (year, month), grp in df_vault.groupby(
        [df_vault["_obs_date"].dt.year, df_vault["_obs_date"].dt.month]
    ):
        write_partition(
            grp.drop(columns=["_obs_date"]),
            vault_root,
            int(year),
            int(month),
            filename,
        )

    return len(df_vault)


def run() -> int:
    log.info("=" * 70)
    log.info("StatCan CAN — All 5 vault products")
    log.info("Series: %d  |  pit_coverage_type: RELEASE_DATE_ONLY/accumulating", len(SERIES))
    log.info("=" * 70)

    total = 0
    for entry in SERIES:
        total += _ingest_vector(*entry)

    log.info("\nStatCan CAN ingestion complete: %d rows written", total)
    if total > 0:
        from tools.trigger_downstream import trigger_quality_live
        trigger_quality_live()
    return total


if __name__ == "__main__":
    run()
