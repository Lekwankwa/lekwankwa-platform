"""
SSB Norway full ingestion — all 5 vault products.

Fetches all series via SSB PX-Web API, builds PIT-compliant vault rows,
and writes to Hive-partitioned parquet vault.

Usage:
    python -m scrapers.ssb.ingest_all
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path
from typing import Any

import pandas as pd

_SCRAPER_ROOT = get_vault_root(str(Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"))
sys.path.insert(0, str(_SCRAPER_ROOT))
from scrapers.utilities.vault_io import get_vault_root

from scrapers.ssb.ssb_client import fetch_table, get_table_meta
from scrapers.ssb.series_map import (
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
def _ingest_series(
    table_id: str,
    query_body: dict[str, Any],
    metric_code: str,
    vault_product: str,
    macro_metric_name: str,
    unit: str,
    lag: int,
    freq: str,
    sub_cat: str,
    dedup_dim: str | None = None,
) -> int:
    log.info("  SSB table=%s (%s) -> %s", table_id, metric_code, vault_product)

    df_raw = fetch_table(table_id, query_body, dedup_dim=dedup_dim)
    if df_raw.empty:
        log.warning("  SKIP table=%s %s: no data returned", table_id, metric_code)
        return 0

    log.info("  table=%s %s: %d obs", table_id, metric_code, len(df_raw))

    vault_rows = []
    for _, row in df_raw.iterrows():
        vault_rows.append(
            build_vault_row(
                source_prefix=SOURCE_AGENCY,
                iso3=ISO3,
                metric_code=metric_code,
                sovereign_series_id=f"{metric_code}_{ISO3}",
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
                extra_fields={"ssb_table_id": table_id},
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


def _probe_bop_codes(table_id: str = "10644") -> list[str]:
    """
    Discover available ContentsCode values in SSB BOP table at runtime.
    Returns list of available codes for logging.
    """
    meta = get_table_meta(table_id)
    if not meta:
        return []
    variables = meta.get("variables", [])
    for var in variables:
        if var.get("code") == "ContentsCode":
            return list(zip(var.get("values", []), var.get("valueTexts", [])))
    return []


def run() -> int:
    log.info("=" * 70)
    log.info("SSB NOR — All 5 vault products")
    log.info("Series: %d  |  pit_coverage_type: RELEASE_DATE_ONLY/accumulating", len(SERIES))
    log.info("=" * 70)

    # Log available BOP codes so we can correct series_map if needed
    bop_codes = _probe_bop_codes("10644")
    if bop_codes:
        log.info("BOP table 10644 ContentsCode values: %s", bop_codes[:10])

    total = 0
    for entry in SERIES:
        total += _ingest_series(*entry)

    log.info("\nSSB NOR ingestion complete: %d rows written", total)
    return total


if __name__ == "__main__":
    run()
