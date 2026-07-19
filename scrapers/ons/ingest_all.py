"""
ONS GBR full ingestion — all 5 vault products.

Fetches 12 confirmed series via direct ONS website URIs, builds
PIT-compliant vault rows, and writes to Hive-partitioned parquet vault.

URIs are hardcoded (ONS search API does not index economic CDIDs).
Data endpoint: https://www.ons.gov.uk{uri}/data

Usage:
    python -m scrapers.ons.ingest_all
"""

from __future__ import annotations

import logging
import sys

import pandas as pd

from scrapers.utilities.vault_io import get_vault_root
from tools.secret_manager import load_all_secrets_to_env
load_all_secrets_to_env()

from scrapers.ons.ons_client import fetch_timeseries
from scrapers.ons.series_map import (
    ISO3, PIT_COVERAGE, SERIES, SOURCE, SOURCE_AGENCY, VAULT_PRODUCT_MAP,
)
from scrapers.shared_pit_tracker import build_vault_row, write_partition

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

_VAULT_BASE = get_vault_root("lekwankwa-historical-vault")
def _ingest_cdid(
    cdid: str,
    uri: str,
    metric_code: str,
    vault_product: str,
    macro_metric_name: str,
    unit: str,
    lag: int,
    freq: str,
    sub_cat: str,
) -> int:
    log.info("  ONS %-6s (%s) -> %s", cdid, metric_code, vault_product)

    df_raw = fetch_timeseries(cdid, uri)
    if df_raw.empty:
        log.warning("  SKIP %s: no data returned", cdid)
        return 0

    log.info("  %s: %d obs", cdid, len(df_raw))

    vault_rows = []
    for _, row in df_raw.iterrows():
        vault_rows.append(
            build_vault_row(
                source_prefix=SOURCE_AGENCY,
                iso3=ISO3,
                metric_code=metric_code,
                sovereign_series_id=f"{metric_code}_{ISO3}_{cdid}",
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
                extra_fields={"ons_cdid": cdid},
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


def _validate_product(product: str) -> None:
    """Run the 9-stage validation for one product's GBR data. Escalates to
    self-healing on exception or CRITICAL/HIGH findings."""
    from datetime import date
    context = {"product": product, "country": ISO3,
               "source": SOURCE, "run_date": date.today().isoformat(), "layer": "VALIDATION"}
    try:
        from tools.vault_audit import run_9_stage_validation
        val = run_9_stage_validation(product=product, country=ISO3)
    except Exception as exc:
        log.error("validation raised for %s: %s", product, exc, exc_info=True)
        try:
            from tools.self_healing.handler import handle_exception
            handle_exception(program=__file__, exception=exc, context=context)
        except ImportError:
            pass
        return

    if val.severity in ("CRITICAL", "HIGH"):
        from tools.self_healing.handler import handle_validation_finding
        handle_validation_finding(program=__file__, context=context, result=val)


def run() -> int:
    log.info("=" * 70)
    log.info("ONS GBR -- All 5 vault products")
    log.info("Series: %d  |  pit_coverage_type: RELEASE_DATE_ONLY/accumulating", len(SERIES))
    log.info("=" * 70)

    total = 0
    rows_by_product: dict[str, int] = {}

    log.info("\nONS GBR ingestion complete: %d rows written", total)

    if total == 0:
        log.error(
            "ONS GBR ingestion produced 0 rows across all %d series -- "
            "likely upstream fetch failure (ONS API unreachable or empty "
            "responses). Escalating instead of running validation on stale/"
            "missing data.",
            len(SERIES),
        )
        try:
            from datetime import date
            from tools.self_healing.handler import handle_exception
            context = {
                "product": "global_macro", "country": ISO3,
                "source": SOURCE, "run_date": date.today().isoformat(),
                "layer": "INGESTION",
            }
            handle_exception(
                program=__file__,
                exception=RuntimeError(
                    "ONS GBR ingestion returned 0 rows for all series"
                ),
                context=context,
            )
        except ImportError:
            pass
        return total

    for product, rows in rows_by_product.items():
        if rows > 0:
            log.info("  Validating %s ...", product)    log.info("\nONS GBR ingestion complete: %d rows written", total)

    for product, rows in rows_by_product.items():
        if rows > 0:
            log.info("  Validating %s ...", product)
            _validate_product(product)

    if total > 0:
        from tools.trigger_downstream import trigger_all_metadata
        trigger_all_metadata()
    return total


if __name__ == "__main__":
    run()
