from __future__ import annotations

import logging
import time
import sys

import pandas as pd
Usage:
    python -m scrapers.statcan.ingest_all
"""

from __future__ import annotations

import logging
import sys

import pandas as pd

from scrapers.utilities.vault_io import get_vault_root
from tools.secret_manager import load_all_secrets_to_env
load_all_secrets_to_env()

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

_VAULT_BASE = get_vault_root("lekwankwa-historical-vault")

) -> int:
    log.info("  StatCan %s/%s (%s) -> %s", table_id, vector_str, metric_code, vault_product)

    df_raw = pd.DataFrame()
    last_exc = None
    for attempt in range(1, 4):
        try:
            df_raw = fetch_vector(table_id, vector_str)
            if not df_raw.empty:
                break
            log.warning("  attempt %d/3: empty response for %s/%s, retrying...",
                        attempt, table_id, vector_str)
        except Exception as exc:  # noqa: BLE001
            last_exc = exc
            log.warning("  attempt %d/3 failed for %s/%s: %s", attempt, table_id, vector_str, exc)
        time.sleep(2 * attempt)

    if df_raw.empty:
        log.warning("  SKIP %s/%s: no data", table_id, vector_str)
        return 0
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


def _validate_product(product: str) -> None:
    """Run the 9-stage validation for one product's CAN data. Escalates to
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
    total = 0
    rows_by_product: dict[str, int] = {}
    products_with_series: dict[str, int] = {}
    for entry in SERIES:
        rows = _ingest_vector(*entry)
        total += rows
        vault_product = entry[3]
        rows_by_product[vault_product] = rows_by_product.get(vault_product, 0) + rows
        products_with_series[vault_product] = products_with_series.get(vault_product, 0) + 1

    log.info("\nStatCan CAN ingestion complete: %d rows written", total)

    for product, expected in products_with_series.items():
        if rows_by_product.get(product, 0) == 0:
            msg = (f"No rows ingested for product={product} country={ISO3} "
                   f"despite {expected} configured series; aborting before validation.")
            log.error(msg)
            exc = RuntimeError(msg)
            try:
                from tools.self_healing.handler import handle_exception
                from datetime import date
                handle_exception(
                    program=__file__,
                    exception=exc,
                    context={"product": product, "country": ISO3, "source": SOURCE,
                             "run_date": date.today().isoformat(), "layer": "INGESTION"},
                )
            except ImportError:
                pass

    for product, rows in rows_by_product.items():
        if rows > 0:
            log.info("  Validating %s ...", product)
            _validate_product(product)
    total = 0
    rows_by_product: dict[str, int] = {}
    for entry in SERIES:
        rows = _ingest_vector(*entry)
        total += rows
        vault_product = entry[3]
        rows_by_product[vault_product] = rows_by_product.get(vault_product, 0) + rows

    log.info("\nStatCan CAN ingestion complete: %d rows written", total)

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
