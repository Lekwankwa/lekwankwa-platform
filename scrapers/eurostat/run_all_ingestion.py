"""
Eurostat Pipeline Orchestrator

Runs all 5 dataset ingestors in sequence for all 27 EU member states.
Each ingestor fetches data from the Eurostat Statistics API and writes
Hive-partitioned parquet files to the vault.

Usage:
    python run_all_ingestion.py [--datasets food wages housing trade macro]

If --datasets is omitted, all 5 datasets are run in the default order.
"""

from __future__ import annotations

import argparse
import logging
import sys
import time
from pathlib import Path

_SCRAPER_ROOT = get_vault_root(str(Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"))
sys.path.insert(0, str(_SCRAPER_ROOT))
from scrapers.utilities.vault_io import get_vault_root

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler("eurostat_ingestion.log")],
)
log = logging.getLogger(__name__)


def _run_dataset(name: str) -> tuple[str, int, float, str]:
    """Run one dataset ingestor. Returns (name, rows_written, seconds, status)."""
    t0 = time.time()
    try:
        if name == "food":
            from scrapers.eurostat.ingest_food_pricing import run
        elif name == "wages":
            from scrapers.eurostat.ingest_wages_labor import run
        elif name == "housing":
            from scrapers.eurostat.ingest_housing_credit import run
        elif name == "trade":
            from scrapers.eurostat.ingest_trade_flows import run
        elif name == "macro":
            from scrapers.eurostat.ingest_global_macro import run
        else:
            return name, 0, 0.0, f"UNKNOWN dataset '{name}'"

        rows = run()
        elapsed = time.time() - t0
        status = "OK" if rows > 0 else "EMPTY"
        return name, rows, elapsed, status

    except Exception as exc:
        elapsed = time.time() - t0
        log.error(f"[{name}] FAILED: {exc}", exc_info=True)
        return name, 0, elapsed, f"ERROR: {exc}"


ALL_DATASETS = ["food", "wages", "housing", "trade", "macro"]

DATASET_DESCRIPTIONS = {
    "food":    "Food Pricing         (prc_hicp_minr)           -> product=food_micropricing",
    "wages":   "Wages & Labor        (une_rt_m + lc_lci_r2)   -> product=wages_and_employment",
    "housing": "Housing & Credit     (prc_hpi_q + sts_cobp_q) -> product=Housing_Supply_*",
    "trade":   "Trade Flows          (namq_10_gdp P6+P7)      -> product=trade_flows",
    "macro":   "Global Macro         (namq_10_gdp + hicp_manr)-> product=global_macro",
}


def run(datasets: list[str] | None = None) -> dict[str, int]:
    targets = datasets or ALL_DATASETS

    log.info("=" * 72)
    log.info("EUROSTAT PIPELINE  —  27 EU countries × 5 datasets")
    log.info(f"Datasets to run: {targets}")
    log.info("=" * 72)
    for ds in targets:
        log.info(f"  {ds:8}  {DATASET_DESCRIPTIONS.get(ds, '')}")
    log.info("-" * 72)

    results: dict[str, int] = {}
    pipeline_start = time.time()

    for name in targets:
        log.info(f"\n>>> Starting: {name}")
        ds_name, rows, elapsed, status = _run_dataset(name)
        results[ds_name] = rows
        log.info(f">>> {name}: {rows:,} rows in {elapsed:.0f}s  [{status}]")
        # Brief pause between dataset ingestions
        if name != targets[-1]:
            time.sleep(1.0)

    total_elapsed = time.time() - pipeline_start
    total_rows    = sum(results.values())

    log.info("\n" + "=" * 72)
    log.info("EUROSTAT PIPELINE — SUMMARY")
    log.info("=" * 72)
    log.info(f"{'Dataset':<10}  {'Rows Written':>14}")
    log.info("-" * 30)
    for ds, rows in results.items():
        flag = "  ✓" if rows > 0 else "  ✗ (check log)"
        log.info(f"{ds:<10}  {rows:>14,}{flag}")
    log.info("-" * 30)
    log.info(f"{'TOTAL':<10}  {total_rows:>14,}")
    log.info(f"Wall clock: {total_elapsed:.0f}s")
    log.info("=" * 72)

    return results


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Run Eurostat ingestion pipeline")
    parser.add_argument(
        "--datasets", nargs="*",
        choices=ALL_DATASETS + ["all"],
        default=None,
        help="Datasets to run (default: all)",
    )
    args = parser.parse_args()

    target_datasets: list[str] | None = None
    if args.datasets:
        if "all" in args.datasets:
            target_datasets = ALL_DATASETS
        else:
            target_datasets = args.datasets

    run(target_datasets)
