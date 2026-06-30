"""
ALFRED PIT Retrofit Orchestrator — All 5 Datasets

Runs the full ALFRED vintage retrofit for:
  1. Wages & Employment
  2. Housing
  3. Global Macro (FRED series + IMF WEO patch)
  4. Food Pricing
  5. Trade Flows

Then runs the live revision detector to capture any current-day revisions.

Usage:
    python scrapers/alfred_pit/run_all_retrofits.py

Author: Lekwankwa Corporation
Date: 2026-06-16
"""

import logging
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from scrapers.utilities.vault_io import get_vault_root

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout),
              logging.FileHandler("alfred_retrofit_all.log")],
)
logger = logging.getLogger(__name__)


def main() -> None:
    start = datetime.now(timezone.utc)
    logger.info("=" * 70)
    logger.info("ALFRED PIT RETROFIT — ALL 5 DATASETS")
    logger.info(f"Started: {start.isoformat()}")
    logger.info("=" * 70)

    results = {}

    # ── 1. Wages & Employment ─────────────────────────────────────────────────
    logger.info("\n[1/5] WAGES & EMPLOYMENT")
    try:
        from scrapers.alfred_pit.retrofit_wages import run as run_wages
        run_wages()
        results["wages"] = "OK"
    except Exception as exc:
        logger.error(f"Wages retrofit failed: {exc}")
        results["wages"] = f"ERROR: {exc}"

    # ── 2. Housing ────────────────────────────────────────────────────────────
    logger.info("\n[2/5] HOUSING")
    try:
        from scrapers.alfred_pit.retrofit_housing import run as run_housing
        run_housing()
        results["housing"] = "OK"
    except Exception as exc:
        logger.error(f"Housing retrofit failed: {exc}")
        results["housing"] = f"ERROR: {exc}"

    # ── 3. Global Macro ───────────────────────────────────────────────────────
    logger.info("\n[3/5] GLOBAL MACRO")
    try:
        from scrapers.alfred_pit.retrofit_global_macro import run as run_macro
        run_macro()
        results["global_macro"] = "OK"
    except Exception as exc:
        logger.error(f"Global macro retrofit failed: {exc}")
        results["global_macro"] = f"ERROR: {exc}"

    # ── 4. Food Pricing ───────────────────────────────────────────────────────
    logger.info("\n[4/5] FOOD PRICING")
    try:
        from scrapers.alfred_pit.retrofit_food_pricing import run as run_food
        run_food()
        results["food_pricing"] = "OK"
    except Exception as exc:
        logger.error(f"Food pricing retrofit failed: {exc}")
        results["food_pricing"] = f"ERROR: {exc}"

    # ── 5. Trade Flows ────────────────────────────────────────────────────────
    logger.info("\n[5/5] TRADE FLOWS")
    try:
        from scrapers.alfred_pit.retrofit_trade_flows import run as run_trade
        run_trade()
        results["trade_flows"] = "OK"
    except Exception as exc:
        logger.error(f"Trade flows retrofit failed: {exc}")
        results["trade_flows"] = f"ERROR: {exc}"

    # ── Summary ───────────────────────────────────────────────────────────────
    elapsed = (datetime.now(timezone.utc) - start).total_seconds()
    logger.info("\n" + "=" * 70)
    logger.info("RETROFIT COMPLETE")
    logger.info(f"Elapsed: {elapsed:.1f}s")
    logger.info("=" * 70)
    for name, status in results.items():
        marker = "[OK]" if status == "OK" else "[FAIL]"
        logger.info(f"  {marker} {name}: {status}")


if __name__ == "__main__":
    main()
