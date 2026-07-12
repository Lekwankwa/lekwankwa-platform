"""
Extended EU27 Ingestion Runner — Parts 1 and 2

Runs in order:
  1a. EU27 Unemployment (une_rt_m) -> unemployment.json gold standard
  1b. EU27 Building Permits (sts_cobp_m) -> housing_building_permits.json gold standard
  1c. EU27 HPI (prc_hpi_q purchase-only) -> housing gold standard
  2.  Housing + Trade gap backfill

Writes no status check-ins between steps.
"""

from __future__ import annotations

import sys
import logging
from pathlib import Path

from scrapers.utilities.vault_io import get_vault_root
_SCRAPER_ROOT = get_vault_root(str(Path(__file__).resolve().parents[2] / "lekwankwa-historical-vault"))
sys.path.insert(0, str(_SCRAPER_ROOT))

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)


def main() -> None:
    from scrapers.eurostat.ingest_unemployment_eu27 import run as run_unemp
    from scrapers.eurostat.ingest_permits_eu27_v2   import run as run_permits
    from scrapers.eurostat.ingest_hpi_eu27_v2        import run as run_hpi
    from scrapers.eurostat.backfill_housing_trade_gaps import run as run_gaps

    log.info("#" * 70)
    log.info("# PART 1a — EU27 Unemployment (unemployment.json schema)")
    log.info("#" * 70)
    n_unemp = run_unemp()

    log.info("#" * 70)
    log.info("# PART 1b — EU27 Building Permits (housing_building_permits.json schema)")
    log.info("#" * 70)
    n_permits = run_permits()

    log.info("#" * 70)
    log.info("# PART 1c — EU27 HPI purchase-only (housing gold standard)")
    log.info("#" * 70)
    n_hpi = run_hpi()

    log.info("#" * 70)
    log.info("# PART 2  — Housing + Trade gap backfill")
    log.info("#" * 70)
    gap_summary = run_gaps()

    log.info("")
    log.info("=" * 70)
    log.info("EXTENDED INGESTION COMPLETE")
    log.info(f"  Part 1a unemployment rows:  {n_unemp:,}")
    log.info(f"  Part 1b permits rows:        {n_permits:,}")
    log.info(f"  Part 1c HPI rows:            {n_hpi:,}")
    log.info(f"  Part 2  housing gap rows:    {gap_summary['housing_gaps_fixed']:,}")
    log.info(f"  Part 2  trade gap rows:      {gap_summary['trade_gaps_fixed']:,}")
    log.info(f"  Grand total new rows:        "
             f"{n_unemp + n_permits + n_hpi + gap_summary['housing_gaps_fixed'] + gap_summary['trade_gaps_fixed']:,}")
    log.info("=" * 70)


if __name__ == "__main__":
    main()
