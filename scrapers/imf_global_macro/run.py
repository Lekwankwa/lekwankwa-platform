"""
scrapers/imf_global_macro/run.py — Lekwankwa Corporation
Cloud Scheduler entry point for global_macro (IMF WEO).

IMF WEO publishes in April and October. The release calendar check
exits cleanly on all other days — no wasted API calls.

Usage:
    python scrapers/imf_global_macro/run.py --country ALL
    python scrapers/imf_global_macro/run.py --country USA --dry-run
"""
from __future__ import annotations

import argparse
import logging
import sys
from datetime import date
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from tools.secrets import load_all_secrets_to_env
load_all_secrets_to_env()

from tools.release_calendar_extractor import is_release_due

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
log = logging.getLogger(__name__)

PRODUCT = "global_macro"
SOURCE  = "imf_weo"
TODAY   = date.today().isoformat()

# IMF WEO covers all 32 countries via one API call — country=ALL triggers a
# single run of the scraper which writes all countries.
SUPPORTED_COUNTRIES = ["ALL", "USA"]


def main():
    parser = argparse.ArgumentParser(
        description="global_macro IMF WEO cloud scraper entry point"
    )
    parser.add_argument("--country", required=True,
                        help="ALL (all WEO countries) or specific ISO code")
    parser.add_argument("--mode", choices=["incremental", "full"], default="incremental")
    parser.add_argument("--since", type=str, default=None, metavar="YYYY or YYYY-MM")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    log.info("=" * 60)
    log.info("GLOBAL MACRO (IMF WEO) — run.py  [%s]", TODAY)
    log.info("Country: %s | Mode: %s", args.country, args.mode)
    log.info("=" * 60)

    # Release check — IMF WEO only publishes in April and October
    if not is_release_due(product=PRODUCT, country=args.country,
                          source=SOURCE, as_of=TODAY):
        log.info("No IMF WEO release due today — skipping.")
        sys.exit(0)

    if args.dry_run:
        log.info("[DRY-RUN] Would scrape %s/%s/%s", PRODUCT, args.country, SOURCE)
        sys.exit(0)

    try:
        from scrapers.imf_global_macro.imf_datamapper_usa_scraper import main as imf_main
        imf_main(mode=args.mode, since=args.since)
        log.info("Completed %s/%s", PRODUCT, args.country)
        from tools.vault_audit import run_9_stage_validation
        from tools.live_feed_audit import run_post_delta_audit
        from tools.trigger_downstream import trigger_all_metadata
        val = run_9_stage_validation(product=PRODUCT, country=args.country)
        if val.severity not in ("CRITICAL", "HIGH"):
            audit = run_post_delta_audit(product=PRODUCT, country=args.country)
            if audit.severity not in ("CRITICAL", "HIGH"):
                trigger_all_metadata()
        sys.exit(0)
    except Exception as exc:
        log.error("Failed %s/%s: %s", PRODUCT, args.country, exc, exc_info=True)
        try:
            from tools.self_healing.handler import handle_exception
            handle_exception(
                program=__file__, exception=exc,
                context={"product": PRODUCT, "country": args.country,
                         "source": SOURCE, "run_date": TODAY, "layer": "SCRAPER"},
            )
        except ImportError:
            pass
        sys.exit(1)


if __name__ == "__main__":
    main()
